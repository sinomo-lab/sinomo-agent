//! DFS fetch-transfer handlers for pending write recovery.
//!
//! Fetch transfers move blob bytes from an already-durable replica back to a
//! node that needs them before staging or repairing a pending write. This
//! module owns fetch requests, reader-backed responses, and the
//! retry wakeups that follow successful or failed fetches.

use sinomo_core::node::NodeId;

use crate::transfer::wake_repair_retry_state;
use crate::*;

/// Process-local proof; only a guarded current metadata read can construct it.
/// The opened reader owns its content snapshot/inline leases until response
/// delivery or cancellation drops the attachment. It is never a wire flag.
pub(crate) struct VerifiedDfsBlobFetch {
    local_node_id: NodeId,
    requester: NodeId,
    command: DfsFetchBlobCommand,
    opened: Result<(u64, ObjectVersionReader), DfsError>,
    response_owner: Option<Box<dyn std::any::Any + Send>>,
}

impl VerifiedDfsBlobFetch {
    pub(crate) fn with_response_owner<G: Send + 'static>(mut self, owner: G) -> Self {
        self.response_owner = Some(Box::new(owner));
        self
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn verify_immutable_blob_fetch(
        &self,
        backend: &dyn DfsMetadataBackend,
        requester: &NodeId,
        command: &DfsFetchBlobCommand,
        certified_shard: &DfsMetadataShardConfig,
        certified_shards: &[DfsMetadataShardConfig],
    ) -> Result<Option<VerifiedDfsBlobFetch>, DfsError> {
        let Some(intent) = backend.get_intent(&command.object_id, &command.intent_id)? else {
            return Ok(None);
        };
        let Some(object) = backend
            .get_object(&command.object_id)?
            .or(intent.object_metadata.clone())
        else {
            return Ok(None);
        };
        if select_metadata_shard_config_for_path(
            &object.file_system_id,
            certified_shards,
            &object.path,
        )?
        .as_ref()
            != Some(certified_shard)
            || command.authority_shard.as_ref() != Some(certified_shard)
        {
            return Ok(None);
        }
        let projected_primary = certified_shard.primary_node_id == *requester;
        // An exact physical shard can retain historical rows after a move.
        // Coordinator/desired-replica rights are bound to the stored immutable
        // intent itself. Additional current-primary/source-only permissions
        // require the global router's current-object provenance, so preserve
        // that handler until this read lane can prove them independently.
        if intent.coordinator_node_id != *requester
            && !intent.desired_replica_node_ids.contains(requester)
        {
            return Ok(None);
        }
        if !intent.desired_replica_node_ids.contains(&self.node_id)
            && backend
                .get_receipt_on_shard(
                    &DfsMetadataShardId(certified_shard.shard_id.clone()),
                    &command.object_id,
                    &intent.target_version,
                    &self.node_id,
                )?
                .is_none()
        {
            return Ok(None);
        }
        let opened = crate::service_replica::ensure_blob_fetch_intent_permission(
            &self.node_id,
            requester,
            command,
            &intent,
            projected_primary,
            projected_primary,
            || {
                Ok(backend
                    .get_receipt_on_shard(
                        &DfsMetadataShardId(certified_shard.shard_id.clone()),
                        &command.object_id,
                        &intent.target_version,
                        &self.node_id,
                    )?
                    .is_some())
            },
        )
        .and_then(|()| {
            if let Some(expected) = intent.content_hash.as_deref()
                && expected != command.content_hash
            {
                return Err(DfsError::ContentHashMismatch {
                    expected: expected.to_string(),
                    actual: command.content_hash.clone(),
                });
            }
            self.open_content_hash_stream(
                &command.object_id,
                &command.version,
                &command.content_hash,
                None,
            )
        });
        Ok(Some(VerifiedDfsBlobFetch {
            local_node_id: self.node_id.clone(),
            requester: requester.clone(),
            command: command.clone(),
            opened,
            response_owner: None,
        }))
    }

    pub(crate) fn handle_verified_blob_fetch(
        &mut self,
        requester: NodeId,
        command: DfsFetchBlobCommand,
        verified: VerifiedDfsBlobFetch,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        if verified.local_node_id != self.node_id
            || verified.requester != requester
            || verified.command != command
        {
            return Err(DfsError::UnauthorizedReplicaCommand {
                source_node_id: requester,
                reason: "immutable fetch proof does not match request".to_string(),
            });
        }
        self.finish_fetch_blob(requester, command, verified.opened, verified.response_owner)
    }

    pub(crate) fn handle_fetch_blob(
        &mut self,
        requester_node_id: NodeId,
        command: DfsFetchBlobCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let open_result = self
            .ensure_blob_fetch_authorized(&requester_node_id, &command)
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

        self.finish_fetch_blob(requester_node_id, command, open_result, None)
    }

    fn finish_fetch_blob(
        &mut self,
        requester_node_id: NodeId,
        command: DfsFetchBlobCommand,
        open_result: Result<(u64, ObjectVersionReader), DfsError>,
        response_owner: Option<Box<dyn std::any::Any + Send>>,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let mut sent_body_response = false;
        let result = match open_result {
            Ok((total_size_bytes, reader)) => {
                self.send_fetch_blob_response_attachment(
                    &requester_node_id,
                    &command,
                    reader,
                    total_size_bytes,
                    response_owner,
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
        response_owner: Option<Box<dyn std::any::Any + Send>>,
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
        let mut attachment =
            DfsOutboundAttachment::from_reader(reader, total_size_bytes, &command.content_hash);
        if let Some(owner) = response_owner {
            attachment
                .source
                .hold_lifetime_guard(owner)
                .map_err(DfsError::from_network)?;
        }
        self.transport
            .send_routed_call(
                requester_node_id,
                DfsRoutedCall::with_attachment(routed, attachment),
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
        let Some(pending) = self.writes.pending_writes.get(&response.intent_id).cloned() else {
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
        // Expiry releases scheduling activity, but retains the original named
        // request. Its exact authenticated reply still belongs to this owner.
        let expected_request_id = pending
            .fetch_inflight_request_ids
            .get(&frame_source_node_id);
        let restored_fetch_response = pending.fetch_inflight_from.is_empty()
            && pending.fetch_inflight_request_ids.is_empty()
            && self.restored_fetch_response_source_is_authorized_without_inflight(
                &response,
                &frame_source_node_id,
            )?;
        if !pending.fetch_inflight_from.contains(&frame_source_node_id)
            && expected_request_id.is_none()
            && !restored_fetch_response
        {
            return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                source_node_id: frame_source_node_id,
                reason: "unexpected DFS fetch response source".to_string(),
            });
        }
        if let Some(expected_request_id) = expected_request_id {
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
        let transfer_started_at = self
            .passive_fetch_started_at
            .remove(&(response.intent_id.clone(), frame_source_node_id.clone()));
        if let Some(pending) = self.writes.pending_writes.get_mut(&response.intent_id) {
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
                    if let Some(started_at) = transfer_started_at {
                        self.placement_snapshot_cache.observe_transfer(
                            &frame_source_node_id,
                            &self.node_id,
                            local_size_bytes,
                            started_at.elapsed(),
                        );
                    }
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
                if let Some(started_at) = transfer_started_at {
                    self.placement_snapshot_cache.observe_transfer(
                        &frame_source_node_id,
                        &self.node_id,
                        bytes.len().try_into().unwrap_or(u64::MAX),
                        started_at.elapsed(),
                    );
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
        &mut self,
        response: &DfsFetchBlobResponse,
        source_node_id: &NodeId,
    ) -> Result<bool, DfsError> {
        let Some(intent) = self.coordinator.intent(&response.intent_id) else {
            return Ok(false);
        };
        if intent.desired_replica_nodes.contains(source_node_id)
            && self
                .coordinator
                .receipts_for_intent(&response.intent_id)
                .iter()
                .any(|receipt| receipt.replica_node_id == *source_node_id)
        {
            return Ok(true);
        }
        // The designated requester can be excluded from W/N by placement.
        // Its immutable staged bytes remain an existing transfer source, but
        // a newly reconstructed service has no volatile in-flight request map.
        // Admit that source only for this exact durable active write at the
        // independently current metadata primary. The normal ingest path then
        // verifies bytes and records a receipt only for a selected local node.
        if !self
            .stage_blob_sources(&response.intent_id)
            .contains(source_node_id)
        {
            return Ok(false);
        }
        let Some(record) = self
            .metadata_backend
            .as_ref()
            .map(|backend| backend.get_intent(&response.object_id, &response.intent_id))
            .transpose()?
            .flatten()
        else {
            return Ok(false);
        };
        let DfsFetchBlobResult::Found { content_hash, .. } = &response.result else {
            return Ok(false);
        };
        if record.coordinator_node_id != *source_node_id
            || record.object_id != response.object_id
            || record.target_version != response.version
            || record.content_hash.as_deref() != Some(content_hash.as_str())
        {
            return Ok(false);
        }
        Ok(self.local_pending_write_authority_shard(&record)?.is_some())
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
        if let Some(pending) = self.writes.pending_writes.get_mut(&intent_id)
            && pending.size_bytes == 0
        {
            pending.size_bytes = size_bytes;
        }
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
        let file_system_id = intent.file_system_id;
        self.with_placement_intent_authority(intent_id, |service| {
            if !service.ensure_local_repair_manifest(object_id, version, content_hash)? {
                return Ok(());
            }
            service.put_metadata_receipt(
                &file_system_id,
                object_id,
                version,
                &local_node_id,
                content_hash,
            )?;
            service
                .coordinator
                .record_durable_receipt(intent_id, local_node_id)?;
            service.try_apply_committed_intent_if_ready(intent_id)?;
            service.send_untracked_completed_routed_write_response_if_committed(
                intent_id, object_id,
            )?;
            service.cleanup_committed_pending_write_if_repaired(intent_id);
            Ok(())
        })?;
        Ok(())
    }

    fn wake_pending_retries_after_fetch_success(&mut self, intent_id: &str) {
        if let Some(pending) = self.writes.pending_writes.get_mut(intent_id) {
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
            .writes
            .pending_writes
            .get_mut(intent_id)
            .and_then(|pending| pending.fetch_inflight_request_ids.remove(source_node_id));
        let _ = stale_request_id;
        if let Some(pending) = self.writes.pending_writes.get_mut(intent_id) {
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
