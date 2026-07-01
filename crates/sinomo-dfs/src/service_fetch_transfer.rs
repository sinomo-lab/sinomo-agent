//! DFS fetch-transfer handlers for pending write recovery.
//!
//! Fetch transfers move blob bytes from an already-durable replica back to a
//! node that needs them before staging or repairing a pending write. This
//! module owns fetch requests, inline/chunked fetch responses, and the retry
//! wakeups that follow successful or failed fetches.

use sinomo_node::NodeId;

use crate::transfer::{
    DFS_BLOB_CHUNK_BYTES, InProgressFetchBlobTransfer, blob_chunk_count_for_size, chunk_hash,
    validate_chunk_bytes, validate_chunk_hash, wake_repair_retry_state,
};
#[cfg(test)]
use crate::transfer::{blob_chunk_count, fetch_response_window_end};
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

        let mut sent_chunked_response = false;
        let result = match open_result {
            Ok((total_size_bytes, reader)) => {
                if total_size_bytes > DFS_BLOB_INLINE_LIMIT_BYTES as u64 {
                    let reader = if command.start_chunk_index == 0 {
                        reader
                    } else {
                        self.open_fetch_response_chunk_reader(total_size_bytes, &command)?
                    };
                    self.send_fetch_blob_response_chunks_from_reader(
                        &requester_node_id,
                        &command,
                        reader,
                        total_size_bytes,
                    )?;
                    sent_chunked_response = true;
                    DfsFetchBlobResult::Found {
                        content_hash: command.content_hash.clone(),
                        bytes: Vec::new(),
                    }
                } else if command.start_chunk_index > 0 {
                    match self.open_fetch_response_chunk_reader(total_size_bytes, &command) {
                        Ok(reader) => {
                            self.send_fetch_blob_response_chunks_from_reader(
                                &requester_node_id,
                                &command,
                                reader,
                                total_size_bytes,
                            )?;
                            sent_chunked_response = true;
                            DfsFetchBlobResult::Found {
                                content_hash: command.content_hash.clone(),
                                bytes: Vec::new(),
                            }
                        }
                        Err(error) => DfsFetchBlobResult::Rejected {
                            reason: error.to_string(),
                        },
                    }
                } else {
                    let mut reader = reader;
                    match read_small_blob_from_reader(
                        &command.content_hash,
                        total_size_bytes,
                        &mut reader,
                    ) {
                        Ok(bytes) => DfsFetchBlobResult::Found {
                            content_hash: command.content_hash.clone(),
                            bytes,
                        },
                        Err(error) => DfsFetchBlobResult::Rejected {
                            reason: error.to_string(),
                        },
                    }
                }
            }
            Err(DfsError::ContentStore { .. }) => DfsFetchBlobResult::NotFound,
            Err(error) => DfsFetchBlobResult::Rejected {
                reason: error.to_string(),
            },
        };
        match &result {
            DfsFetchBlobResult::Found { .. } if sent_chunked_response => {}
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

    #[cfg(test)]
    pub(crate) fn send_fetch_blob_response_chunks(
        &mut self,
        requester_node_id: &NodeId,
        command: &DfsFetchBlobCommand,
        bytes: &[u8],
    ) -> Result<(), DfsError> {
        let total_chunks = blob_chunk_count(bytes)?;
        let end_chunk_index =
            fetch_response_window_end(command.start_chunk_index, total_chunks, command.max_chunks);
        for (chunk_index, chunk) in bytes
            .chunks(DFS_BLOB_CHUNK_BYTES)
            .enumerate()
            .skip(command.start_chunk_index as usize)
            .take(end_chunk_index.saturating_sub(command.start_chunk_index) as usize)
        {
            let sent = self.send_dfs_message_if_routable(
                requester_node_id,
                DfsNetworkMessage::FetchBlobResponseChunk(DfsFetchBlobResponseChunk {
                    request_id: command.request_id.clone(),
                    intent_id: command.intent_id.clone(),
                    object_id: command.object_id.clone(),
                    version: command.version.clone(),
                    source_node_id: self.node_id.clone(),
                    content_hash: command.content_hash.clone(),
                    total_size_bytes: bytes.len() as u64,
                    chunk_index: chunk_index as u32,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(chunk)),
                    bytes: chunk.to_vec(),
                }),
            )?;
            if !sent {
                break;
            }
        }
        Ok(())
    }

    fn send_fetch_blob_response_chunks_from_reader(
        &mut self,
        requester_node_id: &NodeId,
        command: &DfsFetchBlobCommand,
        reader: ObjectVersionReader,
        total_size_bytes: u64,
    ) -> Result<(), DfsError> {
        let source_node_id = self.node_id.clone();
        self.send_object_version_response_chunks_from_reader(
            requester_node_id,
            command.start_chunk_index,
            command.max_chunks,
            total_size_bytes,
            &command.content_hash,
            reader,
            |chunk_index, total_chunks, bytes| {
                DfsNetworkMessage::FetchBlobResponseChunk(DfsFetchBlobResponseChunk {
                    request_id: command.request_id.clone(),
                    intent_id: command.intent_id.clone(),
                    object_id: command.object_id.clone(),
                    version: command.version.clone(),
                    source_node_id: source_node_id.clone(),
                    content_hash: command.content_hash.clone(),
                    total_size_bytes,
                    chunk_index,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(&bytes)),
                    bytes,
                })
            },
        )
    }

    fn open_fetch_response_chunk_reader(
        &self,
        total_size_bytes: u64,
        command: &DfsFetchBlobCommand,
    ) -> Result<ObjectVersionReader, DfsError> {
        let total_chunks = blob_chunk_count_for_size(total_size_bytes)?;
        if command.start_chunk_index >= total_chunks {
            return Err(DfsError::Distributed {
                message: format!(
                    "DFS fetch resume chunk {} is outside total chunks {} for {} {}",
                    command.start_chunk_index, total_chunks, command.object_id, command.version
                ),
            });
        }
        let start_offset = u64::from(command.start_chunk_index) * DFS_BLOB_CHUNK_BYTES as u64;
        let remaining_bytes = total_size_bytes.saturating_sub(start_offset);
        let remaining_bytes = usize::try_from(remaining_bytes).map_err(|_| {
            DfsError::Distributed {
                message: format!(
                    "DFS fetch response range is too large to fit in memory window: {remaining_bytes} bytes"
                ),
            }
        })?;
        self.open_content_hash_range_stream(
            &command.object_id,
            &command.version,
            &command.content_hash,
            Some(total_size_bytes),
            start_offset,
            remaining_bytes,
        )
    }

    pub(crate) fn fetch_window_end_chunk_index(
        start_chunk_index: u32,
        max_chunks: u32,
    ) -> Option<u32> {
        (max_chunks > 0).then(|| start_chunk_index.saturating_add(max_chunks))
    }

    pub(crate) fn read_window_end_chunk_index(
        start_chunk_index: u32,
        max_chunks: u32,
    ) -> Option<u32> {
        (max_chunks > 0).then(|| start_chunk_index.saturating_add(max_chunks))
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

    pub(crate) fn handle_fetch_blob_response_chunk(
        &mut self,
        frame_source_node_id: NodeId,
        chunk: DfsFetchBlobResponseChunk,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if frame_source_node_id != chunk.source_node_id {
            let reason = format!(
                "DFS fetch response source mismatch: frame source {}, payload source {}",
                frame_source_node_id, chunk.source_node_id
            );
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: chunk.intent_id,
                object_id: chunk.object_id,
                source_node_id: frame_source_node_id,
                reason,
            }));
        }
        self.restore_pending_write_for_intent_if_missing(&chunk.intent_id, &chunk.object_id)?;
        let Some(pending) = self.pending_writes.get(&chunk.intent_id).cloned() else {
            let reason = format!(
                "distributed DFS error: missing pending write for fetched blob chunk: {}",
                chunk.intent_id
            );
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: chunk.intent_id,
                object_id: chunk.object_id,
                source_node_id: chunk.source_node_id,
                reason,
            }));
        };
        let restored_fetch_response =
            self.restored_fetch_response_source_is_authorized_without_inflight(
                &chunk.intent_id,
                &frame_source_node_id,
            ) && pending.fetch_inflight_from.is_empty()
                && pending.fetch_inflight_request_ids.is_empty();
        if !pending.fetch_inflight_from.contains(&frame_source_node_id) && !restored_fetch_response
        {
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: chunk.intent_id,
                object_id: chunk.object_id,
                source_node_id: frame_source_node_id,
                reason: "unexpected DFS fetch response source".to_string(),
            }));
        }
        if let Some(expected_request_id) = pending
            .fetch_inflight_request_ids
            .get(&frame_source_node_id)
        {
            if expected_request_id != &chunk.request_id {
                return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                    intent_id: chunk.intent_id,
                    object_id: chunk.object_id,
                    source_node_id: frame_source_node_id,
                    reason: "unexpected DFS fetch response request id".to_string(),
                }));
            }
        } else if !restored_fetch_response {
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: chunk.intent_id,
                object_id: chunk.object_id,
                source_node_id: frame_source_node_id,
                reason: "unexpected DFS fetch response request id".to_string(),
            }));
        }
        if pending.object_id != chunk.object_id
            || pending.version != chunk.version
            || pending.content_hash != chunk.content_hash
        {
            let reason = DfsError::ContentHashMismatch {
                expected: pending.content_hash,
                actual: chunk.content_hash.clone(),
            }
            .to_string();
            return self.reject_fetch_response_chunk_and_fail_source(chunk, reason);
        }
        if let Some(local_size_bytes) = self.local_object_version_manifest_size_if_matches(
            &chunk.object_id,
            &chunk.version,
            &chunk.content_hash,
        ) {
            self.clear_fetch_response_transfer_state(
                &chunk.intent_id,
                &chunk.source_node_id,
                &chunk.request_id,
            );
            return Ok(Some(self.complete_fetched_blob(
                chunk.intent_id,
                chunk.object_id,
                chunk.version,
                frame_source_node_id,
                chunk.content_hash,
                local_size_bytes,
            )?));
        }
        if chunk.total_chunks == 0 {
            let reason = format!(
                "chunked DFS fetch response {} has no chunks",
                chunk.request_id
            );
            return self.reject_fetch_response_chunk_and_fail_source(chunk, reason);
        }
        if pending.size_bytes != 0 && chunk.total_size_bytes != pending.size_bytes {
            let reason = format!(
                "chunked DFS fetch response size does not match pending write: expected {}, actual {}",
                pending.size_bytes, chunk.total_size_bytes
            );
            return self.reject_fetch_response_chunk_and_fail_source(chunk, reason);
        }
        if let Err(error) = validate_chunk_bytes(
            "chunked DFS fetch response",
            chunk.total_size_bytes,
            chunk.chunk_index,
            chunk.total_chunks,
            &chunk.bytes,
        )
        .and_then(|()| {
            validate_chunk_hash(
                "chunked DFS fetch response",
                chunk.chunk_index,
                &chunk.bytes,
                chunk.chunk_hash.as_deref(),
            )
        }) {
            self.incoming_fetch_transfers.remove(&chunk.request_id);
            return self.reject_fetch_response_chunk_and_fail_source(chunk, error.to_string());
        }

        if !self
            .incoming_fetch_transfers
            .contains_key(&chunk.request_id)
        {
            let assembler = match self.content.begin_chunked_object_version(
                chunk.object_id.as_str(),
                chunk.version.as_str(),
                &chunk.content_hash,
                chunk.total_size_bytes,
                chunk.total_chunks,
                DFS_BLOB_CHUNK_BYTES,
            ) {
                Ok(assembler) => assembler,
                Err(error) => {
                    return self
                        .reject_fetch_response_chunk_and_fail_source(chunk, error.to_string());
                }
            };
            let transfer = match InProgressFetchBlobTransfer::new(
                chunk.object_id.clone(),
                chunk.version.clone(),
                chunk.content_hash.clone(),
                chunk.total_size_bytes,
                chunk.total_chunks,
                assembler,
            ) {
                Ok(transfer) => transfer,
                Err(error) => {
                    return self
                        .reject_fetch_response_chunk_and_fail_source(chunk, error.to_string());
                }
            };
            self.incoming_fetch_transfers
                .insert(chunk.request_id.clone(), transfer);
        }
        let transfer = self
            .incoming_fetch_transfers
            .get_mut(&chunk.request_id)
            .expect("transfer inserted");
        if transfer.object_id != chunk.object_id
            || transfer.version != chunk.version
            || transfer.content_hash != chunk.content_hash
            || transfer.total_size_bytes() != chunk.total_size_bytes
        {
            let reason = format!(
                "conflicting DFS fetch response chunk metadata for {}",
                chunk.request_id
            );
            return self.reject_fetch_response_chunk_and_fail_source(chunk, reason);
        }
        let request_id = chunk.request_id.clone();
        let intent_id = chunk.intent_id.clone();
        let object_id = chunk.object_id.clone();
        let version = chunk.version.clone();
        let source_node_id = chunk.source_node_id.clone();
        let content_hash = chunk.content_hash.clone();
        let transfer_complete =
            match transfer.insert_chunk(chunk.chunk_index, chunk.total_chunks, chunk.bytes.clone())
            {
                Ok(true) => true,
                Ok(false) => false,
                Err(error) => {
                    return self
                        .reject_fetch_response_chunk_and_fail_source(chunk, error.to_string());
                }
            };
        let next_chunk_index = transfer.contiguous_received_chunk_count();
        if let Some(pending) = self.pending_writes.get_mut(&intent_id) {
            let entry = pending
                .fetch_next_chunk_indexes
                .entry(source_node_id.clone())
                .or_insert(0);
            *entry = (*entry).max(next_chunk_index);
        }
        if !transfer_complete {
            self.request_next_fetch_window_if_due(
                &intent_id,
                &source_node_id,
                next_chunk_index,
                chunk.total_chunks,
            )?;
            return Ok(None);
        }
        let transfer = self
            .incoming_fetch_transfers
            .remove(&request_id)
            .expect("completed fetch transfer");
        if let Some(pending) = self.pending_writes.get_mut(&intent_id) {
            pending.fetch_inflight_from.remove(&source_node_id);
            pending.fetch_inflight_request_ids.remove(&source_node_id);
            pending.fetch_inflight_started_ticks.remove(&source_node_id);
            pending.fetch_next_chunk_indexes.remove(&source_node_id);
            pending
                .fetch_window_end_chunk_indexes
                .remove(&source_node_id);
        }
        if let Some(local_size_bytes) =
            self.local_object_version_manifest_size_if_matches(&object_id, &version, &content_hash)
        {
            return Ok(Some(self.complete_fetched_blob(
                intent_id,
                object_id,
                version,
                source_node_id,
                content_hash,
                local_size_bytes,
            )?));
        }
        let manifest = match self
            .content
            .commit_chunked_object_version(transfer.chunks.assembler)
        {
            Ok(manifest) => manifest,
            Err(error) => {
                self.record_fetch_failed_source_and_wake_retries(&intent_id, &source_node_id);
                return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                    intent_id,
                    object_id,
                    source_node_id,
                    reason: error.to_string(),
                }));
            }
        };
        if manifest.content_hash != content_hash {
            self.record_fetch_failed_source_and_wake_retries(&intent_id, &source_node_id);
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id,
                object_id,
                source_node_id,
                reason: DfsError::ContentHashMismatch {
                    expected: content_hash,
                    actual: manifest.content_hash,
                }
                .to_string(),
            }));
        }
        Ok(Some(self.complete_fetched_blob(
            intent_id,
            object_id,
            version,
            source_node_id,
            manifest.content_hash,
            manifest.size_bytes,
        )?))
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

    fn reject_fetch_response_chunk_and_fail_source(
        &mut self,
        chunk: DfsFetchBlobResponseChunk,
        reason: String,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        self.clear_fetch_response_transfer_state(
            &chunk.intent_id,
            &chunk.source_node_id,
            &chunk.request_id,
        );
        self.record_fetch_failed_source_and_wake_retries(&chunk.intent_id, &chunk.source_node_id);
        Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
            intent_id: chunk.intent_id,
            object_id: chunk.object_id,
            source_node_id: chunk.source_node_id,
            reason,
        }))
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
        let stale_request_ids = self
            .pending_writes
            .get(intent_id)
            .map(|pending| {
                pending
                    .fetch_inflight_request_ids
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for request_id in stale_request_ids {
            self.incoming_fetch_transfers.remove(&request_id);
        }
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
            pending.fetch_next_chunk_indexes.clear();
            pending.fetch_window_end_chunk_indexes.clear();
            pending.fetch_failed_from.clear();
        }
    }

    fn clear_fetch_response_transfer_state(
        &mut self,
        intent_id: &str,
        source_node_id: &NodeId,
        request_id: &str,
    ) {
        self.incoming_fetch_transfers.remove(request_id);
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            pending.fetch_inflight_from.remove(source_node_id);
            pending.fetch_inflight_request_ids.remove(source_node_id);
            pending.fetch_inflight_started_ticks.remove(source_node_id);
            pending.fetch_next_chunk_indexes.remove(source_node_id);
            pending
                .fetch_window_end_chunk_indexes
                .remove(source_node_id);
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
        if let Some(request_id) = stale_request_id {
            self.incoming_fetch_transfers.remove(&request_id);
        }
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            pending.fetch_inflight_from.remove(source_node_id);
            pending.fetch_inflight_started_ticks.remove(source_node_id);
            pending.fetch_next_chunk_indexes.remove(source_node_id);
            pending
                .fetch_window_end_chunk_indexes
                .remove(source_node_id);
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
