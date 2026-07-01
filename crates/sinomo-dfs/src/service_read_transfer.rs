//! DFS networked read transfer message handlers.
//!
//! This module owns remote read serving and response assembly: read requests,
//! bulk read requests, inline responses, range responses, and chunked read
//! continuation windows.

use std::io::Read;
use std::time::Instant;

use sinomo_node::NodeId;

use crate::transfer::{
    DFS_BLOB_CHUNK_BYTES, InProgressReadMemoryTransfer, InProgressReadTransfer,
    PendingNetworkedRead, blob_chunk_count, blob_chunk_count_for_size, blob_chunk_len, chunk_hash,
    chunk_prefix_bytes, dfs_byte_count_to_usize, fetch_response_window_end,
    range_transfer_size_bytes, read_response_window_range_len, validate_chunk_bytes,
    validate_chunk_hash, validate_chunked_transfer_shape,
};
use crate::*;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn handle_networked_read_request(
        &mut self,
        requester_node_id: NodeId,
        request: DfsReadRequest,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let requested_range = match (request.range_offset, request.range_max_bytes) {
            (Some(offset), Some(max_bytes)) => Ok(Some((offset, max_bytes))),
            (None, None) => Ok(None),
            _ => Err("DFS read range request must include both offset and max bytes".to_string()),
        };
        let mut sent_chunked_response = false;
        let result = match requested_range {
            Err(reason) => DfsReadResult::Rejected { reason },
            Ok(range) => {
                if let Some((result, sent_chunked)) = self.serve_networked_read_from_request_hints(
                    &requester_node_id,
                    &request,
                    range,
                )? {
                    sent_chunked_response = sent_chunked;
                    result
                } else {
                    match self.current_read_object_metadata(&request.object_id) {
                        Ok(object) if !object.current_locations.contains(&self.node_id) => {
                            DfsReadResult::Rejected {
                                reason: format!(
                                    "node {} is not a current location for DFS object {}",
                                    self.node_id, request.object_id
                                ),
                            }
                        }
                        Ok(_) => {
                            match range {
                                Some((offset, max_bytes)) => {
                                    let chunked_range = max_bytes > DFS_BLOB_INLINE_LIMIT_BYTES
                                        || request.start_chunk_index != 0
                                        || request.max_chunks != 0;
                                    if chunked_range {
                                        match self.open_local_current_object_range_stream_with_state(
                                &request.object_id,
                                offset,
                                max_bytes,
                                request.start_chunk_index,
                            )? {
                                NetworkedDfsRangeReadStreamOutcome::Found(stream)
                                    if stream.range_size_bytes > 0 =>
                                {
                                    self.send_networked_range_read_response_chunks_from_reader(
                                        &requester_node_id,
                                        &request,
                                        &stream.data,
                                        stream.range_size_bytes,
                                        stream.reader,
                                    )?;
                                    sent_chunked_response = true;
                                    DfsReadResult::RangeFound(stream.data)
                                }
                                NetworkedDfsRangeReadStreamOutcome::Found(stream) => {
                                    DfsReadResult::RangeFound(stream.data)
                                }
                                NetworkedDfsRangeReadStreamOutcome::Conflicted { conflicts } => {
                                    DfsReadResult::Rejected {
                                        reason: read_conflicts_reason(conflicts),
                                    }
                                }
                                NetworkedDfsRangeReadStreamOutcome::PendingReplication
                                | NetworkedDfsRangeReadStreamOutcome::NotFound => {
                                    DfsReadResult::NotFound
                                }
                            }
                                    } else {
                                        match self.read_local_object_range_with_state(
                                            &request.object_id,
                                            offset,
                                            max_bytes,
                                            false,
                                        )? {
                                            NetworkedDfsRangeReadOutcome::Found {
                                                data, ..
                                            } => DfsReadResult::RangeFound(data),
                                            NetworkedDfsRangeReadOutcome::Conflicted {
                                                conflicts,
                                                ..
                                            } => DfsReadResult::Rejected {
                                                reason: read_conflicts_reason(conflicts),
                                            },
                                            NetworkedDfsRangeReadOutcome::PendingReplication {
                                                ..
                                            }
                                            | NetworkedDfsRangeReadOutcome::NotConfirmed {
                                                ..
                                            }
                                            | NetworkedDfsRangeReadOutcome::NotFound => {
                                                DfsReadResult::NotFound
                                            }
                                        }
                                    }
                                }
                                None => {
                                    let stream_outcome = self
                                        .open_local_current_object_stream_with_state(
                                            &request.object_id,
                                        )?;
                                    match stream_outcome {
                                        NetworkedDfsReadStreamOutcome::Found(stream)
                                            if stream.total_size_bytes
                                                > DFS_BLOB_INLINE_LIMIT_BYTES as u64 =>
                                        {
                                            let window_outcome = if request.start_chunk_index == 0
                                                && request.max_chunks == 0
                                            {
                                                Ok(NetworkedDfsReadStreamOutcome::Found(stream))
                                            } else {
                                                self.open_local_current_object_read_window_stream_with_state(
                                        &request.object_id,
                                        request.start_chunk_index,
                                        request.max_chunks,
                                    )
                                            };
                                            match window_outcome {
                                    Ok(NetworkedDfsReadStreamOutcome::Found(stream)) => {
                                        self.send_networked_read_response_chunks_from_reader(
                                            &requester_node_id,
                                            &request,
                                            &stream.data,
                                            stream.total_size_bytes,
                                            stream.reader,
                                        )?;
                                        sent_chunked_response = true;
                                        DfsReadResult::Found(stream.data)
                                    }
                                    Ok(NetworkedDfsReadStreamOutcome::Conflicted { conflicts }) => {
                                        DfsReadResult::Rejected {
                                            reason: read_conflicts_reason(conflicts),
                                        }
                                    }
                                    Ok(NetworkedDfsReadStreamOutcome::PendingReplication)
                                    | Ok(NetworkedDfsReadStreamOutcome::NotFound) => {
                                        DfsReadResult::NotFound
                                    }
                                    Err(error) => DfsReadResult::Rejected {
                                        reason: error.to_string(),
                                    },
                                }
                                        }
                                        NetworkedDfsReadStreamOutcome::Found(_) => {
                                            if request.start_chunk_index != 0 {
                                                DfsReadResult::Rejected {
                                                    reason: format!(
                                                        "DFS read chunk window starts outside inline object: {}",
                                                        request.start_chunk_index
                                                    ),
                                                }
                                            } else {
                                                match self
                                        .read_local_object_with_state(&request.object_id, false)?
                                    {
                                        NetworkedDfsReadOutcome::Found { data, .. } => {
                                            DfsReadResult::Found(data)
                                        }
                                        NetworkedDfsReadOutcome::Conflicted {
                                            conflicts, ..
                                        } => DfsReadResult::Rejected {
                                            reason: read_conflicts_reason(conflicts),
                                        },
                                        NetworkedDfsReadOutcome::PendingReplication { .. }
                                        | NetworkedDfsReadOutcome::NotConfirmed { .. }
                                        | NetworkedDfsReadOutcome::NotFound => {
                                            DfsReadResult::NotFound
                                        }
                                    }
                                            }
                                        }
                                        NetworkedDfsReadStreamOutcome::Conflicted { conflicts } => {
                                            DfsReadResult::Rejected {
                                                reason: read_conflicts_reason(conflicts),
                                            }
                                        }
                                        NetworkedDfsReadStreamOutcome::PendingReplication
                                        | NetworkedDfsReadStreamOutcome::NotFound => {
                                            DfsReadResult::NotFound
                                        }
                                    }
                                }
                            }
                        }
                        Err(DfsError::UnknownObject { .. }) => DfsReadResult::NotFound,
                        Err(error) => return Err(error),
                    }
                }
            }
        };
        match &result {
            DfsReadResult::Found(_) if sent_chunked_response => {}
            DfsReadResult::RangeFound(_) if sent_chunked_response => {}
            DfsReadResult::Found(data)
                if data.bytes.len() > DFS_BLOB_INLINE_LIMIT_BYTES
                    && !direct_full_read_response_allowed(&request, data.bytes.len() as u64) =>
            {
                self.send_networked_read_response_chunks(&requester_node_id, &request, data)?;
            }
            _ => {
                self.send_dfs_message_if_routable(
                    &requester_node_id,
                    DfsNetworkMessage::ReadResponse(DfsReadResponse {
                        request_id: request.request_id.clone(),
                        object_id: request.object_id.clone(),
                        result: result.clone(),
                    }),
                )?;
            }
        }
        Ok(NetworkedDfsServiceEvent::RemoteReadServed {
            request_id: request.request_id,
            object_id: request.object_id,
            requester_node_id,
            result,
        })
    }

    fn serve_networked_read_from_request_hints(
        &mut self,
        requester_node_id: &NodeId,
        request: &DfsReadRequest,
        requested_range: Option<(u64, usize)>,
    ) -> Result<Option<(DfsReadResult, bool)>, DfsError> {
        let (Some(version), Some(content_hash)) =
            (request.version.as_ref(), request.content_hash.as_ref())
        else {
            return Ok(None);
        };
        let hinted_size = request.content_size_bytes;
        let object_id = request.object_id.clone();
        let version = version.clone();
        let content_hash = content_hash.clone();
        match requested_range {
            Some((offset, max_bytes)) => {
                let Some(total_size_bytes) = hinted_size else {
                    return Ok(None);
                };
                let range_offset = offset.min(total_size_bytes);
                let range_size_bytes =
                    range_transfer_size_bytes(total_size_bytes, offset, max_bytes);
                let chunked_range = max_bytes > DFS_BLOB_INLINE_LIMIT_BYTES
                    || request.start_chunk_index != 0
                    || request.max_chunks != 0;
                if chunked_range {
                    let window_prefix_bytes =
                        chunk_prefix_bytes(range_size_bytes, request.start_chunk_index);
                    let window_offset = range_offset.saturating_add(window_prefix_bytes);
                    let range_len = read_response_window_range_len(
                        request.start_chunk_index,
                        range_size_bytes,
                        request.max_chunks,
                    )?;
                    let reader = match self.content.open_blob_range_stream(
                        &content_hash,
                        window_offset,
                        range_len,
                    ) {
                        Ok(reader) => ObjectVersionReader::from_blob_range(reader),
                        Err(_) => return Ok(Some((DfsReadResult::NotFound, false))),
                    };
                    let data = DfsReadRangeData {
                        object_id,
                        version,
                        content_hash: Some(content_hash),
                        offset: range_offset,
                        total_size_bytes,
                        bytes: Vec::new(),
                    };
                    self.send_networked_range_read_response_chunks_from_reader(
                        requester_node_id,
                        request,
                        &data,
                        range_size_bytes,
                        reader,
                    )?;
                    return Ok(Some((DfsReadResult::RangeFound(data), true)));
                }
                let bytes = match self
                    .content
                    .read_blob_range(&content_hash, offset, max_bytes)
                {
                    Ok(bytes) => bytes,
                    Err(_) => return Ok(Some((DfsReadResult::NotFound, false))),
                };
                return Ok(Some((
                    DfsReadResult::RangeFound(DfsReadRangeData {
                        object_id,
                        version,
                        content_hash: Some(content_hash),
                        offset: range_offset,
                        total_size_bytes,
                        bytes,
                    }),
                    false,
                )));
            }
            None => {
                let total_size_bytes = match hinted_size {
                    Some(size) => size,
                    None => {
                        let Some(bytes) = self.read_blob_once_for_remote_response(&content_hash)?
                        else {
                            return Ok(Some((DfsReadResult::NotFound, false)));
                        };
                        return Ok(Some((
                            DfsReadResult::Found(DfsReadData {
                                object_id,
                                version,
                                content_hash: Some(content_hash),
                                bytes,
                            }),
                            false,
                        )));
                    }
                };
                if total_size_bytes > DFS_BLOB_INLINE_LIMIT_BYTES as u64
                    || request.start_chunk_index != 0
                    || request.max_chunks != 0
                {
                    if direct_full_read_response_allowed(request, total_size_bytes) {
                        let Some(bytes) = self.read_blob_for_direct_remote_response(
                            &content_hash,
                            total_size_bytes,
                        )?
                        else {
                            return Ok(Some((DfsReadResult::NotFound, false)));
                        };
                        return Ok(Some((
                            DfsReadResult::Found(DfsReadData {
                                object_id,
                                version,
                                content_hash: Some(content_hash),
                                bytes,
                            }),
                            false,
                        )));
                    }
                    let window_offset =
                        chunk_prefix_bytes(total_size_bytes, request.start_chunk_index);
                    let range_len = read_response_window_range_len(
                        request.start_chunk_index,
                        total_size_bytes,
                        request.max_chunks,
                    )?;
                    let reader = match self.content.open_blob_range_stream(
                        &content_hash,
                        window_offset,
                        range_len,
                    ) {
                        Ok(reader) => ObjectVersionReader::from_blob_range(reader),
                        Err(_) => return Ok(Some((DfsReadResult::NotFound, false))),
                    };
                    let data = DfsReadData {
                        object_id,
                        version,
                        content_hash: Some(content_hash),
                        bytes: Vec::new(),
                    };
                    self.send_networked_read_response_chunks_from_reader(
                        requester_node_id,
                        request,
                        &data,
                        total_size_bytes,
                        reader,
                    )?;
                    return Ok(Some((DfsReadResult::Found(data), true)));
                }
                let Some(bytes) = self.read_blob_once_for_remote_response(&content_hash)? else {
                    return Ok(Some((DfsReadResult::NotFound, false)));
                };
                Ok(Some((
                    DfsReadResult::Found(DfsReadData {
                        object_id,
                        version,
                        content_hash: Some(content_hash),
                        bytes,
                    }),
                    false,
                )))
            }
        }
    }

    fn read_blob_once_for_remote_response(
        &self,
        content_hash: &str,
    ) -> Result<Option<Vec<u8>>, DfsError> {
        let mut reader = match self.content.open_blob_stream(content_hash) {
            Ok(reader) => reader,
            Err(_) => return Ok(None),
        };
        let mut bytes =
            Vec::with_capacity(usize::try_from(reader.size_bytes()).unwrap_or(usize::MAX));
        reader
            .read_to_end(&mut bytes)
            .map_err(|source| DfsError::ContentStore {
                message: format!("failed to read blob {content_hash}: {source}"),
            })?;
        Ok(Some(bytes))
    }

    fn read_blob_for_direct_remote_response(
        &self,
        content_hash: &str,
        expected_size_bytes: u64,
    ) -> Result<Option<Vec<u8>>, DfsError> {
        if expected_size_bytes > DFS_DIRECT_REMOTE_READ_MAX_BYTES {
            return Ok(None);
        }
        let mut reader = match self.content.open_blob_stream(content_hash) {
            Ok(reader) => reader,
            Err(_) => return Ok(None),
        };
        if reader.size_bytes() != expected_size_bytes {
            return Ok(None);
        }
        let capacity = dfs_byte_count_to_usize("DFS direct remote read", expected_size_bytes)?;
        let mut bytes = Vec::with_capacity(capacity);
        reader
            .read_to_end(&mut bytes)
            .map_err(|source| DfsError::ContentStore {
                message: format!("failed to read blob {content_hash}: {source}"),
            })?;
        if bytes.len() != capacity {
            return Err(DfsError::ContentStore {
                message: format!(
                    "failed to read full blob {content_hash}: expected {capacity} bytes, actual {}",
                    bytes.len()
                ),
            });
        }
        Ok(Some(bytes))
    }

    pub(crate) fn handle_networked_bulk_read_request(
        &mut self,
        requester_node_id: NodeId,
        request: DfsBulkReadRequest,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let result_count = request.reads.len();
        let mut results = Vec::with_capacity(request.reads.len());
        for item in request.reads {
            let result = self.bulk_read_item_result(&item)?;
            results.push(DfsBulkReadItemResponse {
                request_id: item.request_id,
                object_id: item.object_id,
                result,
            });
        }
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::BulkReadResponse(DfsBulkReadResponse {
                request_id: request.request_id.clone(),
                results,
            }),
        )?;
        Ok(NetworkedDfsServiceEvent::RemoteBulkReadServed {
            request_id: request.request_id,
            requester_node_id,
            result_count,
        })
    }

    fn bulk_read_item_result(&self, item: &DfsBulkReadItem) -> Result<DfsReadResult, DfsError> {
        if item.content_size_bytes > DFS_BLOB_INLINE_LIMIT_BYTES as u64 {
            return Ok(DfsReadResult::Rejected {
                reason: format!(
                    "bulk read item {} is {} bytes, above inline limit {}",
                    item.object_id, item.content_size_bytes, DFS_BLOB_INLINE_LIMIT_BYTES
                ),
            });
        }
        let Some(bytes) = self.read_blob_once_for_remote_response(&item.content_hash)? else {
            return Ok(DfsReadResult::NotFound);
        };
        if bytes.len() as u64 != item.content_size_bytes {
            return Ok(DfsReadResult::Rejected {
                reason: format!(
                    "bulk read item {} size mismatch: expected {}, actual {}",
                    item.object_id,
                    item.content_size_bytes,
                    bytes.len()
                ),
            });
        }
        Ok(DfsReadResult::Found(DfsReadData {
            object_id: item.object_id.clone(),
            version: item.version.clone(),
            content_hash: Some(item.content_hash.clone()),
            bytes,
        }))
    }

    pub(crate) fn handle_networked_bulk_read_response(
        &mut self,
        source_node_id: NodeId,
        response: DfsBulkReadResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        Ok(NetworkedDfsServiceEvent::RemoteBulkReadCompleted {
            request_id: response.request_id,
            source_node_id,
            results: response.results,
        })
    }

    pub(crate) fn send_networked_read_response_chunks(
        &mut self,
        requester_node_id: &NodeId,
        request: &DfsReadRequest,
        data: &DfsReadData,
    ) -> Result<(), DfsError> {
        let total_chunks = blob_chunk_count(&data.bytes)?;
        let end_chunk_index =
            fetch_response_window_end(request.start_chunk_index, total_chunks, request.max_chunks);
        for (chunk_index, chunk) in data
            .bytes
            .chunks(DFS_BLOB_CHUNK_BYTES)
            .enumerate()
            .skip(request.start_chunk_index as usize)
            .take(end_chunk_index.saturating_sub(request.start_chunk_index) as usize)
        {
            let sent = self.send_dfs_message_if_routable(
                requester_node_id,
                DfsNetworkMessage::ReadResponseChunk(DfsReadResponseChunk {
                    request_id: request.request_id.clone(),
                    object_id: request.object_id.clone(),
                    version: data.version.clone(),
                    content_hash: data.content_hash.clone(),
                    range_offset: None,
                    object_total_size_bytes: None,
                    total_size_bytes: data.bytes.len() as u64,
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

    fn send_networked_read_response_chunks_from_reader(
        &mut self,
        requester_node_id: &NodeId,
        request: &DfsReadRequest,
        data: &DfsReadData,
        total_size_bytes: u64,
        reader: ObjectVersionReader,
    ) -> Result<(), DfsError> {
        let read_error_subject = format!("{:?}", data.content_hash);
        self.send_object_version_response_chunks_from_reader(
            requester_node_id,
            request.start_chunk_index,
            request.max_chunks,
            total_size_bytes,
            &read_error_subject,
            reader,
            |chunk_index, total_chunks, bytes| {
                DfsNetworkMessage::ReadResponseChunk(DfsReadResponseChunk {
                    request_id: request.request_id.clone(),
                    object_id: request.object_id.clone(),
                    version: data.version.clone(),
                    content_hash: data.content_hash.clone(),
                    range_offset: None,
                    object_total_size_bytes: None,
                    total_size_bytes,
                    chunk_index,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(&bytes)),
                    bytes,
                })
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn send_object_version_response_chunks_from_reader(
        &mut self,
        requester_node_id: &NodeId,
        start_chunk_index: u32,
        max_chunks: u32,
        total_size_bytes: u64,
        read_error_subject: &str,
        mut reader: ObjectVersionReader,
        mut message_for_chunk: impl FnMut(u32, u32, Vec<u8>) -> DfsNetworkMessage,
    ) -> Result<(), DfsError> {
        let total_chunks = blob_chunk_count_for_size(total_size_bytes)?;
        let end_chunk_index =
            fetch_response_window_end(start_chunk_index, total_chunks, max_chunks);
        tracing::debug!(
            target: "sinomo.dfs.transfer",
            local_node_id = %self.node_id,
            requester_node_id = %requester_node_id,
            read_error_subject = %read_error_subject,
            total_size_bytes,
            start_chunk_index,
            end_chunk_index,
            total_chunks,
            "sending DFS response chunk window"
        );
        let mut buffer = vec![0_u8; DFS_BLOB_CHUNK_BYTES];
        for chunk_index in start_chunk_index..end_chunk_index {
            let chunk_len = blob_chunk_len(total_size_bytes, chunk_index)?;
            reader
                .read_exact(&mut buffer[..chunk_len])
                .map_err(|source| DfsError::ContentStore {
                    message: format!(
                        "failed to read blob {read_error_subject} chunk {chunk_index}: {source}"
                    ),
                })?;
            let sent = self.send_dfs_message_if_routable(
                requester_node_id,
                message_for_chunk(chunk_index, total_chunks, buffer[..chunk_len].to_vec()),
            )?;
            if !sent {
                tracing::warn!(
                    target: "sinomo.dfs.transfer",
                    local_node_id = %self.node_id,
                    requester_node_id = %requester_node_id,
                    read_error_subject = %read_error_subject,
                    failed_chunk_index = chunk_index,
                    total_chunks,
                    "DFS response chunk window stopped because requester was not routable"
                );
                break;
            }
        }
        tracing::debug!(
            target: "sinomo.dfs.transfer",
            local_node_id = %self.node_id,
            requester_node_id = %requester_node_id,
            read_error_subject = %read_error_subject,
            next_chunk_index = end_chunk_index,
            total_chunks,
            "sent DFS response chunk window"
        );
        Ok(())
    }

    fn send_networked_range_read_response_chunks_from_reader(
        &mut self,
        requester_node_id: &NodeId,
        request: &DfsReadRequest,
        data: &DfsReadRangeData,
        range_size_bytes: u64,
        reader: ObjectVersionReader,
    ) -> Result<(), DfsError> {
        let total_chunks = blob_chunk_count_for_size(range_size_bytes)?;
        let max_chunks = if request.max_chunks == 0 {
            self.adaptive_read_chunk_window(requester_node_id, range_size_bytes)
        } else {
            request.max_chunks
        };
        let end_chunk_index =
            fetch_response_window_end(request.start_chunk_index, total_chunks, max_chunks);
        if end_chunk_index <= request.start_chunk_index {
            return Ok(());
        }
        let read_error_subject = format!("{:?} range", data.content_hash);
        self.send_object_version_response_chunks_from_reader(
            requester_node_id,
            request.start_chunk_index,
            max_chunks,
            range_size_bytes,
            &read_error_subject,
            reader,
            |chunk_index, total_chunks, bytes| {
                DfsNetworkMessage::ReadResponseChunk(DfsReadResponseChunk {
                    request_id: request.request_id.clone(),
                    object_id: request.object_id.clone(),
                    version: data.version.clone(),
                    content_hash: data.content_hash.clone(),
                    range_offset: Some(data.offset),
                    object_total_size_bytes: Some(data.total_size_bytes),
                    total_size_bytes: range_size_bytes,
                    chunk_index,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(&bytes)),
                    bytes,
                })
            },
        )
    }

    pub(crate) fn handle_networked_read_response(
        &mut self,
        source_node_id: NodeId,
        response: DfsReadResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let Some(pending) = self.pending_reads.get(&response.request_id).cloned() else {
            return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: response.request_id,
                object_id: response.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "unknown DFS read request".to_string(),
                },
            });
        };
        if source_node_id != pending.target_node_id {
            let reason = format!(
                "unexpected DFS read response source: expected {}, actual {}",
                pending.target_node_id, source_node_id
            );
            return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: response.request_id,
                object_id: response.object_id,
                source_node_id,
                result: DfsReadResult::Rejected { reason },
            });
        }
        self.pending_reads.remove(&response.request_id);
        self.incoming_read_memory_transfers
            .remove(&response.request_id);
        if pending.object_id != response.object_id {
            self.incoming_read_transfers.remove(&response.request_id);
            self.incoming_read_memory_transfers
                .remove(&response.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: response.request_id,
                object_id: response.object_id.clone(),
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "unexpected DFS read response object: expected {}, actual {}",
                        pending.object_id, response.object_id
                    ),
                },
            });
        }
        match &response.result {
            DfsReadResult::Found(data) => {
                if pending.range_offset.is_some() || pending.range_max_bytes.is_some() {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: "unexpected full DFS read response for range request"
                                .to_string(),
                        },
                    });
                }
                if data.object_id != pending.object_id {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "unexpected DFS read response payload object: expected {}, actual {}",
                                pending.object_id, data.object_id
                            ),
                        },
                    });
                }
                if pending.cache_response && self.pending_read_metadata_advanced(&pending)? {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: "stale DFS read response: object metadata advanced".to_string(),
                        },
                    });
                }
                if pending.cache_response
                    && !self.pending_read_source_still_current(&pending, &source_node_id)?
                {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason:
                                "stale DFS read response: source is no longer a current location"
                                    .to_string(),
                        },
                    });
                }
                if data.version != pending.version {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "stale DFS read response: expected version {}, actual {}",
                                pending.version, data.version
                            ),
                        },
                    });
                }
                if data.content_hash != pending.content_hash {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "stale DFS read response: expected content hash {:?}, actual {:?}",
                                pending.content_hash, data.content_hash
                            ),
                        },
                    });
                }
                let validation_started = Instant::now();
                let validation = if pending.cache_response {
                    self.validate_remote_read_data(data)
                } else {
                    self.validate_remote_read_data_for_pending(&pending, data)
                };
                dfs_perf_trace(format_args!(
                    "remote-read-validate request={} object={} bytes={} cache_response={} elapsed_ms={}",
                    response.request_id,
                    pending.object_id,
                    data.bytes.len(),
                    pending.cache_response,
                    validation_started.elapsed().as_millis()
                ));
                if let Err(error) = validation {
                    if matches!(error, DfsError::ContentHashMismatch { .. }) {
                        self.mark_remote_read_location_missing(
                            &pending.object_id,
                            &pending.version,
                            &pending.content_hash,
                            &source_node_id,
                        )?;
                        return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                            request_id: response.request_id,
                            object_id: response.object_id,
                            source_node_id,
                            result: DfsReadResult::Rejected {
                                reason: error.to_string(),
                            },
                        });
                    }
                    return Err(error);
                }
                if pending.cache_response
                    && let Err(error) = self.store_remote_read_data(&source_node_id, data)
                {
                    if matches!(error, DfsError::ContentHashMismatch { .. }) {
                        self.mark_remote_read_location_missing(
                            &pending.object_id,
                            &pending.version,
                            &pending.content_hash,
                            &source_node_id,
                        )?;
                        return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                            request_id: response.request_id,
                            object_id: response.object_id,
                            source_node_id,
                            result: DfsReadResult::Rejected {
                                reason: error.to_string(),
                            },
                        });
                    }
                    return Err(error);
                }
                if !pending.cache_response {
                    self.clear_transient_remote_read_location_prunes_for_object(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                    );
                }
                self.clear_pending_remote_reads_for_object_version(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                );
            }
            DfsReadResult::RangeFound(data) => {
                let (Some(requested_offset), Some(requested_max_bytes)) =
                    (pending.range_offset, pending.range_max_bytes)
                else {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: "unexpected range DFS read response for full-object request"
                                .to_string(),
                        },
                    });
                };
                if data.object_id != pending.object_id {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "unexpected DFS range read response payload object: expected {}, actual {}",
                                pending.object_id, data.object_id
                            ),
                        },
                    });
                }
                if pending.cache_response && self.pending_read_metadata_advanced(&pending)? {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: "stale DFS read response: object metadata advanced".to_string(),
                        },
                    });
                }
                if pending.cache_response
                    && !self.pending_read_source_still_current(&pending, &source_node_id)?
                {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason:
                                "stale DFS read response: source is no longer a current location"
                                    .to_string(),
                        },
                    });
                }
                if data.version != pending.version {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "stale DFS range read response: expected version {}, actual {}",
                                pending.version, data.version
                            ),
                        },
                    });
                }
                if data.content_hash != pending.content_hash {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "stale DFS range read response: expected content hash {:?}, actual {:?}",
                                pending.content_hash, data.content_hash
                            ),
                        },
                    });
                }
                if let Some(expected_size) = pending.content_size_bytes
                    && expected_size != data.total_size_bytes
                {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "DFS range read response size does not match object metadata: expected {expected_size}, actual {}",
                                data.total_size_bytes
                            ),
                        },
                    });
                }
                let expected_offset = requested_offset.min(data.total_size_bytes);
                if data.offset != expected_offset {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "DFS range read response offset mismatch: expected {expected_offset}, actual {}",
                                data.offset
                            ),
                        },
                    });
                }
                if data.bytes.len() > requested_max_bytes {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "DFS range read response exceeds requested window: expected at most {requested_max_bytes}, actual {}",
                                data.bytes.len()
                            ),
                        },
                    });
                }
                let Some(range_end) = data.offset.checked_add(data.bytes.len() as u64) else {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: "DFS range read response offset overflow".to_string(),
                        },
                    });
                };
                if range_end > data.total_size_bytes {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "DFS range read response exceeds total size: end {range_end}, total {}",
                                data.total_size_bytes
                            ),
                        },
                    });
                }
            }
            DfsReadResult::NotFound => {
                if pending.cache_response && self.pending_read_metadata_advanced(&pending)? {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: "stale DFS read response: object metadata advanced".to_string(),
                        },
                    });
                }
                self.mark_remote_read_location_missing(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                    &source_node_id,
                )?;
            }
            DfsReadResult::Rejected { reason } => {
                if remote_read_rejection_prunes_location(reason) {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                }
            }
        }
        Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id: response.request_id,
            object_id: response.object_id,
            source_node_id,
            result: response.result,
        })
    }

    pub(crate) fn mark_remote_read_location_missing(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &Option<String>,
        source_node_id: &NodeId,
    ) -> Result<(), DfsError> {
        self.record_transient_remote_read_location_prune(
            object_id,
            version,
            content_hash,
            source_node_id,
        );
        if let Some(object) = self.metadata.objects.get_mut(object_id) {
            if object.version == *version && object.content_hash == *content_hash {
                object
                    .current_locations
                    .retain(|node_id| node_id != source_node_id);
            }
        }
        let backend_object = if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(mut object) = backend.get_object(object_id)?
            && object.version == *version
            && object.content_hash == *content_hash
        {
            if !object.current_locations.contains(source_node_id) {
                None
            } else {
                object
                    .current_locations
                    .retain(|node_id| node_id != source_node_id);
                Some(object)
            }
        } else {
            None
        };
        let backend_version = if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(mut version_record) = backend.get_version(object_id, version)?
            && version_record.content_hash == *content_hash
        {
            if !version_record.locations.contains(source_node_id) {
                None
            } else {
                version_record
                    .locations
                    .retain(|node_id| node_id != source_node_id);
                Some(version_record)
            }
        } else {
            None
        };
        let should_persist = self.should_persist_remote_read_location_prune(
            backend_object.as_ref(),
            backend_version.as_ref(),
        )?;
        if should_persist && let Some(backend) = self.metadata_backend.as_mut() {
            let _ = backend.remove_receipt(object_id, version, source_node_id)?;
        }
        if should_persist && let Some(object) = backend_object {
            self.put_visible_object_in_metadata_backend(&object)?;
        } else if should_persist
            && let Some(version_record) = backend_version
            && let Some(backend) = self.metadata_backend.as_mut()
        {
            backend.put_version(&version_record)?;
        }
        Ok(())
    }

    fn should_persist_remote_read_location_prune(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        version_record: Option<&DfsObjectVersionRecord>,
    ) -> Result<bool, DfsError> {
        let Some(object) = object else {
            return Ok(version_record.is_some() && self.control_plane.is_none());
        };
        Ok(self
            .select_metadata_shard_for_object(object)?
            .is_none_or(|shard| shard.primary_node_id == self.node_id))
    }

    pub(crate) fn handle_networked_range_read_response_chunk(
        &mut self,
        source_node_id: NodeId,
        chunk: DfsReadResponseChunk,
        pending: PendingNetworkedRead,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        let request_id = chunk.request_id.clone();
        let object_id = chunk.object_id.clone();
        let version = chunk.version.clone();
        let content_hash = chunk.content_hash.clone();
        let Some(requested_offset) = pending.range_offset else {
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "range read chunk is missing requested offset state".to_string(),
                },
            }));
        };
        let Some(requested_max_bytes) = pending.range_max_bytes else {
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "range read chunk is missing requested max bytes state".to_string(),
                },
            }));
        };
        if pending.version != chunk.version || pending.content_hash != chunk.content_hash {
            self.pending_reads.remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "stale DFS range read response chunk".to_string(),
                },
            }));
        }
        if pending.cache_response && self.pending_read_metadata_advanced(&pending)? {
            self.pending_reads.remove(&chunk.request_id);
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "stale DFS range read response: object metadata advanced".to_string(),
                },
            }));
        }
        if pending.cache_response
            && !self.pending_read_source_still_current(&pending, &source_node_id)?
        {
            self.pending_reads.remove(&chunk.request_id);
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "stale DFS range read response: source is no longer a current location"
                        .to_string(),
                },
            }));
        }
        let Some(object_total_size_bytes) = chunk.object_total_size_bytes else {
            self.pending_reads.remove(&chunk.request_id);
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "chunked DFS range read response is missing object size".to_string(),
                },
            }));
        };
        if let Some(expected_size) = pending.content_size_bytes
            && expected_size != object_total_size_bytes
        {
            self.pending_reads.remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "chunked DFS range read response object size does not match metadata: expected {expected_size}, actual {object_total_size_bytes}"
                    ),
                },
            }));
        }
        let expected_range_offset = requested_offset.min(object_total_size_bytes);
        if chunk.range_offset != Some(expected_range_offset) {
            self.pending_reads.remove(&chunk.request_id);
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "chunked DFS range read response offset mismatch: expected {expected_range_offset}, actual {:?}",
                        chunk.range_offset
                    ),
                },
            }));
        }
        let expected_range_size_bytes = range_transfer_size_bytes(
            object_total_size_bytes,
            requested_offset,
            requested_max_bytes,
        );
        if chunk.total_size_bytes != expected_range_size_bytes {
            self.pending_reads.remove(&chunk.request_id);
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "chunked DFS range read response size mismatch: expected {expected_range_size_bytes}, actual {}",
                        chunk.total_size_bytes
                    ),
                },
            }));
        }
        if chunk.chunk_index != pending.next_chunk_index {
            self.pending_reads.remove(&chunk.request_id);
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "chunked DFS range read response is out of order: expected chunk {}, actual {}",
                        pending.next_chunk_index, chunk.chunk_index
                    ),
                },
            }));
        }
        if let Err(error) = validate_chunk_bytes(
            "chunked DFS range read response",
            chunk.total_size_bytes,
            chunk.chunk_index,
            chunk.total_chunks,
            &chunk.bytes,
        )
        .and_then(|()| {
            validate_chunk_hash(
                "chunked DFS range read response",
                chunk.chunk_index,
                &chunk.bytes,
                chunk.chunk_hash.as_deref(),
            )
        }) {
            self.pending_reads.remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: error.to_string(),
                },
            }));
        }
        let next_chunk_index = chunk.chunk_index.saturating_add(1);
        let complete = next_chunk_index >= chunk.total_chunks;
        if complete {
            self.pending_reads.remove(&chunk.request_id);
        } else if let Some(pending) = self.pending_reads.get_mut(&chunk.request_id) {
            pending.next_chunk_index = next_chunk_index;
        }
        let total_chunks = chunk.total_chunks;
        let chunk_index = chunk.chunk_index;
        let bytes = chunk.bytes;
        if !complete
            && let Some(event) = self.request_next_read_window_if_due(
                &request_id,
                &source_node_id,
                next_chunk_index,
                total_chunks,
            )?
        {
            return Ok(Some(event));
        }
        Ok(Some(
            NetworkedDfsServiceEvent::RemoteRangeReadChunkReceived {
                request_id,
                object_id,
                source_node_id,
                version,
                content_hash,
                range_offset: expected_range_offset,
                range_size_bytes: expected_range_size_bytes,
                object_total_size_bytes,
                chunk_index,
                total_chunks,
                bytes,
                complete,
            },
        ))
    }

    pub(crate) fn handle_networked_read_response_chunk(
        &mut self,
        source_node_id: NodeId,
        chunk: DfsReadResponseChunk,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        let Some(pending) = self.pending_reads.get(&chunk.request_id).cloned() else {
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "unknown DFS read request".to_string(),
                },
            }));
        };
        if source_node_id != pending.target_node_id {
            let reason = format!(
                "unexpected DFS read response source: expected {}, actual {}",
                pending.target_node_id, source_node_id
            );
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected { reason },
            }));
        }
        if pending.object_id != chunk.object_id {
            let reason = format!(
                "unexpected DFS read response object: expected {}, actual {}",
                pending.object_id, chunk.object_id
            );
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.incoming_read_memory_transfers
                .remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected { reason },
            }));
        }
        if pending.range_offset.is_some() || pending.range_max_bytes.is_some() {
            return self.handle_networked_range_read_response_chunk(source_node_id, chunk, pending);
        }
        if pending.version != chunk.version || pending.content_hash != chunk.content_hash {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.incoming_read_memory_transfers
                .remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "stale DFS read response chunk".to_string(),
                },
            }));
        }
        if pending.cache_response && self.pending_read_metadata_advanced(&pending)? {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.incoming_read_memory_transfers
                .remove(&chunk.request_id);
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "stale DFS read response: object metadata advanced".to_string(),
                },
            }));
        }
        if pending.cache_response
            && !self.pending_read_source_still_current(&pending, &source_node_id)?
        {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.incoming_read_memory_transfers
                .remove(&chunk.request_id);
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "stale DFS read response: source is no longer a current location"
                        .to_string(),
                },
            }));
        }
        if let Some(expected_size) = pending.content_size_bytes
            && expected_size != chunk.total_size_bytes
        {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.incoming_read_memory_transfers
                .remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "chunked DFS read response size does not match object metadata: expected {expected_size}, actual {}",
                        chunk.total_size_bytes
                    ),
                },
            }));
        }
        if chunk.total_chunks == 0 {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.incoming_read_memory_transfers
                .remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id.clone(),
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "chunked DFS read response {} has no chunks",
                        chunk.request_id
                    ),
                },
            }));
        }

        let Some(chunk_content_hash) = chunk.content_hash.clone() else {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.incoming_read_memory_transfers
                .remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "chunked DFS read response is missing content hash".to_string(),
                },
            }));
        };

        if let Err(error) = validate_chunk_bytes(
            "chunked DFS read response",
            chunk.total_size_bytes,
            chunk.chunk_index,
            chunk.total_chunks,
            &chunk.bytes,
        )
        .and_then(|()| {
            validate_chunk_hash(
                "chunked DFS read response",
                chunk.chunk_index,
                &chunk.bytes,
                chunk.chunk_hash.as_deref(),
            )
        }) {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.incoming_read_memory_transfers
                .remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: error.to_string(),
                },
            }));
        }

        if !pending.cache_response {
            return self.handle_networked_uncached_read_response_chunk(
                source_node_id,
                chunk,
                pending,
                chunk_content_hash,
            );
        }

        if !self.incoming_read_transfers.contains_key(&chunk.request_id) {
            if let Err(error) = validate_chunked_transfer_shape(
                "chunked DFS read response",
                chunk.total_size_bytes,
                chunk.total_chunks,
            ) {
                self.clear_read_response_transfer_state(&chunk.request_id);
                self.mark_remote_read_location_missing(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                    &source_node_id,
                )?;
                return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                    request_id: chunk.request_id,
                    object_id: chunk.object_id,
                    source_node_id,
                    result: DfsReadResult::Rejected {
                        reason: error.to_string(),
                    },
                }));
            }
            let assembler = match self.content.begin_chunked_object_version(
                chunk.object_id.as_str(),
                chunk.version.as_str(),
                &chunk_content_hash,
                chunk.total_size_bytes,
                chunk.total_chunks,
                DFS_BLOB_CHUNK_BYTES,
            ) {
                Ok(assembler) => assembler,
                Err(error) => {
                    self.clear_read_response_transfer_state(&chunk.request_id);
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: chunk.request_id,
                        object_id: chunk.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: error.to_string(),
                        },
                    }));
                }
            };
            let transfer = match InProgressReadTransfer::new(
                chunk.object_id.clone(),
                chunk.version.clone(),
                chunk_content_hash.clone(),
                chunk.total_size_bytes,
                chunk.total_chunks,
                assembler,
            ) {
                Ok(transfer) => transfer,
                Err(error) => {
                    self.clear_read_response_transfer_state(&chunk.request_id);
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: chunk.request_id,
                        object_id: chunk.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: error.to_string(),
                        },
                    }));
                }
            };
            self.incoming_read_transfers
                .insert(chunk.request_id.clone(), transfer);
        }
        let transfer = self
            .incoming_read_transfers
            .get_mut(&chunk.request_id)
            .expect("transfer inserted");
        if transfer.object_id != chunk.object_id
            || transfer.version != chunk.version
            || transfer.content_hash != chunk_content_hash
            || transfer.total_size_bytes() != chunk.total_size_bytes
        {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id.clone(),
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "conflicting DFS read response chunk metadata for {}",
                        chunk.request_id
                    ),
                },
            }));
        }
        let request_id = chunk.request_id.clone();
        let object_id = chunk.object_id.clone();
        let version = chunk.version.clone();
        let content_hash = chunk_content_hash.clone();
        let transfer_complete =
            match transfer.insert_chunk(chunk.chunk_index, chunk.total_chunks, chunk.bytes) {
                Ok(true) => true,
                Ok(false) => false,
                Err(error) => {
                    self.clear_read_response_transfer_state(&chunk.request_id);
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: chunk.request_id,
                        object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: error.to_string(),
                        },
                    }));
                }
            };
        let next_chunk_index = transfer.contiguous_received_chunk_count();
        if let Some(pending) = self.pending_reads.get_mut(&request_id) {
            pending.next_chunk_index = pending.next_chunk_index.max(next_chunk_index);
        }
        if !transfer_complete {
            return self.request_next_read_window_if_due(
                &request_id,
                &source_node_id,
                next_chunk_index,
                chunk.total_chunks,
            );
        }
        let transfer = self
            .incoming_read_transfers
            .remove(&request_id)
            .expect("completed read transfer");
        self.pending_reads.remove(&request_id);
        let manifest = match self
            .content
            .commit_chunked_object_version(transfer.chunks.assembler)
        {
            Ok(manifest) => manifest,
            Err(error) => {
                self.mark_remote_read_location_missing(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                    &source_node_id,
                )?;
                return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                    request_id,
                    object_id,
                    source_node_id,
                    result: DfsReadResult::Rejected {
                        reason: error.to_string(),
                    },
                }));
            }
        };
        if manifest.content_hash != content_hash {
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: DfsError::ContentHashMismatch {
                        expected: content_hash,
                        actual: manifest.content_hash,
                    }
                    .to_string(),
                },
            }));
        }
        let data = DfsReadData {
            object_id: object_id.clone(),
            version,
            content_hash: Some(manifest.content_hash),
            bytes: Vec::new(),
        };
        if let Err(error) = self.store_remote_read_manifest(&data, manifest.size_bytes) {
            if matches!(error, DfsError::ContentHashMismatch { .. }) {
                self.mark_remote_read_location_missing(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                    &source_node_id,
                )?;
                return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                    request_id,
                    object_id,
                    source_node_id,
                    result: DfsReadResult::Rejected {
                        reason: error.to_string(),
                    },
                }));
            }
            return Err(error);
        }
        self.clear_pending_remote_reads_for_object_version(
            &data.object_id,
            &data.version,
            &data.content_hash,
        );
        Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id,
            object_id,
            source_node_id,
            result: DfsReadResult::Found(data),
        }))
    }

    pub(crate) fn handle_networked_uncached_read_response_chunk(
        &mut self,
        source_node_id: NodeId,
        chunk: DfsReadResponseChunk,
        pending: PendingNetworkedRead,
        chunk_content_hash: String,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if !self
            .incoming_read_memory_transfers
            .contains_key(&chunk.request_id)
        {
            let transfer = match InProgressReadMemoryTransfer::new(
                chunk.object_id.clone(),
                chunk.version.clone(),
                chunk_content_hash.clone(),
                chunk.total_size_bytes,
                chunk.total_chunks,
                DFS_BLOB_CHUNK_BYTES,
            ) {
                Ok(transfer) => transfer,
                Err(error) => {
                    self.clear_read_response_transfer_state(&chunk.request_id);
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: chunk.request_id,
                        object_id: chunk.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: error.to_string(),
                        },
                    }));
                }
            };
            self.incoming_read_memory_transfers
                .insert(chunk.request_id.clone(), transfer);
        }
        let transfer = self
            .incoming_read_memory_transfers
            .get_mut(&chunk.request_id)
            .expect("transfer inserted");
        if transfer.object_id != chunk.object_id
            || transfer.version != chunk.version
            || transfer.content_hash != chunk_content_hash
            || transfer.total_size_bytes() != chunk.total_size_bytes
        {
            self.clear_read_response_transfer_state(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id.clone(),
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "conflicting DFS read response chunk metadata for {}",
                        chunk.request_id
                    ),
                },
            }));
        }
        let request_id = chunk.request_id.clone();
        let object_id = chunk.object_id.clone();
        let version = chunk.version.clone();
        let content_hash = chunk_content_hash.clone();
        let total_chunks = chunk.total_chunks;
        let transfer_complete =
            match transfer.insert_chunk(chunk.chunk_index, chunk.total_chunks, chunk.bytes) {
                Ok(complete) => complete,
                Err(error) => {
                    self.clear_read_response_transfer_state(&chunk.request_id);
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: chunk.request_id,
                        object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: error.to_string(),
                        },
                    }));
                }
            };
        let next_chunk_index = transfer.contiguous_received_chunk_count();
        if let Some(pending) = self.pending_reads.get_mut(&request_id) {
            pending.next_chunk_index = pending.next_chunk_index.max(next_chunk_index);
        }
        if !transfer_complete {
            return self.request_next_read_window_if_due(
                &request_id,
                &source_node_id,
                next_chunk_index,
                total_chunks,
            );
        }
        let transfer = self
            .incoming_read_memory_transfers
            .remove(&request_id)
            .expect("completed memory read transfer");
        self.pending_reads.remove(&request_id);
        let bytes = match transfer.into_bytes() {
            Ok(bytes) => bytes,
            Err(error) => {
                self.mark_remote_read_location_missing(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                    &source_node_id,
                )?;
                return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                    request_id,
                    object_id,
                    source_node_id,
                    result: DfsReadResult::Rejected {
                        reason: error.to_string(),
                    },
                }));
            }
        };
        let data = DfsReadData {
            object_id: object_id.clone(),
            version,
            content_hash: Some(content_hash),
            bytes,
        };
        if let Err(error) = self.validate_remote_read_data_for_pending(&pending, &data) {
            if matches!(error, DfsError::ContentHashMismatch { .. }) {
                self.mark_remote_read_location_missing(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                    &source_node_id,
                )?;
                return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                    request_id,
                    object_id,
                    source_node_id,
                    result: DfsReadResult::Rejected {
                        reason: error.to_string(),
                    },
                }));
            }
            return Err(error);
        }
        self.clear_transient_remote_read_location_prunes_for_object(
            &data.object_id,
            &data.version,
            &data.content_hash,
        );
        self.clear_pending_remote_reads_for_object_version(
            &data.object_id,
            &data.version,
            &data.content_hash,
        );
        Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id,
            object_id,
            source_node_id,
            result: DfsReadResult::Found(data),
        }))
    }

    fn request_next_read_window_if_due(
        &mut self,
        request_id: &str,
        source_node_id: &NodeId,
        next_chunk_index: u32,
        total_chunks: u32,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if next_chunk_index >= total_chunks {
            return Ok(None);
        }
        let Some(pending) = self.pending_reads.get(request_id).cloned() else {
            return Ok(None);
        };
        let Some(window_end) = pending.window_end_chunk_index else {
            return Ok(None);
        };
        if next_chunk_index < window_end {
            return Ok(None);
        }
        let transfer_size_bytes = pending
            .range_max_bytes
            .map(|max_bytes| max_bytes as u64)
            .or(pending.content_size_bytes)
            .unwrap_or_else(|| u64::from(total_chunks) * DFS_BLOB_CHUNK_BYTES as u64);
        let max_chunks = self.adaptive_read_chunk_window(source_node_id, transfer_size_bytes);
        let sent = self.send_dfs_message_if_routable(
            source_node_id,
            DfsNetworkMessage::ReadRequest(DfsReadRequest {
                request_id: request_id.to_string(),
                object_id: pending.object_id.clone(),
                version: Some(pending.version.clone()),
                content_hash: pending.content_hash.clone(),
                content_size_bytes: pending.content_size_bytes,
                range_offset: pending.range_offset,
                range_max_bytes: pending.range_max_bytes,
                start_chunk_index: next_chunk_index,
                max_chunks,
            }),
        )?;
        if sent {
            if let Some(pending) = self.pending_reads.get_mut(request_id) {
                pending.next_chunk_index = next_chunk_index;
                pending.window_end_chunk_index =
                    Self::read_window_end_chunk_index(next_chunk_index, max_chunks);
                pending.next_retry_at = Instant::now() + DFS_REMOTE_READ_RETRY_DELAY;
            }
            return Ok(None);
        }

        self.clear_read_response_transfer_state(request_id);
        self.mark_remote_read_location_missing(
            &pending.object_id,
            &pending.version,
            &pending.content_hash,
            source_node_id,
        )?;
        Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id: request_id.to_string(),
            object_id: pending.object_id,
            source_node_id: source_node_id.clone(),
            result: DfsReadResult::Rejected {
                reason: "unable to route DFS read continuation request".to_string(),
            },
        }))
    }

    fn clear_read_response_transfer_state(&mut self, request_id: &str) {
        self.pending_reads.remove(request_id);
        self.incoming_read_transfers.remove(request_id);
        self.incoming_read_memory_transfers.remove(request_id);
    }
}
