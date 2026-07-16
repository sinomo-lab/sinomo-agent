//! DFS networked read transfer message handlers.
//!
//! This module owns remote read serving and response assembly: read requests,
//! bulk read requests, inline responses, range responses, and reader-backed
//! response bodies.

use std::io::Read;
use std::time::Instant;

use sinomo_core::node::NodeId;

use crate::transfer::{dfs_byte_count_to_usize, range_transfer_size_bytes};
use crate::*;

const DFS_MISSING_CONTENT_AUTHORITY_GUARD_RESELECT_LIMIT: usize = 4;

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
        let mut sent_body_response = false;
        let result = match requested_range {
            Err(reason) => DfsReadResult::Rejected { reason },
            Ok(range) => {
                if let Some((result, sent_body)) = self.serve_networked_read_from_request_hints(
                    &requester_node_id,
                    &request,
                    range,
                )? {
                    sent_body_response = sent_body;
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
                        Ok(_) => match range {
                            Some((offset, max_bytes)) => {
                                let streamed_range = max_bytes > DFS_BLOB_INLINE_LIMIT_BYTES;
                                if streamed_range {
                                    match self.open_local_current_object_range_stream_with_state(
                                        &request.object_id,
                                        offset,
                                        max_bytes,
                                    )? {
                                        NetworkedDfsRangeReadStreamOutcome::Found(stream)
                                            if stream.range_size_bytes > 0 =>
                                        {
                                            self.send_networked_range_read_response_body_from_reader(
                                        &requester_node_id,
                                        &request,
                                        &stream.data,
                                        stream.reader,
                                    )?;
                                            sent_body_response = true;
                                            DfsReadResult::RangeFound(stream.data)
                                        }
                                        NetworkedDfsRangeReadStreamOutcome::Found(stream) => {
                                            DfsReadResult::RangeFound(stream.data)
                                        }
                                        NetworkedDfsRangeReadStreamOutcome::Conflicted {
                                            conflicts,
                                        } => DfsReadResult::Rejected {
                                            reason: read_conflicts_reason(conflicts),
                                        },
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
                                        NetworkedDfsRangeReadOutcome::Found { data, .. } => {
                                            DfsReadResult::RangeFound(data)
                                        }
                                        NetworkedDfsRangeReadOutcome::Conflicted {
                                            conflicts,
                                            ..
                                        } => DfsReadResult::Rejected {
                                            reason: read_conflicts_reason(conflicts),
                                        },
                                        NetworkedDfsRangeReadOutcome::PendingReplication {
                                            ..
                                        }
                                        | NetworkedDfsRangeReadOutcome::NotConfirmed { .. }
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
                                        self.send_networked_read_response_body_from_reader(
                                            &requester_node_id,
                                            &request,
                                            &stream.data,
                                            stream.reader,
                                        )?;
                                        sent_body_response = true;
                                        DfsReadResult::Found(stream.data)
                                    }
                                    NetworkedDfsReadStreamOutcome::Found(_) => {
                                        match self.read_local_object_with_state(
                                            &request.object_id,
                                            false,
                                        )? {
                                            NetworkedDfsReadOutcome::Found { data, .. } => {
                                                DfsReadResult::Found(data)
                                            }
                                            NetworkedDfsReadOutcome::Conflicted {
                                                conflicts,
                                                ..
                                            } => DfsReadResult::Rejected {
                                                reason: read_conflicts_reason(conflicts),
                                            },
                                            NetworkedDfsReadOutcome::PendingReplication {
                                                ..
                                            }
                                            | NetworkedDfsReadOutcome::NotConfirmed { .. }
                                            | NetworkedDfsReadOutcome::NotFound => {
                                                DfsReadResult::NotFound
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
                        },
                        Err(DfsError::UnknownObject { .. }) => DfsReadResult::NotFound,
                        Err(error) => return Err(error),
                    }
                }
            }
        };
        match &result {
            DfsReadResult::Found(_) if sent_body_response => {}
            DfsReadResult::RangeFound(_) if sent_body_response => {}
            DfsReadResult::Found(data)
                if data.bytes.len() > DFS_BLOB_INLINE_LIMIT_BYTES
                    && !direct_full_read_response_allowed(&request, data.bytes.len() as u64) =>
            {
                self.send_networked_read_response_body_from_data(
                    &requester_node_id,
                    &request,
                    data,
                )?;
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
                let streamed_range = max_bytes > DFS_BLOB_INLINE_LIMIT_BYTES;
                if streamed_range {
                    let range_len =
                        dfs_byte_count_to_usize("DFS range response", range_size_bytes)?;
                    let reader = match self.content.open_blob_range_stream(
                        &content_hash,
                        range_offset,
                        range_len,
                    ) {
                        Ok(reader) => ObjectVersionReader::from_blob_range(reader),
                        Err(_) => match self.content.open_object_content_range_stream(
                            object_id.as_str(),
                            &content_hash,
                            total_size_bytes,
                            range_offset,
                            range_len,
                        )? {
                            Some((_, reader)) => reader,
                            None => return Ok(Some((DfsReadResult::NotFound, false))),
                        },
                    };
                    let data = DfsReadRangeData {
                        object_id,
                        version,
                        content_hash: Some(content_hash),
                        offset: range_offset,
                        total_size_bytes,
                        bytes: Vec::new(),
                    };
                    self.send_networked_range_read_response_body_from_reader(
                        requester_node_id,
                        request,
                        &data,
                        reader,
                    )?;
                    return Ok(Some((DfsReadResult::RangeFound(data), true)));
                }
                let bytes = match self
                    .content
                    .read_blob_range(&content_hash, offset, max_bytes)
                {
                    Ok(bytes) => bytes,
                    Err(_) => match self.content.read_object_content_range(
                        object_id.as_str(),
                        &content_hash,
                        total_size_bytes,
                        offset,
                        max_bytes,
                    )? {
                        Some((_, bytes)) => bytes,
                        None => return Ok(Some((DfsReadResult::NotFound, false))),
                    },
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
                if total_size_bytes > DFS_BLOB_INLINE_LIMIT_BYTES as u64 {
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
                    let range_len = dfs_byte_count_to_usize("DFS read response", total_size_bytes)?;
                    let reader =
                        match self
                            .content
                            .open_blob_range_stream(&content_hash, 0, range_len)
                        {
                            Ok(reader) => ObjectVersionReader::from_blob_range(reader),
                            Err(_) => return Ok(Some((DfsReadResult::NotFound, false))),
                        };
                    let data = DfsReadData {
                        object_id,
                        version,
                        content_hash: Some(content_hash),
                        bytes: Vec::new(),
                    };
                    self.send_networked_read_response_body_from_reader(
                        requester_node_id,
                        request,
                        &data,
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

    pub(crate) fn send_networked_read_response_body_from_data(
        &mut self,
        requester_node_id: &NodeId,
        request: &DfsReadRequest,
        data: &DfsReadData,
    ) -> Result<(), DfsError> {
        let mut head = data.clone();
        let bytes = std::mem::take(&mut head.bytes);
        let digest = head
            .content_hash
            .clone()
            .unwrap_or_else(|| content_hash(&bytes));
        self.send_networked_read_response_body(
            requester_node_id,
            request,
            DfsReadResult::Found(head),
            DfsOutboundBody::from_bytes(bytes, digest),
        )
    }

    fn send_networked_read_response_body_from_reader(
        &mut self,
        requester_node_id: &NodeId,
        request: &DfsReadRequest,
        data: &DfsReadData,
        reader: ObjectVersionReader,
    ) -> Result<(), DfsError> {
        let body_size_bytes = reader.size_bytes();
        self.send_networked_read_response_body(
            requester_node_id,
            request,
            DfsReadResult::Found(data.clone()),
            DfsOutboundBody::from_reader_with_transport_integrity(reader, body_size_bytes),
        )
    }

    fn send_networked_read_response_body(
        &mut self,
        requester_node_id: &NodeId,
        request: &DfsReadRequest,
        result: DfsReadResult,
        body: DfsOutboundBody,
    ) -> Result<(), DfsError> {
        let message = DfsNetworkMessage::ReadResponse(DfsReadResponse {
            request_id: request.request_id.clone(),
            object_id: request.object_id.clone(),
            result,
        });
        let routed = self.next_dfs_routed_message(requester_node_id, message);
        self.transport
            .send_routed_message_with_body(requester_node_id, routed, body)
            .map_err(DfsError::from_network)
    }

    fn send_networked_range_read_response_body_from_reader(
        &mut self,
        requester_node_id: &NodeId,
        request: &DfsReadRequest,
        data: &DfsReadRangeData,
        reader: ObjectVersionReader,
    ) -> Result<(), DfsError> {
        let body_size_bytes = reader.size_bytes();
        self.send_networked_read_response_body(
            requester_node_id,
            request,
            DfsReadResult::RangeFound(data.clone()),
            DfsOutboundBody::from_reader_with_transport_integrity(reader, body_size_bytes),
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
        if pending.object_id != response.object_id {
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
                let Some(expected_size) = pending.content_size_bytes else {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: "DFS range read request lacks authoritative object size"
                                .to_string(),
                        },
                    });
                };
                if expected_size != data.total_size_bytes {
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
                let expected_offset = requested_offset.min(expected_size);
                if data.offset != expected_offset {
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
                                "DFS range read response offset mismatch: expected {expected_offset}, actual {}",
                                data.offset
                            ),
                        },
                    });
                }
                let expected_range_size =
                    range_transfer_size_bytes(expected_size, requested_offset, requested_max_bytes);
                let actual_range_size = u64::try_from(data.bytes.len()).unwrap_or(u64::MAX);
                if actual_range_size != expected_range_size {
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
                                "DFS range read response length mismatch: expected {expected_range_size}, actual {actual_range_size}"
                            ),
                        },
                    });
                }
                let Some(range_end) = data.offset.checked_add(data.bytes.len() as u64) else {
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
                            reason: "DFS range read response offset overflow".to_string(),
                        },
                    });
                };
                if range_end > data.total_size_bytes {
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
                                "DFS range read response exceeds total size: end {range_end}, total {}",
                                data.total_size_bytes
                            ),
                        },
                    });
                }
                // A partial range cannot be checked against the committed
                // whole-object hash without a content proof. Preserve that
                // range-read boundary, but when the requested range happens
                // to contain the complete object, enforce the same byte-level
                // hash validation as the full-read path.
                if data.offset == 0 && range_end == data.total_size_bytes {
                    let actual_hash = content_hash(&data.bytes);
                    if pending.content_hash.as_deref() != Some(actual_hash.as_str()) {
                        let expected_hash = pending
                            .content_hash
                            .clone()
                            .unwrap_or_else(|| "committed content hash".to_string());
                        self.mark_remote_read_location_missing(
                            &pending.object_id,
                            &pending.version,
                            &pending.content_hash,
                            &source_node_id,
                        )?;
                        return Err(DfsError::ContentHashMismatch {
                            expected: expected_hash,
                            actual: actual_hash,
                        });
                    }
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
            && let Some(object) = backend.get_object(object_id)?
            && object.version == *version
            && object.content_hash == *content_hash
        {
            Some(object)
        } else {
            None
        };
        let backend_object_update = backend_object.as_ref().and_then(|object| {
            if !object.current_locations.contains(source_node_id) {
                return None;
            }
            let mut object = object.clone();
            object
                .current_locations
                .retain(|node_id| node_id != source_node_id);
            Some(object)
        });
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
        if should_persist && let Some(object) = backend_object_update {
            self.put_visible_object_in_metadata_backend(&object)?;
        } else if should_persist
            && let Some(version_record) = backend_version
            && let Some(backend) = self.metadata_backend.as_mut()
        {
            backend.put_version(&version_record)?;
        }
        Ok(())
    }

    /// Turn a stale local durability claim into ordinary committed-intent
    /// repair work, but only when this node can durably mutate the exact
    /// object's current metadata shard.
    ///
    /// A detached `CommitVersion` sender may retire its exact notification
    /// only after this transition succeeds. Non-authority replicas leave the
    /// receipt untouched and return a retryable rejection instead, so a
    /// transient local read failure cannot silently discard durability.
    pub(crate) fn transition_missing_local_commit_content_to_repair(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<bool, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(false);
        };
        let Some(object) = backend.get_object(object_id)? else {
            return Ok(false);
        };
        if object.version != *version || object.content_hash.as_deref() != Some(content_hash) {
            return Ok(false);
        }
        let publication_guard = {
            let mut attempts = 0usize;
            loop {
                attempts = attempts.saturating_add(1);
                let selected_shard = self.select_metadata_shard_for_object(&object)?;
                let guard = selected_shard.as_ref().map(|shard| {
                    crate::service_replica::metadata_shard_publication_guard(
                        &shard.file_system_id,
                        &shard.shard_id,
                    )
                });
                let certified_shard =
                    match self.certified_local_metadata_owner_shard_for_object(&object) {
                        Ok(shard) => shard,
                        Err(DfsError::StaleOwnerEpoch { .. }) => return Ok(false),
                        Err(error) => return Err(error),
                    };
                let guarded_certified_key_matches = match (&selected_shard, &certified_shard) {
                    (Some(selected), Some(certified)) => {
                        selected.file_system_id == certified.file_system_id
                            && selected.shard_id == certified.shard_id
                    }
                    (None, None) => true,
                    _ => false,
                };
                if guarded_certified_key_matches {
                    break guard;
                }
                drop(guard);
                if attempts >= DFS_MISSING_CONTENT_AUTHORITY_GUARD_RESELECT_LIMIT {
                    return Ok(false);
                }
            }
        };
        // A cached shard snapshot naming this node as primary is not enough to
        // withdraw durable metadata. Reuse the normal metadata-authority path
        // so the exact assignment is certified through a KV write barrier and
        // any pending membership-handoff marker fences the transition. Hold
        // the same process-wide publication guard used by retiring-primary
        // fence capture through certification, durable prune, and the final
        // re-read so a handoff that starts afterward must observe this change.
        // A stale certification is deliberately retryable: the sender must
        // not retire its detached promotion until a current authority performs
        // the durable prune. Other metadata failures remain visible to the
        // caller.
        let local_node_id = self.node_id.clone();
        self.mark_remote_read_location_missing(
            object_id,
            version,
            &Some(content_hash.to_string()),
            &local_node_id,
        )?;

        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(false);
        };
        let object_pruned = backend.get_object(object_id)?.is_some_and(|object| {
            object.version == *version
                && object.content_hash.as_deref() == Some(content_hash)
                && !object.current_locations.contains(&local_node_id)
        });
        let version_pruned =
            backend
                .get_version(object_id, version)?
                .is_none_or(|version_record| {
                    version_record.content_hash.as_deref() == Some(content_hash)
                        && !version_record.locations.contains(&local_node_id)
                });
        let receipt_pruned = !backend
            .receipts_for_version(object_id, version)?
            .iter()
            .any(|receipt| receipt.replica_node_id == local_node_id);
        drop(publication_guard);
        Ok(object_pruned && version_pruned && receipt_pruned)
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
}
