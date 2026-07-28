//! Direct DFS read object-RPC helpers.
//!
//! The normal DFS data plane uses framed [`DfsNetworkMessage`] traffic. Direct
//! read is the optimized read-only path: its response value carries status and
//! integrity metadata, while found content uses the stream-native object-RPC
//! attachment carried by `sinomo-network`.

use crate::content_store::LocalContentStore;
use crate::protocol::{
    DfsDirectReadRequest, DfsReadData, DfsReadRangeData, DfsReadRequest, DfsReadResponse,
    DfsReadResult,
};
use crate::routes::{DFS_DIRECT_READ_ROUTE, DFS_RPC_CATEGORY};
use crate::{DfsError, DfsNodeTransport, DfsObjectMetadata, NetworkedDfsService};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    NetworkError, ObjectRpcBodyChunk, ObjectRpcBodySource, ObjectRpcCall, ObjectRpcIncomingBody,
    ObjectRpcRequest, ObjectRpcResponse, ObjectRpcResult,
};

pub(crate) const DFS_DIRECT_READ_STATUS_METADATA_KEY: &str = "sinomo.dfs.direct-read.status";
pub(crate) const DFS_DIRECT_READ_STATUS_FOUND: &str = "found";
const DFS_DIRECT_READ_STATUS_NOT_FOUND: &str = "not-found";
const DFS_DIRECT_READ_STATUS_REJECTED: &str = "rejected";
const DFS_DIRECT_READ_STATUS_RANGE_FOUND: &str = "range-found";
const DFS_DIRECT_READ_REQUEST_ID_METADATA_KEY: &str = "sinomo.dfs.direct-read.request-id";
const DFS_DIRECT_READ_OBJECT_ID_METADATA_KEY: &str = "sinomo.dfs.direct-read.object-id";
const DFS_DIRECT_READ_VERSION_METADATA_KEY: &str = "sinomo.dfs.direct-read.version";
const DFS_DIRECT_READ_CONTENT_HASH_METADATA_KEY: &str = "sinomo.dfs.direct-read.content-hash";
const DFS_DIRECT_READ_CONTENT_SIZE_METADATA_KEY: &str = "sinomo.dfs.direct-read.content-size";
const DFS_DIRECT_READ_RANGE_OFFSET_METADATA_KEY: &str = "sinomo.dfs.direct-read.range-offset";
const DFS_DIRECT_READ_TOTAL_SIZE_METADATA_KEY: &str = "sinomo.dfs.direct-read.total-size";
const DFS_DIRECT_READ_REJECTION_METADATA_KEY: &str = "sinomo.dfs.direct-read.rejection";

#[derive(Debug)]
pub struct DfsDirectReadAttachmentResponse {
    pub response: ObjectRpcResponse,
    pub attachment: ObjectRpcBodySource,
}

#[derive(Debug)]
pub struct DfsDirectReadClientResponse {
    pub request_id: String,
    pub object_id: crate::DfsObjectId,
    pub result: DfsDirectReadClientResult,
}

#[derive(Debug)]
pub enum DfsDirectReadClientResult {
    Found(DfsDirectReadClientData),
    RangeFound(DfsDirectReadClientRangeData),
    NotFound,
    Rejected { reason: String },
}

/// Successful completion proof for one incrementally consumed DFS read.
///
/// Stream consumers must not publish externally visible effects derived from
/// the bytes until this proof has been returned. Full-object reads verify the
/// immutable DFS content digest; range reads verify their declared transfer
/// length because a subset cannot reproduce the full-object digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsReadStreamCompletion {
    pub content_size_bytes: usize,
    pub verified_content_hash: Option<String>,
}

/// Stream-native DFS response body with incremental application-level
/// integrity verification.
///
/// Transport framing proves ordered delivery and length. DFS additionally
/// owns immutable object identity, so full-object reads update one BLAKE3
/// hasher as the application consumes transport-owned chunks and compare it at
/// EOF. No contiguous whole-body buffer is retained by this type.
#[derive(Debug)]
pub struct DfsVerifiedReadStream {
    body: ObjectRpcIncomingBody,
    expected_size_bytes: usize,
    expected_content_hash: Option<String>,
    observed_size_bytes: usize,
    hasher: Option<blake3::Hasher>,
    completion: Option<DfsReadStreamCompletion>,
}

impl DfsVerifiedReadStream {
    pub(crate) fn new(
        body: ObjectRpcIncomingBody,
        expected_size_bytes: usize,
        expected_content_hash: Option<String>,
    ) -> Result<Self, NetworkError> {
        if let Some(declared_size_bytes) = body.total_bytes() {
            let expected_size_bytes_u64 = u64::try_from(expected_size_bytes).unwrap_or(u64::MAX);
            if declared_size_bytes != expected_size_bytes_u64 {
                return Err(dfs_network_serialization_error(format!(
                    "DFS read attachment length mismatch: metadata expected {expected_size_bytes} bytes, transport declared {declared_size_bytes}"
                )));
            }
        }
        Ok(Self {
            body,
            expected_size_bytes,
            hasher: expected_content_hash
                .as_ref()
                .map(|_| blake3::Hasher::new()),
            expected_content_hash,
            observed_size_bytes: 0,
            completion: None,
        })
    }

    pub const fn expected_size_bytes(&self) -> usize {
        self.expected_size_bytes
    }

    pub fn expected_content_hash(&self) -> Option<&str> {
        self.expected_content_hash.as_deref()
    }

    pub const fn observed_size_bytes(&self) -> usize {
        self.observed_size_bytes
    }

    pub fn completion(&self) -> Option<&DfsReadStreamCompletion> {
        self.completion.as_ref()
    }

    /// Returns the next ownership-bearing transport chunk.
    ///
    /// The chunk is hashed before it is returned, but the digest is not trusted
    /// until EOF. Call [`Self::finish`] (or read through EOF) before committing
    /// effects derived from the stream.
    pub async fn next_chunk(&mut self) -> Result<Option<ObjectRpcBodyChunk>, NetworkError> {
        if self.completion.is_some() {
            return Ok(None);
        }
        let chunk = self
            .body
            .next_chunk()
            .await
            .map_err(|source| NetworkError::Io {
                operation: "read stream-native DFS response chunk".to_string(),
                detail: source.to_string(),
            })?;
        let Some(chunk) = chunk else {
            self.complete()?;
            return Ok(None);
        };
        let next_size_bytes = self
            .observed_size_bytes
            .checked_add(chunk.len())
            .ok_or_else(|| {
                dfs_network_serialization_error("DFS read attachment length overflow")
            })?;
        if next_size_bytes > self.expected_size_bytes {
            return Err(dfs_network_serialization_error(format!(
                "DFS read attachment exceeded its declared length: expected {} bytes, received at least {next_size_bytes}",
                self.expected_size_bytes
            )));
        }
        if let Some(hasher) = self.hasher.as_mut() {
            hasher.update(chunk.as_ref());
        }
        self.observed_size_bytes = next_size_bytes;
        Ok(Some(chunk))
    }

    /// Drains any unread chunks and returns the terminal DFS integrity proof.
    pub async fn finish(mut self) -> Result<DfsReadStreamCompletion, NetworkError> {
        while self.next_chunk().await?.is_some() {}
        Ok(self
            .completion
            .expect("DFS stream completion is recorded at EOF"))
    }

    /// Explicit contiguous-buffer compatibility boundary.
    ///
    /// Canonical callers should consume [`Self::next_chunk`]. Existing APIs
    /// that return `DfsReadResponse` can opt into this one final copy without a
    /// second whole-buffer hash pass.
    pub async fn collect_to_vec(
        mut self,
        max_bytes: usize,
    ) -> Result<(Vec<u8>, DfsReadStreamCompletion), NetworkError> {
        if self.expected_size_bytes > max_bytes {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: self.expected_size_bytes,
                max_bytes,
            });
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(self.expected_size_bytes)
            .map_err(|_| NetworkError::ResourceExhausted {
                operation: "reserve compatible DFS read response".to_string(),
                requested_bytes: self.expected_size_bytes,
            })?;
        while let Some(chunk) = self.next_chunk().await? {
            bytes.extend_from_slice(chunk.as_ref());
        }
        let completion = self
            .completion
            .expect("DFS stream completion is recorded at EOF");
        Ok((bytes, completion))
    }

    fn complete(&mut self) -> Result<(), NetworkError> {
        if self.observed_size_bytes != self.expected_size_bytes {
            return Err(dfs_network_serialization_error(format!(
                "DFS read attachment has {} bytes, metadata expected {}",
                self.observed_size_bytes, self.expected_size_bytes
            )));
        }
        let verified_content_hash = match (
            self.expected_content_hash.as_ref(),
            self.hasher.as_ref(),
        ) {
            (Some(expected), Some(hasher)) => {
                let actual = format!("blake3:{}", hasher.finalize().to_hex());
                if &actual != expected {
                    return Err(dfs_network_serialization_error(format!(
                        "DFS read attachment hash mismatch: expected {expected}, actual {actual}"
                    )));
                }
                Some(expected.clone())
            }
            (None, None) => None,
            _ => unreachable!("DFS read verifier hash state is constructed together"),
        };
        self.completion = Some(DfsReadStreamCompletion {
            content_size_bytes: self.observed_size_bytes,
            verified_content_hash,
        });
        Ok(())
    }
}

#[derive(Debug)]
pub struct DfsDirectReadClientData {
    pub object_id: crate::DfsObjectId,
    pub version: crate::DfsVersion,
    pub content_hash: String,
    pub content_size_bytes: usize,
    pub stream: DfsVerifiedReadStream,
}

#[derive(Debug)]
pub struct DfsDirectReadClientRangeData {
    pub object_id: crate::DfsObjectId,
    pub version: crate::DfsVersion,
    /// Immutable identity of the complete object. A partial response cannot
    /// independently reproduce this digest, so its stream finalizer proves
    /// transfer length rather than claiming full-object digest verification.
    pub content_hash: String,
    pub offset: u64,
    pub total_size_bytes: u64,
    pub range_size_bytes: usize,
    pub stream: DfsVerifiedReadStream,
}

impl DfsDirectReadClientResponse {
    pub async fn collect_to_read_response(self) -> Result<DfsReadResponse, NetworkError> {
        let result = match self.result {
            DfsDirectReadClientResult::Found(data) => {
                let (bytes, completion) =
                    data.stream.collect_to_vec(data.content_size_bytes).await?;
                debug_assert_eq!(
                    completion.verified_content_hash.as_deref(),
                    Some(data.content_hash.as_str())
                );
                DfsReadResult::Found(DfsReadData {
                    object_id: data.object_id,
                    version: data.version,
                    content_hash: Some(data.content_hash),
                    bytes,
                })
            }
            DfsDirectReadClientResult::RangeFound(data) => {
                let (bytes, completion) = data.stream.collect_to_vec(data.range_size_bytes).await?;
                debug_assert!(completion.verified_content_hash.is_none());
                DfsReadResult::RangeFound(DfsReadRangeData {
                    object_id: data.object_id,
                    version: data.version,
                    content_hash: Some(data.content_hash),
                    offset: data.offset,
                    total_size_bytes: data.total_size_bytes,
                    bytes,
                })
            }
            DfsDirectReadClientResult::NotFound => DfsReadResult::NotFound,
            DfsDirectReadClientResult::Rejected { reason } => DfsReadResult::Rejected { reason },
        };
        Ok(DfsReadResponse {
            request_id: self.request_id,
            object_id: self.object_id,
            result,
        })
    }
}

impl NetworkedDfsService<DfsNodeTransport> {
    /// Opens a full-object response from a current remote content location
    /// without collecting its object-RPC body.
    ///
    /// This is the service-level canonical remote-read boundary. The returned
    /// [`DfsVerifiedReadStream`] still owns transport chunks and must reach its
    /// finalizer before callers publish effects derived from the bytes.
    pub async fn read_remote_object_stream(
        &mut self,
        object_id: &crate::DfsObjectId,
    ) -> Result<DfsDirectReadClientResponse, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        self.read_remote_object_metadata_stream(&object, None).await
    }

    /// Opens a bounded range from a current remote content location without
    /// collecting its object-RPC body.
    pub async fn read_remote_object_range_stream(
        &mut self,
        object_id: &crate::DfsObjectId,
        offset: u64,
        max_bytes: usize,
    ) -> Result<DfsDirectReadClientResponse, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        self.read_remote_object_metadata_stream(&object, Some((offset, max_bytes)))
            .await
    }

    pub(crate) async fn read_remote_object_metadata_stream(
        &mut self,
        object: &DfsObjectMetadata,
        range: Option<(u64, usize)>,
    ) -> Result<DfsDirectReadClientResponse, DfsError> {
        let content_hash = object
            .content_hash
            .clone()
            .ok_or_else(|| DfsError::Distributed {
                message: format!(
                    "object {} has no committed content hash for a remote read",
                    object.object_id
                ),
            })?;
        let content_size_bytes =
            object
                .content_size_bytes
                .ok_or_else(|| DfsError::Distributed {
                    message: format!(
                        "object {} has no committed content length for a remote read",
                        object.object_id
                    ),
                })?;
        let targets = object
            .current_locations
            .iter()
            .filter(|target| *target != &self.node_id)
            .cloned()
            .collect::<Vec<_>>();
        if targets.is_empty() {
            return Err(DfsError::Distributed {
                message: format!(
                    "object {} version {} has no remote content location",
                    object.object_id, object.version
                ),
            });
        }

        let mut last_terminal_response = None;
        let mut last_network_error = None;
        for target_node_id in targets {
            let request = DfsDirectReadRequest {
                request_id: self.next_request_id(),
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: content_hash.clone(),
                content_size_bytes,
                range_offset: range.map(|(offset, _)| offset),
                range_max_bytes: range.map(|(_, max_bytes)| max_bytes),
            };
            match self
                .transport
                .read_object_stream(&target_node_id, &request)
                .await
            {
                Ok(response) => {
                    validate_service_direct_read_response(&request, &response)?;
                    if service_direct_read_response_found(&response, range.is_some()) {
                        return Ok(response);
                    }
                    last_terminal_response = Some(response);
                }
                Err(error) => {
                    last_network_error = Some((target_node_id, error));
                }
            }
        }

        if let Some(response) = last_terminal_response {
            return Ok(response);
        }
        let (target_node_id, error) =
            last_network_error.expect("remote targets produced an outcome");
        Err(DfsError::Network {
            message: format!("stream DFS read from {target_node_id}: {error}"),
        })
    }
}

fn service_direct_read_response_found(
    response: &DfsDirectReadClientResponse,
    range_requested: bool,
) -> bool {
    matches!(
        (&response.result, range_requested),
        (DfsDirectReadClientResult::Found(_), false)
            | (DfsDirectReadClientResult::RangeFound(_), true)
    )
}

fn validate_service_direct_read_response(
    request: &DfsDirectReadRequest,
    response: &DfsDirectReadClientResponse,
) -> Result<(), DfsError> {
    if response.request_id != request.request_id || response.object_id != request.object_id {
        return Err(DfsError::Serialization {
            message: format!(
                "direct read response identity mismatch: expected request {} object {}, received request {} object {}",
                request.request_id, request.object_id, response.request_id, response.object_id
            ),
        });
    }
    match (
        &response.result,
        request.range_offset,
        request.range_max_bytes,
    ) {
        (DfsDirectReadClientResult::Found(data), None, None)
            if data.object_id == request.object_id
                && data.version == request.version
                && data.content_hash == request.content_hash
                && u64::try_from(data.content_size_bytes).ok()
                    == Some(request.content_size_bytes) =>
        {
            Ok(())
        }
        (
            DfsDirectReadClientResult::RangeFound(data),
            Some(expected_offset),
            Some(expected_max_bytes),
        ) if data.object_id == request.object_id
            && data.version == request.version
            && data.content_hash == request.content_hash
            && data.offset == expected_offset
            && data.total_size_bytes == request.content_size_bytes
            && data.range_size_bytes <= expected_max_bytes =>
        {
            Ok(())
        }
        (DfsDirectReadClientResult::NotFound, _, _)
        | (DfsDirectReadClientResult::Rejected { .. }, _, _) => Ok(()),
        _ => Err(DfsError::Serialization {
            message: format!(
                "direct read response metadata did not match request {} for object {}",
                request.request_id, request.object_id
            ),
        }),
    }
}

pub fn dfs_direct_read_rpc_request(
    request: &DfsDirectReadRequest,
) -> Result<ObjectRpcRequest, NetworkError> {
    let rpc_request =
        ObjectRpcRequest::encode_body(DFS_DIRECT_READ_ROUTE, DFS_RPC_CATEGORY, request)?;
    Ok(rpc_request.with_response_retry_safe())
}

pub fn dfs_direct_read_rpc_call(
    request: &DfsDirectReadRequest,
) -> Result<ObjectRpcCall, NetworkError> {
    let mut call =
        ObjectRpcCall::encode_borrowed_value(DFS_DIRECT_READ_ROUTE, DFS_RPC_CATEGORY, request)?;
    call.request = call.request.with_response_retry_safe();
    Ok(call)
}

pub(crate) fn dfs_direct_read_request_from_read_request(
    request: &DfsReadRequest,
) -> Option<DfsDirectReadRequest> {
    let content_size_bytes = request.content_size_bytes?;
    if !direct_read_response_allowed(request) {
        return None;
    }
    Some(DfsDirectReadRequest {
        request_id: request.request_id.clone(),
        object_id: request.object_id.clone(),
        version: request.version.clone()?,
        content_hash: request.content_hash.clone()?,
        content_size_bytes,
        range_offset: request.range_offset,
        range_max_bytes: request.range_max_bytes,
    })
}

pub fn handle_dfs_direct_read_rpc_request(
    data_dir: &SinomoDataDir,
    request: ObjectRpcRequest,
) -> Result<DfsDirectReadAttachmentResponse, DfsError> {
    let request = decode_dfs_direct_read_rpc_request(request)?;
    ensure_dfs_direct_read_root_layout(data_dir)?;
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
    serve_dfs_direct_read_request(&content, request)
}

pub(crate) fn handle_dfs_direct_read_request_with_initialized_content_store(
    content: &LocalContentStore,
    request: DfsDirectReadRequest,
) -> Result<DfsDirectReadAttachmentResponse, DfsError> {
    serve_dfs_direct_read_request(content, request)
}

fn decode_dfs_direct_read_rpc_request(
    request: ObjectRpcRequest,
) -> Result<DfsDirectReadRequest, DfsError> {
    if request.route != DFS_DIRECT_READ_ROUTE {
        return Err(DfsError::Network {
            message: format!("unknown DFS direct read route {}", request.route),
        });
    }
    request.decode_body().map_err(DfsError::from_network)
}

fn serve_dfs_direct_read_request(
    content: &LocalContentStore,
    request: DfsDirectReadRequest,
) -> Result<DfsDirectReadAttachmentResponse, DfsError> {
    let range = match (request.range_offset, request.range_max_bytes) {
        (None, None) => None,
        (Some(offset), Some(max_bytes)) => Some((offset, max_bytes)),
        _ => {
            return direct_read_terminal_response(
                request,
                DfsReadResult::Rejected {
                    reason: "DFS direct read range requires both offset and max bytes".to_string(),
                },
            );
        }
    };
    if let Some((offset, max_bytes)) = range {
        let range_offset = offset.min(request.content_size_bytes);
        let requested_bytes = u64::try_from(max_bytes).unwrap_or(u64::MAX);
        let range_size_bytes_u64 = request
            .content_size_bytes
            .saturating_sub(range_offset)
            .min(requested_bytes);
        let range_size_bytes =
            usize::try_from(range_size_bytes_u64).map_err(|_| DfsError::Network {
                message: format!(
                    "DFS direct read range is too large for this platform: {range_size_bytes_u64}"
                ),
            })?;
        let reader = match content.open_blob_range_stream(
            &request.content_hash,
            range_offset,
            range_size_bytes,
        ) {
            Ok(reader) if reader.remaining_bytes() == range_size_bytes_u64 => reader,
            Ok(_) | Err(_) => {
                return direct_read_terminal_response(request, DfsReadResult::NotFound);
            }
        };
        let mut response = direct_read_found_response_head(
            &request,
            DFS_DIRECT_READ_STATUS_RANGE_FOUND,
            range_size_bytes,
        );
        response.metadata.insert(
            DFS_DIRECT_READ_RANGE_OFFSET_METADATA_KEY.to_string(),
            range_offset.to_string(),
        );
        response.metadata.insert(
            DFS_DIRECT_READ_TOTAL_SIZE_METADATA_KEY.to_string(),
            request.content_size_bytes.to_string(),
        );
        return Ok(DfsDirectReadAttachmentResponse {
            response,
            attachment: ObjectRpcBodySource::from_reader(reader, Some(range_size_bytes_u64)),
        });
    }

    let reader = match content.open_blob_stream(&request.content_hash) {
        Ok(reader) if reader.size_bytes() == request.content_size_bytes => reader,
        Ok(_) | Err(_) => {
            return direct_read_terminal_response(request, DfsReadResult::NotFound);
        }
    };
    let response = direct_read_found_response_head(
        &request,
        DFS_DIRECT_READ_STATUS_FOUND,
        usize::try_from(request.content_size_bytes).map_err(|_| DfsError::Network {
            message: format!(
                "DFS direct read is too large for this platform: {}",
                request.content_size_bytes
            ),
        })?,
    );
    Ok(DfsDirectReadAttachmentResponse {
        response,
        attachment: ObjectRpcBodySource::from_reader(reader, Some(request.content_size_bytes)),
    })
}

fn direct_read_found_response_head(
    request: &DfsDirectReadRequest,
    status: &str,
    attachment_size_bytes: usize,
) -> ObjectRpcResponse {
    let mut response = ObjectRpcResponse::from_encoded_body(DFS_DIRECT_READ_ROUTE, Vec::new());
    response.metadata.insert(
        DFS_DIRECT_READ_STATUS_METADATA_KEY.to_string(),
        status.to_string(),
    );
    response.metadata.insert(
        DFS_DIRECT_READ_REQUEST_ID_METADATA_KEY.to_string(),
        request.request_id.clone(),
    );
    response.metadata.insert(
        DFS_DIRECT_READ_OBJECT_ID_METADATA_KEY.to_string(),
        request.object_id.to_string(),
    );
    response.metadata.insert(
        DFS_DIRECT_READ_VERSION_METADATA_KEY.to_string(),
        request.version.to_string(),
    );
    response.metadata.insert(
        DFS_DIRECT_READ_CONTENT_HASH_METADATA_KEY.to_string(),
        request.content_hash.clone(),
    );
    response.metadata.insert(
        DFS_DIRECT_READ_CONTENT_SIZE_METADATA_KEY.to_string(),
        attachment_size_bytes.to_string(),
    );
    response
}

fn direct_read_terminal_response(
    request: DfsDirectReadRequest,
    result: DfsReadResult,
) -> Result<DfsDirectReadAttachmentResponse, DfsError> {
    let response = DfsReadResponse {
        request_id: request.request_id,
        object_id: request.object_id,
        result,
    };
    Ok(DfsDirectReadAttachmentResponse {
        response: dfs_direct_read_rpc_response(response).map_err(DfsError::from_network)?,
        attachment: ObjectRpcBodySource::empty(),
    })
}

fn ensure_dfs_direct_read_root_layout(data_dir: &SinomoDataDir) -> Result<(), DfsError> {
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })
}

#[cfg(test)]
pub(crate) fn dfs_direct_read_rpc_result(
    response: DfsReadResponse,
) -> Result<ObjectRpcResult, NetworkError> {
    let mut response = encode_dfs_direct_read_rpc_response(response)?;
    let attachment = ObjectRpcIncomingBody::bytes(std::mem::take(&mut response.body));
    Ok(ObjectRpcResult::new(response, attachment))
}

fn dfs_direct_read_rpc_response(
    response: DfsReadResponse,
) -> Result<ObjectRpcResponse, NetworkError> {
    encode_dfs_direct_read_rpc_response(response)
}

fn encode_dfs_direct_read_rpc_response(
    response: DfsReadResponse,
) -> Result<ObjectRpcResponse, NetworkError> {
    let mut rpc_response = ObjectRpcResponse::from_encoded_body(DFS_DIRECT_READ_ROUTE, Vec::new());
    rpc_response.metadata.insert(
        DFS_DIRECT_READ_REQUEST_ID_METADATA_KEY.to_string(),
        response.request_id.clone(),
    );
    rpc_response.metadata.insert(
        DFS_DIRECT_READ_OBJECT_ID_METADATA_KEY.to_string(),
        response.object_id.to_string(),
    );
    match response.result {
        DfsReadResult::Found(data) => {
            rpc_response.metadata.insert(
                DFS_DIRECT_READ_STATUS_METADATA_KEY.to_string(),
                DFS_DIRECT_READ_STATUS_FOUND.to_string(),
            );
            rpc_response.metadata.insert(
                DFS_DIRECT_READ_OBJECT_ID_METADATA_KEY.to_string(),
                data.object_id.to_string(),
            );
            rpc_response.metadata.insert(
                DFS_DIRECT_READ_VERSION_METADATA_KEY.to_string(),
                data.version.to_string(),
            );
            if let Some(content_hash) = data.content_hash {
                rpc_response.metadata.insert(
                    DFS_DIRECT_READ_CONTENT_HASH_METADATA_KEY.to_string(),
                    content_hash,
                );
            }
            rpc_response.metadata.insert(
                DFS_DIRECT_READ_CONTENT_SIZE_METADATA_KEY.to_string(),
                data.bytes.len().to_string(),
            );
            rpc_response.body = data.bytes;
        }
        DfsReadResult::NotFound => {
            rpc_response.metadata.insert(
                DFS_DIRECT_READ_STATUS_METADATA_KEY.to_string(),
                DFS_DIRECT_READ_STATUS_NOT_FOUND.to_string(),
            );
        }
        DfsReadResult::Rejected { reason } => {
            rpc_response.metadata.insert(
                DFS_DIRECT_READ_STATUS_METADATA_KEY.to_string(),
                DFS_DIRECT_READ_STATUS_REJECTED.to_string(),
            );
            rpc_response
                .metadata
                .insert(DFS_DIRECT_READ_REJECTION_METADATA_KEY.to_string(), reason);
        }
        DfsReadResult::RangeFound(data) => {
            rpc_response.metadata.insert(
                DFS_DIRECT_READ_STATUS_METADATA_KEY.to_string(),
                DFS_DIRECT_READ_STATUS_RANGE_FOUND.to_string(),
            );
            rpc_response.metadata.insert(
                DFS_DIRECT_READ_OBJECT_ID_METADATA_KEY.to_string(),
                data.object_id.to_string(),
            );
            rpc_response.metadata.insert(
                DFS_DIRECT_READ_VERSION_METADATA_KEY.to_string(),
                data.version.to_string(),
            );
            if let Some(content_hash) = data.content_hash {
                rpc_response.metadata.insert(
                    DFS_DIRECT_READ_CONTENT_HASH_METADATA_KEY.to_string(),
                    content_hash,
                );
            }
            rpc_response.metadata.insert(
                DFS_DIRECT_READ_CONTENT_SIZE_METADATA_KEY.to_string(),
                data.bytes.len().to_string(),
            );
            rpc_response.metadata.insert(
                DFS_DIRECT_READ_RANGE_OFFSET_METADATA_KEY.to_string(),
                data.offset.to_string(),
            );
            rpc_response.metadata.insert(
                DFS_DIRECT_READ_TOTAL_SIZE_METADATA_KEY.to_string(),
                data.total_size_bytes.to_string(),
            );
            rpc_response.body = data.bytes;
        }
    }
    Ok(rpc_response)
}

pub fn dfs_direct_read_response_from_rpc_result(
    rpc_result: ObjectRpcResult,
) -> Result<DfsDirectReadClientResponse, NetworkError> {
    let (response, attachment) = rpc_result.into_parts();
    response.error_detail()?;
    let status = dfs_direct_read_metadata(&response, DFS_DIRECT_READ_STATUS_METADATA_KEY)?;
    let request_id =
        dfs_direct_read_metadata(&response, DFS_DIRECT_READ_REQUEST_ID_METADATA_KEY)?.to_string();
    let object_id = dfs_direct_read_metadata_id(&response, DFS_DIRECT_READ_OBJECT_ID_METADATA_KEY)?;
    let result = match status {
        DFS_DIRECT_READ_STATUS_FOUND => {
            if !response.body.is_empty() {
                return Err(dfs_network_serialization_error(
                    "DFS direct read content must use the session attachment",
                ));
            }
            let version =
                dfs_direct_read_metadata_id(&response, DFS_DIRECT_READ_VERSION_METADATA_KEY)?;
            let content_hash =
                dfs_direct_read_metadata(&response, DFS_DIRECT_READ_CONTENT_HASH_METADATA_KEY)?
                    .to_string();
            let content_size_bytes = dfs_direct_read_metadata_usize(
                &response,
                DFS_DIRECT_READ_CONTENT_SIZE_METADATA_KEY,
            )?;
            let stream = DfsVerifiedReadStream::new(
                attachment,
                content_size_bytes,
                Some(content_hash.clone()),
            )?;
            DfsDirectReadClientResult::Found(DfsDirectReadClientData {
                object_id: object_id.clone(),
                version,
                content_hash,
                content_size_bytes,
                stream,
            })
        }
        DFS_DIRECT_READ_STATUS_RANGE_FOUND => {
            if !response.body.is_empty() {
                return Err(dfs_network_serialization_error(
                    "DFS direct range-read content must use the session attachment",
                ));
            }
            let version =
                dfs_direct_read_metadata_id(&response, DFS_DIRECT_READ_VERSION_METADATA_KEY)?;
            let content_hash =
                dfs_direct_read_metadata(&response, DFS_DIRECT_READ_CONTENT_HASH_METADATA_KEY)?
                    .to_string();
            let range_size_bytes = dfs_direct_read_metadata_usize(
                &response,
                DFS_DIRECT_READ_CONTENT_SIZE_METADATA_KEY,
            )?;
            let offset =
                dfs_direct_read_metadata_u64(&response, DFS_DIRECT_READ_RANGE_OFFSET_METADATA_KEY)?;
            let total_size_bytes =
                dfs_direct_read_metadata_u64(&response, DFS_DIRECT_READ_TOTAL_SIZE_METADATA_KEY)?;
            if offset > total_size_bytes {
                return Err(dfs_network_serialization_error(format!(
                    "DFS direct range-read offset {offset} exceeds total size {total_size_bytes}"
                )));
            }
            let remaining_size_bytes = total_size_bytes.saturating_sub(offset);
            let range_size_bytes_u64 = u64::try_from(range_size_bytes).unwrap_or(u64::MAX);
            if range_size_bytes_u64 > remaining_size_bytes {
                return Err(dfs_network_serialization_error(format!(
                    "DFS direct range-read has {range_size_bytes} bytes after offset {offset}, exceeding total size {total_size_bytes}"
                )));
            }
            let stream = DfsVerifiedReadStream::new(attachment, range_size_bytes, None)?;
            DfsDirectReadClientResult::RangeFound(DfsDirectReadClientRangeData {
                object_id: object_id.clone(),
                version,
                content_hash,
                offset,
                total_size_bytes,
                range_size_bytes,
                stream,
            })
        }
        DFS_DIRECT_READ_STATUS_NOT_FOUND => DfsDirectReadClientResult::NotFound,
        DFS_DIRECT_READ_STATUS_REJECTED => {
            let reason =
                dfs_direct_read_metadata(&response, DFS_DIRECT_READ_REJECTION_METADATA_KEY)?
                    .to_string();
            DfsDirectReadClientResult::Rejected { reason }
        }
        other => {
            return Err(dfs_network_serialization_error(format!(
                "unknown DFS direct read status {other}"
            )));
        }
    };
    Ok(DfsDirectReadClientResponse {
        request_id,
        object_id,
        result,
    })
}

fn dfs_direct_read_metadata<'a>(
    response: &'a ObjectRpcResponse,
    key: &str,
) -> Result<&'a str, NetworkError> {
    response
        .metadata
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| {
            dfs_network_serialization_error(format!("missing DFS direct read metadata {key}"))
        })
}

fn dfs_direct_read_metadata_id(
    response: &ObjectRpcResponse,
    key: &str,
) -> Result<sinomo_core::Id, NetworkError> {
    let value = dfs_direct_read_metadata(response, key)?;
    sinomo_core::Id::new(value).map_err(|source| {
        dfs_network_serialization_error(format!("invalid DFS direct read metadata {key}: {source}"))
    })
}

fn dfs_direct_read_metadata_usize(
    response: &ObjectRpcResponse,
    key: &str,
) -> Result<usize, NetworkError> {
    let value = dfs_direct_read_metadata(response, key)?;
    value.parse::<usize>().map_err(|source| {
        dfs_network_serialization_error(format!("invalid DFS direct read metadata {key}: {source}"))
    })
}

fn dfs_direct_read_metadata_u64(
    response: &ObjectRpcResponse,
    key: &str,
) -> Result<u64, NetworkError> {
    let value = dfs_direct_read_metadata(response, key)?;
    value.parse::<u64>().map_err(|source| {
        dfs_network_serialization_error(format!("invalid DFS direct read metadata {key}: {source}"))
    })
}

fn dfs_network_serialization_error(detail: impl Into<String>) -> NetworkError {
    NetworkError::Serialization {
        detail: detail.into(),
    }
}

pub(crate) fn direct_read_response_allowed(request: &DfsReadRequest) -> bool {
    matches!(
        (request.range_offset, request.range_max_bytes),
        (None, None) | (Some(_), Some(_))
    )
}
