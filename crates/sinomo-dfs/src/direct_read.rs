//! Direct DFS read object-RPC helpers.
//!
//! The normal DFS data plane uses framed [`DfsNetworkMessage`] traffic. Direct
//! read is the optimized read-only path used when a full-object read can be
//! served as a single object-RPC response or a generic streamed object-RPC
//! body carried by `sinomo-network`.

use std::io::Read;

use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    NetworkError, ObjectRpcRequest, ObjectRpcResponse, ObjectRpcStreamBody, ObjectRpcStreamResponse,
};

use crate::DfsError;
use crate::content_store::{BlobReader, LocalContentStore};
use crate::protocol::{
    DfsDirectReadRequest, DfsReadData, DfsReadRequest, DfsReadResponse, DfsReadResult,
};
#[cfg(test)]
use crate::protocol::{DfsMessageCommand, DfsMessageResponse, DfsNetworkMessage, DfsRoutedMessage};
use crate::routes::{DFS_DIRECT_READ_ROUTE, DFS_RPC_CATEGORY};
use crate::transfer::dfs_byte_count_to_usize;

pub(crate) const DFS_DIRECT_READ_STATUS_METADATA_KEY: &str = "sinomo.dfs.direct-read.status";
pub(crate) const DFS_DIRECT_READ_STATUS_FOUND: &str = "found";
const DFS_DIRECT_READ_STATUS_NOT_FOUND: &str = "not-found";
const DFS_DIRECT_READ_STATUS_REJECTED: &str = "rejected";
const DFS_DIRECT_READ_REQUEST_ID_METADATA_KEY: &str = "sinomo.dfs.direct-read.request-id";
const DFS_DIRECT_READ_OBJECT_ID_METADATA_KEY: &str = "sinomo.dfs.direct-read.object-id";
const DFS_DIRECT_READ_VERSION_METADATA_KEY: &str = "sinomo.dfs.direct-read.version";
const DFS_DIRECT_READ_CONTENT_HASH_METADATA_KEY: &str = "sinomo.dfs.direct-read.content-hash";
const DFS_DIRECT_READ_CONTENT_SIZE_METADATA_KEY: &str = "sinomo.dfs.direct-read.content-size";
const DFS_DIRECT_READ_REJECTION_METADATA_KEY: &str = "sinomo.dfs.direct-read.rejection";

pub const DFS_DIRECT_READ_STREAM_CHUNK_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const DFS_DIRECT_REMOTE_READ_MAX_BYTES: u64 = 128 * 1024 * 1024;

#[derive(Debug)]
pub struct DfsDirectReadStreamResponse {
    pub response: ObjectRpcResponse,
    pub reader: Option<BlobReader>,
}

#[derive(Debug)]
pub struct DfsDirectReadClientStreamResponse {
    pub request_id: String,
    pub object_id: crate::DfsObjectId,
    pub result: DfsDirectReadClientStreamResult,
}

#[derive(Debug)]
pub enum DfsDirectReadClientStreamResult {
    Found(DfsDirectReadClientStreamData),
    NotFound,
    Rejected { reason: String },
}

#[derive(Debug)]
pub struct DfsDirectReadClientStreamData {
    pub object_id: crate::DfsObjectId,
    pub version: crate::DfsVersion,
    pub content_hash: String,
    pub content_size_bytes: usize,
    pub body: ObjectRpcStreamBody,
}

impl DfsDirectReadClientStreamResponse {
    pub async fn collect_to_read_response(self) -> Result<DfsReadResponse, NetworkError> {
        let result = match self.result {
            DfsDirectReadClientStreamResult::Found(mut data) => {
                let bytes = data.body.collect_to_vec().await?;
                if bytes.len() != data.content_size_bytes {
                    return Err(dfs_network_serialization_error(format!(
                        "DFS direct read streamed body has {} bytes, metadata expected {}",
                        bytes.len(),
                        data.content_size_bytes
                    )));
                }
                let actual_hash = crate::content_hash(&bytes);
                if actual_hash != data.content_hash {
                    return Err(NetworkError::Serialization {
                        detail: format!(
                            "DFS direct read streamed body hash mismatch: expected {}, actual {}",
                            data.content_hash, actual_hash
                        ),
                    });
                }
                DfsReadResult::Found(DfsReadData {
                    object_id: data.object_id,
                    version: data.version,
                    content_hash: Some(data.content_hash),
                    bytes,
                })
            }
            DfsDirectReadClientStreamResult::NotFound => DfsReadResult::NotFound,
            DfsDirectReadClientStreamResult::Rejected { reason } => {
                DfsReadResult::Rejected { reason }
            }
        };
        Ok(DfsReadResponse {
            request_id: self.request_id,
            object_id: self.object_id,
            result,
        })
    }
}

pub fn dfs_direct_read_rpc_request(
    request: &DfsDirectReadRequest,
) -> Result<ObjectRpcRequest, NetworkError> {
    let mut rpc_request =
        ObjectRpcRequest::encode_body(DFS_DIRECT_READ_ROUTE, DFS_RPC_CATEGORY, request)?;
    sinomo_core::network::set_object_rpc_request_stream_mode(
        &mut rpc_request,
        sinomo_core::network::ObjectRpcRequestStreamMode::RawStreamedDaemonResponse,
    );
    Ok(rpc_request.with_response_retry_safe())
}

#[cfg(test)]
pub(crate) fn dfs_direct_read_rpc_request_from_message_command(
    command: &DfsMessageCommand,
) -> Result<Option<ObjectRpcRequest>, NetworkError> {
    let DfsNetworkMessage::ReadRequest(request) = &command.routed_message.message else {
        return Ok(None);
    };
    let Some(request) = dfs_direct_read_request_from_read_request(request) else {
        return Ok(None);
    };
    dfs_direct_read_rpc_request(&request).map(Some)
}

pub(crate) fn dfs_direct_read_request_from_read_request(
    request: &DfsReadRequest,
) -> Option<DfsDirectReadRequest> {
    let content_size_bytes = request.content_size_bytes?;
    if !direct_full_read_response_allowed(request, content_size_bytes) {
        return None;
    }
    Some(DfsDirectReadRequest {
        request_id: request.request_id.clone(),
        object_id: request.object_id.clone(),
        version: request.version.clone()?,
        content_hash: request.content_hash.clone()?,
        content_size_bytes,
    })
}

pub fn handle_dfs_direct_read_rpc_request(
    data_dir: &SinomoDataDir,
    request: ObjectRpcRequest,
) -> Result<ObjectRpcResponse, DfsError> {
    if request.route != DFS_DIRECT_READ_ROUTE {
        return Err(DfsError::Network {
            message: format!("unknown DFS direct read route {}", request.route),
        });
    }
    let request: DfsDirectReadRequest = request.decode_body().map_err(DfsError::from_network)?;
    let response = serve_dfs_direct_read_request(data_dir, request)?;
    dfs_direct_read_rpc_response(response).map_err(DfsError::from_network)
}

pub fn handle_dfs_direct_read_stream_rpc_request(
    data_dir: &SinomoDataDir,
    request: ObjectRpcRequest,
) -> Result<DfsDirectReadStreamResponse, DfsError> {
    if request.route != DFS_DIRECT_READ_ROUTE {
        return Err(DfsError::Network {
            message: format!("unknown DFS direct read route {}", request.route),
        });
    }
    let request: DfsDirectReadRequest = request.decode_body().map_err(DfsError::from_network)?;
    serve_dfs_direct_read_stream_request(data_dir, request)
}

fn serve_dfs_direct_read_request(
    data_dir: &SinomoDataDir,
    request: DfsDirectReadRequest,
) -> Result<DfsReadResponse, DfsError> {
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?;
    let object_id = request.object_id.clone();
    let result = if request.content_size_bytes > DFS_DIRECT_REMOTE_READ_MAX_BYTES {
        DfsReadResult::Rejected {
            reason: format!(
                "DFS direct read {} is {} bytes, above direct response limit {}",
                request.object_id, request.content_size_bytes, DFS_DIRECT_REMOTE_READ_MAX_BYTES
            ),
        }
    } else {
        let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
        match read_blob_for_direct_read_rpc_response(
            &content,
            &request.content_hash,
            request.content_size_bytes,
        )? {
            Some(bytes) => DfsReadResult::Found(DfsReadData {
                object_id: request.object_id,
                version: request.version,
                content_hash: Some(request.content_hash),
                bytes,
            }),
            None => DfsReadResult::NotFound,
        }
    };
    Ok(DfsReadResponse {
        request_id: request.request_id,
        object_id,
        result,
    })
}

fn serve_dfs_direct_read_stream_request(
    data_dir: &SinomoDataDir,
    request: DfsDirectReadRequest,
) -> Result<DfsDirectReadStreamResponse, DfsError> {
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?;
    if request.content_size_bytes > DFS_DIRECT_REMOTE_READ_MAX_BYTES {
        let response = DfsReadResponse {
            request_id: request.request_id,
            object_id: request.object_id.clone(),
            result: DfsReadResult::Rejected {
                reason: format!(
                    "DFS direct read {} is {} bytes, above direct response limit {}",
                    request.object_id, request.content_size_bytes, DFS_DIRECT_REMOTE_READ_MAX_BYTES
                ),
            },
        };
        return Ok(DfsDirectReadStreamResponse {
            response: dfs_direct_read_rpc_response(response).map_err(DfsError::from_network)?,
            reader: None,
        });
    }
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
    let reader = match content.open_blob_stream(&request.content_hash) {
        Ok(reader) => reader,
        Err(_) => {
            let response = DfsReadResponse {
                request_id: request.request_id,
                object_id: request.object_id,
                result: DfsReadResult::NotFound,
            };
            return Ok(DfsDirectReadStreamResponse {
                response: dfs_direct_read_rpc_response(response).map_err(DfsError::from_network)?,
                reader: None,
            });
        }
    };
    if reader.size_bytes() != request.content_size_bytes {
        let response = DfsReadResponse {
            request_id: request.request_id,
            object_id: request.object_id,
            result: DfsReadResult::NotFound,
        };
        return Ok(DfsDirectReadStreamResponse {
            response: dfs_direct_read_rpc_response(response).map_err(DfsError::from_network)?,
            reader: None,
        });
    }
    let mut response = ObjectRpcResponse::from_encoded_body(DFS_DIRECT_READ_ROUTE, Vec::new());
    response.metadata.insert(
        DFS_DIRECT_READ_STATUS_METADATA_KEY.to_string(),
        DFS_DIRECT_READ_STATUS_FOUND.to_string(),
    );
    response.metadata.insert(
        DFS_DIRECT_READ_REQUEST_ID_METADATA_KEY.to_string(),
        request.request_id,
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
        request.content_hash,
    );
    response.metadata.insert(
        DFS_DIRECT_READ_CONTENT_SIZE_METADATA_KEY.to_string(),
        request.content_size_bytes.to_string(),
    );
    sinomo_core::network::set_object_rpc_response_stream_mode(
        &mut response,
        sinomo_core::network::ObjectRpcResponseStreamMode::RawStreamedDaemonBody,
    );
    Ok(DfsDirectReadStreamResponse {
        response,
        reader: Some(reader),
    })
}

fn read_blob_for_direct_read_rpc_response(
    content: &LocalContentStore,
    content_hash: &str,
    expected_size_bytes: u64,
) -> Result<Option<Vec<u8>>, DfsError> {
    let mut reader = match content.open_blob_stream(content_hash) {
        Ok(reader) => reader,
        Err(_) => return Ok(None),
    };
    if reader.size_bytes() != expected_size_bytes {
        return Ok(None);
    }
    let capacity = dfs_byte_count_to_usize("DFS direct read RPC", expected_size_bytes)?;
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

#[cfg(test)]
pub(crate) fn dfs_direct_read_rpc_response(
    response: DfsReadResponse,
) -> Result<ObjectRpcResponse, NetworkError> {
    encode_dfs_direct_read_rpc_response(response)
}

#[cfg(not(test))]
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
        DfsReadResult::RangeFound(_) => {
            return Err(dfs_network_serialization_error(
                "DFS direct read route cannot encode range responses",
            ));
        }
    }
    Ok(rpc_response)
}

#[cfg(test)]
pub(crate) fn dfs_message_response_from_direct_read_rpc_response(
    command: &DfsMessageCommand,
    response: ObjectRpcResponse,
) -> Result<DfsMessageResponse, NetworkError> {
    let response = dfs_direct_read_response_from_rpc_response(response)?;
    Ok(DfsMessageResponse {
        request_id: command.request_id.clone(),
        outbound_messages: vec![DfsRoutedMessage {
            source_node_id: command.routed_message.target_node_id.clone(),
            target_node_id: command.routed_message.source_node_id.clone(),
            frame_id: command.routed_message.frame_id,
            message_id: command.routed_message.message_id,
            message: DfsNetworkMessage::ReadResponse(response),
        }],
    })
}

pub fn dfs_direct_read_response_from_rpc_response(
    mut response: ObjectRpcResponse,
) -> Result<DfsReadResponse, NetworkError> {
    response.error_detail()?;
    let status = dfs_direct_read_metadata(&response, DFS_DIRECT_READ_STATUS_METADATA_KEY)?;
    let request_id =
        dfs_direct_read_metadata(&response, DFS_DIRECT_READ_REQUEST_ID_METADATA_KEY)?.to_string();
    let object_id = dfs_direct_read_metadata_id(&response, DFS_DIRECT_READ_OBJECT_ID_METADATA_KEY)?;
    let result = match status {
        DFS_DIRECT_READ_STATUS_FOUND => {
            let version =
                dfs_direct_read_metadata_id(&response, DFS_DIRECT_READ_VERSION_METADATA_KEY)?;
            let content_hash =
                dfs_direct_read_metadata(&response, DFS_DIRECT_READ_CONTENT_HASH_METADATA_KEY)?
                    .to_string();
            let content_size_bytes = dfs_direct_read_metadata_usize(
                &response,
                DFS_DIRECT_READ_CONTENT_SIZE_METADATA_KEY,
            )?;
            if response.body.len() != content_size_bytes {
                return Err(dfs_network_serialization_error(format!(
                    "DFS direct read response body has {} bytes, metadata expected {}",
                    response.body.len(),
                    content_size_bytes
                )));
            }
            DfsReadResult::Found(DfsReadData {
                object_id: object_id.clone(),
                version,
                content_hash: Some(content_hash),
                bytes: std::mem::take(&mut response.body),
            })
        }
        DFS_DIRECT_READ_STATUS_NOT_FOUND => DfsReadResult::NotFound,
        DFS_DIRECT_READ_STATUS_REJECTED => {
            let reason =
                dfs_direct_read_metadata(&response, DFS_DIRECT_READ_REJECTION_METADATA_KEY)?
                    .to_string();
            DfsReadResult::Rejected { reason }
        }
        other => {
            return Err(dfs_network_serialization_error(format!(
                "unknown DFS direct read status {other}"
            )));
        }
    };
    Ok(DfsReadResponse {
        request_id,
        object_id,
        result,
    })
}

pub fn dfs_direct_read_response_from_rpc_stream_response(
    stream_response: ObjectRpcStreamResponse,
) -> Result<DfsDirectReadClientStreamResponse, NetworkError> {
    let ObjectRpcStreamResponse { response, body } = stream_response;
    response.error_detail()?;
    let status = dfs_direct_read_metadata(&response, DFS_DIRECT_READ_STATUS_METADATA_KEY)?;
    let request_id =
        dfs_direct_read_metadata(&response, DFS_DIRECT_READ_REQUEST_ID_METADATA_KEY)?.to_string();
    let object_id = dfs_direct_read_metadata_id(&response, DFS_DIRECT_READ_OBJECT_ID_METADATA_KEY)?;
    let result = match status {
        DFS_DIRECT_READ_STATUS_FOUND => {
            let version =
                dfs_direct_read_metadata_id(&response, DFS_DIRECT_READ_VERSION_METADATA_KEY)?;
            let content_hash =
                dfs_direct_read_metadata(&response, DFS_DIRECT_READ_CONTENT_HASH_METADATA_KEY)?
                    .to_string();
            let content_size_bytes = dfs_direct_read_metadata_usize(
                &response,
                DFS_DIRECT_READ_CONTENT_SIZE_METADATA_KEY,
            )?;
            DfsDirectReadClientStreamResult::Found(DfsDirectReadClientStreamData {
                object_id: object_id.clone(),
                version,
                content_hash,
                content_size_bytes,
                body,
            })
        }
        DFS_DIRECT_READ_STATUS_NOT_FOUND => DfsDirectReadClientStreamResult::NotFound,
        DFS_DIRECT_READ_STATUS_REJECTED => {
            let reason =
                dfs_direct_read_metadata(&response, DFS_DIRECT_READ_REJECTION_METADATA_KEY)?
                    .to_string();
            DfsDirectReadClientStreamResult::Rejected { reason }
        }
        other => {
            return Err(dfs_network_serialization_error(format!(
                "unknown DFS direct read status {other}"
            )));
        }
    };
    Ok(DfsDirectReadClientStreamResponse {
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

fn dfs_network_serialization_error(detail: impl Into<String>) -> NetworkError {
    NetworkError::Serialization {
        detail: detail.into(),
    }
}

pub(crate) fn direct_full_read_response_allowed(
    request: &DfsReadRequest,
    total_size_bytes: u64,
) -> bool {
    request.range_offset.is_none()
        && request.range_max_bytes.is_none()
        && request.start_chunk_index == 0
        && request.max_chunks == 0
        && total_size_bytes <= DFS_DIRECT_REMOTE_READ_MAX_BYTES
}
