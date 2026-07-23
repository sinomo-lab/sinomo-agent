//! DFS object-RPC attachment head helpers.
//!
//! DFS uses an attachment only when the domain operation provides a semantic
//! attachment source, such as a reader-backed object transfer. Ordinary DFS values,
//! regardless of encoded size, stay on the stream-native object-RPC value path.

use crate::{
    DfsMessageCommand, DfsMessageResponse, DfsNamespaceOperation, DfsNamespaceTransaction,
    DfsNetworkMessage, DfsReadResult, content_hash,
};
use serde::{Deserialize, Serialize};
use sinomo_core::network::{NetworkError, ObjectRpcRequest, ObjectRpcResponse, SinomoObjectCodec};

/// Integrity contract for one DFS request or response attachment.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsRpcAttachmentDescriptor {
    pub total_size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
}

/// Typed SOBJ request head used whenever a DFS message carries an attachment.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAttachmentMessageCommand {
    pub command: DfsMessageCommand,
    pub attachment: DfsRpcAttachmentDescriptor,
}

/// Typed SOBJ response head used whenever a DFS response carries an attachment.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAttachmentMessageResponse {
    pub response: DfsMessageResponse,
    pub attachment: DfsRpcAttachmentDescriptor,
}

pub(crate) fn encode_dfs_attachment_message_rpc_request(
    request: &ObjectRpcRequest,
    command: DfsMessageCommand,
    attachment: DfsRpcAttachmentDescriptor,
) -> Result<ObjectRpcRequest, NetworkError> {
    let mut head = request.clone();
    head.body = SinomoObjectCodec::network_frame().encode(&DfsAttachmentMessageCommand {
        command,
        attachment,
    })?;
    Ok(head)
}

#[cfg(test)]
pub(crate) fn decode_dfs_attachment_message_rpc_request(
    request: &ObjectRpcRequest,
) -> Result<DfsAttachmentMessageCommand, NetworkError> {
    request.decode_body()
}

pub(crate) fn attach_dfs_message_request_attachment(
    mut head: DfsAttachmentMessageCommand,
    bytes: Vec<u8>,
) -> Result<DfsMessageCommand, NetworkError> {
    validate_attachment(&head.attachment, &bytes, "DFS request")?;
    match &mut head.command.routed_message.message {
        DfsNetworkMessage::WriteRequest(command) => command.bytes = bytes,
        DfsNetworkMessage::DistributedWriteRequest(command)
        | DfsNetworkMessage::ExclusiveCreateRequest(command) => {
            command.content_hash = None;
            command.content_size_bytes = None;
            command.bytes = bytes;
        }
        DfsNetworkMessage::StageBlob(command) => command.bytes = bytes,
        DfsNetworkMessage::StagePatch(command) => command.bytes = bytes,
        DfsNetworkMessage::RepairReplica(command) => command.bytes = bytes,
        DfsNetworkMessage::MetadataSnapshotInstall(command) => {
            command.snapshot = SinomoObjectCodec::network_frame().decode(&bytes)?;
        }
        DfsNetworkMessage::NamespaceRequest(command) => {
            let DfsNamespaceOperation::CommitLocalEventualBatch { transaction, .. } =
                &mut command.operation
            else {
                return Err(serialization_error(
                    "DFS namespace request attachment requires a local-eventual batch head",
                ));
            };
            let decoded: DfsNamespaceTransaction =
                SinomoObjectCodec::network_frame().decode(&bytes)?;
            if decoded.transaction_id != transaction.transaction_id
                || decoded.file_system_id != transaction.file_system_id
                || decoded.file_system_id != command.file_system_id
            {
                return Err(serialization_error(
                    "DFS namespace transaction attachment identity does not match its request head",
                ));
            }
            *transaction = decoded;
        }
        DfsNetworkMessage::FetchBlobResponse(response) => match &mut response.result {
            crate::DfsFetchBlobResult::Found {
                content_hash,
                bytes: response_bytes,
            } => {
                *content_hash =
                    required_attachment_hash(&head.attachment, "DFS fetch response")?.to_string();
                *response_bytes = bytes;
            }
            _ => {
                return Err(serialization_error(
                    "DFS fetch response attachment requires a found response head",
                ));
            }
        },
        DfsNetworkMessage::ReadResponse(response) => match &mut response.result {
            DfsReadResult::Found(data) => data.bytes = bytes,
            DfsReadResult::RangeFound(data) => data.bytes = bytes,
            _ => {
                return Err(serialization_error(
                    "DFS read response attachment requires a found response head",
                ));
            }
        },
        message => {
            return Err(serialization_error(format!(
                "DFS route attachment is not valid for message {}",
                crate::dfs_network_message_trace_label(message)
            )));
        }
    }
    Ok(head.command)
}

pub(crate) fn attach_dfs_routed_message_attachment(
    message: crate::DfsRoutedMessage,
    attachment: DfsRpcAttachmentDescriptor,
    bytes: Vec<u8>,
) -> Result<crate::DfsRoutedMessage, NetworkError> {
    let command = attach_dfs_message_request_attachment(
        DfsAttachmentMessageCommand {
            command: DfsMessageCommand {
                request_id: format!("dfs-attachment-{}-{}", message.frame_id, message.message_id),
                routed_message: message,
            },
            attachment,
        },
        bytes,
    )?;
    Ok(command.routed_message)
}

pub(crate) fn encode_dfs_attachment_message_rpc_response(
    route: &str,
    response: DfsMessageResponse,
    attachment: DfsRpcAttachmentDescriptor,
) -> Result<ObjectRpcResponse, NetworkError> {
    ObjectRpcResponse::encode_body(
        route,
        &DfsAttachmentMessageResponse {
            response,
            attachment,
        },
    )
}

pub(crate) fn decode_dfs_attachment_message_rpc_response(
    response: &ObjectRpcResponse,
) -> Result<DfsAttachmentMessageResponse, NetworkError> {
    response.decode_body()
}

pub(crate) fn attach_dfs_message_response_attachment(
    mut head: DfsAttachmentMessageResponse,
    bytes: Vec<u8>,
) -> Result<DfsMessageResponse, NetworkError> {
    validate_attachment(&head.attachment, &bytes, "DFS response")?;
    for routed in &mut head.response.outbound_messages {
        match &mut routed.message {
            DfsNetworkMessage::ReadResponse(response) => match &mut response.result {
                DfsReadResult::Found(data) => {
                    data.bytes = bytes;
                    return Ok(head.response);
                }
                DfsReadResult::RangeFound(data) => {
                    data.bytes = bytes;
                    return Ok(head.response);
                }
                _ => {}
            },
            DfsNetworkMessage::FetchBlobResponse(response) => {
                if let crate::DfsFetchBlobResult::Found {
                    content_hash,
                    bytes: response_bytes,
                } = &mut response.result
                {
                    *content_hash =
                        required_attachment_hash(&head.attachment, "DFS fetch response")?
                            .to_string();
                    *response_bytes = bytes;
                    return Ok(head.response);
                }
            }
            _ => {}
        }
    }
    Err(serialization_error(
        "DFS response attachment head has no byte-bearing response message",
    ))
}

fn validate_attachment(
    descriptor: &DfsRpcAttachmentDescriptor,
    bytes: &[u8],
    operation: &str,
) -> Result<(), NetworkError> {
    let actual_size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if actual_size != descriptor.total_size_bytes {
        return Err(serialization_error(format!(
            "{operation} attachment length mismatch: expected {}, actual {actual_size}",
            descriptor.total_size_bytes
        )));
    }
    if let Some(expected_hash) = descriptor.content_hash.as_deref() {
        let actual_hash = content_hash(bytes);
        if actual_hash != expected_hash {
            return Err(serialization_error(format!(
                "{operation} attachment hash mismatch: expected {expected_hash}, actual {actual_hash}"
            )));
        }
    }
    Ok(())
}

fn required_attachment_hash<'a>(
    descriptor: &'a DfsRpcAttachmentDescriptor,
    operation: &str,
) -> Result<&'a str, NetworkError> {
    descriptor.content_hash.as_deref().ok_or_else(|| {
        serialization_error(format!(
            "{operation} attachment is missing its content hash"
        ))
    })
}

fn serialization_error(detail: impl Into<String>) -> NetworkError {
    NetworkError::Serialization {
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DfsFileSystemId, DfsObjectId, DfsRoutedMessage, DfsStageBlobCommand, DfsVersion};
    use sinomo_core::node::NodeId;

    fn stage_blob_command(bytes: Vec<u8>) -> DfsMessageCommand {
        let digest = content_hash(&bytes);
        DfsMessageCommand {
            request_id: "attachment-request".to_string(),
            routed_message: DfsRoutedMessage {
                source_node_id: NodeId::from_legacy_named("attachment-source").expect("source id"),
                target_node_id: NodeId::from_legacy_named("attachment-target").expect("target id"),
                frame_id: 7,
                message_id: 11,
                message: DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
                    request_id: "stage-request".to_string(),
                    intent_id: "stage-intent".to_string(),
                    file_system_id: DfsFileSystemId::from_legacy_named("attachment-fs")
                        .expect("filesystem id"),
                    object_id: DfsObjectId::from_legacy_named("attachment-object")
                        .expect("object id"),
                    expected_base_version: None,
                    version: DfsVersion::from_legacy_named("attachment-version").expect("version"),
                    content_hash: digest,
                    bytes,
                    object_metadata: None,
                    authority_shard: None,
                }),
            },
        }
    }

    #[test]
    fn explicit_stage_blob_attachment_round_trips() {
        let bytes = (0..4097)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let digest = content_hash(&bytes);
        let command = stage_blob_command(Vec::new());
        let descriptor = DfsRpcAttachmentDescriptor {
            total_size_bytes: bytes.len() as u64,
            content_hash: Some(digest),
        };
        let attached = attach_dfs_message_request_attachment(
            DfsAttachmentMessageCommand {
                command,
                attachment: descriptor,
            },
            bytes.clone(),
        )
        .expect("attach stage attachment");
        let DfsNetworkMessage::StageBlob(command) = attached.routed_message.message else {
            panic!("expected stage blob");
        };
        assert_eq!(command.bytes, bytes);
    }

    #[test]
    fn request_attachment_rejects_hash_mismatch() {
        let mut bytes = vec![1, 2, 3, 4];
        let command = stage_blob_command(Vec::new());
        let descriptor = DfsRpcAttachmentDescriptor {
            total_size_bytes: bytes.len() as u64,
            content_hash: Some(content_hash(&bytes)),
        };
        bytes[0] ^= 0xff;
        let error = attach_dfs_message_request_attachment(
            DfsAttachmentMessageCommand {
                command,
                attachment: descriptor,
            },
            bytes,
        )
        .expect_err("hash mismatch");
        assert!(error.to_string().contains("hash mismatch"));
    }

    #[test]
    fn transport_integrity_only_attachment_still_checks_length() {
        let command = stage_blob_command(Vec::new());
        let descriptor = DfsRpcAttachmentDescriptor {
            total_size_bytes: 3,
            content_hash: None,
        };
        let attached = attach_dfs_message_request_attachment(
            DfsAttachmentMessageCommand {
                command,
                attachment: descriptor.clone(),
            },
            vec![1, 2, 3],
        )
        .expect("transport verified attachment");
        let DfsNetworkMessage::StageBlob(command) = attached.routed_message.message else {
            panic!("expected stage blob");
        };
        assert_eq!(command.bytes, vec![1, 2, 3]);

        let error = validate_attachment(&descriptor, &[1, 2], "test").expect_err("length mismatch");
        assert!(error.to_string().contains("length mismatch"));
    }
}
