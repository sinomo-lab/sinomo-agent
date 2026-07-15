//! Body-aware DFS object-RPC head helpers.
//!
//! DFS names operations and declares expected lengths/digests in SOBJ heads;
//! `sinomo-network` owns the private transfer frames.  The helpers here keep
//! the existing in-process [`DfsNetworkMessage`] state machine while moving
//! large byte vectors and oversized structured values out of its wire
//! representation.

use crate::{
    DfsMessageCommand, DfsMessageResponse, DfsMetadataTableSnapshot, DfsNamespaceOperation,
    DfsNamespaceTransaction, DfsNetworkMessage, DfsReadResult, content_hash,
};
use serde::{Deserialize, Serialize};
use sinomo_core::network::{NetworkError, ObjectRpcRequest, ObjectRpcResponse, SinomoObjectCodec};

/// Integrity contract for one DFS request or response attachment.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsRpcBodyDescriptor {
    pub total_size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
}

/// Typed SOBJ request head used whenever a DFS message carries an attachment.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsBodyMessageCommand {
    pub command: DfsMessageCommand,
    pub body: DfsRpcBodyDescriptor,
}

/// Typed SOBJ response head used whenever a DFS response carries an attachment.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsBodyMessageResponse {
    pub response: DfsMessageResponse,
    pub body: DfsRpcBodyDescriptor,
}

pub(crate) fn split_dfs_message_request_body(
    command: &DfsMessageCommand,
) -> Result<Option<(DfsMessageCommand, DfsRpcBodyDescriptor, Vec<u8>)>, NetworkError> {
    let mut head = command.clone();
    let (bytes, declared_hash) = match &mut head.routed_message.message {
        DfsNetworkMessage::WriteRequest(command) => {
            let bytes = std::mem::take(&mut command.bytes);
            let digest = content_hash(&bytes);
            (bytes, digest)
        }
        DfsNetworkMessage::DistributedWriteRequest(command)
        | DfsNetworkMessage::ExclusiveCreateRequest(command) => {
            let bytes = std::mem::take(&mut command.bytes);
            // These bytes are still an inline DFS write semantically; only
            // their wire transport is streamed as an RPC attachment. Keep the
            // manifest fields empty in the head so reassembly cannot turn the
            // command into an invalid manifest-plus-inline hybrid.
            let digest = content_hash(&bytes);
            command.content_hash = None;
            command.content_size_bytes = None;
            (bytes, digest)
        }
        DfsNetworkMessage::StageBlob(command) => (
            std::mem::take(&mut command.bytes),
            command.content_hash.clone(),
        ),
        DfsNetworkMessage::StagePatch(command) => (
            std::mem::take(&mut command.bytes),
            command.patch_hash.clone(),
        ),
        DfsNetworkMessage::RepairReplica(command) => (
            std::mem::take(&mut command.bytes),
            command.content_hash.clone(),
        ),
        DfsNetworkMessage::MetadataSnapshotInstall(command) => {
            let snapshot = std::mem::replace(
                &mut command.snapshot,
                DfsMetadataTableSnapshot {
                    // Invalid as a standalone snapshot. The request-body
                    // decoder replaces this sentinel before DFS dispatch.
                    format_version: 0,
                    materialized_through_sequence: None,
                    tables: None,
                    ops: Vec::new(),
                    semantic_state_fingerprint: None,
                    prefix_proof: None,
                },
            );
            let bytes = SinomoObjectCodec::network_frame().encode(&snapshot)?;
            let digest = content_hash(&bytes);
            (bytes, digest)
        }
        DfsNetworkMessage::NamespaceRequest(command) => {
            let DfsNamespaceOperation::ApplyTransaction { transaction } = &mut command.operation
            else {
                return Ok(None);
            };
            // A same-shard bulk write is one atomic metadata transaction even
            // when its encoded object/version rows exceed the bounded session
            // control head. Move that complete value into one authenticated
            // body instead of splitting it into independently visible RPCs.
            // The retained identity/filesystem plus empty row sets form an
            // invalid standalone sentinel that cannot be applied before body
            // collection and validation reconstruct the original value.
            let sentinel = DfsNamespaceTransaction {
                transaction_id: transaction.transaction_id.clone(),
                file_system_id: transaction.file_system_id.clone(),
                objects: Vec::new(),
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: transaction.created_at,
            };
            let transaction = std::mem::replace(transaction, sentinel);
            let bytes = SinomoObjectCodec::network_frame().encode(&transaction)?;
            let digest = content_hash(&bytes);
            (bytes, digest)
        }
        _ => return Ok(None),
    };
    if bytes.is_empty() {
        return Ok(None);
    }
    let descriptor = DfsRpcBodyDescriptor {
        total_size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        content_hash: Some(declared_hash),
    };
    Ok(Some((head, descriptor, bytes)))
}

pub(crate) fn encode_dfs_body_message_rpc_request(
    request: &ObjectRpcRequest,
    command: DfsMessageCommand,
    body: DfsRpcBodyDescriptor,
) -> Result<ObjectRpcRequest, NetworkError> {
    let mut head = request.clone();
    head.body =
        SinomoObjectCodec::network_frame().encode(&DfsBodyMessageCommand { command, body })?;
    Ok(head)
}

pub(crate) fn decode_dfs_body_message_rpc_request(
    request: &ObjectRpcRequest,
) -> Result<DfsBodyMessageCommand, NetworkError> {
    request.decode_body()
}

pub(crate) fn attach_dfs_message_request_body(
    mut head: DfsBodyMessageCommand,
    bytes: Vec<u8>,
) -> Result<DfsMessageCommand, NetworkError> {
    validate_body(&head.body, &bytes, "DFS request")?;
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
            let DfsNamespaceOperation::ApplyTransaction { transaction } = &mut command.operation
            else {
                return Err(serialization_error(
                    "DFS namespace request body requires an apply-transaction head",
                ));
            };
            let decoded: DfsNamespaceTransaction =
                SinomoObjectCodec::network_frame().decode(&bytes)?;
            if decoded.transaction_id != transaction.transaction_id
                || decoded.file_system_id != transaction.file_system_id
                || decoded.file_system_id != command.file_system_id
            {
                return Err(serialization_error(
                    "DFS namespace transaction body identity does not match its request head",
                ));
            }
            *transaction = decoded;
        }
        DfsNetworkMessage::FetchBlobResponse(response) => match &mut response.result {
            crate::DfsFetchBlobResult::Found {
                content_hash,
                bytes: response_bytes,
            } => {
                *content_hash = required_body_hash(&head.body, "DFS fetch response")?.to_string();
                *response_bytes = bytes;
            }
            _ => {
                return Err(serialization_error(
                    "DFS fetch response body requires a found response head",
                ));
            }
        },
        DfsNetworkMessage::ReadResponse(response) => match &mut response.result {
            DfsReadResult::Found(data) => data.bytes = bytes,
            DfsReadResult::RangeFound(data) => data.bytes = bytes,
            _ => {
                return Err(serialization_error(
                    "DFS read response body requires a found response head",
                ));
            }
        },
        message => {
            return Err(serialization_error(format!(
                "DFS route body is not valid for message {}",
                crate::dfs_network_message_trace_label(message)
            )));
        }
    }
    Ok(head.command)
}

pub(crate) fn attach_dfs_routed_message_body(
    message: crate::DfsRoutedMessage,
    body: DfsRpcBodyDescriptor,
    bytes: Vec<u8>,
) -> Result<crate::DfsRoutedMessage, NetworkError> {
    let command = attach_dfs_message_request_body(
        DfsBodyMessageCommand {
            command: DfsMessageCommand {
                request_id: format!("dfs-body-{}-{}", message.frame_id, message.message_id),
                routed_message: message,
            },
            body,
        },
        bytes,
    )?;
    Ok(command.routed_message)
}

/// Moves one byte-bearing DFS response out of its message envelope.
pub(crate) fn split_dfs_message_response_body(
    response: &DfsMessageResponse,
) -> Option<(DfsMessageResponse, DfsRpcBodyDescriptor, Vec<u8>)> {
    let mut head = response.clone();
    for routed in &mut head.outbound_messages {
        let (bytes, digest) = match &mut routed.message {
            DfsNetworkMessage::ReadResponse(response) => match &mut response.result {
                DfsReadResult::Found(data) => {
                    let bytes = std::mem::take(&mut data.bytes);
                    let digest = data
                        .content_hash
                        .clone()
                        .unwrap_or_else(|| content_hash(&bytes));
                    data.content_hash = Some(digest.clone());
                    (bytes, digest)
                }
                DfsReadResult::RangeFound(data) => {
                    let bytes = std::mem::take(&mut data.bytes);
                    let digest = content_hash(&bytes);
                    (bytes, digest)
                }
                _ => continue,
            },
            DfsNetworkMessage::FetchBlobResponse(response) => match &mut response.result {
                crate::DfsFetchBlobResult::Found {
                    content_hash,
                    bytes,
                } => (std::mem::take(bytes), content_hash.clone()),
                _ => continue,
            },
            _ => continue,
        };
        if bytes.is_empty() {
            return None;
        }
        let descriptor = DfsRpcBodyDescriptor {
            total_size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            content_hash: Some(digest),
        };
        return Some((head, descriptor, bytes));
    }
    None
}

pub(crate) fn encode_dfs_body_message_rpc_response(
    route: &str,
    response: DfsMessageResponse,
    body: DfsRpcBodyDescriptor,
) -> Result<ObjectRpcResponse, NetworkError> {
    ObjectRpcResponse::encode_body(route, &DfsBodyMessageResponse { response, body })
}

pub(crate) fn decode_dfs_body_message_rpc_response(
    response: &ObjectRpcResponse,
) -> Result<DfsBodyMessageResponse, NetworkError> {
    response.decode_body()
}

pub(crate) fn attach_dfs_message_response_body(
    mut head: DfsBodyMessageResponse,
    bytes: Vec<u8>,
) -> Result<DfsMessageResponse, NetworkError> {
    validate_body(&head.body, &bytes, "DFS response")?;
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
                        required_body_hash(&head.body, "DFS fetch response")?.to_string();
                    *response_bytes = bytes;
                    return Ok(head.response);
                }
            }
            _ => {}
        }
    }
    Err(serialization_error(
        "DFS response body head has no byte-bearing response message",
    ))
}

fn validate_body(
    descriptor: &DfsRpcBodyDescriptor,
    bytes: &[u8],
    operation: &str,
) -> Result<(), NetworkError> {
    let actual_size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if actual_size != descriptor.total_size_bytes {
        return Err(serialization_error(format!(
            "{operation} body length mismatch: expected {}, actual {actual_size}",
            descriptor.total_size_bytes
        )));
    }
    if let Some(expected_hash) = descriptor.content_hash.as_deref() {
        let actual_hash = content_hash(bytes);
        if actual_hash != expected_hash {
            return Err(serialization_error(format!(
                "{operation} body hash mismatch: expected {expected_hash}, actual {actual_hash}"
            )));
        }
    }
    Ok(())
}

fn required_body_hash<'a>(
    descriptor: &'a DfsRpcBodyDescriptor,
    operation: &str,
) -> Result<&'a str, NetworkError> {
    descriptor
        .content_hash
        .as_deref()
        .ok_or_else(|| serialization_error(format!("{operation} body is missing its content hash")))
}

fn serialization_error(detail: impl Into<String>) -> NetworkError {
    NetworkError::Serialization {
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_records::DfsMetadataTableOp;
    use crate::{
        DfsDistributedWriteCommand, DfsFileSystemId, DfsMetadataShardId,
        DfsMetadataSnapshotInstallCommand, DfsNamespaceCommand, DfsObjectId, DfsObjectKind,
        DfsObjectMetadata, DfsReplicationPolicy, DfsRoutedMessage, DfsStageBlobCommand, DfsVersion,
        namespace_transaction_id_for_batch, object_version_record, sifs_file_system_id,
    };
    use sinomo_core::node::NodeId;
    use sinomo_core::{DfsObjectPolicy, DfsWriteRequest};

    const SESSION_CONTROL_FRAME_MAX_BYTES: usize = 64 * 1024;

    fn stage_blob_command(bytes: Vec<u8>) -> DfsMessageCommand {
        let digest = content_hash(&bytes);
        DfsMessageCommand {
            request_id: "body-request".to_string(),
            routed_message: DfsRoutedMessage {
                source_node_id: NodeId::from_legacy_named("body-source").expect("source id"),
                target_node_id: NodeId::from_legacy_named("body-target").expect("target id"),
                frame_id: 7,
                message_id: 11,
                message: DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
                    request_id: "stage-request".to_string(),
                    intent_id: "stage-intent".to_string(),
                    file_system_id: DfsFileSystemId::from_legacy_named("body-fs")
                        .expect("filesystem id"),
                    object_id: DfsObjectId::from_legacy_named("body-object").expect("object id"),
                    expected_base_version: None,
                    version: DfsVersion::from_legacy_named("body-version").expect("version"),
                    content_hash: digest,
                    bytes,
                    object_metadata: None,
                    authority_shard: None,
                }),
            },
        }
    }

    fn oversized_namespace_transaction_command() -> DfsMessageCommand {
        let source =
            NodeId::from_legacy_named("namespace-body-source").expect("namespace body source");
        let target =
            NodeId::from_legacy_named("namespace-body-target").expect("namespace body target");
        let mut objects = Vec::new();
        let mut versions = Vec::new();
        for index in 0..65 {
            let object = DfsObjectMetadata {
                object_id: DfsObjectId::from_legacy_named(format!(
                    "namespace_body_object_{index:03}"
                ))
                .expect("namespace body object"),
                file_system_id: sifs_file_system_id(),
                path: format!(
                    "/services/cluster-tests/body-transport/file-{index:03}-{}.bin",
                    "x".repeat(64)
                ),
                kind: DfsObjectKind::File,
                owner_node_id: source.clone(),
                current_locations: vec![source.clone()],
                content_hash: Some(format!(
                    "namespace-body-content-{index:03}-{}",
                    "a".repeat(64)
                )),
                content_size_bytes: Some(4096),
                link_target_object_id: None,
                mode: 0o644,
                uid: 0,
                gid: 0,
                accessed_at_unix_nanos: 1,
                modified_at_unix_nanos: 1,
                created_at_unix_nanos: 1,
                changed_at_unix_nanos: 1,
                version: DfsVersion::from_legacy_named(format!(
                    "namespace_body_version_{index:03}"
                ))
                .expect("namespace body version"),
                policy: DfsObjectPolicy::ReplicatedEventual,
                created_by_event_id: None,
                last_modified_by_event_id: None,
            };
            versions.push(object_version_record(&object, object.content_size_bytes));
            objects.push(object);
        }
        let file_system_id = sifs_file_system_id();
        let transaction = DfsNamespaceTransaction {
            transaction_id: namespace_transaction_id_for_batch(
                "visible-batch",
                &file_system_id,
                &objects,
                &versions,
            ),
            file_system_id: file_system_id.clone(),
            objects,
            versions,
            advisory_lock_states: Vec::new(),
            created_at: time::OffsetDateTime::UNIX_EPOCH,
        };
        DfsMessageCommand {
            request_id: "namespace-body-request".to_string(),
            routed_message: DfsRoutedMessage {
                source_node_id: source.clone(),
                target_node_id: target,
                frame_id: 29,
                message_id: 31,
                message: DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
                    request_id: "namespace-body-apply".to_string(),
                    file_system_id,
                    requesting_node_id: source,
                    include_metadata_journal_batches: false,
                    metadata_journal_after_sequences: Vec::new(),
                    authority_shards: Vec::new(),
                    operation: DfsNamespaceOperation::ApplyTransaction { transaction },
                }),
            },
        }
    }

    #[test]
    fn request_body_split_and_attach_round_trip() {
        let command = stage_blob_command((0..4097).map(|index| (index % 251) as u8).collect());
        let (head, descriptor, bytes) = split_dfs_message_request_body(&command)
            .expect("encode body")
            .expect("split body");
        assert_eq!(descriptor.total_size_bytes, bytes.len() as u64);
        assert_eq!(
            descriptor.content_hash.as_deref(),
            Some(content_hash(&bytes).as_str())
        );
        let DfsNetworkMessage::StageBlob(head_command) = &head.routed_message.message else {
            panic!("expected stage blob head");
        };
        assert!(head_command.bytes.is_empty());

        let attached = attach_dfs_message_request_body(
            DfsBodyMessageCommand {
                command: head,
                body: descriptor,
            },
            bytes,
        )
        .expect("attach body");
        assert_eq!(attached, command);
    }

    #[test]
    fn large_metadata_snapshot_install_streams_outside_the_message_head() {
        let source = NodeId::from_legacy_named("snapshot-body-source").expect("source id");
        let target = NodeId::from_legacy_named("snapshot-body-target").expect("target id");
        let file_system_id =
            DfsFileSystemId::from_legacy_named("snapshot-body-fs").expect("filesystem id");
        let snapshot = DfsMetadataTableSnapshot {
            format_version: crate::metadata_table_store::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            materialized_through_sequence: Some(41),
            tables: None,
            ops: (0..1024)
                .map(|index| DfsMetadataTableOp::RemovePath {
                    file_system_id: file_system_id.clone(),
                    path: format!("/streamed-snapshot/{index:04}/{}", "x".repeat(128)),
                })
                .collect(),
            semantic_state_fingerprint: None,
            prefix_proof: None,
        };
        let command = DfsMessageCommand {
            request_id: "snapshot-body-request".to_string(),
            routed_message: DfsRoutedMessage {
                source_node_id: source.clone(),
                target_node_id: target,
                frame_id: 19,
                message_id: 23,
                message: DfsNetworkMessage::MetadataSnapshotInstall(
                    DfsMetadataSnapshotInstallCommand {
                        request_id: "snapshot-install".to_string(),
                        source_node_id: source,
                        file_system_id: Some(file_system_id),
                        authority_epoch: Some(7),
                        shard_id: DfsMetadataShardId("snapshot-shard".to_string()),
                        next_after_sequence: 41,
                        snapshot,
                        reason: "repair a compacted metadata prefix".to_string(),
                    },
                ),
            },
        };

        let (head, descriptor, body) = split_dfs_message_request_body(&command)
            .expect("encode snapshot body")
            .expect("split snapshot body");
        assert!(
            body.len() > 64 * 1024,
            "fixture must exceed message frame cap"
        );
        assert_eq!(descriptor.total_size_bytes, body.len() as u64);
        assert!(
            SinomoObjectCodec::network_frame()
                .encode(&head)
                .expect("encode snapshot head")
                .len()
                < 64 * 1024,
            "streaming must keep the message head under the daemon frame cap"
        );
        let DfsNetworkMessage::MetadataSnapshotInstall(head_command) = &head.routed_message.message
        else {
            panic!("expected snapshot install head");
        };
        assert_eq!(head_command.snapshot.format_version, 0);
        assert!(head_command.snapshot.ops.is_empty());

        let attached = attach_dfs_message_request_body(
            DfsBodyMessageCommand {
                command: head,
                body: descriptor,
            },
            body,
        )
        .expect("attach snapshot body");
        assert_eq!(attached, command);
    }

    #[test]
    fn oversized_namespace_transaction_streams_as_one_exact_atomic_body() {
        let command = oversized_namespace_transaction_command();
        let inline_request = crate::dfs_message_rpc_request(&command).expect("inline request");
        assert!(
            inline_request.body.len() > SESSION_CONTROL_FRAME_MAX_BYTES,
            "fixture must reproduce an oversized inline namespace request: {} bytes",
            inline_request.body.len()
        );

        let (head, descriptor, body) = split_dfs_message_request_body(&command)
            .expect("encode namespace transaction body")
            .expect("split namespace transaction body");
        assert!(
            body.len() > SESSION_CONTROL_FRAME_MAX_BYTES,
            "fixture must move the oversized transaction into the body"
        );
        assert_eq!(descriptor.total_size_bytes, body.len() as u64);
        assert_eq!(
            descriptor.content_hash.as_deref(),
            Some(content_hash(&body).as_str())
        );

        let streamed_request =
            encode_dfs_body_message_rpc_request(&inline_request, head.clone(), descriptor.clone())
                .expect("streamed namespace request head");
        assert!(
            streamed_request.body.len() < SESSION_CONTROL_FRAME_MAX_BYTES,
            "streamed namespace request head must fit the control frame: {} bytes",
            streamed_request.body.len()
        );

        let DfsNetworkMessage::NamespaceRequest(head_command) = &head.routed_message.message else {
            panic!("expected namespace request head");
        };
        let DfsNamespaceOperation::ApplyTransaction {
            transaction: sentinel,
        } = &head_command.operation
        else {
            panic!("expected apply-transaction sentinel");
        };
        assert!(sentinel.objects.is_empty());
        assert!(sentinel.versions.is_empty());
        assert!(sentinel.advisory_lock_states.is_empty());

        let DfsNetworkMessage::NamespaceRequest(original_command) = &command.routed_message.message
        else {
            panic!("expected original namespace request");
        };
        let DfsNamespaceOperation::ApplyTransaction {
            transaction: original_transaction,
        } = &original_command.operation
        else {
            panic!("expected original apply transaction");
        };
        assert_eq!(sentinel.transaction_id, original_transaction.transaction_id);
        assert_eq!(sentinel.file_system_id, original_transaction.file_system_id);
        let decoded_transaction: DfsNamespaceTransaction = SinomoObjectCodec::network_frame()
            .decode(&body)
            .expect("decode transaction body");
        assert_eq!(&decoded_transaction, original_transaction);
        assert_eq!(
            decoded_transaction
                .objects
                .iter()
                .map(|object| object.object_id.clone())
                .collect::<Vec<_>>(),
            original_transaction
                .objects
                .iter()
                .map(|object| object.object_id.clone())
                .collect::<Vec<_>>(),
            "body transport must preserve transaction record order"
        );

        let attached = attach_dfs_message_request_body(
            DfsBodyMessageCommand {
                command: head,
                body: descriptor,
            },
            body,
        )
        .expect("attach namespace transaction body");
        assert_eq!(attached, command);
    }

    #[test]
    fn namespace_transaction_body_rejects_truncation_before_reconstruction() {
        let command = oversized_namespace_transaction_command();
        let (head, descriptor, mut body) = split_dfs_message_request_body(&command)
            .expect("encode namespace transaction body")
            .expect("split namespace transaction body");
        body.pop().expect("non-empty transaction body");
        let error = attach_dfs_message_request_body(
            DfsBodyMessageCommand {
                command: head,
                body: descriptor,
            },
            body,
        )
        .expect_err("truncated transaction body");
        assert!(error.to_string().contains("length mismatch"));
    }

    #[test]
    fn namespace_transaction_body_rejects_head_identity_mismatch() {
        let command = oversized_namespace_transaction_command();
        let (mut head, descriptor, body) = split_dfs_message_request_body(&command)
            .expect("encode namespace transaction body")
            .expect("split namespace transaction body");
        let DfsNetworkMessage::NamespaceRequest(head_command) = &mut head.routed_message.message
        else {
            panic!("expected namespace request head");
        };
        let DfsNamespaceOperation::ApplyTransaction { transaction } = &mut head_command.operation
        else {
            panic!("expected apply-transaction sentinel");
        };
        transaction.transaction_id = "mismatched-transaction-id".to_string();

        let error = attach_dfs_message_request_body(
            DfsBodyMessageCommand {
                command: head,
                body: descriptor,
            },
            body,
        )
        .expect_err("mismatched transaction identity");
        assert!(error.to_string().contains("identity does not match"));
    }

    #[test]
    fn distributed_write_attachment_round_trip_preserves_inline_semantics() {
        let source = NodeId::from_legacy_named("body-inline-source").expect("source id");
        let target = NodeId::from_legacy_named("body-inline-target").expect("target id");
        let bytes = b"streamed on the wire, inline in DFS".to_vec();
        let command = DfsMessageCommand {
            request_id: "body-inline-request".to_string(),
            routed_message: DfsRoutedMessage {
                source_node_id: source.clone(),
                target_node_id: target,
                frame_id: 13,
                message_id: 17,
                message: DfsNetworkMessage::DistributedWriteRequest(DfsDistributedWriteCommand {
                    request_id: "distributed-inline".to_string(),
                    request: DfsWriteRequest {
                        file_system_id: DfsFileSystemId::from_legacy_named("body-inline-fs")
                            .expect("filesystem id"),
                        object_id: DfsObjectId::from_legacy_named("body-inline-object")
                            .expect("object id"),
                        target_version: DfsVersion::from_legacy_named("body-inline-version")
                            .expect("version"),
                        requesting_node_id: source.clone(),
                    },
                    expected_base_version: None,
                    metadata_journal_after_sequence: None,
                    bytes: bytes.clone(),
                    content_hash: None,
                    content_size_bytes: None,
                    desired_replica_nodes: vec![source],
                    policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                    object_metadata: None,
                    authority_shard: None,
                    touch_parent_on_create: true,
                    foreground_replica_repair: true,
                    exclusive_create: false,
                }),
            },
        };

        let (head, descriptor, body) = split_dfs_message_request_body(&command)
            .expect("encode body")
            .expect("split body");
        let DfsNetworkMessage::DistributedWriteRequest(head_write) = &head.routed_message.message
        else {
            panic!("expected distributed write head");
        };
        assert!(head_write.bytes.is_empty());
        assert_eq!(head_write.content_hash, None);
        assert_eq!(head_write.content_size_bytes, None);
        assert_eq!(
            descriptor.content_hash.as_deref(),
            Some(content_hash(&bytes).as_str())
        );

        let attached = attach_dfs_message_request_body(
            DfsBodyMessageCommand {
                command: head,
                body: descriptor,
            },
            body,
        )
        .expect("attach body");
        assert_eq!(attached, command);
    }

    #[test]
    fn request_body_attach_rejects_hash_mismatch() {
        let command = stage_blob_command(vec![1, 2, 3, 4]);
        let (head, descriptor, mut bytes) = split_dfs_message_request_body(&command)
            .expect("encode body")
            .expect("split body");
        bytes[0] ^= 0xff;
        let error = attach_dfs_message_request_body(
            DfsBodyMessageCommand {
                command: head,
                body: descriptor,
            },
            bytes,
        )
        .expect_err("hash mismatch");
        assert!(error.to_string().contains("hash mismatch"));
    }

    #[test]
    fn transport_integrity_only_body_still_checks_length() {
        let command = stage_blob_command(Vec::new());
        let descriptor = DfsRpcBodyDescriptor {
            total_size_bytes: 3,
            content_hash: None,
        };
        let attached = attach_dfs_message_request_body(
            DfsBodyMessageCommand {
                command,
                body: descriptor.clone(),
            },
            vec![1, 2, 3],
        )
        .expect("transport verified body");
        let DfsNetworkMessage::StageBlob(command) = attached.routed_message.message else {
            panic!("expected stage blob");
        };
        assert_eq!(command.bytes, vec![1, 2, 3]);

        let error = validate_body(&descriptor, &[1, 2], "test").expect_err("length mismatch");
        assert!(error.to_string().contains("length mismatch"));
    }
}
