//! One-call-per-stream object-RPC contract and in-memory conformance engine.
//!
//! This module depends only on an authenticated peer context and a duplex byte
//! stream. It does not select nodes, endpoints, paths, or relays and it owns no
//! service registry. The functions here are the replacement contract for
//! `start_object_rpc_client_session` and `accept_object_rpc_server_session`;
//! production callers remain on those carriers until a bounded migration can
//! delete the old path in the same change.

use crate::error::NetworkError;
use crate::object::{
    OBJECT_FRAME_WIRE_PREFIX_BYTES, ObjectFrameIoOptions, SinomoObjectCodec,
    encode_object_frame_to_vec, read_object_frame_async_with_max_bytes,
};
use serde::{Deserialize, Serialize};
use sinomo_base::Id;
use sinomo_node::NodeId;
use std::collections::BTreeMap;
use std::future::Future;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

pub const OBJECT_RPC_STREAM_MAX_HEAD_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RpcPeerIdentity {
    Authenticated {
        local_node_id: NodeId,
        peer_node_id: NodeId,
    },
    UnauthenticatedLocal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RpcPeerContext {
    pub identity: RpcPeerIdentity,
    pub session_id: Id,
    pub channel_binding: [u8; 32],
}

impl RpcPeerContext {
    pub fn authenticated(
        local_node_id: NodeId,
        peer_node_id: NodeId,
        session_id: Id,
        channel_binding: [u8; 32],
    ) -> Result<Self, NetworkError> {
        if local_node_id == peer_node_id {
            return Err(NetworkError::Serialization {
                detail: "authenticated RPC peers must have distinct node identities".to_string(),
            });
        }
        Ok(Self {
            identity: RpcPeerIdentity::Authenticated {
                local_node_id,
                peer_node_id,
            },
            session_id,
            channel_binding,
        })
    }

    pub fn unauthenticated_local(session_id: Id, channel_binding: [u8; 32]) -> Self {
        Self {
            identity: RpcPeerIdentity::UnauthenticatedLocal,
            session_id,
            channel_binding,
        }
    }

    fn wire_endpoints(&self) -> (Option<NodeId>, Option<NodeId>) {
        match &self.identity {
            RpcPeerIdentity::Authenticated {
                local_node_id,
                peer_node_id,
            } => (Some(local_node_id.clone()), Some(peer_node_id.clone())),
            RpcPeerIdentity::UnauthenticatedLocal => (None, None),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectRpcStreamCall {
    pub request_id: Id,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    #[serde(with = "serde_bytes")]
    pub body: Vec<u8>,
}

impl ObjectRpcStreamCall {
    pub fn encode_body<T: Serialize + ?Sized>(
        method: impl Into<String>,
        operation_id: Option<String>,
        body: &T,
    ) -> Result<Self, NetworkError> {
        Ok(Self {
            request_id: Id::generate(),
            method: method.into(),
            operation_id,
            metadata: BTreeMap::new(),
            body: SinomoObjectCodec::network_frame().encode(body)?,
        })
    }

    pub fn decode_body<T: serde::de::DeserializeOwned>(&self) -> Result<T, NetworkError> {
        SinomoObjectCodec::network_frame().decode(&self.body)
    }

    fn validate(&self) -> Result<(), NetworkError> {
        if self.method.trim().is_empty() {
            return Err(NetworkError::Serialization {
                detail: "object RPC service method cannot be empty".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectRpcStreamReply {
    pub request_id: Id,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    #[serde(with = "serde_bytes")]
    pub body: Vec<u8>,
}

impl ObjectRpcStreamReply {
    pub fn encode_body<T: Serialize + ?Sized>(
        request_id: Id,
        body: &T,
    ) -> Result<Self, NetworkError> {
        Ok(Self {
            request_id,
            metadata: BTreeMap::new(),
            body: SinomoObjectCodec::network_frame().encode(body)?,
        })
    }

    pub fn decode_body<T: serde::de::DeserializeOwned>(&self) -> Result<T, NetworkError> {
        SinomoObjectCodec::network_frame().decode(&self.body)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RpcDeliveryOutcome {
    NotSent { error: NetworkError },
    Rejected { detail: String },
    PossiblyApplied { error: NetworkError },
    Completed(ObjectRpcStreamReply),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ObjectRpcStreamRequestEnvelope {
    origin_node_id: Option<NodeId>,
    destination_node_id: Option<NodeId>,
    session_id: Id,
    channel_binding: [u8; 32],
    call: ObjectRpcStreamCall,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum ObjectRpcStreamResponseStatus {
    Completed(ObjectRpcStreamReply),
    Rejected { detail: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ObjectRpcStreamResponseEnvelope {
    origin_node_id: Option<NodeId>,
    destination_node_id: Option<NodeId>,
    session_id: Id,
    channel_binding: [u8; 32],
    request_id: Id,
    status: ObjectRpcStreamResponseStatus,
}

fn delivery_error(operation: &str, detail: impl Into<String>) -> NetworkError {
    NetworkError::Io {
        operation: operation.to_string(),
        detail: detail.into(),
    }
}

async fn write_encoded_frame<W>(writer: &mut W, frame: &[u8]) -> Result<(), (NetworkError, bool)>
where
    W: AsyncWrite + Unpin,
{
    let mut written = 0_usize;
    while written < frame.len() {
        match writer.write(&frame[written..]).await {
            Ok(0) => {
                return Err((
                    delivery_error(
                        "write object RPC stream frame",
                        format!("peer stopped accepting bytes after {written} bytes"),
                    ),
                    written != 0,
                ));
            }
            Ok(bytes) => written = written.saturating_add(bytes),
            Err(error) => {
                return Err((
                    delivery_error("write object RPC stream frame", error.to_string()),
                    written != 0,
                ));
            }
        }
    }
    writer.flush().await.map_err(|error| {
        (
            delivery_error("flush object RPC stream frame", error.to_string()),
            !frame.is_empty(),
        )
    })
}

fn validate_request_context(
    context: &RpcPeerContext,
    envelope: &ObjectRpcStreamRequestEnvelope,
) -> Result<(), NetworkError> {
    let (expected_origin, expected_destination) = match &context.identity {
        RpcPeerIdentity::Authenticated {
            local_node_id,
            peer_node_id,
        } => (Some(peer_node_id), Some(local_node_id)),
        RpcPeerIdentity::UnauthenticatedLocal => (None, None),
    };
    if envelope.origin_node_id.as_ref() != expected_origin
        || envelope.destination_node_id.as_ref() != expected_destination
        || envelope.session_id != context.session_id
        || envelope.channel_binding != context.channel_binding
    {
        return Err(NetworkError::AuthDenied);
    }
    envelope.call.validate()
}

fn validate_response_context(
    context: &RpcPeerContext,
    request_id: &Id,
    envelope: &ObjectRpcStreamResponseEnvelope,
) -> Result<(), NetworkError> {
    let (expected_origin, expected_destination) = match &context.identity {
        RpcPeerIdentity::Authenticated {
            local_node_id,
            peer_node_id,
        } => (Some(peer_node_id), Some(local_node_id)),
        RpcPeerIdentity::UnauthenticatedLocal => (None, None),
    };
    if envelope.origin_node_id.as_ref() != expected_origin
        || envelope.destination_node_id.as_ref() != expected_destination
        || envelope.session_id != context.session_id
        || envelope.channel_binding != context.channel_binding
        || &envelope.request_id != request_id
        || matches!(
            &envelope.status,
            ObjectRpcStreamResponseStatus::Completed(reply) if &reply.request_id != request_id
        )
    {
        return Err(NetworkError::AuthDenied);
    }
    Ok(())
}

/// Executes one object-RPC call on one authenticated duplex stream.
pub async fn call_object_rpc_stream<S>(
    stream: &mut S,
    context: &RpcPeerContext,
    call: ObjectRpcStreamCall,
    options: ObjectFrameIoOptions,
) -> RpcDeliveryOutcome
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if let Err(error) = call.validate() {
        return RpcDeliveryOutcome::NotSent { error };
    }
    let request_id = call.request_id.clone();
    let (origin_node_id, destination_node_id) = context.wire_endpoints();
    let envelope = ObjectRpcStreamRequestEnvelope {
        origin_node_id,
        destination_node_id,
        session_id: context.session_id.clone(),
        channel_binding: context.channel_binding,
        call,
    };
    let frame = match encode_object_frame_to_vec(&envelope) {
        Ok(frame)
            if frame.len().saturating_sub(OBJECT_FRAME_WIRE_PREFIX_BYTES)
                <= OBJECT_RPC_STREAM_MAX_HEAD_BYTES =>
        {
            frame
        }
        Ok(frame) => {
            return RpcDeliveryOutcome::NotSent {
                error: NetworkError::FrameTooLarge {
                    actual_bytes: frame.len().saturating_sub(OBJECT_FRAME_WIRE_PREFIX_BYTES),
                    max_bytes: OBJECT_RPC_STREAM_MAX_HEAD_BYTES,
                },
            };
        }
        Err(error) => return RpcDeliveryOutcome::NotSent { error },
    };
    if let Err((error, wrote_any)) = write_encoded_frame(stream, &frame).await {
        return if wrote_any {
            RpcDeliveryOutcome::PossiblyApplied { error }
        } else {
            RpcDeliveryOutcome::NotSent { error }
        };
    }
    if let Err(error) = stream.shutdown().await {
        return RpcDeliveryOutcome::PossiblyApplied {
            error: delivery_error("finish object RPC request direction", error.to_string()),
        };
    }
    let response: ObjectRpcStreamResponseEnvelope = match read_object_frame_async_with_max_bytes(
        stream,
        options,
        OBJECT_RPC_STREAM_MAX_HEAD_BYTES,
    )
    .await
    {
        Ok(response) => response,
        Err(error) => return RpcDeliveryOutcome::PossiblyApplied { error },
    };
    if let Err(error) = validate_response_context(context, &request_id, &response) {
        return RpcDeliveryOutcome::PossiblyApplied { error };
    }
    match response.status {
        ObjectRpcStreamResponseStatus::Completed(reply) => RpcDeliveryOutcome::Completed(reply),
        ObjectRpcStreamResponseStatus::Rejected { detail } => {
            RpcDeliveryOutcome::Rejected { detail }
        }
    }
}

/// Serves exactly one object-RPC call from one authenticated duplex stream.
pub async fn serve_object_rpc_stream<S, F, Fut>(
    stream: &mut S,
    context: &RpcPeerContext,
    options: ObjectFrameIoOptions,
    handler: F,
) -> Result<(), NetworkError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: FnOnce(ObjectRpcStreamCall, RpcPeerContext) -> Fut,
    Fut: Future<Output = Result<ObjectRpcStreamReply, NetworkError>>,
{
    let request: ObjectRpcStreamRequestEnvelope =
        read_object_frame_async_with_max_bytes(stream, options, OBJECT_RPC_STREAM_MAX_HEAD_BYTES)
            .await?;
    validate_request_context(context, &request)?;
    let request_id = request.call.request_id.clone();
    let status = match handler(request.call, context.clone()).await {
        Ok(reply) if reply.request_id == request_id => {
            ObjectRpcStreamResponseStatus::Completed(reply)
        }
        Ok(_) => ObjectRpcStreamResponseStatus::Rejected {
            detail: "object RPC handler response used the wrong request id".to_string(),
        },
        Err(error) => ObjectRpcStreamResponseStatus::Rejected {
            detail: error.to_string(),
        },
    };
    let (origin_node_id, destination_node_id) = context.wire_endpoints();
    let response = ObjectRpcStreamResponseEnvelope {
        origin_node_id,
        destination_node_id,
        session_id: context.session_id.clone(),
        channel_binding: context.channel_binding,
        request_id,
        status,
    };
    let frame = encode_object_frame_to_vec(&response)?;
    if frame.len().saturating_sub(OBJECT_FRAME_WIRE_PREFIX_BYTES) > OBJECT_RPC_STREAM_MAX_HEAD_BYTES
    {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: frame.len().saturating_sub(OBJECT_FRAME_WIRE_PREFIX_BYTES),
            max_bytes: OBJECT_RPC_STREAM_MAX_HEAD_BYTES,
        });
    }
    write_encoded_frame(stream, &frame)
        .await
        .map_err(|(error, _)| error)?;
    stream
        .shutdown()
        .await
        .map_err(|error| delivery_error("finish object RPC response direction", error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).expect("node id")
    }

    fn peer_contexts() -> (RpcPeerContext, RpcPeerContext) {
        let client = node("node_rpc_stream_client");
        let server = node("node_rpc_stream_server");
        let session_id = Id::from_legacy_named("session_rpc_stream").expect("session id");
        let binding = *blake3::hash(b"rpc-stream-test-binding").as_bytes();
        (
            RpcPeerContext::authenticated(
                client.clone(),
                server.clone(),
                session_id.clone(),
                binding,
            )
            .expect("client context"),
            RpcPeerContext::authenticated(server, client, session_id, binding)
                .expect("server context"),
        )
    }

    #[tokio::test]
    async fn fake_duplex_runs_complete_one_call_state_machine() {
        let (mut client, mut server) = tokio::io::duplex(64 * 1024);
        let (client_context, server_context) = peer_contexts();
        let call = ObjectRpcStreamCall::encode_body("test.echo", None, &"hello").expect("request");
        let server_task = tokio::spawn(async move {
            serve_object_rpc_stream(
                &mut server,
                &server_context,
                ObjectFrameIoOptions { idle_timeout: None },
                |call, context| async move {
                    assert_eq!(
                        context.identity,
                        RpcPeerIdentity::Authenticated {
                            local_node_id: node("node_rpc_stream_server"),
                            peer_node_id: node("node_rpc_stream_client"),
                        }
                    );
                    let value: String = call.decode_body()?;
                    ObjectRpcStreamReply::encode_body(call.request_id, &format!("{value} world"))
                },
            )
            .await
        });

        let outcome = call_object_rpc_stream(
            &mut client,
            &client_context,
            call,
            ObjectFrameIoOptions { idle_timeout: None },
        )
        .await;
        let RpcDeliveryOutcome::Completed(reply) = outcome else {
            panic!("expected completed RPC, got {outcome:?}");
        };
        assert_eq!(
            reply.decode_body::<String>().expect("response body"),
            "hello world"
        );
        server_task.await.expect("server task").expect("serve RPC");
    }

    #[tokio::test]
    async fn verified_handler_rejection_is_distinct_from_ambiguous_delivery() {
        let (mut client, mut server) = tokio::io::duplex(64 * 1024);
        let (client_context, server_context) = peer_contexts();
        let call = ObjectRpcStreamCall::encode_body("test.reject", None, &()).expect("request");
        let server_task = tokio::spawn(async move {
            serve_object_rpc_stream(
                &mut server,
                &server_context,
                ObjectFrameIoOptions { idle_timeout: None },
                |_call, _context| async { Err(NetworkError::AuthDenied) },
            )
            .await
        });

        let outcome = call_object_rpc_stream(
            &mut client,
            &client_context,
            call,
            ObjectFrameIoOptions { idle_timeout: None },
        )
        .await;
        assert_eq!(
            outcome,
            RpcDeliveryOutcome::Rejected {
                detail: "network authorization was denied".to_string()
            }
        );
        server_task.await.expect("server task").expect("serve RPC");
    }

    #[tokio::test]
    async fn invalid_call_is_not_sent() {
        let (mut client, _server) = tokio::io::duplex(64);
        let (client_context, _) = peer_contexts();
        let call = ObjectRpcStreamCall {
            request_id: Id::generate(),
            method: " ".to_string(),
            operation_id: None,
            metadata: BTreeMap::new(),
            body: Vec::new(),
        };
        assert!(matches!(
            call_object_rpc_stream(
                &mut client,
                &client_context,
                call,
                ObjectFrameIoOptions { idle_timeout: None },
            )
            .await,
            RpcDeliveryOutcome::NotSent { .. }
        ));
    }
}
