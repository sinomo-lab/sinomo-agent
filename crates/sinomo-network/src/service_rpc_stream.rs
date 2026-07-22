//! Service-catalog adapter for stream-native object RPC.
//!
//! The service registry continues to own typed [`ObjectRpcRequest`] and
//! [`ObjectRpcResponse`] values. This module moves that head across the opaque
//! application-head slot of one Stage-E call and preserves whether the single
//! streamed body is a typed value or a semantic attachment. Routing and the
//! stream protocol never inspect either service type.

use crate::object_rpc_stream::{
    ObjectRpcStreamBody, ObjectRpcStreamCall, ObjectRpcStreamIncomingBody,
    ObjectRpcStreamIncomingCall, ObjectRpcStreamReply, ObjectRpcStreamResponse, RpcDeliveryOutcome,
    RpcPeerContext, call_object_rpc_stream, serve_object_rpc_stream,
};
use crate::session::{
    ObjectRpcIncomingBody, ObjectRpcSessionBodyRole, ObjectRpcSessionCall, ObjectRpcSessionRequest,
    ObjectRpcSessionResponse, ObjectRpcSessionResult,
};
use crate::{
    NetworkDeliveryState, NetworkError, ObjectFrameIoOptions, ObjectRpcRequest, ObjectRpcResponse,
    SinomoObjectCodec,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::future::Future;
use tokio::io::{AsyncReadExt, AsyncWrite};

const SERVICE_RPC_STREAM_PROTOCOL: &str = "sinomo.service-rpc.stream.v1";

#[derive(Debug, Serialize, Deserialize)]
struct ServiceRequestHead {
    protocol: String,
    request: ObjectRpcRequest,
    body_role: ObjectRpcSessionBodyRole,
}

#[derive(Debug, Serialize, Deserialize)]
struct ServiceResponseHead {
    protocol: String,
    response: ObjectRpcResponse,
    body_role: ObjectRpcSessionBodyRole,
}

fn encode_service_head<T: Serialize>(head: &T) -> Result<Vec<u8>, NetworkError> {
    SinomoObjectCodec::network_frame().encode(head)
}

fn decode_service_request_head(bytes: &[u8]) -> Result<ServiceRequestHead, NetworkError> {
    let head: ServiceRequestHead = SinomoObjectCodec::network_frame().decode(bytes)?;
    if head.protocol != SERVICE_RPC_STREAM_PROTOCOL {
        return Err(NetworkError::Serialization {
            detail: format!("unsupported service RPC stream protocol {}", head.protocol),
        });
    }
    Ok(head)
}

fn decode_service_response_head(bytes: &[u8]) -> Result<ServiceResponseHead, NetworkError> {
    let head: ServiceResponseHead = SinomoObjectCodec::network_frame().decode(bytes)?;
    if head.protocol != SERVICE_RPC_STREAM_PROTOCOL {
        return Err(NetworkError::Serialization {
            detail: format!("unsupported service RPC stream protocol {}", head.protocol),
        });
    }
    Ok(head)
}

fn validate_service_body_role(
    role: ObjectRpcSessionBodyRole,
    total_bytes: Option<u64>,
) -> Result<(), NetworkError> {
    match (role, total_bytes) {
        (ObjectRpcSessionBodyRole::None, Some(0))
        | (ObjectRpcSessionBodyRole::None, None)
        | (ObjectRpcSessionBodyRole::Value | ObjectRpcSessionBodyRole::Attachment, None)
        | (ObjectRpcSessionBodyRole::Value | ObjectRpcSessionBodyRole::Attachment, Some(1..)) => {
            Ok(())
        }
        (ObjectRpcSessionBodyRole::None, Some(length)) => Err(NetworkError::Serialization {
            detail: format!("service RPC none body declared {length} bytes"),
        }),
        (ObjectRpcSessionBodyRole::Value | ObjectRpcSessionBodyRole::Attachment, Some(0)) => {
            Err(NetworkError::Serialization {
                detail: "service RPC non-empty body role declared an empty body".to_string(),
            })
        }
    }
}

fn bridge_stream_body(body: ObjectRpcStreamIncomingBody) -> ObjectRpcIncomingBody {
    let total_bytes = body.total_bytes();
    let (mut sender, incoming) = ObjectRpcIncomingBody::multiplexed_channel(total_bytes);
    tokio::spawn(async move {
        let mut body = body;
        let mut received_bytes = 0_u64;
        let mut buffer =
            vec![0_u8; crate::object_rpc_stream::OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES];
        let result = async {
            loop {
                let read = body
                    .read(&mut buffer)
                    .await
                    .map_err(|source| NetworkError::Io {
                        operation: "bridge stream-native service body".to_string(),
                        detail: source.to_string(),
                    })?;
                if read == 0 {
                    body.verifier().verify().await?;
                    return sender.finish(received_bytes).await;
                }
                received_bytes = received_bytes.checked_add(read as u64).ok_or_else(|| {
                    NetworkError::Serialization {
                        detail: "stream-native service body length overflow".to_string(),
                    }
                })?;
                if !sender.send(Bytes::copy_from_slice(&buffer[..read])).await? {
                    // The public consumer has gone away. Keep draining the
                    // Stage-E body so its authenticated finish and the peer's
                    // response lifecycle can still complete.
                }
            }
        }
        .await;
        if let Err(error) = result {
            sender.fail(error.to_string());
        }
    });
    incoming
}

async fn service_request_from_stream(
    incoming: ObjectRpcStreamIncomingCall,
) -> Result<ObjectRpcSessionRequest, NetworkError> {
    let ObjectRpcStreamIncomingCall {
        method,
        operation_id,
        application_head,
        body,
        ..
    } = incoming;
    let mut head = decode_service_request_head(&application_head)?;
    if head.request.route != method || head.request.operation_id() != operation_id.as_deref() {
        return Err(NetworkError::AuthDenied);
    }
    validate_service_body_role(head.body_role, body.total_bytes())?;
    match head.body_role {
        ObjectRpcSessionBodyRole::None => Ok(ObjectRpcSessionRequest::value(head.request)),
        ObjectRpcSessionBodyRole::Value => {
            if !head.request.body.is_empty() {
                return Err(NetworkError::Serialization {
                    detail: "service RPC value is present in both head and stream".to_string(),
                });
            }
            head.request.body = body.collect_to_vec(usize::MAX).await?;
            Ok(ObjectRpcSessionRequest::value(head.request))
        }
        ObjectRpcSessionBodyRole::Attachment => Ok(ObjectRpcSessionRequest::new(
            head.request,
            bridge_stream_body(body),
        )),
    }
}

fn service_reply_to_stream(
    request_id: sinomo_base::Id,
    response: ObjectRpcSessionResponse,
) -> Result<ObjectRpcStreamReply, NetworkError> {
    let (response, body_role, body) = response.into_stream_native_parts();
    let application_head = encode_service_head(&ServiceResponseHead {
        protocol: SERVICE_RPC_STREAM_PROTOCOL.to_string(),
        response,
        body_role,
    })?;
    Ok(ObjectRpcStreamReply {
        request_id,
        metadata: Default::default(),
        application_head,
        body: ObjectRpcStreamBody::source(body),
    })
}

async fn service_result_from_stream(
    expected_route: String,
    response: ObjectRpcStreamResponse,
) -> Result<ObjectRpcSessionResult, NetworkError> {
    let mut head = decode_service_response_head(&response.application_head)?;
    if head.response.route != expected_route {
        return Err(NetworkError::AuthDenied);
    }
    validate_service_body_role(head.body_role, response.body.total_bytes())?;
    match head.body_role {
        ObjectRpcSessionBodyRole::None => Ok(ObjectRpcSessionResult::value(head.response)),
        ObjectRpcSessionBodyRole::Value => {
            if !head.response.body.is_empty() {
                return Err(NetworkError::Serialization {
                    detail: "service RPC response value is present in both head and stream"
                        .to_string(),
                });
            }
            head.response.body = response.body.collect_to_vec(usize::MAX).await?;
            Ok(ObjectRpcSessionResult::value(head.response))
        }
        ObjectRpcSessionBodyRole::Attachment => Ok(ObjectRpcSessionResult::new(
            head.response,
            bridge_stream_body(response.body),
        )),
    }
}

#[doc(hidden)]
pub async fn call_service_rpc_stream<S>(
    stream: S,
    context: &RpcPeerContext,
    call: ObjectRpcSessionCall,
    request_auth: Vec<u8>,
    options: ObjectFrameIoOptions,
) -> Result<ObjectRpcSessionResult, NetworkError>
where
    S: tokio::io::AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let operation_id = call.request.operation_id().map(str::to_string);
    let route = call.request.route.clone();
    let (mut request, body_role, body) = call.into_stream_native_parts();
    request.auth = Some(request_auth);
    let application_head = encode_service_head(&ServiceRequestHead {
        protocol: SERVICE_RPC_STREAM_PROTOCOL.to_string(),
        request,
        body_role,
    })?;
    let stream_call = ObjectRpcStreamCall {
        request_id: sinomo_base::Id::generate(),
        method: route.clone(),
        operation_id: operation_id.clone(),
        metadata: Default::default(),
        application_head,
        body: ObjectRpcStreamBody::source(body),
    };
    match call_object_rpc_stream(stream, context, stream_call, options, move |response| {
        service_result_from_stream(route, response)
    })
    .await
    {
        RpcDeliveryOutcome::Completed(result) => Ok(result),
        RpcDeliveryOutcome::Rejected { detail } => {
            if detail == NetworkError::AuthDenied.to_string() {
                Err(NetworkError::AuthDenied)
            } else {
                Err(NetworkError::delivery_failed(
                    "dispatch stream-native service RPC",
                    NetworkDeliveryState::PossiblyApplied,
                    detail,
                ))
            }
        }
        RpcDeliveryOutcome::NotSent { error } => Err(if error.delivery_state().is_some() {
            error
        } else {
            NetworkError::delivery_failed(
                "send stream-native service RPC",
                NetworkDeliveryState::DefinitelyNotSent,
                error.to_string(),
            )
        }),
        RpcDeliveryOutcome::PossiblyApplied { error } => Err(NetworkError::outcome_unknown(
            "stream-native service RPC",
            operation_id.as_deref(),
            error.to_string(),
        )),
    }
}

#[doc(hidden)]
pub async fn serve_service_rpc_stream<S, F, Fut>(
    stream: S,
    context: &RpcPeerContext,
    options: ObjectFrameIoOptions,
    handler: F,
) -> Result<(), NetworkError>
where
    S: tokio::io::AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: FnOnce(ObjectRpcSessionRequest, RpcPeerContext) -> Fut,
    Fut: Future<Output = Result<ObjectRpcSessionResponse, NetworkError>>,
{
    serve_object_rpc_stream(stream, context, options, move |incoming, peer| async move {
        let request_id = incoming.request_id.clone();
        let request = service_request_from_stream(incoming).await?;
        let response = handler(request, peer).await?;
        service_reply_to_stream(request_id, response)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_rpc_stream::{
        OBJECT_RPC_STREAM_INLINE_BODY_BYTES, OBJECT_RPC_STREAM_RECEIVE_WINDOW_BYTES,
    };

    fn local_context() -> RpcPeerContext {
        RpcPeerContext::unauthenticated_local(
            sinomo_base::Id::generate(),
            *blake3::hash(b"service-stream-adapter").as_bytes(),
        )
    }

    fn authenticated_contexts() -> (RpcPeerContext, RpcPeerContext) {
        let source =
            sinomo_node::NodeId::from_legacy_named("service_stream_source").expect("source node");
        let destination = sinomo_node::NodeId::from_legacy_named("service_stream_destination")
            .expect("destination node");
        let session_id = sinomo_base::Id::generate();
        let binding = *blake3::hash(session_id.as_str().as_bytes()).as_bytes();
        (
            RpcPeerContext::authenticated(
                source.clone(),
                destination.clone(),
                session_id.clone(),
                binding,
            )
            .expect("client context"),
            RpcPeerContext::authenticated(destination, source, session_id, binding)
                .expect("server context"),
        )
    }

    async fn round_trip(
        request_bytes: usize,
        response_bytes: usize,
    ) -> Result<ObjectRpcSessionResult, NetworkError> {
        let (client, server) = tokio::io::duplex(2 * OBJECT_RPC_STREAM_RECEIVE_WINDOW_BYTES);
        let client_context = local_context();
        let server_context = client_context.clone();
        let server_task = tokio::spawn(async move {
            serve_service_rpc_stream(
                server,
                &server_context,
                ObjectFrameIoOptions::default(),
                move |request, _| async move {
                    assert_eq!(request.request.route, "/test/service-stream");
                    assert_eq!(request.request.body, vec![7; request_bytes]);
                    ObjectRpcResponse::from_encoded_body(
                        "/test/service-stream",
                        vec![9; response_bytes],
                    )
                    .pipe(ObjectRpcSessionResponse::value)
                    .pipe(Ok)
                },
            )
            .await
        });
        let request = ObjectRpcRequest::from_encoded_body(
            "/test/service-stream",
            "test.service-stream",
            vec![7; request_bytes],
        );
        let result = call_service_rpc_stream(
            client,
            &client_context,
            ObjectRpcSessionCall::value(request),
            b"test-auth".to_vec(),
            ObjectFrameIoOptions::default(),
        )
        .await?;
        server_task.await.expect("server task")?;
        Ok(result)
    }

    trait Pipe: Sized {
        fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
            f(self)
        }
    }
    impl<T> Pipe for T {}

    #[tokio::test]
    async fn service_values_cross_inline_frame_and_window_boundaries() {
        for bytes in [
            0,
            1,
            OBJECT_RPC_STREAM_INLINE_BODY_BYTES - 1,
            OBJECT_RPC_STREAM_INLINE_BODY_BYTES,
            OBJECT_RPC_STREAM_INLINE_BODY_BYTES + 1,
            OBJECT_RPC_STREAM_RECEIVE_WINDOW_BYTES + 1,
        ] {
            let result = round_trip(bytes, bytes + 1).await.expect("round trip");
            assert_eq!(result.response.body, vec![9; bytes + 1]);
        }
    }

    async fn attachment_round_trip(relayed: bool, bytes: usize) -> Result<Vec<u8>, NetworkError> {
        let (client, server, relay_task): (
            Box<dyn crate::NetworkDuplexStream>,
            Box<dyn crate::NetworkDuplexStream>,
            Option<tokio::task::JoinHandle<()>>,
        ) = if relayed {
            let (client, mut relay_incoming) =
                tokio::io::duplex(2 * OBJECT_RPC_STREAM_RECEIVE_WINDOW_BYTES);
            let (mut relay_outgoing, server) =
                tokio::io::duplex(2 * OBJECT_RPC_STREAM_RECEIVE_WINDOW_BYTES);
            let relay_task = tokio::spawn(async move {
                tokio::io::copy_bidirectional(&mut relay_incoming, &mut relay_outgoing)
                    .await
                    .expect("opaque relay copy");
            });
            (Box::new(client), Box::new(server), Some(relay_task))
        } else {
            let (client, server) = tokio::io::duplex(2 * OBJECT_RPC_STREAM_RECEIVE_WINDOW_BYTES);
            (Box::new(client), Box::new(server), None)
        };
        let (client_context, server_context) = authenticated_contexts();
        let expected_source = match &server_context.identity {
            crate::object_rpc_stream::RpcPeerIdentity::Authenticated { peer_node_id, .. } => {
                peer_node_id.clone()
            }
            crate::object_rpc_stream::RpcPeerIdentity::UnauthenticatedLocal => unreachable!(),
        };
        let server_task = tokio::spawn(async move {
            serve_service_rpc_stream(
                server,
                &server_context,
                ObjectFrameIoOptions::default(),
                move |mut request, peer| async move {
                    assert_eq!(
                        request.request.auth.as_deref(),
                        Some(b"origin-auth".as_slice())
                    );
                    assert_eq!(request.request.body, b"typed-head");
                    assert!(matches!(
                        peer.identity,
                        crate::object_rpc_stream::RpcPeerIdentity::Authenticated {
                            peer_node_id,
                            ..
                        } if peer_node_id == expected_source
                    ));
                    let mut attachment = Vec::new();
                    request
                        .body
                        .read_to_end(&mut attachment)
                        .await
                        .expect("read request attachment");
                    assert_eq!(attachment, vec![3; bytes]);
                    Ok(ObjectRpcSessionResponse::new(
                        ObjectRpcResponse::from_encoded_body(
                            "/test/service-attachment",
                            b"typed-response-head".to_vec(),
                        ),
                        crate::ObjectRpcBodySource::bytes(vec![5; bytes + 1]),
                    ))
                },
            )
            .await
        });
        let result = call_service_rpc_stream(
            client,
            &client_context,
            ObjectRpcSessionCall::new(
                ObjectRpcRequest::from_encoded_body(
                    "/test/service-attachment",
                    "test.service-attachment",
                    b"typed-head".to_vec(),
                ),
                crate::ObjectRpcBodySource::bytes(vec![3; bytes]),
            ),
            b"origin-auth".to_vec(),
            ObjectFrameIoOptions::default(),
        )
        .await?;
        assert_eq!(result.response.body, b"typed-response-head");
        let response = result.body.collect_to_vec(bytes + 1).await?;
        server_task.await.expect("server task")?;
        if let Some(relay_task) = relay_task {
            relay_task.abort();
        }
        Ok(response)
    }

    #[tokio::test]
    async fn service_attachments_match_direct_and_opaque_relay_paths() {
        for bytes in [
            1,
            OBJECT_RPC_STREAM_INLINE_BODY_BYTES + 1,
            crate::object_rpc_stream::OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES + 1,
            OBJECT_RPC_STREAM_RECEIVE_WINDOW_BYTES + 1,
            OBJECT_RPC_STREAM_RECEIVE_WINDOW_BYTES * 12 + 211,
        ] {
            let direct = attachment_round_trip(false, bytes)
                .await
                .expect("direct attachment");
            let relayed = attachment_round_trip(true, bytes)
                .await
                .expect("relayed attachment");
            assert_eq!(direct, vec![5; bytes + 1]);
            assert_eq!(relayed, direct);
        }
    }

    #[tokio::test]
    async fn stream_native_service_proxy_preserves_declared_attachment_lengths() {
        let request_bytes =
            crate::object_rpc_stream::OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES * 3 + 17;
        let response_bytes =
            crate::object_rpc_stream::OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES * 2 + 23;
        let (client, proxy_incoming) =
            tokio::io::duplex(2 * OBJECT_RPC_STREAM_RECEIVE_WINDOW_BYTES);
        let (proxy_outgoing, server) =
            tokio::io::duplex(2 * OBJECT_RPC_STREAM_RECEIVE_WINDOW_BYTES);
        let upstream_client_context = local_context();
        let upstream_proxy_context = upstream_client_context.clone();
        let downstream_proxy_context = local_context();
        let downstream_server_context = downstream_proxy_context.clone();

        let server_task = tokio::spawn(async move {
            serve_service_rpc_stream(
                server,
                &downstream_server_context,
                ObjectFrameIoOptions::default(),
                move |mut request, _| async move {
                    assert_eq!(request.request.route, "/test/service-proxy-attachment");
                    assert_eq!(request.body.total_bytes(), Some(request_bytes as u64));
                    let mut received = Vec::new();
                    request
                        .body
                        .read_to_end(&mut received)
                        .await
                        .expect("read proxied request attachment");
                    assert_eq!(received, vec![0; request_bytes]);
                    Ok(ObjectRpcSessionResponse::new(
                        ObjectRpcResponse::from_encoded_body(
                            "/test/service-proxy-attachment",
                            b"proxied-response-head".to_vec(),
                        ),
                        crate::ObjectRpcBodySource::zeroes(response_bytes as u64),
                    ))
                },
            )
            .await
        });

        let proxy_task = tokio::spawn(async move {
            serve_service_rpc_stream(
                proxy_incoming,
                &upstream_proxy_context,
                ObjectFrameIoOptions::default(),
                move |request, _| async move {
                    assert_eq!(request.body.total_bytes(), Some(request_bytes as u64));
                    let call = ObjectRpcSessionCall::from_relay_body(request.request, request.body);
                    let result = call_service_rpc_stream(
                        proxy_outgoing,
                        &downstream_proxy_context,
                        call,
                        b"proxy-auth".to_vec(),
                        ObjectFrameIoOptions::default(),
                    )
                    .await?;
                    assert_eq!(result.body.total_bytes(), Some(response_bytes as u64));
                    result.into_relay_response()
                },
            )
            .await
        });

        let result = call_service_rpc_stream(
            client,
            &upstream_client_context,
            ObjectRpcSessionCall::new(
                ObjectRpcRequest::from_encoded_body(
                    "/test/service-proxy-attachment",
                    "test.service-proxy-attachment",
                    b"proxied-request-head".to_vec(),
                ),
                crate::ObjectRpcBodySource::zeroes(request_bytes as u64),
            ),
            b"origin-auth".to_vec(),
            ObjectFrameIoOptions::default(),
        )
        .await
        .expect("stream-native proxied attachment call");
        assert_eq!(result.response.body, b"proxied-response-head");
        assert_eq!(result.body.total_bytes(), Some(response_bytes as u64));
        assert_eq!(
            result
                .body
                .collect_to_vec(response_bytes)
                .await
                .expect("read proxied response attachment"),
            vec![0; response_bytes]
        );
        proxy_task.await.expect("proxy task").expect("proxy stream");
        server_task
            .await
            .expect("server task")
            .expect("server stream");
    }
}
