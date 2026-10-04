//! Typed service-head adapter for stream-native object RPC.
//!
//! The service registry continues to own typed [`ObjectRpcRequest`] and
//! [`ObjectRpcResponse`] values. This module moves that head across the opaque
//! application-head slot of one call and preserves whether the single
//! streamed body is a typed value or a semantic attachment. Routing and the
//! stream protocol never inspect either service type.

#[cfg(test)]
use crate::object_rpc_stream::call_object_rpc_stream_with_consumption;
use crate::object_rpc_stream::{
    ObjectRpcStreamBody, ObjectRpcStreamCall, ObjectRpcStreamIncomingBody,
    ObjectRpcStreamIncomingCall, ObjectRpcStreamReply, ObjectRpcStreamResponse, RpcDeliveryOutcome,
    RpcPeerContext, RpcResponseConsumption, call_object_rpc_chunk_stream_with_consumption,
    serve_object_rpc_chunk_stream, serve_object_rpc_stream,
};
use crate::rpc_body::{
    ObjectRpcBodyRole, ObjectRpcCall, ObjectRpcIncomingBody, ObjectRpcIncomingRequest,
    ObjectRpcOutgoingResponse, ObjectRpcResult,
};
use crate::{
    NetworkDeliveryState, NetworkError, ObjectFrameIoOptions, ObjectRpcRequest, ObjectRpcResponse,
    PeerStream, SinomoObjectCodec,
};
use serde::{Deserialize, Serialize};
use std::future::Future;
#[cfg(test)]
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tracing::Instrument;

const SERVICE_RPC_HEAD_SCHEMA: &str = "sinomo.service-rpc.head.v1";

#[derive(Debug, Serialize, Deserialize)]
struct ServiceRequestHead {
    schema: String,
    request: ObjectRpcRequest,
    body_role: ObjectRpcBodyRole,
}

#[derive(Debug, Serialize, Deserialize)]
struct ServiceResponseHead {
    schema: String,
    response: ObjectRpcResponse,
    body_role: ObjectRpcBodyRole,
}

fn encode_service_head<T: Serialize>(head: &T) -> Result<Vec<u8>, NetworkError> {
    SinomoObjectCodec::network_frame().encode(head)
}

#[inline]
fn service_timeline_started_ns(trace: Option<crate::NetworkTimelineTraceContext>) -> u64 {
    trace
        .map(|_| crate::network_timeline_monotonic_ns())
        .unwrap_or(0)
}

#[inline]
fn record_service_timeline_duration_ns(
    trace: Option<crate::NetworkTimelineTraceContext>,
    stage: u16,
    flags: u16,
    duration_ns: u64,
    success: bool,
) {
    let Some(trace) = trace else {
        return;
    };
    crate::record_network_timeline_event_now(crate::NetworkTimelineEvent::new(
        trace,
        stage,
        flags
            | if success {
                crate::network_timeline_flag::SUCCESS
            } else {
                0
            },
        0,
        0,
        0,
        0,
        0,
        duration_ns,
    ));
}

#[inline]
fn record_service_timeline_since(
    trace: Option<crate::NetworkTimelineTraceContext>,
    stage: u16,
    flags: u16,
    started_ns: u64,
    success: bool,
) {
    if trace.is_none() {
        return;
    }
    record_service_timeline_duration_ns(
        trace,
        stage,
        flags,
        crate::network_timeline_monotonic_ns().saturating_sub(started_ns),
        success,
    );
}

fn decode_service_request_head(bytes: &[u8]) -> Result<ServiceRequestHead, NetworkError> {
    let head: ServiceRequestHead = SinomoObjectCodec::network_frame().decode(bytes)?;
    if head.schema != SERVICE_RPC_HEAD_SCHEMA {
        return Err(NetworkError::Serialization {
            detail: format!("unsupported service RPC head schema {}", head.schema),
        });
    }
    Ok(head)
}

fn decode_service_response_head(bytes: &[u8]) -> Result<ServiceResponseHead, NetworkError> {
    let head: ServiceResponseHead = SinomoObjectCodec::network_frame().decode(bytes)?;
    if head.schema != SERVICE_RPC_HEAD_SCHEMA {
        return Err(NetworkError::Serialization {
            detail: format!("unsupported service RPC head schema {}", head.schema),
        });
    }
    Ok(head)
}

fn validate_service_body_role(
    role: ObjectRpcBodyRole,
    total_bytes: Option<u64>,
) -> Result<(), NetworkError> {
    match (role, total_bytes) {
        (ObjectRpcBodyRole::None, Some(0))
        | (ObjectRpcBodyRole::None, None)
        | (ObjectRpcBodyRole::Value | ObjectRpcBodyRole::Attachment, None)
        | (ObjectRpcBodyRole::Value | ObjectRpcBodyRole::Attachment, Some(1..)) => Ok(()),
        (ObjectRpcBodyRole::None, Some(length)) => Err(NetworkError::Serialization {
            detail: format!("service RPC none body declared {length} bytes"),
        }),
        (ObjectRpcBodyRole::Value | ObjectRpcBodyRole::Attachment, Some(0)) => {
            Err(NetworkError::Serialization {
                detail: "service RPC non-empty body role declared an empty body".to_string(),
            })
        }
    }
}

fn bridge_stream_body(body: ObjectRpcStreamIncomingBody) -> ObjectRpcIncomingBody {
    // The protocol pump already owns the application body's bounded channel.
    // Move it across this service adapter instead of spawning a second pump
    // and copying every transport chunk through an intermediate buffer.
    body.into_application_body()
}

async fn service_request_from_stream(
    incoming: ObjectRpcStreamIncomingCall,
    preserve_streamed_value: bool,
) -> Result<ObjectRpcIncomingRequest, NetworkError> {
    let ObjectRpcStreamIncomingCall {
        request_id: _,
        method,
        operation_id,
        metadata,
        application_head,
        body,
        request_ingress_duration_ns,
    } = incoming;
    if !metadata.is_empty() {
        return Err(NetworkError::Serialization {
            detail: "service RPC outer request metadata must be empty".to_string(),
        });
    }
    let decode_started_ns = if crate::network_timeline_capture_is_active() {
        crate::network_timeline_monotonic_ns()
    } else {
        0
    };
    let mut head = decode_service_request_head(&application_head)?;
    let timeline_trace = crate::network_timeline_trace_context_for_request(&head.request);
    record_service_timeline_duration_ns(
        timeline_trace,
        crate::network_timeline_stage::RPC_REQUEST_INGRESS,
        crate::network_timeline_flag::REQUEST,
        request_ingress_duration_ns,
        true,
    );
    record_service_timeline_since(
        timeline_trace,
        crate::network_timeline_stage::SERVICE_REQUEST_DECODE,
        crate::network_timeline_flag::REQUEST,
        decode_started_ns,
        true,
    );
    if head.request.route != method || head.request.operation_id() != operation_id.as_deref() {
        return Err(NetworkError::AuthDenied);
    }
    validate_service_body_role(head.body_role, body.total_bytes())?;
    match head.body_role {
        ObjectRpcBodyRole::None => Ok(ObjectRpcIncomingRequest::value(head.request)),
        ObjectRpcBodyRole::Value => {
            if !head.request.body.is_empty() {
                return Err(NetworkError::Serialization {
                    detail: "service RPC value is present in both head and stream".to_string(),
                });
            }
            if preserve_streamed_value {
                Ok(ObjectRpcIncomingRequest::streamed_value(
                    head.request,
                    bridge_stream_body(body),
                ))
            } else {
                head.request.body = body.materialize_to_vec().await?;
                Ok(ObjectRpcIncomingRequest::value(head.request))
            }
        }
        ObjectRpcBodyRole::Attachment => Ok(ObjectRpcIncomingRequest::new(
            head.request,
            bridge_stream_body(body),
        )),
    }
}

fn service_reply_to_stream(
    request_id: sinomo_base::Id,
    response: ObjectRpcOutgoingResponse,
    timeline_trace: Option<crate::NetworkTimelineTraceContext>,
) -> Result<ObjectRpcStreamReply, NetworkError> {
    let encode_started_ns = service_timeline_started_ns(timeline_trace);
    let (response, body_role, body) = response.into_stream_native_parts();
    let application_head_result = encode_service_head(&ServiceResponseHead {
        schema: SERVICE_RPC_HEAD_SCHEMA.to_string(),
        response,
        body_role,
    });
    record_service_timeline_since(
        timeline_trace,
        crate::network_timeline_stage::SERVICE_RESPONSE_ENCODE,
        crate::network_timeline_flag::RESPONSE,
        encode_started_ns,
        application_head_result.is_ok(),
    );
    let application_head = application_head_result?;
    Ok(ObjectRpcStreamReply {
        request_id,
        metadata: Default::default(),
        application_head,
        body: ObjectRpcStreamBody::source(body),
        timeline_trace,
    })
}

async fn service_result_from_stream(
    expected_route: String,
    response: ObjectRpcStreamResponse,
    preserve_streamed_value: bool,
    timeline_trace: Option<crate::NetworkTimelineTraceContext>,
) -> Result<RpcResponseConsumption<ObjectRpcResult>, NetworkError> {
    let ObjectRpcStreamResponse {
        metadata,
        application_head,
        body,
    } = response;
    if !metadata.is_empty() {
        return Err(NetworkError::Serialization {
            detail: "service RPC outer response metadata must be empty".to_string(),
        });
    }
    let decode_started_ns = service_timeline_started_ns(timeline_trace);
    let head_result = decode_service_response_head(&application_head);
    record_service_timeline_since(
        timeline_trace,
        crate::network_timeline_stage::SERVICE_RESPONSE_DECODE,
        crate::network_timeline_flag::RESPONSE,
        decode_started_ns,
        head_result.is_ok(),
    );
    let mut head = head_result?;
    if head.response.route != expected_route {
        return Err(NetworkError::AuthDenied);
    }
    validate_service_body_role(head.body_role, body.total_bytes())?;
    match head.body_role {
        ObjectRpcBodyRole::None => Ok(RpcResponseConsumption::Complete(ObjectRpcResult::value(
            head.response,
        ))),
        ObjectRpcBodyRole::Value => {
            if !head.response.body.is_empty() {
                return Err(NetworkError::Serialization {
                    detail: "service RPC response value is present in both head and stream"
                        .to_string(),
                });
            }
            if preserve_streamed_value {
                Ok(RpcResponseConsumption::Streaming(
                    ObjectRpcResult::streamed_value(head.response, bridge_stream_body(body)),
                ))
            } else {
                head.response.body = body.materialize_to_vec().await?;
                Ok(RpcResponseConsumption::Complete(ObjectRpcResult::value(
                    head.response,
                )))
            }
        }
        ObjectRpcBodyRole::Attachment => Ok(RpcResponseConsumption::Streaming(
            ObjectRpcResult::new(head.response, bridge_stream_body(body)),
        )),
    }
}

#[cfg(test)]
#[tracing::instrument(target = "sinomo.perf", level = "trace", name = "network.service_rpc",
    skip_all, fields(stage = "network.service_rpc", rpc_route = call.request.route.as_str(), rpc_request_id = tracing::field::Empty,
        observation = call.request.route.starts_with("/observe/"), traceparent = call.request.traceparent().unwrap_or_default()))]
pub(crate) async fn call_service_rpc_stream<S>(
    stream: S,
    call: ObjectRpcCall,
    options: ObjectFrameIoOptions,
) -> Result<ObjectRpcResult, NetworkError>
where
    S: tokio::io::AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let timeline_trace = crate::network_timeline_trace_context_for_request(&call.request);
    let operation_id = call.request.operation_id().map(str::to_string);
    let route = call.request.route.clone();
    let (request, body_role, body) = call.into_stream_native_parts();
    let encode_started_ns = service_timeline_started_ns(timeline_trace);
    let application_head_result = encode_service_head(&ServiceRequestHead {
        schema: SERVICE_RPC_HEAD_SCHEMA.to_string(),
        request,
        body_role,
    });
    record_service_timeline_since(
        timeline_trace,
        crate::network_timeline_stage::SERVICE_REQUEST_ENCODE,
        crate::network_timeline_flag::REQUEST,
        encode_started_ns,
        application_head_result.is_ok(),
    );
    let application_head = application_head_result?;
    let request_id = sinomo_base::Id::generate();
    tracing::Span::current().record("rpc_request_id", request_id.as_str());
    let stream_call = ObjectRpcStreamCall {
        request_id,
        method: route.clone(),
        operation_id: operation_id.clone(),
        metadata: Default::default(),
        application_head,
        body: ObjectRpcStreamBody::source(body),
        timeline_trace,
    };
    match call_object_rpc_stream_with_consumption(stream, stream_call, options, move |response| {
        service_result_from_stream(route, response, false, timeline_trace)
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

#[tracing::instrument(target = "sinomo.perf", level = "trace", name = "network.service_rpc",
    skip_all, fields(stage = "network.service_rpc", rpc_route = call.request.route.as_str(), rpc_request_id = tracing::field::Empty, observation = call.request.route.starts_with("/observe/"), traceparent = call.request.traceparent().unwrap_or_default()))]
pub async fn call_service_rpc_peer_stream(
    stream: PeerStream,
    call: ObjectRpcCall,
    options: ObjectFrameIoOptions,
) -> Result<ObjectRpcResult, NetworkError> {
    let timeline_trace = crate::network_timeline_trace_context_for_request(&call.request);
    let operation_id = call.request.operation_id().map(str::to_string);
    let route = call.request.route.clone();
    let (request, body_role, body) = call.into_stream_native_parts();
    let encode_started_ns = service_timeline_started_ns(timeline_trace);
    let application_head_result = encode_service_head(&ServiceRequestHead {
        schema: SERVICE_RPC_HEAD_SCHEMA.to_string(),
        request,
        body_role,
    });
    record_service_timeline_since(
        timeline_trace,
        crate::network_timeline_stage::SERVICE_REQUEST_ENCODE,
        crate::network_timeline_flag::REQUEST,
        encode_started_ns,
        application_head_result.is_ok(),
    );
    let application_head = application_head_result?;
    let request_id = sinomo_base::Id::generate();
    tracing::Span::current().record("rpc_request_id", request_id.as_str());
    let stream_call = ObjectRpcStreamCall {
        request_id,
        method: route.clone(),
        operation_id: operation_id.clone(),
        metadata: Default::default(),
        application_head,
        body: ObjectRpcStreamBody::source(body),
        timeline_trace,
    };
    let chunks = stream.into_chunk_stream()?;
    match call_object_rpc_chunk_stream_with_consumption(
        chunks,
        stream_call,
        options,
        move |response| service_result_from_stream(route, response, true, timeline_trace),
    )
    .await
    {
        RpcDeliveryOutcome::Completed(result) => Ok(result),
        RpcDeliveryOutcome::Rejected { detail } => {
            if detail == NetworkError::AuthDenied.to_string() {
                Err(NetworkError::AuthDenied)
            } else {
                Err(NetworkError::delivery_failed(
                    "dispatch owned stream-native service RPC",
                    NetworkDeliveryState::PossiblyApplied,
                    detail,
                ))
            }
        }
        RpcDeliveryOutcome::NotSent { error } => Err(if error.delivery_state().is_some() {
            error
        } else {
            NetworkError::delivery_failed(
                "send owned stream-native service RPC",
                NetworkDeliveryState::DefinitelyNotSent,
                error.to_string(),
            )
        }),
        RpcDeliveryOutcome::PossiblyApplied { error } => Err(NetworkError::outcome_unknown(
            "owned stream-native service RPC",
            operation_id.as_deref(),
            error.to_string(),
        )),
    }
}

#[tracing::instrument(target = "sinomo.perf", level = "trace", name = "network.server",
    skip_all, fields(stage = "network.server", rpc_route = tracing::field::Empty, rpc_request_id = tracing::field::Empty, traceparent = tracing::field::Empty, observation = false,
        peer = performance_peer(context)))]
pub async fn serve_service_rpc_stream<S, F, Fut>(
    stream: S,
    context: &RpcPeerContext,
    options: ObjectFrameIoOptions,
    handler: F,
) -> Result<(), NetworkError>
where
    S: tokio::io::AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: FnOnce(ObjectRpcIncomingRequest, RpcPeerContext) -> Fut,
    Fut: Future<Output = Result<ObjectRpcOutgoingResponse, NetworkError>>,
{
    serve_object_rpc_stream(stream, context, options, move |incoming, peer| async move {
        let request_id = incoming.request_id.clone();
        tracing::Span::current().record("rpc_request_id", request_id.as_str());
        let request = service_request_from_stream(incoming, false).await?;
        tracing::Span::current().record("rpc_route", request.request.route.as_str());
        let timeline_trace = crate::network_timeline_trace_context_for_request(&request.request);
        let handler_started_ns = service_timeline_started_ns(timeline_trace);
        tracing::Span::current().record(
            "traceparent",
            request.request.traceparent().unwrap_or_default(),
        );
        if request.request.route.starts_with("/observe/") {
            tracing::Span::current().record("observation", true);
            // A snapshot contains its own unfinished RPC. Mark its ancestry
            // before dispatch so reports can exclude observer-generated work.
            tracing::trace!(target: "sinomo.perf", observation = true, "performance.observation");
        }
        let handler_span = tracing::trace_span!(target: "sinomo.perf", "network.handler",
            stage = "network.handler", outcome = "unknown");
        let response_result = handler(request, peer)
            .instrument(handler_span.clone())
            .await;
        handler_span.record(
            "outcome",
            if response_result.is_ok() {
                "ok"
            } else {
                "error"
            },
        );
        drop(handler_span);
        record_service_timeline_since(
            timeline_trace,
            crate::network_timeline_stage::SERVICE_HANDLER,
            crate::network_timeline_flag::REQUEST | crate::network_timeline_flag::RESPONSE,
            handler_started_ns,
            response_result.is_ok(),
        );
        let response = response_result?;
        service_reply_to_stream(request_id, response, timeline_trace)
    })
    .await
}

#[tracing::instrument(target = "sinomo.perf", level = "trace", name = "network.server",
    skip_all, fields(stage = "network.server", rpc_route = tracing::field::Empty, rpc_request_id = tracing::field::Empty, traceparent = tracing::field::Empty, observation = false,
        peer = performance_peer(context)))]
pub async fn serve_service_rpc_peer_stream<F, Fut>(
    stream: PeerStream,
    context: &RpcPeerContext,
    options: ObjectFrameIoOptions,
    handler: F,
) -> Result<(), NetworkError>
where
    F: FnOnce(ObjectRpcIncomingRequest, RpcPeerContext) -> Fut,
    Fut: Future<Output = Result<ObjectRpcOutgoingResponse, NetworkError>>,
{
    // Capture the local mux identity before consuming the owned stream. The
    // sampled request trace becomes available only after service-head validation.
    let request_timeline_identity =
        crate::network_timeline_capture_is_active().then(|| stream.timeline_stream_identity());
    let chunks = stream.into_chunk_stream()?;
    serve_object_rpc_chunk_stream(chunks, context, options, move |incoming, peer| async move {
        let request_id = incoming.request_id.clone();
        tracing::Span::current().record("rpc_request_id", request_id.as_str());
        let request = service_request_from_stream(incoming, true).await?;
        tracing::Span::current().record("rpc_route", request.request.route.as_str());
        let timeline_trace = crate::network_timeline_trace_context_for_request(&request.request);
        if let (Some(trace), Some((connection, call))) = (timeline_trace, request_timeline_identity)
        {
            // The request head's control slab was released before this adapter
            // decoded its application head. Carrier and stream bridge the
            // earlier readiness/dispatch points to this exact sampled RPC.
            crate::timeline::network_timeline_record_point_with_trace(
                Some(trace),
                crate::network_timeline_stage::RPC_REQUEST_HEAD,
                crate::network_timeline_flag::REQUEST | crate::network_timeline_flag::SUCCESS,
                connection,
                call,
                0,
                0,
                0,
            );
        }
        let handler_started_ns = service_timeline_started_ns(timeline_trace);
        tracing::Span::current().record(
            "traceparent",
            request.request.traceparent().unwrap_or_default(),
        );
        if request.request.route.starts_with("/observe/") {
            tracing::Span::current().record("observation", true);
            // A snapshot contains its own unfinished RPC. Mark its ancestry
            // before dispatch so reports can exclude observer-generated work.
            tracing::trace!(target: "sinomo.perf", observation = true, "performance.observation");
        }
        let handler_span = tracing::trace_span!(target: "sinomo.perf", "network.handler",
            stage = "network.handler", outcome = "unknown");
        let response_result = handler(request, peer)
            .instrument(handler_span.clone())
            .await;
        handler_span.record(
            "outcome",
            if response_result.is_ok() {
                "ok"
            } else {
                "error"
            },
        );
        drop(handler_span);
        record_service_timeline_since(
            timeline_trace,
            crate::network_timeline_stage::SERVICE_HANDLER,
            crate::network_timeline_flag::REQUEST | crate::network_timeline_flag::RESPONSE,
            handler_started_ns,
            response_result.is_ok(),
        );
        let response = response_result?;
        service_reply_to_stream(request_id, response, timeline_trace)
    })
    .await
}

fn performance_peer(context: &RpcPeerContext) -> &str {
    match &context.identity {
        crate::RpcPeerIdentity::Authenticated { peer_node_id, .. } => peer_node_id.as_str(),
        crate::RpcPeerIdentity::UnauthenticatedLocal => "local",
    }
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
    ) -> Result<ObjectRpcResult, NetworkError> {
        let (client, server) = tokio::io::duplex(2 * OBJECT_RPC_STREAM_RECEIVE_WINDOW_BYTES);
        let server_context = local_context();
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
                    .pipe(ObjectRpcOutgoingResponse::value)
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
        let call = ObjectRpcCall::value(request);
        let result = call_service_rpc_stream(client, call, ObjectFrameIoOptions::default()).await?;
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

    #[tokio::test]
    async fn service_handler_failure_after_effect_keeps_unknown_delivery() {
        let effects = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let server_effects = effects.clone();
        let (client, server) = tokio::io::duplex(4096);
        let context = local_context();
        let server_task = tokio::spawn(async move {
            serve_service_rpc_stream(
                server,
                &context,
                ObjectFrameIoOptions::default(),
                move |_request, _| async move {
                    server_effects.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Err(NetworkError::Io {
                        operation: "complete committed service response".into(),
                        detail: "response publication failed".into(),
                    })
                },
            )
            .await
        });
        let request = ObjectRpcRequest::from_encoded_body(
            "/test/after-effect",
            "test.after-effect",
            Vec::new(),
        );
        let error = call_service_rpc_stream(
            client,
            ObjectRpcCall::value(request),
            ObjectFrameIoOptions::default(),
        )
        .await
        .expect_err("a handler failure cannot certify that its effect did not occur");
        assert_eq!(effects.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            error.delivery_state(),
            Some(NetworkDeliveryState::PossiblyApplied)
        );
        server_task.await.unwrap().unwrap();
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
        let (_, server_context) = authenticated_contexts();
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
                    Ok(ObjectRpcOutgoingResponse::new(
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
        let mut request = ObjectRpcRequest::from_encoded_body(
            "/test/service-attachment",
            "test.service-attachment",
            b"typed-head".to_vec(),
        );
        // Authentication data is application-owned and is transported only
        // when the caller explicitly supplies it.
        request.auth = Some(b"origin-auth".to_vec());
        let call = ObjectRpcCall::new(request, crate::ObjectRpcBodySource::bytes(vec![3; bytes]));
        let result = call_service_rpc_stream(client, call, ObjectFrameIoOptions::default()).await?;
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
        let upstream_proxy_context = local_context();
        let downstream_server_context = local_context();

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
                    Ok(ObjectRpcOutgoingResponse::new(
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
                    let call = ObjectRpcCall::from_forwarded_body(request.request, request.body);
                    let result = call_service_rpc_stream(
                        proxy_outgoing,
                        call,
                        ObjectFrameIoOptions::default(),
                    )
                    .await?;
                    assert_eq!(result.body.total_bytes(), Some(response_bytes as u64));
                    result.into_forwarded_response()
                },
            )
            .await
        });

        let call = ObjectRpcCall::new(
            ObjectRpcRequest::from_encoded_body(
                "/test/service-proxy-attachment",
                "test.service-proxy-attachment",
                b"proxied-request-head".to_vec(),
            ),
            crate::ObjectRpcBodySource::zeroes(request_bytes as u64),
        );
        let result = call_service_rpc_stream(client, call, ObjectFrameIoOptions::default())
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
