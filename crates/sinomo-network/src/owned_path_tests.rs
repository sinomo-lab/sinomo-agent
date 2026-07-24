use crate::ObjectFrameIoOptions;
use crate::SinomoObjectCodec;
use crate::object_rpc_stream::{
    OBJECT_RPC_STREAM_INLINE_BODY_BYTES, ObjectRpcStreamBody, ObjectRpcStreamCall,
    ObjectRpcStreamReply, RpcDeliveryOutcome, RpcPeerContext, RpcResponseConsumption,
    call_object_rpc_chunk_stream_with_consumption, serve_object_rpc_chunk_stream,
};
use crate::owned_chunk::install_chunk_identity_observer;
use crate::owned_mux::{OwnedRoutedStreamClient, accept_owned_routed_stream_carrier};
use crate::routing::{PeerStream, RouteTrace, encrypted_routed_session_chunk_stream};
use crate::rpc_body::{ObjectRpcBodySource, ObjectRpcCall, ObjectRpcOutgoingResponse};
use crate::service_rpc_stream::{call_service_rpc_peer_stream, serve_service_rpc_peer_stream};
use serde::{Deserialize, Serialize};
use sinomo_base::Id;
use sinomo_node::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

fn node(name: &str) -> NodeId {
    NodeId::from_legacy_named(name).expect("node id")
}

fn identity_observer_test_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn owned_carrier_pair(
    source_node: &NodeId,
    destination_node: &NodeId,
    session_name: &str,
) -> (PeerStream, PeerStream) {
    let session_id = Id::from_legacy_named(session_name).expect("session id");
    let channel_binding = *blake3::hash(format!("{session_name}:binding").as_bytes()).as_bytes();
    let shared_secret = *blake3::hash(format!("{session_name}:secret").as_bytes()).as_bytes();
    let (source_wire, destination_wire) = tokio::io::duplex(512 * 1024);
    let source_secure = encrypted_routed_session_chunk_stream(
        Box::new(source_wire),
        shared_secret,
        session_id.clone(),
        channel_binding,
        true,
    )
    .expect("source secure chunk stream");
    let destination_secure = encrypted_routed_session_chunk_stream(
        Box::new(destination_wire),
        shared_secret,
        session_id.clone(),
        channel_binding,
        false,
    )
    .expect("destination secure chunk stream");
    let source = PeerStream::new_owned(
        source_secure,
        source_node.clone(),
        destination_node.clone(),
        session_id.clone(),
        channel_binding,
        RouteTrace {
            nodes: vec![source_node.clone(), destination_node.clone()],
            endpoint_ids: vec![Id::generate()],
        },
    )
    .expect("source owned carrier");
    let destination = PeerStream::new_owned(
        destination_secure,
        destination_node.clone(),
        source_node.clone(),
        session_id,
        channel_binding,
        RouteTrace {
            nodes: vec![destination_node.clone(), source_node.clone()],
            endpoint_ids: vec![Id::generate()],
        },
    )
    .expect("destination owned carrier");
    (source, destination)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct OwnedServiceValue {
    label: String,
    bytes: Vec<u8>,
    offsets: Vec<u64>,
}

#[tokio::test]
async fn owned_service_values_decode_incrementally_without_byte_fallback() {
    let source_node = node("node_owned_service_source");
    let destination_node = node("node_owned_service_destination");
    let carrier_session =
        Id::from_legacy_named("session_owned_service_carrier").expect("session id");
    let carrier_binding = *blake3::hash(b"owned service carrier binding").as_bytes();
    let shared_secret = *blake3::hash(b"owned service shared secret").as_bytes();
    let (source_wire, destination_wire) = tokio::io::duplex(512 * 1024);
    let source_secure = encrypted_routed_session_chunk_stream(
        Box::new(source_wire),
        shared_secret,
        carrier_session.clone(),
        carrier_binding,
        true,
    )
    .expect("source secure chunk stream");
    let destination_secure = encrypted_routed_session_chunk_stream(
        Box::new(destination_wire),
        shared_secret,
        carrier_session.clone(),
        carrier_binding,
        false,
    )
    .expect("destination secure chunk stream");
    let source_carrier = PeerStream::new_owned(
        source_secure,
        source_node.clone(),
        destination_node.clone(),
        carrier_session.clone(),
        carrier_binding,
        RouteTrace {
            nodes: vec![source_node.clone(), destination_node.clone()],
            endpoint_ids: vec![Id::generate()],
        },
    )
    .expect("source owned carrier");
    let destination_carrier = PeerStream::new_owned(
        destination_secure,
        destination_node.clone(),
        source_node.clone(),
        carrier_session,
        carrier_binding,
        RouteTrace {
            nodes: vec![destination_node.clone(), source_node.clone()],
            endpoint_ids: vec![Id::generate()],
        },
    )
    .expect("destination owned carrier");
    let client = Arc::new(OwnedRoutedStreamClient::new(source_carrier).expect("owned mux client"));
    let mut acceptor =
        accept_owned_routed_stream_carrier(destination_carrier).expect("owned mux acceptor");
    let expected_request = OwnedServiceValue {
        label: "request".repeat(47),
        bytes: vec![0x5a; 512 * 1024 + 73],
        offsets: (0..8193).collect(),
    };
    let expected_response = OwnedServiceValue {
        label: "response".repeat(31),
        bytes: vec![0x7c; 384 * 1024 + 19],
        offsets: (0..4097).map(|value| value * 3).collect(),
    };
    let server_request = expected_request.clone();
    let server_response = OwnedServiceValue {
        label: expected_response.label.clone(),
        bytes: expected_response.bytes.clone(),
        offsets: expected_response.offsets.clone(),
    };
    let (client_done, server_done) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let stream = acceptor
            .accept()
            .await
            .expect("accept owned logical stream")
            .expect("owned logical stream");
        let context = RpcPeerContext::authenticated(
            stream.local_node_id.clone(),
            stream.peer_node_id.clone(),
            stream.session_id.clone(),
            stream.channel_binding,
        )
        .expect("server RPC context");
        serve_service_rpc_peer_stream(
            stream,
            &context,
            ObjectFrameIoOptions::default(),
            move |request, _| async move {
                assert!(request.has_streamed_value());
                assert!(request.request.body.is_empty());
                let decoded = request
                    .decode_body::<OwnedServiceValue>()
                    .await
                    .expect("incrementally decode request");
                assert_eq!(decoded, server_request);
                ObjectRpcOutgoingResponse::encode_value("/test/owned-service", server_response)
            },
        )
        .await
        .expect("serve owned service RPC");
        let _ = server_done.await;
    });

    let stream = client
        .open(Duration::from_secs(1))
        .await
        .expect("open owned logical stream");
    let context = RpcPeerContext::authenticated(
        stream.local_node_id.clone(),
        stream.peer_node_id.clone(),
        stream.session_id.clone(),
        stream.channel_binding,
    )
    .expect("client RPC context");
    let call = ObjectRpcCall::encode_value(
        "/test/owned-service",
        "test.owned-service",
        expected_request,
    )
    .expect("encode transport-owned request");
    let result =
        call_service_rpc_peer_stream(stream, &context, call, ObjectFrameIoOptions::default())
            .await
            .expect("call owned service RPC");
    assert!(result.has_streamed_value());
    assert!(result.response.body.is_empty());
    assert_eq!(
        result
            .decode_body::<OwnedServiceValue>()
            .await
            .expect("incrementally decode response"),
        expected_response
    );
    let _ = client_done.send(());
    server.await.expect("join owned service server");
}

#[tokio::test]
async fn owned_rpc_keeps_slab_identity_through_every_endpoint_layer() {
    let _identity_test_guard = identity_observer_test_lock().lock().await;
    let (_identity_guard, identity_events) = install_chunk_identity_observer();
    let source_node = node("node_owned_identity_source");
    let destination_node = node("node_owned_identity_destination");
    let carrier_session =
        Id::from_legacy_named("session_owned_identity_carrier").expect("session id");
    let carrier_binding = *blake3::hash(b"owned identity carrier binding").as_bytes();
    let shared_secret = *blake3::hash(b"owned identity shared secret").as_bytes();
    let (source_wire, destination_wire) = tokio::io::duplex(512 * 1024);
    let source_secure = encrypted_routed_session_chunk_stream(
        Box::new(source_wire),
        shared_secret,
        carrier_session.clone(),
        carrier_binding,
        true,
    )
    .expect("source secure chunk stream");
    let destination_secure = encrypted_routed_session_chunk_stream(
        Box::new(destination_wire),
        shared_secret,
        carrier_session.clone(),
        carrier_binding,
        false,
    )
    .expect("destination secure chunk stream");
    let source_carrier = PeerStream::new_owned(
        source_secure,
        source_node.clone(),
        destination_node.clone(),
        carrier_session.clone(),
        carrier_binding,
        RouteTrace {
            nodes: vec![source_node.clone(), destination_node.clone()],
            endpoint_ids: vec![Id::generate()],
        },
    )
    .expect("source owned carrier");
    let destination_carrier = PeerStream::new_owned(
        destination_secure,
        destination_node.clone(),
        source_node.clone(),
        carrier_session,
        carrier_binding,
        RouteTrace {
            nodes: vec![destination_node.clone(), source_node.clone()],
            endpoint_ids: vec![Id::generate()],
        },
    )
    .expect("destination owned carrier");
    let client = Arc::new(OwnedRoutedStreamClient::new(source_carrier).expect("owned mux client"));
    let mut acceptor =
        accept_owned_routed_stream_carrier(destination_carrier).expect("owned mux acceptor");
    let expected_payload = vec![0x5a; OBJECT_RPC_STREAM_INLINE_BODY_BYTES + 2048];
    let server_payload = expected_payload.clone();
    let server = tokio::spawn(async move {
        let stream = acceptor
            .accept()
            .await
            .expect("accept owned logical stream")
            .expect("owned logical stream");
        let context = RpcPeerContext::authenticated(
            stream.local_node_id.clone(),
            stream.peer_node_id.clone(),
            stream.session_id.clone(),
            stream.channel_binding,
        )
        .expect("server RPC context");
        let chunks = stream.into_chunk_stream().expect("server logical chunks");
        serve_object_rpc_chunk_stream(
            chunks,
            &context,
            ObjectFrameIoOptions::default(),
            |incoming, _| async move {
                let request_id = incoming.request_id.clone();
                let mut body = incoming.body.into_application_body();
                let chunk = body
                    .next_chunk()
                    .await
                    .expect("read application body")
                    .expect("application body chunk");
                let decoded: Vec<u8> = SinomoObjectCodec::network_frame()
                    .decode(&chunk)
                    .expect("decode direct SOBJ chunk");
                assert_eq!(decoded, server_payload);
                assert!(
                    body.next_chunk()
                        .await
                        .expect("finish application body")
                        .is_none()
                );
                Ok(ObjectRpcStreamReply {
                    request_id,
                    metadata: BTreeMap::new(),
                    application_head: Vec::new(),
                    body: ObjectRpcStreamBody::bytes(Vec::new()),
                    timeline_trace: None,
                })
            },
        )
        .await
        .expect("serve owned layered RPC");
    });
    let stream = client
        .open(Duration::from_secs(1))
        .await
        .expect("open owned logical stream");
    let context = RpcPeerContext::authenticated(
        stream.local_node_id.clone(),
        stream.peer_node_id.clone(),
        stream.session_id.clone(),
        stream.channel_binding,
    )
    .expect("client RPC context");
    let chunks = stream.into_chunk_stream().expect("client logical chunks");
    let body = ObjectRpcBodySource::sobj_value(expected_payload)
        .expect("encode transport-shaped SOBJ body");
    let result = call_object_rpc_chunk_stream_with_consumption(
        chunks,
        &context,
        ObjectRpcStreamCall {
            request_id: Id::generate(),
            method: "test.identity".to_string(),
            operation_id: None,
            metadata: BTreeMap::new(),
            application_head: Vec::new(),
            body: ObjectRpcStreamBody::source(body),
            timeline_trace: None,
        },
        ObjectFrameIoOptions::default(),
        |response| async move {
            assert!(response.body.materialize_to_vec().await?.is_empty());
            Ok(RpcResponseConsumption::Complete(()))
        },
    )
    .await;
    assert!(
        matches!(result, RpcDeliveryOutcome::Completed(())),
        "owned layered RPC failed: {result:?}"
    );
    server.await.expect("join owned layered RPC server");

    let mut stages_by_slab = BTreeMap::<u64, BTreeSet<&'static str>>::new();
    for event in identity_events.try_iter() {
        stages_by_slab
            .entry(event.slab_id)
            .or_default()
            .insert(event.stage);
    }
    let expected_source_stages = [
        "producer-buffer-adopted",
        "rpc-body-produced",
        "rpc-record-framed",
        "mux-rpc-record",
        "mux-record-framed",
        "secure-record-plaintext",
        "secure-record-socket-staging",
    ];
    let source_stages = stages_by_slab
        .values()
        .find(|stages| {
            expected_source_stages
                .iter()
                .all(|stage| stages.contains(stage))
        })
        .unwrap_or_else(|| panic!("no source slab crossed every layer: {stages_by_slab:?}"));
    assert!(
        expected_source_stages
            .iter()
            .all(|stage| source_stages.contains(stage))
    );

    let expected_destination_stages = [
        "secure-record-decrypted",
        "mux-record-received",
        "rpc-record-received",
        "application-body-delivery",
    ];
    let destination_stages = stages_by_slab
        .values()
        .find(|stages| {
            expected_destination_stages
                .iter()
                .all(|stage| stages.contains(stage))
        })
        .unwrap_or_else(|| panic!("no destination slab crossed every layer: {stages_by_slab:?}"));
    assert!(
        expected_destination_stages
            .iter()
            .all(|stage| destination_stages.contains(stage))
    );
}

#[tokio::test]
async fn received_rpc_chunks_keep_slab_identity_when_forwarded_to_another_rpc() {
    let _identity_test_guard = identity_observer_test_lock().lock().await;
    let (_identity_guard, identity_events) = install_chunk_identity_observer();
    let source_node = node("node_owned_forward_source");
    let forwarder_node = node("node_owned_forward_middle");
    let destination_node = node("node_owned_forward_destination");
    let (source_carrier, forwarder_inbound_carrier) = owned_carrier_pair(
        &source_node,
        &forwarder_node,
        "session_owned_forward_inbound",
    );
    let (forwarder_outbound_carrier, destination_carrier) = owned_carrier_pair(
        &forwarder_node,
        &destination_node,
        "session_owned_forward_outbound",
    );
    let source_client =
        Arc::new(OwnedRoutedStreamClient::new(source_carrier).expect("source mux client"));
    let forwarder_client = Arc::new(
        OwnedRoutedStreamClient::new(forwarder_outbound_carrier)
            .expect("forwarder outbound mux client"),
    );
    let mut forwarder_acceptor = accept_owned_routed_stream_carrier(forwarder_inbound_carrier)
        .expect("forwarder mux acceptor");
    let mut destination_acceptor =
        accept_owned_routed_stream_carrier(destination_carrier).expect("destination mux acceptor");
    let expected_payload = (0..(OBJECT_RPC_STREAM_INLINE_BODY_BYTES * 2 + 137))
        .map(|offset| (offset % 251) as u8)
        .collect::<Vec<_>>();
    let destination_payload = expected_payload.clone();

    let destination_server = tokio::spawn(async move {
        let stream = destination_acceptor
            .accept()
            .await
            .expect("accept destination logical stream")
            .expect("destination logical stream");
        let context = RpcPeerContext::authenticated(
            stream.local_node_id.clone(),
            stream.peer_node_id.clone(),
            stream.session_id.clone(),
            stream.channel_binding,
        )
        .expect("destination RPC context");
        serve_object_rpc_chunk_stream(
            stream.into_chunk_stream().expect("destination chunks"),
            &context,
            ObjectFrameIoOptions::default(),
            move |incoming, _| async move {
                let request_id = incoming.request_id.clone();
                let actual = incoming
                    .body
                    .materialize_to_vec()
                    .await
                    .expect("consume forwarded body");
                assert_eq!(actual, destination_payload);
                Ok(ObjectRpcStreamReply {
                    request_id,
                    metadata: BTreeMap::new(),
                    application_head: Vec::new(),
                    body: ObjectRpcStreamBody::bytes(Vec::new()),
                    timeline_trace: None,
                })
            },
        )
        .await
        .expect("serve destination RPC");
    });

    let forwarder_server = tokio::spawn(async move {
        let stream = forwarder_acceptor
            .accept()
            .await
            .expect("accept forwarder logical stream")
            .expect("forwarder logical stream");
        let context = RpcPeerContext::authenticated(
            stream.local_node_id.clone(),
            stream.peer_node_id.clone(),
            stream.session_id.clone(),
            stream.channel_binding,
        )
        .expect("forwarder inbound RPC context");
        serve_object_rpc_chunk_stream(
            stream
                .into_chunk_stream()
                .expect("forwarder inbound chunks"),
            &context,
            ObjectFrameIoOptions::default(),
            move |incoming, _| {
                let forwarder_client = Arc::clone(&forwarder_client);
                async move {
                    let upstream_request_id = incoming.request_id.clone();
                    let forward_body = ObjectRpcBodySource::from_incoming_body(
                        incoming.body.into_application_body(),
                    );
                    let downstream = forwarder_client
                        .open(Duration::from_secs(1))
                        .await
                        .expect("open downstream logical stream");
                    let downstream_context = RpcPeerContext::authenticated(
                        downstream.local_node_id.clone(),
                        downstream.peer_node_id.clone(),
                        downstream.session_id.clone(),
                        downstream.channel_binding,
                    )
                    .expect("forwarder outbound RPC context");
                    let outcome = call_object_rpc_chunk_stream_with_consumption(
                        downstream
                            .into_chunk_stream()
                            .expect("forwarder outbound chunks"),
                        &downstream_context,
                        ObjectRpcStreamCall {
                            request_id: Id::generate(),
                            method: "test.forward.destination".to_string(),
                            operation_id: None,
                            metadata: BTreeMap::new(),
                            application_head: Vec::new(),
                            body: ObjectRpcStreamBody::source(forward_body),
                            timeline_trace: None,
                        },
                        ObjectFrameIoOptions::default(),
                        |response| async move {
                            assert!(response.body.materialize_to_vec().await?.is_empty());
                            Ok(RpcResponseConsumption::Complete(()))
                        },
                    )
                    .await;
                    match outcome {
                        RpcDeliveryOutcome::Completed(()) => {}
                        RpcDeliveryOutcome::NotSent { error }
                        | RpcDeliveryOutcome::PossiblyApplied { error } => return Err(error),
                        RpcDeliveryOutcome::Rejected { detail } => {
                            return Err(crate::NetworkError::ObjectRpc {
                                route: "test.forward.destination".to_string(),
                                detail,
                            });
                        }
                    }
                    Ok(ObjectRpcStreamReply {
                        request_id: upstream_request_id,
                        metadata: BTreeMap::new(),
                        application_head: Vec::new(),
                        body: ObjectRpcStreamBody::bytes(Vec::new()),
                        timeline_trace: None,
                    })
                }
            },
        )
        .await
        .expect("serve forwarder RPC");
    });

    let source_stream = source_client
        .open(Duration::from_secs(1))
        .await
        .expect("open source logical stream");
    let source_context = RpcPeerContext::authenticated(
        source_stream.local_node_id.clone(),
        source_stream.peer_node_id.clone(),
        source_stream.session_id.clone(),
        source_stream.channel_binding,
    )
    .expect("source RPC context");
    let outcome = call_object_rpc_chunk_stream_with_consumption(
        source_stream.into_chunk_stream().expect("source chunks"),
        &source_context,
        ObjectRpcStreamCall {
            request_id: Id::generate(),
            method: "test.forward.middle".to_string(),
            operation_id: None,
            metadata: BTreeMap::new(),
            application_head: Vec::new(),
            body: ObjectRpcStreamBody::source(ObjectRpcBodySource::bytes(expected_payload)),
            timeline_trace: None,
        },
        ObjectFrameIoOptions::default(),
        |response| async move {
            assert!(response.body.materialize_to_vec().await?.is_empty());
            Ok(RpcResponseConsumption::Complete(()))
        },
    )
    .await;
    assert!(
        matches!(outcome, RpcDeliveryOutcome::Completed(())),
        "forwarded owned RPC failed: {outcome:?}"
    );
    forwarder_server.await.expect("join forwarder server");
    destination_server.await.expect("join destination server");

    let mut stages_by_slab = BTreeMap::<u64, BTreeSet<&'static str>>::new();
    for event in identity_events.try_iter() {
        stages_by_slab
            .entry(event.slab_id)
            .or_default()
            .insert(event.stage);
    }
    let forwarding_stages = [
        "secure-record-decrypted",
        "mux-record-received",
        "rpc-record-received",
        "application-body-forwarding",
        "rpc-body-produced",
        "rpc-record-framed",
        "mux-record-framed",
        "secure-record-socket-staging",
    ];
    assert!(
        stages_by_slab
            .values()
            .any(|stages| { forwarding_stages.iter().all(|stage| stages.contains(stage)) }),
        "no received slab retained its identity through forwarding: {stages_by_slab:?}"
    );
}
