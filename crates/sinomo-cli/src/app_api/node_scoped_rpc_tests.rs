use super::*;
use sinomo_runtime::api::{
    DAEMON_DFS_BROWSER_LIST_ROUTE, DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE,
    DaemonDfsBrowserListRequest, DaemonDfsBrowserListResponse, DaemonDfsBrowserSortDirection,
    DaemonDfsBrowserSortKey, DaemonObjectRpcForwardRequest, DaemonObjectRpcForwardResponse,
};
use sinomo_runtime::network::{
    AsyncDaemonControlListener, ObjectFrameIoOptions, ObjectRpcOutgoingResponse, ObjectRpcResponse,
    RpcPeerContext, accept_local_object_rpc_stream_carrier, read_local_object_rpc_carrier_prelude,
    serve_service_rpc_peer_stream,
};

fn list_request() -> DaemonDfsBrowserListRequest {
    DaemonDfsBrowserListRequest {
        path: "/services".into(),
        shallow: true,
        missing_is_empty: false,
        sort_key: DaemonDfsBrowserSortKey::Name,
        sort_direction: DaemonDfsBrowserSortDirection::Ascending,
        offset: 0,
        limit: Some(4),
        include_metadata: false,
    }
}

#[derive(Clone, Copy, PartialEq)]
enum ReplyGate {
    Head,
    Body,
    Rejected,
}

struct HeldNativeReply {
    bytes: Vec<u8>,
    offset: usize,
    entered: Option<tokio::sync::oneshot::Sender<()>>,
    release: tokio::sync::oneshot::Receiver<()>,
    released: bool,
}

impl tokio::io::AsyncRead for HeldNativeReply {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::future::Future;
        if let Some(entered) = self.entered.take() {
            let _ = entered.send(());
        }
        if !self.released {
            match std::pin::Pin::new(&mut self.release).poll(cx) {
                std::task::Poll::Pending => return std::task::Poll::Pending,
                std::task::Poll::Ready(_) => self.released = true,
            }
        }
        let len = buffer.remaining().min(self.bytes.len() - self.offset);
        buffer.put_slice(&self.bytes[self.offset..self.offset + len]);
        self.offset += len;
        std::task::Poll::Ready(Ok(()))
    }
}

fn held_node_scoped_read(deadline: Option<Duration>, gate: ReplyGate) {
    let root = sinomo_testdir::TestDir::new("node-scoped-browser-healthy-wait");
    let data = SinomoDataDir::new(root.path());
    let _native = crate::native_test_fixture::NativeCliFixture::start(&data);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let state = sinomo_runtime::cluster::read_cluster_state_without_kv(
        &sinomo_runtime::network::ClusterPaths::new(&data),
    )
    .unwrap();
    // The held reply is produced by the actual native browser and its normal
    // initialized authority, not a successful metadata ACK invented by this test.
    let native = crate::native_test_fixture::browser(&data, runtime.handle()).unwrap();
    let expected: DaemonDfsBrowserListResponse = native
        .read_rpc(DAEMON_DFS_BROWSER_LIST_ROUTE, &list_request())
        .unwrap();
    drop(native);
    runtime.block_on(async {
        let listener = AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = listener.local_addr_string().unwrap();
        let mut browser = RemoteSifsBrowser::for_main_owner(
            data.clone(),
            address,
            state.local_node_id.clone(),
            &tokio::runtime::Handle::current(),
        );
        // Exercise the ordinary catalog-selected, node-scoped client. It must
        // retain the main-only forwarding contract, not use the direct shortcut.
        browser.direct_main_owner = false;
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let expected_target = state.local_node_id.clone();
        let server_expected = expected.clone();
        let server = tokio::spawn(async move {
            let mut carrier = listener.accept().await.unwrap().into_inner();
            let context = read_local_object_rpc_carrier_prelude(&mut carrier)
                .await
                .unwrap();
            let mut acceptor =
                accept_local_object_rpc_stream_carrier(Box::new(carrier), &context).unwrap();
            let mut entered_tx = Some(entered_tx);
            let mut release_rx = Some(release_rx);
            for index in 0..2 {
                let stream = acceptor.accept().await.unwrap().unwrap();
                let context = RpcPeerContext::unauthenticated_local(
                    stream.session_id.clone(),
                    stream.channel_binding,
                );
                let expected_target = expected_target.clone();
                let expected = server_expected.clone();
                let entered = (index == 1).then(|| entered_tx.take().unwrap());
                let release = (index == 1).then(|| release_rx.take().unwrap());
                // Warm the real stream-native carrier before shortening health
                // budgets, then hold only the selected reply after real ingress.
                let result = serve_service_rpc_peer_stream(
                    stream,
                    &context,
                    ObjectFrameIoOptions::default(),
                    move |inbound, _| async move {
                        assert_eq!(inbound.request.route, DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE);
                        let forward: DaemonObjectRpcForwardRequest = inbound.decode_body().await?;
                        assert_eq!(forward.target_node_id, expected_target);
                        let inner = if index == 0 {
                            assert_eq!(forward.request.route, "/test/warm-scoped-browser");
                            ObjectRpcResponse::encode_body(forward.request.route, &"warm")?
                        } else {
                            assert_eq!(forward.request.route, DAEMON_DFS_BROWSER_LIST_ROUTE);
                            let body: DaemonDfsBrowserListRequest =
                                forward.request.decode_body()?;
                            assert_eq!(body.path, "/services");
                            if gate == ReplyGate::Body {
                                let encoded = ObjectRpcResponse::encode_body(
                                    &forward.request.route,
                                    &expected,
                                )?;
                                let len = encoded.body.len() as u64;
                                let envelope = ObjectRpcResponse::encode_body(
                                    DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE,
                                    &DaemonObjectRpcForwardResponse::Success {
                                        response: ObjectRpcResponse::from_encoded_body(
                                            forward.request.route,
                                            Vec::new(),
                                        ),
                                        streamed_value_body: true,
                                        forward_elapsed_micros: 0,
                                    },
                                )?;
                                return Ok(ObjectRpcOutgoingResponse::new(
                                    envelope,
                                    sinomo_runtime::network::ObjectRpcBodySource::async_reader(
                                        HeldNativeReply {
                                            bytes: encoded.body,
                                            offset: 0,
                                            entered,
                                            release: release.unwrap(),
                                            released: false,
                                        },
                                        Some(len),
                                    ),
                                ));
                            }
                            entered.unwrap().send(()).unwrap();
                            if gate == ReplyGate::Rejected {
                                ObjectRpcResponse::encode_error(
                                    forward.request.route,
                                    "fixture denies this exact scoped read",
                                )?
                            } else {
                                release.unwrap().await.unwrap();
                                ObjectRpcResponse::encode_body(forward.request.route, &expected)?
                            }
                        };
                        ObjectRpcOutgoingResponse::encode_borrowed_value(
                            DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE,
                            &DaemonObjectRpcForwardResponse::Success {
                                response: inner,
                                streamed_value_body: false,
                                forward_elapsed_micros: 0,
                            },
                        )
                    },
                )
                .await;
                if index == 0 {
                    result.unwrap();
                } else {
                    return result;
                }
            }
            unreachable!()
        });
        // Keep the gates for the second request, not the carrier warmup.
        let warmed: String = browser.rpc("/test/warm-scoped-browser", &"warm").unwrap();
        assert_eq!(warmed, "warm");
        browser.timeout = Duration::from_millis(100);
        browser.operation_deadline = deadline.map(|budget| Instant::now() + budget);
        let mut read = tokio::task::spawn_blocking(move || {
            browser.rpc::<_, DaemonDfsBrowserListResponse>(
                DAEMON_DFS_BROWSER_LIST_ROUTE,
                &list_request(),
            )
        });
        entered_rx.await.unwrap();
        let early = tokio::time::timeout(Duration::from_millis(300), &mut read).await;
        let ended_while_held = early.is_ok();
        let _ = release_tx.send(());
        let result = match early {
            Ok(joined) => joined.unwrap(),
            Err(_) => read.await.unwrap(),
        };
        let delivered = server.await.unwrap();
        if gate == ReplyGate::Rejected {
            assert!(ended_while_held);
            delivered.unwrap();
            assert!(
                result
                    .unwrap_err()
                    .contains("fixture denies this exact scoped read")
            );
        } else if deadline.is_some() {
            assert!(
                ended_while_held,
                "original operation deadline must remain finite"
            );
            let error = result.unwrap_err();
            assert!(
                error.contains("Overall") && error.contains("PossiblyApplied"),
                "{error}"
            );
        } else {
            assert!(
                !ended_while_held,
                "healthy admitted read was cut off by an invented operation deadline: {result:?}"
            );
            delivered.unwrap();
            assert_eq!(result.unwrap(), expected);
        }
    });
}

#[test]
fn node_scoped_read_health_timeout_does_not_become_an_operation_deadline() {
    held_node_scoped_read(None, ReplyGate::Head);
}

#[test]
fn node_scoped_read_waits_for_a_healthy_owned_response_body() {
    held_node_scoped_read(None, ReplyGate::Body);
}

#[test]
fn node_scoped_read_preserves_explicit_deadline_before_response_head() {
    held_node_scoped_read(Some(Duration::from_millis(150)), ReplyGate::Head);
}

#[test]
fn node_scoped_read_preserves_explicit_deadline_during_body_materialization() {
    held_node_scoped_read(Some(Duration::from_millis(150)), ReplyGate::Body);
}

#[test]
fn node_scoped_read_preserves_remote_error_and_main_affinity() {
    held_node_scoped_read(None, ReplyGate::Rejected);
}

#[test]
fn node_scoped_expired_deadline_remains_definitely_not_sent() {
    let root = sinomo_testdir::TestDir::new("node-scoped-browser-expired");
    let data = SinomoDataDir::new(root.path());
    let _native = crate::native_test_fixture::NativeCliFixture::start(&data);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let listener = runtime
        .block_on(AsyncDaemonControlListener::bind("127.0.0.1:0"))
        .unwrap();
    let state = sinomo_runtime::cluster::read_cluster_state_without_kv(
        &sinomo_runtime::network::ClusterPaths::new(&data),
    )
    .unwrap();
    let mut browser = RemoteSifsBrowser::for_main_owner(
        data,
        listener.local_addr_string().unwrap(),
        state.local_node_id,
        runtime.handle(),
    );
    browser.direct_main_owner = false;
    browser.operation_deadline = Some(Instant::now() - Duration::from_millis(1));
    let request = ObjectRpcRequest::encode_body(
        DAEMON_DFS_BROWSER_LIST_ROUTE,
        sinomo_runtime::api::DAEMON_RPC_CATEGORY,
        &list_request(),
    )
    .unwrap();
    assert!(matches!(
        browser.rpc_response(request),
        Err(sinomo_runtime::api::DaemonApiError::NetworkDelivery {
            delivery_state: sinomo_runtime::network::NetworkDeliveryState::DefinitelyNotSent,
            ..
        })
    ));
    runtime.block_on(async {
        assert!(
            tokio::time::timeout(Duration::from_millis(30), listener.accept())
                .await
                .is_err()
        );
    });
}
