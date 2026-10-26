use super::*;
use crate::owned_mux::{OwnedRoutedStreamClient, accept_owned_routed_stream_carrier};

struct ShutdownOwnedSockets(Vec<std::net::TcpStream>);
impl Drop for ShutdownOwnedSockets {
    fn drop(&mut self) {
        for socket in &self.0 {
            let _ = socket.shutdown(std::net::Shutdown::Both);
        }
    }
}

async fn tcp_pair() -> (tokio::net::TcpStream, tokio::net::TcpStream) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (client, server) = tokio::join!(
        tokio::net::TcpStream::connect(listener.local_addr().unwrap()),
        listener.accept(),
    );
    (client.unwrap(), server.unwrap().0)
}

async fn encrypted_owned_retirement(cancel_response: bool) {
    let (source, incoming) = tcp_pair().await;
    let (destination, outgoing) = tcp_pair().await;
    let source = source.into_std().unwrap();
    let destination = destination.into_std().unwrap();
    let _cleanup = ShutdownOwnedSockets(vec![
        source.try_clone().unwrap(),
        destination.try_clone().unwrap(),
    ]);
    let source = tokio::net::TcpStream::from_std(source).unwrap();
    let destination = tokio::net::TcpStream::from_std(destination).unwrap();
    let relay = tokio::spawn(forward_routed_relay_pair(
        Box::new(incoming),
        Box::new(outgoing),
    ));
    let local = NodeId::from_legacy_named("owned-retirement-source").unwrap();
    let remote = NodeId::from_legacy_named("owned-retirement-destination").unwrap();
    let session = Id::generate();
    let binding = [7; 32];
    let secret = [19; 32];
    let endpoints = vec![Id::generate()];
    let source_carrier = PeerStream::new_owned_with_protection(
        encrypted_routed_session_chunk_stream(
            Box::new(source),
            secret,
            session.clone(),
            binding,
            true,
        )
        .unwrap(),
        local.clone(),
        remote.clone(),
        session.clone(),
        binding,
        RoutedSessionProtection::Aead,
        RouteTrace {
            nodes: vec![local.clone(), remote.clone()],
            endpoint_ids: endpoints.clone(),
        },
    )
    .unwrap();
    let destination_carrier = PeerStream::new_owned_with_protection(
        encrypted_routed_session_chunk_stream(
            Box::new(destination),
            secret,
            session.clone(),
            binding,
            false,
        )
        .unwrap(),
        remote.clone(),
        local.clone(),
        session,
        binding,
        RoutedSessionProtection::Aead,
        RouteTrace {
            nodes: vec![remote, local],
            endpoint_ids: endpoints,
        },
    )
    .unwrap();
    let client = Arc::new(OwnedRoutedStreamClient::new(source_carrier).unwrap());
    let mut acceptor = accept_owned_routed_stream_carrier(destination_carrier).unwrap();
    let mut stream = client.open().unwrap();
    stream.write_all(b"request").await.unwrap();
    stream.shutdown().await.unwrap();
    let mut accepted = acceptor.accept().await.unwrap().unwrap();
    let mut request = Vec::new();
    accepted.read_to_end(&mut request).await.unwrap();
    assert_eq!(request, b"request");
    let response = vec![0x6a; BODY_CHUNK_MAX * 2 + 17];
    let receive = async move {
        let mut received = Vec::new();
        if cancel_response {
            let mut first = [0; 1];
            stream.read_exact(&mut first).await?;
            received.extend(first);
        } else {
            stream.read_to_end(&mut received).await?;
        }
        // Cancellation/retirement releases this last client owner while the
        // server can still have reply DATA or credit records in flight.
        drop(stream);
        drop(client);
        Ok::<_, std::io::Error>(received)
    };
    let (sent, read) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(
            async {
                accepted.write_all(&response).await?;
                accepted.shutdown().await
            },
            receive
        )
    })
    .await
    .expect("bounded reply exchange and cancellation");
    let received = read.unwrap();
    if cancel_response {
        assert_eq!(received, vec![0x6a]);
        eprintln!("cancelled response sender result: {sent:?}");
    } else {
        sent.unwrap();
        assert_eq!(received, response);
    }
    drop(accepted);
    let closed = tokio::time::timeout(Duration::from_secs(3), acceptor.accept()).await;
    drop(acceptor);
    let relayed = tokio::time::timeout(Duration::from_secs(3), relay).await;
    let peer_closed = closed
        .as_ref()
        .map(|result| result.as_ref().map(Option::is_none));
    eprintln!(
        "retirement cancel_response={cancel_response} peer_closed={peer_closed:?} relay={relayed:?}"
    );
    assert!(
        relayed.unwrap().unwrap().is_ok(),
        "owned retirement must not reset the opaque TCP relay"
    );
    assert!(
        closed.unwrap().unwrap().is_none(),
        "authenticated carrier finish must reach the destination"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encrypted_owned_idle_retirement_closes_actual_tcp_relay_cleanly() {
    encrypted_owned_retirement(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_response_retirement_finishes_actual_tcp_relay() {
    encrypted_owned_retirement(true).await;
}

async fn reset_ingress_case(outgoing_reset: bool, opaque_input: bool, reverse_input: bool) {
    use std::os::fd::AsRawFd;
    struct Sink(std::sync::atomic::AtomicUsize);
    impl ConnectivityFailureSink for Sink {
        fn submit_edge_failure(
            &self,
            _: DirectConnectivityFailureEvidence,
        ) -> Result<(), NetworkError> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }
    let (mut source, incoming) = tcp_pair().await;
    let (mut destination, outgoing) = tcp_pair().await;
    let relay = tokio::spawn(forward_routed_relay_pair(
        Box::new(incoming),
        Box::new(outgoing),
    ));
    if opaque_input {
        source
            .write_all(b"already admitted opaque input")
            .await
            .unwrap();
        let mut received = vec![0; b"already admitted opaque input".len()];
        destination.read_exact(&mut received).await.unwrap();
        assert_eq!(received, b"already admitted opaque input");
    }
    if reverse_input {
        destination
            .write_all(b"reverse opaque input")
            .await
            .unwrap();
        let mut received = vec![0; b"reverse opaque input".len()];
        source.read_exact(&mut received).await.unwrap();
        assert_eq!(received, b"reverse opaque input");
    }
    let (reset, mut observer) = if outgoing_reset {
        (destination, source)
    } else {
        (source, destination)
    };
    let reset = reset.into_std().unwrap();
    let linger = libc::linger {
        l_onoff: 1,
        l_linger: 0,
    };
    // SAFETY: this is the fixture's live owned TCP descriptor and `linger`
    // is a valid socket-option value for the supplied length.
    assert_eq!(
        unsafe {
            libc::setsockopt(
                reset.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                (&linger as *const libc::linger).cast(),
                std::mem::size_of::<libc::linger>() as libc::socklen_t,
            )
        },
        0
    );
    drop(reset);
    let mut received = Vec::new();
    let read =
        tokio::time::timeout(Duration::from_secs(3), observer.read_to_end(&mut received)).await;
    drop(observer);
    let error = tokio::time::timeout(Duration::from_secs(3), relay)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(read.is_ok());
    assert!(received.is_empty());
    let local = NodeId::from_legacy_named("empty-relay-local").unwrap();
    let peer = NodeId::from_legacy_named("empty-relay-peer").unwrap();
    let endpoint = Id::generate();
    let expected = NetworkError::Io {
        operation: "forward encrypted routed-session stream".into(),
        detail: format!("{}: {}", error.operation, error.source),
    };
    let duplicate = RoutedRelayCopyError {
        peer_closed_before_opaque: error.peer_closed_before_opaque,
        edge: error.edge,
        operation: error.operation,
        source: std::io::Error::from_raw_os_error(error.source.raw_os_error().unwrap()),
    };
    let public: NetworkError =
        routed_relay_copy_error(duplicate, None, &local, &peer, &endpoint).into();
    assert_eq!(
        public, expected,
        "public client error must retain its representation"
    );
    assert_eq!(public.delivery_state(), expected.delivery_state());
    let sink = Sink(std::sync::atomic::AtomicUsize::new(0));
    let outcome = routed_handshake_ingress_outcome(
        Err(routed_relay_copy_error(error, Some(&sink), &local, &peer, &endpoint).into()),
        &local,
        &peer,
    );
    assert_eq!(
        sink.0.load(std::sync::atomic::Ordering::SeqCst),
        usize::from(outgoing_reset)
    );
    if outgoing_reset || opaque_input || reverse_input {
        assert!(
            matches!(outcome, Err(NetworkError::Io { .. })),
            "transfer or outgoing-edge errors remain errors"
        );
    } else {
        assert!(
            matches!(outcome, Ok(None)),
            "no opaque application traffic was admitted"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incoming_reset_before_opaque_bytes_is_a_pre_application_close() {
    reset_ingress_case(false, false, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incoming_reset_after_opaque_bytes_remains_an_error() {
    reset_ingress_case(false, true, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outgoing_reset_before_opaque_bytes_remains_edge_failure() {
    reset_ingress_case(true, false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incoming_reset_after_reverse_opaque_bytes_remains_an_error() {
    reset_ingress_case(false, false, true).await;
}
