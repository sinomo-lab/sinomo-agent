//! Physical TCP carrier for a local multiplexed object-RPC channel.
//!
//! The fixed handshake only selects this protocol on the daemon control
//! listener. Control frames use the ordinary Sinomo object-frame codec; Data
//! frames use the fixed private SCDF header and an ownership-bearing payload.

use crate::channel_wire::{
    ObjectRpcChannelReceiveBufferPool, ObjectRpcChannelTransportFrame,
    read_object_rpc_channel_transport_frame_async, write_object_rpc_channel_transport_frame_async,
    write_object_rpc_channel_transport_frame_async_timed,
};
use crate::daemon_auth::{LocalChannelAuthIdentity, LocalChannelProofRole};
use crate::{
    AsyncDaemonControlStream, DEFAULT_NETWORK_TIMEOUT, DaemonRpcTimeouts, NetworkDeliveryState,
    NetworkError, NetworkTimeoutPhase, ObjectFrameIoOptions, ObjectRpcChannelClient,
    ObjectRpcChannelConfig, ObjectRpcChannelEndpoint, ObjectRpcChannelIngress,
    ObjectRpcChannelRole, ObjectRpcChannelServer, network_timeline_flag,
};
use ring::rand::{SecureRandom, SystemRandom};
use sinomo_local::SinomoDataDir;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Current loopback object-RPC channel handshake version.
pub const LOCAL_OBJECT_RPC_CHANNEL_PROTOCOL_VERSION: u16 = 4;

const LOCAL_OBJECT_RPC_CHANNEL_CLIENT_MAGIC: [u8; 4] = *b"SXC4";
const LOCAL_OBJECT_RPC_CHANNEL_SERVER_MAGIC: [u8; 4] = *b"SXS4";
const LOCAL_OBJECT_RPC_CHANNEL_CLIENT_PROOF_MAGIC: [u8; 4] = *b"SXP4";
const LOCAL_OBJECT_RPC_CHANNEL_SERVER_ACK_MAGIC: [u8; 4] = *b"SXA4";
const LOCAL_OBJECT_RPC_CHANNEL_NEGOTIATION_BYTES: usize = 8;
// Retained as an internal alias while raw old-daemon compatibility tests
// construct only the fixed negotiation prefix.
const LOCAL_OBJECT_RPC_CHANNEL_HANDSHAKE_BYTES: usize = LOCAL_OBJECT_RPC_CHANNEL_NEGOTIATION_BYTES;
const LOCAL_OBJECT_RPC_CHANNEL_NONCE_BYTES: usize = 32;
const LOCAL_OBJECT_RPC_CHANNEL_IDENTITY_BINDING_BYTES: usize = 32;
const LOCAL_OBJECT_RPC_CHANNEL_SIGNATURE_BYTES: usize = 64;
const LOCAL_OBJECT_RPC_CHANNEL_CLIENT_HELLO_BYTES: usize =
    LOCAL_OBJECT_RPC_CHANNEL_NEGOTIATION_BYTES
        + LOCAL_OBJECT_RPC_CHANNEL_NONCE_BYTES
        + LOCAL_OBJECT_RPC_CHANNEL_IDENTITY_BINDING_BYTES;
const LOCAL_OBJECT_RPC_CHANNEL_SERVER_PROOF_BYTES: usize =
    LOCAL_OBJECT_RPC_CHANNEL_NEGOTIATION_BYTES
        + LOCAL_OBJECT_RPC_CHANNEL_NONCE_BYTES
        + LOCAL_OBJECT_RPC_CHANNEL_IDENTITY_BINDING_BYTES
        + LOCAL_OBJECT_RPC_CHANNEL_SIGNATURE_BYTES;
const LOCAL_OBJECT_RPC_CHANNEL_TRANSCRIPT_BYTES: usize = LOCAL_OBJECT_RPC_CHANNEL_CLIENT_HELLO_BYTES
    + LOCAL_OBJECT_RPC_CHANNEL_NEGOTIATION_BYTES
    + LOCAL_OBJECT_RPC_CHANNEL_NONCE_BYTES
    + LOCAL_OBJECT_RPC_CHANNEL_IDENTITY_BINDING_BYTES;
const LOCAL_OBJECT_RPC_CHANNEL_CLIENT_TRANSCRIPT_BYTES: usize =
    LOCAL_OBJECT_RPC_CHANNEL_TRANSCRIPT_BYTES + LOCAL_OBJECT_RPC_CHANNEL_SIGNATURE_BYTES;
const LOCAL_OBJECT_RPC_CHANNEL_CLIENT_PROOF_BYTES: usize =
    LOCAL_OBJECT_RPC_CHANNEL_CLIENT_PROOF_MAGIC.len() + LOCAL_OBJECT_RPC_CHANNEL_SIGNATURE_BYTES;
const LOCAL_OBJECT_RPC_CHANNEL_HANDSHAKE_TIMEOUT: Duration = Duration::from_millis(250);
const LOCAL_OBJECT_RPC_CHANNEL_ACCEPTED: u16 = 1;
const LOCAL_OBJECT_RPC_CHANNEL_REJECTED: u16 = 0;

/// Returns whether a non-consuming listener peek can still identify the local
/// object-RPC channel handshake.
pub fn local_object_rpc_channel_wire_prefix_matches(prefix: &[u8]) -> bool {
    !prefix.is_empty()
        && prefix[..prefix
            .len()
            .min(LOCAL_OBJECT_RPC_CHANNEL_CLIENT_MAGIC.len())]
            == LOCAL_OBJECT_RPC_CHANNEL_CLIENT_MAGIC[..prefix
                .len()
                .min(LOCAL_OBJECT_RPC_CHANNEL_CLIENT_MAGIC.len())]
}

/// Explicit lifecycle handle for a physical channel carrier.
///
/// The carrier supervisor also owns an ingress clone, so dropping this handle
/// does not accidentally close an accepted server. Pools should call
/// [`Self::close`] before evicting a live client connection.
pub struct LocalObjectRpcChannelTransport {
    ingress: ObjectRpcChannelIngress,
    carrier: tokio::task::AbortHandle,
}

impl LocalObjectRpcChannelTransport {
    pub fn close(&self, detail: impl Into<String>) {
        self.ingress.close(detail);
        self.carrier.abort();
    }

    #[cfg(test)]
    fn abort_carrier_for_test(&self) {
        self.carrier.abort();
    }
}

struct CloseIngressOnCarrierDrop {
    ingress: ObjectRpcChannelIngress,
}

impl Drop for CloseIngressOnCarrierDrop {
    fn drop(&mut self) {
        // Tokio runtime shutdown aborts spawned carrier tasks. Keeping this
        // guard captured by the task future makes that path observable to pool
        // users even when the future was never polled.
        self.ingress.close("physical carrier task stopped");
    }
}

pub struct ConnectedLocalObjectRpcChannel {
    pub client: ObjectRpcChannelClient,
    pub transport: LocalObjectRpcChannelTransport,
}

pub struct AcceptedLocalObjectRpcChannel {
    pub server: ObjectRpcChannelServer,
    pub transport: LocalObjectRpcChannelTransport,
}

/// Negotiates one persistent object-RPC channel to a local daemon.
///
/// `Ok(None)` means that the peer returned a valid `SXS4` protocol rejection.
/// TCP, timeout, EOF, and malformed-handshake failures remain errors so callers
/// can fall back for the current call without negative-caching a transient
/// failure as protocol incompatibility.
pub async fn connect_local_object_rpc_channel_async(
    data_dir: &SinomoDataDir,
    address: &str,
    timeouts: DaemonRpcTimeouts,
) -> Result<Option<ConnectedLocalObjectRpcChannel>, NetworkError> {
    if !crate::is_loopback_connect_address(address) {
        return Ok(None);
    }
    let identity = LocalChannelAuthIdentity::from_data_dir(data_dir)?;
    let mut stream = AsyncDaemonControlStream::connect_with_options(address, timeouts)
        .await?
        .into_inner();
    let handshake_timeout = timeouts
        .idle_timeout
        .map(|timeout| timeout.min(LOCAL_OBJECT_RPC_CHANNEL_HANDSHAKE_TIMEOUT))
        .unwrap_or(LOCAL_OBJECT_RPC_CHANNEL_HANDSHAKE_TIMEOUT);
    let client_nonce = fresh_local_channel_nonce()?;
    let request = client_hello(&identity, client_nonce);
    write_handshake_bytes(&mut stream, &request, handshake_timeout).await?;
    let mut response_prefix = [0_u8; LOCAL_OBJECT_RPC_CHANNEL_NEGOTIATION_BYTES];
    read_handshake_bytes(&mut stream, &mut response_prefix, handshake_timeout).await?;
    match parse_server_handshake(&response_prefix)? {
        LocalObjectRpcChannelServerHandshake::Accepted => {}
        LocalObjectRpcChannelServerHandshake::Rejected => {
            let _ = stream.shutdown().await;
            return Ok(None);
        }
    }
    let mut response_extension = [0_u8;
        LOCAL_OBJECT_RPC_CHANNEL_SERVER_PROOF_BYTES - LOCAL_OBJECT_RPC_CHANNEL_NEGOTIATION_BYTES];
    read_handshake_bytes(&mut stream, &mut response_extension, handshake_timeout).await?;
    let server_nonce: [u8; LOCAL_OBJECT_RPC_CHANNEL_NONCE_BYTES] = response_extension
        [..LOCAL_OBJECT_RPC_CHANNEL_NONCE_BYTES]
        .try_into()
        .expect("server nonce slice");
    let binding_start = LOCAL_OBJECT_RPC_CHANNEL_NONCE_BYTES;
    let signature_start = binding_start + LOCAL_OBJECT_RPC_CHANNEL_IDENTITY_BINDING_BYTES;
    let server_binding = &response_extension[binding_start..signature_start];
    if server_binding != identity.binding() {
        return Err(NetworkError::AuthDenied);
    }
    let server_signature = &response_extension[signature_start..];
    let transcript =
        local_channel_server_transcript(&request, &response_prefix, server_nonce, server_binding);
    identity.verify(LocalChannelProofRole::Server, &transcript, server_signature)?;
    let client_transcript = local_channel_client_transcript(&transcript, server_signature);
    let client_signature = identity.sign(LocalChannelProofRole::Client, &client_transcript);
    let client_proof = client_proof(&client_signature)?;
    write_handshake_bytes(&mut stream, &client_proof, handshake_timeout).await?;
    let mut ack = [0_u8; LOCAL_OBJECT_RPC_CHANNEL_SERVER_ACK_MAGIC.len()];
    read_handshake_bytes(&mut stream, &mut ack, handshake_timeout).await?;
    if ack != LOCAL_OBJECT_RPC_CHANNEL_SERVER_ACK_MAGIC {
        return Err(NetworkError::AuthDenied);
    }

    let endpoint = ObjectRpcChannelEndpoint::new(
        ObjectRpcChannelRole::Dialer,
        ObjectRpcChannelConfig::default(),
    )?;
    let ObjectRpcChannelEndpoint {
        client,
        server: _,
        ingress,
        egress,
    } = endpoint;
    let transport = start_tcp_carrier(
        stream,
        ingress,
        egress,
        timeouts.idle_timeout.unwrap_or(DEFAULT_NETWORK_TIMEOUT),
    );
    Ok(Some(ConnectedLocalObjectRpcChannel { client, transport }))
}

/// Accepts a previously detected local object-RPC channel handshake.
///
/// The control listener must call this only after a non-consuming prefix
/// match. Non-loopback peers are rejected before any handshake bytes are
/// consumed.
pub async fn accept_local_object_rpc_channel_async(
    stream: AsyncDaemonControlStream,
    data_dir: &SinomoDataDir,
) -> Result<AcceptedLocalObjectRpcChannel, NetworkError> {
    let peer = stream.peer_addr()?;
    ensure_loopback_peer(peer)?;
    let identity = LocalChannelAuthIdentity::from_data_dir(data_dir)?;
    let mut stream = stream.into_inner();
    let mut request_prefix = [0_u8; LOCAL_OBJECT_RPC_CHANNEL_NEGOTIATION_BYTES];
    read_handshake_bytes(
        &mut stream,
        &mut request_prefix,
        LOCAL_OBJECT_RPC_CHANNEL_HANDSHAKE_TIMEOUT,
    )
    .await?;
    let negotiated = negotiate_client_handshake(&request_prefix);
    let response = server_handshake(negotiated);
    let Some(_version) = negotiated else {
        write_handshake_bytes(
            &mut stream,
            &response,
            LOCAL_OBJECT_RPC_CHANNEL_HANDSHAKE_TIMEOUT,
        )
        .await?;
        let _ = stream.shutdown().await;
        return Err(NetworkError::Io {
            operation: "accept local object RPC channel".to_string(),
            detail: "peer requested an unsupported channel protocol version".to_string(),
        });
    };
    let mut request_extension = [0_u8;
        LOCAL_OBJECT_RPC_CHANNEL_CLIENT_HELLO_BYTES - LOCAL_OBJECT_RPC_CHANNEL_NEGOTIATION_BYTES];
    read_handshake_bytes(
        &mut stream,
        &mut request_extension,
        LOCAL_OBJECT_RPC_CHANNEL_HANDSHAKE_TIMEOUT,
    )
    .await?;
    let client_binding = &request_extension[LOCAL_OBJECT_RPC_CHANNEL_NONCE_BYTES..];
    if client_binding != identity.binding() {
        return Err(NetworkError::AuthDenied);
    }
    let mut request = [0_u8; LOCAL_OBJECT_RPC_CHANNEL_CLIENT_HELLO_BYTES];
    request[..LOCAL_OBJECT_RPC_CHANNEL_NEGOTIATION_BYTES].copy_from_slice(&request_prefix);
    request[LOCAL_OBJECT_RPC_CHANNEL_NEGOTIATION_BYTES..].copy_from_slice(&request_extension);
    let server_nonce = fresh_local_channel_nonce()?;
    let transcript =
        local_channel_server_transcript(&request, &response, server_nonce, &identity.binding());
    let server_signature = identity.sign(LocalChannelProofRole::Server, &transcript);
    let response = server_proof(
        &response,
        server_nonce,
        &identity.binding(),
        &server_signature,
    )?;
    write_handshake_bytes(
        &mut stream,
        &response,
        LOCAL_OBJECT_RPC_CHANNEL_HANDSHAKE_TIMEOUT,
    )
    .await?;
    let mut proof = [0_u8; LOCAL_OBJECT_RPC_CHANNEL_CLIENT_PROOF_BYTES];
    read_handshake_bytes(
        &mut stream,
        &mut proof,
        LOCAL_OBJECT_RPC_CHANNEL_HANDSHAKE_TIMEOUT,
    )
    .await?;
    if proof[..LOCAL_OBJECT_RPC_CHANNEL_CLIENT_PROOF_MAGIC.len()]
        != LOCAL_OBJECT_RPC_CHANNEL_CLIENT_PROOF_MAGIC
    {
        return Err(NetworkError::AuthDenied);
    }
    let client_transcript = local_channel_client_transcript(&transcript, &server_signature);
    identity.verify(
        LocalChannelProofRole::Client,
        &client_transcript,
        &proof[LOCAL_OBJECT_RPC_CHANNEL_CLIENT_PROOF_MAGIC.len()..],
    )?;
    write_handshake_bytes(
        &mut stream,
        &LOCAL_OBJECT_RPC_CHANNEL_SERVER_ACK_MAGIC,
        LOCAL_OBJECT_RPC_CHANNEL_HANDSHAKE_TIMEOUT,
    )
    .await?;

    let endpoint = ObjectRpcChannelEndpoint::new(
        ObjectRpcChannelRole::Acceptor,
        ObjectRpcChannelConfig::default(),
    )?;
    let ObjectRpcChannelEndpoint {
        client: _,
        server,
        ingress,
        egress,
    } = endpoint;
    let transport = start_tcp_carrier(stream, ingress, egress, DEFAULT_NETWORK_TIMEOUT);
    Ok(AcceptedLocalObjectRpcChannel { server, transport })
}

fn ensure_loopback_peer(peer: SocketAddr) -> Result<(), NetworkError> {
    if peer.ip().is_loopback() {
        Ok(())
    } else {
        Err(NetworkError::AuthDenied)
    }
}

fn fresh_local_channel_nonce() -> Result<[u8; LOCAL_OBJECT_RPC_CHANNEL_NONCE_BYTES], NetworkError> {
    let mut nonce = [0_u8; LOCAL_OBJECT_RPC_CHANNEL_NONCE_BYTES];
    SystemRandom::new()
        .fill(&mut nonce)
        .map_err(|_| NetworkError::Io {
            operation: "generate local object RPC channel nonce".to_string(),
            detail: "secure random source unavailable".to_string(),
        })?;
    Ok(nonce)
}

fn client_hello(
    identity: &LocalChannelAuthIdentity,
    nonce: [u8; LOCAL_OBJECT_RPC_CHANNEL_NONCE_BYTES],
) -> [u8; LOCAL_OBJECT_RPC_CHANNEL_CLIENT_HELLO_BYTES] {
    let mut hello = [0_u8; LOCAL_OBJECT_RPC_CHANNEL_CLIENT_HELLO_BYTES];
    hello[..LOCAL_OBJECT_RPC_CHANNEL_NEGOTIATION_BYTES].copy_from_slice(&client_handshake());
    let nonce_end =
        LOCAL_OBJECT_RPC_CHANNEL_NEGOTIATION_BYTES + LOCAL_OBJECT_RPC_CHANNEL_NONCE_BYTES;
    hello[LOCAL_OBJECT_RPC_CHANNEL_NEGOTIATION_BYTES..nonce_end].copy_from_slice(&nonce);
    hello[nonce_end..].copy_from_slice(&identity.binding());
    hello
}

fn local_channel_server_transcript(
    client_hello: &[u8; LOCAL_OBJECT_RPC_CHANNEL_CLIENT_HELLO_BYTES],
    server_prefix: &[u8; LOCAL_OBJECT_RPC_CHANNEL_NEGOTIATION_BYTES],
    server_nonce: [u8; LOCAL_OBJECT_RPC_CHANNEL_NONCE_BYTES],
    server_binding: &[u8],
) -> [u8; LOCAL_OBJECT_RPC_CHANNEL_TRANSCRIPT_BYTES] {
    debug_assert_eq!(
        server_binding.len(),
        LOCAL_OBJECT_RPC_CHANNEL_IDENTITY_BINDING_BYTES
    );
    let mut transcript = [0_u8; LOCAL_OBJECT_RPC_CHANNEL_TRANSCRIPT_BYTES];
    let mut offset = 0;
    transcript[offset..offset + client_hello.len()].copy_from_slice(client_hello);
    offset += client_hello.len();
    transcript[offset..offset + server_prefix.len()].copy_from_slice(server_prefix);
    offset += server_prefix.len();
    transcript[offset..offset + server_nonce.len()].copy_from_slice(&server_nonce);
    offset += server_nonce.len();
    transcript[offset..].copy_from_slice(server_binding);
    transcript
}

fn local_channel_client_transcript(
    server_transcript: &[u8; LOCAL_OBJECT_RPC_CHANNEL_TRANSCRIPT_BYTES],
    server_signature: &[u8],
) -> [u8; LOCAL_OBJECT_RPC_CHANNEL_CLIENT_TRANSCRIPT_BYTES] {
    debug_assert_eq!(
        server_signature.len(),
        LOCAL_OBJECT_RPC_CHANNEL_SIGNATURE_BYTES
    );
    let mut transcript = [0_u8; LOCAL_OBJECT_RPC_CHANNEL_CLIENT_TRANSCRIPT_BYTES];
    transcript[..server_transcript.len()].copy_from_slice(server_transcript);
    transcript[server_transcript.len()..].copy_from_slice(server_signature);
    transcript
}

fn server_proof(
    prefix: &[u8; LOCAL_OBJECT_RPC_CHANNEL_NEGOTIATION_BYTES],
    nonce: [u8; LOCAL_OBJECT_RPC_CHANNEL_NONCE_BYTES],
    binding: &[u8],
    signature: &[u8],
) -> Result<[u8; LOCAL_OBJECT_RPC_CHANNEL_SERVER_PROOF_BYTES], NetworkError> {
    if binding.len() != LOCAL_OBJECT_RPC_CHANNEL_IDENTITY_BINDING_BYTES
        || signature.len() != LOCAL_OBJECT_RPC_CHANNEL_SIGNATURE_BYTES
    {
        return Err(NetworkError::Serialization {
            detail: "invalid local object RPC channel server proof width".to_string(),
        });
    }
    let mut proof = [0_u8; LOCAL_OBJECT_RPC_CHANNEL_SERVER_PROOF_BYTES];
    let mut offset = 0;
    proof[offset..offset + prefix.len()].copy_from_slice(prefix);
    offset += prefix.len();
    proof[offset..offset + nonce.len()].copy_from_slice(&nonce);
    offset += nonce.len();
    proof[offset..offset + binding.len()].copy_from_slice(binding);
    offset += binding.len();
    proof[offset..].copy_from_slice(signature);
    Ok(proof)
}

fn client_proof(
    signature: &[u8],
) -> Result<[u8; LOCAL_OBJECT_RPC_CHANNEL_CLIENT_PROOF_BYTES], NetworkError> {
    if signature.len() != LOCAL_OBJECT_RPC_CHANNEL_SIGNATURE_BYTES {
        return Err(NetworkError::Serialization {
            detail: "invalid local object RPC channel client proof width".to_string(),
        });
    }
    let mut proof = [0_u8; LOCAL_OBJECT_RPC_CHANNEL_CLIENT_PROOF_BYTES];
    proof[..LOCAL_OBJECT_RPC_CHANNEL_CLIENT_PROOF_MAGIC.len()]
        .copy_from_slice(&LOCAL_OBJECT_RPC_CHANNEL_CLIENT_PROOF_MAGIC);
    proof[LOCAL_OBJECT_RPC_CHANNEL_CLIENT_PROOF_MAGIC.len()..].copy_from_slice(signature);
    Ok(proof)
}

fn client_handshake() -> [u8; LOCAL_OBJECT_RPC_CHANNEL_HANDSHAKE_BYTES] {
    let mut handshake = [0_u8; LOCAL_OBJECT_RPC_CHANNEL_HANDSHAKE_BYTES];
    handshake[..4].copy_from_slice(&LOCAL_OBJECT_RPC_CHANNEL_CLIENT_MAGIC);
    handshake[4..6].copy_from_slice(&LOCAL_OBJECT_RPC_CHANNEL_PROTOCOL_VERSION.to_be_bytes());
    handshake[6..8].copy_from_slice(&LOCAL_OBJECT_RPC_CHANNEL_PROTOCOL_VERSION.to_be_bytes());
    handshake
}

fn negotiate_client_handshake(
    handshake: &[u8; LOCAL_OBJECT_RPC_CHANNEL_HANDSHAKE_BYTES],
) -> Option<u16> {
    if handshake[..4] != LOCAL_OBJECT_RPC_CHANNEL_CLIENT_MAGIC {
        return None;
    }
    let minimum = u16::from_be_bytes([handshake[4], handshake[5]]);
    let maximum = u16::from_be_bytes([handshake[6], handshake[7]]);
    (minimum <= LOCAL_OBJECT_RPC_CHANNEL_PROTOCOL_VERSION
        && maximum >= LOCAL_OBJECT_RPC_CHANNEL_PROTOCOL_VERSION)
        .then_some(LOCAL_OBJECT_RPC_CHANNEL_PROTOCOL_VERSION)
}

fn server_handshake(negotiated: Option<u16>) -> [u8; LOCAL_OBJECT_RPC_CHANNEL_HANDSHAKE_BYTES] {
    let mut handshake = [0_u8; LOCAL_OBJECT_RPC_CHANNEL_HANDSHAKE_BYTES];
    handshake[..4].copy_from_slice(&LOCAL_OBJECT_RPC_CHANNEL_SERVER_MAGIC);
    handshake[4..6].copy_from_slice(&negotiated.unwrap_or_default().to_be_bytes());
    handshake[6..8].copy_from_slice(
        &(if negotiated.is_some() {
            LOCAL_OBJECT_RPC_CHANNEL_ACCEPTED
        } else {
            LOCAL_OBJECT_RPC_CHANNEL_REJECTED
        })
        .to_be_bytes(),
    );
    handshake
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalObjectRpcChannelServerHandshake {
    Accepted,
    Rejected,
}

fn parse_server_handshake(
    handshake: &[u8; LOCAL_OBJECT_RPC_CHANNEL_HANDSHAKE_BYTES],
) -> Result<LocalObjectRpcChannelServerHandshake, NetworkError> {
    if handshake[..4] != LOCAL_OBJECT_RPC_CHANNEL_SERVER_MAGIC {
        return Err(NetworkError::Serialization {
            detail: "local object RPC channel server returned invalid handshake magic".to_string(),
        });
    }
    let version = u16::from_be_bytes([handshake[4], handshake[5]]);
    let status = u16::from_be_bytes([handshake[6], handshake[7]]);
    match (version, status) {
        (LOCAL_OBJECT_RPC_CHANNEL_PROTOCOL_VERSION, LOCAL_OBJECT_RPC_CHANNEL_ACCEPTED) => {
            Ok(LocalObjectRpcChannelServerHandshake::Accepted)
        }
        (0, LOCAL_OBJECT_RPC_CHANNEL_REJECTED) => {
            Ok(LocalObjectRpcChannelServerHandshake::Rejected)
        }
        (_, LOCAL_OBJECT_RPC_CHANNEL_ACCEPTED) => Err(NetworkError::Serialization {
            detail: format!(
                "local object RPC channel server accepted unsupported protocol version {version}"
            ),
        }),
        (_, LOCAL_OBJECT_RPC_CHANNEL_REJECTED) => Err(NetworkError::Serialization {
            detail: format!(
                "local object RPC channel server rejected negotiation with selected version {version}"
            ),
        }),
        _ => Err(NetworkError::Serialization {
            detail: format!(
                "local object RPC channel server returned invalid handshake status {status}"
            ),
        }),
    }
}

async fn read_handshake_bytes<R>(
    reader: &mut R,
    output: &mut [u8],
    timeout: Duration,
) -> Result<(), NetworkError>
where
    R: AsyncRead + Unpin,
{
    tokio::time::timeout(timeout, reader.read_exact(output))
        .await
        .map_err(|_| {
            NetworkError::timeout(
                "read local object RPC channel handshake",
                NetworkTimeoutPhase::Idle,
                timeout,
                NetworkDeliveryState::DefinitelyNotSent,
            )
        })?
        .map(|_| ())
        .map_err(|source| NetworkError::Io {
            operation: "read local object RPC channel handshake".to_string(),
            detail: source.to_string(),
        })
}

async fn write_handshake_bytes<W>(
    writer: &mut W,
    handshake: &[u8],
    timeout: Duration,
) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin,
{
    tokio::time::timeout(timeout, writer.write_all(handshake))
        .await
        .map_err(|_| {
            NetworkError::timeout(
                "write local object RPC channel handshake",
                NetworkTimeoutPhase::Write,
                timeout,
                NetworkDeliveryState::DefinitelyNotSent,
            )
        })?
        .map_err(|source| NetworkError::Io {
            operation: "write local object RPC channel handshake".to_string(),
            detail: source.to_string(),
        })
}

fn start_tcp_carrier(
    stream: tokio::net::TcpStream,
    ingress: ObjectRpcChannelIngress,
    egress: crate::ObjectRpcChannelEgress,
    no_progress_timeout: Duration,
) -> LocalObjectRpcChannelTransport {
    let (reader, writer) = stream.into_split();
    start_carrier_halves(reader, writer, ingress, egress, no_progress_timeout)
}

fn start_carrier_halves<R, W>(
    mut reader: R,
    mut writer: W,
    ingress: ObjectRpcChannelIngress,
    mut egress: crate::ObjectRpcChannelEgress,
    no_progress_timeout: Duration,
) -> LocalObjectRpcChannelTransport
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let transport_ingress = ingress.clone();
    let close_on_drop = CloseIngressOnCarrierDrop {
        ingress: ingress.clone(),
    };
    let carrier = tokio::spawn(async move {
        let _close_on_drop = close_on_drop;
        let read_ingress = ingress.clone();
        let read = async move {
            let receive_pool = ObjectRpcChannelReceiveBufferPool::default_for_carrier();
            loop {
                let frame = read_object_rpc_channel_transport_frame_async(
                    &mut reader,
                    ObjectFrameIoOptions::idle_timeout(no_progress_timeout),
                    &receive_pool,
                    |header| read_ingress.validate_data_header(header),
                )
                .await?;
                // This synchronous ingress boundary is load-bearing: a stalled
                // application call must never park the physical socket reader.
                match frame {
                    ObjectRpcChannelTransportFrame::Control(frame) => {
                        read_ingress.record_frame_read_timeline(
                            &frame,
                            None,
                            network_timeline_flag::LOCAL_CARRIER,
                        );
                        read_ingress.receive_control(frame)?;
                    }
                    ObjectRpcChannelTransportFrame::Data(frame) => {
                        read_ingress.receive_data(frame)?;
                    }
                }
            }
            #[allow(unreachable_code)]
            Ok::<(), NetworkError>(())
        };
        let write = async move {
            while let Some(frame) = egress.next_transport_frame().await {
                let mut timeline = egress.pending_write_timeline();
                if let Some(timeline) = timeline.as_mut() {
                    timeline.flags |= network_timeline_flag::LOCAL_CARRIER;
                }
                let (result, timing) = if timeline.is_some() {
                    write_object_rpc_channel_transport_frame_async_timed(
                        &mut writer,
                        &frame,
                        ObjectFrameIoOptions::idle_timeout(no_progress_timeout),
                    )
                    .await
                } else {
                    (
                        write_object_rpc_channel_transport_frame_async(
                            &mut writer,
                            &frame,
                            ObjectFrameIoOptions::idle_timeout(no_progress_timeout),
                        )
                        .await,
                        None,
                    )
                };
                if let Some(timeline) = timeline {
                    timeline.record_frame_write_timing(timing, result.is_ok());
                }
                egress.complete_frame_write(result.is_ok());
                result?;
            }
            Ok::<(), NetworkError>(())
        };
        let result = tokio::select! {
            result = read => result,
            result = write => result,
        };
        let detail = result
            .err()
            .map(|error| error.to_string())
            .unwrap_or_else(|| "physical carrier stopped".to_string());
        ingress.close(detail);
    });
    LocalObjectRpcChannelTransport {
        ingress: transport_ingress,
        carrier: carrier.abort_handle(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        MAX_OBJECT_RPC_CHANNEL_WIRE_FRAME_BYTES, ObjectRpcBodySource, ObjectRpcRequest,
        ObjectRpcSessionCall, ObjectRpcSessionResponse,
    };
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    fn test_data_dir(label: &str) -> (sinomo_testdir::TestDir, SinomoDataDir) {
        let root = sinomo_testdir::TestDir::new(label);
        let data_dir = SinomoDataDir::new(root.path());
        data_dir.ensure_root_layout().expect("test data dir");
        (root, data_dir)
    }

    #[test]
    fn handshake_prefix_is_incremental_and_disjoint_from_other_framing() {
        assert!(!local_object_rpc_channel_wire_prefix_matches(b""));
        for length in 1..=LOCAL_OBJECT_RPC_CHANNEL_CLIENT_MAGIC.len() {
            assert!(local_object_rpc_channel_wire_prefix_matches(
                &LOCAL_OBJECT_RPC_CHANNEL_CLIENT_MAGIC[..length]
            ));
        }
        assert!(!local_object_rpc_channel_wire_prefix_matches(b"SXF2"));
        assert!(!local_object_rpc_channel_wire_prefix_matches(b"SOF2"));
    }

    #[test]
    fn handshake_negotiates_only_the_supported_version() {
        let request = client_handshake();
        assert_eq!(
            negotiate_client_handshake(&request),
            Some(LOCAL_OBJECT_RPC_CHANNEL_PROTOCOL_VERSION)
        );
        assert_eq!(
            parse_server_handshake(&server_handshake(Some(
                LOCAL_OBJECT_RPC_CHANNEL_PROTOCOL_VERSION
            )))
            .expect("accepted server handshake"),
            LocalObjectRpcChannelServerHandshake::Accepted
        );

        let mut unsupported = request;
        unsupported[4..6].copy_from_slice(&1_u16.to_be_bytes());
        unsupported[6..8].copy_from_slice(&1_u16.to_be_bytes());
        assert_eq!(negotiate_client_handshake(&unsupported), None);
        assert_eq!(
            parse_server_handshake(&server_handshake(None)).expect("rejected server handshake"),
            LocalObjectRpcChannelServerHandshake::Rejected
        );
    }

    #[test]
    fn malformed_server_handshakes_are_not_protocol_rejections() {
        let mut wrong_magic = server_handshake(None);
        wrong_magic[..4].copy_from_slice(b"BAD!");
        assert!(matches!(
            parse_server_handshake(&wrong_magic),
            Err(NetworkError::Serialization { .. })
        ));

        let mut rejected_with_version = server_handshake(None);
        rejected_with_version[4..6]
            .copy_from_slice(&LOCAL_OBJECT_RPC_CHANNEL_PROTOCOL_VERSION.to_be_bytes());
        assert!(matches!(
            parse_server_handshake(&rejected_with_version),
            Err(NetworkError::Serialization { .. })
        ));

        let mut accepted_wrong_version =
            server_handshake(Some(LOCAL_OBJECT_RPC_CHANNEL_PROTOCOL_VERSION));
        accepted_wrong_version[4..6].copy_from_slice(&1_u16.to_be_bytes());
        assert!(matches!(
            parse_server_handshake(&accepted_wrong_version),
            Err(NetworkError::Serialization { .. })
        ));
    }

    #[test]
    fn accepted_carrier_requires_a_loopback_peer() {
        assert!(ensure_loopback_peer(SocketAddr::from(([127, 0, 0, 1], 1))).is_ok());
        assert!(ensure_loopback_peer(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 1)).is_ok());
        assert_eq!(
            ensure_loopback_peer(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)), 1)),
            Err(NetworkError::AuthDenied)
        );
    }

    #[test]
    fn local_channel_proof_rejects_replay_under_a_fresh_server_challenge() {
        let (_root, data_dir) = test_data_dir("sinomo-local-channel-replay");
        let identity = LocalChannelAuthIdentity::from_data_dir(&data_dir).expect("identity");
        let hello = client_hello(&identity, [0x11; LOCAL_OBJECT_RPC_CHANNEL_NONCE_BYTES]);
        let prefix = server_handshake(Some(LOCAL_OBJECT_RPC_CHANNEL_PROTOCOL_VERSION));
        let first = local_channel_server_transcript(
            &hello,
            &prefix,
            [0x21; LOCAL_OBJECT_RPC_CHANNEL_NONCE_BYTES],
            &identity.binding(),
        );
        let first_server_signature = identity.sign(LocalChannelProofRole::Server, &first);
        let first_client_transcript =
            local_channel_client_transcript(&first, &first_server_signature);
        let captured = identity.sign(LocalChannelProofRole::Client, &first_client_transcript);
        identity
            .verify(
                LocalChannelProofRole::Client,
                &first_client_transcript,
                &captured,
            )
            .expect("original proof");

        let fresh = local_channel_server_transcript(
            &hello,
            &prefix,
            [0x22; LOCAL_OBJECT_RPC_CHANNEL_NONCE_BYTES],
            &identity.binding(),
        );
        let fresh_server_signature = identity.sign(LocalChannelProofRole::Server, &fresh);
        let fresh_client_transcript =
            local_channel_client_transcript(&fresh, &fresh_server_signature);
        assert_eq!(
            identity.verify(
                LocalChannelProofRole::Client,
                &fresh_client_transcript,
                &captured,
            ),
            Err(NetworkError::AuthDenied)
        );
    }

    #[test]
    fn local_channel_proof_roles_prevent_reflection() {
        let (_root, data_dir) = test_data_dir("sinomo-local-channel-reflection");
        let identity = LocalChannelAuthIdentity::from_data_dir(&data_dir).expect("identity");
        let transcript = b"same transcript bytes";
        let server_signature = identity.sign(LocalChannelProofRole::Server, transcript);

        assert_eq!(
            identity.verify(LocalChannelProofRole::Client, transcript, &server_signature,),
            Err(NetworkError::AuthDenied)
        );
    }

    #[test]
    fn local_channel_proof_rejects_tampered_transcript() {
        let (_root, data_dir) = test_data_dir("sinomo-local-channel-tamper");
        let identity = LocalChannelAuthIdentity::from_data_dir(&data_dir).expect("identity");
        let transcript = b"authenticated local channel transcript";
        let signature = identity.sign(LocalChannelProofRole::Server, transcript);
        let mut tampered = transcript.to_vec();
        tampered[0] ^= 0x01;

        assert_eq!(
            identity.verify(LocalChannelProofRole::Server, &tampered, &signature),
            Err(NetworkError::AuthDenied)
        );
    }

    #[tokio::test]
    async fn loopback_alone_does_not_authorize_a_different_daemon_identity() {
        let (_client_root, client_data_dir) =
            test_data_dir("sinomo-local-channel-wrong-identity-client");
        let (_server_root, server_data_dir) =
            test_data_dir("sinomo-local-channel-wrong-identity-server");
        let listener = crate::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr_string().expect("address");
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept");
            accept_local_object_rpc_channel_async(stream, &server_data_dir).await
        });

        let client = connect_local_object_rpc_channel_async(
            &client_data_dir,
            &address,
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
        )
        .await;
        assert!(client.is_err(), "wrong daemon identity must not connect");
        assert!(
            matches!(
                server.await.expect("server task"),
                Err(NetworkError::AuthDenied)
            ),
            "server must reject a loopback peer with another data-root identity"
        );
    }

    async fn assert_fresh_carrier_round_trip() {
        let left = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("replacement dialer");
        let right = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Acceptor,
            ObjectRpcChannelConfig::default(),
        )
        .expect("replacement acceptor");
        let ObjectRpcChannelEndpoint {
            client: replacement_client,
            server: _,
            ingress: left_ingress,
            egress: left_egress,
        } = left;
        let ObjectRpcChannelEndpoint {
            client: _,
            server: mut replacement_server,
            ingress: right_ingress,
            egress: right_egress,
        } = right;
        let (left_wire, right_wire) = tokio::io::duplex(64 * 1024);
        let (left_reader, left_writer) = tokio::io::split(left_wire);
        let (right_reader, right_writer) = tokio::io::split(right_wire);
        let left_transport = start_carrier_halves(
            left_reader,
            left_writer,
            left_ingress,
            left_egress,
            Duration::from_secs(1),
        );
        let right_transport = start_carrier_halves(
            right_reader,
            right_writer,
            right_ingress,
            right_egress,
            Duration::from_secs(1),
        );
        let request = ObjectRpcRequest::encode_body("/test/replacement-carrier", "test", &"ping")
            .expect("replacement request");
        let pending = replacement_client
            .call(ObjectRpcSessionCall::value(request))
            .await
            .expect("replacement call");
        let inbound = replacement_server
            .accept()
            .await
            .expect("replacement request");
        inbound
            .respond(ObjectRpcSessionResponse::value(
                crate::ObjectRpcResponse::encode_body("/test/replacement-carrier", &"pong")
                    .expect("replacement response"),
            ))
            .await
            .expect("replacement respond");
        let result = pending.result().await.expect("replacement result");
        assert_eq!(
            result.response.decode_body::<String>().expect("response"),
            "pong"
        );
        left_transport.close("test complete");
        right_transport.close("test complete");
    }

    #[tokio::test]
    async fn local_carrier_streams_raw_scdf_data_in_both_directions() {
        let left = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("dialer");
        let right = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Acceptor,
            ObjectRpcChannelConfig::default(),
        )
        .expect("acceptor");
        let ObjectRpcChannelEndpoint {
            client,
            server: _,
            ingress: left_ingress,
            egress: left_egress,
        } = left;
        let ObjectRpcChannelEndpoint {
            client: _,
            mut server,
            ingress: right_ingress,
            egress: right_egress,
        } = right;
        let (left_wire, right_wire) = tokio::io::duplex(64 * 1024);
        let (left_reader, left_writer) = tokio::io::split(left_wire);
        let (right_reader, right_writer) = tokio::io::split(right_wire);
        let left_transport = start_carrier_halves(
            left_reader,
            left_writer,
            left_ingress,
            left_egress,
            Duration::from_secs(1),
        );
        let right_transport = start_carrier_halves(
            right_reader,
            right_writer,
            right_ingress,
            right_egress,
            Duration::from_secs(1),
        );

        let request_bytes = vec![0x35; 1024 * 1024 + 17];
        let response_bytes = vec![0xa7; 1024 * 1024 + 31];
        let pending = client
            .call(ObjectRpcSessionCall::new(
                ObjectRpcRequest::from_encoded_body("/test/scdf", "test", Vec::new()),
                ObjectRpcBodySource::bytes(request_bytes.clone()),
            ))
            .await
            .expect("call");
        let mut incoming = server.accept().await.expect("incoming request");
        let mut received_request = Vec::new();
        incoming
            .request
            .body
            .read_to_end(&mut received_request)
            .await
            .expect("request body");
        assert_eq!(received_request, request_bytes);
        incoming
            .respond(ObjectRpcSessionResponse::new(
                crate::ObjectRpcResponse::from_encoded_body("/test/scdf", Vec::new()),
                ObjectRpcBodySource::bytes(response_bytes.clone()),
            ))
            .await
            .expect("respond");
        let mut result = pending.result().await.expect("result");
        let mut received_response = Vec::new();
        result
            .body
            .read_to_end(&mut received_response)
            .await
            .expect("response body");
        assert_eq!(received_response, response_bytes);

        left_transport.close("test complete");
        right_transport.close("test complete");
    }

    #[tokio::test]
    async fn blackholed_writer_closes_carrier_and_a_fresh_carrier_replaces_it() {
        let endpoint = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("blackholed endpoint");
        let ObjectRpcChannelEndpoint {
            client,
            server: _,
            ingress,
            egress,
        } = endpoint;
        let (carrier_wire, blackhole) = tokio::io::duplex(64);
        let (reader, writer) = tokio::io::split(carrier_wire);
        let transport =
            start_carrier_halves(reader, writer, ingress, egress, Duration::from_millis(20));
        let request = ObjectRpcRequest::encode_body(
            "/test/blackholed-carrier",
            "test",
            &vec![0x41_u8; 4 * 1024],
        )
        .expect("blackholed request");
        let pending = client
            .call(ObjectRpcSessionCall::value(request))
            .await
            .expect("queue blackholed call");
        tokio::time::timeout(Duration::from_secs(1), async {
            while !client.is_closed() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("write no-progress closes carrier");
        assert!(pending.result().await.is_err());
        drop(blackhole);
        transport.close("blackhole test complete");

        assert_fresh_carrier_round_trip().await;
    }

    #[tokio::test]
    async fn partial_frame_stalls_close_carriers_and_allow_replacement() {
        const NO_PROGRESS_TIMEOUT: Duration = Duration::from_millis(20);

        let partial_prefix = b"S".to_vec();
        let mut partial_body = b"SOF2".to_vec();
        partial_body.extend_from_slice(&64_u64.to_be_bytes());
        partial_body.extend_from_slice(b"partial");

        for (index, (label, stalled_bytes)) in [
            ("partial prefix", partial_prefix.as_slice()),
            ("partial body", partial_body.as_slice()),
        ]
        .into_iter()
        .enumerate()
        {
            let endpoint = ObjectRpcChannelEndpoint::new(
                ObjectRpcChannelRole::Dialer,
                ObjectRpcChannelConfig::default(),
            )
            .expect("stalled endpoint");
            let ObjectRpcChannelEndpoint {
                client,
                server: _,
                ingress,
                egress,
            } = endpoint;
            let (carrier_wire, mut stalled_peer) = tokio::io::duplex(64 * 1024);
            let (reader, writer) = tokio::io::split(carrier_wire);
            let transport =
                start_carrier_halves(reader, writer, ingress, egress, NO_PROGRESS_TIMEOUT);

            if index == 0 {
                tokio::time::sleep(Duration::from_millis(60)).await;
                assert!(
                    !client.is_closed(),
                    "carrier must stay open while idle between complete frames"
                );
            }

            stalled_peer
                .write_all(stalled_bytes)
                .await
                .expect("write stalled frame bytes");
            tokio::time::timeout(Duration::from_secs(1), async {
                while !client.is_closed() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("{label} no-progress must close the carrier"));

            let mut eof = [0_u8; 1];
            let read = tokio::time::timeout(Duration::from_secs(1), stalled_peer.read(&mut eof))
                .await
                .unwrap_or_else(|_| panic!("{label} carrier socket must close"))
                .expect("read carrier close");
            assert_eq!(read, 0, "{label} carrier socket must reach EOF");
            transport.close("partial-frame test complete");
        }

        assert_fresh_carrier_round_trip().await;
    }

    #[tokio::test]
    async fn loopback_carrier_uses_object_frames_for_calls() {
        let (_root, data_dir) = test_data_dir("sinomo-local-channel-call");
        let listener = crate::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr_string().expect("address");
        let server_data_dir = data_dir.clone();
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept");
            let accepted = accept_local_object_rpc_channel_async(stream, &server_data_dir)
                .await
                .expect("handshake");
            let AcceptedLocalObjectRpcChannel {
                mut server,
                transport,
            } = accepted;
            let inbound = server.accept().await.expect("request");
            let response = crate::ObjectRpcResponse::encode_body(
                inbound.request.request.route.clone(),
                &"pong".to_string(),
            )
            .expect("response");
            inbound
                .respond(ObjectRpcSessionResponse::value(response))
                .await
                .expect("respond");
            transport
        });

        let handshake_started = std::time::Instant::now();
        let connected = connect_local_object_rpc_channel_async(
            &data_dir,
            &address,
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
        )
        .await
        .expect("connect")
        .expect("supported");
        eprintln!(
            "authenticated local channel setup: {}us",
            handshake_started.elapsed().as_micros()
        );
        let request = ObjectRpcRequest::encode_body("/test/channel", "test", &"ping".to_string())
            .expect("request");
        let result = connected
            .client
            .call(ObjectRpcSessionCall::value(request))
            .await
            .expect("call")
            .result()
            .await
            .expect("result");
        assert_eq!(
            result.response.decode_body::<String>().expect("body"),
            "pong"
        );
        assert!(result.body.is_empty());

        connected.transport.close("test complete");
        let server_transport = server.await.expect("server task");
        server_transport.close("test complete");
    }

    /// Diagnostic lower bound for the complete multiplexed channel machinery
    /// over loopback TCP. This is ignored because wall-clock assertions are not
    /// suitable for shared CI hosts; run it explicitly with `--ignored
    /// --nocapture` when comparing the channel against the naïve object-frame
    /// example.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "diagnostic latency benchmark"]
    async fn benchmark_loopback_bodyless_channel_round_trip() {
        const WARMUP: usize = 100;
        const REQUESTS: usize = 1_000;

        let (_root, data_dir) = test_data_dir("sinomo-local-channel-benchmark");
        let listener = crate::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr_string().expect("address");
        let server_data_dir = data_dir.clone();
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept");
            let accepted = accept_local_object_rpc_channel_async(stream, &server_data_dir)
                .await
                .expect("handshake");
            let AcceptedLocalObjectRpcChannel {
                mut server,
                transport,
            } = accepted;
            let mut handlers = tokio::task::JoinSet::new();
            for _ in 0..WARMUP + REQUESTS {
                let inbound = server.accept().await.expect("request");
                handlers.spawn(async move {
                    let route = inbound.request.request.route.clone();
                    inbound
                        .respond(ObjectRpcSessionResponse::value(
                            crate::ObjectRpcResponse::from_encoded_body(route, Vec::new()),
                        ))
                        .await
                        .expect("respond");
                });
            }
            while let Some(result) = handlers.join_next().await {
                result.expect("dispatch task");
            }
            transport
        });

        let connected = connect_local_object_rpc_channel_async(
            &data_dir,
            &address,
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
        )
        .await
        .expect("connect")
        .expect("supported");
        let request =
            ObjectRpcRequest::from_encoded_body("/test/channel-benchmark", "test", Vec::new());
        for _ in 0..WARMUP {
            connected
                .client
                .call(ObjectRpcSessionCall::value(request.clone()))
                .await
                .expect("warmup call")
                .result()
                .await
                .expect("warmup result");
        }

        let mut latencies_us = Vec::with_capacity(REQUESTS);
        for _ in 0..REQUESTS {
            let started = std::time::Instant::now();
            connected
                .client
                .call(ObjectRpcSessionCall::value(request.clone()))
                .await
                .expect("measured call")
                .result()
                .await
                .expect("measured result");
            latencies_us.push(started.elapsed().as_micros() as u64);
        }
        latencies_us.sort_unstable();
        let percentile = |percent: usize| {
            let rank = latencies_us
                .len()
                .saturating_mul(percent)
                .saturating_add(99)
                / 100;
            latencies_us[rank.saturating_sub(1).min(latencies_us.len() - 1)]
        };
        eprintln!(
            "multiplexed loopback TCP channel with spawned dispatch: requests={REQUESTS} min={}us p50={}us p95={}us max={}us",
            latencies_us[0],
            percentile(50),
            percentile(95),
            latencies_us[latencies_us.len() - 1],
        );

        connected.transport.close("benchmark complete");
        let server_transport = server.await.expect("server task");
        server_transport.close("benchmark complete");
    }

    #[tokio::test]
    async fn incompatible_peer_returns_none_for_canonical_fallback() {
        let (_root, data_dir) = test_data_dir("sinomo-local-channel-incompatible");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address").to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = [0_u8; LOCAL_OBJECT_RPC_CHANNEL_HANDSHAKE_BYTES];
            stream.read_exact(&mut request).await.expect("handshake");
            stream
                .write_all(&server_handshake(None))
                .await
                .expect("reject");
        });

        let connected = connect_local_object_rpc_channel_async(
            &data_dir,
            &address,
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
        )
        .await
        .expect("connect result");
        assert!(connected.is_none());
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn aborting_carrier_marks_client_closed() {
        let (_root, data_dir) = test_data_dir("sinomo-local-channel-abort");
        let listener = crate::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr_string().expect("address");
        let server_data_dir = data_dir.clone();
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept");
            accept_local_object_rpc_channel_async(stream, &server_data_dir)
                .await
                .expect("handshake")
        });
        let connected = connect_local_object_rpc_channel_async(
            &data_dir,
            &address,
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
        )
        .await
        .expect("connect")
        .expect("supported");
        let accepted = server.await.expect("server task");

        connected.transport.abort_carrier_for_test();
        tokio::task::yield_now().await;
        assert!(connected.client.is_closed());
        accepted.transport.close("test complete");
    }

    #[tokio::test]
    async fn loopback_carrier_rejects_oversized_frame_from_length_prefix() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        let stream = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect");
        let (mut peer, _) = listener.accept().await.expect("accept");
        let endpoint = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("channel endpoint");
        let ObjectRpcChannelEndpoint {
            client,
            server: _,
            ingress,
            egress,
        } = endpoint;
        let transport = start_tcp_carrier(stream, ingress, egress, Duration::from_secs(1));
        let server = tokio::spawn(async move {
            peer.write_all(b"SOF2").await.expect("frame magic");
            peer.write_all(
                &u64::try_from(MAX_OBJECT_RPC_CHANNEL_WIRE_FRAME_BYTES + 1)
                    .expect("wire limit")
                    .to_be_bytes(),
            )
            .await
            .expect("oversized frame length");
            let mut closed = [0_u8; 1];
            let _ = tokio::time::timeout(Duration::from_secs(1), peer.read(&mut closed)).await;
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !client.is_closed() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("oversized prefix closes carrier");

        transport.close("test complete");
        server.await.expect("server task");
    }
}
