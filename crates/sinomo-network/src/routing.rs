//! Pure route planning and target-bound routed-stream contracts.
//!
//! Routing consumes immutable directory and direct-connectivity snapshots. It
//! owns transitive paths and stream establishment, but forwards opaque bytes
//! and has no dependency on SOBJ, service methods, or application retries.

use crate::connectivity_snapshot::{
    ConnectivityFailureSink, ConnectivitySnapshot, DirectConnectivityFailureEvidence,
    DirectConnectivityObservation, DirectConnectivityState,
};
use crate::direct_link::{
    AuthenticatedDirectStream, BoxedNetworkStream, DirectStreamCapabilities, LinkProvider,
};
use crate::directory::{ClusterDirectory, DirectorySnapshot};
use crate::error::{NetworkDeliveryState, NetworkError, NetworkTimeoutPhase};
use serde::{Deserialize, Serialize};
use sinomo_base::{ClusterEndpointRecord, Id};
use sinomo_node::NodeId;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

pub const MAX_ROUTE_CANDIDATES: usize = 16;
const MAX_ROUTE_SEARCH_STATES: usize = 4096;
const MAX_ROUTED_SESSION_HOPS: usize = 16;
const ROUTED_CANDIDATE_BASE_HANDSHAKE_BUDGET: Duration = Duration::from_millis(100);
const ROUTED_CANDIDATE_PER_HOP_HANDSHAKE_BUDGET: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RouteConstraints {
    pub require_full_duplex: bool,
    pub require_confidentiality: bool,
    pub require_relay_forwarding: bool,
    pub max_hops: usize,
}

impl Default for RouteConstraints {
    fn default() -> Self {
        Self {
            require_full_duplex: true,
            require_confidentiality: true,
            require_relay_forwarding: false,
            max_hops: 8,
        }
    }
}

impl RouteConstraints {
    fn accepts(&self, observation: &DirectConnectivityObservation) -> bool {
        self.accepts_capabilities(observation.capabilities)
    }

    fn accepts_capabilities(&self, capabilities: DirectStreamCapabilities) -> bool {
        (!self.require_full_duplex || capabilities.full_duplex)
            && (!self.require_confidentiality || capabilities.confidential)
            && (!self.require_relay_forwarding || capabilities.relay_forwarding)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteHop {
    pub source_node_id: NodeId,
    pub target_node_id: NodeId,
    pub endpoint: ClusterEndpointRecord,
    pub capabilities: DirectStreamCapabilities,
    pub connectivity_state: DirectConnectivityState,
    pub latency_micros: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidatePath {
    pub nodes: Vec<NodeId>,
    pub hops: Vec<RouteHop>,
}

impl CandidatePath {
    fn at_source(source_node_id: NodeId) -> Self {
        Self {
            nodes: vec![source_node_id],
            hops: Vec::new(),
        }
    }

    fn extend(&self, hop: RouteHop) -> Self {
        let mut nodes = self.nodes.clone();
        nodes.push(hop.target_node_id.clone());
        let mut hops = self.hops.clone();
        hops.push(hop);
        Self { nodes, hops }
    }

    fn total_latency_micros(&self) -> u64 {
        self.hops
            .iter()
            .map(|hop| hop.latency_micros.unwrap_or(u64::MAX / 4))
            .fold(0_u64, u64::saturating_add)
    }

    fn degraded_hop_count(&self) -> usize {
        self.hops
            .iter()
            .filter(|hop| hop.connectivity_state == DirectConnectivityState::Degraded)
            .count()
    }
}

fn compare_candidate_paths(left: &CandidatePath, right: &CandidatePath) -> std::cmp::Ordering {
    // Fully healthy paths always precede paths containing degraded evidence,
    // even when the latter are shorter. Degraded edges remain executable as a
    // last resort so a transient observation generation cannot make an
    // otherwise connected node publish zero candidates.
    left.degraded_hop_count()
        .cmp(&right.degraded_hop_count())
        .then_with(|| left.hops.len().cmp(&right.hops.len()))
        .then_with(|| {
            left.total_latency_micros()
                .cmp(&right.total_latency_micros())
        })
        .then_with(|| left.nodes.cmp(&right.nodes))
}

fn retain_ranked_candidate(candidates: &mut Vec<CandidatePath>, candidate: CandidatePath) {
    candidates.push(candidate);
    candidates.sort_by(compare_candidate_paths);
    candidates.truncate(MAX_ROUTE_CANDIDATES);
}

fn directory_contains_endpoint(
    directory: &DirectorySnapshot,
    observation: &DirectConnectivityObservation,
) -> bool {
    directory
        .node(&observation.target_node_id)
        .is_some_and(|node| {
            node.endpoints.iter().any(|endpoint| {
                endpoint.endpoint_id == observation.endpoint.endpoint_id
                    && endpoint.transport == observation.endpoint.transport
                    && endpoint.address == observation.endpoint.address
            })
        })
}

/// Deterministically plans executable directed paths from immutable inputs.
///
/// It performs no dialing, persistence, probing, retries, or RPC. The single
/// routed-stream executor consumes this contract for direct and relayed paths.
pub fn plan_routes(
    directory: &DirectorySnapshot,
    connectivity: &ConnectivitySnapshot,
    source_node_id: &NodeId,
    destination_node_id: &NodeId,
    constraints: RouteConstraints,
) -> Vec<CandidatePath> {
    if connectivity.directory_generation() != directory.version().0
        || !directory.is_authorized(source_node_id)
        || !directory.is_authorized(destination_node_id)
        || constraints.max_hops == 0
    {
        return Vec::new();
    }
    if source_node_id == destination_node_id {
        return vec![CandidatePath::at_source(source_node_id.clone())];
    }

    let mut adjacency = BTreeMap::<NodeId, Vec<RouteHop>>::new();
    for observation in connectivity.edges() {
        if observation.state == DirectConnectivityState::Unreachable
            || !directory.is_authorized(&observation.source_node_id)
            || !directory.is_authorized(&observation.target_node_id)
            || !directory_contains_endpoint(directory, observation)
            || !constraints.accepts(observation)
        {
            continue;
        }
        adjacency
            .entry(observation.source_node_id.clone())
            .or_default()
            .push(RouteHop {
                source_node_id: observation.source_node_id.clone(),
                target_node_id: observation.target_node_id.clone(),
                endpoint: observation.endpoint.clone(),
                capabilities: observation.capabilities,
                connectivity_state: observation.state,
                latency_micros: observation.latency_micros,
            });
    }
    for hops in adjacency.values_mut() {
        hops.sort_by(|left, right| {
            left.target_node_id
                .cmp(&right.target_node_id)
                .then_with(|| left.endpoint.endpoint_id.cmp(&right.endpoint.endpoint_id))
        });
    }

    let mut candidates = Vec::new();
    let mut pending = VecDeque::from([CandidatePath::at_source(source_node_id.clone())]);
    let mut visited_states = 0_usize;
    while let Some(path) = pending.pop_front() {
        visited_states = visited_states.saturating_add(1);
        if visited_states > MAX_ROUTE_SEARCH_STATES {
            break;
        }
        if path.hops.len() >= constraints.max_hops.min(MAX_ROUTED_SESSION_HOPS) {
            continue;
        }
        let Some(last_node) = path.nodes.last() else {
            continue;
        };
        let Some(next_hops) = adjacency.get(last_node) else {
            continue;
        };
        for hop in next_hops {
            if path.nodes.contains(&hop.target_node_id) {
                continue;
            }
            let next_path = path.extend(hop.clone());
            if &hop.target_node_id == destination_node_id {
                if next_path.hops.len() == 1
                    || next_path
                        .hops
                        .iter()
                        .all(|hop| hop.capabilities.relay_forwarding)
                {
                    retain_ranked_candidate(&mut candidates, next_path);
                }
            } else {
                pending.push_back(next_path);
            }
        }
    }
    candidates
}

const ROUTED_SESSION_PROTOCOL: &str = "sinomo.routed-session.v1";
const ROUTED_SESSION_WIRE_PREFIX: [u8; 4] = *b"SRT1";
const ROUTED_SESSION_OPEN_FRAME: u8 = 1;
const ROUTED_SESSION_ACCEPT_FRAME: u8 = 2;
const MAX_ROUTED_SESSION_FRAME_BYTES: usize = 64 * 1024;
const ROUTED_SESSION_CRYPTO_PIPE_BYTES: usize = 256 * 1024;
const ROUTED_SESSION_DATA_FRAME_BYTES: usize = 64 * 1024;
const ROUTED_SESSION_RELAY_BUFFER_BYTES: usize = 64 * 1024;
const ROUTED_SESSION_DATA_WIRE_PREFIX: [u8; 4] = *b"SRE1";
const ROUTED_SESSION_BINDING_DOMAIN: &[u8] = b"sinomo.routed-session.binding.v1\0";
const ROUTED_SESSION_KEY_DOMAIN: &[u8] = b"sinomo.routed-session.data-key.v1\0";
const ROUTED_SESSION_AAD_DOMAIN: &[u8] = b"sinomo.routed-session.data-frame.v1\0";

/// Signing boundary for routed-session establishment.
///
/// The routing layer receives authorized public-key material from the
/// directory, but does not own key storage or a concrete identity primitive.
/// Implementations may use the daemon identity, a hardware key, or a
/// transport-native identity as long as they sign the exact bounded
/// transcript supplied here.
pub trait RoutedSessionIdentity: Send + Sync {
    fn local_node_id(&self) -> &NodeId;

    fn sign(&self, transcript: &[u8]) -> Result<Vec<u8>, NetworkError>;

    fn verify(
        &self,
        signer_node_id: &NodeId,
        authorized_public_key: &str,
        transcript: &[u8],
        signature: &[u8],
    ) -> Result<(), NetworkError>;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct RoutedSessionDescriptor {
    protocol: String,
    source_node_id: NodeId,
    destination_node_id: NodeId,
    session_id: Id,
    path: Vec<NodeId>,
    endpoint_ids: Vec<Id>,
    source_ephemeral_public_key: [u8; 32],
    hop_limit: u16,
    deadline_unix_millis: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SignedRoutedSessionOpen {
    descriptor: RoutedSessionDescriptor,
    signature: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct RoutedSessionAcceptance {
    protocol: String,
    source_node_id: NodeId,
    destination_node_id: NodeId,
    session_id: Id,
    open_transcript_digest: [u8; 32],
    destination_ephemeral_public_key: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SignedRoutedSessionAcceptance {
    acceptance: RoutedSessionAcceptance,
    signature: Vec<u8>,
}

fn routed_session_encode<T: Serialize>(value: &T) -> Result<Vec<u8>, NetworkError> {
    let encoded = serde_json::to_vec(value).map_err(|error| NetworkError::Serialization {
        detail: format!("encode routed-session handshake: {error}"),
    })?;
    if encoded.len() > MAX_ROUTED_SESSION_FRAME_BYTES {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: encoded.len(),
            max_bytes: MAX_ROUTED_SESSION_FRAME_BYTES,
        });
    }
    Ok(encoded)
}

fn routed_session_decode<T: for<'de> Deserialize<'de>>(encoded: &[u8]) -> Result<T, NetworkError> {
    serde_json::from_slice(encoded).map_err(|error| NetworkError::Serialization {
        detail: format!("decode routed-session handshake: {error}"),
    })
}

fn routed_session_io_error(operation: &str, error: std::io::Error) -> NetworkError {
    NetworkError::Io {
        operation: operation.to_string(),
        detail: error.to_string(),
    }
}

async fn write_routed_session_frame<S>(
    stream: &mut S,
    frame_kind: u8,
    payload: &[u8],
) -> Result<(), NetworkError>
where
    S: AsyncWrite + Unpin + ?Sized,
{
    if payload.len() > MAX_ROUTED_SESSION_FRAME_BYTES {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: payload.len(),
            max_bytes: MAX_ROUTED_SESSION_FRAME_BYTES,
        });
    }
    let payload_len = u32::try_from(payload.len()).map_err(|_| NetworkError::FrameTooLarge {
        actual_bytes: payload.len(),
        max_bytes: MAX_ROUTED_SESSION_FRAME_BYTES,
    })?;
    let mut header = [0_u8; 9];
    header[..4].copy_from_slice(&ROUTED_SESSION_WIRE_PREFIX);
    header[4] = frame_kind;
    header[5..].copy_from_slice(&payload_len.to_be_bytes());
    stream
        .write_all(&header)
        .await
        .map_err(|error| routed_session_io_error("write routed-session frame header", error))?;
    stream
        .write_all(payload)
        .await
        .map_err(|error| routed_session_io_error("write routed-session frame payload", error))?;
    stream
        .flush()
        .await
        .map_err(|error| routed_session_io_error("flush routed-session frame", error))
}

async fn read_routed_session_frame<S>(
    stream: &mut S,
    expected_kind: u8,
) -> Result<Vec<u8>, NetworkError>
where
    S: AsyncRead + Unpin + ?Sized,
{
    let mut header = [0_u8; 9];
    stream
        .read_exact(&mut header)
        .await
        .map_err(|error| routed_session_io_error("read routed-session frame header", error))?;
    if header[..4] != ROUTED_SESSION_WIRE_PREFIX || header[4] != expected_kind {
        return Err(NetworkError::Serialization {
            detail: "unexpected routed-session handshake frame".to_string(),
        });
    }
    let payload_len = usize::try_from(u32::from_be_bytes(
        header[5..]
            .try_into()
            .expect("routed-session length header"),
    ))
    .expect("u32 routed-session length fits usize");
    if payload_len > MAX_ROUTED_SESSION_FRAME_BYTES {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: payload_len,
            max_bytes: MAX_ROUTED_SESSION_FRAME_BYTES,
        });
    }
    let mut payload = vec![0_u8; payload_len];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|error| routed_session_io_error("read routed-session frame payload", error))?;
    Ok(payload)
}

fn routed_session_now_unix_millis() -> i64 {
    let millis = time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
    i64::try_from(millis).unwrap_or(if millis.is_negative() {
        i64::MIN
    } else {
        i64::MAX
    })
}

fn routed_session_deadline_unix_millis(timeout: Duration) -> i64 {
    routed_session_now_unix_millis()
        .saturating_add(i64::try_from(timeout.as_millis()).unwrap_or(i64::MAX))
}

fn routed_session_channel_binding(
    shared_secret: &[u8; 32],
    open_transcript: &[u8],
    acceptance_transcript: &[u8],
) -> [u8; 32] {
    let mut input = Vec::with_capacity(
        ROUTED_SESSION_BINDING_DOMAIN.len()
            + shared_secret.len()
            + open_transcript.len()
            + acceptance_transcript.len(),
    );
    input.extend_from_slice(ROUTED_SESSION_BINDING_DOMAIN);
    input.extend_from_slice(shared_secret);
    input.extend_from_slice(open_transcript);
    input.extend_from_slice(acceptance_transcript);
    *blake3::hash(&input).as_bytes()
}

struct RoutedSessionEphemeralKey {
    private: ring::agreement::EphemeralPrivateKey,
    public: [u8; 32],
}

fn routed_session_ephemeral_key() -> Result<RoutedSessionEphemeralKey, NetworkError> {
    let rng = ring::rand::SystemRandom::new();
    let private = ring::agreement::EphemeralPrivateKey::generate(&ring::agreement::X25519, &rng)
        .map_err(|_| NetworkError::Io {
            operation: "generate routed-session ephemeral key".to_string(),
            detail: "system random source failed".to_string(),
        })?;
    let public = private
        .compute_public_key()
        .map_err(|_| NetworkError::AuthDenied)?;
    let public = public
        .as_ref()
        .try_into()
        .map_err(|_| NetworkError::AuthDenied)?;
    Ok(RoutedSessionEphemeralKey { private, public })
}

fn routed_session_agree(
    private: ring::agreement::EphemeralPrivateKey,
    peer_public: &[u8; 32],
) -> Result<[u8; 32], NetworkError> {
    let peer = ring::agreement::UnparsedPublicKey::new(&ring::agreement::X25519, peer_public);
    ring::agreement::agree_ephemeral(private, &peer, |secret| {
        secret.try_into().map_err(|_| NetworkError::AuthDenied)
    })
    .map_err(|_| NetworkError::AuthDenied)?
}

fn routed_session_data_key(
    shared_secret: &[u8; 32],
    channel_binding: &[u8; 32],
    direction: u8,
) -> [u8; 32] {
    let mut input = Vec::with_capacity(
        ROUTED_SESSION_KEY_DOMAIN.len() + channel_binding.len() + std::mem::size_of::<u8>(),
    );
    input.extend_from_slice(ROUTED_SESSION_KEY_DOMAIN);
    input.extend_from_slice(channel_binding);
    input.push(direction);
    *blake3::keyed_hash(shared_secret, &input).as_bytes()
}

fn routed_session_data_nonce(direction: u8, sequence: u64) -> ring::aead::Nonce {
    let mut nonce = [0_u8; 12];
    nonce[0] = direction;
    nonce[4..].copy_from_slice(&sequence.to_be_bytes());
    ring::aead::Nonce::assume_unique_for_key(nonce)
}

fn routed_session_data_aad(
    session_id: &Id,
    channel_binding: &[u8; 32],
    direction: u8,
    sequence: u64,
    plaintext_bytes: u32,
) -> [u8; 32] {
    let mut aad = blake3::Hasher::new();
    aad.update(ROUTED_SESSION_AAD_DOMAIN);
    aad.update(session_id.as_str().as_bytes());
    aad.update(&[0]);
    aad.update(channel_binding);
    aad.update(&[direction]);
    aad.update(&sequence.to_be_bytes());
    aad.update(&plaintext_bytes.to_be_bytes());
    *aad.finalize().as_bytes()
}

fn routed_session_data_io(operation: &str, detail: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(format!("{operation}: {detail}"))
}

async fn encrypt_routed_session_data<R, W>(
    mut plaintext: R,
    mut ciphertext: W,
    key: [u8; 32],
    session_id: Id,
    channel_binding: [u8; 32],
    direction: u8,
) -> Result<(), std::io::Error>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let unbound = ring::aead::UnboundKey::new(&ring::aead::CHACHA20_POLY1305, &key)
        .map_err(|_| routed_session_data_io("create routed-session sealing key", "invalid key"))?;
    let key = ring::aead::LessSafeKey::new(unbound);
    let mut sequence = 0_u64;
    let max_sealed_bytes =
        ROUTED_SESSION_DATA_FRAME_BYTES.saturating_add(ring::aead::CHACHA20_POLY1305.tag_len());
    let mut frame = vec![0_u8; max_sealed_bytes];
    loop {
        let read = plaintext
            .read(&mut frame[..ROUTED_SESSION_DATA_FRAME_BYTES])
            .await?;
        let plaintext_bytes = u32::try_from(read).expect("bounded routed data frame");
        let aad = routed_session_data_aad(
            &session_id,
            &channel_binding,
            direction,
            sequence,
            plaintext_bytes,
        );
        frame.truncate(read);
        key.seal_in_place_append_tag(
            routed_session_data_nonce(direction, sequence),
            ring::aead::Aad::from(aad),
            &mut frame,
        )
        .map_err(|_| routed_session_data_io("encrypt routed-session data", "AEAD failure"))?;
        let mut header = [0_u8; 16];
        header[..4].copy_from_slice(&ROUTED_SESSION_DATA_WIRE_PREFIX);
        header[4..12].copy_from_slice(&sequence.to_be_bytes());
        header[12..].copy_from_slice(&plaintext_bytes.to_be_bytes());
        ciphertext.write_all(&header).await?;
        ciphertext.write_all(&frame).await?;
        if read == 0 {
            ciphertext.shutdown().await?;
            return Ok(());
        }
        frame.resize(max_sealed_bytes, 0);
        sequence = sequence.checked_add(1).ok_or_else(|| {
            routed_session_data_io("encrypt routed-session data", "sequence exhausted")
        })?;
    }
}

async fn decrypt_routed_session_data<R, W>(
    mut ciphertext: R,
    mut plaintext: W,
    key: [u8; 32],
    session_id: Id,
    channel_binding: [u8; 32],
    direction: u8,
) -> Result<(), std::io::Error>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let unbound = ring::aead::UnboundKey::new(&ring::aead::CHACHA20_POLY1305, &key)
        .map_err(|_| routed_session_data_io("create routed-session opening key", "invalid key"))?;
    let key = ring::aead::LessSafeKey::new(unbound);
    let mut expected_sequence = 0_u64;
    let mut frame = vec![
        0_u8;
        ROUTED_SESSION_DATA_FRAME_BYTES
            .saturating_add(ring::aead::CHACHA20_POLY1305.tag_len())
    ];
    loop {
        let mut header = [0_u8; 16];
        let first = ciphertext.read(&mut header[..1]).await?;
        if first == 0 {
            return Err(routed_session_data_io(
                "decrypt routed-session data",
                "missing authenticated final frame",
            ));
        }
        ciphertext.read_exact(&mut header[1..]).await?;
        if header[..4] != ROUTED_SESSION_DATA_WIRE_PREFIX {
            return Err(routed_session_data_io(
                "decrypt routed-session data",
                "invalid frame prefix",
            ));
        }
        let sequence = u64::from_be_bytes(
            header[4..12]
                .try_into()
                .expect("routed data sequence header"),
        );
        if sequence != expected_sequence {
            return Err(routed_session_data_io(
                "decrypt routed-session data",
                "non-monotonic frame sequence",
            ));
        }
        let plaintext_bytes =
            u32::from_be_bytes(header[12..].try_into().expect("routed data length header"));
        let plaintext_len = usize::try_from(plaintext_bytes).expect("u32 fits usize");
        if plaintext_len > ROUTED_SESSION_DATA_FRAME_BYTES {
            return Err(routed_session_data_io(
                "decrypt routed-session data",
                "oversized encrypted frame",
            ));
        }
        let frame_len = plaintext_len.saturating_add(ring::aead::CHACHA20_POLY1305.tag_len());
        ciphertext.read_exact(&mut frame[..frame_len]).await?;
        let aad = routed_session_data_aad(
            &session_id,
            &channel_binding,
            direction,
            sequence,
            plaintext_bytes,
        );
        let opened = key
            .open_in_place(
                routed_session_data_nonce(direction, sequence),
                ring::aead::Aad::from(aad),
                &mut frame[..frame_len],
            )
            .map_err(|_| {
                routed_session_data_io("decrypt routed-session data", "authentication failed")
            })?;
        if plaintext_len == 0 {
            plaintext.shutdown().await?;
            return Ok(());
        }
        plaintext.write_all(opened).await?;
        expected_sequence = expected_sequence.checked_add(1).ok_or_else(|| {
            routed_session_data_io("decrypt routed-session data", "sequence exhausted")
        })?;
    }
}

fn encrypted_routed_session_stream(
    io: BoxedNetworkStream,
    shared_secret: [u8; 32],
    session_id: Id,
    channel_binding: [u8; 32],
    source_side: bool,
) -> BoxedNetworkStream {
    let (application, crypto) = tokio::io::duplex(ROUTED_SESSION_CRYPTO_PIPE_BYTES);
    let (application_read, application_write) = tokio::io::split(crypto);
    let (network_read, network_write) = tokio::io::split(io);
    let outgoing_direction = if source_side { 0 } else { 1 };
    let incoming_direction = if source_side { 1 } else { 0 };
    let outgoing_key =
        routed_session_data_key(&shared_secret, &channel_binding, outgoing_direction);
    let incoming_key =
        routed_session_data_key(&shared_secret, &channel_binding, incoming_direction);
    let outgoing_session_id = session_id.clone();
    tokio::spawn(async move {
        let outgoing = encrypt_routed_session_data(
            application_read,
            network_write,
            outgoing_key,
            outgoing_session_id,
            channel_binding,
            outgoing_direction,
        );
        let incoming = decrypt_routed_session_data(
            network_read,
            application_write,
            incoming_key,
            session_id,
            channel_binding,
            incoming_direction,
        );
        if let Err(error) = tokio::try_join!(outgoing, incoming) {
            tracing::debug!(%error, "encrypted routed-session stream stopped");
        }
    });
    Box::new(application)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoutedAvailability {
    Candidate,
    Connected,
    NoPath,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteTrace {
    pub nodes: Vec<NodeId>,
    pub endpoint_ids: Vec<Id>,
}

impl From<&CandidatePath> for RouteTrace {
    fn from(path: &CandidatePath) -> Self {
        Self {
            nodes: path.nodes.clone(),
            endpoint_ids: path
                .hops
                .iter()
                .map(|hop| hop.endpoint.endpoint_id.clone())
                .collect(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectOptions {
    pub constraints: RouteConstraints,
    pub timeout: Duration,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            constraints: RouteConstraints::default(),
            timeout: Duration::from_secs(10),
        }
    }
}

/// Socket-like stream bound to one authenticated destination and route.
pub struct PeerStream {
    io: BoxedNetworkStream,
    pub local_node_id: NodeId,
    pub peer_node_id: NodeId,
    pub session_id: Id,
    pub channel_binding: [u8; 32],
    pub route_trace: RouteTrace,
}

impl PeerStream {
    pub fn new(
        io: BoxedNetworkStream,
        local_node_id: NodeId,
        peer_node_id: NodeId,
        session_id: Id,
        channel_binding: [u8; 32],
        route_trace: RouteTrace,
    ) -> Result<Self, NetworkError> {
        if local_node_id == peer_node_id {
            return Err(NetworkError::Serialization {
                detail: "a routed peer stream must bind distinct endpoint identities".to_string(),
            });
        }
        if route_trace.nodes.first() != Some(&local_node_id)
            || route_trace.nodes.last() != Some(&peer_node_id)
            || route_trace.endpoint_ids.len().saturating_add(1) != route_trace.nodes.len()
        {
            return Err(NetworkError::Serialization {
                detail: "routed peer stream trace does not match its endpoint identities"
                    .to_string(),
            });
        }
        Ok(Self {
            io,
            local_node_id,
            peer_node_id,
            session_id,
            channel_binding,
            route_trace,
        })
    }

    pub fn into_inner(self) -> BoxedNetworkStream {
        self.io
    }
}

impl AsyncRead for PeerStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut *self.io).poll_read(cx, buf)
    }
}

impl AsyncWrite for PeerStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut *self.io).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut *self.io).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut *self.io).poll_shutdown(cx)
    }
}

fn validate_routed_session_descriptor(
    directory: &DirectorySnapshot,
    descriptor: &RoutedSessionDescriptor,
) -> Result<(), NetworkError> {
    let hop_count = descriptor.path.len().saturating_sub(1);
    if descriptor.protocol != ROUTED_SESSION_PROTOCOL
        || descriptor.path.len() < 2
        || hop_count > MAX_ROUTED_SESSION_HOPS
        || descriptor.endpoint_ids.len() != hop_count
        || descriptor.path.first() != Some(&descriptor.source_node_id)
        || descriptor.path.last() != Some(&descriptor.destination_node_id)
        || usize::from(descriptor.hop_limit) < hop_count
        || usize::from(descriptor.hop_limit) > MAX_ROUTED_SESSION_HOPS
        || descriptor.deadline_unix_millis < routed_session_now_unix_millis()
        || descriptor.source_ephemeral_public_key == [0; 32]
    {
        return Err(NetworkError::AuthDenied);
    }
    let unique = descriptor.path.iter().collect::<BTreeSet<_>>();
    if unique.len() != descriptor.path.len() {
        return Err(NetworkError::AuthDenied);
    }
    for node_id in &descriptor.path {
        if !directory.is_authorized(node_id) {
            return Err(NetworkError::UnauthorizedNode {
                node_id: node_id.clone(),
            });
        }
    }
    for (index, endpoint_id) in descriptor.endpoint_ids.iter().enumerate() {
        let target = &descriptor.path[index + 1];
        if !directory.node(target).is_some_and(|node| {
            node.endpoints
                .iter()
                .any(|endpoint| endpoint.endpoint_id == *endpoint_id)
        }) {
            return Err(NetworkError::AuthDenied);
        }
    }
    Ok(())
}

fn incoming_routed_session_position(
    stream: &AuthenticatedDirectStream,
    descriptor: &RoutedSessionDescriptor,
) -> Result<usize, NetworkError> {
    let Some(position) = descriptor
        .path
        .iter()
        .position(|node_id| node_id == &stream.local_node_id)
    else {
        return Err(NetworkError::FrameTargetMismatch {
            requested_target_node_id: stream.local_node_id.clone(),
            frame_target_node_id: descriptor.destination_node_id.clone(),
        });
    };
    if position == 0 || descriptor.path[position - 1] != stream.peer_node_id {
        return Err(NetworkError::FrameSourceMismatch {
            local_node_id: stream.peer_node_id.clone(),
            source_node_id: descriptor
                .path
                .get(position.saturating_sub(1))
                .cloned()
                .unwrap_or_else(|| descriptor.source_node_id.clone()),
        });
    }
    if descriptor.path.len() > 2 && !stream.capabilities.relay_forwarding {
        return Err(NetworkError::AuthDenied);
    }
    Ok(position)
}

fn verify_routed_session_open(
    directory: &DirectorySnapshot,
    identity: &dyn RoutedSessionIdentity,
    open: &SignedRoutedSessionOpen,
) -> Result<Vec<u8>, NetworkError> {
    validate_routed_session_descriptor(directory, &open.descriptor)?;
    let source = directory
        .node(&open.descriptor.source_node_id)
        .ok_or_else(|| NetworkError::UnauthorizedNode {
            node_id: open.descriptor.source_node_id.clone(),
        })?;
    let transcript = routed_session_encode(&open.descriptor)?;
    identity.verify(
        &source.node_id,
        &source.public_key,
        &transcript,
        &open.signature,
    )?;
    Ok(transcript)
}

fn verify_routed_session_acceptance(
    directory: &DirectorySnapshot,
    identity: &dyn RoutedSessionIdentity,
    descriptor: &RoutedSessionDescriptor,
    descriptor_transcript: &[u8],
    signed: &SignedRoutedSessionAcceptance,
) -> Result<Vec<u8>, NetworkError> {
    let acceptance = &signed.acceptance;
    if acceptance.protocol != ROUTED_SESSION_PROTOCOL
        || acceptance.source_node_id != descriptor.source_node_id
        || acceptance.destination_node_id != descriptor.destination_node_id
        || acceptance.session_id != descriptor.session_id
        || acceptance.open_transcript_digest != *blake3::hash(descriptor_transcript).as_bytes()
        || acceptance.destination_ephemeral_public_key == [0; 32]
    {
        return Err(NetworkError::AuthDenied);
    }
    let destination = directory
        .node(&descriptor.destination_node_id)
        .ok_or_else(|| NetworkError::UnauthorizedNode {
            node_id: descriptor.destination_node_id.clone(),
        })?;
    let transcript = routed_session_encode(acceptance)?;
    identity.verify(
        &destination.node_id,
        &destination.public_key,
        &transcript,
        &signed.signature,
    )?;
    Ok(transcript)
}

async fn initiate_routed_session(
    mut stream: AuthenticatedDirectStream,
    directory: &DirectorySnapshot,
    path: &CandidatePath,
    identity: &dyn RoutedSessionIdentity,
    deadline_unix_millis: i64,
) -> Result<PeerStream, NetworkError> {
    let Some(hop) = path.hops.first() else {
        return Err(NetworkError::Serialization {
            detail: "routed session requires at least one route hop".to_string(),
        });
    };
    if path.hops.is_empty()
        || path.nodes.len() != path.hops.len().saturating_add(1)
        || path.nodes.first() != Some(identity.local_node_id())
        || stream.local_node_id != *identity.local_node_id()
        || stream.peer_node_id != hop.target_node_id
        || (path.hops.len() > 1 && !stream.capabilities.relay_forwarding)
    {
        return Err(NetworkError::Serialization {
            detail: "routed-session path does not match its first authenticated link".to_string(),
        });
    }
    let ephemeral = routed_session_ephemeral_key()?;
    let descriptor = RoutedSessionDescriptor {
        protocol: ROUTED_SESSION_PROTOCOL.to_string(),
        source_node_id: stream.local_node_id.clone(),
        destination_node_id: path.nodes.last().cloned().expect("nonempty routed path"),
        session_id: Id::generate(),
        path: path.nodes.clone(),
        endpoint_ids: path
            .hops
            .iter()
            .map(|hop| hop.endpoint.endpoint_id.clone())
            .collect(),
        source_ephemeral_public_key: ephemeral.public,
        hop_limit: u16::try_from(path.hops.len()).map_err(|_| NetworkError::AuthDenied)?,
        deadline_unix_millis,
    };
    validate_routed_session_descriptor(directory, &descriptor)?;
    let descriptor_transcript = routed_session_encode(&descriptor)?;
    let open = SignedRoutedSessionOpen {
        signature: identity.sign(&descriptor_transcript)?,
        descriptor,
    };
    let open_frame = routed_session_encode(&open)?;
    write_routed_session_frame(&mut *stream.io, ROUTED_SESSION_OPEN_FRAME, &open_frame).await?;

    let acceptance_frame =
        read_routed_session_frame(&mut *stream.io, ROUTED_SESSION_ACCEPT_FRAME).await?;
    let signed_acceptance: SignedRoutedSessionAcceptance =
        routed_session_decode(&acceptance_frame)?;
    let acceptance_transcript = verify_routed_session_acceptance(
        directory,
        identity,
        &open.descriptor,
        &descriptor_transcript,
        &signed_acceptance,
    )?;
    let shared_secret = routed_session_agree(
        ephemeral.private,
        &signed_acceptance
            .acceptance
            .destination_ephemeral_public_key,
    )?;
    let channel_binding = routed_session_channel_binding(
        &shared_secret,
        &descriptor_transcript,
        &acceptance_transcript,
    );
    let io = encrypted_routed_session_stream(
        stream.io,
        shared_secret,
        open.descriptor.session_id.clone(),
        channel_binding,
        true,
    );
    PeerStream::new(
        io,
        open.descriptor.source_node_id,
        open.descriptor.destination_node_id,
        open.descriptor.session_id,
        channel_binding,
        RouteTrace::from(path),
    )
}

async fn accept_routed_session_destination(
    mut stream: AuthenticatedDirectStream,
    identity: &dyn RoutedSessionIdentity,
    open: SignedRoutedSessionOpen,
    descriptor_transcript: Vec<u8>,
) -> Result<PeerStream, NetworkError> {
    let descriptor = &open.descriptor;
    if descriptor.destination_node_id != stream.local_node_id {
        return Err(NetworkError::FrameTargetMismatch {
            requested_target_node_id: stream.local_node_id,
            frame_target_node_id: descriptor.destination_node_id.clone(),
        });
    }
    let ephemeral = routed_session_ephemeral_key()?;
    let shared_secret =
        routed_session_agree(ephemeral.private, &descriptor.source_ephemeral_public_key)?;
    let acceptance = RoutedSessionAcceptance {
        protocol: ROUTED_SESSION_PROTOCOL.to_string(),
        source_node_id: descriptor.source_node_id.clone(),
        destination_node_id: descriptor.destination_node_id.clone(),
        session_id: descriptor.session_id.clone(),
        open_transcript_digest: *blake3::hash(&descriptor_transcript).as_bytes(),
        destination_ephemeral_public_key: ephemeral.public,
    };
    let acceptance_transcript = routed_session_encode(&acceptance)?;
    let signed_acceptance = SignedRoutedSessionAcceptance {
        signature: identity.sign(&acceptance_transcript)?,
        acceptance,
    };
    let acceptance_frame = routed_session_encode(&signed_acceptance)?;
    write_routed_session_frame(
        &mut *stream.io,
        ROUTED_SESSION_ACCEPT_FRAME,
        &acceptance_frame,
    )
    .await?;
    let channel_binding = routed_session_channel_binding(
        &shared_secret,
        &descriptor_transcript,
        &acceptance_transcript,
    );
    let io = encrypted_routed_session_stream(
        stream.io,
        shared_secret,
        descriptor.session_id.clone(),
        channel_binding,
        false,
    );
    let mut nodes = descriptor.path.clone();
    nodes.reverse();
    let mut endpoint_ids = descriptor.endpoint_ids.clone();
    endpoint_ids.reverse();
    PeerStream::new(
        io,
        descriptor.destination_node_id.clone(),
        descriptor.source_node_id.clone(),
        descriptor.session_id.clone(),
        channel_binding,
        RouteTrace {
            nodes,
            endpoint_ids,
        },
    )
}

/// Establishes a target-bound routed session over one already-selected direct
/// endpoint.
///
/// This is the direct-path execution primitive shared by the route-planning
/// connector and composition boundaries that already hold a generation-fenced
/// endpoint choice. It still rechecks current directory authority, transport
/// authentication, capabilities, and the signed routed-session handshake.
pub async fn connect_direct_routed_endpoint(
    directory: &DirectorySnapshot,
    endpoint: &ClusterEndpointRecord,
    link_provider: &dyn LinkProvider,
    identity: &dyn RoutedSessionIdentity,
    options: ConnectOptions,
) -> Result<PeerStream, NetworkError> {
    let local_node_id = identity.local_node_id().clone();
    if !directory.is_authorized(&local_node_id) {
        return Err(NetworkError::UnauthorizedNode {
            node_id: local_node_id,
        });
    }
    let destination =
        directory
            .node(&endpoint.node_id)
            .ok_or_else(|| NetworkError::UnauthorizedNode {
                node_id: endpoint.node_id.clone(),
            })?;
    if !destination.endpoints.iter().any(|candidate| {
        candidate.endpoint_id == endpoint.endpoint_id
            && candidate.transport == endpoint.transport
            && candidate.address == endpoint.address
    }) {
        return Err(NetworkError::AuthDenied);
    }

    let deadline_unix_millis = routed_session_deadline_unix_millis(options.timeout);
    let establish = async {
        let link = link_provider.connect(endpoint).await?;
        if link.local_node_id() != &local_node_id
            || link.peer_node_id() != &endpoint.node_id
            || !options
                .constraints
                .accepts_capabilities(link.capabilities())
        {
            return Err(NetworkError::AuthDenied);
        }
        let path = CandidatePath {
            nodes: vec![local_node_id.clone(), endpoint.node_id.clone()],
            hops: vec![RouteHop {
                source_node_id: local_node_id,
                target_node_id: endpoint.node_id.clone(),
                endpoint: endpoint.clone(),
                capabilities: link.capabilities(),
                connectivity_state: DirectConnectivityState::Reachable,
                latency_micros: None,
            }],
        };
        let stream = link.open_stream().await?;
        initiate_routed_session(stream, directory, &path, identity, deadline_unix_millis).await
    };
    tokio::time::timeout(options.timeout, establish)
        .await
        .map_err(|_| {
            NetworkError::timeout(
                "establish direct routed endpoint session",
                NetworkTimeoutPhase::Connect,
                options.timeout,
                NetworkDeliveryState::DefinitelyNotSent,
            )
        })?
}

/// Accepts one direct routed-session handshake before application bytes.
///
/// The direct transport supplies the authenticated adjacent peer. The signed
/// open must describe exactly one hop from that peer to this destination. Only
/// after those checks and a destination-signed acceptance does the function
/// return a socket-like [`PeerStream`]. Multi-hop ingress uses
/// [`accept_or_relay_routed_session`] instead.
pub async fn accept_direct_routed_session(
    mut stream: AuthenticatedDirectStream,
    directory: &DirectorySnapshot,
    identity: &dyn RoutedSessionIdentity,
) -> Result<PeerStream, NetworkError> {
    if stream.local_node_id != *identity.local_node_id() {
        return Err(NetworkError::AuthDenied);
    }
    let open_frame = read_routed_session_frame(&mut *stream.io, ROUTED_SESSION_OPEN_FRAME).await?;
    let signed_open: SignedRoutedSessionOpen = routed_session_decode(&open_frame)?;
    let descriptor_transcript = verify_routed_session_open(directory, identity, &signed_open)?;
    if signed_open.descriptor.path.len() != 2 {
        return Err(NetworkError::AuthDenied);
    }
    let position = incoming_routed_session_position(&stream, &signed_open.descriptor)?;
    if position + 1 != signed_open.descriptor.path.len() {
        return Err(NetworkError::FrameTargetMismatch {
            requested_target_node_id: stream.local_node_id,
            frame_target_node_id: signed_open.descriptor.destination_node_id.clone(),
        });
    }
    accept_routed_session_destination(stream, identity, signed_open, descriptor_transcript).await
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutedRelayReport {
    pub session_id: Id,
    pub source_node_id: NodeId,
    pub destination_node_id: NodeId,
    pub relay_node_id: NodeId,
    pub next_node_id: NodeId,
    pub bytes_toward_destination: u64,
    pub bytes_toward_source: u64,
}

/// Result of accepting one routed-session stream at a path node.
pub enum RoutedSessionDisposition {
    Destination(PeerStream),
    Relayed(RoutedRelayReport),
}

fn report_routed_edge_failure(
    sink: Option<&dyn ConnectivityFailureSink>,
    source_node_id: &NodeId,
    target_node_id: &NodeId,
    endpoint_id: &Id,
    reason: &str,
) {
    let Some(sink) = sink else {
        return;
    };
    let max_bytes = crate::connectivity_snapshot::MAX_CONNECTIVITY_FAILURE_REASON_BYTES;
    let mut end = reason.len().min(max_bytes);
    while !reason.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    let reason = reason[..end].to_string();
    if let Err(error) = sink.submit_edge_failure(DirectConnectivityFailureEvidence {
        observer_node_id: source_node_id.clone(),
        source_node_id: source_node_id.clone(),
        target_node_id: target_node_id.clone(),
        endpoint_id: endpoint_id.clone(),
        reason,
        observed_at: time::OffsetDateTime::now_utc(),
    }) {
        tracing::debug!(%error, "could not publish routed-session edge failure");
    }
}

fn routed_session_remaining_connect_time(
    descriptor: &RoutedSessionDescriptor,
) -> Result<Duration, NetworkError> {
    let remaining = descriptor
        .deadline_unix_millis
        .saturating_sub(routed_session_now_unix_millis());
    if remaining <= 0 {
        return Err(NetworkError::AuthExpired);
    }
    Ok(Duration::from_millis(
        u64::try_from(remaining).unwrap_or(u64::MAX),
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RoutedRelayAdjacentEdge {
    Incoming,
    Outgoing,
}

struct RoutedRelayCopyError {
    edge: RoutedRelayAdjacentEdge,
    operation: &'static str,
    source: std::io::Error,
}

async fn copy_routed_relay_direction<R, W>(
    mut reader: R,
    reader_edge: RoutedRelayAdjacentEdge,
    mut writer: W,
    writer_edge: RoutedRelayAdjacentEdge,
) -> Result<u64, RoutedRelayCopyError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut copied = 0_u64;
    let mut buffer = vec![0_u8; ROUTED_SESSION_RELAY_BUFFER_BYTES];
    loop {
        let read = reader
            .read(&mut buffer)
            .await
            .map_err(|source| RoutedRelayCopyError {
                edge: reader_edge,
                operation: "read opaque routed-session relay bytes",
                source,
            })?;
        if read == 0 {
            writer
                .shutdown()
                .await
                .map_err(|source| RoutedRelayCopyError {
                    edge: writer_edge,
                    operation: "half-close opaque routed-session relay edge",
                    source,
                })?;
            return Ok(copied);
        }
        writer
            .write_all(&buffer[..read])
            .await
            .map_err(|source| RoutedRelayCopyError {
                edge: writer_edge,
                operation: "write opaque routed-session relay bytes",
                source,
            })?;
        copied = copied.saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
    }
}

fn routed_relay_copy_error(
    error: RoutedRelayCopyError,
    failure_sink: Option<&dyn ConnectivityFailureSink>,
    relay_node_id: &NodeId,
    next_node_id: &NodeId,
    endpoint_id: &Id,
) -> NetworkError {
    let detail = format!("{}: {}", error.operation, error.source);
    if error.edge == RoutedRelayAdjacentEdge::Outgoing {
        report_routed_edge_failure(
            failure_sink,
            relay_node_id,
            next_node_id,
            endpoint_id,
            &detail,
        );
    }
    NetworkError::Io {
        operation: "forward encrypted routed-session stream".to_string(),
        detail,
    }
}

/// Accepts a signed routed-session open at either an intermediate relay or the
/// named destination.
///
/// Relays validate the immutable path and both endpoint signatures, establish
/// exactly the next direct edge, and then copy only bounded encrypted frames.
/// They never construct a [`PeerStream`] or receive endpoint key material.
pub async fn accept_or_relay_routed_session(
    mut stream: AuthenticatedDirectStream,
    directory: &DirectorySnapshot,
    identity: &dyn RoutedSessionIdentity,
    link_provider: &dyn LinkProvider,
    failure_sink: Option<&dyn ConnectivityFailureSink>,
) -> Result<RoutedSessionDisposition, NetworkError> {
    if stream.local_node_id != *identity.local_node_id() {
        return Err(NetworkError::AuthDenied);
    }
    let open_frame = read_routed_session_frame(&mut *stream.io, ROUTED_SESSION_OPEN_FRAME).await?;
    let signed_open: SignedRoutedSessionOpen = routed_session_decode(&open_frame)?;
    let descriptor_transcript = verify_routed_session_open(directory, identity, &signed_open)?;
    let position = incoming_routed_session_position(&stream, &signed_open.descriptor)?;
    if position + 1 == signed_open.descriptor.path.len() {
        return accept_routed_session_destination(
            stream,
            identity,
            signed_open,
            descriptor_transcript,
        )
        .await
        .map(RoutedSessionDisposition::Destination);
    }

    let descriptor = &signed_open.descriptor;
    let relay_node_id = stream.local_node_id.clone();
    let next_node_id = descriptor.path[position + 1].clone();
    let endpoint_id = descriptor.endpoint_ids[position].clone();
    let endpoint = directory
        .node(&next_node_id)
        .and_then(|node| {
            node.endpoints
                .iter()
                .find(|endpoint| endpoint.endpoint_id == endpoint_id)
        })
        .cloned()
        .ok_or(NetworkError::AuthDenied)?;
    let connect_time = routed_session_remaining_connect_time(descriptor)?;
    enum RelayEstablishmentError {
        DirectEdge(NetworkError),
        DownstreamSession(NetworkError),
    }
    let direct_edge_established = Arc::new(AtomicBool::new(false));
    let establish_edge = Arc::clone(&direct_edge_established);
    let establish = async {
        let link = link_provider
            .connect(&endpoint)
            .await
            .map_err(RelayEstablishmentError::DirectEdge)?;
        if link.local_node_id() != &relay_node_id
            || link.peer_node_id() != &next_node_id
            || !link.capabilities().relay_forwarding
            || !link.capabilities().full_duplex
        {
            return Err(RelayEstablishmentError::DirectEdge(
                NetworkError::AuthDenied,
            ));
        }
        let mut outgoing = link
            .open_stream()
            .await
            .map_err(RelayEstablishmentError::DirectEdge)?;
        // From this point the direct edge has been authenticated and opened.
        // A missing acceptance is a downstream routed-session rejection (for
        // example while a newly admitted member is still materializing), not
        // evidence that this adjacent edge is unreachable.
        establish_edge.store(true, Ordering::Release);
        write_routed_session_frame(&mut *outgoing.io, ROUTED_SESSION_OPEN_FRAME, &open_frame)
            .await
            .map_err(RelayEstablishmentError::DownstreamSession)?;
        let acceptance_frame =
            read_routed_session_frame(&mut *outgoing.io, ROUTED_SESSION_ACCEPT_FRAME)
                .await
                .map_err(RelayEstablishmentError::DownstreamSession)?;
        let signed_acceptance: SignedRoutedSessionAcceptance =
            routed_session_decode(&acceptance_frame)
                .map_err(RelayEstablishmentError::DownstreamSession)?;
        verify_routed_session_acceptance(
            directory,
            identity,
            descriptor,
            &descriptor_transcript,
            &signed_acceptance,
        )
        .map_err(RelayEstablishmentError::DownstreamSession)?;
        Ok::<_, RelayEstablishmentError>((outgoing, acceptance_frame))
    };
    let (outgoing, acceptance_frame) = match tokio::time::timeout(connect_time, establish).await {
        Ok(Ok(established)) => established,
        Ok(Err(RelayEstablishmentError::DirectEdge(error))) => {
            report_routed_edge_failure(
                failure_sink,
                &relay_node_id,
                &next_node_id,
                &endpoint_id,
                &error.to_string(),
            );
            return Err(error);
        }
        Ok(Err(RelayEstablishmentError::DownstreamSession(error))) => return Err(error),
        Err(_) => {
            let error = NetworkError::timeout(
                if direct_edge_established.load(Ordering::Acquire) {
                    format!(
                        "await routed-session acceptance through {relay_node_id} -> {next_node_id}"
                    )
                } else {
                    format!("establish routed relay edge {relay_node_id} -> {next_node_id}")
                },
                NetworkTimeoutPhase::Connect,
                connect_time,
                NetworkDeliveryState::DefinitelyNotSent,
            );
            if !direct_edge_established.load(Ordering::Acquire) {
                report_routed_edge_failure(
                    failure_sink,
                    &relay_node_id,
                    &next_node_id,
                    &endpoint_id,
                    &error.to_string(),
                );
            }
            return Err(error);
        }
    };
    write_routed_session_frame(
        &mut *stream.io,
        ROUTED_SESSION_ACCEPT_FRAME,
        &acceptance_frame,
    )
    .await?;
    let (incoming_read, incoming_write) = tokio::io::split(stream.io);
    let (outgoing_read, outgoing_write) = tokio::io::split(outgoing.io);
    let toward_destination = copy_routed_relay_direction(
        incoming_read,
        RoutedRelayAdjacentEdge::Incoming,
        outgoing_write,
        RoutedRelayAdjacentEdge::Outgoing,
    );
    let toward_source = copy_routed_relay_direction(
        outgoing_read,
        RoutedRelayAdjacentEdge::Outgoing,
        incoming_write,
        RoutedRelayAdjacentEdge::Incoming,
    );
    tokio::pin!(toward_destination);
    tokio::pin!(toward_source);
    let (bytes_toward_destination, bytes_toward_source) = tokio::select! {
        result = &mut toward_destination => match result {
            Ok(bytes) => (
                bytes,
                toward_source.await.map_err(|error| routed_relay_copy_error(
                    error,
                    failure_sink,
                    &relay_node_id,
                    &next_node_id,
                    &endpoint_id,
                ))?,
            ),
            Err(error) => return Err(routed_relay_copy_error(
                error,
                failure_sink,
                &relay_node_id,
                &next_node_id,
                &endpoint_id,
            )),
        },
        result = &mut toward_source => match result {
            Ok(bytes) => (
                toward_destination.await.map_err(|error| routed_relay_copy_error(
                    error,
                    failure_sink,
                    &relay_node_id,
                    &next_node_id,
                    &endpoint_id,
                ))?,
                bytes,
            ),
            Err(error) => return Err(routed_relay_copy_error(
                error,
                failure_sink,
                &relay_node_id,
                &next_node_id,
                &endpoint_id,
            )),
        },
    };
    Ok(RoutedSessionDisposition::Relayed(RoutedRelayReport {
        session_id: descriptor.session_id.clone(),
        source_node_id: descriptor.source_node_id.clone(),
        destination_node_id: descriptor.destination_node_id.clone(),
        relay_node_id,
        next_node_id,
        bytes_toward_destination,
        bytes_toward_source,
    }))
}

pub type RoutedConnectFuture<'a> =
    Pin<Box<dyn Future<Output = Result<PeerStream, NetworkError>> + Send + 'a>>;

/// Sole node-addressed stream establishment boundary.
pub trait RoutedConnector: Send + Sync {
    fn connect<'a>(
        &'a self,
        destination_node_id: NodeId,
        options: ConnectOptions,
    ) -> RoutedConnectFuture<'a>;

    fn availability(&self, destination_node_id: &NodeId) -> RoutedAvailability;
}

/// Stage-C routed connector for one-edge paths.
///
/// It replaces endpoint selection plus raw stream opening as one atomic
/// operation: snapshots authorize and plan the path, the link provider proves
/// the adjacent peer, and the routed-session handshake proves the named
/// destination before the caller can exchange application bytes.
pub struct DirectRoutedConnector {
    local_node_id: NodeId,
    directory: Arc<dyn ClusterDirectory>,
    connectivity: Arc<dyn crate::connectivity_snapshot::ConnectivityView>,
    link_provider: Arc<dyn LinkProvider>,
    identity: Arc<dyn RoutedSessionIdentity>,
}

impl DirectRoutedConnector {
    pub fn new(
        local_node_id: NodeId,
        directory: Arc<dyn ClusterDirectory>,
        connectivity: Arc<dyn crate::connectivity_snapshot::ConnectivityView>,
        link_provider: Arc<dyn LinkProvider>,
        identity: Arc<dyn RoutedSessionIdentity>,
    ) -> Result<Self, NetworkError> {
        if identity.local_node_id() != &local_node_id {
            return Err(NetworkError::AuthDenied);
        }
        Ok(Self {
            local_node_id,
            directory,
            connectivity,
            link_provider,
            identity,
        })
    }

    async fn connect_inner(
        &self,
        destination_node_id: NodeId,
        options: ConnectOptions,
    ) -> Result<PeerStream, NetworkError> {
        let directory = self.directory.snapshot()?;
        if !directory.is_authorized(&self.local_node_id) {
            return Err(NetworkError::UnauthorizedNode {
                node_id: self.local_node_id.clone(),
            });
        }
        if !directory.is_authorized(&destination_node_id) {
            return Err(NetworkError::UnauthorizedNode {
                node_id: destination_node_id,
            });
        }
        let connectivity = self.connectivity.snapshot();
        let paths = plan_routes(
            &directory,
            &connectivity,
            &self.local_node_id,
            &destination_node_id,
            options.constraints,
        );
        let direct_paths = paths.into_iter().filter(|path| path.hops.len() == 1);
        let deadline_unix_millis = routed_session_deadline_unix_millis(options.timeout);
        let mut failures = Vec::new();
        let mut attempts = 0_usize;
        for path in direct_paths {
            let hop = &path.hops[0];
            attempts = attempts.saturating_add(1);
            let link = match self.link_provider.connect(&hop.endpoint).await {
                Ok(link) => link,
                Err(error) => {
                    failures.push(format!("{}: {error}", hop.endpoint.address));
                    continue;
                }
            };
            if link.local_node_id() != &self.local_node_id
                || link.peer_node_id() != &destination_node_id
                || !options
                    .constraints
                    .accepts_capabilities(link.capabilities())
            {
                failures.push(format!(
                    "{}: authenticated link identity or capabilities did not match route",
                    hop.endpoint.address
                ));
                continue;
            }
            let stream = match link.open_stream().await {
                Ok(stream) => stream,
                Err(error) => {
                    failures.push(format!("{}: {error}", hop.endpoint.address));
                    continue;
                }
            };
            match initiate_routed_session(
                stream,
                &directory,
                &path,
                self.identity.as_ref(),
                deadline_unix_millis,
            )
            .await
            {
                Ok(stream) => return Ok(stream),
                Err(error) => failures.push(format!("{}: {error}", hop.endpoint.address)),
            }
        }
        Err(NetworkError::RetryExhausted {
            operation: format!("establish direct routed session to {destination_node_id}"),
            attempts,
            delivery_state: NetworkDeliveryState::DefinitelyNotSent,
            detail: if failures.is_empty() {
                "no eligible one-edge path in the current snapshots".to_string()
            } else {
                failures.join("; ")
            },
        })
    }
}

impl RoutedConnector for DirectRoutedConnector {
    fn connect<'a>(
        &'a self,
        destination_node_id: NodeId,
        options: ConnectOptions,
    ) -> RoutedConnectFuture<'a> {
        Box::pin(async move {
            tokio::time::timeout(
                options.timeout,
                self.connect_inner(destination_node_id, options),
            )
            .await
            .map_err(|_| {
                NetworkError::timeout(
                    "establish direct routed session",
                    NetworkTimeoutPhase::Connect,
                    options.timeout,
                    NetworkDeliveryState::DefinitelyNotSent,
                )
            })?
        })
    }

    fn availability(&self, destination_node_id: &NodeId) -> RoutedAvailability {
        let Ok(directory) = self.directory.snapshot() else {
            return RoutedAvailability::Unknown;
        };
        if !directory.is_authorized(&self.local_node_id)
            || !directory.is_authorized(destination_node_id)
        {
            return RoutedAvailability::NoPath;
        }
        let connectivity = self.connectivity.snapshot();
        if plan_routes(
            &directory,
            &connectivity,
            &self.local_node_id,
            destination_node_id,
            RouteConstraints::default(),
        )
        .iter()
        .any(|path| path.hops.len() == 1)
        {
            RoutedAvailability::Candidate
        } else {
            RoutedAvailability::NoPath
        }
    }
}

/// Component-wide routed connector that realizes every eligible path emitted
/// by [`plan_routes`], including paths through intermediate relays.
pub struct MultiHopRoutedConnector {
    local_node_id: NodeId,
    directory: DirectorySnapshot,
    connectivity: Arc<dyn crate::connectivity_snapshot::ConnectivityView>,
    failure_sink: Arc<dyn ConnectivityFailureSink>,
    link_provider: Arc<dyn LinkProvider>,
    identity: Arc<dyn RoutedSessionIdentity>,
}

impl MultiHopRoutedConnector {
    pub fn new(
        local_node_id: NodeId,
        directory: Arc<dyn ClusterDirectory>,
        connectivity: Arc<dyn crate::connectivity_snapshot::ConnectivityView>,
        failure_sink: Arc<dyn ConnectivityFailureSink>,
        link_provider: Arc<dyn LinkProvider>,
        identity: Arc<dyn RoutedSessionIdentity>,
    ) -> Result<Self, NetworkError> {
        Self::from_snapshot(
            local_node_id,
            directory.snapshot()?,
            connectivity,
            failure_sink,
            link_provider,
            identity,
        )
    }

    pub fn from_snapshot(
        local_node_id: NodeId,
        directory: DirectorySnapshot,
        connectivity: Arc<dyn crate::connectivity_snapshot::ConnectivityView>,
        failure_sink: Arc<dyn ConnectivityFailureSink>,
        link_provider: Arc<dyn LinkProvider>,
        identity: Arc<dyn RoutedSessionIdentity>,
    ) -> Result<Self, NetworkError> {
        if identity.local_node_id() != &local_node_id {
            return Err(NetworkError::AuthDenied);
        }
        Ok(Self {
            local_node_id,
            directory,
            connectivity,
            failure_sink,
            link_provider,
            identity,
        })
    }

    async fn connect_inner(
        &self,
        destination_node_id: NodeId,
        options: ConnectOptions,
    ) -> Result<PeerStream, NetworkError> {
        let directory = &self.directory;
        if !directory.is_authorized(&self.local_node_id) {
            return Err(NetworkError::UnauthorizedNode {
                node_id: self.local_node_id.clone(),
            });
        }
        if !directory.is_authorized(&destination_node_id) {
            return Err(NetworkError::UnauthorizedNode {
                node_id: destination_node_id,
            });
        }
        let connectivity = self.connectivity.snapshot();
        let paths = plan_routes(
            directory,
            &connectivity,
            &self.local_node_id,
            &destination_node_id,
            options.constraints,
        );
        let deadline = tokio::time::Instant::now() + options.timeout;
        let path_count = paths.len();
        let mut attempts = 0_usize;
        let mut failures = Vec::new();
        for (path_index, path) in paths.into_iter().enumerate() {
            let Some(first_hop) = path.hops.first() else {
                continue;
            };
            attempts = attempts.saturating_add(1);
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let paths_left =
                u32::try_from(path_count.saturating_sub(path_index)).unwrap_or(u32::MAX);
            let attempt_timeout =
                routed_candidate_attempt_timeout(remaining, paths_left, path.hops.len());
            let attempt_deadline_unix_millis = routed_session_deadline_unix_millis(attempt_timeout);
            enum PathAttemptError {
                DirectEdge(NetworkError),
                RoutedSession(NetworkError),
            }
            let direct_edge_established = Arc::new(AtomicBool::new(false));
            let established = Arc::clone(&direct_edge_established);
            let attempt = async {
                let link = self
                    .link_provider
                    .connect(&first_hop.endpoint)
                    .await
                    .map_err(PathAttemptError::DirectEdge)?;
                if link.local_node_id() != &self.local_node_id
                    || link.peer_node_id() != &first_hop.target_node_id
                    || !options
                        .constraints
                        .accepts_capabilities(link.capabilities())
                    || (path.hops.len() > 1 && !link.capabilities().relay_forwarding)
                {
                    return Err(PathAttemptError::DirectEdge(NetworkError::AuthDenied));
                }
                let stream = link
                    .open_stream()
                    .await
                    .map_err(PathAttemptError::DirectEdge)?;
                established.store(true, Ordering::Release);
                initiate_routed_session(
                    stream,
                    directory,
                    &path,
                    self.identity.as_ref(),
                    attempt_deadline_unix_millis,
                )
                .await
                .map_err(PathAttemptError::RoutedSession)
            };
            match tokio::time::timeout(attempt_timeout, attempt).await {
                Ok(Ok(stream)) => return Ok(stream),
                Ok(Err(PathAttemptError::DirectEdge(error))) => {
                    report_routed_edge_failure(
                        Some(self.failure_sink.as_ref()),
                        &first_hop.source_node_id,
                        &first_hop.target_node_id,
                        &first_hop.endpoint.endpoint_id,
                        &error.to_string(),
                    );
                    failures.push(format!("{}: {error}", first_hop.endpoint.address));
                }
                Ok(Err(PathAttemptError::RoutedSession(error))) => failures.push(format!(
                    "path {}: {error}",
                    path.nodes
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(" -> ")
                )),
                Err(_) => {
                    let error = NetworkError::timeout(
                        format!(
                            "establish routed candidate path {}",
                            path.nodes
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                                .join(" -> ")
                        ),
                        NetworkTimeoutPhase::Connect,
                        attempt_timeout,
                        NetworkDeliveryState::DefinitelyNotSent,
                    );
                    if !direct_edge_established.load(Ordering::Acquire) {
                        report_routed_edge_failure(
                            Some(self.failure_sink.as_ref()),
                            &first_hop.source_node_id,
                            &first_hop.target_node_id,
                            &first_hop.endpoint.endpoint_id,
                            &error.to_string(),
                        );
                    }
                    failures.push(error.to_string());
                }
            }
        }
        Err(NetworkError::RetryExhausted {
            operation: format!("establish routed session to {destination_node_id}"),
            attempts,
            delivery_state: NetworkDeliveryState::DefinitelyNotSent,
            detail: if failures.is_empty() {
                "no executable path in the current snapshots".to_string()
            } else {
                failures.join("; ")
            },
        })
    }
}

fn routed_candidate_attempt_timeout(
    remaining: Duration,
    paths_left: u32,
    hop_count: usize,
) -> Duration {
    let paths_left = paths_left.max(1);
    let equal_share = remaining.checked_div(paths_left).unwrap_or(remaining);
    if paths_left == 1 {
        return remaining.max(Duration::from_millis(1));
    }

    // A routed-session open is verified and forwarded once per hop. Dividing
    // the operation deadline equally across every graph candidate can leave a
    // multi-hop candidate only a few milliseconds at its final relay when the
    // planner emits the full candidate set. Preserve a usable hop-scaled
    // handshake window while reserving at least half of the current budget for
    // another candidate. Fast authentication or dial failures still move to
    // the next path immediately.
    let hop_budget = ROUTED_CANDIDATE_PER_HOP_HANDSHAKE_BUDGET
        .checked_mul(u32::try_from(hop_count).unwrap_or(u32::MAX))
        .unwrap_or(Duration::MAX)
        .saturating_add(ROUTED_CANDIDATE_BASE_HANDSHAKE_BUDGET);
    let fallback_preserving_cap = remaining.checked_div(2).unwrap_or(remaining);
    equal_share
        .max(hop_budget.min(fallback_preserving_cap))
        .min(remaining)
        .max(Duration::from_millis(1))
}

impl RoutedConnector for MultiHopRoutedConnector {
    fn connect<'a>(
        &'a self,
        destination_node_id: NodeId,
        options: ConnectOptions,
    ) -> RoutedConnectFuture<'a> {
        Box::pin(async move {
            tokio::time::timeout(
                options.timeout,
                self.connect_inner(destination_node_id, options),
            )
            .await
            .map_err(|_| {
                NetworkError::timeout(
                    "establish multi-hop routed session",
                    NetworkTimeoutPhase::Connect,
                    options.timeout,
                    NetworkDeliveryState::DefinitelyNotSent,
                )
            })?
        })
    }

    fn availability(&self, destination_node_id: &NodeId) -> RoutedAvailability {
        let directory = &self.directory;
        if !directory.is_authorized(&self.local_node_id)
            || !directory.is_authorized(destination_node_id)
        {
            return RoutedAvailability::NoPath;
        }
        let connectivity = self.connectivity.snapshot();
        if plan_routes(
            directory,
            &connectivity,
            &self.local_node_id,
            destination_node_id,
            RouteConstraints::default(),
        )
        .is_empty()
        {
            RoutedAvailability::NoPath
        } else {
            RoutedAvailability::Candidate
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectivity::ClusterConnectivityRecord;
    use crate::connectivity_snapshot::{
        ConnectivityGeneration, ConnectivityTracker, ConnectivityView,
        DirectConnectivityObservation,
    };
    use crate::direct_link::{DirectLinkFuture, DirectStreamCapabilities};
    use crate::directory::{DirectoryNode, DirectoryVersion};
    use sinomo_base::{
        ClusterEndpointPrivacy, ClusterEndpointSource, ClusterMemberRecord,
        ClusterNodeStatusRecord, ClusterTransportKind,
    };
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use time::OffsetDateTime;
    use tokio::sync::Notify;

    const TEST_ROUTED_PAYLOAD: &[u8; 32] = b"opaque-multi-hop-payload-1234567";

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).expect("node id")
    }

    fn edge(
        source: &NodeId,
        target: &NodeId,
        name: &str,
        directory_generation: u64,
        latency_micros: u64,
    ) -> DirectConnectivityObservation {
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1);
        DirectConnectivityObservation {
            observer_node_id: source.clone(),
            source_node_id: source.clone(),
            target_node_id: target.clone(),
            endpoint: ClusterEndpointRecord {
                endpoint_id: Id::from_legacy_named(name).expect("endpoint id"),
                node_id: target.clone(),
                transport: ClusterTransportKind::LanQuic,
                address: format!("quic://{name}"),
                source: ClusterEndpointSource::Observed,
                privacy: ClusterEndpointPrivacy::Lan,
                priority: 10,
                last_observed_latency_ms: None,
            },
            capabilities: DirectStreamCapabilities::multiplexed_full_duplex(64),
            state: DirectConnectivityState::Reachable,
            failure_reason: None,
            latency_micros: Some(latency_micros),
            delivered_bytes_per_second: None,
            provenance: "routing-test".to_string(),
            source_generation: 1,
            directory_generation,
            observed_at: now - time::Duration::seconds(1),
            expires_at: now + time::Duration::minutes(1),
        }
    }

    fn fixtures(
        directory_generation: u64,
        edges: Vec<DirectConnectivityObservation>,
    ) -> (DirectorySnapshot, ConnectivitySnapshot) {
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1);
        let mut endpoints = BTreeMap::<NodeId, Vec<ClusterEndpointRecord>>::new();
        for edge in &edges {
            endpoints
                .entry(edge.target_node_id.clone())
                .or_default()
                .push(edge.endpoint.clone());
            endpoints.entry(edge.source_node_id.clone()).or_default();
        }
        let directory = DirectorySnapshot::try_new(
            DirectoryVersion(directory_generation),
            endpoints
                .into_iter()
                .map(|(node_id, endpoints)| {
                    let public_key = node_id.to_string();
                    DirectoryNode::try_new(node_id, public_key, endpoints).expect("directory node")
                })
                .collect::<Vec<_>>(),
        )
        .expect("directory");
        let connectivity = ConnectivitySnapshot::try_new(
            ConnectivityGeneration(1),
            directory_generation,
            now,
            edges,
        )
        .expect("connectivity");
        (directory, connectivity)
    }

    #[test]
    fn planner_prefers_direct_then_deterministic_relay_paths() {
        let a = node("node_route_plan_a");
        let b = node("node_route_plan_b");
        let c = node("node_route_plan_c");
        let (directory, connectivity) = fixtures(
            7,
            vec![
                edge(&a, &c, "endpoint_route_a_c", 7, 30),
                edge(&a, &b, "endpoint_route_a_b", 7, 10),
                edge(&b, &c, "endpoint_route_b_c", 7, 10),
            ],
        );

        let paths = plan_routes(
            &directory,
            &connectivity,
            &a,
            &c,
            RouteConstraints::default(),
        );

        assert_eq!(paths.len(), 2);
        assert_eq!(paths[0].nodes, vec![a.clone(), c.clone()]);
        assert_eq!(paths[1].nodes, vec![a, b, c]);
    }

    #[test]
    fn planner_prefers_healthy_relay_but_keeps_degraded_direct_fallback() {
        let a = node("node_route_degraded_a");
        let b = node("node_route_degraded_b");
        let c = node("node_route_degraded_c");
        let mut degraded_direct = edge(&a, &c, "endpoint_route_degraded_a_c", 11, 1);
        degraded_direct.state = DirectConnectivityState::Degraded;
        let (directory, connectivity) = fixtures(
            11,
            vec![
                degraded_direct,
                edge(&a, &b, "endpoint_route_degraded_a_b", 11, 10),
                edge(&b, &c, "endpoint_route_degraded_b_c", 11, 10),
            ],
        );

        let paths = plan_routes(
            &directory,
            &connectivity,
            &a,
            &c,
            RouteConstraints::default(),
        );

        assert_eq!(paths.len(), 2);
        assert_eq!(paths[0].nodes, vec![a.clone(), b, c.clone()]);
        assert_eq!(paths[1].nodes, vec![a, c]);
        assert_eq!(paths[1].degraded_hop_count(), 1);
    }

    #[test]
    fn planner_preserves_direction_and_never_emits_loops() {
        let a = node("node_route_directed_a");
        let b = node("node_route_directed_b");
        let c = node("node_route_directed_c");
        let (directory, connectivity) = fixtures(
            3,
            vec![
                edge(&a, &b, "endpoint_route_directed_a_b", 3, 10),
                edge(&b, &a, "endpoint_route_directed_b_a", 3, 10),
                edge(&b, &c, "endpoint_route_directed_b_c", 3, 10),
            ],
        );

        let paths = plan_routes(
            &directory,
            &connectivity,
            &a,
            &c,
            RouteConstraints::default(),
        );
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].nodes, vec![a.clone(), b.clone(), c.clone()]);
        assert_eq!(
            plan_routes(
                &directory,
                &connectivity,
                &c,
                &a,
                RouteConstraints::default(),
            ),
            Vec::<CandidatePath>::new()
        );
        for path in paths {
            let mut sorted = path.nodes.clone();
            sorted.sort();
            sorted.dedup();
            assert_eq!(sorted.len(), path.nodes.len());
        }
    }

    #[test]
    fn planner_rejects_stale_directory_and_missing_capability() {
        let a = node("node_route_stale_a");
        let b = node("node_route_stale_b");
        let mut direct = edge(&a, &b, "endpoint_route_stale_a_b", 5, 10);
        direct.capabilities.confidential = false;
        let (directory, connectivity) = fixtures(5, vec![direct]);
        assert!(
            plan_routes(
                &directory,
                &connectivity,
                &a,
                &b,
                RouteConstraints::default(),
            )
            .is_empty()
        );

        let stale_directory = DirectorySnapshot::try_new(
            DirectoryVersion(6),
            directory.nodes().values().cloned().collect::<Vec<_>>(),
        )
        .expect("stale directory");
        let relaxed = RouteConstraints {
            require_confidentiality: false,
            ..RouteConstraints::default()
        };
        assert!(plan_routes(&stale_directory, &connectivity, &a, &b, relaxed).is_empty());
    }

    #[test]
    fn planner_never_emits_a_relay_path_over_a_non_forwarding_link() {
        let a = node("node_route_relay_capability_a");
        let b = node("node_route_relay_capability_b");
        let c = node("node_route_relay_capability_c");
        let mut first = edge(&a, &b, "endpoint_route_relay_capability_a_b", 8, 10);
        first.capabilities.relay_forwarding = false;
        let second = edge(&b, &c, "endpoint_route_relay_capability_b_c", 8, 10);
        let (directory, connectivity) = fixtures(8, vec![first, second]);

        assert!(
            plan_routes(
                &directory,
                &connectivity,
                &a,
                &c,
                RouteConstraints::default(),
            )
            .is_empty()
        );
    }

    #[test]
    fn planner_never_emits_a_path_longer_than_the_session_protocol_limit() {
        let nodes = (0..18)
            .map(|index| node(&format!("node_route_protocol_limit_{index}")))
            .collect::<Vec<_>>();
        let edges = nodes
            .windows(2)
            .enumerate()
            .map(|(index, pair)| {
                edge(
                    &pair[0],
                    &pair[1],
                    &format!("endpoint_route_protocol_limit_{index}"),
                    9,
                    1,
                )
            })
            .collect();
        let (directory, connectivity) = fixtures(9, edges);
        let paths = plan_routes(
            &directory,
            &connectivity,
            &nodes[0],
            &nodes[17],
            RouteConstraints {
                max_hops: usize::MAX,
                ..RouteConstraints::default()
            },
        );
        assert!(paths.is_empty());
    }

    #[derive(Clone)]
    struct FixedDirectory(DirectorySnapshot);

    impl ClusterDirectory for FixedDirectory {
        fn members(&self) -> Result<Vec<ClusterMemberRecord>, NetworkError> {
            Ok(Vec::new())
        }

        fn endpoints(&self) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
            Ok(Vec::new())
        }

        fn node_statuses(&self) -> Result<BTreeMap<NodeId, ClusterNodeStatusRecord>, NetworkError> {
            Ok(BTreeMap::new())
        }

        fn connectivity_records(&self) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
            Ok(Vec::new())
        }

        fn version(&self) -> Result<DirectoryVersion, NetworkError> {
            Ok(self.0.version())
        }

        fn snapshot(&self) -> Result<DirectorySnapshot, NetworkError> {
            Ok(self.0.clone())
        }
    }

    struct ChangingDirectory {
        first: DirectorySnapshot,
        later: DirectorySnapshot,
        snapshots: AtomicUsize,
    }

    impl ClusterDirectory for ChangingDirectory {
        fn members(&self) -> Result<Vec<ClusterMemberRecord>, NetworkError> {
            panic!("changing test directory must be consumed through snapshot")
        }

        fn endpoints(&self) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
            panic!("changing test directory must be consumed through snapshot")
        }

        fn node_statuses(&self) -> Result<BTreeMap<NodeId, ClusterNodeStatusRecord>, NetworkError> {
            Ok(BTreeMap::new())
        }

        fn connectivity_records(&self) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
            Ok(Vec::new())
        }

        fn version(&self) -> Result<DirectoryVersion, NetworkError> {
            panic!("changing test directory must be consumed through snapshot")
        }

        fn snapshot(&self) -> Result<DirectorySnapshot, NetworkError> {
            let call = self.snapshots.fetch_add(1, Ordering::SeqCst);
            Ok(if call == 0 {
                self.first.clone()
            } else {
                self.later.clone()
            })
        }
    }

    #[derive(Clone)]
    struct TestRoutedIdentity {
        local_node_id: NodeId,
    }

    impl TestRoutedIdentity {
        fn signature_key(node_id: &NodeId) -> [u8; 32] {
            *blake3::hash(node_id.as_str().as_bytes()).as_bytes()
        }
    }

    impl RoutedSessionIdentity for TestRoutedIdentity {
        fn local_node_id(&self) -> &NodeId {
            &self.local_node_id
        }

        fn sign(&self, transcript: &[u8]) -> Result<Vec<u8>, NetworkError> {
            Ok(
                blake3::keyed_hash(&Self::signature_key(&self.local_node_id), transcript)
                    .as_bytes()
                    .to_vec(),
            )
        }

        fn verify(
            &self,
            signer_node_id: &NodeId,
            authorized_public_key: &str,
            transcript: &[u8],
            signature: &[u8],
        ) -> Result<(), NetworkError> {
            let expected = blake3::keyed_hash(&Self::signature_key(signer_node_id), transcript);
            if authorized_public_key == signer_node_id.as_str()
                && expected.as_bytes().as_slice() == signature
            {
                Ok(())
            } else {
                Err(NetworkError::AuthDenied)
            }
        }
    }

    struct TestDirectLinkProvider {
        local_node_id: NodeId,
        server_directory: DirectorySnapshot,
        server_identity: Arc<TestRoutedIdentity>,
        attempts: Arc<Mutex<Vec<String>>>,
        accepted_bindings: Arc<Mutex<Vec<[u8; 32]>>>,
        close_after_accept: bool,
    }

    struct RecordingStream {
        inner: tokio::io::DuplexStream,
        writes: Arc<Mutex<Vec<Vec<u8>>>>,
        tamper_next_data_frame: Arc<AtomicBool>,
        encrypted_payload_pending: bool,
    }

    impl AsyncRead for RecordingStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for RecordingStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, std::io::Error>> {
            let mut altered = Vec::new();
            let write = if self.encrypted_payload_pending
                && self.tamper_next_data_frame.swap(false, Ordering::SeqCst)
                && !buf.is_empty()
            {
                altered.extend_from_slice(buf);
                altered[0] ^= 1;
                &altered[..]
            } else {
                buf
            };
            match Pin::new(&mut self.inner).poll_write(cx, write) {
                Poll::Ready(Ok(written)) => {
                    if self.encrypted_payload_pending && written > 0 {
                        self.encrypted_payload_pending = false;
                    } else if written == buf.len()
                        && buf.starts_with(&ROUTED_SESSION_DATA_WIRE_PREFIX)
                    {
                        self.encrypted_payload_pending = true;
                    }
                    self.writes
                        .lock()
                        .expect("wire writes lock")
                        .push(write[..written].to_vec());
                    Poll::Ready(Ok(written))
                }
                other => other,
            }
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    #[derive(Clone)]
    struct TestRelayNetwork {
        directory: Arc<DirectorySnapshot>,
        connectivity: Arc<ConnectivityTracker>,
        wire_writes: Arc<Mutex<Vec<Vec<u8>>>>,
        dialed_edges: Arc<Mutex<Vec<(NodeId, NodeId, Id)>>>,
        destination_routes: Arc<Mutex<Vec<RouteTrace>>>,
        relay_reports: Arc<Mutex<Vec<RoutedRelayReport>>>,
        errors: Arc<Mutex<Vec<String>>>,
        failed_endpoints: Arc<Mutex<BTreeSet<Id>>>,
        stalled_endpoints: Arc<Mutex<BTreeSet<Id>>>,
        delayed_endpoints: Arc<Mutex<BTreeMap<Id, Duration>>>,
        rejected_origins: Arc<Mutex<BTreeMap<NodeId, BTreeSet<NodeId>>>>,
        blocked_destinations: Arc<Mutex<BTreeSet<NodeId>>>,
        blocked_destination_release: Arc<Notify>,
        tamper_next_data_frame: Arc<AtomicBool>,
        accepted_payloads: Arc<AtomicUsize>,
        active_tasks: Arc<AtomicUsize>,
        idle: Arc<Notify>,
    }

    impl TestRelayNetwork {
        fn new(directory: DirectorySnapshot, connectivity: ConnectivitySnapshot) -> Self {
            Self {
                directory: Arc::new(directory),
                connectivity: Arc::new(ConnectivityTracker::new(connectivity)),
                wire_writes: Arc::new(Mutex::new(Vec::new())),
                dialed_edges: Arc::new(Mutex::new(Vec::new())),
                destination_routes: Arc::new(Mutex::new(Vec::new())),
                relay_reports: Arc::new(Mutex::new(Vec::new())),
                errors: Arc::new(Mutex::new(Vec::new())),
                failed_endpoints: Arc::new(Mutex::new(BTreeSet::new())),
                stalled_endpoints: Arc::new(Mutex::new(BTreeSet::new())),
                delayed_endpoints: Arc::new(Mutex::new(BTreeMap::new())),
                rejected_origins: Arc::new(Mutex::new(BTreeMap::new())),
                blocked_destinations: Arc::new(Mutex::new(BTreeSet::new())),
                blocked_destination_release: Arc::new(Notify::new()),
                tamper_next_data_frame: Arc::new(AtomicBool::new(false)),
                accepted_payloads: Arc::new(AtomicUsize::new(0)),
                active_tasks: Arc::new(AtomicUsize::new(0)),
                idle: Arc::new(Notify::new()),
            }
        }

        fn provider(&self, local_node_id: NodeId) -> TestRelayLinkProvider {
            TestRelayLinkProvider {
                local_node_id,
                network: self.clone(),
            }
        }

        fn spawn_accept(&self, stream: AuthenticatedDirectStream) {
            self.active_tasks.fetch_add(1, Ordering::SeqCst);
            let network = self.clone();
            tokio::spawn(async move {
                let local_node_id = stream.local_node_id.clone();
                let rejected_origins = network
                    .rejected_origins
                    .lock()
                    .expect("rejected origins lock")
                    .get(&local_node_id)
                    .cloned()
                    .unwrap_or_default();
                let directory = DirectorySnapshot::try_new(
                    network.directory.version(),
                    network
                        .directory
                        .nodes()
                        .values()
                        .filter(|node| !rejected_origins.contains(&node.node_id))
                        .cloned()
                        .collect::<Vec<_>>(),
                )
                .expect("node-specific test directory");
                let identity = TestRoutedIdentity {
                    local_node_id: local_node_id.clone(),
                };
                let provider = network.provider(local_node_id.clone());
                match accept_or_relay_routed_session(
                    stream,
                    &directory,
                    &identity,
                    &provider,
                    Some(network.connectivity.as_ref()),
                )
                .await
                {
                    Ok(RoutedSessionDisposition::Destination(mut stream)) => {
                        network
                            .destination_routes
                            .lock()
                            .expect("destination routes lock")
                            .push(stream.route_trace.clone());
                        let blocked = network
                            .blocked_destinations
                            .lock()
                            .expect("blocked destinations lock")
                            .contains(&local_node_id);
                        if blocked {
                            network.blocked_destination_release.notified().await;
                            let _ = stream.shutdown().await;
                        } else {
                            let mut payload = [0_u8; TEST_ROUTED_PAYLOAD.len()];
                            match stream.read_exact(&mut payload).await {
                                Ok(_) if payload == *TEST_ROUTED_PAYLOAD => {
                                    network.accepted_payloads.fetch_add(1, Ordering::SeqCst);
                                    if let Err(error) = stream.write_all(&payload).await {
                                        network
                                            .errors
                                            .lock()
                                            .expect("relay errors lock")
                                            .push(error.to_string());
                                    }
                                    let _ = stream.shutdown().await;
                                }
                                Ok(_) => network
                                    .errors
                                    .lock()
                                    .expect("relay errors lock")
                                    .push("destination received altered payload".to_string()),
                                Err(error) => network
                                    .errors
                                    .lock()
                                    .expect("relay errors lock")
                                    .push(error.to_string()),
                            }
                        }
                    }
                    Ok(RoutedSessionDisposition::Relayed(report)) => network
                        .relay_reports
                        .lock()
                        .expect("relay reports lock")
                        .push(report),
                    Err(error) => network
                        .errors
                        .lock()
                        .expect("relay errors lock")
                        .push(error.to_string()),
                }
                if network.active_tasks.fetch_sub(1, Ordering::SeqCst) == 1 {
                    network.idle.notify_waiters();
                }
            });
        }

        async fn wait_idle(&self) {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let notified = self.idle.notified();
                    if self.active_tasks.load(Ordering::SeqCst) == 0 {
                        return;
                    }
                    notified.await;
                }
            })
            .await
            .expect("relay tasks became idle");
        }
    }

    struct TestRelayLinkProvider {
        local_node_id: NodeId,
        network: TestRelayNetwork,
    }

    impl LinkProvider for TestRelayLinkProvider {
        fn connect<'a>(
            &'a self,
            endpoint: &'a ClusterEndpointRecord,
        ) -> DirectLinkFuture<'a, crate::direct_link::AuthenticatedLink> {
            let endpoint = endpoint.clone();
            let local_node_id = self.local_node_id.clone();
            let network = self.network.clone();
            Box::pin(async move {
                network
                    .dialed_edges
                    .lock()
                    .expect("dialed edges lock")
                    .push((
                        local_node_id.clone(),
                        endpoint.node_id.clone(),
                        endpoint.endpoint_id.clone(),
                    ));
                if network
                    .failed_endpoints
                    .lock()
                    .expect("failed endpoints lock")
                    .contains(&endpoint.endpoint_id)
                {
                    return Err(NetworkError::Io {
                        operation: "test routed relay dial".to_string(),
                        detail: "synthetic edge rejection".to_string(),
                    });
                }
                if network
                    .stalled_endpoints
                    .lock()
                    .expect("stalled endpoints lock")
                    .contains(&endpoint.endpoint_id)
                {
                    std::future::pending::<()>().await;
                }
                let delay = network
                    .delayed_endpoints
                    .lock()
                    .expect("delayed endpoints lock")
                    .get(&endpoint.endpoint_id)
                    .copied();
                if let Some(delay) = delay {
                    tokio::time::sleep(delay).await;
                }
                let peer_node_id = endpoint.node_id.clone();
                let link_id = Id::derived(
                    "sinomo.test.routed-relay-link",
                    format!(
                        "{}\0{}\0{}",
                        local_node_id, peer_node_id, endpoint.endpoint_id
                    )
                    .as_bytes(),
                );
                let link_channel_binding = *blake3::hash(link_id.as_str().as_bytes()).as_bytes();
                let capabilities = DirectStreamCapabilities::multiplexed_full_duplex(64);
                crate::direct_link::AuthenticatedLink::new(
                    local_node_id.clone(),
                    peer_node_id.clone(),
                    link_id.clone(),
                    link_channel_binding,
                    capabilities,
                    move || {
                        let network = network.clone();
                        let local_node_id = local_node_id.clone();
                        let peer_node_id = peer_node_id.clone();
                        let link_id = link_id.clone();
                        Box::pin(async move {
                            let (client, server) = tokio::io::duplex(256 * 1024);
                            let client = RecordingStream {
                                inner: client,
                                writes: Arc::clone(&network.wire_writes),
                                tamper_next_data_frame: Arc::clone(&network.tamper_next_data_frame),
                                encrypted_payload_pending: false,
                            };
                            let server = RecordingStream {
                                inner: server,
                                writes: Arc::clone(&network.wire_writes),
                                tamper_next_data_frame: Arc::clone(&network.tamper_next_data_frame),
                                encrypted_payload_pending: false,
                            };
                            network.spawn_accept(AuthenticatedDirectStream {
                                io: Box::new(server),
                                local_node_id: peer_node_id,
                                peer_node_id: local_node_id,
                                link_id,
                                stream_id: Id::generate(),
                                link_channel_binding,
                                channel_binding: [0; 32],
                                capabilities,
                            });
                            Ok(Box::new(client) as BoxedNetworkStream)
                        })
                    },
                )
            })
        }

        fn accept(&self) -> DirectLinkFuture<'_, crate::direct_link::AuthenticatedLink> {
            Box::pin(async {
                Err(NetworkError::Io {
                    operation: "test routed relay accept".to_string(),
                    detail: "test provider accepts streams inside its opener".to_string(),
                })
            })
        }
    }

    impl LinkProvider for TestDirectLinkProvider {
        fn connect<'a>(
            &'a self,
            endpoint: &'a ClusterEndpointRecord,
        ) -> DirectLinkFuture<'a, crate::direct_link::AuthenticatedLink> {
            let endpoint = endpoint.clone();
            let local_node_id = self.local_node_id.clone();
            let server_directory = self.server_directory.clone();
            let server_identity = Arc::clone(&self.server_identity);
            let attempts = Arc::clone(&self.attempts);
            let accepted_bindings = Arc::clone(&self.accepted_bindings);
            let close_after_accept = self.close_after_accept;
            Box::pin(async move {
                attempts
                    .lock()
                    .expect("attempt lock")
                    .push(endpoint.address.clone());
                if endpoint.address.contains("bad") {
                    return Err(NetworkError::Io {
                        operation: "test direct dial".to_string(),
                        detail: "synthetic endpoint failure".to_string(),
                    });
                }
                let peer_node_id = endpoint.node_id.clone();
                let link_id = Id::from_legacy_named("link_direct_routed_test")
                    .expect("direct routed link id");
                let link_channel_binding = *blake3::hash(endpoint.address.as_bytes()).as_bytes();
                crate::direct_link::AuthenticatedLink::new(
                    local_node_id.clone(),
                    peer_node_id.clone(),
                    link_id.clone(),
                    link_channel_binding,
                    DirectStreamCapabilities::multiplexed_full_duplex(16),
                    move || {
                        let local_node_id = local_node_id.clone();
                        let peer_node_id = peer_node_id.clone();
                        let link_id = link_id.clone();
                        let server_directory = server_directory.clone();
                        let server_identity = Arc::clone(&server_identity);
                        let accepted_bindings = Arc::clone(&accepted_bindings);
                        Box::pin(async move {
                            let (client, server) = tokio::io::duplex(1024 * 1024);
                            let server_stream = AuthenticatedDirectStream {
                                io: Box::new(server),
                                local_node_id: peer_node_id,
                                peer_node_id: local_node_id,
                                link_id,
                                stream_id: Id::generate(),
                                link_channel_binding,
                                channel_binding: [0; 32],
                                capabilities: DirectStreamCapabilities::multiplexed_full_duplex(16),
                            };
                            tokio::spawn(async move {
                                let Ok(mut stream) = accept_direct_routed_session(
                                    server_stream,
                                    &server_directory,
                                    server_identity.as_ref(),
                                )
                                .await
                                else {
                                    return;
                                };
                                accepted_bindings
                                    .lock()
                                    .expect("binding lock")
                                    .push(stream.channel_binding);
                                if close_after_accept {
                                    let _ = stream.shutdown().await;
                                    return;
                                }
                                let mut request = [0_u8; 4];
                                if stream.read_exact(&mut request).await.is_ok()
                                    && request == *b"ping"
                                {
                                    let _ = stream.write_all(b"pong").await;
                                    let _ = stream.shutdown().await;
                                }
                            });
                            Ok(Box::new(client) as BoxedNetworkStream)
                        })
                    },
                )
            })
        }

        fn accept(&self) -> DirectLinkFuture<'_, crate::direct_link::AuthenticatedLink> {
            Box::pin(async {
                Err(NetworkError::Io {
                    operation: "test direct accept".to_string(),
                    detail: "test provider accepts streams inside its opener".to_string(),
                })
            })
        }
    }

    #[tokio::test]
    async fn direct_connector_tries_alternative_endpoints_and_binds_peer_before_bytes() {
        let source = node("node_direct_connector_source");
        let destination = node("node_direct_connector_destination");
        let mut bad = edge(
            &source,
            &destination,
            "endpoint_direct_connector_01_bad",
            11,
            1,
        );
        bad.endpoint.address = "quic://bad".to_string();
        let mut good = edge(
            &source,
            &destination,
            "endpoint_direct_connector_02_good",
            11,
            2,
        );
        good.endpoint.address = "quic://good".to_string();
        let (directory, connectivity) = fixtures(11, vec![bad, good]);
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let accepted_bindings = Arc::new(Mutex::new(Vec::new()));
        let server_identity = Arc::new(TestRoutedIdentity {
            local_node_id: destination.clone(),
        });
        let provider = Arc::new(TestDirectLinkProvider {
            local_node_id: source.clone(),
            server_directory: directory.clone(),
            server_identity,
            attempts: Arc::clone(&attempts),
            accepted_bindings: Arc::clone(&accepted_bindings),
            close_after_accept: false,
        });
        let connector = DirectRoutedConnector::new(
            source,
            Arc::new(FixedDirectory(directory)),
            Arc::new(ConnectivityTracker::new(connectivity)),
            provider,
            Arc::new(TestRoutedIdentity {
                local_node_id: node("node_direct_connector_source"),
            }),
        )
        .expect("direct connector");

        assert_eq!(
            connector.availability(&destination),
            RoutedAvailability::Candidate
        );
        let mut stream = connector
            .connect(destination, ConnectOptions::default())
            .await
            .expect("routed stream");
        stream.write_all(b"ping").await.expect("write ping");
        let mut response = [0_u8; 4];
        stream.read_exact(&mut response).await.expect("read pong");
        assert_eq!(&response, b"pong");
        assert_eq!(
            attempts.lock().expect("attempt lock").as_slice(),
            ["quic://bad", "quic://good"]
        );
        assert_eq!(
            accepted_bindings.lock().expect("binding lock").as_slice(),
            [stream.channel_binding]
        );
    }

    #[tokio::test]
    async fn direct_connector_rejects_removed_node_without_dialing() {
        let source = node("node_direct_removed_source");
        let authorized_destination = node("node_direct_removed_authorized");
        let removed_destination = node("node_direct_removed_target");
        let observation = edge(
            &source,
            &authorized_destination,
            "endpoint_direct_removed_authorized",
            13,
            1,
        );
        let (directory, connectivity) = fixtures(13, vec![observation]);
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let provider = Arc::new(TestDirectLinkProvider {
            local_node_id: source.clone(),
            server_directory: directory.clone(),
            server_identity: Arc::new(TestRoutedIdentity {
                local_node_id: authorized_destination,
            }),
            attempts: Arc::clone(&attempts),
            accepted_bindings: Arc::new(Mutex::new(Vec::new())),
            close_after_accept: false,
        });
        let connector = DirectRoutedConnector::new(
            source.clone(),
            Arc::new(FixedDirectory(directory)),
            Arc::new(ConnectivityTracker::new(connectivity)),
            provider,
            Arc::new(TestRoutedIdentity {
                local_node_id: source,
            }),
        )
        .expect("direct connector");

        let result = connector
            .connect(removed_destination.clone(), ConnectOptions::default())
            .await;
        let error = match result {
            Ok(_) => panic!("removed destination was connected"),
            Err(error) => error,
        };
        assert_eq!(
            error,
            NetworkError::UnauthorizedNode {
                node_id: removed_destination,
            }
        );
        assert!(attempts.lock().expect("attempt lock").is_empty());
    }

    #[tokio::test]
    async fn returned_peer_stream_failure_never_redials_or_replays_bytes() {
        let source = node("node_direct_failure_source");
        let destination = node("node_direct_failure_destination");
        let observation = edge(&source, &destination, "endpoint_direct_failure", 14, 1);
        let (directory, connectivity) = fixtures(14, vec![observation]);
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let provider = Arc::new(TestDirectLinkProvider {
            local_node_id: source.clone(),
            server_directory: directory.clone(),
            server_identity: Arc::new(TestRoutedIdentity {
                local_node_id: destination.clone(),
            }),
            attempts: Arc::clone(&attempts),
            accepted_bindings: Arc::new(Mutex::new(Vec::new())),
            close_after_accept: true,
        });
        let connector = DirectRoutedConnector::new(
            source.clone(),
            Arc::new(FixedDirectory(directory)),
            Arc::new(ConnectivityTracker::new(connectivity)),
            provider,
            Arc::new(TestRoutedIdentity {
                local_node_id: source,
            }),
        )
        .expect("direct connector");
        let mut stream = connector
            .connect(destination, ConnectOptions::default())
            .await
            .expect("routed stream");

        let mut byte = [0_u8; 1];
        assert_eq!(stream.read(&mut byte).await.expect("peer close"), 0);
        let _ = stream.write_all(b"must not replay").await;
        tokio::task::yield_now().await;
        assert_eq!(attempts.lock().expect("attempt lock").len(), 1);
    }

    #[tokio::test]
    async fn direct_accept_rejects_destination_mismatch_before_returning_stream() {
        let source = node("node_direct_accept_source");
        let requested_destination = node("node_direct_accept_requested");
        let actual_destination = node("node_direct_accept_actual");
        let endpoint = edge(
            &source,
            &requested_destination,
            "endpoint_direct_accept_requested",
            12,
            1,
        )
        .endpoint;
        let directory = DirectorySnapshot::try_new(
            DirectoryVersion(12),
            vec![
                DirectoryNode::try_new(source.clone(), source.to_string(), Vec::new())
                    .expect("source node"),
                DirectoryNode::try_new(
                    requested_destination.clone(),
                    requested_destination.to_string(),
                    vec![endpoint.clone()],
                )
                .expect("requested node"),
                DirectoryNode::try_new(
                    actual_destination.clone(),
                    actual_destination.to_string(),
                    Vec::new(),
                )
                .expect("actual node"),
            ],
        )
        .expect("directory");
        let identity = TestRoutedIdentity {
            local_node_id: source.clone(),
        };
        let ephemeral = routed_session_ephemeral_key().expect("source ephemeral key");
        let descriptor = RoutedSessionDescriptor {
            protocol: ROUTED_SESSION_PROTOCOL.to_string(),
            source_node_id: source.clone(),
            destination_node_id: requested_destination.clone(),
            session_id: Id::generate(),
            path: vec![source.clone(), requested_destination.clone()],
            endpoint_ids: vec![endpoint.endpoint_id],
            source_ephemeral_public_key: ephemeral.public,
            hop_limit: 1,
            deadline_unix_millis: routed_session_deadline_unix_millis(Duration::from_secs(10)),
        };
        let transcript = routed_session_encode(&descriptor).expect("descriptor transcript");
        let open = SignedRoutedSessionOpen {
            signature: identity.sign(&transcript).expect("open signature"),
            descriptor,
        };
        let open = routed_session_encode(&open).expect("open frame");
        let (mut client, server) = tokio::io::duplex(4096);
        let server_stream = AuthenticatedDirectStream {
            io: Box::new(server),
            local_node_id: actual_destination.clone(),
            peer_node_id: source,
            link_id: Id::generate(),
            stream_id: Id::generate(),
            link_channel_binding: [7; 32],
            channel_binding: [0; 32],
            capabilities: DirectStreamCapabilities::multiplexed_full_duplex(16),
        };
        let server_identity = TestRoutedIdentity {
            local_node_id: actual_destination.clone(),
        };
        let accepted = tokio::spawn(async move {
            accept_direct_routed_session(server_stream, &directory, &server_identity).await
        });

        write_routed_session_frame(&mut client, ROUTED_SESSION_OPEN_FRAME, &open)
            .await
            .expect("write routed open");
        client
            .write_all(b"application bytes")
            .await
            .expect("write queued application bytes");
        let error = match accepted.await.expect("server task") {
            Ok(_) => panic!("destination mismatch was accepted"),
            Err(error) => error,
        };
        assert_eq!(
            error,
            NetworkError::FrameTargetMismatch {
                requested_target_node_id: actual_destination,
                frame_target_node_id: requested_destination,
            }
        );
    }

    fn topology_fixture(
        label: &str,
        generation: u64,
        node_count: usize,
        undirected_edges: &[(usize, usize)],
    ) -> (Vec<NodeId>, DirectorySnapshot, ConnectivitySnapshot) {
        let nodes = (0..node_count)
            .map(|index| node(&format!("node_relay_{label}_{index}")))
            .collect::<Vec<_>>();
        let mut observations = Vec::new();
        for (edge_index, &(left, right)) in undirected_edges.iter().enumerate() {
            observations.push(edge(
                &nodes[left],
                &nodes[right],
                &format!("endpoint_relay_{label}_{left}_{right}"),
                generation,
                u64::try_from(edge_index + 1).expect("edge latency"),
            ));
            observations.push(edge(
                &nodes[right],
                &nodes[left],
                &format!("endpoint_relay_{label}_{right}_{left}"),
                generation,
                u64::try_from(edge_index + 1).expect("edge latency"),
            ));
        }
        let (directory, connectivity) = fixtures(generation, observations);
        (nodes, directory, connectivity)
    }

    async fn realize_test_path(network: &TestRelayNetwork, path: &CandidatePath) {
        let source_node_id = path.nodes.first().expect("path source").clone();
        let first_hop = path.hops.first().expect("path first hop");
        let provider = network.provider(source_node_id.clone());
        let identity = TestRoutedIdentity {
            local_node_id: source_node_id,
        };
        let path = path.clone();
        tokio::time::timeout(Duration::from_secs(2), async {
            let link = provider
                .connect(&first_hop.endpoint)
                .await
                .expect("connect first route edge");
            let stream = link.open_stream().await.expect("open first route edge");
            let mut stream = initiate_routed_session(
                stream,
                network.directory.as_ref(),
                &path,
                &identity,
                routed_session_deadline_unix_millis(Duration::from_secs(2)),
            )
            .await
            .expect("realize planned route");
            assert_eq!(stream.route_trace, RouteTrace::from(&path));
            stream
                .write_all(TEST_ROUTED_PAYLOAD)
                .await
                .expect("write routed payload");
            let mut echoed = [0_u8; TEST_ROUTED_PAYLOAD.len()];
            stream
                .read_exact(&mut echoed)
                .await
                .expect("read routed echo");
            assert_eq!(echoed, *TEST_ROUTED_PAYLOAD);
            stream.shutdown().await.expect("close routed session");
        })
        .await
        .expect("planned route completed before deadline");
    }

    #[tokio::test]
    async fn multi_hop_connector_timeslices_candidates_before_retrying_another_endpoint() {
        let source = node("node_relay_endpoint_budget_source");
        let destination = node("node_relay_endpoint_budget_destination");
        let first = edge(
            &source,
            &destination,
            "endpoint_relay_endpoint_budget_01_stalled",
            39,
            1,
        );
        let first_endpoint_id = first.endpoint.endpoint_id.clone();
        let second = edge(
            &source,
            &destination,
            "endpoint_relay_endpoint_budget_02_ready",
            39,
            2,
        );
        let second_endpoint_id = second.endpoint.endpoint_id.clone();
        let (directory, connectivity) = fixtures(39, vec![first, second]);
        let network = TestRelayNetwork::new(directory.clone(), connectivity);
        network
            .stalled_endpoints
            .lock()
            .expect("stalled endpoints lock")
            .insert(first_endpoint_id.clone());
        let connector = MultiHopRoutedConnector::new(
            source.clone(),
            Arc::new(FixedDirectory(directory)),
            network.connectivity.clone(),
            network.connectivity.clone(),
            Arc::new(network.provider(source.clone())),
            Arc::new(TestRoutedIdentity {
                local_node_id: source.clone(),
            }),
        )
        .expect("multi-hop connector");

        let mut stream = connector
            .connect(
                destination.clone(),
                ConnectOptions {
                    timeout: Duration::from_millis(400),
                    ..ConnectOptions::default()
                },
            )
            .await
            .expect("second endpoint should retain a bounded attempt window");
        stream
            .write_all(TEST_ROUTED_PAYLOAD)
            .await
            .expect("write routed payload");
        let mut echoed = [0_u8; TEST_ROUTED_PAYLOAD.len()];
        stream
            .read_exact(&mut echoed)
            .await
            .expect("read routed echo");
        assert_eq!(echoed, *TEST_ROUTED_PAYLOAD);
        stream.shutdown().await.expect("close routed session");
        network.wait_idle().await;

        let dialed = network.dialed_edges.lock().expect("dialed edges lock");
        assert_eq!(dialed.len(), 2);
        assert_eq!(dialed[0].2, first_endpoint_id);
        assert_eq!(dialed[1].2, second_endpoint_id);
        drop(dialed);
        let snapshot = network.connectivity.snapshot();
        assert_eq!(
            snapshot
                .edges()
                .iter()
                .find(|edge| edge.endpoint.endpoint_id == first_endpoint_id)
                .expect("first endpoint")
                .state,
            DirectConnectivityState::Unreachable
        );
        assert_eq!(stream.route_trace.endpoint_ids, vec![second_endpoint_id]);
    }

    #[tokio::test]
    async fn multi_hop_connector_preserves_handshake_budget_with_full_candidate_set() {
        let source = node("node_relay_full_budget_source");
        let destination = node("node_relay_full_budget_destination");
        let mut edges = (0..MAX_ROUTE_CANDIDATES)
            .map(|index| {
                edge(
                    &source,
                    &destination,
                    &format!("endpoint_relay_full_budget_{index:02}"),
                    57,
                    u64::try_from(index).expect("latency index") + 1,
                )
            })
            .collect::<Vec<_>>();
        let first_endpoint_id = edges[0].endpoint.endpoint_id.clone();
        let second_endpoint_id = edges[1].endpoint.endpoint_id.clone();
        let (directory, connectivity) = fixtures(57, std::mem::take(&mut edges));
        let network = TestRelayNetwork::new(directory.clone(), connectivity);
        network
            .stalled_endpoints
            .lock()
            .expect("stalled endpoints lock")
            .insert(first_endpoint_id);
        network
            .delayed_endpoints
            .lock()
            .expect("delayed endpoints lock")
            .insert(second_endpoint_id.clone(), Duration::from_millis(75));
        let connector = MultiHopRoutedConnector::new(
            source.clone(),
            Arc::new(FixedDirectory(directory)),
            network.connectivity.clone(),
            network.connectivity.clone(),
            Arc::new(network.provider(source)),
            Arc::new(TestRoutedIdentity {
                local_node_id: node("node_relay_full_budget_source"),
            }),
        )
        .expect("multi-hop connector");

        let mut stream = connector
            .connect(
                destination,
                ConnectOptions {
                    timeout: Duration::from_millis(400),
                    ..ConnectOptions::default()
                },
            )
            .await
            .expect("the delayed second path retains a viable handshake budget");
        stream
            .write_all(TEST_ROUTED_PAYLOAD)
            .await
            .expect("write routed payload");
        let mut echoed = [0_u8; TEST_ROUTED_PAYLOAD.len()];
        stream
            .read_exact(&mut echoed)
            .await
            .expect("read routed echo");
        assert_eq!(echoed, *TEST_ROUTED_PAYLOAD);
        assert_eq!(stream.route_trace.endpoint_ids, vec![second_endpoint_id]);
        stream.shutdown().await.expect("close routed session");
        network.wait_idle().await;
    }

    #[tokio::test]
    async fn multi_hop_connector_freezes_one_directory_generation_before_connect() {
        let source = node("node_relay_frozen_directory_source");
        let destination = node("node_relay_frozen_directory_destination");
        let (directory, connectivity) = fixtures(
            58,
            vec![edge(
                &source,
                &destination,
                "endpoint_relay_frozen_directory",
                58,
                1,
            )],
        );
        let network = TestRelayNetwork::new(directory.clone(), connectivity);
        let changing = Arc::new(ChangingDirectory {
            first: directory.clone(),
            later: DirectorySnapshot::try_new(
                DirectoryVersion(directory.version().0.saturating_add(1)),
                Vec::new(),
            )
            .expect("empty later directory"),
            snapshots: AtomicUsize::new(0),
        });
        let directory_provider: Arc<dyn ClusterDirectory> = changing.clone();
        let connector = MultiHopRoutedConnector::new(
            source.clone(),
            directory_provider,
            network.connectivity.clone(),
            network.connectivity.clone(),
            Arc::new(network.provider(source.clone())),
            Arc::new(TestRoutedIdentity {
                local_node_id: source,
            }),
        )
        .expect("multi-hop connector");

        let mut stream = connector
            .connect(destination, ConnectOptions::default())
            .await
            .expect("connector uses the directory generation captured at construction");
        stream
            .write_all(TEST_ROUTED_PAYLOAD)
            .await
            .expect("write routed payload");
        let mut echoed = [0_u8; TEST_ROUTED_PAYLOAD.len()];
        stream
            .read_exact(&mut echoed)
            .await
            .expect("read routed echo");
        assert_eq!(echoed, *TEST_ROUTED_PAYLOAD);
        stream.shutdown().await.expect("close routed session");
        network.wait_idle().await;

        assert_eq!(
            changing.snapshots.load(Ordering::SeqCst),
            1,
            "route execution must not resnapshot a newer directory generation"
        );
    }

    #[tokio::test]
    async fn degraded_endpoint_variants_cannot_crowd_out_a_healthy_relay() {
        let source = node("node_candidate_cap_source");
        let destination = node("node_candidate_cap_00_destination");
        let relay = node("node_candidate_cap_99_relay");
        let mut observations = (0..MAX_ROUTE_CANDIDATES)
            .map(|index| {
                let mut observation = edge(
                    &source,
                    &destination,
                    &format!("endpoint_candidate_cap_degraded_{index:02}"),
                    59,
                    u64::try_from(index).expect("latency index") + 1,
                );
                observation.state = DirectConnectivityState::Degraded;
                observation
            })
            .collect::<Vec<_>>();
        observations.push(edge(
            &source,
            &relay,
            "endpoint_candidate_cap_source_relay",
            59,
            20,
        ));
        observations.push(edge(
            &relay,
            &destination,
            "endpoint_candidate_cap_relay_destination",
            59,
            20,
        ));
        let (directory, connectivity) = fixtures(59, observations);
        let network = TestRelayNetwork::new(directory.clone(), connectivity);
        let connector = MultiHopRoutedConnector::new(
            source.clone(),
            Arc::new(FixedDirectory(directory)),
            network.connectivity.clone(),
            network.connectivity.clone(),
            Arc::new(network.provider(source.clone())),
            Arc::new(TestRoutedIdentity {
                local_node_id: source.clone(),
            }),
        )
        .expect("multi-hop connector");

        let mut stream = connector
            .connect(destination.clone(), ConnectOptions::default())
            .await
            .expect("healthy relay remains in the bounded candidate set");
        assert_eq!(
            stream.route_trace.nodes,
            vec![source, relay, destination],
            "the healthy relay must rank ahead of every degraded direct variant"
        );
        stream
            .write_all(TEST_ROUTED_PAYLOAD)
            .await
            .expect("write routed payload");
        let mut echoed = [0_u8; TEST_ROUTED_PAYLOAD.len()];
        stream
            .read_exact(&mut echoed)
            .await
            .expect("read routed echo");
        assert_eq!(echoed, *TEST_ROUTED_PAYLOAD);
        stream.shutdown().await.expect("close routed session");
        network.wait_idle().await;
        assert_eq!(network.accepted_payloads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn every_planned_line_ring_star_and_mesh_path_is_realizable_and_opaque() {
        let topologies = vec![
            ("line", vec![(0, 1), (1, 2), (2, 3)]),
            ("ring", vec![(0, 1), (1, 2), (2, 3), (3, 0)]),
            ("star", vec![(0, 1), (0, 2), (0, 3)]),
            ("mesh", vec![(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]),
        ];

        for (topology_index, (label, topology_edges)) in topologies.into_iter().enumerate() {
            let generation = u64::try_from(40 + topology_index).expect("generation");
            let (nodes, directory, connectivity) =
                topology_fixture(label, generation, 4, &topology_edges);
            let network = TestRelayNetwork::new(directory, connectivity);
            let snapshot = network.connectivity.snapshot();
            let mut realized_paths = 0_usize;
            let mut expected_relays = 0_usize;
            for source in &nodes {
                for destination in &nodes {
                    if source == destination {
                        continue;
                    }
                    let paths = plan_routes(
                        network.directory.as_ref(),
                        snapshot.as_ref(),
                        source,
                        destination,
                        RouteConstraints::default(),
                    );
                    assert!(
                        !paths.is_empty(),
                        "{label} has no route from {source} to {destination}"
                    );
                    for path in paths {
                        expected_relays =
                            expected_relays.saturating_add(path.hops.len().saturating_sub(1));
                        realize_test_path(&network, &path).await;
                        realized_paths = realized_paths.saturating_add(1);
                    }
                }
            }
            network.wait_idle().await;
            assert_eq!(
                network
                    .destination_routes
                    .lock()
                    .expect("destination routes lock")
                    .len(),
                realized_paths,
                "{label} destination count"
            );
            assert_eq!(
                network
                    .relay_reports
                    .lock()
                    .expect("relay reports lock")
                    .len(),
                expected_relays,
                "{label} relay count"
            );
            assert!(
                network.errors.lock().expect("relay errors lock").is_empty(),
                "{label} errors: {:?}",
                network.errors.lock().expect("relay errors lock")
            );
            assert!(
                network
                    .wire_writes
                    .lock()
                    .expect("wire writes lock")
                    .iter()
                    .all(|write| !write
                        .windows(TEST_ROUTED_PAYLOAD.len())
                        .any(|window| window == TEST_ROUTED_PAYLOAD)),
                "{label} exposed application plaintext on a direct edge"
            );
        }
    }

    #[test]
    fn signed_route_rejects_tampering_impersonation_loops_expiry_and_hop_exhaustion() {
        let (nodes, directory, connectivity) =
            topology_fixture("validation", 51, 3, &[(0, 1), (1, 2)]);
        let path = plan_routes(
            &directory,
            &connectivity,
            &nodes[0],
            &nodes[2],
            RouteConstraints::default(),
        )
        .into_iter()
        .next()
        .expect("validation path");
        let ephemeral = routed_session_ephemeral_key().expect("source ephemeral key");
        let descriptor = RoutedSessionDescriptor {
            protocol: ROUTED_SESSION_PROTOCOL.to_string(),
            source_node_id: nodes[0].clone(),
            destination_node_id: nodes[2].clone(),
            session_id: Id::generate(),
            path: path.nodes,
            endpoint_ids: path
                .hops
                .iter()
                .map(|hop| hop.endpoint.endpoint_id.clone())
                .collect(),
            source_ephemeral_public_key: ephemeral.public,
            hop_limit: 2,
            deadline_unix_millis: routed_session_deadline_unix_millis(Duration::from_secs(5)),
        };
        let source_identity = TestRoutedIdentity {
            local_node_id: nodes[0].clone(),
        };
        let transcript = routed_session_encode(&descriptor).expect("route transcript");
        let signature = source_identity.sign(&transcript).expect("route signature");

        let mut tampered = SignedRoutedSessionOpen {
            descriptor: descriptor.clone(),
            signature: signature.clone(),
        };
        tampered.descriptor.hop_limit = 3;
        assert_eq!(
            verify_routed_session_open(&directory, &source_identity, &tampered),
            Err(NetworkError::AuthDenied)
        );

        let impersonator = TestRoutedIdentity {
            local_node_id: nodes[1].clone(),
        };
        let impersonated = SignedRoutedSessionOpen {
            descriptor: descriptor.clone(),
            signature: impersonator
                .sign(&transcript)
                .expect("impersonated signature"),
        };
        assert_eq!(
            verify_routed_session_open(&directory, &source_identity, &impersonated),
            Err(NetworkError::AuthDenied)
        );

        let acceptance = RoutedSessionAcceptance {
            protocol: ROUTED_SESSION_PROTOCOL.to_string(),
            source_node_id: descriptor.source_node_id.clone(),
            destination_node_id: descriptor.destination_node_id.clone(),
            session_id: descriptor.session_id.clone(),
            open_transcript_digest: *blake3::hash(&transcript).as_bytes(),
            destination_ephemeral_public_key: [3; 32],
        };
        let acceptance_transcript =
            routed_session_encode(&acceptance).expect("acceptance transcript");
        let forged_acceptance = SignedRoutedSessionAcceptance {
            signature: impersonator
                .sign(&acceptance_transcript)
                .expect("forged acceptance signature"),
            acceptance,
        };
        assert_eq!(
            verify_routed_session_acceptance(
                &directory,
                &source_identity,
                &descriptor,
                &transcript,
                &forged_acceptance,
            ),
            Err(NetworkError::AuthDenied)
        );

        let mut looped = descriptor.clone();
        looped.path = vec![
            nodes[0].clone(),
            nodes[1].clone(),
            nodes[0].clone(),
            nodes[2].clone(),
        ];
        looped.endpoint_ids.push(Id::generate());
        assert_eq!(
            validate_routed_session_descriptor(&directory, &looped),
            Err(NetworkError::AuthDenied)
        );

        let mut expired = descriptor.clone();
        expired.deadline_unix_millis = routed_session_now_unix_millis().saturating_sub(1);
        assert_eq!(
            validate_routed_session_descriptor(&directory, &expired),
            Err(NetworkError::AuthDenied)
        );

        let mut exhausted = descriptor;
        exhausted.hop_limit = 1;
        assert_eq!(
            validate_routed_session_descriptor(&directory, &exhausted),
            Err(NetworkError::AuthDenied)
        );
    }

    #[tokio::test]
    async fn relay_edge_rejection_updates_only_the_failed_observed_edge() {
        let (nodes, directory, connectivity) =
            topology_fixture("failure", 52, 3, &[(0, 1), (1, 2)]);
        let now = OffsetDateTime::now_utc();
        let connectivity = ConnectivitySnapshot::try_new(
            connectivity.generation(),
            connectivity.directory_generation(),
            now,
            connectivity.edges().iter().cloned().map(|mut edge| {
                edge.observed_at = now - time::Duration::seconds(1);
                edge.expires_at = now + time::Duration::minutes(1);
                edge
            }),
        )
        .expect("current failure-test connectivity");
        let failed_endpoint = connectivity
            .edges()
            .iter()
            .find(|edge| edge.source_node_id == nodes[1] && edge.target_node_id == nodes[2])
            .expect("relay outgoing edge")
            .endpoint
            .endpoint_id
            .clone();
        let edge_count = connectivity.edges().len();
        let directory_node_count = directory.nodes().len();
        let network = TestRelayNetwork::new(directory.clone(), connectivity);
        network
            .failed_endpoints
            .lock()
            .expect("failed endpoints lock")
            .insert(failed_endpoint.clone());
        let connector = MultiHopRoutedConnector::new(
            nodes[0].clone(),
            Arc::new(FixedDirectory(directory)),
            network.connectivity.clone(),
            network.connectivity.clone(),
            Arc::new(network.provider(nodes[0].clone())),
            Arc::new(TestRoutedIdentity {
                local_node_id: nodes[0].clone(),
            }),
        )
        .expect("multi-hop connector");

        let result = connector
            .connect(
                nodes[2].clone(),
                ConnectOptions {
                    timeout: Duration::from_secs(2),
                    ..ConnectOptions::default()
                },
            )
            .await;
        assert!(matches!(result, Err(NetworkError::RetryExhausted { .. })));
        network.wait_idle().await;

        let snapshot = network.connectivity.snapshot();
        assert_eq!(snapshot.edges().len(), edge_count);
        assert_eq!(snapshot.directory_generation(), 52);
        assert_eq!(network.directory.nodes().len(), directory_node_count);
        let first_edge = snapshot
            .edges()
            .iter()
            .find(|edge| edge.source_node_id == nodes[0] && edge.target_node_id == nodes[1])
            .expect("source first edge");
        assert_eq!(first_edge.state, DirectConnectivityState::Reachable);
        let rejected = snapshot
            .edges()
            .iter()
            .find(|edge| edge.endpoint.endpoint_id == failed_endpoint)
            .expect("rejected relay edge");
        assert_eq!(rejected.source_node_id, nodes[1]);
        assert_eq!(rejected.target_node_id, nodes[2]);
        assert_eq!(rejected.state, DirectConnectivityState::Unreachable);
        assert_eq!(
            rejected.provenance,
            "routed-session-edge-failure".to_string()
        );
    }

    #[tokio::test]
    async fn downstream_membership_rejection_does_not_invalidate_authenticated_edge() {
        let (nodes, directory, connectivity) =
            topology_fixture("downstream-membership-lag", 55, 3, &[(0, 1), (1, 2)]);
        let network = TestRelayNetwork::new(directory.clone(), connectivity);
        network
            .rejected_origins
            .lock()
            .expect("rejected origins lock")
            .entry(nodes[2].clone())
            .or_default()
            .insert(nodes[0].clone());
        let connector = MultiHopRoutedConnector::new(
            nodes[0].clone(),
            Arc::new(FixedDirectory(directory)),
            network.connectivity.clone(),
            network.connectivity.clone(),
            Arc::new(network.provider(nodes[0].clone())),
            Arc::new(TestRoutedIdentity {
                local_node_id: nodes[0].clone(),
            }),
        )
        .expect("multi-hop connector");

        let result = connector
            .connect(
                nodes[2].clone(),
                ConnectOptions {
                    timeout: Duration::from_secs(2),
                    ..ConnectOptions::default()
                },
            )
            .await;
        assert!(matches!(result, Err(NetworkError::RetryExhausted { .. })));
        network.wait_idle().await;

        let snapshot = network.connectivity.snapshot();
        let relay_edge = snapshot
            .edges()
            .iter()
            .find(|edge| edge.source_node_id == nodes[1] && edge.target_node_id == nodes[2])
            .expect("relay outgoing edge");
        assert_eq!(
            relay_edge.state,
            DirectConnectivityState::Reachable,
            "the downstream node rejected the routed origin after the direct link was authenticated"
        );
        assert_eq!(relay_edge.provenance, "routing-test");
        assert!(
            network
                .errors
                .lock()
                .expect("relay errors lock")
                .iter()
                .any(|error| error.contains(nodes[0].as_str())),
            "the destination must still report the unauthorized routed origin"
        );
    }

    #[tokio::test]
    async fn altered_relay_ciphertext_never_reaches_the_destination_application() {
        let (nodes, directory, connectivity) = topology_fixture("tamper", 54, 3, &[(0, 1), (1, 2)]);
        let network = TestRelayNetwork::new(directory.clone(), connectivity);
        let connector = MultiHopRoutedConnector::new(
            nodes[0].clone(),
            Arc::new(FixedDirectory(directory)),
            network.connectivity.clone(),
            network.connectivity.clone(),
            Arc::new(network.provider(nodes[0].clone())),
            Arc::new(TestRoutedIdentity {
                local_node_id: nodes[0].clone(),
            }),
        )
        .expect("multi-hop connector");
        let mut stream = connector
            .connect(nodes[2].clone(), ConnectOptions::default())
            .await
            .expect("connect tampered route");
        network.tamper_next_data_frame.store(true, Ordering::SeqCst);
        stream
            .write_all(TEST_ROUTED_PAYLOAD)
            .await
            .expect("write payload before wire tamper");
        let mut response = [0_u8; TEST_ROUTED_PAYLOAD.len()];
        let result = tokio::time::timeout(Duration::from_secs(2), stream.read_exact(&mut response))
            .await
            .expect("tampered stream terminated");
        assert!(result.is_err());
        drop(stream);
        network.wait_idle().await;
        assert_eq!(network.accepted_payloads.load(Ordering::SeqCst), 0);
        assert!(
            !network.errors.lock().expect("relay errors lock").is_empty(),
            "destination did not report the rejected protected payload"
        );
    }

    #[tokio::test]
    async fn stalled_destination_applies_backpressure_across_the_relay_chain() {
        let (nodes, directory, connectivity) =
            topology_fixture("backpressure", 53, 3, &[(0, 1), (1, 2)]);
        let network = TestRelayNetwork::new(directory.clone(), connectivity);
        network
            .blocked_destinations
            .lock()
            .expect("blocked destinations lock")
            .insert(nodes[2].clone());
        let connector = MultiHopRoutedConnector::new(
            nodes[0].clone(),
            Arc::new(FixedDirectory(directory)),
            network.connectivity.clone(),
            network.connectivity.clone(),
            Arc::new(network.provider(nodes[0].clone())),
            Arc::new(TestRoutedIdentity {
                local_node_id: nodes[0].clone(),
            }),
        )
        .expect("multi-hop connector");
        let mut stream = connector
            .connect(
                nodes[2].clone(),
                ConnectOptions {
                    timeout: Duration::from_secs(2),
                    ..ConnectOptions::default()
                },
            )
            .await
            .expect("connect blocked destination");
        let payload = vec![0x5a; 8 * 1024 * 1024];
        assert!(
            tokio::time::timeout(Duration::from_millis(100), stream.write_all(&payload))
                .await
                .is_err(),
            "unread relay chain buffered an unbounded payload"
        );
        network.blocked_destination_release.notify_one();
        drop(stream);
        network.wait_idle().await;
    }
}
