//! Pure route planning and target-bound routed-stream contracts.
//!
//! Routing consumes immutable directory and direct-connectivity snapshots. It
//! owns transitive paths and stream establishment, but forwards opaque bytes
//! and has no dependency on SOBJ, service methods, or application retries.

use crate::connectivity_snapshot::{
    ConnectivitySnapshot, DirectConnectivityObservation, DirectConnectivityState,
};
use crate::direct_link::{
    AuthenticatedDirectStream, BoxedNetworkStream, DirectStreamCapabilities, LinkProvider,
};
use crate::directory::{ClusterDirectory, DirectorySnapshot};
use crate::error::{NetworkDeliveryState, NetworkError, NetworkTimeoutPhase};
use serde::{Deserialize, Serialize};
use sinomo_base::{ClusterEndpointRecord, Id};
use sinomo_node::NodeId;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

pub const MAX_ROUTE_CANDIDATES: usize = 16;
const MAX_ROUTE_SEARCH_STATES: usize = 4096;

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
/// This replaces the planning responsibility of
/// `cluster_delivery_route_plan`; it performs no dialing, persistence,
/// probing, retries, or RPC. Module-backed direct delivery now consumes this
/// contract; remaining service families stay on their named carrier until a
/// bounded migration can replace and delete that path in the same slice.
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
        if observation.state != DirectConnectivityState::Reachable
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
        if visited_states > MAX_ROUTE_SEARCH_STATES || candidates.len() >= MAX_ROUTE_CANDIDATES {
            break;
        }
        if path.hops.len() >= constraints.max_hops {
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
                    candidates.push(next_path);
                }
                if candidates.len() >= MAX_ROUTE_CANDIDATES {
                    break;
                }
            } else {
                pending.push_back(next_path);
            }
        }
    }
    candidates.sort_by(|left, right| {
        left.hops
            .len()
            .cmp(&right.hops.len())
            .then_with(|| {
                left.total_latency_micros()
                    .cmp(&right.total_latency_micros())
            })
            .then_with(|| left.nodes.cmp(&right.nodes))
    });
    candidates
}

const ROUTED_SESSION_PROTOCOL: &str = "sinomo.routed-session.v1";
const ROUTED_SESSION_WIRE_PREFIX: [u8; 4] = *b"SRT1";
const ROUTED_SESSION_OPEN_FRAME: u8 = 1;
const ROUTED_SESSION_ACCEPT_FRAME: u8 = 2;
const MAX_ROUTED_SESSION_FRAME_BYTES: usize = 64 * 1024;
const ROUTED_SESSION_BINDING_DOMAIN: &[u8] = b"sinomo.routed-session.binding.v1\0";

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
struct DirectRoutedSessionDescriptor {
    protocol: String,
    source_node_id: NodeId,
    destination_node_id: NodeId,
    session_id: Id,
    path: Vec<NodeId>,
    endpoint_id: Id,
    link_channel_binding: [u8; 32],
    hop_limit: u16,
    deadline_unix_millis: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SignedDirectRoutedSessionOpen {
    descriptor: DirectRoutedSessionDescriptor,
    signature: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct DirectRoutedSessionAcceptance {
    protocol: String,
    source_node_id: NodeId,
    destination_node_id: NodeId,
    session_id: Id,
    open_transcript_digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SignedDirectRoutedSessionAcceptance {
    acceptance: DirectRoutedSessionAcceptance,
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
    link_channel_binding: &[u8; 32],
    open_transcript: &[u8],
    acceptance_transcript: &[u8],
) -> [u8; 32] {
    let mut input = Vec::with_capacity(
        ROUTED_SESSION_BINDING_DOMAIN.len()
            + link_channel_binding.len()
            + open_transcript.len()
            + acceptance_transcript.len(),
    );
    input.extend_from_slice(ROUTED_SESSION_BINDING_DOMAIN);
    input.extend_from_slice(link_channel_binding);
    input.extend_from_slice(open_transcript);
    input.extend_from_slice(acceptance_transcript);
    *blake3::hash(&input).as_bytes()
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

async fn initiate_direct_routed_session(
    mut stream: AuthenticatedDirectStream,
    directory: &DirectorySnapshot,
    path: &CandidatePath,
    identity: &dyn RoutedSessionIdentity,
    deadline_unix_millis: i64,
) -> Result<PeerStream, NetworkError> {
    let Some(hop) = path.hops.first() else {
        return Err(NetworkError::Serialization {
            detail: "direct routed session requires one route hop".to_string(),
        });
    };
    if path.hops.len() != 1
        || path.nodes.len() != 2
        || path.nodes.first() != Some(identity.local_node_id())
        || path.nodes.last() != Some(&stream.peer_node_id)
        || stream.local_node_id != *identity.local_node_id()
        || stream.peer_node_id != hop.target_node_id
    {
        return Err(NetworkError::Serialization {
            detail: "direct routed-session path does not match authenticated link identities"
                .to_string(),
        });
    }
    let source =
        directory
            .node(&stream.local_node_id)
            .ok_or_else(|| NetworkError::UnauthorizedNode {
                node_id: stream.local_node_id.clone(),
            })?;
    let destination =
        directory
            .node(&stream.peer_node_id)
            .ok_or_else(|| NetworkError::UnauthorizedNode {
                node_id: stream.peer_node_id.clone(),
            })?;
    if source.node_id != stream.local_node_id
        || !destination
            .endpoints
            .iter()
            .any(|endpoint| endpoint.endpoint_id == hop.endpoint.endpoint_id)
    {
        return Err(NetworkError::AuthDenied);
    }

    let descriptor = DirectRoutedSessionDescriptor {
        protocol: ROUTED_SESSION_PROTOCOL.to_string(),
        source_node_id: stream.local_node_id.clone(),
        destination_node_id: stream.peer_node_id.clone(),
        session_id: Id::generate(),
        path: path.nodes.clone(),
        endpoint_id: hop.endpoint.endpoint_id.clone(),
        link_channel_binding: stream.link_channel_binding,
        hop_limit: 1,
        deadline_unix_millis,
    };
    let descriptor_transcript = routed_session_encode(&descriptor)?;
    let open = SignedDirectRoutedSessionOpen {
        signature: identity.sign(&descriptor_transcript)?,
        descriptor,
    };
    let open_frame = routed_session_encode(&open)?;
    write_routed_session_frame(&mut *stream.io, ROUTED_SESSION_OPEN_FRAME, &open_frame).await?;

    let acceptance_frame =
        read_routed_session_frame(&mut *stream.io, ROUTED_SESSION_ACCEPT_FRAME).await?;
    let signed_acceptance: SignedDirectRoutedSessionAcceptance =
        routed_session_decode(&acceptance_frame)?;
    let expected_open_digest = *blake3::hash(&descriptor_transcript).as_bytes();
    let acceptance = &signed_acceptance.acceptance;
    if acceptance.protocol != ROUTED_SESSION_PROTOCOL
        || acceptance.source_node_id != open.descriptor.source_node_id
        || acceptance.destination_node_id != open.descriptor.destination_node_id
        || acceptance.session_id != open.descriptor.session_id
        || acceptance.open_transcript_digest != expected_open_digest
    {
        return Err(NetworkError::AuthDenied);
    }
    let acceptance_transcript = routed_session_encode(acceptance)?;
    identity.verify(
        &destination.node_id,
        &destination.public_key,
        &acceptance_transcript,
        &signed_acceptance.signature,
    )?;
    let channel_binding = routed_session_channel_binding(
        &stream.link_channel_binding,
        &descriptor_transcript,
        &acceptance_transcript,
    );
    PeerStream::new(
        stream.io,
        open.descriptor.source_node_id,
        open.descriptor.destination_node_id,
        open.descriptor.session_id,
        channel_binding,
        RouteTrace::from(path),
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
                latency_micros: None,
            }],
        };
        let stream = link.open_stream().await?;
        initiate_direct_routed_session(stream, directory, &path, identity, deadline_unix_millis)
            .await
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
/// The direct transport supplies the authenticated adjacent peer and a shared
/// exporter. The signed open must name that peer as its source and this node
/// as its destination. Only after those checks and a destination-signed
/// acceptance does the function return a socket-like [`PeerStream`].
pub async fn accept_direct_routed_session(
    mut stream: AuthenticatedDirectStream,
    directory: &DirectorySnapshot,
    identity: &dyn RoutedSessionIdentity,
) -> Result<PeerStream, NetworkError> {
    if stream.local_node_id != *identity.local_node_id() {
        return Err(NetworkError::AuthDenied);
    }
    let open_frame = read_routed_session_frame(&mut *stream.io, ROUTED_SESSION_OPEN_FRAME).await?;
    let signed_open: SignedDirectRoutedSessionOpen = routed_session_decode(&open_frame)?;
    let descriptor = &signed_open.descriptor;
    if descriptor.protocol != ROUTED_SESSION_PROTOCOL
        || descriptor.destination_node_id != stream.local_node_id
    {
        return Err(NetworkError::FrameTargetMismatch {
            requested_target_node_id: stream.local_node_id,
            frame_target_node_id: descriptor.destination_node_id.clone(),
        });
    }
    if descriptor.source_node_id != stream.peer_node_id {
        return Err(NetworkError::FrameSourceMismatch {
            local_node_id: stream.peer_node_id,
            source_node_id: descriptor.source_node_id.clone(),
        });
    }
    if descriptor.path
        != vec![
            descriptor.source_node_id.clone(),
            descriptor.destination_node_id.clone(),
        ]
        || descriptor.hop_limit != 1
        || descriptor.link_channel_binding != stream.link_channel_binding
        || descriptor.deadline_unix_millis < routed_session_now_unix_millis()
    {
        return Err(NetworkError::AuthDenied);
    }
    let source = directory.node(&descriptor.source_node_id).ok_or_else(|| {
        NetworkError::UnauthorizedNode {
            node_id: descriptor.source_node_id.clone(),
        }
    })?;
    let destination = directory
        .node(&descriptor.destination_node_id)
        .ok_or_else(|| NetworkError::UnauthorizedNode {
            node_id: descriptor.destination_node_id.clone(),
        })?;
    if !destination
        .endpoints
        .iter()
        .any(|endpoint| endpoint.endpoint_id == descriptor.endpoint_id)
    {
        return Err(NetworkError::AuthDenied);
    }
    let descriptor_transcript = routed_session_encode(descriptor)?;
    identity.verify(
        &source.node_id,
        &source.public_key,
        &descriptor_transcript,
        &signed_open.signature,
    )?;

    let acceptance = DirectRoutedSessionAcceptance {
        protocol: ROUTED_SESSION_PROTOCOL.to_string(),
        source_node_id: descriptor.source_node_id.clone(),
        destination_node_id: descriptor.destination_node_id.clone(),
        session_id: descriptor.session_id.clone(),
        open_transcript_digest: *blake3::hash(&descriptor_transcript).as_bytes(),
    };
    let acceptance_transcript = routed_session_encode(&acceptance)?;
    let signed_acceptance = SignedDirectRoutedSessionAcceptance {
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
        &stream.link_channel_binding,
        &descriptor_transcript,
        &acceptance_transcript,
    );
    let route_trace = RouteTrace {
        nodes: vec![
            descriptor.destination_node_id.clone(),
            descriptor.source_node_id.clone(),
        ],
        endpoint_ids: vec![descriptor.endpoint_id.clone()],
    };
    PeerStream::new(
        stream.io,
        descriptor.destination_node_id.clone(),
        descriptor.source_node_id.clone(),
        descriptor.session_id.clone(),
        channel_binding,
        route_trace,
    )
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
            match initiate_direct_routed_session(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectivity::ClusterConnectivityRecord;
    use crate::connectivity_snapshot::{
        ConnectivityGeneration, ConnectivityTracker, DirectConnectivityObservation,
    };
    use crate::direct_link::{DirectLinkFuture, DirectStreamCapabilities};
    use crate::directory::{DirectoryNode, DirectoryVersion};
    use sinomo_base::{
        ClusterEndpointPrivacy, ClusterEndpointSource, ClusterMemberRecord,
        ClusterNodeStatusRecord, ClusterTransportKind,
    };
    use std::sync::Mutex;
    use time::OffsetDateTime;

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
        let descriptor = DirectRoutedSessionDescriptor {
            protocol: ROUTED_SESSION_PROTOCOL.to_string(),
            source_node_id: source.clone(),
            destination_node_id: requested_destination.clone(),
            session_id: Id::generate(),
            path: vec![source.clone(), requested_destination.clone()],
            endpoint_id: endpoint.endpoint_id,
            link_channel_binding: [7; 32],
            hop_limit: 1,
            deadline_unix_millis: routed_session_deadline_unix_millis(Duration::from_secs(10)),
        };
        let transcript = routed_session_encode(&descriptor).expect("descriptor transcript");
        let open = SignedDirectRoutedSessionOpen {
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
}
