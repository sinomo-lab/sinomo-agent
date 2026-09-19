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
use crate::owned_chunk::{
    AuthenticatedChunkReader, AuthenticatedChunkStream, AuthenticatedChunkWriter, BODY_CHUNK_MAX,
    BoxedAuthenticatedChunkReader, BoxedAuthenticatedChunkWriter, ChunkAllocator, ChunkClass,
    ChunkLease, ChunkStreamFuture, MUX_RECORD_HEADER_BYTES, OwnedChunk, PayloadCopyReason,
    RPC_CONTROL_PAYLOAD_MAX, RPC_RECORD_HEADER_BYTES, ResetReason, SECURE_PLAINTEXT_MAX,
    SECURE_RECORD_HEADER_BYTES, SECURE_RECORD_TAG_BYTES, observe_chunk_identity,
};
#[cfg(test)]
use crate::owned_chunk::{ChunkPool, MUX_RECORD_MAX, SECURE_WIRE_MAX};
use serde::{Deserialize, Serialize};
use sinomo_base::{ClusterEndpointRecord, Id};
use sinomo_node::NodeId;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::io::IoSlice;
use std::pin::Pin;
use std::sync::{
    Arc, Mutex,
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
/// Receiver-side wall-clock tolerance for a signed routed-session deadline.
///
/// The caller's monotonic timeout still bounds its own work. This tolerance
/// only prevents a modestly fast peer clock from rejecting an otherwise-live
/// open before the caller's deadline; without it, a two-second connectivity
/// probe fails whenever two cluster nodes differ by more than two seconds.
const ROUTED_SESSION_DEADLINE_CLOCK_SKEW_MILLIS: i64 = 30_000;

/// Selects which daemon process may terminate a routed relationship.
///
/// Main and shadow daemons intentionally share one cluster node identity. The
/// affinity constrains endpoint selection and carrier reuse. An initiating
/// service dispatcher may advertise it for reverse opens; this routing fact
/// never grants service or membership authority.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DestinationProcessAffinity {
    /// Any process endpoint advertising the destination node identity.
    #[default]
    Any,
    /// Only an endpoint owned by the node's main daemon.
    MainDaemon,
}

impl DestinationProcessAffinity {
    fn accepts_terminal_endpoint(self, endpoint: &ClusterEndpointRecord) -> bool {
        match self {
            Self::Any => true,
            Self::MainDaemon => {
                crate::endpoint_selector::cluster_endpoint_can_carry_main_daemon_rpc(endpoint)
            }
        }
    }
}

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
            require_confidentiality: false,
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

    fn unreachable_hop_count(&self) -> usize {
        self.hops
            .iter()
            .filter(|hop| hop.connectivity_state == DirectConnectivityState::Unreachable)
            .count()
    }
}

fn compare_candidate_paths(left: &CandidatePath, right: &CandidatePath) -> std::cmp::Ordering {
    // Fully healthy paths always precede paths containing degraded evidence,
    // even when the latter are shorter. Degraded edges remain executable as a
    // last resort so a transient observation generation cannot make an
    // otherwise connected node publish zero candidates. A stale-unreachable
    // direct recovery is weaker still and must never outrank either kind.
    left.unreachable_hop_count()
        .cmp(&right.unreachable_hop_count())
        .then_with(|| left.degraded_hop_count().cmp(&right.degraded_hop_count()))
        .then_with(|| left.hops.len().cmp(&right.hops.len()))
        .then_with(|| {
            left.total_latency_micros()
                .cmp(&right.total_latency_micros())
        })
        .then_with(|| left.nodes.cmp(&right.nodes))
        .then_with(|| {
            left.hops
                .iter()
                .map(|hop| &hop.endpoint.endpoint_id)
                .cmp(right.hops.iter().map(|hop| &hop.endpoint.endpoint_id))
        })
}

fn retain_ranked_candidate(candidates: &mut Vec<CandidatePath>, candidate: CandidatePath) {
    candidates.push(candidate);
    candidates.sort_by(compare_candidate_paths);
    candidates.truncate(MAX_ROUTE_CANDIDATES);
}

fn append_candidate_bucket(
    candidates: &mut Vec<CandidatePath>,
    bucket: impl IntoIterator<Item = CandidatePath>,
) {
    let remaining = MAX_ROUTE_CANDIDATES.saturating_sub(candidates.len());
    candidates.extend(bucket.into_iter().take(remaining));
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

/// Deterministically plans executable directed paths from immutable directory
/// and connectivity inputs plus the current local module-dialer registry.
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
    plan_routes_with_destination_process_affinity(
        directory,
        connectivity,
        source_node_id,
        destination_node_id,
        constraints,
        DestinationProcessAffinity::Any,
    )
}

fn plan_routes_with_destination_process_affinity(
    directory: &DirectorySnapshot,
    connectivity: &ConnectivitySnapshot,
    source_node_id: &NodeId,
    destination_node_id: &NodeId,
    constraints: RouteConstraints,
    destination_process_affinity: DestinationProcessAffinity,
) -> Vec<CandidatePath> {
    if connectivity.directory_generation() != directory.version().0
        || !directory.is_authorized(source_node_id)
        || !directory.is_authorized(destination_node_id)
        || constraints.max_hops == 0
    {
        return Vec::new();
    }
    if source_node_id == destination_node_id {
        return match destination_process_affinity {
            DestinationProcessAffinity::Any => {
                vec![CandidatePath::at_source(source_node_id.clone())]
            }
            // A node identity does not distinguish its main and shadow
            // processes. Without a terminal endpoint dial there is no positive
            // proof that this process is the main daemon.
            DestinationProcessAffinity::MainDaemon => Vec::new(),
        };
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
            if &hop.target_node_id == destination_node_id
                && !destination_process_affinity.accepts_terminal_endpoint(&hop.endpoint)
            {
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
    let allow_unreachable_recovery = candidates.is_empty();
    let first_degraded = candidates.partition_point(|path| path.degraded_hop_count() == 0);
    let degraded_observed = candidates.split_off(first_degraded);
    let healthy_observed = candidates;

    let mut unobserved_module_recovery = Vec::new();
    append_unobserved_direct_module_recovery_candidates(
        &mut unobserved_module_recovery,
        directory,
        connectivity,
        source_node_id,
        destination_node_id,
        constraints,
        destination_process_affinity,
    );

    let mut selected = Vec::new();
    append_candidate_bucket(&mut selected, healthy_observed);
    append_candidate_bucket(&mut selected, unobserved_module_recovery);
    append_candidate_bucket(&mut selected, degraded_observed);
    if allow_unreachable_recovery && selected.len() < MAX_ROUTE_CANDIDATES {
        let mut unreachable_recovery = Vec::new();
        append_unreachable_direct_recovery_candidates(
            &mut unreachable_recovery,
            directory,
            connectivity,
            source_node_id,
            destination_node_id,
            constraints,
            destination_process_affinity,
        );
        append_candidate_bucket(&mut selected, unreachable_recovery);
    }
    selected
}

/// Adds current directory endpoints owned by an enabled local transport module
/// when that module cannot participate in the built-in connectivity probe.
///
/// These are deliberately terminal, one-edge recovery candidates rather than
/// adjacency-graph edges: directory publication authorizes the exact endpoint
/// and node, but it is not reachability evidence and therefore cannot authorize
/// relay forwarding. The authenticated link and routed-session handshakes still
/// prove the adjacent node, requested destination, capabilities, and process
/// affinity before any application bytes are exposed.
fn append_unobserved_direct_module_recovery_candidates(
    candidates: &mut Vec<CandidatePath>,
    directory: &DirectorySnapshot,
    connectivity: &ConnectivitySnapshot,
    source_node_id: &NodeId,
    destination_node_id: &NodeId,
    constraints: RouteConstraints,
    destination_process_affinity: DestinationProcessAffinity,
) {
    let Some(registry) = crate::network_module::network_module_registry() else {
        return;
    };
    let Some(destination) = directory.node(destination_node_id) else {
        return;
    };
    // The module contract guarantees a full-duplex authenticated stream. No
    // unobserved capability is assumed beyond that; realization re-checks the
    // actual link before opening the routed session.
    let conservative_capabilities = DirectStreamCapabilities {
        full_duplex: true,
        relay_forwarding: false,
        confidential: false,
        max_concurrent_streams: 1,
    };
    if !constraints.accepts_capabilities(conservative_capabilities) {
        return;
    }
    for endpoint in &destination.endpoints {
        let endpoint_role = registry.endpoint_role(endpoint);
        let process_affinity_matches = match destination_process_affinity {
            DestinationProcessAffinity::Any => true,
            DestinationProcessAffinity::MainDaemon => {
                endpoint_role == crate::network_module::NetworkModuleEndpointRole::MainDaemon
            }
        };
        let already_observed = connectivity.edges().iter().any(|observation| {
            &observation.source_node_id == source_node_id
                && &observation.target_node_id == destination_node_id
                && observation.endpoint.endpoint_id == endpoint.endpoint_id
                && observation.endpoint.transport == endpoint.transport
                && observation.endpoint.address == endpoint.address
        });
        if endpoint.privacy == sinomo_base::ClusterEndpointPrivacy::LocalOnly
            || crate::endpoint_selector::cluster_endpoint_is_builtin_probe_target(endpoint)
            || already_observed
            || endpoint_role == crate::network_module::NetworkModuleEndpointRole::Unknown
            || !process_affinity_matches
        {
            continue;
        }
        retain_ranked_candidate(
            candidates,
            CandidatePath::at_source(source_node_id.clone()).extend(RouteHop {
                source_node_id: source_node_id.clone(),
                target_node_id: destination_node_id.clone(),
                endpoint: endpoint.clone(),
                capabilities: conservative_capabilities,
                // There is no durable reachability observation. Degraded is
                // the conservative availability exposed on this synthetic
                // path; the planner's explicit recovery bucket places it
                // ahead of observed degraded paths. It is not published.
                connectivity_state: DirectConnectivityState::Degraded,
                latency_micros: None,
            }),
        );
    }
}

/// Keeps a committed direct endpoint recoverable after its last observation
/// marked it unreachable.
///
/// `Unreachable` remains excluded from the normal adjacency graph above: it can
/// never authorize a relay or displace a healthy/degraded path. When that graph
/// has no executable path at all, the authenticated direct-link handshake is
/// the reactive probe that can disprove stale failure evidence. Directory
/// membership and the exact endpoint record still fence the attempt, and the
/// connector re-checks the established link's identity and capabilities before
/// opening the routed session. Structured failure evidence remains fail-closed:
/// identity/authentication negatives, as well as an in-process routed failure,
/// must be cleared by the dedicated connectivity-maintenance probe rather than
/// challenged by every application call.
fn append_unreachable_direct_recovery_candidates(
    candidates: &mut Vec<CandidatePath>,
    directory: &DirectorySnapshot,
    connectivity: &ConnectivitySnapshot,
    source_node_id: &NodeId,
    destination_node_id: &NodeId,
    constraints: RouteConstraints,
    destination_process_affinity: DestinationProcessAffinity,
) {
    for observation in connectivity.edges() {
        if observation.state != DirectConnectivityState::Unreachable
            || observation.failure_reason.is_some()
            || &observation.source_node_id != source_node_id
            || &observation.target_node_id != destination_node_id
            || !directory_contains_endpoint(directory, observation)
            || !constraints.accepts(observation)
            || !destination_process_affinity.accepts_terminal_endpoint(&observation.endpoint)
        {
            continue;
        }
        retain_ranked_candidate(
            candidates,
            CandidatePath::at_source(source_node_id.clone()).extend(RouteHop {
                source_node_id: observation.source_node_id.clone(),
                target_node_id: observation.target_node_id.clone(),
                endpoint: observation.endpoint.clone(),
                capabilities: observation.capabilities,
                connectivity_state: observation.state,
                latency_micros: observation.latency_micros,
            }),
        );
    }
}

const ROUTED_SESSION_PROTOCOL: &str = "sinomo.routed-session.v1";
const ROUTED_SESSION_WIRE_PREFIX: [u8; 4] = *b"SRT1";
const ROUTED_SESSION_OPEN_FRAME: u8 = 1;
const ROUTED_SESSION_ACCEPT_FRAME: u8 = 2;
const MAX_ROUTED_SESSION_FRAME_BYTES: usize = 64 * 1024;
// A secure record must fit one complete mux record. These limits are derived
// outwards so a maximum-sized application chunk is not fragmented merely
// because the inner protocols add their framing.
#[cfg(test)]
const ROUTED_STREAM_MUX_HEADER_BYTES: usize = MUX_RECORD_HEADER_BYTES;
#[cfg(test)]
const ROUTED_SESSION_DATA_FRAME_BYTES: usize = MUX_RECORD_MAX;
const ROUTED_SESSION_DATA_WIRE_PREFIX: [u8; 4] = *b"SRE1";
const ROUTED_SESSION_PLAINTEXT_WIRE_PREFIX: [u8; 4] = *b"SRP1";
const ROUTED_SESSION_PLAINTEXT_HEADER_BYTES: usize = 8;
const ROUTED_SESSION_DATA_CONTROL_FLAG: u32 = 1 << 31;
const ROUTED_SESSION_DATA_LENGTH_MASK: u32 = !ROUTED_SESSION_DATA_CONTROL_FLAG;
const ROUTED_SESSION_BINDING_DOMAIN: &[u8] = b"sinomo.routed-session.binding.v1\0";
const ROUTED_SESSION_KEY_DOMAIN: &[u8] = b"sinomo.routed-session.data-key.v1\0";
const ROUTED_SESSION_AAD_DOMAIN: &[u8] = b"sinomo.routed-session.data-frame.v1\0";

/// Local identity boundary for routed-session establishment.
///
/// Adjacent peers are identified once by the direct transport. Routed-session
/// setup therefore needs only the local node identity for path validation; it
/// does not sign every logical session or RPC.
pub trait RoutedSessionIdentity: Send + Sync {
    fn local_node_id(&self) -> &NodeId;

    /// Signs direct-transport establishment metadata. Routed-session and RPC
    /// records do not call this method.
    fn sign(&self, transcript: &[u8]) -> Result<Vec<u8>, NetworkError>;

    /// Verifies direct-transport establishment metadata once per adjacent
    /// transport. Routed-session and RPC records do not call this method.
    fn verify(
        &self,
        signer_node_id: &NodeId,
        authorized_public_key: &str,
        transcript: &[u8],
        signature: &[u8],
    ) -> Result<(), NetworkError>;
}

/// Payload protection applied after the direct transport has established its
/// peer identity.
///
/// Trusted cluster transports default to plaintext. AEAD is retained for
/// endpoints whose operator explicitly requests end-to-end routed-session
/// encryption.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RoutedSessionProtection {
    #[default]
    Plaintext,
    Aead,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct RoutedSessionDescriptor {
    protocol: String,
    source_node_id: NodeId,
    destination_node_id: NodeId,
    session_id: Id,
    path: Vec<NodeId>,
    endpoint_ids: Vec<Id>,
    protection: RoutedSessionProtection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_ephemeral_public_key: Option<[u8; 32]>,
    /// Absent for caller-only and legacy initiators. Never infer this from the receiver.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reverse_process_affinity: Option<DestinationProcessAffinity>,
    hop_limit: u16,
    deadline_unix_millis: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct RoutedSessionOpen {
    descriptor: RoutedSessionDescriptor,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct RoutedSessionAcceptance {
    protocol: String,
    source_node_id: NodeId,
    destination_node_id: NodeId,
    session_id: Id,
    protection: RoutedSessionProtection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    destination_ephemeral_public_key: Option<[u8; 32]>,
    /// Echo confirms support, including agreement on the AEAD binding transcript.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reverse_process_affinity: Option<DestinationProcessAffinity>,
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

fn plaintext_routed_session_binding(session_id: &Id) -> [u8; 32] {
    let mut binding = [0_u8; 32];
    let session_bytes = session_id.as_str().as_bytes();
    let copied = session_bytes.len().min(binding.len());
    binding[..copied].copy_from_slice(&session_bytes[..copied]);
    binding
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

type RoutedSessionReadHalf = Box<dyn AsyncRead + Unpin + Send>;
type RoutedSessionWriteHalf = Box<dyn AsyncWrite + Unpin + Send>;

fn split_routed_session_io(
    io: BoxedNetworkStream,
) -> (RoutedSessionReadHalf, RoutedSessionWriteHalf) {
    if io.as_any().is::<tokio::net::TcpStream>() {
        let io = io
            .into_any()
            .downcast::<tokio::net::TcpStream>()
            .expect("checked routed-session TCP stream");
        let (reader, writer) = (*io).into_split();
        return (Box::new(reader), Box::new(writer));
    }
    // A module transport that does not expose native owned halves retains the
    // compatibility split. TCP, including the Docker/debug carrier, never
    // enters this Arc<Mutex>-backed path.
    let (reader, writer) = tokio::io::split(io);
    (Box::new(reader), Box::new(writer))
}

struct EncryptedRoutedChunkReader {
    reader: RoutedSessionReadHalf,
    opening_key: ring::aead::LessSafeKey,
    session_id: Id,
    channel_binding: [u8; 32],
    incoming_direction: u8,
    incoming_sequence: u64,
    allocator: ChunkAllocator,
    read_closed: bool,
}

impl EncryptedRoutedChunkReader {
    async fn recv_record(&mut self) -> Result<Option<ChunkLease>, NetworkError> {
        if self.read_closed {
            return Ok(None);
        }
        let mut header = [0_u8; SECURE_RECORD_HEADER_BYTES];
        self.reader
            .read_exact(&mut header)
            .await
            .map_err(|error| NetworkError::Io {
                operation: "read owned routed-session secure header".to_string(),
                detail: error.to_string(),
            })?;
        if header[..4] != ROUTED_SESSION_DATA_WIRE_PREFIX {
            return Err(NetworkError::Serialization {
                detail: "invalid owned routed-session secure record prefix".to_string(),
            });
        }
        let sequence =
            u64::from_be_bytes(header[4..12].try_into().expect("secure record sequence"));
        if sequence != self.incoming_sequence {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "non-monotonic owned routed-session sequence {sequence}; expected {}",
                    self.incoming_sequence
                ),
            });
        }
        let encoded_length =
            u32::from_be_bytes(header[12..].try_into().expect("secure record length"));
        let class = if encoded_length & ROUTED_SESSION_DATA_CONTROL_FLAG == 0 {
            ChunkClass::Data
        } else {
            ChunkClass::Control
        };
        let plaintext_bytes = usize::try_from(encoded_length & ROUTED_SESSION_DATA_LENGTH_MASK)
            .expect("u32 length fits usize");
        let class_plaintext_max = match class {
            ChunkClass::Data => SECURE_PLAINTEXT_MAX,
            ChunkClass::Control => {
                MUX_RECORD_HEADER_BYTES + RPC_RECORD_HEADER_BYTES + RPC_CONTROL_PAYLOAD_MAX
            }
        };
        if plaintext_bytes > class_plaintext_max {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: plaintext_bytes,
                max_bytes: class_plaintext_max,
            });
        }
        if plaintext_bytes == 0 && class != ChunkClass::Control {
            return Err(NetworkError::Serialization {
                detail: "owned routed-session final record used the data slab class".to_string(),
            });
        }
        let mut chunk = match class {
            ChunkClass::Data => self.allocator.allocate_data(0).await?,
            ChunkClass::Control => self.allocator.allocate_control(0).await?,
        };
        let wire_bytes = SECURE_RECORD_HEADER_BYTES
            .saturating_add(plaintext_bytes)
            .saturating_add(SECURE_RECORD_TAG_BYTES);
        chunk.set_range(0, wire_bytes)?;
        chunk.as_mut_slice()[..SECURE_RECORD_HEADER_BYTES].copy_from_slice(&header);
        self.reader
            .read_exact(&mut chunk.as_mut_slice()[SECURE_RECORD_HEADER_BYTES..])
            .await
            .map_err(|error| NetworkError::Io {
                operation: "read owned routed-session ciphertext".to_string(),
                detail: error.to_string(),
            })?;
        let aad = routed_session_data_aad(
            &self.session_id,
            &self.channel_binding,
            self.incoming_direction,
            self.incoming_sequence,
            encoded_length,
        );
        self.opening_key
            .open_in_place(
                routed_session_data_nonce(self.incoming_direction, self.incoming_sequence),
                ring::aead::Aad::from(aad),
                &mut chunk.as_mut_slice()[SECURE_RECORD_HEADER_BYTES..],
            )
            .map_err(|_| NetworkError::AuthDenied)?;
        chunk.strip_prefix(SECURE_RECORD_HEADER_BYTES)?;
        chunk.strip_suffix(SECURE_RECORD_TAG_BYTES)?;
        observe_chunk_identity(chunk.slab_id(), "secure-record-decrypted");
        if plaintext_bytes == 0 {
            self.read_closed = true;
            return Ok(None);
        }
        self.incoming_sequence =
            self.incoming_sequence
                .checked_add(1)
                .ok_or_else(|| NetworkError::Serialization {
                    detail: "owned routed-session receive sequence exhausted".to_string(),
                })?;
        Ok(Some(chunk.into_lease()))
    }
}

impl AuthenticatedChunkReader for EncryptedRoutedChunkReader {
    fn allocator(&self) -> &ChunkAllocator {
        &self.allocator
    }

    fn max_payload_bytes(&self) -> usize {
        SECURE_PLAINTEXT_MAX
    }

    fn recv_chunk(&mut self) -> ChunkStreamFuture<'_, Option<ChunkLease>> {
        Box::pin(self.recv_record())
    }
}

struct EncryptedRoutedChunkWriter {
    writer: RoutedSessionWriteHalf,
    sealing_key: ring::aead::LessSafeKey,
    session_id: Id,
    channel_binding: [u8; 32],
    outgoing_direction: u8,
    outgoing_sequence: u64,
    allocator: ChunkAllocator,
    write_closed: bool,
}

impl EncryptedRoutedChunkWriter {
    async fn send_record(
        &mut self,
        mut chunk: OwnedChunk,
        final_record: bool,
    ) -> Result<(), NetworkError> {
        if self.write_closed {
            return Err(NetworkError::Io {
                operation: "write owned routed-session record".to_string(),
                detail: "write direction is closed".to_string(),
            });
        }
        let plaintext_bytes = chunk.len();
        if plaintext_bytes > SECURE_PLAINTEXT_MAX {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: plaintext_bytes,
                max_bytes: SECURE_PLAINTEXT_MAX,
            });
        }
        if final_record && plaintext_bytes != 0 {
            return Err(NetworkError::Serialization {
                detail: "owned routed-session final record carried plaintext".to_string(),
            });
        }
        let plaintext_u32 =
            u32::try_from(plaintext_bytes).map_err(|_| NetworkError::FrameTooLarge {
                actual_bytes: plaintext_bytes,
                max_bytes: SECURE_PLAINTEXT_MAX,
            })?;
        let class_flag = match chunk.class() {
            ChunkClass::Data => 0,
            ChunkClass::Control => ROUTED_SESSION_DATA_CONTROL_FLAG,
        };
        let encoded_length = plaintext_u32 | class_flag;
        let aad = routed_session_data_aad(
            &self.session_id,
            &self.channel_binding,
            self.outgoing_direction,
            self.outgoing_sequence,
            encoded_length,
        );
        observe_chunk_identity(chunk.slab_id(), "secure-record-plaintext");
        let tag = self
            .sealing_key
            .seal_in_place_separate_tag(
                routed_session_data_nonce(self.outgoing_direction, self.outgoing_sequence),
                ring::aead::Aad::from(aad),
                chunk.as_mut_slice(),
            )
            .map_err(|_| NetworkError::Io {
                operation: "encrypt owned routed-session record".to_string(),
                detail: "AEAD failure".to_string(),
            })?;
        chunk.append(tag.as_ref())?;
        let mut header = [0_u8; SECURE_RECORD_HEADER_BYTES];
        header[..4].copy_from_slice(&ROUTED_SESSION_DATA_WIRE_PREFIX);
        header[4..12].copy_from_slice(&self.outgoing_sequence.to_be_bytes());
        header[12..].copy_from_slice(&encoded_length.to_be_bytes());
        chunk.prepend(&header)?;
        observe_chunk_identity(chunk.slab_id(), "secure-record-socket-staging");
        self.writer
            .write_all(chunk.as_slice())
            .await
            .map_err(|error| NetworkError::Io {
                operation: "write owned routed-session secure record".to_string(),
                detail: error.to_string(),
            })?;
        if final_record {
            self.write_closed = true;
            self.writer
                .shutdown()
                .await
                .map_err(|error| NetworkError::Io {
                    operation: "half-close owned routed-session".to_string(),
                    detail: error.to_string(),
                })?;
        } else {
            self.outgoing_sequence = self.outgoing_sequence.checked_add(1).ok_or_else(|| {
                NetworkError::Serialization {
                    detail: "owned routed-session send sequence exhausted".to_string(),
                }
            })?;
        }
        Ok(())
    }
}

impl AuthenticatedChunkWriter for EncryptedRoutedChunkWriter {
    fn max_payload_bytes(&self) -> usize {
        SECURE_PLAINTEXT_MAX
    }

    fn allocator(&self) -> &ChunkAllocator {
        &self.allocator
    }

    fn send_chunk(&mut self, chunk: OwnedChunk) -> ChunkStreamFuture<'_, ()> {
        Box::pin(self.send_record(chunk, false))
    }

    fn finish(&mut self) -> ChunkStreamFuture<'_, ()> {
        Box::pin(async move {
            if self.write_closed {
                return Ok(());
            }
            let final_record = self.allocator.allocate_control(0).await?;
            self.send_record(final_record, true).await
        })
    }

    fn reset(&mut self, reason: ResetReason) -> ChunkStreamFuture<'_, ()> {
        Box::pin(async move {
            self.write_closed = true;
            self.writer
                .shutdown()
                .await
                .map_err(|error| NetworkError::Io {
                    operation: format!("reset owned routed-session: {}", reason.detail),
                    detail: error.to_string(),
                })
        })
    }
}

struct PlainRoutedChunkReader {
    reader: RoutedSessionReadHalf,
    allocator: ChunkAllocator,
    read_closed: bool,
}

impl PlainRoutedChunkReader {
    async fn recv_record(&mut self) -> Result<Option<ChunkLease>, NetworkError> {
        if self.read_closed {
            return Ok(None);
        }
        let mut header = [0_u8; ROUTED_SESSION_PLAINTEXT_HEADER_BYTES];
        self.reader
            .read_exact(&mut header)
            .await
            .map_err(|error| NetworkError::Io {
                operation: "read routed-session record header".to_string(),
                detail: error.to_string(),
            })?;
        if header[..4] != ROUTED_SESSION_PLAINTEXT_WIRE_PREFIX {
            return Err(NetworkError::Serialization {
                detail: "invalid routed-session plaintext record prefix".to_string(),
            });
        }
        let encoded_length =
            u32::from_be_bytes(header[4..].try_into().expect("plaintext record length"));
        let class = if encoded_length & ROUTED_SESSION_DATA_CONTROL_FLAG == 0 {
            ChunkClass::Data
        } else {
            ChunkClass::Control
        };
        let plaintext_bytes = usize::try_from(encoded_length & ROUTED_SESSION_DATA_LENGTH_MASK)
            .expect("u32 length fits usize");
        let class_plaintext_max = match class {
            ChunkClass::Data => SECURE_PLAINTEXT_MAX,
            ChunkClass::Control => {
                MUX_RECORD_HEADER_BYTES + RPC_RECORD_HEADER_BYTES + RPC_CONTROL_PAYLOAD_MAX
            }
        };
        if plaintext_bytes > class_plaintext_max {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: plaintext_bytes,
                max_bytes: class_plaintext_max,
            });
        }
        if plaintext_bytes == 0 {
            if class != ChunkClass::Control {
                return Err(NetworkError::Serialization {
                    detail: "routed-session final record used the data slab class".to_string(),
                });
            }
            self.read_closed = true;
            return Ok(None);
        }
        let mut chunk = match class {
            ChunkClass::Data => self.allocator.allocate_data(0).await?,
            ChunkClass::Control => self.allocator.allocate_control(0).await?,
        };
        let wire_bytes = ROUTED_SESSION_PLAINTEXT_HEADER_BYTES.saturating_add(plaintext_bytes);
        chunk.set_range(0, wire_bytes)?;
        chunk.as_mut_slice()[..ROUTED_SESSION_PLAINTEXT_HEADER_BYTES].copy_from_slice(&header);
        self.reader
            .read_exact(&mut chunk.as_mut_slice()[ROUTED_SESSION_PLAINTEXT_HEADER_BYTES..])
            .await
            .map_err(|error| NetworkError::Io {
                operation: "read routed-session plaintext record".to_string(),
                detail: error.to_string(),
            })?;
        chunk.strip_prefix(ROUTED_SESSION_PLAINTEXT_HEADER_BYTES)?;
        observe_chunk_identity(chunk.slab_id(), "plaintext-record-received");
        Ok(Some(chunk.into_lease()))
    }
}

impl AuthenticatedChunkReader for PlainRoutedChunkReader {
    fn allocator(&self) -> &ChunkAllocator {
        &self.allocator
    }

    fn max_payload_bytes(&self) -> usize {
        SECURE_PLAINTEXT_MAX
    }

    fn recv_chunk(&mut self) -> ChunkStreamFuture<'_, Option<ChunkLease>> {
        Box::pin(self.recv_record())
    }
}

struct PlainRoutedChunkWriter {
    writer: RoutedSessionWriteHalf,
    allocator: ChunkAllocator,
    write_closed: bool,
}

impl PlainRoutedChunkWriter {
    async fn send_record(
        &mut self,
        mut chunk: OwnedChunk,
        final_record: bool,
    ) -> Result<(), NetworkError> {
        if self.write_closed {
            return Err(NetworkError::Io {
                operation: "write routed-session record".to_string(),
                detail: "write direction is closed".to_string(),
            });
        }
        let plaintext_bytes = chunk.len();
        if plaintext_bytes > SECURE_PLAINTEXT_MAX {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: plaintext_bytes,
                max_bytes: SECURE_PLAINTEXT_MAX,
            });
        }
        if final_record && plaintext_bytes != 0 {
            return Err(NetworkError::Serialization {
                detail: "routed-session final record carried payload".to_string(),
            });
        }
        let plaintext_u32 =
            u32::try_from(plaintext_bytes).map_err(|_| NetworkError::FrameTooLarge {
                actual_bytes: plaintext_bytes,
                max_bytes: SECURE_PLAINTEXT_MAX,
            })?;
        let class_flag = match chunk.class() {
            ChunkClass::Data => 0,
            ChunkClass::Control => ROUTED_SESSION_DATA_CONTROL_FLAG,
        };
        let encoded_length = plaintext_u32 | class_flag;
        let mut header = [0_u8; ROUTED_SESSION_PLAINTEXT_HEADER_BYTES];
        header[..4].copy_from_slice(&ROUTED_SESSION_PLAINTEXT_WIRE_PREFIX);
        header[4..].copy_from_slice(&encoded_length.to_be_bytes());
        chunk.prepend(&header)?;
        observe_chunk_identity(chunk.slab_id(), "plaintext-record-socket-staging");
        self.writer
            .write_all(chunk.as_slice())
            .await
            .map_err(|error| NetworkError::Io {
                operation: "write routed-session plaintext record".to_string(),
                detail: error.to_string(),
            })?;
        if final_record {
            self.write_closed = true;
            self.writer
                .shutdown()
                .await
                .map_err(|error| NetworkError::Io {
                    operation: "half-close routed-session".to_string(),
                    detail: error.to_string(),
                })?;
        }
        Ok(())
    }
}

impl AuthenticatedChunkWriter for PlainRoutedChunkWriter {
    fn max_payload_bytes(&self) -> usize {
        SECURE_PLAINTEXT_MAX
    }

    fn allocator(&self) -> &ChunkAllocator {
        &self.allocator
    }

    fn send_chunk(&mut self, chunk: OwnedChunk) -> ChunkStreamFuture<'_, ()> {
        Box::pin(self.send_record(chunk, false))
    }

    fn finish(&mut self) -> ChunkStreamFuture<'_, ()> {
        Box::pin(async move {
            if self.write_closed {
                return Ok(());
            }
            let final_record = self.allocator.allocate_control(0).await?;
            self.send_record(final_record, true).await
        })
    }

    fn reset(&mut self, reason: ResetReason) -> ChunkStreamFuture<'_, ()> {
        Box::pin(async move {
            self.write_closed = true;
            self.writer
                .shutdown()
                .await
                .map_err(|error| NetworkError::Io {
                    operation: format!("reset routed-session: {}", reason.detail),
                    detail: error.to_string(),
                })
        })
    }
}

fn plaintext_routed_session_chunk_stream(
    io: BoxedNetworkStream,
) -> Result<AuthenticatedChunkStream, NetworkError> {
    let allocator = ChunkAllocator::default();
    let (reader, writer) = split_routed_session_io(io);
    AuthenticatedChunkStream::new(
        Box::new(PlainRoutedChunkReader {
            reader,
            allocator: allocator.clone(),
            read_closed: false,
        }),
        Box::new(PlainRoutedChunkWriter {
            writer,
            allocator,
            write_closed: false,
        }),
    )
}

pub(crate) fn encrypted_routed_session_chunk_stream(
    io: BoxedNetworkStream,
    shared_secret: [u8; 32],
    session_id: Id,
    channel_binding: [u8; 32],
    source_side: bool,
) -> Result<AuthenticatedChunkStream, NetworkError> {
    debug_assert_eq!(
        ring::aead::CHACHA20_POLY1305.tag_len(),
        SECURE_RECORD_TAG_BYTES
    );
    let outgoing_direction = if source_side { 0 } else { 1 };
    let incoming_direction = if source_side { 1 } else { 0 };
    let outgoing_key =
        routed_session_data_key(&shared_secret, &channel_binding, outgoing_direction);
    let incoming_key =
        routed_session_data_key(&shared_secret, &channel_binding, incoming_direction);
    let sealing_key = ring::aead::UnboundKey::new(&ring::aead::CHACHA20_POLY1305, &outgoing_key)
        .map(ring::aead::LessSafeKey::new)
        .map_err(|_| NetworkError::Serialization {
            detail: "invalid owned routed-session sealing key".to_string(),
        })?;
    let opening_key = ring::aead::UnboundKey::new(&ring::aead::CHACHA20_POLY1305, &incoming_key)
        .map(ring::aead::LessSafeKey::new)
        .map_err(|_| NetworkError::Serialization {
            detail: "invalid owned routed-session opening key".to_string(),
        })?;
    let allocator = ChunkAllocator::default();
    let (reader, writer) = split_routed_session_io(io);
    let reader: BoxedAuthenticatedChunkReader = Box::new(EncryptedRoutedChunkReader {
        reader,
        opening_key,
        session_id: session_id.clone(),
        channel_binding,
        incoming_direction,
        incoming_sequence: 0,
        allocator: allocator.clone(),
        read_closed: false,
    });
    let writer: BoxedAuthenticatedChunkWriter = Box::new(EncryptedRoutedChunkWriter {
        writer,
        sealing_key,
        session_id,
        channel_binding,
        outgoing_direction,
        outgoing_sequence: 0,
        allocator,
        write_closed: false,
    });
    AuthenticatedChunkStream::new(reader, writer)
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
    /// End-to-end routed-session payload protection. The direct transport
    /// remains responsible for authenticating its adjacent peer.
    pub protection: RoutedSessionProtection,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            constraints: RouteConstraints::default(),
            timeout: Duration::from_secs(10),
            protection: RoutedSessionProtection::Plaintext,
        }
    }
}

/// Socket-like stream bound to one authenticated destination and route.
pub struct PeerStream {
    io: BoxedNetworkStream,
    chunk_io: Option<Arc<Mutex<Option<AuthenticatedChunkStream>>>>,
    pub local_node_id: NodeId,
    pub peer_node_id: NodeId,
    pub session_id: Id,
    pub channel_binding: [u8; 32],
    pub protection: RoutedSessionProtection,
    pub route_trace: RouteTrace,
    pub(crate) reverse_process_affinity: Option<DestinationProcessAffinity>,
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
        Self::new_with_protection(
            io,
            local_node_id,
            peer_node_id,
            session_id,
            channel_binding,
            RoutedSessionProtection::Plaintext,
            route_trace,
        )
    }

    pub fn new_with_protection(
        io: BoxedNetworkStream,
        local_node_id: NodeId,
        peer_node_id: NodeId,
        session_id: Id,
        channel_binding: [u8; 32],
        protection: RoutedSessionProtection,
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
            chunk_io: None,
            local_node_id,
            peer_node_id,
            session_id,
            channel_binding,
            protection,
            route_trace,
            reverse_process_affinity: None,
        })
    }

    pub(crate) fn new_owned(
        chunk_io: AuthenticatedChunkStream,
        local_node_id: NodeId,
        peer_node_id: NodeId,
        session_id: Id,
        channel_binding: [u8; 32],
        route_trace: RouteTrace,
    ) -> Result<Self, NetworkError> {
        Self::new_owned_with_protection(
            chunk_io,
            local_node_id,
            peer_node_id,
            session_id,
            channel_binding,
            RoutedSessionProtection::Plaintext,
            route_trace,
        )
    }

    pub(crate) fn new_owned_with_protection(
        chunk_io: AuthenticatedChunkStream,
        local_node_id: NodeId,
        peer_node_id: NodeId,
        session_id: Id,
        channel_binding: [u8; 32],
        protection: RoutedSessionProtection,
        route_trace: RouteTrace,
    ) -> Result<Self, NetworkError> {
        let chunk_io = Arc::new(Mutex::new(Some(chunk_io)));
        let mut stream = Self::new_with_protection(
            Box::new(LazyChunkByteStream::new(Arc::clone(&chunk_io))),
            local_node_id,
            peer_node_id,
            session_id,
            channel_binding,
            protection,
            route_trace,
        )?;
        stream.chunk_io = Some(chunk_io);
        Ok(stream)
    }

    #[cfg(test)]
    pub(crate) const fn has_owned_chunks(&self) -> bool {
        self.chunk_io.is_some()
    }

    pub(crate) fn into_chunk_stream(mut self) -> Result<AuthenticatedChunkStream, NetworkError> {
        let holder = self.chunk_io.take().ok_or_else(|| NetworkError::Io {
            operation: "open authenticated owned-chunk stream".to_string(),
            detail: "peer stream is a byte-compatibility transport".to_string(),
        })?;
        holder
            .lock()
            .map_err(|_| NetworkError::Io {
                operation: "open authenticated owned-chunk stream".to_string(),
                detail: "owned-chunk stream lock poisoned".to_string(),
            })?
            .take()
            .ok_or_else(|| NetworkError::Io {
                operation: "open authenticated owned-chunk stream".to_string(),
                detail: "owned-chunk stream already entered byte compatibility mode".to_string(),
            })
    }

    pub fn into_inner(self) -> BoxedNetworkStream {
        self.io
    }
}

struct LazyChunkByteStream {
    holder: Arc<Mutex<Option<AuthenticatedChunkStream>>>,
    io: Option<ChunkByteCompatibilityStream>,
}

impl LazyChunkByteStream {
    fn new(holder: Arc<Mutex<Option<AuthenticatedChunkStream>>>) -> Self {
        Self { holder, io: None }
    }

    fn ensure_io(&mut self) -> Result<&mut ChunkByteCompatibilityStream, std::io::Error> {
        if self.io.is_none() {
            let chunk_stream = self
                .holder
                .lock()
                .map_err(|_| std::io::Error::other("owned-chunk stream lock poisoned"))?
                .take()
                .ok_or_else(|| {
                    std::io::Error::other(
                        "owned-chunk stream was already consumed by its canonical API",
                    )
                })?;
            self.io = Some(spawn_chunk_byte_compatibility_adapter(chunk_stream));
        }
        self.io
            .as_mut()
            .ok_or_else(|| std::io::Error::other("missing owned-chunk byte adapter"))
    }
}

struct ChunkByteCompatibilityStream {
    io: tokio::io::DuplexStream,
    write_drained: tokio::sync::oneshot::Receiver<Result<(), String>>,
    shutdown_started: bool,
}

fn spawn_chunk_byte_compatibility_adapter(
    chunk_stream: AuthenticatedChunkStream,
) -> ChunkByteCompatibilityStream {
    let (application, bridge) = tokio::io::duplex(4 * BODY_CHUNK_MAX);
    let (mut bridge_reader, mut bridge_writer) = tokio::io::split(bridge);
    let (mut chunk_reader, mut chunk_writer) = chunk_stream.into_halves();
    let write_allocator = chunk_writer.allocator().clone();
    let (write_drained, write_drained_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let result = async {
            loop {
                let mut chunk = write_allocator
                    .allocate_data(BODY_CHUNK_MAX)
                    .await
                    .map_err(|error| error.to_string())?;
                let read = bridge_reader
                    .read(chunk.writable_payload())
                    .await
                    .map_err(|error| error.to_string())?;
                if read == 0 {
                    return chunk_writer
                        .finish()
                        .await
                        .map_err(|error| error.to_string());
                }
                chunk
                    .commit_payload(read)
                    .map_err(|error| error.to_string())?;
                write_allocator
                    .record_payload_copy(read, PayloadCopyReason::ByteStreamWriteAdapter);
                chunk_writer
                    .send_chunk(chunk)
                    .await
                    .map_err(|error| error.to_string())?;
            }
        }
        .await;
        if let Err(error) = &result {
            tracing::debug!(%error, "owned-chunk byte write adapter stopped");
            let _ = chunk_writer
                .reset(ResetReason {
                    detail: error.clone(),
                })
                .await;
        }
        let _ = write_drained.send(result);
    });
    let read_allocator = chunk_reader.allocator().clone();
    tokio::spawn(async move {
        loop {
            match chunk_reader.recv_chunk().await {
                Ok(Some(chunk)) => {
                    read_allocator
                        .record_payload_copy(chunk.len(), PayloadCopyReason::ByteStreamReadAdapter);
                    if bridge_writer.write_all(chunk.as_slice()).await.is_err() {
                        return;
                    }
                }
                Ok(None) => {
                    let _ = bridge_writer.shutdown().await;
                    return;
                }
                Err(error) => {
                    tracing::debug!(%error, "owned-chunk byte read adapter stopped");
                    let _ = bridge_writer.shutdown().await;
                    return;
                }
            }
        }
    });
    ChunkByteCompatibilityStream {
        io: application,
        write_drained: write_drained_rx,
        shutdown_started: false,
    }
}

impl AsyncRead for ChunkByteCompatibilityStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.io).poll_read(context, buffer)
    }
}

impl AsyncWrite for ChunkByteCompatibilityStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.io).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.io).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if !self.shutdown_started {
            match Pin::new(&mut self.io).poll_shutdown(context) {
                Poll::Ready(Ok(())) => self.shutdown_started = true,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }
        match Pin::new(&mut self.write_drained).poll(context) {
            Poll::Ready(Ok(Ok(()))) => Poll::Ready(Ok(())),
            Poll::Ready(Ok(Err(detail))) => Poll::Ready(Err(std::io::Error::other(detail))),
            Poll::Ready(Err(_)) => Poll::Ready(Err(std::io::Error::other(
                "owned-chunk byte write adapter stopped before draining",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncRead for LazyChunkByteStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.ensure_io() {
            Ok(io) => Pin::new(io).poll_read(context, buffer),
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

impl AsyncWrite for LazyChunkByteStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match self.ensure_io() {
            Ok(io) => Pin::new(io).poll_write(context, buffer),
            Err(error) => Poll::Ready(Err(error)),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.ensure_io() {
            Ok(io) => Pin::new(io).poll_flush(context),
            Err(error) => Poll::Ready(Err(error)),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.ensure_io() {
            Ok(io) => Pin::new(io).poll_shutdown(context),
            Err(error) => Poll::Ready(Err(error)),
        }
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

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut *self.io).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
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
    let key_configuration_valid = match descriptor.protection {
        RoutedSessionProtection::Plaintext => descriptor.source_ephemeral_public_key.is_none(),
        RoutedSessionProtection::Aead => descriptor
            .source_ephemeral_public_key
            .is_some_and(|key| key != [0; 32]),
    };
    if descriptor.protocol != ROUTED_SESSION_PROTOCOL
        || descriptor.path.len() < 2
        || hop_count > MAX_ROUTED_SESSION_HOPS
        || descriptor.endpoint_ids.len() != hop_count
        || descriptor.path.first() != Some(&descriptor.source_node_id)
        || descriptor.path.last() != Some(&descriptor.destination_node_id)
        || usize::from(descriptor.hop_limit) < hop_count
        || usize::from(descriptor.hop_limit) > MAX_ROUTED_SESSION_HOPS
        || descriptor
            .deadline_unix_millis
            .saturating_add(ROUTED_SESSION_DEADLINE_CLOCK_SKEW_MILLIS)
            < routed_session_now_unix_millis()
        || !key_configuration_valid
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
    open: &RoutedSessionOpen,
) -> Result<Vec<u8>, NetworkError> {
    validate_routed_session_descriptor(directory, &open.descriptor)?;
    routed_session_encode(&open.descriptor)
}

fn verify_routed_session_acceptance(
    descriptor: &RoutedSessionDescriptor,
    acceptance: &RoutedSessionAcceptance,
) -> Result<Vec<u8>, NetworkError> {
    let key_configuration_valid = match descriptor.protection {
        RoutedSessionProtection::Plaintext => acceptance.destination_ephemeral_public_key.is_none(),
        RoutedSessionProtection::Aead => acceptance
            .destination_ephemeral_public_key
            .is_some_and(|key| key != [0; 32]),
    };
    if acceptance.protocol != ROUTED_SESSION_PROTOCOL
        || acceptance.source_node_id != descriptor.source_node_id
        || acceptance.destination_node_id != descriptor.destination_node_id
        || acceptance.session_id != descriptor.session_id
        || acceptance.protection != descriptor.protection
        || (acceptance.reverse_process_affinity.is_some()
            && acceptance.reverse_process_affinity != descriptor.reverse_process_affinity)
        || !key_configuration_valid
    {
        return Err(NetworkError::AuthDenied);
    }
    routed_session_encode(acceptance)
}

// Old peers omit the capability echo and derive AEAD keys from the legacy
// descriptor. New peers bind the advertised role into the complete transcript.
fn negotiated_routed_session_descriptor_transcript(
    descriptor: &RoutedSessionDescriptor,
    acceptance: &RoutedSessionAcceptance,
) -> Result<Vec<u8>, NetworkError> {
    if acceptance.reverse_process_affinity.is_none()
        && descriptor.reverse_process_affinity.is_some()
    {
        let mut legacy = descriptor.clone();
        legacy.reverse_process_affinity = None;
        routed_session_encode(&legacy)
    } else {
        routed_session_encode(descriptor)
    }
}

async fn initiate_routed_session(
    mut stream: AuthenticatedDirectStream,
    directory: &DirectorySnapshot,
    path: &CandidatePath,
    identity: &dyn RoutedSessionIdentity,
    deadline_unix_millis: i64,
    protection: RoutedSessionProtection,
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
    let ephemeral = match protection {
        RoutedSessionProtection::Plaintext => None,
        RoutedSessionProtection::Aead => Some(routed_session_ephemeral_key()?),
    };
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
        protection,
        source_ephemeral_public_key: ephemeral.as_ref().map(|key| key.public),
        reverse_process_affinity:
            crate::routed_stream_pool::registered_routed_stream_incoming_affinity(
                identity.local_node_id(),
            ),
        hop_limit: u16::try_from(path.hops.len()).map_err(|_| NetworkError::AuthDenied)?,
        deadline_unix_millis,
    };
    validate_routed_session_descriptor(directory, &descriptor)?;
    let open = RoutedSessionOpen { descriptor };
    let open_frame = routed_session_encode(&open)?;
    write_routed_session_frame(&mut *stream.io, ROUTED_SESSION_OPEN_FRAME, &open_frame).await?;

    let acceptance_frame =
        read_routed_session_frame(&mut *stream.io, ROUTED_SESSION_ACCEPT_FRAME).await?;
    let acceptance: RoutedSessionAcceptance = routed_session_decode(&acceptance_frame)?;
    let acceptance_transcript = verify_routed_session_acceptance(&open.descriptor, &acceptance)?;
    let descriptor_transcript =
        negotiated_routed_session_descriptor_transcript(&open.descriptor, &acceptance)?;
    let (channel_binding, io) = match protection {
        RoutedSessionProtection::Plaintext => {
            let binding = plaintext_routed_session_binding(&open.descriptor.session_id);
            (binding, plaintext_routed_session_chunk_stream(stream.io)?)
        }
        RoutedSessionProtection::Aead => {
            let ephemeral = ephemeral.expect("AEAD routed session generated an ephemeral key");
            let destination_key = acceptance
                .destination_ephemeral_public_key
                .as_ref()
                .expect("validated AEAD acceptance key");
            let shared_secret = routed_session_agree(ephemeral.private, destination_key)?;
            let binding = routed_session_channel_binding(
                &shared_secret,
                &descriptor_transcript,
                &acceptance_transcript,
            );
            let io = encrypted_routed_session_chunk_stream(
                stream.io,
                shared_secret,
                open.descriptor.session_id.clone(),
                binding,
                true,
            )?;
            (binding, io)
        }
    };
    PeerStream::new_owned_with_protection(
        io,
        open.descriptor.source_node_id,
        open.descriptor.destination_node_id,
        open.descriptor.session_id,
        channel_binding,
        open.descriptor.protection,
        RouteTrace::from(path),
    )
}

async fn accept_routed_session_destination(
    mut stream: AuthenticatedDirectStream,
    open: RoutedSessionOpen,
    descriptor_transcript: Vec<u8>,
) -> Result<PeerStream, NetworkError> {
    let descriptor = &open.descriptor;
    if descriptor.destination_node_id != stream.local_node_id {
        return Err(NetworkError::FrameTargetMismatch {
            requested_target_node_id: stream.local_node_id,
            frame_target_node_id: descriptor.destination_node_id.clone(),
        });
    }
    let ephemeral = match descriptor.protection {
        RoutedSessionProtection::Plaintext => None,
        RoutedSessionProtection::Aead => Some(routed_session_ephemeral_key()?),
    };
    let acceptance = RoutedSessionAcceptance {
        protocol: ROUTED_SESSION_PROTOCOL.to_string(),
        source_node_id: descriptor.source_node_id.clone(),
        destination_node_id: descriptor.destination_node_id.clone(),
        session_id: descriptor.session_id.clone(),
        protection: descriptor.protection,
        destination_ephemeral_public_key: ephemeral.as_ref().map(|key| key.public),
        reverse_process_affinity: descriptor.reverse_process_affinity,
    };
    let acceptance_transcript = routed_session_encode(&acceptance)?;
    let acceptance_frame = routed_session_encode(&acceptance)?;
    write_routed_session_frame(
        &mut *stream.io,
        ROUTED_SESSION_ACCEPT_FRAME,
        &acceptance_frame,
    )
    .await?;
    let (channel_binding, io) = match descriptor.protection {
        RoutedSessionProtection::Plaintext => {
            let binding = plaintext_routed_session_binding(&descriptor.session_id);
            (binding, plaintext_routed_session_chunk_stream(stream.io)?)
        }
        RoutedSessionProtection::Aead => {
            let ephemeral = ephemeral.expect("AEAD routed session generated an ephemeral key");
            let source_key = descriptor
                .source_ephemeral_public_key
                .as_ref()
                .expect("validated AEAD source key");
            let shared_secret = routed_session_agree(ephemeral.private, source_key)?;
            let binding = routed_session_channel_binding(
                &shared_secret,
                &descriptor_transcript,
                &acceptance_transcript,
            );
            let io = encrypted_routed_session_chunk_stream(
                stream.io,
                shared_secret,
                descriptor.session_id.clone(),
                binding,
                false,
            )?;
            (binding, io)
        }
    };
    let mut nodes = descriptor.path.clone();
    nodes.reverse();
    let mut endpoint_ids = descriptor.endpoint_ids.clone();
    endpoint_ids.reverse();
    let mut peer = PeerStream::new_owned_with_protection(
        io,
        descriptor.destination_node_id.clone(),
        descriptor.source_node_id.clone(),
        descriptor.session_id.clone(),
        channel_binding,
        descriptor.protection,
        RouteTrace {
            nodes,
            endpoint_ids,
        },
    )?;
    peer.reverse_process_affinity = descriptor.reverse_process_affinity;
    Ok(peer)
}

/// Establishes a target-bound routed session over one already-selected direct
/// endpoint.
///
/// This is the direct-path execution primitive shared by the route-planning
/// connector and composition boundaries that already hold a generation-fenced
/// endpoint choice. It still rechecks current directory authority, transport
/// authentication, capabilities, and the routed-session handshake.
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
        initiate_routed_session(
            stream,
            directory,
            &path,
            identity,
            deadline_unix_millis,
            options.protection,
        )
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
/// The direct transport supplies the authenticated adjacent peer. The open
/// must describe exactly one hop from that peer to this destination. Only
/// after those checks and a matching destination acceptance does the function
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
    let open: RoutedSessionOpen = routed_session_decode(&open_frame)?;
    let descriptor_transcript = verify_routed_session_open(directory, &open)?;
    if open.descriptor.path.len() != 2 {
        return Err(NetworkError::AuthDenied);
    }
    let position = incoming_routed_session_position(&stream, &open.descriptor)?;
    if position + 1 != open.descriptor.path.len() {
        return Err(NetworkError::FrameTargetMismatch {
            requested_target_node_id: stream.local_node_id,
            frame_target_node_id: open.descriptor.destination_node_id.clone(),
        });
    }
    accept_routed_session_destination(stream, open, descriptor_transcript).await
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

#[derive(Debug)]
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
    allocator: ChunkAllocator,
) -> Result<u64, RoutedRelayCopyError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut copied = 0_u64;
    loop {
        let mut chunk = allocator
            .allocate_data(BODY_CHUNK_MAX)
            .await
            .map_err(|error| RoutedRelayCopyError {
                edge: reader_edge,
                operation: "allocate opaque routed-session relay slab",
                source: std::io::Error::other(error.to_string()),
            })?;
        let read = reader
            .read(chunk.writable_payload())
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
        chunk
            .commit_payload(read)
            .map_err(|error| RoutedRelayCopyError {
                edge: reader_edge,
                operation: "commit opaque routed-session relay slab",
                source: std::io::Error::other(error.to_string()),
            })?;
        writer
            .write_all(chunk.as_slice())
            .await
            .map_err(|source| RoutedRelayCopyError {
                edge: writer_edge,
                operation: "write opaque routed-session relay bytes",
                source,
            })?;
        copied = copied.saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
    }
}

#[cfg(target_os = "linux")]
fn splice_routed_tcp_direction(
    reader: std::net::TcpStream,
    reader_edge: RoutedRelayAdjacentEdge,
    writer: std::net::TcpStream,
    writer_edge: RoutedRelayAdjacentEdge,
) -> Result<u64, RoutedRelayCopyError> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    fn io_error(edge: RoutedRelayAdjacentEdge, operation: &'static str) -> RoutedRelayCopyError {
        RoutedRelayCopyError {
            edge,
            operation,
            source: std::io::Error::last_os_error(),
        }
    }

    let mut pipe_fds = [-1; 2];
    // SAFETY: `pipe_fds` points to two valid integers and successful return
    // initializes both descriptors. Each is immediately adopted by OwnedFd.
    if unsafe { libc::pipe2(pipe_fds.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        return Err(io_error(
            reader_edge,
            "create Linux routed-session splice pipe",
        ));
    }
    // SAFETY: successful pipe2 returned two fresh owned descriptors.
    let pipe_read = unsafe { OwnedFd::from_raw_fd(pipe_fds[0]) };
    // SAFETY: successful pipe2 returned two fresh owned descriptors.
    let pipe_write = unsafe { OwnedFd::from_raw_fd(pipe_fds[1]) };
    let mut copied = 0_u64;
    loop {
        let moved_to_pipe = loop {
            // SAFETY: all descriptors are live for the duration of the call;
            // null offsets select and advance their current stream positions.
            let moved = unsafe {
                libc::splice(
                    reader.as_raw_fd(),
                    std::ptr::null_mut(),
                    pipe_write.as_raw_fd(),
                    std::ptr::null_mut(),
                    BODY_CHUNK_MAX,
                    libc::SPLICE_F_MOVE,
                )
            };
            if moved >= 0 {
                break moved as usize;
            }
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            let _ = reader.shutdown(std::net::Shutdown::Both);
            let _ = writer.shutdown(std::net::Shutdown::Both);
            return Err(RoutedRelayCopyError {
                edge: reader_edge,
                operation: "splice opaque routed-session bytes into kernel pipe",
                source: error,
            });
        };
        if moved_to_pipe == 0 {
            if let Err(source) = writer.shutdown(std::net::Shutdown::Write)
                && source.kind() != std::io::ErrorKind::NotConnected
            {
                return Err(RoutedRelayCopyError {
                    edge: writer_edge,
                    operation: "half-close spliced routed-session relay edge",
                    source,
                });
            }
            return Ok(copied);
        }
        let mut remaining = moved_to_pipe;
        while remaining != 0 {
            let moved_to_socket = loop {
                // SAFETY: all descriptors are live, the pipe contains at least
                // `remaining` bytes, and null offsets advance stream positions.
                let moved = unsafe {
                    libc::splice(
                        pipe_read.as_raw_fd(),
                        std::ptr::null_mut(),
                        writer.as_raw_fd(),
                        std::ptr::null_mut(),
                        remaining,
                        libc::SPLICE_F_MOVE,
                    )
                };
                if moved > 0 {
                    break moved as usize;
                }
                let error = if moved == 0 {
                    std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "splice wrote zero routed-session bytes",
                    )
                } else {
                    std::io::Error::last_os_error()
                };
                if error.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                if error.kind() == std::io::ErrorKind::BrokenPipe {
                    // The peer can finish the routed carrier and close its
                    // receive half while the opposite splice task still has a
                    // final opaque record staged in this direction's kernel
                    // pipe. Nothing can consume those bytes once the socket
                    // reports EPIPE, so retire only this direction. Shutting
                    // down both cloned sockets here would also abort the
                    // independent reverse direction during normal teardown.
                    let _ = reader.shutdown(std::net::Shutdown::Read);
                    return Ok(copied);
                }
                let _ = reader.shutdown(std::net::Shutdown::Both);
                let _ = writer.shutdown(std::net::Shutdown::Both);
                return Err(RoutedRelayCopyError {
                    edge: writer_edge,
                    operation: "splice opaque routed-session bytes from kernel pipe",
                    source: error,
                });
            };
            remaining -= moved_to_socket;
        }
        copied = copied.saturating_add(moved_to_pipe as u64);
    }
}

#[cfg(target_os = "linux")]
async fn splice_routed_tcp_pair(
    incoming: tokio::net::TcpStream,
    outgoing: tokio::net::TcpStream,
) -> Result<(u64, u64), RoutedRelayCopyError> {
    let incoming = incoming.into_std().map_err(|source| RoutedRelayCopyError {
        edge: RoutedRelayAdjacentEdge::Incoming,
        operation: "take incoming TCP relay stream",
        source,
    })?;
    let outgoing = outgoing.into_std().map_err(|source| RoutedRelayCopyError {
        edge: RoutedRelayAdjacentEdge::Outgoing,
        operation: "take outgoing TCP relay stream",
        source,
    })?;
    incoming
        .set_nonblocking(false)
        .map_err(|source| RoutedRelayCopyError {
            edge: RoutedRelayAdjacentEdge::Incoming,
            operation: "configure incoming TCP relay stream for splice",
            source,
        })?;
    outgoing
        .set_nonblocking(false)
        .map_err(|source| RoutedRelayCopyError {
            edge: RoutedRelayAdjacentEdge::Outgoing,
            operation: "configure outgoing TCP relay stream for splice",
            source,
        })?;
    let toward_destination_reader =
        incoming
            .try_clone()
            .map_err(|source| RoutedRelayCopyError {
                edge: RoutedRelayAdjacentEdge::Incoming,
                operation: "clone incoming TCP relay stream",
                source,
            })?;
    let toward_destination_writer =
        outgoing
            .try_clone()
            .map_err(|source| RoutedRelayCopyError {
                edge: RoutedRelayAdjacentEdge::Outgoing,
                operation: "clone outgoing TCP relay stream",
                source,
            })?;
    let toward_destination = tokio::task::spawn_blocking(move || {
        splice_routed_tcp_direction(
            toward_destination_reader,
            RoutedRelayAdjacentEdge::Incoming,
            toward_destination_writer,
            RoutedRelayAdjacentEdge::Outgoing,
        )
    });
    let toward_source = tokio::task::spawn_blocking(move || {
        splice_routed_tcp_direction(
            outgoing,
            RoutedRelayAdjacentEdge::Outgoing,
            incoming,
            RoutedRelayAdjacentEdge::Incoming,
        )
    });
    let (toward_destination, toward_source) = tokio::join!(toward_destination, toward_source);
    let toward_destination = toward_destination.map_err(|source| RoutedRelayCopyError {
        edge: RoutedRelayAdjacentEdge::Incoming,
        operation: "join destination-bound TCP splice relay",
        source: std::io::Error::other(source.to_string()),
    })??;
    let toward_source = toward_source.map_err(|source| RoutedRelayCopyError {
        edge: RoutedRelayAdjacentEdge::Outgoing,
        operation: "join source-bound TCP splice relay",
        source: std::io::Error::other(source.to_string()),
    })??;
    Ok((toward_destination, toward_source))
}

async fn forward_routed_relay_pair(
    incoming: BoxedNetworkStream,
    outgoing: BoxedNetworkStream,
) -> Result<(u64, u64), RoutedRelayCopyError> {
    forward_routed_relay_pair_with_allocator(incoming, outgoing, ChunkAllocator::default()).await
}

async fn forward_routed_relay_pair_with_allocator(
    incoming: BoxedNetworkStream,
    outgoing: BoxedNetworkStream,
    relay_allocator: ChunkAllocator,
) -> Result<(u64, u64), RoutedRelayCopyError> {
    #[cfg(target_os = "linux")]
    if incoming.as_any().is::<tokio::net::TcpStream>()
        && outgoing.as_any().is::<tokio::net::TcpStream>()
    {
        let incoming = incoming
            .into_any()
            .downcast::<tokio::net::TcpStream>()
            .expect("checked incoming TCP relay stream");
        let outgoing = outgoing
            .into_any()
            .downcast::<tokio::net::TcpStream>()
            .expect("checked outgoing TCP relay stream");
        metrics::counter!("sinomo.network.relay.mode", "mode" => "linux-splice").increment(1);
        return splice_routed_tcp_pair(*incoming, *outgoing).await;
    }

    metrics::counter!("sinomo.network.relay.mode", "mode" => "pooled-slab").increment(1);
    let (incoming_read, incoming_write) = tokio::io::split(incoming);
    let (outgoing_read, outgoing_write) = tokio::io::split(outgoing);
    let toward_destination = copy_routed_relay_direction(
        incoming_read,
        RoutedRelayAdjacentEdge::Incoming,
        outgoing_write,
        RoutedRelayAdjacentEdge::Outgoing,
        relay_allocator.clone(),
    );
    let toward_source = copy_routed_relay_direction(
        outgoing_read,
        RoutedRelayAdjacentEdge::Outgoing,
        incoming_write,
        RoutedRelayAdjacentEdge::Incoming,
        relay_allocator,
    );
    tokio::pin!(toward_destination);
    tokio::pin!(toward_source);
    tokio::select! {
        result = &mut toward_destination => match result {
            Ok(bytes) => Ok((bytes, toward_source.await?)),
            Err(error) => Err(error),
        },
        result = &mut toward_source => match result {
            Ok(bytes) => Ok((toward_destination.await?, bytes)),
            Err(error) => Err(error),
        },
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

/// Accepts a routed-session open at either an intermediate relay or the named
/// destination.
///
/// Relays validate the immutable path, establish exactly the next
/// transport-authenticated direct edge, and then copy only bounded opaque
/// frames. They never construct a [`PeerStream`] or receive endpoint key
/// material.
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
    let open: RoutedSessionOpen = routed_session_decode(&open_frame)?;
    let descriptor_transcript = verify_routed_session_open(directory, &open)?;
    let position = incoming_routed_session_position(&stream, &open.descriptor)?;
    if position + 1 == open.descriptor.path.len() {
        return accept_routed_session_destination(stream, open, descriptor_transcript)
            .await
            .map(RoutedSessionDisposition::Destination);
    }

    let descriptor = &open.descriptor;
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
        let acceptance: RoutedSessionAcceptance = routed_session_decode(&acceptance_frame)
            .map_err(RelayEstablishmentError::DownstreamSession)?;
        verify_routed_session_acceptance(descriptor, &acceptance)
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
    let (bytes_toward_destination, bytes_toward_source) =
        forward_routed_relay_pair(stream.io, outgoing.io)
            .await
            .map_err(|error| {
                routed_relay_copy_error(
                    error,
                    failure_sink,
                    &relay_node_id,
                    &next_node_id,
                    &endpoint_id,
                )
            })?;
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
                options.protection,
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
        destination_process_affinity: DestinationProcessAffinity,
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
        let paths = plan_routes_with_destination_process_affinity(
            directory,
            &connectivity,
            &self.local_node_id,
            &destination_node_id,
            options.constraints,
            destination_process_affinity,
        );
        let deadline = tokio::time::Instant::now() + options.timeout;
        let path_count = paths.len();
        let mut attempts = 0_usize;
        let mut failures = Vec::new();
        for (path_index, path) in paths.iter().enumerate() {
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
                    path,
                    self.identity.as_ref(),
                    attempt_deadline_unix_millis,
                    options.protection,
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

    pub(crate) fn connect_with_destination_process_affinity<'a>(
        &'a self,
        destination_node_id: NodeId,
        options: ConnectOptions,
        destination_process_affinity: DestinationProcessAffinity,
    ) -> RoutedConnectFuture<'a> {
        Box::pin(async move {
            tokio::time::timeout(
                options.timeout,
                self.connect_inner(destination_node_id, options, destination_process_affinity),
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
        self.connect_with_destination_process_affinity(
            destination_node_id,
            options,
            DestinationProcessAffinity::Any,
        )
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
    use crate::network_module::{
        NetworkFuture, NetworkModule, NetworkModuleContext, NetworkModuleDescriptor,
        NetworkModuleEndpointRole, NetworkModuleRegistry, set_network_module_registry,
    };
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

    #[tokio::test]
    async fn encrypted_record_keeps_one_complete_maximum_mux_frame() {
        let (client, mut wire) = tokio::io::duplex(2 * ROUTED_SESSION_DATA_FRAME_BYTES);
        let session_id = Id::from_legacy_named("session_vectored_record").expect("session id");
        let binding = [0x29; 32];
        let shared_secret = [0x31; 32];
        let encrypted = encrypted_routed_session_chunk_stream(
            Box::new(client),
            shared_secret,
            session_id.clone(),
            binding,
            true,
        )
        .expect("owned encrypted stream");
        let (_reader, mut writer) = encrypted.into_halves();
        let mux_header = [0x53; ROUTED_STREAM_MUX_HEADER_BYTES];
        let rpc_header = [0x52; RPC_RECORD_HEADER_BYTES];
        let mux_payload = vec![0x71; BODY_CHUNK_MAX];
        let mut record = writer
            .allocator()
            .allocate_data(BODY_CHUNK_MAX)
            .await
            .expect("allocate maximum body chunk");
        record.writable_payload()[..mux_payload.len()].copy_from_slice(&mux_payload);
        record
            .commit_payload(mux_payload.len())
            .expect("commit maximum body");
        record.prepend(&rpc_header).expect("prepend RPC header");
        record.prepend(&mux_header).expect("prepend mux header");
        assert_eq!(record.len(), ROUTED_SESSION_DATA_FRAME_BYTES);
        writer.send_chunk(record).await.expect("send secure record");

        let mut header = [0_u8; 16];
        wire.read_exact(&mut header)
            .await
            .expect("read secure header");
        assert_eq!(&header[..4], &ROUTED_SESSION_DATA_WIRE_PREFIX);
        assert_eq!(
            u32::from_be_bytes(header[12..].try_into().expect("length")) as usize,
            ROUTED_SESSION_DATA_FRAME_BYTES
        );
        let mut ciphertext =
            vec![0_u8; ROUTED_SESSION_DATA_FRAME_BYTES + ring::aead::CHACHA20_POLY1305.tag_len()];
        wire.read_exact(&mut ciphertext)
            .await
            .expect("read secure payload");
        let aad = routed_session_data_aad(
            &session_id,
            &binding,
            0,
            0,
            ROUTED_SESSION_DATA_FRAME_BYTES as u32,
        );
        let outgoing_key = routed_session_data_key(&shared_secret, &binding, 0);
        let key = ring::aead::UnboundKey::new(&ring::aead::CHACHA20_POLY1305, &outgoing_key)
            .expect("test key");
        let plaintext = ring::aead::LessSafeKey::new(key)
            .open_in_place(
                routed_session_data_nonce(0, 0),
                ring::aead::Aad::from(aad),
                &mut ciphertext,
            )
            .expect("decrypt record");
        assert_eq!(&plaintext[..mux_header.len()], &mux_header);
        assert_eq!(
            &plaintext[mux_header.len()..mux_header.len() + rpc_header.len()],
            &rpc_header
        );
        assert_eq!(
            &plaintext[mux_header.len() + rpc_header.len()..],
            &mux_payload
        );
    }

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).expect("node id")
    }

    struct TestDirectoryIrohNetworkModule;

    impl NetworkModule for TestDirectoryIrohNetworkModule {
        fn descriptor(&self) -> NetworkModuleDescriptor {
            NetworkModuleDescriptor {
                id: "routing-test-iroh".to_string(),
                transports: vec![ClusterTransportKind::Iroh],
                address_schemes: vec!["iroh".to_string()],
                dialer: true,
                listener: false,
                supports_runtime_toggle: true,
            }
        }

        fn start(&self, _ctx: &NetworkModuleContext) -> Result<(), NetworkError> {
            Ok(())
        }

        fn stop(&self) -> Result<(), NetworkError> {
            Ok(())
        }

        fn connect_link(
            &self,
            endpoint: &ClusterEndpointRecord,
            _health: crate::NetworkHealthPolicy,
        ) -> NetworkFuture<crate::direct_link::AuthenticatedLink> {
            let address = endpoint.address.clone();
            Box::pin(async move {
                Err(NetworkError::Io {
                    operation: "routing test iroh dial".to_string(),
                    detail: format!("test module does not directly dial {address}"),
                })
            })
        }

        fn local_endpoints(&self) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
            Ok(Vec::new())
        }

        fn endpoint_role(&self, endpoint: &ClusterEndpointRecord) -> NetworkModuleEndpointRole {
            if endpoint.transport != ClusterTransportKind::Iroh {
                return NetworkModuleEndpointRole::Unknown;
            }
            let Some(public_key) = endpoint
                .address
                .strip_prefix("iroh://")
                .and_then(|rest| rest.split(['?', '#']).next())
                .filter(|public_key| !public_key.is_empty())
            else {
                return NetworkModuleEndpointRole::Unknown;
            };
            let shadow_node_id = sinomo_node::shadow_daemon_node_id(&endpoint.node_id);
            if endpoint.endpoint_id
                == Id::derived(
                    "sinomo.iroh.endpoint",
                    format!("{}\0{public_key}", shadow_node_id.as_str()),
                )
            {
                return NetworkModuleEndpointRole::ShadowDaemon;
            }
            if endpoint.endpoint_id
                == Id::derived(
                    "sinomo.iroh.endpoint",
                    format!("{}\0{public_key}", endpoint.node_id.as_str()),
                )
            {
                return NetworkModuleEndpointRole::MainDaemon;
            }
            NetworkModuleEndpointRole::Unknown
        }
    }

    fn install_test_directory_iroh_network_module() {
        static INSTALL: std::sync::Once = std::sync::Once::new();
        INSTALL.call_once(|| {
            let registry = NetworkModuleRegistry::new();
            registry.register(Arc::new(TestDirectoryIrohNetworkModule), None, true, false);
            set_network_module_registry(Arc::new(registry));
        });
    }

    fn directory_iroh_endpoint(
        node_id: &NodeId,
        endpoint_public_key: &str,
        shadow: bool,
    ) -> ClusterEndpointRecord {
        let endpoint_identity = if shadow {
            sinomo_node::shadow_daemon_node_id(node_id)
        } else {
            node_id.clone()
        };
        ClusterEndpointRecord {
            endpoint_id: Id::derived(
                "sinomo.iroh.endpoint",
                format!("{}\0{endpoint_public_key}", endpoint_identity.as_str()),
            ),
            node_id: node_id.clone(),
            transport: ClusterTransportKind::Iroh,
            address: format!(
                "iroh://{endpoint_public_key}?relay=https://relay.routing-test.invalid./"
            ),
            source: ClusterEndpointSource::Rendezvous,
            privacy: ClusterEndpointPrivacy::Relayed,
            priority: 90,
            last_observed_latency_ms: None,
        }
    }

    fn directory_fixture_with_endpoints(
        directory_generation: u64,
        nodes: impl IntoIterator<Item = (NodeId, Vec<ClusterEndpointRecord>)>,
    ) -> DirectorySnapshot {
        DirectorySnapshot::try_new(
            DirectoryVersion(directory_generation),
            nodes
                .into_iter()
                .map(|(node_id, endpoints)| {
                    DirectoryNode::try_new(node_id.clone(), node_id.to_string(), endpoints)
                        .expect("directory node")
                })
                .collect::<Vec<_>>(),
        )
        .expect("directory")
    }

    #[tokio::test]
    async fn portable_relay_forwards_both_directions_with_bounded_owned_buffers() {
        let (mut source, incoming_relay) = tokio::io::duplex(4096);
        let (outgoing_relay, mut destination) = tokio::io::duplex(4096);
        let pool = ChunkPool::new(SECURE_WIRE_MAX * 2, SECURE_WIRE_MAX, 2, 0);
        let allocator = ChunkAllocator::new(pool, SECURE_WIRE_MAX * 2, SECURE_WIRE_MAX);
        let metrics = allocator.clone();
        let relay = tokio::spawn(forward_routed_relay_pair_with_allocator(
            Box::new(incoming_relay),
            Box::new(outgoing_relay),
            allocator,
        ));
        let request = vec![0x31; BODY_CHUNK_MAX * 3 + 79];
        let mut received_request = Vec::new();
        let (sent, received) = tokio::join!(
            async {
                source.write_all(&request).await?;
                source.shutdown().await
            },
            destination.read_to_end(&mut received_request)
        );
        sent.expect("write relay request");
        received.expect("read relayed request");
        assert_eq!(received_request, request);

        let response = vec![0x73; BODY_CHUNK_MAX * 2 + 113];
        let mut received_response = Vec::new();
        let (sent, received) = tokio::join!(
            async {
                destination.write_all(&response).await?;
                destination.shutdown().await
            },
            source.read_to_end(&mut received_response)
        );
        sent.expect("write relay response");
        received.expect("read relayed response");
        assert_eq!(received_response, response);
        assert_eq!(
            relay
                .await
                .expect("join portable relay")
                .expect("relay pair"),
            (request.len() as u64, response.len() as u64)
        );
        let metrics = metrics.metrics();
        assert_eq!(
            metrics.payload_copy_bytes, 0,
            "the portable relay must transfer the same pooled slab without a userspace payload copy"
        );
        assert_eq!(metrics.active_bytes, 0);
        assert!(
            metrics.peak_active_bytes <= SECURE_WIRE_MAX * 2,
            "the portable relay may retain at most one slab per active direction"
        );
        assert!(
            metrics.allocations <= 2,
            "the portable relay must reuse its per-direction slabs across the complete transfer"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn linux_tcp_relay_uses_splice_for_full_duplex_transfer() {
        let incoming_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind incoming relay listener");
        let outgoing_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind outgoing relay listener");
        let source = tokio::net::TcpStream::connect(
            incoming_listener.local_addr().expect("incoming address"),
        );
        let destination = tokio::net::TcpStream::connect(
            outgoing_listener.local_addr().expect("outgoing address"),
        );
        let (source, incoming, destination, outgoing) = tokio::join!(
            source,
            incoming_listener.accept(),
            destination,
            outgoing_listener.accept()
        );
        let mut source = source.expect("connect source");
        let mut destination = destination.expect("connect destination");
        let (incoming_relay, _) = incoming.expect("accept incoming relay");
        let (outgoing_relay, _) = outgoing.expect("accept outgoing relay");
        let relay = tokio::spawn(forward_routed_relay_pair(
            Box::new(incoming_relay),
            Box::new(outgoing_relay),
        ));

        let request = vec![0x41; BODY_CHUNK_MAX * 2 + 37];
        let mut received_request = Vec::new();
        let (sent, received) = tokio::join!(
            async {
                source.write_all(&request).await?;
                source.shutdown().await
            },
            destination.read_to_end(&mut received_request)
        );
        sent.expect("write splice request");
        received.expect("read spliced request");
        assert_eq!(received_request, request);

        let response = vec![0x82; BODY_CHUNK_MAX + 101];
        let mut received_response = Vec::new();
        let (sent, received) = tokio::join!(
            async {
                destination.write_all(&response).await?;
                destination.shutdown().await
            },
            source.read_to_end(&mut received_response)
        );
        sent.expect("write splice response");
        received.expect("read spliced response");
        assert_eq!(received_response, response);
        assert_eq!(
            relay
                .await
                .expect("join splice relay")
                .expect("splice pair"),
            (request.len() as u64, response.len() as u64)
        );
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

    fn folded_shadow_edge(
        source: &NodeId,
        destination: &NodeId,
        name: &str,
        directory_generation: u64,
        latency_micros: u64,
        address: &str,
    ) -> DirectConnectivityObservation {
        let mut observation = edge(
            source,
            destination,
            name,
            directory_generation,
            latency_micros,
        );
        observation.endpoint.transport = ClusterTransportKind::LanTcp;
        observation.endpoint.address = address.to_string();
        observation.endpoint.endpoint_id = sinomo_node::daemon_lan_endpoint_id(
            &sinomo_node::shadow_daemon_node_id(destination),
            address,
        );
        observation.endpoint.node_id = destination.clone();
        observation
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
    fn planner_applies_main_daemon_affinity_only_to_the_terminal_endpoint() {
        let source = node("node_route_affinity_source");
        let relay = node("node_route_affinity_relay");
        let destination = node("node_route_affinity_destination");
        let mut shadow_direct = edge(
            &source,
            &destination,
            "endpoint_route_affinity_shadow_direct",
            61,
            1,
        );
        shadow_direct.endpoint.transport = ClusterTransportKind::LanTcp;
        shadow_direct.endpoint.address = "198.51.100.61:21354".to_string();
        let shadow_direct_id = shadow_direct.endpoint.endpoint_id.clone();

        let mut main_direct = edge(
            &source,
            &destination,
            "endpoint_route_affinity_main_direct",
            61,
            30,
        );
        main_direct.endpoint.transport = ClusterTransportKind::LanTcp;
        main_direct.endpoint.address = "198.51.100.61:21353".to_string();
        let main_direct_id = main_direct.endpoint.endpoint_id.clone();

        // A shadow-looking endpoint is still valid as an intermediate hop. The
        // affinity describes the process that terminates the relationship.
        let mut shadow_relay = edge(
            &source,
            &relay,
            "endpoint_route_affinity_shadow_relay",
            61,
            5,
        );
        shadow_relay.endpoint.transport = ClusterTransportKind::LanTcp;
        shadow_relay.endpoint.address = "198.51.100.62:21354".to_string();
        let shadow_relay_id = shadow_relay.endpoint.endpoint_id.clone();

        let mut relay_to_main = edge(
            &relay,
            &destination,
            "endpoint_route_affinity_relay_to_main",
            61,
            5,
        );
        relay_to_main.endpoint.transport = ClusterTransportKind::LanTcp;
        relay_to_main.endpoint.address = "198.51.100.61:21353".to_string();
        let relay_to_main_id = relay_to_main.endpoint.endpoint_id.clone();

        let (directory, connectivity) = fixtures(
            61,
            vec![shadow_direct, main_direct, shadow_relay, relay_to_main],
        );
        let unrestricted = plan_routes(
            &directory,
            &connectivity,
            &source,
            &destination,
            RouteConstraints::default(),
        );
        assert!(unrestricted.iter().any(|path| {
            path.hops
                .last()
                .is_some_and(|hop| hop.endpoint.endpoint_id == shadow_direct_id)
        }));

        let main_only = plan_routes_with_destination_process_affinity(
            &directory,
            &connectivity,
            &source,
            &destination,
            RouteConstraints::default(),
            DestinationProcessAffinity::MainDaemon,
        );

        assert_eq!(main_only.len(), 2);
        assert!(main_only.iter().all(|path| {
            path.hops.last().is_some_and(|hop| {
                hop.endpoint.endpoint_id == main_direct_id
                    || hop.endpoint.endpoint_id == relay_to_main_id
            })
        }));
        assert!(main_only.iter().any(|path| {
            path.hops.first().is_some_and(|hop| {
                hop.endpoint.endpoint_id == shadow_relay_id
                    && path
                        .hops
                        .last()
                        .is_some_and(|last| last.endpoint.endpoint_id == relay_to_main_id)
            })
        }));
    }

    #[test]
    fn planner_rejects_unknown_terminal_process_for_main_affinity() {
        let source = node("node_route_affinity_unknown_source");
        let destination = node("node_route_affinity_unknown_destination");
        let (directory, connectivity) = fixtures(
            62,
            vec![edge(
                &source,
                &destination,
                "endpoint_route_affinity_unknown_terminal",
                62,
                1,
            )],
        );

        assert_eq!(
            plan_routes(
                &directory,
                &connectivity,
                &source,
                &destination,
                RouteConstraints::default(),
            )
            .len(),
            1
        );
        assert!(
            plan_routes_with_destination_process_affinity(
                &directory,
                &connectivity,
                &source,
                &destination,
                RouteConstraints::default(),
                DestinationProcessAffinity::MainDaemon,
            )
            .is_empty(),
            "an unclassified terminal endpoint is not positive main-daemon reachability"
        );
    }

    #[test]
    fn planner_rejects_a_folded_shadow_only_terminal_for_main_daemon_affinity() {
        let source = node("node_route_affinity_shadow_only_source");
        let destination = node("node_route_affinity_shadow_only_destination");
        let address = "198.51.100.81:22154";
        let folded_shadow = folded_shadow_edge(
            &source,
            &destination,
            "endpoint_route_affinity_folded_shadow",
            81,
            1,
            address,
        );

        let (directory, connectivity) = fixtures(81, vec![folded_shadow]);
        assert_eq!(
            plan_routes(
                &directory,
                &connectivity,
                &source,
                &destination,
                RouteConstraints::default(),
            )
            .len(),
            1,
            "an affinity-neutral caller may use a folded shadow endpoint"
        );
        assert!(
            plan_routes_with_destination_process_affinity(
                &directory,
                &connectivity,
                &source,
                &destination,
                RouteConstraints::default(),
                DestinationProcessAffinity::MainDaemon,
            )
            .is_empty(),
            "a folded shadow endpoint is not positive main-daemon reachability"
        );
    }

    #[test]
    fn planner_cannot_treat_a_self_target_as_main_daemon_process_proof() {
        let source = node("node_route_affinity_self_target");
        let other = node("node_route_affinity_self_target_other");
        let (directory, connectivity) = fixtures(
            82,
            vec![edge(
                &source,
                &other,
                "endpoint_route_affinity_self_target_other",
                82,
                1,
            )],
        );

        assert_eq!(
            plan_routes(
                &directory,
                &connectivity,
                &source,
                &source,
                RouteConstraints::default(),
            )
            .len(),
            1,
            "the compatibility planner retains its affinity-neutral zero-hop path"
        );
        assert!(
            plan_routes_with_destination_process_affinity(
                &directory,
                &connectivity,
                &source,
                &source,
                RouteConstraints::default(),
                DestinationProcessAffinity::MainDaemon,
            )
            .is_empty(),
            "a shared node id cannot prove that the current process is the main daemon"
        );
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
    fn planner_places_unobserved_module_recovery_between_healthy_and_degraded_paths() {
        install_test_directory_iroh_network_module();
        let source = node("node_route_recovery_order_source");
        let destination = node("node_route_recovery_order_destination");
        let mut healthy = edge(
            &source,
            &destination,
            "endpoint_route_recovery_order_healthy",
            93,
            30,
        );
        healthy.endpoint.transport = ClusterTransportKind::LanTcp;
        healthy.endpoint.address = "10.93.0.1:21353".to_string();
        healthy.endpoint.endpoint_id =
            sinomo_node::daemon_lan_endpoint_id(&destination, &healthy.endpoint.address);
        let healthy_endpoint_id = healthy.endpoint.endpoint_id.clone();

        let mut degraded = edge(
            &source,
            &destination,
            "endpoint_route_recovery_order_degraded",
            93,
            1,
        );
        degraded.endpoint.transport = ClusterTransportKind::LanTcp;
        degraded.endpoint.address = "10.93.0.2:21353".to_string();
        degraded.endpoint.endpoint_id =
            sinomo_node::daemon_lan_endpoint_id(&destination, &degraded.endpoint.address);
        degraded.state = DirectConnectivityState::Degraded;
        let degraded_endpoint_id = degraded.endpoint.endpoint_id.clone();

        let iroh =
            directory_iroh_endpoint(&destination, "directory_module_recovery_order_key", false);
        let iroh_endpoint_id = iroh.endpoint_id.clone();
        let mut local_only_iroh = directory_iroh_endpoint(
            &destination,
            "directory_module_recovery_order_local_only",
            false,
        );
        local_only_iroh.privacy = ClusterEndpointPrivacy::LocalOnly;
        let directory = directory_fixture_with_endpoints(
            93,
            [
                (source.clone(), Vec::new()),
                (
                    destination.clone(),
                    vec![
                        degraded.endpoint.clone(),
                        local_only_iroh,
                        iroh,
                        healthy.endpoint.clone(),
                    ],
                ),
            ],
        );
        let connectivity = ConnectivitySnapshot::try_new(
            ConnectivityGeneration(93),
            93,
            OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1),
            [degraded, healthy],
        )
        .expect("connectivity");

        let paths = plan_routes_with_destination_process_affinity(
            &directory,
            &connectivity,
            &source,
            &destination,
            RouteConstraints::default(),
            DestinationProcessAffinity::MainDaemon,
        );
        let endpoint_ids = paths
            .iter()
            .map(|path| path.hops[0].endpoint.endpoint_id.clone())
            .collect::<Vec<_>>();

        assert_eq!(
            endpoint_ids,
            vec![healthy_endpoint_id, iroh_endpoint_id, degraded_endpoint_id],
            "healthy observed built-in paths must stay first, while current unobserved module recovery must precede degraded evidence"
        );
        assert!(paths.iter().all(|path| path.hops.len() == 1));
        assert!(!paths[1].hops[0].capabilities.relay_forwarding);
    }

    #[test]
    fn planner_bounds_and_deterministically_orders_module_then_unreachable_recovery() {
        install_test_directory_iroh_network_module();
        let source = node("node_route_recovery_bound_source");
        let destination = node("node_route_recovery_bound_destination");
        let module_endpoints = vec![
            directory_iroh_endpoint(&destination, "directory_module_recovery_bound_z", false),
            directory_iroh_endpoint(&destination, "directory_module_recovery_bound_a", false),
        ];
        let mut expected_module_endpoint_ids = module_endpoints
            .iter()
            .map(|endpoint| endpoint.endpoint_id.clone())
            .collect::<Vec<_>>();
        expected_module_endpoint_ids.sort();

        let unreachable = (0..MAX_ROUTE_CANDIDATES + 4)
            .map(|index| {
                let mut observation = edge(
                    &source,
                    &destination,
                    &format!("endpoint_route_recovery_bound_{index:02}"),
                    94,
                    u64::try_from(index + 1).expect("latency"),
                );
                observation.endpoint.transport = ClusterTransportKind::LanTcp;
                observation.endpoint.address = format!("10.94.0.{}:21353", index + 1);
                observation.endpoint.endpoint_id = sinomo_node::daemon_lan_endpoint_id(
                    &destination,
                    &observation.endpoint.address,
                );
                observation.state = DirectConnectivityState::Unreachable;
                observation
            })
            .collect::<Vec<_>>();
        let mut endpoints = unreachable
            .iter()
            .map(|observation| observation.endpoint.clone())
            .collect::<Vec<_>>();
        endpoints.extend(module_endpoints);
        endpoints.reverse();
        let directory = directory_fixture_with_endpoints(
            94,
            [
                (source.clone(), Vec::new()),
                (destination.clone(), endpoints),
            ],
        );
        let connectivity = ConnectivitySnapshot::try_new(
            ConnectivityGeneration(94),
            94,
            OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1),
            unreachable,
        )
        .expect("connectivity");

        let paths = plan_routes_with_destination_process_affinity(
            &directory,
            &connectivity,
            &source,
            &destination,
            RouteConstraints::default(),
            DestinationProcessAffinity::MainDaemon,
        );

        assert_eq!(paths.len(), MAX_ROUTE_CANDIDATES);
        assert_eq!(
            paths[..expected_module_endpoint_ids.len()]
                .iter()
                .map(|path| path.hops[0].endpoint.endpoint_id.clone())
                .collect::<Vec<_>>(),
            expected_module_endpoint_ids,
            "unobserved module recovery order must be stable even when directory input order is reversed"
        );
        assert!(paths[..2].iter().all(|path| {
            path.hops.len() == 1
                && path.hops[0].endpoint.transport == ClusterTransportKind::Iroh
                && !path.hops[0].capabilities.relay_forwarding
        }));
        assert!(paths[2..].iter().all(|path| {
            path.hops.len() == 1
                && path.hops[0].connectivity_state == DirectConnectivityState::Unreachable
        }));
    }

    #[test]
    fn planner_retries_unreachable_direct_only_when_no_live_path_exists() {
        let source = node("node_route_unreachable_recovery_source");
        let relay = node("node_route_unreachable_recovery_relay");
        let destination = node("node_route_unreachable_recovery_destination");
        let mut unreachable_direct = edge(
            &source,
            &destination,
            "endpoint_route_unreachable_recovery_direct",
            83,
            1,
        );
        unreachable_direct.state = DirectConnectivityState::Unreachable;
        let (directory, connectivity) = fixtures(
            83,
            vec![
                unreachable_direct,
                edge(
                    &source,
                    &relay,
                    "endpoint_route_unreachable_recovery_source_relay",
                    83,
                    10,
                ),
                edge(
                    &relay,
                    &destination,
                    "endpoint_route_unreachable_recovery_relay_destination",
                    83,
                    10,
                ),
            ],
        );

        let paths = plan_routes(
            &directory,
            &connectivity,
            &source,
            &destination,
            RouteConstraints::default(),
        );

        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].nodes, vec![source, relay, destination]);
        assert!(
            paths[0]
                .hops
                .iter()
                .all(|hop| hop.connectivity_state == DirectConnectivityState::Reachable)
        );
    }

    #[test]
    fn planner_never_uses_an_unreachable_edge_as_a_relay() {
        let source = node("node_route_unreachable_relay_source");
        let relay = node("node_route_unreachable_relay_middle");
        let destination = node("node_route_unreachable_relay_destination");
        let mut unreachable_first_hop = edge(
            &source,
            &relay,
            "endpoint_route_unreachable_relay_first",
            84,
            1,
        );
        unreachable_first_hop.state = DirectConnectivityState::Unreachable;
        let (directory, connectivity) = fixtures(
            84,
            vec![
                unreachable_first_hop,
                edge(
                    &relay,
                    &destination,
                    "endpoint_route_unreachable_relay_second",
                    84,
                    1,
                ),
            ],
        );

        assert!(
            plan_routes(
                &directory,
                &connectivity,
                &source,
                &destination,
                RouteConstraints::default(),
            )
            .is_empty(),
            "failed evidence may be retried only as the exact terminal hop"
        );
    }

    #[test]
    fn planner_does_not_retry_unreachable_hard_negative_evidence() {
        let source = node("node_route_unreachable_hard_negative_source");
        let destination = node("node_route_unreachable_hard_negative_destination");
        let mut observation = edge(
            &source,
            &destination,
            "endpoint_route_unreachable_hard_negative",
            86,
            1,
        );
        observation.state = DirectConnectivityState::Unreachable;
        observation.failure_reason = Some("IdentityMismatch".to_string());
        let (directory, connectivity) = fixtures(86, vec![observation]);

        assert!(
            plan_routes(
                &directory,
                &connectivity,
                &source,
                &destination,
                RouteConstraints::default(),
            )
            .is_empty(),
            "hard authentication evidence must remain fail-closed until maintenance reprobes it"
        );
    }

    #[test]
    fn planner_never_uses_a_directory_module_endpoint_for_the_wrong_node() {
        install_test_directory_iroh_network_module();
        let source = node("node_route_directory_module_wrong_source");
        let destination = node("node_route_directory_module_wrong_destination");
        let other = node("node_route_directory_module_wrong_other");
        let endpoint = directory_iroh_endpoint(&other, "directory_module_wrong_node_key", false);
        let directory = directory_fixture_with_endpoints(
            87,
            [
                (source.clone(), Vec::new()),
                (destination.clone(), Vec::new()),
                (other, vec![endpoint]),
            ],
        );
        let connectivity = ConnectivitySnapshot::empty(
            ConnectivityGeneration(87),
            87,
            OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1),
        );

        assert!(
            plan_routes_with_destination_process_affinity(
                &directory,
                &connectivity,
                &source,
                &destination,
                RouteConstraints::default(),
                DestinationProcessAffinity::MainDaemon,
            )
            .is_empty(),
            "a directory module endpoint for another node must never become a target path"
        );
    }

    #[test]
    fn planner_rejects_a_directory_module_shadow_endpoint_for_main_affinity() {
        install_test_directory_iroh_network_module();
        let source = node("node_route_directory_module_shadow_source");
        let destination = node("node_route_directory_module_shadow_destination");
        let endpoint = directory_iroh_endpoint(&destination, "directory_module_shadow_key", true);
        let directory = directory_fixture_with_endpoints(
            88,
            [
                (source.clone(), Vec::new()),
                (destination.clone(), vec![endpoint]),
            ],
        );
        let connectivity = ConnectivitySnapshot::empty(
            ConnectivityGeneration(88),
            88,
            OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1),
        );

        assert!(
            plan_routes_with_destination_process_affinity(
                &directory,
                &connectivity,
                &source,
                &destination,
                RouteConstraints::default(),
                DestinationProcessAffinity::MainDaemon,
            )
            .is_empty(),
            "main-daemon affinity requires positive main-process endpoint proof"
        );
    }

    #[test]
    fn planner_rejects_a_directory_endpoint_without_a_local_module_dialer() {
        install_test_directory_iroh_network_module();
        let source = node("node_route_directory_module_unsupported_source");
        let destination = node("node_route_directory_module_unsupported_destination");
        let endpoint = ClusterEndpointRecord {
            endpoint_id: Id::from_legacy_named("endpoint_directory_module_unsupported")
                .expect("endpoint"),
            node_id: destination.clone(),
            transport: ClusterTransportKind::WebRtc,
            address: "webrtc://directory-module-unsupported".to_string(),
            source: ClusterEndpointSource::Rendezvous,
            privacy: ClusterEndpointPrivacy::Relayed,
            priority: 90,
            last_observed_latency_ms: None,
        };
        let directory = directory_fixture_with_endpoints(
            89,
            [
                (source.clone(), Vec::new()),
                (destination.clone(), vec![endpoint]),
            ],
        );
        let connectivity = ConnectivitySnapshot::empty(
            ConnectivityGeneration(89),
            89,
            OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1),
        );

        assert!(
            plan_routes_with_destination_process_affinity(
                &directory,
                &connectivity,
                &source,
                &destination,
                RouteConstraints::default(),
                DestinationProcessAffinity::Any,
            )
            .is_empty(),
            "a directory endpoint is not executable without an enabled local module dialer"
        );
    }

    #[test]
    fn planner_rejects_an_unclassified_directory_module_endpoint() {
        install_test_directory_iroh_network_module();
        let source = node("node_route_directory_module_unknown_source");
        let destination = node("node_route_directory_module_unknown_destination");
        let mut endpoint =
            directory_iroh_endpoint(&destination, "directory_module_unknown_key", false);
        endpoint.endpoint_id =
            Id::from_legacy_named("endpoint_directory_module_unknown").expect("endpoint");
        let directory = directory_fixture_with_endpoints(
            90,
            [
                (source.clone(), Vec::new()),
                (destination.clone(), vec![endpoint]),
            ],
        );
        let connectivity = ConnectivitySnapshot::empty(
            ConnectivityGeneration(90),
            90,
            OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1),
        );

        assert!(
            plan_routes_with_destination_process_affinity(
                &directory,
                &connectivity,
                &source,
                &destination,
                RouteConstraints::default(),
                DestinationProcessAffinity::Any,
            )
            .is_empty(),
            "module dialability without a positive process-role identity must fail closed"
        );
    }

    #[tokio::test]
    async fn directory_iroh_first_attempt_succeeds_with_shared_candidate_deadline() {
        install_test_directory_iroh_network_module();
        let source = node("node_route_directory_module_priority_source");
        let destination = node("node_route_directory_module_priority_destination");
        let iroh = directory_iroh_endpoint(&destination, "directory_module_priority_key", false);
        let iroh_endpoint_id = iroh.endpoint_id.clone();
        let mut unreachable = (0..6)
            .map(|index| {
                let mut observation = edge(
                    &source,
                    &destination,
                    &format!("endpoint_route_directory_module_stale_{index}"),
                    91,
                    u64::try_from(index + 1).expect("latency"),
                );
                let address = format!("10.0.0.{}:21353", index + 10);
                observation.endpoint.transport = ClusterTransportKind::LanTcp;
                observation.endpoint.address = address.clone();
                observation.endpoint.endpoint_id =
                    sinomo_node::daemon_lan_endpoint_id(&destination, &address);
                observation.state = DirectConnectivityState::Unreachable;
                observation
            })
            .collect::<Vec<_>>();
        let mut endpoints = vec![iroh.clone()];
        endpoints.extend(
            unreachable
                .iter()
                .map(|observation| observation.endpoint.clone()),
        );
        let directory = directory_fixture_with_endpoints(
            91,
            [
                (source.clone(), Vec::new()),
                (destination.clone(), endpoints),
            ],
        );
        let connectivity = ConnectivitySnapshot::try_new(
            ConnectivityGeneration(91),
            91,
            OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1),
            unreachable.drain(..),
        )
        .expect("connectivity");

        let paths = plan_routes_with_destination_process_affinity(
            &directory,
            &connectivity,
            &source,
            &destination,
            RouteConstraints::default(),
            DestinationProcessAffinity::MainDaemon,
        );

        assert_eq!(paths.len(), 7);
        assert_eq!(paths[0].hops[0].endpoint.endpoint_id, iroh.endpoint_id);
        assert!(paths[1..].iter().all(|path| {
            path.hops[0].connectivity_state == DirectConnectivityState::Unreachable
        }));

        let overall_timeout = Duration::from_millis(700);
        let first_attempt_timeout = routed_candidate_attempt_timeout(
            overall_timeout,
            u32::try_from(paths.len()).expect("candidate count"),
            paths[0].hops.len(),
        );
        assert!(
            first_attempt_timeout < overall_timeout,
            "Iroh must share the unchanged deadline with every stale fallback candidate"
        );

        let network = TestRelayNetwork::new(directory.clone(), connectivity);
        network
            .delayed_endpoints
            .lock()
            .expect("delayed endpoints lock")
            .insert(iroh_endpoint_id.clone(), Duration::from_millis(25));
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
            .connect_with_destination_process_affinity(
                destination.clone(),
                ConnectOptions {
                    timeout: overall_timeout,
                    ..ConnectOptions::default()
                },
                DestinationProcessAffinity::MainDaemon,
            )
            .await
            .expect("directory Iroh should complete within its shared candidate allocation");
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
            network
                .dialed_edges
                .lock()
                .expect("dialed edges lock")
                .as_slice(),
            &[(source, destination, iroh_endpoint_id)],
            "a prompt Iroh dial must succeed before stale LAN fallbacks are attempted"
        );
    }

    #[test]
    fn planner_does_not_bypass_hard_negative_module_endpoint_evidence() {
        install_test_directory_iroh_network_module();
        let source = node("node_route_directory_module_hard_negative_source");
        let destination = node("node_route_directory_module_hard_negative_destination");
        let endpoint =
            directory_iroh_endpoint(&destination, "directory_module_hard_negative_key", false);
        let mut observation = edge(
            &source,
            &destination,
            "endpoint_route_directory_module_hard_negative_placeholder",
            92,
            1,
        );
        observation.endpoint = endpoint.clone();
        observation.state = DirectConnectivityState::Unreachable;
        observation.failure_reason = Some("IdentityMismatch".to_string());
        let directory = directory_fixture_with_endpoints(
            92,
            [
                (source.clone(), Vec::new()),
                (destination.clone(), vec![endpoint]),
            ],
        );
        let connectivity = ConnectivitySnapshot::try_new(
            ConnectivityGeneration(92),
            92,
            OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1),
            [observation],
        )
        .expect("connectivity");

        assert!(
            plan_routes_with_destination_process_affinity(
                &directory,
                &connectivity,
                &source,
                &destination,
                RouteConstraints::default(),
                DestinationProcessAffinity::MainDaemon,
            )
            .is_empty(),
            "directory recovery must not challenge a current authentication failure"
        );
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
                RouteConstraints {
                    require_confidentiality: true,
                    ..RouteConstraints::default()
                },
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
            let write = if buf.starts_with(&ROUTED_SESSION_DATA_WIRE_PREFIX)
                && buf.len() > 16
                && self.tamper_next_data_frame.swap(false, Ordering::SeqCst)
            {
                altered.extend_from_slice(buf);
                altered[16] ^= 1;
                &altered[..]
            } else if self.encrypted_payload_pending
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
                    } else if written == 16
                        && buf.len() == 16
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
        reverse_affinities: Arc<Mutex<Vec<Option<DestinationProcessAffinity>>>>,
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
                reverse_affinities: Arc::new(Mutex::new(Vec::new())),
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
                            .reverse_affinities
                            .lock()
                            .unwrap()
                            .push(stream.reverse_process_affinity);
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
        let descriptor = RoutedSessionDescriptor {
            protocol: ROUTED_SESSION_PROTOCOL.to_string(),
            source_node_id: source.clone(),
            destination_node_id: requested_destination.clone(),
            session_id: Id::generate(),
            path: vec![source.clone(), requested_destination.clone()],
            endpoint_ids: vec![endpoint.endpoint_id],
            protection: RoutedSessionProtection::Plaintext,
            source_ephemeral_public_key: None,
            reverse_process_affinity: None,
            hop_limit: 1,
            deadline_unix_millis: routed_session_deadline_unix_millis(Duration::from_secs(10)),
        };
        let open = RoutedSessionOpen { descriptor };
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
                RoutedSessionProtection::Plaintext,
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
    async fn exact_main_daemon_call_uses_a_directory_iroh_endpoint_without_connectivity_evidence() {
        install_test_directory_iroh_network_module();
        let source = node("node_relay_directory_iroh_source");
        let destination = node("node_relay_directory_iroh_destination");
        let endpoint =
            directory_iroh_endpoint(&destination, "directory_iroh_exact_call_key", false);
        let endpoint_id = endpoint.endpoint_id.clone();
        let directory = directory_fixture_with_endpoints(
            90,
            [
                (source.clone(), Vec::new()),
                (destination.clone(), vec![endpoint]),
            ],
        );
        let connectivity =
            ConnectivitySnapshot::empty(ConnectivityGeneration(90), 90, OffsetDateTime::now_utc());
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
            .connect_with_destination_process_affinity(
                destination.clone(),
                ConnectOptions::default(),
                DestinationProcessAffinity::MainDaemon,
            )
            .await
            .expect("directory Iroh endpoint should establish the exact main-daemon call");
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
        assert_eq!(stream.route_trace.endpoint_ids, vec![endpoint_id.clone()]);
        stream.shutdown().await.expect("close routed session");
        network.wait_idle().await;
        assert_eq!(
            network
                .dialed_edges
                .lock()
                .expect("dialed edges lock")
                .as_slice(),
            &[(source, destination, endpoint_id)]
        );
    }

    #[tokio::test]
    async fn multi_hop_connector_authenticates_an_unreachable_direct_recovery_attempt() {
        let source = node("node_relay_unreachable_recovery_source");
        let destination = node("node_relay_unreachable_recovery_destination");
        let mut observation = edge(
            &source,
            &destination,
            "endpoint_relay_unreachable_recovery",
            85,
            1,
        );
        observation.state = DirectConnectivityState::Unreachable;
        let endpoint_id = observation.endpoint.endpoint_id.clone();
        let (directory, connectivity) = fixtures(85, vec![observation]);
        let network = TestRelayNetwork::new(directory.clone(), connectivity);
        let connector = MultiHopRoutedConnector::new(
            source.clone(),
            Arc::new(FixedDirectory(directory)),
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
            .expect("authenticated direct retry should disprove stale unreachable evidence");
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
        assert_eq!(stream.route_trace.endpoint_ids, vec![endpoint_id.clone()]);
        stream.shutdown().await.expect("close routed session");
        network.wait_idle().await;
        assert_eq!(
            network
                .dialed_edges
                .lock()
                .expect("dialed edges lock")
                .as_slice(),
            &[(
                node("node_relay_unreachable_recovery_source"),
                node("node_relay_unreachable_recovery_destination"),
                endpoint_id,
            )]
        );
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
    async fn multi_hop_main_affinity_does_not_dial_a_shadow_only_terminal() {
        let source = node("node_relay_affinity_shadow_only_source");
        let destination = node("node_relay_affinity_shadow_only_destination");
        let folded_shadow = folded_shadow_edge(
            &source,
            &destination,
            "endpoint_relay_affinity_folded_shadow",
            83,
            1,
            "198.51.100.83:22154",
        );
        let (directory, connectivity) = fixtures(83, vec![folded_shadow]);
        let network = TestRelayNetwork::new(directory.clone(), connectivity);
        let connector = MultiHopRoutedConnector::from_snapshot(
            source.clone(),
            directory,
            network.connectivity.clone(),
            network.connectivity.clone(),
            Arc::new(network.provider(source.clone())),
            Arc::new(TestRoutedIdentity {
                local_node_id: source,
            }),
        )
        .expect("multi-hop connector");

        match connector
            .connect_with_destination_process_affinity(
                destination,
                ConnectOptions::default(),
                DestinationProcessAffinity::MainDaemon,
            )
            .await
        {
            Err(NetworkError::RetryExhausted {
                attempts,
                delivery_state,
                ..
            }) => {
                assert_eq!(attempts, 0);
                assert_eq!(delivery_state, NetworkDeliveryState::DefinitelyNotSent);
            }
            Err(error) => panic!("unexpected main-affinity failure: {error}"),
            Ok(_) => panic!("shadow-only reachability must not satisfy main-daemon affinity"),
        }
        assert!(
            network
                .dialed_edges
                .lock()
                .expect("dialed edges lock")
                .is_empty(),
            "a shadow-only terminal must be rejected before dialing"
        );
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
    async fn every_planned_line_ring_star_and_mesh_path_is_realizable() {
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
        }
    }

    #[tokio::test]
    async fn reverse_dispatcher_role_survives_relayed_plaintext_and_aead_handshakes() {
        for (label, role) in [
            ("caller", None),
            ("shadow", Some(DestinationProcessAffinity::Any)),
            ("main", Some(DestinationProcessAffinity::MainDaemon)),
        ] {
            for protection in [
                RoutedSessionProtection::Plaintext,
                RoutedSessionProtection::Aead,
            ] {
                let name = format!("reverse-role-{label}-{protection:?}");
                let (nodes, directory, connectivity) =
                    topology_fixture(&name, 77, 3, &[(0, 1), (1, 2)]);
                let _incoming = role.map(|affinity| {
                    crate::register_routed_stream_incoming_with_process_affinity(
                        nodes[0].clone(),
                        affinity,
                    )
                    .unwrap()
                });
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
                .unwrap();
                let mut stream = connector
                    .connect(
                        nodes[2].clone(),
                        ConnectOptions {
                            protection,
                            ..ConnectOptions::default()
                        },
                    )
                    .await
                    .unwrap();
                stream.write_all(TEST_ROUTED_PAYLOAD).await.unwrap();
                let mut response = [0; TEST_ROUTED_PAYLOAD.len()];
                stream.read_exact(&mut response).await.unwrap();
                assert_eq!(response, *TEST_ROUTED_PAYLOAD);
                stream.shutdown().await.unwrap();
                network.wait_idle().await;
                assert_eq!(*network.reverse_affinities.lock().unwrap(), vec![role]);
                assert!(network.errors.lock().unwrap().is_empty());
            }
        }
    }

    #[tokio::test]
    async fn advertised_reverse_role_connects_to_a_legacy_aead_receiver() {
        let (nodes, directory, connectivity) =
            topology_fixture("reverse-role-legacy", 78, 2, &[(0, 1)]);
        let _incoming = crate::register_routed_stream_incoming_with_process_affinity(
            nodes[0].clone(),
            DestinationProcessAffinity::MainDaemon,
        )
        .unwrap();
        let path = plan_routes(
            &directory,
            &connectivity,
            &nodes[0],
            &nodes[1],
            RouteConstraints::default(),
        )
        .remove(0);
        let (client_io, server_io) = tokio::io::duplex(8192);
        let direct =
            |io: tokio::io::DuplexStream, local: NodeId, peer: NodeId| AuthenticatedDirectStream {
                io: Box::new(io),
                local_node_id: local,
                peer_node_id: peer,
                link_id: Id::generate(),
                stream_id: Id::generate(),
                link_channel_binding: [7; 32],
                channel_binding: [0; 32],
                capabilities: DirectStreamCapabilities::multiplexed_full_duplex(16),
            };
        let client = direct(client_io, nodes[0].clone(), nodes[1].clone());
        let mut server = direct(server_io, nodes[1].clone(), nodes[0].clone());
        let receive = tokio::spawn(async move {
            let bytes = read_routed_session_frame(&mut *server.io, ROUTED_SESSION_OPEN_FRAME)
                .await
                .unwrap();
            let mut open: RoutedSessionOpen = routed_session_decode(&bytes).unwrap();
            assert_eq!(
                open.descriptor.reverse_process_affinity,
                Some(DestinationProcessAffinity::MainDaemon)
            );
            // Emulate the old descriptor codec ignoring the unknown field,
            // then use exactly its legacy transcript and acceptance semantics.
            open.descriptor.reverse_process_affinity = None;
            let transcript = routed_session_encode(&open.descriptor).unwrap();
            let mut peer = accept_routed_session_destination(server, open, transcript)
                .await
                .unwrap();
            assert_eq!(peer.reverse_process_affinity, None);
            let mut bytes = [0; 4];
            peer.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes, b"ping");
            peer.write_all(b"pong").await.unwrap();
            peer.shutdown().await.unwrap();
        });
        let mut peer = initiate_routed_session(
            client,
            &directory,
            &path,
            &TestRoutedIdentity {
                local_node_id: nodes[0].clone(),
            },
            routed_session_deadline_unix_millis(Duration::from_secs(2)),
            RoutedSessionProtection::Aead,
        )
        .await
        .unwrap();
        peer.write_all(b"ping").await.unwrap();
        let mut bytes = [0; 4];
        peer.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"pong");
        receive.await.unwrap();
    }

    #[test]
    fn reverse_role_negotiation_preserves_legacy_aead_transcripts() {
        let mut descriptor = RoutedSessionDescriptor {
            protocol: ROUTED_SESSION_PROTOCOL.into(),
            source_node_id: Id::generate(),
            destination_node_id: Id::generate(),
            session_id: Id::generate(),
            path: Vec::new(),
            endpoint_ids: Vec::new(),
            protection: RoutedSessionProtection::Aead,
            source_ephemeral_public_key: Some([1; 32]),
            reverse_process_affinity: None,
            hop_limit: 1,
            deadline_unix_millis: 1,
        };
        let legacy_bytes = routed_session_encode(&descriptor).unwrap();
        assert!(!String::from_utf8_lossy(&legacy_bytes).contains("reverse_process_affinity"));
        let legacy_decoded: RoutedSessionDescriptor = routed_session_decode(&legacy_bytes).unwrap();
        assert_eq!(legacy_decoded.reverse_process_affinity, None);
        descriptor.reverse_process_affinity = Some(DestinationProcessAffinity::MainDaemon);
        let mut acceptance = RoutedSessionAcceptance {
            protocol: descriptor.protocol.clone(),
            source_node_id: descriptor.source_node_id.clone(),
            destination_node_id: descriptor.destination_node_id.clone(),
            session_id: descriptor.session_id.clone(),
            protection: descriptor.protection,
            destination_ephemeral_public_key: Some([2; 32]),
            reverse_process_affinity: None,
        };
        verify_routed_session_acceptance(&descriptor, &acceptance).unwrap();
        assert_eq!(
            negotiated_routed_session_descriptor_transcript(&descriptor, &acceptance).unwrap(),
            legacy_bytes
        );
        acceptance.reverse_process_affinity = descriptor.reverse_process_affinity;
        verify_routed_session_acceptance(&descriptor, &acceptance).unwrap();
        assert_eq!(
            negotiated_routed_session_descriptor_transcript(&descriptor, &acceptance).unwrap(),
            routed_session_encode(&descriptor).unwrap()
        );
        acceptance.reverse_process_affinity = Some(DestinationProcessAffinity::Any);
        assert!(matches!(
            verify_routed_session_acceptance(&descriptor, &acceptance),
            Err(NetworkError::AuthDenied)
        ));
    }

    #[tokio::test]
    async fn routed_session_protection_defaults_to_plaintext() {
        let (nodes, directory, connectivity) =
            topology_fixture("plaintext-default", 55, 2, &[(0, 1)]);
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
        .expect("plaintext connector");
        let mut stream = connector
            .connect(nodes[1].clone(), ConnectOptions::default())
            .await
            .expect("connect plaintext route");
        stream
            .write_all(TEST_ROUTED_PAYLOAD)
            .await
            .expect("write plaintext payload");
        let mut response = [0_u8; TEST_ROUTED_PAYLOAD.len()];
        stream
            .read_exact(&mut response)
            .await
            .expect("read plaintext echo");
        assert_eq!(response, *TEST_ROUTED_PAYLOAD);
        stream.shutdown().await.expect("close plaintext route");
        network.wait_idle().await;
        assert!(
            network
                .wire_writes
                .lock()
                .expect("wire writes lock")
                .iter()
                .any(|write| write
                    .windows(TEST_ROUTED_PAYLOAD.len())
                    .any(|window| window == TEST_ROUTED_PAYLOAD))
        );
    }

    #[tokio::test]
    async fn routed_session_aead_remains_an_explicit_opt_in() {
        let (nodes, directory, connectivity) = topology_fixture("aead-opt-in", 56, 2, &[(0, 1)]);
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
        .expect("AEAD connector");
        let mut stream = connector
            .connect(
                nodes[1].clone(),
                ConnectOptions {
                    protection: RoutedSessionProtection::Aead,
                    ..ConnectOptions::default()
                },
            )
            .await
            .expect("connect AEAD route");
        stream
            .write_all(TEST_ROUTED_PAYLOAD)
            .await
            .expect("write AEAD payload");
        let mut response = [0_u8; TEST_ROUTED_PAYLOAD.len()];
        stream
            .read_exact(&mut response)
            .await
            .expect("read AEAD echo");
        assert_eq!(response, *TEST_ROUTED_PAYLOAD);
        stream.shutdown().await.expect("close AEAD route");
        network.wait_idle().await;
        assert!(
            network
                .wire_writes
                .lock()
                .expect("wire writes lock")
                .iter()
                .all(|write| !write
                    .windows(TEST_ROUTED_PAYLOAD.len())
                    .any(|window| window == TEST_ROUTED_PAYLOAD))
        );
    }

    #[test]
    fn route_rejects_loops_expiry_and_hop_exhaustion() {
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
            protection: RoutedSessionProtection::Plaintext,
            source_ephemeral_public_key: None,
            reverse_process_affinity: None,
            hop_limit: 2,
            deadline_unix_millis: routed_session_deadline_unix_millis(Duration::from_secs(5)),
        };

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

        let mut receiver_clock_ahead = descriptor.clone();
        receiver_clock_ahead.deadline_unix_millis = routed_session_now_unix_millis()
            .saturating_sub(ROUTED_SESSION_DEADLINE_CLOCK_SKEW_MILLIS);
        assert_eq!(
            validate_routed_session_descriptor(&directory, &receiver_clock_ahead),
            Ok(()),
            "a modestly fast receiver clock must not reject a live routed-session open"
        );

        let mut expired = descriptor.clone();
        expired.deadline_unix_millis = routed_session_now_unix_millis()
            .saturating_sub(ROUTED_SESSION_DEADLINE_CLOCK_SKEW_MILLIS)
            .saturating_sub(1);
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
