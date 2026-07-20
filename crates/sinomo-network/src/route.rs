//! Multi-hop delivery of cluster object RPCs across a segmented network.
//!
//! Callers name a target node. This module builds one delivery graph from authorized
//! member endpoints and connectivity records, chooses the lowest-cost next hop, wraps the
//! request in a [`RelayEnvelope`], and retries the same algorithm after failed local
//! edges. Signing and dialing are supplied by daemon-host callbacks so route construction
//! stays here without coupling the network crate to daemon identity storage.

use crate::endpoint_selector::cluster_endpoint_can_carry_main_daemon_rpc;
use crate::file_mark::{ClusterFileMark, cluster_file_mark};
use crate::prelude::*;
use crate::session::start_object_rpc_client_session;
use crate::{
    ClusterConnectivityGraph, ClusterConnectivityRecord, ClusterConnectivityState, ClusterPaths,
    DaemonControlRequest, DaemonRpcTimeouts, NetworkCallPolicy, NetworkDeliveryState,
    NetworkDuplexStream, NetworkError, NetworkHealthPolicy, NetworkTimeoutPhase,
    OBJECT_RPC_CHANNEL_MAX_MATERIALIZED_VALUE_BYTES, ObjectRpcBodySource, ObjectRpcChannelClient,
    ObjectRpcRequest, ObjectRpcResponse, ObjectRpcSessionCall, ObjectRpcSessionResult,
};
#[cfg(all(feature = "test-support", not(test)))]
#[cfg(test)]
use sinomo_local::SinomoDataDir;
use sinomo_node::cluster_member_is_local_process;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex, OnceLock};
use std::time::{Duration, Instant};
use time::OffsetDateTime;

pub(crate) fn network_perf_trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("SINOMO_NETWORK_PERF_TRACE").as_deref() == Ok("1"))
}

pub(crate) fn network_perf_trace_elapsed_us(started: Instant) -> u64 {
    started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
}

fn network_rpc_outcome(result: &Result<ObjectRpcSessionResult, NetworkError>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(NetworkError::Timeout { .. }) => "timeout",
        Err(
            NetworkError::AuthDenied
            | NetworkError::AuthExpired
            | NetworkError::UnauthorizedNode { .. }
            | NetworkError::CertificateExpired { .. }
            | NetworkError::CertificateKeyMismatch { .. }
            | NetworkError::CertificateSignatureMissing { .. },
        ) => "auth",
        Err(
            NetworkError::ClusterOrphan
            | NetworkError::UnknownNode { .. }
            | NetworkError::UnknownRoute { .. },
        ) => "no_route",
        Err(
            NetworkError::DeliveryLedgerFull { .. }
            | NetworkError::NetworkServiceCommandQueueFull { .. }
            | NetworkError::MailboxFull { .. }
            | NetworkError::ObjectFrameMemoryUnavailable { .. }
            | NetworkError::ObjectRpcRouteSaturated { .. }
            | NetworkError::FrameTooLarge { .. }
            | NetworkError::ResourceExhausted { .. },
        ) => "saturated",
        Err(
            NetworkError::OutcomeUnknown { .. }
            | NetworkError::StalePooledConnection { .. }
            | NetworkError::RetryExhausted { .. }
            | NetworkError::DeliveryFailed { .. },
        ) => "delivery",
        Err(NetworkError::Io { .. } | NetworkError::StorageIo { .. }) => "io",
        Err(_) => "protocol",
    }
}

fn record_network_rpc_metrics(
    path: &'static str,
    started: Instant,
    request_head_bytes: usize,
    result: &Result<ObjectRpcSessionResult, NetworkError>,
) {
    let outcome = network_rpc_outcome(result);
    metrics::counter!(
        "sinomo.network.rpc.requests",
        "path" => path,
        "outcome" => outcome
    )
    .increment(1);
    metrics::histogram!(
        "sinomo.network.rpc.total.us",
        "path" => path,
        "outcome" => outcome
    )
    .record(network_perf_trace_elapsed_us(started) as f64);
    metrics::counter!("sinomo.network.rpc.request_head.bytes", "path" => path)
        .increment(request_head_bytes as u64);
    if let Ok(result) = result {
        metrics::counter!("sinomo.network.rpc.response_head.bytes", "path" => path)
            .increment(result.response.body.len() as u64);
    }
}

fn network_transport_label(transport: &sinomo_base::ClusterTransportKind) -> &'static str {
    match transport {
        sinomo_base::ClusterTransportKind::InProcess => "in_process",
        sinomo_base::ClusterTransportKind::Loopback => "loopback",
        sinomo_base::ClusterTransportKind::LanTcp => "lan_tcp",
        sinomo_base::ClusterTransportKind::LanQuic => "lan_quic",
        sinomo_base::ClusterTransportKind::Iroh => "iroh",
        sinomo_base::ClusterTransportKind::WebRtc => "web_rtc",
        sinomo_base::ClusterTransportKind::TurnRelay => "turn_relay",
        sinomo_base::ClusterTransportKind::HttpsRelay => "https_relay",
    }
}

fn record_network_endpoint_open(
    path: &'static str,
    transport: &sinomo_base::ClusterTransportKind,
    started: Instant,
    succeeded: bool,
) {
    let transport = network_transport_label(transport);
    let outcome = if succeeded { "ok" } else { "error" };
    metrics::counter!(
        "sinomo.network.endpoint.opens",
        "path" => path,
        "transport" => transport,
        "outcome" => outcome
    )
    .increment(1);
    metrics::histogram!(
        "sinomo.network.endpoint.open.us",
        "path" => path,
        "transport" => transport,
        "outcome" => outcome
    )
    .record(network_perf_trace_elapsed_us(started) as f64);
}

fn record_network_route_plan(
    started: Instant,
    cache: &'static str,
    result: &Result<Option<ClusterDeliveryRoutePlan>, NetworkError>,
) {
    let outcome = match result {
        Ok(Some(_)) => "found",
        Ok(None) => "missing",
        Err(_) => "error",
    };
    metrics::counter!(
        "sinomo.network.route.plans",
        "cache" => cache,
        "outcome" => outcome
    )
    .increment(1);
    metrics::histogram!(
        "sinomo.network.route.plan.us",
        "cache" => cache,
        "outcome" => outcome
    )
    .record(network_perf_trace_elapsed_us(started) as f64);
}

fn object_rpc_call_is_channel_compatible(call: &ObjectRpcSessionCall) -> bool {
    if call.body.has_value_intent() {
        return call.request.body.len() <= OBJECT_RPC_CHANNEL_MAX_MATERIALIZED_VALUE_BYTES;
    }
    if call.body.is_empty() {
        return true;
    }
    // The pooled channel remains the bounded latency path. Bulk or unknown-size
    // attachments use the canonical session, whose independently negotiated
    // chunks and BDP-sized receive window can fill a high-bandwidth path.
    call.body.total_bytes().is_some_and(|bytes| {
        bytes <= u64::try_from(OBJECT_RPC_CHANNEL_MAX_MATERIALIZED_VALUE_BYTES).unwrap_or(u64::MAX)
    })
}

fn record_route_timeline_stage(
    trace: Option<crate::NetworkTimelineTraceContext>,
    stage: u16,
    started_ns: u64,
    success: bool,
    aux: u32,
) {
    let Some(trace) = trace else {
        return;
    };
    crate::record_network_timeline_event_now(crate::NetworkTimelineEvent::new(
        trace,
        stage,
        crate::network_timeline_flag::REQUEST
            | if success {
                crate::network_timeline_flag::SUCCESS
            } else {
                0
            },
        0,
        0,
        0,
        0,
        aux,
        crate::network_timeline_monotonic_ns().saturating_sub(started_ns),
    ));
}

fn record_route_channel_lookup_timeline(
    trace: Option<crate::NetworkTimelineTraceContext>,
    started_ns: u64,
    channel: Option<&crate::ObjectRpcChannelClient>,
    outcome: u32,
    context: u32,
) {
    let Some(trace) = trace else {
        return;
    };
    crate::record_network_timeline_event_now(crate::NetworkTimelineEvent::new(
        trace,
        crate::network_timeline_stage::CHANNEL_LOOKUP,
        crate::network_timeline_flag::REQUEST
            | if outcome == crate::network_timeline_channel_lookup_outcome::AVAILABLE {
                crate::network_timeline_flag::SUCCESS
            } else {
                0
            },
        channel.map_or(0, crate::ObjectRpcChannelClient::connection_id),
        0,
        0,
        context,
        outcome,
        crate::network_timeline_monotonic_ns().saturating_sub(started_ns),
    ));
}

/// RPC category for routing/relay traffic.
pub const CLUSTER_ROUTING_RPC_CATEGORY: &str = "cluster.routing";
/// Route an RPC is sent on when it is wrapped in a [`RelayEnvelope`] for forwarding.
pub const CLUSTER_RELAY_ROUTE: &str = "/cluster/relay";
pub const CLUSTER_ROUTING_RPC_ROUTE_METADATA: &[(&str, &str)] =
    &[(CLUSTER_RELAY_ROUTE, CLUSTER_ROUTING_RPC_CATEGORY)];
/// Origin node retained by a routed request so an authenticated final-hop
/// channel can reuse cluster membership authority without re-verifying the
/// same per-call signature. Canonical stream fallbacks still verify it.
pub const CLUSTER_RELAY_ORIGIN_NODE_ID_METADATA_KEY: &str = "sinomo.cluster-relay.origin-node-id";
const CLUSTER_RELAY_FRESH_SIGNATURE_RETRY_METADATA_KEY: &str =
    "sinomo.cluster-relay.retry-from-origin";
const CLUSTER_RELAY_FRESH_SIGNATURE_RETRY_METADATA_VALUE: &str = "fresh-signature";
const CLUSTER_RELAY_OUTCOME_UNKNOWN_METADATA_KEY: &str = "sinomo.cluster-relay.outcome-unknown";
const CLUSTER_RELAY_OUTCOME_UNKNOWN_METADATA_VALUE: &str = "terminal";
/// Default hop budget for a relayed RPC (roughly the cluster diameter). Each forward
/// decrements it; a relay is dropped when it reaches zero.
const CLUSTER_RELAY_DEFAULT_TTL: u8 = 8;
// Origin-side explicit operation deadlines are preserved across relay hops,
// while calls without an explicit deadline have no total wall-clock limit.
// Connect and no-progress stalls remain independently bounded by the selected
// health policy (five seconds by default).
// Unverified endpoints are capped below the whole-route deadline so a stale
// candidate cannot consume the fallback budget.
const CLUSTER_DELIVERY_DEGRADED_LINK_COST: u64 = 50_000;
const CLUSTER_DELIVERY_UNOBSERVED_TARGET_COST: u64 = CLUSTER_DELIVERY_DEGRADED_LINK_COST / 2;
const CLUSTER_DELIVERY_UNOBSERVED_NEIGHBOR_COST: u64 = 75_000;
const CLUSTER_DELIVERY_UNREACHABLE_LINK_COST: u64 = 500_000;
// Synthetic TCP tunnels are fallback route evidence, not advertised sockets.
// Prefer any observed healthy direct endpoint first; a stale tunnel should not
// consume the fast app-facing budget when a direct public/LAN path is healthy.
const CLUSTER_DELIVERY_TCP_TUNNEL_PATH_COST: u64 = 12_000;
const MAX_CLUSTER_DELIVERY_ROUTING_TABLE_CACHE_ENTRIES: usize = 128;
const CLUSTER_DELIVERY_ROUTING_TABLE_VALIDATION_LEASE: Duration = Duration::from_millis(250);

static CLUSTER_DELIVERY_ROUTING_TABLE_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static CLUSTER_DELIVERY_ROUTING_TABLES: LazyLock<
    Mutex<HashMap<PathBuf, ClusterDeliveryRoutingTableCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// A discovered route to a target node: the immediate next hop to send to, and the full
/// node path (`local` … `target`, inclusive) for diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterRoute {
    pub next_hop: NodeId,
    pub path: Vec<NodeId>,
}

fn try_clone_route_node_id(node_id: &NodeId, operation: &str) -> Result<NodeId, NetworkError> {
    crate::object::try_clone_id(node_id, operation)
}

fn try_clone_route_endpoint(
    endpoint: &ClusterEndpointRecord,
    operation: &str,
) -> Result<ClusterEndpointRecord, NetworkError> {
    Ok(ClusterEndpointRecord {
        endpoint_id: crate::object::try_clone_id(&endpoint.endpoint_id, operation)?,
        node_id: try_clone_route_node_id(&endpoint.node_id, operation)?,
        transport: endpoint.transport.clone(),
        address: crate::object::try_clone_string(&endpoint.address, operation)?,
        source: endpoint.source.clone(),
        privacy: endpoint.privacy.clone(),
        priority: endpoint.priority,
        last_observed_latency_ms: endpoint.last_observed_latency_ms,
    })
}

fn try_clone_route_node_ids(
    node_ids: &[NodeId],
    operation: &str,
) -> Result<Vec<NodeId>, NetworkError> {
    let mut cloned = Vec::new();
    crate::object::try_reserve_vec(&mut cloned, node_ids.len(), operation)?;
    for node_id in node_ids {
        cloned.push(try_clone_route_node_id(node_id, operation)?);
    }
    Ok(cloned)
}

#[derive(Clone, Debug, Default)]
struct RouteNodeSet {
    nodes: Vec<NodeId>,
}

impl RouteNodeSet {
    fn try_insert_clone(
        &mut self,
        node_id: &NodeId,
        operation: &str,
    ) -> Result<bool, NetworkError> {
        match self.position(node_id) {
            Ok(_) => Ok(false),
            Err(index) => {
                let node_id = try_clone_route_node_id(node_id, operation)?;
                crate::object::try_reserve_vec(&mut self.nodes, 1, operation)?;
                self.nodes.insert(index, node_id);
                Ok(true)
            }
        }
    }

    fn remove(&mut self, node_id: &NodeId) -> bool {
        let Ok(index) = self.position(node_id) else {
            return false;
        };
        self.nodes.remove(index);
        true
    }

    fn min_by_distance(
        &self,
        distance: &RouteDistanceIndex,
    ) -> Result<Option<NodeId>, NetworkError> {
        let Some(node_id) = self.nodes.iter().min_by(|left, right| {
            let left_distance = distance.get(left).unwrap_or(u64::MAX);
            let right_distance = distance.get(right).unwrap_or(u64::MAX);
            (left_distance, left.as_str()).cmp(&(right_distance, right.as_str()))
        }) else {
            return Ok(None);
        };
        Ok(Some(try_clone_route_node_id(
            node_id,
            "select cluster delivery route node",
        )?))
    }

    fn position(&self, node_id: &NodeId) -> Result<usize, usize> {
        self.nodes.binary_search(node_id)
    }
}

#[derive(Clone, Debug, Default)]
struct RoutePredecessorIndex {
    entries: Vec<(NodeId, NodeId)>,
}

impl RoutePredecessorIndex {
    fn get(&self, node_id: &NodeId) -> Option<&NodeId> {
        self.position(node_id)
            .ok()
            .and_then(|index| self.entries.get(index).map(|(_, previous)| previous))
    }

    fn try_insert_clone(
        &mut self,
        node_id: &NodeId,
        previous: &NodeId,
        operation: &str,
    ) -> Result<(), NetworkError> {
        match self.position(node_id) {
            Ok(index) => {
                self.entries[index].1 = try_clone_route_node_id(previous, operation)?;
            }
            Err(index) => {
                let node_id = try_clone_route_node_id(node_id, operation)?;
                let previous = try_clone_route_node_id(previous, operation)?;
                crate::object::try_reserve_vec(&mut self.entries, 1, operation)?;
                self.entries.insert(index, (node_id, previous));
            }
        }
        Ok(())
    }

    fn position(&self, node_id: &NodeId) -> Result<usize, usize> {
        self.entries
            .binary_search_by(|(stored, _)| stored.cmp(node_id))
    }
}

#[derive(Clone, Debug, Default)]
struct RouteDistanceIndex {
    entries: Vec<(NodeId, u64)>,
}

impl RouteDistanceIndex {
    fn get(&self, node_id: &NodeId) -> Option<u64> {
        self.position(node_id)
            .ok()
            .and_then(|index| self.entries.get(index).map(|(_, distance)| *distance))
    }

    fn contains_key(&self, node_id: &NodeId) -> bool {
        self.position(node_id).is_ok()
    }

    fn iter(&self) -> impl Iterator<Item = (&NodeId, u64)> {
        self.entries
            .iter()
            .map(|(node_id, distance)| (node_id, *distance))
    }

    fn try_insert_or_update(
        &mut self,
        node_id: &NodeId,
        distance: u64,
        operation: &str,
    ) -> Result<(), NetworkError> {
        match self.position(node_id) {
            Ok(index) => {
                self.entries[index].1 = distance;
            }
            Err(index) => {
                let node_id = try_clone_route_node_id(node_id, operation)?;
                crate::object::try_reserve_vec(&mut self.entries, 1, operation)?;
                self.entries.insert(index, (node_id, distance));
            }
        }
        Ok(())
    }

    fn try_insert_min(
        &mut self,
        node_id: &NodeId,
        distance: u64,
        operation: &str,
    ) -> Result<(), NetworkError> {
        match self.position(node_id) {
            Ok(index) => {
                self.entries[index].1 = self.entries[index].1.min(distance);
            }
            Err(index) => {
                let node_id = try_clone_route_node_id(node_id, operation)?;
                crate::object::try_reserve_vec(&mut self.entries, 1, operation)?;
                self.entries.insert(index, (node_id, distance));
            }
        }
        Ok(())
    }

    fn position(&self, node_id: &NodeId) -> Result<usize, usize> {
        self.entries
            .binary_search_by(|(stored, _)| stored.cmp(node_id))
    }
}

/// Breadth-first search the connectivity graph for a shortest path from `local` to
/// `target` over reachable edges (records whose state is not `Unreachable`), returning the
/// first hop along it. Returns `None` when `target == local` or no reachable path exists.
pub fn cluster_route_next_hop(
    graph: &ClusterConnectivityGraph,
    local: &NodeId,
    target: &NodeId,
) -> Option<ClusterRoute> {
    cluster_route_next_hop_fallible(graph, local, target)
        .ok()
        .flatten()
}

pub fn cluster_healthy_route_next_hop(
    graph: &ClusterConnectivityGraph,
    local: &NodeId,
    target: &NodeId,
) -> Option<ClusterRoute> {
    cluster_route_next_hop_excluding(graph, local, target, &[], ClusterRouteAdjacency::Healthy)
        .ok()
        .flatten()
}

pub(crate) fn cluster_route_next_hop_fallible(
    graph: &ClusterConnectivityGraph,
    local: &NodeId,
    target: &NodeId,
) -> Result<Option<ClusterRoute>, NetworkError> {
    cluster_route_next_hop_excluding(graph, local, target, &[], ClusterRouteAdjacency::Reachable)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClusterRouteAdjacency {
    Reachable,
    Healthy,
}

fn cluster_route_next_hop_excluding(
    graph: &ClusterConnectivityGraph,
    local: &NodeId,
    target: &NodeId,
    excluded: &[NodeId],
    adjacency: ClusterRouteAdjacency,
) -> Result<Option<ClusterRoute>, NetworkError> {
    if local == target {
        return Ok(None);
    }
    let mut seen = RouteNodeSet::default();
    for excluded_node in excluded {
        seen.try_insert_clone(excluded_node, "cluster route excluded nodes")?;
    }
    seen.try_insert_clone(local, "cluster route seen nodes")?;
    let mut predecessor = RoutePredecessorIndex::default();
    let mut queue = VecDeque::new();
    queue
        .try_reserve(1)
        .map_err(|_| crate::object::network_allocation_error("cluster route queue", 1))?;
    queue.push_back(try_clone_route_node_id(local, "cluster route queue")?);
    while let Some(current) = queue.pop_front() {
        match adjacency {
            ClusterRouteAdjacency::Reachable => {
                for next in graph.reachable_target_refs(&current) {
                    if let Some(route) = cluster_route_visit_next(
                        local,
                        target,
                        &current,
                        next,
                        &mut seen,
                        &mut predecessor,
                        &mut queue,
                    )? {
                        return Ok(Some(route));
                    }
                }
            }
            ClusterRouteAdjacency::Healthy => {
                for next in graph.healthy_target_refs(&current) {
                    if let Some(route) = cluster_route_visit_next(
                        local,
                        target,
                        &current,
                        next,
                        &mut seen,
                        &mut predecessor,
                        &mut queue,
                    )? {
                        return Ok(Some(route));
                    }
                }
            }
        }
    }
    Ok(None)
}

fn cluster_route_visit_next(
    local: &NodeId,
    target: &NodeId,
    current: &NodeId,
    next: &NodeId,
    seen: &mut RouteNodeSet,
    predecessor: &mut RoutePredecessorIndex,
    queue: &mut VecDeque<NodeId>,
) -> Result<Option<ClusterRoute>, NetworkError> {
    if !seen.try_insert_clone(next, "cluster route seen nodes")? {
        return Ok(None);
    }
    predecessor.try_insert_clone(next, current, "cluster route predecessors")?;
    if next == target {
        return route_from_predecessors(local, target, predecessor);
    }
    queue
        .try_reserve(1)
        .map_err(|_| crate::object::network_allocation_error("cluster route queue", 1))?;
    queue.push_back(try_clone_route_node_id(next, "cluster route queue")?);
    Ok(None)
}

fn route_from_predecessors(
    local: &NodeId,
    target: &NodeId,
    predecessor: &RoutePredecessorIndex,
) -> Result<Option<ClusterRoute>, NetworkError> {
    let mut reversed = Vec::new();
    crate::object::try_reserve_vec(&mut reversed, 1, "cluster route path")?;
    reversed.push(try_clone_route_node_id(target, "cluster route path")?);
    let mut current = target;
    while current != local {
        let Some(previous) = predecessor.get(current) else {
            return Ok(None);
        };
        crate::object::try_reserve_vec(&mut reversed, 1, "cluster route path")?;
        reversed.push(try_clone_route_node_id(previous, "cluster route path")?);
        current = previous;
    }
    reversed.reverse();
    let Some(next_hop) = reversed.get(1) else {
        return Ok(None);
    };
    let next_hop = try_clone_route_node_id(next_hop, "cluster route next hop")?;
    Ok(Some(ClusterRoute {
        next_hop,
        path: reversed,
    }))
}

/// An inner cluster RPC wrapped for hop-by-hop delivery toward `target_node_id`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RelayEnvelope {
    pub(crate) cluster_id: ClusterId,
    pub target_node_id: NodeId,
    pub(crate) ttl: u8,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "crate::fallible_serde::deserialize_vec"
    )]
    pub visited: Vec<NodeId>,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "crate::fallible_serde::deserialize_vec"
    )]
    pub planned_path: Vec<NodeId>,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "crate::fallible_serde::deserialize_vec"
    )]
    pub target_endpoints: Vec<ClusterEndpointRecord>,
    /// Maximum per-hop transport timeout, in milliseconds, inherited from the
    /// originating caller. Intermediate relays must not expand this budget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) max_hop_timeout_ms: Option<u64>,
    /// The original signed request to deliver at the target, re-injected through its
    /// normal auth + dispatch path (its own signature/invite-proof is carried within).
    pub(crate) inner: DaemonControlRequest,
}

impl RelayEnvelope {
    /// A fresh envelope with the default TTL and an empty visited set.
    pub fn new(cluster_id: ClusterId, target_node_id: NodeId, inner: DaemonControlRequest) -> Self {
        Self {
            cluster_id,
            target_node_id,
            ttl: CLUSTER_RELAY_DEFAULT_TTL,
            visited: Vec::new(),
            planned_path: Vec::new(),
            target_endpoints: Vec::new(),
            max_hop_timeout_ms: None,
            inner,
        }
    }

    /// Encode this envelope as an [`ObjectRpcRequest`] on [`CLUSTER_RELAY_ROUTE`] for
    /// transport to the next hop.
    pub fn into_request(&self) -> Result<ObjectRpcRequest, NetworkError> {
        let mut request =
            ObjectRpcRequest::encode_body(CLUSTER_RELAY_ROUTE, CLUSTER_ROUTING_RPC_CATEGORY, self)?;
        propagate_relay_response_metadata(&self.inner, &mut request);
        Ok(request)
    }

    /// Decode an envelope from a relay-route [`ObjectRpcRequest`].
    pub fn from_request(request: &ObjectRpcRequest) -> Result<Self, NetworkError> {
        request.decode_body()
    }

    pub(crate) fn inner(&self) -> &DaemonControlRequest {
        &self.inner
    }

    pub(crate) fn into_inner(self) -> DaemonControlRequest {
        self.inner
    }
}

pub fn relay_request_inner_route(request: &ObjectRpcRequest) -> Result<String, NetworkError> {
    RelayEnvelope::from_request(request).map(|envelope| envelope.inner.route)
}

#[derive(Clone, Debug)]
struct ClusterDeliveryRouteEdge {
    target: NodeId,
    endpoint: Option<ClusterEndpointRecord>,
    cost: u64,
}

#[derive(Clone, Debug, Default)]
pub struct ClusterDeliveryRouteGraph {
    edges_by_source: Vec<(NodeId, Vec<ClusterDeliveryRouteEdge>)>,
}

impl ClusterDeliveryRouteGraph {
    pub fn push_edge_clone(
        &mut self,
        source: &NodeId,
        target: &NodeId,
        endpoint: Option<&ClusterEndpointRecord>,
        cost: u64,
    ) -> Result<(), NetworkError> {
        let edge = ClusterDeliveryRouteEdge {
            target: try_clone_route_node_id(target, "cluster delivery route edge")?,
            endpoint: endpoint
                .map(|endpoint| try_clone_route_endpoint(endpoint, "cluster delivery route edge"))
                .transpose()?,
            cost,
        };
        match self.position(source) {
            Ok(index) => {
                crate::object::try_reserve_vec(
                    &mut self.edges_by_source[index].1,
                    1,
                    "cluster delivery route graph edges",
                )?;
                self.edges_by_source[index].1.push(edge);
            }
            Err(index) => {
                let source = try_clone_route_node_id(source, "cluster delivery route source")?;
                let mut edges = Vec::new();
                crate::object::try_reserve_vec(
                    &mut edges,
                    1,
                    "cluster delivery route graph edges",
                )?;
                edges.push(edge);
                crate::object::try_reserve_vec(
                    &mut self.edges_by_source,
                    1,
                    "cluster delivery route graph sources",
                )?;
                self.edges_by_source.insert(index, (source, edges));
            }
        }
        Ok(())
    }

    fn edges(&self, source: &NodeId) -> &[ClusterDeliveryRouteEdge] {
        self.position(source)
            .ok()
            .and_then(|index| {
                self.edges_by_source
                    .get(index)
                    .map(|(_, edges)| edges.as_slice())
            })
            .unwrap_or(&[])
    }

    fn endpoints_for_next_hop(
        &self,
        source: &NodeId,
        next_hop: &NodeId,
    ) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
        let mut endpoints = Vec::new();
        for edge in self
            .edges(source)
            .iter()
            .filter(|edge| edge.target == *next_hop)
        {
            let Some(endpoint) = edge.endpoint.as_ref() else {
                continue;
            };
            if endpoints
                .iter()
                .any(|(_, retained): &(u64, ClusterEndpointRecord)| {
                    retained.endpoint_id == endpoint.endpoint_id
                        && retained.address == endpoint.address
                })
            {
                continue;
            }
            crate::object::try_reserve_vec(&mut endpoints, 1, "cluster delivery route endpoints")?;
            endpoints.push((
                edge.cost,
                try_clone_route_endpoint(endpoint, "cluster delivery route endpoints")?,
            ));
        }
        endpoints.sort_by(|(left_cost, left), (right_cost, right)| {
            left_cost
                .cmp(right_cost)
                .then_with(|| left.priority.cmp(&right.priority))
                .then_with(|| {
                    cluster_delivery_transport_sort_rank(&left.transport)
                        .cmp(&cluster_delivery_transport_sort_rank(&right.transport))
                })
                .then_with(|| left.address.cmp(&right.address))
        });
        let mut ordered = Vec::new();
        crate::object::try_reserve_vec(
            &mut ordered,
            endpoints.len(),
            "cluster delivery route endpoints",
        )?;
        for (_, endpoint) in endpoints {
            ordered.push(endpoint);
        }
        Ok(ordered)
    }

    fn position(&self, source: &NodeId) -> Result<usize, usize> {
        self.edges_by_source
            .binary_search_by(|(stored, _)| stored.cmp(source))
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ClusterDeliveryFailedEdge {
    source: NodeId,
    target: NodeId,
}

impl ClusterDeliveryFailedEdge {
    fn try_new(source: &NodeId, target: &NodeId) -> Result<Self, NetworkError> {
        Ok(Self {
            source: try_clone_route_node_id(source, "cluster delivery failed edge")?,
            target: try_clone_route_node_id(target, "cluster delivery failed edge")?,
        })
    }

    fn matches(&self, source: &NodeId, target: &NodeId) -> bool {
        self.source == *source && self.target == *target
    }
}

#[derive(Clone, Debug, Default)]
pub struct ClusterDeliveryFailedEdges {
    edges: Vec<ClusterDeliveryFailedEdge>,
}

impl ClusterDeliveryFailedEdges {
    fn contains(&self, source: &NodeId, target: &NodeId) -> bool {
        self.edges.iter().any(|edge| edge.matches(source, target))
    }

    pub fn try_insert(&mut self, source: &NodeId, target: &NodeId) -> Result<bool, NetworkError> {
        if self.contains(source, target) {
            return Ok(false);
        }
        let edge = ClusterDeliveryFailedEdge::try_new(source, target)?;
        let index = self
            .edges
            .binary_search_by(|stored| {
                (&stored.source, &stored.target).cmp(&(&edge.source, &edge.target))
            })
            .unwrap_or_else(|index| index);
        crate::object::try_reserve_vec(&mut self.edges, 1, "cluster delivery failed edges")?;
        self.edges.insert(index, edge);
        Ok(true)
    }

    fn try_clone(&self) -> Result<Self, NetworkError> {
        let mut cloned = Vec::new();
        crate::object::try_reserve_vec(
            &mut cloned,
            self.edges.len(),
            "clone cluster delivery failed edges",
        )?;
        for edge in &self.edges {
            cloned.push(ClusterDeliveryFailedEdge::try_new(
                &edge.source,
                &edge.target,
            )?);
        }
        Ok(Self { edges: cloned })
    }
}

#[derive(Clone, Debug)]
pub struct ClusterDeliveryRoutePlan {
    pub next_hop: NodeId,
    pub endpoints: Vec<ClusterEndpointRecord>,
    pub path: Vec<NodeId>,
}

#[derive(Clone, Debug)]
struct ClusterDeliveryRoutePath {
    nodes: Vec<NodeId>,
    cost: u64,
}

#[derive(Clone, Debug)]
struct ClusterDeliveryRouteCandidate {
    plan: ClusterDeliveryRoutePlan,
    path: Vec<NodeId>,
    first_hop_cost: u64,
    cost: u64,
    complete: bool,
}

impl ClusterDeliveryRouteCandidate {
    fn verified_complete(&self) -> bool {
        self.complete && self.cost < CLUSTER_DELIVERY_UNOBSERVED_TARGET_COST
    }
}

#[derive(Clone, Debug, Default)]
struct ClusterDeliveryRouteIndex {
    routes: Vec<(NodeId, Vec<ClusterDeliveryRouteCandidate>)>,
}

impl ClusterDeliveryRouteIndex {
    fn get(&self, target_node_id: &NodeId) -> Option<&[ClusterDeliveryRouteCandidate]> {
        self.position(target_node_id).ok().and_then(|index| {
            self.routes
                .get(index)
                .map(|(_, candidates)| candidates.as_slice())
        })
    }

    fn try_insert(
        &mut self,
        target_node_id: &NodeId,
        candidates: Vec<ClusterDeliveryRouteCandidate>,
    ) -> Result<(), NetworkError> {
        match self.position(target_node_id) {
            Ok(index) => {
                self.routes[index].1 = candidates;
            }
            Err(index) => {
                let target_node_id =
                    try_clone_route_node_id(target_node_id, "cluster delivery routing table")?;
                crate::object::try_reserve_vec(
                    &mut self.routes,
                    1,
                    "cluster delivery routing table",
                )?;
                self.routes.insert(index, (target_node_id, candidates));
            }
        }
        Ok(())
    }

    fn position(&self, target_node_id: &NodeId) -> Result<usize, usize> {
        self.routes
            .binary_search_by(|(stored, _)| stored.cmp(target_node_id))
    }
}

#[derive(Clone, Debug)]
pub struct ClusterDeliveryRoutingTable {
    cluster_id: ClusterId,
    local_node_id: NodeId,
    routes: ClusterDeliveryRouteIndex,
    earliest_expires_at: Option<OffsetDateTime>,
}

impl ClusterDeliveryRoutingTable {
    pub fn route_plan(
        &self,
        target_node_id: &NodeId,
        failed_edges: &ClusterDeliveryFailedEdges,
        visited: &[NodeId],
    ) -> Result<Option<ClusterDeliveryRoutePlan>, NetworkError> {
        let Some(candidates) = self.routes.get(target_node_id) else {
            return Ok(None);
        };
        Ok(cluster_delivery_select_route_candidate(
            candidates,
            failed_edges,
            visited,
        ))
    }
}

#[derive(Clone)]
struct ClusterDeliveryRoutingTableCacheEntry {
    mark: ClusterDeliveryRoutingTableCacheMark,
    table: ClusterDeliveryRoutingTable,
    validated_at: Instant,
    last_used: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterDeliveryRoutingTableCacheMark {
    state: ClusterFileMark,
    pub directory_version: Option<crate::directory::DirectoryVersion>,
    local_connectivity: ClusterFileMark,
    local_connectivity_routing_generation: u64,
    module_dialer_transports: Vec<String>,
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_delivery_route_plan(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    failed_edges: &ClusterDeliveryFailedEdges,
    visited: &[NodeId],
) -> Option<ClusterDeliveryRoutePlan> {
    let graph =
        cluster_delivery_route_graph(paths, state, local_node_id, failed_edges, target_node_id)
            .ok()?;
    let local_connectivity = ClusterConnectivityGraph::try_read_for_local(paths, state).ok()?;
    let mut candidates =
        cluster_delivery_route_candidates(&graph, local_node_id, target_node_id).ok()?;
    cluster_delivery_rank_route_candidate_endpoints_for_attempt(
        &local_connectivity,
        &mut candidates,
        true,
    );
    cluster_delivery_select_route_candidate(&candidates, failed_edges, visited)
}

fn cluster_delivery_route_candidates(
    graph: &ClusterDeliveryRouteGraph,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
) -> Result<Vec<ClusterDeliveryRouteCandidate>, NetworkError> {
    let mut next_hop_costs = RouteDistanceIndex::default();
    for edge in graph.edges(local_node_id) {
        if edge.target == *local_node_id {
            continue;
        }
        next_hop_costs.try_insert_min(
            &edge.target,
            edge.cost,
            "cluster delivery next-hop costs",
        )?;
    }

    let mut candidates = Vec::new();
    for (next_hop, edge_cost) in next_hop_costs.iter() {
        let endpoints = graph.endpoints_for_next_hop(local_node_id, &next_hop)?;
        if endpoints.is_empty() {
            continue;
        }
        if *next_hop == *target_node_id {
            let path = cluster_delivery_two_node_path(local_node_id, next_hop)?;
            crate::object::try_reserve_vec(
                &mut candidates,
                1,
                "cluster delivery route candidates",
            )?;
            candidates.push(ClusterDeliveryRouteCandidate {
                plan: ClusterDeliveryRoutePlan {
                    next_hop: try_clone_route_node_id(next_hop, "cluster delivery next hop")?,
                    endpoints,
                    path: try_clone_route_node_ids(&path, "cluster delivery route plan path")?,
                },
                path,
                first_hop_cost: edge_cost,
                cost: edge_cost,
                complete: true,
            });
            continue;
        }
        let excluded_origin = std::slice::from_ref(local_node_id);
        if let Some(path) =
            cluster_delivery_shortest_path(graph, &next_hop, target_node_id, excluded_origin)?
        {
            let mut nodes = Vec::new();
            crate::object::try_reserve_vec(
                &mut nodes,
                path.nodes.len().saturating_add(1),
                "cluster delivery route path",
            )?;
            nodes.push(try_clone_route_node_id(
                local_node_id,
                "cluster delivery route path",
            )?);
            nodes.extend(path.nodes);
            crate::object::try_reserve_vec(
                &mut candidates,
                1,
                "cluster delivery route candidates",
            )?;
            candidates.push(ClusterDeliveryRouteCandidate {
                plan: ClusterDeliveryRoutePlan {
                    next_hop: try_clone_route_node_id(next_hop, "cluster delivery next hop")?,
                    endpoints,
                    path: try_clone_route_node_ids(&nodes, "cluster delivery route plan path")?,
                },
                path: nodes,
                first_hop_cost: edge_cost,
                cost: edge_cost.saturating_add(path.cost),
                complete: true,
            });
        }
        // Do not synthesize an incomplete relay hop here. A non-target next hop is only a
        // valid route when the connectivity graph proves it can reach the target.
    }
    candidates.sort_by(
        |left, right| match (left.verified_complete(), right.verified_complete()) {
            (true, true) => (
                cluster_delivery_route_candidate_hop_rank(left, target_node_id),
                left.cost,
                left.first_hop_cost,
                left.plan.next_hop.as_str(),
            )
                .cmp(&(
                    cluster_delivery_route_candidate_hop_rank(right, target_node_id),
                    right.cost,
                    right.first_hop_cost,
                    right.plan.next_hop.as_str(),
                )),
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            (false, false) => (
                left.first_hop_cost,
                if left.complete { 0 } else { 1 },
                left.cost,
                left.plan.next_hop.as_str(),
            )
                .cmp(&(
                    right.first_hop_cost,
                    if right.complete { 0 } else { 1 },
                    right.cost,
                    right.plan.next_hop.as_str(),
                )),
        },
    );
    Ok(candidates)
}

fn cluster_delivery_rank_route_candidate_endpoints_for_attempt(
    graph: &ClusterConnectivityGraph,
    candidates: &mut [ClusterDeliveryRouteCandidate],
    prefer_direct_endpoints: bool,
) {
    for candidate in candidates {
        cluster_delivery_sort_route_endpoints_for_attempt(
            graph,
            &mut candidate.plan.endpoints,
            prefer_direct_endpoints,
        );
    }
}

fn cluster_delivery_route_candidate_hop_rank(
    candidate: &ClusterDeliveryRouteCandidate,
    target_node_id: &NodeId,
) -> u8 {
    if candidate.plan.next_hop == *target_node_id {
        0
    } else {
        1
    }
}

fn cluster_delivery_two_node_path(
    local_node_id: &NodeId,
    next_hop: &NodeId,
) -> Result<Vec<NodeId>, NetworkError> {
    let mut path = Vec::new();
    crate::object::try_reserve_vec(&mut path, 2, "cluster delivery route path")?;
    path.push(try_clone_route_node_id(
        local_node_id,
        "cluster delivery route path",
    )?);
    path.push(try_clone_route_node_id(
        next_hop,
        "cluster delivery route path",
    )?);
    Ok(path)
}

fn cluster_delivery_select_route_candidate(
    candidates: &[ClusterDeliveryRouteCandidate],
    failed_edges: &ClusterDeliveryFailedEdges,
    visited: &[NodeId],
) -> Option<ClusterDeliveryRoutePlan> {
    candidates
        .iter()
        .filter(|candidate| {
            cluster_delivery_route_candidate_is_compatible(candidate, failed_edges, visited)
        })
        .map(|candidate| candidate.plan.clone())
        .next()
}

fn cluster_delivery_route_candidate_is_compatible(
    candidate: &ClusterDeliveryRouteCandidate,
    failed_edges: &ClusterDeliveryFailedEdges,
    visited: &[NodeId],
) -> bool {
    if candidate
        .path
        .iter()
        .skip(1)
        .any(|node| visited.iter().any(|visited| visited == node))
    {
        return false;
    }
    !candidate
        .path
        .windows(2)
        .any(|edge| failed_edges.contains(&edge[0], &edge[1]))
}

// Kept as the trace-free public entry point for callers and integration tests;
// production routing uses the timeline-aware private variant below.
#[allow(dead_code)]
pub async fn cluster_delivery_route_plan_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    failed_edges: ClusterDeliveryFailedEdges,
    visited: Vec<NodeId>,
) -> Result<Option<ClusterDeliveryRoutePlan>, NetworkError> {
    cluster_delivery_route_plan_async_with_timeline(
        runtime,
        paths,
        state,
        local_node_id,
        target_node_id,
        failed_edges,
        visited,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn cluster_delivery_route_plan_async_with_timeline(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    failed_edges: ClusterDeliveryFailedEdges,
    visited: Vec<NodeId>,
    timeline: Option<crate::NetworkTimelineTraceContext>,
) -> Result<Option<ClusterDeliveryRoutePlan>, NetworkError> {
    cluster_delivery_route_plan_from_routing_table_async(
        runtime,
        paths,
        state,
        local_node_id,
        target_node_id,
        &failed_edges,
        &visited,
        timeline,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn cluster_delivery_route_plan_from_routing_table_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    failed_edges: &ClusterDeliveryFailedEdges,
    visited: &[NodeId],
    timeline: Option<crate::NetworkTimelineTraceContext>,
) -> Result<Option<ClusterDeliveryRoutePlan>, NetworkError> {
    let metric_started = Instant::now();
    let perf_started = network_perf_trace_enabled().then(Instant::now);
    let cache_lookup_started_ns = timeline
        .map(|_| crate::network_timeline_monotonic_ns())
        .unwrap_or(0);
    let cached_plan = cluster_delivery_route_plan_from_cached_routing_table(
        paths,
        state,
        local_node_id,
        target_node_id,
        failed_edges,
        visited,
        OffsetDateTime::now_utc(),
    );
    record_route_timeline_stage(
        timeline,
        crate::network_timeline_stage::ROUTE_CACHE_LOOKUP,
        cache_lookup_started_ns,
        cached_plan.as_ref().is_none_or(|plan| plan.is_ok()),
        u32::from(cached_plan.is_some()),
    );
    if let Some(plan) = cached_plan {
        if let Some(started) = perf_started {
            tracing::info!(
                target: "sinomo.network.perf",
                stage = "route_plan",
                source_node_id = %local_node_id,
                target_node_id = %target_node_id,
                cache_hit = true,
                ok = plan.is_ok(),
                found = matches!(&plan, Ok(Some(_))),
                elapsed_us = network_perf_trace_elapsed_us(started),
            );
        }
        record_network_route_plan(metric_started, "hit", &plan);
        return plan;
    }

    let paths = paths.clone();
    let state = state.clone();
    let owned_local_node_id =
        try_clone_route_node_id(local_node_id, "cluster delivery route local node")?;
    let owned_target_node_id =
        try_clone_route_node_id(target_node_id, "cluster delivery route target node")?;
    let failed_edges = failed_edges.try_clone()?;
    let visited = try_clone_route_node_ids(visited, "cluster delivery route visited nodes")?;
    let blocking_queued_ns = timeline
        .map(|_| crate::network_timeline_monotonic_ns())
        .unwrap_or(0);
    let result = runtime
        .spawn_blocking(move || {
            record_route_timeline_stage(
                timeline,
                crate::network_timeline_stage::ROUTE_BLOCKING_QUEUE,
                blocking_queued_ns,
                true,
                0,
            );
            let before_mark = cluster_delivery_routing_table_cache_mark(&paths);
            let table_build_started_ns = timeline
                .map(|_| crate::network_timeline_monotonic_ns())
                .unwrap_or(0);
            let table_result =
                cluster_delivery_build_routing_table(&paths, &state, &owned_local_node_id);
            record_route_timeline_stage(
                timeline,
                crate::network_timeline_stage::ROUTE_TABLE_BUILD,
                table_build_started_ns,
                table_result.is_ok(),
                0,
            );
            let table = table_result?;
            let table_plan_started_ns = timeline
                .map(|_| crate::network_timeline_monotonic_ns())
                .unwrap_or(0);
            let plan_result = table.route_plan(&owned_target_node_id, &failed_edges, &visited);
            record_route_timeline_stage(
                timeline,
                crate::network_timeline_stage::ROUTE_TABLE_PLAN,
                table_plan_started_ns,
                plan_result.is_ok(),
                0,
            );
            let plan = plan_result?;
            let after_mark = cluster_delivery_routing_table_cache_mark(&paths);
            if before_mark == after_mark {
                store_cluster_delivery_routing_table_cache_entry(&paths, after_mark, table);
            }
            Ok(plan)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster delivery routing table".to_string(),
            detail: format!("routing table task failed: {source}"),
        })?;
    if let Some(started) = perf_started {
        tracing::info!(
            target: "sinomo.network.perf",
            stage = "route_plan",
            source_node_id = %local_node_id,
            target_node_id = %target_node_id,
            cache_hit = false,
            ok = result.is_ok(),
            found = matches!(&result, Ok(Some(_))),
            elapsed_us = network_perf_trace_elapsed_us(started),
        );
    }
    record_network_route_plan(metric_started, "miss", &result);
    result
}

fn cluster_delivery_route_plan_from_cached_routing_table(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    failed_edges: &ClusterDeliveryFailedEdges,
    visited: &[NodeId],
    now: OffsetDateTime,
) -> Option<Result<Option<ClusterDeliveryRoutePlan>, NetworkError>> {
    let checked_at = Instant::now();
    let routing_generation =
        crate::connectivity_store::local_cluster_connectivity_routing_generation(paths);
    {
        let mut cache = CLUSTER_DELIVERY_ROUTING_TABLES.lock().ok()?;
        let stale = {
            let entry = cache.get(&paths.root)?;
            entry.mark.local_connectivity_routing_generation != routing_generation
                || entry.table.cluster_id != state.cluster_id
                || entry.table.local_node_id != *local_node_id
                || entry
                    .table
                    .earliest_expires_at
                    .is_some_and(|expires_at| expires_at <= now)
        };
        if stale {
            cache.remove(&paths.root);
            return None;
        }
        let entry = cache.get_mut(&paths.root)?;
        if checked_at.saturating_duration_since(entry.validated_at)
            < CLUSTER_DELIVERY_ROUTING_TABLE_VALIDATION_LEASE
        {
            entry.last_used = cluster_delivery_routing_table_cache_tick();
            return Some(
                entry
                    .table
                    .route_plan(target_node_id, failed_edges, visited),
            );
        }
    }

    let mark = cluster_delivery_routing_table_cache_mark(paths)?;
    let mut cache = CLUSTER_DELIVERY_ROUTING_TABLES.lock().ok()?;
    let stale = {
        let entry = cache.get(&paths.root)?;
        entry.mark != mark
            || entry.table.cluster_id != state.cluster_id
            || entry.table.local_node_id != *local_node_id
            || entry
                .table
                .earliest_expires_at
                .is_some_and(|expires_at| expires_at <= now)
    };
    if stale {
        cache.remove(&paths.root);
        return None;
    }
    let entry = cache.get_mut(&paths.root)?;
    // The durable mark was validated after `checked_at` was captured, and a
    // concurrent reader may also have refreshed this entry in between locks.
    // Record completion time so the lease is monotonic and fully usable.
    entry.validated_at = Instant::now();
    entry.last_used = cluster_delivery_routing_table_cache_tick();
    Some(
        entry
            .table
            .route_plan(target_node_id, failed_edges, visited),
    )
}

pub fn cluster_delivery_build_routing_table(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
) -> Result<ClusterDeliveryRoutingTable, NetworkError> {
    let mut routes = ClusterDeliveryRouteIndex::default();
    let members = cluster_delivery_members_for_routing(paths, state)?;
    let local_connectivity = ClusterConnectivityGraph::try_read_for_local(paths, state)?;
    let earliest_expires_at = cluster_delivery_routing_table_earliest_expires_at(paths, state);
    for member in &members {
        if member.node_id == *local_node_id || !member.state.is_authorized() {
            continue;
        }
        let graph = cluster_delivery_route_graph_with_members(
            paths,
            state,
            &members,
            local_node_id,
            &ClusterDeliveryFailedEdges::default(),
            &member.node_id,
        )?;
        let mut candidates =
            cluster_delivery_route_candidates(&graph, local_node_id, &member.node_id)?;
        cluster_delivery_rank_route_candidate_endpoints_for_attempt(
            &local_connectivity,
            &mut candidates,
            true,
        );
        if !candidates.is_empty() {
            routes.try_insert(&member.node_id, candidates)?;
        }
    }
    Ok(ClusterDeliveryRoutingTable {
        cluster_id: crate::object::try_clone_id(&state.cluster_id, "cluster delivery table id")?,
        local_node_id: try_clone_route_node_id(local_node_id, "cluster delivery table local node")?,
        routes,
        earliest_expires_at,
    })
}

fn cluster_delivery_routing_table_earliest_expires_at(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Option<OffsetDateTime> {
    let now = OffsetDateTime::now_utc();
    crate::route_graph_read_model::read_cluster_connectivity_graph_for_state(paths, state)
        .unwrap_or_default()
        .into_iter()
        .filter(|record| {
            record.cluster_id == state.cluster_id
                && crate::connectivity::cluster_network_record_live(record.expires_at, now)
        })
        .map(|record| record.expires_at)
        .min()
}

pub fn cluster_delivery_routing_table_cache_mark(
    paths: &ClusterPaths,
) -> Option<ClusterDeliveryRoutingTableCacheMark> {
    let mut module_dialer_transports = crate::network_module::network_module_registry()
        .map(|registry| {
            registry
                .enabled_dialer_transports()
                .into_iter()
                .map(|transport| format!("{transport:?}"))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    module_dialer_transports.sort();
    Some(ClusterDeliveryRoutingTableCacheMark {
        state: cluster_file_mark(&paths.state)?,
        // A directory version error means "unknown": return no mark at all
        // (skip caching) rather than a `None` field that would compare equal
        // to itself and keep validating a stale table.
        directory_version: Some(
            crate::directory::cluster_directory_for(paths)
                .version()
                .ok()?,
        ),
        local_connectivity: cluster_file_mark(&paths.connectivity_local)?,
        local_connectivity_routing_generation:
            crate::connectivity_store::local_cluster_connectivity_routing_generation(paths),
        module_dialer_transports,
    })
}

fn cluster_delivery_routing_table_cache_tick() -> u64 {
    CLUSTER_DELIVERY_ROUTING_TABLE_CACHE_CLOCK.fetch_add(1, Ordering::Relaxed)
}

fn store_cluster_delivery_routing_table_cache_entry(
    paths: &ClusterPaths,
    mark: Option<ClusterDeliveryRoutingTableCacheMark>,
    table: ClusterDeliveryRoutingTable,
) {
    let Some(mark) = mark else {
        return;
    };
    let Ok(mut cache) = CLUSTER_DELIVERY_ROUTING_TABLES.lock() else {
        return;
    };
    evict_cluster_delivery_routing_table_cache_entry_if_needed(&mut cache, &paths.root);
    cache.insert(
        paths.root.clone(),
        ClusterDeliveryRoutingTableCacheEntry {
            mark,
            table,
            validated_at: Instant::now(),
            last_used: cluster_delivery_routing_table_cache_tick(),
        },
    );
}

pub fn invalidate_cluster_delivery_routing_table_cache(paths: &ClusterPaths) {
    if let Ok(mut cache) = CLUSTER_DELIVERY_ROUTING_TABLES.lock() {
        cache.remove(&paths.root);
    }
}

fn evict_cluster_delivery_routing_table_cache_entry_if_needed(
    cache: &mut HashMap<PathBuf, ClusterDeliveryRoutingTableCacheEntry>,
    protected: &PathBuf,
) {
    if cache.contains_key(protected)
        || cache.len() < MAX_CLUSTER_DELIVERY_ROUTING_TABLE_CACHE_ENTRIES
    {
        return;
    }
    let eviction_candidate = cache
        .iter()
        .filter(|(path, _)| *path != protected)
        .min_by_key(|(_, entry)| entry.last_used)
        .map(|(path, _)| path.clone());
    if let Some(path) = eviction_candidate {
        cache.remove(&path);
    }
}

#[cfg(any(test, feature = "test-support"))]
fn cluster_delivery_route_graph(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    failed_edges: &ClusterDeliveryFailedEdges,
    target_node_id: &NodeId,
) -> Result<ClusterDeliveryRouteGraph, NetworkError> {
    let members = cluster_delivery_members_for_routing(paths, state)?;
    cluster_delivery_route_graph_with_members(
        paths,
        state,
        &members,
        local_node_id,
        failed_edges,
        target_node_id,
    )
}

fn cluster_delivery_route_graph_with_members(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    members: &[ClusterMemberRecord],
    local_node_id: &NodeId,
    failed_edges: &ClusterDeliveryFailedEdges,
    target_node_id: &NodeId,
) -> Result<ClusterDeliveryRouteGraph, NetworkError> {
    let local_connectivity = ClusterConnectivityGraph::try_read_for_local(paths, state)?;
    let local_private_prefixes = cluster_delivery_local_private_prefixes(members, local_node_id)?;
    let mut graph = ClusterDeliveryRouteGraph::default();
    let mut member_endpoint_edge_nodes = RouteNodeSet::default();
    for member in members {
        if !member.state.is_authorized() {
            continue;
        }
        for endpoint in &member.endpoints {
            if !cluster_delivery_local_endpoint_is_usable(local_node_id, endpoint) {
                continue;
            }
            if failed_edges.contains(local_node_id, &endpoint.node_id) {
                continue;
            }
            let Some(cost) = cluster_delivery_local_endpoint_cost(
                state,
                &local_connectivity,
                endpoint,
                target_node_id,
                &local_private_prefixes,
            ) else {
                continue;
            };
            graph.push_edge_clone(local_node_id, &endpoint.node_id, Some(endpoint), cost)?;
            member_endpoint_edge_nodes
                .try_insert_clone(&endpoint.node_id, "cluster delivery member endpoint edge")?;
        }
    }
    for member in members {
        if member.node_id == *local_node_id
            || !member.state.is_authorized()
            || failed_edges.contains(local_node_id, &member.node_id)
        {
            continue;
        }
        for endpoint in local_connectivity.reachable_endpoints_matching(
            local_node_id,
            &member.node_id,
            |endpoint| cluster_delivery_local_endpoint_is_usable(local_node_id, endpoint),
        ) {
            if member_endpoint_edge_nodes.position(&member.node_id).is_ok()
                && !cluster_delivery_member_has_endpoint(member, &endpoint)
                && !cluster_delivery_endpoint_is_tcp_tunnel(&endpoint)
            {
                continue;
            }
            let Some(cost) = cluster_delivery_local_endpoint_cost(
                state,
                &local_connectivity,
                &endpoint,
                target_node_id,
                &local_private_prefixes,
            ) else {
                continue;
            };
            graph.push_edge_clone(local_node_id, &member.node_id, Some(&endpoint), cost)?;
        }
    }

    for record in
        crate::route_graph_read_model::read_cluster_connectivity_graph_for_state(paths, state)
            .unwrap_or_default()
    {
        if record.cluster_id != state.cluster_id
            || record.source_node_id == record.target_node_id
            || record.source_node_id == *local_node_id
            || failed_edges.contains(&record.source_node_id, &record.target_node_id)
        {
            continue;
        }
        let Some(cost) = cluster_delivery_connectivity_record_cost(&record) else {
            continue;
        };
        graph.push_edge_clone(&record.source_node_id, &record.target_node_id, None, cost)?;
    }
    Ok(graph)
}

fn cluster_delivery_members_for_routing(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterMemberRecord>, NetworkError> {
    let directory = crate::directory::cluster_directory_for(paths);
    let mut members_by_node: BTreeMap<NodeId, ClusterMemberRecord> = BTreeMap::new();
    for member in state
        .members
        .iter()
        .filter(|member| member.cluster_id == state.cluster_id)
    {
        members_by_node.insert(member.node_id.clone(), member.clone());
    }

    let directory_members = match directory.members() {
        Ok(members) => members,
        Err(error @ NetworkError::ResourceExhausted { .. }) => return Err(error),
        Err(_) => Vec::new(),
    };
    for member in directory_members
        .into_iter()
        .filter(|member| member.cluster_id == state.cluster_id)
    {
        cluster_delivery_merge_member_record(&mut members_by_node, member)?;
    }

    let directory_endpoints = match directory.endpoints() {
        Ok(endpoints) => endpoints,
        Err(error @ NetworkError::ResourceExhausted { .. }) => return Err(error),
        Err(_) => Vec::new(),
    };
    for endpoint in directory_endpoints {
        let Some(member) = members_by_node.get_mut(&endpoint.node_id) else {
            continue;
        };
        cluster_delivery_upsert_member_endpoint(member, endpoint)?;
    }

    Ok(members_by_node.into_values().collect())
}

fn cluster_delivery_member_has_endpoint(
    member: &ClusterMemberRecord,
    endpoint: &ClusterEndpointRecord,
) -> bool {
    member.endpoints.iter().any(|candidate| {
        candidate.endpoint_id == endpoint.endpoint_id && candidate.address == endpoint.address
    })
}

fn cluster_delivery_merge_member_record(
    members_by_node: &mut BTreeMap<NodeId, ClusterMemberRecord>,
    incoming: ClusterMemberRecord,
) -> Result<(), NetworkError> {
    match members_by_node.get_mut(&incoming.node_id) {
        Some(existing) => {
            let retained_endpoints = std::mem::take(&mut existing.endpoints);
            *existing = incoming;
            for endpoint in retained_endpoints {
                if existing
                    .endpoints
                    .iter()
                    .any(|candidate| candidate.endpoint_id == endpoint.endpoint_id)
                {
                    continue;
                }
                cluster_delivery_upsert_member_endpoint(existing, endpoint)?;
            }
        }
        None => {
            members_by_node.insert(incoming.node_id.clone(), incoming);
        }
    }
    Ok(())
}

fn cluster_delivery_upsert_member_endpoint(
    member: &mut ClusterMemberRecord,
    endpoint: ClusterEndpointRecord,
) -> Result<(), NetworkError> {
    if endpoint.node_id != member.node_id {
        return Ok(());
    }
    if let Some(existing) = member
        .endpoints
        .iter_mut()
        .find(|candidate| candidate.endpoint_id == endpoint.endpoint_id)
    {
        *existing = endpoint;
        return Ok(());
    }
    crate::object::try_reserve_vec(
        &mut member.endpoints,
        1,
        "merge cluster directory endpoints",
    )?;
    member.endpoints.push(endpoint);
    Ok(())
}

fn cluster_delivery_local_endpoint_is_usable(
    local_node_id: &NodeId,
    endpoint: &ClusterEndpointRecord,
) -> bool {
    if endpoint.privacy == ClusterEndpointPrivacy::LocalOnly && endpoint.node_id != *local_node_id {
        return false;
    }
    cluster_endpoint_can_carry_main_daemon_rpc(endpoint)
        && cluster_delivery_endpoint_transport_is_dialable(&endpoint.transport)
}

fn cluster_delivery_endpoint_transport_is_dialable(transport: &ClusterTransportKind) -> bool {
    match transport {
        ClusterTransportKind::Loopback | ClusterTransportKind::LanTcp => true,
        other => crate::network_module::network_module_registry()
            .is_some_and(|registry| registry.enabled_dialer_transports().contains(other)),
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_delivery_shortest_path_next_hop(
    graph: &ClusterDeliveryRouteGraph,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    visited: &[NodeId],
) -> Option<NodeId> {
    cluster_delivery_shortest_path(graph, local_node_id, target_node_id, visited)
        .ok()
        .flatten()
        .and_then(|path| path.nodes.get(1).cloned())
}

fn cluster_delivery_shortest_path(
    graph: &ClusterDeliveryRouteGraph,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    visited: &[NodeId],
) -> Result<Option<ClusterDeliveryRoutePath>, NetworkError> {
    if local_node_id == target_node_id {
        let mut nodes = Vec::new();
        crate::object::try_reserve_vec(&mut nodes, 1, "cluster delivery shortest path")?;
        nodes.push(try_clone_route_node_id(
            local_node_id,
            "cluster delivery shortest path",
        )?);
        return Ok(Some(ClusterDeliveryRoutePath { nodes, cost: 0 }));
    }
    let mut distance = RouteDistanceIndex::default();
    distance.try_insert_or_update(local_node_id, 0, "cluster delivery shortest path distance")?;
    let mut predecessor = RoutePredecessorIndex::default();
    let mut unsettled = RouteNodeSet::default();
    unsettled.try_insert_clone(local_node_id, "cluster delivery unsettled nodes")?;
    while let Some(current) = unsettled.min_by_distance(&distance)? {
        unsettled.remove(&current);
        if current == *target_node_id {
            break;
        }
        let Some(current_distance) = distance.get(&current) else {
            continue;
        };
        for edge in graph.edges(&current) {
            if visited.iter().any(|visited| visited == &edge.target) {
                continue;
            }
            let candidate_distance = current_distance.saturating_add(edge.cost);
            if candidate_distance < distance.get(&edge.target).unwrap_or(u64::MAX) {
                distance.try_insert_or_update(
                    &edge.target,
                    candidate_distance,
                    "cluster delivery shortest path distance",
                )?;
                predecessor.try_insert_clone(
                    &edge.target,
                    &current,
                    "cluster delivery shortest path predecessor",
                )?;
                unsettled.try_insert_clone(&edge.target, "cluster delivery unsettled nodes")?;
            }
        }
    }
    if !distance.contains_key(target_node_id) {
        return Ok(None);
    }
    let Some(cost) = distance.get(target_node_id) else {
        return Ok(None);
    };
    let mut reversed = Vec::new();
    crate::object::try_reserve_vec(&mut reversed, 1, "cluster delivery shortest path")?;
    reversed.push(try_clone_route_node_id(
        target_node_id,
        "cluster delivery shortest path",
    )?);
    let mut current = try_clone_route_node_id(target_node_id, "cluster delivery shortest path")?;
    while current != *local_node_id {
        let Some(previous) = predecessor.get(&current) else {
            return Ok(None);
        };
        crate::object::try_reserve_vec(&mut reversed, 1, "cluster delivery shortest path")?;
        reversed.push(try_clone_route_node_id(
            previous,
            "cluster delivery shortest path",
        )?);
        current = try_clone_route_node_id(previous, "cluster delivery shortest path")?;
    }
    reversed.reverse();
    Ok(Some(ClusterDeliveryRoutePath {
        nodes: reversed,
        cost,
    }))
}

fn cluster_delivery_local_endpoint_cost(
    state: &ClusterBootstrapState,
    graph: &ClusterConnectivityGraph,
    endpoint: &ClusterEndpointRecord,
    target_node_id: &NodeId,
    local_private_prefixes: &ClusterDeliveryPrivatePrefixes,
) -> Option<u64> {
    let path_cost = cluster_delivery_endpoint_path_cost(
        &endpoint.transport,
        &endpoint.address,
        &endpoint.privacy,
    );
    let endpoint_priority = u64::from(endpoint.priority);
    match graph.record_for_endpoint(endpoint) {
        Some(record) => {
            // Known-unreachable relay neighbors are never usable. At the final
            // target, suppress only built-in endpoints that the active probe
            // loop can rediscover from membership state; letting every
            // application RPC reprobe those turns one absent node into
            // cluster-wide head-of-line blocking. Directory-only and module
            // endpoints retain the old final-target fallback because the
            // built-in probe loop cannot establish their recovery yet.
            if matches!(record.state, ClusterConnectivityState::Unreachable)
                && (endpoint.node_id != *target_node_id
                    || cluster_delivery_endpoint_is_active_builtin_probe_target(state, endpoint))
            {
                return None;
            }
            cluster_delivery_connectivity_record_cost_with_path(record, path_cost)
                .map(|cost| cost.saturating_add(endpoint_priority))
        }
        None if endpoint.node_id == *target_node_id => Some(
            cluster_delivery_unobserved_target_endpoint_cost(path_cost, endpoint_priority),
        ),
        None if cluster_delivery_endpoint_shares_local_private_prefix(
            endpoint,
            local_private_prefixes,
        ) =>
        {
            Some(CLUSTER_DELIVERY_UNOBSERVED_NEIGHBOR_COST + path_cost + endpoint_priority)
        }
        None => None,
    }
}

fn cluster_delivery_endpoint_is_active_builtin_probe_target(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
) -> bool {
    crate::endpoint_selector::cluster_endpoint_is_builtin_probe_target(endpoint)
        && state.members.iter().any(|member| {
            member.state.is_authorized()
                && !cluster_member_is_local_process(state, member)
                && member.node_id == endpoint.node_id
                && member.endpoints.iter().any(|advertised| {
                    advertised.endpoint_id == endpoint.endpoint_id
                        && advertised.transport == endpoint.transport
                        && advertised.address == endpoint.address
                })
        })
}

fn cluster_delivery_unobserved_target_endpoint_cost(path_cost: u64, endpoint_priority: u64) -> u64 {
    CLUSTER_DELIVERY_UNOBSERVED_TARGET_COST + path_cost + endpoint_priority
}

fn cluster_delivery_connectivity_record_cost(record: &ClusterConnectivityRecord) -> Option<u64> {
    if matches!(record.state, ClusterConnectivityState::Unreachable) {
        return None;
    }
    let path_cost = cluster_delivery_record_path_cost(&record.transport, &record.address);
    cluster_delivery_connectivity_record_cost_with_path(record, path_cost)
}

fn cluster_delivery_connectivity_record_cost_with_path(
    record: &ClusterConnectivityRecord,
    path_cost: u64,
) -> Option<u64> {
    if crate::connectivity::cluster_connectivity_failure_reason_is_identity_mismatch(record) {
        return None;
    }
    cluster_delivery_connectivity_state_cost(
        path_cost,
        record.latency_ms,
        record.consecutive_failures,
        &record.state,
    )
}

fn cluster_delivery_connectivity_state_cost(
    path_cost: u64,
    latency_ms: Option<u64>,
    consecutive_failures: u16,
    state: &ClusterConnectivityState,
) -> Option<u64> {
    match state {
        ClusterConnectivityState::Healthy => Some(path_cost + latency_ms.unwrap_or(0).min(10_000)),
        ClusterConnectivityState::Degraded => Some(
            path_cost
                + CLUSTER_DELIVERY_DEGRADED_LINK_COST
                + u64::from(consecutive_failures) * 1_000,
        ),
        ClusterConnectivityState::Unreachable => Some(
            path_cost
                .saturating_add(CLUSTER_DELIVERY_UNREACHABLE_LINK_COST)
                .saturating_add(u64::from(consecutive_failures) * 10_000),
        ),
    }
}

fn cluster_delivery_endpoint_path_cost(
    transport: &ClusterTransportKind,
    address: &str,
    privacy: &ClusterEndpointPrivacy,
) -> u64 {
    if cluster_delivery_address_is_tcp_tunnel(address) {
        return CLUSTER_DELIVERY_TCP_TUNNEL_PATH_COST;
    }
    match transport {
        ClusterTransportKind::InProcess => 0,
        ClusterTransportKind::Loopback => 1,
        ClusterTransportKind::LanQuic => 10,
        ClusterTransportKind::LanTcp => {
            if *privacy == ClusterEndpointPrivacy::InternetDirect
                || cluster_delivery_address_is_public(address)
            {
                40
            } else {
                20
            }
        }
        ClusterTransportKind::WebRtc => 70,
        ClusterTransportKind::TurnRelay | ClusterTransportKind::HttpsRelay => 100,
        _ => match privacy {
            ClusterEndpointPrivacy::InternetDirect => 60,
            ClusterEndpointPrivacy::Relayed => 100,
            ClusterEndpointPrivacy::Lan => 70,
            ClusterEndpointPrivacy::LocalOnly => 120,
        },
    }
}

fn cluster_delivery_sort_route_endpoints_for_attempt(
    graph: &ClusterConnectivityGraph,
    endpoints: &mut [ClusterEndpointRecord],
    prefer_direct_endpoints: bool,
) {
    endpoints.sort_by(|left, right| {
        (
            graph.unavailability_rank(left),
            prefer_direct_endpoints.then(|| cluster_delivery_endpoint_attempt_sort_rank(left)),
            graph.quality_rank(left),
            cluster_delivery_endpoint_observed_latency_rank(graph, left),
            cluster_delivery_endpoint_path_cost(&left.transport, &left.address, &left.privacy),
            left.priority,
            cluster_delivery_transport_sort_rank(&left.transport),
            left.address.as_str(),
        )
            .cmp(&(
                graph.unavailability_rank(right),
                prefer_direct_endpoints.then(|| cluster_delivery_endpoint_attempt_sort_rank(right)),
                graph.quality_rank(right),
                cluster_delivery_endpoint_observed_latency_rank(graph, right),
                cluster_delivery_endpoint_path_cost(
                    &right.transport,
                    &right.address,
                    &right.privacy,
                ),
                right.priority,
                cluster_delivery_transport_sort_rank(&right.transport),
                right.address.as_str(),
            ))
    });
}

fn cluster_delivery_endpoint_observed_latency_rank(
    graph: &ClusterConnectivityGraph,
    endpoint: &ClusterEndpointRecord,
) -> u64 {
    graph
        .record_for_endpoint(endpoint)
        .and_then(|record| record.latency_ms)
        .or_else(|| endpoint.last_observed_latency_ms.map(u64::from))
        .unwrap_or(u64::MAX)
}

fn cluster_delivery_endpoint_attempt_sort_rank(endpoint: &ClusterEndpointRecord) -> u8 {
    if cluster_delivery_endpoint_is_tcp_tunnel(endpoint) {
        1
    } else {
        0
    }
}

fn cluster_delivery_endpoint_is_tcp_tunnel(endpoint: &ClusterEndpointRecord) -> bool {
    endpoint.transport == ClusterTransportKind::LanTcp
        && cluster_delivery_address_is_tcp_tunnel(&endpoint.address)
}

fn cluster_delivery_address_is_tcp_tunnel(address: &str) -> bool {
    address.starts_with(crate::CLUSTER_TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX)
}

fn cluster_delivery_record_path_cost(transport: &ClusterTransportKind, address: &str) -> u64 {
    let privacy = if cluster_delivery_address_is_public(address) {
        ClusterEndpointPrivacy::InternetDirect
    } else {
        ClusterEndpointPrivacy::Lan
    };
    cluster_delivery_endpoint_path_cost(transport, address, &privacy)
}

fn cluster_delivery_transport_sort_rank(transport: &ClusterTransportKind) -> u8 {
    match transport {
        ClusterTransportKind::InProcess => 0,
        ClusterTransportKind::Loopback => 1,
        ClusterTransportKind::LanTcp => 2,
        ClusterTransportKind::LanQuic => 3,
        ClusterTransportKind::WebRtc => 5,
        ClusterTransportKind::TurnRelay => 6,
        ClusterTransportKind::HttpsRelay => 7,
        _ => 4,
    }
}

fn cluster_delivery_address_is_public(address: &str) -> bool {
    let host = cluster_delivery_address_ip(address);
    let Some(host) = host else {
        return false;
    };
    match host {
        std::net::IpAddr::V4(ip) => {
            !(ip.is_private() || ip.is_loopback() || ip.is_link_local() || ip.is_unspecified())
        }
        std::net::IpAddr::V6(ip) => {
            let first_segment = ip.segments()[0];
            let unique_local = (first_segment & 0xfe00) == 0xfc00;
            let link_local = (first_segment & 0xffc0) == 0xfe80;
            !(ip.is_loopback() || ip.is_unspecified() || unique_local || link_local)
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ClusterDeliveryPrivatePrefix {
    V4([u8; 3]),
    V6([u16; 4]),
}

#[derive(Clone, Debug, Default)]
struct ClusterDeliveryPrivatePrefixes {
    prefixes: Vec<ClusterDeliveryPrivatePrefix>,
}

impl ClusterDeliveryPrivatePrefixes {
    fn try_insert(&mut self, prefix: ClusterDeliveryPrivatePrefix) -> Result<(), NetworkError> {
        match self.prefixes.binary_search(&prefix) {
            Ok(_) => Ok(()),
            Err(index) => {
                crate::object::try_reserve_vec(
                    &mut self.prefixes,
                    1,
                    "cluster delivery private prefixes",
                )?;
                self.prefixes.insert(index, prefix);
                Ok(())
            }
        }
    }

    fn contains(&self, prefix: &ClusterDeliveryPrivatePrefix) -> bool {
        self.prefixes.binary_search(prefix).is_ok()
    }
}

fn cluster_delivery_address_ip(address: &str) -> Option<std::net::IpAddr> {
    address
        .parse::<std::net::SocketAddr>()
        .ok()
        .map(|socket| socket.ip())
        .or_else(|| {
            address
                .rsplit_once(':')
                .and_then(|(host, _)| host.parse::<std::net::IpAddr>().ok())
        })
}

fn cluster_delivery_private_prefix_for_address(
    address: &str,
) -> Option<ClusterDeliveryPrivatePrefix> {
    match cluster_delivery_address_ip(address)? {
        std::net::IpAddr::V4(ip) => {
            if ip.is_loopback() || ip.is_unspecified() || !(ip.is_private() || ip.is_link_local()) {
                return None;
            }
            let octets = ip.octets();
            Some(ClusterDeliveryPrivatePrefix::V4([
                octets[0], octets[1], octets[2],
            ]))
        }
        std::net::IpAddr::V6(ip) => {
            let first_segment = ip.segments()[0];
            let unique_local = (first_segment & 0xfe00) == 0xfc00;
            let link_local = (first_segment & 0xffc0) == 0xfe80;
            if ip.is_loopback() || ip.is_unspecified() || !(unique_local || link_local) {
                return None;
            }
            let segments = ip.segments();
            Some(ClusterDeliveryPrivatePrefix::V6([
                segments[0],
                segments[1],
                segments[2],
                segments[3],
            ]))
        }
    }
}

fn cluster_delivery_local_private_prefixes(
    members: &[ClusterMemberRecord],
    local_node_id: &NodeId,
) -> Result<ClusterDeliveryPrivatePrefixes, NetworkError> {
    let mut prefixes = ClusterDeliveryPrivatePrefixes::default();
    for endpoint in members
        .iter()
        .filter(|member| member.node_id == *local_node_id)
        .flat_map(|member| member.endpoints.iter())
    {
        let Some(prefix) = cluster_delivery_private_prefix_for_address(&endpoint.address) else {
            continue;
        };
        prefixes.try_insert(prefix)?;
    }
    Ok(prefixes)
}

fn cluster_delivery_endpoint_shares_local_private_prefix(
    endpoint: &ClusterEndpointRecord,
    local_private_prefixes: &ClusterDeliveryPrivatePrefixes,
) -> bool {
    cluster_delivery_private_prefix_for_address(&endpoint.address)
        .is_some_and(|prefix| local_private_prefixes.contains(&prefix))
}

fn cluster_delivery_target_endpoint_hints(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let endpoints =
        cluster_delivery_target_endpoint_hints_from_members(&state.members, target_node_id)?;
    if !endpoints.is_empty() {
        return Ok(endpoints);
    }

    // The daemon already holds the cluster snapshot used to plan this call.
    // Only consult the directory when that resident snapshot cannot provide a
    // usable target hint; decoding the same KV member and endpoint tables on
    // every forwarded RPC adds sub-millisecond wall time before any I/O.
    let members = cluster_delivery_members_for_routing(paths, state)?;
    cluster_delivery_target_endpoint_hints_from_members(&members, target_node_id)
}

fn cluster_delivery_target_endpoint_hints_from_members(
    members: &[ClusterMemberRecord],
    target_node_id: &NodeId,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let mut endpoints = Vec::new();
    let Some(member) = members
        .iter()
        .find(|member| member.node_id == *target_node_id && member.state.is_authorized())
    else {
        return Ok(endpoints);
    };
    for endpoint in member.endpoints.iter().filter(|endpoint| {
        endpoint.node_id == *target_node_id
            && endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
            && cluster_endpoint_can_carry_main_daemon_rpc(endpoint)
    }) {
        crate::object::try_reserve_vec(&mut endpoints, 1, "cluster relay endpoint hints")?;
        endpoints.push(try_clone_route_endpoint(
            endpoint,
            "cluster relay endpoint hints",
        )?);
    }
    Ok(endpoints)
}

#[derive(Debug)]
pub enum ClusterRelaySessionRequestAction {
    DeliverLocal(ObjectRpcRequest),
    Forward,
}

/// Selects local delivery or the next forwarding hop for a canonical session
/// relay without exposing the relay envelope's signed inner fields.
pub fn cluster_relay_session_request_action(
    state: &ClusterBootstrapState,
    request: &ObjectRpcRequest,
) -> Result<ClusterRelaySessionRequestAction, NetworkError> {
    let envelope = RelayEnvelope::from_request(request)?;
    if envelope.cluster_id != state.cluster_id {
        return Err(NetworkError::ClusterMismatch {
            expected: state.cluster_id.clone(),
            actual: envelope.cluster_id,
        });
    }
    if envelope.target_node_id == state.local_node_id {
        Ok(ClusterRelaySessionRequestAction::DeliverLocal(
            envelope.into_inner(),
        ))
    } else {
        Ok(ClusterRelaySessionRequestAction::Forward)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterObjectRpcOptions {
    timeouts: DaemonRpcTimeouts,
    overall_timeout: Option<Duration>,
}

impl ClusterObjectRpcOptions {
    /// Override transport phases without creating a whole-call deadline.
    /// Connect and idle limits are not reinterpreted as operation deadlines;
    /// callers with a domain budget must use `with_overall_timeout`.
    pub fn new(timeouts: DaemonRpcTimeouts) -> Self {
        Self {
            timeouts,
            overall_timeout: None,
        }
    }

    /// Builds routed-call options from the canonical health/deadline policy.
    pub fn from_call_policy(policy: NetworkCallPolicy) -> Self {
        let options = Self::new(policy.health.into());
        match policy.overall_timeout {
            Some(timeout) => options.with_overall_timeout(timeout),
            None => options,
        }
    }

    /// Override the whole-call delivery deadline.
    ///
    /// The deadline covers route planning, signing, connecting, writing, and
    /// receiving the response.
    pub fn with_overall_timeout(mut self, timeout: Duration) -> Self {
        self.overall_timeout = Some(timeout);
        self
    }

    fn requested_overall_timeout(self) -> Option<Duration> {
        self.overall_timeout
    }

    fn session_call_policy(self) -> NetworkCallPolicy {
        let defaults = NetworkHealthPolicy::default();
        NetworkCallPolicy {
            health: NetworkHealthPolicy::new(
                self.timeouts
                    .connect_timeout
                    .unwrap_or(defaults.connect_timeout),
                self.timeouts
                    .idle_timeout
                    .unwrap_or(defaults.no_progress_timeout),
            ),
            overall_timeout: self.overall_timeout,
        }
    }
}

impl From<NetworkCallPolicy> for ClusterObjectRpcOptions {
    fn from(policy: NetworkCallPolicy) -> Self {
        Self::from_call_policy(policy)
    }
}

impl Default for ClusterObjectRpcOptions {
    fn default() -> Self {
        Self {
            timeouts: DaemonRpcTimeouts::default(),
            overall_timeout: None,
        }
    }
}

pub type ClusterSignedObjectRpcFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NetworkError>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClusterSignedObjectRpcObservationKind {
    Success,
    IoFailure,
    HardIoFailure,
    AuthDenied,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterSignedObjectRpcObservation {
    kind: ClusterSignedObjectRpcObservationKind,
    latency_ms: Option<u64>,
}

impl ClusterSignedObjectRpcObservation {
    pub fn success(latency_ms: Option<u64>) -> Self {
        Self {
            kind: ClusterSignedObjectRpcObservationKind::Success,
            latency_ms,
        }
    }

    pub fn io_failure() -> Self {
        Self {
            kind: ClusterSignedObjectRpcObservationKind::IoFailure,
            latency_ms: None,
        }
    }

    /// A failed command attempt that consumed its bounded response budget.
    /// Unlike a background probe miss, this should not retain a Healthy route
    /// through the single-failure grace period.
    pub fn hard_io_failure() -> Self {
        Self {
            kind: ClusterSignedObjectRpcObservationKind::HardIoFailure,
            latency_ms: None,
        }
    }

    pub fn auth_denied() -> Self {
        Self {
            kind: ClusterSignedObjectRpcObservationKind::AuthDenied,
            latency_ms: None,
        }
    }

    pub fn is_success(&self) -> bool {
        matches!(self.kind, ClusterSignedObjectRpcObservationKind::Success)
    }

    pub fn is_io_failure(&self) -> bool {
        matches!(
            self.kind,
            ClusterSignedObjectRpcObservationKind::IoFailure
                | ClusterSignedObjectRpcObservationKind::HardIoFailure
        )
    }

    pub fn is_hard_io_failure(&self) -> bool {
        matches!(
            self.kind,
            ClusterSignedObjectRpcObservationKind::HardIoFailure
        )
    }

    pub fn is_auth_denied(&self) -> bool {
        matches!(self.kind, ClusterSignedObjectRpcObservationKind::AuthDenied)
    }

    pub fn latency_ms(&self) -> Option<u64> {
        self.latency_ms
    }
}

pub trait ClusterSignedObjectRpcAsyncHost: Send {
    fn sign_object_rpc_request_async<'a>(
        &'a mut self,
        request: &'a ObjectRpcRequest,
    ) -> ClusterSignedObjectRpcFuture<'a, Vec<u8>>;

    fn observe_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        _endpoint: &'a ClusterEndpointRecord,
        _observation: ClusterSignedObjectRpcObservation,
    ) -> ClusterSignedObjectRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    /// Returns the already-authenticated multiplexed channel for an endpoint,
    /// when the host owns one. The channel authenticates its physical
    /// connection once; logical calls retain the same object-RPC contract and
    /// bounded body flow control without opening and signing a fresh session.
    ///
    /// Hosts that do not own persistent connections keep using the canonical
    /// one-session-per-stream fallback below.
    fn object_rpc_channel_async<'a>(
        &'a mut self,
        _endpoint: &'a ClusterEndpointRecord,
        _health: NetworkHealthPolicy,
    ) -> ClusterSignedObjectRpcFuture<'a, Option<ObjectRpcChannelClient>> {
        Box::pin(async { Ok(None) })
    }

    /// Opens the transport stream for the canonical body-aware session. Hosts
    /// may override synthetic/local transports; ordinary LAN and runtime
    /// modules use the transport-neutral registry path.
    fn open_object_rpc_session_stream_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        health: NetworkHealthPolicy,
    ) -> ClusterSignedObjectRpcFuture<'a, Box<dyn NetworkDuplexStream>> {
        Box::pin(async move {
            crate::roaming::open_daemon_rpc_stream_to_cluster_endpoint_async(endpoint, health).await
        })
    }
}

fn cluster_signed_endpoint_session_observation(
    result: Result<&ObjectRpcSessionResult, &NetworkError>,
    started: Instant,
) -> Option<ClusterSignedObjectRpcObservation> {
    match result {
        // Completing an authenticated session proves that the immediate endpoint is
        // reachable even when the application response denies the requested operation.
        // Session/link authentication failures arrive through the `Err` branch below.
        Ok(_) => Some(ClusterSignedObjectRpcObservation::success(Some(
            cluster_signed_endpoint_rpc_elapsed_latency_ms(started),
        ))),
        Err(error) => cluster_signed_endpoint_rpc_error_observation(error),
    }
}

fn cluster_authorized_member_public_key(
    state: &ClusterBootstrapState,
    node_id: &NodeId,
) -> Result<Vec<u8>, NetworkError> {
    let member = state
        .members
        .iter()
        .find(|member| &member.node_id == node_id && member.state.is_authorized())
        .ok_or_else(|| NetworkError::UnauthorizedNode {
            node_id: node_id.clone(),
        })?;
    crate::daemon_auth_decode_public_key(&member.node_public_key)
}

fn cluster_paths_data_dir(
    paths: Option<&ClusterPaths>,
) -> Result<sinomo_local::SinomoDataDir, NetworkError> {
    if let Some(paths) = paths {
        let root = paths.root.parent().ok_or_else(|| NetworkError::Io {
            operation: "resolve cluster data directory".to_string(),
            detail: format!(
                "cluster path {} has no parent data directory",
                paths.root.display()
            ),
        })?;
        return Ok(sinomo_local::SinomoDataDir::new(root));
    }
    sinomo_local::SinomoDataDir::from_env_or_home().map_err(|error| NetworkError::Io {
        operation: "resolve object RPC session data directory".to_string(),
        detail: error.to_string(),
    })
}

fn cluster_signed_endpoint_rpc_error_observation(
    error: &NetworkError,
) -> Option<ClusterSignedObjectRpcObservation> {
    match error {
        NetworkError::Io { .. }
        | NetworkError::Timeout { .. }
        | NetworkError::DeliveryFailed { .. } => {
            Some(ClusterSignedObjectRpcObservation::io_failure())
        }
        NetworkError::AuthDenied => Some(ClusterSignedObjectRpcObservation::auth_denied()),
        _ => None,
    }
}

fn cluster_signed_endpoint_rpc_elapsed_latency_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

#[derive(Clone, Copy)]
struct ClusterObjectRpcRouteContext<'a> {
    runtime: &'a tokio::runtime::Handle,
    paths: &'a ClusterPaths,
}

/// Cluster object-RPC client facade.
///
/// `call_target` is the canonical routed API: callers name a target node and
/// provide one size-independent call while the network layer chooses the next
/// hop and manages the full-duplex transfer.
pub struct ClusterObjectRpcClient<'a, H> {
    host: &'a mut H,
    route_context: Option<ClusterObjectRpcRouteContext<'a>>,
}

impl<'a, H: ClusterSignedObjectRpcAsyncHost> ClusterObjectRpcClient<'a, H> {
    /// Build a client for a caller that intentionally has only a concrete endpoint.
    pub fn endpoint_only(host: &'a mut H) -> Self {
        Self {
            host,
            route_context: None,
        }
    }

    /// Build the canonical route-aware cluster object-RPC client.
    pub fn routed(
        host: &'a mut H,
        runtime: &'a tokio::runtime::Handle,
        paths: &'a ClusterPaths,
    ) -> Self {
        Self {
            host,
            route_context: Some(ClusterObjectRpcRouteContext { runtime, paths }),
        }
    }
}

impl<'a, H> ClusterObjectRpcClient<'a, H>
where
    H: ClusterSignedObjectRpcAsyncHost + Send,
{
    /// Runs one body-aware RPC session against a concrete endpoint.
    ///
    /// The request head is signed only after the session descriptor has been
    /// attached. The transport owns chunking, flow control, health checks, and
    /// response-body delivery after this method opens the duplex stream.
    pub async fn call_endpoint(
        &mut self,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        call: ObjectRpcSessionCall,
        options: ClusterObjectRpcOptions,
    ) -> Result<ObjectRpcSessionResult, NetworkError> {
        let started = Instant::now();
        let request_head_bytes = call.request.body.len();
        let result = self
            .call_endpoint_inner(state, endpoint, call, options)
            .await;
        record_network_rpc_metrics("endpoint", started, request_head_bytes, &result);
        result
    }

    async fn call_endpoint_inner(
        &mut self,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        call: ObjectRpcSessionCall,
        options: ClusterObjectRpcOptions,
    ) -> Result<ObjectRpcSessionResult, NetworkError> {
        let expected_public_key = cluster_authorized_member_public_key(state, &endpoint.node_id)?;
        let data_dir = cluster_paths_data_dir(self.route_context.map(|context| context.paths))?;
        self.call_authenticated_endpoint(endpoint, call, options, data_dir, expected_public_key)
            .await
    }

    /// Runs a canonical session against this process's loopback daemon before
    /// cluster bootstrap state exists.
    ///
    /// This path is deliberately limited to loopback addresses. It pins the
    /// responder to the local main-daemon identity on disk; non-loopback calls
    /// must use [`Self::call_endpoint`] and an authorized cluster member.
    pub async fn call_loopback(
        &mut self,
        data_dir: &sinomo_local::SinomoDataDir,
        address: &str,
        call: ObjectRpcSessionCall,
        options: ClusterObjectRpcOptions,
    ) -> Result<ObjectRpcSessionResult, NetworkError> {
        let started = Instant::now();
        let request_head_bytes = call.request.body.len();
        let result = self
            .call_loopback_inner(data_dir, address, call, options)
            .await;
        record_network_rpc_metrics("loopback", started, request_head_bytes, &result);
        result
    }

    async fn call_loopback_inner(
        &mut self,
        data_dir: &sinomo_local::SinomoDataDir,
        address: &str,
        call: ObjectRpcSessionCall,
        options: ClusterObjectRpcOptions,
    ) -> Result<ObjectRpcSessionResult, NetworkError> {
        if !crate::is_loopback_connect_address(address) {
            return Err(NetworkError::Io {
                operation: "open local object RPC session".to_string(),
                detail: format!("non-loopback address `{address}` requires cluster authorization"),
            });
        }
        let expected_public_key = crate::daemon_auth::cached_daemon_signing_public_key(data_dir)?;
        let endpoint_node_id = NodeId::derived(
            "sinomo.network.local-object-rpc-session",
            &expected_public_key,
        );
        let endpoint = crate::endpoint_advertisement::daemon_loopback_endpoint(
            &endpoint_node_id,
            address.to_string(),
            0,
        );
        self.call_authenticated_endpoint(
            &endpoint,
            call,
            options,
            data_dir.clone(),
            expected_public_key,
        )
        .await
    }

    async fn call_authenticated_endpoint(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        call: ObjectRpcSessionCall,
        options: ClusterObjectRpcOptions,
        data_dir: sinomo_local::SinomoDataDir,
        expected_public_key: Vec<u8>,
    ) -> Result<ObjectRpcSessionResult, NetworkError> {
        let timeline = crate::network_timeline_trace_context_for_request(&call.request);
        let perf_enabled = network_perf_trace_enabled();
        let perf_route = perf_enabled.then(|| call.request.route.clone());
        let policy = options.session_call_policy();
        let deadline = ClusterEndpointRpcDeadline::new(policy.overall_timeout);
        let channel_compatible = object_rpc_call_is_channel_compatible(&call);
        let channel_lookup_started = perf_enabled.then(Instant::now);
        let timeline_channel_lookup_started = timeline
            .map(|_| crate::network_timeline_monotonic_ns())
            .unwrap_or(0);
        let (channel, channel_lookup_outcome) = if channel_compatible {
            let lookup_timeout = deadline.timeout_error(
                "lookup multiplexed cluster endpoint object RPC channel",
                NetworkTimeoutPhase::Overall,
                NetworkDeliveryState::DefinitelyNotSent,
            );
            match cluster_object_rpc_call_with_deadline(
                self.host.object_rpc_channel_async(endpoint, policy.health),
                deadline,
                lookup_timeout,
            )
            .await
            {
                Ok(Some(channel)) if channel.is_closed() => {
                    (None, crate::network_timeline_channel_lookup_outcome::CLOSED)
                }
                Ok(Some(channel)) => (
                    Some(channel),
                    crate::network_timeline_channel_lookup_outcome::AVAILABLE,
                ),
                Ok(None) => (None, crate::network_timeline_channel_lookup_outcome::ABSENT),
                Err(error) if deadline.checked_remaining().is_none() => return Err(error),
                Err(_) => (None, crate::network_timeline_channel_lookup_outcome::ERROR),
            }
        } else {
            (
                None,
                crate::network_timeline_channel_lookup_outcome::INCOMPATIBLE,
            )
        };
        record_route_channel_lookup_timeline(
            timeline,
            timeline_channel_lookup_started,
            channel.as_ref(),
            channel_lookup_outcome,
            u32::from(channel_compatible),
        );
        if let (Some(started), Some(route)) = (channel_lookup_started, perf_route.as_deref()) {
            tracing::info!(
                target: "sinomo.network.perf",
                stage = "channel_lookup",
                route,
                target_node_id = %endpoint.node_id,
                available = channel.is_some(),
                compatible = channel_compatible,
                elapsed_us = network_perf_trace_elapsed_us(started),
            );
        }
        if let Some(channel) = channel {
            let started = Instant::now();
            let result = cluster_object_rpc_channel_call_with_health(
                channel,
                call,
                policy.health,
                deadline,
                "multiplexed cluster endpoint object RPC",
            )
            .await;
            if let Some(route) = perf_route.as_deref() {
                tracing::info!(
                    target: "sinomo.network.perf",
                    stage = "multiplexed_channel_total",
                    route,
                    target_node_id = %endpoint.node_id,
                    endpoint = %endpoint.address,
                    relay = false,
                    ok = result.is_ok(),
                    elapsed_us = network_perf_trace_elapsed_us(started),
                );
            }
            // The host may return one node-level pooled/tunnel channel for
            // several advertised endpoints. Its result proves that channel,
            // not the concrete endpoint record used to discover it.
            let result = result?;
            if !result.response_origin_verified() {
                return Err(NetworkError::AuthDenied);
            }
            result.response.error_detail()?;
            return Ok(result);
        }
        let open_timeout = deadline.timeout_error(
            "cluster endpoint object RPC session",
            NetworkTimeoutPhase::Overall,
            NetworkDeliveryState::DefinitelyNotSent,
        );
        let open_started = perf_enabled.then(Instant::now);
        let metric_open_started = Instant::now();
        let timeline_open_started = timeline
            .map(|_| crate::network_timeline_monotonic_ns())
            .unwrap_or(0);
        let open_result = cluster_object_rpc_call_with_deadline(
            self.host
                .open_object_rpc_session_stream_async(endpoint, policy.health),
            deadline,
            open_timeout,
        )
        .await;
        record_network_endpoint_open(
            "endpoint",
            &endpoint.transport,
            metric_open_started,
            open_result.is_ok(),
        );
        record_route_timeline_stage(
            timeline,
            crate::network_timeline_stage::ENDPOINT_OPEN,
            timeline_open_started,
            open_result.is_ok(),
            0,
        );
        if let (Some(started), Some(route)) = (open_started, perf_route.as_deref()) {
            tracing::info!(
                target: "sinomo.network.perf",
                stage = "endpoint_open",
                route,
                target_node_id = %endpoint.node_id,
                endpoint = %endpoint.address,
                transport = ?endpoint.transport,
                relay = false,
                ok = open_result.is_ok(),
                elapsed_us = network_perf_trace_elapsed_us(started),
            );
        }
        let stream = match open_result {
            Ok(stream) => stream,
            Err(error) => {
                if let Some(observation) = cluster_signed_endpoint_rpc_error_observation(&error) {
                    let _ = self
                        .host
                        .observe_signed_endpoint_rpc_async(endpoint, observation)
                        .await;
                }
                return Err(error);
            }
        };
        let prepare_started = perf_enabled.then(Instant::now);
        let prepared_result = call.prepare_for_signing();
        if let (Some(started), Some(route)) = (prepare_started, perf_route.as_deref()) {
            tracing::info!(
                target: "sinomo.network.perf",
                stage = "request_prepare",
                route,
                target_node_id = %endpoint.node_id,
                relay = false,
                ok = prepared_result.is_ok(),
                elapsed_us = network_perf_trace_elapsed_us(started),
            );
        }
        let prepared = prepared_result?;
        let sign_started = perf_enabled.then(Instant::now);
        let timeline_sign_started = timeline
            .map(|_| crate::network_timeline_monotonic_ns())
            .unwrap_or(0);
        let request_auth_result = cluster_object_rpc_sign_with_deadline(
            self.host.sign_object_rpc_request_async(&prepared.request),
            deadline,
        )
        .await;
        record_route_timeline_stage(
            timeline,
            crate::network_timeline_stage::REQUEST_SIGN,
            timeline_sign_started,
            request_auth_result.is_ok(),
            0,
        );
        if let (Some(started), Some(route)) = (sign_started, perf_route.as_deref()) {
            tracing::info!(
                target: "sinomo.network.perf",
                stage = "request_sign",
                route,
                target_node_id = %endpoint.node_id,
                signature_kind = "hop",
                relay = false,
                ok = request_auth_result.is_ok(),
                elapsed_us = network_perf_trace_elapsed_us(started),
            );
        }
        let request_auth = request_auth_result?;
        let Some(remaining) = deadline.checked_remaining() else {
            return Err(deadline.timeout_error(
                "cluster endpoint object RPC session",
                NetworkTimeoutPhase::Overall,
                NetworkDeliveryState::DefinitelyNotSent,
            ));
        };
        let session_policy = NetworkCallPolicy {
            health: policy.health,
            overall_timeout: remaining,
        };
        let started = Instant::now();
        let result = start_object_rpc_client_session(
            stream,
            prepared,
            request_auth,
            data_dir,
            expected_public_key.clone(),
            expected_public_key,
            session_policy,
        )
        .await;
        if let Some(route) = perf_route.as_deref() {
            tracing::info!(
                target: "sinomo.network.perf",
                stage = "canonical_session_total",
                route,
                target_node_id = %endpoint.node_id,
                endpoint = %endpoint.address,
                relay = false,
                ok = result.is_ok(),
                elapsed_us = network_perf_trace_elapsed_us(started),
            );
        }
        if let Some(observation) =
            cluster_signed_endpoint_session_observation(result.as_ref(), started)
        {
            let _ = self
                .host
                .observe_signed_endpoint_rpc_async(endpoint, observation)
                .await;
        }
        let result = result?;
        if !result.response_origin_verified() {
            return Err(NetworkError::AuthDenied);
        }
        result.response.error_detail()?;
        Ok(result)
    }

    pub async fn call_target(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        call: ObjectRpcSessionCall,
        options: ClusterObjectRpcOptions,
    ) -> Result<ObjectRpcSessionResult, NetworkError> {
        let started = Instant::now();
        let request_head_bytes = call.request.body.len();
        let result = self
            .call_target_inner(state, target_node_id, call, options)
            .await;
        record_network_rpc_metrics("routed", started, request_head_bytes, &result);
        result
    }

    async fn call_target_inner(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        call: ObjectRpcSessionCall,
        options: ClusterObjectRpcOptions,
    ) -> Result<ObjectRpcSessionResult, NetworkError> {
        let Some(context) = self.route_context else {
            return Err(NetworkError::Io {
                operation: "cluster target object RPC session".to_string(),
                detail: "cluster target RPC session requires a route context".to_string(),
            });
        };
        let route_timeout = options.requested_overall_timeout();
        let deadline = ClusterEndpointRpcDeadline::new(route_timeout);
        let timeout_error = cluster_target_rpc_timeout_error(
            target_node_id,
            route_timeout,
            NetworkDeliveryState::PossiblyApplied,
            &[],
        );
        cluster_object_rpc_call_with_deadline(
            self.call_target_from_paths(
                context.runtime,
                context.paths,
                state,
                target_node_id,
                call,
                options,
                deadline,
            ),
            deadline,
            timeout_error,
        )
        .await
    }

    /// Forwards an already authenticated relay session by one hop.
    ///
    /// The origin-signed inner request and its session descriptor are preserved
    /// on the final hop. Intermediate hops create a fresh outer relay session,
    /// while the incoming body is piped through without application-level
    /// sharding or whole-body buffering.
    pub async fn forward_relay(
        &mut self,
        state: &ClusterBootstrapState,
        request: crate::ObjectRpcSessionRequest,
        options: ClusterObjectRpcOptions,
    ) -> Result<ObjectRpcSessionResult, NetworkError> {
        let started = Instant::now();
        let request_head_bytes = request.request.body.len();
        let result = self.forward_relay_inner(state, request, options).await;
        record_network_rpc_metrics("relay", started, request_head_bytes, &result);
        result
    }

    async fn forward_relay_inner(
        &mut self,
        state: &ClusterBootstrapState,
        request: crate::ObjectRpcSessionRequest,
        options: ClusterObjectRpcOptions,
    ) -> Result<ObjectRpcSessionResult, NetworkError> {
        let Some(context) = self.route_context else {
            return Err(NetworkError::Io {
                operation: "forward cluster relay session".to_string(),
                detail: "relay forwarding requires a route context".to_string(),
            });
        };
        let envelope = RelayEnvelope::from_request(&request.request)?;
        let timeline = crate::network_timeline_trace_context_for_request(&envelope.inner);
        let perf_enabled = network_perf_trace_enabled();
        let perf_route = perf_enabled.then(|| envelope.inner.route.clone());
        if envelope.cluster_id != state.cluster_id {
            return Err(NetworkError::ClusterMismatch {
                expected: state.cluster_id.clone(),
                actual: envelope.cluster_id.clone(),
            });
        }
        if envelope
            .visited
            .iter()
            .any(|node| node == &state.local_node_id)
        {
            return ObjectRpcResponse::encode_error(
                CLUSTER_RELAY_ROUTE,
                format!(
                    "relay dropped: relay loop: {} already on the path",
                    state.local_node_id
                ),
            )
            .map(ObjectRpcSessionResult::value);
        }
        if envelope.ttl == 0 {
            return ObjectRpcResponse::encode_error(
                CLUSTER_RELAY_ROUTE,
                "relay dropped: relay ttl exhausted",
            )
            .map(ObjectRpcSessionResult::value);
        }

        let deadline = ClusterEndpointRpcDeadline::new(relay_envelope_hop_timeout(&envelope));
        let mut failed_edges = ClusterDeliveryFailedEdges::default();
        let mut failures = Vec::new();
        let max_route_attempts = state.members.len().saturating_mul(2).max(1) + 2;
        let mut incoming_body = Some(request.body);
        let data_dir = cluster_paths_data_dir(Some(context.paths))?;
        let response_origin_public_key =
            cluster_authorized_member_public_key(state, &envelope.target_node_id)?;

        for _ in 0..max_route_attempts {
            let timeline_route_plan_started = timeline
                .map(|_| crate::network_timeline_monotonic_ns())
                .unwrap_or(0);
            let plan_timeout = deadline.timeout_error(
                "plan cluster relay object RPC session",
                NetworkTimeoutPhase::Overall,
                NetworkDeliveryState::DefinitelyNotSent,
            );
            let route_plan_result = cluster_object_rpc_call_with_deadline(
                cluster_relay_forward_plan(
                    context.runtime,
                    context.paths,
                    state,
                    &state.local_node_id,
                    &envelope,
                    failed_edges.try_clone()?,
                    timeline,
                ),
                deadline,
                plan_timeout,
            )
            .await;
            record_route_timeline_stage(
                timeline,
                crate::network_timeline_stage::ROUTE_PLAN,
                timeline_route_plan_started,
                route_plan_result.as_ref().is_ok_and(Option::is_some),
                0,
            );
            let route_plan = route_plan_result?;
            let Some(route_plan) = route_plan else {
                break;
            };
            let timeline_next_hop_key_started = timeline
                .map(|_| crate::network_timeline_monotonic_ns())
                .unwrap_or(0);
            let next_hop_public_key_result =
                cluster_authorized_member_public_key(state, &route_plan.next_hop);
            record_route_timeline_stage(
                timeline,
                crate::network_timeline_stage::ROUTE_NEXT_HOP_KEY,
                timeline_next_hop_key_started,
                next_hop_public_key_result.is_ok(),
                0,
            );
            let next_hop_public_key = next_hop_public_key_result?;
            for endpoint in route_plan.endpoints {
                let Some(_) = deadline.checked_remaining() else {
                    break;
                };
                let health = options.session_call_policy().health;
                let final_hop = route_plan.next_hop == envelope.target_node_id;
                let channel_lookup_started = perf_enabled.then(Instant::now);
                let timeline_channel_lookup_started = timeline
                    .map(|_| crate::network_timeline_monotonic_ns())
                    .unwrap_or(0);
                let lookup_timeout = deadline.timeout_error(
                    "lookup forwarded multiplexed cluster relay object RPC channel",
                    NetworkTimeoutPhase::Overall,
                    NetworkDeliveryState::DefinitelyNotSent,
                );
                let (channel, channel_lookup_outcome) = match cluster_object_rpc_call_with_deadline(
                    self.host.object_rpc_channel_async(&endpoint, health),
                    deadline,
                    lookup_timeout,
                )
                .await
                {
                    Ok(Some(channel)) if channel.is_closed() => {
                        (None, crate::network_timeline_channel_lookup_outcome::CLOSED)
                    }
                    Ok(Some(channel)) => (
                        Some(channel),
                        crate::network_timeline_channel_lookup_outcome::AVAILABLE,
                    ),
                    Ok(None) => (None, crate::network_timeline_channel_lookup_outcome::ABSENT),
                    Err(error) if deadline.checked_remaining().is_none() => return Err(error),
                    Err(_) => (None, crate::network_timeline_channel_lookup_outcome::ERROR),
                };
                if let (Some(started), Some(route)) =
                    (channel_lookup_started, perf_route.as_deref())
                {
                    tracing::info!(
                        target: "sinomo.network.perf",
                        stage = "channel_lookup",
                        route,
                        target_node_id = %envelope.target_node_id,
                        next_hop_node_id = %route_plan.next_hop,
                        endpoint = %endpoint.address,
                        relay = true,
                        forwarding = true,
                        final_hop,
                        available = channel.is_some(),
                        elapsed_us = network_perf_trace_elapsed_us(started),
                    );
                }
                record_route_channel_lookup_timeline(
                    timeline,
                    timeline_channel_lookup_started,
                    channel.as_ref(),
                    channel_lookup_outcome,
                    u32::from(final_hop),
                );
                let stream = if channel.is_none() {
                    let open_timeout = deadline.timeout_error(
                        "open cluster relay object RPC session",
                        NetworkTimeoutPhase::Overall,
                        NetworkDeliveryState::DefinitelyNotSent,
                    );
                    let open_started = perf_enabled.then(Instant::now);
                    let metric_open_started = Instant::now();
                    let timeline_open_started = timeline
                        .map(|_| crate::network_timeline_monotonic_ns())
                        .unwrap_or(0);
                    let open_result = cluster_object_rpc_call_with_deadline(
                        self.host
                            .open_object_rpc_session_stream_async(&endpoint, health),
                        deadline,
                        open_timeout,
                    )
                    .await;
                    record_network_endpoint_open(
                        "relay",
                        &endpoint.transport,
                        metric_open_started,
                        open_result.is_ok(),
                    );
                    record_route_timeline_stage(
                        timeline,
                        crate::network_timeline_stage::ENDPOINT_OPEN,
                        timeline_open_started,
                        open_result.is_ok(),
                        u32::from(final_hop),
                    );
                    if let (Some(started), Some(route)) = (open_started, perf_route.as_deref()) {
                        tracing::info!(
                            target: "sinomo.network.perf",
                            stage = "endpoint_open",
                            route,
                            target_node_id = %envelope.target_node_id,
                            next_hop_node_id = %route_plan.next_hop,
                            endpoint = %endpoint.address,
                            transport = ?endpoint.transport,
                            relay = true,
                            forwarding = true,
                            ok = open_result.is_ok(),
                            elapsed_us = network_perf_trace_elapsed_us(started),
                        );
                    }
                    match open_result {
                        Ok(stream) => Some(stream),
                        Err(error) => {
                            if let Some(observation) =
                                cluster_signed_endpoint_rpc_error_observation(&error)
                            {
                                let _ = self
                                    .host
                                    .observe_signed_endpoint_rpc_async(&endpoint, observation)
                                    .await;
                            }
                            if deadline.checked_remaining().is_none() {
                                return Err(error);
                            }
                            failures.push(format!(
                                "{} ({:?}) open failed: {error}",
                                endpoint.address, endpoint.transport
                            ));
                            continue;
                        }
                    }
                } else {
                    None
                };
                let body = incoming_body.take().ok_or_else(|| {
                    NetworkError::outcome_unknown(
                        "retry cluster relay session",
                        envelope.inner.operation_id(),
                        "incoming relay body is a one-pass stream and was already dispatched",
                    )
                })?;
                let body = ObjectRpcBodySource::from_incoming_chunk_stream(body);
                let prepare_started = perf_enabled.then(Instant::now);
                let timeline_prepare_started = timeline
                    .map(|_| crate::network_timeline_monotonic_ns())
                    .unwrap_or(0);
                let Some(remaining) = deadline.checked_remaining() else {
                    return Err(deadline.timeout_error(
                        "forward cluster relay object RPC session",
                        NetworkTimeoutPhase::Overall,
                        NetworkDeliveryState::DefinitelyNotSent,
                    ));
                };
                let policy = NetworkCallPolicy {
                    health,
                    overall_timeout: remaining,
                };
                let started = Instant::now();
                let used_channel = channel.is_some();
                metrics::counter!("sinomo.network.rpc.attempts", "path" => "relay").increment(1);
                let result = if let Some(channel) = channel {
                    let channel_call = if final_hop {
                        // The inner origin signature remains attached. The
                        // authenticated channel validates the transferred body
                        // contract at the receiver without another hop proof.
                        ObjectRpcSessionCall::new(envelope.inner.clone(), body)
                    } else {
                        let mut forwarded = envelope.clone();
                        forwarded.ttl = forwarded.ttl.saturating_sub(1);
                        forwarded.visited.push(state.local_node_id.clone());
                        forwarded.max_hop_timeout_ms =
                            cluster_relay_forward_remaining_timeout_ms(deadline).flatten();
                        ObjectRpcSessionCall::new(forwarded.into_request()?, body)
                    };
                    if let (Some(started), Some(route)) = (prepare_started, perf_route.as_deref()) {
                        tracing::info!(
                            target: "sinomo.network.perf",
                            stage = "request_prepare",
                            route,
                            target_node_id = %envelope.target_node_id,
                            next_hop_node_id = %route_plan.next_hop,
                            relay = true,
                            forwarding = true,
                            final_hop,
                            channel = true,
                            ok = true,
                            elapsed_us = network_perf_trace_elapsed_us(started),
                        );
                    }
                    record_route_timeline_stage(
                        timeline,
                        crate::network_timeline_stage::RELAY_PREPARE,
                        timeline_prepare_started,
                        true,
                        u32::from(final_hop),
                    );
                    let result = cluster_object_rpc_channel_call_with_health(
                        channel,
                        channel_call,
                        health,
                        deadline,
                        "multiplexed forwarded cluster relay object RPC",
                    )
                    .await;
                    if let Some(route) = perf_route.as_deref() {
                        tracing::info!(
                            target: "sinomo.network.perf",
                            stage = "multiplexed_channel_total",
                            route,
                            target_node_id = %envelope.target_node_id,
                            next_hop_node_id = %route_plan.next_hop,
                            endpoint = %endpoint.address,
                            relay = true,
                            forwarding = true,
                            final_hop,
                            ok = result.is_ok(),
                            elapsed_us = network_perf_trace_elapsed_us(started),
                        );
                    }
                    result
                } else {
                    let (prepared, request_auth) = if final_hop {
                        ObjectRpcSessionCall::new(envelope.inner.clone(), body)
                            .prepare_from_signed_request()?
                    } else {
                        let mut forwarded = envelope.clone();
                        forwarded.ttl = forwarded.ttl.saturating_sub(1);
                        forwarded.visited.push(state.local_node_id.clone());
                        forwarded.max_hop_timeout_ms =
                            cluster_relay_forward_remaining_timeout_ms(deadline).flatten();
                        let prepared = ObjectRpcSessionCall::new(forwarded.into_request()?, body)
                            .prepare_for_signing()?;
                        let sign_started = perf_enabled.then(Instant::now);
                        let timeline_sign_started = timeline
                            .map(|_| crate::network_timeline_monotonic_ns())
                            .unwrap_or(0);
                        let request_auth_result = cluster_object_rpc_sign_with_deadline(
                            self.host.sign_object_rpc_request_async(&prepared.request),
                            deadline,
                        )
                        .await;
                        record_route_timeline_stage(
                            timeline,
                            crate::network_timeline_stage::REQUEST_SIGN,
                            timeline_sign_started,
                            request_auth_result.is_ok(),
                            0,
                        );
                        if let (Some(started), Some(route)) = (sign_started, perf_route.as_deref())
                        {
                            tracing::info!(
                                target: "sinomo.network.perf",
                                stage = "request_sign",
                                route,
                                target_node_id = %envelope.target_node_id,
                                next_hop_node_id = %route_plan.next_hop,
                                signature_kind = "relay_hop",
                                relay = true,
                                forwarding = true,
                                ok = request_auth_result.is_ok(),
                                elapsed_us = network_perf_trace_elapsed_us(started),
                            );
                        }
                        let request_auth = request_auth_result?;
                        (prepared, request_auth)
                    };
                    if let (Some(started), Some(route)) = (prepare_started, perf_route.as_deref()) {
                        tracing::info!(
                            target: "sinomo.network.perf",
                            stage = "request_prepare",
                            route,
                            target_node_id = %envelope.target_node_id,
                            next_hop_node_id = %route_plan.next_hop,
                            relay = true,
                            forwarding = true,
                            final_hop,
                            channel = false,
                            ok = true,
                            elapsed_us = network_perf_trace_elapsed_us(started),
                        );
                    }
                    record_route_timeline_stage(
                        timeline,
                        crate::network_timeline_stage::RELAY_PREPARE,
                        timeline_prepare_started,
                        true,
                        u32::from(final_hop),
                    );
                    let result = start_object_rpc_client_session(
                        stream.expect("canonical stream exists without a channel"),
                        prepared,
                        request_auth,
                        data_dir.clone(),
                        next_hop_public_key,
                        response_origin_public_key.clone(),
                        policy,
                    )
                    .await;
                    if let Some(route) = perf_route.as_deref() {
                        tracing::info!(
                            target: "sinomo.network.perf",
                            stage = "canonical_session_total",
                            route,
                            target_node_id = %envelope.target_node_id,
                            next_hop_node_id = %route_plan.next_hop,
                            endpoint = %endpoint.address,
                            relay = true,
                            forwarding = true,
                            final_hop,
                            ok = result.is_ok(),
                            elapsed_us = network_perf_trace_elapsed_us(started),
                        );
                    }
                    result
                };
                // A canonical stream is physically tied to `endpoint`; a
                // pooled channel can be synthetic or shared by every endpoint
                // for the next-hop node and has no endpoint provenance.
                if !used_channel
                    && let Some(observation) =
                        cluster_signed_endpoint_session_observation(result.as_ref(), started)
                {
                    let _ = self
                        .host
                        .observe_signed_endpoint_rpc_async(&endpoint, observation)
                        .await;
                }
                return match result {
                    Ok(mut result) => {
                        validate_and_normalize_forwarded_relay_result(
                            &envelope,
                            used_channel,
                            &mut result,
                        )?;
                        Ok(result)
                    }
                    Err(error) => cluster_relay_session_forward_failure_result(&envelope, &error),
                };
            }
            failed_edges.try_insert(&state.local_node_id, &route_plan.next_hop)?;
        }

        ObjectRpcResponse::encode_error(
            CLUSTER_RELAY_ROUTE,
            format!(
                "relay dropped: no reachable route from {} to {}{}",
                state.local_node_id,
                envelope.target_node_id,
                if failures.is_empty() {
                    String::new()
                } else {
                    format!(": {}", failures.join("; "))
                }
            ),
        )
        .map(ObjectRpcSessionResult::value)
    }

    async fn call_target_from_paths(
        &mut self,
        runtime: &tokio::runtime::Handle,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        call: ObjectRpcSessionCall,
        options: ClusterObjectRpcOptions,
        deadline: ClusterEndpointRpcDeadline,
    ) -> Result<ObjectRpcSessionResult, NetworkError> {
        let timeline = crate::network_timeline_trace_context_for_request(&call.request);
        let perf_enabled = network_perf_trace_enabled();
        let perf_route = perf_enabled.then(|| call.request.route.clone());
        let mut failed_edges = ClusterDeliveryFailedEdges::default();
        let mut failures = Vec::new();
        let mut send_attempts = 0_usize;
        let mut possibly_applied = false;
        let max_route_attempts = state.members.len().saturating_mul(2).max(1) + 2;
        let request_retry_safe = call.request.is_response_retry_safe();
        let operation_id = call.request.operation_id().map(str::to_string);
        let channel_compatible = object_rpc_call_is_channel_compatible(&call);
        let replayable_call = call.try_clone_replayable();
        let mut one_pass_call = Some(call);
        let data_dir = cluster_paths_data_dir(Some(paths))?;
        let response_origin_public_key =
            cluster_authorized_member_public_key(state, target_node_id)?;

        'route_attempts: for _ in 0..max_route_attempts {
            let route_plan_started_ns = timeline
                .map(|_| crate::network_timeline_monotonic_ns())
                .unwrap_or(0);
            let route_plan = cluster_delivery_route_plan_async_with_timeline(
                runtime,
                paths,
                state,
                &state.local_node_id,
                target_node_id,
                failed_edges.try_clone()?,
                Vec::new(),
                timeline,
            )
            .await;
            record_route_timeline_stage(
                timeline,
                crate::network_timeline_stage::ROUTE_PLAN,
                route_plan_started_ns,
                route_plan.as_ref().is_ok_and(Option::is_some),
                u32::try_from(send_attempts).unwrap_or(u32::MAX),
            );
            let Some(route_plan) = route_plan? else {
                break;
            };
            if route_plan.endpoints.is_empty() {
                failures.push(format!("{}: no usable route", route_plan.next_hop));
                failed_edges.try_insert(&state.local_node_id, &route_plan.next_hop)?;
                continue;
            }

            let timeline_next_hop_key_started = timeline
                .map(|_| crate::network_timeline_monotonic_ns())
                .unwrap_or(0);
            let next_hop_public_key_result =
                cluster_authorized_member_public_key(state, &route_plan.next_hop);
            record_route_timeline_stage(
                timeline,
                crate::network_timeline_stage::ROUTE_NEXT_HOP_KEY,
                timeline_next_hop_key_started,
                next_hop_public_key_result.is_ok(),
                0,
            );
            let next_hop_public_key = next_hop_public_key_result?;
            let timeline_target_hints_started = timeline
                .map(|_| crate::network_timeline_monotonic_ns())
                .unwrap_or(0);
            let relay_target_endpoints_result = if &route_plan.next_hop == target_node_id {
                Ok(Vec::new())
            } else {
                cluster_delivery_target_endpoint_hints(paths, state, target_node_id)
            };
            record_route_timeline_stage(
                timeline,
                crate::network_timeline_stage::ROUTE_TARGET_HINTS,
                timeline_target_hints_started,
                relay_target_endpoints_result.is_ok(),
                relay_target_endpoints_result
                    .as_ref()
                    .map_or(0, |endpoints| {
                        u32::try_from(endpoints.len()).unwrap_or(u32::MAX)
                    }),
            );
            let relay_target_endpoints = relay_target_endpoints_result?;
            // The routing table stores endpoints in attempt order and its
            // cache mark already tracks the same connectivity generations.
            // Re-reading and cloning the graph here made every successful RPC
            // validate the route graph twice. A failed endpoint still records
            // connectivity below, invalidating the table before a later plan.
            let route_endpoints = route_plan.endpoints;

            for endpoint in route_endpoints {
                let Some(remaining) = deadline.checked_remaining() else {
                    return Err(deadline.timeout_error(
                        "cluster target object RPC session",
                        NetworkTimeoutPhase::Overall,
                        NetworkDeliveryState::PossiblyApplied,
                    ));
                };
                let policy = NetworkCallPolicy {
                    health: options.session_call_policy().health,
                    overall_timeout: remaining,
                };
                let final_hop = &route_plan.next_hop == target_node_id;
                // A persistent channel lookup never consumes the call. If no
                // authenticated channel exists, dial the canonical stream
                // before taking ownership of a one-pass body so connect
                // failures remain safely retryable.
                let channel_lookup_started = perf_enabled.then(Instant::now);
                let timeline_channel_lookup_started = timeline
                    .map(|_| crate::network_timeline_monotonic_ns())
                    .unwrap_or(0);
                let (channel, channel_lookup_outcome) = if channel_compatible {
                    let lookup_timeout = deadline.timeout_error(
                        "lookup multiplexed cluster target object RPC channel",
                        NetworkTimeoutPhase::Overall,
                        NetworkDeliveryState::DefinitelyNotSent,
                    );
                    match cluster_object_rpc_call_with_deadline(
                        self.host.object_rpc_channel_async(&endpoint, policy.health),
                        deadline,
                        lookup_timeout,
                    )
                    .await
                    {
                        Ok(Some(channel)) if channel.is_closed() => {
                            (None, crate::network_timeline_channel_lookup_outcome::CLOSED)
                        }
                        Ok(Some(channel)) => (
                            Some(channel),
                            crate::network_timeline_channel_lookup_outcome::AVAILABLE,
                        ),
                        Ok(None) => (None, crate::network_timeline_channel_lookup_outcome::ABSENT),
                        Err(error) if deadline.checked_remaining().is_none() => return Err(error),
                        Err(_) => (None, crate::network_timeline_channel_lookup_outcome::ERROR),
                    }
                } else {
                    (
                        None,
                        crate::network_timeline_channel_lookup_outcome::INCOMPATIBLE,
                    )
                };
                record_route_channel_lookup_timeline(
                    timeline,
                    timeline_channel_lookup_started,
                    channel.as_ref(),
                    channel_lookup_outcome,
                    u32::try_from(send_attempts).unwrap_or(u32::MAX),
                );
                if let (Some(started), Some(route)) =
                    (channel_lookup_started, perf_route.as_deref())
                {
                    tracing::info!(
                        target: "sinomo.network.perf",
                        stage = "channel_lookup",
                        route,
                        target_node_id = %target_node_id,
                        next_hop_node_id = %route_plan.next_hop,
                        endpoint = %endpoint.address,
                        relay = !final_hop,
                        available = channel.is_some(),
                        compatible = channel_compatible,
                        elapsed_us = network_perf_trace_elapsed_us(started),
                    );
                }
                let stream = if channel.is_none() {
                    let open_started = perf_enabled.then(Instant::now);
                    let metric_open_started = Instant::now();
                    let timeline_open_started = timeline
                        .map(|_| crate::network_timeline_monotonic_ns())
                        .unwrap_or(0);
                    let open_result = self
                        .host
                        .open_object_rpc_session_stream_async(&endpoint, policy.health)
                        .await;
                    record_network_endpoint_open(
                        "routed",
                        &endpoint.transport,
                        metric_open_started,
                        open_result.is_ok(),
                    );
                    record_route_timeline_stage(
                        timeline,
                        crate::network_timeline_stage::ENDPOINT_OPEN,
                        timeline_open_started,
                        open_result.is_ok(),
                        u32::from(final_hop),
                    );
                    if let (Some(started), Some(route)) = (open_started, perf_route.as_deref()) {
                        tracing::info!(
                            target: "sinomo.network.perf",
                            stage = "endpoint_open",
                            route,
                            target_node_id = %target_node_id,
                            next_hop_node_id = %route_plan.next_hop,
                            endpoint = %endpoint.address,
                            transport = ?endpoint.transport,
                            relay = !final_hop,
                            ok = open_result.is_ok(),
                            elapsed_us = network_perf_trace_elapsed_us(started),
                        );
                    }
                    match open_result {
                        Ok(stream) => Some(stream),
                        Err(error) => {
                            if let Some(observation) =
                                cluster_signed_endpoint_rpc_error_observation(&error)
                            {
                                let _ = self
                                    .host
                                    .observe_signed_endpoint_rpc_async(&endpoint, observation)
                                    .await;
                            }
                            failures.push(format!(
                                "{} ({:?}) open failed: {error}",
                                endpoint.address, endpoint.transport
                            ));
                            continue;
                        }
                    }
                } else {
                    None
                };
                let mut attempt_call = if let Some(template) = replayable_call.as_ref() {
                    template
                        .try_clone_replayable()
                        .expect("replayable template clones")
                } else if let Some(call) = one_pass_call.take() {
                    call
                } else {
                    return Err(NetworkError::delivery_failed(
                        "retry cluster object RPC session",
                        NetworkDeliveryState::PossiblyApplied,
                        "the one-pass request body was already consumed by a prior session attempt",
                    ));
                };
                if !final_hop {
                    attempt_call.request.metadata.insert(
                        CLUSTER_RELAY_ORIGIN_NODE_ID_METADATA_KEY.to_string(),
                        state.local_node_id.to_string(),
                    );
                }

                let prepare_started = perf_enabled.then(Instant::now);
                let timeline_prepare_started = timeline
                    .map(|_| crate::network_timeline_monotonic_ns())
                    .unwrap_or(0);
                send_attempts = send_attempts.saturating_add(1);
                metrics::counter!("sinomo.network.rpc.attempts", "path" => "routed").increment(1);
                let started = Instant::now();
                let used_channel = channel.is_some();
                let result = if let Some(channel) = channel {
                    let channel_call = if final_hop {
                        attempt_call
                    } else {
                        // Relays keep one origin signature so the final node
                        // preserves end-to-end caller authority. The physical
                        // channel already authenticates every hop, so the
                        // outer relay head needs no per-call signature.
                        let prepared_inner = attempt_call.prepare_for_signing()?;
                        let inner_sign_started = perf_enabled.then(Instant::now);
                        let timeline_inner_sign_started = timeline
                            .map(|_| crate::network_timeline_monotonic_ns())
                            .unwrap_or(0);
                        let inner_auth_result = self
                            .host
                            .sign_object_rpc_request_async(&prepared_inner.request)
                            .await;
                        record_route_timeline_stage(
                            timeline,
                            crate::network_timeline_stage::REQUEST_SIGN,
                            timeline_inner_sign_started,
                            inner_auth_result.is_ok(),
                            1,
                        );
                        if let (Some(started), Some(route)) =
                            (inner_sign_started, perf_route.as_deref())
                        {
                            tracing::info!(
                                target: "sinomo.network.perf",
                                stage = "request_sign",
                                route,
                                target_node_id = %target_node_id,
                                next_hop_node_id = %route_plan.next_hop,
                                signature_kind = "origin_inner",
                                relay = true,
                                ok = inner_auth_result.is_ok(),
                                elapsed_us = network_perf_trace_elapsed_us(started),
                            );
                        }
                        let inner_auth = inner_auth_result?;
                        let outer = relay_request_for_signed_rpc_with_max_hop_timeout(
                            &state.cluster_id,
                            target_node_id,
                            &prepared_inner.request,
                            inner_auth,
                            std::iter::once(state.local_node_id.clone()),
                            &route_plan.path,
                            &relay_target_endpoints,
                            remaining,
                        )?;
                        ObjectRpcSessionCall::new(outer, prepared_inner.body)
                    };
                    if let (Some(started), Some(route)) = (prepare_started, perf_route.as_deref()) {
                        tracing::info!(
                            target: "sinomo.network.perf",
                            stage = "request_prepare",
                            route,
                            target_node_id = %target_node_id,
                            next_hop_node_id = %route_plan.next_hop,
                            relay = !final_hop,
                            channel = true,
                            ok = true,
                            elapsed_us = network_perf_trace_elapsed_us(started),
                        );
                    }
                    if !final_hop {
                        record_route_timeline_stage(
                            timeline,
                            crate::network_timeline_stage::RELAY_PREPARE,
                            timeline_prepare_started,
                            true,
                            1,
                        );
                    }
                    let result = cluster_object_rpc_channel_call_with_health(
                        channel,
                        channel_call,
                        policy.health,
                        deadline,
                        "multiplexed cluster target object RPC",
                    )
                    .await;
                    if let Some(route) = perf_route.as_deref() {
                        tracing::info!(
                            target: "sinomo.network.perf",
                            stage = "multiplexed_channel_total",
                            route,
                            target_node_id = %target_node_id,
                            next_hop_node_id = %route_plan.next_hop,
                            endpoint = %endpoint.address,
                            relay = !final_hop,
                            ok = result.is_ok(),
                            elapsed_us = network_perf_trace_elapsed_us(started),
                        );
                    }
                    result
                } else {
                    let prepared = if final_hop {
                        attempt_call.prepare_for_signing()?
                    } else {
                        let prepared_inner = attempt_call.prepare_for_signing()?;
                        let inner_sign_started = perf_enabled.then(Instant::now);
                        let timeline_inner_sign_started = timeline
                            .map(|_| crate::network_timeline_monotonic_ns())
                            .unwrap_or(0);
                        let inner_auth_result = self
                            .host
                            .sign_object_rpc_request_async(&prepared_inner.request)
                            .await;
                        record_route_timeline_stage(
                            timeline,
                            crate::network_timeline_stage::REQUEST_SIGN,
                            timeline_inner_sign_started,
                            inner_auth_result.is_ok(),
                            1,
                        );
                        if let (Some(started), Some(route)) =
                            (inner_sign_started, perf_route.as_deref())
                        {
                            tracing::info!(
                                target: "sinomo.network.perf",
                                stage = "request_sign",
                                route,
                                target_node_id = %target_node_id,
                                next_hop_node_id = %route_plan.next_hop,
                                signature_kind = "origin_inner",
                                relay = true,
                                ok = inner_auth_result.is_ok(),
                                elapsed_us = network_perf_trace_elapsed_us(started),
                            );
                        }
                        let inner_auth = inner_auth_result?;
                        let outer = relay_request_for_signed_rpc_with_max_hop_timeout(
                            &state.cluster_id,
                            target_node_id,
                            &prepared_inner.request,
                            inner_auth,
                            std::iter::once(state.local_node_id.clone()),
                            &route_plan.path,
                            &relay_target_endpoints,
                            remaining,
                        )?;
                        ObjectRpcSessionCall::new(outer, prepared_inner.body)
                            .prepare_for_signing()?
                    };
                    if let (Some(started), Some(route)) = (prepare_started, perf_route.as_deref()) {
                        tracing::info!(
                            target: "sinomo.network.perf",
                            stage = "request_prepare",
                            route,
                            target_node_id = %target_node_id,
                            next_hop_node_id = %route_plan.next_hop,
                            relay = !final_hop,
                            channel = false,
                            ok = true,
                            elapsed_us = network_perf_trace_elapsed_us(started),
                        );
                    }
                    let sign_started = perf_enabled.then(Instant::now);
                    let timeline_sign_started = timeline
                        .map(|_| crate::network_timeline_monotonic_ns())
                        .unwrap_or(0);
                    let request_auth_result = self
                        .host
                        .sign_object_rpc_request_async(&prepared.request)
                        .await;
                    record_route_timeline_stage(
                        timeline,
                        crate::network_timeline_stage::REQUEST_SIGN,
                        timeline_sign_started,
                        request_auth_result.is_ok(),
                        0,
                    );
                    if let (Some(started), Some(route)) = (sign_started, perf_route.as_deref()) {
                        tracing::info!(
                            target: "sinomo.network.perf",
                            stage = "request_sign",
                            route,
                            target_node_id = %target_node_id,
                            next_hop_node_id = %route_plan.next_hop,
                            signature_kind = "hop",
                            relay = !final_hop,
                            ok = request_auth_result.is_ok(),
                            elapsed_us = network_perf_trace_elapsed_us(started),
                        );
                    }
                    let request_auth = request_auth_result?;
                    if !final_hop {
                        record_route_timeline_stage(
                            timeline,
                            crate::network_timeline_stage::RELAY_PREPARE,
                            timeline_prepare_started,
                            true,
                            0,
                        );
                    }
                    let result = start_object_rpc_client_session(
                        stream.expect("canonical stream exists without a channel"),
                        prepared,
                        request_auth,
                        data_dir.clone(),
                        next_hop_public_key.clone(),
                        response_origin_public_key.clone(),
                        policy,
                    )
                    .await;
                    if let Some(route) = perf_route.as_deref() {
                        tracing::info!(
                            target: "sinomo.network.perf",
                            stage = "canonical_session_total",
                            route,
                            target_node_id = %target_node_id,
                            next_hop_node_id = %route_plan.next_hop,
                            endpoint = %endpoint.address,
                            relay = !final_hop,
                            ok = result.is_ok(),
                            elapsed_us = network_perf_trace_elapsed_us(started),
                        );
                    }
                    result
                };
                // Only the canonical stream was opened from this endpoint.
                // Node-level pooled channels must not bias endpoint routing.
                if !used_channel
                    && let Some(observation) =
                        cluster_signed_endpoint_session_observation(result.as_ref(), started)
                {
                    let _ = self
                        .host
                        .observe_signed_endpoint_rpc_async(&endpoint, observation)
                        .await;
                }
                match result {
                    Ok(mut result) => {
                        if used_channel
                            && !final_hop
                            && let Some(detail) = cluster_relay_channel_response_attestation_error(
                                &result.response,
                                target_node_id,
                            )
                        {
                            return Err(NetworkError::outcome_unknown(
                                format!("cluster target object RPC session to {target_node_id}"),
                                operation_id.as_deref(),
                                detail,
                            ));
                        }
                        if !cluster_relay_session_response_origin_is_acceptable(
                            &result.response,
                            result.response_origin_verified(),
                        ) {
                            return Err(NetworkError::outcome_unknown(
                                format!("cluster target object RPC session to {target_node_id}"),
                                operation_id.as_deref(),
                                "relay returned a delivered response head without a valid target attestation",
                            ));
                        }
                        match cluster_relay_session_response_disposition(&result.response) {
                            ClusterRelaySessionResponseDisposition::OutcomeUnknown => {
                                return Err(NetworkError::outcome_unknown(
                                    format!(
                                        "cluster target object RPC session to {target_node_id}"
                                    ),
                                    operation_id.as_deref(),
                                    relay_response_outcome_unknown_detail(&result.response),
                                ));
                            }
                            ClusterRelaySessionResponseDisposition::FreshOriginSignature => {
                                possibly_applied = true;
                                let detail = relay_response_routing_error_detail(&result.response);
                                if !request_retry_safe || replayable_call.is_none() {
                                    return Err(NetworkError::outcome_unknown(
                                        format!(
                                            "cluster target object RPC session to {target_node_id}"
                                        ),
                                        operation_id.as_deref(),
                                        detail,
                                    ));
                                }
                                failures.push(format!(
                                    "{} ({:?}) requested a fresh origin signature: {detail}",
                                    endpoint.address, endpoint.transport
                                ));
                                continue 'route_attempts;
                            }
                            ClusterRelaySessionResponseDisposition::RoutingError => {
                                failures.push(format!(
                                    "{} ({:?}) returned routing error: {}",
                                    endpoint.address,
                                    endpoint.transport,
                                    relay_response_routing_error_detail(&result.response)
                                ));
                                break;
                            }
                            ClusterRelaySessionResponseDisposition::Delivered => {}
                        }
                        if let Some(responder_node_id) = cluster_object_rpc_wrong_responder_node_id(
                            &result.response,
                            target_node_id,
                        ) {
                            possibly_applied = true;
                            let detail = format!(
                                "{} ({:?}) responder mismatch: expected {}, got {}",
                                endpoint.address,
                                endpoint.transport,
                                target_node_id,
                                responder_node_id
                            );
                            if !request_retry_safe {
                                return Err(NetworkError::outcome_unknown(
                                    "cluster object RPC session responder mismatch",
                                    operation_id.as_deref(),
                                    detail,
                                ));
                            }
                            failures.push(detail);
                            continue 'route_attempts;
                        }
                        result.response.error_detail()?;
                        // A canonical session authenticates the final responder
                        // in its signed response transcript, whereas a pooled
                        // channel carries that identity as response metadata.
                        // Normalize both successful paths here so an upstream
                        // pooled relay can preserve the already-verified target
                        // instead of attributing the response to itself.
                        result
                            .response
                            .mark_responder_node_id(target_node_id.to_string());
                        return Ok(result);
                    }
                    Err(error @ NetworkError::AuthDenied) => return Err(error),
                    Err(error) => {
                        let delivery_state = error
                            .delivery_state()
                            .unwrap_or(NetworkDeliveryState::PossiblyApplied);
                        if delivery_state == NetworkDeliveryState::PossiblyApplied {
                            possibly_applied = true;
                        }
                        failures.push(format!(
                            "{} ({:?}): {error}",
                            endpoint.address, endpoint.transport
                        ));
                        if delivery_state == NetworkDeliveryState::PossiblyApplied
                            && !request_retry_safe
                        {
                            return Err(NetworkError::outcome_unknown(
                                format!("cluster target object RPC session to {target_node_id}"),
                                operation_id.as_deref(),
                                error.to_string(),
                            ));
                        }
                        if replayable_call.is_none() {
                            return Err(
                                if delivery_state == NetworkDeliveryState::PossiblyApplied {
                                    NetworkError::outcome_unknown(
                                        format!(
                                            "cluster target object RPC session to {target_node_id}"
                                        ),
                                        operation_id.as_deref(),
                                        error.to_string(),
                                    )
                                } else {
                                    error
                                },
                            );
                        }
                    }
                }
            }
            failed_edges.try_insert(&state.local_node_id, &route_plan.next_hop)?;
        }

        Err(NetworkError::RetryExhausted {
            operation: format!("cluster target object RPC session to {target_node_id}"),
            attempts: send_attempts,
            delivery_state: cluster_object_rpc_delivery_state(possibly_applied),
            detail: format!(
                "route to target node {target_node_id} failed{}",
                if failures.is_empty() {
                    String::new()
                } else {
                    format!(": {}", failures.join("; "))
                }
            ),
        })
    }
}

fn cluster_object_rpc_delivery_state(possibly_applied: bool) -> NetworkDeliveryState {
    if possibly_applied {
        NetworkDeliveryState::PossiblyApplied
    } else {
        NetworkDeliveryState::DefinitelyNotSent
    }
}

fn cluster_object_rpc_wrong_responder_node_id<'a>(
    response: &'a ObjectRpcResponse,
    target_node_id: &NodeId,
) -> Option<&'a str> {
    let expected = target_node_id.to_string();
    response
        .responder_node_id()
        .filter(|responder_node_id| *responder_node_id != expected.as_str())
}

fn cluster_relay_channel_response_attestation_error(
    response: &ObjectRpcResponse,
    target_node_id: &NodeId,
) -> Option<String> {
    if cluster_relay_session_response_disposition(response)
        != ClusterRelaySessionResponseDisposition::Delivered
    {
        return None;
    }
    match response.responder_node_id() {
        Some(responder_node_id) if responder_node_id == target_node_id.as_str() => None,
        Some(responder_node_id) => Some(format!(
            "relay channel returned a delivered response from {responder_node_id}; expected {target_node_id}"
        )),
        None => Some(format!(
            "relay channel returned a delivered response without target {target_node_id} attestation"
        )),
    }
}

fn propagate_relay_response_metadata(inner: &ObjectRpcRequest, outer: &mut ObjectRpcRequest) {
    if let Some(traceparent) = inner.traceparent() {
        outer.metadata.insert(
            crate::OBJECT_RPC_TRACEPARENT_METADATA_KEY.to_string(),
            traceparent.to_string(),
        );
    }
    if let Some(tracestate) = inner.tracestate() {
        outer.metadata.insert(
            crate::OBJECT_RPC_TRACESTATE_METADATA_KEY.to_string(),
            tracestate.to_string(),
        );
    }
    if inner
        .metadata
        .contains_key(crate::OBJECT_RPC_OBSERVE_SAMPLED_METADATA_KEY)
    {
        outer.metadata.insert(
            crate::OBJECT_RPC_OBSERVE_SAMPLED_METADATA_KEY.to_string(),
            inner.observe_sampled().to_string(),
        );
    }
}

fn relay_envelope_hop_timeout(envelope: &RelayEnvelope) -> Option<Duration> {
    envelope
        .max_hop_timeout_ms
        .map(|timeout_ms| Duration::from_millis(timeout_ms.max(1)))
}

#[derive(Clone, Copy, Debug)]
struct ClusterEndpointRpcDeadline {
    deadline_at: Option<tokio::time::Instant>,
    timeout: Option<Duration>,
}

impl ClusterEndpointRpcDeadline {
    fn new(timeout: Option<Duration>) -> Self {
        Self {
            deadline_at: timeout.map(|timeout| tokio::time::Instant::now() + timeout),
            timeout,
        }
    }

    fn checked_remaining(&self) -> Option<Option<Duration>> {
        match self.deadline_at {
            Some(deadline_at) => {
                let remaining = deadline_at.saturating_duration_since(tokio::time::Instant::now());
                (!remaining.is_zero()).then_some(Some(remaining))
            }
            None => Some(None),
        }
    }

    fn timeout_error(
        &self,
        operation: &str,
        phase: NetworkTimeoutPhase,
        delivery_state: NetworkDeliveryState,
    ) -> NetworkError {
        NetworkError::timeout(
            operation,
            phase,
            self.timeout.unwrap_or_default(),
            delivery_state,
        )
    }
}

async fn cluster_object_rpc_channel_call_with_health(
    channel: ObjectRpcChannelClient,
    call: ObjectRpcSessionCall,
    health: NetworkHealthPolicy,
    deadline: ClusterEndpointRpcDeadline,
    operation: &'static str,
) -> Result<ObjectRpcSessionResult, NetworkError> {
    let pending = cluster_object_rpc_call_with_deadline(
        channel.call(call),
        deadline,
        deadline.timeout_error(
            operation,
            NetworkTimeoutPhase::Overall,
            NetworkDeliveryState::PossiblyApplied,
        ),
    )
    .await?;
    let call_id = pending.call_id();
    let mut progress = pending.progress();
    let result = pending.result();
    tokio::pin!(result);

    let overall = async move {
        match deadline.deadline_at {
            Some(deadline_at) => tokio::time::sleep_until(deadline_at).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::pin!(overall);
    let mut progress_open = true;
    let probe_after = health.no_progress_timeout / 2;
    let mut idle_deadline = tokio::time::Instant::now() + health.no_progress_timeout;
    let mut probe_deadline = tokio::time::Instant::now() + probe_after;
    let mut probe_sent = false;
    loop {
        let idle = tokio::time::sleep_until(idle_deadline);
        tokio::pin!(idle);
        let probe = tokio::time::sleep_until(probe_deadline);
        tokio::pin!(probe);
        tokio::select! {
            biased;
            () = &mut overall => {
                return Err(deadline.timeout_error(
                    operation,
                    NetworkTimeoutPhase::Overall,
                    NetworkDeliveryState::PossiblyApplied,
                ));
            }
            result = &mut result => return result,
            changed = progress.changed(), if progress_open => {
                match changed {
                    Ok(()) => {
                        idle_deadline = tokio::time::Instant::now()
                            + health.no_progress_timeout;
                        probe_deadline = tokio::time::Instant::now() + probe_after;
                        probe_sent = false;
                    }
                    Err(_) => progress_open = false,
                }
            }
            () = &mut probe, if !probe_sent => {
                // The local write is deliberately not progress. Only the
                // peer's matching response proves that this call's physical
                // path is still responsive and refreshes its idle deadline.
                channel.request_liveness_probe(call_id)?;
                probe_sent = true;
            }
            () = &mut idle => {
                return Err(NetworkError::timeout(
                    operation,
                    NetworkTimeoutPhase::Idle,
                    health.no_progress_timeout,
                    NetworkDeliveryState::PossiblyApplied,
                ));
            }
        }
    }
}

async fn cluster_object_rpc_call_with_deadline<T, F>(
    call: F,
    deadline: ClusterEndpointRpcDeadline,
    timeout_error: NetworkError,
) -> Result<T, NetworkError>
where
    F: Future<Output = Result<T, NetworkError>>,
{
    let Some(deadline_at) = deadline.deadline_at else {
        return call.await;
    };
    match tokio::time::timeout_at(deadline_at, call).await {
        Ok(result) => result,
        Err(_) => Err(timeout_error),
    }
}

async fn cluster_object_rpc_sign_with_deadline<T, F>(
    sign: F,
    deadline: ClusterEndpointRpcDeadline,
) -> Result<T, NetworkError>
where
    F: Future<Output = Result<T, NetworkError>>,
{
    let Some(deadline_at) = deadline.deadline_at else {
        return sign.await;
    };
    match tokio::time::timeout_at(deadline_at, sign).await {
        Ok(result) => result,
        Err(_) => Err(deadline.timeout_error(
            "cluster object rpc signing",
            NetworkTimeoutPhase::Signing,
            NetworkDeliveryState::DefinitelyNotSent,
        )),
    }
}

fn cluster_target_rpc_timeout_error(
    target_node_id: &NodeId,
    timeout: Option<Duration>,
    delivery_state: NetworkDeliveryState,
    failures: &[String],
) -> NetworkError {
    let mut detail = format!(
        "timed out routing to {target_node_id} after {}ms",
        timeout.unwrap_or_default().as_millis()
    );
    if !failures.is_empty() {
        detail.push_str(": ");
        detail.push_str(&failures.join("; "));
    }
    NetworkError::Timeout {
        operation: format!("cluster target rpc to {target_node_id}: {detail}"),
        phase: NetworkTimeoutPhase::Overall,
        timeout: timeout.unwrap_or_default(),
        delivery_state,
    }
}

fn relay_request_for_signed_rpc_with_max_hop_timeout(
    cluster_id: &ClusterId,
    target_node_id: &NodeId,
    request: &ObjectRpcRequest,
    request_auth: impl Into<Vec<u8>>,
    visited: impl IntoIterator<Item = NodeId>,
    planned_path: &[NodeId],
    target_endpoints: &[ClusterEndpointRecord],
    max_hop_timeout: Option<Duration>,
) -> Result<ObjectRpcRequest, NetworkError> {
    let inner = request.clone().with_auth(request_auth.into());
    let mut envelope = RelayEnvelope::new(cluster_id.clone(), target_node_id.clone(), inner);
    envelope.visited.extend(visited);
    envelope.planned_path =
        try_clone_route_node_ids(planned_path, "cluster relay planned route path")?;
    crate::object::try_reserve_vec(
        &mut envelope.target_endpoints,
        target_endpoints.len(),
        "cluster relay target endpoint hints",
    )?;
    for endpoint in target_endpoints {
        envelope.target_endpoints.push(try_clone_route_endpoint(
            endpoint,
            "cluster relay target endpoint hints",
        )?);
    }
    envelope.max_hop_timeout_ms = max_hop_timeout.map(duration_millis_u64);
    envelope.into_request()
}

fn duration_millis_u64(duration: Duration) -> u64 {
    duration.as_millis().clamp(1, u128::from(u64::MAX)) as u64
}

enum ClusterRelayFailureAction {
    RetryAlternateEndpoint,
    RetryFromOrigin,
    Terminal,
}

fn cluster_relay_failure_action(
    inner: &ObjectRpcRequest,
    error: &NetworkError,
) -> ClusterRelayFailureAction {
    if error.delivery_state() == Some(NetworkDeliveryState::DefinitelyNotSent) {
        return ClusterRelayFailureAction::RetryAlternateEndpoint;
    }
    if inner.is_response_retry_safe() {
        return if inner.auth.is_some() {
            ClusterRelayFailureAction::RetryFromOrigin
        } else {
            ClusterRelayFailureAction::RetryAlternateEndpoint
        };
    }
    ClusterRelayFailureAction::Terminal
}

fn cluster_relay_session_forward_failure_result(
    envelope: &RelayEnvelope,
    error: &NetworkError,
) -> Result<ObjectRpcSessionResult, NetworkError> {
    let response = match cluster_relay_failure_action(envelope.inner(), error) {
        ClusterRelayFailureAction::RetryAlternateEndpoint => ObjectRpcResponse::encode_error(
            CLUSTER_RELAY_ROUTE,
            format!("relay could not forward the object RPC session: {error}"),
        )?,
        ClusterRelayFailureAction::RetryFromOrigin => {
            cluster_relay_fresh_signature_retry_response(envelope.inner(), error)?
        }
        ClusterRelayFailureAction::Terminal => {
            cluster_relay_outcome_unknown_response(envelope.inner(), error)?
        }
    };
    Ok(ObjectRpcSessionResult::value(response))
}

fn cluster_relay_forward_remaining_timeout_ms(
    deadline: ClusterEndpointRpcDeadline,
) -> Option<Option<u64>> {
    deadline
        .checked_remaining()
        .map(|remaining| remaining.map(duration_millis_u64))
}

fn cluster_relay_outcome_unknown_response(
    inner: &ObjectRpcRequest,
    error: &NetworkError,
) -> Result<ObjectRpcResponse, NetworkError> {
    let mut response = ObjectRpcResponse::encode_error(
        inner.route.clone(),
        format!("relay delivery outcome is unknown; alternate endpoint was not attempted: {error}"),
    )?;
    response.metadata.insert(
        CLUSTER_RELAY_OUTCOME_UNKNOWN_METADATA_KEY.to_string(),
        CLUSTER_RELAY_OUTCOME_UNKNOWN_METADATA_VALUE.to_string(),
    );
    if let Some(operation_id) = inner.operation_id() {
        response.metadata.insert(
            crate::rpc::OBJECT_RPC_OPERATION_ID_METADATA_KEY.to_string(),
            operation_id.to_string(),
        );
    }
    Ok(response)
}

fn cluster_relay_fresh_signature_retry_response(
    inner: &ObjectRpcRequest,
    error: &NetworkError,
) -> Result<ObjectRpcResponse, NetworkError> {
    let mut response = ObjectRpcResponse::encode_error(
        CLUSTER_RELAY_ROUTE,
        format!(
            "relay delivery outcome is unknown; retry from the origin with a fresh signature: {error}"
        ),
    )?;
    response.metadata.insert(
        CLUSTER_RELAY_FRESH_SIGNATURE_RETRY_METADATA_KEY.to_string(),
        CLUSTER_RELAY_FRESH_SIGNATURE_RETRY_METADATA_VALUE.to_string(),
    );
    if let Some(operation_id) = inner.operation_id() {
        response.metadata.insert(
            crate::rpc::OBJECT_RPC_OPERATION_ID_METADATA_KEY.to_string(),
            operation_id.to_string(),
        );
    }
    Ok(response)
}

async fn cluster_relay_forward_plan(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    envelope: &RelayEnvelope,
    failed_edges: ClusterDeliveryFailedEdges,
    timeline: Option<crate::NetworkTimelineTraceContext>,
) -> Result<Option<ClusterDeliveryRoutePlan>, NetworkError> {
    let planned_started_ns = timeline
        .map(|_| crate::network_timeline_monotonic_ns())
        .unwrap_or(0);
    let planned = cluster_relay_planned_forward_plan(
        paths,
        state,
        local_node_id,
        envelope,
        &failed_edges,
        timeline,
    );
    record_route_timeline_stage(
        timeline,
        crate::network_timeline_stage::ROUTE_RELAY_PLANNED_PATH,
        planned_started_ns,
        planned.is_ok(),
        u32::from(planned.as_ref().is_ok_and(|plan| plan.is_some())),
    );
    if let Some(plan) = planned? {
        return Ok(Some(plan));
    }
    let dynamic_started_ns = timeline
        .map(|_| crate::network_timeline_monotonic_ns())
        .unwrap_or(0);
    let dynamic = async {
        cluster_delivery_route_plan_async_with_timeline(
            runtime,
            paths,
            state,
            local_node_id,
            &envelope.target_node_id,
            failed_edges.try_clone()?,
            try_clone_route_node_ids(&envelope.visited, "cluster relay visited nodes")?,
            timeline,
        )
        .await
    }
    .await;
    record_route_timeline_stage(
        timeline,
        crate::network_timeline_stage::ROUTE_RELAY_DYNAMIC_FALLBACK,
        dynamic_started_ns,
        dynamic.is_ok(),
        u32::from(dynamic.as_ref().is_ok_and(|plan| plan.is_some())),
    );
    let dynamic = dynamic?;
    if dynamic.is_some() {
        return Ok(dynamic);
    }
    let target_hint_started_ns = timeline
        .map(|_| crate::network_timeline_monotonic_ns())
        .unwrap_or(0);
    let target_hint = cluster_relay_target_hint_forward_plan(
        paths,
        state,
        local_node_id,
        envelope,
        &failed_edges,
        timeline,
    );
    record_route_timeline_stage(
        timeline,
        crate::network_timeline_stage::ROUTE_RELAY_TARGET_HINT_FALLBACK,
        target_hint_started_ns,
        target_hint.is_ok(),
        u32::from(target_hint.as_ref().is_ok_and(|plan| plan.is_some())),
    );
    target_hint
}

fn cluster_relay_target_hint_forward_plan(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    envelope: &RelayEnvelope,
    failed_edges: &ClusterDeliveryFailedEdges,
    timeline: Option<crate::NetworkTimelineTraceContext>,
) -> Result<Option<ClusterDeliveryRoutePlan>, NetworkError> {
    if envelope.target_endpoints.is_empty()
        || envelope
            .visited
            .iter()
            .any(|node| node == &envelope.target_node_id)
        || failed_edges.contains(local_node_id, &envelope.target_node_id)
    {
        return Ok(None);
    }
    let endpoints = cluster_relay_planned_target_endpoints(
        paths,
        state,
        local_node_id,
        &envelope.target_node_id,
        &envelope.target_endpoints,
        timeline,
    )?;
    if endpoints.is_empty() {
        return Ok(None);
    }
    Ok(Some(ClusterDeliveryRoutePlan {
        next_hop: try_clone_route_node_id(&envelope.target_node_id, "cluster relay hint target")?,
        endpoints,
        path: cluster_delivery_two_node_path(local_node_id, &envelope.target_node_id)?,
    }))
}

fn cluster_relay_planned_forward_plan(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    envelope: &RelayEnvelope,
    failed_edges: &ClusterDeliveryFailedEdges,
    timeline: Option<crate::NetworkTimelineTraceContext>,
) -> Result<Option<ClusterDeliveryRoutePlan>, NetworkError> {
    if envelope.planned_path.is_empty() {
        return Ok(None);
    }
    let Some(position) = envelope
        .planned_path
        .iter()
        .position(|node| node == local_node_id)
    else {
        return Ok(None);
    };
    let Some(next_hop) = envelope.planned_path.get(position.saturating_add(1)) else {
        return Ok(None);
    };
    if envelope.visited.iter().any(|node| node == next_hop)
        || failed_edges.contains(local_node_id, next_hop)
    {
        return Ok(None);
    }
    let endpoints = if next_hop == &envelope.target_node_id && !envelope.target_endpoints.is_empty()
    {
        cluster_relay_planned_target_endpoints(
            paths,
            state,
            local_node_id,
            &envelope.target_node_id,
            &envelope.target_endpoints,
            timeline,
        )?
    } else {
        cluster_relay_planned_next_hop_endpoints(paths, state, local_node_id, next_hop, timeline)?
    };
    if endpoints.is_empty() {
        return Ok(None);
    }
    Ok(Some(ClusterDeliveryRoutePlan {
        next_hop: try_clone_route_node_id(next_hop, "cluster relay planned next hop")?,
        endpoints,
        path: try_clone_route_node_ids(
            &envelope.planned_path[position..],
            "cluster relay planned route path",
        )?,
    }))
}

fn cluster_relay_planned_target_endpoints(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    target_endpoints: &[ClusterEndpointRecord],
    timeline: Option<crate::NetworkTimelineTraceContext>,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let connectivity_started_ns = timeline
        .map(|_| crate::network_timeline_monotonic_ns())
        .unwrap_or(0);
    let local_connectivity = ClusterConnectivityGraph::try_read_for_local(paths, state);
    record_route_timeline_stage(
        timeline,
        crate::network_timeline_stage::ROUTE_RELAY_CONNECTIVITY_READ,
        connectivity_started_ns,
        local_connectivity.is_ok(),
        0,
    );
    let local_connectivity = local_connectivity?;
    let local_private_prefixes =
        cluster_delivery_local_private_prefixes(&state.members, local_node_id)?;
    let mut candidates = Vec::new();
    for endpoint in target_endpoints
        .iter()
        .filter(|endpoint| cluster_delivery_local_endpoint_is_usable(local_node_id, endpoint))
    {
        let Some(cost) = cluster_delivery_local_endpoint_cost(
            state,
            &local_connectivity,
            endpoint,
            target_node_id,
            &local_private_prefixes,
        ) else {
            continue;
        };
        crate::object::try_reserve_vec(&mut candidates, 1, "cluster relay planned endpoints")?;
        candidates.push((
            cost,
            endpoint.priority,
            cluster_delivery_transport_sort_rank(&endpoint.transport),
            endpoint.address.clone(),
            try_clone_route_endpoint(endpoint, "cluster relay planned endpoints")?,
        ));
    }
    for endpoint in
        local_connectivity.reachable_endpoints_matching(local_node_id, target_node_id, |endpoint| {
            cluster_delivery_local_endpoint_is_usable(local_node_id, endpoint)
        })
    {
        if let Some(cost) = cluster_delivery_local_endpoint_cost(
            state,
            &local_connectivity,
            &endpoint,
            target_node_id,
            &local_private_prefixes,
        ) {
            if !candidates.iter().any(
                |(_, _, _, _, retained): &(u64, u16, u8, String, ClusterEndpointRecord)| {
                    retained.endpoint_id == endpoint.endpoint_id
                        && retained.address == endpoint.address
                },
            ) {
                crate::object::try_reserve_vec(
                    &mut candidates,
                    1,
                    "cluster relay planned endpoints",
                )?;
                candidates.push((
                    cost,
                    endpoint.priority,
                    cluster_delivery_transport_sort_rank(&endpoint.transport),
                    endpoint.address.clone(),
                    endpoint,
                ));
            }
        }
    }
    candidates.sort_by(|left, right| {
        (&left.0, &left.1, &left.2, left.3.as_str()).cmp(&(
            &right.0,
            &right.1,
            &right.2,
            right.3.as_str(),
        ))
    });
    let mut endpoints = Vec::new();
    crate::object::try_reserve_vec(
        &mut endpoints,
        candidates.len(),
        "cluster relay planned endpoints",
    )?;
    for (_, _, _, _, endpoint) in candidates {
        endpoints.push(endpoint);
    }
    Ok(endpoints)
}

fn cluster_relay_planned_next_hop_endpoints(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    next_hop: &NodeId,
    timeline: Option<crate::NetworkTimelineTraceContext>,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let connectivity_started_ns = timeline
        .map(|_| crate::network_timeline_monotonic_ns())
        .unwrap_or(0);
    let local_connectivity = ClusterConnectivityGraph::try_read_for_local(paths, state);
    record_route_timeline_stage(
        timeline,
        crate::network_timeline_stage::ROUTE_RELAY_CONNECTIVITY_READ,
        connectivity_started_ns,
        local_connectivity.is_ok(),
        0,
    );
    let local_connectivity = local_connectivity?;
    let members = &state.members;
    let local_private_prefixes = cluster_delivery_local_private_prefixes(members, local_node_id)?;
    let mut candidates = Vec::new();
    for endpoint in members
        .iter()
        .filter(|member| member.node_id == *next_hop && member.state.is_authorized())
        .flat_map(|member| member.endpoints.iter())
        .filter(|endpoint| cluster_delivery_local_endpoint_is_usable(local_node_id, endpoint))
    {
        let Some(cost) = cluster_delivery_local_endpoint_cost(
            state,
            &local_connectivity,
            endpoint,
            next_hop,
            &local_private_prefixes,
        ) else {
            continue;
        };
        crate::object::try_reserve_vec(&mut candidates, 1, "cluster relay planned endpoints")?;
        candidates.push((
            cost,
            endpoint.priority,
            cluster_delivery_transport_sort_rank(&endpoint.transport),
            endpoint.address.clone(),
            try_clone_route_endpoint(endpoint, "cluster relay planned endpoints")?,
        ));
    }
    for endpoint in
        local_connectivity.reachable_endpoints_matching(local_node_id, next_hop, |endpoint| {
            cluster_delivery_local_endpoint_is_usable(local_node_id, endpoint)
        })
    {
        if let Some(cost) = cluster_delivery_local_endpoint_cost(
            state,
            &local_connectivity,
            &endpoint,
            next_hop,
            &local_private_prefixes,
        ) {
            if !candidates.iter().any(
                |(_, _, _, _, retained): &(u64, u16, u8, String, ClusterEndpointRecord)| {
                    retained.endpoint_id == endpoint.endpoint_id
                        && retained.address == endpoint.address
                },
            ) {
                crate::object::try_reserve_vec(
                    &mut candidates,
                    1,
                    "cluster relay planned endpoints",
                )?;
                candidates.push((
                    cost,
                    endpoint.priority,
                    cluster_delivery_transport_sort_rank(&endpoint.transport),
                    endpoint.address.clone(),
                    endpoint,
                ));
            }
        }
    }
    candidates.sort_by(|left, right| {
        (&left.0, &left.1, &left.2, left.3.as_str()).cmp(&(
            &right.0,
            &right.1,
            &right.2,
            right.3.as_str(),
        ))
    });
    let mut endpoints = Vec::new();
    crate::object::try_reserve_vec(
        &mut endpoints,
        candidates.len(),
        "cluster relay planned endpoints",
    )?;
    for (_, _, _, _, endpoint) in candidates {
        endpoints.push(endpoint);
    }
    Ok(endpoints)
}

pub fn relay_response_is_routing_error(response: &ObjectRpcResponse) -> bool {
    response.route == CLUSTER_RELAY_ROUTE && response.error_detail().is_err()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClusterRelaySessionResponseDisposition {
    Delivered,
    RoutingError,
    FreshOriginSignature,
    OutcomeUnknown,
}

fn cluster_relay_session_response_disposition(
    response: &ObjectRpcResponse,
) -> ClusterRelaySessionResponseDisposition {
    if relay_response_reports_outcome_unknown(response) {
        ClusterRelaySessionResponseDisposition::OutcomeUnknown
    } else if relay_response_requires_fresh_origin_signature(response) {
        ClusterRelaySessionResponseDisposition::FreshOriginSignature
    } else if relay_response_is_routing_error(response) {
        ClusterRelaySessionResponseDisposition::RoutingError
    } else {
        ClusterRelaySessionResponseDisposition::Delivered
    }
}

fn cluster_relay_session_response_origin_is_acceptable(
    response: &ObjectRpcResponse,
    response_origin_verified: bool,
) -> bool {
    response_origin_verified
        || cluster_relay_session_response_disposition(response)
            != ClusterRelaySessionResponseDisposition::Delivered
}

fn validate_and_normalize_forwarded_relay_result(
    envelope: &RelayEnvelope,
    used_channel: bool,
    result: &mut ObjectRpcSessionResult,
) -> Result<(), NetworkError> {
    if cluster_relay_session_response_disposition(&result.response)
        != ClusterRelaySessionResponseDisposition::Delivered
    {
        return Ok(());
    }
    if !cluster_relay_session_response_origin_is_acceptable(
        &result.response,
        result.response_origin_verified(),
    ) {
        return Err(NetworkError::outcome_unknown(
            format!(
                "forward cluster relay object RPC session to {}",
                envelope.target_node_id
            ),
            envelope.inner.operation_id(),
            "relay returned a delivered response without a verified target origin",
        ));
    }
    if used_channel {
        if let Some(detail) = cluster_relay_channel_response_attestation_error(
            &result.response,
            &envelope.target_node_id,
        ) {
            return Err(NetworkError::outcome_unknown(
                format!(
                    "forward cluster relay object RPC session to {}",
                    envelope.target_node_id
                ),
                envelope.inner.operation_id(),
                detail,
            ));
        }
    } else {
        // Canonical relay sessions authenticate the final response origin in
        // the signed transcript, but do not carry pooled-channel responder
        // metadata. Normalize that verified identity before returning through
        // an upstream channel so the forwarding node cannot be mistaken for
        // the final target.
        result.response.metadata.insert(
            crate::rpc::OBJECT_RPC_RESPONDER_NODE_ID_METADATA_KEY.to_string(),
            envelope.target_node_id.to_string(),
        );
    }
    Ok(())
}

fn relay_response_requires_fresh_origin_signature(response: &ObjectRpcResponse) -> bool {
    relay_response_is_routing_error(response)
        && response
            .metadata
            .get(CLUSTER_RELAY_FRESH_SIGNATURE_RETRY_METADATA_KEY)
            .is_some_and(|value| value == CLUSTER_RELAY_FRESH_SIGNATURE_RETRY_METADATA_VALUE)
}

fn relay_response_reports_outcome_unknown(response: &ObjectRpcResponse) -> bool {
    response
        .metadata
        .get(CLUSTER_RELAY_OUTCOME_UNKNOWN_METADATA_KEY)
        .is_some_and(|value| value == CLUSTER_RELAY_OUTCOME_UNKNOWN_METADATA_VALUE)
}

fn relay_response_outcome_unknown_detail(response: &ObjectRpcResponse) -> String {
    response
        .metadata
        .get(crate::OBJECT_RPC_ERROR_DETAIL_METADATA_KEY)
        .cloned()
        .unwrap_or_else(|| "relay delivery outcome is unknown".to_string())
}

fn relay_response_routing_error_detail(response: &ObjectRpcResponse) -> String {
    response
        .metadata
        .get(crate::OBJECT_RPC_ERROR_DETAIL_METADATA_KEY)
        .cloned()
        .unwrap_or_else(|| "routing error".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Read};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    #[test]
    fn network_rpc_outcomes_are_bounded_error_classes() {
        let timeout = Err(NetworkError::timeout(
            "test",
            NetworkTimeoutPhase::Overall,
            Duration::from_secs(1),
            NetworkDeliveryState::DefinitelyNotSent,
        ));
        let saturation = Err(NetworkError::MailboxFull {
            node_id: NodeId::from_legacy_named("peer").expect("peer node id"),
            max_frames: 1,
        });
        let route = Err(NetworkError::UnknownNode {
            node_id: NodeId::from_legacy_named("missing").expect("missing node id"),
        });

        assert_eq!(network_rpc_outcome(&timeout), "timeout");
        assert_eq!(network_rpc_outcome(&saturation), "saturated");
        assert_eq!(network_rpc_outcome(&route), "no_route");
    }

    #[test]
    fn bulk_and_unknown_attachments_use_the_bdp_tuned_session() {
        let request = || {
            ObjectRpcRequest::from_encoded_body("/test/channel-compatibility", "test", Vec::new())
        };
        let small =
            ObjectRpcSessionCall::new(request(), ObjectRpcBodySource::bytes(vec![0_u8; 1024]));
        let bulk = ObjectRpcSessionCall::new(
            request(),
            ObjectRpcBodySource::bytes(vec![
                0_u8;
                OBJECT_RPC_CHANNEL_MAX_MATERIALIZED_VALUE_BYTES + 1
            ]),
        );
        let unknown = ObjectRpcSessionCall::new(
            request(),
            ObjectRpcBodySource::from_reader(Cursor::new(Vec::<u8>::new()), None),
        );

        assert!(object_rpc_call_is_channel_compatible(&small));
        assert!(!object_rpc_call_is_channel_compatible(&bulk));
        assert!(!object_rpc_call_is_channel_compatible(&unknown));
    }

    enum RecordingChannelAction {
        Respond {
            responder_node_id: Option<NodeId>,
        },
        ReadBodyThenRespond {
            responder_node_id: Option<NodeId>,
        },
        DelayedResponse {
            delay: Duration,
            responder_node_id: Option<NodeId>,
        },
        BlackholeAfterAccept,
        CloseAfterAccept,
    }

    struct RecordingChannel {
        client: ObjectRpcChannelClient,
        ingress: crate::ObjectRpcChannelIngress,
        requests: tokio::sync::mpsc::UnboundedReceiver<ObjectRpcRequest>,
        received_body_bytes: Arc<AtomicUsize>,
        tasks: Vec<tokio::task::JoinHandle<()>>,
    }

    impl RecordingChannel {
        fn close(&self, detail: &str) {
            self.ingress.close(detail);
        }
    }

    impl Drop for RecordingChannel {
        fn drop(&mut self) {
            for task in &self.tasks {
                task.abort();
            }
        }
    }

    fn recording_channel_response(
        request: ObjectRpcRequest,
        responder_node_id: Option<NodeId>,
    ) -> ObjectRpcResponse {
        let route = if request.route == CLUSTER_RELAY_ROUTE {
            RelayEnvelope::from_request(&request)
                .expect("route test relay envelope")
                .inner
                .route
        } else {
            request.route
        };
        let mut response = ObjectRpcResponse::encode_body(route, &"channel-ok")
            .expect("route test channel response");
        if let Some(responder_node_id) = responder_node_id {
            response.mark_responder_node_id(responder_node_id.to_string());
        }
        response
    }

    fn recording_channel(action: RecordingChannelAction) -> RecordingChannel {
        let blackhole_after_request =
            matches!(&action, RecordingChannelAction::BlackholeAfterAccept);
        let left = crate::ObjectRpcChannelEndpoint::new(
            crate::ObjectRpcChannelRole::Dialer,
            crate::ObjectRpcChannelConfig::default(),
        )
        .expect("route test channel dialer");
        let right = crate::ObjectRpcChannelEndpoint::new(
            crate::ObjectRpcChannelRole::Acceptor,
            crate::ObjectRpcChannelConfig::default(),
        )
        .expect("route test channel acceptor");
        let crate::ObjectRpcChannelEndpoint {
            client,
            server: _left_server,
            ingress: left_ingress,
            egress: mut left_egress,
        } = left;
        let crate::ObjectRpcChannelEndpoint {
            client: _right_client,
            mut server,
            ingress: right_ingress,
            egress: mut right_egress,
        } = right;
        let response_ingress = left_ingress.clone();
        let close_ingress = left_ingress.clone();
        let left_pump = tokio::spawn(async move {
            let mut request_delivered = false;
            while let Some(frame) = left_egress.next_frame().await {
                if blackhole_after_request && request_delivered {
                    continue;
                }
                request_delivered |=
                    matches!(&frame, crate::ObjectRpcChannelWireFrame::Request { .. });
                if right_ingress.receive(frame).is_err() {
                    break;
                }
            }
        });
        let right_pump = tokio::spawn(async move {
            while let Some(frame) = right_egress.next_frame().await {
                if response_ingress.receive(frame).is_err() {
                    break;
                }
            }
        });
        let (requests_sender, requests) = tokio::sync::mpsc::unbounded_channel();
        let received_body_bytes = Arc::new(AtomicUsize::new(0));
        let server_received_body_bytes = Arc::clone(&received_body_bytes);
        let server_task = tokio::spawn(async move {
            let Some(mut inbound) = server.accept().await else {
                return;
            };
            let request = inbound.request.request.clone();
            let _ = requests_sender.send(request.clone());
            match action {
                RecordingChannelAction::CloseAfterAccept => {
                    close_ingress.close("route test post-enqueue close");
                }
                RecordingChannelAction::Respond { responder_node_id } => {
                    let response = recording_channel_response(request, responder_node_id);
                    inbound
                        .respond(crate::ObjectRpcSessionResponse::value(response))
                        .await
                        .expect("route test channel respond");
                }
                RecordingChannelAction::ReadBodyThenRespond { responder_node_id } => {
                    let mut body = Vec::new();
                    tokio::io::AsyncReadExt::read_to_end(&mut inbound.request.body, &mut body)
                        .await
                        .expect("read route test channel body");
                    server_received_body_bytes.store(body.len(), AtomicOrdering::Relaxed);
                    let response = recording_channel_response(request, responder_node_id);
                    inbound
                        .respond(crate::ObjectRpcSessionResponse::value(response))
                        .await
                        .expect("route test channel respond");
                }
                RecordingChannelAction::DelayedResponse {
                    delay,
                    responder_node_id,
                } => {
                    tokio::time::sleep(delay).await;
                    let response = recording_channel_response(request, responder_node_id);
                    inbound
                        .respond(crate::ObjectRpcSessionResponse::value(response))
                        .await
                        .expect("route test delayed channel respond");
                }
                RecordingChannelAction::BlackholeAfterAccept => {
                    std::future::pending::<()>().await;
                }
            }
        });
        RecordingChannel {
            client,
            ingress: left_ingress,
            requests,
            received_body_bytes,
            tasks: vec![left_pump, right_pump, server_task],
        }
    }

    #[test]
    fn signed_endpoint_observation_separates_application_and_session_auth_denials() {
        let response = ObjectRpcResponse::encode_error(
            "/test/application-auth-denied",
            "network authorization was denied",
        )
        .expect("application denial response");
        let result = ObjectRpcSessionResult::value(response);
        assert!(result.response_origin_verified());
        assert!(matches!(
            result.response.error_detail(),
            Err(NetworkError::AuthDenied)
        ));

        let application_denial =
            cluster_signed_endpoint_session_observation(Ok(&result), Instant::now())
                .expect("completed session observation");
        assert!(application_denial.is_success());
        assert!(!application_denial.is_auth_denied());

        let session_error = NetworkError::AuthDenied;
        let session_denial =
            cluster_signed_endpoint_session_observation(Err(&session_error), Instant::now())
                .expect("session authentication observation");
        assert!(session_denial.is_auth_denied());
        assert!(!session_denial.is_success());
    }

    fn relay_retry_test_fixture(
        name: &str,
    ) -> (
        sinomo_testdir::TestDir,
        ClusterPaths,
        ClusterBootstrapState,
        NodeId,
        NodeId,
        [ClusterEndpointRecord; 2],
    ) {
        let root = sinomo_testdir::TestDir::new(name);
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named(format!("cluster_{name}")).expect("cluster id");
        let local_node =
            NodeId::from_legacy_named(format!("node_{name}_local")).expect("local node");
        let target_node =
            NodeId::from_legacy_named(format!("node_{name}_target")).expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            name,
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let first_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            format!("{name}-first.test:21353"),
            0,
        );
        let second_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            format!("{name}-second.test:21353"),
            10,
        );
        let mut target_member = ClusterMemberRecord::local_voter(
            cluster_id,
            target_node.clone(),
            "target",
            "pk-target",
            now,
        );
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![first_endpoint.clone(), second_endpoint.clone()];
        state.members.push(target_member);
        crate::write_object_file(&paths.state, &state).expect("write state");

        (
            root,
            paths,
            state,
            local_node,
            target_node,
            [first_endpoint, second_endpoint],
        )
    }

    fn relay_test_delivery_error(delivery_state: NetworkDeliveryState) -> NetworkError {
        NetworkError::delivery_failed(
            "test relay endpoint",
            delivery_state,
            "test response was not observed",
        )
    }

    #[derive(Default)]
    struct RecordingSignedHost {
        observations: Vec<(ClusterEndpointRecord, ClusterSignedObjectRpcObservation)>,
        signed_routes: Vec<String>,
        signed_requests: Vec<ObjectRpcRequest>,
        object_rpc_channel: Option<ObjectRpcChannelClient>,
        channel_lookups: usize,
        channel_lookup_stall: Option<Duration>,
        session_opens: usize,
        session_peers: Vec<tokio::io::DuplexStream>,
        session_server_data_dir: Option<sinomo_local::SinomoDataDir>,
        session_response_body: Option<Vec<u8>>,
        stall_first_sign: Option<Duration>,
        stall_session_open: Option<Duration>,
    }

    impl ClusterSignedObjectRpcAsyncHost for RecordingSignedHost {
        fn sign_object_rpc_request_async<'a>(
            &'a mut self,
            request: &'a ObjectRpcRequest,
        ) -> ClusterSignedObjectRpcFuture<'a, Vec<u8>> {
            let stall = self
                .stall_first_sign
                .filter(|_| self.signed_routes.is_empty());
            let next = self.signed_routes.len() + 1;
            self.signed_routes.push(request.route.clone());
            self.signed_requests.push(request.clone());
            let request = request.clone();
            let signing_data_dir = self.session_server_data_dir.clone();
            Box::pin(async move {
                if let Some(stall) = stall {
                    tokio::time::sleep(stall).await;
                }
                if let Some(data_dir) = signing_data_dir {
                    return crate::sign_daemon_rpc_request_from_data_dir(&data_dir, &request);
                }
                Ok(format!("test-auth-{next}").into_bytes())
            })
        }

        fn observe_signed_endpoint_rpc_async<'a>(
            &'a mut self,
            endpoint: &'a ClusterEndpointRecord,
            observation: ClusterSignedObjectRpcObservation,
        ) -> ClusterSignedObjectRpcFuture<'a, ()> {
            let endpoint = endpoint.clone();
            let observations = &mut self.observations;
            Box::pin(async move {
                observations.push((endpoint, observation));
                Ok(())
            })
        }

        fn object_rpc_channel_async<'a>(
            &'a mut self,
            _endpoint: &'a ClusterEndpointRecord,
            _health: NetworkHealthPolicy,
        ) -> ClusterSignedObjectRpcFuture<'a, Option<ObjectRpcChannelClient>> {
            self.channel_lookups = self.channel_lookups.saturating_add(1);
            let channel = self.object_rpc_channel.clone();
            let stall = self.channel_lookup_stall;
            Box::pin(async move {
                if let Some(stall) = stall {
                    tokio::time::sleep(stall).await;
                }
                Ok(channel)
            })
        }

        fn open_object_rpc_session_stream_async<'a>(
            &'a mut self,
            _endpoint: &'a ClusterEndpointRecord,
            _health: NetworkHealthPolicy,
        ) -> ClusterSignedObjectRpcFuture<'a, Box<dyn NetworkDuplexStream>> {
            self.session_opens = self.session_opens.saturating_add(1);
            let (client, peer) = tokio::io::duplex(1024);
            if let Some(data_dir) = self.session_server_data_dir.clone() {
                let response_body = self.session_response_body.clone();
                tokio::spawn(async move {
                    let (request, responder) = crate::accept_object_rpc_server_session(
                        Box::new(peer),
                        data_dir,
                        NetworkCallPolicy::default(),
                        |_request, _claimed_node_id, _claimed_public_key| Ok(()),
                    )
                    .await
                    .expect("accept test loopback session");
                    let response = match response_body {
                        Some(body) => ObjectRpcResponse::from_encoded_body(
                            request.request.route.clone(),
                            body,
                        ),
                        None => ObjectRpcResponse::encode_body(
                            request.request.route.clone(),
                            &"loopback-ok",
                        )
                        .expect("encode test loopback response"),
                    };
                    responder
                        .respond(crate::ObjectRpcSessionResponse::value(response))
                        .await
                        .expect("respond to test loopback session");
                });
            } else {
                self.session_peers.push(peer);
            }
            let stall = self.stall_session_open;
            Box::pin(async move {
                if let Some(stall) = stall {
                    tokio::time::sleep(stall).await;
                }
                Ok(Box::new(client) as Box<dyn NetworkDuplexStream>)
            })
        }
    }

    fn set_member_public_key(
        state: &mut ClusterBootstrapState,
        node_id: &NodeId,
        public_key: String,
    ) {
        state
            .members
            .iter_mut()
            .find(|member| member.node_id == *node_id)
            .expect("route test member")
            .node_public_key = public_key;
    }

    fn relay_channel_test_fixture(
        name: &str,
    ) -> (
        sinomo_testdir::TestDir,
        ClusterPaths,
        ClusterBootstrapState,
        NodeId,
        NodeId,
        ClusterEndpointRecord,
    ) {
        let root = sinomo_testdir::TestDir::new(name);
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named(format!("cluster_{name}")).expect("cluster id");
        let local_node =
            NodeId::from_legacy_named(format!("node_{name}_local")).expect("local node");
        let relay_node =
            NodeId::from_legacy_named(format!("node_{name}_relay")).expect("relay node");
        let target_node =
            NodeId::from_legacy_named(format!("node_{name}_target")).expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            name,
            local_node,
            "local",
            crate::daemon_auth_encode_public_key(&[1_u8; 32]),
            now,
        );
        let relay_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &relay_node,
            format!("{name}-relay.test:21353"),
            0,
        );
        let target_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            format!("{name}-target.test:21353"),
            0,
        );
        let mut relay_member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            relay_node.clone(),
            "relay",
            crate::daemon_auth_encode_public_key(&[2_u8; 32]),
            now,
        );
        relay_member.capabilities = main_daemon_capabilities(&relay_member.role);
        relay_member.allowed_capabilities = relay_member.capabilities.clone();
        relay_member.endpoints = vec![relay_endpoint.clone()];
        state.members.push(relay_member);
        let mut target_member = ClusterMemberRecord::local_voter(
            cluster_id,
            target_node.clone(),
            "target",
            crate::daemon_auth_encode_public_key(&[3_u8; 32]),
            now,
        );
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![target_endpoint.clone()];
        state.members.push(target_member);
        crate::write_object_file(&paths.state, &state).expect("write state");

        crate::connectivity_store::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[
                crate::connectivity::cluster_connectivity_record(
                    &state,
                    &relay_endpoint,
                    ClusterConnectivityState::Healthy,
                    Some(1),
                    0,
                    Some(now.to_string()),
                    None,
                    now,
                    60,
                ),
                crate::connectivity::cluster_connectivity_record(
                    &state,
                    &target_endpoint,
                    ClusterConnectivityState::Unreachable,
                    None,
                    crate::CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD,
                    None,
                    Some(now.to_string()),
                    now,
                    60,
                ),
            ],
        )
        .expect("write local connectivity");
        let mut relay_state = state.clone();
        relay_state.local_node_id = relay_node.clone();
        let relay_to_target = crate::connectivity::cluster_connectivity_record(
            &relay_state,
            &target_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some(now.to_string()),
            None,
            now,
            60,
        );
        crate::connectivity_store::write_local_cluster_connectivity_record_set(
            &paths,
            &relay_node,
            &[relay_to_target],
        )
        .expect("write relay connectivity");
        (root, paths, state, relay_node, target_node, relay_endpoint)
    }

    #[tokio::test]
    async fn endpoint_fast_path_uses_channel_without_opening_or_signing() {
        let (_root, _paths, mut state, _local_node, target_node, endpoints) =
            relay_retry_test_fixture("endpoint_channel_fast_path");
        set_member_public_key(
            &mut state,
            &target_node,
            crate::daemon_auth_encode_public_key(&[9_u8; 32]),
        );
        let mut channel = recording_channel(RecordingChannelAction::Respond {
            responder_node_id: Some(target_node.clone()),
        });
        let mut host = RecordingSignedHost {
            object_rpc_channel: Some(channel.client.clone()),
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::endpoint_only(&mut host);
        let request =
            ObjectRpcRequest::encode_body("/test/endpoint-channel-fast-path", "test", &"request")
                .expect("request");

        let result = client
            .call_endpoint(
                &state,
                &endpoints[0],
                ObjectRpcSessionCall::value(request),
                ClusterObjectRpcOptions::default(),
            )
            .await
            .expect("channel endpoint call");

        assert_eq!(
            result.response.decode_body::<String>().expect("response"),
            "channel-ok"
        );
        assert_eq!(host.channel_lookups, 1);
        assert_eq!(host.session_opens, 0);
        assert!(host.signed_routes.is_empty());
        assert!(
            host.observations.is_empty(),
            "a pooled channel is not provenance for the concrete endpoint passed to call_endpoint"
        );
        assert_eq!(
            channel
                .requests
                .recv()
                .await
                .expect("channel request")
                .route,
            "/test/endpoint-channel-fast-path"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn endpoint_channel_no_progress_times_out_and_cancels_only_the_call() {
        let (_root, _paths, mut state, _local_node, target_node, endpoints) =
            relay_retry_test_fixture("endpoint_channel_idle_timeout");
        set_member_public_key(
            &mut state,
            &target_node,
            crate::daemon_auth_encode_public_key(&[10_u8; 32]),
        );
        let channel = recording_channel(RecordingChannelAction::BlackholeAfterAccept);
        let mut host = RecordingSignedHost {
            object_rpc_channel: Some(channel.client.clone()),
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::endpoint_only(&mut host);
        let idle_timeout = Duration::from_millis(100);
        let started_at = tokio::time::Instant::now();
        let request = ObjectRpcRequest::encode_body(
            "/test/endpoint-channel-idle-timeout",
            "test",
            &"request",
        )
        .expect("request");

        let error = client
            .call_endpoint(
                &state,
                &endpoints[0],
                ObjectRpcSessionCall::value(request),
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::idle_only(idle_timeout)),
            )
            .await
            .expect_err("stalled channel call must reach its no-progress timeout");

        assert!(
            matches!(
                &error,
                NetworkError::Timeout {
                    operation,
                    phase: NetworkTimeoutPhase::Idle,
                    timeout,
                    delivery_state: NetworkDeliveryState::PossiblyApplied,
                } if operation == "multiplexed cluster endpoint object RPC"
                    && *timeout == idle_timeout
            ),
            "unexpected stalled channel error: {error:?}"
        );
        assert_eq!(
            tokio::time::Instant::now().duration_since(started_at),
            idle_timeout
        );
        assert_eq!(channel.client.queue_snapshot().active_calls, 0);
        assert_eq!(host.session_opens, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn endpoint_channel_liveness_responses_keep_a_slow_handler_healthy() {
        let (_root, _paths, mut state, _local_node, target_node, endpoints) =
            relay_retry_test_fixture("endpoint_channel_slow_handler_liveness");
        set_member_public_key(
            &mut state,
            &target_node,
            crate::daemon_auth_encode_public_key(&[13_u8; 32]),
        );
        let handler_delay = Duration::from_millis(350);
        let idle_timeout = Duration::from_millis(100);
        let channel = recording_channel(RecordingChannelAction::DelayedResponse {
            delay: handler_delay,
            responder_node_id: Some(target_node.clone()),
        });
        let mut host = RecordingSignedHost {
            object_rpc_channel: Some(channel.client.clone()),
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::endpoint_only(&mut host);
        let request = ObjectRpcRequest::encode_body(
            "/test/endpoint-channel-slow-handler-liveness",
            "test",
            &"request",
        )
        .expect("request");
        let started_at = tokio::time::Instant::now();

        let result = client
            .call_endpoint(
                &state,
                &endpoints[0],
                ObjectRpcSessionCall::value(request),
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::idle_only(idle_timeout)),
            )
            .await
            .expect("a responsive carrier must keep a slow handler call healthy");

        assert_eq!(
            result.response.decode_body::<String>().expect("response"),
            "channel-ok"
        );
        assert!(
            tokio::time::Instant::now().duration_since(started_at) >= handler_delay,
            "handler must outlive multiple logical-call idle windows"
        );
        assert_eq!(channel.client.queue_snapshot().active_calls, 0);
        assert_eq!(host.session_opens, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn endpoint_channel_progressing_body_outlives_idle_window() {
        const CHUNK_BYTES: usize = 1024;
        const CHUNKS: usize = 5;

        let (_root, _paths, mut state, _local_node, target_node, endpoints) =
            relay_retry_test_fixture("endpoint_channel_progressing_body");
        set_member_public_key(
            &mut state,
            &target_node,
            crate::daemon_auth_encode_public_key(&[11_u8; 32]),
        );
        let channel = recording_channel(RecordingChannelAction::ReadBodyThenRespond {
            responder_node_id: Some(target_node.clone()),
        });
        let mut host = RecordingSignedHost {
            object_rpc_channel: Some(channel.client.clone()),
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::endpoint_only(&mut host);
        let idle_timeout = Duration::from_millis(100);
        let progress_interval = Duration::from_millis(80);
        let total_bytes = CHUNK_BYTES * CHUNKS;
        let (mut body_writer, body_reader) = tokio::io::duplex(CHUNK_BYTES * 2);
        let producer = tokio::spawn(async move {
            let chunk = [0x5a_u8; CHUNK_BYTES];
            for _ in 0..CHUNKS {
                tokio::time::sleep(progress_interval).await;
                tokio::io::AsyncWriteExt::write_all(&mut body_writer, &chunk)
                    .await
                    .expect("write progressing request body");
            }
            tokio::io::AsyncWriteExt::shutdown(&mut body_writer)
                .await
                .expect("finish progressing request body");
        });
        let request = ObjectRpcRequest::from_encoded_body(
            "/test/endpoint-channel-progressing-body",
            "test",
            Vec::new(),
        );
        let started_at = tokio::time::Instant::now();

        let result = client
            .call_endpoint(
                &state,
                &endpoints[0],
                ObjectRpcSessionCall::new(
                    request,
                    ObjectRpcBodySource::async_reader(body_reader, Some(total_bytes as u64)),
                ),
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::idle_only(idle_timeout)),
            )
            .await
            .expect("continuous body progress must keep the channel call healthy");

        producer.await.expect("progressing body producer");
        assert_eq!(
            result.response.decode_body::<String>().expect("response"),
            "channel-ok"
        );
        assert_eq!(
            channel.received_body_bytes.load(AtomicOrdering::Relaxed),
            total_bytes
        );
        assert!(
            tokio::time::Instant::now().duration_since(started_at)
                >= progress_interval * CHUNKS as u32,
            "call should outlive one idle window through repeated progress"
        );
        assert_eq!(channel.client.queue_snapshot().active_calls, 0);
        assert_eq!(host.session_opens, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn endpoint_channel_whole_call_deadline_precedes_idle_timeout() {
        let (_root, _paths, mut state, _local_node, target_node, endpoints) =
            relay_retry_test_fixture("endpoint_channel_overall_precedence");
        set_member_public_key(
            &mut state,
            &target_node,
            crate::daemon_auth_encode_public_key(&[12_u8; 32]),
        );
        let channel = recording_channel(RecordingChannelAction::BlackholeAfterAccept);
        let mut host = RecordingSignedHost {
            object_rpc_channel: Some(channel.client.clone()),
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::endpoint_only(&mut host);
        let idle_timeout = Duration::from_millis(200);
        let overall_timeout = Duration::from_millis(50);
        let started_at = tokio::time::Instant::now();
        let request = ObjectRpcRequest::encode_body(
            "/test/endpoint-channel-overall-precedence",
            "test",
            &"request",
        )
        .expect("request");

        let error = client
            .call_endpoint(
                &state,
                &endpoints[0],
                ObjectRpcSessionCall::value(request),
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::idle_only(idle_timeout))
                    .with_overall_timeout(overall_timeout),
            )
            .await
            .expect_err("whole-call deadline must preempt the longer idle timeout");

        assert!(
            matches!(
                &error,
                NetworkError::Timeout {
                    phase: NetworkTimeoutPhase::Overall,
                    timeout,
                    delivery_state: NetworkDeliveryState::PossiblyApplied,
                    ..
                } if *timeout == overall_timeout
            ),
            "unexpected whole-call deadline error: {error:?}"
        );
        assert_eq!(
            tokio::time::Instant::now().duration_since(started_at),
            overall_timeout
        );
        assert_eq!(channel.client.queue_snapshot().active_calls, 0);
    }

    #[tokio::test]
    async fn routed_direct_fast_path_uses_channel_without_opening_or_signing() {
        let (_root, paths, mut state, _local_node, target_node, endpoints) =
            relay_retry_test_fixture("routed_direct_channel_fast_path");
        assert_ne!(
            endpoints[0].endpoint_id, endpoints[1].endpoint_id,
            "the regression requires multiple physical endpoint candidates"
        );
        set_member_public_key(
            &mut state,
            &target_node,
            crate::daemon_auth_encode_public_key(&[8_u8; 32]),
        );
        crate::write_object_file(&paths.state, &state).expect("rewrite state");
        let mut channel = recording_channel(RecordingChannelAction::Respond {
            responder_node_id: Some(target_node.clone()),
        });
        let mut host = RecordingSignedHost {
            object_rpc_channel: Some(channel.client.clone()),
            ..RecordingSignedHost::default()
        };
        let runtime = tokio::runtime::Handle::current();
        let mut client = ClusterObjectRpcClient::routed(&mut host, &runtime, &paths);
        let request = ObjectRpcRequest::encode_body(
            "/test/routed-direct-channel-fast-path",
            "test",
            &"request",
        )
        .expect("request");

        let result = client
            .call_target(
                &state,
                &target_node,
                ObjectRpcSessionCall::value(request),
                ClusterObjectRpcOptions::default(),
            )
            .await
            .expect("routed direct channel call");

        assert_eq!(
            result.response.decode_body::<String>().expect("response"),
            "channel-ok"
        );
        assert_eq!(host.channel_lookups, 1);
        assert_eq!(host.session_opens, 0);
        assert!(host.signed_routes.is_empty());
        assert!(
            host.observations.is_empty(),
            "a node-level pooled channel must not mark the selected physical endpoint healthy"
        );
        assert_eq!(
            channel
                .requests
                .recv()
                .await
                .expect("channel request")
                .route,
            "/test/routed-direct-channel-fast-path"
        );
    }

    #[tokio::test]
    async fn one_hop_channel_relay_signs_only_the_origin_request() {
        let (_root, paths, state, relay_node, target_node, _relay_endpoint) =
            relay_channel_test_fixture("one_hop_channel_relay");
        let mut channel = recording_channel(RecordingChannelAction::Respond {
            responder_node_id: Some(target_node.clone()),
        });
        let mut host = RecordingSignedHost {
            object_rpc_channel: Some(channel.client.clone()),
            ..RecordingSignedHost::default()
        };
        let runtime = tokio::runtime::Handle::current();
        let mut client = ClusterObjectRpcClient::routed(&mut host, &runtime, &paths);
        let inner_route = "/test/one-hop-channel-relay";
        let request =
            ObjectRpcRequest::encode_body(inner_route, "test", &"request").expect("request");

        client
            .call_target(
                &state,
                &target_node,
                ObjectRpcSessionCall::value(request),
                ClusterObjectRpcOptions::default(),
            )
            .await
            .expect("one-hop channel relay");

        assert_eq!(host.signed_routes, vec![inner_route.to_string()]);
        assert_eq!(host.session_opens, 0);
        let outer = channel.requests.recv().await.expect("relay request");
        assert_eq!(outer.route, CLUSTER_RELAY_ROUTE);
        let envelope = RelayEnvelope::from_request(&outer).expect("relay envelope");
        assert_eq!(envelope.visited, vec![state.local_node_id.clone()]);
        assert_eq!(
            envelope
                .inner
                .metadata
                .get(CLUSTER_RELAY_ORIGIN_NODE_ID_METADATA_KEY)
                .map(String::as_str),
            Some(state.local_node_id.as_str())
        );
        assert_eq!(
            envelope.planned_path,
            vec![state.local_node_id, relay_node, target_node]
        );
        assert_eq!(envelope.inner.route, inner_route);
        assert!(envelope.inner.auth.is_some(), "inner origin signature");
        assert!(
            outer.auth.is_none(),
            "channel relay head must not add a hop signature"
        );
    }

    #[tokio::test]
    async fn forwarded_relay_channel_does_not_observe_selected_physical_endpoint() {
        let (_root, paths, mut state, local_node, target_node, endpoints) =
            relay_retry_test_fixture("forwarded_relay_channel_endpoint_provenance");
        set_member_public_key(
            &mut state,
            &target_node,
            crate::daemon_auth_encode_public_key(&[13_u8; 32]),
        );
        crate::write_object_file(&paths.state, &state).expect("rewrite state");
        assert_ne!(
            endpoints[0].endpoint_id, endpoints[1].endpoint_id,
            "the regression requires multiple physical endpoint candidates"
        );
        let channel = recording_channel(RecordingChannelAction::Respond {
            responder_node_id: Some(target_node.clone()),
        });
        let mut host = RecordingSignedHost {
            object_rpc_channel: Some(channel.client.clone()),
            ..RecordingSignedHost::default()
        };
        let runtime = tokio::runtime::Handle::current();
        let mut client = ClusterObjectRpcClient::routed(&mut host, &runtime, &paths);
        let inner = ObjectRpcRequest::encode_body(
            "/test/forwarded-relay-channel-endpoint-provenance",
            "test",
            &"request",
        )
        .expect("inner request")
        .with_auth(b"origin-auth".to_vec());
        let mut envelope = RelayEnvelope::new(state.cluster_id.clone(), target_node.clone(), inner);
        envelope.planned_path = vec![local_node, target_node.clone()];
        envelope.target_endpoints = endpoints.to_vec();
        let request = crate::ObjectRpcSessionRequest::value(
            envelope.into_request().expect("relay session request"),
        );

        let result = client
            .forward_relay(&state, request, ClusterObjectRpcOptions::default())
            .await
            .expect("forwarded relay channel call");

        assert_eq!(
            result.response.decode_body::<String>().expect("response"),
            "channel-ok"
        );
        assert_eq!(host.channel_lookups, 1);
        assert_eq!(host.session_opens, 0);
        assert!(
            host.observations.is_empty(),
            "a forwarded node-level channel must not mark either physical endpoint healthy"
        );
    }

    struct CountingReader {
        inner: Cursor<Vec<u8>>,
        bytes_read: Arc<AtomicUsize>,
    }

    impl Read for CountingReader {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            let read = self.inner.read(buffer)?;
            self.bytes_read.fetch_add(read, AtomicOrdering::Relaxed);
            Ok(read)
        }
    }

    async fn assert_unavailable_channel_enters_canonical_without_consuming_body(
        name: &str,
        object_rpc_channel: Option<ObjectRpcChannelClient>,
    ) {
        let (_root, _paths, mut state, _local_node, target_node, endpoints) =
            relay_retry_test_fixture(name);
        set_member_public_key(
            &mut state,
            &target_node,
            crate::daemon_auth_encode_public_key(&[4_u8; 32]),
        );
        let bytes_read = Arc::new(AtomicUsize::new(0));
        let mut host = RecordingSignedHost {
            object_rpc_channel,
            stall_session_open: Some(Duration::from_secs(60)),
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::endpoint_only(&mut host);
        let body = b"one-pass-canonical-fallback".to_vec();
        let request = ObjectRpcRequest::from_encoded_body(
            "/test/unavailable-channel-fallback",
            "test",
            Vec::new(),
        );

        client
            .call_endpoint(
                &state,
                &endpoints[0],
                ObjectRpcSessionCall::new(
                    request,
                    ObjectRpcBodySource::from_reader(
                        CountingReader {
                            inner: Cursor::new(body.clone()),
                            bytes_read: Arc::clone(&bytes_read),
                        },
                        Some(body.len() as u64),
                    ),
                ),
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::none())
                    .with_overall_timeout(Duration::from_millis(100)),
            )
            .await
            .expect_err("canonical fallback open timeout");

        assert_eq!(host.channel_lookups, 1);
        assert_eq!(host.session_opens, 1);
        assert!(host.signed_routes.is_empty());
        assert_eq!(bytes_read.load(AtomicOrdering::Relaxed), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn absent_and_preclosed_channels_fall_back_without_consuming_one_pass_body() {
        assert_unavailable_channel_enters_canonical_without_consuming_body(
            "absent_channel_canonical_fallback",
            None,
        )
        .await;

        let channel = recording_channel(RecordingChannelAction::Respond {
            responder_node_id: None,
        });
        channel.close("closed before route lookup");
        assert_unavailable_channel_enters_canonical_without_consuming_body(
            "closed_channel_canonical_fallback",
            Some(channel.client.clone()),
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn oversized_typed_value_skips_channel_and_enters_canonical_open() {
        let (_root, _paths, mut state, _local_node, target_node, endpoints) =
            relay_retry_test_fixture("oversized_value_skips_channel");
        set_member_public_key(
            &mut state,
            &target_node,
            crate::daemon_auth_encode_public_key(&[7_u8; 32]),
        );
        let channel = recording_channel(RecordingChannelAction::Respond {
            responder_node_id: Some(target_node.clone()),
        });
        let mut host = RecordingSignedHost {
            object_rpc_channel: Some(channel.client.clone()),
            stall_session_open: Some(Duration::from_secs(60)),
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::endpoint_only(&mut host);
        let request = ObjectRpcRequest::from_encoded_body(
            "/test/oversized-value-skips-channel",
            "test",
            vec![5_u8; OBJECT_RPC_CHANNEL_MAX_MATERIALIZED_VALUE_BYTES + 1],
        );

        let error = client
            .call_endpoint(
                &state,
                &endpoints[0],
                ObjectRpcSessionCall::value(request),
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::none())
                    .with_overall_timeout(Duration::from_millis(100)),
            )
            .await
            .expect_err("oversized value canonical open timeout");

        assert!(matches!(error, NetworkError::Timeout { .. }));
        assert_eq!(host.channel_lookups, 0);
        assert_eq!(host.session_opens, 1);
        assert!(host.signed_routes.is_empty());
    }

    #[tokio::test]
    async fn post_enqueue_channel_failure_is_not_silently_replayed() {
        let (_root, paths, mut state, _local_node, target_node, _endpoints) =
            relay_retry_test_fixture("post_enqueue_channel_failure");
        set_member_public_key(
            &mut state,
            &target_node,
            crate::daemon_auth_encode_public_key(&[6_u8; 32]),
        );
        crate::write_object_file(&paths.state, &state).expect("rewrite state");
        let mut channel = recording_channel(RecordingChannelAction::CloseAfterAccept);
        let mut host = RecordingSignedHost {
            object_rpc_channel: Some(channel.client.clone()),
            ..RecordingSignedHost::default()
        };
        let runtime = tokio::runtime::Handle::current();
        let mut client = ClusterObjectRpcClient::routed(&mut host, &runtime, &paths);
        let request = ObjectRpcRequest::encode_body(
            "/test/post-enqueue-channel-failure",
            "test",
            &"mutation",
        )
        .expect("request")
        .with_operation_id("post-enqueue-channel-failure");

        let error = client
            .call_target(
                &state,
                &target_node,
                ObjectRpcSessionCall::value(request),
                ClusterObjectRpcOptions::default(),
            )
            .await
            .expect_err("post-enqueue close must make outcome unknown");

        assert!(matches!(error, NetworkError::OutcomeUnknown { .. }));
        assert_eq!(host.channel_lookups, 1);
        assert_eq!(host.session_opens, 0);
        assert!(host.signed_routes.is_empty());
        assert_eq!(
            channel
                .requests
                .recv()
                .await
                .expect("one dispatched request")
                .route,
            "/test/post-enqueue-channel-failure"
        );
        assert!(
            channel.requests.try_recv().is_err(),
            "request must not replay"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn endpoint_channel_lookup_obeys_whole_call_deadline() {
        let (_root, _paths, mut state, _local_node, target_node, endpoints) =
            relay_retry_test_fixture("endpoint_channel_lookup_deadline");
        set_member_public_key(
            &mut state,
            &target_node,
            crate::daemon_auth_encode_public_key(&[5_u8; 32]),
        );
        let mut host = RecordingSignedHost {
            channel_lookup_stall: Some(Duration::from_secs(60)),
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::endpoint_only(&mut host);
        let started_at = tokio::time::Instant::now();
        let request = ObjectRpcRequest::encode_body(
            "/test/endpoint-channel-lookup-deadline",
            "test",
            &"request",
        )
        .expect("request");

        let error = client
            .call_endpoint(
                &state,
                &endpoints[0],
                ObjectRpcSessionCall::value(request),
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::none())
                    .with_overall_timeout(Duration::from_millis(100)),
            )
            .await
            .expect_err("channel lookup deadline");

        assert!(matches!(
            error,
            NetworkError::Timeout {
                delivery_state: NetworkDeliveryState::DefinitelyNotSent,
                ..
            }
        ));
        assert_eq!(
            tokio::time::Instant::now().duration_since(started_at),
            Duration::from_millis(100)
        );
        assert_eq!(host.session_opens, 0);
        assert!(host.signed_routes.is_empty());
    }

    #[tokio::test]
    async fn relay_channel_requires_exact_target_responder_attestation() {
        let (_root, paths, state, _relay_node, target_node, _relay_endpoint) =
            relay_channel_test_fixture("relay_channel_missing_attestation");
        let channel = recording_channel(RecordingChannelAction::Respond {
            responder_node_id: None,
        });
        let mut host = RecordingSignedHost {
            object_rpc_channel: Some(channel.client.clone()),
            ..RecordingSignedHost::default()
        };
        let runtime = tokio::runtime::Handle::current();
        let mut client = ClusterObjectRpcClient::routed(&mut host, &runtime, &paths);
        let request = ObjectRpcRequest::encode_body(
            "/test/relay-channel-missing-attestation",
            "test",
            &"request",
        )
        .expect("request");

        let error = client
            .call_target(
                &state,
                &target_node,
                ObjectRpcSessionCall::value(request),
                ClusterObjectRpcOptions::default(),
            )
            .await
            .expect_err("missing target channel attestation");

        assert!(matches!(error, NetworkError::OutcomeUnknown { .. }));
        assert_eq!(host.signed_routes.len(), 1);
        assert_eq!(host.session_opens, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn endpoint_session_whole_call_deadline_covers_signing() {
        let (_root, _paths, mut state, _local_node, target_node, endpoints) =
            relay_retry_test_fixture("endpoint_session_sign_deadline");
        state
            .members
            .iter_mut()
            .find(|member| member.node_id == target_node)
            .expect("target member")
            .node_public_key = crate::daemon_auth_encode_public_key(&[9_u8; 32]);
        let request = ObjectRpcRequest::encode_body("/test/session-sign-deadline", "test", &"head")
            .expect("request");
        let mut host = RecordingSignedHost {
            stall_first_sign: Some(Duration::from_secs(60)),
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::endpoint_only(&mut host);
        let started_at = tokio::time::Instant::now();

        let error = client
            .call_endpoint(
                &state,
                &endpoints[0],
                ObjectRpcSessionCall::value(request),
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::none())
                    .with_overall_timeout(Duration::from_millis(100)),
            )
            .await
            .expect_err("session signing must be bounded by the whole-call deadline");

        assert!(
            matches!(
                &error,
                NetworkError::Timeout {
                    phase: NetworkTimeoutPhase::Signing,
                    delivery_state: NetworkDeliveryState::DefinitelyNotSent,
                    ..
                }
            ),
            "unexpected session signing deadline error: {error:?}"
        );
        assert_eq!(
            tokio::time::Instant::now().duration_since(started_at),
            Duration::from_millis(100)
        );
        assert_eq!(host.session_peers.len(), 1);
    }

    #[tokio::test]
    async fn loopback_session_uses_local_identity_without_cluster_state() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-pre-cluster-loopback-session");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir layout");
        assert!(
            !ClusterPaths::new(&data_dir).state.exists(),
            "fixture must remain a pre-cluster orphan"
        );
        let request =
            ObjectRpcRequest::encode_body("/test/pre-cluster-loopback-session", "test", &"request")
                .expect("request");
        let mut host = RecordingSignedHost {
            session_server_data_dir: Some(data_dir.clone()),
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::endpoint_only(&mut host);

        let result = client
            .call_loopback(
                &data_dir,
                "127.0.0.1:21353",
                ObjectRpcSessionCall::value(request),
                ClusterObjectRpcOptions::default(),
            )
            .await
            .expect("pre-cluster loopback session");

        assert_eq!(result.response.route, "/test/pre-cluster-loopback-session");
        let body: String = result.response.decode_body().expect("response body");
        assert_eq!(body, "loopback-ok");
        drop(client);
        assert_eq!(host.signed_requests.len(), 1);
    }

    #[tokio::test]
    async fn loopback_session_restores_arbitrary_value_response_at_origin() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-large-loopback-response");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data dir layout");
        let expected = vec![0x5a_u8; 237_451];
        let request =
            ObjectRpcRequest::encode_body("/test/large-loopback-response", "test", &"request")
                .expect("request");
        let mut host = RecordingSignedHost {
            session_server_data_dir: Some(data_dir.clone()),
            session_response_body: Some(expected.clone()),
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::endpoint_only(&mut host);

        let result = client
            .call_loopback(
                &data_dir,
                "127.0.0.1:21353",
                ObjectRpcSessionCall::value(request),
                ClusterObjectRpcOptions::default(),
            )
            .await
            .expect("large loopback response");

        assert_eq!(result.response.body, expected);
        assert!(result.body.is_empty());
    }

    #[tokio::test]
    async fn loopback_session_rejects_non_loopback_before_opening_transport() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-non-loopback-local-session");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        let request =
            ObjectRpcRequest::encode_body("/test/not-loopback", "test", &()).expect("request");
        let mut host = RecordingSignedHost::default();
        let mut client = ClusterObjectRpcClient::endpoint_only(&mut host);

        let error = client
            .call_loopback(
                &data_dir,
                "10.0.0.8:21353",
                ObjectRpcSessionCall::value(request),
                ClusterObjectRpcOptions::default(),
            )
            .await
            .expect_err("non-loopback must require cluster authorization");

        assert!(matches!(error, NetworkError::Io { .. }));
        drop(client);
        assert!(host.session_peers.is_empty());
        assert!(host.signed_requests.is_empty());
    }

    #[test]
    fn cluster_object_rpc_defaults_use_shared_network_progress_timeout() {
        let options = ClusterObjectRpcOptions::default();

        assert_eq!(options.timeouts, DaemonRpcTimeouts::default());
        assert_eq!(
            options.timeouts.connect_timeout,
            Some(crate::DEFAULT_NETWORK_TIMEOUT)
        );
        assert_eq!(
            options.timeouts.idle_timeout,
            Some(crate::DEFAULT_NETWORK_TIMEOUT)
        );
    }

    #[test]
    fn relay_envelope_round_trips_hop_timeout() {
        let cluster_id =
            ClusterId::from_legacy_named("cluster_relay_hop_timeout").expect("cluster id");
        let source_node =
            NodeId::from_legacy_named("node_relay_hop_timeout_source").expect("source node");
        let target_node =
            NodeId::from_legacy_named("node_relay_hop_timeout_target").expect("target node");
        let request =
            ObjectRpcRequest::encode_body("/test/data", "test", &"payload").expect("request");
        let outer = relay_request_for_signed_rpc_with_max_hop_timeout(
            &cluster_id,
            &target_node,
            &request,
            b"auth".to_vec(),
            std::iter::once(source_node.clone()),
            &[source_node, target_node.clone()],
            &[],
            Some(Duration::from_secs(20)),
        )
        .expect("relay request");
        let envelope = RelayEnvelope::from_request(&outer).expect("relay envelope");

        assert_eq!(envelope.max_hop_timeout_ms, Some(20_000));
        let mut oversized = envelope.clone();
        oversized.max_hop_timeout_ms = Some(u64::MAX);
        assert_eq!(
            relay_envelope_hop_timeout(&oversized),
            Some(Duration::from_millis(u64::MAX)),
            "an authenticated explicit deadline must survive the relay envelope exactly"
        );
    }

    #[test]
    fn session_relay_response_markers_precede_generic_routing_and_delivery() {
        let request =
            ObjectRpcRequest::encode_body("/test/session-relay-disposition", "test", &"mutate")
                .expect("request")
                .with_operation_id("operation-session-relay-disposition")
                .with_auth(b"origin-auth".to_vec());
        let error = relay_test_delivery_error(NetworkDeliveryState::PossiblyApplied);

        let mut outcome_unknown =
            cluster_relay_outcome_unknown_response(&request, &error).expect("outcome response");
        outcome_unknown.mark_responder_node_id("node_wrong_responder");
        assert_eq!(
            cluster_relay_session_response_disposition(&outcome_unknown),
            ClusterRelaySessionResponseDisposition::OutcomeUnknown,
            "the terminal marker must win even though its response uses the inner route"
        );

        let fresh = cluster_relay_fresh_signature_retry_response(
            &request.clone().with_response_retry_safe(),
            &error,
        )
        .expect("fresh-signature response");
        assert_eq!(
            cluster_relay_session_response_disposition(&fresh),
            ClusterRelaySessionResponseDisposition::FreshOriginSignature,
            "the retry marker must win over the generic relay error route"
        );

        let routing = ObjectRpcResponse::encode_error(CLUSTER_RELAY_ROUTE, "no route")
            .expect("routing response");
        assert_eq!(
            cluster_relay_session_response_disposition(&routing),
            ClusterRelaySessionResponseDisposition::RoutingError
        );
    }

    #[test]
    fn session_relay_requires_target_attestation_only_for_delivered_responses() {
        let delivered =
            ObjectRpcResponse::encode_body("/test/session-relay-target-proof", &"delivered")
                .expect("delivered response");
        assert!(cluster_relay_session_response_origin_is_acceptable(
            &delivered, true,
        ));
        assert!(!cluster_relay_session_response_origin_is_acceptable(
            &delivered, false,
        ));

        let routing = ObjectRpcResponse::encode_error(CLUSTER_RELAY_ROUTE, "no route")
            .expect("routing response");
        assert!(cluster_relay_session_response_origin_is_acceptable(
            &routing, false,
        ));
    }

    #[test]
    fn canonical_forward_relay_stamps_verified_target_responder() {
        let cluster_id =
            ClusterId::from_legacy_named("cluster_canonical_forward_target").expect("cluster id");
        let target_node =
            NodeId::from_legacy_named("node_canonical_forward_target").expect("target node");
        let inner =
            ObjectRpcRequest::encode_body("/test/canonical-forward-target", "test", &"request")
                .expect("inner request")
                .with_operation_id("operation-canonical-forward-target")
                .with_auth(b"origin-auth".to_vec());
        let envelope = RelayEnvelope::new(cluster_id, target_node.clone(), inner);
        let mut response =
            ObjectRpcResponse::encode_body("/test/canonical-forward-target", &"response")
                .expect("response");
        response.mark_responder_node_id("node_forwarding_relay");
        let mut result = ObjectRpcSessionResult::value(response);

        validate_and_normalize_forwarded_relay_result(&envelope, false, &mut result)
            .expect("verified canonical target response");

        assert_eq!(
            result.response.responder_node_id(),
            Some(target_node.as_str()),
            "a canonical final-hop response must retain the signed target identity when returned through an upstream pooled channel"
        );
    }

    #[test]
    fn session_relay_forward_failure_preserves_domain_retry_safety() {
        let cluster_id =
            ClusterId::from_legacy_named("cluster_session_relay_failure").expect("cluster id");
        let target_node =
            NodeId::from_legacy_named("node_session_relay_failure_target").expect("target node");
        let unsafe_inner =
            ObjectRpcRequest::encode_body("/test/session-relay-failure", "test", &"mutate")
                .expect("inner request")
                .with_operation_id("operation-session-relay-failure")
                .with_auth(b"origin-auth".to_vec());
        let unsafe_envelope = RelayEnvelope::new(
            cluster_id.clone(),
            target_node.clone(),
            unsafe_inner.clone(),
        );
        let ambiguous = relay_test_delivery_error(NetworkDeliveryState::PossiblyApplied);

        let terminal = cluster_relay_session_forward_failure_result(&unsafe_envelope, &ambiguous)
            .expect("terminal relay result");
        assert_eq!(
            cluster_relay_session_response_disposition(&terminal.response),
            ClusterRelaySessionResponseDisposition::OutcomeUnknown,
            "an unsafe possibly-applied request must never be exposed as a retryable route miss"
        );
        assert_eq!(
            terminal
                .response
                .metadata
                .get(crate::rpc::OBJECT_RPC_OPERATION_ID_METADATA_KEY)
                .map(String::as_str),
            Some("operation-session-relay-failure"),
        );

        let safe_envelope = RelayEnvelope::new(
            cluster_id,
            target_node,
            unsafe_inner.with_response_retry_safe(),
        );
        let fresh = cluster_relay_session_forward_failure_result(&safe_envelope, &ambiguous)
            .expect("retry-safe relay result");
        assert_eq!(
            cluster_relay_session_response_disposition(&fresh.response),
            ClusterRelaySessionResponseDisposition::FreshOriginSignature
        );

        let definitely_unsent = relay_test_delivery_error(NetworkDeliveryState::DefinitelyNotSent);
        let routing =
            cluster_relay_session_forward_failure_result(&unsafe_envelope, &definitely_unsent)
                .expect("definitely-unsent relay result");
        assert_eq!(
            cluster_relay_session_response_disposition(&routing.response),
            ClusterRelaySessionResponseDisposition::RoutingError
        );
    }

    fn route_test_connectivity_record(
        cluster_id: &ClusterId,
        source: &NodeId,
        target: &NodeId,
        endpoint_name: &str,
        address: &str,
        state: ClusterConnectivityState,
    ) -> ClusterConnectivityRecord {
        let now = OffsetDateTime::now_utc();
        let healthy = state == ClusterConnectivityState::Healthy;
        ClusterConnectivityRecord {
            cluster_id: cluster_id.clone(),
            source_node_id: source.clone(),
            target_node_id: target.clone(),
            endpoint_id: ClusterEndpointId::from_legacy_named(endpoint_name).expect("endpoint"),
            transport: ClusterTransportKind::LanTcp,
            address: address.to_string(),
            state,
            latency_ms: if healthy { Some(4) } else { None },
            consecutive_failures: if healthy { 0 } else { 1 },
            last_success_at: if healthy {
                Some("ok".to_string())
            } else {
                None
            },
            last_failure_at: if healthy {
                None
            } else {
                Some("failed".to_string())
            },
            failure_reason: None,
            observed_at: now,
            expires_at: now + time::Duration::seconds(900),
        }
    }

    #[test]
    fn cluster_route_next_hop_uses_degraded_live_edges() {
        let cluster_id =
            ClusterId::from_legacy_named("cluster_route_degraded_edges").expect("cluster");
        let local = NodeId::from_legacy_named("node_route_degraded_local").expect("local node");
        let bridge = NodeId::from_legacy_named("node_route_degraded_bridge").expect("bridge node");
        let target = NodeId::from_legacy_named("node_route_degraded_target").expect("target node");
        let graph = ClusterConnectivityGraph::from_records(
            local.clone(),
            vec![
                route_test_connectivity_record(
                    &cluster_id,
                    &local,
                    &bridge,
                    "endpoint_route_degraded_local_bridge",
                    "10.90.1.2:21353",
                    ClusterConnectivityState::Degraded,
                ),
                route_test_connectivity_record(
                    &cluster_id,
                    &bridge,
                    &target,
                    "endpoint_route_degraded_bridge_target",
                    "10.90.1.3:21353",
                    ClusterConnectivityState::Healthy,
                ),
            ],
        );

        let route = cluster_route_next_hop(&graph, &local, &target).expect("reachable route");

        assert_eq!(route.next_hop, bridge);
        assert_eq!(route.path, vec![local.clone(), bridge, target]);
        assert!(
            cluster_healthy_route_next_hop(&graph, &local, route.path.last().expect("target"))
                .is_none(),
            "probe suppression must still require a fully healthy alternate path"
        );
    }

    #[test]
    fn route_candidates_reject_relay_without_verified_path_to_target() {
        let local = NodeId::from_legacy_named("node_route_hairpin_origin").expect("local node");
        let neighbor =
            NodeId::from_legacy_named("node_route_hairpin_neighbor").expect("neighbor node");
        let target = NodeId::from_legacy_named("node_route_hairpin_target").expect("target node");
        let neighbor_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &neighbor,
            "10.92.0.2:21353".to_string(),
            0,
        );
        let mut graph = ClusterDeliveryRouteGraph::default();
        graph
            .push_edge_clone(&local, &neighbor, Some(&neighbor_endpoint), 1)
            .expect("local to neighbor");
        graph
            .push_edge_clone(&neighbor, &local, None, 1)
            .expect("neighbor to local");
        graph
            .push_edge_clone(&local, &target, None, 1)
            .expect("local to target");

        let candidates =
            cluster_delivery_route_candidates(&graph, &local, &target).expect("route candidates");

        assert!(
            candidates.is_empty(),
            "a relay candidate must have a graph-proven path to the target"
        );
    }

    #[test]
    fn route_candidates_use_relay_with_verified_path_to_target() {
        let local = NodeId::from_legacy_named("node_route_verified_origin").expect("local node");
        let bridge = NodeId::from_legacy_named("node_route_verified_bridge").expect("bridge node");
        let target = NodeId::from_legacy_named("node_route_verified_target").expect("target node");
        let bridge_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &bridge,
            "bridge.example.test:21353".to_string(),
            0,
        );
        let mut graph = ClusterDeliveryRouteGraph::default();
        graph
            .push_edge_clone(&local, &bridge, Some(&bridge_endpoint), 10)
            .expect("local to bridge");
        graph
            .push_edge_clone(&bridge, &target, None, 20)
            .expect("bridge to target");

        let candidates =
            cluster_delivery_route_candidates(&graph, &local, &target).expect("route candidates");

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].plan.next_hop, bridge);
        assert_eq!(
            candidates[0].path,
            vec![local.clone(), bridge.clone(), target.clone()]
        );
        assert_eq!(candidates[0].cost, 30);
        assert!(candidates[0].complete);
    }

    #[tokio::test(start_paused = true)]
    async fn relay_forward_carries_only_the_remaining_hop_budget() {
        let deadline = ClusterEndpointRpcDeadline::new(Some(Duration::from_secs(2)));
        tokio::time::advance(Duration::from_millis(750)).await;

        let remaining = cluster_relay_forward_remaining_timeout_ms(deadline)
            .expect("relay budget should remain")
            .expect("bounded relay deadline");

        assert_eq!(remaining, 1_250);
    }

    #[tokio::test(start_paused = true)]
    async fn relay_session_hop_deadline_covers_stream_open() {
        let (_root, paths, mut state, local_node, target_node, endpoints) =
            relay_retry_test_fixture("relay_session_open_deadline");
        let target_public_key = crate::daemon_auth_encode_public_key(&[7_u8; 32]);
        state
            .members
            .iter_mut()
            .find(|member| member.node_id == target_node)
            .expect("target member")
            .node_public_key = target_public_key;
        crate::write_object_file(&paths.state, &state).expect("write state");

        let inner =
            ObjectRpcRequest::encode_body("/test/relay-session-open-deadline", "test", &"mutate")
                .expect("inner request")
                .with_auth(b"origin-auth".to_vec());
        let mut envelope = RelayEnvelope::new(state.cluster_id.clone(), target_node, inner);
        envelope.planned_path = vec![local_node, envelope.target_node_id.clone()];
        envelope.target_endpoints = endpoints.to_vec();
        envelope.max_hop_timeout_ms = Some(100);
        let request = crate::ObjectRpcSessionRequest::value(
            envelope.into_request().expect("relay session request"),
        );
        let mut host = RecordingSignedHost {
            stall_session_open: Some(Duration::from_secs(60)),
            ..RecordingSignedHost::default()
        };
        let runtime = tokio::runtime::Handle::current();
        let mut client = ClusterObjectRpcClient::routed(&mut host, &runtime, &paths);
        let started_at = tokio::time::Instant::now();

        let error = client
            .forward_relay(&state, request, ClusterObjectRpcOptions::default())
            .await
            .expect_err("relay stream open must remain inside the inherited hop deadline");

        assert!(matches!(
            error,
            NetworkError::Timeout {
                phase: NetworkTimeoutPhase::Overall,
                delivery_state: NetworkDeliveryState::DefinitelyNotSent,
                ..
            }
        ));
        assert_eq!(
            tokio::time::Instant::now().duration_since(started_at),
            Duration::from_millis(100)
        );
        assert_eq!(host.session_peers.len(), 1);
    }

    #[test]
    fn relay_preserves_explicit_deadlines_longer_than_sixty_seconds() {
        let cluster_id =
            ClusterId::from_legacy_named("cluster_long_relay_deadline").expect("cluster id");
        let target = NodeId::from_legacy_named("node_long_relay_deadline").expect("node id");
        let inner = ObjectRpcRequest::from_encoded_body("/test/long-relay", "test", Vec::new());
        let mut envelope = RelayEnvelope::new(cluster_id, target, inner);
        envelope.max_hop_timeout_ms = Some(120_000);

        assert_eq!(
            relay_envelope_hop_timeout(&envelope),
            Some(Duration::from_secs(120))
        );
    }

    #[test]
    fn route_plan_uses_observed_local_connectivity_endpoint_when_member_endpoint_is_local_only() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-route-observed-first-hop");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_route_observed_first_hop").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_route_observed_first_hop_local").expect("local node");
        let target_node =
            NodeId::from_legacy_named("node_route_observed_first_hop_target").expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "route observed first hop",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut target_member = ClusterMemberRecord::local_voter(
            cluster_id,
            target_node.clone(),
            "target",
            "pk-target",
            now,
        );
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![crate::endpoint_advertisement::daemon_loopback_endpoint(
            &target_node,
            "127.0.0.1:21353".to_string(),
            10,
        )];
        state.members.push(target_member);

        let observed_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "node3:21353".to_string(),
            5,
        );
        let record = crate::connectivity::cluster_connectivity_record(
            &state,
            &observed_endpoint,
            ClusterConnectivityState::Healthy,
            Some(2),
            0,
            Some(now.to_string()),
            None,
            now,
            60,
        );
        crate::connectivity_store::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[record],
        )
        .expect("write local connectivity");

        let plan = cluster_delivery_route_plan(
            &paths,
            &state,
            &local_node,
            &target_node,
            &ClusterDeliveryFailedEdges::default(),
            &[],
        )
        .expect("route plan");

        assert_eq!(plan.next_hop, target_node);
        assert_eq!(plan.endpoints.len(), 1);
        assert_eq!(plan.endpoints[0].address, "node3:21353");
        assert_eq!(plan.endpoints[0].endpoint_id, observed_endpoint.endpoint_id);

        let planned_relay_endpoints = cluster_relay_planned_next_hop_endpoints(
            &paths,
            &state,
            &local_node,
            &target_node,
            None,
        )
        .expect("planned relay endpoints");
        assert_eq!(planned_relay_endpoints.len(), 1);
        assert_eq!(planned_relay_endpoints[0].address, "node3:21353");
        assert_eq!(
            planned_relay_endpoints[0].endpoint_id,
            observed_endpoint.endpoint_id
        );
    }

    #[tokio::test]
    async fn cached_route_plan_preserves_direct_before_tunnel_attempt_order() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-route-direct-before-tunnel");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_route_real_before_tunnel").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_route_real_before_tunnel_local").expect("local node");
        let target_node =
            NodeId::from_legacy_named("node_route_real_before_tunnel_target").expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "route real before tunnel",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let direct_endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named(
                "endpoint_route_real_before_tunnel_public",
            )
            .expect("endpoint"),
            node_id: target_node.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "198.51.100.76:21353".to_string(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::InternetDirect,
            priority: 0,
            last_observed_latency_ms: None,
        };
        let tunnel_endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named(
                "endpoint_route_real_before_tunnel_tunnel",
            )
            .expect("endpoint"),
            node_id: target_node.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: format!(
                "{}{}",
                crate::CLUSTER_TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX,
                target_node.as_str()
            ),
            source: ClusterEndpointSource::Observed,
            privacy: ClusterEndpointPrivacy::Relayed,
            priority: 0,
            last_observed_latency_ms: None,
        };
        let mut target_member = ClusterMemberRecord::local_voter(
            cluster_id,
            target_node.clone(),
            "target",
            "pk-target",
            now,
        );
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![direct_endpoint.clone()];
        state.members.push(target_member);
        crate::write_object_file(&paths.state, &state).expect("write state");
        crate::connectivity_store::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[
                crate::connectivity::cluster_connectivity_record(
                    &state,
                    &direct_endpoint,
                    // The path-cost route order would put the healthy tunnel
                    // first. RPC attempts deliberately prefer a usable direct
                    // endpoint, even when its latest probe is degraded.
                    ClusterConnectivityState::Degraded,
                    None,
                    1,
                    Some(now.to_string()),
                    None,
                    now,
                    60,
                ),
                crate::connectivity::cluster_connectivity_record(
                    &state,
                    &tunnel_endpoint,
                    ClusterConnectivityState::Healthy,
                    Some(0),
                    0,
                    Some(now.to_string()),
                    None,
                    now,
                    60,
                ),
            ],
        )
        .expect("write local connectivity");

        let plan = cluster_delivery_route_plan(
            &paths,
            &state,
            &local_node,
            &target_node,
            &ClusterDeliveryFailedEdges::default(),
            &[],
        )
        .expect("route plan");

        assert_eq!(plan.next_hop, target_node);
        let endpoint_ids = plan
            .endpoints
            .iter()
            .map(|endpoint| endpoint.endpoint_id.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            endpoint_ids,
            vec![
                direct_endpoint.endpoint_id.clone(),
                tunnel_endpoint.endpoint_id.clone()
            ]
        );

        let runtime = tokio::runtime::Handle::current();
        let cached = cluster_delivery_route_plan_async(
            &runtime,
            &paths,
            &state,
            &local_node,
            &target_node,
            ClusterDeliveryFailedEdges::default(),
            Vec::new(),
        )
        .await
        .expect("cached route plan")
        .expect("cached target route");
        let cached_endpoint_ids = cached
            .endpoints
            .iter()
            .map(|endpoint| endpoint.endpoint_id.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            cached_endpoint_ids,
            vec![
                direct_endpoint.endpoint_id.clone(),
                tunnel_endpoint.endpoint_id.clone()
            ],
            "the cached plan must retain the final endpoint-attempt order"
        );

        let failure_now = now + time::Duration::seconds(1);
        crate::connectivity_store::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[
                crate::connectivity::cluster_connectivity_record(
                    &state,
                    &direct_endpoint,
                    ClusterConnectivityState::Unreachable,
                    None,
                    crate::CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD,
                    None,
                    Some(failure_now.to_string()),
                    failure_now,
                    60,
                ),
                crate::connectivity::cluster_connectivity_record(
                    &state,
                    &tunnel_endpoint,
                    ClusterConnectivityState::Healthy,
                    Some(0),
                    0,
                    Some(failure_now.to_string()),
                    None,
                    failure_now,
                    60,
                ),
            ],
        )
        .expect("mark direct endpoint unreachable");

        let refreshed = cluster_delivery_route_plan_async(
            &runtime,
            &paths,
            &state,
            &local_node,
            &target_node,
            ClusterDeliveryFailedEdges::default(),
            Vec::new(),
        )
        .await
        .expect("refreshed route plan")
        .expect("tunnel fallback route");
        assert_eq!(refreshed.endpoints.len(), 1);
        assert_eq!(
            refreshed.endpoints[0].endpoint_id, tunnel_endpoint.endpoint_id,
            "a connectivity-generation change must still refresh the cached attempt order"
        );
    }

    #[tokio::test]
    async fn routing_table_hot_lease_skips_marks_then_refreshes_and_honors_invalidation() {
        let (_root, paths, state, local_node, target_node, _endpoints) =
            relay_retry_test_fixture("routing_table_validation_lease");
        let runtime = tokio::runtime::Handle::current();
        macro_rules! route_plan {
            () => {
                cluster_delivery_route_plan_async(
                    &runtime,
                    &paths,
                    &state,
                    &local_node,
                    &target_node,
                    ClusterDeliveryFailedEdges::default(),
                    Vec::new(),
                )
            };
        }

        route_plan!()
            .await
            .expect("initial route plan")
            .expect("initial route");
        let first_mark = CLUSTER_DELIVERY_ROUTING_TABLES
            .lock()
            .expect("routing table cache")
            .get(&paths.root)
            .expect("cached routing table")
            .mark
            .clone();

        let mut replacement = state.clone();
        replacement.cluster_name = "routing table replacement mark".to_string();
        replacement.updated_at += time::Duration::seconds(1);
        crate::write_object_file(&paths.state, &replacement).expect("replace state file");
        let replacement_mark = cluster_delivery_routing_table_cache_mark(&paths)
            .expect("replacement routing table mark");
        assert_ne!(first_mark, replacement_mark);
        CLUSTER_DELIVERY_ROUTING_TABLES
            .lock()
            .expect("routing table cache")
            .get_mut(&paths.root)
            .expect("cached routing table")
            .validated_at = Instant::now();

        route_plan!()
            .await
            .expect("leased route plan")
            .expect("leased route");
        assert_eq!(
            CLUSTER_DELIVERY_ROUTING_TABLES
                .lock()
                .expect("routing table cache")
                .get(&paths.root)
                .expect("cached routing table")
                .mark,
            first_mark,
            "a hot leased hit must not recompute the durable routing mark"
        );

        CLUSTER_DELIVERY_ROUTING_TABLES
            .lock()
            .expect("routing table cache")
            .get_mut(&paths.root)
            .expect("cached routing table")
            .validated_at = Instant::now()
            .checked_sub(CLUSTER_DELIVERY_ROUTING_TABLE_VALIDATION_LEASE)
            .expect("backdated routing lease");
        route_plan!()
            .await
            .expect("expired route plan")
            .expect("expired route");
        assert_eq!(
            CLUSTER_DELIVERY_ROUTING_TABLES
                .lock()
                .expect("routing table cache")
                .get(&paths.root)
                .expect("refreshed routing table")
                .mark,
            replacement_mark,
            "an expired lease must refresh a changed durable routing mark"
        );

        invalidate_cluster_delivery_routing_table_cache(&paths);
        assert!(
            !CLUSTER_DELIVERY_ROUTING_TABLES
                .lock()
                .expect("routing table cache")
                .contains_key(&paths.root),
            "explicit invalidation must remove a leased routing table immediately"
        );
        route_plan!()
            .await
            .expect("route after invalidation")
            .expect("route after invalidation");
        assert!(
            CLUSTER_DELIVERY_ROUTING_TABLES
                .lock()
                .expect("routing table cache")
                .contains_key(&paths.root)
        );
    }

    #[test]
    fn route_plan_prefers_healthy_direct_target_before_verified_relay() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-route-direct-before-relay");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_route_direct_before_relay").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_route_direct_before_relay_local").expect("local node");
        let relay_node =
            NodeId::from_legacy_named("node_route_direct_before_relay_relay").expect("relay node");
        let target_node = NodeId::from_legacy_named("node_route_direct_before_relay_target")
            .expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "route direct before relay",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let relay_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &relay_node,
            "relay-lan.example.test:21353".to_string(),
            0,
        );
        let direct_target_endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named(
                "endpoint_route_direct_before_relay_target",
            )
            .expect("target endpoint"),
            node_id: target_node.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "target-public.example.test:21353".to_string(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::InternetDirect,
            priority: 0,
            last_observed_latency_ms: None,
        };

        let mut relay_member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            relay_node.clone(),
            "relay",
            "pk-relay",
            now,
        );
        relay_member.capabilities = main_daemon_capabilities(&relay_member.role);
        relay_member.allowed_capabilities = relay_member.capabilities.clone();
        relay_member.endpoints = vec![relay_endpoint.clone()];
        state.members.push(relay_member);

        let mut target_member = ClusterMemberRecord::local_voter(
            cluster_id,
            target_node.clone(),
            "target",
            "pk-target",
            now,
        );
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![direct_target_endpoint.clone()];
        state.members.push(target_member);
        crate::write_object_file(&paths.state, &state).expect("write state");

        crate::connectivity_store::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[
                crate::connectivity::cluster_connectivity_record(
                    &state,
                    &direct_target_endpoint,
                    ClusterConnectivityState::Healthy,
                    Some(400),
                    0,
                    Some(now.to_string()),
                    None,
                    now,
                    60,
                ),
                crate::connectivity::cluster_connectivity_record(
                    &state,
                    &relay_endpoint,
                    ClusterConnectivityState::Healthy,
                    Some(1),
                    0,
                    Some(now.to_string()),
                    None,
                    now,
                    60,
                ),
            ],
        )
        .expect("write local connectivity");

        let mut relay_state = state.clone();
        relay_state.local_node_id = relay_node.clone();
        let relay_to_target = crate::connectivity::cluster_connectivity_record(
            &relay_state,
            &direct_target_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some(now.to_string()),
            None,
            now,
            60,
        );
        crate::connectivity_store::write_local_cluster_connectivity_record_set(
            &paths,
            &relay_node,
            &[relay_to_target],
        )
        .expect("write relay connectivity");

        let plan = cluster_delivery_route_plan(
            &paths,
            &state,
            &local_node,
            &target_node,
            &ClusterDeliveryFailedEdges::default(),
            &[],
        )
        .expect("route plan");

        assert_eq!(plan.next_hop, target_node);
        assert_eq!(plan.endpoints.len(), 1);
        assert_eq!(
            plan.endpoints[0].endpoint_id,
            direct_target_endpoint.endpoint_id
        );
    }

    #[test]
    fn route_plan_uses_synthetic_tcp_tunnel_when_it_is_the_only_endpoint() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-route-tunnel-only");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_route_tunnel_only").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_route_tunnel_only_local").expect("local node");
        let target_node =
            NodeId::from_legacy_named("node_route_tunnel_only_target").expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "route tunnel only",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let tunnel_endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_route_tunnel_only_tunnel")
                .expect("endpoint"),
            node_id: target_node.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: format!(
                "{}{}",
                crate::CLUSTER_TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX,
                target_node.as_str()
            ),
            source: ClusterEndpointSource::Observed,
            privacy: ClusterEndpointPrivacy::Relayed,
            priority: 0,
            last_observed_latency_ms: None,
        };
        let mut target_member = ClusterMemberRecord::local_voter(
            cluster_id,
            target_node.clone(),
            "target",
            "pk-target",
            now,
        );
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        state.members.push(target_member);
        crate::write_object_file(&paths.state, &state).expect("write state");
        crate::connectivity_store::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[crate::connectivity::cluster_connectivity_record(
                &state,
                &tunnel_endpoint,
                ClusterConnectivityState::Healthy,
                Some(0),
                0,
                Some(now.to_string()),
                None,
                now,
                60,
            )],
        )
        .expect("write local connectivity");

        let plan = cluster_delivery_route_plan(
            &paths,
            &state,
            &local_node,
            &target_node,
            &ClusterDeliveryFailedEdges::default(),
            &[],
        )
        .expect("route plan");

        assert_eq!(plan.next_hop, target_node);
        assert_eq!(plan.endpoints.len(), 1);
        assert_eq!(plan.endpoints[0].endpoint_id, tunnel_endpoint.endpoint_id);
    }

    #[test]
    fn route_plan_keeps_all_observed_main_endpoints_when_shadow_is_fastest() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-route-observed-main-endpoints");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_route_observed_main_endpoints")
            .expect("cluster id");
        let local_node = NodeId::from_legacy_named("node_route_observed_main_endpoints_local")
            .expect("local node");
        let target_node = NodeId::from_legacy_named("node_route_observed_main_endpoints_target")
            .expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "route observed main endpoints",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut target_member = ClusterMemberRecord::local_voter(
            cluster_id,
            target_node.clone(),
            "target",
            "pk-target",
            now,
        );
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        state.members.push(target_member);

        let shadow_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "target.example:21354".to_string(),
            0,
        );
        let private_main_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "lan-main.test:21353".to_string(),
            0,
        );
        let public_main_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "198.51.100.76:21353".to_string(),
            0,
        );
        let records = [
            crate::connectivity::cluster_connectivity_record(
                &state,
                &shadow_endpoint,
                ClusterConnectivityState::Healthy,
                Some(1),
                0,
                Some(now.to_string()),
                None,
                now,
                60,
            ),
            crate::connectivity::cluster_connectivity_record(
                &state,
                &private_main_endpoint,
                ClusterConnectivityState::Healthy,
                Some(2),
                0,
                Some(now.to_string()),
                None,
                now,
                60,
            ),
            crate::connectivity::cluster_connectivity_record(
                &state,
                &public_main_endpoint,
                ClusterConnectivityState::Healthy,
                Some(3),
                0,
                Some(now.to_string()),
                None,
                now,
                60,
            ),
        ];
        crate::connectivity_store::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &records,
        )
        .expect("write local connectivity");

        let plan = cluster_delivery_route_plan(
            &paths,
            &state,
            &local_node,
            &target_node,
            &ClusterDeliveryFailedEdges::default(),
            &[],
        )
        .expect("route plan");

        assert_eq!(plan.next_hop, target_node);
        let addresses = plan
            .endpoints
            .iter()
            .map(|endpoint| endpoint.address.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            addresses,
            vec!["lan-main.test:21353", "198.51.100.76:21353"]
        );
    }

    #[test]
    fn route_plan_keeps_degraded_main_endpoint_fallbacks_after_preferred_endpoint() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-route-degraded-fallbacks");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_route_degraded_fallbacks").expect("cluster");
        let local_node =
            NodeId::from_legacy_named("node_route_degraded_fallbacks_local").expect("local");
        let target_node =
            NodeId::from_legacy_named("node_route_degraded_fallbacks_target").expect("target");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "route degraded fallbacks",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut target_member = ClusterMemberRecord::local_voter(
            cluster_id,
            target_node.clone(),
            "target",
            "pk-target",
            now,
        );
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        let preferred_bad = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "foreign:21353".to_string(),
            0,
        );
        let degraded_hostname = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "node3:21353".to_string(),
            10,
        );
        let degraded_lan = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "172.26.0.4:21353".to_string(),
            20,
        );
        target_member.endpoints = vec![
            preferred_bad.clone(),
            degraded_hostname.clone(),
            degraded_lan.clone(),
        ];
        state.members.push(target_member);

        let records = [
            crate::connectivity::cluster_connectivity_record(
                &state,
                &preferred_bad,
                ClusterConnectivityState::Healthy,
                Some(0),
                0,
                Some(now.to_string()),
                None,
                now,
                60,
            ),
            crate::connectivity::cluster_connectivity_record(
                &state,
                &degraded_hostname,
                ClusterConnectivityState::Degraded,
                None,
                crate::CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD.saturating_sub(1),
                None,
                Some(now.to_string()),
                now,
                60,
            ),
            crate::connectivity::cluster_connectivity_record(
                &state,
                &degraded_lan,
                ClusterConnectivityState::Degraded,
                None,
                crate::CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD.saturating_sub(1),
                None,
                Some(now.to_string()),
                now,
                60,
            ),
        ];
        crate::connectivity_store::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &records,
        )
        .expect("write local connectivity");

        let plan = cluster_delivery_route_plan(
            &paths,
            &state,
            &local_node,
            &target_node,
            &ClusterDeliveryFailedEdges::default(),
            &[],
        )
        .expect("route plan");

        assert_eq!(plan.next_hop, target_node);
        let addresses = plan
            .endpoints
            .iter()
            .map(|endpoint| endpoint.address.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            addresses,
            vec!["foreign:21353", "node3:21353", "172.26.0.4:21353"]
        );
    }

    #[tokio::test]
    async fn async_route_plan_invalidates_cached_target_when_endpoint_becomes_unreachable() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-route-unreachable-fallback");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_route_unreachable_fallback").expect("cluster");
        let local_node =
            NodeId::from_legacy_named("node_route_unreachable_fallback_local").expect("local");
        let target_node =
            NodeId::from_legacy_named("node_route_unreachable_fallback_target").expect("target");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "route unreachable fallback",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut target_member = ClusterMemberRecord::local_voter(
            state.cluster_id.clone(),
            target_node.clone(),
            "target",
            "pk-target",
            now,
        );
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        let stale_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "target:21353".to_string(),
            0,
        );
        target_member.endpoints = vec![stale_endpoint.clone()];
        state.members.push(target_member);
        assert!(cluster_delivery_endpoint_is_active_builtin_probe_target(
            &state,
            &stale_endpoint
        ));
        let mut directory_only_state = state.clone();
        directory_only_state
            .members
            .iter_mut()
            .find(|member| member.node_id == target_node)
            .expect("target member")
            .endpoints
            .clear();
        assert!(
            !cluster_delivery_endpoint_is_active_builtin_probe_target(
                &directory_only_state,
                &stale_endpoint
            ),
            "a directory-only endpoint is not rediscoverable by membership maintenance"
        );
        let mut local_process_state = state.clone();
        local_process_state.local_node_id = target_node.clone();
        assert!(
            !cluster_delivery_endpoint_is_active_builtin_probe_target(
                &local_process_state,
                &stale_endpoint
            ),
            "the maintenance loop deliberately omits local-process members"
        );

        let healthy = crate::connectivity::cluster_connectivity_record(
            &state,
            &stale_endpoint,
            ClusterConnectivityState::Healthy,
            Some(4),
            0,
            Some(now.to_string()),
            None,
            now,
            7200,
        );
        crate::connectivity_store::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[healthy],
        )
        .expect("write healthy local connectivity");

        let runtime = tokio::runtime::Handle::current();
        let initial = cluster_delivery_route_plan_async(
            &runtime,
            &paths,
            &state,
            &local_node,
            &target_node,
            ClusterDeliveryFailedEdges::default(),
            Vec::new(),
        )
        .await
        .expect("initial route plan")
        .expect("healthy target route");
        assert_eq!(initial.next_hop, target_node);
        assert_eq!(initial.endpoints.len(), 1);

        let failure_now = now + time::Duration::seconds(1);
        let stale_failure = crate::connectivity::cluster_connectivity_record(
            &state,
            &stale_endpoint,
            ClusterConnectivityState::Unreachable,
            None,
            3,
            None,
            Some(failure_now.to_string()),
            failure_now,
            7200,
        );
        crate::connectivity_store::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[stale_failure],
        )
        .expect("write local connectivity");

        let plan = cluster_delivery_route_plan_async(
            &runtime,
            &paths,
            &state,
            &local_node,
            &target_node,
            ClusterDeliveryFailedEdges::default(),
            Vec::new(),
        )
        .await
        .expect("updated route plan");

        assert!(
            plan.is_none(),
            "cached application routes must invalidate when the target becomes unreachable"
        );

        let recovery_now = failure_now + time::Duration::seconds(1);
        let recovered = crate::connectivity::cluster_connectivity_record(
            &state,
            &stale_endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some(recovery_now.to_string()),
            None,
            recovery_now,
            7200,
        );
        crate::connectivity_store::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[recovered],
        )
        .expect("write recovered local connectivity");

        let recovered_plan = cluster_delivery_route_plan_async(
            &runtime,
            &paths,
            &state,
            &local_node,
            &target_node,
            ClusterDeliveryFailedEdges::default(),
            Vec::new(),
        )
        .await
        .expect("recovered route plan")
        .expect("rediscovered target route");
        assert_eq!(recovered_plan.next_hop, target_node);
        assert_eq!(recovered_plan.endpoints.len(), 1);
    }

    #[test]
    fn route_plan_does_not_use_unreachable_neighbor_as_relay_fallback() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-route-unreachable-relay");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_route_unreachable_relay").expect("cluster");
        let local_node =
            NodeId::from_legacy_named("node_route_unreachable_relay_local").expect("local");
        let target_node =
            NodeId::from_legacy_named("node_route_unreachable_relay_target").expect("target");
        let relay_node =
            NodeId::from_legacy_named("node_route_unreachable_relay_relay").expect("relay");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "route unreachable relay",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut target_member = ClusterMemberRecord::local_voter(
            state.cluster_id.clone(),
            target_node.clone(),
            "target",
            "pk-target",
            now,
        );
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "target:21353".to_string(),
            0,
        )];
        let mut relay_member = ClusterMemberRecord::local_voter(
            state.cluster_id.clone(),
            relay_node.clone(),
            "relay",
            "pk-relay",
            now,
        );
        relay_member.capabilities = main_daemon_capabilities(&relay_member.role);
        relay_member.allowed_capabilities = relay_member.capabilities.clone();
        let relay_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &relay_node,
            "relay:21353".to_string(),
            0,
        );
        relay_member.endpoints = vec![relay_endpoint.clone()];
        state.members.push(target_member);
        state.members.push(relay_member);

        let relay_failure = crate::connectivity::cluster_connectivity_record(
            &state,
            &relay_endpoint,
            ClusterConnectivityState::Unreachable,
            None,
            3,
            None,
            Some(now.to_string()),
            now,
            7200,
        );
        crate::connectivity_store::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[relay_failure],
        )
        .expect("write local connectivity");

        let mut failed_edges = ClusterDeliveryFailedEdges::default();
        failed_edges
            .try_insert(&local_node, &target_node)
            .expect("failed target edge");

        let plan = cluster_delivery_route_plan(
            &paths,
            &state,
            &local_node,
            &target_node,
            &failed_edges,
            &[],
        );

        assert!(
            plan.is_none(),
            "a known-unreachable neighbor must not become a relay fallback"
        );
    }

    #[test]
    fn route_plan_does_not_use_unproven_neighbor_as_relay_after_target_edge_fails() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-route-unproven-relay");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_route_unproven_relay").expect("cluster");
        let local_node =
            NodeId::from_legacy_named("node_route_unproven_relay_local").expect("local");
        let target_node =
            NodeId::from_legacy_named("node_route_unproven_relay_target").expect("target");
        let relay_node =
            NodeId::from_legacy_named("node_route_unproven_relay_neighbor").expect("relay");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "route unproven relay",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut target_member = ClusterMemberRecord::local_voter(
            state.cluster_id.clone(),
            target_node.clone(),
            "target",
            "pk-target",
            now,
        );
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "target.example.test:21353".to_string(),
            0,
        )];
        let mut relay_member = ClusterMemberRecord::local_voter(
            state.cluster_id.clone(),
            relay_node.clone(),
            "relay",
            "pk-relay",
            now,
        );
        relay_member.capabilities = main_daemon_capabilities(&relay_member.role);
        relay_member.allowed_capabilities = relay_member.capabilities.clone();
        let relay_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &relay_node,
            "relay.example.test:21353".to_string(),
            0,
        );
        relay_member.endpoints = vec![relay_endpoint.clone()];
        state.members.push(target_member);
        state.members.push(relay_member);

        let relay_reachable = crate::connectivity::cluster_connectivity_record(
            &state,
            &relay_endpoint,
            ClusterConnectivityState::Healthy,
            Some(5),
            0,
            Some(now.to_string()),
            None,
            now,
            60,
        );
        crate::connectivity_store::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[relay_reachable],
        )
        .expect("write local connectivity");

        let mut failed_edges = ClusterDeliveryFailedEdges::default();
        failed_edges
            .try_insert(&local_node, &target_node)
            .expect("failed target edge");

        let plan = cluster_delivery_route_plan(
            &paths,
            &state,
            &local_node,
            &target_node,
            &failed_edges,
            &[],
        );

        assert!(
            plan.is_none(),
            "a reachable neighbor without a graph path to the target must not become a relay fallback"
        );
    }

    #[test]
    fn route_plan_excludes_identity_mismatch_unreachable_target_endpoint() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-route-identity-mismatch");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_route_identity_mismatch").expect("cluster");
        let local_node =
            NodeId::from_legacy_named("node_route_identity_mismatch_local").expect("local");
        let target_node =
            NodeId::from_legacy_named("node_route_identity_mismatch_target").expect("target");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "route identity mismatch",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut target_member = ClusterMemberRecord::local_voter(
            state.cluster_id.clone(),
            target_node.clone(),
            "target",
            "pk-target",
            now,
        );
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        let mismatched_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "target:21353".to_string(),
            0,
        );
        target_member.endpoints = vec![mismatched_endpoint.clone()];
        state.members.push(target_member);

        let mut mismatch = crate::connectivity::cluster_connectivity_record(
            &state,
            &mismatched_endpoint,
            ClusterConnectivityState::Unreachable,
            None,
            3,
            None,
            Some(now.to_string()),
            now,
            7200,
        );
        mismatch.failure_reason = Some(crate::ClusterConnectivityFailureReason::IdentityMismatch);
        crate::connectivity_store::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[mismatch],
        )
        .expect("write local connectivity");

        let plan = cluster_delivery_route_plan(
            &paths,
            &state,
            &local_node,
            &target_node,
            &ClusterDeliveryFailedEdges::default(),
            &[],
        );

        assert!(
            plan.is_none(),
            "identity mismatch must remain a hard routing exclusion"
        );
    }

    #[test]
    fn relay_planned_target_endpoints_exclude_unreachable_builtin_routes() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-relay-target-connectivity");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_relay_target_connectivity").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_relay_target_connectivity_local").expect("local node");
        let target_node = NodeId::from_legacy_named("node_relay_target_connectivity_target")
            .expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay target connectivity",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut target_member = ClusterMemberRecord::local_voter(
            state.cluster_id.clone(),
            target_node.clone(),
            "target",
            "pk-target",
            now,
        );
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        let unreachable_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "unreachable-target.test:21353".to_string(),
            0,
        );
        let healthy_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "target:21353".to_string(),
            10,
        );
        target_member.endpoints = vec![unreachable_endpoint.clone(), healthy_endpoint.clone()];
        state.members.push(target_member);

        let unreachable = crate::connectivity::cluster_connectivity_record(
            &state,
            &unreachable_endpoint,
            ClusterConnectivityState::Unreachable,
            None,
            3,
            None,
            Some(now.to_string()),
            now,
            60,
        );
        let healthy = crate::connectivity::cluster_connectivity_record(
            &state,
            &healthy_endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some(now.to_string()),
            None,
            now,
            60,
        );
        crate::connectivity_store::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[unreachable, healthy],
        )
        .expect("write local connectivity");

        let planned = cluster_relay_planned_target_endpoints(
            &paths,
            &state,
            &local_node,
            &target_node,
            &[unreachable_endpoint.clone(), healthy_endpoint.clone()],
            None,
        )
        .expect("planned target endpoints");

        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].endpoint_id, healthy_endpoint.endpoint_id);
        assert_eq!(planned[0].address, "target:21353");
    }
}
