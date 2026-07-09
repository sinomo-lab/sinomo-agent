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
use crate::{
    ClusterConnectivityGraph, ClusterConnectivityRecord, ClusterConnectivityState, ClusterPaths,
    DaemonControlRequest, DaemonRpcTimeouts, NetworkError, ObjectRpcRequest, ObjectRpcResponse,
};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};
use time::OffsetDateTime;

/// RPC category for routing/relay traffic.
pub const CLUSTER_ROUTING_RPC_CATEGORY: &str = "cluster.routing";
/// Route an RPC is sent on when it is wrapped in a [`RelayEnvelope`] for forwarding.
pub const CLUSTER_RELAY_ROUTE: &str = "/cluster/relay";
pub const CLUSTER_ROUTING_RPC_ROUTE_METADATA: &[(&str, &str)] =
    &[(CLUSTER_RELAY_ROUTE, CLUSTER_ROUTING_RPC_CATEGORY)];
/// Default hop budget for a relayed RPC (roughly the cluster diameter). Each forward
/// decrements it; a relay is dropped when it reaches zero.
const CLUSTER_RELAY_DEFAULT_TTL: u8 = 8;
const CLUSTER_RELAY_MAX_FAILURE_DETAILS: usize = 8;
const CLUSTER_RELAY_FAILURE_DETAIL_MAX_CHARS: usize = 512;
pub const CLUSTER_TARGET_ENDPOINT_ATTEMPT_MAX: Duration = Duration::from_secs(5);
const CLUSTER_TARGET_ROUTE_DEADLINE_MAX: Duration = Duration::from_secs(5);
const CLUSTER_TARGET_UNVERIFIED_ENDPOINT_ATTEMPT_MAX: Duration = Duration::from_secs(2);
const CLUSTER_TARGET_VERIFIED_DIRECT_ENDPOINT_ATTEMPT_PREFERRED: Duration =
    Duration::from_millis(3500);
const CLUSTER_TARGET_VERIFIED_DIRECT_ENDPOINT_FALLBACK_RESERVE: Duration =
    Duration::from_millis(500);
const CLUSTER_DELIVERY_DEGRADED_LINK_COST: u64 = 50_000;
const CLUSTER_DELIVERY_UNOBSERVED_TARGET_COST: u64 = CLUSTER_DELIVERY_DEGRADED_LINK_COST / 2;
const CLUSTER_DELIVERY_UNOBSERVED_NEIGHBOR_COST: u64 = 75_000;
const CLUSTER_DELIVERY_UNREACHABLE_LINK_COST: u64 = 500_000;
// Synthetic TCP tunnels are fallback route evidence, not advertised sockets.
// Prefer any observed healthy direct endpoint first; a stale tunnel should not
// consume the fast app-facing budget when a direct public/LAN path is healthy.
const CLUSTER_DELIVERY_TCP_TUNNEL_PATH_COST: u64 = 12_000;
const MAX_CLUSTER_DELIVERY_ROUTING_TABLE_CACHE_ENTRIES: usize = 128;

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
    last_used: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterDeliveryRoutingTableCacheMark {
    state: ClusterFileMark,
    pub directory_version: Option<crate::directory::DirectoryVersion>,
    local_connectivity: ClusterFileMark,
    local_connectivity_generation: u64,
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
    cluster_delivery_select_route_candidate(
        &cluster_delivery_route_candidates(&graph, local_node_id, target_node_id)
            .ok()
            .unwrap_or_default(),
        failed_edges,
        visited,
    )
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

pub async fn cluster_delivery_route_plan_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    failed_edges: ClusterDeliveryFailedEdges,
    visited: Vec<NodeId>,
) -> Result<Option<ClusterDeliveryRoutePlan>, NetworkError> {
    cluster_delivery_route_plan_from_routing_table_async(
        runtime,
        paths,
        state,
        local_node_id,
        target_node_id,
        &failed_edges,
        &visited,
    )
    .await
}

async fn cluster_delivery_route_plan_from_routing_table_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    failed_edges: &ClusterDeliveryFailedEdges,
    visited: &[NodeId],
) -> Result<Option<ClusterDeliveryRoutePlan>, NetworkError> {
    if let Some(plan) = cluster_delivery_route_plan_from_cached_routing_table(
        paths,
        state,
        local_node_id,
        target_node_id,
        failed_edges,
        visited,
        OffsetDateTime::now_utc(),
    ) {
        return plan;
    }

    let paths = paths.clone();
    let state = state.clone();
    let local_node_id =
        try_clone_route_node_id(local_node_id, "cluster delivery route local node")?;
    let target_node_id =
        try_clone_route_node_id(target_node_id, "cluster delivery route target node")?;
    let failed_edges = failed_edges.try_clone()?;
    let visited = try_clone_route_node_ids(visited, "cluster delivery route visited nodes")?;
    runtime
        .spawn_blocking(move || {
            let before_mark = cluster_delivery_routing_table_cache_mark(&paths);
            let table = cluster_delivery_build_routing_table(&paths, &state, &local_node_id)?;
            let plan = table.route_plan(&target_node_id, &failed_edges, &visited)?;
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
        })?
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
        let candidates = cluster_delivery_route_candidates(&graph, local_node_id, &member.node_id)?;
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
        local_connectivity_generation:
            crate::connectivity_store::local_cluster_connectivity_content_generation(paths),
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
            last_used: cluster_delivery_routing_table_cache_tick(),
        },
    );
}

pub fn invalidate_cluster_delivery_routing_table_cache(paths: &ClusterPaths) {
    if let Ok(mut cache) = CLUSTER_DELIVERY_ROUTING_TABLES.lock() {
        cache.remove(&paths.root);
    }
}

fn record_cluster_delivery_endpoint_identity_mismatch(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
) {
    let _ = crate::connectivity_store::record_local_cluster_endpoint_command_connectivity(
        paths,
        state,
        endpoint,
        ClusterConnectivityState::Unreachable,
        None,
        false,
    );
    invalidate_cluster_delivery_routing_table_cache(paths);
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
            if endpoint.node_id != *target_node_id
                && matches!(record.state, ClusterConnectivityState::Unreachable)
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
) {
    endpoints.sort_by(|left, right| {
        (
            graph.unavailability_rank(left),
            cluster_delivery_endpoint_attempt_sort_rank(left),
            graph.quality_rank(left),
            cluster_delivery_endpoint_path_cost(&left.transport, &left.address, &left.privacy),
            left.priority,
            cluster_delivery_transport_sort_rank(&left.transport),
            left.address.as_str(),
        )
            .cmp(&(
                graph.unavailability_rank(right),
                cluster_delivery_endpoint_attempt_sort_rank(right),
                graph.quality_rank(right),
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
    let mut endpoints = Vec::new();
    let members = cluster_delivery_members_for_routing(paths, state)?;
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterRelayOptions {
    timeouts: DaemonRpcTimeouts,
}

impl ClusterRelayOptions {
    pub fn new(timeouts: DaemonRpcTimeouts) -> Self {
        Self { timeouts }
    }
}

pub trait ClusterRelayAsyncHost {
    fn try_deliver_local_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        inner_request: &'a ObjectRpcRequest,
        timeouts: DaemonRpcTimeouts,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ObjectRpcResponse>, NetworkError>> + Send + 'a>>;

    fn send_relay_request_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ObjectRpcRequest,
        timeouts: DaemonRpcTimeouts,
    ) -> Pin<Box<dyn Future<Output = Result<ObjectRpcResponse, NetworkError>> + Send + 'a>>;

    fn send_target_request_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ObjectRpcRequest,
        timeouts: DaemonRpcTimeouts,
    ) -> Pin<Box<dyn Future<Output = Result<ObjectRpcResponse, NetworkError>> + Send + 'a>> {
        self.send_relay_request_async(endpoint, request, timeouts)
    }
}

#[derive(Debug)]
pub enum ClusterRelayStreamSendOutcome {
    Forwarded,
    RoutingError(ObjectRpcResponse),
    StreamStartedThenFailed(NetworkError),
}

#[derive(Debug)]
pub enum ClusterRelayStreamForwardOutcome {
    Forwarded,
    ErrorResponse(ObjectRpcResponse),
    StreamStartedThenFailed(NetworkError),
}

#[derive(Debug)]
pub enum ClusterRelayStreamRequestAction {
    NotStreamed,
    DeliverLocal(ObjectRpcRequest),
    Forward,
}

pub fn cluster_relay_stream_request_action(
    local_node_id: &NodeId,
    request: &ObjectRpcRequest,
) -> Result<ClusterRelayStreamRequestAction, NetworkError> {
    let envelope = RelayEnvelope::from_request(request)?;
    if !relay_envelope_inner_expects_streamed_response(&envelope) {
        return Ok(ClusterRelayStreamRequestAction::NotStreamed);
    }
    if envelope.target_node_id == *local_node_id {
        Ok(ClusterRelayStreamRequestAction::DeliverLocal(
            envelope.into_inner(),
        ))
    } else {
        Ok(ClusterRelayStreamRequestAction::Forward)
    }
}

pub trait ClusterRelayStreamForwardAsyncHost {
    fn send_relay_stream_forward_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ObjectRpcRequest,
        timeouts: DaemonRpcTimeouts,
    ) -> Pin<
        Box<dyn Future<Output = Result<ClusterRelayStreamSendOutcome, NetworkError>> + Send + 'a>,
    >;
}

/// Forwards an already-authenticated streamed object-RPC request to one
/// concrete cluster endpoint and writes the streamed response to `upstream`.
///
/// This is the protocol-generic data-plane primitive used by relay code:
/// built-in TCP/loopback endpoints preserve the raw byte stream without
/// buffering the body, while network modules are dispatched through their
/// raw-response hook.
pub async fn forward_streamed_daemon_rpc_to_cluster_endpoint_async(
    endpoint: &ClusterEndpointRecord,
    request: &DaemonControlRequest,
    timeouts: DaemonRpcTimeouts,
    upstream: &mut crate::object::AsyncDaemonControlStream,
    upstream_address: &str,
    max_response_bytes: usize,
) -> Result<ClusterRelayStreamSendOutcome, NetworkError> {
    match endpoint.transport {
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback => {
            forward_streamed_daemon_rpc_to_tcp_endpoint_async(
                endpoint,
                request,
                timeouts,
                upstream,
                upstream_address,
                max_response_bytes,
            )
            .await
        }
        _ => {
            let response =
                crate::roaming::send_daemon_control_rpc_request_raw_response_to_cluster_endpoint_async(
                    endpoint, request, timeouts,
                )
                .await?;
            if relay_response_is_routing_error(&response) {
                return Ok(ClusterRelayStreamSendOutcome::RoutingError(response));
            }
            write_streamed_daemon_rpc_response_to_upstream(
                response,
                upstream,
                upstream_address,
                timeouts,
            )
            .await?;
            Ok(ClusterRelayStreamSendOutcome::Forwarded)
        }
    }
}

/// Signs and forwards a streamed object-RPC request to one concrete endpoint
/// through the caller's endpoint host.
///
/// This keeps stream transport selection on the same endpoint-delivery seam as
/// ordinary routed object RPC. Host implementations may reuse persistent
/// connections or module transports, but callers do not need transport-specific
/// branches.
pub async fn forward_signed_streamed_daemon_rpc_to_cluster_endpoint_with_host_async(
    host: &mut impl ClusterSignedObjectRpcAsyncHost,
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeouts: DaemonRpcTimeouts,
    upstream: &mut crate::object::AsyncDaemonControlStream,
    upstream_address: &str,
    max_response_bytes: usize,
) -> Result<ClusterRelayStreamSendOutcome, NetworkError> {
    let request_auth = host.sign_object_rpc_request_async(request).await?;
    host.forward_signed_endpoint_stream_rpc_async(
        endpoint,
        &request_auth,
        request,
        timeouts,
        upstream,
        upstream_address,
        max_response_bytes,
    )
    .await
}

async fn forward_streamed_daemon_rpc_to_tcp_endpoint_async(
    endpoint: &ClusterEndpointRecord,
    request: &DaemonControlRequest,
    timeouts: DaemonRpcTimeouts,
    upstream: &mut crate::object::AsyncDaemonControlStream,
    upstream_address: &str,
    max_response_bytes: usize,
) -> Result<ClusterRelayStreamSendOutcome, NetworkError> {
    let frame_options =
        crate::object::ObjectFrameIoOptions::none().with_idle_timeout(timeouts.idle_timeout);
    let mut downstream =
        crate::object::AsyncDaemonControlStream::connect_with_options(&endpoint.address, timeouts)
            .await?;
    downstream
        .write_request(&endpoint.address, request, frame_options)
        .await?;
    let response = downstream
        .read_rpc_response(&endpoint.address, max_response_bytes, frame_options)
        .await?;
    if relay_response_is_routing_error(&response) {
        return Ok(ClusterRelayStreamSendOutcome::RoutingError(response));
    }

    upstream
        .write_rpc_response(upstream_address, &response, frame_options)
        .await?;
    if crate::object_rpc_response_stream_mode(&response)
        == crate::ObjectRpcResponseStreamMode::RawStreamedDaemonBody
        && let Err(error) = downstream
            .forward_raw_byte_stream_chunks_to(
                &endpoint.address,
                upstream,
                upstream_address,
                crate::object::RawByteStreamChunkOptions {
                    chunk_bytes: crate::OBJECT_STREAM_MAX_CHUNK_BYTES,
                    max_total_bytes: u64::try_from(max_response_bytes).ok(),
                    idle_timeout: timeouts.idle_timeout,
                },
            )
            .await
    {
        return Ok(ClusterRelayStreamSendOutcome::StreamStartedThenFailed(
            error,
        ));
    }
    Ok(ClusterRelayStreamSendOutcome::Forwarded)
}

async fn write_streamed_daemon_rpc_response_to_upstream(
    mut response: ObjectRpcResponse,
    upstream: &mut crate::object::AsyncDaemonControlStream,
    upstream_address: &str,
    timeouts: DaemonRpcTimeouts,
) -> Result<(), NetworkError> {
    let frame_options =
        crate::object::ObjectFrameIoOptions::none().with_idle_timeout(timeouts.idle_timeout);
    let raw_body = if crate::object_rpc_response_stream_mode(&response)
        == crate::ObjectRpcResponseStreamMode::RawStreamedDaemonBody
    {
        Some(std::mem::take(&mut response.body))
    } else {
        None
    };
    upstream
        .write_rpc_response(upstream_address, &response, frame_options)
        .await?;
    if let Some(body) = raw_body {
        for chunk in body.chunks(crate::OBJECT_STREAM_MAX_CHUNK_BYTES) {
            upstream
                .write_raw_byte_stream_chunk(upstream_address, chunk, frame_options)
                .await?;
        }
        upstream
            .write_raw_byte_stream_chunk(upstream_address, &[], frame_options)
            .await?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterObjectRpcOptions {
    timeouts: DaemonRpcTimeouts,
}

impl ClusterObjectRpcOptions {
    pub fn new(timeouts: DaemonRpcTimeouts) -> Self {
        Self { timeouts }
    }
}

pub type ClusterSignedObjectRpcFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NetworkError>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClusterSignedObjectRpcObservationKind {
    Success,
    IoFailure,
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
        matches!(self.kind, ClusterSignedObjectRpcObservationKind::IoFailure)
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

    fn send_signed_object_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        auth: &'a [u8],
        request: &'a ObjectRpcRequest,
        timeouts: DaemonRpcTimeouts,
    ) -> ClusterSignedObjectRpcFuture<'a, ObjectRpcResponse> {
        Box::pin(async move {
            let started = Instant::now();
            let result = crate::roaming::send_daemon_rpc_to_cluster_endpoint_async(
                endpoint,
                Some(auth),
                request,
                timeouts,
            )
            .await;
            if let Some(observation) =
                cluster_signed_endpoint_rpc_observation(result.as_ref(), started)
            {
                let _ = self
                    .observe_signed_endpoint_rpc_async(endpoint, observation)
                    .await;
            }
            result
        })
    }

    fn send_signed_endpoint_stream_response_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        auth: &'a [u8],
        request: &'a ObjectRpcRequest,
        timeouts: DaemonRpcTimeouts,
    ) -> ClusterSignedObjectRpcFuture<'a, crate::ObjectRpcStreamResponse> {
        Box::pin(async move {
            let started = Instant::now();
            let mut signed = request.clone();
            signed.auth = Some(auth.to_vec());
            let result =
                crate::roaming::send_daemon_control_rpc_request_stream_response_to_cluster_endpoint_async(
                    endpoint,
                    &signed,
                    timeouts,
                )
                .await;
            if let Some(observation) =
                cluster_signed_endpoint_stream_response_rpc_observation(result.as_ref(), started)
            {
                let _ = self
                    .observe_signed_endpoint_rpc_async(endpoint, observation)
                    .await;
            }
            result
        })
    }

    fn forward_signed_endpoint_stream_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        auth: &'a [u8],
        request: &'a ObjectRpcRequest,
        timeouts: DaemonRpcTimeouts,
        upstream: &'a mut crate::object::AsyncDaemonControlStream,
        upstream_address: &'a str,
        max_response_bytes: usize,
    ) -> Pin<
        Box<dyn Future<Output = Result<ClusterRelayStreamSendOutcome, NetworkError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let started = Instant::now();
            let mut signed = request.clone();
            signed.auth = Some(auth.to_vec());
            let result = forward_streamed_daemon_rpc_to_cluster_endpoint_async(
                endpoint,
                &signed,
                timeouts,
                upstream,
                upstream_address,
                max_response_bytes,
            )
            .await;
            if let Some(observation) =
                cluster_signed_endpoint_stream_rpc_observation(result.as_ref(), started)
            {
                let _ = self
                    .observe_signed_endpoint_rpc_async(endpoint, observation)
                    .await;
            }
            result
        })
    }
}

fn cluster_signed_endpoint_rpc_observation(
    result: Result<&ObjectRpcResponse, &NetworkError>,
    started: Instant,
) -> Option<ClusterSignedObjectRpcObservation> {
    match result {
        Ok(response) => Some(cluster_signed_endpoint_response_observation(
            response, started,
        )),
        Err(error) => cluster_signed_endpoint_rpc_error_observation(error),
    }
}

fn cluster_signed_endpoint_stream_response_rpc_observation(
    result: Result<&crate::ObjectRpcStreamResponse, &NetworkError>,
    started: Instant,
) -> Option<ClusterSignedObjectRpcObservation> {
    match result {
        Ok(response) => Some(cluster_signed_endpoint_response_observation(
            &response.response,
            started,
        )),
        Err(error) => cluster_signed_endpoint_rpc_error_observation(error),
    }
}

fn cluster_signed_endpoint_response_observation(
    response: &ObjectRpcResponse,
    started: Instant,
) -> ClusterSignedObjectRpcObservation {
    match response.error_detail() {
        Err(NetworkError::AuthDenied) => ClusterSignedObjectRpcObservation::auth_denied(),
        _ => ClusterSignedObjectRpcObservation::success(Some(
            cluster_signed_endpoint_rpc_elapsed_latency_ms(started),
        )),
    }
}

fn cluster_signed_endpoint_stream_rpc_observation(
    result: Result<&ClusterRelayStreamSendOutcome, &NetworkError>,
    started: Instant,
) -> Option<ClusterSignedObjectRpcObservation> {
    match result {
        Ok(ClusterRelayStreamSendOutcome::Forwarded)
        | Ok(ClusterRelayStreamSendOutcome::RoutingError(_)) => {
            Some(ClusterSignedObjectRpcObservation::success(Some(
                cluster_signed_endpoint_rpc_elapsed_latency_ms(started),
            )))
        }
        Ok(ClusterRelayStreamSendOutcome::StreamStartedThenFailed(_)) => {
            Some(ClusterSignedObjectRpcObservation::io_failure())
        }
        Err(error) => cluster_signed_endpoint_rpc_error_observation(error),
    }
}

fn cluster_signed_endpoint_rpc_error_observation(
    error: &NetworkError,
) -> Option<ClusterSignedObjectRpcObservation> {
    match error {
        NetworkError::Io { .. } => Some(ClusterSignedObjectRpcObservation::io_failure()),
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
/// the network layer chooses the next hop. `call_endpoint` is intentionally a
/// raw explicit-endpoint send for bootstrap, probes, and other code that already
/// owns a concrete endpoint.
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
    pub async fn call_endpoint(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        options: ClusterObjectRpcOptions,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        self.call_endpoint_direct(endpoint, request, options).await
    }

    pub async fn call_endpoint_stream(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        options: ClusterObjectRpcOptions,
    ) -> Result<crate::ObjectRpcStreamResponse, NetworkError> {
        self.call_endpoint_stream_direct(endpoint, request, options)
            .await
    }

    pub async fn call_target(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        options: ClusterObjectRpcOptions,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        let Some(context) = self.route_context else {
            return Err(NetworkError::Io {
                operation: "cluster target rpc".to_string(),
                detail: "cluster target RPC requires a route context".to_string(),
            });
        };
        self.call_target_from_paths(
            context.runtime,
            context.paths,
            state,
            target_node_id,
            request,
            options,
        )
        .await
    }

    pub async fn call_target_stream(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        options: ClusterObjectRpcOptions,
    ) -> Result<crate::ObjectRpcStreamResponse, NetworkError> {
        let Some(context) = self.route_context else {
            return Err(NetworkError::Io {
                operation: "cluster target stream rpc".to_string(),
                detail: "cluster target stream RPC requires a route context".to_string(),
            });
        };
        self.call_target_stream_from_paths(
            context.runtime,
            context.paths,
            state,
            target_node_id,
            request,
            options,
        )
        .await
    }

    async fn call_endpoint_direct(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        options: ClusterObjectRpcOptions,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        let deadline = ClusterEndpointRpcDeadline::new(options.timeouts.connect_timeout);
        let Some(remaining) = deadline.checked_remaining() else {
            return Err(deadline.timeout_error(endpoint));
        };
        let request_auth = self.host.sign_object_rpc_request_async(request).await?;
        cluster_object_rpc_attempt_with_timeout(
            self.host.send_signed_object_rpc_async(
                endpoint,
                &request_auth,
                request,
                cluster_endpoint_transport_timeouts(options.timeouts, remaining),
            ),
            remaining,
            endpoint,
        )
        .await
    }

    async fn call_endpoint_stream_direct(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        options: ClusterObjectRpcOptions,
    ) -> Result<crate::ObjectRpcStreamResponse, NetworkError> {
        let deadline = ClusterEndpointRpcDeadline::new(options.timeouts.connect_timeout);
        let Some(remaining) = deadline.checked_remaining() else {
            return Err(deadline.timeout_error(endpoint));
        };
        let request_auth = self.host.sign_object_rpc_request_async(request).await?;
        cluster_object_rpc_attempt_with_timeout(
            self.host.send_signed_endpoint_stream_response_rpc_async(
                endpoint,
                &request_auth,
                request,
                cluster_endpoint_transport_timeouts(options.timeouts, remaining),
            ),
            remaining,
            endpoint,
        )
        .await
    }

    pub(crate) async fn call_target_from_paths(
        &mut self,
        runtime: &tokio::runtime::Handle,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        options: ClusterObjectRpcOptions,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        let traceparent = request.traceparent().unwrap_or_default().to_string();
        let call_started_at = Instant::now();
        let route_timeout = cluster_target_route_timeout(options.timeouts.connect_timeout);
        let deadline = ClusterEndpointRpcDeadline::new(route_timeout);
        let mut failed_edges = ClusterDeliveryFailedEdges::default();
        let mut failures = Vec::new();
        let max_route_attempts = state.members.len().saturating_mul(2).max(1) + 2;
        metrics::counter!(
            "sinomo.object_rpc.target.calls",
            "route" => request.route.clone(),
            "category" => request.category.clone()
        )
        .increment(1);
        tracing::debug!(
            target: "sinomo.network.object_rpc",
            %traceparent,
            route = %request.route,
            category = %request.category,
            %target_node_id,
            "cluster object rpc route start"
        );

        for _ in 0..max_route_attempts {
            let plan_started_at = Instant::now();
            let Some(route_plan) = cluster_delivery_route_plan_async(
                runtime,
                paths,
                state,
                &state.local_node_id,
                target_node_id,
                failed_edges.try_clone()?,
                Vec::new(),
            )
            .await?
            else {
                break;
            };
            let plan_us = cluster_object_rpc_elapsed_us(plan_started_at);
            metrics::histogram!(
                "sinomo.object_rpc.route_plan.us",
                "route" => request.route.clone(),
                "category" => request.category.clone()
            )
            .record(plan_us as f64);
            if route_plan.endpoints.is_empty() {
                failed_edges.try_insert(&state.local_node_id, &route_plan.next_hop)?;
                failures.push(format!("{}: no usable route", route_plan.next_hop));
                continue;
            }
            tracing::trace!(
                target: "sinomo.network.object_rpc",
                %traceparent,
                route = %request.route,
                category = %request.category,
                %target_node_id,
                next_hop = %route_plan.next_hop,
                plan_us,
                endpoints = %cluster_object_rpc_endpoint_trace_list(&route_plan.endpoints),
                "cluster object rpc route plan selected"
            );
            let local_connectivity = ClusterConnectivityGraph::try_read_for_local(paths, state)?;
            let mut route_endpoints = route_plan.endpoints;
            cluster_delivery_sort_route_endpoints_for_attempt(
                &local_connectivity,
                &mut route_endpoints,
            );
            let relay_target_endpoints = if &route_plan.next_hop == target_node_id {
                Vec::new()
            } else {
                cluster_delivery_target_endpoint_hints(paths, state, target_node_id)?
            };
            let relay_has_direct_target_fallback = relay_target_endpoints.iter().any(|endpoint| {
                cluster_delivery_local_endpoint_is_usable(&state.local_node_id, endpoint)
            });
            let route_endpoint_count = route_endpoints.len();
            for (endpoint_index, endpoint) in route_endpoints.into_iter().enumerate() {
                let attempt_started_at = Instant::now();
                let Some(remaining) = deadline.checked_remaining() else {
                    return Err(cluster_target_rpc_timeout_error(
                        target_node_id,
                        route_timeout,
                        &failures,
                    ));
                };
                let final_hop = &route_plan.next_hop == target_node_id;
                let endpoints_left = route_endpoint_count.saturating_sub(endpoint_index).max(1);
                let reserve_fallback_budget =
                    cluster_target_endpoint_should_reserve_fallback_budget(
                        &endpoint,
                        endpoints_left,
                    );
                let endpoint_attempt_budget = cluster_target_endpoint_attempt_budget_for_endpoint(
                    &local_connectivity,
                    &endpoint,
                    remaining,
                    endpoints_left,
                    route_endpoint_count,
                );
                let endpoint_attempt_budget = if final_hop {
                    endpoint_attempt_budget
                } else {
                    cluster_target_relay_endpoint_attempt_budget(
                        endpoint_attempt_budget,
                        remaining,
                        endpoints_left,
                        relay_has_direct_target_fallback,
                    )
                };
                let final_hop_verified_endpoint = final_hop
                    && cluster_endpoint_has_healthy_route_evidence(&local_connectivity, &endpoint)
                    && !reserve_fallback_budget;
                let final_hop_full_response_budget = final_hop
                    && !reserve_fallback_budget
                    && (endpoints_left <= 1 || final_hop_verified_endpoint)
                    && cluster_target_final_hop_request_uses_full_response_budget(request);
                let final_hop_attempt_budget = if final_hop {
                    cluster_target_final_hop_attempt_budget(
                        remaining,
                        endpoints_left,
                        endpoint_attempt_budget,
                        final_hop_verified_endpoint,
                        final_hop_full_response_budget,
                    )
                } else {
                    endpoint_attempt_budget
                };
                let hop_timeouts = if final_hop {
                    cluster_target_final_hop_transport_timeouts(
                        options.timeouts,
                        endpoint_attempt_budget,
                        final_hop_attempt_budget,
                        remaining,
                    )
                } else {
                    cluster_relay_hop_transport_timeouts(options.timeouts, endpoint_attempt_budget)
                };
                let attempt_timeout = final_hop_attempt_budget;
                let sign_started_at = Instant::now();
                let request_auth = self.host.sign_object_rpc_request_async(request).await?;
                let request_sign_us = cluster_object_rpc_elapsed_us(sign_started_at);
                metrics::histogram!(
                    "sinomo.object_rpc.sign.us",
                    "route" => request.route.clone(),
                    "category" => request.category.clone()
                )
                .record(request_sign_us as f64);
                let (outer_build_us, outer_sign_us, send_us, response) = if final_hop {
                    let send_started_at = Instant::now();
                    let response = cluster_object_rpc_attempt_with_timeout(
                        self.host.send_signed_object_rpc_async(
                            &endpoint,
                            &request_auth,
                            request,
                            hop_timeouts,
                        ),
                        attempt_timeout,
                        &endpoint,
                    )
                    .await;
                    (
                        0,
                        0,
                        cluster_object_rpc_elapsed_us(send_started_at),
                        response,
                    )
                } else {
                    let outer_build_started_at = Instant::now();
                    let outer = relay_request_for_signed_rpc_with_max_hop_timeout(
                        &state.cluster_id,
                        target_node_id,
                        request,
                        request_auth.clone(),
                        std::iter::once(state.local_node_id.clone()),
                        &route_plan.path,
                        &relay_target_endpoints,
                        route_timeout,
                    )?;
                    let outer_build_us = cluster_object_rpc_elapsed_us(outer_build_started_at);
                    let outer_sign_started_at = Instant::now();
                    let outer_auth = self.host.sign_object_rpc_request_async(&outer).await?;
                    let outer_sign_us = cluster_object_rpc_elapsed_us(outer_sign_started_at);
                    let send_started_at = Instant::now();
                    let response = cluster_object_rpc_attempt_with_timeout(
                        self.host.send_signed_object_rpc_async(
                            &endpoint,
                            &outer_auth,
                            &outer,
                            hop_timeouts,
                        ),
                        endpoint_attempt_budget,
                        &endpoint,
                    )
                    .await;
                    (
                        outer_build_us,
                        outer_sign_us,
                        cluster_object_rpc_elapsed_us(send_started_at),
                        response,
                    )
                };
                match response {
                    Ok(response) if relay_response_is_routing_error(&response) => {
                        metrics::counter!(
                            "sinomo.object_rpc.target.routing_errors",
                            "route" => request.route.clone(),
                            "category" => request.category.clone()
                        )
                        .increment(1);
                        tracing::debug!(
                            target: "sinomo.network.object_rpc",
                            %traceparent,
                            route = %request.route,
                            category = %request.category,
                            %target_node_id,
                            next_hop = %route_plan.next_hop,
                            endpoint = %endpoint.address,
                            transport = ?endpoint.transport,
                            final_hop,
                            "cluster object rpc relay returned routing error"
                        );
                        failures.push(format!(
                            "{} ({:?}) returned routing error: {}",
                            endpoint.address,
                            endpoint.transport,
                            relay_response_routing_error_detail(&response)
                        ));
                        break;
                    }
                    Ok(response) => {
                        if let Some(detail) =
                            cluster_object_rpc_retryable_endpoint_error_detail(&response)
                        {
                            metrics::histogram!(
                                "sinomo.object_rpc.target.attempt.us",
                                "route" => request.route.clone(),
                                "category" => request.category.clone(),
                                "outcome" => "endpoint_error"
                            )
                            .record(cluster_object_rpc_elapsed_us(attempt_started_at) as f64);
                            tracing::debug!(
                                target: "sinomo.network.object_rpc",
                                %traceparent,
                                route = %request.route,
                                category = %request.category,
                                %target_node_id,
                                next_hop = %route_plan.next_hop,
                                endpoint = %endpoint.address,
                                transport = ?endpoint.transport,
                                final_hop,
                                error = %detail,
                                "cluster object rpc endpoint returned retryable error"
                            );
                            failures.push(format!(
                                "{} ({:?}) returned endpoint error: {detail}",
                                endpoint.address, endpoint.transport
                            ));
                            continue;
                        }
                        if let Some(responder_node_id) =
                            cluster_object_rpc_wrong_responder_node_id(&response, target_node_id)
                        {
                            metrics::counter!(
                                "sinomo.object_rpc.target.responder_mismatch",
                                "route" => request.route.clone(),
                                "category" => request.category.clone()
                            )
                            .increment(1);
                            tracing::debug!(
                                target: "sinomo.network.object_rpc",
                                %traceparent,
                                route = %request.route,
                                category = %request.category,
                                %target_node_id,
                                next_hop = %route_plan.next_hop,
                                endpoint = %endpoint.address,
                                transport = ?endpoint.transport,
                                final_hop,
                                expected = %target_node_id,
                                got = %responder_node_id,
                                sign_us = request_sign_us,
                                plan_us,
                                outer_build_us,
                                outer_sign_us,
                                send_us,
                                attempt_us = cluster_object_rpc_elapsed_us(attempt_started_at),
                                total_us = cluster_object_rpc_elapsed_us(call_started_at),
                                "cluster object rpc responder mismatch"
                            );
                            let failure = if final_hop {
                                record_cluster_delivery_endpoint_identity_mismatch(
                                    paths, state, &endpoint,
                                );
                                format!(
                                    "{} ({:?}) responder mismatch: expected {}, got {}",
                                    endpoint.address,
                                    endpoint.transport,
                                    target_node_id,
                                    responder_node_id
                                )
                            } else {
                                format!(
                                    "{} ({:?}) relay responder mismatch: expected final {}, got {}",
                                    endpoint.address,
                                    endpoint.transport,
                                    target_node_id,
                                    responder_node_id
                                )
                            };
                            failures.push(failure);
                            continue;
                        }
                        let attempt_us = cluster_object_rpc_elapsed_us(attempt_started_at);
                        let total_us = cluster_object_rpc_elapsed_us(call_started_at);
                        metrics::histogram!(
                            "sinomo.object_rpc.target.attempt.us",
                            "route" => request.route.clone(),
                            "category" => request.category.clone(),
                            "outcome" => "ok"
                        )
                        .record(attempt_us as f64);
                        metrics::histogram!(
                            "sinomo.object_rpc.target.total.us",
                            "route" => request.route.clone(),
                            "category" => request.category.clone(),
                            "outcome" => "ok"
                        )
                        .record(total_us as f64);
                        tracing::debug!(
                            target: "sinomo.network.object_rpc",
                            %traceparent,
                            route = %request.route,
                            category = %request.category,
                            %target_node_id,
                            next_hop = %route_plan.next_hop,
                            endpoint = %endpoint.address,
                            transport = ?endpoint.transport,
                            final_hop,
                            sign_us = request_sign_us,
                            plan_us,
                            outer_build_us,
                            outer_sign_us,
                            send_us,
                            attempt_us,
                            total_us,
                            "cluster object rpc delivered"
                        );
                        return Ok(response);
                    }
                    Err(error) => {
                        if cluster_object_rpc_error_is_route_attempt_timeout(&error) {
                            let _ = self
                                .host
                                .observe_signed_endpoint_rpc_async(
                                    &endpoint,
                                    ClusterSignedObjectRpcObservation::io_failure(),
                                )
                                .await;
                        }
                        let attempt_us = cluster_object_rpc_elapsed_us(attempt_started_at);
                        let total_us = cluster_object_rpc_elapsed_us(call_started_at);
                        metrics::histogram!(
                            "sinomo.object_rpc.target.attempt.us",
                            "route" => request.route.clone(),
                            "category" => request.category.clone(),
                            "outcome" => "error"
                        )
                        .record(attempt_us as f64);
                        metrics::histogram!(
                            "sinomo.object_rpc.target.total.us",
                            "route" => request.route.clone(),
                            "category" => request.category.clone(),
                            "outcome" => "error"
                        )
                        .record(total_us as f64);
                        tracing::debug!(
                            target: "sinomo.network.object_rpc",
                            %traceparent,
                            route = %request.route,
                            category = %request.category,
                            %target_node_id,
                            next_hop = %route_plan.next_hop,
                            endpoint = %endpoint.address,
                            transport = ?endpoint.transport,
                            final_hop,
                            sign_us = request_sign_us,
                            plan_us,
                            outer_build_us,
                            outer_sign_us,
                            send_us,
                            attempt_us,
                            total_us,
                            error = %error,
                            "cluster object rpc attempt failed"
                        );
                        failures.push(format!(
                            "{} ({:?}): {error}",
                            endpoint.address, endpoint.transport
                        ));
                    }
                }
            }
            failed_edges.try_insert(&state.local_node_id, &route_plan.next_hop)?;
        }

        Err(NetworkError::Io {
            operation: "cluster target rpc".to_string(),
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

    pub(crate) async fn call_target_stream_from_paths(
        &mut self,
        runtime: &tokio::runtime::Handle,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        options: ClusterObjectRpcOptions,
    ) -> Result<crate::ObjectRpcStreamResponse, NetworkError> {
        let traceparent = request.traceparent().unwrap_or_default().to_string();
        let call_started_at = Instant::now();
        let route_timeout = cluster_target_route_timeout(options.timeouts.connect_timeout);
        let deadline = ClusterEndpointRpcDeadline::new(route_timeout);
        let mut failed_edges = ClusterDeliveryFailedEdges::default();
        let mut failures = Vec::new();
        let max_route_attempts = state.members.len().saturating_mul(2).max(1) + 2;
        metrics::counter!(
            "sinomo.object_rpc.target.calls",
            "route" => request.route.clone(),
            "category" => request.category.clone()
        )
        .increment(1);
        tracing::debug!(
            target: "sinomo.network.object_rpc",
            %traceparent,
            route = %request.route,
            category = %request.category,
            %target_node_id,
            stream = true,
            "cluster object rpc stream route start"
        );

        for _ in 0..max_route_attempts {
            let plan_started_at = Instant::now();
            let Some(route_plan) = cluster_delivery_route_plan_async(
                runtime,
                paths,
                state,
                &state.local_node_id,
                target_node_id,
                failed_edges.try_clone()?,
                Vec::new(),
            )
            .await?
            else {
                break;
            };
            let plan_us = cluster_object_rpc_elapsed_us(plan_started_at);
            metrics::histogram!(
                "sinomo.object_rpc.route_plan.us",
                "route" => request.route.clone(),
                "category" => request.category.clone()
            )
            .record(plan_us as f64);
            if route_plan.endpoints.is_empty() {
                failed_edges.try_insert(&state.local_node_id, &route_plan.next_hop)?;
                failures.push(format!("{}: no usable route", route_plan.next_hop));
                continue;
            }
            tracing::trace!(
                target: "sinomo.network.object_rpc",
                %traceparent,
                route = %request.route,
                category = %request.category,
                %target_node_id,
                next_hop = %route_plan.next_hop,
                plan_us,
                endpoints = %cluster_object_rpc_endpoint_trace_list(&route_plan.endpoints),
                stream = true,
                "cluster object rpc stream route plan selected"
            );
            let local_connectivity = ClusterConnectivityGraph::try_read_for_local(paths, state)?;
            let mut route_endpoints = route_plan.endpoints;
            cluster_delivery_sort_route_endpoints_for_attempt(
                &local_connectivity,
                &mut route_endpoints,
            );
            let relay_target_endpoints = if &route_plan.next_hop == target_node_id {
                Vec::new()
            } else {
                cluster_delivery_target_endpoint_hints(paths, state, target_node_id)?
            };
            let relay_has_direct_target_fallback = relay_target_endpoints.iter().any(|endpoint| {
                cluster_delivery_local_endpoint_is_usable(&state.local_node_id, endpoint)
            });
            let route_endpoint_count = route_endpoints.len();
            for (endpoint_index, endpoint) in route_endpoints.into_iter().enumerate() {
                let attempt_started_at = Instant::now();
                let Some(remaining) = deadline.checked_remaining() else {
                    return Err(cluster_target_rpc_timeout_error(
                        target_node_id,
                        route_timeout,
                        &failures,
                    ));
                };
                let final_hop = &route_plan.next_hop == target_node_id;
                let endpoints_left = route_endpoint_count.saturating_sub(endpoint_index).max(1);
                let reserve_fallback_budget =
                    cluster_target_endpoint_should_reserve_fallback_budget(
                        &endpoint,
                        endpoints_left,
                    );
                let endpoint_attempt_budget = cluster_target_endpoint_attempt_budget_for_endpoint(
                    &local_connectivity,
                    &endpoint,
                    remaining,
                    endpoints_left,
                    route_endpoint_count,
                );
                let endpoint_attempt_budget = if final_hop {
                    endpoint_attempt_budget
                } else {
                    cluster_target_relay_endpoint_attempt_budget(
                        endpoint_attempt_budget,
                        remaining,
                        endpoints_left,
                        relay_has_direct_target_fallback,
                    )
                };
                let final_hop_verified_endpoint = final_hop
                    && cluster_endpoint_has_healthy_route_evidence(&local_connectivity, &endpoint)
                    && !reserve_fallback_budget;
                let final_hop_full_response_budget = final_hop
                    && !reserve_fallback_budget
                    && (endpoints_left <= 1 || final_hop_verified_endpoint);
                let final_hop_attempt_budget = if final_hop {
                    cluster_target_final_hop_attempt_budget(
                        remaining,
                        endpoints_left,
                        endpoint_attempt_budget,
                        final_hop_verified_endpoint,
                        final_hop_full_response_budget,
                    )
                } else {
                    endpoint_attempt_budget
                };
                let hop_timeouts = if final_hop {
                    cluster_target_final_hop_transport_timeouts(
                        options.timeouts,
                        endpoint_attempt_budget,
                        final_hop_attempt_budget,
                        remaining,
                    )
                } else {
                    cluster_relay_hop_transport_timeouts(options.timeouts, endpoint_attempt_budget)
                };
                let attempt_timeout = final_hop_attempt_budget;
                let sign_started_at = Instant::now();
                let request_auth = self.host.sign_object_rpc_request_async(request).await?;
                let request_sign_us = cluster_object_rpc_elapsed_us(sign_started_at);
                metrics::histogram!(
                    "sinomo.object_rpc.sign.us",
                    "route" => request.route.clone(),
                    "category" => request.category.clone()
                )
                .record(request_sign_us as f64);
                let (outer_build_us, outer_sign_us, send_us, response) = if final_hop {
                    let send_started_at = Instant::now();
                    let response = cluster_object_rpc_attempt_with_timeout(
                        self.host.send_signed_endpoint_stream_response_rpc_async(
                            &endpoint,
                            &request_auth,
                            request,
                            hop_timeouts,
                        ),
                        attempt_timeout,
                        &endpoint,
                    )
                    .await;
                    (
                        0,
                        0,
                        cluster_object_rpc_elapsed_us(send_started_at),
                        response,
                    )
                } else {
                    let outer_build_started_at = Instant::now();
                    let outer = relay_request_for_signed_rpc_with_max_hop_timeout(
                        &state.cluster_id,
                        target_node_id,
                        request,
                        request_auth.clone(),
                        std::iter::once(state.local_node_id.clone()),
                        &route_plan.path,
                        &relay_target_endpoints,
                        route_timeout,
                    )?;
                    let outer_build_us = cluster_object_rpc_elapsed_us(outer_build_started_at);
                    let outer_sign_started_at = Instant::now();
                    let outer_auth = self.host.sign_object_rpc_request_async(&outer).await?;
                    let outer_sign_us = cluster_object_rpc_elapsed_us(outer_sign_started_at);
                    let send_started_at = Instant::now();
                    let response = cluster_object_rpc_attempt_with_timeout(
                        self.host.send_signed_endpoint_stream_response_rpc_async(
                            &endpoint,
                            &outer_auth,
                            &outer,
                            hop_timeouts,
                        ),
                        endpoint_attempt_budget,
                        &endpoint,
                    )
                    .await;
                    (
                        outer_build_us,
                        outer_sign_us,
                        cluster_object_rpc_elapsed_us(send_started_at),
                        response,
                    )
                };
                match response {
                    Ok(response) if relay_response_is_routing_error(&response.response) => {
                        metrics::counter!(
                            "sinomo.object_rpc.target.routing_errors",
                            "route" => request.route.clone(),
                            "category" => request.category.clone()
                        )
                        .increment(1);
                        tracing::debug!(
                            target: "sinomo.network.object_rpc",
                            %traceparent,
                            route = %request.route,
                            category = %request.category,
                            %target_node_id,
                            next_hop = %route_plan.next_hop,
                            endpoint = %endpoint.address,
                            transport = ?endpoint.transport,
                            final_hop,
                            stream = true,
                            "cluster object rpc stream relay returned routing error"
                        );
                        failures.push(format!(
                            "{} ({:?}) returned routing error: {}",
                            endpoint.address,
                            endpoint.transport,
                            relay_response_routing_error_detail(&response.response)
                        ));
                        break;
                    }
                    Ok(response) => {
                        if let Some(detail) =
                            cluster_object_rpc_retryable_endpoint_error_detail(&response.response)
                        {
                            metrics::histogram!(
                                "sinomo.object_rpc.target.attempt.us",
                                "route" => request.route.clone(),
                                "category" => request.category.clone(),
                                "outcome" => "endpoint_error"
                            )
                            .record(cluster_object_rpc_elapsed_us(attempt_started_at) as f64);
                            tracing::debug!(
                                target: "sinomo.network.object_rpc",
                                %traceparent,
                                route = %request.route,
                                category = %request.category,
                                %target_node_id,
                                next_hop = %route_plan.next_hop,
                                endpoint = %endpoint.address,
                                transport = ?endpoint.transport,
                                final_hop,
                                stream = true,
                                error = %detail,
                                "cluster object rpc stream endpoint returned retryable error"
                            );
                            failures.push(format!(
                                "{} ({:?}) returned endpoint error: {detail}",
                                endpoint.address, endpoint.transport
                            ));
                            continue;
                        }
                        if let Some(responder_node_id) = cluster_object_rpc_wrong_responder_node_id(
                            &response.response,
                            target_node_id,
                        ) {
                            metrics::counter!(
                                "sinomo.object_rpc.target.responder_mismatch",
                                "route" => request.route.clone(),
                                "category" => request.category.clone()
                            )
                            .increment(1);
                            tracing::debug!(
                                target: "sinomo.network.object_rpc",
                                %traceparent,
                                route = %request.route,
                                category = %request.category,
                                %target_node_id,
                                next_hop = %route_plan.next_hop,
                                endpoint = %endpoint.address,
                                transport = ?endpoint.transport,
                                final_hop,
                                expected = %target_node_id,
                                got = %responder_node_id,
                                sign_us = request_sign_us,
                                plan_us,
                                outer_build_us,
                                outer_sign_us,
                                send_us,
                                attempt_us = cluster_object_rpc_elapsed_us(attempt_started_at),
                                total_us = cluster_object_rpc_elapsed_us(call_started_at),
                                stream = true,
                                "cluster object rpc stream responder mismatch"
                            );
                            let failure = if final_hop {
                                record_cluster_delivery_endpoint_identity_mismatch(
                                    paths, state, &endpoint,
                                );
                                format!(
                                    "{} ({:?}) responder mismatch: expected {}, got {}",
                                    endpoint.address,
                                    endpoint.transport,
                                    target_node_id,
                                    responder_node_id
                                )
                            } else {
                                format!(
                                    "{} ({:?}) relay responder mismatch: expected final {}, got {}",
                                    endpoint.address,
                                    endpoint.transport,
                                    target_node_id,
                                    responder_node_id
                                )
                            };
                            failures.push(failure);
                            continue;
                        }
                        let attempt_us = cluster_object_rpc_elapsed_us(attempt_started_at);
                        let total_us = cluster_object_rpc_elapsed_us(call_started_at);
                        metrics::histogram!(
                            "sinomo.object_rpc.target.attempt.us",
                            "route" => request.route.clone(),
                            "category" => request.category.clone(),
                            "outcome" => "ok"
                        )
                        .record(attempt_us as f64);
                        metrics::histogram!(
                            "sinomo.object_rpc.target.total.us",
                            "route" => request.route.clone(),
                            "category" => request.category.clone(),
                            "outcome" => "ok"
                        )
                        .record(total_us as f64);
                        tracing::debug!(
                            target: "sinomo.network.object_rpc",
                            %traceparent,
                            route = %request.route,
                            category = %request.category,
                            %target_node_id,
                            next_hop = %route_plan.next_hop,
                            endpoint = %endpoint.address,
                            transport = ?endpoint.transport,
                            final_hop,
                            sign_us = request_sign_us,
                            plan_us,
                            outer_build_us,
                            outer_sign_us,
                            send_us,
                            attempt_us,
                            total_us,
                            stream = true,
                            "cluster object rpc stream delivered"
                        );
                        return Ok(response);
                    }
                    Err(error) => {
                        if cluster_object_rpc_error_is_route_attempt_timeout(&error) {
                            let _ = self
                                .host
                                .observe_signed_endpoint_rpc_async(
                                    &endpoint,
                                    ClusterSignedObjectRpcObservation::io_failure(),
                                )
                                .await;
                        }
                        let attempt_us = cluster_object_rpc_elapsed_us(attempt_started_at);
                        let total_us = cluster_object_rpc_elapsed_us(call_started_at);
                        metrics::histogram!(
                            "sinomo.object_rpc.target.attempt.us",
                            "route" => request.route.clone(),
                            "category" => request.category.clone(),
                            "outcome" => "error"
                        )
                        .record(attempt_us as f64);
                        metrics::histogram!(
                            "sinomo.object_rpc.target.total.us",
                            "route" => request.route.clone(),
                            "category" => request.category.clone(),
                            "outcome" => "error"
                        )
                        .record(total_us as f64);
                        tracing::debug!(
                            target: "sinomo.network.object_rpc",
                            %traceparent,
                            route = %request.route,
                            category = %request.category,
                            %target_node_id,
                            next_hop = %route_plan.next_hop,
                            endpoint = %endpoint.address,
                            transport = ?endpoint.transport,
                            final_hop,
                            sign_us = request_sign_us,
                            plan_us,
                            outer_build_us,
                            outer_sign_us,
                            send_us,
                            attempt_us,
                            total_us,
                            error = %error,
                            stream = true,
                            "cluster object rpc stream attempt failed"
                        );
                        failures.push(format!(
                            "{} ({:?}): {error}",
                            endpoint.address, endpoint.transport
                        ));
                    }
                }
            }
            failed_edges.try_insert(&state.local_node_id, &route_plan.next_hop)?;
        }

        Err(NetworkError::Io {
            operation: "cluster target stream rpc".to_string(),
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

fn cluster_object_rpc_elapsed_us(started: Instant) -> u64 {
    started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
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

fn cluster_object_rpc_retryable_endpoint_error_detail(
    response: &ObjectRpcResponse,
) -> Option<String> {
    match response.error_detail() {
        Ok(()) => None,
        Err(NetworkError::AuthDenied) => Some(NetworkError::AuthDenied.to_string()),
        Err(_) => None,
    }
}

fn cluster_object_rpc_endpoint_trace_list(endpoints: &[ClusterEndpointRecord]) -> String {
    endpoints
        .iter()
        .map(|endpoint| {
            format!(
                "{}:{:?}:priority={}:source={:?}:privacy={:?}:latency={:?}",
                endpoint.address,
                endpoint.transport,
                endpoint.priority,
                endpoint.source,
                endpoint.privacy,
                endpoint.last_observed_latency_ms
            )
        })
        .collect::<Vec<_>>()
        .join(",")
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
    if crate::object_rpc_request_expects_raw_daemon_response(inner) {
        outer.metadata.insert(
            crate::DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_KEY.to_string(),
            crate::DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_VALUE.to_string(),
        );
    }
    if crate::object_rpc_request_accepts_streamed_daemon_response(inner) {
        outer.metadata.insert(
            crate::DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_KEY.to_string(),
            crate::DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_VALUE.to_string(),
        );
    }
}

fn cluster_endpoint_transport_timeouts(
    timeouts: DaemonRpcTimeouts,
    remaining_route_budget: Option<Duration>,
) -> DaemonRpcTimeouts {
    let connect_timeout = match (timeouts.connect_timeout, remaining_route_budget) {
        (Some(connect_timeout), Some(remaining)) => Some(connect_timeout.min(remaining)),
        (Some(connect_timeout), None) => Some(connect_timeout),
        (None, Some(remaining)) => Some(remaining),
        (None, None) => None,
    };
    let idle_timeout = match (timeouts.idle_timeout, remaining_route_budget) {
        (Some(idle_timeout), Some(remaining)) => Some(idle_timeout.min(remaining)),
        (Some(idle_timeout), None) => Some(idle_timeout),
        (None, _) => None,
    };
    timeouts
        .with_connect_timeout(connect_timeout)
        .with_idle_timeout(idle_timeout)
}

fn cluster_relay_hop_transport_timeouts(
    timeouts: DaemonRpcTimeouts,
    remaining_route_budget: Option<Duration>,
) -> DaemonRpcTimeouts {
    let shaped = cluster_endpoint_transport_timeouts(timeouts, remaining_route_budget);
    let Some(idle_timeout) = shaped.idle_timeout else {
        return shaped;
    };
    shaped.with_idle_timeout(Some(idle_timeout.min(CLUSTER_RELAY_MIN_RESPONSE_BUDGET)))
}

fn cluster_target_final_hop_transport_timeouts(
    timeouts: DaemonRpcTimeouts,
    endpoint_connect_budget: Option<Duration>,
    endpoint_response_budget: Option<Duration>,
    remaining_route_budget: Option<Duration>,
) -> DaemonRpcTimeouts {
    let connect_timeout = match (timeouts.connect_timeout, endpoint_connect_budget) {
        (Some(connect_timeout), Some(remaining)) => Some(connect_timeout.min(remaining)),
        (Some(connect_timeout), None) => Some(connect_timeout),
        (None, Some(remaining)) => Some(remaining),
        (None, None) => None,
    };
    let idle_timeout = timeouts.idle_timeout.map(|idle_timeout| {
        let mut capped = idle_timeout;
        if let Some(remaining) = remaining_route_budget {
            capped = capped.min(remaining);
        }
        if let Some(remaining) = endpoint_response_budget {
            capped = capped.min(remaining);
        }
        capped
    });
    timeouts
        .with_connect_timeout(connect_timeout)
        .with_idle_timeout(idle_timeout)
}

fn cluster_target_final_hop_request_uses_full_response_budget(request: &ObjectRpcRequest) -> bool {
    crate::object_rpc_request_stream_mode(request).expects_raw_streamed_response()
        || request.uses_full_response_budget()
}

fn cluster_target_endpoint_attempt_budget(
    remaining_route_budget: Option<Duration>,
    endpoints_left: usize,
    endpoint_count: usize,
) -> Option<Duration> {
    if endpoint_count <= 1 {
        return remaining_route_budget;
    }
    remaining_route_budget
        .map(|remaining| duration_div_ceil(remaining, endpoints_left).min(remaining))
        .map(|budget| budget.min(CLUSTER_TARGET_ENDPOINT_ATTEMPT_MAX))
}

fn cluster_target_endpoint_attempt_budget_for_endpoint(
    local_connectivity: &ClusterConnectivityGraph,
    endpoint: &ClusterEndpointRecord,
    remaining_route_budget: Option<Duration>,
    endpoints_left: usize,
    endpoint_count: usize,
) -> Option<Duration> {
    let budget = cluster_target_endpoint_attempt_budget(
        remaining_route_budget,
        endpoints_left,
        endpoint_count,
    );
    if cluster_endpoint_has_healthy_route_evidence(local_connectivity, endpoint)
        && !cluster_target_endpoint_should_reserve_fallback_budget(endpoint, endpoints_left)
    {
        return budget;
    }
    budget.map(|budget| budget.min(CLUSTER_TARGET_UNVERIFIED_ENDPOINT_ATTEMPT_MAX))
}

fn cluster_target_relay_endpoint_attempt_budget(
    endpoint_attempt_budget: Option<Duration>,
    remaining_route_budget: Option<Duration>,
    endpoints_left: usize,
    has_direct_target_fallback: bool,
) -> Option<Duration> {
    if !has_direct_target_fallback {
        return endpoint_attempt_budget;
    }
    let Some(remaining) = remaining_route_budget else {
        return endpoint_attempt_budget;
    };
    let budget_with_fallback_slot =
        duration_div_ceil(remaining, endpoints_left.saturating_add(1).max(1));
    Some(match endpoint_attempt_budget {
        Some(endpoint_attempt_budget) => endpoint_attempt_budget.min(budget_with_fallback_slot),
        None => budget_with_fallback_slot,
    })
}

fn cluster_target_endpoint_should_reserve_fallback_budget(
    endpoint: &ClusterEndpointRecord,
    endpoints_left: usize,
) -> bool {
    endpoints_left > 1 && cluster_delivery_endpoint_is_tcp_tunnel(endpoint)
}

fn cluster_target_final_hop_attempt_budget(
    remaining_route_budget: Option<Duration>,
    endpoints_left: usize,
    endpoint_attempt_budget: Option<Duration>,
    verified_endpoint: bool,
    use_full_response_budget: bool,
) -> Option<Duration> {
    if use_full_response_budget && endpoints_left <= 1 {
        return remaining_route_budget;
    }
    if use_full_response_budget && !verified_endpoint {
        return endpoint_attempt_budget;
    }
    if verified_endpoint
        && let (Some(budget), Some(remaining)) = (endpoint_attempt_budget, remaining_route_budget)
    {
        let preferred =
            cluster_target_verified_direct_endpoint_attempt_preferred(remaining, endpoints_left);
        if preferred > Duration::ZERO {
            return Some(preferred.min(remaining));
        }
        return Some(budget.min(remaining));
    }
    endpoint_attempt_budget
}

fn cluster_target_verified_direct_endpoint_attempt_preferred(
    remaining_route_budget: Duration,
    endpoints_left: usize,
) -> Duration {
    let fallback_reserve = if endpoints_left > 1 {
        CLUSTER_TARGET_VERIFIED_DIRECT_ENDPOINT_FALLBACK_RESERVE
    } else {
        Duration::ZERO
    };
    remaining_route_budget
        .saturating_sub(fallback_reserve)
        .min(CLUSTER_TARGET_VERIFIED_DIRECT_ENDPOINT_ATTEMPT_PREFERRED)
}

fn cluster_endpoint_has_healthy_route_evidence(
    local_connectivity: &ClusterConnectivityGraph,
    endpoint: &ClusterEndpointRecord,
) -> bool {
    if matches!(
        endpoint.transport,
        ClusterTransportKind::InProcess | ClusterTransportKind::Loopback
    ) {
        return true;
    }
    local_connectivity
        .record_for_endpoint(endpoint)
        .is_some_and(|record| {
            record.state == ClusterConnectivityState::Healthy
                && !crate::connectivity::cluster_connectivity_failure_reason_is_identity_mismatch(
                    record,
                )
        })
}

fn duration_div_ceil(duration: Duration, divisor: usize) -> Duration {
    if divisor <= 1 {
        return duration;
    }
    let divisor = divisor as u128;
    let nanos = duration.as_nanos();
    let per_endpoint = nanos
        .saturating_add(divisor.saturating_sub(1))
        .saturating_div(divisor)
        .clamp(1, u128::from(u64::MAX));
    Duration::from_nanos(per_endpoint as u64)
}

async fn cluster_object_rpc_attempt_with_timeout<T, F>(
    send: F,
    timeout: Option<Duration>,
    endpoint: &ClusterEndpointRecord,
) -> Result<T, NetworkError>
where
    F: Future<Output = Result<T, NetworkError>>,
{
    let Some(timeout) = timeout else {
        return send.await;
    };
    match tokio::time::timeout(timeout, send).await {
        Ok(result) => result,
        Err(_) => Err(cluster_object_rpc_attempt_timeout_error(endpoint, timeout)),
    }
}

fn cluster_object_rpc_attempt_timeout_error(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
) -> NetworkError {
    NetworkError::Io {
        operation: "cluster endpoint rpc attempt".to_string(),
        detail: format!(
            "timed out sending to {} ({:?}) after {}ms",
            endpoint.address,
            endpoint.transport,
            timeout.as_millis()
        ),
    }
}

fn cluster_object_rpc_error_is_route_attempt_timeout(error: &NetworkError) -> bool {
    matches!(
        error,
        NetworkError::Io { operation, .. } if operation == "cluster endpoint rpc attempt"
    )
}

pub(crate) fn relay_envelope_hop_transport_timeouts(
    timeouts: DaemonRpcTimeouts,
    envelope: &RelayEnvelope,
) -> DaemonRpcTimeouts {
    cluster_relay_hop_transport_timeouts(timeouts, relay_envelope_hop_timeout(envelope))
}

fn relay_envelope_hop_timeout(envelope: &RelayEnvelope) -> Option<Duration> {
    envelope
        .max_hop_timeout_ms
        .map(|timeout_ms| Duration::from_millis(timeout_ms.max(1)))
}

fn cluster_target_route_timeout(timeout: Option<Duration>) -> Option<Duration> {
    Some(
        timeout
            .unwrap_or(CLUSTER_TARGET_ROUTE_DEADLINE_MAX)
            .min(CLUSTER_TARGET_ROUTE_DEADLINE_MAX),
    )
}

#[derive(Clone, Copy, Debug)]
struct ClusterEndpointRpcDeadline {
    started: Instant,
    timeout: Option<Duration>,
}

impl ClusterEndpointRpcDeadline {
    fn new(timeout: Option<Duration>) -> Self {
        Self {
            started: Instant::now(),
            timeout,
        }
    }

    fn checked_remaining(&self) -> Option<Option<Duration>> {
        match self.timeout {
            Some(timeout) => timeout
                .checked_sub(self.started.elapsed())
                .filter(|remaining| !remaining.is_zero())
                .map(Some),
            None => Some(None),
        }
    }

    fn timeout_error(&self, endpoint: &ClusterEndpointRecord) -> NetworkError {
        let timeout = self.timeout.unwrap_or_default();
        NetworkError::Io {
            operation: "cluster endpoint rpc".to_string(),
            detail: format!(
                "timed out sending to {} after {}ms",
                endpoint.address,
                timeout.as_millis()
            ),
        }
    }
}

fn cluster_target_rpc_timeout_error(
    target_node_id: &NodeId,
    timeout: Option<Duration>,
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
    NetworkError::Io {
        operation: "cluster target rpc".to_string(),
        detail,
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

const CLUSTER_RELAY_MIN_RESPONSE_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);

fn push_cluster_relay_failure(failures: &mut Vec<String>, detail: impl Into<String>) {
    if failures.len() >= CLUSTER_RELAY_MAX_FAILURE_DETAILS {
        if failures.len() == CLUSTER_RELAY_MAX_FAILURE_DETAILS {
            failures.push("additional relay failures suppressed".to_string());
        }
        return;
    }
    let detail = detail.into();
    let mut chars = detail.chars();
    let truncated: String = chars
        .by_ref()
        .take(CLUSTER_RELAY_FAILURE_DETAIL_MAX_CHARS)
        .collect();
    if chars.next().is_some() {
        failures.push(format!("{truncated}..."));
    } else {
        failures.push(truncated);
    }
}

pub async fn handle_cluster_relay_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    request: &ObjectRpcRequest,
    options: ClusterRelayOptions,
    host: &mut impl ClusterRelayAsyncHost,
) -> Result<ObjectRpcResponse, NetworkError> {
    let envelope = RelayEnvelope::from_request(request)?;
    if relay_envelope_inner_expects_streamed_response(&envelope) {
        return Err(NetworkError::Io {
            operation: "cluster relay rpc".to_string(),
            detail: "streamed raw relay responses require socket streaming dispatch".to_string(),
        });
    }
    handle_cluster_relay_envelope_async_with_host(
        runtime,
        paths,
        state,
        local_node_id,
        envelope,
        options,
        host,
    )
    .await
}

async fn handle_cluster_relay_envelope_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    envelope: RelayEnvelope,
    options: ClusterRelayOptions,
    host: &mut impl ClusterRelayAsyncHost,
) -> Result<ObjectRpcResponse, NetworkError> {
    let relay_started_at = Instant::now();
    let inner_route = envelope.inner.route.clone();
    let inner_category = envelope.inner.category.clone();
    let traceparent = envelope.inner.traceparent().unwrap_or_default().to_string();
    metrics::counter!(
        "sinomo.object_rpc.relay.calls",
        "route" => inner_route.clone(),
        "category" => inner_category.clone(),
        "stream" => "false"
    )
    .increment(1);
    tracing::debug!(
        target: "sinomo.network.object_rpc.relay",
        %traceparent,
        route = %inner_route,
        category = %inner_category,
        %local_node_id,
        target_node_id = %envelope.target_node_id,
        ttl = envelope.ttl,
        visited = envelope.visited.len(),
        stream = false,
        "cluster relay start"
    );
    if sinomo_node::cluster_node_is_local_process(state, &envelope.target_node_id) {
        if let Some(response) = host
            .try_deliver_local_async(
                state,
                &envelope.target_node_id,
                envelope.inner(),
                options.timeouts,
            )
            .await?
        {
            cluster_relay_record_total_us(
                &inner_route,
                &inner_category,
                false,
                "local",
                relay_started_at,
            );
            tracing::debug!(
                target: "sinomo.network.object_rpc.relay",
                %traceparent,
                route = %inner_route,
                category = %inner_category,
                %local_node_id,
                target_node_id = %envelope.target_node_id,
                total_us = cluster_object_rpc_elapsed_us(relay_started_at),
                "cluster relay delivered locally"
            );
            return Ok(response);
        }
    }

    if envelope.visited.iter().any(|node| node == local_node_id) {
        cluster_relay_record_total_us(
            &inner_route,
            &inner_category,
            false,
            "loop",
            relay_started_at,
        );
        return ObjectRpcResponse::encode_error(
            CLUSTER_RELAY_ROUTE,
            format!("relay dropped: relay loop: {local_node_id} already on the path"),
        );
    }
    if envelope.ttl == 0 {
        cluster_relay_record_total_us(
            &inner_route,
            &inner_category,
            false,
            "ttl_exhausted",
            relay_started_at,
        );
        return ObjectRpcResponse::encode_error(
            CLUSTER_RELAY_ROUTE,
            "relay dropped: relay ttl exhausted",
        );
    }

    let mut failures = Vec::new();
    let mut failed_edges = ClusterDeliveryFailedEdges::default();
    let max_route_attempts = state.members.len().saturating_mul(2).max(1) + 2;
    let relay_deadline = ClusterEndpointRpcDeadline::new(
        relay_envelope_hop_timeout(&envelope).or(options.timeouts.connect_timeout),
    );
    'route_attempts: for _ in 0..max_route_attempts {
        let plan_started_at = Instant::now();
        let Some(route_plan) = cluster_relay_forward_plan(
            runtime,
            paths,
            state,
            local_node_id,
            &envelope,
            failed_edges.try_clone()?,
        )
        .await?
        else {
            break;
        };
        let plan_us = cluster_object_rpc_elapsed_us(plan_started_at);
        metrics::histogram!(
            "sinomo.object_rpc.relay.route_plan.us",
            "route" => inner_route.clone(),
            "category" => inner_category.clone(),
            "stream" => "false"
        )
        .record(plan_us as f64);
        if route_plan.endpoints.is_empty() {
            push_cluster_relay_failure(
                &mut failures,
                format!(
                    "{}: relay next hop has no reachable endpoint",
                    route_plan.next_hop
                ),
            );
            failed_edges.try_insert(local_node_id, &route_plan.next_hop)?;
            continue;
        }
        tracing::trace!(
            target: "sinomo.network.object_rpc.relay",
            %traceparent,
            route = %inner_route,
            category = %inner_category,
            %local_node_id,
            target_node_id = %envelope.target_node_id,
            next_hop = %route_plan.next_hop,
            plan_us,
            endpoints = %cluster_object_rpc_endpoint_trace_list(&route_plan.endpoints),
            stream = false,
            "cluster relay route plan selected"
        );
        let route_endpoint_count = route_plan.endpoints.len();
        let local_connectivity = ClusterConnectivityGraph::try_read_for_local(paths, state)?;
        for (endpoint_index, endpoint) in route_plan.endpoints.into_iter().enumerate() {
            let attempt_started_at = Instant::now();
            let Some(remaining) = relay_deadline.checked_remaining() else {
                push_cluster_relay_failure(
                    &mut failures,
                    format!(
                        "relay hop budget exhausted after {}ms",
                        relay_deadline.timeout.unwrap_or_default().as_millis()
                    ),
                );
                break 'route_attempts;
            };
            let endpoints_left = route_endpoint_count.saturating_sub(endpoint_index).max(1);
            let endpoint_attempt_budget = cluster_target_endpoint_attempt_budget_for_endpoint(
                &local_connectivity,
                &endpoint,
                remaining,
                endpoints_left,
                route_endpoint_count,
            );
            let final_hop = &route_plan.next_hop == &envelope.target_node_id;
            let hop_timeouts =
                cluster_relay_hop_transport_timeouts(options.timeouts, endpoint_attempt_budget);
            let response = if final_hop {
                cluster_object_rpc_attempt_with_timeout(
                    host.send_target_request_async(&endpoint, envelope.inner(), hop_timeouts),
                    endpoint_attempt_budget,
                    &endpoint,
                )
                .await
            } else {
                let mut forwarded = envelope.clone();
                forwarded.ttl = envelope.ttl.saturating_sub(1);
                forwarded.visited.push(local_node_id.clone());
                let outer = forwarded.into_request()?;
                cluster_object_rpc_attempt_with_timeout(
                    host.send_relay_request_async(&endpoint, &outer, hop_timeouts),
                    endpoint_attempt_budget,
                    &endpoint,
                )
                .await
            };
            match response {
                Ok(response) if relay_response_is_routing_error(&response) => {
                    cluster_relay_record_attempt_us(
                        &inner_route,
                        &inner_category,
                        false,
                        "routing_error",
                        attempt_started_at,
                    );
                    tracing::debug!(
                        target: "sinomo.network.object_rpc.relay",
                        %traceparent,
                        route = %inner_route,
                        category = %inner_category,
                        %local_node_id,
                        target_node_id = %envelope.target_node_id,
                        next_hop = %route_plan.next_hop,
                        endpoint = %endpoint.address,
                        transport = ?endpoint.transport,
                        final_hop,
                        attempt_us = cluster_object_rpc_elapsed_us(attempt_started_at),
                        "cluster relay hop returned routing error"
                    );
                    push_cluster_relay_failure(
                        &mut failures,
                        format!(
                            "{} ({:?}) returned routing error",
                            endpoint.address, endpoint.transport
                        ),
                    );
                    break;
                }
                Ok(response) => {
                    cluster_relay_record_attempt_us(
                        &inner_route,
                        &inner_category,
                        false,
                        "ok",
                        attempt_started_at,
                    );
                    cluster_relay_record_total_us(
                        &inner_route,
                        &inner_category,
                        false,
                        "ok",
                        relay_started_at,
                    );
                    tracing::debug!(
                        target: "sinomo.network.object_rpc.relay",
                        %traceparent,
                        route = %inner_route,
                        category = %inner_category,
                        %local_node_id,
                        target_node_id = %envelope.target_node_id,
                        next_hop = %route_plan.next_hop,
                        endpoint = %endpoint.address,
                        transport = ?endpoint.transport,
                        final_hop,
                        attempt_us = cluster_object_rpc_elapsed_us(attempt_started_at),
                        total_us = cluster_object_rpc_elapsed_us(relay_started_at),
                        "cluster relay forwarded"
                    );
                    return Ok(response);
                }
                Err(error) => {
                    cluster_relay_record_attempt_us(
                        &inner_route,
                        &inner_category,
                        false,
                        "error",
                        attempt_started_at,
                    );
                    tracing::debug!(
                        target: "sinomo.network.object_rpc.relay",
                        %traceparent,
                        route = %inner_route,
                        category = %inner_category,
                        %local_node_id,
                        target_node_id = %envelope.target_node_id,
                        next_hop = %route_plan.next_hop,
                        endpoint = %endpoint.address,
                        transport = ?endpoint.transport,
                        final_hop,
                        attempt_us = cluster_object_rpc_elapsed_us(attempt_started_at),
                        error = %error,
                        "cluster relay hop failed"
                    );
                    push_cluster_relay_failure(
                        &mut failures,
                        format!(
                            "{} ({:?}) failed: {}",
                            endpoint.address, endpoint.transport, error
                        ),
                    );
                }
            }
        }
        failed_edges.try_insert(local_node_id, &route_plan.next_hop)?;
    }

    cluster_relay_record_total_us(
        &inner_route,
        &inner_category,
        false,
        "no_route",
        relay_started_at,
    );
    ObjectRpcResponse::encode_error(
        CLUSTER_RELAY_ROUTE,
        format!(
            "relay dropped: no reachable route from {local_node_id} to {}{}",
            envelope.target_node_id,
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    )
}

fn relay_envelope_inner_expects_streamed_response(envelope: &RelayEnvelope) -> bool {
    crate::object_rpc_request_stream_mode(envelope.inner()).expects_raw_streamed_response()
}

pub async fn handle_cluster_relay_stream_forward_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    request: &ObjectRpcRequest,
    options: ClusterRelayOptions,
    host: &mut impl ClusterRelayStreamForwardAsyncHost,
) -> Result<ClusterRelayStreamForwardOutcome, NetworkError> {
    let envelope = RelayEnvelope::from_request(request)?;
    if !relay_envelope_inner_expects_streamed_response(&envelope) {
        return ObjectRpcResponse::encode_error(
            CLUSTER_RELAY_ROUTE,
            "stream relay forwarding requires an inner request that expects a streamed raw response",
        )
        .map(ClusterRelayStreamForwardOutcome::ErrorResponse);
    }
    handle_cluster_relay_stream_forward_envelope_async_with_host(
        runtime,
        paths,
        state,
        local_node_id,
        envelope,
        options,
        host,
    )
    .await
}

async fn handle_cluster_relay_stream_forward_envelope_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    envelope: RelayEnvelope,
    options: ClusterRelayOptions,
    host: &mut impl ClusterRelayStreamForwardAsyncHost,
) -> Result<ClusterRelayStreamForwardOutcome, NetworkError> {
    let relay_started_at = Instant::now();
    let inner_route = envelope.inner.route.clone();
    let inner_category = envelope.inner.category.clone();
    let traceparent = envelope.inner.traceparent().unwrap_or_default().to_string();
    metrics::counter!(
        "sinomo.object_rpc.relay.calls",
        "route" => inner_route.clone(),
        "category" => inner_category.clone(),
        "stream" => "true"
    )
    .increment(1);
    tracing::debug!(
        target: "sinomo.network.object_rpc.relay",
        %traceparent,
        route = %inner_route,
        category = %inner_category,
        %local_node_id,
        target_node_id = %envelope.target_node_id,
        ttl = envelope.ttl,
        visited = envelope.visited.len(),
        stream = true,
        "cluster stream relay start"
    );
    if sinomo_node::cluster_node_is_local_process(state, &envelope.target_node_id) {
        cluster_relay_record_total_us(
            &inner_route,
            &inner_category,
            true,
            "local_stream",
            relay_started_at,
        );
        return ObjectRpcResponse::encode_error(
            CLUSTER_RELAY_ROUTE,
            "stream relay local delivery must be handled by the daemon control loop",
        )
        .map(ClusterRelayStreamForwardOutcome::ErrorResponse);
    }

    if envelope.visited.iter().any(|node| node == local_node_id) {
        cluster_relay_record_total_us(
            &inner_route,
            &inner_category,
            true,
            "loop",
            relay_started_at,
        );
        return ObjectRpcResponse::encode_error(
            CLUSTER_RELAY_ROUTE,
            format!("relay dropped: relay loop: {local_node_id} already on the path"),
        )
        .map(ClusterRelayStreamForwardOutcome::ErrorResponse);
    }
    if envelope.ttl == 0 {
        cluster_relay_record_total_us(
            &inner_route,
            &inner_category,
            true,
            "ttl_exhausted",
            relay_started_at,
        );
        return ObjectRpcResponse::encode_error(
            CLUSTER_RELAY_ROUTE,
            "relay dropped: relay ttl exhausted",
        )
        .map(ClusterRelayStreamForwardOutcome::ErrorResponse);
    }

    let mut failures = Vec::new();
    let mut failed_edges = ClusterDeliveryFailedEdges::default();
    let max_route_attempts = state.members.len().saturating_mul(2).max(1) + 2;
    for _ in 0..max_route_attempts {
        let plan_started_at = Instant::now();
        let Some(route_plan) = cluster_relay_forward_plan(
            runtime,
            paths,
            state,
            local_node_id,
            &envelope,
            failed_edges.try_clone()?,
        )
        .await?
        else {
            break;
        };
        let plan_us = cluster_object_rpc_elapsed_us(plan_started_at);
        metrics::histogram!(
            "sinomo.object_rpc.relay.route_plan.us",
            "route" => inner_route.clone(),
            "category" => inner_category.clone(),
            "stream" => "true"
        )
        .record(plan_us as f64);
        if route_plan.endpoints.is_empty() {
            push_cluster_relay_failure(
                &mut failures,
                format!(
                    "{}: relay next hop has no reachable endpoint",
                    route_plan.next_hop
                ),
            );
            failed_edges.try_insert(local_node_id, &route_plan.next_hop)?;
            continue;
        }
        tracing::trace!(
            target: "sinomo.network.object_rpc.relay",
            %traceparent,
            route = %inner_route,
            category = %inner_category,
            %local_node_id,
            target_node_id = %envelope.target_node_id,
            next_hop = %route_plan.next_hop,
            plan_us,
            endpoints = %cluster_object_rpc_endpoint_trace_list(&route_plan.endpoints),
            stream = true,
            "cluster stream relay route plan selected"
        );
        for endpoint in route_plan.endpoints {
            let attempt_started_at = Instant::now();
            let mut forwarded = envelope.clone();
            forwarded.ttl = envelope.ttl.saturating_sub(1);
            forwarded.visited.push(local_node_id.clone());
            let outer = forwarded.into_request()?;
            let hop_timeouts = relay_envelope_hop_transport_timeouts(options.timeouts, &envelope);
            match host
                .send_relay_stream_forward_async(&endpoint, &outer, hop_timeouts)
                .await
            {
                Ok(ClusterRelayStreamSendOutcome::Forwarded) => {
                    cluster_relay_record_attempt_us(
                        &inner_route,
                        &inner_category,
                        true,
                        "ok",
                        attempt_started_at,
                    );
                    cluster_relay_record_total_us(
                        &inner_route,
                        &inner_category,
                        true,
                        "ok",
                        relay_started_at,
                    );
                    tracing::debug!(
                        target: "sinomo.network.object_rpc.relay",
                        %traceparent,
                        route = %inner_route,
                        category = %inner_category,
                        %local_node_id,
                        target_node_id = %envelope.target_node_id,
                        next_hop = %route_plan.next_hop,
                        endpoint = %endpoint.address,
                        transport = ?endpoint.transport,
                        attempt_us = cluster_object_rpc_elapsed_us(attempt_started_at),
                        total_us = cluster_object_rpc_elapsed_us(relay_started_at),
                        stream = true,
                        "cluster stream relay forwarded"
                    );
                    return Ok(ClusterRelayStreamForwardOutcome::Forwarded);
                }
                Ok(ClusterRelayStreamSendOutcome::RoutingError(response)) => {
                    cluster_relay_record_attempt_us(
                        &inner_route,
                        &inner_category,
                        true,
                        "routing_error",
                        attempt_started_at,
                    );
                    tracing::debug!(
                        target: "sinomo.network.object_rpc.relay",
                        %traceparent,
                        route = %inner_route,
                        category = %inner_category,
                        %local_node_id,
                        target_node_id = %envelope.target_node_id,
                        next_hop = %route_plan.next_hop,
                        endpoint = %endpoint.address,
                        transport = ?endpoint.transport,
                        attempt_us = cluster_object_rpc_elapsed_us(attempt_started_at),
                        stream = true,
                        "cluster stream relay hop returned routing error"
                    );
                    push_cluster_relay_failure(
                        &mut failures,
                        format!(
                            "{} ({:?}) returned routing error: {}",
                            endpoint.address,
                            endpoint.transport,
                            relay_response_routing_error_detail(&response)
                        ),
                    );
                    break;
                }
                Ok(ClusterRelayStreamSendOutcome::StreamStartedThenFailed(error)) => {
                    cluster_relay_record_attempt_us(
                        &inner_route,
                        &inner_category,
                        true,
                        "stream_started_failed",
                        attempt_started_at,
                    );
                    cluster_relay_record_total_us(
                        &inner_route,
                        &inner_category,
                        true,
                        "stream_started_failed",
                        relay_started_at,
                    );
                    return Ok(ClusterRelayStreamForwardOutcome::StreamStartedThenFailed(
                        error,
                    ));
                }
                Err(error) => {
                    cluster_relay_record_attempt_us(
                        &inner_route,
                        &inner_category,
                        true,
                        "error",
                        attempt_started_at,
                    );
                    tracing::debug!(
                        target: "sinomo.network.object_rpc.relay",
                        %traceparent,
                        route = %inner_route,
                        category = %inner_category,
                        %local_node_id,
                        target_node_id = %envelope.target_node_id,
                        next_hop = %route_plan.next_hop,
                        endpoint = %endpoint.address,
                        transport = ?endpoint.transport,
                        attempt_us = cluster_object_rpc_elapsed_us(attempt_started_at),
                        error = %error,
                        stream = true,
                        "cluster stream relay hop failed"
                    );
                    push_cluster_relay_failure(
                        &mut failures,
                        format!(
                            "{} ({:?}) failed: {}",
                            endpoint.address, endpoint.transport, error
                        ),
                    );
                }
            }
        }
        failed_edges.try_insert(local_node_id, &route_plan.next_hop)?;
    }

    cluster_relay_record_total_us(
        &inner_route,
        &inner_category,
        true,
        "no_route",
        relay_started_at,
    );
    ObjectRpcResponse::encode_error(
        CLUSTER_RELAY_ROUTE,
        format!(
            "relay dropped: no reachable route from {local_node_id} to {}{}",
            envelope.target_node_id,
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    )
    .map(ClusterRelayStreamForwardOutcome::ErrorResponse)
}

fn cluster_relay_record_attempt_us(
    route: &str,
    category: &str,
    stream: bool,
    outcome: &str,
    started: Instant,
) {
    metrics::histogram!(
        "sinomo.object_rpc.relay.attempt.us",
        "route" => route.to_string(),
        "category" => category.to_string(),
        "stream" => cluster_relay_stream_label(stream),
        "outcome" => outcome.to_string()
    )
    .record(cluster_object_rpc_elapsed_us(started) as f64);
}

fn cluster_relay_record_total_us(
    route: &str,
    category: &str,
    stream: bool,
    outcome: &str,
    started: Instant,
) {
    metrics::histogram!(
        "sinomo.object_rpc.relay.total.us",
        "route" => route.to_string(),
        "category" => category.to_string(),
        "stream" => cluster_relay_stream_label(stream),
        "outcome" => outcome.to_string()
    )
    .record(cluster_object_rpc_elapsed_us(started) as f64);
}

fn cluster_relay_stream_label(stream: bool) -> &'static str {
    if stream { "true" } else { "false" }
}

async fn cluster_relay_forward_plan(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    envelope: &RelayEnvelope,
    failed_edges: ClusterDeliveryFailedEdges,
) -> Result<Option<ClusterDeliveryRoutePlan>, NetworkError> {
    if let Some(plan) =
        cluster_relay_planned_forward_plan(paths, state, local_node_id, envelope, &failed_edges)?
    {
        return Ok(Some(plan));
    }
    let dynamic = cluster_delivery_route_plan_async(
        runtime,
        paths,
        state,
        local_node_id,
        &envelope.target_node_id,
        failed_edges.try_clone()?,
        try_clone_route_node_ids(&envelope.visited, "cluster relay visited nodes")?,
    )
    .await?;
    if dynamic.is_some() {
        return Ok(dynamic);
    }
    cluster_relay_target_hint_forward_plan(paths, state, local_node_id, envelope, &failed_edges)
}

fn cluster_relay_target_hint_forward_plan(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    envelope: &RelayEnvelope,
    failed_edges: &ClusterDeliveryFailedEdges,
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
        )?
    } else {
        cluster_relay_planned_next_hop_endpoints(paths, state, local_node_id, next_hop)?
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
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let local_connectivity = ClusterConnectivityGraph::try_read_for_local(paths, state)?;
    let members = cluster_delivery_members_for_routing(paths, state)?;
    let local_private_prefixes = cluster_delivery_local_private_prefixes(&members, local_node_id)?;
    let mut candidates = Vec::new();
    for endpoint in target_endpoints
        .iter()
        .filter(|endpoint| cluster_delivery_local_endpoint_is_usable(local_node_id, endpoint))
    {
        let Some(cost) = cluster_delivery_local_endpoint_cost(
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
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let local_connectivity = ClusterConnectivityGraph::try_read_for_local(paths, state)?;
    let members = cluster_delivery_members_for_routing(paths, state)?;
    let local_private_prefixes = cluster_delivery_local_private_prefixes(&members, local_node_id)?;
    let mut candidates = Vec::new();
    for endpoint in members
        .iter()
        .filter(|member| member.node_id == *next_hop && member.state.is_authorized())
        .flat_map(|member| member.endpoints.iter())
        .filter(|endpoint| cluster_delivery_local_endpoint_is_usable(local_node_id, endpoint))
    {
        let Some(cost) = cluster_delivery_local_endpoint_cost(
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

    #[derive(Default)]
    struct RecordingRelayHost {
        relay_sends: Vec<(ClusterEndpointRecord, ObjectRpcRequest, DaemonRpcTimeouts)>,
        target_sends: Vec<(ClusterEndpointRecord, ObjectRpcRequest, DaemonRpcTimeouts)>,
        stall_first_target_send: Option<Duration>,
    }

    impl ClusterRelayAsyncHost for RecordingRelayHost {
        fn try_deliver_local_async<'a>(
            &'a mut self,
            _state: &'a ClusterBootstrapState,
            _target_node_id: &'a NodeId,
            _inner_request: &'a ObjectRpcRequest,
            _timeouts: DaemonRpcTimeouts,
        ) -> Pin<
            Box<dyn Future<Output = Result<Option<ObjectRpcResponse>, NetworkError>> + Send + 'a>,
        > {
            Box::pin(async { Ok(None) })
        }

        fn send_relay_request_async<'a>(
            &'a mut self,
            endpoint: &'a ClusterEndpointRecord,
            request: &'a ObjectRpcRequest,
            timeouts: DaemonRpcTimeouts,
        ) -> Pin<Box<dyn Future<Output = Result<ObjectRpcResponse, NetworkError>> + Send + 'a>>
        {
            let endpoint = endpoint.clone();
            let request = request.clone();
            let sends = &mut self.relay_sends;
            Box::pin(async move {
                sends.push((endpoint, request, timeouts));
                ObjectRpcResponse::encode_error(CLUSTER_RELAY_ROUTE, "unexpected relay send")
            })
        }

        fn send_target_request_async<'a>(
            &'a mut self,
            endpoint: &'a ClusterEndpointRecord,
            request: &'a ObjectRpcRequest,
            timeouts: DaemonRpcTimeouts,
        ) -> Pin<Box<dyn Future<Output = Result<ObjectRpcResponse, NetworkError>> + Send + 'a>>
        {
            let endpoint = endpoint.clone();
            let request = request.clone();
            let stall = self
                .stall_first_target_send
                .filter(|_| self.target_sends.is_empty());
            let sends = &mut self.target_sends;
            Box::pin(async move {
                sends.push((endpoint, request, timeouts));
                if let Some(stall) = stall {
                    tokio::time::sleep(stall).await;
                }
                ObjectRpcResponse::encode_body("/test/relay-target", &"ok")
            })
        }
    }

    #[derive(Default)]
    struct RecordingSignedHost {
        target_sends: Vec<(ClusterEndpointRecord, ObjectRpcRequest, DaemonRpcTimeouts)>,
        target_auths: Vec<Vec<u8>>,
        stream_target_sends: Vec<(ClusterEndpointRecord, ObjectRpcRequest, DaemonRpcTimeouts)>,
        stream_target_auths: Vec<Vec<u8>>,
        observations: Vec<(ClusterEndpointRecord, ClusterSignedObjectRpcObservation)>,
        signed_routes: Vec<String>,
        signed_requests: Vec<ObjectRpcRequest>,
        fail_first_send: bool,
        error_first_send: bool,
        stream_error_first_send: bool,
        stall_first_send: Option<Duration>,
        stall_second_send: Option<Duration>,
        stall_first_stream_send: Option<Duration>,
    }

    impl ClusterSignedObjectRpcAsyncHost for RecordingSignedHost {
        fn sign_object_rpc_request_async<'a>(
            &'a mut self,
            request: &'a ObjectRpcRequest,
        ) -> ClusterSignedObjectRpcFuture<'a, Vec<u8>> {
            let next = self.signed_routes.len() + 1;
            self.signed_routes.push(request.route.clone());
            self.signed_requests.push(request.clone());
            let auth = format!("test-auth-{next}").into_bytes();
            Box::pin(async move { Ok(auth) })
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

        fn send_signed_object_rpc_async<'a>(
            &'a mut self,
            endpoint: &'a ClusterEndpointRecord,
            _auth: &'a [u8],
            request: &'a ObjectRpcRequest,
            timeouts: DaemonRpcTimeouts,
        ) -> ClusterSignedObjectRpcFuture<'a, ObjectRpcResponse> {
            let endpoint = endpoint.clone();
            let request = request.clone();
            let should_fail = self.fail_first_send && self.target_sends.is_empty();
            let should_error = self.error_first_send && self.target_sends.is_empty();
            let stall = self
                .stall_first_send
                .filter(|_| self.target_sends.is_empty());
            let stall_second = self
                .stall_second_send
                .filter(|_| self.target_sends.len() == 1);
            let sends = &mut self.target_sends;
            let auths = &mut self.target_auths;
            let auth = _auth.to_vec();
            Box::pin(async move {
                sends.push((endpoint, request, timeouts));
                auths.push(auth);
                if let Some(stall) = stall {
                    tokio::time::sleep(stall).await;
                }
                if let Some(stall) = stall_second {
                    tokio::time::sleep(stall).await;
                }
                if should_fail {
                    Err(NetworkError::Io {
                        operation: "test target send".to_string(),
                        detail: "first endpoint stalled".to_string(),
                    })
                } else if should_error {
                    ObjectRpcResponse::encode_error(
                        "/test/target",
                        "device authorization was denied",
                    )
                } else {
                    ObjectRpcResponse::encode_body("/test/target", &"ok")
                }
            })
        }

        fn send_signed_endpoint_stream_response_rpc_async<'a>(
            &'a mut self,
            endpoint: &'a ClusterEndpointRecord,
            _auth: &'a [u8],
            request: &'a ObjectRpcRequest,
            timeouts: DaemonRpcTimeouts,
        ) -> ClusterSignedObjectRpcFuture<'a, crate::ObjectRpcStreamResponse> {
            let endpoint = endpoint.clone();
            let request = request.clone();
            let should_error = self.stream_error_first_send && self.stream_target_sends.is_empty();
            let stall = self
                .stall_first_stream_send
                .filter(|_| self.stream_target_sends.is_empty());
            let sends = &mut self.stream_target_sends;
            let auths = &mut self.stream_target_auths;
            let auth = _auth.to_vec();
            Box::pin(async move {
                sends.push((endpoint, request, timeouts));
                auths.push(auth);
                if let Some(stall) = stall {
                    tokio::time::sleep(stall).await;
                }
                if should_error {
                    let response = ObjectRpcResponse::encode_error(
                        "/test/stream-target",
                        "device authorization was denied",
                    )?;
                    return Ok(crate::ObjectRpcStreamResponse::from_buffered_response(
                        response,
                        crate::RawByteStreamChunkOptions {
                            chunk_bytes: 4,
                            max_total_bytes: Some(1024),
                            idle_timeout: Some(Duration::from_secs(1)),
                        },
                    ));
                }
                let mut response = ObjectRpcResponse::from_encoded_body(
                    "/test/stream-target",
                    b"stream-body".to_vec(),
                );
                crate::set_object_rpc_response_stream_mode(
                    &mut response,
                    crate::ObjectRpcResponseStreamMode::RawStreamedDaemonBody,
                );
                Ok(crate::ObjectRpcStreamResponse::from_buffered_response(
                    response,
                    crate::RawByteStreamChunkOptions {
                        chunk_bytes: 4,
                        max_total_bytes: Some(1024),
                        idle_timeout: Some(Duration::from_secs(1)),
                    },
                ))
            })
        }
    }

    #[tokio::test]
    async fn endpoint_rpc_does_not_inject_trace_context_before_signing() {
        let node_id = NodeId::from_legacy_named("node_trace_target").expect("node id");
        let endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &node_id,
            "127.0.0.1:21353".to_string(),
            0,
        );
        let request =
            ObjectRpcRequest::encode_body("/test/no-trace", "test", &"body").expect("request");
        assert!(request.traceparent().is_none());
        let mut host = RecordingSignedHost::default();
        let mut client = ClusterObjectRpcClient::endpoint_only(&mut host);

        client
            .call_endpoint(
                &endpoint,
                &request,
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(
                    Duration::from_secs(1),
                )),
            )
            .await
            .expect("endpoint call");

        assert_eq!(host.signed_requests.len(), 1);
        assert!(
            host.signed_requests[0].traceparent().is_none(),
            "endpoint signing must not mutate auth-sensitive requests with synthetic trace metadata"
        );
        assert_eq!(host.target_sends.len(), 1);
        assert!(
            host.target_sends[0].1.traceparent().is_none(),
            "endpoint send must use the same unmodified request that was signed"
        );
    }

    #[tokio::test]
    async fn streamed_endpoint_forward_preserves_raw_body_chunks() {
        let downstream_listener = crate::object::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("downstream listener");
        let downstream_address = downstream_listener
            .local_addr_string()
            .expect("downstream address");
        let downstream_server = tokio::spawn(async move {
            let mut stream = downstream_listener
                .accept()
                .await
                .expect("accept downstream");
            let request = stream
                .read_request(
                    "downstream",
                    1024,
                    crate::object::ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
                )
                .await
                .expect("downstream request");
            assert_eq!(request.route, "/test/stream-forward");
            assert_eq!(
                crate::object_rpc_request_stream_mode(&request),
                crate::ObjectRpcRequestStreamMode::RawStreamedDaemonResponse
            );
            let mut response =
                ObjectRpcResponse::from_encoded_body("/test/stream-forward", Vec::new());
            crate::set_object_rpc_response_stream_mode(
                &mut response,
                crate::ObjectRpcResponseStreamMode::RawStreamedDaemonBody,
            );
            let frame_options =
                crate::object::ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1));
            stream
                .write_rpc_response("downstream", &response, frame_options)
                .await
                .expect("downstream response");
            for chunk in [b"alpha".as_slice(), b"beta".as_slice()] {
                stream
                    .write_raw_byte_stream_chunk("downstream", chunk, frame_options)
                    .await
                    .expect("downstream chunk");
            }
            stream
                .write_raw_byte_stream_chunk("downstream", &[], frame_options)
                .await
                .expect("downstream final chunk");
        });

        let upstream_listener = crate::object::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("upstream listener");
        let upstream_address = upstream_listener
            .local_addr_string()
            .expect("upstream address");
        let upstream_client = tokio::spawn(async move {
            let mut stream = crate::object::AsyncDaemonControlStream::connect(&upstream_address)
                .await
                .expect("connect upstream");
            let frame_options =
                crate::object::ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1));
            let response = stream
                .read_rpc_response("upstream", 1024, frame_options)
                .await
                .expect("upstream response");
            assert_eq!(
                crate::object_rpc_response_stream_mode(&response),
                crate::ObjectRpcResponseStreamMode::RawStreamedDaemonBody
            );
            assert!(response.body.is_empty());
            let mut body = Vec::new();
            let mut raw = stream.into_inner();
            crate::object::read_raw_byte_stream_chunks_to_vec_async(
                &mut raw,
                &mut body,
                crate::object::RawByteStreamChunkOptions {
                    chunk_bytes: crate::OBJECT_STREAM_MAX_CHUNK_BYTES,
                    max_total_bytes: Some(1024),
                    idle_timeout: Some(Duration::from_secs(1)),
                },
            )
            .await
            .expect("upstream chunks");
            body
        });
        let mut upstream = upstream_listener.accept().await.expect("accept upstream");

        let target_node =
            NodeId::from_legacy_named("node_stream_forward_target").expect("target node id");
        let endpoint =
            crate::endpoint_advertisement::daemon_lan_endpoint(&target_node, downstream_address, 0);
        let mut request =
            ObjectRpcRequest::from_encoded_body("/test/stream-forward", "test", Vec::new());
        crate::set_object_rpc_request_stream_mode(
            &mut request,
            crate::ObjectRpcRequestStreamMode::RawStreamedDaemonResponse,
        );

        let outcome = forward_streamed_daemon_rpc_to_cluster_endpoint_async(
            &endpoint,
            &request,
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
            &mut upstream,
            "upstream",
            1024,
        )
        .await
        .expect("stream forward");

        assert!(matches!(outcome, ClusterRelayStreamSendOutcome::Forwarded));
        assert_eq!(
            upstream_client.await.expect("upstream client"),
            b"alphabeta"
        );
        downstream_server.await.expect("downstream server");
    }

    #[test]
    fn cluster_target_final_hop_transport_timeouts_cap_unverified_response_budget_per_endpoint() {
        let base = DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(15));
        let shaped = cluster_target_final_hop_transport_timeouts(
            base,
            Some(Duration::from_secs(2)),
            Some(Duration::from_secs(2)),
            Some(Duration::from_secs(12)),
        );

        assert_eq!(shaped.connect_timeout, Some(Duration::from_secs(2)));
        assert_eq!(shaped.idle_timeout, Some(Duration::from_secs(2)));
    }

    #[test]
    fn cluster_target_final_hop_transport_timeouts_preserve_verified_response_budget() {
        let base = DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(15));
        let shaped = cluster_target_final_hop_transport_timeouts(
            base,
            Some(Duration::from_secs(2)),
            Some(Duration::from_secs(12)),
            Some(Duration::from_secs(12)),
        );

        assert_eq!(shaped.connect_timeout, Some(Duration::from_secs(2)));
        assert_eq!(shaped.idle_timeout, Some(Duration::from_secs(12)));
    }

    #[test]
    fn cluster_target_rpc_caps_connect_and_idle_attempts_per_endpoint() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-target-endpoint-attempt-budget");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_target_endpoint_attempt_budget")
            .expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_target_endpoint_budget_local").expect("local node");
        let target_node =
            NodeId::from_legacy_named("node_target_endpoint_budget_target").expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "target endpoint attempt budget",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut target_member =
            ClusterMemberRecord::local_voter(cluster_id, target_node.clone(), "target", "pk", now);
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![
            crate::endpoint_advertisement::daemon_lan_endpoint(
                &target_node,
                "lan-target.test:21353".to_string(),
                0,
            ),
            crate::endpoint_advertisement::daemon_lan_endpoint(
                &target_node,
                "198.51.100.76:21353".to_string(),
                10,
            ),
        ];
        state.members.push(target_member);

        let request =
            ObjectRpcRequest::encode_body("/test/target", "test", &"ping").expect("request");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = RecordingSignedHost {
            fail_first_send: true,
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::routed(&mut host, runtime.handle(), &paths);

        runtime
            .block_on(client.call_target(
                &state,
                &target_node,
                &request,
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(
                    Duration::from_secs(15),
                )),
            ))
            .expect("second endpoint should be attempted");

        assert_eq!(host.target_sends.len(), 2);
        assert_eq!(host.target_sends[0].0.address, "lan-target.test:21353");
        assert_eq!(host.target_sends[1].0.address, "198.51.100.76:21353");
        let first_timeouts = &host.target_sends[0].2;
        let first_connect = first_timeouts
            .connect_timeout
            .expect("first connect timeout");
        let first_idle = first_timeouts.idle_timeout.expect("first idle timeout");
        assert_eq!(
            first_connect,
            CLUSTER_TARGET_UNVERIFIED_ENDPOINT_ATTEMPT_MAX
        );
        assert_eq!(first_idle, CLUSTER_TARGET_UNVERIFIED_ENDPOINT_ATTEMPT_MAX);

        let second_timeouts = &host.target_sends[1].2;
        let second_connect = second_timeouts
            .connect_timeout
            .expect("second connect timeout");
        let second_idle = second_timeouts.idle_timeout.expect("second idle timeout");
        assert_eq!(
            second_connect,
            CLUSTER_TARGET_UNVERIFIED_ENDPOINT_ATTEMPT_MAX
        );
        assert_eq!(second_idle, CLUSTER_TARGET_UNVERIFIED_ENDPOINT_ATTEMPT_MAX);
    }

    #[test]
    fn cluster_target_endpoint_attempt_budget_splits_route_budget_across_fallbacks() {
        let budget = cluster_target_endpoint_attempt_budget(Some(Duration::from_secs(10)), 6, 6)
            .expect("budget");

        assert_eq!(budget, Duration::from_nanos(1_666_666_667));
    }

    #[test]
    fn cluster_target_endpoint_attempt_budget_keeps_short_timeouts_divided() {
        let budget = cluster_target_endpoint_attempt_budget(Some(Duration::from_millis(120)), 2, 2)
            .expect("budget");

        assert_eq!(budget, Duration::from_millis(60));
    }

    #[test]
    fn cluster_target_endpoint_attempt_budget_caps_unverified_endpoint() {
        let local = NodeId::from_legacy_named("node_target_endpoint_unverified_budget_local")
            .expect("local");
        let target = NodeId::from_legacy_named("node_target_endpoint_unverified_budget_target")
            .expect("target");
        let endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target,
            "unverified-target.test:21353".to_string(),
            0,
        );
        let graph = ClusterConnectivityGraph::from_records(local, Vec::new());

        let budget = cluster_target_endpoint_attempt_budget_for_endpoint(
            &graph,
            &endpoint,
            Some(Duration::from_secs(10)),
            6,
            6,
        )
        .expect("budget");

        assert_eq!(budget, Duration::from_nanos(1_666_666_667));
    }

    #[test]
    fn cluster_target_endpoint_attempt_budget_keeps_healthy_endpoint_fairly_bounded() {
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_target_endpoint_healthy_budget")
            .expect("cluster");
        let local =
            NodeId::from_legacy_named("node_target_endpoint_healthy_budget_local").expect("local");
        let target = NodeId::from_legacy_named("node_target_endpoint_healthy_budget_target")
            .expect("target");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "healthy endpoint budget",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target,
            "healthy-target.test:21353".to_string(),
            0,
        );
        let record = crate::connectivity::cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(4),
            0,
            Some("ok".to_string()),
            None,
            now,
            crate::connectivity::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let graph = ClusterConnectivityGraph::from_records(local, vec![record]);

        let budget = cluster_target_endpoint_attempt_budget_for_endpoint(
            &graph,
            &endpoint,
            Some(Duration::from_secs(10)),
            6,
            6,
        )
        .expect("budget");

        assert_eq!(budget, Duration::from_nanos(1_666_666_667));
    }

    #[test]
    fn cluster_target_endpoint_attempt_budget_bounds_healthy_synthetic_tcp_tunnel_with_fallbacks() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_target_tunnel_budget").expect("cluster");
        let local = NodeId::from_legacy_named("node_target_tunnel_budget_local").expect("local");
        let target = NodeId::from_legacy_named("node_target_tunnel_budget_target").expect("target");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "tunnel endpoint budget",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_target_tunnel_budget")
                .expect("endpoint"),
            node_id: target,
            transport: ClusterTransportKind::LanTcp,
            address: format!(
                "{}node_target_tunnel_budget_target",
                crate::CLUSTER_TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX
            ),
            source: ClusterEndpointSource::Observed,
            privacy: ClusterEndpointPrivacy::Relayed,
            priority: 0,
            last_observed_latency_ms: None,
        };
        let record = crate::connectivity::cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(0),
            0,
            Some("ok".to_string()),
            None,
            now,
            crate::connectivity::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let graph = ClusterConnectivityGraph::from_records(local, vec![record]);

        let budget = cluster_target_endpoint_attempt_budget_for_endpoint(
            &graph,
            &endpoint,
            Some(Duration::from_secs(60)),
            3,
            3,
        )
        .expect("budget");

        assert_eq!(budget, CLUSTER_TARGET_UNVERIFIED_ENDPOINT_ATTEMPT_MAX);
    }

    #[test]
    fn cluster_target_endpoint_attempt_budget_trusts_healthy_synthetic_tcp_tunnel_when_it_is_last()
    {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_target_tunnel_only_budget").expect("cluster");
        let local =
            NodeId::from_legacy_named("node_target_tunnel_only_budget_local").expect("local");
        let target =
            NodeId::from_legacy_named("node_target_tunnel_only_budget_target").expect("target");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "tunnel-only endpoint budget",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_target_tunnel_only_budget")
                .expect("endpoint"),
            node_id: target,
            transport: ClusterTransportKind::LanTcp,
            address: format!(
                "{}node_target_tunnel_only_budget_target",
                crate::CLUSTER_TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX
            ),
            source: ClusterEndpointSource::Observed,
            privacy: ClusterEndpointPrivacy::Relayed,
            priority: 0,
            last_observed_latency_ms: None,
        };
        let record = crate::connectivity::cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(0),
            0,
            Some("ok".to_string()),
            None,
            now,
            crate::connectivity::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let graph = ClusterConnectivityGraph::from_records(local, vec![record]);

        let budget = cluster_target_endpoint_attempt_budget_for_endpoint(
            &graph,
            &endpoint,
            Some(Duration::from_secs(60)),
            1,
            3,
        )
        .expect("budget");

        assert_eq!(budget, CLUSTER_TARGET_ENDPOINT_ATTEMPT_MAX);
    }

    #[test]
    fn cluster_target_final_hop_attempt_budget_expands_verified_direct_response_window() {
        let split_budget = Some(Duration::from_millis(800));
        let preferred = cluster_target_final_hop_attempt_budget(
            Some(Duration::from_secs(4)),
            5,
            split_budget,
            true,
            false,
        )
        .expect("preferred budget");

        assert_eq!(
            preferred,
            CLUSTER_TARGET_VERIFIED_DIRECT_ENDPOINT_ATTEMPT_PREFERRED
        );

        let unverified = cluster_target_final_hop_attempt_budget(
            Some(Duration::from_secs(4)),
            5,
            split_budget,
            false,
            false,
        )
        .expect("unverified budget");
        assert_eq!(unverified, Duration::from_millis(800));

        let streamed = cluster_target_final_hop_attempt_budget(
            Some(Duration::from_secs(4)),
            5,
            split_budget,
            true,
            true,
        )
        .expect("streamed budget");
        assert_eq!(
            streamed,
            CLUSTER_TARGET_VERIFIED_DIRECT_ENDPOINT_ATTEMPT_PREFERRED
        );

        let streamed_last_endpoint = cluster_target_final_hop_attempt_budget(
            Some(Duration::from_secs(4)),
            1,
            split_budget,
            true,
            true,
        )
        .expect("streamed last endpoint budget");
        assert_eq!(streamed_last_endpoint, Duration::from_secs(4));
    }

    #[test]
    fn cluster_target_final_hop_attempt_budget_keeps_unverified_full_response_fair() {
        let split_budget = Some(Duration::from_millis(1000));
        let budget = cluster_target_final_hop_attempt_budget(
            Some(Duration::from_secs(5)),
            5,
            split_budget,
            false,
            true,
        )
        .expect("full response budget");

        assert_eq!(budget, split_budget.expect("split budget"));
    }

    #[test]
    fn cluster_target_final_hop_attempt_budget_caps_long_ordinary_verified_endpoint() {
        let budget = cluster_target_final_hop_attempt_budget(
            Some(Duration::from_secs(60)),
            3,
            Some(CLUSTER_TARGET_ENDPOINT_ATTEMPT_MAX),
            true,
            false,
        )
        .expect("ordinary budget");

        assert_eq!(
            budget,
            CLUSTER_TARGET_VERIFIED_DIRECT_ENDPOINT_ATTEMPT_PREFERRED
        );

        let streamed = cluster_target_final_hop_attempt_budget(
            Some(Duration::from_secs(60)),
            3,
            Some(CLUSTER_TARGET_ENDPOINT_ATTEMPT_MAX),
            true,
            true,
        )
        .expect("streamed budget");
        assert_eq!(
            streamed,
            CLUSTER_TARGET_VERIFIED_DIRECT_ENDPOINT_ATTEMPT_PREFERRED
        );
    }

    #[test]
    fn cluster_target_final_hop_request_metadata_uses_full_response_budget() {
        let ordinary = ObjectRpcRequest::from_encoded_body("/test/control", "test", Vec::new());
        assert!(!cluster_target_final_hop_request_uses_full_response_budget(
            &ordinary
        ));

        let data_plane = ObjectRpcRequest::from_encoded_body("/test/data", "test", Vec::new())
            .with_full_response_budget();
        assert!(cluster_target_final_hop_request_uses_full_response_budget(
            &data_plane
        ));

        let budget = cluster_target_final_hop_attempt_budget(
            Some(Duration::from_secs(60)),
            3,
            Some(CLUSTER_TARGET_ENDPOINT_ATTEMPT_MAX),
            true,
            cluster_target_final_hop_request_uses_full_response_budget(&data_plane),
        )
        .expect("data-plane budget");
        assert_eq!(
            budget,
            CLUSTER_TARGET_VERIFIED_DIRECT_ENDPOINT_ATTEMPT_PREFERRED
        );

        let last_endpoint_budget = cluster_target_final_hop_attempt_budget(
            Some(Duration::from_secs(60)),
            1,
            Some(CLUSTER_TARGET_ENDPOINT_ATTEMPT_MAX),
            true,
            cluster_target_final_hop_request_uses_full_response_budget(&data_plane),
        )
        .expect("data-plane last endpoint budget");
        assert_eq!(last_endpoint_budget, Duration::from_secs(60));
    }

    #[test]
    fn network_attempt_budget_matches_bounded_model_cases() {
        let ordinary_unverified = cluster_target_final_hop_attempt_budget(
            Some(Duration::from_secs(5)),
            5,
            Some(Duration::from_secs(1)),
            false,
            false,
        )
        .expect("ordinary unverified budget");
        assert_eq!(ordinary_unverified, Duration::from_secs(1));

        let full_response_unverified = cluster_target_final_hop_attempt_budget(
            Some(Duration::from_secs(5)),
            5,
            Some(Duration::from_secs(1)),
            false,
            true,
        )
        .expect("full response unverified budget");
        assert_eq!(full_response_unverified, Duration::from_secs(1));

        let verified_direct = cluster_target_final_hop_attempt_budget(
            Some(Duration::from_secs(5)),
            5,
            Some(Duration::from_secs(1)),
            true,
            false,
        )
        .expect("verified direct budget");
        assert_eq!(
            verified_direct,
            CLUSTER_TARGET_VERIFIED_DIRECT_ENDPOINT_ATTEMPT_PREFERRED
        );

        let full_response_last_endpoint = cluster_target_final_hop_attempt_budget(
            Some(Duration::from_secs(5)),
            1,
            Some(Duration::from_secs(1)),
            false,
            true,
        )
        .expect("last endpoint full response budget");
        assert_eq!(full_response_last_endpoint, Duration::from_secs(5));

        let full_response_verified_direct = cluster_target_final_hop_attempt_budget(
            Some(Duration::from_secs(5)),
            5,
            Some(Duration::from_secs(1)),
            true,
            true,
        )
        .expect("verified full response budget");
        assert_eq!(
            full_response_verified_direct,
            CLUSTER_TARGET_VERIFIED_DIRECT_ENDPOINT_ATTEMPT_PREFERRED
        );
    }

    #[test]
    fn network_endpoint_order_keeps_tunnel_as_fallback() {
        let target =
            NodeId::from_legacy_named("node_quint_route_endpoint_order_target").expect("target");
        let direct_endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named(
                "endpoint_quint_route_endpoint_order_direct",
            )
            .expect("direct endpoint"),
            node_id: target.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "10.0.0.42:21353".to_string(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 0,
            last_observed_latency_ms: None,
        };
        let tunnel_endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named(
                "endpoint_quint_route_endpoint_order_tunnel",
            )
            .expect("tunnel endpoint"),
            node_id: target,
            transport: ClusterTransportKind::LanTcp,
            address: format!(
                "{}node_quint_route_endpoint_order_target",
                crate::CLUSTER_TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX
            ),
            source: ClusterEndpointSource::Observed,
            privacy: ClusterEndpointPrivacy::Relayed,
            priority: 0,
            last_observed_latency_ms: None,
        };

        assert!(
            cluster_delivery_endpoint_attempt_sort_rank(&direct_endpoint)
                < cluster_delivery_endpoint_attempt_sort_rank(&tunnel_endpoint),
            "real direct endpoints must be attempted before synthetic tunnel fallbacks"
        );
        assert!(cluster_target_endpoint_should_reserve_fallback_budget(
            &tunnel_endpoint,
            2
        ));
    }

    #[test]
    fn cluster_target_rpc_retries_after_unverified_endpoint_timeout() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-target-unverified-timeout-retry");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_target_unverified_timeout_retry")
            .expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_target_unverified_timeout_local").expect("local node");
        let target_node = NodeId::from_legacy_named("node_target_unverified_timeout_target")
            .expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "target unverified timeout retry",
            local_node,
            "local",
            "pk-local",
            now,
        );
        let mut target_member =
            ClusterMemberRecord::local_voter(cluster_id, target_node.clone(), "target", "pk", now);
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![
            crate::endpoint_advertisement::daemon_lan_endpoint(
                &target_node,
                "stalling-target.test:21353".to_string(),
                0,
            ),
            crate::endpoint_advertisement::daemon_lan_endpoint(
                &target_node,
                "retry-target.test:21353".to_string(),
                10,
            ),
        ];
        state.members.push(target_member);

        let request = ObjectRpcRequest::encode_body("/test/target", "test", &"ping")
            .expect("request")
            .with_full_response_budget();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = RecordingSignedHost {
            stall_first_send: Some(Duration::from_secs(60)),
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::routed(&mut host, runtime.handle(), &paths);
        let started = Instant::now();

        runtime
            .block_on(client.call_target(
                &state,
                &target_node,
                &request,
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(
                    Duration::from_secs(15),
                )),
            ))
            .expect("second endpoint should be attempted");

        assert!(
            started.elapsed() < Duration::from_secs(4),
            "unverified stalling endpoint should not consume the whole target timeout"
        );
        assert_eq!(host.target_sends.len(), 2);
        assert_eq!(host.target_sends[0].0.address, "stalling-target.test:21353");
        assert_eq!(host.target_sends[1].0.address, "retry-target.test:21353");
    }

    #[test]
    fn cluster_target_rpc_relay_timeouts_preserve_direct_target_fallback_budget() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-target-relay-fallback-budget");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_target_relay_fallback_budget")
            .expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_target_relay_fallback_local").expect("local node");
        let relay_node =
            NodeId::from_legacy_named("node_target_relay_fallback_relay").expect("relay node");
        let target_node =
            NodeId::from_legacy_named("node_target_relay_fallback_target").expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "target relay fallback budget",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );

        let relay_lan_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &relay_node,
            "relay-a.example.test:21353".to_string(),
            0,
        );
        let relay_tunnel_endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named(
                "endpoint_target_relay_fallback_tunnel",
            )
            .expect("relay tunnel endpoint"),
            node_id: relay_node.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: format!(
                "{}node_target_relay_fallback_relay",
                crate::CLUSTER_TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX
            ),
            source: ClusterEndpointSource::Observed,
            privacy: ClusterEndpointPrivacy::Relayed,
            priority: 10,
            last_observed_latency_ms: None,
        };
        let direct_target_endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named(
                "endpoint_target_relay_fallback_direct",
            )
            .expect("direct target endpoint"),
            node_id: target_node.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "203.0.113.77:21353".to_string(),
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
        relay_member.endpoints = vec![relay_lan_endpoint.clone(), relay_tunnel_endpoint.clone()];
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

        let local_to_relay_lan = crate::connectivity::cluster_connectivity_record(
            &state,
            &relay_lan_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some(now.to_string()),
            None,
            now,
            crate::connectivity::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let local_to_relay_tunnel = crate::connectivity::cluster_connectivity_record(
            &state,
            &relay_tunnel_endpoint,
            ClusterConnectivityState::Healthy,
            Some(2),
            0,
            Some(now.to_string()),
            None,
            now,
            crate::connectivity::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        crate::connectivity_store::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[local_to_relay_lan, local_to_relay_tunnel],
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
            crate::connectivity::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        crate::connectivity_store::write_local_cluster_connectivity_record_set(
            &paths,
            &relay_node,
            &[relay_to_target],
        )
        .expect("write relay connectivity");

        let request =
            ObjectRpcRequest::encode_body("/test/target", "test", &"ping").expect("request");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = RecordingSignedHost {
            stall_first_send: Some(Duration::from_secs(60)),
            stall_second_send: Some(Duration::from_secs(60)),
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::routed(&mut host, runtime.handle(), &paths);
        let started = Instant::now();

        runtime
            .block_on(client.call_target(
                &state,
                &target_node,
                &request,
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(
                    Duration::from_secs(5),
                )),
            ))
            .expect("direct target fallback should retain budget after relay timeouts");

        assert!(
            started.elapsed() < Duration::from_secs(4),
            "relay endpoints must not consume the whole target timeout before direct fallback"
        );
        assert_eq!(host.target_sends.len(), 3);
        assert_eq!(host.target_sends[0].0.node_id, relay_node);
        assert_eq!(host.target_sends[1].0.node_id, relay_node);
        assert_eq!(
            host.target_sends[2].0.endpoint_id,
            direct_target_endpoint.endpoint_id
        );
    }

    #[test]
    fn cluster_target_rpc_does_not_let_tunnel_fallback_starve_other_endpoints() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-target-healthy-timeout-retry");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_target_healthy_timeout_retry")
            .expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_target_healthy_timeout_local").expect("local node");
        let target_node =
            NodeId::from_legacy_named("node_target_healthy_timeout_target").expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "target healthy timeout retry",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let tunnel_endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named(
                "endpoint_target_healthy_timeout_tunnel",
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
        let mut target_member =
            ClusterMemberRecord::local_voter(cluster_id, target_node.clone(), "target", "pk", now);
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        let public_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "198.51.100.76:21353".to_string(),
            5,
        );
        target_member.endpoints = vec![
            tunnel_endpoint.clone(),
            public_endpoint.clone(),
            crate::endpoint_advertisement::daemon_lan_endpoint(
                &target_node,
                "fallback-a.test:21353".to_string(),
                10,
            ),
            crate::endpoint_advertisement::daemon_lan_endpoint(
                &target_node,
                "fallback-b.test:21353".to_string(),
                20,
            ),
            crate::endpoint_advertisement::daemon_lan_endpoint(
                &target_node,
                "fallback-c.test:21353".to_string(),
                30,
            ),
        ];
        state.members.push(target_member);
        crate::connectivity_store::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[
                crate::connectivity::cluster_connectivity_record(
                    &state,
                    &tunnel_endpoint,
                    ClusterConnectivityState::Healthy,
                    Some(0),
                    0,
                    Some("ok".to_string()),
                    None,
                    now,
                    60,
                ),
                crate::connectivity::cluster_connectivity_record(
                    &state,
                    &public_endpoint,
                    ClusterConnectivityState::Degraded,
                    None,
                    1,
                    Some("ok".to_string()),
                    None,
                    now,
                    60,
                ),
            ],
        )
        .expect("write local connectivity");

        let request =
            ObjectRpcRequest::encode_body("/test/target", "test", &"ping").expect("request");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = RecordingSignedHost {
            stall_first_send: Some(Duration::from_secs(60)),
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::routed(&mut host, runtime.handle(), &paths);
        let started = Instant::now();

        runtime
            .block_on(client.call_target(
                &state,
                &target_node,
                &request,
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(
                    Duration::from_secs(30),
                )),
            ))
            .expect("fallback endpoint should be attempted within the target timeout");

        assert!(
            started.elapsed() < Duration::from_secs(4),
            "a stale preferred endpoint must not spend most of the route timeout"
        );
        assert_eq!(host.target_sends.len(), 2);
        assert_ne!(
            host.target_sends[0].0.endpoint_id, tunnel_endpoint.endpoint_id,
            "synthetic tunnel should remain a fallback behind real endpoints"
        );
        assert_ne!(
            host.target_sends[1].0.endpoint_id, tunnel_endpoint.endpoint_id,
            "a real endpoint fallback should be attempted before the synthetic tunnel"
        );
    }

    #[test]
    fn cluster_target_rpc_does_not_let_slow_reachable_data_plane_endpoint_starve_fallback() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-target-data-plane-budget");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_target_data_plane_budget").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_target_data_plane_budget_local").expect("local node");
        let target_node =
            NodeId::from_legacy_named("node_target_data_plane_budget_target").expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "target data-plane budget",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let tunnel_endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named(
                "endpoint_target_data_plane_budget_tunnel",
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
        let direct_endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named(
                "endpoint_target_data_plane_budget_direct",
            )
            .expect("endpoint"),
            node_id: target_node.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "direct-target.example.test:21353".to_string(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::InternetDirect,
            priority: 10,
            last_observed_latency_ms: None,
        };
        let fallback_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "fallback-target.example.test:21353".to_string(),
            20,
        );
        let mut target_member =
            ClusterMemberRecord::local_voter(cluster_id, target_node.clone(), "target", "pk", now);
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![
            tunnel_endpoint.clone(),
            direct_endpoint.clone(),
            fallback_endpoint,
        ];
        state.members.push(target_member);
        crate::connectivity_store::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[
                crate::connectivity::cluster_connectivity_record(
                    &state,
                    &tunnel_endpoint,
                    ClusterConnectivityState::Healthy,
                    Some(0),
                    0,
                    Some("ok".to_string()),
                    None,
                    now,
                    60,
                ),
                crate::connectivity::cluster_connectivity_record(
                    &state,
                    &direct_endpoint,
                    ClusterConnectivityState::Degraded,
                    None,
                    1,
                    Some("ok".to_string()),
                    Some("slow".to_string()),
                    now,
                    60,
                ),
            ],
        )
        .expect("write local connectivity");

        let request = ObjectRpcRequest::encode_body("/test/data", "test", &"ping")
            .expect("request")
            .with_full_response_budget();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = RecordingSignedHost {
            stall_first_send: Some(Duration::from_secs(60)),
            stall_second_send: Some(Duration::from_secs(5)),
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::routed(&mut host, runtime.handle(), &paths);
        let started = Instant::now();

        runtime
            .block_on(client.call_target(
                &state,
                &target_node,
                &request,
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(
                    Duration::from_secs(60),
                )),
            ))
            .expect("fallback endpoint should be attempted inside the route budget");

        assert!(
            started.elapsed() < Duration::from_secs(6),
            "slow data-plane endpoint should not consume the caller's 60s timeout before fallback"
        );
        assert_eq!(host.target_sends.len(), 3);
        assert_ne!(
            host.target_sends[0].0.endpoint_id, tunnel_endpoint.endpoint_id,
            "synthetic tunnel should remain behind real endpoints"
        );
        assert!(
            host.target_sends
                .iter()
                .any(|(endpoint, _, _)| endpoint.address == "fallback-target.example.test:21353"),
            "fallback endpoint should be attempted"
        );
        let direct_idle_timeout = host
            .target_sends
            .iter()
            .find(|(endpoint, _, _)| endpoint.endpoint_id == direct_endpoint.endpoint_id)
            .expect("direct endpoint send")
            .2
            .idle_timeout
            .expect("direct idle timeout");
        assert!(
            direct_idle_timeout <= CLUSTER_TARGET_ROUTE_DEADLINE_MAX,
            "direct data-plane idle timeout should stay inside route budget, got {direct_idle_timeout:?}"
        );
    }

    #[test]
    fn cluster_target_rpc_caps_long_caller_timeout_to_route_budget() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-target-route-budget-cap");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_target_route_budget_cap").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_target_route_budget_cap_local").expect("local node");
        let target_node =
            NodeId::from_legacy_named("node_target_route_budget_cap_target").expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "target route budget cap",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "198.51.100.44:21353".to_string(),
            0,
        );
        let mut target_member =
            ClusterMemberRecord::local_voter(cluster_id, target_node.clone(), "target", "pk", now);
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![endpoint.clone()];
        state.members.push(target_member);
        crate::connectivity_store::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[crate::connectivity::cluster_connectivity_record(
                &state,
                &endpoint,
                ClusterConnectivityState::Healthy,
                Some(5),
                0,
                Some("ok".to_string()),
                None,
                now,
                60,
            )],
        )
        .expect("write local connectivity");

        let request = ObjectRpcRequest::encode_body("/test/data", "test", &"ping")
            .expect("request")
            .with_full_response_budget();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = RecordingSignedHost::default();
        let mut client = ClusterObjectRpcClient::routed(&mut host, runtime.handle(), &paths);

        runtime
            .block_on(client.call_target(
                &state,
                &target_node,
                &request,
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(
                    Duration::from_secs(60),
                )),
            ))
            .expect("target rpc");

        assert_eq!(host.target_sends.len(), 1);
        let connect_timeout = host.target_sends[0]
            .2
            .connect_timeout
            .expect("connect timeout");
        let idle_timeout = host.target_sends[0].2.idle_timeout.expect("idle timeout");
        assert!(
            connect_timeout <= CLUSTER_TARGET_ROUTE_DEADLINE_MAX,
            "connect timeout should be capped by route budget, got {connect_timeout:?}"
        );
        assert!(
            idle_timeout <= CLUSTER_TARGET_ROUTE_DEADLINE_MAX,
            "idle timeout should be capped by route budget, got {idle_timeout:?}"
        );
    }

    #[test]
    fn cluster_target_rpc_does_not_let_healthy_direct_full_response_starve_fallbacks() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-target-direct-timeout-retry");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_target_direct_timeout_retry")
            .expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_target_direct_timeout_local").expect("local node");
        let target_node =
            NodeId::from_legacy_named("node_target_direct_timeout_target").expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "target healthy direct timeout retry",
            local_node,
            "local",
            "pk-local",
            now,
        );
        let direct_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "198.51.100.88:21353".to_string(),
            0,
        );
        let fallback_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "fallback-direct.test:21353".to_string(),
            10,
        );
        let mut target_member =
            ClusterMemberRecord::local_voter(cluster_id, target_node.clone(), "target", "pk", now);
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![direct_endpoint.clone(), fallback_endpoint.clone()];
        state.members.push(target_member);
        crate::connectivity_store::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[crate::connectivity::cluster_connectivity_record(
                &state,
                &direct_endpoint,
                ClusterConnectivityState::Healthy,
                Some(4),
                0,
                Some("ok".to_string()),
                None,
                now,
                60,
            )],
        )
        .expect("write local connectivity");

        let request = ObjectRpcRequest::encode_body("/test/data", "test", &"ping")
            .expect("request")
            .with_full_response_budget();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = RecordingSignedHost {
            stall_first_send: Some(Duration::from_secs(60)),
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::routed(&mut host, runtime.handle(), &paths);
        let started = Instant::now();

        runtime
            .block_on(client.call_target(
                &state,
                &target_node,
                &request,
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(
                    Duration::from_secs(30),
                )),
            ))
            .expect("fallback endpoint should be attempted within the target timeout");

        assert!(
            started.elapsed() < Duration::from_secs(5),
            "a stale healthy direct full-response endpoint must not spend the 10s response window before fallback"
        );
        assert_eq!(host.target_sends.len(), 2);
        assert_eq!(
            host.target_sends[0].0.endpoint_id,
            direct_endpoint.endpoint_id
        );
        assert_eq!(
            host.target_sends[1].0.endpoint_id,
            fallback_endpoint.endpoint_id
        );
    }

    #[test]
    fn cluster_target_rpc_preserves_verified_direct_final_hop_response_budget_for_streamed_request()
    {
        let root = sinomo_testdir::TestDir::new("sinomo-network-target-verified-response-budget");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_target_verified_response_budget")
            .expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_target_verified_response_local").expect("local node");
        let target_node =
            NodeId::from_legacy_named("node_target_verified_response_target").expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "target verified response budget",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let public_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "198.51.100.76:21353".to_string(),
            0,
        );
        let mut target_member =
            ClusterMemberRecord::local_voter(cluster_id, target_node.clone(), "target", "pk", now);
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![
            public_endpoint.clone(),
            crate::endpoint_advertisement::daemon_lan_endpoint(
                &target_node,
                "fallback-a.test:21353".to_string(),
                10,
            ),
            crate::endpoint_advertisement::daemon_lan_endpoint(
                &target_node,
                "fallback-b.test:21353".to_string(),
                20,
            ),
            crate::endpoint_advertisement::daemon_lan_endpoint(
                &target_node,
                "fallback-c.test:21353".to_string(),
                30,
            ),
        ];
        state.members.push(target_member);
        crate::connectivity_store::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[crate::connectivity::cluster_connectivity_record(
                &state,
                &public_endpoint,
                ClusterConnectivityState::Healthy,
                Some(30),
                0,
                Some("ok".to_string()),
                None,
                now,
                60,
            )],
        )
        .expect("write local connectivity");

        let mut request =
            ObjectRpcRequest::encode_body("/test/target", "test", &"ping").expect("request");
        crate::set_object_rpc_request_stream_mode(
            &mut request,
            crate::ObjectRpcRequestStreamMode::RawStreamedDaemonResponse,
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = RecordingSignedHost {
            stall_first_send: Some(Duration::from_millis(1500)),
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::routed(&mut host, runtime.handle(), &paths);

        runtime
            .block_on(client.call_target(
                &state,
                &target_node,
                &request,
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(
                    Duration::from_secs(4),
                )),
            ))
            .expect("verified direct endpoint should keep the full response budget");

        assert_eq!(host.target_sends.len(), 1);
        assert_eq!(
            host.target_sends[0].0.endpoint_id,
            public_endpoint.endpoint_id
        );
        let connect_timeout = host.target_sends[0]
            .2
            .connect_timeout
            .expect("connect timeout");
        assert!(
            connect_timeout > Duration::from_millis(900)
                && connect_timeout <= Duration::from_secs(1),
            "connect timeout should be roughly one quarter of the 4s route budget, got {connect_timeout:?}"
        );
        let idle_timeout = host.target_sends[0].2.idle_timeout.expect("idle timeout");
        assert!(
            idle_timeout >= Duration::from_millis(3400)
                && idle_timeout <= CLUSTER_TARGET_VERIFIED_DIRECT_ENDPOINT_ATTEMPT_PREFERRED,
            "idle timeout should preserve a long response window while reserving fallback budget, got {idle_timeout:?}"
        );
    }

    #[test]
    fn cluster_target_rpc_retries_after_verified_non_stream_endpoint_timeout() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-target-verified-non-stream-retry");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_target_verified_non_stream_retry")
            .expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_target_verified_non_stream_local").expect("local node");
        let target_node = NodeId::from_legacy_named("node_target_verified_non_stream_target")
            .expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "target verified non-stream retry",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let public_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "198.51.100.76:21353".to_string(),
            0,
        );
        let fallback_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "fallback-a.test:21353".to_string(),
            10,
        );
        let mut target_member =
            ClusterMemberRecord::local_voter(cluster_id, target_node.clone(), "target", "pk", now);
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![public_endpoint.clone(), fallback_endpoint.clone()];
        state.members.push(target_member);
        crate::connectivity_store::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[crate::connectivity::cluster_connectivity_record(
                &state,
                &public_endpoint,
                ClusterConnectivityState::Healthy,
                Some(30),
                0,
                Some("ok".to_string()),
                None,
                now,
                60,
            )],
        )
        .expect("write local connectivity");

        let request =
            ObjectRpcRequest::encode_body("/test/target", "test", &"ping").expect("request");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = RecordingSignedHost {
            stall_first_send: Some(Duration::from_secs(60)),
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::routed(&mut host, runtime.handle(), &paths);
        let started = Instant::now();

        runtime
            .block_on(client.call_target(
                &state,
                &target_node,
                &request,
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(
                    Duration::from_secs(4),
                )),
            ))
            .expect("fallback endpoint should be attempted for ordinary RPC");

        assert!(
            started.elapsed() < Duration::from_secs(4),
            "ordinary RPC fallback should stay within the route timeout"
        );
        assert_eq!(host.target_sends.len(), 2);
        assert_eq!(
            host.target_sends[0].0.endpoint_id,
            public_endpoint.endpoint_id
        );
        let first_connect_timeout = host.target_sends[0]
            .2
            .connect_timeout
            .expect("first endpoint connect timeout");
        assert!(
            first_connect_timeout >= Duration::from_millis(1900)
                && first_connect_timeout <= Duration::from_secs(2),
            "healthy direct endpoint connect should keep the fair split budget, got {first_connect_timeout:?}"
        );
        let first_idle_timeout = host.target_sends[0]
            .2
            .idle_timeout
            .expect("first endpoint idle timeout");
        assert!(
            first_idle_timeout >= Duration::from_secs(3)
                && first_idle_timeout <= Duration::from_millis(3500),
            "healthy direct endpoint should keep the preferred app-facing response budget, got {first_idle_timeout:?}"
        );
        assert_eq!(
            host.target_sends[1].0.endpoint_id,
            fallback_endpoint.endpoint_id
        );
        assert_eq!(host.observations.len(), 1);
        assert_eq!(
            host.observations[0].0.endpoint_id,
            public_endpoint.endpoint_id
        );
        assert!(host.observations[0].1.is_io_failure());
    }

    #[test]
    fn cluster_target_rpc_retries_after_endpoint_error_response() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-target-endpoint-error-retry");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_target_endpoint_error_retry")
            .expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_target_endpoint_error_local").expect("local node");
        let target_node =
            NodeId::from_legacy_named("node_target_endpoint_error_target").expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "target endpoint error retry",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut target_member =
            ClusterMemberRecord::local_voter(cluster_id, target_node.clone(), "target", "pk", now);
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![
            crate::endpoint_advertisement::daemon_lan_endpoint(
                &target_node,
                "auth-error-target.test:21353".to_string(),
                0,
            ),
            crate::endpoint_advertisement::daemon_lan_endpoint(
                &target_node,
                "retry-target.test:21353".to_string(),
                10,
            ),
        ];
        state.members.push(target_member);

        let request =
            ObjectRpcRequest::encode_body("/test/target", "test", &"ping").expect("request");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = RecordingSignedHost {
            error_first_send: true,
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::routed(&mut host, runtime.handle(), &paths);

        let response = runtime
            .block_on(client.call_target(
                &state,
                &target_node,
                &request,
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(
                    Duration::from_secs(15),
                )),
            ))
            .expect("second endpoint should be attempted");

        assert_eq!(
            response.decode_body::<String>().expect("response body"),
            "ok"
        );
        assert_eq!(host.target_sends.len(), 2);
        assert_eq!(
            host.target_sends[0].0.address,
            "auth-error-target.test:21353"
        );
        assert_eq!(host.target_sends[1].0.address, "retry-target.test:21353");
        assert_eq!(host.target_auths.len(), 2);
        assert_ne!(
            host.target_auths[0], host.target_auths[1],
            "each endpoint retry must carry a fresh daemon auth nonce"
        );
    }

    #[test]
    fn cluster_target_rpc_hard_times_out_endpoint_attempts() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-target-hard-attempt-timeout");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_target_hard_attempt_timeout")
            .expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_target_hard_timeout_local").expect("local node");
        let target_node =
            NodeId::from_legacy_named("node_target_hard_timeout_target").expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "target hard attempt timeout",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut target_member =
            ClusterMemberRecord::local_voter(cluster_id, target_node.clone(), "target", "pk", now);
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![
            crate::endpoint_advertisement::daemon_lan_endpoint(
                &target_node,
                "stall-target.test:21353".to_string(),
                0,
            ),
            crate::endpoint_advertisement::daemon_lan_endpoint(
                &target_node,
                "fast-target.test:21353".to_string(),
                10,
            ),
        ];
        state.members.push(target_member);

        let request =
            ObjectRpcRequest::encode_body("/test/target", "test", &"ping").expect("request");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = RecordingSignedHost {
            stall_first_send: Some(Duration::from_millis(200)),
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::routed(&mut host, runtime.handle(), &paths);
        let started = Instant::now();

        runtime
            .block_on(client.call_target(
                &state,
                &target_node,
                &request,
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(
                    Duration::from_millis(120),
                )),
            ))
            .expect("second endpoint should be attempted after hard timeout");

        assert!(
            started.elapsed() < Duration::from_millis(180),
            "hard endpoint timeout should abort the stalled send before it sleeps fully"
        );
        assert_eq!(host.target_sends.len(), 2);
        assert_eq!(host.target_sends[0].0.address, "stall-target.test:21353");
        assert_eq!(host.target_sends[1].0.address, "fast-target.test:21353");
    }

    #[test]
    fn cluster_target_stream_routes_to_stream_endpoint_method() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-target-stream-route");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_target_stream_route").expect("cluster id");
        let local_node = NodeId::from_legacy_named("node_target_stream_local").expect("local node");
        let target_node =
            NodeId::from_legacy_named("node_target_stream_target").expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "target stream route",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut target_member =
            ClusterMemberRecord::local_voter(cluster_id, target_node.clone(), "target", "pk", now);
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "stream-target.test:21353".to_string(),
            0,
        )];
        state.members.push(target_member);

        let mut request =
            ObjectRpcRequest::encode_body("/test/stream-target", "test", &"ping").expect("request");
        crate::set_object_rpc_request_stream_mode(
            &mut request,
            crate::ObjectRpcRequestStreamMode::RawStreamedDaemonResponse,
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = RecordingSignedHost::default();
        let mut client = ClusterObjectRpcClient::routed(&mut host, runtime.handle(), &paths);

        let mut response = runtime
            .block_on(client.call_target_stream(
                &state,
                &target_node,
                &request,
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(
                    Duration::from_secs(15),
                )),
            ))
            .expect("stream target route");
        let body = runtime
            .block_on(response.body.collect_to_vec())
            .expect("stream body");

        assert_eq!(response.response.route, "/test/stream-target");
        assert_eq!(body, b"stream-body");
        assert_eq!(host.target_sends.len(), 0);
        assert_eq!(host.stream_target_sends.len(), 1);
        assert_eq!(
            host.stream_target_sends[0].0.address,
            "stream-target.test:21353"
        );
    }

    #[test]
    fn cluster_target_stream_retries_after_endpoint_error_response() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-target-stream-error-retry");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_target_stream_error_retry").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_target_stream_error_local").expect("local node");
        let target_node =
            NodeId::from_legacy_named("node_target_stream_error_target").expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "target stream endpoint error retry",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut target_member =
            ClusterMemberRecord::local_voter(cluster_id, target_node.clone(), "target", "pk", now);
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![
            crate::endpoint_advertisement::daemon_lan_endpoint(
                &target_node,
                "stream-auth-error-target.test:21353".to_string(),
                0,
            ),
            crate::endpoint_advertisement::daemon_lan_endpoint(
                &target_node,
                "stream-retry-target.test:21353".to_string(),
                10,
            ),
        ];
        state.members.push(target_member);

        let mut request =
            ObjectRpcRequest::encode_body("/test/stream-target", "test", &"ping").expect("request");
        crate::set_object_rpc_request_stream_mode(
            &mut request,
            crate::ObjectRpcRequestStreamMode::RawStreamedDaemonResponse,
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = RecordingSignedHost {
            stream_error_first_send: true,
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::routed(&mut host, runtime.handle(), &paths);

        let mut response = runtime
            .block_on(client.call_target_stream(
                &state,
                &target_node,
                &request,
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(
                    Duration::from_secs(15),
                )),
            ))
            .expect("second stream endpoint should be attempted");
        let body = runtime
            .block_on(response.body.collect_to_vec())
            .expect("stream body");

        assert_eq!(response.response.route, "/test/stream-target");
        assert_eq!(body, b"stream-body");
        assert_eq!(host.stream_target_sends.len(), 2);
        assert_eq!(
            host.stream_target_sends[0].0.address,
            "stream-auth-error-target.test:21353"
        );
        assert_eq!(
            host.stream_target_sends[1].0.address,
            "stream-retry-target.test:21353"
        );
        assert_eq!(host.stream_target_auths.len(), 2);
        assert_ne!(
            host.stream_target_auths[0], host.stream_target_auths[1],
            "each streamed endpoint retry must carry a fresh daemon auth nonce"
        );
    }

    #[test]
    fn cluster_target_stream_records_route_timeout_observation_before_retry() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-target-stream-timeout-observe");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_target_stream_timeout_observe")
            .expect("cluster id");
        let local_node = NodeId::from_legacy_named("node_target_stream_timeout_observe_local")
            .expect("local node");
        let target_node = NodeId::from_legacy_named("node_target_stream_timeout_observe_target")
            .expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "target stream timeout observation",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let stalling_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "stream-stall-target.test:21353".to_string(),
            0,
        );
        let fallback_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "stream-fallback-target.test:21353".to_string(),
            10,
        );
        let mut target_member =
            ClusterMemberRecord::local_voter(cluster_id, target_node.clone(), "target", "pk", now);
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![stalling_endpoint.clone(), fallback_endpoint.clone()];
        state.members.push(target_member);

        let mut request =
            ObjectRpcRequest::encode_body("/test/stream-target", "test", &"ping").expect("request");
        crate::set_object_rpc_request_stream_mode(
            &mut request,
            crate::ObjectRpcRequestStreamMode::RawStreamedDaemonResponse,
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = RecordingSignedHost {
            stall_first_stream_send: Some(Duration::from_secs(60)),
            ..RecordingSignedHost::default()
        };
        let mut client = ClusterObjectRpcClient::routed(&mut host, runtime.handle(), &paths);

        let mut response = runtime
            .block_on(client.call_target_stream(
                &state,
                &target_node,
                &request,
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(
                    Duration::from_secs(4),
                )),
            ))
            .expect("fallback stream endpoint should be attempted");
        let body = runtime
            .block_on(response.body.collect_to_vec())
            .expect("stream body");

        assert_eq!(body, b"stream-body");
        assert_eq!(host.stream_target_sends.len(), 2);
        assert_eq!(
            host.stream_target_sends[0].0.endpoint_id,
            stalling_endpoint.endpoint_id
        );
        assert_eq!(
            host.stream_target_sends[1].0.endpoint_id,
            fallback_endpoint.endpoint_id
        );
        assert_eq!(host.observations.len(), 1);
        assert_eq!(
            host.observations[0].0.endpoint_id,
            stalling_endpoint.endpoint_id
        );
        assert!(host.observations[0].1.is_io_failure());
    }

    #[test]
    fn signed_endpoint_response_observation_classifies_auth_error_frame() {
        let response =
            ObjectRpcResponse::encode_error("/test/target", "network authorization was denied")
                .expect("auth error response");
        let observation = cluster_signed_endpoint_rpc_observation(Ok(&response), Instant::now())
            .expect("observation");

        assert!(observation.is_auth_denied());
    }

    #[test]
    fn signed_endpoint_stream_response_observation_classifies_auth_error_frame() {
        let response = crate::ObjectRpcStreamResponse::from_buffered_response(
            ObjectRpcResponse::encode_error(
                "/test/stream-target",
                "network authorization was denied",
            )
            .expect("auth error response"),
            crate::RawByteStreamChunkOptions {
                chunk_bytes: 4,
                max_total_bytes: Some(1024),
                idle_timeout: Some(Duration::from_secs(1)),
            },
        );
        let observation =
            cluster_signed_endpoint_stream_response_rpc_observation(Ok(&response), Instant::now())
                .expect("observation");

        assert!(observation.is_auth_denied());
    }

    #[test]
    fn cluster_relay_hop_transport_timeouts_cap_response_budget() {
        let base = DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(15));
        let shaped = cluster_relay_hop_transport_timeouts(base, Some(Duration::from_secs(12)));

        assert_eq!(shaped.connect_timeout, Some(Duration::from_secs(12)));
        assert_eq!(shaped.idle_timeout, Some(CLUSTER_RELAY_MIN_RESPONSE_BUDGET));
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

    #[test]
    fn relay_uses_target_endpoint_hints_when_dynamic_route_is_empty() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-relay-target-hint-fallback");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_relay_target_hint").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_relay_target_hint_local").expect("local node");
        let target_node =
            NodeId::from_legacy_named("node_relay_target_hint_target").expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "relay target hint",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut target_member =
            ClusterMemberRecord::local_voter(cluster_id, target_node.clone(), "target", "pk", now);
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        state.members.push(target_member);
        crate::write_object_file(&paths.state, &state).expect("write state");

        let hinted_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "node1:21353".into(),
            10,
        );
        let inner =
            ObjectRpcRequest::encode_body("/test/target", "test", &"ping").expect("inner request");
        let mut envelope = RelayEnvelope::new(state.cluster_id.clone(), target_node.clone(), inner);
        envelope.target_endpoints.push(hinted_endpoint.clone());
        let request = envelope.into_request().expect("relay request");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = RecordingRelayHost::default();

        runtime
            .block_on(handle_cluster_relay_async_with_host(
                runtime.handle(),
                &paths,
                &state,
                &local_node,
                &request,
                ClusterRelayOptions::new(DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(
                    2,
                ))),
                &mut host,
            ))
            .expect("relay should use target hint");

        assert!(host.relay_sends.is_empty());
        assert_eq!(host.target_sends.len(), 1);
        assert_eq!(
            host.target_sends[0].0.endpoint_id,
            hinted_endpoint.endpoint_id
        );
        assert_eq!(host.target_sends[0].0.address, "node1:21353");
    }

    #[test]
    fn relay_final_hop_splits_hop_budget_across_target_endpoints() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-relay-target-endpoint-budget");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_relay_target_budget").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_relay_target_budget_local").expect("local node");
        let target_node =
            NodeId::from_legacy_named("node_relay_target_budget_target").expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "relay target budget",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut target_member =
            ClusterMemberRecord::local_voter(cluster_id, target_node.clone(), "target", "pk", now);
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        let stalled_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "stalled-target.test:21353".into(),
            0,
        );
        let healthy_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "healthy-target.test:21353".into(),
            10,
        );
        let backup_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &target_node,
            "backup-target.test:21353".into(),
            20,
        );
        target_member.endpoints = vec![
            stalled_endpoint.clone(),
            healthy_endpoint.clone(),
            backup_endpoint.clone(),
        ];
        state.members.push(target_member);
        crate::write_object_file(&paths.state, &state).expect("write state");

        let inner =
            ObjectRpcRequest::encode_body("/test/target", "test", &"ping").expect("inner request");
        let mut envelope = RelayEnvelope::new(state.cluster_id.clone(), target_node.clone(), inner);
        envelope.planned_path = vec![local_node.clone(), target_node.clone()];
        envelope.target_endpoints = vec![
            stalled_endpoint.clone(),
            healthy_endpoint.clone(),
            backup_endpoint,
        ];
        envelope.max_hop_timeout_ms = Some(2_000);
        let request = envelope.into_request().expect("relay request");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = RecordingRelayHost {
            stall_first_target_send: Some(Duration::from_millis(1_200)),
            ..RecordingRelayHost::default()
        };

        let response = runtime
            .block_on(handle_cluster_relay_async_with_host(
                runtime.handle(),
                &paths,
                &state,
                &local_node,
                &request,
                ClusterRelayOptions::new(DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(
                    5,
                ))),
                &mut host,
            ))
            .expect("second endpoint should complete within relay budget");

        assert_eq!(
            response.decode_body::<String>().expect("response body"),
            "ok"
        );
        assert_eq!(host.target_sends.len(), 2);
        assert_eq!(
            host.target_sends[0].0.endpoint_id,
            stalled_endpoint.endpoint_id
        );
        assert_eq!(
            host.target_sends[1].0.endpoint_id,
            healthy_endpoint.endpoint_id
        );
        let first_timeout = host.target_sends[0]
            .2
            .connect_timeout
            .expect("first timeout");
        assert!(
            first_timeout > Duration::from_millis(600)
                && first_timeout <= Duration::from_millis(700),
            "first endpoint should get roughly one third of the 2s relay hop budget, got {first_timeout:?}"
        );
        assert_eq!(host.target_sends[0].2.idle_timeout, Some(first_timeout));
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

        let planned_relay_endpoints =
            cluster_relay_planned_next_hop_endpoints(&paths, &state, &local_node, &target_node)
                .expect("planned relay endpoints");
        assert_eq!(planned_relay_endpoints.len(), 1);
        assert_eq!(planned_relay_endpoints[0].address, "node3:21353");
        assert_eq!(
            planned_relay_endpoints[0].endpoint_id,
            observed_endpoint.endpoint_id
        );
    }

    #[test]
    fn route_plan_prefers_healthy_direct_endpoint_before_synthetic_tcp_tunnel() {
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
                    ClusterConnectivityState::Healthy,
                    Some(480),
                    0,
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
    fn cluster_target_rpc_keeps_real_endpoint_before_synthetic_tunnel_fallback() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-target-resort-cached-endpoints");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_target_resort_cached").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_target_resort_cached_local").expect("local node");
        let target_node =
            NodeId::from_legacy_named("node_target_resort_cached_target").expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "target resort cached endpoints",
            local_node.clone(),
            "local",
            "pk-local",
            now,
        );
        let direct_endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named(
                "endpoint_target_resort_cached_direct",
            )
            .expect("endpoint"),
            node_id: target_node.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "10.0.0.41:21353".to_string(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 0,
            last_observed_latency_ms: None,
        };
        let tunnel_endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named(
                "endpoint_target_resort_cached_tunnel",
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
            cluster_id.clone(),
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
                    ClusterConnectivityState::Degraded,
                    None,
                    2,
                    None,
                    Some(now.to_string()),
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

        let mark = cluster_delivery_routing_table_cache_mark(&paths).expect("cache mark");
        let mut routes = ClusterDeliveryRouteIndex::default();
        routes
            .try_insert(
                &target_node,
                vec![ClusterDeliveryRouteCandidate {
                    plan: ClusterDeliveryRoutePlan {
                        next_hop: target_node.clone(),
                        endpoints: vec![direct_endpoint.clone(), tunnel_endpoint.clone()],
                        path: vec![local_node.clone(), target_node.clone()],
                    },
                    path: vec![local_node.clone(), target_node.clone()],
                    first_hop_cost: 1,
                    cost: 1,
                    complete: true,
                }],
            )
            .expect("insert cached route");
        CLUSTER_DELIVERY_ROUTING_TABLES.lock().unwrap().insert(
            paths.root.clone(),
            ClusterDeliveryRoutingTableCacheEntry {
                mark,
                table: ClusterDeliveryRoutingTable {
                    cluster_id,
                    local_node_id: local_node.clone(),
                    routes,
                    earliest_expires_at: Some(now + time::Duration::minutes(10)),
                },
                last_used: cluster_delivery_routing_table_cache_tick(),
            },
        );

        let request =
            ObjectRpcRequest::encode_body("/test/target", "test", &"ping").expect("request");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = RecordingSignedHost::default();
        let mut client = ClusterObjectRpcClient::routed(&mut host, runtime.handle(), &paths);

        runtime
            .block_on(client.call_target(
                &state,
                &target_node,
                &request,
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(
                    Duration::from_secs(15),
                )),
            ))
            .expect("target rpc");

        assert_eq!(host.target_sends.len(), 1);
        assert_eq!(
            host.target_sends[0].0.endpoint_id,
            direct_endpoint.endpoint_id
        );
        assert_eq!(host.target_sends[0].0.address, direct_endpoint.address);
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

    #[test]
    fn route_plan_retries_non_identity_unreachable_target_endpoint_as_fallback() {
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

        let stale_failure = crate::connectivity::cluster_connectivity_record(
            &state,
            &stale_endpoint,
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
            &[stale_failure],
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
        assert_eq!(plan.endpoints[0].endpoint_id, stale_endpoint.endpoint_id);
        assert_eq!(plan.endpoints[0].address, "target:21353");
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
    fn relay_planned_target_endpoints_prefer_local_connectivity() {
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
        )
        .expect("planned target endpoints");

        assert_eq!(planned.len(), 2);
        assert_eq!(planned[0].endpoint_id, healthy_endpoint.endpoint_id);
        assert_eq!(planned[0].address, "target:21353");
        assert_eq!(planned[1].endpoint_id, unreachable_endpoint.endpoint_id);
        assert_eq!(planned[1].address, "unreachable-target.test:21353");
    }
}
