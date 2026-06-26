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
use std::collections::{HashMap, VecDeque};
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
const CLUSTER_DELIVERY_DEGRADED_LINK_COST: u64 = 50_000;
const CLUSTER_DELIVERY_UNOBSERVED_TARGET_COST: u64 = CLUSTER_DELIVERY_DEGRADED_LINK_COST / 2;
const CLUSTER_DELIVERY_UNOBSERVED_NEIGHBOR_COST: u64 = 75_000;
const MAX_CLUSTER_DELIVERY_ROUTING_TABLE_CACHE_ENTRIES: usize = 128;

static CLUSTER_DELIVERY_ROUTING_TABLE_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static CLUSTER_DELIVERY_ROUTING_TABLES: LazyLock<
    Mutex<HashMap<PathBuf, ClusterDeliveryRoutingTableCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// A discovered route to a target node: the immediate next hop to send to, and the full
/// node path (`local` … `target`, inclusive) for diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClusterRoute {
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
pub(crate) fn cluster_route_next_hop(
    graph: &ClusterConnectivityGraph,
    local: &NodeId,
    target: &NodeId,
) -> Option<ClusterRoute> {
    cluster_route_next_hop_fallible(graph, local, target)
        .ok()
        .flatten()
}

pub(crate) fn cluster_route_next_hop_fallible(
    graph: &ClusterConnectivityGraph,
    local: &NodeId,
    target: &NodeId,
) -> Result<Option<ClusterRoute>, NetworkError> {
    cluster_route_next_hop_excluding(graph, local, target, &[])
}

fn cluster_route_next_hop_excluding(
    graph: &ClusterConnectivityGraph,
    local: &NodeId,
    target: &NodeId,
    excluded: &[NodeId],
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
        for next in graph.healthy_target_refs(&current) {
            if !seen.try_insert_clone(next, "cluster route seen nodes")? {
                continue;
            }
            predecessor.try_insert_clone(next, &current, "cluster route predecessors")?;
            if next == target {
                return route_from_predecessors(local, target, &predecessor);
            }
            queue
                .try_reserve(1)
                .map_err(|_| crate::object::network_allocation_error("cluster route queue", 1))?;
            queue.push_back(try_clone_route_node_id(next, "cluster route queue")?);
        }
    }
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
pub(crate) struct RelayEnvelope {
    pub(crate) cluster_id: ClusterId,
    pub(crate) target_node_id: NodeId,
    pub(crate) ttl: u8,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "crate::fallible_serde::deserialize_vec"
    )]
    pub(crate) visited: Vec<NodeId>,
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
    pub(crate) fn new(
        cluster_id: ClusterId,
        target_node_id: NodeId,
        inner: DaemonControlRequest,
    ) -> Self {
        Self {
            cluster_id,
            target_node_id,
            ttl: CLUSTER_RELAY_DEFAULT_TTL,
            visited: Vec::new(),
            max_hop_timeout_ms: None,
            inner,
        }
    }

    /// Encode this envelope as an [`ObjectRpcRequest`] on [`CLUSTER_RELAY_ROUTE`] for
    /// transport to the next hop.
    pub(crate) fn into_request(&self) -> Result<ObjectRpcRequest, NetworkError> {
        let mut request =
            ObjectRpcRequest::encode_body(CLUSTER_RELAY_ROUTE, CLUSTER_ROUTING_RPC_CATEGORY, self)?;
        propagate_relay_response_metadata(&self.inner, &mut request);
        Ok(request)
    }

    /// Decode an envelope from a relay-route [`ObjectRpcRequest`].
    pub(crate) fn from_request(request: &ObjectRpcRequest) -> Result<Self, NetworkError> {
        request.decode_body()
    }

    pub(crate) fn inner(&self) -> &DaemonControlRequest {
        &self.inner
    }

    pub(crate) fn into_inner(self) -> DaemonControlRequest {
        self.inner
    }
}

#[derive(Clone, Debug)]
struct ClusterDeliveryRouteEdge {
    target: NodeId,
    endpoint: Option<ClusterEndpointRecord>,
    cost: u64,
}

#[derive(Clone, Debug, Default)]
struct ClusterDeliveryRouteGraph {
    edges_by_source: Vec<(NodeId, Vec<ClusterDeliveryRouteEdge>)>,
}

impl ClusterDeliveryRouteGraph {
    fn push_edge_clone(
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
struct ClusterDeliveryFailedEdges {
    edges: Vec<ClusterDeliveryFailedEdge>,
}

impl ClusterDeliveryFailedEdges {
    fn contains(&self, source: &NodeId, target: &NodeId) -> bool {
        self.edges.iter().any(|edge| edge.matches(source, target))
    }

    fn try_insert(&mut self, source: &NodeId, target: &NodeId) -> Result<bool, NetworkError> {
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
struct ClusterDeliveryRoutePlan {
    next_hop: NodeId,
    endpoints: Vec<ClusterEndpointRecord>,
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
    cost: u64,
    complete: bool,
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
struct ClusterDeliveryRoutingTable {
    cluster_id: ClusterId,
    local_node_id: NodeId,
    routes: ClusterDeliveryRouteIndex,
    earliest_expires_at: Option<OffsetDateTime>,
}

impl ClusterDeliveryRoutingTable {
    fn route_plan(
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ClusterDeliveryRoutingTableCacheMark {
    state: ClusterFileMark,
    kv_snapshot: ClusterFileMark,
    local_connectivity_generation: u64,
}

#[cfg(test)]
fn cluster_delivery_route_plan(
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
            crate::object::try_reserve_vec(
                &mut candidates,
                1,
                "cluster delivery route candidates",
            )?;
            candidates.push(ClusterDeliveryRouteCandidate {
                plan: ClusterDeliveryRoutePlan {
                    next_hop: try_clone_route_node_id(next_hop, "cluster delivery next hop")?,
                    endpoints,
                },
                path: cluster_delivery_two_node_path(local_node_id, next_hop)?,
                cost: edge_cost,
                complete: true,
            });
            continue;
        }
        if let Some(path) = cluster_delivery_shortest_path(graph, &next_hop, target_node_id, &[])? {
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
                },
                path: nodes,
                cost: edge_cost.saturating_add(path.cost),
                complete: true,
            });
        } else {
            crate::object::try_reserve_vec(
                &mut candidates,
                1,
                "cluster delivery route candidates",
            )?;
            candidates.push(ClusterDeliveryRouteCandidate {
                plan: ClusterDeliveryRoutePlan {
                    next_hop: try_clone_route_node_id(next_hop, "cluster delivery next hop")?,
                    endpoints,
                },
                path: cluster_delivery_two_node_path(local_node_id, next_hop)?,
                cost: edge_cost,
                complete: false,
            });
        }
    }
    candidates.sort_by_key(|candidate| {
        (
            if candidate.complete { 0 } else { 1 },
            candidate.cost,
            candidate.plan.next_hop.to_string(),
        )
    });
    Ok(candidates)
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
            candidate.complete
                && cluster_delivery_route_candidate_is_compatible(candidate, failed_edges, visited)
        })
        .chain(candidates.iter().filter(|candidate| {
            !candidate.complete
                && cluster_delivery_route_candidate_is_compatible(candidate, failed_edges, visited)
        }))
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

async fn cluster_delivery_route_plan_async(
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

fn cluster_delivery_build_routing_table(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
) -> Result<ClusterDeliveryRoutingTable, NetworkError> {
    let mut routes = ClusterDeliveryRouteIndex::default();
    let earliest_expires_at = cluster_delivery_routing_table_earliest_expires_at(paths, state);
    for member in &state.members {
        if member.node_id == *local_node_id || !member.state.is_authorized() {
            continue;
        }
        let graph = cluster_delivery_route_graph(
            paths,
            state,
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
    crate::connectivity_store::read_local_cluster_connectivity_records(paths)
        .unwrap_or_default()
        .into_iter()
        .chain(
            crate::route_graph_read_model::read_cluster_connectivity_graph(paths)
                .unwrap_or_default(),
        )
        .filter(|record| {
            record.cluster_id == state.cluster_id
                && crate::connectivity_publish::cluster_network_record_live(record.expires_at, now)
        })
        .map(|record| record.expires_at)
        .min()
}

fn cluster_delivery_routing_table_cache_mark(
    paths: &ClusterPaths,
) -> Option<ClusterDeliveryRoutingTableCacheMark> {
    Some(ClusterDeliveryRoutingTableCacheMark {
        state: cluster_file_mark(&paths.state)?,
        kv_snapshot: cluster_file_mark(&paths.kv_snapshot)?,
        local_connectivity_generation:
            crate::connectivity_store::local_cluster_connectivity_content_generation(paths),
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

fn cluster_delivery_route_graph(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    failed_edges: &ClusterDeliveryFailedEdges,
    target_node_id: &NodeId,
) -> Result<ClusterDeliveryRouteGraph, NetworkError> {
    let local_connectivity = ClusterConnectivityGraph::try_read_for_local(paths, state)?;
    let local_private_prefixes = cluster_delivery_local_private_prefixes(state, local_node_id)?;
    let mut graph = ClusterDeliveryRouteGraph::default();
    for member in &state.members {
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
        }
    }

    for record in
        crate::route_graph_read_model::read_cluster_connectivity_graph(paths).unwrap_or_default()
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

fn cluster_delivery_local_endpoint_is_usable(
    local_node_id: &NodeId,
    endpoint: &ClusterEndpointRecord,
) -> bool {
    if endpoint.privacy == ClusterEndpointPrivacy::LocalOnly && endpoint.node_id != *local_node_id {
        return false;
    }
    cluster_endpoint_can_carry_main_daemon_rpc(endpoint)
}

#[cfg(test)]
fn cluster_delivery_shortest_path_next_hop(
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
        Some(record) => cluster_delivery_connectivity_state_cost(
            path_cost,
            record.latency_ms,
            record.consecutive_failures,
            &record.state,
        )
        .map(|cost| cost + endpoint_priority),
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
    let path_cost = cluster_delivery_record_path_cost(&record.transport, &record.address);
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
        ClusterConnectivityState::Unreachable => None,
    }
}

fn cluster_delivery_endpoint_path_cost(
    transport: &ClusterTransportKind,
    address: &str,
    privacy: &ClusterEndpointPrivacy,
) -> u64 {
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
        ClusterTransportKind::Iroh => 60,
        ClusterTransportKind::WebRtc => 70,
        ClusterTransportKind::TurnRelay | ClusterTransportKind::HttpsRelay => 100,
    }
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
        ClusterTransportKind::Iroh => 4,
        ClusterTransportKind::WebRtc => 5,
        ClusterTransportKind::TurnRelay => 6,
        ClusterTransportKind::HttpsRelay => 7,
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
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
) -> Result<ClusterDeliveryPrivatePrefixes, NetworkError> {
    let mut prefixes = ClusterDeliveryPrivatePrefixes::default();
    for endpoint in state
        .members
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterObjectRpcOptions {
    timeouts: DaemonRpcTimeouts,
}

impl ClusterObjectRpcOptions {
    pub fn new(timeouts: DaemonRpcTimeouts) -> Self {
        Self { timeouts }
    }
}

pub type ClusterSignedEndpointRpcFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NetworkError>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClusterSignedEndpointRpcObservationKind {
    Success,
    IoFailure,
    AuthDenied,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterSignedEndpointRpcObservation {
    kind: ClusterSignedEndpointRpcObservationKind,
    latency_ms: Option<u64>,
}

impl ClusterSignedEndpointRpcObservation {
    pub fn success(latency_ms: Option<u64>) -> Self {
        Self {
            kind: ClusterSignedEndpointRpcObservationKind::Success,
            latency_ms,
        }
    }

    pub fn io_failure() -> Self {
        Self {
            kind: ClusterSignedEndpointRpcObservationKind::IoFailure,
            latency_ms: None,
        }
    }

    pub fn auth_denied() -> Self {
        Self {
            kind: ClusterSignedEndpointRpcObservationKind::AuthDenied,
            latency_ms: None,
        }
    }

    pub fn is_success(&self) -> bool {
        matches!(self.kind, ClusterSignedEndpointRpcObservationKind::Success)
    }

    pub fn is_io_failure(&self) -> bool {
        matches!(
            self.kind,
            ClusterSignedEndpointRpcObservationKind::IoFailure
        )
    }

    pub fn is_auth_denied(&self) -> bool {
        matches!(
            self.kind,
            ClusterSignedEndpointRpcObservationKind::AuthDenied
        )
    }

    pub fn latency_ms(&self) -> Option<u64> {
        self.latency_ms
    }
}

pub trait ClusterSignedEndpointRpcAsyncHost: Send {
    fn sign_endpoint_rpc_request_async<'a>(
        &'a mut self,
        request: &'a ObjectRpcRequest,
    ) -> ClusterSignedEndpointRpcFuture<'a, Vec<u8>>;

    fn observe_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        _endpoint: &'a ClusterEndpointRecord,
        _observation: ClusterSignedEndpointRpcObservation,
    ) -> ClusterSignedEndpointRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn send_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        auth: &'a [u8],
        request: &'a ObjectRpcRequest,
        timeouts: DaemonRpcTimeouts,
    ) -> ClusterSignedEndpointRpcFuture<'a, ObjectRpcResponse> {
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
}

fn cluster_signed_endpoint_rpc_observation(
    result: Result<&ObjectRpcResponse, &NetworkError>,
    started: Instant,
) -> Option<ClusterSignedEndpointRpcObservation> {
    match result {
        Ok(_) => Some(ClusterSignedEndpointRpcObservation::success(Some(
            cluster_signed_endpoint_rpc_elapsed_latency_ms(started),
        ))),
        Err(NetworkError::Io { .. }) => Some(ClusterSignedEndpointRpcObservation::io_failure()),
        Err(NetworkError::AuthDenied) => Some(ClusterSignedEndpointRpcObservation::auth_denied()),
        Err(_) => None,
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

impl<'a, H: ClusterSignedEndpointRpcAsyncHost> ClusterObjectRpcClient<'a, H> {
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
    H: ClusterSignedEndpointRpcAsyncHost + Send,
{
    pub async fn call_endpoint(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        options: ClusterObjectRpcOptions,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        self.call_endpoint_direct(endpoint, request, options).await
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
        let request = request.clone().with_trace_context_if_missing();
        let request_auth = self.host.sign_endpoint_rpc_request_async(&request).await?;
        self.host
            .send_signed_endpoint_rpc_async(
                endpoint,
                &request_auth,
                &request,
                cluster_endpoint_transport_timeouts(options.timeouts, remaining),
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
        let request = request.clone().with_trace_context_if_missing();
        let traceparent = request.traceparent().unwrap_or_default().to_string();
        let call_started_at = Instant::now();
        let sign_started_at = Instant::now();
        let request_auth = self.host.sign_endpoint_rpc_request_async(&request).await?;
        let request_sign_us = cluster_object_rpc_elapsed_us(sign_started_at);
        metrics::histogram!(
            "sinomo.object_rpc.sign.us",
            "route" => request.route.clone(),
            "category" => request.category.clone()
        )
        .record(request_sign_us as f64);
        let deadline = ClusterEndpointRpcDeadline::new(options.timeouts.connect_timeout);
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
            sign_us = request_sign_us,
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
                failures.push(format!("{}: no usable endpoint", route_plan.next_hop));
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
            for endpoint in route_plan.endpoints {
                let attempt_started_at = Instant::now();
                let Some(remaining) = deadline.checked_remaining() else {
                    return Err(cluster_target_rpc_timeout_error(
                        target_node_id,
                        options.timeouts.connect_timeout,
                    ));
                };
                let final_hop = &route_plan.next_hop == target_node_id;
                let hop_timeouts =
                    cluster_relay_hop_transport_timeouts(options.timeouts, remaining);
                let (outer_build_us, outer_sign_us, send_us, response) = if final_hop {
                    let send_started_at = Instant::now();
                    let response = self
                        .host
                        .send_signed_endpoint_rpc_async(
                            &endpoint,
                            &request_auth,
                            &request,
                            hop_timeouts,
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
                        &request,
                        request_auth.clone(),
                        std::iter::once(state.local_node_id.clone()),
                        options.timeouts.connect_timeout,
                    )?;
                    let outer_build_us = cluster_object_rpc_elapsed_us(outer_build_started_at);
                    let outer_sign_started_at = Instant::now();
                    let outer_auth = self.host.sign_endpoint_rpc_request_async(&outer).await?;
                    let outer_sign_us = cluster_object_rpc_elapsed_us(outer_sign_started_at);
                    let send_started_at = Instant::now();
                    let response = self
                        .host
                        .send_signed_endpoint_rpc_async(
                            &endpoint,
                            &outer_auth,
                            &outer,
                            hop_timeouts,
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
                            "{} ({:?}) returned routing error",
                            endpoint.address, endpoint.transport
                        ));
                        break;
                    }
                    Ok(response) => {
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

pub(crate) fn relay_envelope_hop_transport_timeouts(
    timeouts: DaemonRpcTimeouts,
    envelope: &RelayEnvelope,
) -> DaemonRpcTimeouts {
    let remaining = envelope
        .max_hop_timeout_ms
        .map(|timeout_ms| Duration::from_millis(timeout_ms.max(1)));
    cluster_relay_hop_transport_timeouts(timeouts, remaining)
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
) -> NetworkError {
    NetworkError::Io {
        operation: "cluster target rpc".to_string(),
        detail: format!(
            "timed out routing to {target_node_id} after {}ms",
            timeout.unwrap_or_default().as_millis()
        ),
    }
}

fn relay_request_for_signed_rpc_with_max_hop_timeout(
    cluster_id: &ClusterId,
    target_node_id: &NodeId,
    request: &ObjectRpcRequest,
    request_auth: impl Into<Vec<u8>>,
    visited: impl IntoIterator<Item = NodeId>,
    max_hop_timeout: Option<Duration>,
) -> Result<ObjectRpcRequest, NetworkError> {
    let inner = request.clone().with_auth(request_auth.into());
    let mut envelope = RelayEnvelope::new(cluster_id.clone(), target_node_id.clone(), inner);
    envelope.visited.extend(visited);
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
        for endpoint in route_plan.endpoints {
            let attempt_started_at = Instant::now();
            let final_hop = &route_plan.next_hop == &envelope.target_node_id;
            let hop_timeouts = relay_envelope_hop_transport_timeouts(options.timeouts, &envelope);
            let response = if final_hop {
                host.send_target_request_async(&endpoint, envelope.inner(), hop_timeouts)
                    .await
            } else {
                let mut forwarded = envelope.clone();
                forwarded.ttl = envelope.ttl.saturating_sub(1);
                forwarded.visited.push(local_node_id.clone());
                let outer = forwarded.into_request()?;
                host.send_relay_request_async(&endpoint, &outer, hop_timeouts)
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
                            response
                                .metadata
                                .get(crate::OBJECT_RPC_ERROR_DETAIL_METADATA_KEY)
                                .cloned()
                                .unwrap_or_else(|| "routing error".to_string())
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
    cluster_delivery_route_plan_async(
        runtime,
        paths,
        state,
        local_node_id,
        &envelope.target_node_id,
        failed_edges,
        try_clone_route_node_ids(&envelope.visited, "cluster relay visited nodes")?,
    )
    .await
}

pub fn relay_response_is_routing_error(response: &ObjectRpcResponse) -> bool {
    response.route == CLUSTER_RELAY_ROUTE && response.error_detail().is_err()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{ClusterMemberRecord, ClusterMemberRole, ClusterMemberState};
    use sinomo_local::SinomoDataDir;
    use sinomo_node::main_daemon_capabilities;
    use std::collections::{BTreeMap, VecDeque};
    use time::OffsetDateTime;

    fn nid(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).expect("node id")
    }

    fn cid(name: &str) -> ClusterId {
        ClusterId::from_legacy_named(name).expect("cluster id")
    }

    fn test_paths(name: &str) -> (sinomo_testdir::TestDir, ClusterPaths) {
        let root = sinomo_testdir::TestDir::new(name);
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        (root, paths)
    }

    fn test_state(cluster_id: ClusterId, local: NodeId) -> ClusterBootstrapState {
        ClusterBootstrapState::initialized_locally(
            cluster_id,
            "route test",
            local,
            "local",
            "pk-local",
            OffsetDateTime::now_utc(),
        )
    }

    fn add_member(
        state: &mut ClusterBootstrapState,
        node_id: NodeId,
        display_name: &str,
        endpoints: Vec<ClusterEndpointRecord>,
    ) {
        let now = OffsetDateTime::now_utc();
        let mut member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            node_id,
            display_name,
            format!("pk-{display_name}"),
            now,
        );
        member.role = ClusterMemberRole::Voter;
        member.state = ClusterMemberState::Voter;
        member.capabilities = main_daemon_capabilities(&member.role);
        member.allowed_capabilities = member.capabilities.clone();
        member.endpoints = endpoints;
        state.members.push(member);
    }

    fn endpoint(node_id: &NodeId, address: &str, priority: u16) -> ClusterEndpointRecord {
        crate::daemon_lan_endpoint(node_id, address.to_string(), priority)
    }

    fn add_local_endpoint(state: &mut ClusterBootstrapState, endpoint: ClusterEndpointRecord) {
        let local_node_id = state.local_node_id.clone();
        let member = state
            .members
            .iter_mut()
            .find(|member| member.node_id == local_node_id)
            .expect("local member");
        member.endpoints.push(endpoint);
    }

    fn request() -> ObjectRpcRequest {
        ObjectRpcRequest {
            route: "/test/target".to_string(),
            category: "test".to_string(),
            metadata: BTreeMap::new(),
            auth: None,
            body: vec![1, 2, 3],
        }
    }

    fn response(value: &str) -> ObjectRpcResponse {
        ObjectRpcResponse::encode_body("/test/ok", &value.to_string()).expect("response")
    }

    fn mark_local_endpoints_healthy(
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoints: &[ClusterEndpointRecord],
    ) {
        let now = OffsetDateTime::now_utc();
        let records = endpoints
            .iter()
            .map(|endpoint| {
                crate::cluster_connectivity_record(
                    state,
                    endpoint,
                    ClusterConnectivityState::Healthy,
                    Some(1),
                    0,
                    Some(now.to_string()),
                    None,
                    now,
                    crate::CLUSTER_CONNECTIVITY_TTL_SECONDS,
                )
            })
            .collect::<Vec<_>>();
        crate::write_local_cluster_connectivity_records(paths, &state.local_node_id, &records)
            .expect("write local connectivity records");
    }

    fn connectivity_record_from_source(
        cluster_id: &ClusterId,
        source_node_id: &NodeId,
        endpoint: &ClusterEndpointRecord,
        state: ClusterConnectivityState,
        latency_ms: Option<u64>,
    ) -> ClusterConnectivityRecord {
        let now = OffsetDateTime::now_utc();
        ClusterConnectivityRecord {
            cluster_id: cluster_id.clone(),
            source_node_id: source_node_id.clone(),
            target_node_id: endpoint.node_id.clone(),
            endpoint_id: endpoint.endpoint_id.clone(),
            transport: endpoint.transport.clone(),
            address: endpoint.address.clone(),
            state,
            latency_ms,
            consecutive_failures: 0,
            last_success_at: Some(now.to_string()),
            last_failure_at: None,
            failure_reason: None,
            observed_at: now,
            expires_at: now + time::Duration::seconds(crate::CLUSTER_CONNECTIVITY_TTL_SECONDS),
        }
    }

    #[test]
    fn delivery_graph_prefers_lowest_total_cost_path() {
        let local = nid("node_delivery_cost_local");
        let bridge = nid("node_delivery_cost_bridge");
        let target = nid("node_delivery_cost_target");
        let mut graph = ClusterDeliveryRouteGraph::default();
        graph
            .push_edge_clone(&local, &target, None, 50)
            .expect("direct edge");
        graph
            .push_edge_clone(&local, &bridge, None, 10)
            .expect("bridge edge");
        graph
            .push_edge_clone(&bridge, &target, None, 10)
            .expect("target edge");

        assert_eq!(
            cluster_delivery_shortest_path_next_hop(&graph, &local, &target, &[]),
            Some(bridge)
        );
    }

    #[test]
    fn delivery_plan_returns_ordered_direct_target_endpoints() {
        let (_root, paths) = test_paths("sinomo-network-delivery-plan-direct");
        let cluster_id = cid("cluster_delivery_plan_direct");
        let local = nid("node_delivery_plan_direct_local");
        let target = nid("node_delivery_plan_direct_target");
        let mut state = test_state(cluster_id, local.clone());
        let slow = endpoint(&target, "10.90.1.10:21353", 20);
        let fast = endpoint(&target, "10.90.1.11:21353", 1);
        add_member(
            &mut state,
            target.clone(),
            "target",
            vec![slow.clone(), fast.clone()],
        );
        mark_local_endpoints_healthy(&paths, &state, &[slow, fast.clone()]);

        let plan = cluster_delivery_route_plan(
            &paths,
            &state,
            &local,
            &target,
            &ClusterDeliveryFailedEdges::default(),
            &[],
        )
        .expect("route plan");

        assert_eq!(plan.next_hop, target);
        assert_eq!(plan.endpoints[0].address, fast.address);
    }

    #[test]
    fn delivery_plan_prefers_healthy_multihop_over_unobserved_direct_target() {
        let (_root, paths) = test_paths("sinomo-network-delivery-plan-direct-unobserved");
        let cluster_id = cid("cluster_delivery_plan_direct_unobserved");
        let local = nid("node_delivery_plan_direct_unobserved_local");
        let target = nid("node_delivery_plan_direct_unobserved_target");
        let bridge = nid("node_delivery_plan_direct_unobserved_bridge");
        let mut state = test_state(cluster_id.clone(), local.clone());
        add_local_endpoint(&mut state, endpoint(&local, "172.25.0.9:21353", 0));
        let target_endpoint = endpoint(&target, "172.25.0.10:21353", 0);
        let bridge_endpoint = endpoint(&bridge, "172.25.0.2:21353", 0);
        add_member(
            &mut state,
            target.clone(),
            "target",
            vec![target_endpoint.clone()],
        );
        add_member(
            &mut state,
            bridge.clone(),
            "bridge",
            vec![bridge_endpoint.clone()],
        );
        mark_local_endpoints_healthy(&paths, &state, std::slice::from_ref(&bridge_endpoint));
        let bridge_to_target_record = connectivity_record_from_source(
            &cluster_id,
            &bridge,
            &target_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
        );
        crate::write_local_cluster_connectivity_records(
            &paths,
            &bridge,
            std::slice::from_ref(&bridge_to_target_record),
        )
        .expect("write bridge record");

        let plan = cluster_delivery_route_plan(
            &paths,
            &state,
            &local,
            &target,
            &ClusterDeliveryFailedEdges::default(),
            &[],
        )
        .expect("route plan");

        assert_eq!(plan.next_hop, bridge);
        assert_eq!(plan.endpoints.len(), 1);
        assert_eq!(plan.endpoints[0].address, bridge_endpoint.address);
    }

    #[test]
    fn delivery_plan_can_choose_lower_cost_multihop_over_direct_target() {
        let (_root, paths) = test_paths("sinomo-network-delivery-plan-direct-vs-multihop");
        let cluster_id = cid("cluster_delivery_plan_direct_vs_multihop");
        let local = nid("node_delivery_plan_direct_vs_multihop_local");
        let target = nid("node_delivery_plan_direct_vs_multihop_target");
        let bridge = nid("node_delivery_plan_direct_vs_multihop_bridge");
        let mut state = test_state(cluster_id.clone(), local.clone());
        add_local_endpoint(&mut state, endpoint(&local, "172.25.0.9:21353", 0));
        let target_endpoint = endpoint(&target, "172.25.0.10:21353", 100);
        let bridge_endpoint = endpoint(&bridge, "172.25.0.2:21353", 0);
        add_member(
            &mut state,
            target.clone(),
            "target",
            vec![target_endpoint.clone()],
        );
        add_member(
            &mut state,
            bridge.clone(),
            "bridge",
            vec![bridge_endpoint.clone()],
        );
        mark_local_endpoints_healthy(&paths, &state, std::slice::from_ref(&bridge_endpoint));
        let bridge_to_target_record = connectivity_record_from_source(
            &cluster_id,
            &bridge,
            &target_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
        );
        crate::write_local_cluster_connectivity_records(
            &paths,
            &bridge,
            std::slice::from_ref(&bridge_to_target_record),
        )
        .expect("write bridge record");

        let plan = cluster_delivery_route_plan(
            &paths,
            &state,
            &local,
            &target,
            &ClusterDeliveryFailedEdges::default(),
            &[],
        )
        .expect("route plan");

        assert_eq!(plan.next_hop, bridge);
        assert_eq!(plan.endpoints.len(), 1);
        assert_eq!(plan.endpoints[0].address, bridge_endpoint.address);
    }

    #[test]
    fn delivery_plan_uses_exploratory_neighbor_after_failed_direct_edge() {
        let (_root, paths) = test_paths("sinomo-network-delivery-plan-failed-direct");
        let cluster_id = cid("cluster_delivery_plan_failed_direct");
        let local = nid("node_delivery_plan_failed_local");
        let target = nid("node_delivery_plan_failed_target");
        let bridge = nid("node_delivery_plan_failed_bridge");
        let mut state = test_state(cluster_id, local.clone());
        add_member(
            &mut state,
            target.clone(),
            "target",
            vec![endpoint(&target, "10.90.2.10:21353", 0)],
        );
        add_member(
            &mut state,
            bridge.clone(),
            "bridge",
            vec![endpoint(&bridge, "10.90.2.20:21353", 10)],
        );
        let bridge_endpoint = state
            .members
            .iter()
            .find(|member| member.node_id == bridge)
            .and_then(|member| member.endpoints.first())
            .cloned()
            .expect("bridge endpoint");
        mark_local_endpoints_healthy(&paths, &state, &[bridge_endpoint]);
        let mut failed_edges = ClusterDeliveryFailedEdges::default();
        failed_edges
            .try_insert(&local, &target)
            .expect("failed direct edge");

        let plan = cluster_delivery_route_plan(&paths, &state, &local, &target, &failed_edges, &[])
            .expect("route plan");

        assert_eq!(plan.next_hop, bridge);
        assert_eq!(plan.endpoints.len(), 1);
    }

    #[test]
    fn delivery_plan_treats_far_direct_target_as_candidate() {
        let (_root, paths) = test_paths("sinomo-network-delivery-plan-observed-bridge");
        let cluster_id = cid("cluster_delivery_plan_observed_bridge");
        let local = nid("node_delivery_plan_observed_local");
        let bridge = nid("node_delivery_plan_observed_bridge");
        let target = nid("node_delivery_plan_observed_target");
        let mut state = test_state(cluster_id, local.clone());
        add_local_endpoint(&mut state, endpoint(&local, "172.25.0.9:21353", 0));
        let target_endpoint = endpoint(&target, "172.23.0.2:21353", 0);
        add_member(
            &mut state,
            target.clone(),
            "target",
            vec![target_endpoint.clone()],
        );
        let bridge_endpoint = endpoint(&bridge, "172.25.0.2:21353", 0);
        add_member(
            &mut state,
            bridge.clone(),
            "bridge",
            vec![bridge_endpoint.clone()],
        );
        mark_local_endpoints_healthy(&paths, &state, &[bridge_endpoint]);

        let plan = cluster_delivery_route_plan(
            &paths,
            &state,
            &local,
            &target,
            &ClusterDeliveryFailedEdges::default(),
            &[],
        )
        .expect("route plan");

        assert_eq!(plan.next_hop, target);
        assert_eq!(plan.endpoints.len(), 1);
        assert_eq!(plan.endpoints[0].address, target_endpoint.address);
    }

    #[test]
    fn delivery_plan_keeps_degraded_direct_target_as_candidate() {
        let (_root, paths) = test_paths("sinomo-network-delivery-plan-degraded-target");
        let cluster_id = cid("cluster_delivery_plan_degraded_target");
        let local = nid("node_delivery_plan_degraded_local");
        let bridge = nid("node_delivery_plan_degraded_bridge");
        let target = nid("node_delivery_plan_degraded_target");
        let mut state = test_state(cluster_id.clone(), local.clone());
        let target_endpoint = endpoint(&target, "node3:21353", 0);
        let bridge_endpoint = endpoint(&bridge, "node4:21353", 0);
        add_member(
            &mut state,
            target.clone(),
            "target",
            vec![target_endpoint.clone()],
        );
        add_member(
            &mut state,
            bridge.clone(),
            "bridge",
            vec![bridge_endpoint.clone()],
        );
        let degraded_target_record = connectivity_record_from_source(
            &cluster_id,
            &local,
            &target_endpoint,
            ClusterConnectivityState::Degraded,
            None,
        );
        crate::write_local_cluster_connectivity_records(
            &paths,
            &local,
            std::slice::from_ref(&degraded_target_record),
        )
        .expect("write degraded target record");
        mark_local_endpoints_healthy(&paths, &state, &[bridge_endpoint]);

        let plan = cluster_delivery_route_plan(
            &paths,
            &state,
            &local,
            &target,
            &ClusterDeliveryFailedEdges::default(),
            &[],
        )
        .expect("route plan");

        assert_eq!(plan.next_hop, target);
        assert_eq!(plan.endpoints.len(), 1);
        assert_eq!(plan.endpoints[0].address, target_endpoint.address);
    }

    #[test]
    fn delivery_plan_prefers_unobserved_direct_target_over_exploratory_bridge() {
        let (_root, paths) = test_paths("sinomo-network-delivery-plan-unobserved-bridge");
        let cluster_id = cid("cluster_delivery_plan_unobserved_bridge");
        let old_cluster_id = cid("cluster_delivery_plan_unobserved_bridge_old");
        let local = nid("node_delivery_plan_unobserved_local");
        let bridge = nid("node_delivery_plan_unobserved_bridge");
        let target = nid("node_delivery_plan_unobserved_target");
        let mut state = test_state(cluster_id.clone(), local.clone());
        add_local_endpoint(&mut state, endpoint(&local, "172.25.0.9:21353", 0));
        let target_endpoint = endpoint(&target, "172.23.0.2:21353", 0);
        let bridge_endpoint = endpoint(&bridge, "172.25.0.2:21353", 0);
        add_member(
            &mut state,
            target.clone(),
            "target",
            vec![target_endpoint.clone()],
        );
        add_member(
            &mut state,
            bridge.clone(),
            "bridge",
            vec![bridge_endpoint.clone()],
        );
        let stale_bridge_record = connectivity_record_from_source(
            &old_cluster_id,
            &local,
            &bridge_endpoint,
            ClusterConnectivityState::Degraded,
            None,
        );
        let bridge_to_target_record = connectivity_record_from_source(
            &cluster_id,
            &bridge,
            &target_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
        );
        crate::write_local_cluster_connectivity_records(
            &paths,
            &local,
            std::slice::from_ref(&stale_bridge_record),
        )
        .expect("write stale local bridge record");
        crate::write_local_cluster_connectivity_records(
            &paths,
            &bridge,
            std::slice::from_ref(&bridge_to_target_record),
        )
        .expect("write remote bridge record");

        let plan = cluster_delivery_route_plan(
            &paths,
            &state,
            &local,
            &target,
            &ClusterDeliveryFailedEdges::default(),
            &[],
        )
        .expect("route plan");

        assert_eq!(plan.next_hop, target);
        assert_eq!(plan.endpoints.len(), 1);
        assert_eq!(plan.endpoints[0].address, target_endpoint.address);
    }

    #[test]
    fn delivery_plan_skips_unobserved_neighbor_on_different_private_prefix() {
        let (_root, paths) = test_paths("sinomo-network-delivery-plan-private-prefix");
        let cluster_id = cid("cluster_delivery_plan_private_prefix");
        let local = nid("node_delivery_plan_prefix_local");
        let target = nid("node_delivery_plan_prefix_target");
        let wrong_neighbor = nid("node_delivery_plan_prefix_wrong");
        let bridge = nid("node_delivery_plan_prefix_bridge");
        let mut state = test_state(cluster_id.clone(), local.clone());
        add_local_endpoint(&mut state, endpoint(&local, "172.25.0.9:21353", 0));
        let target_endpoint = endpoint(&target, "172.23.0.2:21353", 0);
        let wrong_endpoint = endpoint(&wrong_neighbor, "172.23.0.4:21353", 0);
        let bridge_endpoint = endpoint(&bridge, "172.25.0.2:21353", 0);
        add_member(&mut state, target.clone(), "target", Vec::new());
        add_member(
            &mut state,
            wrong_neighbor.clone(),
            "wrong",
            vec![wrong_endpoint.clone()],
        );
        add_member(
            &mut state,
            bridge.clone(),
            "bridge",
            vec![bridge_endpoint.clone()],
        );
        let wrong_to_target_record = connectivity_record_from_source(
            &cluster_id,
            &wrong_neighbor,
            &target_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
        );
        let bridge_to_target_record = connectivity_record_from_source(
            &cluster_id,
            &bridge,
            &target_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
        );
        crate::write_local_cluster_connectivity_records(
            &paths,
            &wrong_neighbor,
            std::slice::from_ref(&wrong_to_target_record),
        )
        .expect("write wrong neighbor record");
        crate::write_local_cluster_connectivity_records(
            &paths,
            &bridge,
            std::slice::from_ref(&bridge_to_target_record),
        )
        .expect("write bridge record");

        let plan = cluster_delivery_route_plan(
            &paths,
            &state,
            &local,
            &target,
            &ClusterDeliveryFailedEdges::default(),
            &[],
        )
        .expect("route plan");

        assert_eq!(plan.next_hop, bridge);
        assert_eq!(plan.endpoints.len(), 1);
        assert_eq!(plan.endpoints[0].address, bridge_endpoint.address);
    }

    enum SendOutcome {
        Response(&'static str),
        ResponseFrom(&'static str, NodeId),
        Error(&'static str),
    }

    struct RecordingTargetHost {
        outcomes: VecDeque<SendOutcome>,
        signed_routes: Vec<String>,
        sent: Vec<(
            ClusterEndpointRecord,
            ObjectRpcRequest,
            Vec<u8>,
            DaemonRpcTimeouts,
        )>,
    }

    impl RecordingTargetHost {
        fn new(outcomes: impl IntoIterator<Item = SendOutcome>) -> Self {
            Self {
                outcomes: outcomes.into_iter().collect(),
                signed_routes: Vec::new(),
                sent: Vec::new(),
            }
        }
    }

    impl ClusterSignedEndpointRpcAsyncHost for RecordingTargetHost {
        fn sign_endpoint_rpc_request_async<'a>(
            &'a mut self,
            request: &'a ObjectRpcRequest,
        ) -> ClusterSignedEndpointRpcFuture<'a, Vec<u8>> {
            self.signed_routes.push(request.route.clone());
            let auth = format!("auth-{}", self.signed_routes.len()).into_bytes();
            Box::pin(async move { Ok(auth) })
        }

        fn send_signed_endpoint_rpc_async<'a>(
            &'a mut self,
            endpoint: &'a ClusterEndpointRecord,
            auth: &'a [u8],
            request: &'a ObjectRpcRequest,
            timeouts: DaemonRpcTimeouts,
        ) -> ClusterSignedEndpointRpcFuture<'a, ObjectRpcResponse> {
            self.sent
                .push((endpoint.clone(), request.clone(), auth.to_vec(), timeouts));
            let outcome = self
                .outcomes
                .pop_front()
                .unwrap_or(SendOutcome::Response("ok"));
            Box::pin(async move {
                match outcome {
                    SendOutcome::Response(value) => Ok(response(value)),
                    SendOutcome::ResponseFrom(value, responder_node_id) => {
                        let mut response = response(value);
                        response.mark_responder_node_id(responder_node_id.to_string());
                        Ok(response)
                    }
                    SendOutcome::Error(detail) => Err(NetworkError::Io {
                        operation: "test target send".to_string(),
                        detail: detail.to_string(),
                    }),
                }
            })
        }
    }

    #[tokio::test]
    async fn object_rpc_call_target_sends_direct_final_hop() {
        let (_root, paths) = test_paths("sinomo-network-call-target-envelope");
        let cluster_id = cid("cluster_call_target_envelope");
        let local = nid("node_call_target_envelope_local");
        let target = nid("node_call_target_envelope_target");
        let mut state = test_state(cluster_id.clone(), local.clone());
        add_member(
            &mut state,
            target.clone(),
            "target",
            vec![endpoint(&target, "10.90.3.10:21353", 0)],
        );
        let target_endpoint = state.members[1].endpoints[0].clone();
        mark_local_endpoints_healthy(&paths, &state, &[target_endpoint]);
        let request = request();
        let runtime = tokio::runtime::Handle::current();
        let mut host = RecordingTargetHost::new([SendOutcome::Response("target-ok")]);
        let mut client = ClusterObjectRpcClient::routed(&mut host, &runtime, &paths);

        let rpc_response = client
            .call_target(
                &state,
                &target,
                &request,
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_only(Duration::from_secs(
                    5,
                ))),
            )
            .await
            .expect("target rpc");

        assert_eq!(
            rpc_response.decode_body::<String>().expect("body"),
            "target-ok"
        );
        assert_eq!(host.signed_routes, vec![request.route.clone()]);
        assert_eq!(host.sent.len(), 1);
        assert_eq!(host.sent[0].0.node_id, target);
        assert_eq!(host.sent[0].1.route, request.route);
        assert_eq!(host.sent[0].1.auth, None);
        assert_eq!(host.sent[0].2, b"auth-1".to_vec());
    }

    #[tokio::test]
    async fn object_rpc_call_target_replans_after_failed_edge() {
        let (_root, paths) = test_paths("sinomo-network-call-target-replans");
        let cluster_id = cid("cluster_call_target_replans");
        let local = nid("node_call_target_replans_local");
        let target = nid("node_call_target_replans_target");
        let bridge = nid("node_call_target_replans_bridge");
        let mut state = test_state(cluster_id, local);
        add_member(
            &mut state,
            target.clone(),
            "target",
            vec![endpoint(&target, "10.90.4.10:21353", 0)],
        );
        add_member(
            &mut state,
            bridge.clone(),
            "bridge",
            vec![endpoint(&bridge, "10.90.4.20:21353", 10)],
        );
        let target_endpoint = state.members[1].endpoints[0].clone();
        let bridge_endpoint = state.members[2].endpoints[0].clone();
        mark_local_endpoints_healthy(&paths, &state, &[target_endpoint, bridge_endpoint]);
        let runtime = tokio::runtime::Handle::current();
        let mut host = RecordingTargetHost::new([
            SendOutcome::Error("direct edge failed"),
            SendOutcome::Response("bridge-ok"),
        ]);
        let mut client = ClusterObjectRpcClient::routed(&mut host, &runtime, &paths);

        let rpc_response = client
            .call_target(
                &state,
                &target,
                &request(),
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_only(Duration::from_secs(
                    5,
                ))),
            )
            .await
            .expect("target rpc");

        assert_eq!(
            rpc_response.decode_body::<String>().expect("body"),
            "bridge-ok"
        );
        assert_eq!(host.sent.len(), 2);
        assert_eq!(host.sent[0].0.node_id, target);
        assert_eq!(host.sent[0].1.route, request().route);
        assert_eq!(host.sent[1].0.node_id, bridge);
        let retried_envelope = RelayEnvelope::from_request(&host.sent[1].1).expect("envelope");
        assert_eq!(retried_envelope.target_node_id, target);
    }

    #[tokio::test]
    async fn object_rpc_call_target_retries_after_responder_mismatch() {
        let (_root, paths) = test_paths("sinomo-network-call-target-responder-mismatch");
        let cluster_id = cid("cluster_call_target_responder_mismatch");
        let local = nid("node_call_target_mismatch_local");
        let target = nid("node_call_target_mismatch_target");
        let wrong = nid("node_call_target_mismatch_wrong");
        let stale_endpoint = endpoint(&target, "10.90.5.10:21353", 0);
        let good_endpoint = endpoint(&target, "10.90.5.11:21353", 1);
        let mut state = test_state(cluster_id, local);
        add_member(
            &mut state,
            target.clone(),
            "target",
            vec![stale_endpoint.clone(), good_endpoint.clone()],
        );
        mark_local_endpoints_healthy(&paths, &state, &[stale_endpoint, good_endpoint]);
        let runtime = tokio::runtime::Handle::current();
        let mut host = RecordingTargetHost::new([
            SendOutcome::ResponseFrom("wrong-node", wrong),
            SendOutcome::ResponseFrom("target-ok", target.clone()),
        ]);
        let mut client = ClusterObjectRpcClient::routed(&mut host, &runtime, &paths);

        let rpc_response = client
            .call_target(
                &state,
                &target,
                &request(),
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_only(Duration::from_secs(
                    5,
                ))),
            )
            .await
            .expect("target rpc");

        assert_eq!(
            rpc_response.decode_body::<String>().expect("body"),
            "target-ok"
        );
        assert_eq!(host.sent.len(), 2);
        assert_eq!(host.sent[0].0.address, "10.90.5.10:21353");
        assert_eq!(host.sent[1].0.address, "10.90.5.11:21353");
    }

    #[tokio::test]
    async fn delivery_routing_table_updates_after_connectivity_change() {
        let (_root, paths) = test_paths("sinomo-network-delivery-routing-table-updates");
        let cluster_id = cid("cluster_delivery_routing_table_updates");
        let local = nid("node_delivery_routing_table_local");
        let target = nid("node_delivery_routing_table_target");
        let old_endpoint = endpoint(&target, "10.90.6.10:21353", 0);
        let new_endpoint = endpoint(&target, "10.90.6.11:21353", 1);
        let mut state = test_state(cluster_id, local.clone());
        add_member(
            &mut state,
            target.clone(),
            "target",
            vec![old_endpoint.clone(), new_endpoint.clone()],
        );
        mark_local_endpoints_healthy(&paths, &state, std::slice::from_ref(&old_endpoint));
        let runtime = tokio::runtime::Handle::current();

        let first = cluster_delivery_route_plan_async(
            &runtime,
            &paths,
            &state,
            &local,
            &target,
            ClusterDeliveryFailedEdges::default(),
            Vec::new(),
        )
        .await
        .expect("first plan")
        .expect("first route");
        assert_eq!(first.endpoints[0].address, old_endpoint.address);

        let now = OffsetDateTime::now_utc();
        let old_unreachable = crate::cluster_connectivity_record(
            &state,
            &old_endpoint,
            ClusterConnectivityState::Unreachable,
            None,
            crate::CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD,
            None,
            Some(now.to_string()),
            now,
            crate::CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS,
        );
        let new_healthy = crate::cluster_connectivity_record(
            &state,
            &new_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some(now.to_string()),
            None,
            now,
            crate::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        crate::write_local_cluster_connectivity_records(
            &paths,
            &local,
            &[old_unreachable, new_healthy],
        )
        .expect("write updated connectivity");

        let second = cluster_delivery_route_plan_async(
            &runtime,
            &paths,
            &state,
            &local,
            &target,
            ClusterDeliveryFailedEdges::default(),
            Vec::new(),
        )
        .await
        .expect("second plan")
        .expect("second route");
        assert_eq!(second.endpoints[0].address, new_endpoint.address);
    }

    #[tokio::test]
    async fn delivery_routing_table_serves_relay_with_visited_origin() {
        let (_root, paths) = test_paths("sinomo-network-delivery-routing-table-relay-visited");
        let cluster_id = cid("cluster_delivery_routing_table_relay_visited");
        let origin = nid("node_delivery_routing_table_relay_origin");
        let local = nid("node_delivery_routing_table_relay_local");
        let target = nid("node_delivery_routing_table_relay_target");
        let target_endpoint = endpoint(&target, "10.90.7.10:21353", 0);
        let mut state = test_state(cluster_id, local.clone());
        add_member(&mut state, origin.clone(), "origin", Vec::new());
        add_member(
            &mut state,
            target.clone(),
            "target",
            vec![target_endpoint.clone()],
        );
        mark_local_endpoints_healthy(&paths, &state, &[target_endpoint]);
        let runtime = tokio::runtime::Handle::current();

        let plan = cluster_delivery_route_plan_async(
            &runtime,
            &paths,
            &state,
            &local,
            &target,
            ClusterDeliveryFailedEdges::default(),
            vec![origin],
        )
        .await
        .expect("relay route table plan")
        .expect("route");

        assert_eq!(plan.next_hop, target);
        assert_eq!(plan.endpoints[0].address, "10.90.7.10:21353");
    }

    #[test]
    fn delivery_routing_table_filters_failed_and_visited_candidates() {
        let (_root, paths) = test_paths("sinomo-network-delivery-routing-table-candidates");
        let cluster_id = cid("cluster_delivery_routing_table_candidates");
        let local = nid("node_delivery_routing_table_candidates_local");
        let bridge = nid("node_delivery_routing_table_candidates_bridge");
        let target = nid("node_delivery_routing_table_candidates_target");
        let mut state = test_state(cluster_id.clone(), local.clone());
        add_local_endpoint(&mut state, endpoint(&local, "172.26.0.9:21353", 0));
        let bridge_endpoint = endpoint(&bridge, "172.26.0.2:21353", 0);
        let target_endpoint = endpoint(&target, "172.26.0.10:21353", 100);
        add_member(
            &mut state,
            bridge.clone(),
            "bridge",
            vec![bridge_endpoint.clone()],
        );
        add_member(
            &mut state,
            target.clone(),
            "target",
            vec![target_endpoint.clone()],
        );
        mark_local_endpoints_healthy(
            &paths,
            &state,
            &[bridge_endpoint.clone(), target_endpoint.clone()],
        );
        let bridge_to_target_record = connectivity_record_from_source(
            &cluster_id,
            &bridge,
            &target_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
        );
        crate::write_local_cluster_connectivity_records(
            &paths,
            &bridge,
            std::slice::from_ref(&bridge_to_target_record),
        )
        .expect("write bridge record");

        let table = cluster_delivery_build_routing_table(&paths, &state, &local).expect("table");
        let unconstrained = table
            .route_plan(&target, &ClusterDeliveryFailedEdges::default(), &[])
            .expect("route allocation")
            .expect("unconstrained route");
        assert_eq!(unconstrained.next_hop, bridge);

        let mut failed_bridge = ClusterDeliveryFailedEdges::default();
        failed_bridge
            .try_insert(&local, &bridge)
            .expect("failed bridge edge");
        let after_failed_bridge = table
            .route_plan(&target, &failed_bridge, &[])
            .expect("route allocation")
            .expect("route after failed bridge");
        assert_eq!(after_failed_bridge.next_hop, target);

        let after_visited_bridge = table
            .route_plan(
                &target,
                &ClusterDeliveryFailedEdges::default(),
                std::slice::from_ref(&bridge),
            )
            .expect("route allocation")
            .expect("route after visited bridge");
        assert_eq!(after_visited_bridge.next_hop, target);
    }

    #[test]
    fn stable_connectivity_refresh_does_not_bump_route_generation() {
        let (_root, paths) = test_paths("sinomo-network-stable-connectivity-generation");
        let cluster_id = cid("cluster_stable_connectivity_generation");
        let local = nid("node_stable_connectivity_generation_local");
        let target = nid("node_stable_connectivity_generation_target");
        let mut state = test_state(cluster_id, local.clone());
        let target_endpoint = endpoint(&target, "10.90.8.10:21353", 0);
        add_member(&mut state, target, "target", vec![target_endpoint.clone()]);
        let now = OffsetDateTime::now_utc();
        let first = crate::cluster_connectivity_record(
            &state,
            &target_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some(now.to_string()),
            None,
            now,
            crate::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        crate::write_local_cluster_connectivity_records(
            &paths,
            &local,
            std::slice::from_ref(&first),
        )
        .expect("write first connectivity");
        let generation_after_first =
            crate::connectivity_store::local_cluster_connectivity_content_generation(&paths);

        let later = now + time::Duration::seconds(1);
        let stable_refresh = crate::cluster_connectivity_record(
            &state,
            &target_endpoint,
            ClusterConnectivityState::Healthy,
            Some(9),
            0,
            Some(later.to_string()),
            None,
            later,
            crate::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        crate::write_local_cluster_connectivity_records(
            &paths,
            &local,
            std::slice::from_ref(&stable_refresh),
        )
        .expect("write stable connectivity refresh");

        assert_eq!(
            crate::connectivity_store::local_cluster_connectivity_content_generation(&paths),
            generation_after_first
        );
    }

    #[derive(Default)]
    struct RecordingRelayHost {
        sent: Vec<(ClusterEndpointRecord, RelayEnvelope, DaemonRpcTimeouts)>,
        target_sent: Vec<(ClusterEndpointRecord, ObjectRpcRequest, DaemonRpcTimeouts)>,
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
            match RelayEnvelope::from_request(request) {
                Ok(envelope) => {
                    self.sent.push((endpoint.clone(), envelope, timeouts));
                    Box::pin(async { Ok(response("forwarded")) })
                }
                Err(error) => Box::pin(async move { Err(error) }),
            }
        }

        fn send_target_request_async<'a>(
            &'a mut self,
            endpoint: &'a ClusterEndpointRecord,
            request: &'a ObjectRpcRequest,
            timeouts: DaemonRpcTimeouts,
        ) -> Pin<Box<dyn Future<Output = Result<ObjectRpcResponse, NetworkError>> + Send + 'a>>
        {
            self.target_sent
                .push((endpoint.clone(), request.clone(), timeouts));
            Box::pin(async { Ok(response("forwarded")) })
        }
    }

    #[tokio::test]
    async fn relay_forward_uses_delivery_plan() {
        let (_root, paths) = test_paths("sinomo-network-relay-forward-plan");
        let cluster_id = cid("cluster_relay_forward_plan");
        let local = nid("node_relay_forward_plan_local");
        let target = nid("node_relay_forward_plan_target");
        let mut state = test_state(cluster_id.clone(), local.clone());
        add_member(
            &mut state,
            target.clone(),
            "target",
            vec![endpoint(&target, "10.90.5.10:21353", 0)],
        );
        let target_endpoint = state.members[1].endpoints[0].clone();
        mark_local_endpoints_healthy(&paths, &state, &[target_endpoint]);
        let inner = request().with_auth(b"inner-auth".to_vec());
        let envelope = RelayEnvelope::new(cluster_id, target.clone(), inner);
        let relay_request = envelope.into_request().expect("relay request");
        let runtime = tokio::runtime::Handle::current();
        let mut host = RecordingRelayHost::default();

        let rpc_response = handle_cluster_relay_async_with_host(
            &runtime,
            &paths,
            &state,
            &local,
            &relay_request,
            ClusterRelayOptions::new(DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(5))),
            &mut host,
        )
        .await
        .expect("relay forward");

        assert_eq!(
            rpc_response.decode_body::<String>().expect("body"),
            "forwarded"
        );
        assert_eq!(host.sent.len(), 0);
        assert_eq!(host.target_sent.len(), 1);
        assert_eq!(host.target_sent[0].0.node_id, target);
        assert_eq!(host.target_sent[0].1.route, request().route);
        assert_eq!(host.target_sent[0].1.auth, Some(b"inner-auth".to_vec()));
    }
}
