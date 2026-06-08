//! Multi-hop routing of cluster control-plane RPCs across a segmented network.
//!
//! When a target node is not directly reachable, [`cluster_route_next_hop`] finds a path to
//! it through reachable intermediary nodes by walking the cluster's connectivity graph
//! ([`crate::ClusterConnectivityGraph`]); the RPC is then forwarded hop-by-hop wrapped in a
//! [`RelayEnvelope`]. Each hop's daemon makes a [`relay_decision`] -- deliver locally,
//! forward to the next hop (TTL-decremented, visit-stamped), or drop on a loop/TTL/no-route
//! -- and the response travels back along the chain. This generalizes the existing
//! single-hop "forward to a directly reachable voter" path to "any target, via relay".
//!
//! This module owns routing logic over the graph + envelope. Signing and dialing
//! are supplied by daemon-host callbacks so route construction stays here without
//! coupling the network crate to daemon identity storage.

use crate::prelude::*;
use crate::{
    ClusterConnectivityGraph, ClusterPaths, DaemonControlRequest, NetworkError, ObjectRpcRequest,
    ObjectRpcResponse,
};
use std::time::Instant;

/// RPC category for routing/relay traffic.
pub const CLUSTER_ROUTING_RPC_CATEGORY: &str = "cluster.routing";
/// Route an RPC is sent on when it is wrapped in a [`RelayEnvelope`] for forwarding.
pub const CLUSTER_RELAY_ROUTE: &str = "/cluster/relay";
/// Default hop budget for a relayed RPC (roughly the cluster diameter). Each forward
/// decrements it; a relay is dropped when it reaches zero.
pub const CLUSTER_RELAY_DEFAULT_TTL: u8 = 8;

/// A discovered route to a target node: the immediate next hop to send to, and the full
/// node path (`local` … `target`, inclusive) for diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterRoute {
    pub next_hop: NodeId,
    pub path: Vec<NodeId>,
}

/// Breadth-first search the connectivity graph for a shortest path from `local` to
/// `target` over reachable edges (records whose state is not `Unreachable`), returning the
/// first hop along it. Returns `None` when `target == local` or no reachable path exists.
pub fn cluster_route_next_hop(
    graph: &ClusterConnectivityGraph,
    local: &NodeId,
    target: &NodeId,
) -> Option<ClusterRoute> {
    cluster_route_next_hop_excluding(graph, local, target, &[])
}

fn cluster_route_next_hop_excluding(
    graph: &ClusterConnectivityGraph,
    local: &NodeId,
    target: &NodeId,
    excluded: &[NodeId],
) -> Option<ClusterRoute> {
    if local == target {
        return None;
    }
    let mut seen: BTreeSet<NodeId> = BTreeSet::new();
    seen.extend(excluded.iter().cloned());
    seen.insert(local.clone());
    let mut predecessor = BTreeMap::<NodeId, NodeId>::new();
    let mut queue = VecDeque::from([local.clone()]);
    while let Some(current) = queue.pop_front() {
        for next in graph.healthy_target_refs(&current) {
            if !seen.insert(next.clone()) {
                continue;
            }
            predecessor.insert(next.clone(), current.clone());
            if next == target {
                return route_from_predecessors(local, target, predecessor);
            }
            queue.push_back(next.clone());
        }
    }
    None
}

fn route_from_predecessors(
    local: &NodeId,
    target: &NodeId,
    predecessor: BTreeMap<NodeId, NodeId>,
) -> Option<ClusterRoute> {
    let mut reversed = vec![target.clone()];
    let mut current = target;
    while current != local {
        let previous = predecessor.get(current)?;
        reversed.push(previous.clone());
        current = previous;
    }
    reversed.reverse();
    let next_hop = reversed.get(1).cloned()?;
    Some(ClusterRoute {
        next_hop,
        path: reversed,
    })
}

/// The next hop `local` should send a relay toward `target`.
///
/// Prefers the first hop of a fully-known route ([`cluster_route_next_hop`]). When no full
/// path is known it falls back to any directly-reachable neighbor not in `excluded`, letting
/// that hop re-route with its own (fuller) connectivity graph. This is what carries traffic
/// for a freshly-joined cross-segment learner: it knows it can reach the bridge it joined
/// through, but not the bridge's *onward* edges (those records were published after its join
/// snapshot and it cannot receive ongoing KV replication across the segment), so its own BFS
/// dead-ends at the bridge. `excluded` is the relay's visited set (and, at the origin, the
/// local node itself) so the fallback never bounces back along the path; the TTL bounds any
/// remaining flailing toward a genuinely unreachable target.
pub fn cluster_route_first_hop_best_effort(
    graph: &ClusterConnectivityGraph,
    local: &NodeId,
    target: &NodeId,
    excluded: &[NodeId],
) -> Option<NodeId> {
    cluster_route_next_hops_best_effort(graph, local, target, excluded)
        .into_iter()
        .next()
}

/// Ordered next-hop candidates for best-effort relay forwarding.
///
/// A fully-known route is always returned as the only candidate. When the local
/// graph only knows directly-reachable neighbors, return all usable neighbors so
/// the daemon relay layer can retry another bridge if the first one reports a
/// loop or dead end.
pub fn cluster_route_next_hops_best_effort(
    graph: &ClusterConnectivityGraph,
    local: &NodeId,
    target: &NodeId,
    excluded: &[NodeId],
) -> Vec<NodeId> {
    if let Some(route) = cluster_route_next_hop_excluding(graph, local, target, excluded) {
        return vec![route.next_hop];
    }
    let excluded = excluded.iter().collect::<BTreeSet<_>>();
    let mut candidates = graph
        .healthy_target_refs(local)
        .filter(|candidate| {
            *candidate != target && *candidate != local && !excluded.contains(candidate)
        })
        .cloned()
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        best_effort_candidate_rank(graph, local, target, &excluded, left)
            .cmp(&best_effort_candidate_rank(
                graph, local, target, &excluded, right,
            ))
            .then_with(|| left.cmp(right))
    });
    candidates
}

fn best_effort_candidate_rank(
    graph: &ClusterConnectivityGraph,
    local: &NodeId,
    target: &NodeId,
    excluded: &BTreeSet<&NodeId>,
    candidate: &NodeId,
) -> (u8, usize) {
    let mut target_hint = 1;
    let mut forward_frontier = 0;
    for next in graph.healthy_target_refs(candidate) {
        if next == target {
            target_hint = 0;
        }
        if next != local && next != candidate && !excluded.contains(next) {
            forward_frontier += 1;
        }
    }
    (target_hint, usize::MAX - forward_frontier)
}

/// An inner cluster RPC wrapped for hop-by-hop delivery toward `target_node_id`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RelayEnvelope {
    pub cluster_id: ClusterId,
    pub target_node_id: NodeId,
    pub ttl: u8,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub visited: Vec<NodeId>,
    /// The original signed request to deliver at the target, re-injected through its
    /// normal auth + dispatch path (its own signature/invite-proof is carried within).
    pub inner: DaemonControlRequest,
}

impl RelayEnvelope {
    /// A fresh envelope with the default TTL and an empty visited set.
    pub fn new(cluster_id: ClusterId, target_node_id: NodeId, inner: DaemonControlRequest) -> Self {
        Self {
            cluster_id,
            target_node_id,
            ttl: CLUSTER_RELAY_DEFAULT_TTL,
            visited: Vec::new(),
            inner,
        }
    }

    /// Encode this envelope as an [`ObjectRpcRequest`] on [`CLUSTER_RELAY_ROUTE`] for
    /// transport to the next hop.
    pub fn into_request(&self) -> Result<ObjectRpcRequest, NetworkError> {
        ObjectRpcRequest::encode_body(CLUSTER_RELAY_ROUTE, CLUSTER_ROUTING_RPC_CATEGORY, self)
    }

    /// Decode an envelope from a relay-route [`ObjectRpcRequest`].
    pub fn from_request(request: &ObjectRpcRequest) -> Result<Self, NetworkError> {
        request.decode_body()
    }
}

/// What a node should do with a [`RelayEnvelope`] it received.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RelayDecision {
    /// This node is the target: unwrap `inner` and dispatch it into local handling.
    DeliverLocal,
    /// Forward `envelope` (already TTL-decremented and visit-stamped) to `next_hop`.
    Forward {
        next_hop: NodeId,
        envelope: RelayEnvelope,
    },
    /// Cannot route further; drop with a human-readable reason (loop, TTL, or no path).
    Drop(String),
}

/// Decide how `local` should handle `envelope`, using the connectivity `graph`. Pure: the
/// caller performs the local dispatch (on [`RelayDecision::DeliverLocal`]) or the dial to
/// `next_hop` (on [`RelayDecision::Forward`]).
pub fn relay_decision(
    envelope: &RelayEnvelope,
    local: &NodeId,
    graph: &ClusterConnectivityGraph,
) -> RelayDecision {
    if envelope.target_node_id == *local {
        return RelayDecision::DeliverLocal;
    }
    if envelope.visited.iter().any(|node| node == local) {
        return RelayDecision::Drop(format!("relay loop: {local} already on the path"));
    }
    if envelope.ttl == 0 {
        return RelayDecision::Drop("relay ttl exhausted".to_string());
    }
    match cluster_route_first_hop_best_effort(
        graph,
        local,
        &envelope.target_node_id,
        &envelope.visited,
    ) {
        Some(next_hop) => {
            let mut forwarded = envelope.clone();
            forwarded.ttl = envelope.ttl - 1;
            forwarded.visited.push(local.clone());
            RelayDecision::Forward {
                next_hop,
                envelope: forwarded,
            }
        }
        None => RelayDecision::Drop(format!(
            "no reachable route from {local} to {}",
            envelope.target_node_id
        )),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterRelayOptions {
    pub timeout: Duration,
}

pub trait ClusterRelayHost {
    fn try_deliver_local(
        &mut self,
        state: &ClusterBootstrapState,
        envelope: &RelayEnvelope,
        timeout: Duration,
    ) -> Result<Option<ObjectRpcResponse>, NetworkError>;

    fn send_relay_request(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterSignedRelayRpcOptions {
    pub timeout: Duration,
}

pub trait ClusterSignedRelayRpcHost {
    fn sign_relay_rpc_request(
        &mut self,
        request: &ObjectRpcRequest,
    ) -> Result<Vec<u8>, NetworkError>;

    fn send_signed_relay_rpc(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        command: &str,
        auth: &[u8],
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterSignedEndpointRpcOptions {
    pub timeout: Duration,
}

pub trait ClusterSignedEndpointRpcHost {
    fn try_gateway_signed_rpc(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        request_auth: &[u8],
        request_command: &str,
        timeout: Duration,
    ) -> Result<Option<ObjectRpcResponse>, NetworkError>;

    fn endpoint_prefers_relay(
        &mut self,
        endpoint: &ClusterEndpointRecord,
    ) -> Result<bool, NetworkError>;

    fn send_signed_endpoint_rpc(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        command: &str,
        auth: &[u8],
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError>;

    fn relay_signed_endpoint_rpc(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        request_auth: &[u8],
        request_command: &str,
        relay_command: &str,
        timeout: Duration,
    ) -> Result<Option<ObjectRpcResponse>, NetworkError>;
}

#[derive(Clone, Copy, Debug)]
struct ClusterEndpointRpcDeadline {
    started: Instant,
    timeout: Duration,
}

impl ClusterEndpointRpcDeadline {
    fn new(timeout: Duration) -> Self {
        Self {
            started: Instant::now(),
            timeout,
        }
    }

    fn checked_remaining(&self) -> Option<Duration> {
        self.timeout
            .checked_sub(self.started.elapsed())
            .filter(|remaining| !remaining.is_zero())
    }

    fn timeout_error(&self, endpoint: &ClusterEndpointRecord) -> NetworkError {
        NetworkError::Io {
            operation: "cluster endpoint rpc".to_string(),
            detail: format!(
                "timed out sending to {} after {}ms",
                endpoint.address,
                self.timeout.as_millis()
            ),
        }
    }
}

/// Wrap an already-signed object RPC in a relay request for hop-by-hop delivery.
pub fn relay_request_for_signed_rpc(
    cluster_id: &ClusterId,
    target_node_id: &NodeId,
    request: &ObjectRpcRequest,
    request_auth: impl Into<Vec<u8>>,
    request_command: impl Into<String>,
    visited: impl IntoIterator<Item = NodeId>,
) -> Result<ObjectRpcRequest, NetworkError> {
    let inner = DaemonControlRequest::authenticated_rpc(
        request_command.into(),
        request.clone(),
        request_auth.into(),
    );
    let mut envelope = RelayEnvelope::new(cluster_id.clone(), target_node_id.clone(), inner);
    envelope.visited.extend(visited);
    envelope.into_request()
}

/// Wrap an already-built object RPC for relay to `target_node_id` through `peer`.
///
/// Network owns the relay envelope and routing-error semantics; the caller supplies
/// daemon-specific command strings, signing, timeout budget, and transport.
pub fn relay_rpc_to_node_via_peer_with_host(
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
    peer: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    request_command: impl Into<String>,
    relay_command: &str,
    options: ClusterSignedRelayRpcOptions,
    host: &mut impl ClusterSignedRelayRpcHost,
) -> Result<ObjectRpcResponse, NetworkError> {
    let request_auth = host.sign_relay_rpc_request(request)?;
    let outer = relay_request_for_signed_rpc(
        &state.cluster_id,
        target_node_id,
        request,
        request_auth,
        request_command,
        std::iter::once(state.local_node_id.clone()),
    )?;
    let outer_auth = host.sign_relay_rpc_request(&outer)?;
    let response =
        host.send_signed_relay_rpc(peer, relay_command, &outer_auth, &outer, options.timeout)?;
    if relay_response_is_routing_error(&response) {
        return Err(NetworkError::Io {
            operation: "cluster relay".to_string(),
            detail: format!("peer returned relay routing error for {target_node_id}"),
        });
    }
    Ok(response)
}

/// Send an already-signed RPC to a cluster endpoint, choosing local gateway,
/// direct dial, and relay fallback according to network routing policy.
///
/// The daemon supplies signing output, local gateway checks, direct transport,
/// and relay transport callbacks. The route ordering and fallback behavior live
/// here so endpoint RPC delivery stays part of the network/routing layer.
pub fn send_signed_rpc_to_endpoint_with_host(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    request_auth: &[u8],
    request_command: &str,
    relay_command: &str,
    options: ClusterSignedEndpointRpcOptions,
    host: &mut impl ClusterSignedEndpointRpcHost,
) -> Result<ObjectRpcResponse, NetworkError> {
    let deadline = ClusterEndpointRpcDeadline::new(options.timeout);
    if let Some(remaining) = deadline.checked_remaining() {
        if let Some(response) = host.try_gateway_signed_rpc(
            endpoint,
            request,
            request_auth,
            request_command,
            remaining,
        )? {
            return Ok(response);
        }
    }

    if !host.endpoint_prefers_relay(endpoint)? {
        let Some(remaining) = deadline.checked_remaining() else {
            return Err(deadline.timeout_error(endpoint));
        };
        match host.send_signed_endpoint_rpc(
            endpoint,
            request_command,
            request_auth,
            request,
            remaining,
        ) {
            Ok(response) => return Ok(response),
            Err(direct_error) => {
                let Some(remaining) = deadline.checked_remaining() else {
                    return Err(direct_error);
                };
                return match host.relay_signed_endpoint_rpc(
                    endpoint,
                    request,
                    request_auth,
                    request_command,
                    relay_command,
                    remaining,
                )? {
                    Some(response) => Ok(response),
                    None => Err(direct_error),
                };
            }
        }
    }

    let Some(remaining) = deadline.checked_remaining() else {
        return Err(deadline.timeout_error(endpoint));
    };
    match host.relay_signed_endpoint_rpc(
        endpoint,
        request,
        request_auth,
        request_command,
        relay_command,
        remaining,
    )? {
        Some(response) => Ok(response),
        None => Err(NetworkError::Io {
            operation: "cluster endpoint rpc".to_string(),
            detail: format!(
                "endpoint {} is unreachable and no relay route is available",
                endpoint.node_id
            ),
        }),
    }
}

/// Relay an already-signed object RPC toward `endpoint.node_id` using the local
/// connectivity graph and reachable next-hop endpoints.
///
/// Returns `Ok(None)` when the request is itself a relay request or the graph has
/// no usable route. Once a route is known, retry and routing-error behavior is
/// network-owned; callers only supply daemon-specific signing and transport.
pub fn relay_rpc_to_endpoint_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    request_auth: impl Into<Vec<u8>>,
    request_command: impl Into<String>,
    relay_command: &str,
    options: ClusterSignedRelayRpcOptions,
    host: &mut impl ClusterSignedRelayRpcHost,
) -> Result<Option<ObjectRpcResponse>, NetworkError> {
    if request.route == CLUSTER_RELAY_ROUTE {
        return Ok(None);
    }
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    let next_hops =
        cluster_relay_next_hops(&graph, state, &state.local_node_id, &endpoint.node_id, &[]);
    if next_hops.is_empty() {
        return Ok(None);
    }

    let started = Instant::now();
    let request_auth = request_auth.into();
    let request_command = request_command.into();
    let mut failures = Vec::new();
    'relay_hops: for next_hop_id in next_hops {
        let endpoints =
            cluster_relay_next_hop_endpoints(paths, state, &state.local_node_id, &next_hop_id);
        if endpoints.is_empty() {
            failures.push(format!(
                "{next_hop_id}: relay next hop has no reachable endpoint"
            ));
            continue;
        }
        let outer = relay_request_for_signed_rpc(
            &state.cluster_id,
            &endpoint.node_id,
            request,
            request_auth.clone(),
            request_command.clone(),
            std::iter::once(state.local_node_id.clone()),
        )?;
        let outer_auth = host.sign_relay_rpc_request(&outer)?;
        for next_hop in endpoints {
            let Some(remaining) = options
                .timeout
                .checked_sub(started.elapsed())
                .filter(|remaining| !remaining.is_zero())
            else {
                failures.push(format!("relay budget expired after {:?}", options.timeout));
                break 'relay_hops;
            };
            match host.send_signed_relay_rpc(
                &next_hop,
                relay_command,
                &outer_auth,
                &outer,
                remaining,
            ) {
                Ok(response) if relay_response_is_routing_error(&response) => {
                    failures.push(format!(
                        "{} ({:?}) returned routing error",
                        next_hop.address, next_hop.transport
                    ));
                }
                Ok(response) => return Ok(Some(response)),
                Err(error) => failures.push(format!(
                    "{} ({:?}) failed: {}",
                    next_hop.address, next_hop.transport, error
                )),
            }
        }
    }

    Err(NetworkError::Io {
        operation: "cluster relay".to_string(),
        detail: format!(
            "relay route to {} failed{}",
            endpoint.node_id,
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    })
}

pub fn handle_cluster_relay_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    envelope: RelayEnvelope,
    options: ClusterRelayOptions,
    host: &mut impl ClusterRelayHost,
) -> Result<ObjectRpcResponse, NetworkError> {
    if let Some(response) = host.try_deliver_local(state, &envelope, options.timeout)? {
        return Ok(response);
    }

    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    if envelope.visited.iter().any(|node| node == local_node_id) {
        return ObjectRpcResponse::encode_error(
            CLUSTER_RELAY_ROUTE,
            format!("relay dropped: relay loop: {local_node_id} already on the path"),
        );
    }
    if envelope.ttl == 0 {
        return ObjectRpcResponse::encode_error(
            CLUSTER_RELAY_ROUTE,
            "relay dropped: relay ttl exhausted",
        );
    }

    let next_hops = cluster_relay_next_hops_with_direct_target_fallback(
        paths,
        &graph,
        state,
        local_node_id,
        &envelope.target_node_id,
        &envelope.visited,
    );
    if next_hops.is_empty() {
        return ObjectRpcResponse::encode_error(
            CLUSTER_RELAY_ROUTE,
            format!(
                "relay dropped: no reachable route from {local_node_id} to {}",
                envelope.target_node_id
            ),
        );
    }

    let mut failures = Vec::new();
    for next_hop in next_hops {
        let endpoints = cluster_relay_next_hop_endpoints(paths, state, local_node_id, &next_hop);
        if endpoints.is_empty() {
            failures.push(format!(
                "{next_hop}: relay next hop has no reachable endpoint"
            ));
            continue;
        }
        for endpoint in endpoints {
            let mut forwarded = envelope.clone();
            forwarded.ttl = envelope.ttl.saturating_sub(1);
            forwarded.visited.push(local_node_id.clone());
            let outer = forwarded.into_request()?;
            match host.send_relay_request(&endpoint, &outer, options.timeout) {
                Ok(response) if relay_response_is_routing_error(&response) => {
                    failures.push(format!(
                        "{} ({:?}) returned routing error",
                        endpoint.address, endpoint.transport
                    ));
                }
                Ok(response) => return Ok(response),
                Err(error) => failures.push(format!(
                    "{} ({:?}) failed: {}",
                    endpoint.address, endpoint.transport, error
                )),
            }
        }
    }

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

pub fn relay_response_is_routing_error(response: &ObjectRpcResponse) -> bool {
    response.route == CLUSTER_RELAY_ROUTE && response.error_detail().is_err()
}

pub fn cluster_relay_next_hops(
    graph: &ClusterConnectivityGraph,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    excluded: &[NodeId],
) -> Vec<NodeId> {
    cluster_route_next_hops_best_effort(graph, local_node_id, target_node_id, excluded)
        .into_iter()
        .filter(|candidate| relay_candidate_can_transit(state, candidate, target_node_id))
        .collect()
}

pub fn cluster_relay_next_hops_with_direct_target_fallback(
    paths: &ClusterPaths,
    graph: &ClusterConnectivityGraph,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    excluded: &[NodeId],
) -> Vec<NodeId> {
    let mut next_hops =
        cluster_relay_next_hops(graph, state, local_node_id, target_node_id, excluded);
    if next_hops.is_empty()
        && !cluster_relay_next_hop_endpoints(paths, state, local_node_id, target_node_id).is_empty()
    {
        // Startup joins can race ahead of connectivity publication. If the graph has not
        // learned a route yet but this relay node has a normal command endpoint for the
        // final target, try delivering directly instead of dropping the relay.
        next_hops.push(target_node_id.clone());
    }
    next_hops
}

fn relay_candidate_can_transit(
    state: &ClusterBootstrapState,
    candidate: &NodeId,
    target_node_id: &NodeId,
) -> bool {
    if candidate == target_node_id {
        return true;
    }
    state
        .members
        .iter()
        .find(|member| member.node_id == *candidate)
        .is_some_and(|member| {
            member
                .capabilities
                .iter()
                .any(|capability| capability.as_str() == crate::DAEMON_MAIN_RUN_CAPABILITY)
        })
}

pub fn cluster_relay_next_hop_endpoints(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    source_node_id: &NodeId,
    node_id: &NodeId,
) -> Vec<ClusterEndpointRecord> {
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    let mut endpoints = Vec::new();
    let mut seen = BTreeSet::new();
    if let Some(endpoint) = graph.best_healthy_endpoint(source_node_id, node_id) {
        push_unique_cluster_endpoint(&mut endpoints, &mut seen, endpoint);
    }
    if let Some(member) = state
        .members
        .iter()
        .find(|member| member.node_id == *node_id && member.state.is_authorized())
    {
        for endpoint in member
            .endpoints
            .iter()
            .filter(|endpoint| crate::locally_learned_bridge_route_endpoint(endpoint))
        {
            push_unique_cluster_endpoint(&mut endpoints, &mut seen, endpoint.clone());
        }
    }
    for endpoint in crate::cluster_main_daemon_command_targets(paths, state)
        .into_iter()
        .filter(|endpoint| endpoint.node_id == *node_id)
    {
        push_unique_cluster_endpoint(&mut endpoints, &mut seen, endpoint);
    }
    endpoints
}

fn push_unique_cluster_endpoint(
    endpoints: &mut Vec<ClusterEndpointRecord>,
    seen: &mut BTreeSet<(NodeId, String, String)>,
    endpoint: ClusterEndpointRecord,
) {
    if seen.insert((
        endpoint.node_id.clone(),
        format!("{:?}", endpoint.transport),
        endpoint.address.clone(),
    )) {
        endpoints.push(endpoint);
    }
}

pub fn cluster_endpoint_prefers_relay_with_graph(
    graph: &ClusterConnectivityGraph,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
) -> bool {
    if endpoint.node_id == state.local_node_id {
        return false;
    }
    if graph.state_for_endpoint(endpoint) == Some(&crate::ClusterConnectivityState::Healthy) {
        return false;
    }
    // No healthy direct edge. Relay iff the relay would actually make progress -- i.e. the same
    // best-effort first-hop the relay uses resolves (a full healthy path OR a healthy neighbor
    // to hand off to). Unknown direct reachability is still skipped when a route exists: in
    // segmented clusters, spending the caller's entire health/command budget on an unprobed
    // cross-segment direct dial prevents the already-stabilized graph from being used.
    !cluster_relay_next_hops(graph, state, &state.local_node_id, &endpoint.node_id, &[]).is_empty()
}

/// Whether endpoint RPC delivery should skip direct dial and relay immediately,
/// using the persisted local cluster state and connectivity graph.
///
/// Returns `false` when the cluster state cannot be read. That preserves the
/// direct-first behavior for startup, unclustered clients, and damaged local
/// caches while still letting a healthy graph avoid doomed direct dials.
pub fn cluster_endpoint_prefers_relay(
    paths: &ClusterPaths,
    endpoint: &ClusterEndpointRecord,
) -> bool {
    let Ok(state) = crate::read_cluster_state(paths) else {
        return false;
    };
    if endpoint.node_id == state.local_node_id {
        return false;
    }
    let graph = ClusterConnectivityGraph::read_for_local(paths, &state);
    cluster_endpoint_prefers_relay_with_graph(&graph, &state, endpoint)
}

/// Relay an already-signed endpoint RPC using the persisted local cluster state.
///
/// Returns `Ok(None)` when there is no usable local cluster state, when the
/// request is already a relay request, or when no relay route is known. Once a
/// route exists, relay failures are returned as errors.
pub fn relay_rpc_to_endpoint_from_paths_with_host(
    paths: &ClusterPaths,
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    request_auth: impl Into<Vec<u8>>,
    request_command: impl Into<String>,
    relay_command: &str,
    options: ClusterSignedRelayRpcOptions,
    host: &mut impl ClusterSignedRelayRpcHost,
) -> Result<Option<ObjectRpcResponse>, NetworkError> {
    if request.route == CLUSTER_RELAY_ROUTE {
        return Ok(None);
    }
    let Ok(state) = crate::read_cluster_state(paths) else {
        return Ok(None);
    };
    relay_rpc_to_endpoint_with_host(
        paths,
        &state,
        endpoint,
        request,
        request_auth,
        request_command,
        relay_command,
        options,
        host,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClusterConnectivityRecord, ClusterConnectivityState};

    fn nid(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).expect("node id")
    }

    fn edge(
        source: &NodeId,
        target: &NodeId,
        state: ClusterConnectivityState,
    ) -> ClusterConnectivityRecord {
        let now = OffsetDateTime::now_utc();
        ClusterConnectivityRecord {
            cluster_id: ClusterId::from_legacy_named("cluster_route_test").expect("cluster id"),
            source_node_id: source.clone(),
            target_node_id: target.clone(),
            endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_route_test")
                .expect("endpoint id"),
            transport: ClusterTransportKind::LanTcp,
            address: "10.0.0.1:21353".to_string(),
            state,
            latency_ms: None,
            consecutive_failures: 0,
            last_success_at: None,
            last_failure_at: None,
            observed_at: now,
            expires_at: now + time::Duration::seconds(600),
        }
    }

    fn graph(edges: Vec<ClusterConnectivityRecord>) -> ClusterConnectivityGraph {
        let local = edges
            .first()
            .map(|record| record.source_node_id.clone())
            .unwrap_or_else(|| nid("node_local"));
        ClusterConnectivityGraph::from_records(local, edges)
    }

    fn inner_request() -> DaemonControlRequest {
        DaemonControlRequest::rpc(
            "/cluster/kv/txn".to_string(),
            ObjectRpcRequest {
                route: "/cluster/kv/txn".to_string(),
                category: "cluster.kv".to_string(),
                metadata: BTreeMap::new(),
                body: vec![1, 2, 3, 4],
            },
        )
    }

    #[test]
    fn next_hop_none_for_self() {
        let (a, b) = (nid("node_a"), nid("node_b"));
        let g = graph(vec![edge(&a, &b, ClusterConnectivityState::Healthy)]);
        assert_eq!(cluster_route_next_hop(&g, &a, &a), None);
    }

    #[test]
    fn next_hop_direct_edge() {
        let (a, b) = (nid("node_a"), nid("node_b"));
        let g = graph(vec![edge(&a, &b, ClusterConnectivityState::Healthy)]);
        let route = cluster_route_next_hop(&g, &a, &b).expect("route");
        assert_eq!(route.next_hop, b);
        assert_eq!(route.path, vec![a, b]);
    }

    #[test]
    fn next_hop_two_hops_through_bridge() {
        // a -> b -> c; a cannot reach c directly. Mirrors node4 -> node3 -> node1.
        let (a, b, c) = (nid("node_a"), nid("node_b"), nid("node_c"));
        let g = graph(vec![
            edge(&a, &b, ClusterConnectivityState::Healthy),
            edge(&b, &c, ClusterConnectivityState::Healthy),
        ]);
        let route = cluster_route_next_hop(&g, &a, &c).expect("route");
        assert_eq!(route.next_hop, b);
        assert_eq!(route.path, vec![a, b, c]);
    }

    #[test]
    fn next_hop_long_chain_reconstructs_path_once() {
        let nodes = (0..24)
            .map(|index| nid(&format!("node_chain_{index:02}")))
            .collect::<Vec<_>>();
        let edges = nodes
            .windows(2)
            .map(|window| edge(&window[0], &window[1], ClusterConnectivityState::Healthy))
            .collect::<Vec<_>>();
        let g = graph(edges);

        let route = cluster_route_next_hop(
            &g,
            nodes.first().expect("first node"),
            nodes.last().expect("last node"),
        )
        .expect("route");

        assert_eq!(route.next_hop, nodes[1]);
        assert_eq!(route.path, nodes);
    }

    #[test]
    fn connectivity_graph_cached_adjacency_matches_reachability_rules() {
        let (a, b, c, d) = (nid("node_a"), nid("node_b"), nid("node_c"), nid("node_d"));
        let g = graph(vec![
            edge(&a, &b, ClusterConnectivityState::Healthy),
            edge(&a, &c, ClusterConnectivityState::Degraded),
            edge(&a, &d, ClusterConnectivityState::Unreachable),
            edge(&a, &a, ClusterConnectivityState::Healthy),
        ]);

        assert_eq!(
            g.reachable_targets(&a),
            [b.clone(), c].into_iter().collect::<BTreeSet<_>>()
        );
        assert_eq!(
            g.healthy_targets(&a),
            [b].into_iter().collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn next_hop_excludes_degraded_edges() {
        // Routing only traverses HEALTHY links: a Degraded hop is not a usable direct hop
        // (it may be a never-reachable peer mid-convergence), so no route is found.
        let (a, b, c) = (nid("node_a"), nid("node_b"), nid("node_c"));
        let g = graph(vec![
            edge(&a, &b, ClusterConnectivityState::Healthy),
            edge(&b, &c, ClusterConnectivityState::Degraded),
        ]);
        assert_eq!(cluster_route_next_hop(&g, &a, &c), None);
    }

    #[test]
    fn next_hop_skips_unreachable_direct_edge() {
        // a->c exists but is Unreachable; a->b->c is the usable path.
        let (a, b, c) = (nid("node_a"), nid("node_b"), nid("node_c"));
        let g = graph(vec![
            edge(&a, &c, ClusterConnectivityState::Unreachable),
            edge(&a, &b, ClusterConnectivityState::Healthy),
            edge(&b, &c, ClusterConnectivityState::Healthy),
        ]);
        let route = cluster_route_next_hop(&g, &a, &c).expect("route");
        assert_eq!(route.next_hop, b);
        assert_eq!(route.path, vec![a, b, c]);
    }

    #[test]
    fn next_hop_none_when_no_path() {
        let (a, b, c) = (nid("node_a"), nid("node_b"), nid("node_c"));
        let g = graph(vec![edge(&a, &b, ClusterConnectivityState::Healthy)]);
        assert_eq!(cluster_route_next_hop(&g, &a, &c), None);
    }

    #[test]
    fn best_effort_relays_through_reachable_neighbor_without_full_path() {
        let (a, b, c) = (nid("node_a"), nid("node_b"), nid("node_c"));
        // A can reach B but does not know B's onward edge to C (a freshly-joined learner that
        // has the bridge as a reachable neighbor but not the bridge's published routes), so
        // the full BFS dead-ends. Best-effort still hands off to B, which re-routes onward.
        let g = graph(vec![edge(&a, &b, ClusterConnectivityState::Healthy)]);
        assert_eq!(cluster_route_next_hop(&g, &a, &c), None);
        assert_eq!(
            cluster_route_first_hop_best_effort(&g, &a, &c, &[]),
            Some(b.clone())
        );
        // It honors the visited/excluded set, so a hop never bounces back along the path.
        assert_eq!(
            cluster_route_first_hop_best_effort(&g, &a, &c, std::slice::from_ref(&b)),
            None
        );
    }

    #[test]
    fn best_effort_excludes_visited_nodes_from_full_routes() {
        let (a, b, c) = (nid("node_a"), nid("node_b"), nid("node_c"));
        let g = graph(vec![
            edge(&a, &b, ClusterConnectivityState::Healthy),
            edge(&b, &c, ClusterConnectivityState::Healthy),
        ]);

        assert_eq!(
            cluster_route_first_hop_best_effort(&g, &a, &c, &[]),
            Some(b.clone())
        );
        assert_eq!(
            cluster_route_first_hop_best_effort(&g, &a, &c, std::slice::from_ref(&b)),
            None
        );
    }

    #[test]
    fn best_effort_candidates_include_alternate_neighbors() {
        let (a, b, c, target) = (
            nid("node_a"),
            nid("node_b"),
            nid("node_c"),
            nid("node_target"),
        );
        let g = graph(vec![
            edge(&a, &b, ClusterConnectivityState::Healthy),
            edge(&a, &c, ClusterConnectivityState::Healthy),
        ]);

        assert_eq!(
            cluster_route_next_hops_best_effort(&g, &a, &target, &[]),
            vec![b.clone(), c.clone()]
        );
        assert_eq!(
            cluster_route_next_hops_best_effort(&g, &a, &target, std::slice::from_ref(&b)),
            vec![c]
        );
    }

    #[test]
    fn best_effort_prefers_neighbor_with_forward_frontier() {
        let (a, leaf, bridge, onward, target) = (
            nid("node_a"),
            nid("node_leaf"),
            nid("node_bridge"),
            nid("node_onward"),
            nid("node_target"),
        );
        let g = graph(vec![
            edge(&a, &leaf, ClusterConnectivityState::Healthy),
            edge(&a, &bridge, ClusterConnectivityState::Healthy),
            edge(&bridge, &onward, ClusterConnectivityState::Healthy),
        ]);

        assert_eq!(
            cluster_route_next_hops_best_effort(&g, &a, &target, &[]),
            vec![bridge, leaf]
        );
    }

    #[test]
    fn relay_decision_delivers_to_target() {
        let a = nid("node_a");
        let g = graph(vec![]);
        let envelope = RelayEnvelope::new(
            ClusterId::from_legacy_named("cluster_route_test").expect("cluster"),
            a.clone(),
            inner_request(),
        );
        assert_eq!(
            relay_decision(&envelope, &a, &g),
            RelayDecision::DeliverLocal
        );
    }

    #[test]
    fn relay_decision_forwards_via_next_hop() {
        let (a, b, c) = (nid("node_a"), nid("node_b"), nid("node_c"));
        let g = graph(vec![
            edge(&a, &b, ClusterConnectivityState::Healthy),
            edge(&b, &c, ClusterConnectivityState::Healthy),
        ]);
        let envelope = RelayEnvelope::new(
            ClusterId::from_legacy_named("cluster_route_test").expect("cluster"),
            c.clone(),
            inner_request(),
        );
        match relay_decision(&envelope, &a, &g) {
            RelayDecision::Forward { next_hop, envelope } => {
                assert_eq!(next_hop, b);
                assert_eq!(envelope.ttl, CLUSTER_RELAY_DEFAULT_TTL - 1);
                assert_eq!(envelope.visited, vec![a]);
            }
            other => panic!("expected Forward, got {other:?}"),
        }
    }

    #[test]
    fn relay_decision_drops_on_ttl_zero() {
        let (a, b, c) = (nid("node_a"), nid("node_b"), nid("node_c"));
        let g = graph(vec![
            edge(&a, &b, ClusterConnectivityState::Healthy),
            edge(&b, &c, ClusterConnectivityState::Healthy),
        ]);
        let mut envelope = RelayEnvelope::new(
            ClusterId::from_legacy_named("cluster_route_test").expect("cluster"),
            c,
            inner_request(),
        );
        envelope.ttl = 0;
        assert!(matches!(
            relay_decision(&envelope, &a, &g),
            RelayDecision::Drop(_)
        ));
    }

    #[test]
    fn relay_decision_drops_on_visited_loop() {
        let (a, b, c) = (nid("node_a"), nid("node_b"), nid("node_c"));
        let g = graph(vec![
            edge(&a, &b, ClusterConnectivityState::Healthy),
            edge(&b, &c, ClusterConnectivityState::Healthy),
        ]);
        let mut envelope = RelayEnvelope::new(
            ClusterId::from_legacy_named("cluster_route_test").expect("cluster"),
            c,
            inner_request(),
        );
        envelope.visited.push(a.clone());
        assert!(matches!(
            relay_decision(&envelope, &a, &g),
            RelayDecision::Drop(_)
        ));
    }

    #[test]
    fn relay_envelope_round_trips_through_a_request() {
        let envelope = RelayEnvelope::new(
            ClusterId::from_legacy_named("cluster_route_test").expect("cluster"),
            nid("node_target"),
            inner_request(),
        );
        let request = envelope.into_request().expect("encode");
        assert_eq!(request.route, CLUSTER_RELAY_ROUTE);
        assert_eq!(request.category, CLUSTER_ROUTING_RPC_CATEGORY);
        let decoded = RelayEnvelope::from_request(&request).expect("decode");
        assert_eq!(decoded, envelope);
    }

    #[derive(Default)]
    struct RecordingSignedRelayRpcHost {
        signed_routes: Vec<String>,
        sent: Vec<(
            ClusterEndpointRecord,
            String,
            Vec<u8>,
            RelayEnvelope,
            Duration,
        )>,
    }

    impl ClusterSignedRelayRpcHost for RecordingSignedRelayRpcHost {
        fn sign_relay_rpc_request(
            &mut self,
            request: &ObjectRpcRequest,
        ) -> Result<Vec<u8>, NetworkError> {
            self.signed_routes.push(request.route.clone());
            Ok(format!("sig:{}", request.route).into_bytes())
        }

        fn send_signed_relay_rpc(
            &mut self,
            endpoint: &ClusterEndpointRecord,
            command: &str,
            auth: &[u8],
            request: &ObjectRpcRequest,
            timeout: Duration,
        ) -> Result<ObjectRpcResponse, NetworkError> {
            self.sent.push((
                endpoint.clone(),
                command.to_string(),
                auth.to_vec(),
                RelayEnvelope::from_request(request)?,
                timeout,
            ));
            ObjectRpcResponse::encode_body("/test/signed-relay-ok", &"ok")
        }
    }

    #[test]
    fn signed_relay_rpc_helper_wraps_and_signs_request() {
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_signed_relay").expect("cluster");
        let local = nid("node_signed_relay_local");
        let target = nid("node_signed_relay_target");
        let peer = nid("node_signed_relay_peer");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "signed relay",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_signed_relay")
                .expect("endpoint"),
            node_id: peer.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "10.77.0.7:21353".to_string(),
            source: ClusterEndpointSource::Observed,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 0,
            last_observed_latency_ms: None,
        };
        let request = ObjectRpcRequest {
            route: "/cluster/join/register".to_string(),
            category: "cluster.membership".to_string(),
            metadata: BTreeMap::new(),
            body: vec![9, 8, 7],
        };
        let mut host = RecordingSignedRelayRpcHost::default();

        let response = relay_rpc_to_node_via_peer_with_host(
            &state,
            &target,
            &endpoint,
            &request,
            "RPC /cluster/join/register",
            "RPC /cluster/relay",
            ClusterSignedRelayRpcOptions {
                timeout: Duration::from_secs(15),
            },
            &mut host,
        )
        .expect("relay rpc");

        assert_eq!(response.decode_body::<String>().expect("body"), "ok");
        assert_eq!(
            host.signed_routes,
            vec![
                "/cluster/join/register".to_string(),
                CLUSTER_RELAY_ROUTE.to_string()
            ]
        );
        assert_eq!(host.sent.len(), 1);
        let (sent_endpoint, command, auth, envelope, timeout) =
            host.sent.pop().expect("sent relay");
        assert_eq!(sent_endpoint.node_id, peer);
        assert_eq!(command, "RPC /cluster/relay");
        assert_eq!(auth, b"sig:/cluster/relay".to_vec());
        assert_eq!(timeout, Duration::from_secs(15));
        assert_eq!(envelope.cluster_id, cluster_id);
        assert_eq!(envelope.target_node_id, target);
        assert_eq!(envelope.visited, vec![local]);
        assert_eq!(envelope.inner.command, "RPC /cluster/join/register");
        assert_eq!(
            envelope.inner.auth,
            Some(b"sig:/cluster/join/register".to_vec())
        );
        assert_eq!(envelope.inner.rpc.as_ref(), Some(&request));
    }

    #[test]
    fn signed_relay_endpoint_helper_routes_via_graph_next_hop() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-network-signed-relay-endpoint-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_signed_relay_endpoint").expect("cluster");
        let local = nid("node_signed_relay_endpoint_local");
        let bridge = nid("node_signed_relay_endpoint_bridge");
        let target = nid("node_signed_relay_endpoint_target");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "signed endpoint relay",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let bridge_endpoint = crate::daemon_lan_endpoint(&bridge, "10.80.0.2:21353".to_string(), 5);
        let target_endpoint = crate::daemon_lan_endpoint(&target, "10.81.0.3:21353".to_string(), 5);
        let mut bridge_member = ClusterMemberRecord::local_non_voting(
            cluster_id.clone(),
            bridge.clone(),
            "bridge",
            "pk-bridge",
            now,
        );
        bridge_member.capabilities = main_daemon_capabilities(&bridge_member.role);
        bridge_member.allowed_capabilities = bridge_member.capabilities.clone();
        bridge_member.endpoints = vec![bridge_endpoint.clone()];
        let mut target_member = ClusterMemberRecord::local_non_voting(
            cluster_id.clone(),
            target.clone(),
            "target",
            "pk-target",
            now,
        );
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![target_endpoint.clone()];
        state.members.push(bridge_member);
        state.members.push(target_member);
        crate::write_cluster_state(&paths, &state).expect("write state");
        crate::sync_cluster_connectivity_records_to_kv(
            &paths,
            &local,
            vec![crate::cluster_connectivity_record(
                &state,
                &bridge_endpoint,
                ClusterConnectivityState::Healthy,
                Some(3),
                0,
                Some("ok".to_string()),
                None,
                now,
                60,
            )],
        )
        .expect("connectivity");
        let request = ObjectRpcRequest {
            route: "/cluster/kv/range".to_string(),
            category: "cluster.kv".to_string(),
            metadata: BTreeMap::new(),
            body: vec![1, 2, 3],
        };
        let mut host = RecordingSignedRelayRpcHost::default();

        assert!(
            cluster_endpoint_prefers_relay(&paths, &target_endpoint),
            "path-backed graph read should prefer relay for a routed endpoint with no direct edge"
        );

        let response = relay_rpc_to_endpoint_from_paths_with_host(
            &paths,
            &target_endpoint,
            &request,
            b"inner-auth".to_vec(),
            "RPC /cluster/kv/range",
            "RPC /cluster/relay",
            ClusterSignedRelayRpcOptions {
                timeout: Duration::from_secs(20),
            },
            &mut host,
        )
        .expect("endpoint relay")
        .expect("route");

        assert_eq!(response.decode_body::<String>().expect("body"), "ok");
        assert_eq!(host.signed_routes, vec![CLUSTER_RELAY_ROUTE.to_string()]);
        assert_eq!(host.sent.len(), 1);
        let (sent_endpoint, command, auth, envelope, timeout) =
            host.sent.pop().expect("sent relay");
        assert_eq!(sent_endpoint.node_id, bridge);
        assert_eq!(command, "RPC /cluster/relay");
        assert_eq!(auth, b"sig:/cluster/relay".to_vec());
        assert!(timeout <= Duration::from_secs(20));
        assert_eq!(envelope.cluster_id, cluster_id);
        assert_eq!(envelope.target_node_id, target);
        assert_eq!(envelope.visited, vec![local]);
        assert_eq!(envelope.inner.command, "RPC /cluster/kv/range");
        assert_eq!(envelope.inner.auth, Some(b"inner-auth".to_vec()));
        assert_eq!(envelope.inner.rpc.as_ref(), Some(&request));
        std::fs::remove_dir_all(root).ok();
    }

    struct RecordingSignedEndpointRpcHost {
        calls: Vec<&'static str>,
        prefers_relay: bool,
        gateway_response: Option<ObjectRpcResponse>,
        direct_result: Result<ObjectRpcResponse, NetworkError>,
        relay_result: Result<Option<ObjectRpcResponse>, NetworkError>,
        direct_timeouts: Vec<Duration>,
        relay_timeouts: Vec<Duration>,
        direct_auth: Vec<Vec<u8>>,
        relay_auth: Vec<Vec<u8>>,
    }

    impl RecordingSignedEndpointRpcHost {
        fn new(
            prefers_relay: bool,
            direct_result: Result<ObjectRpcResponse, NetworkError>,
            relay_result: Result<Option<ObjectRpcResponse>, NetworkError>,
        ) -> Self {
            Self {
                calls: Vec::new(),
                prefers_relay,
                gateway_response: None,
                direct_result,
                relay_result,
                direct_timeouts: Vec::new(),
                relay_timeouts: Vec::new(),
                direct_auth: Vec::new(),
                relay_auth: Vec::new(),
            }
        }
    }

    impl ClusterSignedEndpointRpcHost for RecordingSignedEndpointRpcHost {
        fn try_gateway_signed_rpc(
            &mut self,
            _endpoint: &ClusterEndpointRecord,
            _request: &ObjectRpcRequest,
            _request_auth: &[u8],
            _request_command: &str,
            _timeout: Duration,
        ) -> Result<Option<ObjectRpcResponse>, NetworkError> {
            self.calls.push("gateway");
            Ok(self.gateway_response.clone())
        }

        fn endpoint_prefers_relay(
            &mut self,
            _endpoint: &ClusterEndpointRecord,
        ) -> Result<bool, NetworkError> {
            self.calls.push("prefer_relay");
            Ok(self.prefers_relay)
        }

        fn send_signed_endpoint_rpc(
            &mut self,
            _endpoint: &ClusterEndpointRecord,
            command: &str,
            auth: &[u8],
            request: &ObjectRpcRequest,
            timeout: Duration,
        ) -> Result<ObjectRpcResponse, NetworkError> {
            self.calls.push("direct");
            assert_eq!(command, "RPC /cluster/kv/range");
            assert_eq!(request.route, "/cluster/kv/range");
            self.direct_auth.push(auth.to_vec());
            self.direct_timeouts.push(timeout);
            self.direct_result.clone()
        }

        fn relay_signed_endpoint_rpc(
            &mut self,
            _endpoint: &ClusterEndpointRecord,
            request: &ObjectRpcRequest,
            request_auth: &[u8],
            request_command: &str,
            relay_command: &str,
            timeout: Duration,
        ) -> Result<Option<ObjectRpcResponse>, NetworkError> {
            self.calls.push("relay");
            assert_eq!(request.route, "/cluster/kv/range");
            assert_eq!(request_command, "RPC /cluster/kv/range");
            assert_eq!(relay_command, "RPC /cluster/relay");
            self.relay_auth.push(request_auth.to_vec());
            self.relay_timeouts.push(timeout);
            self.relay_result.clone()
        }
    }

    fn endpoint_rpc_request() -> ObjectRpcRequest {
        ObjectRpcRequest {
            route: "/cluster/kv/range".to_string(),
            category: "cluster.kv".to_string(),
            metadata: BTreeMap::new(),
            body: vec![1, 2, 3],
        }
    }

    fn endpoint_rpc_response(route: &str, value: &str) -> ObjectRpcResponse {
        ObjectRpcResponse::encode_body(route, &value.to_string()).expect("response")
    }

    #[test]
    fn signed_endpoint_rpc_direct_failure_relays_when_route_available() {
        let target = nid("node_signed_endpoint_target");
        let endpoint = crate::daemon_lan_endpoint(&target, "10.90.0.3:21353".to_string(), 5);
        let request = endpoint_rpc_request();
        let relay_response = endpoint_rpc_response("/test/endpoint-relay-ok", "relay-ok");
        let mut host = RecordingSignedEndpointRpcHost::new(
            false,
            Err(NetworkError::Io {
                operation: "test direct endpoint rpc".to_string(),
                detail: "direct failed".to_string(),
            }),
            Ok(Some(relay_response)),
        );

        let response = send_signed_rpc_to_endpoint_with_host(
            &endpoint,
            &request,
            b"inner-auth",
            "RPC /cluster/kv/range",
            "RPC /cluster/relay",
            ClusterSignedEndpointRpcOptions {
                timeout: Duration::from_secs(5),
            },
            &mut host,
        )
        .expect("endpoint rpc");

        assert_eq!(response.decode_body::<String>().expect("body"), "relay-ok");
        assert_eq!(
            host.calls,
            vec!["gateway", "prefer_relay", "direct", "relay"]
        );
        assert_eq!(host.direct_auth, vec![b"inner-auth".to_vec()]);
        assert_eq!(host.relay_auth, vec![b"inner-auth".to_vec()]);
        assert_eq!(host.direct_timeouts.len(), 1);
        assert!(host.direct_timeouts[0] <= Duration::from_secs(5));
        assert!(host.direct_timeouts[0] > Duration::from_secs(4));
        assert_eq!(host.relay_timeouts.len(), 1);
        assert!(host.relay_timeouts[0] <= Duration::from_secs(5));
    }

    #[test]
    fn signed_endpoint_rpc_skips_direct_when_relay_preferred() {
        let target = nid("node_signed_endpoint_relay_first_target");
        let endpoint = crate::daemon_lan_endpoint(&target, "10.91.0.3:21353".to_string(), 5);
        let request = endpoint_rpc_request();
        let relay_response = endpoint_rpc_response("/test/endpoint-relay-first-ok", "relay-first");
        let mut host = RecordingSignedEndpointRpcHost::new(
            true,
            Ok(endpoint_rpc_response("/test/direct-unused", "direct")),
            Ok(Some(relay_response)),
        );

        let response = send_signed_rpc_to_endpoint_with_host(
            &endpoint,
            &request,
            b"inner-auth",
            "RPC /cluster/kv/range",
            "RPC /cluster/relay",
            ClusterSignedEndpointRpcOptions {
                timeout: Duration::from_secs(5),
            },
            &mut host,
        )
        .expect("endpoint rpc");

        assert_eq!(
            response.decode_body::<String>().expect("body"),
            "relay-first"
        );
        assert_eq!(host.calls, vec!["gateway", "prefer_relay", "relay"]);
        assert!(host.direct_timeouts.is_empty());
        assert_eq!(host.relay_auth, vec![b"inner-auth".to_vec()]);
    }

    #[derive(Default)]
    struct RecordingRelayHost {
        local_deliveries: usize,
        sent: Vec<(ClusterEndpointRecord, RelayEnvelope)>,
    }

    impl ClusterRelayHost for RecordingRelayHost {
        fn try_deliver_local(
            &mut self,
            _state: &ClusterBootstrapState,
            _envelope: &RelayEnvelope,
            _timeout: Duration,
        ) -> Result<Option<ObjectRpcResponse>, NetworkError> {
            self.local_deliveries += 1;
            Ok(None)
        }

        fn send_relay_request(
            &mut self,
            endpoint: &ClusterEndpointRecord,
            request: &ObjectRpcRequest,
            _timeout: Duration,
        ) -> Result<ObjectRpcResponse, NetworkError> {
            self.sent
                .push((endpoint.clone(), RelayEnvelope::from_request(request)?));
            ObjectRpcResponse::encode_body("/test/relay-ok", &"ok")
        }
    }

    #[test]
    fn relay_driver_forwards_to_next_hop_endpoint() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-network-relay-driver-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_relay_driver").expect("cluster");
        let local = nid("node_relay_driver_local");
        let bridge = nid("node_relay_driver_bridge");
        let target = nid("node_relay_driver_target");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "relay driver",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let bridge_endpoint = crate::daemon_lan_endpoint(&bridge, "10.40.0.2:21353".to_string(), 5);
        let target_endpoint = crate::daemon_lan_endpoint(&target, "10.41.0.3:21353".to_string(), 5);
        let mut bridge_member = ClusterMemberRecord::local_non_voting(
            cluster_id.clone(),
            bridge.clone(),
            "bridge",
            "pk-bridge",
            now,
        );
        bridge_member.capabilities = main_daemon_capabilities(&bridge_member.role);
        bridge_member.allowed_capabilities = bridge_member.capabilities.clone();
        bridge_member.endpoints = vec![bridge_endpoint.clone()];
        let mut target_member = ClusterMemberRecord::local_non_voting(
            cluster_id.clone(),
            target.clone(),
            "target",
            "pk-target",
            now,
        );
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![target_endpoint.clone()];
        state.members.push(bridge_member);
        state.members.push(target_member);
        crate::write_cluster_state(&paths, &state).expect("write state");
        crate::sync_cluster_connectivity_records_to_kv(
            &paths,
            &local,
            vec![crate::cluster_connectivity_record(
                &state,
                &bridge_endpoint,
                ClusterConnectivityState::Healthy,
                Some(3),
                0,
                Some("ok".to_string()),
                None,
                now,
                60,
            )],
        )
        .expect("connectivity");
        let mut host = RecordingRelayHost::default();
        let envelope = RelayEnvelope::new(cluster_id, target.clone(), inner_request());

        let response = handle_cluster_relay_with_host(
            &paths,
            &state,
            &local,
            envelope,
            ClusterRelayOptions {
                timeout: Duration::from_secs(15),
            },
            &mut host,
        )
        .expect("relay");

        assert_eq!(response.decode_body::<String>().expect("body"), "ok");
        assert_eq!(host.local_deliveries, 1);
        assert_eq!(host.sent.len(), 1);
        assert_eq!(host.sent[0].0.node_id, bridge);
        assert_eq!(host.sent[0].1.target_node_id, target);
        assert_eq!(host.sent[0].1.ttl, CLUSTER_RELAY_DEFAULT_TTL - 1);
        assert_eq!(host.sent[0].1.visited, vec![local]);
        std::fs::remove_dir_all(root).ok();
    }
}
