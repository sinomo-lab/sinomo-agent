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
    ClusterConnectivityGraph, ClusterPaths, DaemonControlRequest, DaemonRpcTimeouts, NetworkError,
    ObjectRpcRequest, ObjectRpcResponse,
};
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;

/// RPC category for routing/relay traffic.
pub const CLUSTER_ROUTING_RPC_CATEGORY: &str = "cluster.routing";
/// Route an RPC is sent on when it is wrapped in a [`RelayEnvelope`] for forwarding.
pub const CLUSTER_RELAY_ROUTE: &str = "/cluster/relay";
pub const CLUSTER_ROUTING_RPC_ROUTE_METADATA: &[(&str, &str)] =
    &[(CLUSTER_RELAY_ROUTE, CLUSTER_ROUTING_RPC_CATEGORY)];
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
    pub timeouts: DaemonRpcTimeouts,
}

pub type ClusterRelayFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NetworkError>> + Send + 'a>>;

pub trait ClusterRelayAsyncHost {
    fn try_deliver_local_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        envelope: &'a RelayEnvelope,
        timeouts: DaemonRpcTimeouts,
    ) -> ClusterRelayFuture<'a, Option<ObjectRpcResponse>>;

    fn send_relay_request_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ObjectRpcRequest,
        timeouts: DaemonRpcTimeouts,
    ) -> ClusterRelayFuture<'a, ObjectRpcResponse>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterSignedRelayRpcOptions {
    pub timeouts: DaemonRpcTimeouts,
}

pub type ClusterSignedRelayRpcFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NetworkError>> + Send + 'a>>;

pub trait ClusterSignedRelayRpcAsyncHost {
    fn sign_relay_rpc_request_async<'a>(
        &'a mut self,
        request: &'a ObjectRpcRequest,
    ) -> ClusterSignedRelayRpcFuture<'a, Vec<u8>>;

    fn send_signed_relay_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        command: &'a str,
        auth: &'a [u8],
        request: &'a ObjectRpcRequest,
        timeouts: DaemonRpcTimeouts,
    ) -> ClusterSignedRelayRpcFuture<'a, ObjectRpcResponse>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterSignedEndpointRpcOptions {
    pub timeouts: DaemonRpcTimeouts,
}

pub type ClusterSignedEndpointRpcFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NetworkError>> + Send + 'a>>;

pub trait ClusterSignedEndpointRpcAsyncHost {
    fn sign_endpoint_rpc_request_async<'a>(
        &'a mut self,
        request: &'a ObjectRpcRequest,
    ) -> ClusterSignedEndpointRpcFuture<'a, Vec<u8>>;

    fn try_gateway_signed_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ObjectRpcRequest,
        request_auth: &'a [u8],
        request_command: &'a str,
        timeouts: DaemonRpcTimeouts,
    ) -> ClusterSignedEndpointRpcFuture<'a, Option<ObjectRpcResponse>>;

    fn endpoint_prefers_relay_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
    ) -> ClusterSignedEndpointRpcFuture<'a, bool>;

    fn send_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        command: &'a str,
        auth: &'a [u8],
        request: &'a ObjectRpcRequest,
        timeouts: DaemonRpcTimeouts,
    ) -> ClusterSignedEndpointRpcFuture<'a, ObjectRpcResponse>;

    fn relay_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ObjectRpcRequest,
        request_auth: &'a [u8],
        request_command: &'a str,
        relay_command: &'a str,
        timeouts: DaemonRpcTimeouts,
    ) -> ClusterSignedEndpointRpcFuture<'a, Option<ObjectRpcResponse>>;
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

const CLUSTER_ENDPOINT_DIRECT_ATTEMPT_MAX_BEFORE_RELAY: Duration = Duration::from_secs(3);

fn cluster_endpoint_direct_attempt_timeouts(
    timeouts: DaemonRpcTimeouts,
    remaining_route_budget: Option<Duration>,
) -> DaemonRpcTimeouts {
    let Some(remaining) = remaining_route_budget else {
        return timeouts;
    };
    if remaining <= CLUSTER_RELAY_MIN_BUDGET + CLUSTER_ENDPOINT_DIRECT_ATTEMPT_MAX_BEFORE_RELAY {
        return cluster_endpoint_transport_timeouts(timeouts, Some(remaining));
    }
    let direct_budget = CLUSTER_ENDPOINT_DIRECT_ATTEMPT_MAX_BEFORE_RELAY
        .min(remaining.saturating_sub(CLUSTER_RELAY_MIN_BUDGET));
    cluster_endpoint_transport_timeouts(timeouts, Some(direct_budget))
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
pub async fn relay_rpc_to_node_via_peer_async_with_host(
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
    peer: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    request_command: impl Into<String>,
    relay_command: &str,
    options: ClusterSignedRelayRpcOptions,
    host: &mut impl ClusterSignedRelayRpcAsyncHost,
) -> Result<ObjectRpcResponse, NetworkError> {
    let request_auth = host.sign_relay_rpc_request_async(request).await?;
    let outer = relay_request_for_signed_rpc(
        &state.cluster_id,
        target_node_id,
        request,
        request_auth,
        request_command,
        std::iter::once(state.local_node_id.clone()),
    )?;
    let outer_auth = host.sign_relay_rpc_request_async(&outer).await?;
    let response = host
        .send_signed_relay_rpc_async(peer, relay_command, &outer_auth, &outer, options.timeouts)
        .await?;
    if relay_response_is_routing_error(&response) {
        return Err(NetworkError::Io {
            operation: "cluster relay".to_string(),
            detail: format!("peer returned relay routing error for {target_node_id}"),
        });
    }
    Ok(response)
}

pub async fn send_signed_rpc_to_endpoint_async_with_host(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    request_auth: &[u8],
    request_command: &str,
    relay_command: &str,
    options: ClusterSignedEndpointRpcOptions,
    host: &mut impl ClusterSignedEndpointRpcAsyncHost,
) -> Result<ObjectRpcResponse, NetworkError> {
    let deadline = ClusterEndpointRpcDeadline::new(options.timeouts.connect_timeout);
    if let Some(remaining) = deadline.checked_remaining()
        && let Some(response) = host
            .try_gateway_signed_rpc_async(
                endpoint,
                request,
                request_auth,
                request_command,
                cluster_endpoint_direct_attempt_timeouts(options.timeouts, remaining),
            )
            .await?
    {
        return Ok(response);
    }

    if !host.endpoint_prefers_relay_async(endpoint).await? {
        let Some(remaining) = deadline.checked_remaining() else {
            return Err(deadline.timeout_error(endpoint));
        };
        let direct_auth = host.sign_endpoint_rpc_request_async(request).await?;
        match host
            .send_signed_endpoint_rpc_async(
                endpoint,
                request_command,
                &direct_auth,
                request,
                cluster_endpoint_direct_attempt_timeouts(options.timeouts, remaining),
            )
            .await
        {
            Ok(response) => return Ok(response),
            Err(direct_error) => {
                let Some(remaining) = deadline.checked_remaining() else {
                    return Err(direct_error);
                };
                let relay_auth = host.sign_endpoint_rpc_request_async(request).await?;
                return match host
                    .relay_signed_endpoint_rpc_async(
                        endpoint,
                        request,
                        &relay_auth,
                        request_command,
                        relay_command,
                        cluster_endpoint_transport_timeouts(options.timeouts, remaining),
                    )
                    .await?
                {
                    Some(response) => Ok(response),
                    None => Err(direct_error),
                };
            }
        }
    }

    let Some(remaining) = deadline.checked_remaining() else {
        return Err(deadline.timeout_error(endpoint));
    };
    let relay_auth = host.sign_endpoint_rpc_request_async(request).await?;
    match host
        .relay_signed_endpoint_rpc_async(
            endpoint,
            request,
            &relay_auth,
            request_command,
            relay_command,
            cluster_endpoint_transport_timeouts(options.timeouts, remaining),
        )
        .await?
    {
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

pub async fn relay_rpc_to_endpoint_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    request_auth: impl Into<Vec<u8>>,
    request_command: impl Into<String>,
    relay_command: &str,
    options: ClusterSignedRelayRpcOptions,
    host: &mut impl ClusterSignedRelayRpcAsyncHost,
) -> Result<Option<ObjectRpcResponse>, NetworkError> {
    if request.route == CLUSTER_RELAY_ROUTE {
        return Ok(None);
    }
    let route_plan = relay_route_plan(runtime, paths, state, endpoint).await?;
    if route_plan.is_empty() {
        return Ok(None);
    }

    // A relay spans several hop attempts plus the remote leg; callers tuned
    // for fast direct probes (sub-second connect timeouts) must not starve
    // the whole relay after a single dead endpoint.
    let relay_budget = options
        .timeouts
        .connect_timeout
        .map(|timeout| timeout.max(CLUSTER_RELAY_MIN_BUDGET));
    let deadline = ClusterEndpointRpcDeadline::new(relay_budget);
    let mut next_request_auth = Some(request_auth.into());
    let request_command = request_command.into();
    let mut failures = Vec::new();
    'relay_hops: for (next_hop_id, endpoints) in route_plan {
        if endpoints.is_empty() {
            failures.push(format!(
                "{next_hop_id}: relay next hop has no reachable endpoint"
            ));
            continue;
        }
        for next_hop in endpoints {
            let Some(remaining) = deadline.checked_remaining() else {
                let timeout = options.timeouts.connect_timeout.unwrap_or_default();
                failures.push(format!("relay budget expired after {:?}", timeout));
                break 'relay_hops;
            };
            let request_auth = match next_request_auth.take() {
                Some(auth) => auth,
                None => host.sign_relay_rpc_request_async(request).await?,
            };
            let outer = relay_request_for_signed_rpc(
                &state.cluster_id,
                &endpoint.node_id,
                request,
                request_auth,
                request_command.clone(),
                std::iter::once(state.local_node_id.clone()),
            )?;
            let outer_auth = host.sign_relay_rpc_request_async(&outer).await?;
            match host
                .send_signed_relay_rpc_async(
                    &next_hop,
                    relay_command,
                    &outer_auth,
                    &outer,
                    cluster_endpoint_transport_timeouts(options.timeouts, remaining),
                )
                .await
            {
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

const CLUSTER_RELAY_MIN_BUDGET: std::time::Duration = std::time::Duration::from_secs(3);

async fn relay_route_plan(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
) -> Result<Vec<(NodeId, Vec<ClusterEndpointRecord>)>, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    let endpoint = endpoint.clone();
    runtime
        .spawn_blocking(move || {
            let graph = ClusterConnectivityGraph::read_for_local(&paths, &state);
            let excluded = vec![endpoint.node_id.clone()];
            let next_hops = cluster_relay_next_hops(
                &graph,
                &state,
                &state.local_node_id,
                &endpoint.node_id,
                &excluded,
            );
            Ok(next_hops
                .into_iter()
                .map(|next_hop_id| {
                    let endpoints = cluster_relay_next_hop_endpoints_with_graph(
                        &paths,
                        &graph,
                        &state,
                        &state.local_node_id,
                        &next_hop_id,
                    );
                    (next_hop_id, endpoints)
                })
                .collect())
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster relay route plan".to_string(),
            detail: format!("relay route planning task failed: {source}"),
        })?
}

pub async fn handle_cluster_relay_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    envelope: RelayEnvelope,
    options: ClusterRelayOptions,
    host: &mut impl ClusterRelayAsyncHost,
) -> Result<ObjectRpcResponse, NetworkError> {
    if let Some(response) = host
        .try_deliver_local_async(state, &envelope, options.timeouts)
        .await?
    {
        return Ok(response);
    }

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

    let next_hops =
        cluster_relay_forward_plan(runtime, paths, state, local_node_id, &envelope).await?;
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
    for (next_hop, endpoints) in next_hops {
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
            match host
                .send_relay_request_async(&endpoint, &outer, options.timeouts)
                .await
            {
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

async fn cluster_relay_forward_plan(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    envelope: &RelayEnvelope,
) -> Result<Vec<(NodeId, Vec<ClusterEndpointRecord>)>, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    let local_node_id = local_node_id.clone();
    let target_node_id = envelope.target_node_id.clone();
    let visited = envelope.visited.clone();
    runtime
        .spawn_blocking(move || {
            let graph = ClusterConnectivityGraph::read_for_local(&paths, &state);
            let next_hops = cluster_relay_next_hops_with_direct_target_fallback(
                &paths,
                &graph,
                &state,
                &local_node_id,
                &target_node_id,
                &visited,
            );
            Ok(next_hops
                .into_iter()
                .map(|next_hop| {
                    let endpoints = cluster_relay_next_hop_endpoints_with_graph(
                        &paths,
                        &graph,
                        &state,
                        &local_node_id,
                        &next_hop,
                    );
                    (next_hop, endpoints)
                })
                .collect())
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster relay route plan".to_string(),
            detail: format!("relay route planning task failed: {source}"),
        })?
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
        && !cluster_relay_next_hop_endpoints_with_graph(
            paths,
            graph,
            state,
            local_node_id,
            target_node_id,
        )
        .is_empty()
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
    cluster_relay_next_hop_endpoints_with_graph(paths, &graph, state, source_node_id, node_id)
}

pub fn cluster_relay_next_hop_endpoints_with_graph(
    paths: &ClusterPaths,
    graph: &ClusterConnectivityGraph,
    state: &ClusterBootstrapState,
    source_node_id: &NodeId,
    node_id: &NodeId,
) -> Vec<ClusterEndpointRecord> {
    let mut endpoints = Vec::new();
    let mut seen = BTreeSet::new();
    let has_healthy_next_hop_endpoint =
        if let Some(endpoint) = graph.best_healthy_endpoint(source_node_id, node_id) {
            push_unique_cluster_endpoint(&mut endpoints, &mut seen, endpoint);
            true
        } else {
            false
        };
    if let Some(member) = state
        .members
        .iter()
        .find(|member| member.node_id == *node_id && member.state.is_authorized())
    {
        for endpoint in member
            .endpoints
            .iter()
            .filter(|endpoint| crate::locally_learned_bridge_route_endpoint(endpoint))
            .filter(|endpoint| {
                cluster_relay_next_hop_endpoint_is_usable(
                    graph,
                    *endpoint,
                    has_healthy_next_hop_endpoint,
                )
            })
        {
            push_unique_cluster_endpoint(&mut endpoints, &mut seen, endpoint.clone());
        }
    }
    for endpoint in crate::cluster_main_daemon_command_targets(paths, state)
        .into_iter()
        .filter(|endpoint| endpoint.node_id == *node_id)
        .filter(|endpoint| {
            cluster_relay_next_hop_endpoint_is_usable(
                graph,
                endpoint,
                has_healthy_next_hop_endpoint,
            )
        })
    {
        push_unique_cluster_endpoint(&mut endpoints, &mut seen, endpoint);
    }
    endpoints
}

fn cluster_relay_next_hop_endpoint_is_usable(
    graph: &ClusterConnectivityGraph,
    endpoint: &ClusterEndpointRecord,
    has_healthy_next_hop_endpoint: bool,
) -> bool {
    match graph.state_for_endpoint(endpoint) {
        Some(crate::ClusterConnectivityState::Healthy) => true,
        Some(crate::ClusterConnectivityState::Unreachable) => false,
        Some(crate::ClusterConnectivityState::Degraded) | None => !has_healthy_next_hop_endpoint,
    }
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

pub async fn relay_rpc_to_endpoint_from_paths_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    request_auth: impl Into<Vec<u8>>,
    request_command: impl Into<String>,
    relay_command: &str,
    options: ClusterSignedRelayRpcOptions,
    host: &mut impl ClusterSignedRelayRpcAsyncHost,
) -> Result<Option<ObjectRpcResponse>, NetworkError> {
    if request.route == CLUSTER_RELAY_ROUTE {
        return Ok(None);
    }
    let state_paths = paths.clone();
    let state = match runtime
        .spawn_blocking(move || crate::read_cluster_state(&state_paths))
        .await
    {
        Ok(Ok(state)) => state,
        Err(_) => return Ok(None),
        Ok(Err(_)) => return Ok(None),
    };
    relay_rpc_to_endpoint_async_with_host(
        runtime,
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
    .await
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

    #[test]
    fn relay_next_hop_endpoints_suppress_unverified_fallback_after_healthy_graph_edge() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-relay-endpoint-healthy-filter");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_relay_endpoint_healthy_filter").expect("cluster");
        let local = nid("node_relay_endpoint_filter_local");
        let next_hop = nid("node_relay_endpoint_filter_next");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay endpoint filter",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let stale_member_endpoint =
            crate::daemon_lan_endpoint(&next_hop, "172.24.0.4:21353".to_string(), 5);
        let mut next_hop_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            next_hop.clone(),
            "next-hop",
            "pk-next-hop",
            now,
        );
        next_hop_member.capabilities = main_daemon_capabilities(&next_hop_member.role);
        next_hop_member.allowed_capabilities = next_hop_member.capabilities.clone();
        next_hop_member.endpoints = vec![stale_member_endpoint];
        state.members.push(next_hop_member);
        let healthy_graph_endpoint =
            crate::daemon_lan_endpoint(&next_hop, "172.24.0.3:21353".to_string(), 1);
        let graph = ClusterConnectivityGraph::from_records(
            local.clone(),
            [crate::cluster_connectivity_record(
                &state,
                &healthy_graph_endpoint,
                ClusterConnectivityState::Healthy,
                Some(2),
                0,
                Some("ok".to_string()),
                None,
                now,
                60,
            )],
        );

        let endpoints =
            cluster_relay_next_hop_endpoints_with_graph(&paths, &graph, &state, &local, &next_hop);

        assert_eq!(
            endpoints
                .iter()
                .map(|endpoint| endpoint.address.as_str())
                .collect::<Vec<_>>(),
            vec!["172.24.0.3:21353"]
        );
    }

    #[test]
    fn relay_next_hop_endpoints_keep_unverified_fallback_without_healthy_graph_edge() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-relay-endpoint-startup-fallback");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_relay_endpoint_startup_fallback")
            .expect("cluster");
        let local = nid("node_relay_endpoint_fallback_local");
        let next_hop = nid("node_relay_endpoint_fallback_next");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay endpoint fallback",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let fallback_endpoint =
            crate::daemon_lan_endpoint(&next_hop, "172.24.0.4:21353".to_string(), 5);
        let mut next_hop_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            next_hop.clone(),
            "next-hop",
            "pk-next-hop",
            now,
        );
        next_hop_member.capabilities = main_daemon_capabilities(&next_hop_member.role);
        next_hop_member.allowed_capabilities = next_hop_member.capabilities.clone();
        next_hop_member.endpoints = vec![fallback_endpoint.clone()];
        state.members.push(next_hop_member);
        let graph = ClusterConnectivityGraph::from_records(local.clone(), []);

        let endpoints =
            cluster_relay_next_hop_endpoints_with_graph(&paths, &graph, &state, &local, &next_hop);

        assert_eq!(
            endpoints
                .iter()
                .map(|endpoint| endpoint.address.as_str())
                .collect::<Vec<_>>(),
            vec![fallback_endpoint.address.as_str()]
        );
    }

    #[test]
    fn relay_fallback_excluding_target_uses_intermediary_after_stale_direct_edge() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_relay_stale_direct").expect("cluster");
        let local = nid("node_relay_stale_direct_local");
        let target = nid("node_relay_stale_direct_target");
        let bridge = nid("node_relay_stale_direct_bridge");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay stale direct",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        for node in [&target, &bridge] {
            let mut member = ClusterMemberRecord::local_non_voting(
                state.cluster_id.clone(),
                node.clone(),
                node.as_str(),
                format!("pk-{node}"),
                now,
            );
            member.capabilities = main_daemon_capabilities(&member.role);
            member.allowed_capabilities = member.capabilities.clone();
            state.members.push(member);
        }
        let graph = ClusterConnectivityGraph::from_records(
            local.clone(),
            [
                edge(&local, &target, ClusterConnectivityState::Healthy),
                edge(&local, &bridge, ClusterConnectivityState::Healthy),
                edge(&bridge, &target, ClusterConnectivityState::Healthy),
            ],
        );

        let next_hops = cluster_relay_next_hops(&graph, &state, &local, &target, &[target.clone()]);

        assert_eq!(next_hops, vec![bridge]);
    }

    #[derive(Default)]
    struct RecordingSignedRelayRpcHost {
        signed_routes: Vec<String>,
        sent_timeouts: Vec<DaemonRpcTimeouts>,
        sent: Vec<(
            ClusterEndpointRecord,
            String,
            Vec<u8>,
            RelayEnvelope,
            Duration,
        )>,
    }

    fn recorded_route_timeout(timeouts: DaemonRpcTimeouts) -> Duration {
        timeouts
            .connect_timeout
            .or(timeouts.idle_timeout)
            .expect("route helpers should pass an explicit routing timeout budget")
    }

    impl ClusterSignedRelayRpcAsyncHost for RecordingSignedRelayRpcHost {
        fn sign_relay_rpc_request_async<'a>(
            &'a mut self,
            request: &'a ObjectRpcRequest,
        ) -> ClusterSignedRelayRpcFuture<'a, Vec<u8>> {
            Box::pin(async move {
                self.signed_routes.push(request.route.clone());
                Ok(format!("sig:{}", request.route).into_bytes())
            })
        }

        fn send_signed_relay_rpc_async<'a>(
            &'a mut self,
            endpoint: &'a ClusterEndpointRecord,
            command: &'a str,
            auth: &'a [u8],
            request: &'a ObjectRpcRequest,
            timeouts: DaemonRpcTimeouts,
        ) -> ClusterSignedRelayRpcFuture<'a, ObjectRpcResponse> {
            Box::pin(async move {
                self.sent_timeouts.push(timeouts);
                self.sent.push((
                    endpoint.clone(),
                    command.to_string(),
                    auth.to_vec(),
                    RelayEnvelope::from_request(request)?,
                    recorded_route_timeout(timeouts),
                ));
                ObjectRpcResponse::encode_body("/test/signed-relay-ok", &"ok")
            })
        }
    }

    #[tokio::test]
    async fn signed_relay_endpoint_async_from_paths_prefers_graph_next_hop() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-signed-relay-endpoint");
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

        let runtime = tokio::runtime::Handle::current();
        let response = relay_rpc_to_endpoint_from_paths_async_with_host(
            &runtime,
            &paths,
            &target_endpoint,
            &request,
            b"inner-auth".to_vec(),
            "RPC /cluster/kv/range",
            "RPC /cluster/relay",
            ClusterSignedRelayRpcOptions {
                timeouts: DaemonRpcTimeouts::connect_only(Duration::from_secs(20)),
            },
            &mut host,
        )
        .await
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
        assert_eq!(host.sent_timeouts.len(), 1);
        assert_eq!(host.sent_timeouts[0].idle_timeout, None);
        assert!(host.sent_timeouts[0].connect_timeout <= Some(Duration::from_secs(20)));
        assert_eq!(envelope.cluster_id, cluster_id);
        assert_eq!(envelope.target_node_id, target);
        assert_eq!(envelope.visited, vec![local]);
        assert_eq!(envelope.inner.command, "RPC /cluster/kv/range");
        assert_eq!(envelope.inner.auth, Some(b"inner-auth".to_vec()));
        assert_eq!(envelope.inner.rpc.as_ref(), Some(&request));
    }

    #[tokio::test]
    async fn signed_relay_endpoint_async_helper_routes_via_graph_next_hop() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-signed-relay-endpoint-async");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_signed_relay_endpoint_async").expect("cluster");
        let local = nid("node_signed_relay_endpoint_async_local");
        let bridge = nid("node_signed_relay_endpoint_async_bridge");
        let target = nid("node_signed_relay_endpoint_async_target");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "signed endpoint relay async",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let bridge_endpoint = crate::daemon_lan_endpoint(&bridge, "10.82.0.2:21353".to_string(), 5);
        let target_endpoint = crate::daemon_lan_endpoint(&target, "10.83.0.3:21353".to_string(), 5);
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

        let runtime = tokio::runtime::Handle::current();
        let response = relay_rpc_to_endpoint_from_paths_async_with_host(
            &runtime,
            &paths,
            &target_endpoint,
            &request,
            b"inner-auth".to_vec(),
            "RPC /cluster/kv/range",
            "RPC /cluster/relay",
            ClusterSignedRelayRpcOptions {
                timeouts: DaemonRpcTimeouts::connect_only(Duration::from_secs(20)),
            },
            &mut host,
        )
        .await
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
        assert_eq!(host.sent_timeouts.len(), 1);
        assert_eq!(host.sent_timeouts[0].idle_timeout, None);
        assert!(host.sent_timeouts[0].connect_timeout <= Some(Duration::from_secs(20)));
        assert_eq!(envelope.cluster_id, cluster_id);
        assert_eq!(envelope.target_node_id, target);
        assert_eq!(envelope.visited, vec![local]);
        assert_eq!(envelope.inner.command, "RPC /cluster/kv/range");
        assert_eq!(envelope.inner.auth, Some(b"inner-auth".to_vec()));
        assert_eq!(envelope.inner.rpc.as_ref(), Some(&request));
    }

    struct RecordingSignedEndpointRpcHost {
        calls: Vec<&'static str>,
        prefers_relay: bool,
        gateway_response: Option<ObjectRpcResponse>,
        direct_result: Result<ObjectRpcResponse, NetworkError>,
        relay_result: Result<Option<ObjectRpcResponse>, NetworkError>,
        direct_timeout_policies: Vec<DaemonRpcTimeouts>,
        relay_timeout_policies: Vec<DaemonRpcTimeouts>,
        signed_routes: Vec<String>,
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
                direct_timeout_policies: Vec::new(),
                relay_timeout_policies: Vec::new(),
                signed_routes: Vec::new(),
                direct_auth: Vec::new(),
                relay_auth: Vec::new(),
            }
        }
    }

    impl ClusterSignedEndpointRpcAsyncHost for RecordingSignedEndpointRpcHost {
        fn sign_endpoint_rpc_request_async<'a>(
            &'a mut self,
            request: &'a ObjectRpcRequest,
        ) -> ClusterSignedEndpointRpcFuture<'a, Vec<u8>> {
            Box::pin(async move {
                self.signed_routes.push(request.route.clone());
                Ok(format!("fresh-auth-{}", self.signed_routes.len()).into_bytes())
            })
        }

        fn try_gateway_signed_rpc_async<'a>(
            &'a mut self,
            _endpoint: &'a ClusterEndpointRecord,
            _request: &'a ObjectRpcRequest,
            _request_auth: &'a [u8],
            _request_command: &'a str,
            _timeouts: DaemonRpcTimeouts,
        ) -> ClusterSignedEndpointRpcFuture<'a, Option<ObjectRpcResponse>> {
            Box::pin(async move {
                self.calls.push("gateway");
                Ok(self.gateway_response.clone())
            })
        }

        fn endpoint_prefers_relay_async<'a>(
            &'a mut self,
            _endpoint: &'a ClusterEndpointRecord,
        ) -> ClusterSignedEndpointRpcFuture<'a, bool> {
            Box::pin(async move {
                self.calls.push("prefer_relay");
                Ok(self.prefers_relay)
            })
        }

        fn send_signed_endpoint_rpc_async<'a>(
            &'a mut self,
            _endpoint: &'a ClusterEndpointRecord,
            command: &'a str,
            auth: &'a [u8],
            request: &'a ObjectRpcRequest,
            timeouts: DaemonRpcTimeouts,
        ) -> ClusterSignedEndpointRpcFuture<'a, ObjectRpcResponse> {
            Box::pin(async move {
                self.calls.push("direct");
                assert_eq!(command, "RPC /cluster/kv/range");
                assert_eq!(request.route, "/cluster/kv/range");
                self.direct_auth.push(auth.to_vec());
                self.direct_timeout_policies.push(timeouts);
                self.direct_result.clone()
            })
        }

        fn relay_signed_endpoint_rpc_async<'a>(
            &'a mut self,
            _endpoint: &'a ClusterEndpointRecord,
            request: &'a ObjectRpcRequest,
            request_auth: &'a [u8],
            request_command: &'a str,
            relay_command: &'a str,
            timeouts: DaemonRpcTimeouts,
        ) -> ClusterSignedEndpointRpcFuture<'a, Option<ObjectRpcResponse>> {
            Box::pin(async move {
                self.calls.push("relay");
                assert_eq!(request.route, "/cluster/kv/range");
                assert_eq!(request_command, "RPC /cluster/kv/range");
                assert_eq!(relay_command, "RPC /cluster/relay");
                self.relay_auth.push(request_auth.to_vec());
                self.relay_timeout_policies.push(timeouts);
                self.relay_result.clone()
            })
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

    #[tokio::test]
    async fn signed_endpoint_rpc_async_direct_failure_relays_when_route_available() {
        let target = nid("node_signed_endpoint_async_target");
        let endpoint = crate::daemon_lan_endpoint(&target, "10.90.0.4:21353".to_string(), 5);
        let request = endpoint_rpc_request();
        let relay_response = endpoint_rpc_response("/test/endpoint-async-relay-ok", "relay-ok");
        let mut host = RecordingSignedEndpointRpcHost::new(
            false,
            Err(NetworkError::Io {
                operation: "test direct async endpoint rpc".to_string(),
                detail: "direct failed".to_string(),
            }),
            Ok(Some(relay_response)),
        );

        let response = send_signed_rpc_to_endpoint_async_with_host(
            &endpoint,
            &request,
            b"inner-auth",
            "RPC /cluster/kv/range",
            "RPC /cluster/relay",
            ClusterSignedEndpointRpcOptions {
                timeouts: DaemonRpcTimeouts::connect_only(Duration::from_secs(5)),
            },
            &mut host,
        )
        .await
        .expect("endpoint rpc");

        assert_eq!(response.decode_body::<String>().expect("body"), "relay-ok");
        assert_eq!(
            host.calls,
            vec!["gateway", "prefer_relay", "direct", "relay"]
        );
        assert_eq!(
            host.signed_routes,
            vec![
                "/cluster/kv/range".to_string(),
                "/cluster/kv/range".to_string()
            ]
        );
        assert_eq!(host.direct_auth, vec![b"fresh-auth-1".to_vec()]);
        assert_eq!(host.relay_auth, vec![b"fresh-auth-2".to_vec()]);
        assert_eq!(host.direct_timeout_policies.len(), 1);
        assert_eq!(host.direct_timeout_policies[0].idle_timeout, None);
        assert!(host.direct_timeout_policies[0].connect_timeout <= Some(Duration::from_secs(5)));
        assert_eq!(host.relay_timeout_policies.len(), 1);
        assert_eq!(host.relay_timeout_policies[0].idle_timeout, None);
        assert!(host.relay_timeout_policies[0].connect_timeout <= Some(Duration::from_secs(5)));
    }

    #[tokio::test]
    async fn signed_endpoint_rpc_async_allows_unbounded_transport_policy() {
        let target = nid("node_signed_endpoint_unbounded_target");
        let endpoint = crate::daemon_lan_endpoint(&target, "10.90.0.5:21353".to_string(), 5);
        let request = endpoint_rpc_request();
        let direct_response = endpoint_rpc_response("/test/endpoint-direct-unbounded-ok", "direct");
        let mut host = RecordingSignedEndpointRpcHost::new(false, Ok(direct_response), Ok(None));

        let response = send_signed_rpc_to_endpoint_async_with_host(
            &endpoint,
            &request,
            b"inner-auth",
            "RPC /cluster/kv/range",
            "RPC /cluster/relay",
            ClusterSignedEndpointRpcOptions {
                timeouts: DaemonRpcTimeouts::none(),
            },
            &mut host,
        )
        .await
        .expect("endpoint rpc");

        assert_eq!(response.decode_body::<String>().expect("body"), "direct");
        assert_eq!(host.calls, vec!["gateway", "prefer_relay", "direct"]);
        assert_eq!(
            host.direct_timeout_policies,
            vec![DaemonRpcTimeouts::none()]
        );
        assert_eq!(host.signed_routes, vec!["/cluster/kv/range".to_string()]);
        assert_eq!(host.direct_auth, vec![b"fresh-auth-1".to_vec()]);
        assert!(host.relay_timeout_policies.is_empty());
    }

    #[tokio::test]
    async fn signed_endpoint_rpc_async_skips_direct_when_relay_preferred() {
        let target = nid("node_signed_endpoint_relay_first_target");
        let endpoint = crate::daemon_lan_endpoint(&target, "10.91.0.3:21353".to_string(), 5);
        let request = endpoint_rpc_request();
        let relay_response = endpoint_rpc_response("/test/endpoint-relay-first-ok", "relay-first");
        let mut host = RecordingSignedEndpointRpcHost::new(
            true,
            Ok(endpoint_rpc_response("/test/direct-unused", "direct")),
            Ok(Some(relay_response)),
        );

        let response = send_signed_rpc_to_endpoint_async_with_host(
            &endpoint,
            &request,
            b"inner-auth",
            "RPC /cluster/kv/range",
            "RPC /cluster/relay",
            ClusterSignedEndpointRpcOptions {
                timeouts: DaemonRpcTimeouts::connect_only(Duration::from_secs(5)),
            },
            &mut host,
        )
        .await
        .expect("endpoint rpc");

        assert_eq!(
            response.decode_body::<String>().expect("body"),
            "relay-first"
        );
        assert_eq!(host.calls, vec!["gateway", "prefer_relay", "relay"]);
        assert!(host.direct_auth.is_empty());
        assert_eq!(host.signed_routes, vec!["/cluster/kv/range".to_string()]);
        assert_eq!(host.relay_auth, vec![b"fresh-auth-1".to_vec()]);
    }

    #[derive(Default)]
    struct RecordingRelayHost {
        local_deliveries: usize,
        local_timeouts: Vec<DaemonRpcTimeouts>,
        sent: Vec<(ClusterEndpointRecord, RelayEnvelope)>,
        sent_timeouts: Vec<DaemonRpcTimeouts>,
    }

    impl ClusterRelayAsyncHost for RecordingRelayHost {
        fn try_deliver_local_async<'a>(
            &'a mut self,
            _state: &'a ClusterBootstrapState,
            _envelope: &'a RelayEnvelope,
            timeouts: DaemonRpcTimeouts,
        ) -> ClusterRelayFuture<'a, Option<ObjectRpcResponse>> {
            Box::pin(async move {
                self.local_deliveries += 1;
                self.local_timeouts.push(timeouts);
                Ok(None)
            })
        }

        fn send_relay_request_async<'a>(
            &'a mut self,
            endpoint: &'a ClusterEndpointRecord,
            request: &'a ObjectRpcRequest,
            timeouts: DaemonRpcTimeouts,
        ) -> ClusterRelayFuture<'a, ObjectRpcResponse> {
            Box::pin(async move {
                self.sent
                    .push((endpoint.clone(), RelayEnvelope::from_request(request)?));
                self.sent_timeouts.push(timeouts);
                ObjectRpcResponse::encode_body("/test/relay-ok", &"ok")
            })
        }
    }

    #[tokio::test]
    async fn relay_driver_forwards_to_next_hop_endpoint() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-relay-driver");
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
        let envelope = RelayEnvelope::new(cluster_id, target.clone(), inner_request());

        let mut async_host = RecordingRelayHost::default();
        let runtime = tokio::runtime::Handle::current();
        let async_response = handle_cluster_relay_async_with_host(
            &runtime,
            &paths,
            &state,
            &local,
            envelope,
            ClusterRelayOptions {
                timeouts: DaemonRpcTimeouts::connect_only(Duration::from_secs(15)),
            },
            &mut async_host,
        )
        .await
        .expect("async relay");

        assert_eq!(async_response.decode_body::<String>().expect("body"), "ok");
        assert_eq!(async_host.local_deliveries, 1);
        assert_eq!(
            async_host.local_timeouts,
            vec![DaemonRpcTimeouts::connect_only(Duration::from_secs(15))]
        );
        assert_eq!(async_host.sent.len(), 1);
        assert_eq!(
            async_host.sent_timeouts,
            vec![DaemonRpcTimeouts::connect_only(Duration::from_secs(15))]
        );
        assert_eq!(async_host.sent[0].0.node_id, bridge);
        assert_eq!(async_host.sent[0].1.target_node_id, target);
        assert_eq!(async_host.sent[0].1.ttl, CLUSTER_RELAY_DEFAULT_TTL - 1);
        assert_eq!(async_host.sent[0].1.visited, vec![local]);
    }
}
