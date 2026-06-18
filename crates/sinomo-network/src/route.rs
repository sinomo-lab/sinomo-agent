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
use crate::storage::{DAEMON_SHADOW_SUPERVISE_CAPABILITY, locally_learned_bridge_route_endpoint};
use crate::{
    ClusterConnectivityGraph, ClusterEndpointSelectionPurpose, ClusterEndpointSelector,
    ClusterPaths, DaemonControlRequest, DaemonRpcTimeouts, NetworkError, ObjectRpcRequest,
    ObjectRpcResponse,
};
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

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
const CLUSTER_RELAY_ROUTE_PLAN_CACHE_MAX_AGE: Duration = Duration::from_secs(1);

static CLUSTER_RELAY_ROUTE_PLANS: LazyLock<
    Mutex<HashMap<ClusterRelayRoutePlanCacheKey, ClusterRelayRoutePlanCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ClusterRelayRoutePlanMode {
    Standard,
    DirectTargetFallback,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ClusterRelayRoutePlanCacheKey {
    root: PathBuf,
    mode: ClusterRelayRoutePlanMode,
    local_node_id: String,
    target_node_id: String,
    excluded: Vec<String>,
}

#[derive(Clone, Debug)]
struct ClusterRelayRoutePlanCacheEntry {
    recorded_at: Instant,
    plan: Vec<(NodeId, Vec<ClusterEndpointRecord>)>,
}

/// A discovered route to a target node: the immediate next hop to send to, and the full
/// node path (`local` … `target`, inclusive) for diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClusterRoute {
    pub next_hop: NodeId,
    pub path: Vec<NodeId>,
}

/// Breadth-first search the connectivity graph for a shortest path from `local` to
/// `target` over reachable edges (records whose state is not `Unreachable`), returning the
/// first hop along it. Returns `None` when `target == local` or no reachable path exists.
pub(crate) fn cluster_route_next_hop(
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
#[cfg(test)]
fn cluster_route_first_hop_best_effort(
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
pub(crate) fn cluster_route_next_hops_best_effort(
    graph: &ClusterConnectivityGraph,
    local: &NodeId,
    target: &NodeId,
    excluded: &[NodeId],
) -> Vec<NodeId> {
    let full_path_next_hops =
        cluster_route_shortest_first_hops_excluding(graph, local, target, excluded);
    if !full_path_next_hops.is_empty() {
        return full_path_next_hops;
    }
    let excluded = excluded.iter().collect::<BTreeSet<_>>();
    let mut candidates = graph
        .healthy_target_refs(local)
        .filter(|candidate| {
            *candidate != target && *candidate != local && !excluded.contains(candidate)
        })
        .cloned()
        .collect::<Vec<_>>();
    retain_best_effort_candidates_with_onward_evidence(graph, local, &excluded, &mut candidates);
    candidates.sort_by(|left, right| {
        best_effort_candidate_rank(graph, local, target, &excluded, left)
            .cmp(&best_effort_candidate_rank(
                graph, local, target, &excluded, right,
            ))
            .then_with(|| left.cmp(right))
    });
    candidates
}

fn cluster_route_shortest_first_hops_excluding(
    graph: &ClusterConnectivityGraph,
    local: &NodeId,
    target: &NodeId,
    excluded: &[NodeId],
) -> Vec<NodeId> {
    if local == target {
        return Vec::new();
    }
    let excluded_set = excluded.iter().collect::<BTreeSet<_>>();
    let mut excluded_after_first_hop = excluded.to_vec();
    excluded_after_first_hop.push(local.clone());
    let mut candidates = graph
        .healthy_target_refs(local)
        .filter(|candidate| *candidate != local && !excluded_set.contains(candidate))
        .filter_map(|candidate| {
            if candidate == target {
                return Some((1_usize, candidate.clone()));
            }
            let route = cluster_route_next_hop_excluding(
                graph,
                candidate,
                target,
                &excluded_after_first_hop,
            )?;
            Some((route.path.len(), candidate.clone()))
        })
        .collect::<Vec<_>>();
    let Some(shortest) = candidates.iter().map(|(distance, _)| *distance).min() else {
        return Vec::new();
    };
    candidates.retain(|(distance, _)| *distance == shortest);
    candidates.sort_by(|(_, left), (_, right)| {
        best_effort_candidate_rank(graph, local, target, &excluded_set, left)
            .cmp(&best_effort_candidate_rank(
                graph,
                local,
                target,
                &excluded_set,
                right,
            ))
            .then_with(|| left.cmp(right))
    });
    candidates
        .into_iter()
        .map(|(_, candidate)| candidate)
        .collect()
}

fn retain_best_effort_candidates_with_onward_evidence(
    graph: &ClusterConnectivityGraph,
    local: &NodeId,
    excluded: &BTreeSet<&NodeId>,
    candidates: &mut Vec<NodeId>,
) {
    let has_onward_evidence = candidates.iter().any(|candidate| {
        best_effort_candidate_forward_frontier(graph, local, excluded, candidate) > 0
    });
    if has_onward_evidence {
        candidates.retain(|candidate| {
            best_effort_candidate_forward_frontier(graph, local, excluded, candidate) > 0
        });
    }
}

fn best_effort_candidate_rank(
    graph: &ClusterConnectivityGraph,
    local: &NodeId,
    target: &NodeId,
    excluded: &BTreeSet<&NodeId>,
    candidate: &NodeId,
) -> (u8, usize) {
    let mut target_hint = 1;
    for next in graph.healthy_target_refs(candidate) {
        if next == target {
            target_hint = 0;
        }
    }
    let forward_frontier =
        best_effort_candidate_forward_frontier(graph, local, excluded, candidate);
    (target_hint, usize::MAX - forward_frontier)
}

fn best_effort_candidate_forward_frontier(
    graph: &ClusterConnectivityGraph,
    local: &NodeId,
    excluded: &BTreeSet<&NodeId>,
    candidate: &NodeId,
) -> usize {
    graph
        .healthy_target_refs(candidate)
        .filter(|next| *next != local && *next != candidate && !excluded.contains(*next))
        .count()
}

/// An inner cluster RPC wrapped for hop-by-hop delivery toward `target_node_id`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RelayEnvelope {
    pub cluster_id: ClusterId,
    pub target_node_id: NodeId,
    pub ttl: u8,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub visited: Vec<NodeId>,
    /// Maximum per-hop transport timeout, in milliseconds, inherited from the
    /// originating caller. Intermediate relays must not expand this budget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_hop_timeout_ms: Option<u64>,
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
}

/// What a node should do with a [`RelayEnvelope`] it received.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(test)]
enum RelayDecision {
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
#[cfg(test)]
fn relay_decision(
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

pub trait ClusterRelayStreamForwardAsyncHost {
    fn send_relay_stream_forward_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ObjectRpcRequest,
        timeouts: DaemonRpcTimeouts,
    ) -> ClusterRelayFuture<'a, ClusterRelayStreamSendOutcome>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ClusterSignedRelayRpcOptions {
    pub timeouts: DaemonRpcTimeouts,
}

pub(crate) type ClusterSignedRelayRpcFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NetworkError>> + Send + 'a>>;

pub(crate) trait ClusterSignedRelayRpcAsyncHost: ClusterSignedEndpointRpcAsyncHost {
    fn sign_relay_rpc_request_async<'a>(
        &'a mut self,
        request: &'a ObjectRpcRequest,
    ) -> ClusterSignedRelayRpcFuture<'a, Vec<u8>> {
        self.sign_endpoint_rpc_request_async(request)
    }

    fn send_signed_relay_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        auth: &'a [u8],
        request: &'a ObjectRpcRequest,
        timeouts: DaemonRpcTimeouts,
    ) -> ClusterSignedRelayRpcFuture<'a, ObjectRpcResponse> {
        self.send_signed_endpoint_rpc_async(endpoint, auth, request, timeouts)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ClusterSignedEndpointRpcOptions {
    pub timeouts: DaemonRpcTimeouts,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterObjectRpcOptions {
    pub timeouts: DaemonRpcTimeouts,
}

impl ClusterObjectRpcOptions {
    pub fn new(timeouts: DaemonRpcTimeouts) -> Self {
        Self { timeouts }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterObjectRpcNodeOptions {
    pub endpoint_selection: ClusterEndpointSelectionPurpose,
    pub timeouts: DaemonRpcTimeouts,
}

impl ClusterObjectRpcNodeOptions {
    pub fn new(
        endpoint_selection: ClusterEndpointSelectionPurpose,
        timeouts: DaemonRpcTimeouts,
    ) -> Self {
        Self {
            endpoint_selection,
            timeouts,
        }
    }
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
        _endpoint: &'a ClusterEndpointRecord,
        _request: &'a ObjectRpcRequest,
        _request_auth: &'a [u8],
        _timeouts: DaemonRpcTimeouts,
    ) -> ClusterSignedEndpointRpcFuture<'a, Option<ObjectRpcResponse>> {
        Box::pin(async { Ok(None) })
    }

    fn endpoint_rpc_cluster_id_async<'a>(
        &'a mut self,
        _endpoint: &'a ClusterEndpointRecord,
    ) -> ClusterSignedEndpointRpcFuture<'a, Option<ClusterId>> {
        Box::pin(async { Ok(None) })
    }

    fn send_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        auth: &'a [u8],
        request: &'a ObjectRpcRequest,
        timeouts: DaemonRpcTimeouts,
    ) -> ClusterSignedEndpointRpcFuture<'a, ObjectRpcResponse> {
        Box::pin(async move {
            crate::roaming::send_daemon_rpc_to_cluster_endpoint_async(
                endpoint,
                Some(auth),
                request,
                timeouts,
            )
            .await
        })
    }

    fn relay_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        _endpoint: &'a ClusterEndpointRecord,
        _request: &'a ObjectRpcRequest,
        _request_auth: &'a [u8],
        _timeouts: DaemonRpcTimeouts,
    ) -> ClusterSignedEndpointRpcFuture<'a, Option<ObjectRpcResponse>> {
        Box::pin(async { Ok(None) })
    }
}

impl<H> ClusterSignedRelayRpcAsyncHost for H where H: ClusterSignedEndpointRpcAsyncHost {}

struct ClusterObjectRpcPathRelayHost<'a, H> {
    runtime: tokio::runtime::Handle,
    paths: ClusterPaths,
    inner: &'a mut H,
}

impl<H> ClusterSignedEndpointRpcAsyncHost for ClusterObjectRpcPathRelayHost<'_, H>
where
    H: ClusterSignedEndpointRpcAsyncHost + Send,
{
    fn sign_endpoint_rpc_request_async<'a>(
        &'a mut self,
        request: &'a ObjectRpcRequest,
    ) -> ClusterSignedEndpointRpcFuture<'a, Vec<u8>> {
        self.inner.sign_endpoint_rpc_request_async(request)
    }

    fn try_gateway_signed_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ObjectRpcRequest,
        request_auth: &'a [u8],
        timeouts: DaemonRpcTimeouts,
    ) -> ClusterSignedEndpointRpcFuture<'a, Option<ObjectRpcResponse>> {
        self.inner
            .try_gateway_signed_rpc_async(endpoint, request, request_auth, timeouts)
    }

    fn endpoint_rpc_cluster_id_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
    ) -> ClusterSignedEndpointRpcFuture<'a, Option<ClusterId>> {
        self.inner.endpoint_rpc_cluster_id_async(endpoint)
    }

    fn send_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        auth: &'a [u8],
        request: &'a ObjectRpcRequest,
        timeouts: DaemonRpcTimeouts,
    ) -> ClusterSignedEndpointRpcFuture<'a, ObjectRpcResponse> {
        self.inner
            .send_signed_endpoint_rpc_async(endpoint, auth, request, timeouts)
    }

    fn relay_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ObjectRpcRequest,
        request_auth: &'a [u8],
        timeouts: DaemonRpcTimeouts,
    ) -> ClusterSignedEndpointRpcFuture<'a, Option<ObjectRpcResponse>> {
        Box::pin(async move {
            relay_rpc_to_endpoint_from_paths_async_with_host(
                &self.runtime,
                &self.paths,
                endpoint,
                request,
                request_auth.to_vec(),
                ClusterSignedRelayRpcOptions { timeouts },
                self.inner,
            )
            .await
        })
    }
}

/// Canonical cluster object-RPC client facade.
///
/// Callers provide a typed object-RPC request and a target endpoint. The client
/// owns request signing and the direct-vs-relay routing policy. Lower-level
/// signed endpoint helpers remain for transport adapters and migration shims,
/// but ordinary control-plane callers should enter through this facade.
pub struct ClusterObjectRpcClient<'a, H> {
    host: &'a mut H,
}

impl<'a, H: ClusterSignedEndpointRpcAsyncHost> ClusterObjectRpcClient<'a, H> {
    pub fn new(host: &'a mut H) -> Self {
        Self { host }
    }

    pub async fn call_endpoint(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        options: ClusterObjectRpcOptions,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        self.call_endpoint_with_relay_preference(endpoint, request, options, false)
            .await
    }

    async fn call_endpoint_with_relay_preference(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        options: ClusterObjectRpcOptions,
        prefer_relay: bool,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        let request_auth = self.host.sign_endpoint_rpc_request_async(request).await?;
        send_signed_rpc_to_endpoint_async_with_route_policy(
            endpoint,
            request,
            &request_auth,
            ClusterSignedEndpointRpcOptions {
                timeouts: options.timeouts,
            },
            prefer_relay,
            self.host,
        )
        .await
    }
}

impl<'a, H> ClusterObjectRpcClient<'a, H>
where
    H: ClusterSignedEndpointRpcAsyncHost + Send,
{
    pub async fn call_endpoint_from_paths(
        &mut self,
        runtime: &tokio::runtime::Handle,
        paths: &ClusterPaths,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        options: ClusterObjectRpcOptions,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        let prefer_relay =
            cluster_endpoint_prefers_relay_from_paths_async(runtime, paths, endpoint).await?;
        let mut relay_host = ClusterObjectRpcPathRelayHost {
            runtime: runtime.clone(),
            paths: paths.clone(),
            inner: self.host,
        };
        ClusterObjectRpcClient::new(&mut relay_host)
            .call_endpoint_with_relay_preference(endpoint, request, options, prefer_relay)
            .await
    }

    pub async fn call_node_from_paths(
        &mut self,
        runtime: &tokio::runtime::Handle,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        options: ClusterObjectRpcNodeOptions,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        let endpoints = cluster_object_rpc_node_targets(
            runtime,
            paths,
            state,
            target_node_id,
            options.endpoint_selection,
        )
        .await?;
        if endpoints.is_empty() {
            return Err(NetworkError::Io {
                operation: "cluster node rpc target selection".to_string(),
                detail: format!("no endpoint is available for target node {target_node_id}"),
            });
        }

        let mut failures = Vec::new();
        for endpoint in endpoints {
            match self
                .call_endpoint_from_paths(
                    runtime,
                    paths,
                    &endpoint,
                    request,
                    ClusterObjectRpcOptions::new(options.timeouts),
                )
                .await
            {
                Ok(response) => return Ok(response),
                Err(error) => failures.push(format!(
                    "{} ({:?}): {error}",
                    endpoint.address, endpoint.transport
                )),
            }
        }

        Err(NetworkError::Io {
            operation: "cluster node rpc".to_string(),
            detail: format!(
                "all endpoints for target node {target_node_id} failed{}",
                if failures.is_empty() {
                    String::new()
                } else {
                    format!(": {}", failures.join("; "))
                }
            ),
        })
    }
}

async fn cluster_object_rpc_node_targets(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
    endpoint_selection: ClusterEndpointSelectionPurpose,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    let target_node_id = target_node_id.clone();
    runtime
        .spawn_blocking(move || {
            ClusterEndpointSelector::new(&paths, &state)
                .select_node(endpoint_selection, &target_node_id)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster node rpc target selection".to_string(),
            detail: format!("target selection task failed: {source}"),
        })
}

async fn cluster_endpoint_prefers_relay_from_paths_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    endpoint: &ClusterEndpointRecord,
) -> Result<bool, NetworkError> {
    let paths = paths.clone();
    let endpoint = endpoint.clone();
    runtime
        .spawn_blocking(move || cluster_endpoint_prefers_relay(&paths, &endpoint))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster endpoint relay preference".to_string(),
            detail: format!("relay preference task failed: {source}"),
        })
}

async fn send_targeted_direct_signed_endpoint_rpc_async(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    request_auth: &[u8],
    timeouts: DaemonRpcTimeouts,
    host: &mut impl ClusterSignedEndpointRpcAsyncHost,
) -> Result<ObjectRpcResponse, NetworkError> {
    if !endpoint_rpc_should_use_targeted_direct(request) {
        return host
            .send_signed_endpoint_rpc_async(endpoint, request_auth, request, timeouts)
            .await;
    }
    let Some(cluster_id) = host.endpoint_rpc_cluster_id_async(endpoint).await? else {
        return host
            .send_signed_endpoint_rpc_async(endpoint, request_auth, request, timeouts)
            .await;
    };
    let outer = relay_request_for_signed_rpc(
        &cluster_id,
        &endpoint.node_id,
        request,
        request_auth.to_vec(),
        std::iter::empty::<NodeId>(),
    )?;
    let outer_auth = host.sign_endpoint_rpc_request_async(&outer).await?;
    host.send_signed_endpoint_rpc_async(endpoint, &outer_auth, &outer, timeouts)
        .await
}

fn endpoint_rpc_should_use_targeted_direct(request: &ObjectRpcRequest) -> bool {
    !endpoint_rpc_requests_raw_direct(request)
}

fn endpoint_rpc_requests_raw_direct(request: &ObjectRpcRequest) -> bool {
    request.route == CLUSTER_RELAY_ROUTE
        || request.route == crate::CLUSTER_NETWORK_PING_ROUTE
        || request.category == crate::CLUSTER_MEMBERSHIP_RPC_CATEGORY
        || endpoint_rpc_has_explicit_raw_direct_metadata(request)
        || crate::object_rpc_request_expects_raw_daemon_response(request)
        || crate::object_rpc_request_accepts_streamed_daemon_response(request)
}

fn endpoint_rpc_has_explicit_raw_direct_metadata(request: &ObjectRpcRequest) -> bool {
    request
        .metadata
        .get(crate::ENDPOINT_RPC_RAW_DIRECT_METADATA_KEY)
        .is_some_and(|value| {
            value == crate::ENDPOINT_RPC_RAW_DIRECT_METADATA_VALUE
                || value.eq_ignore_ascii_case("true")
        })
}

fn propagate_relay_response_metadata(inner: &ObjectRpcRequest, outer: &mut ObjectRpcRequest) {
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
pub(crate) fn relay_request_for_signed_rpc(
    cluster_id: &ClusterId,
    target_node_id: &NodeId,
    request: &ObjectRpcRequest,
    request_auth: impl Into<Vec<u8>>,
    visited: impl IntoIterator<Item = NodeId>,
) -> Result<ObjectRpcRequest, NetworkError> {
    relay_request_for_signed_rpc_with_max_hop_timeout(
        cluster_id,
        target_node_id,
        request,
        request_auth,
        visited,
        None,
    )
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

async fn send_signed_rpc_to_endpoint_async_with_route_policy(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    request_auth: &[u8],
    options: ClusterSignedEndpointRpcOptions,
    prefers_relay: bool,
    host: &mut impl ClusterSignedEndpointRpcAsyncHost,
) -> Result<ObjectRpcResponse, NetworkError> {
    let deadline = ClusterEndpointRpcDeadline::new(options.timeouts.connect_timeout);
    let requests_raw_direct = endpoint_rpc_requests_raw_direct(request);
    let direct_attempt_override = endpoint_rpc_has_explicit_raw_direct_metadata(request);
    if !prefers_relay
        && !requests_raw_direct
        && let Some(remaining) = deadline.checked_remaining()
        && let Some(response) = host
            .try_gateway_signed_rpc_async(
                endpoint,
                request,
                request_auth,
                cluster_endpoint_direct_attempt_timeouts(options.timeouts, remaining),
            )
            .await?
    {
        return Ok(response);
    }

    if !prefers_relay || direct_attempt_override {
        let Some(remaining) = deadline.checked_remaining() else {
            return Err(deadline.timeout_error(endpoint));
        };
        match send_targeted_direct_signed_endpoint_rpc_async(
            endpoint,
            request,
            request_auth,
            cluster_endpoint_direct_attempt_timeouts(options.timeouts, remaining),
            host,
        )
        .await
        {
            Ok(response) => return Ok(response),
            Err(direct_error) => {
                // A doomed cross-segment direct probe can consume the caller's small
                // connect budget. Still give relay a minimum attempt before surfacing
                // the direct failure; the relay helper expands this policy as needed.
                let remaining = deadline
                    .checked_remaining()
                    .unwrap_or(Some(CLUSTER_RELAY_MIN_BUDGET));
                return match host
                    .relay_signed_endpoint_rpc_async(
                        endpoint,
                        request,
                        request_auth,
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
    match host
        .relay_signed_endpoint_rpc_async(
            endpoint,
            request,
            request_auth,
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

pub(crate) async fn relay_rpc_to_endpoint_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    request_auth: impl Into<Vec<u8>>,
    options: ClusterSignedRelayRpcOptions,
    host: &mut impl ClusterSignedRelayRpcAsyncHost,
) -> Result<Option<ObjectRpcResponse>, NetworkError> {
    if request.route == CLUSTER_RELAY_ROUTE {
        return Ok(None);
    }
    // Keep the inner target-auth stable across alternate relay paths. The
    // outer relay request is signed per hop below; re-signing the inner RPC on
    // route fallback would turn a lost response into multiple valid target
    // deliveries instead of letting target-side replay protection suppress it.
    let request_auth = request_auth.into();
    let signed_request_for_rotation = request.clone().with_auth(request_auth.clone());
    let rotation_seed =
        relay_inner_next_hop_rotation_seed(&endpoint.node_id, &signed_request_for_rotation);
    let mut route_plan = relay_route_plan(runtime, paths, state, endpoint).await?;
    if route_plan.is_empty() {
        return Ok(None);
    }
    let rotation = rotation_seed % route_plan.len();
    route_plan.rotate_left(rotation);

    // A relay spans several hop attempts plus the remote leg; callers tuned
    // for fast direct probes (sub-second connect timeouts) must not starve
    // the whole relay after a single dead endpoint.
    let relay_budget = options
        .timeouts
        .connect_timeout
        .map(|timeout| timeout.max(CLUSTER_RELAY_MIN_RESPONSE_BUDGET));
    let deadline = ClusterEndpointRpcDeadline::new(relay_budget);
    let mut failures = Vec::new();
    'relay_hops: for (next_hop_id, endpoints) in route_plan {
        if endpoints.is_empty() {
            push_cluster_relay_failure(
                &mut failures,
                format!("{next_hop_id}: relay next hop has no reachable endpoint"),
            );
            continue;
        }
        for next_hop in endpoints {
            let Some(remaining) = deadline.checked_remaining() else {
                let timeout = options.timeouts.connect_timeout.unwrap_or_default();
                push_cluster_relay_failure(
                    &mut failures,
                    format!("relay budget expired after {:?}", timeout),
                );
                break 'relay_hops;
            };
            let outer = relay_request_for_signed_rpc_with_max_hop_timeout(
                &state.cluster_id,
                &endpoint.node_id,
                request,
                request_auth.clone(),
                std::iter::once(state.local_node_id.clone()),
                options.timeouts.connect_timeout,
            )?;
            let outer_auth = host.sign_relay_rpc_request_async(&outer).await?;
            let hop_timeouts = cluster_relay_hop_transport_timeouts(options.timeouts, remaining);
            match host
                .send_signed_relay_rpc_async(&next_hop, &outer_auth, &outer, hop_timeouts)
                .await
            {
                Ok(response) if relay_response_is_routing_error(&response) => {
                    push_cluster_relay_failure(
                        &mut failures,
                        format!(
                            "{} ({:?}) returned routing error",
                            next_hop.address, next_hop.transport
                        ),
                    );
                }
                Ok(response) => return Ok(Some(response)),
                Err(error) => push_cluster_relay_failure(
                    &mut failures,
                    format!(
                        "{} ({:?}) failed: {}",
                        next_hop.address, next_hop.transport, error
                    ),
                ),
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

async fn relay_route_plan(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
) -> Result<Vec<(NodeId, Vec<ClusterEndpointRecord>)>, NetworkError> {
    relay_route_plan_cached(
        runtime,
        paths,
        state,
        &state.local_node_id,
        &endpoint.node_id,
        vec![endpoint.node_id.clone()],
        ClusterRelayRoutePlanMode::Standard,
    )
    .await
}

async fn relay_route_plan_cached(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    excluded: Vec<NodeId>,
    mode: ClusterRelayRoutePlanMode,
) -> Result<Vec<(NodeId, Vec<ClusterEndpointRecord>)>, NetworkError> {
    let key =
        cluster_relay_route_plan_cache_key(paths, mode, local_node_id, target_node_id, &excluded);
    if let Some(plan) = read_cached_cluster_relay_route_plan(&key) {
        return Ok(plan);
    }

    let paths = paths.clone();
    let state = state.clone();
    let local_node_id = local_node_id.clone();
    let target_node_id = target_node_id.clone();
    let excluded_for_compute = excluded.clone();
    let plan = runtime
        .spawn_blocking(move || {
            let graph = ClusterConnectivityGraph::read_for_local(&paths, &state);
            let next_hops = match mode {
                ClusterRelayRoutePlanMode::Standard => cluster_relay_next_hops(
                    &graph,
                    &state,
                    &local_node_id,
                    &target_node_id,
                    &excluded_for_compute,
                ),
                ClusterRelayRoutePlanMode::DirectTargetFallback => {
                    cluster_relay_next_hops_with_direct_target_fallback(
                        &paths,
                        &graph,
                        &state,
                        &local_node_id,
                        &target_node_id,
                        &excluded_for_compute,
                    )
                }
            };
            if std::env::var_os("SINOMO_TRACE_CLUSTER_RELAY").is_some() {
                eprintln!(
                    "sinomo relay route plan: local={} target={} healthy={:?} excluded={:?} next_hops={:?}",
                    local_node_id,
                    target_node_id,
                    graph.healthy_targets(&local_node_id),
                    excluded_for_compute,
                    next_hops,
                );
            }
            Ok::<Vec<(NodeId, Vec<ClusterEndpointRecord>)>, NetworkError>(next_hops
                .into_iter()
                .filter_map(|next_hop_id| {
                    let allow_unknown_locally_learned_endpoint =
                        mode == ClusterRelayRoutePlanMode::DirectTargetFallback
                            && next_hop_id == target_node_id;
                    let endpoints = cluster_relay_next_hop_endpoints_with_graph_and_policy(
                        &paths,
                        &graph,
                        &state,
                        &local_node_id,
                        &next_hop_id,
                        allow_unknown_locally_learned_endpoint,
                    );
                    (!endpoints.is_empty()).then_some((next_hop_id, endpoints))
                })
                .collect::<Vec<_>>())
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster relay route plan".to_string(),
            detail: format!("relay route planning task failed: {source}"),
        })??;
    record_cached_cluster_relay_route_plan(key, plan.clone());
    Ok(plan)
}

fn cluster_relay_route_plan_cache_key(
    paths: &ClusterPaths,
    mode: ClusterRelayRoutePlanMode,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    excluded: &[NodeId],
) -> ClusterRelayRoutePlanCacheKey {
    ClusterRelayRoutePlanCacheKey {
        root: paths.root.clone(),
        mode,
        local_node_id: local_node_id.to_string(),
        target_node_id: target_node_id.to_string(),
        excluded: excluded.iter().map(ToString::to_string).collect(),
    }
}

fn read_cached_cluster_relay_route_plan(
    key: &ClusterRelayRoutePlanCacheKey,
) -> Option<Vec<(NodeId, Vec<ClusterEndpointRecord>)>> {
    let now = Instant::now();
    let mut cache = CLUSTER_RELAY_ROUTE_PLANS.lock().ok()?;
    cache.retain(|_, entry| {
        now.saturating_duration_since(entry.recorded_at) <= CLUSTER_RELAY_ROUTE_PLAN_CACHE_MAX_AGE
    });
    cache.get(key).map(|entry| entry.plan.clone())
}

fn record_cached_cluster_relay_route_plan(
    key: ClusterRelayRoutePlanCacheKey,
    plan: Vec<(NodeId, Vec<ClusterEndpointRecord>)>,
) {
    let Ok(mut cache) = CLUSTER_RELAY_ROUTE_PLANS.lock() else {
        return;
    };
    let now = Instant::now();
    cache.retain(|_, entry| {
        now.saturating_duration_since(entry.recorded_at) <= CLUSTER_RELAY_ROUTE_PLAN_CACHE_MAX_AGE
    });
    cache.insert(
        key,
        ClusterRelayRoutePlanCacheEntry {
            recorded_at: now,
            plan,
        },
    );
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
    if crate::cluster_node_is_local_process(state, &envelope.target_node_id) {
        if let Some(response) = host
            .try_deliver_local_async(state, &envelope, options.timeouts)
            .await?
        {
            return Ok(response);
        }
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
            push_cluster_relay_failure(
                &mut failures,
                format!("{next_hop}: relay next hop has no reachable endpoint"),
            );
            continue;
        }
        for endpoint in endpoints {
            let mut forwarded = envelope.clone();
            forwarded.ttl = envelope.ttl.saturating_sub(1);
            forwarded.visited.push(local_node_id.clone());
            let outer = forwarded.into_request()?;
            let hop_timeouts = relay_envelope_hop_transport_timeouts(options.timeouts, &envelope);
            match host
                .send_relay_request_async(&endpoint, &outer, hop_timeouts)
                .await
            {
                Ok(response) if relay_response_is_routing_error(&response) => {
                    push_cluster_relay_failure(
                        &mut failures,
                        format!(
                            "{} ({:?}) returned routing error",
                            endpoint.address, endpoint.transport
                        ),
                    );
                }
                Ok(response) => return Ok(response),
                Err(error) => push_cluster_relay_failure(
                    &mut failures,
                    format!(
                        "{} ({:?}) failed: {}",
                        endpoint.address, endpoint.transport, error
                    ),
                ),
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

pub async fn handle_cluster_relay_stream_forward_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    envelope: RelayEnvelope,
    options: ClusterRelayOptions,
    host: &mut impl ClusterRelayStreamForwardAsyncHost,
) -> Result<ClusterRelayStreamForwardOutcome, NetworkError> {
    if crate::cluster_node_is_local_process(state, &envelope.target_node_id) {
        return ObjectRpcResponse::encode_error(
            CLUSTER_RELAY_ROUTE,
            "stream relay local delivery must be handled by the daemon control loop",
        )
        .map(ClusterRelayStreamForwardOutcome::ErrorResponse);
    }

    if envelope.visited.iter().any(|node| node == local_node_id) {
        return ObjectRpcResponse::encode_error(
            CLUSTER_RELAY_ROUTE,
            format!("relay dropped: relay loop: {local_node_id} already on the path"),
        )
        .map(ClusterRelayStreamForwardOutcome::ErrorResponse);
    }
    if envelope.ttl == 0 {
        return ObjectRpcResponse::encode_error(
            CLUSTER_RELAY_ROUTE,
            "relay dropped: relay ttl exhausted",
        )
        .map(ClusterRelayStreamForwardOutcome::ErrorResponse);
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
        )
        .map(ClusterRelayStreamForwardOutcome::ErrorResponse);
    }

    let mut failures = Vec::new();
    for (next_hop, endpoints) in next_hops {
        if endpoints.is_empty() {
            push_cluster_relay_failure(
                &mut failures,
                format!("{next_hop}: relay next hop has no reachable endpoint"),
            );
            continue;
        }
        for endpoint in endpoints {
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
                    return Ok(ClusterRelayStreamForwardOutcome::Forwarded);
                }
                Ok(ClusterRelayStreamSendOutcome::RoutingError(response)) => {
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
                }
                Ok(ClusterRelayStreamSendOutcome::StreamStartedThenFailed(error)) => {
                    return Ok(ClusterRelayStreamForwardOutcome::StreamStartedThenFailed(
                        error,
                    ));
                }
                Err(error) => push_cluster_relay_failure(
                    &mut failures,
                    format!(
                        "{} ({:?}) failed: {}",
                        endpoint.address, endpoint.transport, error
                    ),
                ),
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
    .map(ClusterRelayStreamForwardOutcome::ErrorResponse)
}

pub(crate) async fn cluster_relay_forward_plan(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    envelope: &RelayEnvelope,
) -> Result<Vec<(NodeId, Vec<ClusterEndpointRecord>)>, NetworkError> {
    let rotation_seed = relay_envelope_next_hop_rotation_seed(envelope);
    let mut plan = relay_route_plan_cached(
        runtime,
        paths,
        state,
        local_node_id,
        &envelope.target_node_id,
        envelope.visited.clone(),
        ClusterRelayRoutePlanMode::DirectTargetFallback,
    )
    .await?;
    if !plan.is_empty() {
        let rotation = rotation_seed % plan.len();
        plan.rotate_left(rotation);
    }
    Ok(plan)
}

fn relay_envelope_next_hop_rotation_seed(envelope: &RelayEnvelope) -> usize {
    relay_inner_next_hop_rotation_seed(&envelope.target_node_id, &envelope.inner)
}

fn relay_inner_next_hop_rotation_seed(target_node_id: &NodeId, rpc: &ObjectRpcRequest) -> usize {
    let mut hasher = blake3::Hasher::new();
    hasher.update(target_node_id.as_str().as_bytes());
    hasher.update(b"\n");
    hasher.update(rpc.route.as_bytes());
    hasher.update(b"\n");
    if let Some(auth) = rpc.auth.as_deref() {
        hasher.update(auth);
    }
    hasher.update(b"\n");
    hasher.update(&rpc.body);
    let digest = hasher.finalize();
    let mut bytes = [0_u8; std::mem::size_of::<usize>()];
    let digest_bytes = digest.as_bytes();
    let copy_len = bytes.len().min(digest_bytes.len());
    bytes[..copy_len].copy_from_slice(&digest_bytes[..copy_len]);
    usize::from_le_bytes(bytes)
}

pub fn relay_response_is_routing_error(response: &ObjectRpcResponse) -> bool {
    response.route == CLUSTER_RELAY_ROUTE && response.error_detail().is_err()
}

pub(crate) fn cluster_relay_next_hops(
    graph: &ClusterConnectivityGraph,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    excluded: &[NodeId],
) -> Vec<NodeId> {
    let mut next_hops =
        cluster_route_next_hops_best_effort(graph, local_node_id, target_node_id, excluded)
            .into_iter()
            .filter(|candidate| {
                relay_candidate_can_transit(graph, state, local_node_id, candidate, target_node_id)
            })
            .collect::<Vec<_>>();
    if next_hops.is_empty() {
        next_hops =
            cluster_relay_degraded_next_hops(graph, state, local_node_id, target_node_id, excluded);
    }
    if next_hops.is_empty() {
        next_hops = cluster_relay_member_bridge_next_hops(
            graph,
            state,
            local_node_id,
            target_node_id,
            excluded,
        );
    }
    retain_relay_candidates_with_best_member_bridge_evidence(state, &mut next_hops);
    next_hops
}

fn retain_relay_candidates_with_best_member_bridge_evidence(
    state: &ClusterBootstrapState,
    candidates: &mut Vec<NodeId>,
) {
    let Some(best_rank) = candidates
        .iter()
        .map(|candidate| relay_candidate_member_bridge_rank(state, candidate))
        .min()
    else {
        return;
    };
    if best_rank < RELAY_CANDIDATE_NO_BRIDGE_EVIDENCE_RANK {
        candidates
            .retain(|candidate| relay_candidate_member_bridge_rank(state, candidate) == best_rank);
    }
}

const RELAY_CANDIDATE_NO_BRIDGE_EVIDENCE_RANK: u8 = 3;

fn relay_candidate_member_bridge_rank(state: &ClusterBootstrapState, candidate: &NodeId) -> u8 {
    let Some(member) = state
        .members
        .iter()
        .find(|member| member.node_id == *candidate)
    else {
        return RELAY_CANDIDATE_NO_BRIDGE_EVIDENCE_RANK;
    };
    let distinct_non_local_hosts = member
        .endpoints
        .iter()
        .filter(|endpoint| endpoint.privacy != ClusterEndpointPrivacy::LocalOnly)
        .filter_map(|endpoint| {
            endpoint
                .address
                .rsplit_once(':')
                .map(|(host, _)| host)
                .or(Some(endpoint.address.as_str()))
        })
        .collect::<BTreeSet<_>>()
        .len();
    if distinct_non_local_hosts >= 3 {
        return 0;
    }
    RELAY_CANDIDATE_NO_BRIDGE_EVIDENCE_RANK
}

fn cluster_relay_degraded_next_hops(
    graph: &ClusterConnectivityGraph,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    excluded: &[NodeId],
) -> Vec<NodeId> {
    let excluded = excluded.iter().collect::<BTreeSet<_>>();
    let mut candidates = graph
        .reachable_targets(local_node_id)
        .into_iter()
        .filter(|candidate| {
            relay_candidate_can_transit(graph, state, local_node_id, candidate, target_node_id)
        })
        .filter(|candidate| {
            candidate != target_node_id
                && candidate != local_node_id
                && !excluded.contains(candidate)
        })
        .collect::<Vec<_>>();
    retain_best_effort_candidates_with_onward_evidence(
        graph,
        local_node_id,
        &excluded,
        &mut candidates,
    );
    candidates.sort_by(|left, right| {
        best_effort_candidate_rank(graph, local_node_id, target_node_id, &excluded, left)
            .cmp(&best_effort_candidate_rank(
                graph,
                local_node_id,
                target_node_id,
                &excluded,
                right,
            ))
            .then_with(|| left.cmp(right))
    });
    candidates
}

fn cluster_relay_member_bridge_next_hops(
    graph: &ClusterConnectivityGraph,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    excluded: &[NodeId],
) -> Vec<NodeId> {
    let excluded = excluded.iter().collect::<BTreeSet<_>>();
    let mut candidates = state
        .members
        .iter()
        .filter(|member| {
            member.node_id != *local_node_id
                && member.node_id != *target_node_id
                && !excluded.contains(&member.node_id)
                && member.state.is_authorized()
                && !relay_candidate_is_known_shadow_daemon(state, &member.node_id)
                && member
                    .capabilities
                    .iter()
                    .any(|capability| capability.as_str() == crate::DAEMON_MAIN_RUN_CAPABILITY)
                && relay_candidate_member_bridge_rank(state, &member.node_id)
                    < RELAY_CANDIDATE_NO_BRIDGE_EVIDENCE_RANK
                && relay_member_has_usable_bridge_endpoint(graph, local_node_id, member)
        })
        .map(|member| member.node_id.clone())
        .collect::<Vec<_>>();
    candidates.sort_by_key(|candidate| {
        (
            relay_candidate_member_bridge_rank(state, candidate),
            candidate.clone(),
        )
    });
    candidates
}

fn relay_member_has_usable_bridge_endpoint(
    graph: &ClusterConnectivityGraph,
    local_node_id: &NodeId,
    member: &ClusterMemberRecord,
) -> bool {
    let has_healthy_next_hop_endpoint = graph
        .best_healthy_endpoint_matching(
            local_node_id,
            &member.node_id,
            cluster_relay_endpoint_can_carry_main_daemon_rpc,
        )
        .is_some();
    let has_reachable_next_hop_endpoint = has_healthy_next_hop_endpoint
        || graph
            .best_reachable_endpoint_matching(
                local_node_id,
                &member.node_id,
                cluster_relay_endpoint_can_carry_main_daemon_rpc,
            )
            .is_some();
    let local_graph_has_reachable_targets =
        cluster_graph_has_reachable_main_daemon_route_targets(graph, local_node_id);
    member
        .endpoints
        .iter()
        .filter(|endpoint| locally_learned_bridge_route_endpoint(endpoint))
        .any(|endpoint| {
            cluster_relay_next_hop_endpoint_is_usable(
                graph,
                local_node_id,
                endpoint,
                has_healthy_next_hop_endpoint,
                has_reachable_next_hop_endpoint,
                local_graph_has_reachable_targets,
                false,
            )
        })
}

fn cluster_relay_endpoint_can_carry_main_daemon_rpc(endpoint: &ClusterEndpointRecord) -> bool {
    !crate::storage::cluster_endpoint_identifies_shadow_daemon_for_node(endpoint)
}

fn cluster_graph_has_reachable_main_daemon_route_targets(
    graph: &ClusterConnectivityGraph,
    source_node_id: &NodeId,
) -> bool {
    graph
        .reachable_targets(source_node_id)
        .iter()
        .any(|target| {
            graph
                .best_reachable_endpoint_matching(
                    source_node_id,
                    target,
                    cluster_relay_endpoint_can_carry_main_daemon_rpc,
                )
                .is_some()
        })
}

pub(crate) fn cluster_relay_next_hops_with_direct_target_fallback(
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
        && cluster_relay_can_try_direct_target_fallback(graph, state, local_node_id, target_node_id)
        && !cluster_relay_next_hop_endpoints_with_graph_and_policy(
            paths,
            graph,
            state,
            local_node_id,
            target_node_id,
            true,
        )
        .is_empty()
    {
        // Startup joins and dual-homed bridge nodes can race ahead of onward
        // connectivity publication. If the graph has not learned local edges,
        // has concrete target reachability, or the target has a locally learned
        // bridge hostname, try delivering directly instead of dropping the relay
        // on stale graph state. Endpoint selection still suppresses unproven
        // socket/IP fallbacks once the local graph has any reachable edge.
        next_hops.push(target_node_id.clone());
    }
    next_hops
}

fn relay_candidate_can_transit(
    graph: &ClusterConnectivityGraph,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    candidate: &NodeId,
    target_node_id: &NodeId,
) -> bool {
    if candidate == target_node_id {
        return true;
    }
    if relay_candidate_is_known_shadow_daemon(state, candidate) {
        return false;
    }
    if graph
        .best_healthy_endpoint_matching(
            local_node_id,
            candidate,
            cluster_relay_endpoint_can_carry_main_daemon_rpc,
        )
        .is_some()
    {
        return true;
    }
    state
        .members
        .iter()
        .find(|member| member.node_id == *candidate)
        .is_some_and(|member| {
            member.state.is_authorized()
                && member
                    .capabilities
                    .iter()
                    .any(|capability| capability.as_str() == crate::DAEMON_MAIN_RUN_CAPABILITY)
        })
}

fn relay_candidate_is_known_shadow_daemon(
    state: &ClusterBootstrapState,
    candidate: &NodeId,
) -> bool {
    state
        .members
        .iter()
        .find(|member| member.node_id == *candidate)
        .is_some_and(|member| {
            member
                .capabilities
                .iter()
                .any(|capability| capability.as_str() == DAEMON_SHADOW_SUPERVISE_CAPABILITY)
        })
}

fn cluster_relay_can_try_direct_target_fallback(
    graph: &ClusterConnectivityGraph,
    state: &ClusterBootstrapState,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
) -> bool {
    graph
        .best_reachable_endpoint_matching(
            local_node_id,
            target_node_id,
            cluster_relay_endpoint_can_carry_main_daemon_rpc,
        )
        .is_some()
        || !cluster_graph_has_reachable_main_daemon_route_targets(graph, local_node_id)
        || cluster_relay_target_has_locally_learned_endpoint(state, target_node_id)
}

fn cluster_relay_target_has_locally_learned_endpoint(
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
) -> bool {
    state
        .members
        .iter()
        .find(|member| member.node_id == *target_node_id && member.state.is_authorized())
        .is_some_and(|member| {
            member
                .endpoints
                .iter()
                .any(locally_learned_bridge_route_endpoint)
        })
}

#[cfg(test)]
pub(crate) fn cluster_relay_next_hop_endpoints_with_graph(
    paths: &ClusterPaths,
    graph: &ClusterConnectivityGraph,
    state: &ClusterBootstrapState,
    source_node_id: &NodeId,
    node_id: &NodeId,
) -> Vec<ClusterEndpointRecord> {
    cluster_relay_next_hop_endpoints_with_graph_and_policy(
        paths,
        graph,
        state,
        source_node_id,
        node_id,
        false,
    )
}

fn cluster_relay_next_hop_endpoints_with_graph_and_policy(
    paths: &ClusterPaths,
    graph: &ClusterConnectivityGraph,
    state: &ClusterBootstrapState,
    source_node_id: &NodeId,
    node_id: &NodeId,
    allow_unknown_locally_learned_endpoint: bool,
) -> Vec<ClusterEndpointRecord> {
    let mut endpoints = Vec::new();
    let mut seen = BTreeSet::new();
    let has_healthy_next_hop_endpoint = if let Some(endpoint) = graph
        .best_healthy_endpoint_matching(
            source_node_id,
            node_id,
            cluster_relay_endpoint_can_carry_main_daemon_rpc,
        ) {
        push_unique_cluster_endpoint(&mut endpoints, &mut seen, endpoint);
        true
    } else {
        false
    };
    let has_reachable_next_hop_endpoint = if has_healthy_next_hop_endpoint {
        true
    } else if let Some(endpoint) = graph.best_reachable_endpoint_matching(
        source_node_id,
        node_id,
        cluster_relay_endpoint_can_carry_main_daemon_rpc,
    ) {
        push_unique_cluster_endpoint(&mut endpoints, &mut seen, endpoint);
        true
    } else {
        false
    };
    let local_graph_has_reachable_targets =
        cluster_graph_has_reachable_main_daemon_route_targets(graph, source_node_id);
    if let Some(member) = state
        .members
        .iter()
        .find(|member| member.node_id == *node_id && member.state.is_authorized())
    {
        for endpoint in member
            .endpoints
            .iter()
            .filter(|endpoint| locally_learned_bridge_route_endpoint(endpoint))
            .filter(|endpoint| cluster_relay_endpoint_can_carry_main_daemon_rpc(endpoint))
            .filter(|endpoint| {
                cluster_relay_next_hop_endpoint_is_usable(
                    graph,
                    source_node_id,
                    *endpoint,
                    has_healthy_next_hop_endpoint,
                    has_reachable_next_hop_endpoint,
                    local_graph_has_reachable_targets,
                    allow_unknown_locally_learned_endpoint,
                )
            })
        {
            push_unique_cluster_endpoint(&mut endpoints, &mut seen, endpoint.clone());
        }
    }
    for endpoint in ClusterEndpointSelector::new(paths, state)
        .select_node(ClusterEndpointSelectionPurpose::RelayHop, node_id)
        .into_iter()
        .filter(|endpoint| {
            cluster_relay_next_hop_endpoint_is_usable(
                graph,
                source_node_id,
                endpoint,
                has_healthy_next_hop_endpoint,
                has_reachable_next_hop_endpoint,
                local_graph_has_reachable_targets,
                allow_unknown_locally_learned_endpoint,
            )
        })
    {
        push_unique_cluster_endpoint(&mut endpoints, &mut seen, endpoint);
    }
    endpoints
}

fn cluster_relay_next_hop_endpoint_is_usable(
    graph: &ClusterConnectivityGraph,
    source_node_id: &NodeId,
    endpoint: &ClusterEndpointRecord,
    has_healthy_next_hop_endpoint: bool,
    has_reachable_next_hop_endpoint: bool,
    local_graph_has_reachable_targets: bool,
    allow_unknown_locally_learned_endpoint: bool,
) -> bool {
    match graph.state_for_endpoint(endpoint) {
        Some(crate::ClusterConnectivityState::Healthy) => true,
        Some(crate::ClusterConnectivityState::Degraded) => !has_healthy_next_hop_endpoint,
        Some(crate::ClusterConnectivityState::Unreachable) => {
            !has_reachable_next_hop_endpoint
                && graph
                    .reachable_targets(source_node_id)
                    .contains(&endpoint.node_id)
        }
        None => {
            (!has_healthy_next_hop_endpoint && !local_graph_has_reachable_targets)
                || (allow_unknown_locally_learned_endpoint
                    && !has_reachable_next_hop_endpoint
                    && locally_learned_bridge_route_endpoint(endpoint))
        }
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

pub(crate) fn cluster_endpoint_prefers_relay_with_graph(
    graph: &ClusterConnectivityGraph,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
) -> bool {
    if endpoint.node_id == state.local_node_id {
        return false;
    }
    let unknown_hostname_lan = unknown_hostname_lan_endpoint_should_try_direct(endpoint);
    let has_complete_relay_route =
        cluster_route_next_hop(graph, &state.local_node_id, &endpoint.node_id).is_some();
    match graph.state_for_endpoint(endpoint) {
        Some(crate::ClusterConnectivityState::Healthy) => return false,
        Some(crate::ClusterConnectivityState::Degraded)
        | Some(crate::ClusterConnectivityState::Unreachable) => {
            return !unknown_hostname_lan || has_complete_relay_route;
        }
        None => {}
    }
    // Unknown Docker hostnames are cheap and correct when the nodes share a segment,
    // but can burn the caller's budget cross-segment. Skip direct only for a complete
    // healthy route; best-effort bridge handoff is still available after a bounded
    // direct attempt fails.
    if unknown_hostname_lan {
        return has_complete_relay_route;
    }
    true
}

fn unknown_hostname_lan_endpoint_should_try_direct(endpoint: &ClusterEndpointRecord) -> bool {
    matches!(
        endpoint.transport,
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback
    ) && endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
        && endpoint.address.contains(':')
        && !endpoint.address.contains("://")
        && endpoint.address.parse::<std::net::SocketAddr>().is_err()
}

/// Whether endpoint RPC delivery should skip direct dial and relay immediately,
/// using the persisted local cluster state and connectivity graph.
///
/// Returns `false` when the cluster state cannot be read. That preserves the
/// direct-first behavior for startup, unclustered clients, and damaged local
/// caches while still letting a healthy graph avoid doomed direct dials.
pub(crate) fn cluster_endpoint_prefers_relay(
    paths: &ClusterPaths,
    endpoint: &ClusterEndpointRecord,
) -> bool {
    let Ok(state) = crate::read_cluster_state_cached(paths) else {
        return false;
    };
    if endpoint.node_id == state.local_node_id {
        return false;
    }
    let graph = ClusterConnectivityGraph::read_for_local(paths, &state);
    cluster_endpoint_prefers_relay_with_graph(&graph, &state, endpoint)
}

pub(crate) async fn relay_rpc_to_endpoint_from_paths_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    request_auth: impl Into<Vec<u8>>,
    options: ClusterSignedRelayRpcOptions,
    host: &mut impl ClusterSignedRelayRpcAsyncHost,
) -> Result<Option<ObjectRpcResponse>, NetworkError> {
    if request.route == CLUSTER_RELAY_ROUTE {
        return Ok(None);
    }
    let state_paths = paths.clone();
    let state = match runtime
        .spawn_blocking(move || crate::read_cluster_state_cached(&state_paths))
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
        ObjectRpcRequest {
            route: "/cluster/kv/txn".to_string(),
            category: "cluster.kv".to_string(),
            metadata: BTreeMap::new(),
            auth: None,
            body: vec![1, 2, 3, 4],
        }
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
    fn endpoint_prefers_relay_for_known_unreachable_direct_endpoint_without_route() {
        let now = OffsetDateTime::now_utc();
        let local = nid("node_prefers_relay_unreachable_local");
        let target = nid("node_prefers_relay_unreachable_target");
        let mut state = ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_prefers_relay_unreachable").expect("cluster"),
            "prefers relay unreachable",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            target.clone(),
            "target",
            "pk-target",
            now,
        );
        member.capabilities = main_daemon_capabilities(&member.role);
        member.allowed_capabilities = member.capabilities.clone();
        state.members.push(member);
        let endpoint = crate::daemon_lan_endpoint(&target, "10.90.9.9:21353".to_string(), 5);
        let graph = ClusterConnectivityGraph::from_records(
            local,
            [ClusterConnectivityRecord {
                endpoint_id: endpoint.endpoint_id.clone(),
                target_node_id: endpoint.node_id.clone(),
                transport: endpoint.transport.clone(),
                address: endpoint.address.clone(),
                ..edge(
                    &state.local_node_id,
                    &target,
                    ClusterConnectivityState::Unreachable,
                )
            }],
        );

        assert!(
            cluster_endpoint_prefers_relay_with_graph(&graph, &state, &endpoint),
            "known-unreachable endpoints should skip stale direct dials even when no relay route is currently available"
        );
    }

    #[test]
    fn endpoint_prefers_relay_for_degraded_direct_endpoint_when_route_is_healthy() {
        let now = OffsetDateTime::now_utc();
        let local = nid("node_prefers_relay_degraded_local");
        let bridge = nid("node_prefers_relay_degraded_bridge");
        let target = nid("node_prefers_relay_degraded_target");
        let mut state = ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_prefers_relay_degraded").expect("cluster"),
            "prefers relay degraded",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        for node in [&bridge, &target] {
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
        let endpoint = crate::daemon_lan_endpoint(&target, "10.90.9.9:21353".to_string(), 5);
        let graph = ClusterConnectivityGraph::from_records(
            local.clone(),
            [
                ClusterConnectivityRecord {
                    endpoint_id: endpoint.endpoint_id.clone(),
                    target_node_id: endpoint.node_id.clone(),
                    transport: endpoint.transport.clone(),
                    address: endpoint.address.clone(),
                    consecutive_failures: 1,
                    last_failure_at: Some("timeout".to_string()),
                    ..edge(&local, &target, ClusterConnectivityState::Degraded)
                },
                edge(&local, &bridge, ClusterConnectivityState::Healthy),
                edge(&bridge, &target, ClusterConnectivityState::Healthy),
            ],
        );

        assert!(
            cluster_endpoint_prefers_relay_with_graph(&graph, &state, &endpoint),
            "degraded direct endpoints should relay immediately when a healthy route exists"
        );
    }

    #[test]
    fn endpoint_retries_unknown_hostname_direct_for_stale_unreachable_without_complete_route() {
        let now = OffsetDateTime::now_utc();
        let local = nid("node_prefers_stale_hostname_local");
        let target = nid("node_prefers_stale_hostname_target");
        let mut state = ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_prefers_stale_hostname").expect("cluster"),
            "prefers stale hostname",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            target.clone(),
            "target",
            "pk-target",
            now,
        );
        member.capabilities = main_daemon_capabilities(&member.role);
        member.allowed_capabilities = member.capabilities.clone();
        state.members.push(member);
        let endpoint = crate::daemon_lan_endpoint(&target, "node-target:21353".to_string(), 5);
        let graph = ClusterConnectivityGraph::from_records(
            local,
            [ClusterConnectivityRecord {
                endpoint_id: endpoint.endpoint_id.clone(),
                target_node_id: endpoint.node_id.clone(),
                transport: endpoint.transport.clone(),
                address: endpoint.address.clone(),
                ..edge(
                    &state.local_node_id,
                    &target,
                    ClusterConnectivityState::Unreachable,
                )
            }],
        );

        assert!(
            !cluster_endpoint_prefers_relay_with_graph(&graph, &state, &endpoint),
            "a stale unreachable Docker hostname should get a bounded direct retry when no complete relay route exists"
        );
    }

    #[test]
    fn endpoint_prefers_relay_over_unknown_hostname_lan_when_graph_has_route() {
        let now = OffsetDateTime::now_utc();
        let local = nid("node_prefers_hostname_direct_local");
        let target = nid("node_prefers_hostname_direct_target");
        let bridge = nid("node_prefers_hostname_direct_bridge");
        let mut state = ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_prefers_hostname_direct").expect("cluster"),
            "prefers hostname direct",
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
        let endpoint = crate::daemon_lan_endpoint(&target, "node-target:21353".to_string(), 5);
        let graph = ClusterConnectivityGraph::from_records(
            local.clone(),
            [
                edge(&local, &bridge, ClusterConnectivityState::Healthy),
                edge(&bridge, &target, ClusterConnectivityState::Healthy),
            ],
        );

        assert!(
            cluster_endpoint_prefers_relay_with_graph(&graph, &state, &endpoint),
            "segmented clusters should use a known relay route instead of burning the command budget on an unprobed Docker hostname"
        );
    }

    #[test]
    fn endpoint_tries_unknown_hostname_lan_direct_when_graph_has_no_route() {
        let now = OffsetDateTime::now_utc();
        let local = nid("node_prefers_hostname_startup_direct_local");
        let target = nid("node_prefers_hostname_startup_direct_target");
        let mut state = ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_prefers_hostname_startup_direct")
                .expect("cluster"),
            "prefers hostname startup direct",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            target.clone(),
            "target",
            "pk-target",
            now,
        );
        member.capabilities = main_daemon_capabilities(&member.role);
        member.allowed_capabilities = member.capabilities.clone();
        state.members.push(member);
        let endpoint = crate::daemon_lan_endpoint(&target, "node-target:21353".to_string(), 5);
        let graph = ClusterConnectivityGraph::from_records(local, []);

        assert!(
            !cluster_endpoint_prefers_relay_with_graph(&graph, &state, &endpoint),
            "startup with no graph route should still try an advertised Docker hostname directly"
        );
    }

    #[test]
    fn endpoint_tries_unknown_hostname_direct_when_only_best_effort_bridge_exists() {
        let now = OffsetDateTime::now_utc();
        let local = nid("node_prefers_hostname_bridge_fallback_local");
        let target = nid("node_prefers_hostname_bridge_fallback_target");
        let bridge = nid("node_prefers_hostname_bridge_fallback_bridge");
        let mut state = ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_prefers_hostname_bridge_fallback")
                .expect("cluster"),
            "prefers hostname bridge fallback",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        for (node, name, endpoints) in [
            (&target, "target", Vec::new()),
            (
                &bridge,
                "bridge",
                vec![
                    crate::daemon_lan_endpoint(&bridge, "node-bridge-a:21353".to_string(), 5),
                    crate::daemon_lan_endpoint(&bridge, "node-bridge-b:21353".to_string(), 5),
                    crate::daemon_lan_endpoint(&bridge, "node-bridge-c:21353".to_string(), 5),
                ],
            ),
        ] {
            let mut member = ClusterMemberRecord::local_non_voting(
                state.cluster_id.clone(),
                node.clone(),
                name,
                format!("pk-{node}"),
                now,
            );
            member.capabilities = main_daemon_capabilities(&member.role);
            member.allowed_capabilities = member.capabilities.clone();
            member.endpoints = endpoints;
            state.members.push(member);
        }
        let endpoint = crate::daemon_lan_endpoint(&target, "node-target:21353".to_string(), 5);
        let graph = ClusterConnectivityGraph::from_records(local.clone(), []);

        assert!(
            !cluster_relay_next_hops(&graph, &state, &local, &target, &[]).is_empty(),
            "member bridge endpoint should still be a best-effort relay fallback"
        );
        assert!(
            !cluster_endpoint_prefers_relay_with_graph(&graph, &state, &endpoint),
            "best-effort relay fallback without a complete route must not suppress a bounded direct hostname attempt"
        );
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
    fn best_effort_candidates_keep_equal_shortest_full_routes() {
        let (a, b, c, target) = (
            nid("node_a"),
            nid("node_b"),
            nid("node_c"),
            nid("node_target"),
        );
        let g = graph(vec![
            edge(&a, &b, ClusterConnectivityState::Healthy),
            edge(&a, &c, ClusterConnectivityState::Healthy),
            edge(&b, &target, ClusterConnectivityState::Healthy),
            edge(&c, &target, ClusterConnectivityState::Healthy),
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
            vec![bridge]
        );
    }

    #[test]
    fn degraded_relay_fallback_skips_leaf_when_bridge_has_onward_evidence() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_relay_degraded_skip_leaf").expect("cluster");
        let local = nid("node_relay_degraded_skip_leaf_local");
        let leaf = nid("node_relay_degraded_skip_leaf_leaf");
        let bridge = nid("node_relay_degraded_skip_leaf_bridge");
        let onward = nid("node_relay_degraded_skip_leaf_onward");
        let target = nid("node_relay_degraded_skip_leaf_target");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay degraded skip leaf",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        for node in [&leaf, &bridge] {
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
        let graph = graph(vec![
            edge(&local, &leaf, ClusterConnectivityState::Degraded),
            edge(&local, &bridge, ClusterConnectivityState::Degraded),
            edge(&bridge, &onward, ClusterConnectivityState::Healthy),
        ]);

        let next_hops = cluster_relay_next_hops(&graph, &state, &local, &target, &[]);

        assert_eq!(next_hops, vec![bridge]);
    }

    #[test]
    fn relay_fallback_skips_leaf_when_member_endpoints_identify_bridge() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_relay_member_bridge_rank").expect("cluster");
        let local = nid("node_relay_member_bridge_rank_local");
        let leaf = nid("node_relay_member_bridge_rank_leaf");
        let bridge = nid("node_relay_member_bridge_rank_bridge");
        let target = nid("node_relay_member_bridge_rank_target");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay member bridge rank",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut leaf_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            leaf.clone(),
            "leaf",
            "pk-leaf",
            now,
        );
        leaf_member.capabilities = main_daemon_capabilities(&leaf_member.role);
        leaf_member.allowed_capabilities = leaf_member.capabilities.clone();
        leaf_member.endpoints = vec![
            crate::daemon_lan_endpoint(&leaf, "node2:21353".to_string(), 5),
            crate::daemon_lan_endpoint(&leaf, "172.23.0.3:21353".to_string(), 10),
        ];
        state.members.push(leaf_member);
        let mut bridge_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            bridge.clone(),
            "bridge",
            "pk-bridge",
            now,
        );
        bridge_member.capabilities = main_daemon_capabilities(&bridge_member.role);
        bridge_member.allowed_capabilities = bridge_member.capabilities.clone();
        bridge_member.endpoints = vec![
            crate::daemon_lan_endpoint(&bridge, "node3:21353".to_string(), 5),
            crate::daemon_lan_endpoint(&bridge, "172.23.0.4:21353".to_string(), 10),
            crate::daemon_lan_endpoint(&bridge, "172.24.0.2:21353".to_string(), 10),
        ];
        state.members.push(bridge_member);
        let graph = graph(vec![
            edge(&local, &leaf, ClusterConnectivityState::Healthy),
            edge(&local, &bridge, ClusterConnectivityState::Healthy),
        ]);

        let next_hops = cluster_relay_next_hops(&graph, &state, &local, &target, &[]);

        assert_eq!(next_hops, vec![bridge]);
    }

    #[test]
    fn relay_fallback_uses_member_bridge_when_graph_has_no_next_hop() {
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_relay_stale_graph_member_bridge")
            .expect("cluster");
        let local = nid("node_relay_stale_graph_local");
        let leaf = nid("node_relay_stale_graph_leaf");
        let bridge = nid("node_relay_stale_graph_bridge");
        let target = nid("node_relay_stale_graph_target");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay stale graph member bridge",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut leaf_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            leaf,
            "leaf",
            "pk-leaf",
            now,
        );
        leaf_member.capabilities = main_daemon_capabilities(&leaf_member.role);
        leaf_member.allowed_capabilities = leaf_member.capabilities.clone();
        leaf_member.endpoints = vec![
            crate::daemon_lan_endpoint(&leaf_member.node_id, "node6:21353".to_string(), 5),
            crate::daemon_lan_endpoint(&leaf_member.node_id, "172.24.0.4:21353".to_string(), 20),
        ];
        state.members.push(leaf_member);
        let mut bridge_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            bridge.clone(),
            "bridge",
            "pk-bridge",
            now,
        );
        bridge_member.capabilities = main_daemon_capabilities(&bridge_member.role);
        bridge_member.allowed_capabilities = bridge_member.capabilities.clone();
        bridge_member.endpoints = vec![
            crate::daemon_lan_endpoint(&bridge, "node4:21353".to_string(), 5),
            crate::daemon_lan_endpoint(&bridge, "172.23.0.3:21353".to_string(), 20),
            crate::daemon_lan_endpoint(&bridge, "172.24.0.2:21353".to_string(), 20),
        ];
        state.members.push(bridge_member);
        let graph = graph(Vec::new());

        let next_hops = cluster_relay_next_hops(&graph, &state, &local, &target, &[]);

        assert_eq!(
            next_hops,
            vec![bridge],
            "a stale local graph should still relay through a stable hostname bridge candidate"
        );
    }

    #[test]
    fn relay_fallback_skips_member_bridge_when_local_pair_is_unreachable() {
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_relay_unreachable_member_bridge")
            .expect("cluster");
        let local = nid("node_relay_unreachable_bridge_local");
        let bridge = nid("node_relay_unreachable_bridge_bridge");
        let target = nid("node_relay_unreachable_bridge_target");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay unreachable member bridge",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut bridge_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            bridge.clone(),
            "bridge",
            "pk-bridge",
            now,
        );
        bridge_member.capabilities = main_daemon_capabilities(&bridge_member.role);
        bridge_member.allowed_capabilities = bridge_member.capabilities.clone();
        let stale_endpoint = crate::daemon_lan_endpoint(&bridge, "node4:21353".to_string(), 5);
        bridge_member.endpoints = vec![
            stale_endpoint.clone(),
            crate::daemon_lan_endpoint(&bridge, "172.23.0.3:21353".to_string(), 20),
            crate::daemon_lan_endpoint(&bridge, "172.24.0.2:21353".to_string(), 20),
        ];
        state.members.push(bridge_member);
        let graph = ClusterConnectivityGraph::from_records(
            local.clone(),
            [crate::cluster_connectivity_record(
                &state,
                &stale_endpoint,
                ClusterConnectivityState::Unreachable,
                None,
                3,
                None,
                Some("stale bridge endpoint failed".to_string()),
                now,
                60,
            )],
        );

        let next_hops = cluster_relay_next_hops(&graph, &state, &local, &target, &[]);

        assert!(
            next_hops.is_empty(),
            "a member bridge with only unreachable local-pair evidence must not be selected"
        );
    }

    #[test]
    fn relay_uses_healthy_graph_neighbor_when_member_capability_is_stale() {
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_relay_graph_neighbor_stale_member")
            .expect("cluster");
        let local = nid("node_relay_graph_neighbor_stale_local");
        let bridge = nid("node_relay_graph_neighbor_stale_bridge");
        let target = nid("node_relay_graph_neighbor_stale_target");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay graph neighbor stale member",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut bridge_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            bridge.clone(),
            "bridge",
            "pk-bridge",
            now,
        );
        bridge_member.capabilities.clear();
        bridge_member.allowed_capabilities.clear();
        state.members.push(bridge_member);
        let graph = graph(vec![edge(
            &local,
            &bridge,
            ClusterConnectivityState::Healthy,
        )]);

        let next_hops = cluster_relay_next_hops(&graph, &state, &local, &target, &[]);

        assert_eq!(
            next_hops,
            vec![bridge],
            "a healthy observed daemon edge should be a valid handoff even when member capabilities lag"
        );
    }

    #[test]
    fn relay_transit_skips_known_shadow_daemon_neighbor() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_relay_shadow_transit").expect("cluster");
        let local = nid("node_relay_shadow_transit_local");
        let shadow = nid("node_relay_shadow_transit_shadow");
        let bridge = nid("node_relay_shadow_transit_bridge");
        let target = nid("node_relay_shadow_transit_target");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay shadow transit",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut shadow_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            shadow.clone(),
            "shadow",
            "pk-shadow",
            now,
        );
        shadow_member.capabilities = sinomo_node::shadow_daemon_capabilities();
        shadow_member.allowed_capabilities = shadow_member.capabilities.clone();
        state.members.push(shadow_member);
        let mut bridge_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            bridge.clone(),
            "bridge",
            "pk-bridge",
            now,
        );
        bridge_member.capabilities = main_daemon_capabilities(&bridge_member.role);
        bridge_member.allowed_capabilities = bridge_member.capabilities.clone();
        state.members.push(bridge_member);
        let graph = graph(vec![
            edge(&local, &shadow, ClusterConnectivityState::Healthy),
            edge(&shadow, &target, ClusterConnectivityState::Healthy),
            edge(&local, &bridge, ClusterConnectivityState::Healthy),
            edge(&bridge, &target, ClusterConnectivityState::Healthy),
        ]);

        let next_hops = cluster_relay_next_hops(&graph, &state, &local, &target, &[]);

        assert_eq!(
            next_hops,
            vec![bridge],
            "shadow daemons use a separate node identity and cannot be selected as transit for main-daemon RPC routing"
        );
    }

    #[test]
    fn direct_target_fallback_uses_locally_learned_target_hostname_after_stale_bridge() {
        let root =
            sinomo_testdir::TestDir::new("sinomo-network-relay-direct-fallback-target-hostname");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_relay_direct_fallback_hostname")
            .expect("cluster");
        let local = nid("node_relay_direct_fallback_hostname_local");
        let stale_bridge = nid("node_relay_direct_fallback_hostname_stale_bridge");
        let target = nid("node_relay_direct_fallback_hostname_target");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay direct fallback target hostname",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let target_endpoint = crate::daemon_lan_endpoint(&target, "node1:21353".to_string(), 5);
        let mut target_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            target.clone(),
            "target",
            "pk-target",
            now,
        );
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![target_endpoint];
        state.members.push(target_member);
        let graph = graph(vec![edge(
            &local,
            &stale_bridge,
            ClusterConnectivityState::Healthy,
        )]);

        let next_hops = cluster_relay_next_hops_with_direct_target_fallback(
            &paths,
            &graph,
            &state,
            &local,
            &target,
            std::slice::from_ref(&stale_bridge),
        );

        assert_eq!(
            next_hops,
            vec![target],
            "a relay hop that only knows the stale previous bridge should still try the target's locally learned Docker hostname"
        );
    }

    #[test]
    fn direct_target_fallback_uses_graph_observed_target_endpoint_when_no_transit_route() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-relay-direct-fallback-reachable");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_relay_direct_fallback_reachable")
            .expect("cluster");
        let local = nid("node_relay_direct_fallback_local");
        let bridge = nid("node_relay_direct_fallback_bridge");
        let target = nid("node_relay_direct_fallback_target");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay direct fallback reachable",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let target_endpoint = crate::daemon_lan_endpoint(&target, "node1:21353".to_string(), 5);
        let mut target_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            target.clone(),
            "target",
            "pk-target",
            now,
        );
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![target_endpoint.clone()];
        state.members.push(target_member);
        let graph = graph(vec![
            edge(&local, &bridge, ClusterConnectivityState::Healthy),
            edge(&local, &target, ClusterConnectivityState::Degraded),
        ]);

        let next_hops = cluster_relay_next_hops_with_direct_target_fallback(
            &paths,
            &graph,
            &state,
            &local,
            &target,
            std::slice::from_ref(&bridge),
        );

        assert_eq!(
            next_hops,
            vec![target],
            "a graph-observed final-target endpoint remains valid fallback evidence even when no transit route is available"
        );
    }

    #[test]
    fn direct_target_fallback_still_requires_target_evidence_once_graph_has_edges() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-relay-direct-fallback-no-evidence");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_relay_direct_fallback_no_evidence")
            .expect("cluster");
        let local = nid("node_relay_direct_fallback_no_evidence_local");
        let bridge = nid("node_relay_direct_fallback_no_evidence_bridge");
        let target = nid("node_relay_direct_fallback_no_evidence_target");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay direct fallback no evidence",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let target_endpoint = crate::daemon_lan_endpoint(&target, "10.91.0.9:21353".to_string(), 5);
        let mut target_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            target.clone(),
            "target",
            "pk-target",
            now,
        );
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![target_endpoint];
        state.members.push(target_member);
        let graph = graph(vec![edge(
            &local,
            &bridge,
            ClusterConnectivityState::Healthy,
        )]);

        let next_hops = cluster_relay_next_hops_with_direct_target_fallback(
            &paths,
            &graph,
            &state,
            &local,
            &target,
            std::slice::from_ref(&bridge),
        );

        assert!(
            next_hops.is_empty(),
            "a socket-only member endpoint is not enough final-target evidence once the graph has local edges"
        );
    }

    #[test]
    fn direct_target_fallback_keeps_startup_path_when_graph_is_empty() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-relay-direct-fallback-startup");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_relay_direct_fallback_startup").expect("cluster");
        let local = nid("node_relay_direct_fallback_startup_local");
        let target = nid("node_relay_direct_fallback_startup_target");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay direct fallback startup",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let target_endpoint = crate::daemon_lan_endpoint(&target, "node1:21353".to_string(), 5);
        let mut target_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            target.clone(),
            "target",
            "pk-target",
            now,
        );
        target_member.capabilities = main_daemon_capabilities(&target_member.role);
        target_member.allowed_capabilities = target_member.capabilities.clone();
        target_member.endpoints = vec![target_endpoint];
        state.members.push(target_member);
        let graph = ClusterConnectivityGraph::from_records(local.clone(), []);

        let next_hops = cluster_relay_next_hops_with_direct_target_fallback(
            &paths,
            &graph,
            &state,
            &local,
            &target,
            &[],
        );

        assert_eq!(next_hops, vec![target]);
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
    fn relay_envelope_propagates_streamed_raw_response_metadata() {
        let mut inner = inner_request();
        inner.metadata.insert(
            crate::DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_KEY.to_string(),
            crate::DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_VALUE.to_string(),
        );
        inner.metadata.insert(
            crate::DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_KEY.to_string(),
            crate::DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_VALUE.to_string(),
        );
        let envelope = RelayEnvelope::new(
            ClusterId::from_legacy_named("cluster_route_test").expect("cluster"),
            nid("node_target"),
            inner,
        );

        let request = envelope.into_request().expect("encode");

        assert_eq!(
            request
                .metadata
                .get(crate::DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_KEY),
            Some(&crate::DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_VALUE.to_string())
        );
        assert_eq!(
            request
                .metadata
                .get(crate::DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_KEY),
            Some(&crate::DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_VALUE.to_string())
        );
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
    fn relay_next_hop_endpoints_keep_degraded_fallback_without_healthy_graph_edge() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-relay-endpoint-degraded");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_relay_endpoint_degraded").expect("cluster");
        let local = nid("node_relay_endpoint_degraded_local");
        let next_hop = nid("node_relay_endpoint_degraded_next");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay endpoint degraded",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let degraded_endpoint =
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
        next_hop_member.endpoints = vec![degraded_endpoint.clone()];
        state.members.push(next_hop_member);
        let graph = ClusterConnectivityGraph::from_records(
            local.clone(),
            [crate::cluster_connectivity_record(
                &state,
                &degraded_endpoint,
                ClusterConnectivityState::Degraded,
                None,
                1,
                None,
                Some("failed".to_string()),
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
            vec![degraded_endpoint.address.as_str()]
        );
    }

    #[test]
    fn relay_next_hop_endpoints_keep_all_usable_main_daemon_candidates() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-relay-endpoint-dual-home");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_relay_endpoint_dual_home").expect("cluster");
        let local = nid("node_relay_endpoint_dual_home_local");
        let next_hop = nid("node_relay_endpoint_dual_home_next");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay endpoint dual home",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let seg_a_endpoint =
            crate::daemon_lan_endpoint(&next_hop, "172.24.0.4:21353".to_string(), 5);
        let seg_b_endpoint =
            crate::daemon_lan_endpoint(&next_hop, "172.23.0.2:21353".to_string(), 10);
        let mut next_hop_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            next_hop.clone(),
            "next-hop",
            "pk-next-hop",
            now,
        );
        next_hop_member.capabilities = main_daemon_capabilities(&next_hop_member.role);
        next_hop_member.allowed_capabilities = next_hop_member.capabilities.clone();
        next_hop_member.endpoints = vec![seg_b_endpoint.clone(), seg_a_endpoint.clone()];
        state.members.push(next_hop_member);
        let graph = ClusterConnectivityGraph::from_records(local.clone(), []);

        let endpoints =
            cluster_relay_next_hop_endpoints_with_graph(&paths, &graph, &state, &local, &next_hop);
        let addresses = endpoints
            .iter()
            .map(|endpoint| endpoint.address.as_str())
            .collect::<BTreeSet<_>>();

        assert_eq!(
            addresses,
            BTreeSet::from([
                seg_a_endpoint.address.as_str(),
                seg_b_endpoint.address.as_str()
            ]),
            "relay next-hop dialing must keep every usable address for a dual-homed bridge"
        );
    }

    #[test]
    fn relay_next_hop_endpoints_skip_folded_shadow_daemon_candidates() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-relay-endpoint-shadow-filter");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_relay_endpoint_shadow_filter").expect("cluster");
        let local = nid("node_relay_endpoint_shadow_filter_local");
        let next_hop = nid("node_relay_endpoint_shadow_filter_next");
        let shadow = shadow_daemon_node_id(&next_hop);
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay endpoint shadow filter",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let main_endpoint = crate::daemon_lan_endpoint(&next_hop, "node4:21353".to_string(), 5);
        let mut folded_shadow_endpoint =
            crate::daemon_lan_endpoint(&shadow, "node4:21354".to_string(), 5);
        folded_shadow_endpoint.node_id = next_hop.clone();
        let mut next_hop_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            next_hop.clone(),
            "next-hop",
            "pk-next-hop",
            now,
        );
        next_hop_member.capabilities = main_daemon_capabilities(&next_hop_member.role);
        next_hop_member.allowed_capabilities = next_hop_member.capabilities.clone();
        next_hop_member.endpoints = vec![folded_shadow_endpoint, main_endpoint.clone()];
        state.members.push(next_hop_member);
        let graph = ClusterConnectivityGraph::from_records(local.clone(), []);

        let endpoints =
            cluster_relay_next_hop_endpoints_with_graph(&paths, &graph, &state, &local, &next_hop);

        assert_eq!(
            endpoints
                .iter()
                .map(|endpoint| endpoint.address.as_str())
                .collect::<Vec<_>>(),
            vec![main_endpoint.address.as_str()],
            "main-daemon relay next hops must not dial folded shadow supervision ports"
        );
    }

    #[test]
    fn relay_next_hop_endpoints_skip_graph_shadow_daemon_port_candidate() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-relay-graph-shadow-port-filter");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_relay_graph_shadow_port_filter")
            .expect("cluster");
        let local = nid("node_relay_graph_shadow_port_filter_local");
        let next_hop = nid("node_relay_graph_shadow_port_filter_next");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay graph shadow port filter",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let main_endpoint = crate::daemon_lan_endpoint(&next_hop, "node4:21353".to_string(), 5);
        let shadow_port_endpoint =
            crate::daemon_lan_endpoint(&next_hop, "node4:21354".to_string(), 1);
        let mut next_hop_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            next_hop.clone(),
            "next-hop",
            "pk-next-hop",
            now,
        );
        next_hop_member.capabilities = main_daemon_capabilities(&next_hop_member.role);
        next_hop_member.allowed_capabilities = next_hop_member.capabilities.clone();
        next_hop_member.endpoints = vec![shadow_port_endpoint.clone(), main_endpoint.clone()];
        state.members.push(next_hop_member);
        let graph = ClusterConnectivityGraph::from_records(
            local.clone(),
            [
                crate::cluster_connectivity_record(
                    &state,
                    &shadow_port_endpoint,
                    ClusterConnectivityState::Healthy,
                    Some(1),
                    0,
                    Some("shadow supervision port answered".to_string()),
                    None,
                    now,
                    60,
                ),
                crate::cluster_connectivity_record(
                    &state,
                    &main_endpoint,
                    ClusterConnectivityState::Healthy,
                    Some(9),
                    0,
                    Some("main daemon answered".to_string()),
                    None,
                    now,
                    60,
                ),
            ],
        );

        let endpoints =
            cluster_relay_next_hop_endpoints_with_graph(&paths, &graph, &state, &local, &next_hop);

        assert_eq!(
            endpoints
                .iter()
                .map(|endpoint| endpoint.address.as_str())
                .collect::<Vec<_>>(),
            vec![main_endpoint.address.as_str()],
            "main-daemon relay must skip graph records that point at the shadow supervision port"
        );
    }

    #[test]
    fn relay_next_hop_endpoints_shadow_only_graph_allows_member_main_fallback() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-relay-shadow-only-graph-fallback");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_relay_shadow_only_graph_fallback")
            .expect("cluster");
        let local = nid("node_relay_shadow_only_graph_fallback_local");
        let next_hop = nid("node_relay_shadow_only_graph_fallback_next");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay shadow-only graph fallback",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let main_endpoint = crate::daemon_lan_endpoint(&next_hop, "node4:21353".to_string(), 5);
        let shadow_port_endpoint =
            crate::daemon_lan_endpoint(&next_hop, "node4:21354".to_string(), 1);
        let mut next_hop_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            next_hop.clone(),
            "next-hop",
            "pk-next-hop",
            now,
        );
        next_hop_member.capabilities = main_daemon_capabilities(&next_hop_member.role);
        next_hop_member.allowed_capabilities = next_hop_member.capabilities.clone();
        next_hop_member.endpoints = vec![main_endpoint.clone()];
        state.members.push(next_hop_member);
        let graph = ClusterConnectivityGraph::from_records(
            local.clone(),
            [crate::cluster_connectivity_record(
                &state,
                &shadow_port_endpoint,
                ClusterConnectivityState::Healthy,
                Some(1),
                0,
                Some("shadow supervision port answered".to_string()),
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
            vec![main_endpoint.address.as_str()],
            "shadow-only graph reachability must not suppress a member main-daemon fallback"
        );
    }

    #[test]
    fn relay_next_hop_endpoints_suppress_known_unreachable_member_endpoint_when_pair_is_reachable()
    {
        let root = sinomo_testdir::TestDir::new("sinomo-network-relay-endpoint-reachable-pair");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_relay_endpoint_reachable_pair").expect("cluster");
        let local = nid("node_relay_endpoint_reachable_pair_local");
        let next_hop = nid("node_relay_endpoint_reachable_pair_next");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay endpoint reachable pair",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let stale_member_endpoint =
            crate::daemon_lan_endpoint(&next_hop, "node3:21353".to_string(), 5);
        let observed_reachable_endpoint =
            crate::daemon_lan_endpoint(&next_hop, "172.24.0.3:21353".to_string(), 1);
        let mut next_hop_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            next_hop.clone(),
            "next-hop",
            "pk-next-hop",
            now,
        );
        next_hop_member.capabilities = main_daemon_capabilities(&next_hop_member.role);
        next_hop_member.allowed_capabilities = next_hop_member.capabilities.clone();
        next_hop_member.endpoints = vec![stale_member_endpoint.clone()];
        state.members.push(next_hop_member);
        let graph = ClusterConnectivityGraph::from_records(
            local.clone(),
            [
                crate::cluster_connectivity_record(
                    &state,
                    &observed_reachable_endpoint,
                    ClusterConnectivityState::Degraded,
                    Some(8),
                    1,
                    Some("recent relay hop".to_string()),
                    None,
                    now,
                    60,
                ),
                crate::cluster_connectivity_record(
                    &state,
                    &stale_member_endpoint,
                    ClusterConnectivityState::Unreachable,
                    None,
                    1,
                    None,
                    Some("stale before reconnect".to_string()),
                    now,
                    60,
                ),
            ],
        );

        let endpoints =
            cluster_relay_next_hop_endpoints_with_graph(&paths, &graph, &state, &local, &next_hop);

        assert_eq!(
            endpoints
                .iter()
                .map(|endpoint| endpoint.address.as_str())
                .collect::<Vec<_>>(),
            vec![observed_reachable_endpoint.address.as_str()],
            "once a pair has a graph-observed reachable endpoint, relay forwarding must not retain a member endpoint already marked unreachable"
        );
    }

    #[test]
    fn relay_next_hop_endpoints_suppresses_stale_fallback_after_degraded_graph_endpoint() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-relay-endpoint-degraded-graph");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_relay_endpoint_degraded_graph").expect("cluster");
        let local = nid("node_relay_endpoint_degraded_graph_local");
        let next_hop = nid("node_relay_endpoint_degraded_graph_next");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay endpoint degraded graph",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let stale_member_endpoint =
            crate::daemon_lan_endpoint(&next_hop, "node3:21353".to_string(), 5);
        let mut next_hop_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            next_hop.clone(),
            "next-hop",
            "pk-next-hop",
            now,
        );
        next_hop_member.capabilities = main_daemon_capabilities(&next_hop_member.role);
        next_hop_member.allowed_capabilities = next_hop_member.capabilities.clone();
        next_hop_member.endpoints = vec![stale_member_endpoint.clone()];
        state.members.push(next_hop_member);
        let reachable_graph_endpoint =
            crate::daemon_lan_endpoint(&next_hop, "172.24.0.4:21353".to_string(), 1);
        let graph = ClusterConnectivityGraph::from_records(
            local.clone(),
            [
                crate::cluster_connectivity_record(
                    &state,
                    &stale_member_endpoint,
                    ClusterConnectivityState::Unreachable,
                    None,
                    3,
                    None,
                    Some("stale hostname failure".to_string()),
                    now,
                    60,
                ),
                crate::cluster_connectivity_record(
                    &state,
                    &reachable_graph_endpoint,
                    ClusterConnectivityState::Degraded,
                    Some(6),
                    1,
                    Some("recent success".to_string()),
                    Some("transient failure".to_string()),
                    now,
                    60,
                ),
            ],
        );

        let endpoints =
            cluster_relay_next_hop_endpoints_with_graph(&paths, &graph, &state, &local, &next_hop);

        assert_eq!(
            endpoints
                .iter()
                .map(|endpoint| endpoint.address.as_str())
                .collect::<Vec<_>>(),
            vec![reachable_graph_endpoint.address.as_str()],
            "a concrete graph-observed degraded endpoint should suppress stale unreachable member fallbacks for the same pair"
        );
    }

    #[test]
    fn relay_next_hop_endpoints_suppress_degraded_fallback_after_healthy_graph_edge() {
        let root =
            sinomo_testdir::TestDir::new("sinomo-network-relay-endpoint-degraded-suppressed");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_relay_endpoint_degraded_suppressed")
            .expect("cluster");
        let local = nid("node_relay_endpoint_degraded_suppressed_local");
        let next_hop = nid("node_relay_endpoint_degraded_suppressed_next");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay endpoint degraded suppressed",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let degraded_endpoint =
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
        next_hop_member.endpoints = vec![degraded_endpoint];
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
            vec!["172.24.0.3:21353"],
            "a confirmed healthy graph endpoint should suppress stale degraded member endpoints"
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

    #[test]
    fn relay_next_hops_can_fall_back_to_degraded_transit_candidate() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_relay_degraded_transit").expect("cluster");
        let local = nid("node_relay_degraded_transit_local");
        let target = nid("node_relay_degraded_transit_target");
        let bridge = nid("node_relay_degraded_transit_bridge");
        let onward = nid("node_relay_degraded_transit_onward");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "relay degraded transit",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut bridge_member = ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            bridge.clone(),
            "bridge",
            "pk-bridge",
            now,
        );
        bridge_member.capabilities = main_daemon_capabilities(&bridge_member.role);
        bridge_member.allowed_capabilities = bridge_member.capabilities.clone();
        state.members.push(bridge_member);
        let graph = graph(vec![
            edge(&local, &bridge, ClusterConnectivityState::Degraded),
            edge(&bridge, &onward, ClusterConnectivityState::Healthy),
            edge(&onward, &target, ClusterConnectivityState::Healthy),
        ]);

        let next_hops = cluster_relay_next_hops(
            &graph,
            &state,
            &local,
            &target,
            std::slice::from_ref(&target),
        );

        assert_eq!(next_hops, vec![bridge]);
    }

    #[test]
    fn relay_hop_timeouts_cap_large_idle_budget() {
        let shaped = cluster_relay_hop_transport_timeouts(
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(10)),
            Some(Duration::from_secs(9)),
        );

        assert_eq!(shaped.connect_timeout, Some(Duration::from_secs(9)));
        assert_eq!(shaped.idle_timeout, Some(Duration::from_secs(5)));

        let short = cluster_relay_hop_transport_timeouts(
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(3)),
            Some(Duration::from_secs(9)),
        );

        assert_eq!(short.connect_timeout, Some(Duration::from_secs(3)));
        assert_eq!(short.idle_timeout, Some(Duration::from_secs(3)));
    }

    #[derive(Default)]
    struct RecordingSignedRelayRpcHost {
        failures_remaining: usize,
        signed_routes: Vec<String>,
        sent_timeouts: Vec<DaemonRpcTimeouts>,
        sent: Vec<(ClusterEndpointRecord, Vec<u8>, RelayEnvelope, Duration)>,
    }

    fn recorded_route_timeout(timeouts: DaemonRpcTimeouts) -> Duration {
        timeouts
            .connect_timeout
            .or(timeouts.idle_timeout)
            .expect("route helpers should pass an explicit routing timeout budget")
    }

    impl ClusterSignedEndpointRpcAsyncHost for RecordingSignedRelayRpcHost {
        fn sign_endpoint_rpc_request_async<'a>(
            &'a mut self,
            request: &'a ObjectRpcRequest,
        ) -> ClusterSignedEndpointRpcFuture<'a, Vec<u8>> {
            Box::pin(async move {
                self.signed_routes.push(request.route.clone());
                Ok(format!("sig:{}", request.route).into_bytes())
            })
        }

        fn try_gateway_signed_rpc_async<'a>(
            &'a mut self,
            _endpoint: &'a ClusterEndpointRecord,
            _request: &'a ObjectRpcRequest,
            _request_auth: &'a [u8],
            _timeouts: DaemonRpcTimeouts,
        ) -> ClusterSignedEndpointRpcFuture<'a, Option<ObjectRpcResponse>> {
            Box::pin(async { Ok(None) })
        }

        fn send_signed_endpoint_rpc_async<'a>(
            &'a mut self,
            endpoint: &'a ClusterEndpointRecord,
            auth: &'a [u8],
            request: &'a ObjectRpcRequest,
            timeouts: DaemonRpcTimeouts,
        ) -> ClusterSignedEndpointRpcFuture<'a, ObjectRpcResponse> {
            Box::pin(async move {
                self.sent_timeouts.push(timeouts);
                self.sent.push((
                    endpoint.clone(),
                    auth.to_vec(),
                    RelayEnvelope::from_request(request)?,
                    recorded_route_timeout(timeouts),
                ));
                if self.failures_remaining > 0 {
                    self.failures_remaining -= 1;
                    return Err(NetworkError::Io {
                        operation: "test signed relay".to_string(),
                        detail: "forced relay failure".to_string(),
                    });
                }
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
            auth: None,
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
            ClusterSignedRelayRpcOptions {
                timeouts: DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(3)),
            },
            &mut host,
        )
        .await
        .expect("endpoint relay")
        .expect("route");

        assert_eq!(response.decode_body::<String>().expect("body"), "ok");
        assert_eq!(host.signed_routes, vec![CLUSTER_RELAY_ROUTE.to_string()]);
        assert_eq!(host.sent.len(), 1);
        let (sent_endpoint, auth, envelope, timeout) = host.sent.pop().expect("sent relay");
        assert_eq!(sent_endpoint.node_id, bridge);
        assert_eq!(auth, b"sig:/cluster/relay".to_vec());
        assert!(timeout <= Duration::from_secs(3));
        assert_eq!(host.sent_timeouts.len(), 1);
        assert!(host.sent_timeouts[0].connect_timeout <= Some(Duration::from_secs(3)));
        assert_eq!(
            host.sent_timeouts[0].idle_timeout,
            Some(Duration::from_secs(3)),
            "relay should keep a short caller idle budget instead of expanding every hop"
        );
        assert_eq!(envelope.cluster_id, cluster_id);
        assert_eq!(envelope.target_node_id, target);
        assert_eq!(envelope.visited, vec![local]);
        assert_eq!(envelope.max_hop_timeout_ms, Some(3_000));
        assert_eq!(
            envelope.inner,
            request.clone().with_auth(b"inner-auth".to_vec())
        );
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
            auth: None,
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
        let (sent_endpoint, auth, envelope, timeout) = host.sent.pop().expect("sent relay");
        assert_eq!(sent_endpoint.node_id, bridge);
        assert_eq!(auth, b"sig:/cluster/relay".to_vec());
        assert!(timeout <= Duration::from_secs(20));
        assert_eq!(host.sent_timeouts.len(), 1);
        assert_eq!(host.sent_timeouts[0].idle_timeout, None);
        assert!(host.sent_timeouts[0].connect_timeout <= Some(Duration::from_secs(20)));
        assert_eq!(envelope.cluster_id, cluster_id);
        assert_eq!(envelope.target_node_id, target);
        assert_eq!(envelope.visited, vec![local]);
        assert_eq!(envelope.max_hop_timeout_ms, Some(20_000));
        assert_eq!(
            envelope.inner,
            request.clone().with_auth(b"inner-auth".to_vec())
        );
    }

    #[tokio::test]
    async fn signed_relay_endpoint_async_reuses_inner_auth_across_relay_fallbacks() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-signed-relay-inner-auth-stable");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_signed_relay_inner_auth").expect("cluster");
        let local = nid("node_signed_relay_inner_auth_local");
        let bridge_a = nid("node_signed_relay_inner_auth_bridge_a");
        let bridge_b = nid("node_signed_relay_inner_auth_bridge_b");
        let target = nid("node_signed_relay_inner_auth_target");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "signed endpoint relay inner auth",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let bridge_a_endpoint =
            crate::daemon_lan_endpoint(&bridge_a, "10.84.0.2:21353".to_string(), 5);
        let bridge_b_endpoint =
            crate::daemon_lan_endpoint(&bridge_b, "10.84.0.3:21353".to_string(), 5);
        let target_endpoint = crate::daemon_lan_endpoint(&target, "10.85.0.3:21353".to_string(), 5);
        for (node, endpoint) in [
            (bridge_a.clone(), bridge_a_endpoint.clone()),
            (bridge_b.clone(), bridge_b_endpoint.clone()),
            (target.clone(), target_endpoint.clone()),
        ] {
            let mut member = ClusterMemberRecord::local_non_voting(
                cluster_id.clone(),
                node.clone(),
                node.as_str(),
                format!("pk-{node}"),
                now,
            );
            member.capabilities = main_daemon_capabilities(&member.role);
            member.allowed_capabilities = member.capabilities.clone();
            member.endpoints = vec![endpoint];
            state.members.push(member);
        }
        crate::write_cluster_state(&paths, &state).expect("write state");
        crate::sync_cluster_connectivity_records_to_kv(
            &paths,
            &local,
            vec![
                crate::cluster_connectivity_record(
                    &state,
                    &bridge_a_endpoint,
                    ClusterConnectivityState::Healthy,
                    Some(3),
                    0,
                    Some("ok".to_string()),
                    None,
                    now,
                    60,
                ),
                crate::cluster_connectivity_record(
                    &state,
                    &bridge_b_endpoint,
                    ClusterConnectivityState::Healthy,
                    Some(3),
                    0,
                    Some("ok".to_string()),
                    None,
                    now,
                    60,
                ),
            ],
        )
        .expect("connectivity");
        let request = ObjectRpcRequest {
            route: "/cluster/kv/range".to_string(),
            category: "cluster.kv".to_string(),
            metadata: BTreeMap::new(),
            auth: None,
            body: vec![1, 2, 3],
        };
        let mut host = RecordingSignedRelayRpcHost {
            failures_remaining: 1,
            ..Default::default()
        };

        let runtime = tokio::runtime::Handle::current();
        let response = relay_rpc_to_endpoint_from_paths_async_with_host(
            &runtime,
            &paths,
            &target_endpoint,
            &request,
            b"stable-inner-auth".to_vec(),
            ClusterSignedRelayRpcOptions {
                timeouts: DaemonRpcTimeouts::connect_only(Duration::from_secs(20)),
            },
            &mut host,
        )
        .await
        .expect("endpoint relay")
        .expect("route");

        assert_eq!(response.decode_body::<String>().expect("body"), "ok");
        assert_eq!(host.sent.len(), 2);
        assert_eq!(
            host.signed_routes,
            vec![
                CLUSTER_RELAY_ROUTE.to_string(),
                CLUSTER_RELAY_ROUTE.to_string()
            ]
        );
        assert_eq!(
            host.sent[0].2.inner.auth,
            Some(b"stable-inner-auth".to_vec())
        );
        assert_eq!(
            host.sent[1].2.inner.auth,
            Some(b"stable-inner-auth".to_vec())
        );
        assert_ne!(host.sent[0].0.node_id, host.sent[1].0.node_id);
    }

    #[tokio::test]
    async fn signed_relay_endpoint_async_spreads_origin_next_hops_by_inner_auth() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-signed-relay-origin-spread");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_signed_relay_origin_spread").expect("cluster");
        let local = nid("node_signed_relay_origin_spread_local");
        let bridge_a = nid("node_signed_relay_origin_spread_bridge_a");
        let bridge_b = nid("node_signed_relay_origin_spread_bridge_b");
        let target = nid("node_signed_relay_origin_spread_target");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "signed endpoint relay origin spread",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let bridge_a_endpoint =
            crate::daemon_lan_endpoint(&bridge_a, "10.86.0.2:21353".to_string(), 5);
        let bridge_b_endpoint =
            crate::daemon_lan_endpoint(&bridge_b, "10.86.0.3:21353".to_string(), 5);
        let target_endpoint = crate::daemon_lan_endpoint(&target, "10.87.0.3:21353".to_string(), 5);
        for (node, endpoint) in [
            (bridge_a.clone(), bridge_a_endpoint.clone()),
            (bridge_b.clone(), bridge_b_endpoint.clone()),
            (target.clone(), target_endpoint.clone()),
        ] {
            let mut member = ClusterMemberRecord::local_non_voting(
                cluster_id.clone(),
                node.clone(),
                node.as_str(),
                format!("pk-{node}"),
                now,
            );
            member.capabilities = main_daemon_capabilities(&member.role);
            member.allowed_capabilities = member.capabilities.clone();
            member.endpoints = vec![endpoint];
            state.members.push(member);
        }
        crate::write_cluster_state(&paths, &state).expect("write state");
        crate::sync_cluster_connectivity_records_to_kv(
            &paths,
            &local,
            vec![
                crate::cluster_connectivity_record(
                    &state,
                    &bridge_a_endpoint,
                    ClusterConnectivityState::Healthy,
                    Some(3),
                    0,
                    Some("ok".to_string()),
                    None,
                    now,
                    60,
                ),
                crate::cluster_connectivity_record(
                    &state,
                    &bridge_b_endpoint,
                    ClusterConnectivityState::Healthy,
                    Some(3),
                    0,
                    Some("ok".to_string()),
                    None,
                    now,
                    60,
                ),
            ],
        )
        .expect("connectivity");
        let request = ObjectRpcRequest {
            route: "/cluster/kv/range".to_string(),
            category: "cluster.kv".to_string(),
            metadata: BTreeMap::new(),
            auth: None,
            body: vec![1, 2, 3],
        };
        let mut even_auth = None;
        let mut odd_auth = None;
        for attempt in 0..128 {
            let auth = format!("inner-auth-{attempt}").into_bytes();
            let signed_request = request.clone().with_auth(auth.clone());
            let rotation = relay_inner_next_hop_rotation_seed(&target, &signed_request) % 2;
            if rotation == 0 && even_auth.is_none() {
                even_auth = Some(auth.clone());
            }
            if rotation == 1 && odd_auth.is_none() {
                odd_auth = Some(auth);
            }
            if even_auth.is_some() && odd_auth.is_some() {
                break;
            }
        }
        let auth_a = even_auth.expect("auth with rotation 0");
        let auth_b = odd_auth.expect("auth with rotation 1");

        let runtime = tokio::runtime::Handle::current();
        let mut host_a = RecordingSignedRelayRpcHost::default();
        relay_rpc_to_endpoint_from_paths_async_with_host(
            &runtime,
            &paths,
            &target_endpoint,
            &request,
            auth_a,
            ClusterSignedRelayRpcOptions {
                timeouts: DaemonRpcTimeouts::connect_only(Duration::from_secs(20)),
            },
            &mut host_a,
        )
        .await
        .expect("endpoint relay a")
        .expect("route a");
        let mut host_b = RecordingSignedRelayRpcHost::default();
        relay_rpc_to_endpoint_from_paths_async_with_host(
            &runtime,
            &paths,
            &target_endpoint,
            &request,
            auth_b,
            ClusterSignedRelayRpcOptions {
                timeouts: DaemonRpcTimeouts::connect_only(Duration::from_secs(20)),
            },
            &mut host_b,
        )
        .await
        .expect("endpoint relay b")
        .expect("route b");

        assert_eq!(host_a.sent.len(), 1);
        assert_eq!(host_b.sent.len(), 1);
        assert_ne!(host_a.sent[0].0.node_id, host_b.sent[0].0.node_id);
    }

    struct RecordingSignedEndpointRpcHost {
        calls: Vec<&'static str>,
        cluster_id: Option<ClusterId>,
        gateway_response: Option<ObjectRpcResponse>,
        direct_result: Result<ObjectRpcResponse, NetworkError>,
        relay_result: Result<Option<ObjectRpcResponse>, NetworkError>,
        direct_delay: Option<Duration>,
        direct_timeout_policies: Vec<DaemonRpcTimeouts>,
        relay_timeout_policies: Vec<DaemonRpcTimeouts>,
        signed_routes: Vec<String>,
        direct_requests: Vec<ObjectRpcRequest>,
        direct_auth: Vec<Vec<u8>>,
        relay_auth: Vec<Vec<u8>>,
    }

    impl RecordingSignedEndpointRpcHost {
        fn new(
            direct_result: Result<ObjectRpcResponse, NetworkError>,
            relay_result: Result<Option<ObjectRpcResponse>, NetworkError>,
        ) -> Self {
            Self {
                calls: Vec::new(),
                cluster_id: None,
                gateway_response: None,
                direct_result,
                relay_result,
                direct_delay: None,
                direct_timeout_policies: Vec::new(),
                relay_timeout_policies: Vec::new(),
                signed_routes: Vec::new(),
                direct_requests: Vec::new(),
                direct_auth: Vec::new(),
                relay_auth: Vec::new(),
            }
        }

        fn with_cluster_id(mut self, cluster_id: ClusterId) -> Self {
            self.cluster_id = Some(cluster_id);
            self
        }

        fn with_direct_delay(mut self, delay: Duration) -> Self {
            self.direct_delay = Some(delay);
            self
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
            _timeouts: DaemonRpcTimeouts,
        ) -> ClusterSignedEndpointRpcFuture<'a, Option<ObjectRpcResponse>> {
            Box::pin(async move {
                self.calls.push("gateway");
                Ok(self.gateway_response.clone())
            })
        }

        fn endpoint_rpc_cluster_id_async<'a>(
            &'a mut self,
            _endpoint: &'a ClusterEndpointRecord,
        ) -> ClusterSignedEndpointRpcFuture<'a, Option<ClusterId>> {
            Box::pin(async move {
                self.calls.push("cluster_id");
                Ok(self.cluster_id.clone())
            })
        }

        fn send_signed_endpoint_rpc_async<'a>(
            &'a mut self,
            _endpoint: &'a ClusterEndpointRecord,
            auth: &'a [u8],
            request: &'a ObjectRpcRequest,
            timeouts: DaemonRpcTimeouts,
        ) -> ClusterSignedEndpointRpcFuture<'a, ObjectRpcResponse> {
            Box::pin(async move {
                self.calls.push("direct");
                assert!(
                    request.route == "/cluster/kv/range"
                        || request.route == CLUSTER_RELAY_ROUTE
                        || request.route == crate::CLUSTER_JOIN_REGISTER_ROUTE
                        || request.route == crate::CLUSTER_NETWORK_PING_ROUTE,
                    "unexpected route {}",
                    request.route
                );
                self.direct_requests.push(request.clone());
                self.direct_auth.push(auth.to_vec());
                self.direct_timeout_policies.push(timeouts);
                if let Some(delay) = self.direct_delay {
                    tokio::time::sleep(delay).await;
                }
                self.direct_result.clone()
            })
        }

        fn relay_signed_endpoint_rpc_async<'a>(
            &'a mut self,
            _endpoint: &'a ClusterEndpointRecord,
            request: &'a ObjectRpcRequest,
            request_auth: &'a [u8],
            timeouts: DaemonRpcTimeouts,
        ) -> ClusterSignedEndpointRpcFuture<'a, Option<ObjectRpcResponse>> {
            Box::pin(async move {
                self.calls.push("relay");
                assert!(
                    request.route == "/cluster/kv/range"
                        || request.route == crate::CLUSTER_NETWORK_PING_ROUTE,
                    "unexpected relay route {}",
                    request.route
                );
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
            auth: None,
            body: vec![1, 2, 3],
        }
    }

    fn endpoint_rpc_response(route: &str, value: &str) -> ObjectRpcResponse {
        ObjectRpcResponse::encode_body(route, &value.to_string()).expect("response")
    }

    #[tokio::test]
    async fn object_rpc_endpoint_client_signs_and_routes_request() {
        let target = nid("node_object_rpc_endpoint_client_target");
        let endpoint = crate::daemon_lan_endpoint(&target, "10.90.0.39:21353".to_string(), 5);
        let request = endpoint_rpc_request();
        let direct_response =
            endpoint_rpc_response("/test/object-rpc-endpoint-client-ok", "direct");
        let mut host = RecordingSignedEndpointRpcHost::new(Ok(direct_response), Ok(None));
        let mut client = ClusterObjectRpcClient::new(&mut host);

        let response = client
            .call_endpoint(
                &endpoint,
                &request,
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_only(Duration::from_secs(
                    5,
                ))),
            )
            .await
            .expect("endpoint rpc");

        assert_eq!(response.decode_body::<String>().expect("body"), "direct");
        assert_eq!(host.calls, vec!["gateway", "cluster_id", "direct"]);
        assert_eq!(host.signed_routes, vec![request.route.clone()]);
        assert_eq!(host.direct_requests[0].route, request.route);
        assert_eq!(host.direct_auth, vec![b"fresh-auth-1".to_vec()]);
        assert!(host.relay_auth.is_empty());
    }

    #[tokio::test]
    async fn object_rpc_node_client_selects_target_endpoint_and_routes_request() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-object-rpc-node-client");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_object_rpc_node_client").expect("cluster");
        let local = nid("node_object_rpc_node_client_local");
        let target = nid("node_object_rpc_node_client_target");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "object rpc node client",
            local,
            "local",
            "pk-local",
            now,
        );
        let endpoint = crate::daemon_lan_endpoint(&target, "10.90.0.41:21353".to_string(), 5);
        let mut member = ClusterMemberRecord::local_non_voting(
            cluster_id,
            target.clone(),
            "target",
            "pk-target",
            now,
        );
        member.role = ClusterMemberRole::Voter;
        member.state = ClusterMemberState::Voter;
        member.capabilities = main_daemon_capabilities(&member.role);
        member.allowed_capabilities = member.capabilities.clone();
        member.endpoints = vec![endpoint.clone()];
        state.members.push(member);

        let request = endpoint_rpc_request();
        let direct_response = endpoint_rpc_response("/test/object-rpc-node-client-ok", "direct");
        let mut host = RecordingSignedEndpointRpcHost::new(Ok(direct_response), Ok(None));
        let mut client = ClusterObjectRpcClient::new(&mut host);
        let runtime = tokio::runtime::Handle::current();

        let response = client
            .call_node_from_paths(
                &runtime,
                &paths,
                &state,
                &target,
                &request,
                ClusterObjectRpcNodeOptions::new(
                    ClusterEndpointSelectionPurpose::VoterCommand,
                    DaemonRpcTimeouts::connect_only(Duration::from_secs(5)),
                ),
            )
            .await
            .expect("node rpc");

        assert_eq!(response.decode_body::<String>().expect("body"), "direct");
        assert_eq!(host.calls, vec!["gateway", "cluster_id", "direct"]);
        assert_eq!(host.direct_requests[0].route, request.route);
        assert_eq!(host.direct_auth, vec![b"fresh-auth-1".to_vec()]);
        assert!(host.relay_auth.is_empty());
    }

    #[tokio::test]
    async fn signed_endpoint_rpc_async_wraps_clustered_direct_attempt_with_target_envelope() {
        let cluster_id = ClusterId::from_legacy_named("cluster_signed_endpoint_direct_targeted")
            .expect("cluster");
        let target = nid("node_signed_endpoint_direct_targeted_target");
        let endpoint = crate::daemon_lan_endpoint(&target, "10.90.0.40:21353".to_string(), 5);
        let request = endpoint_rpc_request();
        let direct_response = endpoint_rpc_response("/test/endpoint-targeted-direct-ok", "direct");
        let mut host = RecordingSignedEndpointRpcHost::new(Ok(direct_response), Ok(None))
            .with_cluster_id(cluster_id.clone());

        let response = send_signed_rpc_to_endpoint_async_with_route_policy(
            &endpoint,
            &request,
            b"inner-auth",
            ClusterSignedEndpointRpcOptions {
                timeouts: DaemonRpcTimeouts::connect_only(Duration::from_secs(5)),
            },
            false,
            &mut host,
        )
        .await
        .expect("endpoint rpc");

        assert_eq!(response.decode_body::<String>().expect("body"), "direct");
        assert_eq!(host.calls, vec!["gateway", "cluster_id", "direct"]);
        assert_eq!(host.signed_routes, vec![CLUSTER_RELAY_ROUTE.to_string()]);
        assert_eq!(host.direct_requests[0].route, CLUSTER_RELAY_ROUTE);
        assert_ne!(host.direct_auth, vec![b"inner-auth".to_vec()]);
        let envelope =
            RelayEnvelope::from_request(host.direct_requests.first().expect("direct request"))
                .expect("relay envelope");
        assert_eq!(envelope.cluster_id, cluster_id);
        assert_eq!(envelope.target_node_id, target);
        assert_eq!(envelope.inner.auth, Some(b"inner-auth".to_vec()));
        assert_eq!(envelope.inner.route, "/cluster/kv/range");
    }

    #[tokio::test]
    async fn signed_endpoint_rpc_async_keeps_membership_rpc_raw_direct() {
        let cluster_id = ClusterId::from_legacy_named("cluster_signed_endpoint_membership_direct")
            .expect("cluster");
        let target = nid("node_signed_endpoint_membership_direct_target");
        let endpoint = crate::daemon_lan_endpoint(&target, "10.90.0.41:21353".to_string(), 5);
        let request = ObjectRpcRequest {
            route: crate::CLUSTER_JOIN_REGISTER_ROUTE.to_string(),
            category: crate::CLUSTER_MEMBERSHIP_RPC_CATEGORY.to_string(),
            metadata: BTreeMap::new(),
            auth: None,
            body: vec![1, 2, 3],
        };
        let direct_response =
            endpoint_rpc_response("/test/endpoint-membership-direct-ok", "direct");
        let mut host = RecordingSignedEndpointRpcHost::new(Ok(direct_response), Ok(None))
            .with_cluster_id(cluster_id);

        let response = send_signed_rpc_to_endpoint_async_with_route_policy(
            &endpoint,
            &request,
            b"inner-auth",
            ClusterSignedEndpointRpcOptions {
                timeouts: DaemonRpcTimeouts::connect_only(Duration::from_secs(5)),
            },
            false,
            &mut host,
        )
        .await
        .expect("endpoint rpc");

        assert_eq!(response.decode_body::<String>().expect("body"), "direct");
        assert_eq!(host.calls, vec!["direct"]);
        assert!(host.signed_routes.is_empty());
        assert_eq!(host.direct_auth, vec![b"inner-auth".to_vec()]);
        assert_eq!(host.direct_requests, vec![request]);
    }

    #[tokio::test]
    async fn signed_endpoint_rpc_async_keeps_network_ping_raw_direct() {
        let cluster_id =
            ClusterId::from_legacy_named("cluster_signed_endpoint_ping_direct").expect("cluster");
        let target = nid("node_signed_endpoint_ping_direct_target");
        let endpoint = crate::daemon_lan_endpoint(&target, "10.90.0.42:21353".to_string(), 5);
        let request = ObjectRpcRequest {
            route: crate::CLUSTER_NETWORK_PING_ROUTE.to_string(),
            category: crate::CLUSTER_NETWORK_RPC_CATEGORY.to_string(),
            metadata: BTreeMap::new(),
            auth: None,
            body: vec![1, 2, 3],
        };
        let direct_response = endpoint_rpc_response(crate::CLUSTER_NETWORK_PING_ROUTE, "direct");
        let mut host = RecordingSignedEndpointRpcHost::new(Ok(direct_response), Ok(None))
            .with_cluster_id(cluster_id);

        let response = send_signed_rpc_to_endpoint_async_with_route_policy(
            &endpoint,
            &request,
            b"inner-auth",
            ClusterSignedEndpointRpcOptions {
                timeouts: DaemonRpcTimeouts::connect_only(Duration::from_secs(5)),
            },
            false,
            &mut host,
        )
        .await
        .expect("endpoint rpc");

        assert_eq!(response.decode_body::<String>().expect("body"), "direct");
        assert_eq!(host.calls, vec!["direct"]);
        assert!(host.signed_routes.is_empty());
        assert_eq!(host.direct_auth, vec![b"inner-auth".to_vec()]);
        assert_eq!(host.direct_requests, vec![request]);
    }

    #[tokio::test]
    async fn signed_endpoint_rpc_async_relays_network_ping_when_relay_is_preferred() {
        let cluster_id = ClusterId::from_legacy_named("cluster_signed_endpoint_ping_prefers_relay")
            .expect("cluster");
        let target = nid("node_signed_endpoint_ping_prefers_relay_target");
        let endpoint = crate::daemon_lan_endpoint(&target, "10.90.0.46:21353".to_string(), 5);
        let request = ObjectRpcRequest {
            route: crate::CLUSTER_NETWORK_PING_ROUTE.to_string(),
            category: crate::CLUSTER_NETWORK_RPC_CATEGORY.to_string(),
            metadata: BTreeMap::new(),
            auth: None,
            body: vec![1, 2, 3],
        };
        let relay_response = endpoint_rpc_response(crate::CLUSTER_NETWORK_PING_ROUTE, "relay");
        let mut host = RecordingSignedEndpointRpcHost::new(
            Ok(relay_response.clone()),
            Ok(Some(relay_response)),
        )
        .with_cluster_id(cluster_id);

        let response = send_signed_rpc_to_endpoint_async_with_route_policy(
            &endpoint,
            &request,
            b"inner-auth",
            ClusterSignedEndpointRpcOptions {
                timeouts: DaemonRpcTimeouts::connect_only(Duration::from_secs(5)),
            },
            true,
            &mut host,
        )
        .await
        .expect("endpoint rpc");

        assert_eq!(response.decode_body::<String>().expect("body"), "relay");
        assert_eq!(host.calls, vec!["relay"]);
        assert!(host.direct_auth.is_empty());
        assert_eq!(host.relay_auth, vec![b"inner-auth".to_vec()]);
    }

    #[tokio::test]
    async fn signed_endpoint_rpc_async_keeps_metadata_opt_in_raw_direct() {
        let cluster_id = ClusterId::from_legacy_named("cluster_signed_endpoint_raw_direct_opt_in")
            .expect("cluster");
        let target = nid("node_signed_endpoint_raw_direct_opt_in_target");
        let endpoint = crate::daemon_lan_endpoint(&target, "10.90.0.43:21353".to_string(), 5);
        let mut request = endpoint_rpc_request();
        request.metadata.insert(
            crate::ENDPOINT_RPC_RAW_DIRECT_METADATA_KEY.to_string(),
            crate::ENDPOINT_RPC_RAW_DIRECT_METADATA_VALUE.to_string(),
        );
        let direct_response =
            endpoint_rpc_response("/test/endpoint-raw-direct-opt-in-ok", "direct");
        let mut host = RecordingSignedEndpointRpcHost::new(Ok(direct_response), Ok(None))
            .with_cluster_id(cluster_id);

        let response = send_signed_rpc_to_endpoint_async_with_route_policy(
            &endpoint,
            &request,
            b"inner-auth",
            ClusterSignedEndpointRpcOptions {
                timeouts: DaemonRpcTimeouts::connect_only(Duration::from_secs(5)),
            },
            false,
            &mut host,
        )
        .await
        .expect("endpoint rpc");

        assert_eq!(response.decode_body::<String>().expect("body"), "direct");
        assert_eq!(host.calls, vec!["direct"]);
        assert!(host.signed_routes.is_empty());
        assert_eq!(host.direct_auth, vec![b"inner-auth".to_vec()]);
        assert_eq!(host.direct_requests, vec![request]);
    }

    #[tokio::test]
    async fn signed_endpoint_rpc_async_keeps_raw_streamed_response_requests_direct() {
        let cluster_id =
            ClusterId::from_legacy_named("cluster_signed_endpoint_raw_streamed_response")
                .expect("cluster");
        let target = nid("node_signed_endpoint_raw_streamed_response_target");
        let endpoint = crate::daemon_lan_endpoint(&target, "10.90.0.44:21353".to_string(), 5);
        let mut request = endpoint_rpc_request();
        request.metadata.insert(
            crate::DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_KEY.to_string(),
            crate::DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_VALUE.to_string(),
        );
        request.metadata.insert(
            crate::DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_KEY.to_string(),
            crate::DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_VALUE.to_string(),
        );
        let direct_response =
            endpoint_rpc_response("/test/endpoint-raw-streamed-response-ok", "direct");
        let mut host = RecordingSignedEndpointRpcHost::new(Ok(direct_response), Ok(None))
            .with_cluster_id(cluster_id);

        let response = send_signed_rpc_to_endpoint_async_with_route_policy(
            &endpoint,
            &request,
            b"inner-auth",
            ClusterSignedEndpointRpcOptions {
                timeouts: DaemonRpcTimeouts::connect_only(Duration::from_secs(5)),
            },
            false,
            &mut host,
        )
        .await
        .expect("endpoint rpc");

        assert_eq!(response.decode_body::<String>().expect("body"), "direct");
        assert_eq!(host.calls, vec!["direct"]);
        assert!(host.signed_routes.is_empty());
        assert_eq!(host.direct_auth, vec![b"inner-auth".to_vec()]);
        assert_eq!(host.direct_requests, vec![request]);
    }

    #[tokio::test]
    async fn signed_endpoint_rpc_async_keeps_raw_direct_when_relay_is_preferred() {
        let cluster_id =
            ClusterId::from_legacy_named("cluster_signed_endpoint_raw_direct_prefers_relay")
                .expect("cluster");
        let target = nid("node_signed_endpoint_raw_direct_prefers_relay_target");
        let endpoint = crate::daemon_lan_endpoint(&target, "10.90.0.45:21353".to_string(), 5);
        let mut request = endpoint_rpc_request();
        request.metadata.insert(
            crate::ENDPOINT_RPC_RAW_DIRECT_METADATA_KEY.to_string(),
            crate::ENDPOINT_RPC_RAW_DIRECT_METADATA_VALUE.to_string(),
        );
        let direct_response =
            endpoint_rpc_response("/test/endpoint-raw-direct-prefers-relay-ok", "direct");
        let mut host = RecordingSignedEndpointRpcHost::new(Ok(direct_response), Ok(None))
            .with_cluster_id(cluster_id);

        let response = send_signed_rpc_to_endpoint_async_with_route_policy(
            &endpoint,
            &request,
            b"inner-auth",
            ClusterSignedEndpointRpcOptions {
                timeouts: DaemonRpcTimeouts::connect_only(Duration::from_secs(5)),
            },
            true,
            &mut host,
        )
        .await
        .expect("endpoint rpc");

        assert_eq!(response.decode_body::<String>().expect("body"), "direct");
        assert_eq!(host.calls, vec!["direct"]);
        assert!(host.signed_routes.is_empty());
        assert!(host.relay_auth.is_empty());
        assert_eq!(host.direct_auth, vec![b"inner-auth".to_vec()]);
        assert_eq!(host.direct_requests, vec![request]);
    }

    #[tokio::test]
    async fn signed_endpoint_rpc_async_direct_failure_relays_when_route_available() {
        let target = nid("node_signed_endpoint_async_target");
        let endpoint = crate::daemon_lan_endpoint(&target, "10.90.0.4:21353".to_string(), 5);
        let request = endpoint_rpc_request();
        let relay_response = endpoint_rpc_response("/test/endpoint-async-relay-ok", "relay-ok");
        let mut host = RecordingSignedEndpointRpcHost::new(
            Err(NetworkError::Io {
                operation: "test direct async endpoint rpc".to_string(),
                detail: "direct failed".to_string(),
            }),
            Ok(Some(relay_response)),
        );

        let response = send_signed_rpc_to_endpoint_async_with_route_policy(
            &endpoint,
            &request,
            b"inner-auth",
            ClusterSignedEndpointRpcOptions {
                timeouts: DaemonRpcTimeouts::connect_only(Duration::from_secs(5)),
            },
            false,
            &mut host,
        )
        .await
        .expect("endpoint rpc");

        assert_eq!(response.decode_body::<String>().expect("body"), "relay-ok");
        assert_eq!(host.calls, vec!["gateway", "cluster_id", "direct", "relay"]);
        assert!(host.signed_routes.is_empty());
        assert_eq!(host.direct_auth, vec![b"inner-auth".to_vec()]);
        assert_eq!(host.relay_auth, vec![b"inner-auth".to_vec()]);
        assert_eq!(host.direct_timeout_policies.len(), 1);
        assert_eq!(host.direct_timeout_policies[0].idle_timeout, None);
        assert!(host.direct_timeout_policies[0].connect_timeout <= Some(Duration::from_secs(5)));
        assert_eq!(host.relay_timeout_policies.len(), 1);
        assert_eq!(host.relay_timeout_policies[0].idle_timeout, None);
        assert!(host.relay_timeout_policies[0].connect_timeout <= Some(Duration::from_secs(5)));
    }

    #[tokio::test]
    async fn signed_endpoint_rpc_async_relays_after_direct_exhausts_deadline() {
        let target = nid("node_signed_endpoint_async_exhausted_target");
        let endpoint = crate::daemon_lan_endpoint(&target, "10.90.0.44:21353".to_string(), 5);
        let request = endpoint_rpc_request();
        let relay_response =
            endpoint_rpc_response("/test/endpoint-async-exhausted-relay-ok", "relay-ok");
        let mut host = RecordingSignedEndpointRpcHost::new(
            Err(NetworkError::Io {
                operation: "test direct async endpoint rpc".to_string(),
                detail: "direct timed out".to_string(),
            }),
            Ok(Some(relay_response)),
        )
        .with_direct_delay(Duration::from_millis(20));

        let response = send_signed_rpc_to_endpoint_async_with_route_policy(
            &endpoint,
            &request,
            b"inner-auth",
            ClusterSignedEndpointRpcOptions {
                timeouts: DaemonRpcTimeouts::connect_only(Duration::from_millis(1)),
            },
            false,
            &mut host,
        )
        .await
        .expect("endpoint rpc");

        assert_eq!(response.decode_body::<String>().expect("body"), "relay-ok");
        assert_eq!(host.calls, vec!["gateway", "cluster_id", "direct", "relay"]);
        assert_eq!(host.direct_auth, vec![b"inner-auth".to_vec()]);
        assert_eq!(host.relay_auth, vec![b"inner-auth".to_vec()]);
    }

    #[tokio::test]
    async fn signed_endpoint_rpc_async_allows_unbounded_transport_policy() {
        let target = nid("node_signed_endpoint_unbounded_target");
        let endpoint = crate::daemon_lan_endpoint(&target, "10.90.0.5:21353".to_string(), 5);
        let request = endpoint_rpc_request();
        let direct_response = endpoint_rpc_response("/test/endpoint-direct-unbounded-ok", "direct");
        let mut host = RecordingSignedEndpointRpcHost::new(Ok(direct_response), Ok(None));

        let response = send_signed_rpc_to_endpoint_async_with_route_policy(
            &endpoint,
            &request,
            b"inner-auth",
            ClusterSignedEndpointRpcOptions {
                timeouts: DaemonRpcTimeouts::none(),
            },
            false,
            &mut host,
        )
        .await
        .expect("endpoint rpc");

        assert_eq!(response.decode_body::<String>().expect("body"), "direct");
        assert_eq!(host.calls, vec!["gateway", "cluster_id", "direct"]);
        assert_eq!(
            host.direct_timeout_policies,
            vec![DaemonRpcTimeouts::none()]
        );
        assert!(host.signed_routes.is_empty());
        assert_eq!(host.direct_auth, vec![b"inner-auth".to_vec()]);
        assert!(host.relay_timeout_policies.is_empty());
    }

    #[tokio::test]
    async fn signed_endpoint_rpc_async_skips_direct_when_relay_preferred() {
        let target = nid("node_signed_endpoint_relay_first_target");
        let endpoint = crate::daemon_lan_endpoint(&target, "10.91.0.3:21353".to_string(), 5);
        let request = endpoint_rpc_request();
        let relay_response = endpoint_rpc_response("/test/endpoint-relay-first-ok", "relay-first");
        let mut host = RecordingSignedEndpointRpcHost::new(
            Ok(endpoint_rpc_response("/test/direct-unused", "direct")),
            Ok(Some(relay_response)),
        );

        let response = send_signed_rpc_to_endpoint_async_with_route_policy(
            &endpoint,
            &request,
            b"inner-auth",
            ClusterSignedEndpointRpcOptions {
                timeouts: DaemonRpcTimeouts::connect_only(Duration::from_secs(5)),
            },
            true,
            &mut host,
        )
        .await
        .expect("endpoint rpc");

        assert_eq!(
            response.decode_body::<String>().expect("body"),
            "relay-first"
        );
        assert_eq!(host.calls, vec!["relay"]);
        assert!(host.direct_auth.is_empty());
        assert!(host.signed_routes.is_empty());
        assert_eq!(host.relay_auth, vec![b"inner-auth".to_vec()]);
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

    #[derive(Default)]
    struct RecordingRelayStreamForwardHost {
        sent: Vec<(ClusterEndpointRecord, RelayEnvelope)>,
        sent_timeouts: Vec<DaemonRpcTimeouts>,
    }

    impl ClusterRelayStreamForwardAsyncHost for RecordingRelayStreamForwardHost {
        fn send_relay_stream_forward_async<'a>(
            &'a mut self,
            endpoint: &'a ClusterEndpointRecord,
            request: &'a ObjectRpcRequest,
            timeouts: DaemonRpcTimeouts,
        ) -> ClusterRelayFuture<'a, ClusterRelayStreamSendOutcome> {
            Box::pin(async move {
                self.sent
                    .push((endpoint.clone(), RelayEnvelope::from_request(request)?));
                self.sent_timeouts.push(timeouts);
                Ok(ClusterRelayStreamSendOutcome::Forwarded)
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
        let mut envelope = RelayEnvelope::new(cluster_id.clone(), target.clone(), inner_request());
        envelope.max_hop_timeout_ms = Some(1_500);

        let mut async_host = RecordingRelayHost::default();
        let runtime = tokio::runtime::Handle::current();
        let async_response = handle_cluster_relay_async_with_host(
            &runtime,
            &paths,
            &state,
            &local,
            envelope,
            ClusterRelayOptions {
                timeouts: DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(5)),
            },
            &mut async_host,
        )
        .await
        .expect("async relay");

        assert_eq!(async_response.decode_body::<String>().expect("body"), "ok");
        assert_eq!(async_host.local_deliveries, 0);
        assert!(async_host.local_timeouts.is_empty());
        assert_eq!(async_host.sent.len(), 1);
        assert_eq!(
            async_host.sent_timeouts,
            vec![DaemonRpcTimeouts::connect_and_idle(Duration::from_millis(
                1_500
            ))]
        );
        assert_eq!(async_host.sent[0].0.node_id, bridge);
        assert_eq!(async_host.sent[0].1.target_node_id, target);
        assert_eq!(async_host.sent[0].1.ttl, CLUSTER_RELAY_DEFAULT_TTL - 1);
        assert_eq!(async_host.sent[0].1.visited, vec![local.clone()]);
        assert_eq!(async_host.sent[0].1.max_hop_timeout_ms, Some(1_500));

        let mut stream_envelope = RelayEnvelope::new(cluster_id, target.clone(), inner_request());
        stream_envelope.max_hop_timeout_ms = Some(1_500);
        let mut stream_host = RecordingRelayStreamForwardHost::default();
        let stream_outcome = handle_cluster_relay_stream_forward_async_with_host(
            &runtime,
            &paths,
            &state,
            &local,
            stream_envelope,
            ClusterRelayOptions {
                timeouts: DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(5)),
            },
            &mut stream_host,
        )
        .await
        .expect("stream relay");

        assert!(matches!(
            stream_outcome,
            ClusterRelayStreamForwardOutcome::Forwarded
        ));
        assert_eq!(
            stream_host.sent_timeouts,
            vec![DaemonRpcTimeouts::connect_and_idle(Duration::from_millis(
                1_500
            ))]
        );
        assert_eq!(stream_host.sent.len(), 1);
        assert_eq!(stream_host.sent[0].0.node_id, bridge);
        assert_eq!(stream_host.sent[0].1.target_node_id, target);
        assert_eq!(stream_host.sent[0].1.ttl, CLUSTER_RELAY_DEFAULT_TTL - 1);
        assert_eq!(stream_host.sent[0].1.visited, vec![local]);
        assert_eq!(stream_host.sent[0].1.max_hop_timeout_ms, Some(1_500));
    }
}
