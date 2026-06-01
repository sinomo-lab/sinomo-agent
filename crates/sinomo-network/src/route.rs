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
//! This module is pure routing logic over the graph + envelope; signing, dialing, and the
//! relay request handler live in the daemon layer.

use crate::prelude::*;
use crate::{ClusterConnectivityGraph, DaemonControlRequest, NetworkError, ObjectRpcRequest};

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
    if local == target {
        return None;
    }
    let mut seen: BTreeSet<NodeId> = BTreeSet::new();
    seen.insert(local.clone());
    let mut queue: VecDeque<Vec<NodeId>> = VecDeque::new();
    queue.push_back(vec![local.clone()]);
    while let Some(path) = queue.pop_front() {
        let current = path.last().expect("BFS path is never empty");
        for next in graph.reachable_targets(current) {
            if !seen.insert(next.clone()) {
                continue;
            }
            let mut next_path = path.clone();
            next_path.push(next.clone());
            if next == *target {
                let next_hop = next_path
                    .get(1)
                    .cloned()
                    .expect("a path to a distinct target has a hop beyond local");
                return Some(ClusterRoute {
                    next_hop,
                    path: next_path,
                });
            }
            queue.push_back(next_path);
        }
    }
    None
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
    if let Some(route) = cluster_route_next_hop(graph, local, target) {
        return Some(route.next_hop);
    }
    graph.reachable_targets(local).into_iter().find(|candidate| {
        candidate != target && candidate != local && !excluded.contains(candidate)
    })
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
            edge(&b, &c, ClusterConnectivityState::Degraded),
        ]);
        let route = cluster_route_next_hop(&g, &a, &c).expect("route");
        assert_eq!(route.next_hop, b);
        assert_eq!(route.path, vec![a, b, c]);
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
    fn relay_decision_delivers_to_target() {
        let a = nid("node_a");
        let g = graph(vec![]);
        let envelope = RelayEnvelope::new(
            ClusterId::from_legacy_named("cluster_route_test").expect("cluster"),
            a.clone(),
            inner_request(),
        );
        assert_eq!(relay_decision(&envelope, &a, &g), RelayDecision::DeliverLocal);
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
}
