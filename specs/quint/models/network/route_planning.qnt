/// Cluster route graph and next-hop planning safety.
///
/// This module models the route-planning core in
/// `crates/sinomo-network/src/route.rs`: direct target edges, relay paths,
/// failed-edge exclusion, visited-node loop prevention, and cost ordering over
/// connectivity evidence. Endpoint validity is consumed from
/// `network_endpoint_selection`; this module assumes edges are already derived
/// from authorized endpoint candidates and directory connectivity records.
module network_route_planning {
  import common.* from "../../lib/common"

  type Edge = DirectN1N2 | RelayN1N3 | RelayN3N2
  type EdgeState =
    NoEdge
    | Healthy
    | Degraded
    | UnobservedTarget
    | Unreachable
    | IdentityMismatch
  type RoutePlan = NoRoute | DirectRoute | RelayRoute
  type RoutePhase = Idle | Planned
  type Decision = NoDecision | RoutePlanned | TerminalStutter

  pure val LocalNode: Node = N1
  pure val TargetNode: Node = N2
  pure val RelayNode: Node = N3
  pure val Edges: Set[Edge] = Set(DirectN1N2, RelayN1N3, RelayN3N2)
  pure val EdgeStates: Set[EdgeState] =
    Set(NoEdge, Healthy, Degraded, UnobservedTarget, Unreachable, IdentityMismatch)

  var phase: RoutePhase
  var edgeState: Edge -> EdgeState
  var failedEdges: Set[Edge]
  var visitedNodes: Set[Node]
  var selectedPlan: RoutePlan
  var lastDecision: Decision

  pure val initialEdgeState: Edge -> EdgeState =
    Edges.mapBy(_ => NoEdge)

  pure def edgeSource(edge: Edge): Node =
    match edge {
      | DirectN1N2 => N1
      | RelayN1N3 => N1
      | RelayN3N2 => N3
    }

  pure def edgeTarget(edge: Edge): Node =
    match edge {
      | DirectN1N2 => N2
      | RelayN1N3 => N3
      | RelayN3N2 => N2
    }

  pure def stateCost(state: EdgeState): int =
    match state {
      | Healthy => 10
      | UnobservedTarget => 25
      | Degraded => 60
      | Unreachable => 500
      | NoEdge => 1000
      | IdentityMismatch => 1000
    }

  /// Mirrors `cluster_delivery_local_endpoint_cost`: an unreachable direct
  /// target endpoint may remain as a high-cost last-resort fallback, but
  /// unreachable relay edges and identity mismatches are not usable.
  pure def edgeUsable(edge: Edge, state: EdgeState): bool =
    if (state == IdentityMismatch or state == NoEdge) {
      false
    } else if (state == Unreachable) {
      edge == DirectN1N2
    } else {
      true
    }

  pure def edgeAllowed(
    states: Edge -> EdgeState,
    failed: Set[Edge],
    edge: Edge
  ): bool =
    edgeUsable(edge, states.get(edge)) and not(failed.contains(edge))

  pure def pathAvoidsVisited(plan: RoutePlan, visited: Set[Node]): bool =
    match plan {
      | DirectRoute => not(visited.contains(TargetNode))
      | RelayRoute =>
          not(visited.contains(RelayNode)) and not(visited.contains(TargetNode))
      | NoRoute => true
    }

  pure def routeUsesFailedEdge(plan: RoutePlan, failed: Set[Edge]): bool =
    match plan {
      | DirectRoute => failed.contains(DirectN1N2)
      | RelayRoute => failed.contains(RelayN1N3) or failed.contains(RelayN3N2)
      | NoRoute => false
    }

  pure def directAllowed(
    states: Edge -> EdgeState,
    failed: Set[Edge],
    visited: Set[Node]
  ): bool =
    edgeAllowed(states, failed, DirectN1N2) and not(visited.contains(TargetNode))

  pure def relayAllowed(
    states: Edge -> EdgeState,
    failed: Set[Edge],
    visited: Set[Node]
  ): bool =
    all {
      edgeAllowed(states, failed, RelayN1N3),
      edgeAllowed(states, failed, RelayN3N2),
      not(visited.contains(RelayNode)),
      not(visited.contains(TargetNode)),
    }

  pure def directCost(states: Edge -> EdgeState): int =
    stateCost(states.get(DirectN1N2))

  pure def relayCost(states: Edge -> EdgeState): int =
    stateCost(states.get(RelayN1N3)) + stateCost(states.get(RelayN3N2))

  /// Direct target endpoints are preferred over relayed routes when the direct
  /// target edge is healthy. Relay latency can be lower, but a verified direct
  /// data path should not be starved behind a tunnel/relay fallback.
  pure def healthyDirectTargetPreferred(
    states: Edge -> EdgeState,
    failed: Set[Edge],
    visited: Set[Node]
  ): bool =
    directAllowed(states, failed, visited)
      and states.get(DirectN1N2) == Healthy

  pure def plannedRoute(
    states: Edge -> EdgeState,
    failed: Set[Edge],
    visited: Set[Node]
  ): RoutePlan =
    if (healthyDirectTargetPreferred(states, failed, visited)) {
      DirectRoute
    } else if (
      directAllowed(states, failed, visited)
        and (not(relayAllowed(states, failed, visited))
          or directCost(states) <= relayCost(states))
    ) {
      DirectRoute
    } else if (relayAllowed(states, failed, visited)) {
      RelayRoute
    } else {
      NoRoute
    }

  action init = all {
    phase' = Idle,
    edgeState' = initialEdgeState,
    failedEdges' = Set(),
    visitedNodes' = Set(),
    selectedPlan' = NoRoute,
    lastDecision' = NoDecision,
  }

  action planRoute(
    directState: EdgeState,
    relayFirstState: EdgeState,
    relaySecondState: EdgeState,
    directFailed: bool,
    relayFirstFailed: bool,
    relaySecondFailed: bool,
    targetVisited: bool,
    relayVisited: bool
  ): bool = {
    val states = Edges.mapBy(edge =>
      if (edge == DirectN1N2) {
        directState
      } else if (edge == RelayN1N3) {
        relayFirstState
      } else {
        relaySecondState
      }
    )
    val failed =
      (if (directFailed) { Set(DirectN1N2) } else { Set() })
        .union(if (relayFirstFailed) { Set(RelayN1N3) } else { Set() })
        .union(if (relaySecondFailed) { Set(RelayN3N2) } else { Set() })
    val visited =
      (if (targetVisited) { Set(TargetNode) } else { Set() })
        .union(if (relayVisited) { Set(RelayNode) } else { Set() })
    all {
      phase == Idle,
      phase' = Planned,
      edgeState' = states,
      failedEdges' = failed,
      visitedNodes' = visited,
      selectedPlan' = plannedRoute(states, failed, visited),
      lastDecision' = RoutePlanned,
    }
  }

  action stutterPlanned: bool = all {
    phase == Planned,
    phase' = phase,
    edgeState' = edgeState,
    failedEdges' = failedEdges,
    visitedNodes' = visitedNodes,
    selectedPlan' = selectedPlan,
    lastDecision' = TerminalStutter,
  }

  action step = any {
    {
      nondet directState = oneOf(EdgeStates)
      nondet relayFirstState = oneOf(EdgeStates)
      nondet relaySecondState = oneOf(EdgeStates)
      nondet directFailed = oneOf(Set(true, false))
      nondet relayFirstFailed = oneOf(Set(true, false))
      nondet relaySecondFailed = oneOf(Set(true, false))
      nondet targetVisited = oneOf(Set(true, false))
      nondet relayVisited = oneOf(Set(true, false))
      planRoute(
        directState,
        relayFirstState,
        relaySecondState,
        directFailed,
        relayFirstFailed,
        relaySecondFailed,
        targetVisited,
        relayVisited
      )
    },
    stutterPlanned,
  }
}
