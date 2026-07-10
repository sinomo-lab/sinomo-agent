/// Invariant checks for `network_route_planning`.
module network_route_planning_safety {
  import common.* from "../../lib/common"
  import network_route_planning.* from "../../models/network/route_planning"

  def selectedRouteAvoidsVisitedNodes: bool =
    pathAvoidsVisited(selectedPlan, visitedNodes)


  def selectedRouteUsesNoFailedEdge: bool =
    not(routeUsesFailedEdge(selectedPlan, failedEdges))


  def selectedRouteEdgesAreUsable: bool =
    match selectedPlan {
      | DirectRoute => edgeUsable(DirectN1N2, edgeState.get(DirectN1N2))
      | RelayRoute =>
          edgeUsable(RelayN1N3, edgeState.get(RelayN1N3))
            and edgeUsable(RelayN3N2, edgeState.get(RelayN3N2))
      | NoRoute => true
    }


  def identityMismatchEdgesAreNeverSelected: bool =
    match selectedPlan {
      | DirectRoute => edgeState.get(DirectN1N2) != IdentityMismatch
      | RelayRoute =>
          edgeState.get(RelayN1N3) != IdentityMismatch
            and edgeState.get(RelayN3N2) != IdentityMismatch
      | NoRoute => true
    }


  def relayRouteRequiresCompletePath: bool =
    selectedPlan != RelayRoute or relayAllowed(edgeState, failedEdges, visitedNodes)


  def directRouteRequiresDirectEdge: bool =
    selectedPlan != DirectRoute or directAllowed(edgeState, failedEdges, visitedNodes)


  def directPreferredWhenNoMoreExpensive: bool =
    all {
      directAllowed(edgeState, failedEdges, visitedNodes),
      relayAllowed(edgeState, failedEdges, visitedNodes),
      directCost(edgeState) <= relayCost(edgeState),
    } implies selectedPlan == DirectRoute


  def healthyDirectPreferredEvenWhenRelayIsCheaper: bool =
    all {
      healthyDirectTargetPreferred(edgeState, failedEdges, visitedNodes),
      relayAllowed(edgeState, failedEdges, visitedNodes),
      relayCost(edgeState) < directCost(edgeState),
    } implies selectedPlan == DirectRoute


  def relayChosenAfterFailedDirectWhenAvailable: bool =
    all {
      failedEdges.contains(DirectN1N2),
      relayAllowed(edgeState, failedEdges, visitedNodes),
    } implies selectedPlan == RelayRoute


  def noRouteOnlyWhenNoCompatiblePath: bool =
    selectedPlan != NoRoute
      or not(directAllowed(edgeState, failedEdges, visitedNodes))
        and not(relayAllowed(edgeState, failedEdges, visitedNodes))


  def routePlanMatchesAlgorithm: bool =
    selectedPlan == plannedRoute(edgeState, failedEdges, visitedNodes)


  def selectedPathHasNoLoop: bool =
    match selectedPlan {
      | DirectRoute => LocalNode != TargetNode
      | RelayRoute =>
          LocalNode != RelayNode
            and LocalNode != TargetNode
            and RelayNode != TargetNode
      | NoRoute => true
    }


  def safety: bool = all {
    selectedRouteAvoidsVisitedNodes,
    selectedRouteUsesNoFailedEdge,
    selectedRouteEdgesAreUsable,
    identityMismatchEdgesAreNeverSelected,
    relayRouteRequiresCompletePath,
    directRouteRequiresDirectEdge,
    directPreferredWhenNoMoreExpensive,
    healthyDirectPreferredEvenWhenRelayIsCheaper,
    relayChosenAfterFailedDirectWhenAvailable,
    noRouteOnlyWhenNoCompatiblePath,
    routePlanMatchesAlgorithm,
    selectedPathHasNoLoop,
  }
}
