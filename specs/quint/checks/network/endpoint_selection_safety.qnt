/// Invariant checks for `network_endpoint_selection`.
module network_endpoint_selection_safety {
  import common.* from "../../lib/common"
  import network_endpoint_selection.* from "../../models/network/endpoint_selection"

  def selectedCandidatesAreValid: bool =
    selectedCandidates.forall(endpoint =>
      validMainDaemonCommandCandidate(
        memberState,
        availability,
        connectivity,
        endpoint
      )
    )


  def noShadowEndpointSelected: bool =
    selectedCandidates.forall(endpoint => endpointRole(endpoint) == MainDaemon)


  def noRemoteLocalOnlyEndpointSelected: bool =
    selectedCandidates.forall(endpoint => endpointPrivacy(endpoint) != LocalOnly)


  def noUnauthorizedEndpointSelected: bool =
    selectedCandidates.forall(endpoint =>
      memberState.get(endpointNode(endpoint)) == Authorized
    )


  def noOfflineEndpointSelected: bool =
    selectedCandidates.forall(endpoint =>
      availability.get(endpointNode(endpoint)) != Offline
    )


  def identityMismatchEndpointExcluded: bool =
    selectedCandidates.forall(endpoint =>
      connectivity.get(endpoint) != IdentityMismatch
    )


  def degradedAndUnreachableCommandEndpointsExcluded: bool =
    selectedCandidates.forall(endpoint =>
      not(connectivity.get(endpoint) == Degraded)
        and not(connectivity.get(endpoint) == Unreachable)
    )


  def firstCandidatePrefersDialableWhenAvailable: bool = {
    val dialableCandidates =
      selectedCandidates.filter(endpoint => endpointDialable(endpoint, enabledModuleTransports))
    match firstCandidate {
      | Some(endpoint) =>
          dialableCandidates.size() == 0
            or endpointDialable(endpoint, enabledModuleTransports)
      | None => selectedCandidates.size() == 0
    }
  }


  def uniqueNodeTargetsAreSelectedCandidates: bool =
    uniqueNodeTargets.subseteq(selectedCandidates)


  def uniqueNodeTargetsHaveAtMostOneEndpointPerNode: bool =
    Nodes.forall(node =>
      uniqueNodeTargets.filter(endpoint => endpointNode(endpoint) == node).size() <= 1
    )


  def uniqueNodeTargetsCoverSelectedNodes: bool =
    Nodes.forall(node =>
      selectedCandidates.filter(endpoint => endpointNode(endpoint) == node).size() == 0
        or uniqueNodeTargets.filter(endpoint => endpointNode(endpoint) == node).size() == 1
    )


  def firstCandidateIsSelectedWhenPresent: bool =
    match firstCandidate {
      | Some(endpoint) => selectedCandidates.contains(endpoint)
      | None => selectedCandidates.size() == 0
    }


  def safety: bool = all {
    selectedCandidatesAreValid,
    noShadowEndpointSelected,
    noRemoteLocalOnlyEndpointSelected,
    noUnauthorizedEndpointSelected,
    noOfflineEndpointSelected,
    identityMismatchEndpointExcluded,
    degradedAndUnreachableCommandEndpointsExcluded,
    firstCandidatePrefersDialableWhenAvailable,
    uniqueNodeTargetsAreSelectedCandidates,
    uniqueNodeTargetsHaveAtMostOneEndpointPerNode,
    uniqueNodeTargetsCoverSelectedNodes,
    firstCandidateIsSelectedWhenPresent,
  }
}
