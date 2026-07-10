/// Layered full-system guarantees over the shared system base model.
///
/// These predicates are the assume/guarantee seam for integration-level models.
/// Focused specs prove the lower-layer algorithms separately; this file states
/// the cross-layer guarantees the full-system smoke model expects to preserve.
module system_guarantees {
  import common.* from "../../lib/common"
  import system_base.* from "../../models/system/base"

  def activeRouteEndpointIsSelectable: bool =
    match systemRoutedEndpoint {
      | Some(endpoint) => systemEndpointSelectable(endpoint)
      | None => true
    }

  def activeRouteTargetMatchesEndpoint: bool =
    match systemRoutedEndpoint {
      | Some(endpoint) =>
          match systemRoutedTarget {
            | Some(target) => systemEndpointNode(endpoint) == target
            | None => false
          }
      | None => systemRoutedTarget == None
    }

  def activeRouteTargetIsAuthorized: bool =
    match systemRoutedTarget {
      | Some(target) => systemMemberAuthorized(systemMemberState, target)
      | None => true
    }

  def activeRouteUsesNoShadowEndpoint: bool =
    match systemRoutedEndpoint {
      | Some(endpoint) => systemEndpointRole(endpoint) == EndpointMainDaemon
      | None => true
    }

  def identityMismatchEndpointsAreSuppressed: bool =
    SystemEndpoints.forall(endpoint =>
      systemEndpointConnectivity.get(endpoint) == ConnIdentityMismatch
        implies systemSuppressedEndpoints.contains(endpoint)
    )

  def suppressedEndpointsAreNotActivelyRouted: bool =
    match systemRoutedEndpoint {
      | Some(endpoint) => not(systemSuppressedEndpoints.contains(endpoint))
      | None => true
    }

  def pendingDfsWriteRoutesToCurrentPrimary: bool =
    systemPendingDfsWrite implies all {
      systemRoutedTarget == Some(systemDfsPrimary),
      activeRouteEndpointIsSelectable,
      activeRouteTargetIsAuthorized,
    }

  def staleResponsesDoNotCommit: bool =
    systemLastResponseAccepted implies all {
      systemLastResponseMatchedPending,
      systemLastResponseFromPrimary,
    }

  def ignoredResponsesDoNotCommit: bool =
    systemLastDecision != SystemResponseIgnored or not(systemLastResponseAccepted)

  def successfulDfsWritesHaveMetadataAuthority: bool =
    systemVisibleAuthorities.size() == 0 or all {
      systemLastCommitWasAuthorized,
      systemLastCommitPrimaryMatched,
      match systemLastCommitSource {
        | Some(source) => systemVisibleAuthorities == Set(source)
        | None => false
      },
    }

  def noObjectVisibleFromTwoAuthorities: bool =
    systemVisibleAuthorities.size() <= 1

  def removedMembersReceiveNoAuthorizedWork: bool =
    match systemRoutedTarget {
      | Some(target) => systemMemberState.get(target) != MemberRemoved
      | None => true
    }

  def shadowEndpointsNeverOwnQuorumOrDfsWork: bool = all {
    activeRouteUsesNoShadowEndpoint,
    SystemEndpoints.forall(endpoint =>
      systemEndpointRole(endpoint) == EndpointShadowDaemon
        implies not(systemVisibleAuthorities.contains(systemEndpointNode(endpoint))
          and systemRoutedEndpoint == Some(endpoint))
    ),
  }

  def deliveryReplayCacheMatchesCommittedKeys: bool =
    match systemLastCommitKey {
      | Some(key) => systemReplayCache.contains(key)
      | None => systemVisibleAuthorities.size() == 0
    }

  def duplicateReplayDoesNotCommitNewResponse: bool =
    systemLastDecision != SystemDuplicateReplayed or not(systemLastResponseAccepted)

  def lowerLayerGuarantees: bool = all {
    activeRouteEndpointIsSelectable,
    activeRouteTargetMatchesEndpoint,
    activeRouteTargetIsAuthorized,
    activeRouteUsesNoShadowEndpoint,
    identityMismatchEndpointsAreSuppressed,
    suppressedEndpointsAreNotActivelyRouted,
    pendingDfsWriteRoutesToCurrentPrimary,
  }

  def integrationGuarantees: bool = all {
    staleResponsesDoNotCommit,
    ignoredResponsesDoNotCommit,
    successfulDfsWritesHaveMetadataAuthority,
    noObjectVisibleFromTwoAuthorities,
    removedMembersReceiveNoAuthorizedWork,
    shadowEndpointsNeverOwnQuorumOrDfsWork,
    deliveryReplayCacheMatchesCommittedKeys,
    duplicateReplayDoesNotCommitNewResponse,
  }

  def systemSafety: bool = all {
    lowerLayerGuarantees,
    integrationGuarantees,
  }
}
