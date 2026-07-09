/// Shared full-system foundation model for cross-layer integration checks.
///
/// This module is deliberately abstract. It owns the common vocabulary and
/// state shared by full-system smoke models: membership, endpoint identity,
/// connectivity observations, route selection, delivery keys, DFS metadata
/// authority, and visible DFS writes. Focused layer specs prove the lower-level
/// algorithms; this base model composes their minimal public effects.
module system_base {
  import common.* from "./common"

  type SystemMemberState = MemberAuthorized | MemberPending | MemberRemoved
  type SystemEndpoint = EndpointN2Main | EndpointN2Shadow | EndpointN3Main
  type SystemEndpointRole = EndpointMainDaemon | EndpointShadowDaemon
  type SystemEndpointPrivacy = EndpointLan | EndpointLocalOnly | EndpointInternet
  type SystemConnectivity =
    ConnUnknown
    | ConnHealthy
    | ConnDegraded
    | ConnUnreachable
    | ConnIdentityMismatch
  type SystemDeliveryKey = SystemKeyA | SystemKeyB
  type SystemDecision =
    SystemNoDecision
    | SystemMemberChanged
    | SystemPrimaryChanged
    | SystemConnectivityObserved
    | SystemRouteSelected
    | SystemNoRoute
    | SystemResponseAccepted
    | SystemResponseIgnored
    | SystemDuplicateReplayed
    | SystemRouteTimedOut

  pure val SystemLocalNode: Node = N1
  pure val SystemEndpoints: Set[SystemEndpoint] =
    Set(EndpointN2Main, EndpointN2Shadow, EndpointN3Main)
  pure val SystemRemoteNodes: Set[Node] = Set(N2, N3)
  pure val SystemMemberStates: Set[SystemMemberState] =
    Set(MemberAuthorized, MemberPending, MemberRemoved)
  pure val SystemConnectivities: Set[SystemConnectivity] =
    Set(ConnUnknown, ConnHealthy, ConnDegraded, ConnUnreachable, ConnIdentityMismatch)
  pure val SystemDeliveryKeys: Set[SystemDeliveryKey] = Set(SystemKeyA, SystemKeyB)

  var systemMemberState: Node -> SystemMemberState
  var systemEndpointConnectivity: SystemEndpoint -> SystemConnectivity
  var systemSuppressedEndpoints: Set[SystemEndpoint]
  var systemDfsPrimary: Node
  var systemPendingDfsWrite: bool
  var systemRoutedTarget: Option[Node]
  var systemRoutedEndpoint: Option[SystemEndpoint]
  var systemRoutedDeliveryKey: Option[SystemDeliveryKey]
  var systemVisibleAuthorities: Set[Node]
  var systemReplayCache: Set[SystemDeliveryKey]
  var systemLastResponseSource: Option[Node]
  var systemLastResponseKey: Option[SystemDeliveryKey]
  var systemLastResponseMatchedPending: bool
  var systemLastResponseFromPrimary: bool
  var systemLastResponseAccepted: bool
  var systemLastCommitSource: Option[Node]
  var systemLastCommitKey: Option[SystemDeliveryKey]
  var systemLastCommitWasAuthorized: bool
  var systemLastCommitPrimaryMatched: bool
  var systemLastDecision: SystemDecision

  pure val systemInitialMembers: Node -> SystemMemberState =
    Nodes.mapBy(node =>
      if (node == N1 or node == N2) { MemberAuthorized } else { MemberPending }
    )

  pure val systemInitialConnectivity: SystemEndpoint -> SystemConnectivity =
    SystemEndpoints.mapBy(endpoint =>
      if (endpoint == EndpointN2Main) { ConnHealthy } else { ConnUnknown }
    )

  pure def systemEndpointNode(endpoint: SystemEndpoint): Node =
    match endpoint {
      | EndpointN2Main => N2
      | EndpointN2Shadow => N2
      | EndpointN3Main => N3
    }

  pure def systemEndpointRole(endpoint: SystemEndpoint): SystemEndpointRole =
    match endpoint {
      | EndpointN2Shadow => EndpointShadowDaemon
      | _ => EndpointMainDaemon
    }

  pure def systemEndpointPrivacy(endpoint: SystemEndpoint): SystemEndpointPrivacy =
    match endpoint {
      | EndpointN2Shadow => EndpointLan
      | EndpointN2Main => EndpointLan
      | EndpointN3Main => EndpointInternet
    }

  pure def systemMemberAuthorized(
    members: Node -> SystemMemberState,
    node: Node
  ): bool =
    members.get(node) == MemberAuthorized

  pure def systemConnectivitySelectable(connectivity: SystemConnectivity): bool =
    any {
      connectivity == ConnHealthy,
      connectivity == ConnUnknown,
      connectivity == ConnDegraded,
    }

  pure def systemEndpointSelectableWith(
    members: Node -> SystemMemberState,
    connectivities: SystemEndpoint -> SystemConnectivity,
    suppressed: Set[SystemEndpoint],
    endpoint: SystemEndpoint
  ): bool =
    all {
      systemEndpointRole(endpoint) == EndpointMainDaemon,
      systemEndpointPrivacy(endpoint) != EndpointLocalOnly,
      systemMemberAuthorized(members, systemEndpointNode(endpoint)),
      systemConnectivitySelectable(connectivities.get(endpoint)),
      not(suppressed.contains(endpoint)),
    }

  def systemEndpointSelectable(endpoint: SystemEndpoint): bool =
    systemEndpointSelectableWith(
      systemMemberState,
      systemEndpointConnectivity,
      systemSuppressedEndpoints,
      endpoint
    )

  pure def systemBestEndpointForNodeWith(
    members: Node -> SystemMemberState,
    connectivities: SystemEndpoint -> SystemConnectivity,
    suppressed: Set[SystemEndpoint],
    node: Node
  ): Option[SystemEndpoint] =
    if (
      node == N2
        and systemEndpointSelectableWith(members, connectivities, suppressed, EndpointN2Main)
    ) {
      Some(EndpointN2Main)
    } else if (
      node == N3
        and systemEndpointSelectableWith(members, connectivities, suppressed, EndpointN3Main)
    ) {
      Some(EndpointN3Main)
    } else {
      None
    }

  def systemBestEndpointForNode(node: Node): Option[SystemEndpoint] =
    systemBestEndpointForNodeWith(
      systemMemberState,
      systemEndpointConnectivity,
      systemSuppressedEndpoints,
      node
    )

  pure def systemUpdateMembersForRemoval(
    members: Node -> SystemMemberState,
    node: Node,
    newState: SystemMemberState
  ): Node -> SystemMemberState =
    members.set(node, newState)

  pure def systemRouteStillValidAfterMemberChange(
    oldTarget: Option[Node],
    changedNode: Node,
    newState: SystemMemberState
  ): bool =
    match oldTarget {
      | Some(target) => target != changedNode or newState == MemberAuthorized
      | None => true
    }

  action systemInit = all {
    systemMemberState' = systemInitialMembers,
    systemEndpointConnectivity' = systemInitialConnectivity,
    systemSuppressedEndpoints' = Set(),
    systemDfsPrimary' = N2,
    systemPendingDfsWrite' = false,
    systemRoutedTarget' = None,
    systemRoutedEndpoint' = None,
    systemRoutedDeliveryKey' = None,
    systemVisibleAuthorities' = Set(),
    systemReplayCache' = Set(),
    systemLastResponseSource' = None,
    systemLastResponseKey' = None,
    systemLastResponseMatchedPending' = false,
    systemLastResponseFromPrimary' = false,
    systemLastResponseAccepted' = false,
    systemLastCommitSource' = None,
    systemLastCommitKey' = None,
    systemLastCommitWasAuthorized' = false,
    systemLastCommitPrimaryMatched' = false,
    systemLastDecision' = SystemNoDecision,
  }

  action systemSetMemberState(node: Node, newState: SystemMemberState): bool = {
    val routeStillValid =
      systemRouteStillValidAfterMemberChange(systemRoutedTarget, node, newState)
    all {
      node != SystemLocalNode,
      systemMemberState' =
        systemUpdateMembersForRemoval(systemMemberState, node, newState),
      systemEndpointConnectivity' = systemEndpointConnectivity,
      systemSuppressedEndpoints' = systemSuppressedEndpoints,
      systemDfsPrimary' = systemDfsPrimary,
      systemPendingDfsWrite' = if (routeStillValid) { systemPendingDfsWrite } else { false },
      systemRoutedTarget' = if (routeStillValid) { systemRoutedTarget } else { None },
      systemRoutedEndpoint' = if (routeStillValid) { systemRoutedEndpoint } else { None },
      systemRoutedDeliveryKey' = if (routeStillValid) { systemRoutedDeliveryKey } else { None },
      systemVisibleAuthorities' = systemVisibleAuthorities,
      systemReplayCache' = systemReplayCache,
      systemLastResponseSource' = systemLastResponseSource,
      systemLastResponseKey' = systemLastResponseKey,
      systemLastResponseMatchedPending' = false,
      systemLastResponseFromPrimary' = false,
      systemLastResponseAccepted' = false,
      systemLastCommitSource' = systemLastCommitSource,
      systemLastCommitKey' = systemLastCommitKey,
      systemLastCommitWasAuthorized' = systemLastCommitWasAuthorized,
      systemLastCommitPrimaryMatched' = systemLastCommitPrimaryMatched,
      systemLastDecision' = SystemMemberChanged,
    }
  }

  action systemSetDfsPrimary(node: Node): bool = all {
    SystemRemoteNodes.contains(node),
    systemMemberAuthorized(systemMemberState, node),
    systemMemberState' = systemMemberState,
    systemEndpointConnectivity' = systemEndpointConnectivity,
    systemSuppressedEndpoints' = systemSuppressedEndpoints,
    systemDfsPrimary' = node,
    systemPendingDfsWrite' = false,
    systemRoutedTarget' = None,
    systemRoutedEndpoint' = None,
    systemRoutedDeliveryKey' = None,
    systemVisibleAuthorities' = systemVisibleAuthorities,
    systemReplayCache' = systemReplayCache,
    systemLastResponseSource' = systemLastResponseSource,
    systemLastResponseKey' = systemLastResponseKey,
    systemLastResponseMatchedPending' = false,
    systemLastResponseFromPrimary' = false,
    systemLastResponseAccepted' = false,
    systemLastCommitSource' = systemLastCommitSource,
    systemLastCommitKey' = systemLastCommitKey,
    systemLastCommitWasAuthorized' = systemLastCommitWasAuthorized,
    systemLastCommitPrimaryMatched' = systemLastCommitPrimaryMatched,
    systemLastDecision' = SystemPrimaryChanged,
  }

  action systemObserveConnectivity(
    endpoint: SystemEndpoint,
    observed: SystemConnectivity
  ): bool = {
    val suppressed =
      if (observed == ConnIdentityMismatch) {
        systemSuppressedEndpoints.union(Set(endpoint))
      } else {
        systemSuppressedEndpoints
      }
    val routeStillValid =
      not(systemRoutedEndpoint == Some(endpoint)
        and (observed == ConnIdentityMismatch or observed == ConnUnreachable))
    all {
      systemMemberState' = systemMemberState,
      systemEndpointConnectivity' = systemEndpointConnectivity.set(endpoint, observed),
      systemSuppressedEndpoints' = suppressed,
      systemDfsPrimary' = systemDfsPrimary,
      systemPendingDfsWrite' = if (routeStillValid) { systemPendingDfsWrite } else { false },
      systemRoutedTarget' = if (routeStillValid) { systemRoutedTarget } else { None },
      systemRoutedEndpoint' = if (routeStillValid) { systemRoutedEndpoint } else { None },
      systemRoutedDeliveryKey' = if (routeStillValid) { systemRoutedDeliveryKey } else { None },
      systemVisibleAuthorities' = systemVisibleAuthorities,
      systemReplayCache' = systemReplayCache,
      systemLastResponseSource' = systemLastResponseSource,
      systemLastResponseKey' = systemLastResponseKey,
      systemLastResponseMatchedPending' = false,
      systemLastResponseFromPrimary' = false,
      systemLastResponseAccepted' = false,
      systemLastCommitSource' = systemLastCommitSource,
      systemLastCommitKey' = systemLastCommitKey,
      systemLastCommitWasAuthorized' = systemLastCommitWasAuthorized,
      systemLastCommitPrimaryMatched' = systemLastCommitPrimaryMatched,
      systemLastDecision' = SystemConnectivityObserved,
    }
  }

  action systemRouteDfsWrite(key: SystemDeliveryKey): bool = {
    val endpoint = systemBestEndpointForNode(systemDfsPrimary)
    all {
      endpoint != None,
      systemMemberState' = systemMemberState,
      systemEndpointConnectivity' = systemEndpointConnectivity,
      systemSuppressedEndpoints' = systemSuppressedEndpoints,
      systemDfsPrimary' = systemDfsPrimary,
      systemPendingDfsWrite' = true,
      systemRoutedTarget' = Some(systemDfsPrimary),
      systemRoutedEndpoint' = endpoint,
      systemRoutedDeliveryKey' = Some(key),
      systemVisibleAuthorities' = systemVisibleAuthorities,
      systemReplayCache' = systemReplayCache,
      systemLastResponseSource' = None,
      systemLastResponseKey' = None,
      systemLastResponseMatchedPending' = false,
      systemLastResponseFromPrimary' = false,
      systemLastResponseAccepted' = false,
      systemLastCommitSource' = systemLastCommitSource,
      systemLastCommitKey' = systemLastCommitKey,
      systemLastCommitWasAuthorized' = systemLastCommitWasAuthorized,
      systemLastCommitPrimaryMatched' = systemLastCommitPrimaryMatched,
      systemLastDecision' = SystemRouteSelected,
    }
  }

  action systemNoRouteForDfsWrite: bool = all {
    systemBestEndpointForNode(systemDfsPrimary) == None,
    systemMemberState' = systemMemberState,
    systemEndpointConnectivity' = systemEndpointConnectivity,
    systemSuppressedEndpoints' = systemSuppressedEndpoints,
    systemDfsPrimary' = systemDfsPrimary,
    systemPendingDfsWrite' = false,
    systemRoutedTarget' = None,
    systemRoutedEndpoint' = None,
    systemRoutedDeliveryKey' = None,
    systemVisibleAuthorities' = systemVisibleAuthorities,
    systemReplayCache' = systemReplayCache,
    systemLastResponseSource' = None,
    systemLastResponseKey' = None,
    systemLastResponseMatchedPending' = false,
    systemLastResponseFromPrimary' = false,
    systemLastResponseAccepted' = false,
    systemLastCommitSource' = systemLastCommitSource,
    systemLastCommitKey' = systemLastCommitKey,
    systemLastCommitWasAuthorized' = systemLastCommitWasAuthorized,
    systemLastCommitPrimaryMatched' = systemLastCommitPrimaryMatched,
    systemLastDecision' = SystemNoRoute,
  }

  action systemAcceptDfsResponse(source: Node, key: SystemDeliveryKey): bool = all {
    systemPendingDfsWrite,
    systemRoutedTarget == Some(source),
    systemRoutedDeliveryKey == Some(key),
    source == systemDfsPrimary,
    systemMemberAuthorized(systemMemberState, source),
    systemMemberState' = systemMemberState,
    systemEndpointConnectivity' = systemEndpointConnectivity,
    systemSuppressedEndpoints' = systemSuppressedEndpoints,
    systemDfsPrimary' = systemDfsPrimary,
    systemPendingDfsWrite' = false,
    systemRoutedTarget' = None,
    systemRoutedEndpoint' = None,
    systemRoutedDeliveryKey' = None,
    systemVisibleAuthorities' = Set(source),
    systemReplayCache' = systemReplayCache.union(Set(key)),
    systemLastResponseSource' = Some(source),
    systemLastResponseKey' = Some(key),
    systemLastResponseMatchedPending' = true,
    systemLastResponseFromPrimary' = true,
    systemLastResponseAccepted' = true,
    systemLastCommitSource' = Some(source),
    systemLastCommitKey' = Some(key),
    systemLastCommitWasAuthorized' = true,
    systemLastCommitPrimaryMatched' = true,
    systemLastDecision' = SystemResponseAccepted,
  }

  action systemIgnoreDfsResponse(source: Node, key: SystemDeliveryKey): bool = {
    val keyMatched = systemRoutedDeliveryKey == Some(key)
    val targetMatched = systemRoutedTarget == Some(source)
    val sourceWasPrimary = source == systemDfsPrimary
    all {
      any {
        not(systemPendingDfsWrite),
        not(keyMatched),
        not(targetMatched),
        not(sourceWasPrimary),
        not(systemMemberAuthorized(systemMemberState, source)),
      },
      systemMemberState' = systemMemberState,
      systemEndpointConnectivity' = systemEndpointConnectivity,
      systemSuppressedEndpoints' = systemSuppressedEndpoints,
      systemDfsPrimary' = systemDfsPrimary,
      systemPendingDfsWrite' = systemPendingDfsWrite,
      systemRoutedTarget' = systemRoutedTarget,
      systemRoutedEndpoint' = systemRoutedEndpoint,
      systemRoutedDeliveryKey' = systemRoutedDeliveryKey,
      systemVisibleAuthorities' = systemVisibleAuthorities,
      systemReplayCache' = systemReplayCache,
      systemLastResponseSource' = Some(source),
      systemLastResponseKey' = Some(key),
      systemLastResponseMatchedPending' = keyMatched and targetMatched,
      systemLastResponseFromPrimary' = sourceWasPrimary,
      systemLastResponseAccepted' = false,
      systemLastCommitSource' = systemLastCommitSource,
      systemLastCommitKey' = systemLastCommitKey,
      systemLastCommitWasAuthorized' = systemLastCommitWasAuthorized,
      systemLastCommitPrimaryMatched' = systemLastCommitPrimaryMatched,
      systemLastDecision' = SystemResponseIgnored,
    }
  }

  action systemReplayDuplicateResponse(key: SystemDeliveryKey): bool = all {
    systemReplayCache.contains(key),
    systemMemberState' = systemMemberState,
    systemEndpointConnectivity' = systemEndpointConnectivity,
    systemSuppressedEndpoints' = systemSuppressedEndpoints,
    systemDfsPrimary' = systemDfsPrimary,
    systemPendingDfsWrite' = systemPendingDfsWrite,
    systemRoutedTarget' = systemRoutedTarget,
    systemRoutedEndpoint' = systemRoutedEndpoint,
    systemRoutedDeliveryKey' = systemRoutedDeliveryKey,
    systemVisibleAuthorities' = systemVisibleAuthorities,
    systemReplayCache' = systemReplayCache,
    systemLastResponseSource' = systemLastResponseSource,
    systemLastResponseKey' = systemLastResponseKey,
    systemLastResponseMatchedPending' = true,
    systemLastResponseFromPrimary' = true,
    systemLastResponseAccepted' = false,
    systemLastCommitSource' = systemLastCommitSource,
    systemLastCommitKey' = systemLastCommitKey,
    systemLastCommitWasAuthorized' = systemLastCommitWasAuthorized,
    systemLastCommitPrimaryMatched' = systemLastCommitPrimaryMatched,
    systemLastDecision' = SystemDuplicateReplayed,
  }

  action systemTimeoutRoute: bool = all {
    systemPendingDfsWrite,
    systemMemberState' = systemMemberState,
    systemEndpointConnectivity' = systemEndpointConnectivity,
    systemSuppressedEndpoints' = systemSuppressedEndpoints,
    systemDfsPrimary' = systemDfsPrimary,
    systemPendingDfsWrite' = false,
    systemRoutedTarget' = None,
    systemRoutedEndpoint' = None,
    systemRoutedDeliveryKey' = None,
    systemVisibleAuthorities' = systemVisibleAuthorities,
    systemReplayCache' = systemReplayCache,
    systemLastResponseSource' = systemLastResponseSource,
    systemLastResponseKey' = systemLastResponseKey,
    systemLastResponseMatchedPending' = false,
    systemLastResponseFromPrimary' = false,
    systemLastResponseAccepted' = false,
    systemLastCommitSource' = systemLastCommitSource,
    systemLastCommitKey' = systemLastCommitKey,
    systemLastCommitWasAuthorized' = systemLastCommitWasAuthorized,
    systemLastCommitPrimaryMatched' = systemLastCommitPrimaryMatched,
    systemLastDecision' = SystemRouteTimedOut,
  }

  action systemStep = any {
    {
      nondet node = oneOf(SystemRemoteNodes)
      nondet newState = oneOf(SystemMemberStates)
      systemSetMemberState(node, newState)
    },
    {
      nondet node = oneOf(SystemRemoteNodes)
      systemSetDfsPrimary(node)
    },
    {
      nondet endpoint = oneOf(SystemEndpoints)
      nondet observed = oneOf(SystemConnectivities)
      systemObserveConnectivity(endpoint, observed)
    },
    {
      nondet key = oneOf(SystemDeliveryKeys)
      systemRouteDfsWrite(key)
    },
    systemNoRouteForDfsWrite,
    {
      nondet source = oneOf(Nodes)
      nondet key = oneOf(SystemDeliveryKeys)
      any {
        systemAcceptDfsResponse(source, key),
        systemIgnoreDfsResponse(source, key),
      }
    },
    {
      nondet key = oneOf(SystemDeliveryKeys)
      systemReplayDuplicateResponse(key)
    },
    systemTimeoutRoute,
  }
}
