/// Cluster endpoint selection safety for command-style main-daemon routing.
///
/// This module models the candidate filtering/ranking boundary in
/// `crates/sinomo-network/src/endpoint_selector.rs`:
/// - only authorized remote main-daemon members are command targets;
/// - shadow and local-only endpoints are not selected for remote main-daemon
///   commands;
/// - identity-mismatch connectivity evidence excludes an endpoint;
/// - module-owned transports may remain as last-resort evidence, but enabled
///   built-in/dialable endpoints are preferred first;
/// - unique-node selection keeps at most one endpoint per node.
module network_endpoint_selection {
  import common.* from "../../lib/common"

  type Endpoint =
    LocalLoopback
    | N2MainLan
    | N2ShadowLan
    | N2LocalOnly
    | N2Iroh
    | N3MainLan
    | N3LocalOnly

  type MemberState = Authorized | Pending | Removed
  type NodeAvailability = Available | Offline | UnknownAvailability
  type EndpointPrivacy = LocalOnly | Lan | InternetDirect | Relayed
  type Transport = Loopback | LanTcp | Iroh | HttpsRelay
  type EndpointRole = MainDaemon | ShadowDaemon
  type Connectivity = Unknown | Healthy | Degraded | Unreachable | IdentityMismatch
  type SelectionPhase = Idle | Selected
  type Decision = NoDecision | SelectionComputed | TerminalStutter

  pure val LocalNode: Node = N1
  pure val Endpoints: Set[Endpoint] = Set(
    LocalLoopback,
    N2MainLan,
    N2ShadowLan,
    N2LocalOnly,
    N2Iroh,
    N3MainLan,
    N3LocalOnly
  )
  pure val MemberStates: Set[MemberState] = Set(Authorized, Pending, Removed)
  pure val Availabilities: Set[NodeAvailability] =
    Set(Available, Offline, UnknownAvailability)
  pure val Connectivities: Set[Connectivity] =
    Set(Unknown, Healthy, Degraded, Unreachable, IdentityMismatch)

  var phase: SelectionPhase
  var memberState: Node -> MemberState
  var availability: Node -> NodeAvailability
  var enabledModuleTransports: Set[Transport]
  var connectivity: Endpoint -> Connectivity
  var selectedCandidates: Set[Endpoint]
  var uniqueNodeTargets: Set[Endpoint]
  var firstCandidate: Option[Endpoint]
  var lastDecision: Decision

  pure val initialMemberState: Node -> MemberState =
    Nodes.mapBy(node => if (node == LocalNode) { Authorized } else { Removed })

  pure val initialAvailability: Node -> NodeAvailability =
    Nodes.mapBy(_ => UnknownAvailability)

  pure val initialConnectivity: Endpoint -> Connectivity =
    Endpoints.mapBy(_ => Unknown)

  pure def endpointNode(endpoint: Endpoint): Node =
    match endpoint {
      | LocalLoopback => N1
      | N2MainLan => N2
      | N2ShadowLan => N2
      | N2LocalOnly => N2
      | N2Iroh => N2
      | N3MainLan => N3
      | N3LocalOnly => N3
    }

  pure def endpointPrivacy(endpoint: Endpoint): EndpointPrivacy =
    match endpoint {
      | LocalLoopback => LocalOnly
      | N2MainLan => Lan
      | N2ShadowLan => Lan
      | N2LocalOnly => LocalOnly
      | N2Iroh => Relayed
      | N3MainLan => InternetDirect
      | N3LocalOnly => LocalOnly
    }

  pure def endpointTransport(endpoint: Endpoint): Transport =
    match endpoint {
      | LocalLoopback => Loopback
      | N2MainLan => LanTcp
      | N2ShadowLan => LanTcp
      | N2LocalOnly => Loopback
      | N2Iroh => Iroh
      | N3MainLan => LanTcp
      | N3LocalOnly => Loopback
    }

  pure def endpointRole(endpoint: Endpoint): EndpointRole =
    match endpoint {
      | N2ShadowLan => ShadowDaemon
      | _ => MainDaemon
    }

  pure def builtinDialable(transport: Transport): bool =
    transport == Loopback or transport == LanTcp

  pure def transportDialable(
    transport: Transport,
    enabledModules: Set[Transport]
  ): bool =
    builtinDialable(transport) or enabledModules.contains(transport)

  pure def endpointDialable(
    endpoint: Endpoint,
    enabledModules: Set[Transport]
  ): bool =
    transportDialable(endpointTransport(endpoint), enabledModules)

  pure def connectivityUsableForCommand(connectivityValue: Connectivity): bool =
    connectivityValue == Healthy or connectivityValue == Unknown

  pure def validMainDaemonCommandCandidate(
    states: Node -> MemberState,
    availabilities: Node -> NodeAvailability,
    connectivities: Endpoint -> Connectivity,
    endpoint: Endpoint
  ): bool =
    all {
      endpointNode(endpoint) != LocalNode,
      states.get(endpointNode(endpoint)) == Authorized,
      availabilities.get(endpointNode(endpoint)) != Offline,
      endpointRole(endpoint) == MainDaemon,
      endpointPrivacy(endpoint) != LocalOnly,
      connectivityUsableForCommand(connectivities.get(endpoint)),
    }

  pure def candidateSetFor(
    states: Node -> MemberState,
    availabilities: Node -> NodeAvailability,
    connectivities: Endpoint -> Connectivity
  ): Set[Endpoint] =
    Endpoints.filter(endpoint =>
      validMainDaemonCommandCandidate(
        states,
        availabilities,
        connectivities,
        endpoint
      )
    )

  pure def orderedBest(candidates: Set[Endpoint]): Option[Endpoint] =
    if (candidates.contains(N2MainLan)) {
      Some(N2MainLan)
    } else if (candidates.contains(N3MainLan)) {
      Some(N3MainLan)
    } else if (candidates.contains(N2Iroh)) {
      Some(N2Iroh)
    } else {
      None
    }

  pure def bestCandidate(
    candidates: Set[Endpoint],
    enabledModules: Set[Transport]
  ): Option[Endpoint] = {
    val dialable = candidates.filter(endpoint => endpointDialable(endpoint, enabledModules))
    if (dialable.size() > 0) {
      orderedBest(dialable)
    } else {
      orderedBest(candidates)
    }
  }

  pure def bestCandidateForNode(
    candidates: Set[Endpoint],
    enabledModules: Set[Transport],
    node: Node
  ): Option[Endpoint] =
    bestCandidate(
      candidates.filter(endpoint => endpointNode(endpoint) == node),
      enabledModules
    )

  pure def chosenNodeTargetSet(
    candidates: Set[Endpoint],
    enabledModules: Set[Transport],
    node: Node
  ): Set[Endpoint] =
    match bestCandidateForNode(candidates, enabledModules, node) {
      | Some(endpoint) => Set(endpoint)
      | None => Set()
    }

  pure def uniqueNodeTargetSet(
    candidates: Set[Endpoint],
    enabledModules: Set[Transport]
  ): Set[Endpoint] =
    chosenNodeTargetSet(candidates, enabledModules, N2)
      .union(chosenNodeTargetSet(candidates, enabledModules, N3))

  action init = all {
    phase' = Idle,
    memberState' = initialMemberState,
    availability' = initialAvailability,
    enabledModuleTransports' = Set(),
    connectivity' = initialConnectivity,
    selectedCandidates' = Set(),
    uniqueNodeTargets' = Set(),
    firstCandidate' = None,
    lastDecision' = NoDecision,
  }

  action computeSelection(
    n2State: MemberState,
    n3State: MemberState,
    n2Availability: NodeAvailability,
    n3Availability: NodeAvailability,
    irohEnabled: bool,
    n2LanConnectivity: Connectivity,
    n2IrohConnectivity: Connectivity,
    n3LanConnectivity: Connectivity
  ): bool = {
    val states = Nodes.mapBy(node =>
      if (node == N1) {
        Authorized
      } else if (node == N2) {
        n2State
      } else {
        n3State
      }
    )
    val availabilities = Nodes.mapBy(node =>
      if (node == N1) {
        Available
      } else if (node == N2) {
        n2Availability
      } else {
        n3Availability
      }
    )
    val modules = if (irohEnabled) { Set(Iroh) } else { Set() }
    val connectivities = Endpoints.mapBy(endpoint =>
      if (endpoint == N2MainLan) {
        n2LanConnectivity
      } else if (endpoint == N2Iroh) {
        n2IrohConnectivity
      } else if (endpoint == N3MainLan) {
        n3LanConnectivity
      } else {
        Unknown
      }
    )
    val candidates = candidateSetFor(states, availabilities, connectivities)
    all {
      phase == Idle,
      phase' = Selected,
      memberState' = states,
      availability' = availabilities,
      enabledModuleTransports' = modules,
      connectivity' = connectivities,
      selectedCandidates' = candidates,
      uniqueNodeTargets' = uniqueNodeTargetSet(candidates, modules),
      firstCandidate' = bestCandidate(candidates, modules),
      lastDecision' = SelectionComputed,
    }
  }

  action stutterSelected: bool = all {
    phase == Selected,
    phase' = phase,
    memberState' = memberState,
    availability' = availability,
    enabledModuleTransports' = enabledModuleTransports,
    connectivity' = connectivity,
    selectedCandidates' = selectedCandidates,
    uniqueNodeTargets' = uniqueNodeTargets,
    firstCandidate' = firstCandidate,
    lastDecision' = TerminalStutter,
  }

  action step = any {
    {
      nondet n2State = oneOf(MemberStates)
      nondet n3State = oneOf(MemberStates)
      nondet n2Availability = oneOf(Availabilities)
      nondet n3Availability = oneOf(Availabilities)
      nondet irohEnabled = oneOf(Set(true, false))
      nondet n2LanConnectivity = oneOf(Connectivities)
      nondet n2IrohConnectivity = oneOf(Connectivities)
      nondet n3LanConnectivity = oneOf(Connectivities)
      computeSelection(
        n2State,
        n3State,
        n2Availability,
        n3Availability,
        irohEnabled,
        n2LanConnectivity,
        n2IrohConnectivity,
        n3LanConnectivity
      )
    },
    stutterSelected,
  }
}
