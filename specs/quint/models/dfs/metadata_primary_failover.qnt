/// DFS metadata-shard local failover-target selection.
///
/// This module refines the abstract primary-change action in `dfs_metadata`.
/// It models the local decision used during default cluster bootstrap: a node
/// may target itself as replacement metadata primary only when the configured
/// primary is unreachable and the local node is a durable replica with local
/// shard status proving it has replayed metadata.
///
/// This decision does not commit a primary or authority epoch and does not
/// authorize a canonical mutation. The historical `PromoteLocalReplica` label
/// means "select as the local failover candidate"; a caller must still obtain
/// the quorum-backed fencing proof modeled by the canonical authority slices.
module dfs_metadata_primary_failover {
  import common.* from "../../lib/common"

  type FailoverPhase = Idle | Evaluated
  type FailoverDecision = KeepCurrentPrimary | UseDefaultPrimary | PromoteLocalReplica
  type Decision = NoDecision | FailoverEvaluated | TerminalStutter

  var phase: FailoverPhase
  var localNode: Node
  var currentPrimary: Node
  var defaultPrimary: Node
  var localIsServiceNode: bool
  var localInReplicaSet: bool
  var connectivityGraphAvailable: bool
  var routeToCurrentPrimaryHealthy: bool
  var localShardPathExists: bool
  var localShardJournalNonEmpty: bool
  var selectedPrimary: Node
  var failoverDecision: FailoverDecision
  var lastDecision: Decision

  pure def localFailoverCandidate(
    local: Node,
    current: Node,
    isService: bool,
    inReplicaSet: bool,
    graphAvailable: bool,
    routeHealthy: bool
  ): bool =
    all {
      current != local,
      isService,
      inReplicaSet,
      graphAvailable,
      not(routeHealthy),
    }

  pure def localReplicaFailoverReady(
    local: Node,
    current: Node,
    isService: bool,
    inReplicaSet: bool,
    graphAvailable: bool,
    routeHealthy: bool,
    shardPathExists: bool,
    journalNonEmpty: bool
  ): bool =
    all {
      localFailoverCandidate(
        local,
        current,
        isService,
        inReplicaSet,
        graphAvailable,
        routeHealthy
      ),
      shardPathExists,
      journalNonEmpty,
    }

  pure def targetPrimaryAfterFailoverCheck(
    local: Node,
    current: Node,
    defaultTarget: Node,
    isService: bool,
    inReplicaSet: bool,
    graphAvailable: bool,
    routeHealthy: bool,
    shardPathExists: bool,
    journalNonEmpty: bool
  ): Node =
    if (
      localReplicaFailoverReady(
        local,
        current,
        isService,
        inReplicaSet,
        graphAvailable,
        routeHealthy,
        shardPathExists,
        journalNonEmpty
      )
    ) {
      local
    } else if (
      localFailoverCandidate(
        local,
        current,
        isService,
        inReplicaSet,
        graphAvailable,
        routeHealthy
      )
    ) {
      current
    } else {
      defaultTarget
    }

  pure def failoverDecisionFor(
    local: Node,
    current: Node,
    isService: bool,
    inReplicaSet: bool,
    graphAvailable: bool,
    routeHealthy: bool,
    shardPathExists: bool,
    journalNonEmpty: bool,
    selected: Node
  ): FailoverDecision =
    if (
      localReplicaFailoverReady(
        local,
        current,
        isService,
        inReplicaSet,
        graphAvailable,
        routeHealthy,
        shardPathExists,
        journalNonEmpty
      )
    ) {
      PromoteLocalReplica
    } else if (selected == current) {
      KeepCurrentPrimary
    } else {
      UseDefaultPrimary
    }

  action init = all {
    phase' = Idle,
    localNode' = N1,
    currentPrimary' = N2,
    defaultPrimary' = N2,
    localIsServiceNode' = false,
    localInReplicaSet' = false,
    connectivityGraphAvailable' = false,
    routeToCurrentPrimaryHealthy' = false,
    localShardPathExists' = false,
    localShardJournalNonEmpty' = false,
    selectedPrimary' = N2,
    failoverDecision' = KeepCurrentPrimary,
    lastDecision' = NoDecision,
  }

  action evaluateFailover(
    local: Node,
    current: Node,
    defaultTarget: Node,
    isService: bool,
    inReplicaSet: bool,
    graphAvailable: bool,
    routeHealthy: bool,
    shardPathExists: bool,
    journalNonEmpty: bool
  ): bool = {
    val selected =
      targetPrimaryAfterFailoverCheck(
        local,
        current,
        defaultTarget,
        isService,
        inReplicaSet,
        graphAvailable,
        routeHealthy,
        shardPathExists,
        journalNonEmpty
      )
    all {
      phase == Idle,
      localNode' = local,
      currentPrimary' = current,
      defaultPrimary' = defaultTarget,
      localIsServiceNode' = isService,
      localInReplicaSet' = inReplicaSet,
      connectivityGraphAvailable' = graphAvailable,
      routeToCurrentPrimaryHealthy' = routeHealthy,
      localShardPathExists' = shardPathExists,
      localShardJournalNonEmpty' = journalNonEmpty,
      selectedPrimary' = selected,
      failoverDecision' =
        failoverDecisionFor(
          local,
          current,
          isService,
          inReplicaSet,
          graphAvailable,
          routeHealthy,
          shardPathExists,
          journalNonEmpty,
          selected
        ),
      phase' = Evaluated,
      lastDecision' = FailoverEvaluated,
    }
  }

  action stutterEvaluated: bool = all {
    phase == Evaluated,
    localNode' = localNode,
    currentPrimary' = currentPrimary,
    defaultPrimary' = defaultPrimary,
    localIsServiceNode' = localIsServiceNode,
    localInReplicaSet' = localInReplicaSet,
    connectivityGraphAvailable' = connectivityGraphAvailable,
    routeToCurrentPrimaryHealthy' = routeToCurrentPrimaryHealthy,
    localShardPathExists' = localShardPathExists,
    localShardJournalNonEmpty' = localShardJournalNonEmpty,
    selectedPrimary' = selectedPrimary,
    failoverDecision' = failoverDecision,
    phase' = phase,
    lastDecision' = TerminalStutter,
  }

  action step = any {
    {
      nondet local = oneOf(Nodes)
      nondet current = oneOf(Nodes)
      nondet defaultTarget = oneOf(Nodes)
      nondet isService = oneOf(Set(true, false))
      nondet inReplicaSet = oneOf(Set(true, false))
      nondet graphAvailable = oneOf(Set(true, false))
      nondet routeHealthy = oneOf(Set(true, false))
      nondet shardPathExists = oneOf(Set(true, false))
      nondet journalNonEmpty = oneOf(Set(true, false))
      evaluateFailover(
        local,
        current,
        defaultTarget,
        isService,
        inReplicaSet,
        graphAvailable,
        routeHealthy,
        shardPathExists,
        journalNonEmpty
      )
    },
    stutterEvaluated,
  }
}
