/// DFS routed-operation authority over stale shard views.
///
/// This module sits above `dfs_metadata`. It assumes the control plane exposes
/// one committed metadata-shard primary and fenced epoch, then models the
/// requester-side routing behavior used by distributed writes and namespace
/// operations when a caller has a stale cached shard view.
module dfs_authority_routing {
  import common.* from "./common"

  type RoutedOp =
    | RoutedWrite
    | NamespaceMutation
    | NamespaceLookup

  type PendingRoute = {
    op: RoutedOp,
    attemptedPrimary: Node,
    redirectsRemaining: int,
  }

  type Completion = {
    op: RoutedOp,
    primary: Node,
    epoch: int,
    usedLocalReplica: bool,
  }

  pure val MaxEpoch: int = 3
  pure val Epochs: Set[int] = Set(1, 2, 3)
  pure val RedirectLimit: int = 3
  pure val RoutedOps: Set[RoutedOp] =
    Set(RoutedWrite, NamespaceMutation, NamespaceLookup)

  var currentPrimary: Node
  var currentEpoch: int
  var primaryByEpoch: int -> Option[Node]
  var cachedPrimary: Node -> Node
  var canReach: Node -> (Node -> bool)
  var pendingRoute: Node -> Option[PendingRoute]
  var completed: Node -> Option[Completion]
  var failures: Set[Node]
  var discardedResponses: Set[Node]

  pure val emptyPrimaryByEpoch: int -> Option[Node] =
    Epochs.mapBy(_ => None)

  pure val healthyRoutes: Node -> (Node -> bool) =
    Nodes.mapBy(src => Nodes.mapBy(dst => src != dst))

  pure val emptyPendingRoutes: Node -> Option[PendingRoute] =
    Nodes.mapBy(_ => None)

  pure val emptyCompletions: Node -> Option[Completion] =
    Nodes.mapBy(_ => None)

  pure def unwrapPending(value: Option[PendingRoute]): PendingRoute =
    match value {
      | Some(inner) => inner
      | None => {
          op: RoutedWrite,
          attemptedPrimary: N1,
          redirectsRemaining: 0,
        }
    }

  def routeCanStart(requester: Node): bool = {
    val primary = cachedPrimary.get(requester)
    all {
      not(isSome(pendingRoute.get(requester))),
      completed.get(requester) == None,
      not(failures.contains(requester)),
      requester != primary,
    }
  }

  def clearPending(requester: Node): Node -> Option[PendingRoute] =
    pendingRoute.set(requester, None)

  def recordCompletion(
    requester: Node,
    op: RoutedOp,
    primary: Node,
    epoch: int,
    usedLocalReplica: bool,
  ): Node -> Option[Completion] =
    completed.set(requester, Some({
      op: op,
      primary: primary,
      epoch: epoch,
      usedLocalReplica: usedLocalReplica,
    }))

  pure def setDirectedRoute(
    oldRoutes: Node -> (Node -> bool),
    srcNode: Node,
    dstNode: Node,
    isUp: bool,
  ): Node -> (Node -> bool) =
    Nodes.mapBy(src =>
      if (src == srcNode) oldRoutes.get(src).set(dstNode, isUp) else oldRoutes.get(src)
    )

  action init = all {
    currentPrimary' = N1,
    currentEpoch' = 1,
    primaryByEpoch' = emptyPrimaryByEpoch.set(1, Some(N1)),
    cachedPrimary' = Nodes.mapBy(_ => N1),
    canReach' = healthyRoutes,
    pendingRoute' = emptyPendingRoutes,
    completed' = emptyCompletions,
    failures' = Set(),
    discardedResponses' = Set(),
  }

  /// Abstract committed KV update for metadata-shard primary movement.
  /// Requester caches and pending routes are intentionally not updated here.
  action commitPrimaryChange(newPrimary: Node): bool = all {
    newPrimary != currentPrimary,
    currentEpoch < MaxEpoch,
    currentPrimary' = newPrimary,
    currentEpoch' = currentEpoch + 1,
    primaryByEpoch' = primaryByEpoch.set(currentEpoch + 1, Some(newPrimary)),
    cachedPrimary' = cachedPrimary,
    canReach' = canReach,
    pendingRoute' = pendingRoute,
    completed' = completed,
    failures' = failures,
    discardedResponses' = discardedResponses,
  }

  /// Requester refreshes its local shard view from the control plane.
  action refreshCachedPrimary(requester: Node): bool = all {
    cachedPrimary' = cachedPrimary.set(requester, currentPrimary),
    currentPrimary' = currentPrimary,
    currentEpoch' = currentEpoch,
    primaryByEpoch' = primaryByEpoch,
    canReach' = canReach,
    pendingRoute' = pendingRoute,
    completed' = completed,
    failures' = failures,
    discardedResponses' = discardedResponses,
  }

  /// First send for a routed write/namespace operation. If the cached primary
  /// is unreachable, the implementation returns a network error before tracking
  /// a pending routed request; that is modeled by `failStartUnroutable`.
  action startRoutedOp(requester: Node, op: RoutedOp): bool = {
    val primary = cachedPrimary.get(requester)
    all {
      routeCanStart(requester),
      canReach.get(requester).get(primary),
      pendingRoute' = pendingRoute.set(requester, Some({
        op: op,
        attemptedPrimary: primary,
        redirectsRemaining: RedirectLimit,
      })),
      currentPrimary' = currentPrimary,
      currentEpoch' = currentEpoch,
      primaryByEpoch' = primaryByEpoch,
      cachedPrimary' = cachedPrimary,
      canReach' = canReach,
      completed' = completed,
      failures' = failures,
      discardedResponses' = discardedResponses,
    }
  }

  action failStartUnroutable(requester: Node): bool = {
    val primary = cachedPrimary.get(requester)
    all {
      routeCanStart(requester),
      not(canReach.get(requester).get(primary)),
      failures' = failures.union(Set(requester)),
      currentPrimary' = currentPrimary,
      currentEpoch' = currentEpoch,
      primaryByEpoch' = primaryByEpoch,
      cachedPrimary' = cachedPrimary,
      canReach' = canReach,
      pendingRoute' = pendingRoute,
      completed' = completed,
      discardedResponses' = discardedResponses,
    }
  }

  /// The pending primary is still the configured authority, so the operation
  /// may complete and records the authority epoch at completion time.
  action completeAtCurrentPrimary(requester: Node): bool = {
    val pending = unwrapPending(pendingRoute.get(requester))
    all {
      isSome(pendingRoute.get(requester)),
      pending.attemptedPrimary == currentPrimary,
      completed' =
        recordCompletion(requester, pending.op, currentPrimary, currentEpoch, false),
      pendingRoute' = clearPending(requester),
      currentPrimary' = currentPrimary,
      currentEpoch' = currentEpoch,
      primaryByEpoch' = primaryByEpoch,
      cachedPrimary' = cachedPrimary,
      canReach' = canReach,
      failures' = failures,
      discardedResponses' = discardedResponses,
    }
  }

  /// A stale primary rejects with the current primary. If that primary is
  /// reachable, the requester retries against it and consumes one redirect.
  action followAuthorityRedirect(requester: Node): bool = {
    val pending = unwrapPending(pendingRoute.get(requester))
    all {
      isSome(pendingRoute.get(requester)),
      pending.attemptedPrimary != currentPrimary,
      pending.redirectsRemaining > 0,
      currentPrimary != requester,
      canReach.get(requester).get(currentPrimary),
      pendingRoute' = pendingRoute.set(requester, Some({
        op: pending.op,
        attemptedPrimary: currentPrimary,
        redirectsRemaining: pending.redirectsRemaining - 1,
      })),
      currentPrimary' = currentPrimary,
      currentEpoch' = currentEpoch,
      primaryByEpoch' = primaryByEpoch,
      cachedPrimary' = cachedPrimary,
      canReach' = canReach,
      completed' = completed,
      failures' = failures,
      discardedResponses' = discardedResponses,
    }
  }

  /// Namespace lookup has a concrete fast path where a redirect to the local
  /// node can be served from the synchronized local metadata view.
  action completeLookupLocallyAfterRedirect(requester: Node): bool = {
    val pending = unwrapPending(pendingRoute.get(requester))
    all {
      isSome(pendingRoute.get(requester)),
      pending.op == NamespaceLookup,
      pending.attemptedPrimary != currentPrimary,
      pending.redirectsRemaining > 0,
      currentPrimary == requester,
      completed' =
        recordCompletion(requester, pending.op, currentPrimary, currentEpoch, true),
      pendingRoute' = clearPending(requester),
      currentPrimary' = currentPrimary,
      currentEpoch' = currentEpoch,
      primaryByEpoch' = primaryByEpoch,
      cachedPrimary' = cachedPrimary,
      canReach' = canReach,
      failures' = failures,
      discardedResponses' = discardedResponses,
    }
  }

  action failRedirectLimit(requester: Node): bool = {
    val pending = unwrapPending(pendingRoute.get(requester))
    all {
      isSome(pendingRoute.get(requester)),
      pending.attemptedPrimary != currentPrimary,
      pending.redirectsRemaining == 0,
      failures' = failures.union(Set(requester)),
      pendingRoute' = clearPending(requester),
      currentPrimary' = currentPrimary,
      currentEpoch' = currentEpoch,
      primaryByEpoch' = primaryByEpoch,
      cachedPrimary' = cachedPrimary,
      canReach' = canReach,
      completed' = completed,
      discardedResponses' = discardedResponses,
    }
  }

  action failRedirectUnroutable(requester: Node): bool = {
    val pending = unwrapPending(pendingRoute.get(requester))
    all {
      isSome(pendingRoute.get(requester)),
      pending.attemptedPrimary != currentPrimary,
      pending.redirectsRemaining > 0,
      currentPrimary != requester,
      not(canReach.get(requester).get(currentPrimary)),
      failures' = failures.union(Set(requester)),
      pendingRoute' = clearPending(requester),
      currentPrimary' = currentPrimary,
      currentEpoch' = currentEpoch,
      primaryByEpoch' = primaryByEpoch,
      cachedPrimary' = cachedPrimary,
      canReach' = canReach,
      completed' = completed,
      discardedResponses' = discardedResponses,
    }
  }

  /// Late, duplicate, or mismatched responses are discarded while the pending
  /// route remains tied to its current attempted primary.
  action discardMismatchedResponse(requester: Node, responseSource: Node): bool = {
    val pending = unwrapPending(pendingRoute.get(requester))
    all {
      isSome(pendingRoute.get(requester)),
      responseSource != pending.attemptedPrimary,
      discardedResponses' = discardedResponses.union(Set(requester)),
      currentPrimary' = currentPrimary,
      currentEpoch' = currentEpoch,
      primaryByEpoch' = primaryByEpoch,
      cachedPrimary' = cachedPrimary,
      canReach' = canReach,
      pendingRoute' = pendingRoute,
      completed' = completed,
      failures' = failures,
    }
  }

  action timeoutPendingRoute(requester: Node): bool = all {
    isSome(pendingRoute.get(requester)),
    failures' = failures.union(Set(requester)),
    pendingRoute' = clearPending(requester),
    currentPrimary' = currentPrimary,
    currentEpoch' = currentEpoch,
    primaryByEpoch' = primaryByEpoch,
    cachedPrimary' = cachedPrimary,
    canReach' = canReach,
    completed' = completed,
    discardedResponses' = discardedResponses,
  }

  action setRouteState(srcNode: Node, dstNode: Node, isUp: bool): bool = all {
    srcNode != dstNode,
    canReach' = setDirectedRoute(canReach, srcNode, dstNode, isUp),
    currentPrimary' = currentPrimary,
    currentEpoch' = currentEpoch,
    primaryByEpoch' = primaryByEpoch,
    cachedPrimary' = cachedPrimary,
    pendingRoute' = pendingRoute,
    completed' = completed,
    failures' = failures,
    discardedResponses' = discardedResponses,
  }

  action step = any {
    {
      nondet newPrimary = oneOf(Nodes)
      commitPrimaryChange(newPrimary)
    },
    {
      nondet requester = oneOf(Nodes)
      refreshCachedPrimary(requester)
    },
    {
      nondet requester = oneOf(Nodes)
      nondet op = oneOf(RoutedOps)
      any {
        startRoutedOp(requester, op),
        failStartUnroutable(requester),
      }
    },
    {
      nondet requester = oneOf(Nodes)
      any {
        completeAtCurrentPrimary(requester),
        followAuthorityRedirect(requester),
        completeLookupLocallyAfterRedirect(requester),
        failRedirectLimit(requester),
        failRedirectUnroutable(requester),
        timeoutPendingRoute(requester),
      }
    },
    {
      nondet requester = oneOf(Nodes)
      nondet responseSource = oneOf(Nodes)
      discardMismatchedResponse(requester, responseSource)
    },
    {
      nondet srcNode = oneOf(Nodes)
      nondet dstNode = oneOf(Nodes)
      any {
        setRouteState(srcNode, dstNode, true),
        setRouteState(srcNode, dstNode, false),
      }
    },
  }

  def currentEpochHasPrimary: bool =
    primaryByEpoch.get(currentEpoch) == Some(currentPrimary)

  def completedByConfiguredPrimaryForEpoch: bool =
    Nodes.forall(requester =>
      match completed.get(requester) {
        | Some(done) => primaryByEpoch.get(done.epoch) == Some(done.primary)
        | None => true
      }
    )

  def localLookupShortcutOnlyUsesLocalCurrentPrimary: bool =
    Nodes.forall(requester =>
      match completed.get(requester) {
        | Some(done) =>
            not(done.usedLocalReplica)
              or all {
                done.op == NamespaceLookup,
                done.primary == requester,
                primaryByEpoch.get(done.epoch) == Some(requester),
              }
        | None => true
      }
    )

  def pendingRedirectCountersInBounds: bool =
    Nodes.forall(requester =>
      match pendingRoute.get(requester) {
        | Some(pending) =>
            pending.redirectsRemaining >= 0 and pending.redirectsRemaining <= RedirectLimit
        | None => true
      }
    )

  def terminalRoutesHaveNoPendingState: bool =
    Nodes.forall(requester =>
      if (isSome(completed.get(requester)) or failures.contains(requester)) {
        not(isSome(pendingRoute.get(requester)))
      } else {
        true
      }
    )

  def completedAndFailedAreDisjoint: bool =
    Nodes.forall(requester =>
      not(isSome(completed.get(requester)) and failures.contains(requester))
    )

  def selfRoutesStayDisabled: bool =
    Nodes.forall(node => not(canReach.get(node).get(node)))

  def discardedResponsesAreOnlyObservations: bool =
    discardedResponses.subseteq(Nodes)

  def epochsInBounds: bool =
    currentEpoch >= 1 and currentEpoch <= MaxEpoch

  def safety: bool = all {
    currentEpochHasPrimary,
    completedByConfiguredPrimaryForEpoch,
    localLookupShortcutOnlyUsesLocalCurrentPrimary,
    pendingRedirectCountersInBounds,
    terminalRoutesHaveNoPendingState,
    completedAndFailedAreDisjoint,
    selfRoutesStayDisabled,
    discardedResponsesAreOnlyObservations,
    epochsInBounds,
  }
}
