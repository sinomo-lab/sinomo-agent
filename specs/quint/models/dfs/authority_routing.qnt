/// DFS routed-operation authority over stale and multi-shard views.
///
/// This bounded slice distinguishes the shard (or same-primary shard group)
/// that selected the dispatch target from another affected shard whose primary
/// is unrelated to that dispatch. A primary reported by a rejected response is
/// evidence only after a refreshed committed view names it for every dispatch
/// shard. Merely matching the foreign affected shard is insufficient.
///
/// Namespace lookups have one deliberately narrower failure fallback. After an
/// uncorroborated redirect, a current metadata replica may return an existing
/// live local hit. Outsiders, mutations, missing local rows, and tombstones fail
/// closed. This is not the local-first operation model.
module dfs_authority_routing {
  import common.* from "../../lib/common"

  type RoutedOp =
    | RoutedWrite
    | NamespaceMutation
    | NamespaceLookup

  /// Single dispatch models one routed path. Grouped dispatch models a
  /// LookupPaths-style request whose routing and companion shards were both
  /// captured under the same attempted primary.
  type DispatchShape = SingleDispatch | GroupedDispatch
  /// `RemovedDispatchReplica` had a replica-capable cached view but is absent
  /// from the refreshed committed dispatch assignment used for fallback.
  type RequesterRole =
    | CurrentDispatchReplica
    | RemovedDispatchReplica
    | DispatchOutsider
  type LocalPathState = LocalMissing | LocalLive | LocalTombstone

  type CompletionMode =
    | RemoteAuthorityCompletion
    | CorroboratedLocalAuthorityCompletion
    | UncorroboratedReplicaLiveHitCompletion

  type RedirectOutcome =
    | RedirectFollowed
    | CorroboratedLocalAuthorityUsed
    | CorroboratedRedirectUnroutable
    | UncorroboratedOutsiderRejected
    | UncorroboratedRemovedReplicaRejected
    | UncorroboratedReplicaLiveHitUsed
    | UncorroboratedReplicaMissingRejected
    | UncorroboratedReplicaTombstoneRejected
    | UncorroboratedNonLookupRejected

  type CoverageScenario =
    | NoCoverageScenario
    | CoverCorroboratedSingleRedirect
    | CoverCorroboratedGroupedRedirect
    | CoverPartialGroupedRedirectRejected
    | CoverForeignAffectedRedirectRejected
    | CoverUncorroboratedOutsiderRejected
    | CoverReplicaLiveHitFallback
    | CoverRemovedReplicaRejected
    | CoverReplicaMissingRejected
    | CoverReplicaTombstoneRejected
    | CoverDistinctForeignPrimaryCompletion

  type PendingRoute = {
    op: RoutedOp,
    attemptedPrimary: Node,
    redirectsRemaining: int,
    dispatchShape: DispatchShape,
    requesterRole: RequesterRole,
    localPathState: LocalPathState,
  }

  type Completion = {
    op: RoutedOp,
    primary: Node,
    routingEpoch: int,
    companionEpoch: int,
    dispatchShape: DispatchShape,
    servedBy: Node,
    mode: CompletionMode,
    localPathState: LocalPathState,
  }

  /// The epochs are captured from the refreshed committed view used to decide
  /// whether the response's explicit `reportedPrimary` can be trusted.
  type RedirectEvidence = {
    reportedPrimary: Node,
    attemptedPrimary: Node,
    routingEpoch: int,
    companionEpoch: int,
    dispatchShape: DispatchShape,
    requesterRole: RequesterRole,
    localPathState: LocalPathState,
    outcome: RedirectOutcome,
  }

  type AuthorityRoutingState = {
    currentPrimary: Node,
    currentEpoch: int,
    primaryByEpoch: int -> Option[Node],
    companionPrimary: Node,
    companionEpoch: int,
    companionPrimaryByEpoch: int -> Option[Node],
    foreignAffectedPrimary: Node,
    cachedPrimary: Node -> Node,
    canReach: Node -> (Node -> bool),
    pendingRoute: Node -> Option[PendingRoute],
    completed: Node -> Option[Completion],
    redirectEvidence: Node -> Option[RedirectEvidence],
    failures: Set[Node],
    discardedResponses: Set[Node],
    coverageScenario: CoverageScenario,
  }

  pure val MaxEpoch: int = 3
  pure val Epochs: Set[int] = Set(1, 2, 3)
  pure val RedirectLimit: int = 3
  pure val RoutedOps: Set[RoutedOp] =
    Set(RoutedWrite, NamespaceMutation, NamespaceLookup)
  pure val DispatchShapes: Set[DispatchShape] =
    Set(SingleDispatch, GroupedDispatch)
  pure val RequesterRoles: Set[RequesterRole] =
    Set(CurrentDispatchReplica, RemovedDispatchReplica, DispatchOutsider)
  pure val LocalPathStates: Set[LocalPathState] =
    Set(LocalMissing, LocalLive, LocalTombstone)

  pure val emptyPrimaryByEpoch: int -> Option[Node] =
    Epochs.mapBy(_ => None)

  pure val healthyRoutes: Node -> (Node -> bool) =
    Nodes.mapBy(src => Nodes.mapBy(dst => src != dst))

  pure val emptyPendingRoutes: Node -> Option[PendingRoute] =
    Nodes.mapBy(_ => None)

  pure val emptyCompletions: Node -> Option[Completion] =
    Nodes.mapBy(_ => None)

  pure val emptyRedirectEvidence: Node -> Option[RedirectEvidence] =
    Nodes.mapBy(_ => None)

  pure val initialState: AuthorityRoutingState = {
    currentPrimary: N1,
    currentEpoch: 1,
    primaryByEpoch: emptyPrimaryByEpoch.set(1, Some(N1)),
    companionPrimary: N1,
    companionEpoch: 1,
    companionPrimaryByEpoch: emptyPrimaryByEpoch.set(1, Some(N1)),
    foreignAffectedPrimary: N3,
    cachedPrimary: Nodes.mapBy(_ => N1),
    canReach: healthyRoutes,
    pendingRoute: emptyPendingRoutes,
    completed: emptyCompletions,
    redirectEvidence: emptyRedirectEvidence,
    failures: Set(),
    discardedResponses: Set(),
    coverageScenario: NoCoverageScenario,
  }

  var routing: AuthorityRoutingState

  pure def unwrapPending(value: Option[PendingRoute]): PendingRoute =
    match value {
      | Some(inner) => inner
      | None => {
          op: RoutedWrite,
          attemptedPrimary: N1,
          redirectsRemaining: 0,
          dispatchShape: SingleDispatch,
          requesterRole: DispatchOutsider,
          localPathState: LocalMissing,
        }
    }

  pure def routeCanStart(state: AuthorityRoutingState, requester: Node): bool = {
    val primary = state.cachedPrimary.get(requester)
    all {
      not(isSome(state.pendingRoute.get(requester))),
      state.completed.get(requester) == None,
      not(state.failures.contains(requester)),
      requester != primary,
    }
  }

  /// Exact committed corroboration is scoped only to the dispatch set. The
  /// fixed foreign affected shard is intentionally absent.
  pure def dispatchClaimMatchesCurrent(
    state: AuthorityRoutingState,
    shape: DispatchShape,
    reportedPrimary: Node,
  ): bool = all {
    state.currentPrimary == reportedPrimary,
    shape == SingleDispatch or state.companionPrimary == reportedPrimary,
  }

  pure def redirectEvidenceMatchesCommittedDispatch(
    state: AuthorityRoutingState,
    evidence: RedirectEvidence,
  ): bool = all {
    state.primaryByEpoch.get(evidence.routingEpoch) == Some(evidence.reportedPrimary),
    evidence.dispatchShape == SingleDispatch
      or state.companionPrimaryByEpoch.get(evidence.companionEpoch)
        == Some(evidence.reportedPrimary),
  }

  pure def clearPending(
    state: AuthorityRoutingState,
    requester: Node,
  ): Node -> Option[PendingRoute] =
    state.pendingRoute.set(requester, None)

  pure def recordCompletion(
    state: AuthorityRoutingState,
    requester: Node,
    pending: PendingRoute,
    primary: Node,
    servedBy: Node,
    mode: CompletionMode,
  ): Node -> Option[Completion] =
    state.completed.set(requester, Some({
      op: pending.op,
      primary: primary,
      routingEpoch: state.currentEpoch,
      companionEpoch: state.companionEpoch,
      dispatchShape: pending.dispatchShape,
      servedBy: servedBy,
      mode: mode,
      localPathState: pending.localPathState,
    }))

  pure def recordRedirectEvidence(
    state: AuthorityRoutingState,
    requester: Node,
    pending: PendingRoute,
    reportedPrimary: Node,
    outcome: RedirectOutcome,
  ): Node -> Option[RedirectEvidence] =
    state.redirectEvidence.set(requester, Some({
      reportedPrimary: reportedPrimary,
      attemptedPrimary: pending.attemptedPrimary,
      routingEpoch: state.currentEpoch,
      companionEpoch: state.companionEpoch,
      dispatchShape: pending.dispatchShape,
      requesterRole: pending.requesterRole,
      localPathState: pending.localPathState,
      outcome: outcome,
    }))

  pure def rejectedRedirectOutcome(pending: PendingRoute): RedirectOutcome =
    if (pending.requesterRole == DispatchOutsider) {
      UncorroboratedOutsiderRejected
    } else if (pending.requesterRole == RemovedDispatchReplica) {
      UncorroboratedRemovedReplicaRejected
    } else if (pending.op != NamespaceLookup) {
      UncorroboratedNonLookupRejected
    } else {
      match pending.localPathState {
        | LocalMissing => UncorroboratedReplicaMissingRejected
        | LocalTombstone => UncorroboratedReplicaTombstoneRejected
        | LocalLive => UncorroboratedReplicaLiveHitUsed
      }
    }

  pure def setDirectedRoute(
    oldRoutes: Node -> (Node -> bool),
    srcNode: Node,
    dstNode: Node,
    isUp: bool,
  ): Node -> (Node -> bool) =
    Nodes.mapBy(src =>
      if (src == srcNode) oldRoutes.get(src).set(dstNode, isUp) else oldRoutes.get(src)
    )

  action init = routing' = initialState

  /// Abstract committed KV updates for the routing shard and an optional
  /// same-primary companion dispatch shard. They move independently so a
  /// partial grouped handoff is explicitly reachable.
  action commitRoutingPrimaryChange(newPrimary: Node): bool = all {
    newPrimary != routing.currentPrimary,
    routing.currentEpoch < MaxEpoch,
    routing' = routing
      .with("currentPrimary", newPrimary)
      .with("currentEpoch", routing.currentEpoch + 1)
      .with(
        "primaryByEpoch",
        routing.primaryByEpoch.set(routing.currentEpoch + 1, Some(newPrimary))
      ),
  }

  action commitCompanionPrimaryChange(newPrimary: Node): bool = all {
    newPrimary != routing.companionPrimary,
    routing.companionEpoch < MaxEpoch,
    routing' = routing
      .with("companionPrimary", newPrimary)
      .with("companionEpoch", routing.companionEpoch + 1)
      .with(
        "companionPrimaryByEpoch",
        routing.companionPrimaryByEpoch
          .set(routing.companionEpoch + 1, Some(newPrimary))
      ),
  }

  action refreshCachedPrimary(requester: Node): bool =
    routing' = routing.with(
      "cachedPrimary",
      routing.cachedPrimary.set(requester, routing.currentPrimary)
    )

  action startRoutedOp(
    requester: Node,
    op: RoutedOp,
    dispatchShape: DispatchShape,
    requesterRole: RequesterRole,
    localPathState: LocalPathState,
  ): bool = {
    val primary = routing.cachedPrimary.get(requester)
    all {
      routeCanStart(routing, requester),
      routing.canReach.get(requester).get(primary),
      routing' = routing.with(
        "pendingRoute",
        routing.pendingRoute.set(requester, Some({
          op: op,
          attemptedPrimary: primary,
          redirectsRemaining: RedirectLimit,
          dispatchShape: dispatchShape,
          requesterRole: requesterRole,
          localPathState: localPathState,
        }))
      ),
    }
  }

  action failStartUnroutable(requester: Node): bool = {
    val primary = routing.cachedPrimary.get(requester)
    all {
      routeCanStart(routing, requester),
      not(routing.canReach.get(requester).get(primary)),
      routing' = routing.with("failures", routing.failures.union(Set(requester))),
    }
  }

  /// A successful response is admitted only when the attempted source is still
  /// the committed primary of every dispatch shard. The unrelated affected
  /// shard may retain a different primary.
  action completeAtCurrentDispatchAuthority(requester: Node): bool = {
    val pending = unwrapPending(routing.pendingRoute.get(requester))
    all {
      isSome(routing.pendingRoute.get(requester)),
      dispatchClaimMatchesCurrent(
        routing,
        pending.dispatchShape,
        pending.attemptedPrimary
      ),
      routing' = routing
        .with(
          "completed",
          recordCompletion(
            routing,
            requester,
            pending,
            pending.attemptedPrimary,
            pending.attemptedPrimary,
            RemoteAuthorityCompletion
          )
        )
        .with("pendingRoute", clearPending(routing, requester)),
    }
  }

  /// Follow the response's explicit claim only after the refreshed committed
  /// routing and optional companion dispatch assignments both name it.
  action followAuthorityRedirect(
    requester: Node,
    reportedPrimary: Node,
  ): bool = {
    val pending = unwrapPending(routing.pendingRoute.get(requester))
    all {
      isSome(routing.pendingRoute.get(requester)),
      pending.redirectsRemaining > 0,
      reportedPrimary != pending.attemptedPrimary,
      reportedPrimary != requester,
      dispatchClaimMatchesCurrent(routing, pending.dispatchShape, reportedPrimary),
      routing.canReach.get(requester).get(reportedPrimary),
      routing' = routing
        .with(
          "pendingRoute",
          routing.pendingRoute.set(requester, Some({
            op: pending.op,
            attemptedPrimary: reportedPrimary,
            redirectsRemaining: pending.redirectsRemaining - 1,
            dispatchShape: pending.dispatchShape,
            requesterRole: pending.requesterRole,
            localPathState: pending.localPathState,
          }))
        )
        .with(
          "redirectEvidence",
          recordRedirectEvidence(
            routing,
            requester,
            pending,
            reportedPrimary,
            RedirectFollowed
          )
        ),
    }
  }

  /// A corroborated redirect to the requester itself is local authority, not
  /// the replica fallback below.
  action completeLookupAtCorroboratedLocalAuthority(
    requester: Node,
    reportedPrimary: Node,
  ): bool = {
    val pending = unwrapPending(routing.pendingRoute.get(requester))
    all {
      isSome(routing.pendingRoute.get(requester)),
      pending.op == NamespaceLookup,
      pending.redirectsRemaining > 0,
      reportedPrimary != pending.attemptedPrimary,
      reportedPrimary == requester,
      dispatchClaimMatchesCurrent(routing, pending.dispatchShape, reportedPrimary),
      routing' = routing
        .with(
          "completed",
          recordCompletion(
            routing,
            requester,
            pending,
            reportedPrimary,
            requester,
            CorroboratedLocalAuthorityCompletion
          )
        )
        .with("pendingRoute", clearPending(routing, requester))
        .with(
          "redirectEvidence",
          recordRedirectEvidence(
            routing,
            requester,
            pending,
            reportedPrimary,
            CorroboratedLocalAuthorityUsed
          )
        ),
    }
  }

  /// An uncorroborated response remains rejected. A current replica can still
  /// use an already-existing live lookup hit; it cannot turn missing or
  /// tombstoned local state into an authoritative NotFound result.
  action completeLookupFromReplicaLiveHitAfterUncorroboratedRedirect(
    requester: Node,
    reportedPrimary: Node,
  ): bool = {
    val pending = unwrapPending(routing.pendingRoute.get(requester))
    all {
      isSome(routing.pendingRoute.get(requester)),
      not(dispatchClaimMatchesCurrent(routing, pending.dispatchShape, reportedPrimary)),
      pending.op == NamespaceLookup,
      pending.requesterRole == CurrentDispatchReplica,
      pending.localPathState == LocalLive,
      routing' = routing
        .with(
          "completed",
          recordCompletion(
            routing,
            requester,
            pending,
            routing.currentPrimary,
            requester,
            UncorroboratedReplicaLiveHitCompletion
          )
        )
        .with("pendingRoute", clearPending(routing, requester))
        .with(
          "redirectEvidence",
          recordRedirectEvidence(
            routing,
            requester,
            pending,
            reportedPrimary,
            UncorroboratedReplicaLiveHitUsed
          )
        ),
    }
  }

  action rejectUncorroboratedRedirect(
    requester: Node,
    reportedPrimary: Node,
  ): bool = {
    val pending = unwrapPending(routing.pendingRoute.get(requester))
    all {
      isSome(routing.pendingRoute.get(requester)),
      not(dispatchClaimMatchesCurrent(routing, pending.dispatchShape, reportedPrimary)),
      not(all {
        pending.op == NamespaceLookup,
        pending.requesterRole == CurrentDispatchReplica,
        pending.localPathState == LocalLive,
      }),
      routing' = routing
        .with("failures", routing.failures.union(Set(requester)))
        .with("pendingRoute", clearPending(routing, requester))
        .with(
          "redirectEvidence",
          recordRedirectEvidence(
            routing,
            requester,
            pending,
            reportedPrimary,
            rejectedRedirectOutcome(pending)
          )
        ),
    }
  }

  action failCorroboratedRedirectUnroutable(
    requester: Node,
    reportedPrimary: Node,
  ): bool = {
    val pending = unwrapPending(routing.pendingRoute.get(requester))
    all {
      isSome(routing.pendingRoute.get(requester)),
      pending.redirectsRemaining > 0,
      reportedPrimary != requester,
      dispatchClaimMatchesCurrent(routing, pending.dispatchShape, reportedPrimary),
      not(routing.canReach.get(requester).get(reportedPrimary)),
      routing' = routing
        .with("failures", routing.failures.union(Set(requester)))
        .with("pendingRoute", clearPending(routing, requester))
        .with(
          "redirectEvidence",
          recordRedirectEvidence(
            routing,
            requester,
            pending,
            reportedPrimary,
            CorroboratedRedirectUnroutable
          )
        ),
    }
  }

  action failRedirectLimit(requester: Node): bool = {
    val pending = unwrapPending(routing.pendingRoute.get(requester))
    all {
      isSome(routing.pendingRoute.get(requester)),
      pending.redirectsRemaining == 0,
      not(dispatchClaimMatchesCurrent(
        routing,
        pending.dispatchShape,
        pending.attemptedPrimary
      )),
      routing' = routing
        .with("failures", routing.failures.union(Set(requester)))
        .with("pendingRoute", clearPending(routing, requester)),
    }
  }

  /// Late, duplicate, or mismatched responses are observations only.
  action discardMismatchedResponse(requester: Node, responseSource: Node): bool = {
    val pending = unwrapPending(routing.pendingRoute.get(requester))
    all {
      isSome(routing.pendingRoute.get(requester)),
      responseSource != pending.attemptedPrimary,
      routing' = routing.with(
        "discardedResponses",
        routing.discardedResponses.union(Set(requester))
      ),
    }
  }

  action timeoutPendingRoute(requester: Node): bool = all {
    isSome(routing.pendingRoute.get(requester)),
    routing' = routing
      .with("failures", routing.failures.union(Set(requester)))
      .with("pendingRoute", clearPending(routing, requester)),
  }

  action setRouteState(srcNode: Node, dstNode: Node, isUp: bool): bool = all {
    srcNode != dstNode,
    routing' = routing.with(
      "canReach",
      setDirectedRoute(routing.canReach, srcNode, dstNode, isUp)
    ),
  }

  action stutter = routing' = routing

  action disabled = all {
    false,
    routing' = routing,
  }

  action step = any {
    {
      nondet newPrimary = oneOf(Nodes)
      any {
        commitRoutingPrimaryChange(newPrimary),
        commitCompanionPrimaryChange(newPrimary),
      }
    },
    {
      nondet requester = oneOf(Nodes)
      refreshCachedPrimary(requester)
    },
    {
      nondet requester = oneOf(Nodes)
      nondet op = oneOf(RoutedOps)
      nondet dispatchShape = oneOf(DispatchShapes)
      nondet requesterRole = oneOf(RequesterRoles)
      nondet localPathState = oneOf(LocalPathStates)
      any {
        startRoutedOp(
          requester,
          op,
          dispatchShape,
          requesterRole,
          localPathState
        ),
        failStartUnroutable(requester),
      }
    },
    {
      nondet requester = oneOf(Nodes)
      completeAtCurrentDispatchAuthority(requester)
    },
    {
      nondet requester = oneOf(Nodes)
      nondet reportedPrimary = oneOf(Nodes)
      any {
        followAuthorityRedirect(requester, reportedPrimary),
        completeLookupAtCorroboratedLocalAuthority(requester, reportedPrimary),
        completeLookupFromReplicaLiveHitAfterUncorroboratedRedirect(
          requester,
          reportedPrimary
        ),
        rejectUncorroboratedRedirect(requester, reportedPrimary),
        failCorroboratedRedirectUnroutable(requester, reportedPrimary),
      }
    },
    {
      nondet requester = oneOf(Nodes)
      any {
        failRedirectLimit(requester),
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
      nondet isUp = oneOf(Set(true, false))
      setRouteState(srcNode, dstNode, isUp)
    },
    stutter,
  }

  /// Coverage-only schedulers call the same ordinary transition actions while
  /// the symbolic proof retains the unrestricted `step` relation.
  action selectCoverageScenario(scenario: CoverageScenario): bool = all {
    routing.coverageScenario == NoCoverageScenario,
    scenario != NoCoverageScenario,
    routing' = routing.with("coverageScenario", scenario),
  }

  action corroboratedSingleRedirectCoverageStep =
    if (routing.coverageScenario == NoCoverageScenario) {
      selectCoverageScenario(CoverCorroboratedSingleRedirect)
    } else if (routing.coverageScenario != CoverCorroboratedSingleRedirect) {
      disabled
    } else if (routing.currentPrimary == N1) {
      commitRoutingPrimaryChange(N2)
    } else if (not(isSome(routing.pendingRoute.get(N3)))) {
      startRoutedOp(
        N3,
        NamespaceLookup,
        SingleDispatch,
        DispatchOutsider,
        LocalMissing
      )
    } else if (not(isSome(routing.redirectEvidence.get(N3)))) {
      followAuthorityRedirect(N3, N2)
    } else {
      stutter
    }

  action corroboratedGroupedRedirectCoverageStep =
    if (routing.coverageScenario == NoCoverageScenario) {
      selectCoverageScenario(CoverCorroboratedGroupedRedirect)
    } else if (routing.coverageScenario != CoverCorroboratedGroupedRedirect) {
      disabled
    } else if (routing.currentPrimary == N1) {
      commitRoutingPrimaryChange(N2)
    } else if (routing.companionPrimary == N1) {
      commitCompanionPrimaryChange(N2)
    } else if (not(isSome(routing.pendingRoute.get(N3)))) {
      startRoutedOp(
        N3,
        NamespaceLookup,
        GroupedDispatch,
        DispatchOutsider,
        LocalMissing
      )
    } else if (not(isSome(routing.redirectEvidence.get(N3)))) {
      followAuthorityRedirect(N3, N2)
    } else {
      stutter
    }

  action partialGroupedRedirectCoverageStep =
    if (routing.coverageScenario == NoCoverageScenario) {
      selectCoverageScenario(CoverPartialGroupedRedirectRejected)
    } else if (routing.coverageScenario != CoverPartialGroupedRedirectRejected) {
      disabled
    } else if (routing.currentPrimary == N1) {
      commitRoutingPrimaryChange(N2)
    } else if (not(isSome(routing.pendingRoute.get(N3)))
      and not(routing.failures.contains(N3))) {
      startRoutedOp(
        N3,
        NamespaceLookup,
        GroupedDispatch,
        DispatchOutsider,
        LocalMissing
      )
    } else if (not(isSome(routing.redirectEvidence.get(N3)))) {
      rejectUncorroboratedRedirect(N3, N2)
    } else {
      stutter
    }

  action foreignAffectedRedirectCoverageStep =
    if (routing.coverageScenario == NoCoverageScenario) {
      selectCoverageScenario(CoverForeignAffectedRedirectRejected)
    } else if (routing.coverageScenario != CoverForeignAffectedRedirectRejected) {
      disabled
    } else if (not(isSome(routing.pendingRoute.get(N2)))
      and not(routing.failures.contains(N2))) {
      startRoutedOp(
        N2,
        NamespaceLookup,
        SingleDispatch,
        DispatchOutsider,
        LocalLive
      )
    } else if (not(isSome(routing.redirectEvidence.get(N2)))) {
      rejectUncorroboratedRedirect(N2, routing.foreignAffectedPrimary)
    } else {
      stutter
    }

  action outsiderRedirectCoverageStep =
    if (routing.coverageScenario == NoCoverageScenario) {
      selectCoverageScenario(CoverUncorroboratedOutsiderRejected)
    } else if (routing.coverageScenario != CoverUncorroboratedOutsiderRejected) {
      disabled
    } else if (not(isSome(routing.pendingRoute.get(N2)))
      and not(routing.failures.contains(N2))) {
      startRoutedOp(
        N2,
        NamespaceLookup,
        SingleDispatch,
        DispatchOutsider,
        LocalLive
      )
    } else if (not(isSome(routing.redirectEvidence.get(N2)))) {
      rejectUncorroboratedRedirect(N2, N2)
    } else {
      stutter
    }

  action replicaLiveHitCoverageStep =
    if (routing.coverageScenario == NoCoverageScenario) {
      selectCoverageScenario(CoverReplicaLiveHitFallback)
    } else if (routing.coverageScenario != CoverReplicaLiveHitFallback) {
      disabled
    } else if (not(isSome(routing.pendingRoute.get(N2)))
      and not(isSome(routing.completed.get(N2)))) {
      startRoutedOp(
        N2,
        NamespaceLookup,
        SingleDispatch,
        CurrentDispatchReplica,
        LocalLive
      )
    } else if (not(isSome(routing.redirectEvidence.get(N2)))) {
      completeLookupFromReplicaLiveHitAfterUncorroboratedRedirect(
        N2,
        routing.foreignAffectedPrimary
      )
    } else {
      stutter
    }

  action removedReplicaCoverageStep =
    if (routing.coverageScenario == NoCoverageScenario) {
      selectCoverageScenario(CoverRemovedReplicaRejected)
    } else if (routing.coverageScenario != CoverRemovedReplicaRejected) {
      disabled
    } else if (not(isSome(routing.pendingRoute.get(N2)))
      and not(routing.failures.contains(N2))) {
      startRoutedOp(
        N2,
        NamespaceLookup,
        SingleDispatch,
        RemovedDispatchReplica,
        LocalLive
      )
    } else if (not(isSome(routing.redirectEvidence.get(N2)))) {
      rejectUncorroboratedRedirect(N2, routing.foreignAffectedPrimary)
    } else {
      stutter
    }

  action replicaMissingCoverageStep =
    if (routing.coverageScenario == NoCoverageScenario) {
      selectCoverageScenario(CoverReplicaMissingRejected)
    } else if (routing.coverageScenario != CoverReplicaMissingRejected) {
      disabled
    } else if (not(isSome(routing.pendingRoute.get(N2)))
      and not(routing.failures.contains(N2))) {
      startRoutedOp(
        N2,
        NamespaceLookup,
        SingleDispatch,
        CurrentDispatchReplica,
        LocalMissing
      )
    } else if (not(isSome(routing.redirectEvidence.get(N2)))) {
      rejectUncorroboratedRedirect(N2, routing.foreignAffectedPrimary)
    } else {
      stutter
    }

  action replicaTombstoneCoverageStep =
    if (routing.coverageScenario == NoCoverageScenario) {
      selectCoverageScenario(CoverReplicaTombstoneRejected)
    } else if (routing.coverageScenario != CoverReplicaTombstoneRejected) {
      disabled
    } else if (not(isSome(routing.pendingRoute.get(N2)))
      and not(routing.failures.contains(N2))) {
      startRoutedOp(
        N2,
        NamespaceLookup,
        SingleDispatch,
        CurrentDispatchReplica,
        LocalTombstone
      )
    } else if (not(isSome(routing.redirectEvidence.get(N2)))) {
      rejectUncorroboratedRedirect(N2, routing.foreignAffectedPrimary)
    } else {
      stutter
    }

  action distinctForeignPrimaryCompletionCoverageStep =
    if (routing.coverageScenario == NoCoverageScenario) {
      selectCoverageScenario(CoverDistinctForeignPrimaryCompletion)
    } else if (routing.coverageScenario != CoverDistinctForeignPrimaryCompletion) {
      disabled
    } else if (not(isSome(routing.pendingRoute.get(N2)))
      and not(isSome(routing.completed.get(N2)))) {
      startRoutedOp(
        N2,
        NamespaceMutation,
        SingleDispatch,
        DispatchOutsider,
        LocalMissing
      )
    } else if (not(isSome(routing.completed.get(N2)))) {
      completeAtCurrentDispatchAuthority(N2)
    } else {
      stutter
    }

  action coverageStep = any {
    corroboratedSingleRedirectCoverageStep,
    corroboratedGroupedRedirectCoverageStep,
    partialGroupedRedirectCoverageStep,
    foreignAffectedRedirectCoverageStep,
    outsiderRedirectCoverageStep,
    replicaLiveHitCoverageStep,
    removedReplicaCoverageStep,
    replicaMissingCoverageStep,
    replicaTombstoneCoverageStep,
    distinctForeignPrimaryCompletionCoverageStep,
  }
}
