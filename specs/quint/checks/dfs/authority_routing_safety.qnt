/// Invariant checks for `dfs_authority_routing`.
module dfs_authority_routing_safety {
  import common.* from "../../lib/common"
  import dfs_authority_routing.* from "../../models/dfs/authority_routing"

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
