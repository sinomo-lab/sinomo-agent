/// Invariant checks for `dfs_pending_request_lifecycle`.
module dfs_pending_request_lifecycle_safety {
  import common.* from "../../lib/common"
  import dfs_pending_request_lifecycle.* from "../../models/dfs/pending_request_lifecycle"

  def activeRequestIdsAreIssued: bool =
    RequestIds.forall(requestId =>
      not(requestIsActive(requestId)) or issued.contains(requestId)
    )


  def settledRequestIdsAreIssued: bool =
    settled.subseteq(issued)


  def timedOutOrSettledRequestsAreNotActiveExceptTerminalRoutedCache: bool =
    RequestIds.forall(requestId =>
      if (settled.contains(requestId)) {
        all {
          not(isSome(pendingReads.get(requestId))),
          not(isSome(pendingNamespaces.get(requestId))),
          not(remoteWriteResponseHasRequest(requestId)),
          not(stageHasRequest(requestId)),
          not(repairHasRequest(requestId)),
          not(routedWriteIsActive(requestId)),
        }
      } else {
        true
      }
    )


  def requestIdHasAtMostOneActiveOwner: bool =
    RequestIds.forall(requestId => activeRequestMapCount(requestId) <= 1)


  def stageInflightRequestIdsAreUniqueByReplica: bool =
    RequestIds.forall(requestId =>
      Nodes.filter(node => stageInflight.get(node) == Some(requestId)).size() <= 1
    )


  def repairInflightRequestIdsAreUniqueByReplica: bool =
    RequestIds.forall(requestId =>
      Nodes.filter(node => repairInflight.get(node) == Some(requestId)).size() <= 1
    )


  def routedWriteTerminalCacheIsNotActive: bool =
    RequestIds.forall(requestId =>
      not(routedWriteIsTerminal(requestId)) or not(routedWriteIsActive(requestId))
    )


  def noWrongRequestSettles: bool =
    wrongSettles == Set()


  def discardedRequestIdsAreIssuedOrUntracked: bool =
    discarded.subseteq(RequestIds)


  def safety: bool = all {
    activeRequestIdsAreIssued,
    settledRequestIdsAreIssued,
    timedOutOrSettledRequestsAreNotActiveExceptTerminalRoutedCache,
    requestIdHasAtMostOneActiveOwner,
    stageInflightRequestIdsAreUniqueByReplica,
    repairInflightRequestIdsAreUniqueByReplica,
    routedWriteTerminalCacheIsNotActive,
    noWrongRequestSettles,
    discardedRequestIdsAreIssuedOrUntracked,
  }
}
