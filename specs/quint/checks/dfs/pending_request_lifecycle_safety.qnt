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


  def readIdentityErrorsAreRealMismatches: bool =
    readIdentityErrors.forall(error =>
      error.actualSource != error.expectedSource
        or error.actualObject != error.expectedObject
    )


  def readIdentityErrorsAreIssuedAndDiscarded: bool =
    readIdentityErrors.forall(error => all {
      issued.contains(error.requestId),
      discarded.contains(error.requestId),
    })


  def terminalWrongObjectReadResponsesDoNotSettle: bool =
    readIdentityErrors.forall(error =>
      if (error.actualSource == error.expectedSource
        and error.actualObject != error.expectedObject) {
        all {
          not(settled.contains(error.requestId)),
          not(isSome(pendingReads.get(error.requestId))),
        }
      } else {
        true
      }
    )


  def readResponseObservationsAreConsistent: bool =
    match lastObservation {
      | ReadSettled(requestId) => all {
          settled.contains(requestId),
          not(isSome(pendingReads.get(requestId))),
        }
      | ReadUnknownDiscarded(requestId) =>
          discarded.contains(requestId)
      | ReadWrongSourceDiscarded(requestId) => all {
          discarded.contains(requestId),
          isSome(pendingReads.get(requestId)),
          not(settled.contains(requestId)),
        }
      | ReadWrongObjectRejected(requestId) => all {
          discarded.contains(requestId),
          not(isSome(pendingReads.get(requestId))),
          not(settled.contains(requestId)),
        }
      | _ => true
    }


  /// Coverage witnesses are intentionally not part of `safety`; the runner
  /// reports whether exploration reached each response disposition.
  def successfulReadSettlementWitness: bool =
    match lastObservation {
      | ReadSettled(_) => true
      | _ => false
    }


  def wrongSourceDiscardWithPendingWitness: bool =
    match lastObservation {
      | ReadWrongSourceDiscarded(requestId) => all {
          isSome(pendingReads.get(requestId)),
          not(settled.contains(requestId)),
        }
      | _ => false
    }


  def wrongObjectTerminalRejectionWitness: bool =
    match lastObservation {
      | ReadWrongObjectRejected(requestId) => all {
          not(isSome(pendingReads.get(requestId))),
          not(settled.contains(requestId)),
        }
      | _ => false
    }


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
    readIdentityErrorsAreRealMismatches,
    readIdentityErrorsAreIssuedAndDiscarded,
    terminalWrongObjectReadResponsesDoNotSettle,
    readResponseObservationsAreConsistent,
    discardedRequestIdsAreIssuedOrUntracked,
  }
}
