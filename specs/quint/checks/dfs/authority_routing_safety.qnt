/// Invariant checks and bounded witnesses for `dfs_authority_routing`.
module dfs_authority_routing_safety {
  import common.* from "../../lib/common"
  import dfs_authority_routing.* from "../../models/dfs/authority_routing"

  def currentEpochsHaveCommittedPrimaries: bool = all {
    routing.primaryByEpoch.get(routing.currentEpoch) == Some(routing.currentPrimary),
    routing.companionPrimaryByEpoch.get(routing.companionEpoch)
      == Some(routing.companionPrimary),
  }

  def authorityCompletionsHaveExactCommittedDispatchEvidence: bool =
    Nodes.forall(requester =>
      match routing.completed.get(requester) {
        | Some(done) =>
            done.mode == UncorroboratedReplicaLiveHitCompletion or all {
              routing.primaryByEpoch.get(done.routingEpoch) == Some(done.primary),
              done.dispatchShape == SingleDispatch
                or routing.companionPrimaryByEpoch.get(done.companionEpoch)
                  == Some(done.primary),
            }
        | None => true
      }
    )

  def corroboratedRedirectEvidenceIsExactForEveryDispatchShard: bool =
    Nodes.forall(requester =>
      match routing.redirectEvidence.get(requester) {
        | Some(evidence) =>
            any {
              evidence.outcome == RedirectFollowed,
              evidence.outcome == CorroboratedLocalAuthorityUsed,
              evidence.outcome == CorroboratedRedirectUnroutable,
            } implies redirectEvidenceMatchesCommittedDispatch(routing, evidence)
        | None => true
      }
    )

  def uncorroboratedRedirectEvidenceNeverMatchesEveryDispatchShard: bool =
    Nodes.forall(requester =>
      match routing.redirectEvidence.get(requester) {
        | Some(evidence) =>
            any {
              evidence.outcome == UncorroboratedOutsiderRejected,
              evidence.outcome == UncorroboratedRemovedReplicaRejected,
              evidence.outcome == UncorroboratedReplicaLiveHitUsed,
              evidence.outcome == UncorroboratedReplicaMissingRejected,
              evidence.outcome == UncorroboratedReplicaTombstoneRejected,
              evidence.outcome == UncorroboratedNonLookupRejected,
            } implies not(redirectEvidenceMatchesCommittedDispatch(routing, evidence))
        | None => true
      }
    )

  /// A primary belonging only to an unrelated affected shard is never enough
  /// to authorize a retry of the dispatch request.
  def foreignOnlyRedirectCannotBeFollowed: bool =
    Nodes.forall(requester =>
      match routing.redirectEvidence.get(requester) {
        | Some(evidence) => all {
            evidence.reportedPrimary == routing.foreignAffectedPrimary,
            not(redirectEvidenceMatchesCommittedDispatch(routing, evidence)),
          } implies all {
            evidence.outcome != RedirectFollowed,
            evidence.outcome != CorroboratedLocalAuthorityUsed,
          }
        | None => true
      }
    )

  def localReplicaFallbackOnlyReturnsLiveLookupHit: bool =
    Nodes.forall(requester =>
      match routing.completed.get(requester) {
        | Some(done) =>
            done.mode != UncorroboratedReplicaLiveHitCompletion or all {
              done.op == NamespaceLookup,
              done.servedBy == requester,
              done.localPathState == LocalLive,
              match routing.redirectEvidence.get(requester) {
                | Some(evidence) => all {
                    evidence.outcome == UncorroboratedReplicaLiveHitUsed,
                    evidence.requesterRole == CurrentDispatchReplica,
                    evidence.localPathState == LocalLive,
                    not(redirectEvidenceMatchesCommittedDispatch(routing, evidence)),
                  }
                | None => false
              },
            }
        | None => true
      }
    )

  def uncorroboratedOutsidersFailClosed: bool =
    Nodes.forall(requester =>
      match routing.redirectEvidence.get(requester) {
        | Some(evidence) =>
            evidence.outcome == UncorroboratedOutsiderRejected implies all {
              evidence.requesterRole == DispatchOutsider,
              routing.failures.contains(requester),
              routing.completed.get(requester) == None,
            }
        | None => true
      }
    )

  def missingTombstoneAndNonLookupFallbacksFailClosed: bool =
    Nodes.forall(requester =>
      match routing.redirectEvidence.get(requester) {
        | Some(evidence) =>
            any {
              evidence.outcome == UncorroboratedReplicaMissingRejected,
              evidence.outcome == UncorroboratedReplicaTombstoneRejected,
              evidence.outcome == UncorroboratedNonLookupRejected,
            } implies all {
              routing.failures.contains(requester),
              routing.completed.get(requester) == None,
            }
        | None => true
      }
    )

  def removedReplicasFailClosed: bool =
    Nodes.forall(requester =>
      match routing.redirectEvidence.get(requester) {
        | Some(evidence) =>
            evidence.outcome == UncorroboratedRemovedReplicaRejected implies all {
              evidence.requesterRole == RemovedDispatchReplica,
              routing.failures.contains(requester),
              routing.completed.get(requester) == None,
            }
        | None => true
      }
    )

  def pendingRedirectCountersInBounds: bool =
    Nodes.forall(requester =>
      match routing.pendingRoute.get(requester) {
        | Some(pending) =>
            pending.redirectsRemaining >= 0 and pending.redirectsRemaining <= RedirectLimit
        | None => true
      }
    )

  def terminalRoutesHaveNoPendingState: bool =
    Nodes.forall(requester =>
      if (isSome(routing.completed.get(requester)) or routing.failures.contains(requester)) {
        not(isSome(routing.pendingRoute.get(requester)))
      } else {
        true
      }
    )

  def completedAndFailedAreDisjoint: bool =
    Nodes.forall(requester =>
      not(isSome(routing.completed.get(requester)) and routing.failures.contains(requester))
    )

  def selfRoutesStayDisabled: bool =
    Nodes.forall(node => not(routing.canReach.get(node).get(node)))

  def discardedResponsesAreOnlyObservations: bool =
    routing.discardedResponses.subseteq(Nodes)

  def epochsInBounds: bool = all {
    routing.currentEpoch >= 1,
    routing.currentEpoch <= MaxEpoch,
    routing.companionEpoch >= 1,
    routing.companionEpoch <= MaxEpoch,
  }

  def witnessCorroboratedDispatchRedirectFollowed: bool =
    Nodes.exists(requester =>
      match routing.redirectEvidence.get(requester) {
        | Some(evidence) => all {
            evidence.outcome == RedirectFollowed,
            evidence.dispatchShape == SingleDispatch,
            redirectEvidenceMatchesCommittedDispatch(routing, evidence),
            match routing.pendingRoute.get(requester) {
              | Some(pending) => pending.attemptedPrimary == evidence.reportedPrimary
              | None => false
            },
          }
        | None => false
      }
    )

  def witnessGroupedDispatchRedirectCorroboratesEveryShard: bool =
    Nodes.exists(requester =>
      match routing.redirectEvidence.get(requester) {
        | Some(evidence) => all {
            evidence.outcome == RedirectFollowed,
            evidence.dispatchShape == GroupedDispatch,
            redirectEvidenceMatchesCommittedDispatch(routing, evidence),
          }
        | None => false
      }
    )

  def witnessPartialGroupedDispatchRedirectRejected: bool =
    Nodes.exists(requester =>
      match routing.redirectEvidence.get(requester) {
        | Some(evidence) => all {
            evidence.dispatchShape == GroupedDispatch,
            routing.primaryByEpoch.get(evidence.routingEpoch)
              == Some(evidence.reportedPrimary),
            routing.companionPrimaryByEpoch.get(evidence.companionEpoch)
              != Some(evidence.reportedPrimary),
            evidence.outcome == UncorroboratedOutsiderRejected,
            routing.failures.contains(requester),
          }
        | None => false
      }
    )

  def witnessForeignAffectedPrimaryRedirectRejected: bool =
    Nodes.exists(requester =>
      match routing.redirectEvidence.get(requester) {
        | Some(evidence) => all {
            evidence.reportedPrimary == routing.foreignAffectedPrimary,
            not(redirectEvidenceMatchesCommittedDispatch(routing, evidence)),
            evidence.outcome == UncorroboratedOutsiderRejected,
            routing.failures.contains(requester),
          }
        | None => false
      }
    )

  def witnessUncorroboratedOutsiderFails: bool =
    Nodes.exists(requester =>
      match routing.redirectEvidence.get(requester) {
        | Some(evidence) => all {
            evidence.outcome == UncorroboratedOutsiderRejected,
            evidence.requesterRole == DispatchOutsider,
            routing.failures.contains(requester),
            routing.completed.get(requester) == None,
          }
        | None => false
      }
    )

  def witnessCurrentReplicaLiveHitFallback: bool =
    Nodes.exists(requester =>
      match routing.completed.get(requester) {
        | Some(done) => all {
            done.mode == UncorroboratedReplicaLiveHitCompletion,
            done.localPathState == LocalLive,
            done.servedBy == requester,
            localReplicaFallbackOnlyReturnsLiveLookupHit,
          }
        | None => false
      }
    )

  def witnessCurrentReplicaMissingHitRejected: bool =
    Nodes.exists(requester =>
      match routing.redirectEvidence.get(requester) {
        | Some(evidence) => all {
            evidence.outcome == UncorroboratedReplicaMissingRejected,
            evidence.localPathState == LocalMissing,
            routing.failures.contains(requester),
            routing.completed.get(requester) == None,
          }
        | None => false
      }
    )

  def witnessRemovedReplicaLiveHitRejected: bool =
    Nodes.exists(requester =>
      match routing.redirectEvidence.get(requester) {
        | Some(evidence) => all {
            evidence.outcome == UncorroboratedRemovedReplicaRejected,
            evidence.requesterRole == RemovedDispatchReplica,
            evidence.localPathState == LocalLive,
            routing.failures.contains(requester),
            routing.completed.get(requester) == None,
          }
        | None => false
      }
    )

  def witnessCurrentReplicaTombstoneRejected: bool =
    Nodes.exists(requester =>
      match routing.redirectEvidence.get(requester) {
        | Some(evidence) => all {
            evidence.outcome == UncorroboratedReplicaTombstoneRejected,
            evidence.localPathState == LocalTombstone,
            routing.failures.contains(requester),
            routing.completed.get(requester) == None,
          }
        | None => false
      }
    )

  def witnessDistinctForeignPrimaryDoesNotBlockDispatchCompletion: bool =
    Nodes.exists(requester =>
      match routing.completed.get(requester) {
        | Some(done) => all {
            done.mode == RemoteAuthorityCompletion,
            done.primary != routing.foreignAffectedPrimary,
            done.dispatchShape == SingleDispatch,
            routing.primaryByEpoch.get(done.routingEpoch) == Some(done.primary),
          }
        | None => false
      }
    )

  def safety: bool = all {
    currentEpochsHaveCommittedPrimaries,
    authorityCompletionsHaveExactCommittedDispatchEvidence,
    corroboratedRedirectEvidenceIsExactForEveryDispatchShard,
    uncorroboratedRedirectEvidenceNeverMatchesEveryDispatchShard,
    foreignOnlyRedirectCannotBeFollowed,
    localReplicaFallbackOnlyReturnsLiveLookupHit,
    uncorroboratedOutsidersFailClosed,
    missingTombstoneAndNonLookupFallbacksFailClosed,
    removedReplicasFailClosed,
    pendingRedirectCountersInBounds,
    terminalRoutesHaveNoPendingState,
    completedAndFailedAreDisjoint,
    selfRoutesStayDisabled,
    discardedResponsesAreOnlyObservations,
    epochsInBounds,
  }
}
