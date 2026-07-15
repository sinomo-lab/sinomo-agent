/// Safety and refinement checks for DFS coordinator sparse intent indexes.
module dfs_intent_index_equivalence_safety {
  import common.* from "../../lib/common"
  import dfs_intent_index_equivalence.* from "../../models/dfs/intent_index_equivalence"

  def validationResultsAreEquivalent: bool =
    Objects.forall(object =>
      Targets.forall(target =>
        referenceValidation(object, target)
          == indexedValidation(object, target)
      )
    )

  def awaitingTargetLookupsAreEquivalent: bool =
    ObjectIds.forall(objectId =>
      Targets.forall(target =>
        referenceHasAwaitingTarget(objectId, target)
          == indexedHasAwaitingTarget(objectId, target)
      )
    )

  def openIntentLookupsAreEquivalent: bool =
    ObjectIds.forall(objectId =>
      referenceHasOpenIntent(objectId) == indexedHasOpenIntent(objectId)
    )

  def awaitingIndexPointsAtExactActiveIntent: bool =
    Objects.forall(object =>
      match awaitingByObject.get(object) {
        | Some(intentId) =>
            match intents.get(intentId) {
              | Some(intent) => all {
                  intent.object == object,
                  intent.state == Awaiting,
                }
              | None => false
            }
        | None => referenceAwaitingIds(object).size() == 0
      }
    )

  def objectIdAwaitingIndexExactlyAggregatesHistory: bool =
    ObjectIds.forall(objectId =>
      awaitingByObjectId.get(objectId)
        == referenceAwaitingIdsByObjectId(objectId)
    )

  def everyAwaitingIntentIsIndexedExactly: bool =
    IntentIds.forall(intentId =>
      match intents.get(intentId) {
        | Some(intent) =>
            intent.state == Awaiting implies all {
              awaitingByObject.get(intent.object) == Some(intentId),
              awaitingByObjectId.get(objectIdOf(intent.object))
                .contains(intentId),
              claimedByTarget.get(intent.object).get(intent.target)
                .contains(intentId),
            }
        | None => true
      }
    )

  def targetIndexPointsAtExactClaim: bool =
    Objects.forall(object =>
      Targets.forall(target =>
        claimedByTarget.get(object).get(target)
          == referenceClaimedTargetIds(object, target)
      )
    )

  def everyActiveOrCommittedClaimIsIndexedExactly: bool =
    IntentIds.forall(intentId =>
      match intents.get(intentId) {
        | Some(intent) =>
            any {
              intent.state == Awaiting,
              intent.state == Committed,
            } implies
              claimedByTarget.get(intent.object).get(intent.target)
                .contains(intentId)
        | None => true
      }
    )

  def awaitingIntentIsUniquePerObject: bool =
    Objects.forall(object => referenceAwaitingIds(object).size() <= 1)

  def successfulStartUpdatedAllViews: bool =
    lastTransition == StartSucceeded implies all {
      intents == startIntents(
        beforeIntents,
        lastIntent,
        lastObject,
        lastTarget
      ),
      awaitingByObject == startAwaitingIndex(
        beforeAwaitingByObject,
        lastIntent,
        lastObject
      ),
      awaitingByObjectId == startAwaitingObjectIdIndex(
        beforeAwaitingByObjectId,
        lastIntent,
        lastObject
      ),
      claimedByTarget == startTargetIndex(
        beforeClaimedByTarget,
        lastIntent,
        lastObject,
        lastTarget
      ),
    }

  def failedStartRolledBackAllViews: bool =
    lastTransition == StartRolledBack implies all {
      intents == beforeIntents,
      awaitingByObject == beforeAwaitingByObject,
      awaitingByObjectId == beforeAwaitingByObjectId,
      claimedByTarget == beforeClaimedByTarget,
    }

  def commitRemovedOnlyAwaitingLookup: bool =
    lastTransition == CommitSucceeded implies all {
      intents == closeIntents(beforeIntents, lastIntent, Committed),
      awaitingByObject == removeAwaitingIndex(
        beforeAwaitingByObject,
        lastIntent,
        lastObject
      ),
      awaitingByObjectId == removeAwaitingObjectIdIndex(
        beforeAwaitingByObjectId,
        lastIntent,
        lastObject
      ),
      claimedByTarget == beforeClaimedByTarget,
      indexedValidation(lastObject, lastTarget) == DuplicateTarget,
    }

  def abortRemovedBothSparseLookups: bool =
    lastTransition == AbortSucceeded implies all {
      intents == closeIntents(beforeIntents, lastIntent, Aborted),
      awaitingByObject == removeAwaitingIndex(
        beforeAwaitingByObject,
        lastIntent,
        lastObject
      ),
      awaitingByObjectId == removeAwaitingObjectIdIndex(
        beforeAwaitingByObjectId,
        lastIntent,
        lastObject
      ),
      claimedByTarget == removeTargetIndex(
        beforeClaimedByTarget,
        lastIntent,
        lastObject,
        lastTarget
      ),
    }

  def restoreUpdatedExactViews: bool =
    lastTransition == RestoreSucceeded implies all {
      intents == restoreIntents(
        beforeIntents,
        lastIntent,
        lastObject,
        lastTarget,
        lastState
      ),
      awaitingByObject == restoreAwaitingIndex(
        beforeAwaitingByObject,
        beforeIntents.get(lastIntent),
        lastIntent,
        lastObject,
        lastState
      ),
      awaitingByObjectId == restoreAwaitingObjectIdIndex(
        beforeAwaitingByObjectId,
        beforeIntents.get(lastIntent),
        lastIntent,
        lastObject,
        lastState
      ),
      claimedByTarget == restoreTargetIndex(
        beforeClaimedByTarget,
        beforeIntents.get(lastIntent),
        lastIntent,
        lastObject,
        lastTarget,
        lastState
      ),
    }

  def witnessAwaitingTargetIndexed: bool = all {
    lastTransition == StartSucceeded,
    awaitingByObjectId.get(objectIdOf(lastObject)).contains(lastIntent),
    indexedHasAwaitingTarget(objectIdOf(lastObject), lastTarget),
    validationResultsAreEquivalent,
  }

  def witnessDuplicateTargetRejected: bool = all {
    indexedValidation(lastObject, lastTarget) == DuplicateTarget,
    referenceValidation(lastObject, lastTarget) == DuplicateTarget,
  }

  def witnessPendingDifferentTargetRejected: bool =
    Objects.exists(object =>
      Targets.exists(target => all {
        indexedValidation(object, target) == PendingObject,
        referenceValidation(object, target) == PendingObject,
      })
    )

  def witnessCommitRetainsTargetClaim: bool = all {
    lastTransition == CommitSucceeded,
    awaitingByObject.get(lastObject) == None,
    not(awaitingByObjectId.get(objectIdOf(lastObject)).contains(lastIntent)),
    claimedByTarget.get(lastObject).get(lastTarget).contains(lastIntent),
    indexedValidation(lastObject, lastTarget) == DuplicateTarget,
  }

  def witnessAbortFreesIndexes: bool = all {
    lastTransition == AbortSucceeded,
    awaitingByObject.get(lastObject) == None,
    not(awaitingByObjectId.get(objectIdOf(lastObject)).contains(lastIntent)),
    claimedByTarget.get(lastObject).get(lastTarget).size() == 0,
    indexedValidation(lastObject, lastTarget) == Accept,
  }

  def witnessFailedStartRollback: bool = all {
    lastTransition == StartRolledBack,
    failedStartRolledBackAllViews,
  }

  def witnessRestoreAwaitingIndexes: bool = all {
    lastTransition == RestoreSucceeded,
    lastState == Awaiting,
    beforeIntents.get(lastIntent) == None,
    awaitingByObject.get(lastObject) == Some(lastIntent),
    awaitingByObjectId.get(objectIdOf(lastObject)).contains(lastIntent),
    claimedByTarget.get(lastObject).get(lastTarget).contains(lastIntent),
    restoreUpdatedExactViews,
  }

  def witnessRestoreCommittedClaim: bool = all {
    lastTransition == RestoreSucceeded,
    lastState == Committed,
    beforeIntents.get(lastIntent) == None,
    awaitingByObject.get(lastObject) == beforeAwaitingByObject.get(lastObject),
    awaitingByObjectId == beforeAwaitingByObjectId,
    claimedByTarget.get(lastObject).get(lastTarget).contains(lastIntent),
    restoreUpdatedExactViews,
  }

  def witnessRestoreAbortedHistoryOnly: bool = all {
    lastTransition == RestoreSucceeded,
    lastState == Aborted,
    beforeIntents.get(lastIntent) == None,
    awaitingByObject == beforeAwaitingByObject,
    awaitingByObjectId == beforeAwaitingByObjectId,
    claimedByTarget == beforeClaimedByTarget,
    restoreUpdatedExactViews,
  }

  def witnessRestoreAwaitingToCommitted: bool = all {
    lastTransition == RestoreSucceeded,
    lastState == Committed,
    isSome(beforeIntents.get(lastIntent)),
    unwrapIntent(beforeIntents.get(lastIntent)).state == Awaiting,
    awaitingByObject.get(lastObject) == None,
    not(awaitingByObjectId.get(objectIdOf(lastObject)).contains(lastIntent)),
    claimedByTarget.get(lastObject).get(lastTarget).contains(lastIntent),
    restoreUpdatedExactViews,
  }

  def witnessRestoreCommittedToAborted: bool = all {
    lastTransition == RestoreSucceeded,
    lastState == Aborted,
    isSome(beforeIntents.get(lastIntent)),
    unwrapIntent(beforeIntents.get(lastIntent)).state == Committed,
    awaitingByObject.get(lastObject) == None,
    not(awaitingByObjectId.get(objectIdOf(lastObject)).contains(lastIntent)),
    not(claimedByTarget.get(lastObject).get(lastTarget).contains(lastIntent)),
    restoreUpdatedExactViews,
  }

  def witnessRestoreAbortedToAwaiting: bool = all {
    lastTransition == RestoreSucceeded,
    lastState == Awaiting,
    isSome(beforeIntents.get(lastIntent)),
    unwrapIntent(beforeIntents.get(lastIntent)).state == Aborted,
    awaitingByObject.get(lastObject) == Some(lastIntent),
    awaitingByObjectId.get(objectIdOf(lastObject)).contains(lastIntent),
    claimedByTarget.get(lastObject).get(lastTarget).contains(lastIntent),
    restoreUpdatedExactViews,
  }

  def witnessCrossFileSystemSharedObjectAggregates: bool = all {
    isSome(awaitingByObject.get(FsAShared)),
    isSome(awaitingByObject.get(FsBShared)),
    fileSystemOf(FsAShared) != fileSystemOf(FsBShared),
    objectIdOf(FsAShared) == objectIdOf(FsBShared),
    awaitingByObjectId.get(SharedObject).size() == 2,
    referenceAwaitingIdsByObjectId(SharedObject).size() == 2,
    indexedHasOpenIntent(SharedObject),
  }

  def witnessCrossFileSystemTargetLookup: bool = all {
    witnessCrossFileSystemSharedObjectAggregates,
    indexedHasAwaitingTarget(SharedObject, Target1),
    referenceHasAwaitingTarget(SharedObject, Target1),
  }

  def witnessStartAddsSecondSharedFileSystemIntent: bool = all {
    lastTransition == StartSucceeded,
    objectIdOf(lastObject) == SharedObject,
    beforeAwaitingByObjectId.get(SharedObject).size() == 1,
    awaitingByObjectId.get(SharedObject).size() == 2,
    witnessCrossFileSystemSharedObjectAggregates,
  }

  def witnessRestoreAddsSecondSharedFileSystemIntent: bool = all {
    lastTransition == RestoreSucceeded,
    lastState == Awaiting,
    beforeIntents.get(lastIntent) == None,
    objectIdOf(lastObject) == SharedObject,
    beforeAwaitingByObjectId.get(SharedObject).size() == 1,
    awaitingByObjectId.get(SharedObject).size() == 2,
    witnessCrossFileSystemSharedObjectAggregates,
  }

  def witnessCommitKeepsOtherSharedFileSystemIntentOpen: bool = all {
    lastTransition == CommitSucceeded,
    objectIdOf(lastObject) == SharedObject,
    beforeAwaitingByObjectId.get(SharedObject).size() == 2,
    awaitingByObjectId.get(SharedObject).size() == 1,
    indexedHasOpenIntent(SharedObject),
    not(awaitingByObjectId.get(SharedObject).contains(lastIntent)),
  }

  def witnessAbortKeepsOtherSharedFileSystemIntentOpen: bool = all {
    lastTransition == AbortSucceeded,
    objectIdOf(lastObject) == SharedObject,
    beforeAwaitingByObjectId.get(SharedObject).size() == 2,
    awaitingByObjectId.get(SharedObject).size() == 1,
    indexedHasOpenIntent(SharedObject),
    not(awaitingByObjectId.get(SharedObject).contains(lastIntent)),
  }

  def safety: bool = all {
    validationResultsAreEquivalent,
    awaitingTargetLookupsAreEquivalent,
    openIntentLookupsAreEquivalent,
    awaitingIndexPointsAtExactActiveIntent,
    objectIdAwaitingIndexExactlyAggregatesHistory,
    everyAwaitingIntentIsIndexedExactly,
    targetIndexPointsAtExactClaim,
    everyActiveOrCommittedClaimIsIndexedExactly,
    awaitingIntentIsUniquePerObject,
    successfulStartUpdatedAllViews,
    failedStartRolledBackAllViews,
    commitRemovedOnlyAwaitingLookup,
    abortRemovedBothSparseLookups,
    restoreUpdatedExactViews,
  }
}
