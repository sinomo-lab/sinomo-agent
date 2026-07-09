/// Invariant checks for `dfs_routed_namespace_freshness`.
module dfs_routed_namespace_freshness_safety {
  import common.* from "../../lib/common"
  import dfs_routed_namespace_freshness.* from "../../models/dfs/routed_namespace_freshness"

  def journalSourceIsCurrentPrimary: bool =
    acceptedJournalBatchSources.forall(source => source == currentPrimary)


  def completedReturnsAuthoritativePathState: bool =
    terminalOutcome == CompletedFresh implies returnedPathState == primaryPathState


  def completedOnlyWhenRequesterFresh: bool =
    terminalOutcome == CompletedFresh implies
      requesterHasFreshPath(
        localPathState,
        primaryPathState,
        requesterSeq,
        targetSeq,
        syncedPathApplied
      )


  def partialJournalBatchCannotComplete: bool =
    terminalOutcome == CompletedFresh implies not(lastBatchLimitReached)


  def nonReplicaCompletionRequiresPrimaryCatchUp: bool =
    all {
      terminalOutcome == CompletedFresh,
      requesterRole == NonReplicaMirror,
    } implies any {
      requesterSeq >= targetSeq,
      syncedPathApplied,
    }


  def routedMutationReadYourWrite: bool =
    all {
      terminalOutcome == CompletedFresh,
      operation == RoutedCreate or operation == RoutedDelete,
    } implies all {
      returnedPathState == mutationTargetState(operation),
      requesterHasFreshPath(
        localPathState,
        mutationTargetState(operation),
        requesterSeq,
        targetSeq,
        syncedPathApplied
      ),
    }


  def primaryTombstoneWinsOverStaleLiveLocal: bool =
    all {
      terminalOutcome == CompletedFresh,
      primaryPathState == PathTombstone,
    } implies returnedPathState == PathTombstone


  def terminalOutcomesAreConsistent: bool =
    phase == Terminal implies terminalOutcome != NoOutcome


  def sequenceBounds: bool =
    all {
      SeqValues.contains(primarySeq),
      SeqValues.contains(requesterSeq),
      SeqValues.contains(targetSeq),
      requesterSeq <= targetSeq,
      targetSeq == primarySeq,
    }


  def safety: bool = all {
    journalSourceIsCurrentPrimary,
    completedReturnsAuthoritativePathState,
    completedOnlyWhenRequesterFresh,
    partialJournalBatchCannotComplete,
    nonReplicaCompletionRequiresPrimaryCatchUp,
    routedMutationReadYourWrite,
    primaryTombstoneWinsOverStaleLiveLocal,
    terminalOutcomesAreConsistent,
    sequenceBounds,
  }
}
