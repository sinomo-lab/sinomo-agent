/// Invariant checks for `dfs_routed_namespace_freshness`.
module dfs_routed_namespace_freshness_safety {
  import common.* from "../../lib/common"
  import dfs_routed_namespace_freshness.* from "../../models/dfs/routed_namespace_freshness"

  def journalSourceIsCurrentPrimary: bool =
    acceptedJournalBatchSources.forall(source => source == currentPrimary)


  def completedReturnsAuthoritativePathState: bool =
    terminalOutcome == CompletedFresh implies returnedPathState == primaryPathState


  def localCompletionOnlyFromFreshReplica: bool =
    all {
      terminalOutcome == CompletedFresh,
      not(answeredByPrimary),
    } implies all {
      requesterRole == MetadataReplica,
      requesterHasFreshPath(
        localPathState,
        primaryPathState,
        requesterSeq,
        targetSeq,
        syncedPathApplied
      ),
    }


  def partialJournalBatchCannotCompleteLocally: bool =
    all {
      terminalOutcome == CompletedFresh,
      not(answeredByPrimary),
    } implies not(lastBatchLimitReached)


  def nonReplicaNeverIngests: bool =
    requesterRole == NonReplica implies all {
      acceptedJournalBatchSources.size() == 0,
      not(syncedPathApplied),
    }


  def nonReplicaCompletesFromPrimary: bool =
    all {
      terminalOutcome == CompletedFresh,
      requesterRole == NonReplica,
    } implies answeredByPrimary


  def routedMutationReadYourWrite: bool =
    all {
      terminalOutcome == CompletedFresh,
      operation == RoutedCreate or operation == RoutedDelete,
    } implies returnedPathState == mutationTargetState(operation)


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
    localCompletionOnlyFromFreshReplica,
    partialJournalBatchCannotCompleteLocally,
    nonReplicaNeverIngests,
    nonReplicaCompletesFromPrimary,
    routedMutationReadYourWrite,
    primaryTombstoneWinsOverStaleLiveLocal,
    terminalOutcomesAreConsistent,
    sequenceBounds,
  }
}
