/// Invariant checks for `dfs_metadata_primary_failover`.
module dfs_metadata_primary_failover_safety {
  import common.* from "../../lib/common"
  import dfs_metadata_primary_failover.* from "../../models/dfs/metadata_primary_failover"

  def selectedPrimaryMatchesAlgorithm: bool =
    selectedPrimary
      == targetPrimaryAfterFailoverCheck(
        localNode,
        currentPrimary,
        defaultPrimary,
        localIsServiceNode,
        localInReplicaSet,
        connectivityGraphAvailable,
        routeToCurrentPrimaryHealthy,
        localShardPathExists,
        localShardJournalNonEmpty
      )


  /// `PromoteLocalReplica` is candidate selection only. This predicate proves
  /// the local durability precondition, not quorum-backed authority transfer.
  def localPromotionRequiresDurableReplica: bool =
    failoverDecision == PromoteLocalReplica implies
      localReplicaFailoverReady(
        localNode,
        currentPrimary,
        localIsServiceNode,
        localInReplicaSet,
        connectivityGraphAvailable,
        routeToCurrentPrimaryHealthy,
        localShardPathExists,
        localShardJournalNonEmpty
      )


  def reachablePrimaryIsNotLocallyPromoted: bool =
    routeToCurrentPrimaryHealthy implies failoverDecision != PromoteLocalReplica


  def missingConnectivityGraphIsNotLocallyPromoted: bool =
    not(connectivityGraphAvailable) implies failoverDecision != PromoteLocalReplica


  def nonReplicaIsNotLocallyPromoted: bool =
    not(localInReplicaSet) implies failoverDecision != PromoteLocalReplica


  def readyUnreachableLocalReplicaIsPromoted: bool =
    localReplicaFailoverReady(
      localNode,
      currentPrimary,
      localIsServiceNode,
      localInReplicaSet,
      connectivityGraphAvailable,
      routeToCurrentPrimaryHealthy,
      localShardPathExists,
      localShardJournalNonEmpty
    ) implies all {
      selectedPrimary == localNode,
      failoverDecision == PromoteLocalReplica,
    }


  def safety: bool = all {
    selectedPrimaryMatchesAlgorithm,
    localPromotionRequiresDurableReplica,
    reachablePrimaryIsNotLocallyPromoted,
    missingConnectivityGraphIsNotLocallyPromoted,
    nonReplicaIsNotLocallyPromoted,
    readyUnreachableLocalReplicaIsPromoted,
  }
}
