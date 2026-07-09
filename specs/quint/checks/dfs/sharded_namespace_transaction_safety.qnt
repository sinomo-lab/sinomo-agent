/// Invariant checks for `dfs_sharded_namespace_transaction`.
module dfs_sharded_namespace_transaction_safety {
  import common.* from "../../lib/common"
  import dfs_sharded_namespace_transaction.* from "../../models/dfs/sharded_namespace_transaction"

  def affectedRouteShardsMatchServiceHelper: bool =
    affectedRouteShards == affectedRouteShardsFor(
      controlPlaneConfigured,
      transactionShape
    )


  def sourceCleanupShardComesFromExistingIndex: bool =
    sourceCleanupShards == initialSourceCleanupShards(transactionShape)


  def targetRowsComeFromRecordedTargetTransactions: bool =
    targetRowPresent implies targetTxnRecorded


  def indexPointsOnlyAtMaterializedTarget: bool =
    objectShardIndex == Some(ServicesShard) implies targetRowPresent


  def targetIndexNeverMovesBack: bool =
    targetIndexEverWritten implies objectShardIndex == Some(ServicesShard)


  def sourceTombstoneAfterTargetIndex: bool =
    sourceTombstonePresent implies all {
      targetRowPresent,
      targetIndexEverWritten,
      objectShardIndex == Some(ServicesShard),
    }


  def noTwoVisiblePathsForObject: bool =
    not(sourcePathVisible and targetPathVisible)


  def targetShardFailurePreservesSourceLookup: bool =
    phase == TargetShardFailed implies all {
      sourcePathVisible,
      objectShardIndex == Some(RootShard),
      not(targetRowPresent),
      not(targetTxnRecorded),
      not(targetPathVisible),
    }


  def indexWriteFailurePreservesSourceLookup: bool =
    phase == TargetIndexFailed implies all {
      sourcePathVisible,
      targetRowPresent,
      targetTxnRecorded,
      not(targetPathVisible),
      objectShardIndex == Some(RootShard),
    }


  def sourceTombstoneFailureHidesStaleSourceLookup: bool =
    phase == SourceTombstoneFailed implies all {
      objectByIdUsesTargetShard,
      targetPathVisible,
      not(sourcePathVisible),
      sourceRowPresent,
      not(sourceTombstonePresent),
    }


  def completeMoveHasOnlyTargetVisible: bool =
    phase == Complete implies all {
      objectByIdUsesTargetShard,
      targetPathVisible,
      not(sourcePathVisible),
      sourceTombstonePresent,
      sourceTombstoneRecorded,
    }


  def replayDoesNotCreateNewVisibleSource: bool =
    replayedShards.contains(ServicesShard) implies all {
      targetRowPresent,
      targetTxnRecorded,
      not(sourcePathVisible and targetPathVisible),
    }


  def noObjectScenarioHasNoRows: bool =
    phase == NoOp implies all {
      not(transactionHasObjects),
      affectedRouteShards.size() == 0,
      sourceCleanupShards.size() == 0,
      not(sourceRowPresent),
      not(targetRowPresent),
      objectShardIndex == None,
    }


  def safety: bool = all {
    affectedRouteShardsMatchServiceHelper,
    sourceCleanupShardComesFromExistingIndex,
    targetRowsComeFromRecordedTargetTransactions,
    indexPointsOnlyAtMaterializedTarget,
    targetIndexNeverMovesBack,
    sourceTombstoneAfterTargetIndex,
    noTwoVisiblePathsForObject,
    targetShardFailurePreservesSourceLookup,
    indexWriteFailurePreservesSourceLookup,
    sourceTombstoneFailureHidesStaleSourceLookup,
    completeMoveHasOnlyTargetVisible,
    replayDoesNotCreateNewVisibleSource,
    noObjectScenarioHasNoRows,
  }
}
