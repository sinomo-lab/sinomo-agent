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


  /// A tombstone on the retired physical source shard is migration history,
  /// not the logical path winner once the durable object-shard index and the
  /// path-selected target shard both expose the live object.
  def indexedTargetLiveObjectWinsRetiredSourceTombstone: bool =
    all {
      affectedRouteShards == Set(ServicesShard),
      sourceTombstonePresent,
      targetPathVisible,
      objectShardIndex == Some(ServicesShard),
    } implies all {
      legacyGlobalTombstoneFirstLookup == PathTombstone,
      shardAwareSelectedLookup == PathLive,
    }


  /// The replica-local index is derived cache state. Both a stale root entry
  /// and an absent entry must leave the current path-selected services row
  /// visible; repairing the index is a later convergence step.
  def replicaDerivedIndexDoesNotAuthorizePathVisibility: bool =
    all {
      phase == Complete,
      affectedRouteShards == Set(ServicesShard),
      targetRowPresent,
      any {
        replicaObjectShardIndex == Some(RootShard),
        replicaObjectShardIndex == None,
      },
    } implies shardAwareSelectedLookup == PathLive


  def legacyShadowingExplainsPostCommitFalseRedirect: bool =
    postMoveProbe.outcome == LegacyResurrectionThenUncorroboratedRedirect implies all {
      phase == Complete,
      legacyGlobalTombstoneFirstLookup == PathTombstone,
      postMoveProbe.legacyBackendLookupBefore == PathMissing,
      shardAwareSelectedLookup == PathLive,
      postMoveProbe.committedMutations == 1,
      postMoveProbe.replicaIndexBeforeLookup == Some(RootShard),
      postMoveProbe.derivedIndexRepairs == 0,
      replicaObjectShardIndex == Some(ServicesShard),
      postMoveProbe.reportedRedirectShard == Some(RootShard),
      not(postMoveProbe.redirectAccepted),
      not(postMoveProbe.returnedSuccess),
    }


  def shardAwareMkdirIsAZeroMutationSuccessfulNoOp: bool =
    postMoveProbe.outcome == ShardAwareExistingDirectorySucceeded implies all {
      phase == Complete,
      shardAwareSelectedLookup == PathLive,
      postMoveProbe.committedMutations == 0,
      postMoveProbe.replicaIndexBeforeLookup != Some(ServicesShard),
      postMoveProbe.derivedIndexRepairs == 1,
      replicaObjectShardIndex == Some(ServicesShard),
      postMoveProbe.reportedRedirectShard == None,
      not(postMoveProbe.redirectAccepted),
      postMoveProbe.returnedSuccess,
    }


  def retiredSourceRedirectIsNeverAcceptedForServicesDispatch: bool =
    postMoveProbe.reportedRedirectShard == Some(RootShard) implies all {
      affectedRouteShards == Set(ServicesShard),
      postMoveProbe.outcome == LegacyResurrectionThenUncorroboratedRedirect,
      not(postMoveProbe.redirectAccepted),
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


  def witnessRetiredSourceTombstoneWouldShadowIndexedLiveObject: bool = all {
    phase == Complete,
    affectedRouteShards == Set(ServicesShard),
    sourceTombstonePresent,
    targetPathVisible,
    replicaObjectShardIndex == Some(RootShard),
    legacyGlobalTombstoneFirstLookup == PathTombstone,
    legacyReplicaBackendLookup == PathMissing,
    shardAwareSelectedLookup == PathLive,
  }


  def witnessLegacyMkdirCommitsBeforeUncorroboratedRedirect: bool = all {
    postMoveProbe.outcome == LegacyResurrectionThenUncorroboratedRedirect,
    postMoveProbe.committedMutations == 1,
    postMoveProbe.reportedRedirectShard == Some(RootShard),
    not(postMoveProbe.redirectAccepted),
    not(postMoveProbe.returnedSuccess),
    legacyShadowingExplainsPostCommitFalseRedirect,
  }


  def witnessShardAwareLookupPrefersPathSelectedLiveObject: bool = all {
    phase == Complete,
    affectedRouteShards == Set(ServicesShard),
    sourceTombstonePresent,
    objectShardIndex == Some(ServicesShard),
    replicaObjectShardIndex == Some(RootShard),
    targetRowPresent,
    shardAwareSelectedLookup == PathLive,
    indexedTargetLiveObjectWinsRetiredSourceTombstone,
  }


  def witnessMissingReplicaIndexStillSeesPathSelectedLiveObject: bool = all {
    phase == Complete,
    affectedRouteShards == Set(ServicesShard),
    sourceTombstonePresent,
    replicaObjectShardIndex == None,
    targetRowPresent,
    shardAwareSelectedLookup == PathLive,
    replicaDerivedIndexDoesNotAuthorizePathVisibility,
  }


  def witnessShardAwareMkdirRepairsStaleIndexWithoutMutation: bool = all {
    postMoveProbe.outcome == ShardAwareExistingDirectorySucceeded,
    postMoveProbe.replicaIndexBeforeLookup == Some(RootShard),
    postMoveProbe.committedMutations == 0,
    postMoveProbe.derivedIndexRepairs == 1,
    postMoveProbe.reportedRedirectShard == None,
    postMoveProbe.returnedSuccess,
    shardAwareMkdirIsAZeroMutationSuccessfulNoOp,
  }


  def witnessShardAwareMkdirRepairsMissingIndexWithoutMutation: bool = all {
    postMoveProbe.outcome == ShardAwareExistingDirectorySucceeded,
    postMoveProbe.replicaIndexBeforeLookup == None,
    postMoveProbe.committedMutations == 0,
    postMoveProbe.derivedIndexRepairs == 1,
    postMoveProbe.reportedRedirectShard == None,
    postMoveProbe.returnedSuccess,
    shardAwareMkdirIsAZeroMutationSuccessfulNoOp,
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
    indexedTargetLiveObjectWinsRetiredSourceTombstone,
    replicaDerivedIndexDoesNotAuthorizePathVisibility,
    legacyShadowingExplainsPostCommitFalseRedirect,
    shardAwareMkdirIsAZeroMutationSuccessfulNoOp,
    retiredSourceRedirectIsNeverAcceptedForServicesDispatch,
    replayDoesNotCreateNewVisibleSource,
    noObjectScenarioHasNoRows,
  }
}
