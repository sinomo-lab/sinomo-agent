/// Invariant checks for `dfs_posix_cross_shard_namespace`.
module dfs_posix_cross_shard_namespace_safety {
  import common.* from "../../lib/common"
  import dfs_posix_cross_shard_namespace.* from "../../models/dfs/posix_cross_shard_namespace"

  def activeJobsAreCrossShard: bool =
    jobPhase != Idle implies crossShard(sourcePath, destinationPath)


  def activeOrRecoverableJobsBlockTouchedPaths: bool =
    any {
      jobPhase == Prepared,
      jobPhase == DestinationAdopted,
      jobPhase == FailedRecoverable,
    } implies all {
      blockedPaths.contains(sourcePath),
      blockedPaths.contains(destinationPath),
    }


  def failedBeforeDestinationLeavesNoChange: bool =
    jobPhase == FailedNoChange implies all {
      kindAt == beforeKindAt,
      objectAt == beforeObjectAt,
      blockedPaths == Set(),
    }


  def recoverableFailureKeepsExplicitJob: bool =
    jobPhase == FailedRecoverable implies all {
      blockedPaths.contains(sourcePath),
      blockedPaths.contains(destinationPath),
      isLive(kindAt.get(destinationPath)),
    }


  def completedRenameHasSingleVisiblePath: bool =
    all {
      jobPhase == Completed,
      jobOp == RenameMove,
    } implies all {
      kindAt.get(sourcePath) == Tombstone,
      isLive(kindAt.get(destinationPath)),
      objectAt.get(sourcePath) == None,
      objectAt.get(destinationPath) == beforeObjectAt.get(sourcePath),
    }


  def completedHardlinkPreservesSourceAndAlias: bool =
    all {
      jobPhase == Completed,
      jobOp == HardlinkAlias,
    } implies all {
      isLive(kindAt.get(sourcePath)),
      kindAt.get(destinationPath) == Hardlink,
      objectAt.get(destinationPath) == beforeObjectAt.get(sourcePath),
      objectAt.get(sourcePath) == beforeObjectAt.get(sourcePath),
    }


  def halfVisibleRenameRequiresRecoverableOrActiveJob: bool =
    all {
      jobOp == RenameMove,
      isLive(kindAt.get(sourcePath)),
      isLive(kindAt.get(destinationPath)),
      objectAt.get(sourcePath) == objectAt.get(destinationPath),
    } implies any {
      jobPhase == DestinationAdopted,
      jobPhase == FailedRecoverable,
    }


  def retryCountInBounds: bool =
    retryCount >= 0 and retryCount <= MaxRetries


  def safety: bool = all {
    activeJobsAreCrossShard,
    activeOrRecoverableJobsBlockTouchedPaths,
    failedBeforeDestinationLeavesNoChange,
    recoverableFailureKeepsExplicitJob,
    completedRenameHasSingleVisiblePath,
    completedHardlinkPreservesSourceAndAlias,
    halfVisibleRenameRequiresRecoverableOrActiveJob,
    retryCountInBounds,
  }
}
