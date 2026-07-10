/// DFS POSIX cross-shard namespace correctness.
///
/// This module models the distributed namespace job used when POSIX rename or
/// hardlink crosses metadata shards. Lower layers provide shard-primary routing
/// and metadata transaction atomicity; this layer states the visible job
/// contract: retries are idempotent, conflicting local operations stay blocked,
/// terminal rename does not leave both old and new paths live, and failures are
/// either no-change or explicitly recoverable.
module dfs_posix_cross_shard_namespace {
  import common.* from "../../lib/common"

  type CrossPath = LeftFile | LeftAlias | RightFile | RightAlias
  type CrossShard = LeftShard | RightShard
  type CrossObject = ObjA | ObjB
  type CrossKind = Missing | File | Hardlink | Tombstone
  type CrossOp = RenameMove | HardlinkAlias
  type JobPhase =
    | Idle
    | Prepared
    | DestinationAdopted
    | Completed
    | FailedNoChange
    | FailedRecoverable
  type CrossDecision =
    | NoDecision
    | JobStarted
    | DestinationApplied
    | SourceFinalized
    | FailedBeforeDestination
    | FailedAfterDestination
    | RecoverableRetried
    | RetryBudgetExhaustedForwarded
    | TerminalStutter

  pure val Paths: Set[CrossPath] =
    Set(LeftFile, LeftAlias, RightFile, RightAlias)
  pure val SourcePaths: Set[CrossPath] = Set(LeftFile, LeftAlias)
  pure val DestinationPaths: Set[CrossPath] = Set(RightFile, RightAlias)
  pure val MaxRetries: int = 3

  var kindAt: CrossPath -> CrossKind
  var objectAt: CrossPath -> Option[CrossObject]
  var beforeKindAt: CrossPath -> CrossKind
  var beforeObjectAt: CrossPath -> Option[CrossObject]
  var jobOp: CrossOp
  var jobPhase: JobPhase
  var sourcePath: CrossPath
  var destinationPath: CrossPath
  var blockedPaths: Set[CrossPath]
  var retryCount: int
  var lastDecision: CrossDecision

  pure val initialKinds: CrossPath -> CrossKind =
    Paths.mapBy(path =>
      if (path == LeftFile) { File } else { Missing }
    )

  pure val initialObjects: CrossPath -> Option[CrossObject] =
    Paths.mapBy(path =>
      if (path == LeftFile) { Some(ObjA) } else { None }
    )

  pure def shardOf(path: CrossPath): CrossShard =
    match path {
      | LeftFile => LeftShard
      | LeftAlias => LeftShard
      | RightFile => RightShard
      | RightAlias => RightShard
    }

  pure def isLive(kind: CrossKind): bool =
    kind != Missing and kind != Tombstone

  pure def crossShard(source: CrossPath, dest: CrossPath): bool =
    shardOf(source) != shardOf(dest)

  pure def sourceObject(objects: CrossPath -> Option[CrossObject], path: CrossPath): CrossObject =
    unwrapOr(objects.get(path), ObjA)

  pure def adoptedKind(op: CrossOp): CrossKind =
    match op {
      | RenameMove => File
      | HardlinkAlias => Hardlink
    }

  pure def destinationMatchesJob(
    kinds: CrossPath -> CrossKind,
    objects: CrossPath -> Option[CrossObject],
    op: CrossOp,
    source: CrossPath,
    dest: CrossPath,
    sourceObjectId: CrossObject
  ): bool =
    all {
      kinds.get(dest) == adoptedKind(op),
      objects.get(dest) == Some(sourceObjectId),
      if (op == HardlinkAlias) {
        isLive(kinds.get(source)) and objects.get(source) == Some(sourceObjectId)
      } else {
        true
      },
    }

  action init = all {
    kindAt' = initialKinds,
    objectAt' = initialObjects,
    beforeKindAt' = initialKinds,
    beforeObjectAt' = initialObjects,
    jobOp' = RenameMove,
    jobPhase' = Idle,
    sourcePath' = LeftFile,
    destinationPath' = RightFile,
    blockedPaths' = Set(),
    retryCount' = 0,
    lastDecision' = NoDecision,
  }

  action startJob(op: CrossOp, source: CrossPath, dest: CrossPath): bool = all {
    jobPhase == Idle or jobPhase == Completed or jobPhase == FailedNoChange,
    crossShard(source, dest),
    isLive(kindAt.get(source)),
    kindAt.get(source) != Hardlink or objectAt.get(source) != None,
    not(isLive(kindAt.get(dest))),
    beforeKindAt' = kindAt,
    beforeObjectAt' = objectAt,
    jobOp' = op,
    jobPhase' = Prepared,
    sourcePath' = source,
    destinationPath' = dest,
    blockedPaths' = Set(source, dest),
    retryCount' = 0,
    kindAt' = kindAt,
    objectAt' = objectAt,
    lastDecision' = JobStarted,
  }

  action applyDestination: bool = {
    val object = sourceObject(objectAt, sourcePath)
    all {
      jobPhase == Prepared or jobPhase == FailedRecoverable,
      retryCount < MaxRetries,
      kindAt' = kindAt.set(destinationPath, adoptedKind(jobOp)),
      objectAt' = objectAt.set(destinationPath, Some(object)),
      beforeKindAt' = beforeKindAt,
      beforeObjectAt' = beforeObjectAt,
      jobOp' = jobOp,
      jobPhase' = DestinationAdopted,
      sourcePath' = sourcePath,
      destinationPath' = destinationPath,
      blockedPaths' = Set(sourcePath, destinationPath),
      retryCount' = retryCount + 1,
      lastDecision' = DestinationApplied,
    }
  }

  action finalizeSource: bool = all {
    jobPhase == DestinationAdopted,
    kindAt' =
      if (jobOp == RenameMove) { kindAt.set(sourcePath, Tombstone) } else { kindAt },
    objectAt' =
      if (jobOp == RenameMove) { objectAt.set(sourcePath, None) } else { objectAt },
    beforeKindAt' = beforeKindAt,
    beforeObjectAt' = beforeObjectAt,
    jobOp' = jobOp,
    jobPhase' = Completed,
    sourcePath' = sourcePath,
    destinationPath' = destinationPath,
    blockedPaths' = Set(),
    retryCount' = retryCount,
    lastDecision' = SourceFinalized,
  }

  action failBeforeDestination: bool = all {
    jobPhase == Prepared,
    kindAt' = beforeKindAt,
    objectAt' = beforeObjectAt,
    beforeKindAt' = beforeKindAt,
    beforeObjectAt' = beforeObjectAt,
    jobOp' = jobOp,
    jobPhase' = FailedNoChange,
    sourcePath' = sourcePath,
    destinationPath' = destinationPath,
    blockedPaths' = Set(),
    retryCount' = retryCount,
    lastDecision' = FailedBeforeDestination,
  }

  action failAfterDestination: bool = all {
    jobPhase == DestinationAdopted,
    beforeKindAt' = beforeKindAt,
    beforeObjectAt' = beforeObjectAt,
    kindAt' = kindAt,
    objectAt' = objectAt,
    jobOp' = jobOp,
    jobPhase' = FailedRecoverable,
    sourcePath' = sourcePath,
    destinationPath' = destinationPath,
    blockedPaths' = Set(sourcePath, destinationPath),
    retryCount' = retryCount,
    lastDecision' = FailedAfterDestination,
  }

  action retryRecoverable: bool = {
    val object = sourceObject(beforeObjectAt, sourcePath)
    all {
      jobPhase == FailedRecoverable,
      retryCount < MaxRetries,
      kindAt' =
        if (jobOp == RenameMove) {
          kindAt.set(destinationPath, File).set(sourcePath, Tombstone)
        } else {
          kindAt.set(destinationPath, Hardlink)
        },
      objectAt' =
        if (jobOp == RenameMove) {
          objectAt.set(destinationPath, Some(object)).set(sourcePath, None)
        } else {
          objectAt.set(destinationPath, Some(object))
        },
      beforeKindAt' = beforeKindAt,
      beforeObjectAt' = beforeObjectAt,
      jobOp' = jobOp,
      jobPhase' = Completed,
      sourcePath' = sourcePath,
      destinationPath' = destinationPath,
      blockedPaths' = Set(),
      retryCount' = retryCount + 1,
      lastDecision' = RecoverableRetried,
    }
  }

  /// Exhausting the bounded destination retry budget must not strand a
  /// half-visible namespace operation. The destination adoption already
  /// contains enough durable evidence to finish the job forward, so the
  /// recovery worker finalizes the source without consuming another retry.
  action finishExhaustedRecoverable: bool = {
    val object = sourceObject(beforeObjectAt, sourcePath)
    all {
      jobPhase == FailedRecoverable,
      retryCount == MaxRetries,
      kindAt' =
        if (jobOp == RenameMove) {
          kindAt.set(destinationPath, File).set(sourcePath, Tombstone)
        } else {
          kindAt.set(destinationPath, Hardlink)
        },
      objectAt' =
        if (jobOp == RenameMove) {
          objectAt.set(destinationPath, Some(object)).set(sourcePath, None)
        } else {
          objectAt.set(destinationPath, Some(object))
        },
      beforeKindAt' = beforeKindAt,
      beforeObjectAt' = beforeObjectAt,
      jobOp' = jobOp,
      jobPhase' = Completed,
      sourcePath' = sourcePath,
      destinationPath' = destinationPath,
      blockedPaths' = Set(),
      retryCount' = retryCount,
      lastDecision' = RetryBudgetExhaustedForwarded,
    }
  }

  action stutterTerminal: bool = all {
    jobPhase == Completed or jobPhase == FailedNoChange,
    kindAt' = kindAt,
    objectAt' = objectAt,
    beforeKindAt' = beforeKindAt,
    beforeObjectAt' = beforeObjectAt,
    jobOp' = jobOp,
    jobPhase' = jobPhase,
    sourcePath' = sourcePath,
    destinationPath' = destinationPath,
    blockedPaths' = blockedPaths,
    retryCount' = retryCount,
    lastDecision' = TerminalStutter,
  }

  action step = any {
    {
      nondet source = oneOf(SourcePaths)
      nondet dest = oneOf(DestinationPaths)
      nondet op = oneOf(Set(RenameMove, HardlinkAlias))
      startJob(op, source, dest)
    },
    applyDestination,
    finalizeSource,
    failBeforeDestination,
    failAfterDestination,
    retryRecoverable,
    finishExhaustedRecoverable,
    stutterTerminal,
  }
}
