/// DFS normal-operation no-conflict invariant.
///
/// Current DFS reserves conflict records for future split-cluster recovery.
/// Normal quorum-backed writes must serialize through the metadata authority:
/// one pending intent may advance the visible version, while stale-base,
/// duplicate-target, wrong-primary, and concurrent pending-intent requests
/// reject or retry without minting conflict records.
module dfs_no_conflict {
  import common.* from "./common"

  type Writer = WriterA | WriterB
  type WriteDecision =
    NoDecision
    | IntentAccepted
    | VisibleCommitted
    | StaleBaseRejected
    | PendingIntentRejected
    | DuplicateTargetRejected
    | AuthorityRejected
    | IntentAborted
    | ConflictRecorded

  type PendingIntent = {
    writer: Writer,
    coordinator: Node,
    epoch: int,
    baseVersion: int,
    targetVersion: int,
  }

  type CommitRecord = {
    writer: Writer,
    coordinator: Node,
    epoch: int,
    baseVersion: int,
  }

  pure val Writers: Set[Writer] = Set(WriterA, WriterB)
  pure val MaxVersion: int = 3
  pure val Versions: Set[int] = Set(1, 2, 3)
  pure val BaseVersions: Set[int] = Set(0, 1, 2, 3)
  pure val MaxEpoch: int = 3
  pure val Epochs: Set[int] = Set(1, 2, 3)

  var shardPrimary: Node
  var shardEpoch: int
  var primaryByEpoch: int -> Option[Node]
  var visibleVersion: int
  var pendingIntent: Option[PendingIntent]
  var commitLog: int -> Option[CommitRecord]
  var conflictRecords: Set[int]
  var lastDecision: WriteDecision

  pure val emptyPrimaryByEpoch: int -> Option[Node] =
    Epochs.mapBy(_ => None)

  pure val emptyCommitLog: int -> Option[CommitRecord] =
    Versions.mapBy(_ => None)

  pure def unwrapPending(value: Option[PendingIntent]): PendingIntent =
    match value {
      | Some(inner) => inner
      | None => {
          writer: WriterA,
          coordinator: N1,
          epoch: 0,
          baseVersion: 0,
          targetVersion: 0,
        }
    }

  def pendingCanCommit: bool =
    match pendingIntent {
      | Some(intent) => all {
          intent.coordinator == shardPrimary,
          intent.epoch == shardEpoch,
          intent.baseVersion == visibleVersion,
          intent.targetVersion == visibleVersion + 1,
          intent.targetVersion <= MaxVersion,
          commitLog.get(intent.targetVersion) == None,
        }
      | None => false
    }

  action init = all {
    shardPrimary' = N1,
    shardEpoch' = 1,
    primaryByEpoch' = emptyPrimaryByEpoch.set(1, Some(N1)),
    visibleVersion' = 0,
    pendingIntent' = None,
    commitLog' = emptyCommitLog,
    conflictRecords' = Set(),
    lastDecision' = NoDecision,
  }

  /// Abstract committed metadata-shard primary movement. A previous pending
  /// intent may remain locally visible, but it no longer has commit authority.
  action commitPrimaryChange(newPrimary: Node): bool = all {
    newPrimary != shardPrimary,
    shardEpoch < MaxEpoch,
    shardPrimary' = newPrimary,
    shardEpoch' = shardEpoch + 1,
    primaryByEpoch' = primaryByEpoch.set(shardEpoch + 1, Some(newPrimary)),
    visibleVersion' = visibleVersion,
    pendingIntent' = pendingIntent,
    commitLog' = commitLog,
    conflictRecords' = conflictRecords,
    lastDecision' = lastDecision,
  }

  action acceptWriteIntent(
    writer: Writer,
    routedTo: Node,
    baseVersion: int,
    targetVersion: int
  ): bool = all {
    pendingIntent == None,
    routedTo == shardPrimary,
    baseVersion == visibleVersion,
    targetVersion == visibleVersion + 1,
    targetVersion <= MaxVersion,
    commitLog.get(targetVersion) == None,
    pendingIntent' = Some({
      writer: writer,
      coordinator: routedTo,
      epoch: shardEpoch,
      baseVersion: baseVersion,
      targetVersion: targetVersion,
    }),
    shardPrimary' = shardPrimary,
    shardEpoch' = shardEpoch,
    primaryByEpoch' = primaryByEpoch,
    visibleVersion' = visibleVersion,
    commitLog' = commitLog,
    conflictRecords' = conflictRecords,
    lastDecision' = IntentAccepted,
  }

  action rejectStaleBase(
    routedTo: Node,
    baseVersion: int,
    targetVersion: int
  ): bool = all {
    routedTo == shardPrimary,
    targetVersion <= MaxVersion,
    baseVersion != visibleVersion,
    shardPrimary' = shardPrimary,
    shardEpoch' = shardEpoch,
    primaryByEpoch' = primaryByEpoch,
    visibleVersion' = visibleVersion,
    pendingIntent' = pendingIntent,
    commitLog' = commitLog,
    conflictRecords' = conflictRecords,
    lastDecision' = StaleBaseRejected,
  }

  action rejectWrongPrimary(routedTo: Node): bool = all {
    routedTo != shardPrimary,
    shardPrimary' = shardPrimary,
    shardEpoch' = shardEpoch,
    primaryByEpoch' = primaryByEpoch,
    visibleVersion' = visibleVersion,
    pendingIntent' = pendingIntent,
    commitLog' = commitLog,
    conflictRecords' = conflictRecords,
    lastDecision' = AuthorityRejected,
  }

  /// Matches `validate_new_intent`: an active intent for the same object and
  /// target rejects as duplicate before the broader pending-intent conflict.
  action rejectDuplicateTarget(
    routedTo: Node,
    baseVersion: int,
    targetVersion: int
  ): bool = {
    val pending = unwrapPending(pendingIntent)
    all {
      routedTo == shardPrimary,
      baseVersion == visibleVersion,
      any {
        isSome(pendingIntent) and pending.targetVersion == targetVersion,
        commitLog.get(targetVersion) != None,
      },
      shardPrimary' = shardPrimary,
      shardEpoch' = shardEpoch,
      primaryByEpoch' = primaryByEpoch,
      visibleVersion' = visibleVersion,
      pendingIntent' = pendingIntent,
      commitLog' = commitLog,
      conflictRecords' = conflictRecords,
      lastDecision' = DuplicateTargetRejected,
    }
  }

  /// Matches the broader pending-intent conflict: another active intent for the
  /// object fences the request, but does not create a conflict record.
  action rejectPendingIntent(
    routedTo: Node,
    baseVersion: int,
    targetVersion: int
  ): bool = {
    val pending = unwrapPending(pendingIntent)
    all {
      routedTo == shardPrimary,
      baseVersion == visibleVersion,
      isSome(pendingIntent),
      pending.targetVersion != targetVersion,
      shardPrimary' = shardPrimary,
      shardEpoch' = shardEpoch,
      primaryByEpoch' = primaryByEpoch,
      visibleVersion' = visibleVersion,
      pendingIntent' = pendingIntent,
      commitLog' = commitLog,
      conflictRecords' = conflictRecords,
      lastDecision' = PendingIntentRejected,
    }
  }

  action commitPendingIntent: bool = {
    val pending = unwrapPending(pendingIntent)
    all {
      pendingCanCommit,
      visibleVersion' = pending.targetVersion,
      commitLog' = commitLog.set(pending.targetVersion, Some({
        writer: pending.writer,
        coordinator: pending.coordinator,
        epoch: pending.epoch,
        baseVersion: pending.baseVersion,
      })),
      pendingIntent' = None,
      shardPrimary' = shardPrimary,
      shardEpoch' = shardEpoch,
      primaryByEpoch' = primaryByEpoch,
      conflictRecords' = conflictRecords,
      lastDecision' = VisibleCommitted,
    }
  }

  action abortPendingIntent: bool = all {
    isSome(pendingIntent),
    pendingIntent' = None,
    shardPrimary' = shardPrimary,
    shardEpoch' = shardEpoch,
    primaryByEpoch' = primaryByEpoch,
    visibleVersion' = visibleVersion,
    commitLog' = commitLog,
    conflictRecords' = conflictRecords,
    lastDecision' = IntentAborted,
  }

  action step = any {
    {
      nondet newPrimary = oneOf(Nodes)
      commitPrimaryChange(newPrimary)
    },
    {
      nondet writer = oneOf(Writers)
      nondet routedTo = oneOf(Nodes)
      nondet baseVersion = oneOf(BaseVersions)
      nondet targetVersion = oneOf(Versions)
      any {
        acceptWriteIntent(writer, routedTo, baseVersion, targetVersion),
        rejectStaleBase(routedTo, baseVersion, targetVersion),
        rejectDuplicateTarget(routedTo, baseVersion, targetVersion),
        rejectPendingIntent(routedTo, baseVersion, targetVersion),
        rejectWrongPrimary(routedTo),
      }
    },
    commitPendingIntent,
    abortPendingIntent,
  }

  def normalOperationHasNoConflictRecords: bool =
    conflictRecords == Set()

  def normalWriteOutcomeNeverRecordsConflict: bool =
    lastDecision != ConflictRecorded

  def currentEpochHasPrimary: bool =
    primaryByEpoch.get(shardEpoch) == Some(shardPrimary)

  def visibleVersionInBounds: bool =
    visibleVersion >= 0 and visibleVersion <= MaxVersion

  def committedVersionsAreDense: bool =
    Versions.forall(version =>
      if (version <= visibleVersion) {
        isSome(commitLog.get(version))
      } else {
        not(isSome(commitLog.get(version)))
      }
    )

  def commitsAdvanceByOneFromBase: bool =
    Versions.forall(version =>
      match commitLog.get(version) {
        | Some(record) => record.baseVersion + 1 == version
        | None => true
      }
    )

  def commitsUseConfiguredPrimaryForTheirEpoch: bool =
    Versions.forall(version =>
      match commitLog.get(version) {
        | Some(record) => primaryByEpoch.get(record.epoch) == Some(record.coordinator)
        | None => true
      }
    )

  def pendingIntentDoesNotDuplicateCommittedVersion: bool =
    match pendingIntent {
      | Some(intent) => commitLog.get(intent.targetVersion) == None
      | None => true
    }

  def stalePendingCannotBeCommitReady: bool =
    match pendingIntent {
      | Some(intent) =>
          all {
            any {
              intent.coordinator != shardPrimary,
              intent.epoch != shardEpoch,
              intent.baseVersion != visibleVersion,
            },
            commitLog.get(intent.targetVersion) == None,
          } implies not(pendingCanCommit)
      | None => true
    }

  def safety: bool = all {
    normalOperationHasNoConflictRecords,
    normalWriteOutcomeNeverRecordsConflict,
    currentEpochHasPrimary,
    visibleVersionInBounds,
    committedVersionsAreDense,
    commitsAdvanceByOneFromBase,
    commitsUseConfiguredPrimaryForTheirEpoch,
    pendingIntentDoesNotDuplicateCommittedVersion,
    stalePendingCannotBeCommitReady,
  }
}
