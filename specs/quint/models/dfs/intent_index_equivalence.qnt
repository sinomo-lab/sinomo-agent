/// DFS coordinator intent-index equivalence.
///
/// Evidence class: target hardening for replacing historical intent scans in
/// `DistributedEventualCoordinator` with sparse lookup indexes. `ObjectKey`
/// abstracts the concrete `(file_system_id, object_id)` key and the bounded
/// history retains closed intents, just like the in-memory coordinator.
///
/// The reference algorithm scans every historical intent. The indexed
/// algorithm uses three deliberately different views:
///
/// - `awaitingByObject` contains only the one intent still awaiting receipts
///   for a full filesystem/object key;
/// - `awaitingByObjectId` aggregates every awaiting intent for the object id
///   used by filesystem-agnostic read and move-fencing helpers;
/// - `claimedByTarget` contains awaiting and committed target claims.
///
/// A committed claim must remain indexed because the reference duplicate
/// check rejects an exact object/target match even after the intent commits.
/// Abort removes all applicable sparse entries. Failed start persistence rolls
/// all three indexes and the history row back together. Receipt accounting,
/// replication policy validation, committed-version/base validation, repair
/// jobs, and local-first behavior are outside this refinement slice.
module dfs_intent_index_equivalence {
  import common.* from "../../lib/common"

  type FileSystem = FsA | FsB
  type ObjectId = SharedObject | OtherObject
  type ObjectKey = FsAShared | FsBShared | FsAOther
  type TargetVersion = Target1 | Target2
  type IntentId = Intent1 | Intent2 | Intent3
  type IntentState = Awaiting | Committed | Aborted
  type ValidationResult = Accept | DuplicateTarget | PendingObject
  type Transition = Initialized | StartSucceeded | StartRolledBack
    | CommitSucceeded | AbortSucceeded | RestoreSucceeded
    | RejectionObserved

  type IntentRecord = {
    object: ObjectKey,
    target: TargetVersion,
    state: IntentState,
  }

  pure val FileSystems: Set[FileSystem] = Set(FsA, FsB)
  pure val ObjectIds: Set[ObjectId] = Set(SharedObject, OtherObject)
  pure val Objects: Set[ObjectKey] = Set(FsAShared, FsBShared, FsAOther)
  pure val Targets: Set[TargetVersion] = Set(Target1, Target2)
  pure val IntentIds: Set[IntentId] = Set(Intent1, Intent2, Intent3)

  pure val emptyIntents: IntentId -> Option[IntentRecord] =
    IntentIds.mapBy(_ => None)

  pure val emptyAwaitingByObject: ObjectKey -> Option[IntentId] =
    Objects.mapBy(_ => None)

  pure val emptyAwaitingByObjectId: ObjectId -> Set[IntentId] =
    ObjectIds.mapBy(_ => Set())

  pure val emptyTargetsForObject: TargetVersion -> Set[IntentId] =
    Targets.mapBy(_ => Set())

  pure val emptyClaimedByTarget:
    ObjectKey -> (TargetVersion -> Set[IntentId]) =
      Objects.mapBy(_ => emptyTargetsForObject)

  var intents: IntentId -> Option[IntentRecord]
  var awaitingByObject: ObjectKey -> Option[IntentId]
  var awaitingByObjectId: ObjectId -> Set[IntentId]
  var claimedByTarget: ObjectKey -> (TargetVersion -> Set[IntentId])

  /// Snapshots immediately before the most recent transition. These make
  /// successful index maintenance and failed-start rollback explicit safety
  /// obligations rather than relying only on derived consistency checks.
  var beforeIntents: IntentId -> Option[IntentRecord]
  var beforeAwaitingByObject: ObjectKey -> Option[IntentId]
  var beforeAwaitingByObjectId: ObjectId -> Set[IntentId]
  var beforeClaimedByTarget:
    ObjectKey -> (TargetVersion -> Set[IntentId])
  var lastTransition: Transition
  var lastIntent: IntentId
  var lastObject: ObjectKey
  var lastTarget: TargetVersion
  var lastState: IntentState

  pure def fileSystemOf(object: ObjectKey): FileSystem =
    match object {
      | FsAShared => FsA
      | FsBShared => FsB
      | FsAOther => FsA
    }

  pure def objectIdOf(object: ObjectKey): ObjectId =
    match object {
      | FsAShared => SharedObject
      | FsBShared => SharedObject
      | FsAOther => OtherObject
    }

  pure def unwrapIntent(value: Option[IntentRecord]): IntentRecord =
    match value {
      | Some(intent) => intent
      | None => {
          object: FsAShared,
          target: Target1,
          state: Aborted,
        }
    }

  def referenceAwaitingIds(object: ObjectKey): Set[IntentId] =
    IntentIds.filter(intentId =>
      match intents.get(intentId) {
        | Some(intent) => all {
            intent.object == object,
            intent.state == Awaiting,
          }
        | None => false
      }
    )

  def referenceClaimedTargetIds(
    object: ObjectKey,
    target: TargetVersion,
  ): Set[IntentId] =
    IntentIds.filter(intentId =>
      match intents.get(intentId) {
        | Some(intent) => all {
            intent.object == object,
            intent.target == target,
            any {
              intent.state == Awaiting,
              intent.state == Committed,
            },
          }
        | None => false
      }
    )

  def referenceAwaitingIdsByObjectId(objectId: ObjectId): Set[IntentId] =
    IntentIds.filter(intentId =>
      match intents.get(intentId) {
        | Some(intent) => all {
            objectIdOf(intent.object) == objectId,
            intent.state == Awaiting,
          }
        | None => false
      }
    )

  /// Mirrors `validate_new_intent`: exact active-or-committed target
  /// duplicates are classified before the broader awaiting-object conflict.
  def referenceValidation(
    object: ObjectKey,
    target: TargetVersion,
  ): ValidationResult =
    if (referenceClaimedTargetIds(object, target).size() > 0) {
      DuplicateTarget
    } else if (referenceAwaitingIds(object).size() > 0) {
      PendingObject
    } else {
      Accept
    }

  def indexedValidation(
    object: ObjectKey,
    target: TargetVersion,
  ): ValidationResult =
    if (claimedByTarget.get(object).get(target).size() > 0) {
      DuplicateTarget
    } else if (isSome(awaitingByObject.get(object))) {
      PendingObject
    } else {
      Accept
    }

  def referenceHasAwaitingTarget(
    objectId: ObjectId,
    target: TargetVersion,
  ): bool =
    referenceAwaitingIdsByObjectId(objectId).exists(intentId =>
      unwrapIntent(intents.get(intentId)).target == target
    )

  def indexedHasAwaitingTarget(
    objectId: ObjectId,
    target: TargetVersion,
  ): bool =
    awaitingByObjectId.get(objectId).exists(intentId =>
      match intents.get(intentId) {
        | Some(intent) => all {
            objectIdOf(intent.object) == objectId,
            intent.target == target,
            intent.state == Awaiting,
          }
        | None => false
      }
    )

  def referenceHasOpenIntent(objectId: ObjectId): bool =
    referenceAwaitingIdsByObjectId(objectId).size() > 0

  def indexedHasOpenIntent(objectId: ObjectId): bool =
    awaitingByObjectId.get(objectId).size() > 0

  pure def startIntents(
    base: IntentId -> Option[IntentRecord],
    intentId: IntentId,
    object: ObjectKey,
    target: TargetVersion,
  ): IntentId -> Option[IntentRecord] =
    base.set(intentId, Some({
      object: object,
      target: target,
      state: Awaiting,
    }))

  pure def startAwaitingIndex(
    base: ObjectKey -> Option[IntentId],
    intentId: IntentId,
    object: ObjectKey,
  ): ObjectKey -> Option[IntentId] =
    base.set(object, Some(intentId))

  pure def startAwaitingObjectIdIndex(
    base: ObjectId -> Set[IntentId],
    intentId: IntentId,
    object: ObjectKey,
  ): ObjectId -> Set[IntentId] = {
    val objectId = objectIdOf(object)
    base.set(
      objectId,
      base.get(objectId).union(Set(intentId))
    )
  }

  pure def startTargetIndex(
    base: ObjectKey -> (TargetVersion -> Set[IntentId]),
    intentId: IntentId,
    object: ObjectKey,
    target: TargetVersion,
  ): ObjectKey -> (TargetVersion -> Set[IntentId]) =
    base.set(
      object,
      base.get(object).set(
        target,
        base.get(object).get(target).union(Set(intentId))
      )
    )

  pure def closeIntents(
    base: IntentId -> Option[IntentRecord],
    intentId: IntentId,
    state: IntentState,
  ): IntentId -> Option[IntentRecord] = {
    val intent = unwrapIntent(base.get(intentId))
    base.set(intentId, Some({
      object: intent.object,
      target: intent.target,
      state: state,
    }))
  }

  pure def removeAwaitingIndex(
    base: ObjectKey -> Option[IntentId],
    intentId: IntentId,
    object: ObjectKey,
  ): ObjectKey -> Option[IntentId] =
    if (base.get(object) == Some(intentId)) {
      base.set(object, None)
    } else {
      base
    }

  pure def removeAwaitingObjectIdIndex(
    base: ObjectId -> Set[IntentId],
    intentId: IntentId,
    object: ObjectKey,
  ): ObjectId -> Set[IntentId] = {
    val objectId = objectIdOf(object)
    base.set(
      objectId,
      base.get(objectId).exclude(Set(intentId))
    )
  }

  pure def removeTargetIndex(
    base: ObjectKey -> (TargetVersion -> Set[IntentId]),
    intentId: IntentId,
    object: ObjectKey,
    target: TargetVersion,
  ): ObjectKey -> (TargetVersion -> Set[IntentId]) =
    base.set(
      object,
      base.get(object).set(
        target,
        base.get(object).get(target).exclude(Set(intentId))
      )
    )

  pure def restoreIntents(
    base: IntentId -> Option[IntentRecord],
    intentId: IntentId,
    object: ObjectKey,
    target: TargetVersion,
    state: IntentState,
  ): IntentId -> Option[IntentRecord] =
    base.set(intentId, Some({
      object: object,
      target: target,
      state: state,
    }))

  pure def removePreviousAwaitingForRestore(
    base: ObjectKey -> Option[IntentId],
    previous: Option[IntentRecord],
    intentId: IntentId,
  ): ObjectKey -> Option[IntentId] =
    match previous {
      | Some(intent) =>
          if (intent.state == Awaiting) {
            removeAwaitingIndex(base, intentId, intent.object)
          } else {
            base
          }
      | None => base
    }

  pure def removePreviousObjectIdAwaitingForRestore(
    base: ObjectId -> Set[IntentId],
    previous: Option[IntentRecord],
    intentId: IntentId,
  ): ObjectId -> Set[IntentId] =
    match previous {
      | Some(intent) =>
          if (intent.state == Awaiting) {
            removeAwaitingObjectIdIndex(base, intentId, intent.object)
          } else {
            base
          }
      | None => base
    }

  pure def removePreviousTargetForRestore(
    base: ObjectKey -> (TargetVersion -> Set[IntentId]),
    previous: Option[IntentRecord],
    intentId: IntentId,
  ): ObjectKey -> (TargetVersion -> Set[IntentId]) =
    match previous {
      | Some(intent) =>
          if (intent.state == Awaiting or intent.state == Committed) {
            removeTargetIndex(
              base,
              intentId,
              intent.object,
              intent.target
            )
          } else {
            base
          }
      | None => base
    }

  pure def restoreAwaitingIndex(
    base: ObjectKey -> Option[IntentId],
    previous: Option[IntentRecord],
    intentId: IntentId,
    object: ObjectKey,
    state: IntentState,
  ): ObjectKey -> Option[IntentId] = {
    val withoutPrevious =
      removePreviousAwaitingForRestore(base, previous, intentId)
    if (state == Awaiting) {
      startAwaitingIndex(withoutPrevious, intentId, object)
    } else {
      withoutPrevious
    }
  }

  pure def restoreAwaitingObjectIdIndex(
    base: ObjectId -> Set[IntentId],
    previous: Option[IntentRecord],
    intentId: IntentId,
    object: ObjectKey,
    state: IntentState,
  ): ObjectId -> Set[IntentId] = {
    val withoutPrevious =
      removePreviousObjectIdAwaitingForRestore(base, previous, intentId)
    if (state == Awaiting) {
      startAwaitingObjectIdIndex(withoutPrevious, intentId, object)
    } else {
      withoutPrevious
    }
  }

  pure def restoreTargetIndex(
    base: ObjectKey -> (TargetVersion -> Set[IntentId]),
    previous: Option[IntentRecord],
    intentId: IntentId,
    object: ObjectKey,
    target: TargetVersion,
    state: IntentState,
  ): ObjectKey -> (TargetVersion -> Set[IntentId]) = {
    val withoutPrevious =
      removePreviousTargetForRestore(base, previous, intentId)
    if (state == Awaiting or state == Committed) {
      startTargetIndex(withoutPrevious, intentId, object, target)
    } else {
      withoutPrevious
    }
  }

  action init = all {
    intents' = emptyIntents,
    awaitingByObject' = emptyAwaitingByObject,
    awaitingByObjectId' = emptyAwaitingByObjectId,
    claimedByTarget' = emptyClaimedByTarget,
    beforeIntents' = emptyIntents,
    beforeAwaitingByObject' = emptyAwaitingByObject,
    beforeAwaitingByObjectId' = emptyAwaitingByObjectId,
    beforeClaimedByTarget' = emptyClaimedByTarget,
    lastTransition' = Initialized,
    lastIntent' = Intent1,
    lastObject' = FsAShared,
    lastTarget' = Target1,
    lastState' = Aborted,
  }

  /// A failed durable start is the rollback cut: neither historical truth nor
  /// either sparse lookup may retain the attempted intent.
  action attemptStart(
    intentId: IntentId,
    object: ObjectKey,
    target: TargetVersion,
    persistenceSucceeds: bool,
  ): bool = all {
    intents.get(intentId) == None,
    referenceValidation(object, target) == Accept,
    beforeIntents' = intents,
    beforeAwaitingByObject' = awaitingByObject,
    beforeAwaitingByObjectId' = awaitingByObjectId,
    beforeClaimedByTarget' = claimedByTarget,
    lastIntent' = intentId,
    lastObject' = object,
    lastTarget' = target,
    lastState' = Awaiting,
    if (persistenceSucceeds) {
      all {
        intents' = startIntents(intents, intentId, object, target),
        awaitingByObject' =
          startAwaitingIndex(awaitingByObject, intentId, object),
        awaitingByObjectId' =
          startAwaitingObjectIdIndex(
            awaitingByObjectId,
            intentId,
            object
          ),
        claimedByTarget' =
          startTargetIndex(claimedByTarget, intentId, object, target),
        lastTransition' = StartSucceeded,
      }
    } else {
      all {
        intents' = intents,
        awaitingByObject' = awaitingByObject,
        awaitingByObjectId' = awaitingByObjectId,
        claimedByTarget' = claimedByTarget,
        lastTransition' = StartRolledBack,
      }
    },
  }

  action commitIntent(intentId: IntentId): bool = {
    val intent = unwrapIntent(intents.get(intentId))
    all {
      isSome(intents.get(intentId)),
      intent.state == Awaiting,
      beforeIntents' = intents,
      beforeAwaitingByObject' = awaitingByObject,
      beforeAwaitingByObjectId' = awaitingByObjectId,
      beforeClaimedByTarget' = claimedByTarget,
      intents' = closeIntents(intents, intentId, Committed),
      awaitingByObject' =
        removeAwaitingIndex(awaitingByObject, intentId, intent.object),
      awaitingByObjectId' = removeAwaitingObjectIdIndex(
        awaitingByObjectId,
        intentId,
        intent.object
      ),
      // Retain the target claim: committed duplicates remain duplicates.
      claimedByTarget' = claimedByTarget,
      lastTransition' = CommitSucceeded,
      lastIntent' = intentId,
      lastObject' = intent.object,
      lastTarget' = intent.target,
      lastState' = Committed,
    }
  }

  action abortIntent(intentId: IntentId): bool = {
    val intent = unwrapIntent(intents.get(intentId))
    all {
      isSome(intents.get(intentId)),
      intent.state == Awaiting,
      beforeIntents' = intents,
      beforeAwaitingByObject' = awaitingByObject,
      beforeAwaitingByObjectId' = awaitingByObjectId,
      beforeClaimedByTarget' = claimedByTarget,
      intents' = closeIntents(intents, intentId, Aborted),
      awaitingByObject' =
        removeAwaitingIndex(awaitingByObject, intentId, intent.object),
      awaitingByObjectId' = removeAwaitingObjectIdIndex(
        awaitingByObjectId,
        intentId,
        intent.object
      ),
      claimedByTarget' = removeTargetIndex(
        claimedByTarget,
        intentId,
        intent.object,
        intent.target
      ),
      lastTransition' = AbortSucceeded,
      lastIntent' = intentId,
      lastObject' = intent.object,
      lastTarget' = intent.target,
      lastState' = Aborted,
    }
  }

  /// Hydration restores retained durable rows into an empty intent id or
  /// replaces an existing row whose object and target match. An awaiting row
  /// must pass the same conflict validation as a new start; committed and
  /// aborted rows are accepted without that check, as in
  /// `restore_intent_deferring_repair_rebuild`. Replacement removes the old
  /// state's sparse entries before installing the new state's entries.
  action restoreIntent(
    intentId: IntentId,
    object: ObjectKey,
    target: TargetVersion,
    state: IntentState,
  ): bool = all {
    match intents.get(intentId) {
      | Some(intent) => all {
          intent.object == object,
          intent.target == target,
        }
      | None => true
    },
    state != Awaiting or referenceValidation(object, target) == Accept,
    beforeIntents' = intents,
    beforeAwaitingByObject' = awaitingByObject,
    beforeAwaitingByObjectId' = awaitingByObjectId,
    beforeClaimedByTarget' = claimedByTarget,
    intents' = restoreIntents(intents, intentId, object, target, state),
    awaitingByObject' = restoreAwaitingIndex(
      awaitingByObject,
      intents.get(intentId),
      intentId,
      object,
      state
    ),
    awaitingByObjectId' = restoreAwaitingObjectIdIndex(
      awaitingByObjectId,
      intents.get(intentId),
      intentId,
      object,
      state
    ),
    claimedByTarget' = restoreTargetIndex(
      claimedByTarget,
      intents.get(intentId),
      intentId,
      object,
      target,
      state
    ),
    lastTransition' = RestoreSucceeded,
    lastIntent' = intentId,
    lastObject' = object,
    lastTarget' = target,
    lastState' = state,
  }

  /// Symbolic replacement refinement. The routine runtime action above is
  /// stateful, which requires an initial restore plus replacement to reach a
  /// replacement edge. This proof action constructs each valid one-row prior
  /// state and applies the same pure unindex/reindex functions in one step, so
  /// Apalache can cover every previous/new state pair without a depth-3 state
  /// explosion. Awaiting may replace only Aborted: an Awaiting or Committed
  /// predecessor would make the reference duplicate scan reject the restore.
  action compareRestoreReplacement(
    intentId: IntentId,
    object: ObjectKey,
    target: TargetVersion,
    previousState: IntentState,
    newState: IntentState,
  ): bool = {
    val seededIntents = restoreIntents(
      emptyIntents,
      intentId,
      object,
      target,
      previousState
    )
    val seededAwaiting = restoreAwaitingIndex(
      emptyAwaitingByObject,
      None,
      intentId,
      object,
      previousState
    )
    val seededObjectIdAwaiting = restoreAwaitingObjectIdIndex(
      emptyAwaitingByObjectId,
      None,
      intentId,
      object,
      previousState
    )
    val seededTargets = restoreTargetIndex(
      emptyClaimedByTarget,
      None,
      intentId,
      object,
      target,
      previousState
    )
    all {
      newState != Awaiting or previousState == Aborted,
      beforeIntents' = seededIntents,
      beforeAwaitingByObject' = seededAwaiting,
      beforeAwaitingByObjectId' = seededObjectIdAwaiting,
      beforeClaimedByTarget' = seededTargets,
      intents' = restoreIntents(
        seededIntents,
        intentId,
        object,
        target,
        newState
      ),
      awaitingByObject' = restoreAwaitingIndex(
        seededAwaiting,
        seededIntents.get(intentId),
        intentId,
        object,
        newState
      ),
      awaitingByObjectId' = restoreAwaitingObjectIdIndex(
        seededObjectIdAwaiting,
        seededIntents.get(intentId),
        intentId,
        object,
        newState
      ),
      claimedByTarget' = restoreTargetIndex(
        seededTargets,
        seededIntents.get(intentId),
        intentId,
        object,
        target,
        newState
      ),
      lastTransition' = RestoreSucceeded,
      lastIntent' = intentId,
      lastObject' = object,
      lastTarget' = target,
      lastState' = newState,
    }
  }

  /// Symbolic cross-filesystem addition. The seeded intent uses the shared
  /// object id in FsA; the compared start/restore adds the same object id in
  /// FsB. Full-key validation stays independent while the global object-id
  /// view must contain both intent ids.
  action compareCrossFileSystemAddition(
    transition: Transition,
    target: TargetVersion,
  ): bool = {
    val seededIntents = startIntents(
      emptyIntents,
      Intent1,
      FsAShared,
      Target1
    )
    val seededAwaiting = startAwaitingIndex(
      emptyAwaitingByObject,
      Intent1,
      FsAShared
    )
    val seededObjectIdAwaiting = startAwaitingObjectIdIndex(
      emptyAwaitingByObjectId,
      Intent1,
      FsAShared
    )
    val seededTargets = startTargetIndex(
      emptyClaimedByTarget,
      Intent1,
      FsAShared,
      Target1
    )
    all {
      any {
        transition == StartSucceeded,
        transition == RestoreSucceeded,
      },
      beforeIntents' = seededIntents,
      beforeAwaitingByObject' = seededAwaiting,
      beforeAwaitingByObjectId' = seededObjectIdAwaiting,
      beforeClaimedByTarget' = seededTargets,
      intents' = startIntents(
        seededIntents,
        Intent2,
        FsBShared,
        target
      ),
      awaitingByObject' = startAwaitingIndex(
        seededAwaiting,
        Intent2,
        FsBShared
      ),
      awaitingByObjectId' = startAwaitingObjectIdIndex(
        seededObjectIdAwaiting,
        Intent2,
        FsBShared
      ),
      claimedByTarget' = startTargetIndex(
        seededTargets,
        Intent2,
        FsBShared,
        target
      ),
      lastTransition' = transition,
      lastIntent' = Intent2,
      lastObject' = FsBShared,
      lastTarget' = target,
      lastState' = Awaiting,
    }
  }

  /// Symbolic cross-filesystem close. Both full keys begin awaiting for the
  /// same object id. Commit or abort closes only the FsB intent, and the
  /// aggregate object-id view must preserve the FsA intent.
  action compareCrossFileSystemClose(transition: Transition): bool = {
    val firstIntents = startIntents(
      emptyIntents,
      Intent1,
      FsAShared,
      Target1
    )
    val seededIntents = startIntents(
      firstIntents,
      Intent2,
      FsBShared,
      Target1
    )
    val firstAwaiting = startAwaitingIndex(
      emptyAwaitingByObject,
      Intent1,
      FsAShared
    )
    val seededAwaiting = startAwaitingIndex(
      firstAwaiting,
      Intent2,
      FsBShared
    )
    val firstObjectIdAwaiting = startAwaitingObjectIdIndex(
      emptyAwaitingByObjectId,
      Intent1,
      FsAShared
    )
    val seededObjectIdAwaiting = startAwaitingObjectIdIndex(
      firstObjectIdAwaiting,
      Intent2,
      FsBShared
    )
    val firstTargets = startTargetIndex(
      emptyClaimedByTarget,
      Intent1,
      FsAShared,
      Target1
    )
    val seededTargets = startTargetIndex(
      firstTargets,
      Intent2,
      FsBShared,
      Target1
    )
    all {
      any {
        transition == CommitSucceeded,
        transition == AbortSucceeded,
      },
      beforeIntents' = seededIntents,
      beforeAwaitingByObject' = seededAwaiting,
      beforeAwaitingByObjectId' = seededObjectIdAwaiting,
      beforeClaimedByTarget' = seededTargets,
      intents' = closeIntents(
        seededIntents,
        Intent2,
        if (transition == CommitSucceeded) Committed else Aborted
      ),
      awaitingByObject' = removeAwaitingIndex(
        seededAwaiting,
        Intent2,
        FsBShared
      ),
      awaitingByObjectId' = removeAwaitingObjectIdIndex(
        seededObjectIdAwaiting,
        Intent2,
        FsBShared
      ),
      claimedByTarget' =
        if (transition == CommitSucceeded) {
          seededTargets
        } else {
          removeTargetIndex(
            seededTargets,
            Intent2,
            FsBShared,
            Target1
          )
        },
      lastTransition' = transition,
      lastIntent' = Intent2,
      lastObject' = FsBShared,
      lastTarget' = Target1,
      lastState' =
        if (transition == CommitSucceeded) Committed else Aborted,
    }
  }

  /// Symbolic cross-filesystem same-id replacement. The independent FsA
  /// awaiting intent is seeded first; replacing the FsB subject must update
  /// only that subject's memberships in the aggregate object-id set.
  action compareCrossFileSystemReplacement(
    previousState: IntentState,
    newState: IntentState,
  ): bool = {
    val firstIntents = startIntents(
      emptyIntents,
      Intent1,
      FsAShared,
      Target1
    )
    val seededIntents = restoreIntents(
      firstIntents,
      Intent2,
      FsBShared,
      Target1,
      previousState
    )
    val firstAwaiting = startAwaitingIndex(
      emptyAwaitingByObject,
      Intent1,
      FsAShared
    )
    val seededAwaiting = restoreAwaitingIndex(
      firstAwaiting,
      None,
      Intent2,
      FsBShared,
      previousState
    )
    val firstObjectIdAwaiting = startAwaitingObjectIdIndex(
      emptyAwaitingByObjectId,
      Intent1,
      FsAShared
    )
    val seededObjectIdAwaiting = restoreAwaitingObjectIdIndex(
      firstObjectIdAwaiting,
      None,
      Intent2,
      FsBShared,
      previousState
    )
    val firstTargets = startTargetIndex(
      emptyClaimedByTarget,
      Intent1,
      FsAShared,
      Target1
    )
    val seededTargets = restoreTargetIndex(
      firstTargets,
      None,
      Intent2,
      FsBShared,
      Target1,
      previousState
    )
    all {
      newState != Awaiting or previousState == Aborted,
      beforeIntents' = seededIntents,
      beforeAwaitingByObject' = seededAwaiting,
      beforeAwaitingByObjectId' = seededObjectIdAwaiting,
      beforeClaimedByTarget' = seededTargets,
      intents' = restoreIntents(
        seededIntents,
        Intent2,
        FsBShared,
        Target1,
        newState
      ),
      awaitingByObject' = restoreAwaitingIndex(
        seededAwaiting,
        seededIntents.get(Intent2),
        Intent2,
        FsBShared,
        newState
      ),
      awaitingByObjectId' = restoreAwaitingObjectIdIndex(
        seededObjectIdAwaiting,
        seededIntents.get(Intent2),
        Intent2,
        FsBShared,
        newState
      ),
      claimedByTarget' = restoreTargetIndex(
        seededTargets,
        seededIntents.get(Intent2),
        Intent2,
        FsBShared,
        Target1,
        newState
      ),
      lastTransition' = RestoreSucceeded,
      lastIntent' = Intent2,
      lastObject' = FsBShared,
      lastTarget' = Target1,
      lastState' = newState,
    }
  }

  /// Rejected validation attempts are observations. They exercise exact error
  /// classification without mutating history or either index.
  action observeRejectedStart(
    object: ObjectKey,
    target: TargetVersion,
  ): bool = all {
    referenceValidation(object, target) != Accept,
    referenceValidation(object, target) == indexedValidation(object, target),
    intents' = intents,
    awaitingByObject' = awaitingByObject,
    awaitingByObjectId' = awaitingByObjectId,
    claimedByTarget' = claimedByTarget,
    beforeIntents' = intents,
    beforeAwaitingByObject' = awaitingByObject,
    beforeAwaitingByObjectId' = awaitingByObjectId,
    beforeClaimedByTarget' = claimedByTarget,
    lastTransition' = RejectionObserved,
    lastIntent' = lastIntent,
    lastObject' = object,
    lastTarget' = target,
    lastState' = lastState,
  }

  action step = any {
    {
      nondet intentId = oneOf(IntentIds)
      nondet object = oneOf(Objects)
      nondet target = oneOf(Targets)
      nondet persistenceSucceeds = oneOf(Set(true, false))
      attemptStart(
        intentId,
        object,
        target,
        persistenceSucceeds
      )
    },
    {
      nondet intentId = oneOf(IntentIds)
      any {
        commitIntent(intentId),
        abortIntent(intentId),
      }
    },
    {
      nondet intentId = oneOf(IntentIds)
      nondet object = oneOf(Objects)
      nondet target = oneOf(Targets)
      nondet state = oneOf(Set(Awaiting, Committed, Aborted))
      restoreIntent(intentId, object, target, state)
    },
    {
      nondet object = oneOf(Objects)
      nondet target = oneOf(Targets)
      observeRejectedStart(object, target)
    },
  }

  /// The symbolic entrypoint combines one ordinary lifecycle transition with
  /// bounded all-state replacement and cross-filesystem aggregate comparisons.
  action proofStep = any {
    step,
    {
      nondet intentId = oneOf(IntentIds)
      nondet object = oneOf(Objects)
      nondet target = oneOf(Targets)
      nondet previousState = oneOf(Set(Awaiting, Committed, Aborted))
      nondet newState = oneOf(Set(Awaiting, Committed, Aborted))
      compareRestoreReplacement(
        intentId,
        object,
        target,
        previousState,
        newState
      )
    },
    {
      nondet transition = oneOf(Set(StartSucceeded, RestoreSucceeded))
      nondet target = oneOf(Targets)
      compareCrossFileSystemAddition(transition, target)
    },
    {
      nondet transition = oneOf(Set(CommitSucceeded, AbortSucceeded))
      compareCrossFileSystemClose(transition)
    },
    {
      nondet previousState = oneOf(Set(Awaiting, Committed, Aborted))
      nondet newState = oneOf(Set(Awaiting, Committed, Aborted))
      compareCrossFileSystemReplacement(previousState, newState)
    },
  }
}
