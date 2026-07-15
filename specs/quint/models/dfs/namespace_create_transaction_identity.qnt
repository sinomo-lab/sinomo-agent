/// DFS mkdir transaction identity across tombstone resurrection.
///
/// This model isolates the retained namespace-prepare history involved when a
/// directory is created, moved away from its original shard, and later
/// recreated from the selected tombstone at the same stable object id and
/// path. The recreated object has a newer version and therefore a different
/// immutable transaction payload.
///
/// The legacy identity uses only the operation kind plus stable object/path,
/// so both generations collide. The fixed identity also includes the object
/// version: a later resurrection gets a fresh transaction id, while an exact
/// retry of that resurrection keeps the same id and payload.
module dfs_namespace_create_transaction_identity {
  import common.* from "../../lib/common"

  type IdentityScheme = LegacyPathIdentity | VersionBoundIdentity
  type CreateGeneration = OriginalGeneration | ResurrectedGeneration
  type DirectoryObject = StableDirectoryObject | OtherDirectoryObject
  type DirectoryPath = UsersProject2Path | OtherDirectoryPath
  type CreateTransactionId =
    | LegacyMkdirCreateId
    | OriginalVersionMkdirCreateId
    | ResurrectedVersionMkdirCreateId
  type PathState = PathMissing | PathLive | SelectedTombstone
  type Phase =
    | Idle
    | OriginalCreateCommitted
    | MoveLeftSelectedTombstone
    | LegacyRecreateRejected
    | VersionBoundRecreateApplied
    | VersionBoundExactRetryReplayed

  type CreatePayload = {
    object: DirectoryObject,
    path: DirectoryPath,
    version: int,
  }

  type PrepareAudit = {
    proposedId: CreateTransactionId,
    proposedPayload: CreatePayload,
    retainedPayloadBefore: Option[CreatePayload],
    accepted: bool,
    replayed: bool,
    mutationCountBefore: int,
    mutationCountAfter: int,
  }

  pure val CreateTransactionIds: Set[CreateTransactionId] = Set(
    LegacyMkdirCreateId,
    OriginalVersionMkdirCreateId,
    ResurrectedVersionMkdirCreateId,
  )

  pure val emptyRetainedTransactions:
    CreateTransactionId -> Option[CreatePayload] =
      CreateTransactionIds.mapBy(_ => None)

  pure def payloadFor(generation: CreateGeneration): CreatePayload = {
    object: StableDirectoryObject,
    path: UsersProject2Path,
    version: if (generation == OriginalGeneration) 1 else 2,
  }

  pure def transactionIdFor(
    scheme: IdentityScheme,
    generation: CreateGeneration,
  ): CreateTransactionId =
    if (scheme == LegacyPathIdentity) {
      LegacyMkdirCreateId
    } else if (generation == OriginalGeneration) {
      OriginalVersionMkdirCreateId
    } else {
      ResurrectedVersionMkdirCreateId
    }

  /// Retained prepare history accepts a fresh id or an exact payload replay.
  /// Reusing an existing id with a different payload is rejected.
  pure def prepareAccepts(
    retainedPayload: Option[CreatePayload],
    proposedPayload: CreatePayload,
  ): bool =
    match retainedPayload {
      | Some(existingPayload) => existingPayload == proposedPayload
      | None => true
    }

  var phase: Phase
  var identityScheme: IdentityScheme
  var pathState: PathState
  var retainedTransactions: CreateTransactionId -> Option[CreatePayload]
  var committedMutations: int
  var lastPrepare: Option[PrepareAudit]

  action init = all {
    phase' = Idle,
    identityScheme' = LegacyPathIdentity,
    pathState' = PathMissing,
    retainedTransactions' = emptyRetainedTransactions,
    committedMutations' = 0,
    lastPrepare' = None,
  }

  action commitOriginalCreate(scheme: IdentityScheme): bool = {
    val transactionId = transactionIdFor(scheme, OriginalGeneration)
    val payload = payloadFor(OriginalGeneration)
    val retainedBefore = retainedTransactions.get(transactionId)
    all {
      phase == Idle,
      retainedBefore == None,
      phase' = OriginalCreateCommitted,
      identityScheme' = scheme,
      pathState' = PathLive,
      retainedTransactions' = retainedTransactions.set(
        transactionId,
        Some(payload)
      ),
      committedMutations' = committedMutations + 1,
      lastPrepare' = Some({
        proposedId: transactionId,
        proposedPayload: payload,
        retainedPayloadBefore: retainedBefore,
        accepted: true,
        replayed: false,
        mutationCountBefore: committedMutations,
        mutationCountAfter: committedMutations + 1,
      }),
    }
  }

  /// The cross-shard move preserves retained prepare history and leaves the
  /// original path selected as a tombstone for a later mkdir resurrection.
  action moveDirectoryLeavingSelectedTombstone: bool = all {
    phase == OriginalCreateCommitted,
    pathState == PathLive,
    phase' = MoveLeftSelectedTombstone,
    identityScheme' = identityScheme,
    pathState' = SelectedTombstone,
    retainedTransactions' = retainedTransactions,
    committedMutations' = committedMutations + 1,
    lastPrepare' = lastPrepare,
  }

  action attemptRecreate: bool = {
    val transactionId = transactionIdFor(
      identityScheme,
      ResurrectedGeneration,
    )
    val payload = payloadFor(ResurrectedGeneration)
    val retainedBefore = retainedTransactions.get(transactionId)
    val accepted = prepareAccepts(retainedBefore, payload)
    all {
      phase == MoveLeftSelectedTombstone,
      pathState == SelectedTombstone,
      phase' =
        if (accepted) {
          VersionBoundRecreateApplied
        } else {
          LegacyRecreateRejected
        },
      identityScheme' = identityScheme,
      pathState' = if (accepted) { PathLive } else { SelectedTombstone },
      retainedTransactions' =
        if (accepted) {
          retainedTransactions.set(transactionId, Some(payload))
        } else {
          retainedTransactions
        },
      committedMutations' =
        if (accepted) { committedMutations + 1 } else { committedMutations },
      lastPrepare' = Some({
        proposedId: transactionId,
        proposedPayload: payload,
        retainedPayloadBefore: retainedBefore,
        accepted: accepted,
        replayed: false,
        mutationCountBefore: committedMutations,
        mutationCountAfter:
          if (accepted) { committedMutations + 1 } else { committedMutations },
      }),
    }
  }

  /// An exact retry of the accepted resurrection reuses the version-bound id
  /// and immutable payload, so retained history recognizes a replay and no
  /// second namespace mutation is committed.
  action retryVersionBoundRecreate: bool = {
    val transactionId = transactionIdFor(
      VersionBoundIdentity,
      ResurrectedGeneration,
    )
    val payload = payloadFor(ResurrectedGeneration)
    val retainedBefore = retainedTransactions.get(transactionId)
    all {
      phase == VersionBoundRecreateApplied,
      identityScheme == VersionBoundIdentity,
      retainedBefore == Some(payload),
      prepareAccepts(retainedBefore, payload),
      phase' = VersionBoundExactRetryReplayed,
      identityScheme' = identityScheme,
      pathState' = pathState,
      retainedTransactions' = retainedTransactions,
      committedMutations' = committedMutations,
      lastPrepare' = Some({
        proposedId: transactionId,
        proposedPayload: payload,
        retainedPayloadBefore: retainedBefore,
        accepted: true,
        replayed: true,
        mutationCountBefore: committedMutations,
        mutationCountAfter: committedMutations,
      }),
    }
  }

  action stutterTerminal: bool = all {
    any {
      phase == LegacyRecreateRejected,
      phase == VersionBoundExactRetryReplayed,
    },
    phase' = phase,
    identityScheme' = identityScheme,
    pathState' = pathState,
    retainedTransactions' = retainedTransactions,
    committedMutations' = committedMutations,
    lastPrepare' = lastPrepare,
  }

  action step = any {
    {
      nondet scheme = oneOf(Set(LegacyPathIdentity, VersionBoundIdentity))
      commitOriginalCreate(scheme)
    },
    moveDirectoryLeavingSelectedTombstone,
    attemptRecreate,
    retryVersionBoundRecreate,
    stutterTerminal,
  }
}
