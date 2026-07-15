/// DFS sharded namespace transaction routing and cross-shard move safety.
///
/// This module models the `LocalShardedMetadataBackend` layer above the
/// shard-local `DfsNamespaceTransaction` model:
/// - transaction objects are split by their target metadata shard;
/// - object-shard indexes are written after a target shard transaction;
/// - old source shards are cleaned up with cross-shard tombstones after the
///   target transaction and index update;
/// - partial failures preserve sharded-backend lookup semantics.
///
/// It intentionally consumes the lower-layer guarantee from
/// `dfs_namespace_transaction`: once a target shard accepts a transaction, its
/// object/version rows are shard-locally atomic and replayable.
module dfs_sharded_namespace_transaction {
  import common.* from "../../lib/common"

  type MetadataShard = RootShard | ServicesShard
  type TransactionShape = EmptyTransaction | MoveRootToServices
  type Phase =
    Idle
    | NoOp
    | SourcePresent
    | TargetShardFailed
    | TargetIndexFailed
    | TargetApplied
    | SourceTombstoneFailed
    | Complete
  type Decision =
    NoDecision
    | ScenarioStarted
    | TargetApplyFailed
    | TargetAppliedIndexFailed
    | TargetAppliedAndIndexed
    | TargetReplayed
    | SourceTombstoneApplyFailed
    | SourceTombstoneApplied
    | TerminalStutter

  /// Competing service-level views of one logical path after its physical row
  /// moves from the root shard to the services shard.
  type PathLookupResult = PathMissing | PathLive | PathTombstone
  type PostMoveMkdirOutcome =
    | PostMoveMkdirNotAttempted
    | LegacyResurrectionThenUncorroboratedRedirect
    | ShardAwareExistingDirectorySucceeded
  type PostMoveProbe = {
    outcome: PostMoveMkdirOutcome,
    committedMutations: int,
    replicaIndexBeforeLookup: Option[MetadataShard],
    legacyBackendLookupBefore: PathLookupResult,
    derivedIndexRepairs: int,
    reportedRedirectShard: Option[MetadataShard],
    redirectAccepted: bool,
    returnedSuccess: bool,
  }

  pure val Shards: Set[MetadataShard] = Set(RootShard, ServicesShard)
  pure val TransactionShapes: Set[TransactionShape] =
    Set(EmptyTransaction, MoveRootToServices)
  pure val ReplicaInitialIndexes: Set[Option[MetadataShard]] =
    Set(None, Some(RootShard))
  pure val SelectedPathShard: MetadataShard = ServicesShard

  /// Service-level routing helper output for the modeled transaction object
  /// path. For `/services/readme.md`, both the object path and parent path
  /// select the services shard; the old root shard cleanup is discovered later
  /// from the object-shard index by `LocalShardedMetadataBackend`.
  pure def affectedRouteShardsFor(
    controlPlaneConfigured: bool,
    shape: TransactionShape
  ): Set[MetadataShard] =
    if (controlPlaneConfigured and shape == MoveRootToServices) {
      Set(ServicesShard)
    } else {
      Set()
    }

  pure def initialSourceCleanupShards(shape: TransactionShape): Set[MetadataShard] =
    if (shape == MoveRootToServices) { Set(RootShard) } else { Set() }

  var phase: Phase
  var transactionShape: TransactionShape
  var controlPlaneConfigured: bool
  var affectedRouteShards: Set[MetadataShard]
  var sourceCleanupShards: Set[MetadataShard]
  var sourceRowPresent: bool
  var sourceTombstonePresent: bool
  var targetRowPresent: bool
  var targetTxnRecorded: bool
  var sourceTombstoneRecorded: bool
  var objectShardIndex: Option[MetadataShard]
  /// Replica-local derived routing accelerator. Journal/snapshot ingest may
  /// leave this absent or pointing at the retired source; it is convergent
  /// cache state, not namespace visibility authority.
  var replicaObjectShardIndex: Option[MetadataShard]
  var targetIndexEverWritten: bool
  var replayedShards: Set[MetadataShard]
  var lastDecision: Decision
  var postMoveProbe: PostMoveProbe

  def transactionHasObjects: bool =
    transactionShape == MoveRootToServices

  /// What `get_object_by_path(root_path)` exposes through the sharded backend.
  /// A physical live row left in the old source shard is hidden once the
  /// object-shard index points at the target shard.
  def sourcePathVisible: bool = all {
    sourceRowPresent,
    objectShardIndex == Some(RootShard),
  }

  /// What `get_object_by_path(/services/readme.md)` exposes through the
  /// sharded backend.
  def targetPathVisible: bool = all {
    targetRowPresent,
    objectShardIndex == Some(ServicesShard),
  }

  def objectByIdUsesTargetShard: bool =
    objectShardIndex == Some(ServicesShard)

  /// The old service overlay consulted one global path tombstone map before
  /// asking the sharded backend. A migration-eviction tombstone from the
  /// retired physical source shard could therefore shadow the live row selected
  /// by the current path shard even when the writer's durable object-shard
  /// index already names that target.
  def legacyGlobalTombstoneFirstLookup: PathLookupResult =
    if (sourceTombstonePresent) {
      PathTombstone
    } else if (targetPathVisible) {
      PathLive
    } else if (sourcePathVisible) {
      PathLive
    } else {
      PathMissing
    }

  /// The fixed lookup resolves the current physical shard first. A live row on
  /// the path-selected services shard wins over a same-path tombstone that
  /// exists only to record eviction from the retired root shard.
  def shardAwareSelectedLookup: PathLookupResult =
    if (all {
      SelectedPathShard == ServicesShard,
      targetRowPresent,
    }) {
      PathLive
    } else if (all {
      objectShardIndex == Some(RootShard),
      sourceTombstonePresent,
    }) {
      PathTombstone
    } else if (sourcePathVisible or targetPathVisible) {
      PathLive
    } else {
      PathMissing
    }

  /// The pre-fix sharded backend hid a selected live row when the local
  /// derived index still named another configured shard. With no local index it
  /// admitted the row, but the service's earlier global tombstone lookup could
  /// still shadow it.
  def legacyReplicaBackendLookup: PathLookupResult =
    if (all {
      targetRowPresent,
      replicaObjectShardIndex == Some(RootShard),
    }) {
      PathMissing
    } else if (targetRowPresent) {
      PathLive
    } else if (sourceTombstonePresent) {
      PathTombstone
    } else {
      PathMissing
    }

  pure val emptyPostMoveProbe: PostMoveProbe = {
    outcome: PostMoveMkdirNotAttempted,
    committedMutations: 0,
    replicaIndexBeforeLookup: None,
    legacyBackendLookupBefore: PathMissing,
    derivedIndexRepairs: 0,
    reportedRedirectShard: None,
    redirectAccepted: false,
    returnedSuccess: false,
  }

  action init = all {
    phase' = Idle,
    transactionShape' = EmptyTransaction,
    controlPlaneConfigured' = false,
    affectedRouteShards' = Set(),
    sourceCleanupShards' = Set(),
    sourceRowPresent' = false,
    sourceTombstonePresent' = false,
    targetRowPresent' = false,
    targetTxnRecorded' = false,
    sourceTombstoneRecorded' = false,
    objectShardIndex' = None,
    replicaObjectShardIndex' = None,
    targetIndexEverWritten' = false,
    replayedShards' = Set(),
    lastDecision' = NoDecision,
    postMoveProbe' = emptyPostMoveProbe,
  }

  action beginScenario(
    controlPlane: bool,
    shape: TransactionShape,
    replicaIndex: Option[MetadataShard],
  ): bool = all {
    phase == Idle,
    phase' = if (shape == EmptyTransaction) { NoOp } else { SourcePresent },
    transactionShape' = shape,
    controlPlaneConfigured' = controlPlane,
    affectedRouteShards' = affectedRouteShardsFor(controlPlane, shape),
    sourceCleanupShards' = initialSourceCleanupShards(shape),
    sourceRowPresent' = shape == MoveRootToServices,
    sourceTombstonePresent' = false,
    targetRowPresent' = false,
    targetTxnRecorded' = false,
    sourceTombstoneRecorded' = false,
    objectShardIndex' =
      if (shape == MoveRootToServices) { Some(RootShard) } else { None },
    replicaObjectShardIndex' =
      if (shape == MoveRootToServices) { replicaIndex } else { None },
    targetIndexEverWritten' = false,
    replayedShards' = Set(),
    lastDecision' = ScenarioStarted,
    postMoveProbe' = emptyPostMoveProbe,
  }

  /// The target shard transaction fails before the target row or object-shard
  /// index can be changed. This matches the services-shard journal failure
  /// regression test.
  action failTargetShardApply: bool = all {
    phase == SourcePresent,
    phase' = TargetShardFailed,
    transactionShape' = transactionShape,
    controlPlaneConfigured' = controlPlaneConfigured,
    affectedRouteShards' = affectedRouteShards,
    sourceCleanupShards' = sourceCleanupShards,
    sourceRowPresent' = sourceRowPresent,
    sourceTombstonePresent' = sourceTombstonePresent,
    targetRowPresent' = false,
    targetTxnRecorded' = false,
    sourceTombstoneRecorded' = false,
    objectShardIndex' = Some(RootShard),
    replicaObjectShardIndex' = replicaObjectShardIndex,
    targetIndexEverWritten' = false,
    replayedShards' = replayedShards,
    lastDecision' = TargetApplyFailed,
    postMoveProbe' = postMoveProbe,
  }

  /// The target shard transaction is durable, but the router fails before
  /// writing the object-shard index. The current implementation returns an
  /// error in this window; source path lookup still resolves to the old source
  /// because the index remains on the root shard, while the target row is
  /// hidden until a retry writes the index.
  action applyTargetShardButFailIndexWrite: bool = all {
    phase == SourcePresent,
    phase' = TargetIndexFailed,
    transactionShape' = transactionShape,
    controlPlaneConfigured' = controlPlaneConfigured,
    affectedRouteShards' = affectedRouteShards,
    sourceCleanupShards' = sourceCleanupShards,
    sourceRowPresent' = sourceRowPresent,
    sourceTombstonePresent' = sourceTombstonePresent,
    targetRowPresent' = true,
    targetTxnRecorded' = true,
    sourceTombstoneRecorded' = false,
    objectShardIndex' = Some(RootShard),
    replicaObjectShardIndex' = replicaObjectShardIndex,
    targetIndexEverWritten' = false,
    replayedShards' = replayedShards,
    lastDecision' = TargetAppliedIndexFailed,
    postMoveProbe' = postMoveProbe,
  }

  /// Normal target shard apply followed by
  /// `write_transaction_object_shard_indexes`.
  action applyTargetShardAndIndex: bool = all {
    any {
      phase == SourcePresent,
      phase == TargetIndexFailed,
    },
    phase' = TargetApplied,
    transactionShape' = transactionShape,
    controlPlaneConfigured' = controlPlaneConfigured,
    affectedRouteShards' = affectedRouteShards,
    sourceCleanupShards' = sourceCleanupShards,
    sourceRowPresent' = sourceRowPresent,
    sourceTombstonePresent' = sourceTombstonePresent,
    targetRowPresent' = true,
    targetTxnRecorded' = true,
    sourceTombstoneRecorded' = false,
    objectShardIndex' = Some(ServicesShard),
    // Replica ingest/open does not synchronously repair this derived index.
    replicaObjectShardIndex' = replicaObjectShardIndex,
    targetIndexEverWritten' = true,
    replayedShards' = replayedShards,
    lastDecision' = TargetAppliedAndIndexed,
    postMoveProbe' = postMoveProbe,
  }

  /// Idempotent replay of the target shard transaction does not create a
  /// second visible object or move the index back.
  action replayTargetShardTransaction: bool = all {
    targetTxnRecorded,
    any {
      phase == TargetApplied,
      phase == SourceTombstoneFailed,
      phase == Complete,
    },
    phase' = phase,
    transactionShape' = transactionShape,
    controlPlaneConfigured' = controlPlaneConfigured,
    affectedRouteShards' = affectedRouteShards,
    sourceCleanupShards' = sourceCleanupShards,
    sourceRowPresent' = sourceRowPresent,
    sourceTombstonePresent' = sourceTombstonePresent,
    targetRowPresent' = targetRowPresent,
    targetTxnRecorded' = targetTxnRecorded,
    sourceTombstoneRecorded' = sourceTombstoneRecorded,
    objectShardIndex' = objectShardIndex,
    replicaObjectShardIndex' = replicaObjectShardIndex,
    targetIndexEverWritten' = targetIndexEverWritten,
    replayedShards' = replayedShards.union(Set(ServicesShard)),
    lastDecision' = TargetReplayed,
    postMoveProbe' = postMoveProbe,
  }

  /// The source cleanup journal append fails after the target row and
  /// object-shard index are durable. This matches the root-shard tombstone
  /// failure regression test: object-id and target-path lookups use the target
  /// shard, while the stale source row is hidden by the index.
  action failSourceTombstoneApply: bool = all {
    phase == TargetApplied,
    phase' = SourceTombstoneFailed,
    transactionShape' = transactionShape,
    controlPlaneConfigured' = controlPlaneConfigured,
    affectedRouteShards' = affectedRouteShards,
    sourceCleanupShards' = sourceCleanupShards,
    sourceRowPresent' = sourceRowPresent,
    sourceTombstonePresent' = false,
    targetRowPresent' = targetRowPresent,
    targetTxnRecorded' = targetTxnRecorded,
    sourceTombstoneRecorded' = false,
    objectShardIndex' = objectShardIndex,
    replicaObjectShardIndex' = replicaObjectShardIndex,
    targetIndexEverWritten' = targetIndexEverWritten,
    replayedShards' = replayedShards,
    lastDecision' = SourceTombstoneApplyFailed,
    postMoveProbe' = postMoveProbe,
  }

  action applySourceTombstone: bool = all {
    any {
      phase == TargetApplied,
      phase == SourceTombstoneFailed,
    },
    sourceCleanupShards.contains(RootShard),
    phase' = Complete,
    transactionShape' = transactionShape,
    controlPlaneConfigured' = controlPlaneConfigured,
    affectedRouteShards' = affectedRouteShards,
    sourceCleanupShards' = sourceCleanupShards,
    sourceRowPresent' = false,
    sourceTombstonePresent' = true,
    targetRowPresent' = targetRowPresent,
    targetTxnRecorded' = targetTxnRecorded,
    sourceTombstoneRecorded' = true,
    objectShardIndex' = Some(ServicesShard),
    replicaObjectShardIndex' = replicaObjectShardIndex,
    targetIndexEverWritten' = true,
    replayedShards' = replayedShards,
    lastDecision' = SourceTombstoneApplied,
    postMoveProbe' = postMoveProbe,
  }

  /// Counterexample refinement for the observed Docker failure. The global
  /// tombstone-first lookup treats the moved live directory as deleted, so
  /// mkdir-all commits a resurrection on the services shard. Its subsequent
  /// parent touch belongs to the root shard and reports that unrelated primary;
  /// authority routing correctly rejects the redirect, but only after the
  /// unnecessary mutation has committed.
  action runLegacyMkdirAllAfterMove: bool = all {
    phase == Complete,
    affectedRouteShards == Set(ServicesShard),
    postMoveProbe.outcome == PostMoveMkdirNotAttempted,
    replicaObjectShardIndex == Some(RootShard),
    legacyGlobalTombstoneFirstLookup == PathTombstone,
    legacyReplicaBackendLookup == PathMissing,
    shardAwareSelectedLookup == PathLive,
    phase' = phase,
    transactionShape' = transactionShape,
    controlPlaneConfigured' = controlPlaneConfigured,
    affectedRouteShards' = affectedRouteShards,
    sourceCleanupShards' = sourceCleanupShards,
    sourceRowPresent' = sourceRowPresent,
    sourceTombstonePresent' = sourceTombstonePresent,
    targetRowPresent' = targetRowPresent,
    targetTxnRecorded' = targetTxnRecorded,
    sourceTombstoneRecorded' = sourceTombstoneRecorded,
    objectShardIndex' = objectShardIndex,
    // The unnecessary resurrection rewrites the local index only after the
    // wrong visibility decision and committed side effect.
    replicaObjectShardIndex' = Some(ServicesShard),
    targetIndexEverWritten' = targetIndexEverWritten,
    replayedShards' = replayedShards,
    lastDecision' = lastDecision,
    postMoveProbe' = {
      outcome: LegacyResurrectionThenUncorroboratedRedirect,
      committedMutations: 1,
      replicaIndexBeforeLookup: replicaObjectShardIndex,
      legacyBackendLookupBefore: legacyReplicaBackendLookup,
      derivedIndexRepairs: 0,
      reportedRedirectShard: Some(RootShard),
      redirectAccepted: false,
      returnedSuccess: false,
    },
  }

  /// The shard-aware lookup sees that the directory already exists on the
  /// current path-selected shard. Mkdir-all is a successful no-op: it neither
  /// resurrects the object nor reaches the foreign parent-touch redirect.
  action runShardAwareMkdirAllAfterMove: bool = all {
    phase == Complete,
    affectedRouteShards == Set(ServicesShard),
    postMoveProbe.outcome == PostMoveMkdirNotAttempted,
    shardAwareSelectedLookup == PathLive,
    phase' = phase,
    transactionShape' = transactionShape,
    controlPlaneConfigured' = controlPlaneConfigured,
    affectedRouteShards' = affectedRouteShards,
    sourceCleanupShards' = sourceCleanupShards,
    sourceRowPresent' = sourceRowPresent,
    sourceTombstonePresent' = sourceTombstonePresent,
    targetRowPresent' = targetRowPresent,
    targetTxnRecorded' = targetTxnRecorded,
    sourceTombstoneRecorded' = sourceTombstoneRecorded,
    objectShardIndex' = objectShardIndex,
    // Repair is derived convergence after the path-selected live row wins; it
    // is not a precondition for visibility or operation success.
    replicaObjectShardIndex' = Some(ServicesShard),
    targetIndexEverWritten' = targetIndexEverWritten,
    replayedShards' = replayedShards,
    lastDecision' = lastDecision,
    postMoveProbe' = {
      outcome: ShardAwareExistingDirectorySucceeded,
      committedMutations: 0,
      replicaIndexBeforeLookup: replicaObjectShardIndex,
      legacyBackendLookupBefore: legacyReplicaBackendLookup,
      derivedIndexRepairs:
        if (replicaObjectShardIndex == Some(ServicesShard)) 0 else 1,
      reportedRedirectShard: None,
      redirectAccepted: false,
      returnedSuccess: true,
    },
  }

  action stutterTerminal: bool = all {
    any {
      phase == NoOp,
      phase == TargetShardFailed,
      phase == Complete,
    },
    phase' = phase,
    transactionShape' = transactionShape,
    controlPlaneConfigured' = controlPlaneConfigured,
    affectedRouteShards' = affectedRouteShards,
    sourceCleanupShards' = sourceCleanupShards,
    sourceRowPresent' = sourceRowPresent,
    sourceTombstonePresent' = sourceTombstonePresent,
    targetRowPresent' = targetRowPresent,
    targetTxnRecorded' = targetTxnRecorded,
    sourceTombstoneRecorded' = sourceTombstoneRecorded,
    objectShardIndex' = objectShardIndex,
    replicaObjectShardIndex' = replicaObjectShardIndex,
    targetIndexEverWritten' = targetIndexEverWritten,
    replayedShards' = replayedShards,
    lastDecision' = TerminalStutter,
    postMoveProbe' = postMoveProbe,
  }

  action step = any {
    {
      nondet controlPlane = oneOf(Set(true, false))
      nondet shape = oneOf(TransactionShapes)
      nondet replicaIndex = oneOf(ReplicaInitialIndexes)
      beginScenario(controlPlane, shape, replicaIndex)
    },
    failTargetShardApply,
    applyTargetShardButFailIndexWrite,
    applyTargetShardAndIndex,
    replayTargetShardTransaction,
    failSourceTombstoneApply,
    applySourceTombstone,
    runLegacyMkdirAllAfterMove,
    runShardAwareMkdirAllAfterMove,
    stutterTerminal,
  }
}
