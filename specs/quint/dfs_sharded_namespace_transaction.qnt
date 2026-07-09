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
  import common.* from "./common"

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

  pure val Shards: Set[MetadataShard] = Set(RootShard, ServicesShard)
  pure val TransactionShapes: Set[TransactionShape] =
    Set(EmptyTransaction, MoveRootToServices)

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
  var targetIndexEverWritten: bool
  var replayedShards: Set[MetadataShard]
  var lastDecision: Decision

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
    targetIndexEverWritten' = false,
    replayedShards' = Set(),
    lastDecision' = NoDecision,
  }

  action beginScenario(controlPlane: bool, shape: TransactionShape): bool = all {
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
    targetIndexEverWritten' = false,
    replayedShards' = Set(),
    lastDecision' = ScenarioStarted,
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
    targetIndexEverWritten' = false,
    replayedShards' = replayedShards,
    lastDecision' = TargetApplyFailed,
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
    targetIndexEverWritten' = false,
    replayedShards' = replayedShards,
    lastDecision' = TargetAppliedIndexFailed,
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
    targetIndexEverWritten' = true,
    replayedShards' = replayedShards,
    lastDecision' = TargetAppliedAndIndexed,
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
    targetIndexEverWritten' = targetIndexEverWritten,
    replayedShards' = replayedShards.union(Set(ServicesShard)),
    lastDecision' = TargetReplayed,
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
    targetIndexEverWritten' = targetIndexEverWritten,
    replayedShards' = replayedShards,
    lastDecision' = SourceTombstoneApplyFailed,
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
    targetIndexEverWritten' = true,
    replayedShards' = replayedShards,
    lastDecision' = SourceTombstoneApplied,
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
    targetIndexEverWritten' = targetIndexEverWritten,
    replayedShards' = replayedShards,
    lastDecision' = TerminalStutter,
  }

  action step = any {
    {
      nondet controlPlane = oneOf(Set(true, false))
      nondet shape = oneOf(TransactionShapes)
      beginScenario(controlPlane, shape)
    },
    failTargetShardApply,
    applyTargetShardButFailIndexWrite,
    applyTargetShardAndIndex,
    replayTargetShardTransaction,
    failSourceTombstoneApply,
    applySourceTombstone,
    stutterTerminal,
  }

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
