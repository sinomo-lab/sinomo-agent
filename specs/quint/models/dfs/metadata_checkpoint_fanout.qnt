/// Concurrent DFS metadata catch-up inside ordered durability checkpoints.
///
/// Cross-shard namespace work must preserve its source-prepare,
/// destination-adopt, and source-finalize barriers. Replicas inside one
/// barrier are independent, however: catch-up requests may be in flight and
/// acknowledged in any order. This model isolates that scheduling refinement
/// from the namespace-state model in `dfs_posix_cross_shard_namespace`.
///
/// The primary contributes the first acknowledgement. A majority may advance
/// the barrier after the full-shard grace expires; a late response from the
/// previous checkpoint is request-scoped and cannot acknowledge the next one.
module dfs_metadata_checkpoint_fanout {
  import common.* from "../../lib/common"

  type CheckpointPhase =
    | CheckpointIdle
    | SourcePrepareCatchup
    | DestinationAdoptCatchup
    | SourceFinalizeCatchup
    | CheckpointComplete
  type CheckpointTransition =
    | CheckpointInitialized
    | OperationStarted
    | CatchupSent
    | DirectBudgetExpired
    | RoutedFallbackSent
    | CatchupApplied
    | CatchupRejected
    | SourcePrepareAdvanced
    | DestinationAdoptAdvanced
    | SourceFinalizeAdvanced
    | StaleResponseIgnored
    | CompleteStutter

  type MetadataCheckpointFanoutState = {
    phase: CheckpointPhase,
    generation: int,
    inFlightGeneration: Node -> int,
    acknowledgedGeneration: Node -> int,
    rejected: Set[Node],
    needsRoutedFallback: Set[Node],
    routedFallbackInFlight: Set[Node],
    staleResponseGeneration: Node -> int,
    sourcePrepareDurable: bool,
    destinationAdoptDurable: bool,
    sourceFinalizeDurable: bool,
    lastBarrierAcknowledged: Set[Node],
    lastStaleResponseAcknowledged: Set[Node],
    outOfOrderObserved: bool,
    rejectionObserved: bool,
    retryObserved: bool,
    directBudgetExpiredObserved: bool,
    routedFallbackObserved: bool,
    lastDirectBudgetExpiredReplica: Node,
    staleResponseIgnored: bool,
    lastTransition: CheckpointTransition,
  }

  pure val Primary: Node = N1
  pure val ReplicaTargets: Set[Node] = Set(N2, N3)
  pure val FanoutCap: int = 2

  pure val EmptyGenerationByNode: Node -> int = Nodes.mapBy(_ => 0)

  pure def inFlightNodes(state: MetadataCheckpointFanoutState): Set[Node] =
    ReplicaTargets.filter(node =>
      state.generation > 0
        and state.inFlightGeneration.get(node) == state.generation
    )

  pure def acknowledgedNodes(state: MetadataCheckpointFanoutState): Set[Node] =
    Nodes.filter(node =>
      state.generation > 0
        and state.acknowledgedGeneration.get(node) == state.generation
    )

  pure def staleResponseNodes(state: MetadataCheckpointFanoutState): Set[Node] =
    ReplicaTargets.filter(node => state.staleResponseGeneration.get(node) > 0)

  pure def primaryAcknowledgement(generation: int): Node -> int =
    Nodes.mapBy(node => if (node == Primary) generation else 0)

  pure def carryStaleResponses(
    state: MetadataCheckpointFanoutState
  ): Node -> int =
    Nodes.mapBy(node =>
      if (not(ReplicaTargets.contains(node))) {
        0
      } else if (state.inFlightGeneration.get(node) == state.generation) {
        state.generation
      } else {
        state.staleResponseGeneration.get(node)
      }
    )

  pure val initialState: MetadataCheckpointFanoutState = {
    phase: CheckpointIdle,
    generation: 0,
    inFlightGeneration: EmptyGenerationByNode,
    acknowledgedGeneration: EmptyGenerationByNode,
    rejected: Set(),
    needsRoutedFallback: Set(),
    routedFallbackInFlight: Set(),
    staleResponseGeneration: EmptyGenerationByNode,
    sourcePrepareDurable: false,
    destinationAdoptDurable: false,
    sourceFinalizeDurable: false,
    lastBarrierAcknowledged: Set(),
    lastStaleResponseAcknowledged: Set(),
    outOfOrderObserved: false,
    rejectionObserved: false,
    retryObserved: false,
    directBudgetExpiredObserved: false,
    routedFallbackObserved: false,
    lastDirectBudgetExpiredReplica: Primary,
    staleResponseIgnored: false,
    lastTransition: CheckpointInitialized,
  }

  var fanout: MetadataCheckpointFanoutState

  pure def active(state: MetadataCheckpointFanoutState): bool = any {
    state.phase == SourcePrepareCatchup,
    state.phase == DestinationAdoptCatchup,
    state.phase == SourceFinalizeCatchup,
  }

  pure def barrierSatisfied(state: MetadataCheckpointFanoutState): bool =
    hasQuorum(acknowledgedNodes(state))

  pure def canSend(state: MetadataCheckpointFanoutState, replica: Node): bool = all {
    active(state),
    ReplicaTargets.contains(replica),
    state.acknowledgedGeneration.get(replica) != state.generation,
    state.inFlightGeneration.get(replica) != state.generation,
    not(state.needsRoutedFallback.contains(replica)),
    inFlightNodes(state).size() < FanoutCap,
  }

  pure def canSendRoutedFallback(
    state: MetadataCheckpointFanoutState,
    replica: Node
  ): bool = all {
    active(state),
    state.needsRoutedFallback.contains(replica),
    state.acknowledgedGeneration.get(replica) != state.generation,
    state.inFlightGeneration.get(replica) != state.generation,
    inFlightNodes(state).size() < FanoutCap,
  }

  action init = fanout' = initialState

  action beginOperation: bool = {
    val nextGeneration = fanout.generation + 1
    all {
      fanout.phase == CheckpointIdle or fanout.phase == CheckpointComplete,
      fanout' = {
        phase: SourcePrepareCatchup,
        generation: nextGeneration,
        inFlightGeneration: EmptyGenerationByNode,
        acknowledgedGeneration: primaryAcknowledgement(nextGeneration),
        rejected: Set(),
        needsRoutedFallback: Set(),
        routedFallbackInFlight: Set(),
        staleResponseGeneration: fanout.staleResponseGeneration,
        sourcePrepareDurable: false,
        destinationAdoptDurable: false,
        sourceFinalizeDurable: false,
        lastBarrierAcknowledged: Set(),
        lastStaleResponseAcknowledged: Set(),
        outOfOrderObserved: false,
        rejectionObserved: false,
        retryObserved: false,
        directBudgetExpiredObserved: false,
        routedFallbackObserved: false,
        lastDirectBudgetExpiredReplica: Primary,
        staleResponseIgnored: false,
        lastTransition: OperationStarted,
      },
    }
  }

  action sendCatchup(replica: Node): bool = all {
    canSend(fanout, replica),
    fanout' = fanout
      .with(
        "inFlightGeneration",
        fanout.inFlightGeneration.set(replica, fanout.generation)
      )
      .with("rejected", fanout.rejected.filter(node => node != replica))
      .with(
        "retryObserved",
        fanout.retryObserved or fanout.rejected.contains(replica)
      )
      .with("lastTransition", CatchupSent),
  }

  /// A bounded direct RPC is only an accelerator. If its deadline expires,
  /// the unresolved request becomes canonical routed work for the same
  /// checkpoint generation; it cannot disappear or immediately re-enter the
  /// direct lane and repeat the same timeout forever.
  action expireDirectCatchup(replica: Node): bool = all {
    active(fanout),
    fanout.inFlightGeneration.get(replica) == fanout.generation,
    not(fanout.routedFallbackInFlight.contains(replica)),
    fanout' = fanout
      .with(
        "inFlightGeneration",
        fanout.inFlightGeneration.set(replica, 0)
      )
      .with(
        "needsRoutedFallback",
        fanout.needsRoutedFallback.union(Set(replica))
      )
      .with("directBudgetExpiredObserved", true)
      .with("lastDirectBudgetExpiredReplica", replica)
      .with("lastTransition", DirectBudgetExpired),
  }

  action sendRoutedFallback(replica: Node): bool = all {
    canSendRoutedFallback(fanout, replica),
    fanout' = fanout
      .with(
        "inFlightGeneration",
        fanout.inFlightGeneration.set(replica, fanout.generation)
      )
      .with(
        "needsRoutedFallback",
        fanout.needsRoutedFallback.filter(node => node != replica)
      )
      .with(
        "routedFallbackInFlight",
        fanout.routedFallbackInFlight.union(Set(replica))
      )
      .with("routedFallbackObserved", true)
      .with("lastTransition", RoutedFallbackSent),
  }

  action receiveCatchupApplied(replica: Node): bool = all {
    active(fanout),
    fanout.inFlightGeneration.get(replica) == fanout.generation,
    fanout' = fanout
      .with(
        "inFlightGeneration",
        fanout.inFlightGeneration.set(replica, 0)
      )
      .with(
        "acknowledgedGeneration",
        fanout.acknowledgedGeneration.set(replica, fanout.generation)
      )
      .with(
        "routedFallbackInFlight",
        fanout.routedFallbackInFlight.filter(node => node != replica)
      )
      .with(
        "outOfOrderObserved",
        fanout.outOfOrderObserved
          or (
            replica == N3
              and fanout.inFlightGeneration.get(N2) == fanout.generation
          )
      )
      .with("lastTransition", CatchupApplied),
  }

  action receiveCatchupRejected(replica: Node): bool = all {
    active(fanout),
    fanout.inFlightGeneration.get(replica) == fanout.generation,
    fanout' = fanout
      .with(
        "inFlightGeneration",
        fanout.inFlightGeneration.set(replica, 0)
      )
      .with("rejected", fanout.rejected.union(Set(replica)))
      .with(
        "routedFallbackInFlight",
        fanout.routedFallbackInFlight.filter(node => node != replica)
      )
      .with("rejectionObserved", true)
      .with("lastTransition", CatchupRejected),
  }

  action advanceSourcePrepare: bool = {
    val nextGeneration = fanout.generation + 1
    all {
      fanout.phase == SourcePrepareCatchup,
      barrierSatisfied(fanout),
      fanout' = fanout
        .with("phase", DestinationAdoptCatchup)
        .with("generation", nextGeneration)
        .with("staleResponseGeneration", carryStaleResponses(fanout))
        .with("inFlightGeneration", EmptyGenerationByNode)
        .with("acknowledgedGeneration", primaryAcknowledgement(nextGeneration))
        .with("rejected", Set())
        .with("needsRoutedFallback", Set())
        .with("routedFallbackInFlight", Set())
        .with("sourcePrepareDurable", true)
        .with("lastBarrierAcknowledged", acknowledgedNodes(fanout))
        .with("lastTransition", SourcePrepareAdvanced),
    }
  }

  action advanceDestinationAdopt: bool = {
    val nextGeneration = fanout.generation + 1
    all {
      fanout.phase == DestinationAdoptCatchup,
      fanout.sourcePrepareDurable,
      barrierSatisfied(fanout),
      fanout' = fanout
        .with("phase", SourceFinalizeCatchup)
        .with("generation", nextGeneration)
        .with("staleResponseGeneration", carryStaleResponses(fanout))
        .with("inFlightGeneration", EmptyGenerationByNode)
        .with("acknowledgedGeneration", primaryAcknowledgement(nextGeneration))
        .with("rejected", Set())
        .with("needsRoutedFallback", Set())
        .with("routedFallbackInFlight", Set())
        .with("destinationAdoptDurable", true)
        .with("lastBarrierAcknowledged", acknowledgedNodes(fanout))
        .with("lastTransition", DestinationAdoptAdvanced),
    }
  }

  action advanceSourceFinalize: bool = all {
    fanout.phase == SourceFinalizeCatchup,
    fanout.sourcePrepareDurable,
    fanout.destinationAdoptDurable,
    barrierSatisfied(fanout),
    fanout' = fanout
      .with("phase", CheckpointComplete)
      .with("staleResponseGeneration", carryStaleResponses(fanout))
      .with("inFlightGeneration", EmptyGenerationByNode)
      .with("rejected", Set())
      .with("needsRoutedFallback", Set())
      .with("routedFallbackInFlight", Set())
      .with("sourceFinalizeDurable", true)
      .with("lastBarrierAcknowledged", acknowledgedNodes(fanout))
      .with("lastTransition", SourceFinalizeAdvanced),
  }

  /// Request ids scope responses to one checkpoint. A response left behind by
  /// a quorum fallback can be consumed later, but cannot mutate the current
  /// checkpoint's acknowledgement set.
  action ignoreStaleResponse(replica: Node): bool = {
    val responseGeneration = fanout.staleResponseGeneration.get(replica)
    all {
      responseGeneration > 0,
      any {
        responseGeneration < fanout.generation,
        fanout.phase == CheckpointComplete
          and responseGeneration == fanout.generation,
      },
      fanout' = fanout
        .with(
          "staleResponseGeneration",
          fanout.staleResponseGeneration.set(replica, 0)
        )
        .with("lastStaleResponseAcknowledged", acknowledgedNodes(fanout))
        .with("staleResponseIgnored", true)
        .with("lastTransition", StaleResponseIgnored),
    }
  }

  action stutterComplete: bool = all {
    fanout.phase == CheckpointComplete,
    fanout' = fanout.with("lastTransition", CompleteStutter),
  }

  action step = any {
    beginOperation,
    {
      nondet replica = oneOf(ReplicaTargets)
      sendCatchup(replica)
    },
    {
      nondet replica = oneOf(ReplicaTargets)
      expireDirectCatchup(replica)
    },
    {
      nondet replica = oneOf(ReplicaTargets)
      sendRoutedFallback(replica)
    },
    {
      nondet replica = oneOf(ReplicaTargets)
      receiveCatchupApplied(replica)
    },
    {
      nondet replica = oneOf(ReplicaTargets)
      receiveCatchupRejected(replica)
    },
    advanceSourcePrepare,
    advanceDestinationAdopt,
    advanceSourceFinalize,
    {
      nondet replica = oneOf(ReplicaTargets)
      ignoreStaleResponse(replica)
    },
    stutterComplete,
  }
}
