/// Safety and bounded reachability for concurrent metadata checkpoint fan-out.
module dfs_metadata_checkpoint_fanout_safety {
  import common.* from "../../lib/common"
  import dfs_metadata_checkpoint_fanout.* from "../../models/dfs/metadata_checkpoint_fanout"

  def replicaSetsStayWellFormed: bool = all {
    fanout.generation >= 0,
    Nodes.forall(node => all {
      any {
        fanout.inFlightGeneration.get(node) == 0,
        fanout.inFlightGeneration.get(node) == fanout.generation,
      },
      any {
        fanout.acknowledgedGeneration.get(node) == 0,
        fanout.acknowledgedGeneration.get(node) == fanout.generation,
      },
      fanout.staleResponseGeneration.get(node) >= 0,
      fanout.staleResponseGeneration.get(node) <= fanout.generation,
    }),
    fanout.rejected.subseteq(ReplicaTargets),
    fanout.needsRoutedFallback.subseteq(ReplicaTargets),
    fanout.routedFallbackInFlight.subseteq(inFlightNodes(fanout)),
    inFlightNodes(fanout).intersect(acknowledgedNodes(fanout)) == Set(),
    inFlightNodes(fanout).intersect(fanout.rejected) == Set(),
    acknowledgedNodes(fanout).intersect(fanout.rejected) == Set(),
    fanout.needsRoutedFallback.intersect(inFlightNodes(fanout)) == Set(),
    fanout.needsRoutedFallback.intersect(acknowledgedNodes(fanout)) == Set(),
    fanout.needsRoutedFallback.intersect(fanout.rejected) == Set(),
    fanout.lastBarrierAcknowledged.subseteq(Nodes),
    fanout.lastStaleResponseAcknowledged.subseteq(Nodes),
  }

  def fanoutStaysBounded: bool =
    inFlightNodes(fanout).size() <= FanoutCap

  def activeCheckpointRetainsPrimaryAck: bool =
    active(fanout) implies acknowledgedNodes(fanout).contains(Primary)

  def checkpointOrderIsMonotonic: bool = all {
    fanout.destinationAdoptDurable implies fanout.sourcePrepareDurable,
    fanout.sourceFinalizeDurable implies fanout.destinationAdoptDurable,
    match fanout.phase {
      | CheckpointIdle => all {
          not(fanout.sourcePrepareDurable),
          not(fanout.destinationAdoptDurable),
          not(fanout.sourceFinalizeDurable),
        }
      | SourcePrepareCatchup => all {
          not(fanout.sourcePrepareDurable),
          not(fanout.destinationAdoptDurable),
          not(fanout.sourceFinalizeDurable),
        }
      | DestinationAdoptCatchup => all {
          fanout.sourcePrepareDurable,
          not(fanout.destinationAdoptDurable),
          not(fanout.sourceFinalizeDurable),
        }
      | SourceFinalizeCatchup => all {
          fanout.sourcePrepareDurable,
          fanout.destinationAdoptDurable,
          not(fanout.sourceFinalizeDurable),
        }
      | CheckpointComplete => all {
          fanout.sourcePrepareDurable,
          fanout.destinationAdoptDurable,
          fanout.sourceFinalizeDurable,
        }
    },
  }

  def everyBarrierAdvanceHadQuorum: bool =
    any {
      fanout.lastTransition == SourcePrepareAdvanced,
      fanout.lastTransition == DestinationAdoptAdvanced,
      fanout.lastTransition == SourceFinalizeAdvanced,
    } implies hasQuorum(fanout.lastBarrierAcknowledged)

  def staleResponsesCannotAcknowledgeCurrentCheckpoint: bool =
    fanout.lastTransition == StaleResponseIgnored implies
      acknowledgedNodes(fanout) == fanout.lastStaleResponseAcknowledged

  def directBudgetExpirationPreservesRoutedFallback: bool =
    fanout.lastTransition == DirectBudgetExpired implies
      fanout.needsRoutedFallback.contains(fanout.lastDirectBudgetExpiredReplica)

  def safety: bool = all {
    replicaSetsStayWellFormed,
    fanoutStaysBounded,
    activeCheckpointRetainsPrimaryAck,
    checkpointOrderIsMonotonic,
    everyBarrierAdvanceHadQuorum,
    staleResponsesCannotAcknowledgeCurrentCheckpoint,
    directBudgetExpirationPreservesRoutedFallback,
  }

  def witnessBothReplicaCatchupsInFlight: bool = all {
    fanout.phase == SourcePrepareCatchup,
    inFlightNodes(fanout) == Set(N2, N3),
    inFlightNodes(fanout).size() == FanoutCap,
  }

  def witnessOutOfOrderAckStillReachesQuorum: bool = all {
    fanout.phase == SourcePrepareCatchup,
    fanout.outOfOrderObserved,
    acknowledgedNodes(fanout) == Set(N1, N3),
    inFlightNodes(fanout) == Set(N2),
    barrierSatisfied(fanout),
  }

  def witnessQuorumAdvanceLeavesLateSiblingScoped: bool = all {
    fanout.phase == DestinationAdoptCatchup,
    fanout.sourcePrepareDurable,
    fanout.lastBarrierAcknowledged == Set(N1, N3),
    fanout.staleResponseGeneration.get(N2) == fanout.generation - 1,
  }

  def witnessLateSiblingDoesNotAckNextCheckpoint: bool = all {
    fanout.phase == DestinationAdoptCatchup,
    fanout.lastTransition == StaleResponseIgnored,
    fanout.inFlightGeneration.get(N2) == fanout.generation,
    acknowledgedNodes(fanout) == Set(N1),
    fanout.staleResponseGeneration.get(N2) == 0,
  }

  def witnessRejectedReplicaCanBeRetried: bool = all {
    fanout.rejectionObserved,
    fanout.retryObserved,
    inFlightNodes(fanout).size() > 0,
  }

  def witnessPartialDirectResultUsesRoutedFallback: bool = all {
    fanout.phase == SourcePrepareCatchup,
    fanout.directBudgetExpiredObserved,
    fanout.routedFallbackObserved,
    fanout.routedFallbackInFlight == Set(N3),
    acknowledgedNodes(fanout) == Set(N1, N2),
  }

  def witnessOrderedCheckpointsComplete: bool = all {
    fanout.phase == CheckpointComplete,
    fanout.sourcePrepareDurable,
    fanout.destinationAdoptDurable,
    fanout.sourceFinalizeDurable,
    hasQuorum(fanout.lastBarrierAcknowledged),
  }
}
