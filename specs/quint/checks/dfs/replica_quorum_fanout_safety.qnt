/// Safety and bounded reachability for quorum-aware DFS replica fan-out.
module dfs_replica_quorum_fanout_safety {
  import common.* from "../../lib/common"
  import dfs_replica_quorum_fanout.* from "../../models/dfs/replica_quorum_fanout"

  def replicaSetsStayWellFormed: bool = all {
    fanout.durableReceipts.subseteq(Nodes),
    fanout.stagedReceipts.subseteq(Nodes),
    fanout.stageInflight.subseteq(Nodes),
    fanout.repairInflight.subseteq(Nodes),
    fanout.promotionPending.subseteq(Nodes),
    fanout.promotionInflight.subseteq(Nodes),
    fanout.promotionRetries.subseteq(Nodes),
    fanout.promotedReplicas.subseteq(Nodes),
    fanout.repairJobs.subseteq(Nodes),
    fanout.stageRetries.subseteq(Nodes),
    fanout.repairRetries.subseteq(Nodes),
    fanout.repairAttempts.subseteq(Nodes),
    fanout.stagedReceipts.subseteq(fanout.durableReceipts),
    fanout.durableReceipts.intersect(fanout.stageInflight) == Set(),
    fanout.durableReceipts.intersect(fanout.repairInflight) == Set(),
    fanout.stageInflight.intersect(fanout.repairInflight) == Set(),
    fanout.promotionPending.intersect(fanout.promotionInflight) == Set(),
    fanout.promotionPending.intersect(fanout.promotedReplicas) == Set(),
    fanout.promotionInflight.intersect(fanout.promotedReplicas) == Set(),
    fanout.promotionRetries.subseteq(
      fanout.promotionPending.union(fanout.promotionInflight)
    ),
    fanout.promotionRetries.intersect(fanout.promotedReplicas) == Set(),
    fanout.stageInflight.subseteq(fanout.stageRetries),
    fanout.repairInflight.subseteq(fanout.repairRetries),
    fanout.repairInflight.subseteq(fanout.repairAttempts),
    fanout.repairAttempts == fanout.repairRetries,
    fanout.repairJobs.subseteq(missingReplicas(fanout)),
    if (fanout.visibleCommitted) {
      fanout.promotionPending
        .union(fanout.promotionInflight)
        .union(fanout.promotedReplicas) == fanout.stagedReceipts
    } else {
      all {
        fanout.promotionPending == Set(),
        fanout.promotionInflight == Set(),
        fanout.promotionRetries == Set(),
        fanout.promotedReplicas == Set(),
        fanout.repairJobs == Set(),
      }
    },
  }

  def transferFanoutStaysBounded: bool = all {
    fanout.stageInflight.size() <= StageFanoutCap,
    fanout.repairInflight.size() <= RepairFanoutCap,
    fanout.promotionInflight.size() <= PromotionFanoutCap,
  }

  /// A background write cannot reserve more pre-commit network transfers than
  /// the exact number still required to reach W.
  def backgroundPrecommitStagesStayWithinWDeficit: bool =
    all {
      fanout.phase == FanoutActive,
      fanout.repairMode == BackgroundRepair,
      not(fanout.visibleCommitted),
    } implies
      fanout.durableReceipts.size() + fanout.stageInflight.size()
        <= fanout.minCommitReplicas

  /// Reaching W in background mode necessarily drains every pre-commit stage
  /// reservation, so a slow N-W sibling cannot hold the response boundary.
  def backgroundCommitHasNoStageSibling: bool =
    all {
      fanout.visibleCommitted,
      fanout.repairMode == BackgroundRepair,
    } implies fanout.stageInflight.size() == 0

  def visibleCommitRequiresW: bool =
    fanout.visibleCommitted implies all {
      CommitThresholds.contains(fanout.minCommitReplicas),
      fanout.durableReceipts.size() >= fanout.minCommitReplicas,
    }

  /// The mode copied at commit is the exact effective primary mode selected at
  /// intent creation. Caller-requested foreground writes whose transport or
  /// deadline permits the primary to return at W enter this model as
  /// BackgroundRepair; an outer pending-response layer may still wait. This is
  /// the formal counterpart of retaining that effective mode on the pending
  /// write until an asynchronous stage receipt crosses W.
  def commitPreservesRepairMode: bool =
    if (fanout.visibleCommitted) {
      fanout.committedRepairMode == Some(fanout.repairMode)
    } else {
      fanout.committedRepairMode == None
    }

  /// Background repair is deliberately outside the W-visible response path.
  def backgroundRepairStartsAfterResponse: bool =
    all {
      fanout.repairMode == BackgroundRepair,
      fanout.repairStarted,
    } implies fanout.responsePublished

  def repairStartsOnlyAfterVisibleCommit: bool =
    fanout.repairStarted implies fanout.visibleCommitted

  /// Replica cache promotion is post-response best-effort work. It may remain
  /// pending after durability state is fully settled, but it can never consume
  /// the write response's critical path and it only targets durable stages.
  def promotionWorkStartsAfterResponse: bool =
    all {
      fanout.promotionInflight.size() > 0 or fanout.promotedReplicas.size() > 0,
    } implies fanout.responsePublished

  def promotionTargetsAreDurableStages: bool = all {
    fanout.promotionPending.subseteq(fanout.stagedReceipts),
    fanout.promotionInflight.subseteq(fanout.stagedReceipts),
    fanout.promotedReplicas.subseteq(fanout.stagedReceipts),
    fanout.promotionPending.subseteq(fanout.durableReceipts),
    fanout.promotionInflight.subseteq(fanout.durableReceipts),
    fanout.promotionRetries.subseteq(fanout.durableReceipts),
    fanout.promotedReplicas.subseteq(fanout.durableReceipts),
  }

  /// Once a visible intent has all N durable receipts there is no repair job,
  /// retry/attempt record, transfer reservation, or pending blob left. Queued
  /// cache promotion is deliberately independent and is not write-tail state.
  def fullReplicationClearsWriteTailState: bool =
    all {
      fanout.visibleCommitted,
      fanout.durableReceipts == Nodes,
    } implies all {
      fanout.stageInflight == Set(),
      fanout.repairInflight == Set(),
      fanout.repairJobs == Set(),
      fanout.stageRetries == Set(),
      fanout.repairRetries == Set(),
      fanout.repairAttempts == Set(),
      not(fanout.pendingWriteRetained),
    }

  def releasedPendingWriteHasNoRepairTail: bool =
    not(fanout.pendingWriteRetained) implies all {
      fanout.stageInflight == Set(),
      fanout.repairInflight == Set(),
      fanout.repairJobs == Set(),
      fanout.stageRetries == Set(),
      fanout.repairRetries == Set(),
      fanout.repairAttempts == Set(),
    }

  def publishedResponseHasRequiredDurability: bool =
    fanout.responsePublished implies all {
      fanout.visibleCommitted,
      responseCanPublish(fanout),
      if (fanout.repairMode == ForegroundRepair) {
        fanout.durableReceipts == Nodes
      } else {
        fanout.durableReceipts.size() >= fanout.minCommitReplicas
      },
    }

  def witnessBackgroundReservesOnlyWDeficit: bool = all {
    fanout.coverageScenario == BackgroundW2Scenario,
    fanout.repairMode == BackgroundRepair,
    fanout.minCommitReplicas == 2,
    fanout.durableReceipts == Set(N1),
    fanout.stageInflight == Set(N2),
    not(fanout.stageInflight.contains(N3)),
    not(fanout.visibleCommitted),
  }

  def witnessBackgroundPublishesAtWBeforeNRepair: bool = all {
    fanout.coverageScenario == BackgroundW2Scenario,
    fanout.repairMode == BackgroundRepair,
    fanout.durableReceipts == Set(N1, N2),
    fanout.visibleCommitted,
    fanout.committedRepairMode == Some(BackgroundRepair),
    fanout.responsePublished,
    not(fanout.repairStarted),
    missingReplicas(fanout) == Set(N3),
    fanout.promotionPending == Set(N2),
  }

  def witnessBackgroundRepairRunsAfterResponse: bool = all {
    fanout.coverageScenario == BackgroundW2Scenario,
    fanout.responsePublished,
    fanout.repairStarted,
    fanout.repairInflight == Set(N3),
    missingReplicas(fanout) == Set(N3),
  }

  def witnessForegroundFanoutUsesBothSlots: bool = all {
    fanout.repairMode == ForegroundRepair,
    fanout.stageInflight == Set(N2, N3),
    fanout.stageInflight.size() == StageFanoutCap,
  }

  def witnessForegroundWaitsForNAfterWCommit: bool = all {
    fanout.coverageScenario == ForegroundW2Scenario,
    fanout.repairMode == ForegroundRepair,
    fanout.durableReceipts == Set(N1, N2),
    fanout.stageInflight == Set(N3),
    fanout.visibleCommitted,
    fanout.committedRepairMode == Some(ForegroundRepair),
    not(fanout.responsePublished),
  }

  def witnessOutOfOrderStageCommitsWithoutCancellingSibling: bool = all {
    fanout.coverageScenario == ForegroundOutOfOrderScenario,
    fanout.durableReceipts == Set(N1, N3),
    fanout.stageInflight == Set(N2),
    fanout.visibleCommitted,
    not(fanout.responsePublished),
  }

  def witnessForegroundPublishesOnlyAtN: bool = all {
    fanout.repairMode == ForegroundRepair,
    fanout.durableReceipts == Nodes,
    fanout.visibleCommitted,
    fanout.responsePublished,
  }

  def witnessBackgroundResponsePrecedesPromotion: bool = all {
    fanout.coverageScenario == BackgroundW2Scenario,
    fanout.responsePublished,
    fanout.promotionPending == Set(N2),
    fanout.promotionInflight == Set(),
    fanout.promotedReplicas == Set(),
  }

  def witnessPromotionStartsAfterResponse: bool = all {
    fanout.coverageScenario == BackgroundW2Scenario,
    fanout.responsePublished,
    fanout.promotionInflight == Set(N2),
  }

  def witnessPromotionUsesBothSlots: bool = all {
    fanout.coverageScenario == ForegroundW2Scenario,
    fanout.responsePublished,
    fanout.promotionInflight == Set(N2, N3),
    fanout.promotionInflight.size() == PromotionFanoutCap,
  }

  def witnessRejectedPromotionRequeues: bool = all {
    fanout.coverageScenario == PromotionRetryScenario,
    fanout.responsePublished,
    fanout.promotionPending == Set(N2),
    fanout.promotionInflight == Set(),
    fanout.promotionRetries == Set(N2),
    fanout.lastTransition == PromotionRejected,
  }

  def witnessRejectedPromotionIsResent: bool = all {
    fanout.coverageScenario == PromotionRetryScenario,
    fanout.responsePublished,
    fanout.promotionPending == Set(),
    fanout.promotionInflight == Set(N2),
    fanout.promotionRetries == Set(N2),
    fanout.lastTransition == PromotionSent,
  }

  def witnessFullReplicationClearsWriteTailState: bool = all {
    fanout.visibleCommitted,
    fanout.durableReceipts == Nodes,
    fanout.stageInflight == Set(),
    fanout.repairInflight == Set(),
    fanout.repairJobs == Set(),
    fanout.stageRetries == Set(),
    fanout.repairRetries == Set(),
    fanout.repairAttempts == Set(),
    not(fanout.pendingWriteRetained),
  }

  def safety: bool = all {
    replicaSetsStayWellFormed,
    transferFanoutStaysBounded,
    backgroundPrecommitStagesStayWithinWDeficit,
    backgroundCommitHasNoStageSibling,
    visibleCommitRequiresW,
    commitPreservesRepairMode,
    backgroundRepairStartsAfterResponse,
    repairStartsOnlyAfterVisibleCommit,
    promotionWorkStartsAfterResponse,
    promotionTargetsAreDurableStages,
    fullReplicationClearsWriteTailState,
    releasedPendingWriteHasNoRepairTail,
    publishedResponseHasRequiredDurability,
  }
}
