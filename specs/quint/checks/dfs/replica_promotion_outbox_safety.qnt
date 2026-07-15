/// Safety and bounded witnesses for the DFS CommitVersion promotion outbox.
module dfs_replica_promotion_outbox_safety {
  import dfs_replica_promotion_outbox.* from "../../models/dfs/replica_promotion_outbox"

  def identitiesStayWellFormed: bool = all {
    outbox.durableEvidence.subseteq(Promotions),
    outbox.queued.subseteq(outbox.durableEvidence),
    outbox.inflight.subseteq(outbox.durableEvidence),
    outbox.attemptedRound.subseteq(outbox.durableEvidence),
    outbox.retryPending.subseteq(outbox.durableEvidence),
    outbox.retryCooling.subseteq(outbox.retryPending),
    outbox.retryCooling.subseteq(outbox.queued),
    outbox.durableOnlyAcks.subseteq(outbox.durableEvidence),
    outbox.committedAcks.subseteq(outbox.durableEvidence),
    outbox.settledCurrent.subseteq(outbox.durableEvidence),
    outbox.retiredNotDesired.subseteq(outbox.durableEvidence),
    outbox.repairPending.subseteq(outbox.durableEvidence),
    outbox.queued.intersect(outbox.inflight) == Set(),
    outbox.queued.intersect(outbox.settledCurrent) == Set(),
    outbox.queued.intersect(outbox.retiredNotDesired) == Set(),
    outbox.queued.intersect(outbox.repairPending) == Set(),
    outbox.inflight.intersect(outbox.settledCurrent) == Set(),
    outbox.inflight.intersect(outbox.retiredNotDesired) == Set(),
    outbox.inflight.intersect(outbox.repairPending) == Set(),
    outbox.settledCurrent.intersect(outbox.retiredNotDesired) == Set(),
    outbox.settledCurrent.intersect(outbox.repairPending) == Set(),
    outbox.retiredNotDesired.intersect(outbox.repairPending) == Set(),
    outbox.inflight.subseteq(outbox.attemptedRound),
    outbox.retryPending.subseteq(outbox.queued.union(outbox.inflight)),
    outbox.durableOnlyAcks.subseteq(outbox.retryPending),
    outbox.settledCurrent.subseteq(outbox.committedAcks),
    outbox.durableOnlyAcks.intersect(outbox.settledCurrent) == Set(),
    outbox.retiredNotDesired.intersect(outbox.committedAcks) == Set(),
    outbox.repairPending.intersect(outbox.committedAcks) == Set(),
    if (outbox.recoveryRequired) {
      all {
        outbox.queued == Set(),
        outbox.inflight == Set(),
        outbox.settledCurrent == Set(),
        outbox.retiredNotDesired == Set(),
        outbox.repairPending == Set(),
      }
    } else {
      outbox.queued
        .union(outbox.inflight)
        .union(outbox.settledCurrent)
        .union(outbox.retiredNotDesired)
        .union(outbox.repairPending) == outbox.durableEvidence
    },
  }

  def promotionFanoutStaysBounded: bool =
    outbox.inflight.size() <= PromotionFanoutCap

  /// A ready identity cannot be hidden behind failures attempted earlier in
  /// the drain round. The scheduler may wait only when the cap is occupied or
  /// every remaining identity has already been attempted once.
  def readyWorkRemainsDispatchable: bool =
    all {
      not(outbox.recoveryRequired),
      outbox.inflight.size() < PromotionFanoutCap,
      outbox.queued.filter(queuedPromotion =>
        not(outbox.attemptedRound.contains(queuedPromotion))
          and not(outbox.retryCooling.contains(queuedPromotion))
      ).size() > 0,
    } implies dispatchable(outbox).size() > 0

  def schedulerIdlesOnlyWithoutReadyWork: bool =
    outbox.lastTransition == OutboxSchedulerIdle implies all {
      dispatchable(outbox) == Set(),
      outbox.inflight == Set(),
    }

  def durableOnlyAckNeverSettles: bool =
    outbox.durableOnlyAcks.intersect(outbox.settledCurrent) == Set()

  def coolingRetryNeverDispatches: bool =
    outbox.retryCooling.intersect(dispatchable(outbox)) == Set()

  def terminalBookkeepingHasOneVersionPerKey: bool =
    Promotions.forall(promotion =>
      outbox.settledCurrent
        .union(outbox.retiredNotDesired)
        .union(outbox.repairPending)
        .filter(terminalPromotion =>
          samePromotionKey(terminalPromotion, promotion)
        ).size() <= 1
    )

  /// Repeated daemon restarts may require repeated idempotent reconstruction;
  /// the counter is monotonic and never underflows.
  def recoveryRunsStayMonotonicDomain: bool = all {
    outbox.recoveryRuns >= 0,
    outbox.recoveryScanGeneration >= 0,
    outbox.recoveryScanGeneration <= outbox.authorityGeneration,
  }

  def witnessFailedWaveDoesNotStarveHealthyWork: bool = all {
    outbox.coverageScenario == WorkConservingScenario,
    outbox.retryPending == Set(P1, P3),
    outbox.queued == Set(P1, P3),
    outbox.settledCurrent == Set(P4),
    outbox.attemptedRound == Set(P1, P3, P4),
    outbox.lastTransition == PromotionCommitted,
  }

  def witnessDurableAckRequeues: bool = all {
    outbox.coverageScenario == DurableAckScenario,
    outbox.queued == Set(P1),
    outbox.retryPending == Set(P1),
    outbox.durableOnlyAcks == Set(P1),
    outbox.settledCurrent == Set(),
    outbox.lastTransition == PromotionDurableOnly,
  }

  def witnessDurableAckRetriesUntilCommitted: bool = all {
    outbox.coverageScenario == DurableAckScenario,
    outbox.retryPending == Set(),
    outbox.durableOnlyAcks == Set(),
    outbox.committedAcks == Set(P1),
    outbox.settledCurrent == Set(P1),
    outbox.lastTransition == PromotionCommitted,
  }

  def witnessRestartExposesRecoveryNeed: bool = all {
    outbox.coverageScenario == RestartRecoveryScenario,
    outbox.recoveryRequired,
    outbox.queued == Set(),
    outbox.durableEvidence == Set(P1, P3),
    outbox.lastTransition == VolatileStateLost,
  }

  def witnessRecoveryReconstructsWithoutRead: bool = all {
    outbox.coverageScenario == RestartRecoveryScenario,
    not(outbox.recoveryRequired),
    outbox.recoveryRuns == 1,
    outbox.queued == Set(P1, P3),
    outbox.lastTransition == RecoveryQueued,
  }

  def witnessRecoveredPromotionCanSettle: bool = all {
    outbox.coverageScenario == RestartRecoveryScenario,
    outbox.recoveryRuns == 1,
    outbox.settledCurrent.contains(P1),
  }

  def witnessCacheRebuildPreservesOutbox: bool = all {
    outbox.coverageScenario == CacheRebuildScenario,
    outbox.cacheGeneration == 1,
    outbox.queued == Set(P1),
    outbox.lastTransition == CacheRebuilt,
  }

  def witnessCacheRebuildWorkCanSettle: bool = all {
    outbox.coverageScenario == CacheRebuildScenario,
    outbox.cacheGeneration == 1,
    outbox.settledCurrent == Set(P1),
  }

  def witnessMaintenanceFailurePreservesSharedPromotion: bool = all {
    outbox.coverageScenario == MaintenanceFailureScenario,
    outbox.maintenanceFailed,
    not(outbox.maintenanceRecoveryRan),
    outbox.queued == Set(P1),
    outbox.lastTransition == UnrelatedMaintenanceFailed,
  }

  def witnessIndependentRecoveryRunsAfterMaintenanceFailure: bool = all {
    outbox.coverageScenario == MaintenanceFailureScenario,
    outbox.maintenanceFailed,
    outbox.maintenanceRecoveryRan,
    outbox.recoveryRuns == 1,
    outbox.queued == Set(P1),
    outbox.lastTransition == IndependentRecoveryRan,
  }

  def witnessPromotionSettlesAfterMaintenanceFailure: bool = all {
    outbox.coverageScenario == MaintenanceFailureScenario,
    outbox.maintenanceFailed,
    outbox.maintenanceRecoveryRan,
    outbox.settledCurrent == Set(P1),
  }

  def witnessTopologyChangeForcesRecoveryScan: bool = all {
    outbox.coverageScenario == TopologyChangeScenario,
    outbox.authorityGeneration == 2,
    outbox.recoveryScanGeneration == 1,
    outbox.recoveryRequired,
    outbox.lastTransition == AuthorityTopologyChanged,
  }

  def witnessRetainedObserverAdvancesRecoveryToken: bool = all {
    outbox.coverageScenario == TopologyChangeScenario,
    outbox.authorityGeneration == 2,
    outbox.recoveryScanGeneration == 2,
    not(outbox.recoveryRequired),
    outbox.recoveryRuns == 1,
    outbox.queued == Set(P3),
    outbox.lastTransition == RecoveryQueued,
  }

  def witnessTopologyRecoveryCanSettle: bool = all {
    outbox.coverageScenario == TopologyChangeScenario,
    outbox.authorityGeneration == 2,
    outbox.recoveryScanGeneration == 2,
    outbox.settledCurrent == Set(P3),
  }

  def witnessHealthyWorkBypassesCoolingRetry: bool = all {
    outbox.coverageScenario == RetryPacingScenario,
    outbox.retryCooling == Set(P1),
    outbox.queued == Set(P1),
    outbox.settledCurrent == Set(P3),
    outbox.lastTransition == PromotionCommitted,
  }

  def witnessRetryDelayOpensOnlyAfterHealthyWork: bool = all {
    outbox.coverageScenario == RetryPacingScenario,
    outbox.retryCooling == Set(),
    outbox.queued == Set(P1),
    outbox.settledCurrent == Set(P3),
    outbox.lastTransition == RetryDelayElapsed,
  }

  def witnessCooledRetryCanEventuallySettle: bool = all {
    outbox.coverageScenario == RetryPacingScenario,
    outbox.retryCooling == Set(),
    outbox.queued == Set(),
    outbox.settledCurrent == Set(P1, P3),
  }

  def witnessNotDesiredRejectionRetiresWithoutCommit: bool = all {
    outbox.coverageScenario == NotDesiredRetirementScenario,
    outbox.queued == Set(),
    outbox.inflight == Set(),
    outbox.retryPending == Set(),
    outbox.retryCooling == Set(),
    outbox.committedAcks == Set(),
    outbox.settledCurrent == Set(),
    outbox.retiredNotDesired == Set(P1),
    outbox.lastTransition == PromotionRetiredNotDesired,
  }

  def witnessRepairPendingRejectionRetiresAfterDurablePrune: bool = all {
    outbox.coverageScenario == RepairPendingRetirementScenario,
    outbox.queued == Set(),
    outbox.inflight == Set(),
    outbox.retryPending == Set(),
    outbox.retryCooling == Set(),
    outbox.committedAcks == Set(),
    outbox.settledCurrent == Set(),
    outbox.retiredNotDesired == Set(),
    outbox.repairPending == Set(P1),
    outbox.lastTransition == PromotionRepairPending,
  }

  def witnessSettledVersionIsReplaced: bool = all {
    outbox.coverageScenario == SettledReplacementScenario,
    outbox.durableEvidence == Set(P2),
    outbox.committedAcks == Set(P2),
    outbox.settledCurrent == Set(P2),
    not(outbox.settledCurrent.contains(P1)),
  }

  def safety: bool = all {
    identitiesStayWellFormed,
    promotionFanoutStaysBounded,
    readyWorkRemainsDispatchable,
    schedulerIdlesOnlyWithoutReadyWork,
    durableOnlyAckNeverSettles,
    coolingRetryNeverDispatches,
    terminalBookkeepingHasOneVersionPerKey,
    recoveryRunsStayMonotonicDomain,
  }
}
