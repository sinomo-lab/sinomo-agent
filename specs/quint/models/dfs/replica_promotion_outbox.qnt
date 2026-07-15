/// Work-conserving, restart-recoverable DFS CommitVersion promotion outbox.
///
/// The outbox is shared by all cached service lanes in one daemon generation.
/// Each drain attempts every ready identity at most once with a cap of two,
/// rotates ambiguous failures behind unattempted work, and accepts only an
/// explicit Committed acknowledgement as settlement. Durable staged receipts
/// are the restart source of truth; a dedicated recovery scan reconstructs the
/// volatile outbox without waiting for a namespace read.
module dfs_replica_promotion_outbox {
  type Promotion = P1 | P2 | P3 | P4
  type PromotionKey = K1 | K2 | K3
  type OutboxPhase = OutboxIdle | OutboxActive
  type CoverageScenario =
    | NoCoverageScenario
    | WorkConservingScenario
    | DurableAckScenario
    | RestartRecoveryScenario
    | CacheRebuildScenario
    | MaintenanceFailureScenario
    | TopologyChangeScenario
    | RetryPacingScenario
    | SettledReplacementScenario
  type OutboxTransition =
    | OutboxInitialized
    | OutboxStarted
    | PromotionDispatched
    | PromotionCommitted
    | PromotionDurableOnly
    | PromotionRejected
    | DrainRoundReset
    | VolatileStateLost
    | RecoveryQueued
    | CacheRebuilt
    | MaintenanceRepairQueued
    | UnrelatedMaintenanceFailed
    | IndependentRecoveryRan
    | AuthorityTopologyChanged
    | ConcurrentHealthyQueued
    | RetryDelayElapsed
    | SupersedingPromotionQueued
    | OutboxSchedulerIdle

  type PromotionOutboxState = {
    phase: OutboxPhase,
    coverageScenario: CoverageScenario,
    durableEvidence: Set[Promotion],
    queued: Set[Promotion],
    inflight: Set[Promotion],
    attemptedRound: Set[Promotion],
    retryPending: Set[Promotion],
    retryCooling: Set[Promotion],
    durableOnlyAcks: Set[Promotion],
    committedAcks: Set[Promotion],
    settledCurrent: Set[Promotion],
    recoveryRequired: bool,
    recoveryRuns: int,
    authorityGeneration: int,
    recoveryScanGeneration: int,
    maintenanceRepairQueued: bool,
    maintenanceFailed: bool,
    maintenanceRecoveryRan: bool,
    cacheGeneration: int,
    lastTransition: OutboxTransition,
  }

  pure val Promotions: Set[Promotion] = Set(P1, P2, P3, P4)
  pure val CoverageScenarios: Set[CoverageScenario] = Set(
    WorkConservingScenario,
    DurableAckScenario,
    RestartRecoveryScenario,
    CacheRebuildScenario,
    MaintenanceFailureScenario,
    TopologyChangeScenario,
    RetryPacingScenario,
    SettledReplacementScenario
  )
  pure val PromotionFanoutCap: int = 2

  /// P1 and P2 are successive versions for one target/filesystem/object key.
  /// P3 and P4 are independent keys.
  pure def promotionKey(promotion: Promotion): PromotionKey =
    match promotion {
      | P1 => K1
      | P2 => K1
      | P3 => K2
      | P4 => K3
    }

  pure def samePromotionKey(left: Promotion, right: Promotion): bool =
    promotionKey(left) == promotionKey(right)

  pure def initialEvidence(scenario: CoverageScenario): Set[Promotion] =
    if (scenario == WorkConservingScenario) {
      Set(P1, P3, P4)
    } else if (scenario == RestartRecoveryScenario) {
      Set(P1, P3)
    } else if (scenario == MaintenanceFailureScenario
      or scenario == TopologyChangeScenario) {
      Set()
    } else if (scenario == RetryPacingScenario) {
      Set(P1)
    } else {
      Set(P1)
    }

  pure val initialState: PromotionOutboxState = {
    phase: OutboxIdle,
    coverageScenario: NoCoverageScenario,
    durableEvidence: Set(),
    queued: Set(),
    inflight: Set(),
    attemptedRound: Set(),
    retryPending: Set(),
    retryCooling: Set(),
    durableOnlyAcks: Set(),
    committedAcks: Set(),
    settledCurrent: Set(),
    recoveryRequired: false,
    recoveryRuns: 0,
    authorityGeneration: 0,
    recoveryScanGeneration: 0,
    maintenanceRepairQueued: false,
    maintenanceFailed: false,
    maintenanceRecoveryRan: false,
    cacheGeneration: 0,
    lastTransition: OutboxInitialized,
  }

  var outbox: PromotionOutboxState

  pure def canDispatch(state: PromotionOutboxState, promotion: Promotion): bool = all {
    state.phase == OutboxActive,
    not(state.recoveryRequired),
    state.queued.contains(promotion),
    not(state.attemptedRound.contains(promotion)),
    not(state.retryCooling.contains(promotion)),
    state.inflight.size() < PromotionFanoutCap,
  }

  pure def dispatchable(state: PromotionOutboxState): Set[Promotion] =
    Promotions.filter(promotion => canDispatch(state, promotion))

  action init = outbox' = initialState

  action startCoverage(scenario: CoverageScenario): bool = {
    val evidence = initialEvidence(scenario)
    all {
      outbox.phase == OutboxIdle,
      CoverageScenarios.contains(scenario),
      outbox' = {
        phase: OutboxActive,
        coverageScenario: scenario,
        durableEvidence: evidence,
        queued: evidence,
        inflight: Set(),
        attemptedRound: Set(),
        retryPending: Set(),
        retryCooling: Set(),
        durableOnlyAcks: Set(),
        committedAcks: Set(),
        settledCurrent: Set(),
        recoveryRequired: false,
        recoveryRuns: 0,
        authorityGeneration: 1,
        recoveryScanGeneration: 1,
        maintenanceRepairQueued: false,
        maintenanceFailed: false,
        maintenanceRecoveryRan: false,
        cacheGeneration: 0,
        lastTransition: OutboxStarted,
      },
    }
  }

  action dispatch(promotion: Promotion): bool = all {
    canDispatch(outbox, promotion),
    outbox' = outbox
      .with("queued", outbox.queued.filter(queuedPromotion => queuedPromotion != promotion))
      .with("inflight", outbox.inflight.union(Set(promotion)))
      .with("attemptedRound", outbox.attemptedRound.union(Set(promotion)))
      .with("lastTransition", PromotionDispatched),
  }

  action receiveCommitted(promotion: Promotion): bool = {
    val priorForKey = outbox.settledCurrent.filter(settledPromotion =>
      samePromotionKey(settledPromotion, promotion)
    )
    all {
      outbox.phase == OutboxActive,
      outbox.inflight.contains(promotion),
      outbox' = outbox
        .with("inflight", outbox.inflight.filter(activePromotion => activePromotion != promotion))
        .with("retryPending", outbox.retryPending.filter(retryPromotion => retryPromotion != promotion))
        .with("retryCooling", outbox.retryCooling.filter(retryPromotion => retryPromotion != promotion))
        .with(
          "durableOnlyAcks",
          outbox.durableOnlyAcks.filter(durablePromotion => durablePromotion != promotion)
        )
        .with("committedAcks", outbox.committedAcks.union(Set(promotion)))
        .with(
          "settledCurrent",
          outbox.settledCurrent
            .filter(settledPromotion => not(priorForKey.contains(settledPromotion)))
            .union(Set(promotion))
        )
        .with("lastTransition", PromotionCommitted),
    }
  }

  /// Durable means the bytes are staged, not that CommitVersion made the
  /// receiver's visible cache current. It must remain retryable.
  action receiveDurableOnly(promotion: Promotion): bool = all {
    outbox.phase == OutboxActive,
    outbox.inflight.contains(promotion),
    outbox' = outbox
      .with("queued", outbox.queued.union(Set(promotion)))
      .with("inflight", outbox.inflight.filter(activePromotion => activePromotion != promotion))
      .with("retryPending", outbox.retryPending.union(Set(promotion)))
      .with("retryCooling", outbox.retryCooling.union(Set(promotion)))
      .with("durableOnlyAcks", outbox.durableOnlyAcks.union(Set(promotion)))
      .with("lastTransition", PromotionDurableOnly),
  }

  action receiveRejected(promotion: Promotion): bool = all {
    outbox.phase == OutboxActive,
    outbox.inflight.contains(promotion),
    outbox' = outbox
      .with("queued", outbox.queued.union(Set(promotion)))
      .with("inflight", outbox.inflight.filter(activePromotion => activePromotion != promotion))
      .with("retryPending", outbox.retryPending.union(Set(promotion)))
      .with("retryCooling", outbox.retryCooling.union(Set(promotion)))
      .with("lastTransition", PromotionRejected),
  }

  action retryDelayElapsed(promotion: Promotion): bool = all {
    outbox.phase == OutboxActive,
    outbox.retryCooling.contains(promotion),
    outbox.queued.contains(promotion),
    outbox' = outbox
      .with("retryCooling", outbox.retryCooling.filter(retryPromotion => retryPromotion != promotion))
      .with("lastTransition", RetryDelayElapsed),
  }

  /// A drain round cannot reset while any unattempted identity is ready. Thus
  /// failed identities rotate behind later healthy work instead of starving it.
  action resetDrainRound = all {
    outbox.phase == OutboxActive,
    not(outbox.recoveryRequired),
    outbox.inflight == Set(),
    outbox.queued.size() > 0,
    outbox.queued.subseteq(outbox.attemptedRound),
    outbox' = outbox
      .with("attemptedRound", Set())
      .with("lastTransition", DrainRoundReset),
  }

  action loseVolatileState = all {
    outbox.phase == OutboxActive,
    outbox.coverageScenario == RestartRecoveryScenario,
    outbox.inflight == Set(),
    outbox.queued.size() > 0,
    outbox' = outbox
      .with("queued", Set())
      .with("attemptedRound", Set())
      .with("retryPending", Set())
      .with("retryCooling", Set())
      .with("durableOnlyAcks", Set())
      .with("committedAcks", Set())
      .with("settledCurrent", Set())
      .with("recoveryRequired", true)
      .with("lastTransition", VolatileStateLost),
  }

  action recoverFromDurableEvidence = all {
    outbox.phase == OutboxActive,
    outbox.recoveryRequired,
    outbox.inflight == Set(),
    outbox' = outbox
      .with("queued", outbox.durableEvidence)
      .with("attemptedRound", Set())
      .with("recoveryRequired", false)
      .with("retryCooling", Set())
      .with("recoveryRuns", outbox.recoveryRuns + 1)
      .with("recoveryScanGeneration", outbox.authorityGeneration)
      .with("lastTransition", RecoveryQueued),
  }

  /// Maintenance can discover that durable receipts already constitute a
  /// committed write. The resulting promotion belongs to the daemon-shared
  /// outbox, not to the fallible maintenance service instance.
  action queuePromotionFromMaintenanceRepair = all {
    outbox.phase == OutboxActive,
    outbox.coverageScenario == MaintenanceFailureScenario,
    not(outbox.maintenanceRepairQueued),
    outbox' = outbox
      .with("durableEvidence", Set(P1))
      .with("queued", Set(P1))
      .with("maintenanceRepairQueued", true)
      .with("lastTransition", MaintenanceRepairQueued),
  }

  action failUnrelatedMaintenanceWork = all {
    outbox.phase == OutboxActive,
    outbox.coverageScenario == MaintenanceFailureScenario,
    outbox.maintenanceRepairQueued,
    not(outbox.maintenanceFailed),
    outbox' = outbox
      .with("maintenanceFailed", true)
      .with("lastTransition", UnrelatedMaintenanceFailed),
  }

  /// Promotion recovery is a sibling of broad maintenance, not a child of its
  /// success path. A broad error therefore cannot suppress the shared drain.
  action recoverAfterMaintenanceFailure = all {
    outbox.phase == OutboxActive,
    outbox.coverageScenario == MaintenanceFailureScenario,
    outbox.maintenanceFailed,
    not(outbox.maintenanceRecoveryRan),
    outbox' = outbox
      .with("maintenanceRecoveryRan", true)
      .with("recoveryRuns", outbox.recoveryRuns + 1)
      .with("lastTransition", IndependentRecoveryRan),
  }

  /// The authority observer is process-retained. Its generation therefore
  /// advances from the previously scanned token instead of being recreated at
  /// the same numeric value on every no-read maintenance tick.
  action observeAuthorityTopologyChange = all {
    outbox.phase == OutboxActive,
    outbox.coverageScenario == TopologyChangeScenario,
    outbox.authorityGeneration == outbox.recoveryScanGeneration,
    outbox.durableEvidence == Set(),
    outbox' = outbox
      .with("durableEvidence", Set(P3))
      .with("authorityGeneration", outbox.authorityGeneration + 1)
      .with("recoveryRequired", true)
      .with("lastTransition", AuthorityTopologyChanged),
  }

  /// A newly queued healthy identity is immediately ready even while an older
  /// failure remains in its retry cooling interval.
  action queueConcurrentHealthyPromotion = all {
    outbox.phase == OutboxActive,
    outbox.coverageScenario == RetryPacingScenario,
    outbox.retryCooling.contains(P1),
    not(outbox.durableEvidence.contains(P3)),
    outbox' = outbox
      .with("durableEvidence", outbox.durableEvidence.union(Set(P3)))
      .with("queued", outbox.queued.union(Set(P3)))
      .with("lastTransition", ConcurrentHealthyQueued),
  }

  /// All cache lanes share this outbox state. Rebuilding routing capabilities
  /// changes only the generation, so the exact queued identities survive.
  action rebuildCache = all {
    outbox.phase == OutboxActive,
    outbox.coverageScenario == CacheRebuildScenario,
    outbox.queued.size() > 0,
    outbox' = outbox
      .with("cacheGeneration", outbox.cacheGeneration + 1)
      .with("lastTransition", CacheRebuilt),
  }

  /// Bounded settled bookkeeping retains only the current version for one
  /// target/filesystem/object key. P2 supersedes P1 for K1.
  action enqueueSupersedingPromotion = {
    val priorForKey = outbox.durableEvidence.filter(evidencePromotion =>
      samePromotionKey(evidencePromotion, P2)
    )
    all {
      outbox.phase == OutboxActive,
      outbox.coverageScenario == SettledReplacementScenario,
      outbox.settledCurrent.contains(P1),
      not(outbox.durableEvidence.contains(P2)),
      outbox.inflight == Set(),
      outbox' = outbox
        .with(
          "durableEvidence",
          outbox.durableEvidence
            .filter(evidencePromotion => not(priorForKey.contains(evidencePromotion)))
            .union(Set(P2))
        )
        .with("queued", Set(P2))
        .with("attemptedRound", Set())
        .with("retryPending", Set())
        .with("retryCooling", Set())
        .with("durableOnlyAcks", Set())
        .with("committedAcks", Set())
        .with("settledCurrent", Set())
        .with("lastTransition", SupersedingPromotionQueued),
    }
  }

  action schedulerIdle = all {
    outbox.phase == OutboxActive,
    dispatchable(outbox) == Set(),
    outbox.inflight == Set(),
    outbox' = outbox.with("lastTransition", OutboxSchedulerIdle),
  }

  action step = any {
    {
      nondet scenario = oneOf(CoverageScenarios)
      startCoverage(scenario)
    },
    {
      nondet promotion = oneOf(Promotions)
      any {
        dispatch(promotion),
        receiveCommitted(promotion),
        receiveDurableOnly(promotion),
        receiveRejected(promotion),
        retryDelayElapsed(promotion),
      }
    },
    resetDrainRound,
    loseVolatileState,
    recoverFromDurableEvidence,
    queuePromotionFromMaintenanceRepair,
    failUnrelatedMaintenanceWork,
    recoverAfterMaintenanceFailure,
    observeAuthorityTopologyChange,
    queueConcurrentHealthyPromotion,
    rebuildCache,
    enqueueSupersedingPromotion,
    schedulerIdle,
  }

  action workConservingCoverageStep =
    if (outbox.phase == OutboxIdle) {
      startCoverage(WorkConservingScenario)
    } else if (not(outbox.attemptedRound.contains(P1))) {
      dispatch(P1)
    } else if (not(outbox.attemptedRound.contains(P3))) {
      dispatch(P3)
    } else if (outbox.inflight.contains(P1)) {
      receiveRejected(P1)
    } else if (outbox.inflight.contains(P3)) {
      receiveRejected(P3)
    } else if (not(outbox.attemptedRound.contains(P4))) {
      dispatch(P4)
    } else if (outbox.inflight.contains(P4)) {
      receiveCommitted(P4)
    } else if (outbox.attemptedRound.size() > 0) {
      resetDrainRound
    } else {
      schedulerIdle
    }

  action durableAckCoverageStep =
    if (outbox.phase == OutboxIdle) {
      startCoverage(DurableAckScenario)
    } else if (outbox.settledCurrent.contains(P1)) {
      schedulerIdle
    } else if (outbox.inflight.contains(P1)
      and outbox.durableOnlyAcks.contains(P1)) {
      receiveCommitted(P1)
    } else if (outbox.durableOnlyAcks.contains(P1)
      and outbox.attemptedRound.size() > 0) {
      resetDrainRound
    } else if (outbox.retryCooling.contains(P1)) {
      retryDelayElapsed(P1)
    } else if (outbox.inflight.contains(P1)) {
      receiveDurableOnly(P1)
    } else {
      dispatch(P1)
    }

  action restartRecoveryCoverageStep =
    if (outbox.phase == OutboxIdle) {
      startCoverage(RestartRecoveryScenario)
    } else if (outbox.recoveryRequired) {
      recoverFromDurableEvidence
    } else if (outbox.recoveryRuns == 0) {
      loseVolatileState
    } else if (outbox.inflight.contains(P1)) {
      receiveCommitted(P1)
    } else if (not(outbox.settledCurrent.contains(P1))) {
      dispatch(P1)
    } else {
      schedulerIdle
    }

  action cacheRebuildCoverageStep =
    if (outbox.phase == OutboxIdle) {
      startCoverage(CacheRebuildScenario)
    } else if (outbox.cacheGeneration == 0) {
      rebuildCache
    } else if (outbox.inflight.contains(P1)) {
      receiveCommitted(P1)
    } else if (not(outbox.settledCurrent.contains(P1))) {
      dispatch(P1)
    } else {
      schedulerIdle
    }

  action maintenanceFailureCoverageStep =
    if (outbox.phase == OutboxIdle) {
      startCoverage(MaintenanceFailureScenario)
    } else if (not(outbox.maintenanceRepairQueued)) {
      queuePromotionFromMaintenanceRepair
    } else if (not(outbox.maintenanceFailed)) {
      failUnrelatedMaintenanceWork
    } else if (not(outbox.maintenanceRecoveryRan)) {
      recoverAfterMaintenanceFailure
    } else if (outbox.inflight.contains(P1)) {
      receiveCommitted(P1)
    } else if (outbox.queued.contains(P1)) {
      dispatch(P1)
    } else {
      schedulerIdle
    }

  action topologyChangeCoverageStep =
    if (outbox.phase == OutboxIdle) {
      startCoverage(TopologyChangeScenario)
    } else if (outbox.authorityGeneration == 1) {
      observeAuthorityTopologyChange
    } else if (outbox.recoveryRequired) {
      recoverFromDurableEvidence
    } else if (outbox.inflight.contains(P3)) {
      receiveCommitted(P3)
    } else if (outbox.queued.contains(P3)) {
      dispatch(P3)
    } else {
      schedulerIdle
    }

  action retryPacingCoverageStep =
    if (outbox.phase == OutboxIdle) {
      startCoverage(RetryPacingScenario)
    } else if (outbox.inflight.contains(P1)
      and outbox.settledCurrent.contains(P3)) {
      receiveCommitted(P1)
    } else if (outbox.inflight.contains(P1)) {
      receiveRejected(P1)
    } else if (outbox.retryCooling.contains(P1)
      and not(outbox.durableEvidence.contains(P3))) {
      queueConcurrentHealthyPromotion
    } else if (outbox.inflight.contains(P3)) {
      receiveCommitted(P3)
    } else if (outbox.queued.contains(P3)) {
      dispatch(P3)
    } else if (outbox.retryCooling.contains(P1)
      and outbox.attemptedRound.size() > 0) {
      resetDrainRound
    } else if (outbox.retryCooling.contains(P1)) {
      retryDelayElapsed(P1)
    } else if (outbox.queued.contains(P1)) {
      dispatch(P1)
    } else {
      schedulerIdle
    }

  action settledReplacementCoverageStep =
    if (outbox.phase == OutboxIdle) {
      startCoverage(SettledReplacementScenario)
    } else if (outbox.inflight.contains(P1)) {
      receiveCommitted(P1)
    } else if (outbox.queued.contains(P1)) {
      dispatch(P1)
    } else if (outbox.settledCurrent.contains(P1)) {
      enqueueSupersedingPromotion
    } else if (outbox.inflight.contains(P2)) {
      receiveCommitted(P2)
    } else if (outbox.queued.contains(P2)) {
      dispatch(P2)
    } else {
      schedulerIdle
    }

  action coverageStep =
    if (outbox.phase == OutboxIdle) {
      nondet scenario = oneOf(CoverageScenarios)
      startCoverage(scenario)
    } else if (outbox.coverageScenario == WorkConservingScenario) {
      workConservingCoverageStep
    } else if (outbox.coverageScenario == DurableAckScenario) {
      durableAckCoverageStep
    } else if (outbox.coverageScenario == RestartRecoveryScenario) {
      restartRecoveryCoverageStep
    } else if (outbox.coverageScenario == CacheRebuildScenario) {
      cacheRebuildCoverageStep
    } else if (outbox.coverageScenario == MaintenanceFailureScenario) {
      maintenanceFailureCoverageStep
    } else if (outbox.coverageScenario == TopologyChangeScenario) {
      topologyChangeCoverageStep
    } else if (outbox.coverageScenario == RetryPacingScenario) {
      retryPacingCoverageStep
    } else if (outbox.coverageScenario == SettledReplacementScenario) {
      settledReplacementCoverageStep
    } else {
      schedulerIdle
    }
}
