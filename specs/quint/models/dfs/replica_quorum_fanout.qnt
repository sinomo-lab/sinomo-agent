/// Work-conserving DFS replica fan-out around the W-visible write boundary.
///
/// This bounded slice separates two policies that the implementation must not
/// conflate:
/// - background repair reserves only the missing W quorum slots before commit,
///   publishes the response as soon as W is durable, and starts N-W repair only
///   after that response boundary;
/// - foreground repair may fill every desired replica before returning, while
///   both stage and repair transfer fan-out stay bounded.
///
/// Per-target content/request validation is proved by
/// `dfs_repair_content_validation`; this module focuses on scheduling, quorum
/// visibility, and preservation of the caller's repair mode across an
/// asynchronous stage response. Replica cache promotion is modeled as
/// independent post-response work: a durable staged copy contributes to W/N,
/// while its best-effort CommitVersion notification never gates the caller.
module dfs_replica_quorum_fanout {
  import common.* from "../../lib/common"

  /// Effective execution mode retained by the primary after the transport and
  /// deadline policy have decided whether this primary invocation may return at
  /// W. A caller can request foreground repair yet be converted to
  /// BackgroundRepair here when the outer pending-response layer owns any
  /// additional wait. ForegroundRepair therefore means that this primary
  /// invocation itself must wait for all N desired replicas.
  type RepairMode = BackgroundRepair | ForegroundRepair
  type FanoutPhase = FanoutIdle | FanoutActive
  type CoverageScenario =
    | NoCoverageScenario
    | BackgroundW2Scenario
    | ForegroundW2Scenario
    | ForegroundOutOfOrderScenario
    | PromotionRetryScenario
  type FanoutTransition =
    | FanoutInitialized
    | IntentBegan
    | InitialReceiptRecorded
    | StageSent
    | StageStored
    | StageRejected
    | VisibleCommitPublished
    | WriteResponsePublished
    | RepairSent
    | RepairStored
    | RepairRejected
    | PromotionSent
    | PromotionApplied
    | PromotionRejected
    | FanoutSchedulerIdle

  type ReplicaQuorumFanoutState = {
    phase: FanoutPhase,
    coverageScenario: CoverageScenario,
    repairMode: RepairMode,
    minCommitReplicas: int,
    durableReceipts: Set[Node],
    stagedReceipts: Set[Node],
    stageInflight: Set[Node],
    repairInflight: Set[Node],
    promotionPending: Set[Node],
    promotionInflight: Set[Node],
    promotionRetries: Set[Node],
    promotedReplicas: Set[Node],
    repairJobs: Set[Node],
    stageRetries: Set[Node],
    repairRetries: Set[Node],
    repairAttempts: Set[Node],
    pendingWriteRetained: bool,
    visibleCommitted: bool,
    committedRepairMode: Option[RepairMode],
    responsePublished: bool,
    repairStarted: bool,
    lastTransition: FanoutTransition,
  }

  pure val StageFanoutCap: int = 2
  pure val RepairFanoutCap: int = 2
  pure val PromotionFanoutCap: int = 2
  pure val CommitThresholds: Set[int] = Set(1, 2, 3)
  pure val CoverageScenarios: Set[CoverageScenario] = Set(
    BackgroundW2Scenario,
    ForegroundW2Scenario,
    ForegroundOutOfOrderScenario,
    PromotionRetryScenario
  )

  pure val initialState: ReplicaQuorumFanoutState = {
    phase: FanoutIdle,
    coverageScenario: NoCoverageScenario,
    repairMode: BackgroundRepair,
    minCommitReplicas: 0,
    durableReceipts: Set(),
    stagedReceipts: Set(),
    stageInflight: Set(),
    repairInflight: Set(),
    promotionPending: Set(),
    promotionInflight: Set(),
    promotionRetries: Set(),
    promotedReplicas: Set(),
    repairJobs: Set(),
    stageRetries: Set(),
    repairRetries: Set(),
    repairAttempts: Set(),
    pendingWriteRetained: false,
    visibleCommitted: false,
    committedRepairMode: None,
    responsePublished: false,
    repairStarted: false,
    lastTransition: FanoutInitialized,
  }

  var fanout: ReplicaQuorumFanoutState

  pure def missingReplicas(state: ReplicaQuorumFanoutState): Set[Node] =
    Nodes.filter(node => not(state.durableReceipts.contains(node)))

  pure def responseCanPublish(state: ReplicaQuorumFanoutState): bool = all {
    state.visibleCommitted,
    any {
      state.repairMode == BackgroundRepair,
      all {
        state.repairMode == ForegroundRepair,
        state.durableReceipts == Nodes,
      },
    },
  }

  pure def canSendStage(state: ReplicaQuorumFanoutState, replica: Node): bool = all {
    state.phase == FanoutActive,
    not(state.visibleCommitted),
    not(state.durableReceipts.contains(replica)),
    not(state.stageInflight.contains(replica)),
    not(state.repairInflight.contains(replica)),
    state.stageInflight.size() < StageFanoutCap,
    any {
      state.repairMode == ForegroundRepair,
      all {
        state.repairMode == BackgroundRepair,
        state.durableReceipts.size() + state.stageInflight.size()
          < state.minCommitReplicas,
      },
    },
  }

  pure def canSendRepair(state: ReplicaQuorumFanoutState, replica: Node): bool = all {
    state.phase == FanoutActive,
    state.visibleCommitted,
    not(state.durableReceipts.contains(replica)),
    not(state.stageInflight.contains(replica)),
    not(state.repairInflight.contains(replica)),
    state.repairInflight.size() < RepairFanoutCap,
    state.repairMode == ForegroundRepair or state.responsePublished,
  }

  /// CommitVersion is a cache-promotion notification, not a durability ack.
  /// It is eligible only after the write response so its RPC cannot occupy the
  /// W-visible hot path. Every target has already returned a durable stage
  /// receipt and independent promotions use the same bounded work discipline.
  pure def canSendPromotion(state: ReplicaQuorumFanoutState, replica: Node): bool = all {
    state.phase == FanoutActive,
    state.responsePublished,
    state.promotionPending.contains(replica),
    not(state.promotionInflight.contains(replica)),
    state.promotionInflight.size() < PromotionFanoutCap,
  }

  action init = fanout' = initialState

  action beginIntent(repairMode: RepairMode, minCommitReplicas: int): bool = all {
    fanout.phase == FanoutIdle,
    CommitThresholds.contains(minCommitReplicas),
    fanout' = {
      phase: FanoutActive,
      coverageScenario: NoCoverageScenario,
      repairMode: repairMode,
      minCommitReplicas: minCommitReplicas,
      durableReceipts: Set(),
      stagedReceipts: Set(),
      stageInflight: Set(),
      repairInflight: Set(),
      promotionPending: Set(),
      promotionInflight: Set(),
      promotionRetries: Set(),
      promotedReplicas: Set(),
      repairJobs: Set(),
      stageRetries: Set(),
      repairRetries: Set(),
      repairAttempts: Set(),
      pendingWriteRetained: true,
      visibleCommitted: false,
      committedRepairMode: None,
      responsePublished: false,
      repairStarted: false,
      lastTransition: IntentBegan,
    },
  }

  action beginCoverageIntent(scenario: CoverageScenario): bool = all {
    fanout.phase == FanoutIdle,
    CoverageScenarios.contains(scenario),
    fanout' = {
      phase: FanoutActive,
      coverageScenario: scenario,
      repairMode:
        if (scenario == BackgroundW2Scenario or scenario == PromotionRetryScenario) {
          BackgroundRepair
        } else {
          ForegroundRepair
        },
      minCommitReplicas: 2,
      durableReceipts: Set(),
      stagedReceipts: Set(),
      stageInflight: Set(),
      repairInflight: Set(),
      promotionPending: Set(),
      promotionInflight: Set(),
      promotionRetries: Set(),
      promotedReplicas: Set(),
      repairJobs: Set(),
      stageRetries: Set(),
      repairRetries: Set(),
      repairAttempts: Set(),
      pendingWriteRetained: true,
      visibleCommitted: false,
      committedRepairMode: None,
      responsePublished: false,
      repairStarted: false,
      lastTransition: IntentBegan,
    },
  }

  /// The bounded implementation records local/requester durability before it
  /// reserves remote stage slots. Preventing a later synthetic initial receipt
  /// keeps the model aligned with that ordering and preserves the W-deficit
  /// reservation invariant.
  action recordInitialReceipt(replica: Node): bool = all {
    fanout.phase == FanoutActive,
    not(fanout.visibleCommitted),
    fanout.durableReceipts.size() == 0,
    fanout.stageInflight.size() == 0,
    fanout.repairInflight.size() == 0,
    fanout' = fanout
      .with("durableReceipts", Set(replica))
      .with("lastTransition", InitialReceiptRecorded),
  }

  action sendStage(replica: Node): bool = all {
    canSendStage(fanout, replica),
    fanout' = fanout
      .with("stageInflight", fanout.stageInflight.union(Set(replica)))
      .with("stageRetries", fanout.stageRetries.union(Set(replica)))
      .with("lastTransition", StageSent),
  }

  action receiveStageStored(replica: Node): bool = {
    val durableReceipts = fanout.durableReceipts.union(Set(replica))
    val fullyReplicated = fanout.visibleCommitted and durableReceipts == Nodes
    all {
      fanout.phase == FanoutActive,
      fanout.stageInflight.contains(replica),
      fanout' = fanout
        .with("durableReceipts", durableReceipts)
        .with("stagedReceipts", fanout.stagedReceipts.union(Set(replica)))
        .with("stageInflight", fanout.stageInflight.filter(node => node != replica))
        .with(
          "promotionPending",
          if (fanout.visibleCommitted) fanout.promotionPending.union(Set(replica))
          else fanout.promotionPending
        )
        .with("repairJobs", fanout.repairJobs.filter(node => node != replica))
        .with(
          "stageRetries",
          if (fullyReplicated) Set()
          else fanout.stageRetries.filter(node => node != replica)
        )
        .with(
          "repairRetries",
          if (fullyReplicated) Set()
          else fanout.repairRetries.filter(node => node != replica)
        )
        .with(
          "repairAttempts",
          if (fullyReplicated) Set()
          else fanout.repairAttempts.filter(node => node != replica)
        )
        .with("pendingWriteRetained", not(fullyReplicated))
        .with("lastTransition", StageStored),
    }
  }

  action receiveStageRejected(replica: Node): bool = all {
    fanout.phase == FanoutActive,
    fanout.stageInflight.contains(replica),
    fanout' = fanout
      .with("stageInflight", fanout.stageInflight.filter(node => node != replica))
      .with("lastTransition", StageRejected),
  }

  /// The exact effective primary mode is copied at the W-visible commit. A
  /// later stage response cannot silently turn a W-returning primary invocation
  /// into an N-waiting invocation.
  action publishVisibleCommit = {
    val repairJobs = missingReplicas(fanout)
    val fullyReplicated = fanout.durableReceipts == Nodes
    all {
      fanout.phase == FanoutActive,
      not(fanout.visibleCommitted),
      fanout.durableReceipts.size() >= fanout.minCommitReplicas,
      fanout' = fanout
        .with("visibleCommitted", true)
        .with("committedRepairMode", Some(fanout.repairMode))
        .with("promotionPending", fanout.stagedReceipts)
        .with("repairJobs", repairJobs)
        .with("stageRetries", if (fullyReplicated) Set() else fanout.stageRetries)
        .with("repairRetries", if (fullyReplicated) Set() else fanout.repairRetries)
        .with("repairAttempts", if (fullyReplicated) Set() else fanout.repairAttempts)
        .with("pendingWriteRetained", not(fullyReplicated))
        .with("lastTransition", VisibleCommitPublished),
    }
  }

  action publishWriteResponse = all {
    fanout.phase == FanoutActive,
    not(fanout.responsePublished),
    responseCanPublish(fanout),
    fanout' = fanout
      .with("responsePublished", true)
      .with("lastTransition", WriteResponsePublished),
  }

  action sendRepair(replica: Node): bool = all {
    canSendRepair(fanout, replica),
    fanout' = fanout
      .with("repairInflight", fanout.repairInflight.union(Set(replica)))
      .with("repairRetries", fanout.repairRetries.union(Set(replica)))
      .with("repairAttempts", fanout.repairAttempts.union(Set(replica)))
      .with("repairStarted", true)
      .with("lastTransition", RepairSent),
  }

  action receiveRepairStored(replica: Node): bool = {
    val durableReceipts = fanout.durableReceipts.union(Set(replica))
    val fullyReplicated = durableReceipts == Nodes
    all {
      fanout.phase == FanoutActive,
      fanout.repairInflight.contains(replica),
      fanout' = fanout
        .with("durableReceipts", durableReceipts)
        .with("repairInflight", fanout.repairInflight.filter(node => node != replica))
        .with("repairJobs", fanout.repairJobs.filter(node => node != replica))
        .with("stageRetries", if (fullyReplicated) Set() else fanout.stageRetries)
        .with(
          "repairRetries",
          if (fullyReplicated) Set()
          else fanout.repairRetries.filter(node => node != replica)
        )
        .with(
          "repairAttempts",
          if (fullyReplicated) Set()
          else fanout.repairAttempts.filter(node => node != replica)
        )
        .with("pendingWriteRetained", not(fullyReplicated))
        .with("lastTransition", RepairStored),
    }
  }

  action receiveRepairRejected(replica: Node): bool = all {
    fanout.phase == FanoutActive,
    fanout.repairInflight.contains(replica),
    fanout' = fanout
      .with("repairInflight", fanout.repairInflight.filter(node => node != replica))
      .with("lastTransition", RepairRejected),
  }

  action sendPromotion(replica: Node): bool = all {
    canSendPromotion(fanout, replica),
    fanout' = fanout
      .with("promotionPending", fanout.promotionPending.filter(node => node != replica))
      .with("promotionInflight", fanout.promotionInflight.union(Set(replica)))
      .with("lastTransition", PromotionSent),
  }

  action receivePromotionApplied(replica: Node): bool = all {
    fanout.phase == FanoutActive,
    fanout.promotionInflight.contains(replica),
    fanout' = fanout
      .with("promotionInflight", fanout.promotionInflight.filter(node => node != replica))
      .with("promotionRetries", fanout.promotionRetries.filter(node => node != replica))
      .with("promotedReplicas", fanout.promotedReplicas.union(Set(replica)))
      .with("lastTransition", PromotionApplied),
  }

  action receivePromotionRejected(replica: Node): bool = all {
    fanout.phase == FanoutActive,
    fanout.promotionInflight.contains(replica),
    fanout' = fanout
      .with("promotionPending", fanout.promotionPending.union(Set(replica)))
      .with("promotionInflight", fanout.promotionInflight.filter(node => node != replica))
      .with("promotionRetries", fanout.promotionRetries.union(Set(replica)))
      .with("lastTransition", PromotionRejected),
  }

  action schedulerIdle = all {
    fanout.phase == FanoutActive,
    Nodes.filter(node => canSendStage(fanout, node)).size() == 0,
    Nodes.filter(node => canSendRepair(fanout, node)).size() == 0,
    Nodes.filter(node => canSendPromotion(fanout, node)).size() == 0,
    fanout' = fanout.with("lastTransition", FanoutSchedulerIdle),
  }

  action step = any {
    {
      nondet repairMode = oneOf(Set(BackgroundRepair, ForegroundRepair))
      nondet minCommitReplicas = oneOf(CommitThresholds)
      beginIntent(repairMode, minCommitReplicas)
    },
    {
      nondet replica = oneOf(Nodes)
      any {
        recordInitialReceipt(replica),
        sendStage(replica),
        receiveStageStored(replica),
        receiveStageRejected(replica),
        sendRepair(replica),
        receiveRepairStored(replica),
        receiveRepairRejected(replica),
        sendPromotion(replica),
        receivePromotionApplied(replica),
        receivePromotionRejected(replica),
      }
    },
    publishVisibleCommit,
    publishWriteResponse,
    schedulerIdle,
  }

  action backgroundW2CoverageStep =
    if (fanout.phase == FanoutIdle) {
      beginCoverageIntent(BackgroundW2Scenario)
    } else if (fanout.durableReceipts.size() == 0) {
      recordInitialReceipt(N1)
    } else if (not(fanout.visibleCommitted)
      and fanout.durableReceipts.size() >= fanout.minCommitReplicas) {
      publishVisibleCommit
    } else if (not(fanout.visibleCommitted) and fanout.stageInflight.size() == 0) {
      sendStage(N2)
    } else if (fanout.stageInflight.contains(N2)) {
      receiveStageStored(N2)
    } else if (not(fanout.responsePublished)) {
      publishWriteResponse
    } else if (fanout.promotionPending.contains(N2)) {
      sendPromotion(N2)
    } else if (fanout.promotionInflight.contains(N2)) {
      receivePromotionApplied(N2)
    } else if (not(fanout.repairStarted)) {
      sendRepair(N3)
    } else if (fanout.repairInflight.contains(N3)) {
      receiveRepairStored(N3)
    } else {
      schedulerIdle
    }

  action foregroundW2CoverageStep =
    if (fanout.phase == FanoutIdle) {
      beginCoverageIntent(ForegroundW2Scenario)
    } else if (fanout.durableReceipts.size() == 0) {
      recordInitialReceipt(N1)
    } else if (not(fanout.stageInflight.contains(N2))
      and not(fanout.durableReceipts.contains(N2))) {
      sendStage(N2)
    } else if (not(fanout.stageInflight.contains(N3))
      and not(fanout.durableReceipts.contains(N3))) {
      sendStage(N3)
    } else if (fanout.stageInflight.contains(N2)) {
      receiveStageStored(N2)
    } else if (not(fanout.visibleCommitted)) {
      publishVisibleCommit
    } else if (fanout.stageInflight.contains(N3)) {
      receiveStageStored(N3)
    } else if (not(fanout.responsePublished)) {
      publishWriteResponse
    } else if (fanout.promotionPending.contains(N2)) {
      sendPromotion(N2)
    } else if (fanout.promotionPending.contains(N3)) {
      sendPromotion(N3)
    } else if (fanout.promotionInflight.contains(N2)) {
      receivePromotionApplied(N2)
    } else if (fanout.promotionInflight.contains(N3)) {
      receivePromotionApplied(N3)
    } else {
      schedulerIdle
    }

  action foregroundOutOfOrderCoverageStep =
    if (fanout.phase == FanoutIdle) {
      beginCoverageIntent(ForegroundOutOfOrderScenario)
    } else if (fanout.durableReceipts.size() == 0) {
      recordInitialReceipt(N1)
    } else if (not(fanout.stageInflight.contains(N2))
      and not(fanout.durableReceipts.contains(N2))) {
      sendStage(N2)
    } else if (not(fanout.stageInflight.contains(N3))
      and not(fanout.durableReceipts.contains(N3))) {
      sendStage(N3)
    } else if (fanout.stageInflight.contains(N3)) {
      receiveStageStored(N3)
    } else if (not(fanout.visibleCommitted)) {
      publishVisibleCommit
    } else if (fanout.stageInflight.contains(N2)) {
      receiveStageStored(N2)
    } else if (not(fanout.responsePublished)) {
      publishWriteResponse
    } else if (fanout.promotionPending.contains(N2)) {
      sendPromotion(N2)
    } else if (fanout.promotionPending.contains(N3)) {
      sendPromotion(N3)
    } else if (fanout.promotionInflight.contains(N2)) {
      receivePromotionApplied(N2)
    } else if (fanout.promotionInflight.contains(N3)) {
      receivePromotionApplied(N3)
    } else {
      schedulerIdle
    }

  /// A transport error or explicit rejection is ambiguous with respect to
  /// delivery, so CommitVersion must be replayable and idempotent. This path
  /// makes the production retry queue an explicit, covered part of the model
  /// instead of treating promotion as fire-and-forget work.
  action promotionRetryCoverageStep =
    if (fanout.phase == FanoutIdle) {
      beginCoverageIntent(PromotionRetryScenario)
    } else if (fanout.durableReceipts.size() == 0) {
      recordInitialReceipt(N1)
    } else if (not(fanout.visibleCommitted)
      and fanout.durableReceipts.size() >= fanout.minCommitReplicas) {
      publishVisibleCommit
    } else if (not(fanout.visibleCommitted) and fanout.stageInflight.size() == 0) {
      sendStage(N2)
    } else if (fanout.stageInflight.contains(N2)) {
      receiveStageStored(N2)
    } else if (not(fanout.responsePublished)) {
      publishWriteResponse
    } else if (fanout.promotionPending.contains(N2)) {
      sendPromotion(N2)
    } else if (fanout.promotionInflight.contains(N2)
      and not(fanout.promotionRetries.contains(N2))) {
      receivePromotionRejected(N2)
    } else if (fanout.promotionInflight.contains(N2)) {
      receivePromotionApplied(N2)
    } else if (not(fanout.repairStarted)) {
      sendRepair(N3)
    } else if (fanout.repairInflight.contains(N3)) {
      receiveRepairStored(N3)
    } else {
      schedulerIdle
    }

  action coverageStep =
    if (fanout.phase == FanoutIdle) {
      nondet scenario = oneOf(CoverageScenarios)
      beginCoverageIntent(scenario)
    } else if (fanout.coverageScenario == BackgroundW2Scenario) {
      backgroundW2CoverageStep
    } else if (fanout.coverageScenario == ForegroundW2Scenario) {
      foregroundW2CoverageStep
    } else if (fanout.coverageScenario == ForegroundOutOfOrderScenario) {
      foregroundOutOfOrderCoverageStep
    } else if (fanout.coverageScenario == PromotionRetryScenario) {
      promotionRetryCoverageStep
    } else {
      schedulerIdle
    }
}
