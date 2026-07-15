/// Prompt daemon maintenance for a local retiring-primary handoff.
///
/// Evidence class: target hardening for the durable DFS maintenance probe.
/// `ordinaryWorkPending` abstracts the pre-existing write, namespace-job,
/// replica-lag, and due/unattempted-repair detector fields. The old detector
/// considered only that work. A membership handoff may be committed just
/// after a maintenance tick, so omitting a local retiring-primary handoff can
/// leave it waiting for the long periodic interval.
///
/// The target detector adds exactly one contribution: a pending handoff whose
/// retiring primary is the local coordinator N1. A nonlocal handoff remains
/// another node's work. The triggered reconcile installs the already-specified
/// retiring-primary fence before union transfer; a ready transport can then
/// complete without waiting for the long tick. Transport fairness and wall-
/// clock bounds are outside this bounded reachability slice.
module dfs_metadata_handoff_maintenance_trigger {
  import common.* from "../../lib/common"

  type HandoffPhase = NoHandoff | PendingHandoff | ReconcilingHandoff | CompletedHandoff
  type TriggerTransition =
    | MaintenanceTickCompleted
    | HandoffBegan
    | OrdinaryWorkBegan
    | DetectorsProbed
    | TriggeredReconcileStarted
    | TriggeredReconcileTimedOut
    | RetiringWriteAccepted
    | RetiringWriteRejected
    | TransportBecameReady
    | TransportBecameBlocked
    | HandoffCompleted
    | LongPeriodicTickBecameDue
    | TriggerStuttered

  var handoffPhase: HandoffPhase
  var retiringPrimary: Node
  var ordinaryWorkPending: bool
  var longPeriodicTickDue: bool
  var oldTriggerRequested: bool
  var newTriggerRequested: bool
  var transportReady: bool
  var retiringFenceInstalled: bool
  var acceptedRetiringWrites: int
  var acceptedRetiringWritesAtFence: int
  var reconcileAttemptCount: int
  var lastTransition: TriggerTransition

  pure def localPendingHandoff: bool = all {
    handoffPhase == PendingHandoff,
    retiringPrimary == N1,
  }

  pure def oldDetectorShouldTrigger: bool = ordinaryWorkPending

  pure def newDetectorShouldTrigger: bool =
    oldDetectorShouldTrigger or localPendingHandoff

  action init = all {
    handoffPhase' = NoHandoff,
    retiringPrimary' = N1,
    ordinaryWorkPending' = false,
    longPeriodicTickDue' = false,
    oldTriggerRequested' = false,
    newTriggerRequested' = false,
    transportReady' = false,
    retiringFenceInstalled' = false,
    acceptedRetiringWrites' = 0,
    acceptedRetiringWritesAtFence' = 0,
    reconcileAttemptCount' = 0,
    lastTransition' = MaintenanceTickCompleted,
  }

  /// The handoff appears immediately after the prior maintenance pass, before
  /// the long periodic timer is due again.
  action beginHandoff(primary: Node): bool = all {
    handoffPhase == NoHandoff,
    not(longPeriodicTickDue),
    handoffPhase' = PendingHandoff,
    retiringPrimary' = primary,
    ordinaryWorkPending' = ordinaryWorkPending,
    longPeriodicTickDue' = longPeriodicTickDue,
    oldTriggerRequested' = false,
    newTriggerRequested' = false,
    transportReady' = transportReady,
    retiringFenceInstalled' = false,
    acceptedRetiringWrites' = acceptedRetiringWrites,
    acceptedRetiringWritesAtFence' = acceptedRetiringWritesAtFence,
    reconcileAttemptCount' = reconcileAttemptCount,
    lastTransition' = HandoffBegan,
  }

  action beginOrdinaryWork = all {
    not(ordinaryWorkPending),
    handoffPhase' = handoffPhase,
    retiringPrimary' = retiringPrimary,
    ordinaryWorkPending' = true,
    longPeriodicTickDue' = longPeriodicTickDue,
    oldTriggerRequested' = oldTriggerRequested,
    newTriggerRequested' = newTriggerRequested,
    transportReady' = transportReady,
    retiringFenceInstalled' = retiringFenceInstalled,
    acceptedRetiringWrites' = acceptedRetiringWrites,
    acceptedRetiringWritesAtFence' = acceptedRetiringWritesAtFence,
    reconcileAttemptCount' = reconcileAttemptCount,
    lastTransition' = OrdinaryWorkBegan,
  }

  action probeDetectors = all {
    handoffPhase' = handoffPhase,
    retiringPrimary' = retiringPrimary,
    ordinaryWorkPending' = ordinaryWorkPending,
    longPeriodicTickDue' = longPeriodicTickDue,
    oldTriggerRequested' = oldDetectorShouldTrigger,
    newTriggerRequested' = newDetectorShouldTrigger,
    transportReady' = transportReady,
    retiringFenceInstalled' = retiringFenceInstalled,
    acceptedRetiringWrites' = acceptedRetiringWrites,
    acceptedRetiringWritesAtFence' = acceptedRetiringWritesAtFence,
    reconcileAttemptCount' = reconcileAttemptCount,
    lastTransition' = DetectorsProbed,
  }

  /// Consuming the prompt trigger enters the exact-union reconcile and
  /// installs the retiring-primary fence before any transport progress.
  action startTriggeredReconcile = all {
    newTriggerRequested,
    localPendingHandoff,
    handoffPhase' = ReconcilingHandoff,
    retiringPrimary' = retiringPrimary,
    ordinaryWorkPending' = ordinaryWorkPending,
    longPeriodicTickDue' = longPeriodicTickDue,
    oldTriggerRequested' = false,
    newTriggerRequested' = false,
    transportReady' = transportReady,
    retiringFenceInstalled' = true,
    acceptedRetiringWrites' = acceptedRetiringWrites,
    acceptedRetiringWritesAtFence' = acceptedRetiringWrites,
    reconcileAttemptCount' = reconcileAttemptCount + 1,
    lastTransition' = TriggeredReconcileStarted,
  }

  /// The pending control-plane record is not itself a fence. A write already
  /// inside the retiring publication guard may finish before reconcile starts;
  /// every attempt after the durable fence is an observation-only rejection.
  action attemptRetiringWrite = all {
    handoffPhase != NoHandoff,
    handoffPhase' = handoffPhase,
    retiringPrimary' = retiringPrimary,
    ordinaryWorkPending' = ordinaryWorkPending,
    longPeriodicTickDue' = longPeriodicTickDue,
    oldTriggerRequested' = oldTriggerRequested,
    newTriggerRequested' = newTriggerRequested,
    transportReady' = transportReady,
    retiringFenceInstalled' = retiringFenceInstalled,
    acceptedRetiringWrites' =
      if (retiringFenceInstalled) acceptedRetiringWrites
      else acceptedRetiringWrites + 1,
    acceptedRetiringWritesAtFence' = acceptedRetiringWritesAtFence,
    reconcileAttemptCount' = reconcileAttemptCount,
    lastTransition' =
      if (retiringFenceInstalled) RetiringWriteRejected
      else RetiringWriteAccepted,
  }

  action setTransportReady = all {
    handoffPhase' = handoffPhase,
    retiringPrimary' = retiringPrimary,
    ordinaryWorkPending' = ordinaryWorkPending,
    longPeriodicTickDue' = longPeriodicTickDue,
    oldTriggerRequested' = oldTriggerRequested,
    newTriggerRequested' = newTriggerRequested,
    transportReady' = true,
    retiringFenceInstalled' = retiringFenceInstalled,
    acceptedRetiringWrites' = acceptedRetiringWrites,
    acceptedRetiringWritesAtFence' = acceptedRetiringWritesAtFence,
    reconcileAttemptCount' = reconcileAttemptCount,
    lastTransition' = TransportBecameReady,
  }

  action setTransportBlocked = all {
    transportReady,
    handoffPhase' = handoffPhase,
    retiringPrimary' = retiringPrimary,
    ordinaryWorkPending' = ordinaryWorkPending,
    longPeriodicTickDue' = longPeriodicTickDue,
    oldTriggerRequested' = oldTriggerRequested,
    newTriggerRequested' = newTriggerRequested,
    transportReady' = false,
    retiringFenceInstalled' = retiringFenceInstalled,
    acceptedRetiringWrites' = acceptedRetiringWrites,
    acceptedRetiringWritesAtFence' = acceptedRetiringWritesAtFence,
    reconcileAttemptCount' = reconcileAttemptCount,
    lastTransition' = TransportBecameBlocked,
  }

  /// A bounded blocked attempt fails without consuming the durable handoff.
  /// The installed fence remains monotonic; returning to PendingHandoff lets
  /// the prompt detector rediscover and reschedule the same local work.
  action timeoutBlockedReconcile = all {
    handoffPhase == ReconcilingHandoff,
    retiringPrimary == N1,
    retiringFenceInstalled,
    not(transportReady),
    reconcileAttemptCount > 0,
    handoffPhase' = PendingHandoff,
    retiringPrimary' = retiringPrimary,
    ordinaryWorkPending' = ordinaryWorkPending,
    longPeriodicTickDue' = longPeriodicTickDue,
    oldTriggerRequested' = false,
    newTriggerRequested' = false,
    transportReady' = transportReady,
    retiringFenceInstalled' = retiringFenceInstalled,
    acceptedRetiringWrites' = acceptedRetiringWrites,
    acceptedRetiringWritesAtFence' = acceptedRetiringWritesAtFence,
    reconcileAttemptCount' = reconcileAttemptCount,
    lastTransition' = TriggeredReconcileTimedOut,
  }

  action completeTriggeredReconcile = all {
    handoffPhase == ReconcilingHandoff,
    retiringPrimary == N1,
    retiringFenceInstalled,
    transportReady,
    handoffPhase' = CompletedHandoff,
    retiringPrimary' = retiringPrimary,
    ordinaryWorkPending' = ordinaryWorkPending,
    longPeriodicTickDue' = longPeriodicTickDue,
    oldTriggerRequested' = false,
    newTriggerRequested' = false,
    transportReady' = transportReady,
    retiringFenceInstalled' = retiringFenceInstalled,
    acceptedRetiringWrites' = acceptedRetiringWrites,
    acceptedRetiringWritesAtFence' = acceptedRetiringWritesAtFence,
    reconcileAttemptCount' = reconcileAttemptCount,
    lastTransition' = HandoffCompleted,
  }

  action reachLongPeriodicTick = all {
    not(longPeriodicTickDue),
    handoffPhase' = handoffPhase,
    retiringPrimary' = retiringPrimary,
    ordinaryWorkPending' = ordinaryWorkPending,
    longPeriodicTickDue' = true,
    oldTriggerRequested' = oldTriggerRequested,
    newTriggerRequested' = newTriggerRequested,
    transportReady' = transportReady,
    retiringFenceInstalled' = retiringFenceInstalled,
    acceptedRetiringWrites' = acceptedRetiringWrites,
    acceptedRetiringWritesAtFence' = acceptedRetiringWritesAtFence,
    reconcileAttemptCount' = reconcileAttemptCount,
    lastTransition' = LongPeriodicTickBecameDue,
  }

  action stutter = all {
    handoffPhase' = handoffPhase,
    retiringPrimary' = retiringPrimary,
    ordinaryWorkPending' = ordinaryWorkPending,
    longPeriodicTickDue' = longPeriodicTickDue,
    oldTriggerRequested' = oldTriggerRequested,
    newTriggerRequested' = newTriggerRequested,
    transportReady' = transportReady,
    retiringFenceInstalled' = retiringFenceInstalled,
    acceptedRetiringWrites' = acceptedRetiringWrites,
    acceptedRetiringWritesAtFence' = acceptedRetiringWritesAtFence,
    reconcileAttemptCount' = reconcileAttemptCount,
    lastTransition' = TriggerStuttered,
  }

  action step = any {
    {
      nondet primary = oneOf(Nodes)
      beginHandoff(primary)
    },
    beginOrdinaryWork,
    probeDetectors,
    startTriggeredReconcile,
    attemptRetiringWrite,
    setTransportReady,
    setTransportBlocked,
    timeoutBlockedReconcile,
    completeTriggeredReconcile,
    reachLongPeriodicTick,
    stutter,
  }

  /// Coverage-only scheduler that follows the real transition actions through
  /// one blocked attempt, prompt rediscovery, a second attempt, and ready
  /// completion. `step` remains the symbolic transition relation; the harness
  /// mixes this scheduler with `step` only for deterministic witness coverage.
  action retryCoverageStep =
    if (all {
      handoffPhase == NoHandoff,
      reconcileAttemptCount == 0,
      not(longPeriodicTickDue),
    }) {
      beginHandoff(N1)
    } else if (all {
      handoffPhase == PendingHandoff,
      not(newTriggerRequested),
    }) {
      probeDetectors
    } else if (all {
      handoffPhase == PendingHandoff,
      newTriggerRequested,
    }) {
      startTriggeredReconcile
    } else if (all {
      handoffPhase == ReconcilingHandoff,
      reconcileAttemptCount == 1,
      not(transportReady),
    }) {
      timeoutBlockedReconcile
    } else if (all {
      handoffPhase == ReconcilingHandoff,
      reconcileAttemptCount >= 2,
      not(transportReady),
    }) {
      setTransportReady
    } else if (all {
      handoffPhase == ReconcilingHandoff,
      reconcileAttemptCount >= 2,
      transportReady,
    }) {
      completeTriggeredReconcile
    } else {
      stutter
    }

  action coverageStep = any {
    step,
    retryCoverageStep,
  }
}
