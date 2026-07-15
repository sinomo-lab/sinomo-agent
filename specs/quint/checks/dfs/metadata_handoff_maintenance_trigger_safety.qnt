/// Safety and reachability checks for prompt membership-handoff maintenance.
module dfs_metadata_handoff_maintenance_trigger_safety {
  import common.* from "../../lib/common"
  import dfs_metadata_handoff_maintenance_trigger.* from "../../models/dfs/metadata_handoff_maintenance_trigger"

  /// When ordinary work is absent, a local pending handoff is exactly the new
  /// detector's additional trigger contribution.
  def pendingLocalHandoffIsExactNewDetectorContribution: bool =
    not(oldDetectorShouldTrigger) implies
      (newDetectorShouldTrigger iff localPendingHandoff)

  def detectorProbeStoresExactResults: bool =
    lastTransition == DetectorsProbed implies all {
      oldTriggerRequested == oldDetectorShouldTrigger,
      newTriggerRequested == newDetectorShouldTrigger,
    }

  def triggerNeverActivatesWithoutRelevantWork: bool = all {
    oldTriggerRequested implies ordinaryWorkPending,
    newTriggerRequested implies any {
      ordinaryWorkPending,
      localPendingHandoff,
    },
  }

  def nonLocalHandoffCannotStartLocalReconcile: bool = all {
    handoffPhase == ReconcilingHandoff implies retiringPrimary == N1,
    handoffPhase == CompletedHandoff implies retiringPrimary == N1,
    all {
      handoffPhase == PendingHandoff,
      retiringPrimary != N1,
      not(ordinaryWorkPending),
      lastTransition == DetectorsProbed,
    } implies all {
      not(oldTriggerRequested),
      not(newTriggerRequested),
    },
  }

  def reconcileInstallsRetiringFence: bool =
    any {
      handoffPhase == ReconcilingHandoff,
      handoffPhase == CompletedHandoff,
    } implies retiringFenceInstalled

  /// Accepted writes are snapshotted when the retiring fence is installed and
  /// can never advance on the fenced stale writer afterward.
  def frozenRetiringWritesRemainFenced: bool =
    retiringFenceInstalled implies
      acceptedRetiringWrites == acceptedRetiringWritesAtFence

  def completionRequiresReadyTransportAndFence: bool =
    lastTransition == HandoffCompleted implies all {
      handoffPhase == CompletedHandoff,
      retiringPrimary == N1,
      transportReady,
      retiringFenceInstalled,
      reconcileAttemptCount > 0,
    }

  def timedOutReconcileReturnsToRetryablePending: bool =
    lastTransition == TriggeredReconcileTimedOut implies all {
      localPendingHandoff,
      retiringFenceInstalled,
      not(transportReady),
      reconcileAttemptCount > 0,
      newDetectorShouldTrigger,
      not(newTriggerRequested),
    }

  def witnessQuietProbeDoesNotTrigger: bool = all {
    lastTransition == DetectorsProbed,
    handoffPhase == NoHandoff,
    not(ordinaryWorkPending),
    not(oldTriggerRequested),
    not(newTriggerRequested),
  }

  def witnessLocalHandoffBeginsImmediatelyAfterTick: bool = all {
    lastTransition == HandoffBegan,
    handoffPhase == PendingHandoff,
    retiringPrimary == N1,
    not(longPeriodicTickDue),
  }

  /// Concrete old-detector counterexample: the handoff arrived just after the
  /// tick, no ordinary work exists, and the long periodic tick is not due.
  def witnessOldDetectorMissesLocalHandoff: bool = all {
    lastTransition == DetectorsProbed,
    localPendingHandoff,
    not(ordinaryWorkPending),
    not(longPeriodicTickDue),
    not(oldTriggerRequested),
    newTriggerRequested,
  }

  def witnessNewDetectorTriggersLocalHandoff: bool = all {
    lastTransition == DetectorsProbed,
    localPendingHandoff,
    newTriggerRequested,
    newDetectorShouldTrigger,
  }

  def witnessNonLocalHandoffDoesNotTrigger: bool = all {
    lastTransition == DetectorsProbed,
    handoffPhase == PendingHandoff,
    retiringPrimary != N1,
    not(ordinaryWorkPending),
    not(oldTriggerRequested),
    not(newTriggerRequested),
  }

  def witnessOrdinaryWorkStillTriggers: bool = all {
    lastTransition == DetectorsProbed,
    ordinaryWorkPending,
    oldTriggerRequested,
    newTriggerRequested,
  }

  def witnessPreFenceRetiringWriteMayFinish: bool = all {
    lastTransition == RetiringWriteAccepted,
    handoffPhase == PendingHandoff,
    retiringPrimary == N1,
    not(retiringFenceInstalled),
    acceptedRetiringWrites > 0,
  }

  def witnessTriggeredReconcileInstallsFence: bool = all {
    lastTransition == TriggeredReconcileStarted,
    handoffPhase == ReconcilingHandoff,
    retiringPrimary == N1,
    retiringFenceInstalled,
    acceptedRetiringWrites == acceptedRetiringWritesAtFence,
  }

  def witnessFrozenRetiringWriteRejected: bool = all {
    lastTransition == RetiringWriteRejected,
    retiringFenceInstalled,
    frozenRetiringWritesRemainFenced,
  }

  def witnessBlockedTriggeredReconcileStartsFenced: bool = all {
    lastTransition == TriggeredReconcileStarted,
    handoffPhase == ReconcilingHandoff,
    not(transportReady),
    retiringFenceInstalled,
  }

  def witnessTransportBecomesBlockedDuringReconcile: bool = all {
    lastTransition == TransportBecameBlocked,
    handoffPhase == ReconcilingHandoff,
    retiringFenceInstalled,
    not(transportReady),
  }

  def witnessBlockedReconcileTimesOutToPending: bool = all {
    lastTransition == TriggeredReconcileTimedOut,
    localPendingHandoff,
    retiringFenceInstalled,
    reconcileAttemptCount == 1,
    not(longPeriodicTickDue),
  }

  def witnessTimeoutRetriggersBeforeLongTick: bool = all {
    lastTransition == DetectorsProbed,
    localPendingHandoff,
    retiringFenceInstalled,
    reconcileAttemptCount == 1,
    newTriggerRequested,
    not(longPeriodicTickDue),
  }

  def witnessRetriedReconcileStartsBeforeLongTick: bool = all {
    lastTransition == TriggeredReconcileStarted,
    handoffPhase == ReconcilingHandoff,
    retiringFenceInstalled,
    reconcileAttemptCount >= 2,
    not(longPeriodicTickDue),
  }

  def witnessReadyTriggeredReconcileCompletesBeforeLongTick: bool = all {
    lastTransition == HandoffCompleted,
    handoffPhase == CompletedHandoff,
    transportReady,
    retiringFenceInstalled,
    reconcileAttemptCount >= 2,
    not(longPeriodicTickDue),
  }

  def safety: bool = all {
    pendingLocalHandoffIsExactNewDetectorContribution,
    detectorProbeStoresExactResults,
    triggerNeverActivatesWithoutRelevantWork,
    nonLocalHandoffCannotStartLocalReconcile,
    reconcileInstallsRetiringFence,
    frozenRetiringWritesRemainFenced,
    completionRequiresReadyTransportAndFence,
    timedOutReconcileReturnsToRetryablePending,
  }
}
