/// Safety and bounded reachability for cursor-assisted handoff preflight.
module dfs_metadata_primary_handoff_cursor_preflight_safety {
  import common.* from "../../lib/common"
  import dfs_metadata_primary_handoff_cursor_preflight.* from "../../models/dfs/metadata_primary_handoff_cursor_preflight"

  def headsStayBounded: bool = all {
    preflight.primaryHead >= 1,
    preflight.primaryHead <= 3,
    preflight.certifiedHead >= 1,
    preflight.certifiedHead <= preflight.primaryHead,
    preflight.targetCursor >= 1,
    preflight.targetCursor <= preflight.primaryHead,
    preflight.targetPrefixHead >= 1,
    preflight.targetPrefixHead <= preflight.primaryHead,
    preflight.fencedHead >= 0,
    preflight.fencedHead <= preflight.primaryHead,
  }

  def staleCursorCannotBeginUncertifiedHandoff: bool = all {
    not(certifiedHeadPreflightReady(preflight)),
    not(caughtUpCursorPreflightReady(preflight)),
  } implies not(optimizedPreflightReady(preflight))

  def pendingDoesNotTransferAuthority: bool =
    preflight.phase == Pending implies all {
      preflight.activePrimary == N1,
      not(preflight.retiringFenceInstalled),
      retiringWritable(preflight),
    }

  /// The cursor widens only admission to Pending. It contributes no fresh ACK.
  def cursorPreflightNeverDirectlyActivates: bool =
    preflight.lastTransition == HandoffBeganFromDurableCursor implies all {
      preflight.beganFromCursor,
      preflight.phase == Pending,
      preflight.activePrimary == N1,
      preflight.freshExactAcks == Set(),
    }

  def fenceRevokesRetiringWrites: bool =
    preflight.retiringFenceInstalled implies not(retiringWritable(preflight))

  def acknowledgementsStayFreshAndConfigured: bool = all {
    preflight.freshExactAcks.subseteq(ConfiguredMembers),
    not(preflight.retiringFenceInstalled) implies preflight.freshExactAcks == Set(),
    preflight.freshExactAcks.contains(N2) implies all {
      preflight.targetCursor >= preflight.fencedHead,
      preflight.targetPrefixHead == preflight.fencedHead,
      preflight.targetExactFingerprint,
    },
  }

  def wrongFingerprintCannotAcknowledgeTarget: bool =
    not(preflight.targetExactFingerprint) implies
      not(preflight.freshExactAcks.contains(N2))

  def activationStillRequiresExactFullRingProof: bool =
    preflight.activePrimary == N2 implies all {
      preflight.phase == Activated,
      preflight.retiringFenceInstalled,
      preflight.freshExactAcks == ConfiguredMembers,
      preflight.targetExactFingerprint,
      preflight.targetPrefixHead == preflight.fencedHead,
      promotedWritable(preflight),
      not(retiringWritable(preflight)),
    }

  def atMostOneWritablePrimary: bool =
    not(all { retiringWritable(preflight), promotedWritable(preflight) })

  def witnessHeadAdvanceMakesOldPreflightStale: bool = all {
    preflight.lastTransition == PrimaryHeadAdvanced,
    preflight.phase == Expanded,
    not(certifiedHeadPreflightReady(preflight)),
    not(caughtUpCursorPreflightReady(preflight)),
    not(optimizedPreflightReady(preflight)),
  }

  def witnessExactCursorRestoresOnlyOptimizedPreflight: bool = all {
    preflight.lastTransition == TargetCursorCaughtUpExact,
    preflight.phase == Expanded,
    not(certifiedHeadPreflightReady(preflight)),
    caughtUpCursorPreflightReady(preflight),
    optimizedPreflightReady(preflight),
  }

  def witnessCaughtUpCursorBeginsOnlyPending: bool = all {
    preflight.lastTransition == HandoffBeganFromDurableCursor,
    preflight.phase == Pending,
    preflight.activePrimary == N1,
    preflight.freshExactAcks == Set(),
  }

  def witnessWrongFingerprintCursorBeginsOnlyPending: bool = all {
    preflight.lastTransition == HandoffBeganFromDurableCursor,
    preflight.phase == Pending,
    not(preflight.targetExactFingerprint),
    not(preflight.freshExactAcks.contains(N2)),
    preflight.activePrimary == N1,
  }

  def witnessCertifiedHeadPathStillBegins: bool = all {
    preflight.lastTransition == HandoffBeganFromCertifiedHead,
    preflight.phase == Pending,
    not(preflight.beganFromCursor),
    preflight.activePrimary == N1,
  }

  def witnessLateWriteIsCapturedByFence: bool = all {
    preflight.lastTransition == RetiringPrimaryFenced,
    preflight.fencedHead == 2,
    preflight.primaryHead == 2,
    preflight.retiringFenceInstalled,
    not(retiringWritable(preflight)),
  }

  def witnessDivergentTargetProbeRejects: bool = all {
    preflight.lastTransition == TargetDivergentProbeRejected,
    preflight.phase == Fenced,
    preflight.divergentProbeRejected,
    not(preflight.targetExactFingerprint),
    not(preflight.freshExactAcks.contains(N2)),
    preflight.activePrimary == N1,
  }

  def witnessPartialFullRingProofStaysPending: bool = all {
    preflight.lastTransition == StableReplicaAcknowledged,
    preflight.phase == Fenced,
    preflight.freshExactAcks == Set(N1, N3),
    preflight.activePrimary == N1,
  }

  def witnessAuthoritativeSnapshotRepairsDivergence: bool = all {
    preflight.lastTransition == TargetSnapshotRepaired,
    preflight.phase == Fenced,
    preflight.snapshotRepaired,
    preflight.targetExactFingerprint,
    preflight.targetPrefixHead == preflight.fencedHead,
    not(preflight.freshExactAcks.contains(N2)),
  }

  def witnessCursorPathActivatesOnlyAfterExactFullRingProof: bool = all {
    preflight.lastTransition == TargetActivated,
    preflight.beganFromCursor,
    preflight.activePrimary == N2,
    preflight.retiringFenceInstalled,
    preflight.freshExactAcks == ConfiguredMembers,
    promotedWritable(preflight),
    not(retiringWritable(preflight)),
  }

  def safety: bool = all {
    headsStayBounded,
    staleCursorCannotBeginUncertifiedHandoff,
    pendingDoesNotTransferAuthority,
    cursorPreflightNeverDirectlyActivates,
    fenceRevokesRetiringWrites,
    acknowledgementsStayFreshAndConfigured,
    wrongFingerprintCannotAcknowledgeTarget,
    activationStillRequiresExactFullRingProof,
    atMostOneWritablePrimary,
  }
}
