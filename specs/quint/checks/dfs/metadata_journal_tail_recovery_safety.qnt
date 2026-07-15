/// Safety and bounded reachability for same-instance journal-tail recovery.
module dfs_metadata_journal_tail_recovery_safety {
  import common.* from "../../lib/common"
  import dfs_metadata_journal_tail_recovery.* from "../../models/dfs/metadata_journal_tail_recovery"

  def tailsStayInBounds: bool = all {
    recovery.materializedTail >= 0,
    recovery.materializedTail <= recovery.journalTail,
    recovery.journalTail <= MaxEntries,
    recovery.journalTail <= recovery.materializedTail + 1,
  }

  def journalAndMaterializedProofsStayContiguous: bool = all {
    journalProofIsContiguous(recovery),
    materializedProofIsContiguous(recovery),
    recovery.materializedEntries.subseteq(recovery.journalEntries),
  }

  /// A durable append temporarily exposes the preapplied projection in memory.
  /// The failure and begin-write recovery cuts expose only the last materialized
  /// prefix; every otherwise-idle/admitted state has exact table agreement.
  def liveTablesMatchCurrentPhase: bool =
    if (recovery.phase == DurableAppendPendingMaterialization) {
      all {
        recovery.liveEntries == recovery.journalEntries,
        recovery.journalTail == recovery.materializedTail + 1,
      }
    } else if (any {
      recovery.phase == RecoveringForBeginWrite,
      recovery.phase == Idle and hasDurableTail(recovery),
    }) {
      recovery.liveEntries == recovery.materializedEntries
    } else {
      all {
        recovery.liveEntries == recovery.materializedEntries,
        recovery.materializedEntries == recovery.journalEntries,
        recovery.materializedTail == recovery.journalTail,
      }
    }

  def activeEntryMatchesCurrentPhase: bool =
    if (recovery.phase == WriteAdmitted) {
      recovery.activeEntry == Some(nextEntry(recovery.journalTail))
    } else if (recovery.phase == DurableAppendPendingMaterialization) {
      recovery.activeEntry == Some(nextEntry(recovery.materializedTail))
    } else {
      recovery.activeEntry == None
    }

  def admissionAccountingIsExact: bool =
    recovery.writeAdmissions == recovery.durableAppends
      + (if (recovery.phase == WriteAdmitted) 1 else 0)

  /// Every admission records an equal journal/materialized base. A recovered
  /// begin-write writes the recovered tail into both fields before admission.
  def noWriteAdmissionCrossesDurableGap: bool = all {
    recovery.lastAdmissionJournalBase
      == recovery.lastAdmissionMaterializedBase,
    recovery.phase == WriteAdmitted implies not(hasDurableTail(recovery)),
  }

  /// Durable append can create the next one-entry materialization gap, but its
  /// admitted base must have been contiguous. This audit survives the append.
  def noNewAppendStartsAcrossDurableGap: bool = all {
    recovery.lastAppendJournalBase == recovery.lastAppendMaterializedBase,
    recovery.lastTransition == DurableAppendSucceeded implies all {
      recovery.journalTail == recovery.lastAppendJournalBase + 1,
      recovery.materializedTail == recovery.lastAppendMaterializedBase,
    },
  }

  def failedAppendLeavesDurableTailAndRolledBackTables: bool =
    recovery.lastTransition == TableBatchFailedAndRolledBack implies all {
      recovery.phase == Idle,
      hasDurableTail(recovery),
      recovery.journalTail == recovery.materializedTail + 1,
      recovery.liveEntries == recovery.materializedEntries,
      recovery.failureReplayProjection == recovery.journalEntries,
      recovery.activeEntry == None,
    }

  /// Gap detection is observation-only: neither a write admission nor a
  /// durable append is counted while the begin-write call is recovering.
  def detectedGapBlocksAdmissionAndAppend: bool =
    recovery.phase == RecoveringForBeginWrite implies all {
      hasDurableTail(recovery),
      recovery.writeAdmissions == recovery.detectionAdmissionSnapshot,
      recovery.durableAppends == recovery.detectionAppendSnapshot,
      recovery.activeEntry == None,
    }

  /// Same-instance replay closes the exact durable tail before it increments
  /// admission. The requested new entry is neither active nor appended in the
  /// observable intermediate state.
  def recoveryRestoresContiguousProofBeforeAdmission: bool =
    recovery.lastTransition == DurableTailRecoveredBeforeAdmission implies all {
      recovery.phase == RecoveredBeginWritePendingAdmission,
      not(hasDurableTail(recovery)),
      recovery.materializedTail == recovery.journalTail,
      recovery.materializedEntries == recovery.journalEntries,
      recovery.liveEntries == recovery.journalEntries,
      recovery.materializedEntries == recovery.failureReplayProjection,
      journalProofIsContiguous(recovery),
      materializedProofIsContiguous(recovery),
      recovery.writeAdmissions == recovery.detectionAdmissionSnapshot,
      recovery.durableAppends == recovery.detectionAppendSnapshot,
      recovery.activeEntry == None,
    }

  /// Admission is a separate later transition whose guard consumes only that
  /// recovered contiguous state; durable append is still not performed here.
  def recoveredBeginAdmitsOnlyAfterContiguousProof: bool =
    recovery.lastTransition == RecoveredWriteAdmitted implies all {
      recovery.phase == WriteAdmitted,
      recovery.sameInstanceRecoveries > 0,
      recovery.gapDetections > 0,
      not(hasDurableTail(recovery)),
      recovery.materializedEntries == recovery.journalEntries,
      recovery.liveEntries == recovery.journalEntries,
      recovery.writeAdmissions == recovery.detectionAdmissionSnapshot + 1,
      recovery.durableAppends == recovery.detectionAppendSnapshot,
      recovery.lastAdmissionJournalBase == recovery.journalTail,
      recovery.lastAdmissionMaterializedBase == recovery.journalTail,
    }

  /// Reopen and same-instance recovery use the same pure replay projection for
  /// the same failure cut. Instance lifecycle does not affect the proof rows.
  def reopenAndSameInstanceRecoveryAreEquivalent: bool =
    any {
      recovery.lastTransition == DurableTailRecoveredBeforeAdmission,
      recovery.lastTransition == InstanceReopenedFromDurableTail,
    } implies all {
      recovery.failureReplayProjection == reopenReplayProjection(recovery),
      recovery.materializedEntries == recovery.failureReplayProjection,
      recovery.liveEntries == recovery.failureReplayProjection,
      recovery.materializedTail == recovery.journalTail,
    }

  def aFailureHasAtMostOneRecoverySurface: bool = all {
    recovery.failedTableBatches <= 1,
    recovery.sameInstanceRecoveries + recovery.reopens
      <= recovery.failedTableBatches,
  }

  def witnessHealthyAppend: bool = all {
    recovery.lastTransition == HealthyMaterializationSucceeded,
    recovery.durableAppends == 1,
    recovery.failedTableBatches == 0,
    recovery.journalTail == 1,
    recovery.materializedTail == 1,
    recovery.journalEntries == Set(EntryOne),
    recovery.materializedEntries == Set(EntryOne),
    recovery.liveEntries == Set(EntryOne),
  }

  def witnessDurableAppendTableFailureRollsBack: bool = all {
    recovery.lastTransition == TableBatchFailedAndRolledBack,
    recovery.durableAppends == 1,
    recovery.failedTableBatches == 1,
    recovery.journalEntries == Set(EntryOne),
    recovery.materializedEntries == Set(),
    recovery.liveEntries == Set(),
    failedAppendLeavesDurableTailAndRolledBackTables,
  }

  def witnessNextBeginRecoversBeforeNewAppend: bool = all {
    recovery.lastTransition == RecoveredWriteAdmitted,
    recovery.gapDetections == 1,
    recovery.sameInstanceRecoveries == 1,
    recovery.writeAdmissions == 2,
    recovery.durableAppends == 1,
    recovery.detectionAdmissionSnapshot == 1,
    recovery.detectionAppendSnapshot == 1,
    recovery.activeEntry == Some(EntryTwo),
    recovery.journalTail == 1,
    recovery.materializedTail == 1,
    recovery.journalEntries == Set(EntryOne),
    recovery.materializedEntries == Set(EntryOne),
    recoveredBeginAdmitsOnlyAfterContiguousProof,
    noWriteAdmissionCrossesDurableGap,
  }

  def witnessReopenEquivalentRecovery: bool = all {
    recovery.lastTransition == InstanceReopenedFromDurableTail,
    recovery.failedTableBatches == 1,
    recovery.reopens == 1,
    recovery.sameInstanceRecoveries == 0,
    recovery.journalEntries == Set(EntryOne),
    recovery.materializedEntries == Set(EntryOne),
    recovery.liveEntries == Set(EntryOne),
    reopenAndSameInstanceRecoveryAreEquivalent,
  }

  def safety: bool = all {
    tailsStayInBounds,
    journalAndMaterializedProofsStayContiguous,
    liveTablesMatchCurrentPhase,
    activeEntryMatchesCurrentPhase,
    admissionAccountingIsExact,
    noWriteAdmissionCrossesDurableGap,
    noNewAppendStartsAcrossDurableGap,
    failedAppendLeavesDurableTailAndRolledBackTables,
    detectedGapBlocksAdmissionAndAppend,
    recoveryRestoresContiguousProofBeforeAdmission,
    recoveredBeginAdmitsOnlyAfterContiguousProof,
    reopenAndSameInstanceRecoveryAreEquivalent,
    aFailureHasAtMostOneRecoverySurface,
  }
}
