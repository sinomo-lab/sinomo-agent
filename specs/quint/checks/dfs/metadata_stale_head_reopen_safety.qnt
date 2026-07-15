/// Safety and bounded reachability for stale metadata-journal-head reopen.
module dfs_metadata_stale_head_reopen_safety {
  import dfs_metadata_stale_head_reopen.* from "../../models/dfs/metadata_stale_head_reopen"

  def durableAndMaterializedProofsStayMonotonic: bool = all {
    reopen.materializedProof == DurableSequenceN,
    reopen.durableSegmentTail >= reopen.materializedProof,
    reopen.durableSegmentTail <= DurableSequenceN + 1,
  }

  /// No admission-capable phase may expose an allocator at or below the
  /// durable/materialized prefix.
  def readyAllocatorRespectsDurableFloor: bool =
    any {
      reopen.phase == OpenReady,
      reopen.phase == WriteAdmitted,
      reopen.phase == AppendComplete,
    } implies reopen.nextSequence >= durableRecoveryFloor(reopen)

  /// Merely observing N - 1 cannot admit or append a write. The uninitialized
  /// allocator makes the repair-before-admission boundary explicit.
  def staleObservationBlocksWrites: bool =
    reopen.phase == RepairRequired implies all {
      reopen.staleHeadWasObserved,
      not(reopen.repairedBeforeAdmission),
      reopen.nextSequence == 0,
      reopen.activeSequence == 0,
      reopen.writeAdmissions == reopen.observationAdmissionSnapshot,
      reopen.durableAppends == reopen.observationAppendSnapshot,
    }

  def stalePathAdmissionRequiresCompletedRepair: bool =
    all {
      reopen.staleHeadWasObserved,
      reopen.writeAdmissions > reopen.observationAdmissionSnapshot,
    } implies all {
      reopen.repairedBeforeAdmission,
      reopen.repairs == 1,
      reopen.admissionSequence >= DurableSequenceN + 1,
    }

  /// The allocator and persisted hint are promoted in the repair transition,
  /// while admission and append counters remain at their observation values.
  def repairPromotesHeadBeforeAdmission: bool =
    reopen.lastTransition == ReopenHeadRepaired implies all {
      reopen.phase == OpenReady,
      reopen.persistedNextSequence == DurableSequenceN + 1,
      reopen.nextSequence == DurableSequenceN + 1,
      reopen.nextSequence == durableRecoveryFloor(reopen),
      reopen.writeAdmissions == reopen.observationAdmissionSnapshot,
      reopen.durableAppends == reopen.observationAppendSnapshot,
      reopen.activeSequence == 0,
      reopen.admissionSequence == 0,
      reopen.repairedBeforeAdmission,
    }

  /// Every modeled post-reopen admission and append is strictly newer than N.
  def durableSequenceNIsNeverReused: bool = all {
    reopen.activeSequence == 0 or reopen.activeSequence > DurableSequenceN,
    reopen.admissionSequence == 0 or reopen.admissionSequence > DurableSequenceN,
    reopen.lastAppendedSequence == 0
      or reopen.lastAppendedSequence > DurableSequenceN,
  }

  def appendConsumesTheAdmittedSequenceExactlyOnce: bool = all {
    reopen.writeAdmissions
      == reopen.durableAppends + (if (reopen.phase == WriteAdmitted) 1 else 0),
    reopen.durableAppends <= 1,
    reopen.lastTransition == NextAppendCommitted implies all {
      reopen.lastAppendedSequence == reopen.admissionSequence,
      reopen.lastAppendedSequence == DurableSequenceN + 1,
      reopen.durableSegmentTail == DurableSequenceN + 1,
      reopen.nextSequence == DurableSequenceN + 2,
    },
  }

  def witnessHealthyHead: bool = all {
    reopen.lastTransition == HealthyHeadAccepted,
    reopen.phase == OpenReady,
    reopen.observedHeadHint == DurableSequenceN + 1,
    reopen.persistedNextSequence == DurableSequenceN + 1,
    reopen.nextSequence == DurableSequenceN + 1,
    reopen.repairs == 0,
  }

  def witnessStaleHeadObserved: bool = all {
    reopen.lastTransition == StaleHeadObserved,
    reopen.phase == RepairRequired,
    reopen.observedHeadHint == DurableSequenceN - 1,
    reopen.persistedNextSequence == DurableSequenceN - 1,
    staleObservationBlocksWrites,
  }

  def witnessReopenRepairsBeforeAdmission: bool = all {
    reopen.lastTransition == ReopenHeadRepaired,
    reopen.staleHeadWasObserved,
    reopen.repairs == 1,
    repairPromotesHeadBeforeAdmission,
  }

  def witnessNextAppendAtNPlusOne: bool = all {
    reopen.lastTransition == NextAppendCommitted,
    reopen.staleHeadWasObserved,
    reopen.repairedBeforeAdmission,
    reopen.writeAdmissions == 1,
    reopen.durableAppends == 1,
    reopen.admissionSequence == DurableSequenceN + 1,
    reopen.lastAppendedSequence == DurableSequenceN + 1,
    durableSequenceNIsNeverReused,
  }

  def safety: bool = all {
    durableAndMaterializedProofsStayMonotonic,
    readyAllocatorRespectsDurableFloor,
    staleObservationBlocksWrites,
    stalePathAdmissionRequiresCompletedRepair,
    repairPromotesHeadBeforeAdmission,
    durableSequenceNIsNeverReused,
    appendConsumesTheAdmittedSequenceExactlyOnce,
  }
}
