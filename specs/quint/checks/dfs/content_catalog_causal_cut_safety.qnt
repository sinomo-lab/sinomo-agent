module dfs_content_catalog_causal_cut_safety {
  import dfs_content_catalog_causal_cut.* from "../../models/dfs/content_catalog_causal_cut"
  def safety: bool = all {
    s.frontier >= 0, s.frontier <= 4, s.stamp >= 0, s.stamp <= 5,
    s.revision >= 0, s.revision <= 2,
    s.hasFence implies s.cutFence == s.frontier,
    s.reservation and s.hasFence and postCutRenewal implies aggregateStamp >= s.cutFence,
    s.release == NegativeRelease implies all {
      s.hasFence, allCopiesKnown, s.matchingIncarnation,
      localFreshnessValid, s.graceElapsed,
      s.releasedRevision <= s.cutRevision, aggregateStamp < s.cutFence,
    },
    s.phase == Finished and not(s.hasFence) and not(s.cutContainsRoot) implies s.reservation,
    s.phase == Finished and (s.legacyInstallBugObserved or s.timestampOnlyBugObserved)
      implies s.reservation,
  }
  def witnessDelayedInstallCannotRejectRenewal: bool = all {
    s.phase == Finished, s.legacyInstallBugObserved, s.reservation,
  }
  def witnessRollbackRecoveryDefeatsTimestampOnlyFence: bool = all {
    s.phase == Finished, s.timestampOnlyBugObserved, s.reservation,
  }
  def witnessPreCutAbandonedReservationCanRelease: bool = s.release == NegativeRelease
  def witnessPositiveLegacyCoverageCanRelease: bool = all {
    s.phase == Finished, not(s.hasFence), s.release == PositiveRelease,
  }
  def witnessLegacyAbsenceCannotRelease: bool = all {
    s.phase == Finished, not(s.hasFence), not(s.cutContainsRoot), s.reservation,
  }
  def witnessCachedContinuationKeepsOriginalFence: bool = all {
    s.phase == Finished, s.continued, postCutRenewal, s.reservation,
  }
  def witnessRelabeledContinuationIsRejected: bool = all {
    s.phase == Finished, s.relabelRejected, postCutRenewal, s.reservation,
  }
  def witnessRestartPreservesPostCutProtection: bool = all {
    s.phase == Finished, s.restarted, s.hasFence, postCutRenewal, s.reservation,
  }
  def witnessRemoteClockCannotOrderLocalRenewal: bool = all {
    s.phase == Finished, s.remoteOffset != 0, s.hasFence, postCutRenewal, s.reservation,
  }
  def witnessExpiredCatalogCannotReject: bool = all {
    s.phase == Finished, s.stale, not(s.cutContainsRoot), s.reservation,
  }
  def witnessFutureLocalObservationCannotReject: bool = all {
    s.phase == Finished, s.cutStartWall > s.installWall, not(s.cutContainsRoot), s.reservation,
  }
  def witnessUnexpiredGraceCannotReject: bool = all {
    s.phase == Finished, not(s.graceElapsed), not(s.cutContainsRoot), s.reservation,
  }
  def witnessFailedFenceSyncCannotCreateCut: bool = all {
    s.phase == Reserved, s.failedCutSync, not(s.hasFence), s.frontier == 0,
  }
  def witnessLegacyReservationRemainsConservative: bool = all {
    s.phase == Finished, s.hasFence, not(s.reservationEpochKnown),
    not(s.cutContainsRoot), s.reservation,
  }
  def witnessForeignIncarnationCannotReject: bool = all {
    s.phase == Finished, not(s.matchingIncarnation), not(s.cutContainsRoot), s.reservation,
  }
  def witnessOldInitializedWriterIsFenced: bool = all {
    s.oldWriterRejected, s.hasFence, s.frontier == s.cutFence,
  }
  def witnessDuplicateMergePreservesNewerEpochAfterRollback: bool = all {
    s.phase == Finished, s.duplicateWallBugObserved, s.reservation,
  }
  def witnessUnknownDuplicatePreventsNegativeRelease: bool = all {
    s.phase == Finished, s.duplicate, not(s.duplicateKnown), not(s.cutContainsRoot), s.reservation,
  }
  def witnessForeignDuplicatePreventsNegativeRelease: bool = all {
    s.phase == Finished, s.duplicate, s.duplicateForeign, not(s.cutContainsRoot), s.reservation,
  }
}
