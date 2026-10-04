/// Safety obligations and review witnesses for final authored journal batching.
module dfs_authored_journal_batch_safety {
  import dfs_authored_journal_batch.* from "../../models/dfs/authored_journal_batch"

  def prefixesAreOrdered = all {
    exactPrefix(s.complete), exactPrefix(s.durable),
    s.durable <= s.complete, s.durable <= s.fileSynced, s.fileSynced <= s.complete,
    s.tableHead >= 0, s.tableHead <= s.durable,
    s.liveHead >= s.tableHead, s.liveHead <= s.durable,
  }
  def uncertainRootBlocksAdmission =
    s.certificate == Uncertain implies not(admitted(s))
  def knownSequenceMatchesCertificate =
    certifiedPhysical(s) implies s.nextSequence == s.durable + 1
  def noConsumedSequenceIsReused =
    s.lastAppendSequence == s.lastAppendExpected
  def decodedReopenIsNotDurability = s.last == DecodeOnlyObserved implies all {
    s.certificate == Uncertain, s.phase == NeedsRecovery, not(admitted(s)),
  }
  def failedRecoveryRetainsRootFence = s.last == RecoverySyncFailed implies all {
    s.certificate == Uncertain, s.phase == NeedsRecovery, not(admitted(s)),
  }
  def rolloverRequiresDirectoryBarrier =
    s.rollover and s.durable == 5 implies s.directorySynced
  def completeCorruptionNeverRecertifies =
    s.completeCorrupt implies s.certificate == Uncertain and not(admitted(s))
  def acknowledgedCommitIncludesTerminalOwnership =
    s.last == CommittedReply implies all {
      admitted(s), s.durable == 5, s.tableHead == 5,
      not(s.torn), not(s.completeCorrupt), recoveryOwnerExists(s),
    }
  def visibleCommitAlwaysHasRecoveryOwner =
    s.tableHead >= 4 implies recoveryOwnerExists(s)
  def terminalOnlyRecoveryDoesNotNeedFreshCreateAdmission =
    s.sawCommitOnlyRecovery and s.phase == Ready and s.durable == 4
      implies recoveryOwnerExists(s) and s.nextSequence == 5
  def tableReferenceAccountingIsRetained = all {
    s.tableHead >= 3 implies s.referenceSequence == 3 and s.referenceCost == 1,
    s.tableHead == 0 implies s.referenceSequence == 0 and s.referenceCost == 0,
    s.phase != TablePending implies s.liveHead == s.tableHead,
  }
  def deferredResolvedOpCannotLeakPublication =
    s.phase == TablePending and s.deferredResolved implies all {
      s.liveHead < s.plannedHead,
      not(admitted(s)),
    }
  def failedTableAppendRollsBackOnlyProjection = s.last == TableAppendFailed implies all {
    s.liveHead == s.tableHead, s.phase == DataCertified,
    s.certificate == Certified, s.durable == s.plannedHead,
    not(admitted(s)),
  }
  def generationOrRootMismatchCannotAuthorize =
    any { s.certifiedGeneration != s.diskGeneration, s.handleRootIdentity != s.rootIdentity }
      implies not(admitted(s))
  def safety = all {
    prefixesAreOrdered, uncertainRootBlocksAdmission, knownSequenceMatchesCertificate,
    noConsumedSequenceIsReused, decodedReopenIsNotDurability,
    failedRecoveryRetainsRootFence, rolloverRequiresDirectoryBarrier,
    completeCorruptionNeverRecertifies, acknowledgedCommitIncludesTerminalOwnership,
    visibleCommitAlwaysHasRecoveryOwner, terminalOnlyRecoveryDoesNotNeedFreshCreateAdmission,
    tableReferenceAccountingIsRetained, deferredResolvedOpCannotLeakPublication,
    failedTableAppendRollsBackOnlyProjection, generationOrRootMismatchCannotAuthorize,
  }

  def witnessPostCommitReopenLosesOnlyDerivedTables = all {
    s.last == ProcessReopened, s.sawPostCommitTableLoss, s.tableHead == 0,
    s.durable == 5, s.referenceSequence == 0, s.referenceCost == 0,
    s.certificate == Uncertain,
  }
  def witnessLostDerivedTablesRebuildBeforeAck = all {
    s.last == CommittedReply, s.sawPostCommitTableLoss,
    s.tableHead == 5, s.referenceSequence == 3, s.referenceCost == 1,
    certifiedPhysical(s),
  }
  def witnessSameHeadReplacementInvalidatesCertificate =
    s.last == PhysicalGenerationReplaced and not(admitted(s))
  def witnessReplacementRequiresExplicitRecertification =
    s.last == CommittedReply and s.sawGenerationRecertified
  def witnessForeignRootCannotBorrowCertificate =
    s.last == ForeignRootObservationRejected and not(admitted(s))
  def witnessHealthySharedSyncFinalBatch =
    s.last == CommittedReply and not(s.rollover) and s.fileSyncCount == 1
      and not(s.sawFailedSync) and not(s.sawTorn)
  def witnessCompleteFailedSyncStaysFenced =
    s.last == FileSyncFailed and s.complete == 5 and s.durable < 5
  def witnessPartialCompletePrefixRecovered =
    s.last == TailCertified and s.complete == 4
  def witnessTornTailTruncatesWithoutDroppingCompleteFrames =
    s.last == TailTruncated and s.complete == 4
  def witnessFailedRecoveryDoesNotUnfence = s.last == RecoverySyncFailed
  def witnessReplacementCannotEraseUncertainty = all {
    s.last == HandleReplaced, s.sawReplacementWhileUncertain,
    s.certificate == Uncertain,
  }
  def witnessDecodeOnlyReopenCannotAcknowledge =
    s.last == DecodeOnlyObserved and s.reopenCount > 0
  def witnessRecoveryResyncThenCommittedReply =
    s.last == CommittedReply and s.sawFailedSync
  def witnessCommitOnlyRecoveryEventuallyTerminalizes =
    s.last == CommittedReply and s.sawCommitOnlyRecovery
  def witnessRolloverNeedsSecondFileAndDirectory =
    s.last == CommittedReply and s.rollover and s.sawRollover
  def witnessDirectoryFailureKeepsTerminalUncertified =
    s.last == DirectorySyncFailed and s.complete == 5 and s.durable == 4
  def witnessDeferredPrepareReferenceReplays =
    s.last == TableApplied and s.sawDeferredPrepare and s.referenceSequence == 3
  def witnessDeferredCommitAfterPartialPrefixReplays =
    s.last == TableApplied and s.sawDeferredCommit and s.tableHead == 4
  def witnessNonTrailingCommitRollsBackOnTableFailure =
    s.last == TableAppendFailed and s.sawPreapplyRollback
  def witnessCompleteMalformedFrameFailsClosed = s.last == CompleteCorruptionRejected
  def witnessChangedPayloadRejected = s.last == PayloadRejected
  def witnessFreshCreateAdmissionStillRequired =
    s.last == AdmissionRejected and s.complete == 3 and s.tableHead == 3
  def witnessLostReplyCanReplayExactCommittedOutcome =
    s.last == LostCommittedReply and s.durable == 5 and s.tableHead == 5
}
