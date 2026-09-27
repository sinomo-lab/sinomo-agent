/// Target refinement: authored final namespace-commit -> terminal-intent batch.
/// The unchanged canonical prefix is I(pending intent), R(exact W receipts),
/// P(hidden namespace prepare), at sequences 1..3. This model adds the existing
/// C(namespace commit), T(terminal intent) frames at 4 and 5, without changing
/// their order or byte identities. Complete prefixes are validated exact frames;
/// complete malformed frames are separate from an incomplete EOF suffix.
///
/// A root-owned process certificate survives handle replacement. A new process
/// starts uncertified even if it can decode complete files. Only successful file
/// and required directory sync can certify bytes. The canonical writer flock
/// serializes authors; independent concurrent multiprocess allocators, full
/// namespace/W/epoch validation, cryptographic framing and real disk behavior
/// remain external refinement obligations.
module dfs_authored_journal_batch {
  type Phase = Ready | Appending | NeedsRecovery | DataCertified | TablePending
  type Certificate = Certified | Uncertain
  type Last = Initialized | BatchStarted | FrameWritten | FrameTorn
    | FileSyncSucceeded | FileSyncFailed | DirectorySyncSucceeded | DirectorySyncFailed
    | BatchCertified | HandleReplaced | ProcessReopened | DecodeOnlyObserved
    | TailTruncated | RecoverySyncFailed | TailCertified | CompleteCorruptionRejected
    | TablePlanned | TableAppendFailed | TableApplied | CommittedReply
    | LostCommittedReply | AdmissionRejected | PayloadRejected | CompleteCorruptionInstalled
    | PhysicalGenerationReplaced | GenerationChangeDetected | ForeignRootObservationRejected

  type State = {
    // Complete valid physical frames, known durable prefix, and successful
    // file-sync prefix. With rollover, sequence 5 also needs its directory.
    complete: int, durable: int, fileSynced: int, directorySynced: bool,
    rollover: bool, torn: bool, completeCorrupt: bool,
    nextSequence: int, certificate: Certificate, phase: Phase,
    diskGeneration: int, certifiedGeneration: int, certifiedPrefix: int,
    rootIdentity: int, handleRootIdentity: int,
    tableHead: int, liveHead: int, referenceSequence: int, referenceCost: int,
    plannedHead: int, deferredResolved: bool,
    // Canonical parent/exclusive admission and operation-bound authority are
    // inputs retained at C's publication boundary, not inferred from a hint.
    admissionValid: bool, authorityValid: bool,
    handleGeneration: int, decodedObservation: int, reopenCount: int,
    fileSyncCount: int, replies: int, lastAppendSequence: int, lastAppendExpected: int,
    batchAllocatedStart: int, batchWritten: int,
    sawTorn: bool, sawFailedSync: bool, sawFailedDirectory: bool,
    sawReplacementWhileUncertain: bool, sawDecodeWithoutCertificate: bool,
    sawCommitOnlyRecovery: bool, sawDeferredPrepare: bool,
    sawDeferredCommit: bool, sawPreapplyRollback: bool, sawRollover: bool,
    sawGenerationReplacement: bool, sawGenerationRecertified: bool,
    sawPostCommitTableLoss: bool,
    last: Last,
  }

  pure val initial: State = {
    complete: 3, durable: 3, fileSynced: 3, directorySynced: false,
    rollover: false, torn: false, completeCorrupt: false,
    nextSequence: 4, certificate: Certified, phase: Ready,
    diskGeneration: 0, certifiedGeneration: 0, certifiedPrefix: 3,
    rootIdentity: 1, handleRootIdentity: 1,
    tableHead: 3, liveHead: 3, referenceSequence: 3, referenceCost: 1,
    plannedHead: 3, deferredResolved: false,
    admissionValid: true, authorityValid: true,
    handleGeneration: 0, decodedObservation: 0, reopenCount: 0,
    fileSyncCount: 0, replies: 0, lastAppendSequence: 0, lastAppendExpected: 0,
    batchAllocatedStart: 4, batchWritten: 0,
    sawTorn: false, sawFailedSync: false, sawFailedDirectory: false,
    sawReplacementWhileUncertain: false, sawDecodeWithoutCertificate: false,
    sawCommitOnlyRecovery: false, sawDeferredPrepare: false,
    sawDeferredCommit: false, sawPreapplyRollback: false, sawRollover: false,
    sawGenerationReplacement: false, sawGenerationRecertified: false,
    sawPostCommitTableLoss: false,
    last: Initialized,
  }
  var s: State

  /// 3=I/R/P, 4=I/R/P/C, 5=I/R/P/C/T. The validity assumption includes
  /// exact sequence, payload identity, digest and operation-bound authority.
  pure def exactPrefix(n: int): bool = n >= 3 and n <= 5
  pure def certifiedPhysical(x: State): bool = all {
    x.certificate == Certified, not(x.torn), not(x.completeCorrupt),
    x.certifiedGeneration == x.diskGeneration, x.certifiedPrefix == x.durable,
    x.complete == x.durable, x.fileSynced >= x.durable,
    x.durable == 5 and x.rollover implies x.directorySynced,
    x.nextSequence == x.durable + 1,
  }
  pure def admitted(x: State): bool = all {
    x.phase == Ready, certifiedPhysical(x), x.handleRootIdentity == x.rootIdentity,
    x.tableHead == x.durable, x.liveHead == x.tableHead,
  }
  pure def recoveryOwnerExists(x: State): bool =
    exactPrefix(x.durable) // Durable I/R/P retains exact terminalization owner.

  action init = {
    nondet split = Set(false, true).oneOf()
    nondet oldTable = Set(false, true).oneOf()
    s' = initial.with("rollover", split)
      .with("phase", if (oldTable) DataCertified else Ready)
      .with("tableHead", if (oldTable) 0 else 3)
      .with("liveHead", if (oldTable) 0 else 3)
      .with("referenceSequence", if (oldTable) 0 else 3)
      .with("referenceCost", if (oldTable) 0 else 1)
  }

  action beginFinalBatch = all {
    admitted(s), s.complete < 5,
    // At a recovered C-only prefix, finish T from C's exact durable payload;
    // do not reconstruct the child/parent or repeat a fresh create decision.
    s.complete == 4 or (s.admissionValid and s.authorityValid),
    s' = s.with("certificate", Uncertain).with("phase", Appending)
      .with("batchAllocatedStart", s.nextSequence).with("batchWritten", 0)
      .with("last", BatchStarted),
  }
  action appendExactFrame = all {
    s.phase == Appending, not(s.torn), not(s.completeCorrupt), s.complete < 5,
    // A rollover starts the new segment only after the preceding one is synced.
    not(s.rollover and s.complete == 4) or s.fileSynced >= 4,
    s' = s.with("complete", s.complete + 1).with("diskGeneration", s.diskGeneration + 1)
      .with("lastAppendSequence", s.batchAllocatedStart + s.batchWritten)
      .with("batchWritten", s.batchWritten + 1)
      .with("lastAppendExpected", s.complete + 1).with("last", FrameWritten),
  }
  action appendTornFrame = all {
    s.phase == Appending, s.complete < 5, not(s.torn),
    not(s.rollover and s.complete == 4) or s.fileSynced >= 4,
    s' = s.with("torn", true).with("diskGeneration", s.diskGeneration + 1).with("phase", NeedsRecovery)
      .with("certificate", Uncertain).with("sawTorn", true).with("last", FrameTorn),
  }
  action syncCurrentSegment = all {
    s.phase == Appending, not(s.torn), not(s.completeCorrupt),
    s.complete > s.fileSynced, s.complete == 5 or (s.rollover and s.complete == 4),
    val certifiedEnd = if (s.rollover and s.complete == 5 and not(s.directorySynced)) 4 else s.complete
    s' = s.with("fileSynced", s.complete).with("durable", certifiedEnd)
      .with("fileSyncCount", s.fileSyncCount + 1)
      .with("nextSequence", certifiedEnd + 1).with("last", FileSyncSucceeded),
  }
  /// A write can fail after C but before T; fsync can fail after the whole
  /// chunk. Both leave complete visible bytes uncertified.
  action failCurrentSync = all {
    s.phase == Appending, s.complete > s.durable,
    s' = s.with("phase", NeedsRecovery).with("certificate", Uncertain)
      .with("sawFailedSync", true).with("last", FileSyncFailed),
  }
  action syncNewSegmentDirectory = all {
    s.phase == Appending, s.rollover, s.complete == 5, s.fileSynced == 5,
    not(s.directorySynced),
    s' = s.with("directorySynced", true).with("durable", 5)
      .with("nextSequence", 6).with("sawRollover", true).with("last", DirectorySyncSucceeded),
  }
  action failNewSegmentDirectory = all {
    s.phase == Appending, s.rollover, s.complete == 5, s.fileSynced == 5,
    not(s.directorySynced),
    s' = s.with("phase", NeedsRecovery).with("certificate", Uncertain)
      .with("sawFailedDirectory", true).with("last", DirectorySyncFailed),
  }
  action finishCertifiedBatch = all {
    s.phase == Appending, s.complete == 5, s.durable == 5,
    not(s.torn), not(s.completeCorrupt),
    s' = s.with("phase", DataCertified).with("certificate", Certified)
      .with("certifiedGeneration", s.diskGeneration).with("certifiedPrefix", s.durable)
      .with("last", BatchCertified),
  }

  /// A second resident handle sees the same strong root registry. It can read
  /// bytes but cannot replace an Uncertain certificate with a sequence guess.
  action replaceHandle = all {
    s.phase != Appending, s.phase != TablePending, s.handleGeneration < 2,
    s' = s.with("handleGeneration", s.handleGeneration + 1)
      .with("decodedObservation", s.complete)
      .with("sawReplacementWhileUncertain", s.sawReplacementWhileUncertain or s.certificate == Uncertain)
      .with("last", HandleReplaced),
  }
  action reopenProcess = {
    nondet surviving = Set(3, 4, 5).oneOf()
    nondet loseDerivedTables = Set(false, true).oneOf()
    all {
      s.reopenCount < 2, surviving >= s.durable, surviving <= s.complete,
      s' = s.with("complete", surviving).with("fileSynced", s.durable)
        .with("torn", s.torn and surviving == s.complete)
        .with("tableHead", if (loseDerivedTables) 0 else s.tableHead)
        .with("liveHead", if (loseDerivedTables) 0 else s.tableHead)
        .with("referenceSequence", if (loseDerivedTables) 0 else s.referenceSequence)
        .with("referenceCost", if (loseDerivedTables) 0 else s.referenceCost)
        .with("sawPostCommitTableLoss", s.sawPostCommitTableLoss or (loseDerivedTables and s.tableHead == 5))
        .with("deferredResolved", false)
        .with("certificate", Uncertain).with("phase", NeedsRecovery)
        .with("decodedObservation", surviving).with("reopenCount", s.reopenCount + 1)
        .with("diskGeneration", s.diskGeneration + 1)
        .with("last", ProcessReopened),
    }
  }
  action decodeOnly = all {
    s.phase == NeedsRecovery, not(s.completeCorrupt),
    s' = s.with("decodedObservation", s.complete)
      .with("sawDecodeWithoutCertificate", true).with("last", DecodeOnlyObserved),
  }
  action truncateIncompleteTail = all {
    s.phase == NeedsRecovery, s.torn, not(s.completeCorrupt),
    s' = s.with("torn", false).with("diskGeneration", s.diskGeneration + 1).with("last", TailTruncated),
  }
  action failRecoverySync = all {
    s.phase == NeedsRecovery,
    s' = s.with("certificate", Uncertain).with("last", RecoverySyncFailed),
  }
  /// Abstracts successful validation + file resync + required parent sync.
  /// Decoder success alone is intentionally a different action above.
  action certifyRecoveredTail = all {
    s.phase == NeedsRecovery, not(s.torn), not(s.completeCorrupt),
    s' = s.with("durable", s.complete).with("fileSynced", s.complete)
      .with("fileSyncCount", s.fileSyncCount + 1)
      .with("directorySynced", s.directorySynced or (s.rollover and s.complete == 5))
      .with("nextSequence", s.complete + 1).with("certificate", Certified)
      .with("phase", DataCertified)
      .with("certifiedGeneration", s.diskGeneration).with("certifiedPrefix", s.complete)
      .with("sawGenerationRecertified", s.sawGenerationRecertified or s.sawGenerationReplacement)
      .with("sawCommitOnlyRecovery", s.sawCommitOnlyRecovery or s.complete == 4)
      .with("last", TailCertified),
  }
  action installCompleteCorruption = all {
    s.phase == NeedsRecovery, s.complete > 3, not(s.completeCorrupt),
    s' = s.with("completeCorrupt", true).with("diskGeneration", s.diskGeneration + 1)
      .with("last", CompleteCorruptionInstalled),
  }
  action rejectCompleteCorruption = all {
    s.phase == NeedsRecovery, s.completeCorrupt,
    s' = s.with("certificate", Uncertain).with("last", CompleteCorruptionRejected),
  }

  action planTableReplay = all {
    s.phase == DataCertified, certifiedPhysical(s),
    val deferred = s.durable < 5 and s.tableHead < s.durable
    // P alone and C alone are trailing resolved operations. With C,T the
    // commit is non-trailing and is preapplied with rollback before table IO.
    val provisional = if (deferred) s.durable - 1 else s.durable
    s' = s.with("plannedHead", s.durable).with("phase", TablePending)
      .with("liveHead", if (provisional > s.tableHead) provisional else s.tableHead)
      .with("deferredResolved", deferred)
      .with("sawDeferredPrepare", s.sawDeferredPrepare or (deferred and s.durable == 3))
      .with("sawDeferredCommit", s.sawDeferredCommit or (deferred and s.durable == 4))
      .with("last", TablePlanned),
  }
  action failTableAppend = all {
    s.phase == TablePending,
    s' = s.with("liveHead", s.tableHead).with("phase", DataCertified)
      .with("sawPreapplyRollback", s.sawPreapplyRollback or (s.plannedHead == 5 and s.tableHead < 4))
      .with("deferredResolved", false).with("last", TableAppendFailed),
  }
  action applyTableBatch = all {
    s.phase == TablePending,
    s' = s.with("tableHead", s.plannedHead).with("liveHead", s.plannedHead)
      .with("referenceSequence", 3).with("referenceCost", 1)
      .with("deferredResolved", false).with("phase", Ready).with("last", TableApplied),
  }
  action replyCommitted = all {
    admitted(s), s.durable == 5,
    s' = s.with("replies", s.replies + 1).with("last", CommittedReply),
  }
  action loseCommittedReply = all {
    admitted(s), s.durable == 5,
    s' = s.with("last", LostCommittedReply),
  }
  action rejectChangedPayload = all {
    s.phase == Ready,
    s' = s.with("last", PayloadRejected),
  }
  action revokeFreshCreateAdmission = all {
    s.phase == Ready, s.complete == 3, s.admissionValid,
    s' = s.with("admissionValid", false).with("last", AdmissionRejected),
  }

  /// Opaque generation stands for canonical root/file identity, exact bytes
  /// and validated prefix evidence. Equal length/head alone cannot preserve a
  /// certificate through atomic replacement, reset, or a different root.
  action replacePhysicalGeneration = all {
    s.phase == Ready, not(s.sawGenerationReplacement),
    s' = s.with("diskGeneration", s.diskGeneration + 1)
      .with("sawGenerationReplacement", true).with("last", PhysicalGenerationReplaced),
  }
  action detectChangedGeneration = all {
    s.phase == Ready, s.certifiedGeneration != s.diskGeneration,
    s' = s.with("certificate", Uncertain).with("phase", NeedsRecovery)
      .with("last", GenerationChangeDetected),
  }
  action rejectForeignRootObservation = all {
    s.phase == Ready,
    s' = s.with("handleRootIdentity", 2).with("last", ForeignRootObservationRejected),
  }
  action restoreOwnedHandle = all {
    s.handleRootIdentity != s.rootIdentity,
    s' = s.with("handleRootIdentity", s.rootIdentity).with("last", HandleReplaced),
  }

  action step = any {
    beginFinalBatch, appendExactFrame, appendTornFrame, syncCurrentSegment,
    failCurrentSync, syncNewSegmentDirectory, failNewSegmentDirectory,
    finishCertifiedBatch, replaceHandle, reopenProcess, decodeOnly,
    truncateIncompleteTail, failRecoverySync, certifyRecoveredTail,
    installCompleteCorruption, rejectCompleteCorruption,
    planTableReplay, failTableAppend, applyTableBatch,
    replyCommitted, loseCommittedReply, rejectChangedPayload, revokeFreshCreateAdmission,
    replacePhysicalGeneration, detectChangedGeneration, rejectForeignRootObservation, restoreOwnedHandle,
  }
  /// Simulation-only bias toward successful completion. Every action remains
  /// an existing step alternative; bounded verification uses unrestricted step.
  action progress = any {
    beginFinalBatch, appendExactFrame, syncCurrentSegment,
    syncNewSegmentDirectory, finishCertifiedBatch, truncateIncompleteTail,
    certifyRecoveredTail, planTableReplay, applyTableBatch, replyCommitted,
    restoreOwnedHandle, detectChangedGeneration,
  }
  action coverageStep = any { step, progress, progress, progress }

}
