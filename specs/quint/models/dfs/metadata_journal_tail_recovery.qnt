/// Same-instance recovery for a durable DFS metadata-journal tail.
///
/// Evidence class: target hardening for the failure cut where journal append
/// succeeds durably, the preapplied in-memory table mutations are rolled back,
/// and table-batch materialization fails. The process remains alive, so the
/// next begin-write must compare the durable journal tail with the materialized
/// table proof. It may not admit another append while those tails differ.
///
/// This bounded model contains two ordered journal entries. A healthy write
/// appends and materializes one entry. The failure path leaves EntryOne durable
/// but absent from the materialized and live-table prefixes. The next
/// begin-write detects that gap, replays the exact durable prefix, and only then
/// admits EntryTwo. Reopening from the same failure cut uses the same replay
/// projection, making same-instance recovery and reopen recovery equivalent.
/// Torn journal records, persistence below the append boundary, arbitrary
/// replay failures, and concurrent writers remain outside this slice.
module dfs_metadata_journal_tail_recovery {
  import common.* from "../../lib/common"

  type JournalEntry = EntryOne | EntryTwo

  type RecoveryPhase =
    | Idle
    | WriteAdmitted
    | DurableAppendPendingMaterialization
    | RecoveringForBeginWrite
    | RecoveredBeginWritePendingAdmission

  type RecoveryTransition =
    | RecoveryInitialized
    | ContiguousWriteBegan
    | DurableAppendSucceeded
    | HealthyMaterializationSucceeded
    | TableBatchFailedAndRolledBack
    | BeginWriteDetectedDurableTail
    | DurableTailRecoveredBeforeAdmission
    | RecoveredWriteAdmitted
    | InstanceReopenedFromDurableTail
    | RecoveryStuttered

  type RecoveryState = {
    phase: RecoveryPhase,
    journalTail: int,
    materializedTail: int,
    journalEntries: Set[JournalEntry],
    materializedEntries: Set[JournalEntry],
    liveEntries: Set[JournalEntry],
    activeEntry: Option[JournalEntry],
    writeAdmissions: int,
    durableAppends: int,
    failedTableBatches: int,
    gapDetections: int,
    sameInstanceRecoveries: int,
    reopens: int,
    detectionAdmissionSnapshot: int,
    detectionAppendSnapshot: int,
    lastAdmissionJournalBase: int,
    lastAdmissionMaterializedBase: int,
    lastAppendJournalBase: int,
    lastAppendMaterializedBase: int,
    failureReplayProjection: Set[JournalEntry],
    lastTransition: RecoveryTransition,
  }

  pure val MaxEntries: int = 2

  pure def prefixEntries(endIndex: int): Set[JournalEntry] =
    if (endIndex <= 0) {
      Set()
    } else if (endIndex == 1) {
      Set(EntryOne)
    } else {
      Set(EntryOne, EntryTwo)
    }

  pure def nextEntry(endIndex: int): JournalEntry =
    if (endIndex == 0) EntryOne else EntryTwo

  pure def hasDurableTail(state: RecoveryState): bool =
    state.journalTail > state.materializedTail

  pure def journalProofIsContiguous(state: RecoveryState): bool =
    state.journalEntries == prefixEntries(state.journalTail)

  pure def materializedProofIsContiguous(state: RecoveryState): bool =
    state.materializedEntries == prefixEntries(state.materializedTail)

  /// Both recovery surfaces derive tables from this exact durable projection.
  pure def reopenReplayProjection(state: RecoveryState): Set[JournalEntry] =
    prefixEntries(state.journalTail)

  pure val initialState: RecoveryState = {
    phase: Idle,
    journalTail: 0,
    materializedTail: 0,
    journalEntries: Set(),
    materializedEntries: Set(),
    liveEntries: Set(),
    activeEntry: None,
    writeAdmissions: 0,
    durableAppends: 0,
    failedTableBatches: 0,
    gapDetections: 0,
    sameInstanceRecoveries: 0,
    reopens: 0,
    detectionAdmissionSnapshot: 0,
    detectionAppendSnapshot: 0,
    lastAdmissionJournalBase: 0,
    lastAdmissionMaterializedBase: 0,
    lastAppendJournalBase: 0,
    lastAppendMaterializedBase: 0,
    failureReplayProjection: Set(),
    lastTransition: RecoveryInitialized,
  }

  var recovery: RecoveryState

  action init = recovery' = initialState

  /// The ordinary admission path is enabled only on an exact materialized
  /// prefix. The base-tail audit fields make that precondition observable
  /// after the transition and at the later durable append.
  action beginContiguousWrite = all {
    recovery.phase == Idle,
    not(hasDurableTail(recovery)),
    recovery.journalTail < MaxEntries,
    recovery' = recovery
      .with("phase", WriteAdmitted)
      .with("activeEntry", Some(nextEntry(recovery.journalTail)))
      .with("writeAdmissions", recovery.writeAdmissions + 1)
      .with("lastAdmissionJournalBase", recovery.journalTail)
      .with("lastAdmissionMaterializedBase", recovery.materializedTail)
      .with("lastTransition", ContiguousWriteBegan),
  }

  /// Journal durability follows an admitted write whose base was contiguous.
  /// The live tables are the preapplied projection until table-batch append
  /// either succeeds or rolls them back.
  action appendJournalDurably = all {
    recovery.phase == WriteAdmitted,
    recovery.journalTail == recovery.materializedTail,
    recovery.activeEntry == Some(nextEntry(recovery.journalTail)),
    recovery.journalTail < MaxEntries,
    recovery' = recovery
      .with("phase", DurableAppendPendingMaterialization)
      .with("journalTail", recovery.journalTail + 1)
      .with(
        "journalEntries",
        recovery.journalEntries.union(Set(nextEntry(recovery.journalTail)))
      )
      .with(
        "liveEntries",
        recovery.liveEntries.union(Set(nextEntry(recovery.journalTail)))
      )
      .with("durableAppends", recovery.durableAppends + 1)
      .with("lastAppendJournalBase", recovery.journalTail)
      .with("lastAppendMaterializedBase", recovery.materializedTail)
      .with("lastTransition", DurableAppendSucceeded),
  }

  action materializeHealthyAppend = all {
    recovery.phase == DurableAppendPendingMaterialization,
    recovery' = recovery
      .with("phase", Idle)
      .with("materializedTail", recovery.journalTail)
      .with("materializedEntries", recovery.journalEntries)
      .with("liveEntries", recovery.journalEntries)
      .with("activeEntry", None)
      .with("lastTransition", HealthyMaterializationSucceeded),
  }

  /// This is the target failure cut. Journal durability is retained while the
  /// preapplied live-table mutations are rolled back to the last materialized
  /// prefix. The first-entry guard leaves one bounded entry available for the
  /// proof that a later append is admitted only after recovery.
  action failTableBatchAndRollback = all {
    recovery.phase == DurableAppendPendingMaterialization,
    recovery.journalTail == 1,
    recovery' = recovery
      .with("phase", Idle)
      .with("liveEntries", recovery.materializedEntries)
      .with("activeEntry", None)
      .with("failedTableBatches", recovery.failedTableBatches + 1)
      .with("failureReplayProjection", recovery.journalEntries)
      .with("lastTransition", TableBatchFailedAndRolledBack),
  }

  /// The next begin-write call does not increment the admission counter. It
  /// records that the durable journal outruns materialization and enters the
  /// recovery portion of that same admission attempt.
  action beginWriteDetectsDurableTail = all {
    recovery.phase == Idle,
    hasDurableTail(recovery),
    recovery.journalTail < MaxEntries,
    recovery' = recovery
      .with("phase", RecoveringForBeginWrite)
      .with("activeEntry", None)
      .with("gapDetections", recovery.gapDetections + 1)
      .with("detectionAdmissionSnapshot", recovery.writeAdmissions)
      .with("detectionAppendSnapshot", recovery.durableAppends)
      .with("lastTransition", BeginWriteDetectedDurableTail),
  }

  /// Replay closes the durable/materialized gap first and leaves the admission
  /// counter unchanged. This intermediate state makes the ordering observable:
  /// recovery is complete while the requested write is still not admitted.
  action recoverDurableTailForBeginWrite = all {
    recovery.phase == RecoveringForBeginWrite,
    hasDurableTail(recovery),
    recovery.journalTail < MaxEntries,
    recovery.failureReplayProjection == recovery.journalEntries,
    recovery' = recovery
      .with("phase", RecoveredBeginWritePendingAdmission)
      .with("materializedTail", recovery.journalTail)
      .with("materializedEntries", recovery.journalEntries)
      .with("liveEntries", recovery.journalEntries)
      .with("activeEntry", None)
      .with("sameInstanceRecoveries", recovery.sameInstanceRecoveries + 1)
      .with("lastTransition", DurableTailRecoveredBeforeAdmission),
  }

  /// Only the already-recovered, contiguous begin-write state may now admit
  /// the requested entry. Durable append remains a later transition.
  action admitRecoveredWrite = all {
    recovery.phase == RecoveredBeginWritePendingAdmission,
    not(hasDurableTail(recovery)),
    recovery.journalTail < MaxEntries,
    recovery.materializedEntries == recovery.journalEntries,
    recovery' = recovery
      .with("phase", WriteAdmitted)
      .with("activeEntry", Some(nextEntry(recovery.journalTail)))
      .with("writeAdmissions", recovery.writeAdmissions + 1)
      .with("lastAdmissionJournalBase", recovery.journalTail)
      .with("lastAdmissionMaterializedBase", recovery.journalTail)
      .with("lastTransition", RecoveredWriteAdmitted),
  }

  /// Crash/reopen from the identical durable-tail cut reconstructs the same
  /// prefix as same-instance recovery. Process identity is intentionally not
  /// part of the table/journal proof projection.
  action reopenFromDurableTail = all {
    recovery.phase == Idle,
    hasDurableTail(recovery),
    recovery.failureReplayProjection == recovery.journalEntries,
    recovery' = recovery
      .with("materializedTail", recovery.journalTail)
      .with("materializedEntries", reopenReplayProjection(recovery))
      .with("liveEntries", reopenReplayProjection(recovery))
      .with("activeEntry", None)
      .with("reopens", recovery.reopens + 1)
      .with("lastTransition", InstanceReopenedFromDurableTail),
  }

  action stutter =
    recovery' = recovery.with("lastTransition", RecoveryStuttered)

  action step = any {
    beginContiguousWrite,
    appendJournalDurably,
    materializeHealthyAppend,
    failTableBatchAndRollback,
    beginWriteDetectsDurableTail,
    recoverDurableTailForBeginWrite,
    admitRecoveredWrite,
    reopenFromDurableTail,
    stutter,
  }

  /// Coverage-only schedulers call the real transition actions. Symbolic
  /// verification uses `step`; the harness mixes these schedules into random
  /// execution so each bounded causal cut remains a named witness.
  action healthyCoverageStep =
    if (recovery.phase == Idle and recovery.journalTail == 0) {
      beginContiguousWrite
    } else if (recovery.phase == WriteAdmitted) {
      appendJournalDurably
    } else if (recovery.phase == DurableAppendPendingMaterialization) {
      materializeHealthyAppend
    } else {
      stutter
    }

  action failureRecoveryCoverageStep =
    if (recovery.phase == Idle and recovery.journalTail == 0) {
      beginContiguousWrite
    } else if (recovery.phase == WriteAdmitted and recovery.journalTail == 0) {
      appendJournalDurably
    } else if (recovery.phase == DurableAppendPendingMaterialization) {
      failTableBatchAndRollback
    } else if (recovery.phase == Idle and hasDurableTail(recovery)) {
      beginWriteDetectsDurableTail
    } else if (recovery.phase == RecoveringForBeginWrite) {
      recoverDurableTailForBeginWrite
    } else if (recovery.phase == RecoveredBeginWritePendingAdmission) {
      admitRecoveredWrite
    } else {
      stutter
    }

  action reopenCoverageStep =
    if (recovery.phase == Idle and recovery.journalTail == 0) {
      beginContiguousWrite
    } else if (recovery.phase == WriteAdmitted) {
      appendJournalDurably
    } else if (recovery.phase == DurableAppendPendingMaterialization) {
      failTableBatchAndRollback
    } else if (recovery.phase == Idle and hasDurableTail(recovery)) {
      reopenFromDurableTail
    } else {
      stutter
    }

  action coverageStep = any {
    step,
    healthyCoverageStep,
    failureRecoveryCoverageStep,
    reopenCoverageStep,
  }
}
