/// Crash/reopen recovery when the persisted metadata-journal head is stale.
///
/// Evidence class: target hardening for the cut where both the durable segment
/// tail and the materialized table proof have reached N, but `head.sobj` still
/// contains N - 1. The persisted head is a recovery hint, not sequence
/// authority. Reopen must derive an allocator floor of
/// `max(segment tail, materialized proof) + 1`, promote the in-memory and
/// persisted next-sequence values to at least that floor, and only then admit
/// another mutation.
///
/// This bounded model fixes N = 2 and appends one post-reopen entry. It models
/// both a healthy N + 1 head and the stale N - 1 failure image. Torn segment or
/// table records, disagreement between the durable segment and materialized
/// proof, integer exhaustion, and concurrent writers remain outside this
/// slice.
module dfs_metadata_stale_head_reopen {
  type ReopenPhase =
    | ClosedAtDurablePrefix
    | RepairRequired
    | OpenReady
    | WriteAdmitted
    | AppendComplete

  type ReopenTransition =
    | ReopenInitialized
    | HealthyHeadAccepted
    | StaleHeadObserved
    | ReopenHeadRepaired
    | RepairedWriteAdmitted
    | NextAppendCommitted
    | ReopenStuttered

  type ReopenState = {
    phase: ReopenPhase,
    durableSegmentTail: int,
    materializedProof: int,
    persistedNextSequence: int,
    observedHeadHint: int,
    nextSequence: int,
    activeSequence: int,
    admissionSequence: int,
    lastAppendedSequence: int,
    writeAdmissions: int,
    durableAppends: int,
    repairs: int,
    observationAdmissionSnapshot: int,
    observationAppendSnapshot: int,
    staleHeadWasObserved: bool,
    repairedBeforeAdmission: bool,
    lastTransition: ReopenTransition,
  }

  pure val DurableSequenceN: int = 2

  pure def max2(left: int, right: int): int =
    if (left >= right) left else right

  /// The persisted next-sequence hint may raise this value, but it may never
  /// lower the floor proved by durable segment and materialized state.
  pure def durableRecoveryFloor(state: ReopenState): int =
    max2(state.durableSegmentTail, state.materializedProof) + 1

  pure def recoveredNextSequence(state: ReopenState): int =
    max2(state.persistedNextSequence, durableRecoveryFloor(state))

  pure val initialState: ReopenState = {
    phase: ClosedAtDurablePrefix,
    durableSegmentTail: DurableSequenceN,
    materializedProof: DurableSequenceN,
    persistedNextSequence: 0,
    observedHeadHint: 0,
    nextSequence: 0,
    activeSequence: 0,
    admissionSequence: 0,
    lastAppendedSequence: 0,
    writeAdmissions: 0,
    durableAppends: 0,
    repairs: 0,
    observationAdmissionSnapshot: 0,
    observationAppendSnapshot: 0,
    staleHeadWasObserved: false,
    repairedBeforeAdmission: false,
    lastTransition: ReopenInitialized,
  }

  var reopen: ReopenState

  action init = reopen' = initialState

  /// Healthy reopen may accept an already-current N + 1 hint directly.
  action reopenWithHealthyHead = all {
    reopen.phase == ClosedAtDurablePrefix,
    reopen' = reopen
      .with("phase", OpenReady)
      .with("persistedNextSequence", DurableSequenceN + 1)
      .with("observedHeadHint", DurableSequenceN + 1)
      .with("nextSequence", DurableSequenceN + 1)
      .with("observationAdmissionSnapshot", reopen.writeAdmissions)
      .with("observationAppendSnapshot", reopen.durableAppends)
      .with("lastTransition", HealthyHeadAccepted),
  }

  /// The target crash image exposes the stale hint, but leaves the allocator
  /// uninitialized and write admission disabled until a separate repair step.
  action observeStaleHeadOnReopen = all {
    reopen.phase == ClosedAtDurablePrefix,
    reopen' = reopen
      .with("phase", RepairRequired)
      .with("persistedNextSequence", DurableSequenceN - 1)
      .with("observedHeadHint", DurableSequenceN - 1)
      .with("nextSequence", 0)
      .with("activeSequence", 0)
      .with("observationAdmissionSnapshot", reopen.writeAdmissions)
      .with("observationAppendSnapshot", reopen.durableAppends)
      .with("staleHeadWasObserved", true)
      .with("lastTransition", StaleHeadObserved),
  }

  /// Reopen promotes both the process-local allocator and durable head cache
  /// before it can transition to an admission-capable phase.
  action repairStaleHeadBeforeAdmission = all {
    reopen.phase == RepairRequired,
    reopen.persistedNextSequence == DurableSequenceN - 1,
    reopen.observedHeadHint == DurableSequenceN - 1,
    reopen.nextSequence == 0,
    reopen.writeAdmissions == reopen.observationAdmissionSnapshot,
    reopen.durableAppends == reopen.observationAppendSnapshot,
    reopen' = reopen
      .with("phase", OpenReady)
      .with("persistedNextSequence", recoveredNextSequence(reopen))
      .with("nextSequence", recoveredNextSequence(reopen))
      .with("repairs", reopen.repairs + 1)
      .with("repairedBeforeAdmission", true)
      .with("lastTransition", ReopenHeadRepaired),
  }

  action admitWrite = all {
    reopen.phase == OpenReady,
    reopen.nextSequence >= durableRecoveryFloor(reopen),
    not(reopen.staleHeadWasObserved) or reopen.repairedBeforeAdmission,
    reopen' = reopen
      .with("phase", WriteAdmitted)
      .with("activeSequence", reopen.nextSequence)
      .with("admissionSequence", reopen.nextSequence)
      .with("writeAdmissions", reopen.writeAdmissions + 1)
      .with("lastTransition", RepairedWriteAdmitted),
  }

  /// The admitted sequence is consumed exactly once. The in-memory allocator
  /// advances immediately to the following sequence; the persisted head may
  /// remain a recovery cache because a later reopen will audit durable proof.
  action appendAdmittedWrite = all {
    reopen.phase == WriteAdmitted,
    reopen.activeSequence == reopen.admissionSequence,
    reopen.activeSequence >= durableRecoveryFloor(reopen),
    reopen' = reopen
      .with("phase", AppendComplete)
      .with("durableSegmentTail", reopen.activeSequence)
      .with("nextSequence", reopen.activeSequence + 1)
      .with("activeSequence", 0)
      .with("lastAppendedSequence", reopen.activeSequence)
      .with("durableAppends", reopen.durableAppends + 1)
      .with("lastTransition", NextAppendCommitted),
  }

  action stutter =
    reopen' = reopen.with("lastTransition", ReopenStuttered)

  action step = any {
    reopenWithHealthyHead,
    observeStaleHeadOnReopen,
    repairStaleHeadBeforeAdmission,
    admitWrite,
    appendAdmittedWrite,
    stutter,
  }

  /// Coverage schedules call only the real transition actions. Symbolic
  /// verification continues to explore the unrestricted `step` relation.
  action healthyCoverageStep =
    if (reopen.phase == ClosedAtDurablePrefix) {
      reopenWithHealthyHead
    } else {
      stutter
    }

  action staleRecoveryCoverageStep =
    if (reopen.phase == ClosedAtDurablePrefix) {
      observeStaleHeadOnReopen
    } else if (reopen.phase == RepairRequired) {
      repairStaleHeadBeforeAdmission
    } else if (reopen.phase == OpenReady) {
      admitWrite
    } else if (reopen.phase == WriteAdmitted) {
      appendAdmittedWrite
    } else {
      stutter
    }

  action coverageStep = any {
    step,
    healthyCoverageStep,
    staleRecoveryCoverageStep,
  }
}
