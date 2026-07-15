/// Bootstrap a desired-only metadata member during a pending handoff.
///
/// Evidence class: production liveness counterexample plus target hardening.
/// The retiring assignment is `{N1,N2}` and the desired assignment is
/// `{N1,N3}`. N3 starts behind the frozen retiring prefix. The old receiver
/// admitted an exact prefix probe for N3 but rejected the ordinary contiguous
/// journal suffix required to make that probe eligible, so the handoff could
/// remain frozen forever.
///
/// The fixed admission rule treats the exact pending handoff as a scoped
/// replica-ingest permit. It accepts only the retiring primary, filesystem,
/// epoch, shard, desired member, and contiguous frozen suffix. This does not
/// bind author authority: activation still requires the later exact-prefix
/// acknowledgement and the full-union handoff proof.
module dfs_metadata_handoff_desired_catchup {
  import common.* from "../../lib/common"

  type PendingHandoffEvidence =
    | ExactPendingHandoff
    | MissingPendingHandoff
    | DifferentDesiredMember

  type CatchupAuthorityEvidence =
    | ExactRetiringAuthority
    | ForgedSourceAuthority
    | StaleEpochAuthority
    | ForeignFileSystemAuthority

  type CatchupBatchEvidence = ExactContiguousBatch | NoncontiguousBatch
  type CatchupOutcome = CatchupNotAttempted | CatchupRejected | CatchupAccepted
  type DesiredCatchupTransition =
    | DesiredCatchupInitialized
    | OldExactCatchupRejected
    | FixedCatchupRejected
    | FixedCatchupAccepted
    | FixedPrefixAcknowledged
    | FixedHandoffActivated
    | DesiredCatchupStuttered

  type DesiredCatchupState = {
    frozenSequence: int,
    oldReceiverSequence: int,
    oldSenderCursor: int,
    oldExactCatchupAttempted: bool,
    oldPrefixAcknowledged: bool,
    oldActivated: bool,
    fixedReceiverSequence: int,
    fixedSenderCursor: int,
    previousFixedReceiverSequence: int,
    previousFixedSenderCursor: int,
    fixedPrefixAcknowledged: bool,
    fixedActivated: bool,
    lastPendingEvidence: PendingHandoffEvidence,
    lastAuthorityEvidence: CatchupAuthorityEvidence,
    lastBatchEvidence: CatchupBatchEvidence,
    lastCatchupOutcome: CatchupOutcome,
    lastTransition: DesiredCatchupTransition,
  }

  pure val FrozenSequence: int = 2

  pure def fixedCatchupIsAdmissible(
    pending: PendingHandoffEvidence,
    authority: CatchupAuthorityEvidence,
    batch: CatchupBatchEvidence,
  ): bool = all {
    pending == ExactPendingHandoff,
    authority == ExactRetiringAuthority,
    batch == ExactContiguousBatch,
  }

  pure def oldPrefixProbeEligible(state: DesiredCatchupState): bool = all {
    state.oldReceiverSequence == state.frozenSequence,
    state.oldSenderCursor == state.frozenSequence,
  }

  pure def fixedPrefixProbeEligible(state: DesiredCatchupState): bool = all {
    state.fixedReceiverSequence == state.frozenSequence,
    state.fixedSenderCursor == state.frozenSequence,
  }

  pure val initialState: DesiredCatchupState = {
    frozenSequence: FrozenSequence,
    oldReceiverSequence: 0,
    oldSenderCursor: 0,
    oldExactCatchupAttempted: false,
    oldPrefixAcknowledged: false,
    oldActivated: false,
    fixedReceiverSequence: 0,
    fixedSenderCursor: 0,
    previousFixedReceiverSequence: 0,
    previousFixedSenderCursor: 0,
    fixedPrefixAcknowledged: false,
    fixedActivated: false,
    lastPendingEvidence: MissingPendingHandoff,
    lastAuthorityEvidence: ForgedSourceAuthority,
    lastBatchEvidence: NoncontiguousBatch,
    lastCatchupOutcome: CatchupNotAttempted,
    lastTransition: DesiredCatchupInitialized,
  }

  var desiredCatchup: DesiredCatchupState

  action init = desiredCatchup' = initialState

  /// Counterfactual of the production bug: all authority and handoff evidence
  /// is exact, but desired-only membership is accepted only for a prefix probe.
  /// Because the sender cursor stays behind, that probe can never be issued.
  action attemptOldExactCatchup = all {
    not(desiredCatchup.oldExactCatchupAttempted),
    desiredCatchup' = desiredCatchup
      .with("oldExactCatchupAttempted", true)
      .with("lastPendingEvidence", ExactPendingHandoff)
      .with("lastAuthorityEvidence", ExactRetiringAuthority)
      .with("lastBatchEvidence", ExactContiguousBatch)
      .with("lastCatchupOutcome", CatchupRejected)
      .with("lastTransition", OldExactCatchupRejected),
  }

  action attemptFixedCatchup(
    pending: PendingHandoffEvidence,
    authority: CatchupAuthorityEvidence,
    batch: CatchupBatchEvidence,
  ): bool = {
    val accepted = fixedCatchupIsAdmissible(pending, authority, batch)
    all {
      not(desiredCatchup.fixedPrefixAcknowledged),
      desiredCatchup' = desiredCatchup
        .with(
          "previousFixedReceiverSequence",
          desiredCatchup.fixedReceiverSequence
        )
        .with("previousFixedSenderCursor", desiredCatchup.fixedSenderCursor)
        .with(
          "fixedReceiverSequence",
          if (accepted) desiredCatchup.frozenSequence
          else desiredCatchup.fixedReceiverSequence
        )
        .with(
          "fixedSenderCursor",
          if (accepted) desiredCatchup.frozenSequence
          else desiredCatchup.fixedSenderCursor
        )
        .with("lastPendingEvidence", pending)
        .with("lastAuthorityEvidence", authority)
        .with("lastBatchEvidence", batch)
        .with(
          "lastCatchupOutcome",
          if (accepted) CatchupAccepted else CatchupRejected
        )
        .with(
          "lastTransition",
          if (accepted) FixedCatchupAccepted else FixedCatchupRejected
        ),
    }
  }

  action acknowledgeFixedPrefix = all {
    fixedPrefixProbeEligible(desiredCatchup),
    not(desiredCatchup.fixedPrefixAcknowledged),
    desiredCatchup' = desiredCatchup
      .with("fixedPrefixAcknowledged", true)
      .with("lastTransition", FixedPrefixAcknowledged),
  }

  action activateFixedHandoff = all {
    desiredCatchup.fixedPrefixAcknowledged,
    not(desiredCatchup.fixedActivated),
    desiredCatchup' = desiredCatchup
      .with("fixedActivated", true)
      .with("lastTransition", FixedHandoffActivated),
  }

  action stutter =
    desiredCatchup' = desiredCatchup
      .with("lastTransition", DesiredCatchupStuttered)

  action step = any {
    attemptOldExactCatchup,
    attemptFixedCatchup(ExactPendingHandoff, ExactRetiringAuthority, ExactContiguousBatch),
    attemptFixedCatchup(MissingPendingHandoff, ExactRetiringAuthority, ExactContiguousBatch),
    attemptFixedCatchup(DifferentDesiredMember, ExactRetiringAuthority, ExactContiguousBatch),
    attemptFixedCatchup(ExactPendingHandoff, ForgedSourceAuthority, ExactContiguousBatch),
    attemptFixedCatchup(ExactPendingHandoff, StaleEpochAuthority, ExactContiguousBatch),
    attemptFixedCatchup(ExactPendingHandoff, ForeignFileSystemAuthority, ExactContiguousBatch),
    attemptFixedCatchup(ExactPendingHandoff, ExactRetiringAuthority, NoncontiguousBatch),
    acknowledgeFixedPrefix,
    activateFixedHandoff,
    stutter,
  }
}
