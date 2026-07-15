/// Safety and bounded reachability for desired-only handoff catch-up.
module dfs_metadata_handoff_desired_catchup_safety {
  import dfs_metadata_handoff_desired_catchup.* from "../../models/dfs/metadata_handoff_desired_catchup"

  def sequencesStayBounded: bool = all {
    desiredCatchup.frozenSequence == FrozenSequence,
    desiredCatchup.oldReceiverSequence >= 0,
    desiredCatchup.oldReceiverSequence <= desiredCatchup.frozenSequence,
    desiredCatchup.oldSenderCursor >= 0,
    desiredCatchup.oldSenderCursor <= desiredCatchup.frozenSequence,
    desiredCatchup.fixedReceiverSequence >= 0,
    desiredCatchup.fixedReceiverSequence <= desiredCatchup.frozenSequence,
    desiredCatchup.fixedSenderCursor >= 0,
    desiredCatchup.fixedSenderCursor <= desiredCatchup.frozenSequence,
  }

  def oldExactCatchupRejectionIsStuck: bool =
    desiredCatchup.oldExactCatchupAttempted implies all {
      desiredCatchup.oldReceiverSequence == 0,
      desiredCatchup.oldSenderCursor == 0,
      not(oldPrefixProbeEligible(desiredCatchup)),
      not(desiredCatchup.oldPrefixAcknowledged),
      not(desiredCatchup.oldActivated),
    }

  def acceptedCatchupRequiresExactEvidence: bool =
    desiredCatchup.lastTransition == FixedCatchupAccepted implies all {
      desiredCatchup.lastCatchupOutcome == CatchupAccepted,
      desiredCatchup.lastPendingEvidence == ExactPendingHandoff,
      desiredCatchup.lastAuthorityEvidence == ExactRetiringAuthority,
      desiredCatchup.lastBatchEvidence == ExactContiguousBatch,
      fixedPrefixProbeEligible(desiredCatchup),
    }

  def rejectedCatchupCannotAdvance: bool =
    desiredCatchup.lastTransition == FixedCatchupRejected implies all {
      desiredCatchup.lastCatchupOutcome == CatchupRejected,
      desiredCatchup.fixedReceiverSequence ==
        desiredCatchup.previousFixedReceiverSequence,
      desiredCatchup.fixedSenderCursor ==
        desiredCatchup.previousFixedSenderCursor,
    }

  def prefixAcknowledgementRequiresExactCatchup: bool =
    desiredCatchup.fixedPrefixAcknowledged implies
      fixedPrefixProbeEligible(desiredCatchup)

  def activationRequiresPrefixAcknowledgement: bool =
    desiredCatchup.fixedActivated implies all {
      desiredCatchup.fixedPrefixAcknowledged,
      fixedPrefixProbeEligible(desiredCatchup),
    }

  def safety: bool = all {
    sequencesStayBounded,
    oldExactCatchupRejectionIsStuck,
    acceptedCatchupRequiresExactEvidence,
    rejectedCatchupCannotAdvance,
    prefixAcknowledgementRequiresExactCatchup,
    activationRequiresPrefixAcknowledgement,
  }

  def witnessOldExactCatchupDeadlocksBeforePrefix: bool = all {
    desiredCatchup.lastTransition == OldExactCatchupRejected,
    desiredCatchup.oldExactCatchupAttempted,
    not(oldPrefixProbeEligible(desiredCatchup)),
  }

  def witnessExactPendingDesiredCatchupAccepted: bool = all {
    desiredCatchup.lastTransition == FixedCatchupAccepted,
    desiredCatchup.lastPendingEvidence == ExactPendingHandoff,
    desiredCatchup.lastAuthorityEvidence == ExactRetiringAuthority,
    desiredCatchup.lastBatchEvidence == ExactContiguousBatch,
    fixedPrefixProbeEligible(desiredCatchup),
  }

  def witnessMissingPendingHandoffRejected: bool = all {
    desiredCatchup.lastTransition == FixedCatchupRejected,
    desiredCatchup.lastPendingEvidence == MissingPendingHandoff,
  }

  def witnessDifferentDesiredMemberRejected: bool = all {
    desiredCatchup.lastTransition == FixedCatchupRejected,
    desiredCatchup.lastPendingEvidence == DifferentDesiredMember,
  }

  def witnessForgedSourceRejected: bool = all {
    desiredCatchup.lastTransition == FixedCatchupRejected,
    desiredCatchup.lastAuthorityEvidence == ForgedSourceAuthority,
  }

  def witnessStaleEpochRejected: bool = all {
    desiredCatchup.lastTransition == FixedCatchupRejected,
    desiredCatchup.lastAuthorityEvidence == StaleEpochAuthority,
  }

  def witnessForeignFileSystemRejected: bool = all {
    desiredCatchup.lastTransition == FixedCatchupRejected,
    desiredCatchup.lastAuthorityEvidence == ForeignFileSystemAuthority,
  }

  def witnessNoncontiguousBatchRejected: bool = all {
    desiredCatchup.lastTransition == FixedCatchupRejected,
    desiredCatchup.lastBatchEvidence == NoncontiguousBatch,
  }

  def witnessCatchupThenPrefixThenActivation: bool = all {
    desiredCatchup.lastTransition == FixedHandoffActivated,
    desiredCatchup.fixedActivated,
    desiredCatchup.fixedPrefixAcknowledged,
    fixedPrefixProbeEligible(desiredCatchup),
  }
}
