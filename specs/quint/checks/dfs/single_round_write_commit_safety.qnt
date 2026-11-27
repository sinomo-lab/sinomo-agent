/// Safety and witnesses for the single-round replicated write commit.
/// A bounded result for one W=2 write and a two-member write set; it is not a
/// proof of holder substitution, larger write sets or Rust conformance.
module dfs_single_round_write_commit_safety {
  import dfs_single_round_write_commit.* from "../../models/dfs/single_round_write_commit"

  def committed = s.committedByPrimary or s.committedBySuccessor
  /// A prepared change exists only after its primary stored the content, which
  /// is why no later primary asks the original one to confirm its holding.
  def preparedChangeImpliesPrimaryContent =
    (s.primaryPrepare or s.memberFrames) implies s.primaryContent
  def commitPointHasBothHoldersAndEveryMember = s.mode == Sound and committed implies all {
    s.primaryContent, secondHolderHoldsContent(s), s.memberFrames,
  }
  def memberAcknowledgementConfirmsItsContent =
    s.mode == Sound and s.memberAcknowledged implies all {
      s.memberFrames, s.secondHolder == Member implies s.memberContent,
    }
  /// Whoever serves after success can re-establish the commit point from what
  /// is durable: the change on every member and the content on both holders.
  def acknowledgedChangeStaysCommittable = s.mode == Sound and s.success implies all {
    not(s.aborted), s.memberFrames, s.primaryContent, secondHolderHoldsContent(s),
    s.committedByPrimary implies s.primaryPrepare,
  }
  def abortOnlyBeforeAnyParticipantWasSent =
    s.mode == Sound and s.aborted implies not(s.memberMessage) and not(s.outsideMessage)
  def abortedChangeNeverCommits = s.mode == Sound and s.aborted implies not(committed)
  def successorCommitsOnlyWhatItHolds = s.committedBySuccessor implies s.memberFrames
  def safety = all {
    preparedChangeImpliesPrimaryContent, commitPointHasBothHoldersAndEveryMember,
    memberAcknowledgementConfirmsItsContent, acknowledgedChangeStaysCommittable,
    abortOnlyBeforeAnyParticipantWasSent, abortedChangeNeverCommits,
    successorCommitsOnlyWhatItHolds,
  }

  def sound = s.mode == Sound
  def witnessMemberHolderCommitsInOneExchange =
    sound and s.last == CommittedByPrimary and s.secondHolder == Member and s.crashes == 0
  def witnessOutsideHolderCommitsInOneExchange =
    sound and s.last == CommittedByPrimary and s.secondHolder == Outside and s.crashes == 0
  def witnessSuccessBeforeDurableCommitRecord =
    sound and s.success and not(s.primaryCommitRecord) and s.last == PrimaryCrashed
  def witnessRestartedPrimaryCommitsAcknowledgedChangeAgain =
    sound and s.last == CommittedByPrimary and s.recommitted
  def witnessSuccessorCommitsAcknowledgedChange =
    sound and s.last == CommittedBySuccessor and s.committedByPrimary
  def witnessSuccessorCommitsChangeTheCallerNeverSawSucceed =
    sound and s.last == CommittedBySuccessor and not(s.committedByPrimary)
  def witnessSuccessorConfirmsOutsideHolderBeforeCommit =
    sound and s.last == CommittedBySuccessor and s.secondHolder == Outside
  def witnessUnconfirmedOutsideHolderLeavesChangePrepared = all {
    sound, s.serving == Member, s.memberFrames, s.secondHolder == Outside,
    not(s.outsideContent), not(s.committedBySuccessor), s.last == MemberTookOver,
  }
  def witnessLostMemberMessageLeavesNothingToRecover =
    sound and s.serving == Member and not(s.memberFrames) and s.primaryPrepare
  def witnessAbortBeforeSend = sound and s.aborted
  // Negative controls: each broken mode reaches the state its rule excludes.
  def witnessBrokenCommitWithoutHolderLacksACopy =
    s.mode == CommitWithoutHolder and committed and not(secondHolderHoldsContent(s))
  def witnessBrokenAcknowledgementWithoutContentLacksACopy = all {
    s.mode == AcknowledgeWithoutContent, s.committedByPrimary,
    s.secondHolder == Member, not(s.memberContent),
  }
  def witnessBrokenAbortAfterSendIsCommittedBySuccessor =
    s.mode == AbortAfterSend and s.aborted and s.committedBySuccessor
}
