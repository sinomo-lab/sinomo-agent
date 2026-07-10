/// Safety properties and coverage witnesses for the per-node Raft slice.
module raft_protocol_safety {
  import common.* from "../../lib/common"
  import raft_protocol.* from "../../models/kv/raft_protocol"

  def stateIsBoundedAndContiguous: bool =
    Nodes.forall(node => all {
      state.currentTerm.get(node) >= 0,
      state.currentTerm.get(node) <= MaxTerm,
      state.logLength.get(node) >= 0,
      state.logLength.get(node) <= MaxLogIndex,
      state.commitIndex.get(node) >= 0,
      state.commitIndex.get(node) <= state.logLength.get(node),
      state.appliedIndex.get(node) == state.commitIndex.get(node),
      LogIndexes.forall(index =>
        if (index <= state.logLength.get(node)) {
          match state.log.get(node).get(index) { | Filled(_) => true | Empty => false }
        } else state.log.get(node).get(index) == Empty
      ),
    })

  /// The history deliberately stores a set rather than a single candidate so
  /// this invariant fails if vote-granting ever permits a second candidate in
  /// the same term.
  def voteOncePerTerm: bool =
    Nodes.forall(voter =>
      Terms.forall(term => state.voteHistory.get(voter).get(term).size() <= 1)
    )

  def durableVoteMatchesHistory: bool =
    Nodes.forall(voter =>
      if (state.currentTerm.get(voter) == 0) state.votedFor.get(voter) == None
      else match state.votedFor.get(voter) {
        | None => true
        | Some(candidate) =>
            state.voteHistory.get(voter).get(state.currentTerm.get(voter)).contains(candidate)
      }
    )

  /// Quorum intersection plus vote-once yields at most one leader per term.
  def electionSafety: bool = all {
    Terms.forall(term => state.leadersByTerm.get(term).size() <= 1),
    Terms.forall(term => state.leadersByTerm.get(term).forall(leader =>
      votesFor(leader, term).hasQuorum()
    )),
  }

  /// Raft log matching: equal entry terms at an index imply an identical prefix.
  def logMatching: bool =
    Nodes.forall(left => Nodes.forall(right => LogIndexes.forall(index =>
      sameEntryTermAt(left, right, index) implies
        prefixEqual(state.log.get(left), state.log.get(right), index)
    )))

  /// Every locally committed prefix agrees with the canonical committed prefix.
  def committedPrefixAgreement: bool =
    Nodes.forall(node => LogIndexes
      .filter(index => index <= state.commitIndex.get(node))
      .forall(index => state.log.get(node).get(index) == state.committed.get(index))
    )

  def committedSlotsAreFilled: bool =
    LogIndexes.forall(index =>
      if (state.committedAtTerm.get(index) > 0) {
        match state.committed.get(index) { | Filled(_) => true | Empty => false }
      } else state.committed.get(index) == Empty
    )

  /// Any leader elected after an entry became committed contains that entry.
  def leaderCompleteness: bool =
    Terms.forall(term => state.leadersByTerm.get(term).forall(leader =>
      LogIndexes.forall(index =>
        all {
          state.committedAtTerm.get(index) > 0,
          state.committedAtTerm.get(index) < term,
        } implies state.log.get(leader).get(index) == state.committed.get(index)
      )
    ))

  /// Applying and replaying the same committed prefix are equivalent on every node.
  def stateMachineReplay: bool =
    Nodes.forall(node =>
      state.stateMachine.get(node)
        == replay(state.log.get(node), state.appliedIndex.get(node))
    )

  /// A stopped node cannot mutate its durable term, vote, or log; restart is
  /// enabled only from exactly this persisted image.
  def crashPreservesDurableConsensusState: bool =
    Nodes.forall(node => all {
      not(state.running.get(node)),
      state.crashRecorded.get(node),
    } implies all {
      state.currentTerm.get(node) == state.crashTerm.get(node),
      state.votedFor.get(node) == state.crashVote.get(node),
      state.log.get(node) == state.crashLog.get(node),
      state.logLength.get(node) == state.crashLogLength.get(node),
    })

  def witnessVoteGranted: bool = state.lastEvent == VoteGranted
  def witnessStaleCandidateRejected: bool = state.lastEvent == VoteRejectedStaleLog
  def witnessLeaderElectedFromQuorum: bool = state.lastEvent == LeaderElected
  def witnessAppendPrevMismatchRejected: bool = state.lastEvent == AppendRejected
  def witnessDivergentSuffixTruncated: bool = state.lastEvent == DivergentSuffixRepaired
  def witnessAcknowledgementQuorumCommit: bool = state.lastEvent == CommitAdvanced
  def witnessHigherTermStepDown: bool = state.lastEvent == HigherTermStepDown
  def witnessDurableRestart: bool = state.lastEvent == NodeRestarted
  def witnessFollowerStateMachineReplay: bool = all {
    state.lastEvent == AppendAccepted,
    Nodes.exists(node => state.commitIndex.get(node) > 0),
    stateMachineReplay,
  }
  def witnessLeaderCompletenessAfterElection: bool = all {
    state.lastEvent == LeaderElected,
    Terms.exists(term => state.leadersByTerm.get(term).exists(leader =>
      LogIndexes.exists(index => all {
        state.committedAtTerm.get(index) > 0,
        state.committedAtTerm.get(index) < term,
        state.log.get(leader).get(index) == state.committed.get(index),
      })
    )),
  }

  def safety: bool = all {
    stateIsBoundedAndContiguous,
    voteOncePerTerm,
    durableVoteMatchesHistory,
    electionSafety,
    logMatching,
    committedPrefixAgreement,
    committedSlotsAreFilled,
    leaderCompleteness,
    stateMachineReplay,
    crashPreservesDurableConsensusState,
  }
}
