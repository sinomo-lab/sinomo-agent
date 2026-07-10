/// Finite, per-node Raft protocol slice for Sinomo's three-voter control plane.
///
/// This model intentionally complements (and does not replace) `raft_kv`:
/// `raft_kv` is the compact committed-prefix abstraction, while this slice
/// exposes votes, per-node logs, AppendEntries backtracking, acknowledgements,
/// crashes, and durable restart. Bounds are kept small enough for routine
/// symbolic checking.
///
/// Evidence class: current-behavior refinement for the Rust vote freshness,
/// append conflict, quorum-commit, and durable hard-state/log rules. Transport
/// scheduling and persistence are abstract actions. The non-empty initializers
/// are explicitly documented reachable protocol cuts used to expose long
/// failover/repair histories within a small bound.
module raft_protocol {
  import common.* from "../../lib/common"

  type Role = Follower | Candidate | Leader
  type Command = PutA | PutB | Delete
  type KvCell = NoCell | CellA | CellB
  type Entry = { term: int, command: Command }
  type EntrySlot = Empty | Filled(Entry)
  type ProtocolCase = EmptyElection | DivergentSuffix | CommittedLeaderChange
  type ProtocolEvent =
    | Initialized
    | ElectionStarted
    | VoteGranted
    | VoteRejectedStaleLog
    | VoteRejectedAlreadyCast
    | LeaderElected
    | LocalEntryAppended
    | AppendRejected
    | AppendAccepted
    | DivergentSuffixRepaired
    | CommitAdvanced
    | HigherTermStepDown
    | HigherTermObserved
    | NodeCrashed
    | NodeRestarted
    | TerminalStutter

  pure val MaxTerm: int = 4
  pure val MaxLogIndex: int = 3
  pure val Terms: Set[int] = Set(1, 2, 3, 4)
  pure val LogIndexes: Set[int] = Set(1, 2, 3)
  pure val Commands: Set[Command] = Set(PutA, PutB, Delete)
  pure val ProtocolCases: Set[ProtocolCase] =
    Set(EmptyElection, DivergentSuffix, CommittedLeaderChange)

  pure val emptyLog: int -> EntrySlot = LogIndexes.mapBy(_ => Empty)
  pure val emptyTerms: int -> int = LogIndexes.mapBy(_ => 0)
  pure val emptyTermVotes: int -> Set[Node] = Terms.mapBy(_ => Set())
  pure val emptyVoteHistory: Node -> (int -> Set[Node]) =
    Nodes.mapBy(_ => emptyTermVotes)
  pure val emptyLeadersByTerm: int -> Set[Node] = Terms.mapBy(_ => Set())
  pure val defaultNextIndex: Node -> (Node -> int) =
    Nodes.mapBy(_ => Nodes.mapBy(_ => 1))
  pure val defaultMatchIndex: Node -> (Node -> int) =
    Nodes.mapBy(_ => Nodes.mapBy(_ => 0))

  type ProtocolState = {
    protocolCase: ProtocolCase,
    running: Node -> bool,
    currentTerm: Node -> int,
    votedFor: Node -> Option[Node],
    role: Node -> Role,
    log: Node -> (int -> EntrySlot),
    logLength: Node -> int,
    commitIndex: Node -> int,
    appliedIndex: Node -> int,
    stateMachine: Node -> KvCell,
    voteHistory: Node -> (int -> Set[Node]),
    leadersByTerm: int -> Set[Node],
    nextIndex: Node -> (Node -> int),
    matchIndex: Node -> (Node -> int),
    committed: int -> EntrySlot,
    committedAtTerm: int -> int,
    crashRecorded: Node -> bool,
    crashTerm: Node -> int,
    crashVote: Node -> Option[Node],
    crashLog: Node -> (int -> EntrySlot),
    crashLogLength: Node -> int,
    lastEvent: ProtocolEvent,
  }

  var state: ProtocolState

  pure def applyCommand(cell: KvCell, command: Command): KvCell =
    match command {
      | PutA => CellA
      | PutB => CellB
      | Delete => NoCell
    }

  pure def applySlot(cell: KvCell, slot: EntrySlot): KvCell =
    match slot {
      | Filled(entry) => applyCommand(cell, entry.command)
      | Empty => cell
    }

  pure def replay(entries: int -> EntrySlot, through: int): KvCell = {
    val after1 = if (through >= 1) applySlot(NoCell, entries.get(1)) else NoCell
    val after2 = if (through >= 2) applySlot(after1, entries.get(2)) else after1
    if (through >= 3) applySlot(after2, entries.get(3)) else after2
  }

  pure def slotTerm(slot: EntrySlot): int =
    match slot { | Filled(entry) => entry.term | Empty => 0 }

  def lastLogTerm(node: Node): int =
    if (state.logLength.get(node) == 0) 0
    else slotTerm(state.log.get(node).get(state.logLength.get(node)))

  def candidateLogIsFresh(candidate: Node, voter: Node): bool =
    (lastLogTerm(candidate) > lastLogTerm(voter)) or all {
      lastLogTerm(candidate) == lastLogTerm(voter),
      state.logLength.get(candidate) >= state.logLength.get(voter),
    }

  def votesFor(candidate: Node, term: int): Set[Node] =
    Nodes.filter(voter => state.voteHistory.get(voter).get(term).contains(candidate))

  pure def prefixEqual(
    left: int -> EntrySlot,
    right: int -> EntrySlot,
    through: int,
  ): bool =
    LogIndexes.filter(i => i <= through).forall(i => left.get(i) == right.get(i))

  def sameEntryTermAt(left: Node, right: Node, index: int): bool =
    match state.log.get(left).get(index) {
      | Empty => false
      | Filled(leftEntry) => match state.log.get(right).get(index) {
          | Empty => false
          | Filled(rightEntry) => leftEntry.term == rightEntry.term
        }
    }

  def previousEntryMatches(leader: Node, follower: Node, nextSlot: int): bool =
    nextSlot == 1 or
      state.log.get(leader).get(nextSlot - 1) == state.log.get(follower).get(nextSlot - 1)

  pure def entryOrDefault(slot: EntrySlot): Entry =
    match slot {
      | Filled(entry) => entry
      | Empty => { term: 0, command: Delete }
    }

  pure def replaceConflictingSuffix(
    oldLog: int -> EntrySlot,
    index: int,
    entry: Entry,
  ): int -> EntrySlot =
    if (oldLog.get(index) == Filled(entry)) oldLog
    else LogIndexes.mapBy(i =>
      if (i < index) oldLog.get(i)
      else if (i == index) Filled(entry)
      else Empty
    )

  pure def mergedCommitted(
    oldCommitted: int -> EntrySlot,
    leaderLog: int -> EntrySlot,
    through: int,
  ): int -> EntrySlot =
    LogIndexes.mapBy(i => if (i <= through) leaderLog.get(i) else oldCommitted.get(i))

  pure def mergedCommitTerms(
    oldCommitted: int -> EntrySlot,
    oldTerms: int -> int,
    through: int,
    term: int,
  ): int -> int =
    LogIndexes.mapBy(i =>
      if (i <= through and oldCommitted.get(i) == Empty) term else oldTerms.get(i)
    )

  pure def emptyElectionState: ProtocolState = {
    protocolCase: EmptyElection,
    running: Nodes.mapBy(_ => true),
    currentTerm: Nodes.mapBy(_ => 0),
    votedFor: Nodes.mapBy(_ => None),
    role: Nodes.mapBy(_ => Follower),
    log: Nodes.mapBy(_ => emptyLog),
    logLength: Nodes.mapBy(_ => 0),
    commitIndex: Nodes.mapBy(_ => 0),
    appliedIndex: Nodes.mapBy(_ => 0),
    stateMachine: Nodes.mapBy(_ => NoCell),
    voteHistory: emptyVoteHistory,
    leadersByTerm: emptyLeadersByTerm,
    nextIndex: defaultNextIndex,
    matchIndex: defaultMatchIndex,
    committed: emptyLog,
    committedAtTerm: emptyTerms,
    crashRecorded: Nodes.mapBy(_ => false),
    crashTerm: Nodes.mapBy(_ => 0),
    crashVote: Nodes.mapBy(_ => None),
    crashLog: Nodes.mapBy(_ => emptyLog),
    crashLogLength: Nodes.mapBy(_ => 0),
    lastEvent: Initialized,
  }

  /// Reachable cut: N1 has an uncommitted term-1 entry; term-2 leader N2 and
  /// N3 contain the competing entry. N2 must backtrack N1's nextIndex before
  /// overwriting and truncating the divergent suffix.
  pure def divergentSuffixState: ProtocolState = {
    val oldEntry = Filled({ term: 1, command: PutA })
    val newEntry = Filled({ term: 2, command: PutB })
    val logs = Nodes.mapBy(n =>
      if (n == N1) emptyLog.set(1, oldEntry) else emptyLog.set(1, newEntry)
    )
    val history = Nodes.mapBy(voter => Terms.mapBy(term =>
      if ((voter == N1 or voter == N2) and term == 1) Set(N1)
      else if ((voter == N2 or voter == N3) and term == 2) Set(N2)
      else Set()
    ))
    {
      protocolCase: DivergentSuffix,
      running: Nodes.mapBy(_ => true),
      currentTerm: Nodes.mapBy(n => if (n == N1) 1 else 2),
      votedFor: Nodes.mapBy(n => if (n == N1) Some(N1) else Some(N2)),
      role: Nodes.mapBy(n => if (n == N2) Leader else Follower),
      log: logs,
      logLength: Nodes.mapBy(_ => 1),
      commitIndex: Nodes.mapBy(_ => 0),
      appliedIndex: Nodes.mapBy(_ => 0),
      stateMachine: Nodes.mapBy(_ => NoCell),
      voteHistory: history,
      leadersByTerm: emptyLeadersByTerm.set(1, Set(N1)).set(2, Set(N2)),
      nextIndex: defaultNextIndex.set(N2, Nodes.mapBy(_ => 2)),
      matchIndex: defaultMatchIndex.set(
        N2,
        Nodes.mapBy(n => if (n == N2 or n == N3) 1 else 0)
      ),
      committed: emptyLog,
      committedAtTerm: emptyTerms,
      crashRecorded: Nodes.mapBy(_ => false),
      crashTerm: Nodes.mapBy(_ => 0),
      crashVote: Nodes.mapBy(_ => None),
      crashLog: Nodes.mapBy(_ => emptyLog),
      crashLogLength: Nodes.mapBy(_ => 0),
      lastEvent: Initialized,
    }
  }

  /// Reachable cut: term-1 leader N1 committed index 1 on N1/N2, then crashed.
  /// The empty-log N3 can start an election but cannot obtain N2's vote.
  pure def committedLeaderChangeState: ProtocolState = {
    val entry = Filled({ term: 1, command: PutA })
    val committedLog = emptyLog.set(1, entry)
    val logs = Nodes.mapBy(n => if (n == N3) emptyLog else committedLog)
    val history = Nodes.mapBy(voter => Terms.mapBy(term =>
      if ((voter == N1 or voter == N2) and term == 1) Set(N1) else Set()
    ))
    {
      protocolCase: CommittedLeaderChange,
      running: Nodes.mapBy(n => n != N1),
      currentTerm: Nodes.mapBy(_ => 1),
      votedFor: Nodes.mapBy(n => if (n == N1 or n == N2) Some(N1) else None),
      role: Nodes.mapBy(_ => Follower),
      log: logs,
      logLength: Nodes.mapBy(n => if (n == N3) 0 else 1),
      commitIndex: Nodes.mapBy(n => if (n == N3) 0 else 1),
      appliedIndex: Nodes.mapBy(n => if (n == N3) 0 else 1),
      stateMachine: Nodes.mapBy(n => if (n == N3) NoCell else CellA),
      voteHistory: history,
      leadersByTerm: emptyLeadersByTerm.set(1, Set(N1)),
      nextIndex: defaultNextIndex,
      matchIndex: defaultMatchIndex,
      committed: committedLog,
      committedAtTerm: emptyTerms.set(1, 1),
      crashRecorded: Nodes.mapBy(n => n == N1),
      crashTerm: Nodes.mapBy(n => if (n == N1) 1 else 0),
      crashVote: Nodes.mapBy(n => if (n == N1) Some(N1) else None),
      crashLog: Nodes.mapBy(n => if (n == N1) committedLog else emptyLog),
      crashLogLength: Nodes.mapBy(n => if (n == N1) 1 else 0),
      lastEvent: Initialized,
    }
  }

  action initEmpty = state' = emptyElectionState
  action initDivergentSuffix = state' = divergentSuffixState
  action initCommittedLeaderChange = state' = committedLeaderChangeState

  action init = {
    nondet selected = oneOf(ProtocolCases)
    match selected {
      | EmptyElection => initEmpty
      | DivergentSuffix => initDivergentSuffix
      | CommittedLeaderChange => initCommittedLeaderChange
    }
  }

  action beginElection(candidate: Node): bool = {
    val electionTerm = state.currentTerm.get(candidate) + 1
    val candidateHistory = state.voteHistory.get(candidate)
    all {
      state.running.get(candidate),
      electionTerm <= MaxTerm,
      state' = state
        .with("currentTerm", state.currentTerm.set(candidate, electionTerm))
        .with("votedFor", state.votedFor.set(candidate, Some(candidate)))
        .with("role", state.role.set(candidate, Candidate))
        .with(
          "voteHistory",
          state.voteHistory.set(
            candidate,
            candidateHistory.set(
              electionTerm,
              candidateHistory.get(electionTerm).union(Set(candidate))
            )
          )
        )
        .with("lastEvent", ElectionStarted),
    }
  }

  action deliverVoteRequest(candidate: Node, voter: Node): bool = {
    val requestTerm = state.currentTerm.get(candidate)
    val higherTerm = requestTerm > state.currentTerm.get(voter)
    val voteBefore = if (higherTerm) None else state.votedFor.get(voter)
    val canVote = match voteBefore {
      | None => true
      | Some(existing) => existing == candidate
    }
    val fresh = candidateLogIsFresh(candidate, voter)
    val granted = canVote and fresh
    val voterHistory = state.voteHistory.get(voter)
    val updatedHistory = if (granted)
      voterHistory.set(
        requestTerm,
        voterHistory.get(requestTerm).union(Set(candidate))
      )
    else voterHistory
    val event = if (granted) VoteGranted
      else if (not(fresh)) VoteRejectedStaleLog
      else VoteRejectedAlreadyCast
    all {
      candidate != voter,
      state.running.get(candidate),
      state.running.get(voter),
      state.role.get(candidate) == Candidate,
      requestTerm >= state.currentTerm.get(voter),
      requestTerm >= 1,
      requestTerm <= MaxTerm,
      state' = state
        .with("currentTerm", state.currentTerm.set(voter, requestTerm))
        .with("votedFor", state.votedFor.set(voter, if (granted) Some(candidate) else voteBefore))
        .with(
          "role",
          state.role.set(voter, if (higherTerm) Follower else state.role.get(voter))
        )
        .with("voteHistory", state.voteHistory.set(voter, updatedHistory))
        .with("lastEvent", event),
    }
  }

  action becomeLeader(candidate: Node): bool = {
    val term = state.currentTerm.get(candidate)
    val nextRow = Nodes.mapBy(_ => state.logLength.get(candidate) + 1)
    val matchRow = Nodes.mapBy(n => if (n == candidate) state.logLength.get(candidate) else 0)
    all {
      state.running.get(candidate),
      state.role.get(candidate) == Candidate,
      votesFor(candidate, term).hasQuorum(),
      state' = state
        .with("role", state.role.set(candidate, Leader))
        .with(
          "leadersByTerm",
          state.leadersByTerm.set(
            term,
            state.leadersByTerm.get(term).union(Set(candidate))
          )
        )
        .with("nextIndex", state.nextIndex.set(candidate, nextRow))
        .with("matchIndex", state.matchIndex.set(candidate, matchRow))
        .with("lastEvent", LeaderElected),
    }
  }

  action appendLocal(leader: Node, command: Command): bool = {
    val index = state.logLength.get(leader) + 1
    val entry = Filled({ term: state.currentTerm.get(leader), command: command })
    val leaderMatch = state.matchIndex.get(leader).set(leader, index)
    val leaderNext = state.nextIndex.get(leader).set(leader, index + 1)
    all {
      state.running.get(leader),
      state.role.get(leader) == Leader,
      index <= MaxLogIndex,
      state' = state
        .with("log", state.log.set(leader, state.log.get(leader).set(index, entry)))
        .with("logLength", state.logLength.set(leader, index))
        .with("matchIndex", state.matchIndex.set(leader, leaderMatch))
        .with("nextIndex", state.nextIndex.set(leader, leaderNext))
        .with("lastEvent", LocalEntryAppended),
    }
  }

  action replicate(leader: Node, follower: Node): bool = {
    val leaderTerm = state.currentTerm.get(leader)
    val followerTerm = state.currentTerm.get(follower)
    val nextSlot = state.nextIndex.get(leader).get(follower)
    val leaderLength = state.logLength.get(leader)
    val nextEntry = if (nextSlot <= MaxLogIndex)
      entryOrDefault(state.log.get(leader).get(nextSlot))
    else { term: 0, command: Delete }
    any {
      // A higher-term response invalidates the sender's leadership.
      all {
        leader != follower,
        state.running.get(leader),
        state.running.get(follower),
        state.role.get(leader) == Leader,
        followerTerm > leaderTerm,
        state' = state
          .with("currentTerm", state.currentTerm.set(leader, followerTerm))
          .with("votedFor", state.votedFor.set(leader, None))
          .with("role", state.role.set(leader, Follower))
          .with("lastEvent", HigherTermStepDown),
      },
      // Prev-log mismatch rejects the append and backs up nextIndex.
      all {
        leader != follower,
        state.running.get(leader),
        state.running.get(follower),
        state.role.get(leader) == Leader,
        leaderTerm >= followerTerm,
        not(previousEntryMatches(leader, follower, nextSlot)),
        state' = state
          .with("currentTerm", state.currentTerm.set(follower, leaderTerm))
          .with("votedFor", state.votedFor.set(follower, if (leaderTerm > followerTerm) None else state.votedFor.get(follower)))
          .with("role", state.role.set(follower, Follower))
          .with(
            "nextIndex",
            state.nextIndex.set(
              leader,
              state.nextIndex.get(leader).set(
                follower,
                if (nextSlot > 1) nextSlot - 1 else 1
              )
            )
          )
          .with("lastEvent", AppendRejected),
      },
      // Matching prev-log state accepts one entry. A conflicting entry deletes
      // the follower's uncommitted suffix before the leader entry is installed.
      all {
        leader != follower,
        state.running.get(leader),
        state.running.get(follower),
        state.role.get(leader) == Leader,
        leaderTerm >= followerTerm,
        previousEntryMatches(leader, follower, nextSlot),
        nextSlot <= leaderLength,
        state.log.get(leader).get(nextSlot) != Empty,
        {
          val oldFollowerLog = state.log.get(follower)
          val conflict = oldFollowerLog.get(nextSlot) != Empty and
            oldFollowerLog.get(nextSlot) != Filled(nextEntry)
          val newFollowerLog = replaceConflictingSuffix(oldFollowerLog, nextSlot, nextEntry)
          val newFollowerLength = if (conflict) nextSlot
            else if (state.logLength.get(follower) > nextSlot) state.logLength.get(follower)
            else nextSlot
          val learnedCommit = if (state.commitIndex.get(leader) < newFollowerLength)
            state.commitIndex.get(leader) else newFollowerLength
          val newCommit = if (learnedCommit > state.commitIndex.get(follower))
            learnedCommit else state.commitIndex.get(follower)
          val leaderMatch = state.matchIndex.get(leader).set(follower, nextSlot)
          val leaderNext = state.nextIndex.get(leader).set(follower, nextSlot + 1)
          state' = state
            .with("currentTerm", state.currentTerm.set(follower, leaderTerm))
            .with("votedFor", state.votedFor.set(follower, if (leaderTerm > followerTerm) None else state.votedFor.get(follower)))
            .with("role", state.role.set(follower, Follower))
            .with("log", state.log.set(follower, newFollowerLog))
            .with("logLength", state.logLength.set(follower, newFollowerLength))
            .with("commitIndex", state.commitIndex.set(follower, newCommit))
            .with("appliedIndex", state.appliedIndex.set(follower, newCommit))
            .with("stateMachine", state.stateMachine.set(follower, replay(newFollowerLog, newCommit)))
            .with("matchIndex", state.matchIndex.set(leader, leaderMatch))
            .with("nextIndex", state.nextIndex.set(leader, leaderNext))
            .with("lastEvent", if (conflict) DivergentSuffixRepaired else AppendAccepted)
        },
      },
      // A matching heartbeat acknowledges the whole leader prefix and carries
      // the leader commit index to the follower.
      all {
        leader != follower,
        state.running.get(leader),
        state.running.get(follower),
        state.role.get(leader) == Leader,
        leaderTerm >= followerTerm,
        nextSlot > leaderLength,
        previousEntryMatches(leader, follower, nextSlot),
        prefixEqual(state.log.get(leader), state.log.get(follower), leaderLength),
        {
          val learnedCommit = if (state.commitIndex.get(leader) < leaderLength)
            state.commitIndex.get(leader) else leaderLength
          val newCommit = if (learnedCommit > state.commitIndex.get(follower))
            learnedCommit else state.commitIndex.get(follower)
          val leaderMatch = state.matchIndex.get(leader).set(follower, leaderLength)
          state' = state
            .with("currentTerm", state.currentTerm.set(follower, leaderTerm))
            .with("votedFor", state.votedFor.set(follower, if (leaderTerm > followerTerm) None else state.votedFor.get(follower)))
            .with("role", state.role.set(follower, Follower))
            .with("commitIndex", state.commitIndex.set(follower, newCommit))
            .with("appliedIndex", state.appliedIndex.set(follower, newCommit))
            .with("stateMachine", state.stateMachine.set(follower, replay(state.log.get(follower), newCommit)))
            .with("matchIndex", state.matchIndex.set(leader, leaderMatch))
            .with("lastEvent", AppendAccepted)
        },
      },
    }
  }

  action advanceCommit(leader: Node, index: int): bool = {
    val acknowledgers = Nodes.filter(n => state.matchIndex.get(leader).get(n) >= index)
    val leaderLog = state.log.get(leader)
    all {
      state.running.get(leader),
      state.role.get(leader) == Leader,
      index > state.commitIndex.get(leader),
      index <= state.logLength.get(leader),
      slotTerm(leaderLog.get(index)) == state.currentTerm.get(leader),
      acknowledgers.hasQuorum(),
      state' = state
        .with("commitIndex", state.commitIndex.set(leader, index))
        .with("appliedIndex", state.appliedIndex.set(leader, index))
        .with("stateMachine", state.stateMachine.set(leader, replay(leaderLog, index)))
        .with("committed", mergedCommitted(state.committed, leaderLog, index))
        .with(
          "committedAtTerm",
          mergedCommitTerms(
            state.committed,
            state.committedAtTerm,
            index,
            state.currentTerm.get(leader)
          )
        )
        .with("lastEvent", CommitAdvanced),
    }
  }

  action observeHigherTerm(node: Node, observedTerm: int): bool = {
    val steppedDown = state.role.get(node) != Follower
    all {
      state.running.get(node),
      observedTerm > state.currentTerm.get(node),
      observedTerm <= MaxTerm,
      state' = state
        .with("currentTerm", state.currentTerm.set(node, observedTerm))
        .with("votedFor", state.votedFor.set(node, None))
        .with("role", state.role.set(node, Follower))
        .with("lastEvent", if (steppedDown) HigherTermStepDown else HigherTermObserved),
    }
  }

  action crash(node: Node): bool = all {
    state.running.get(node),
    state' = state
      .with("running", state.running.set(node, false))
      .with("role", state.role.set(node, Follower))
      .with("crashRecorded", state.crashRecorded.set(node, true))
      .with("crashTerm", state.crashTerm.set(node, state.currentTerm.get(node)))
      .with("crashVote", state.crashVote.set(node, state.votedFor.get(node)))
      .with("crashLog", state.crashLog.set(node, state.log.get(node)))
      .with("crashLogLength", state.crashLogLength.set(node, state.logLength.get(node)))
      .with("lastEvent", NodeCrashed),
  }

  action restart(node: Node): bool = all {
    not(state.running.get(node)),
    state.crashRecorded.get(node),
    state.currentTerm.get(node) == state.crashTerm.get(node),
    state.votedFor.get(node) == state.crashVote.get(node),
    state.log.get(node) == state.crashLog.get(node),
    state.logLength.get(node) == state.crashLogLength.get(node),
    state' = state
      .with("running", state.running.set(node, true))
      .with("role", state.role.set(node, Follower))
      .with("lastEvent", NodeRestarted),
  }

  action terminalStutter = state' = state.with("lastEvent", TerminalStutter)

  action step = any {
    { nondet node = oneOf(Nodes) beginElection(node) },
    {
      nondet candidate = oneOf(Nodes)
      nondet voter = oneOf(Nodes)
      deliverVoteRequest(candidate, voter)
    },
    { nondet node = oneOf(Nodes) becomeLeader(node) },
    {
      nondet node = oneOf(Nodes)
      nondet command = oneOf(Commands)
      appendLocal(node, command)
    },
    {
      nondet leader = oneOf(Nodes)
      nondet follower = oneOf(Nodes)
      replicate(leader, follower)
    },
    {
      nondet leader = oneOf(Nodes)
      nondet index = oneOf(LogIndexes)
      advanceCommit(leader, index)
    },
    {
      nondet node = oneOf(Nodes)
      nondet term = oneOf(Terms)
      observeHigherTerm(node, term)
    },
    { nondet node = oneOf(Nodes) crash(node) },
    { nondet node = oneOf(Nodes) restart(node) },
    terminalStutter,
  }
}
