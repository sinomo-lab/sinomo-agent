/// Bounded Raft-backed KV model for Sinomo's compact cluster control plane.
///
/// This first slice verifies the safety skeleton:
/// - a leader can commit only while a voter quorum is online,
/// - at most one leader is elected per term,
/// - every committed node observes the same committed log prefix,
/// - the visible KV cell is exactly the result of replaying committed entries.
module raft_kv {
  import common.* from "../../lib/common"

  type KvValue = V1 | V2
  type KvCommand = PutV1 | PutV2 | Delete
  type KvCell = NoCell | Cell(KvValue)
  type Entry = { term: int, command: KvCommand }
  type EntrySlot = Empty | Filled(Entry)

  pure val MaxLogIndex: int = 3
  pure val LogIndexes: Set[int] = Set(1, 2, 3)
  pure val Commands: Set[KvCommand] = Set(PutV1, PutV2, Delete)

  var online: Node -> bool
  var currentTerm: int
  var leader: Option[Node]
  var votedFor: Node -> Option[Node]
  var log: Node -> (int -> EntrySlot)
  var commitIndex: Node -> int
  var committed: int -> EntrySlot
  var globalCommitIndex: int
  var nextLogIndex: int
  var kvCell: KvCell
  var leadersByTerm: int -> Set[Node]

  pure val emptyLog: int -> EntrySlot =
    LogIndexes.mapBy(_ => Empty)

  pure val emptyLeaders: int -> Set[Node] =
    Set(1, 2, 3, 4).mapBy(_ => Set())

  pure def applyCommand(cell: KvCell, command: KvCommand): KvCell =
    match command {
      | PutV1 => Cell(V1)
      | PutV2 => Cell(V2)
      | Delete => NoCell
    }

  pure def applySlot(cell: KvCell, slot: EntrySlot): KvCell =
    match slot {
      | Filled(entry) => applyCommand(cell, entry.command)
      | Empty => cell
    }

  def replayCommittedAt(index: int): KvCell = {
    val after1 = if (index >= 1) applySlot(NoCell, committed.get(1)) else NoCell
    val after2 = if (index >= 2) applySlot(after1, committed.get(2)) else after1
    if (index >= 3) applySlot(after2, committed.get(3)) else after2
  }

  pure def setEntryOnTargets(
    oldLog: Node -> (int -> EntrySlot),
    targets: Set[Node],
    index: int,
    entry: Entry,
  ): Node -> (int -> EntrySlot) =
    Nodes.mapBy(n =>
      if (targets.contains(n)) oldLog.get(n).set(index, Filled(entry)) else oldLog.get(n)
    )

  pure def setCommitOnTargets(
    oldCommit: Node -> int,
    targets: Set[Node],
    index: int,
  ): Node -> int =
    Nodes.mapBy(n => if (targets.contains(n)) index else oldCommit.get(n))

  def logsMatchCommittedPrefix(node: Node): bool =
    LogIndexes
      .filter(i => i <= commitIndex.get(node))
      .forall(i => log.get(node).get(i) == committed.get(i))

  action init = all {
    online' = Nodes.mapBy(_ => true),
    currentTerm' = 1,
    leader' = Some(N1),
    votedFor' = Nodes.mapBy(_ => None),
    log' = Nodes.mapBy(_ => emptyLog),
    commitIndex' = Nodes.mapBy(_ => 0),
    committed' = emptyLog,
    globalCommitIndex' = 0,
    nextLogIndex' = 1,
    kvCell' = NoCell,
    leadersByTerm' = emptyLeaders.set(1, Set(N1)),
  }

  action setNodeOnline(node: Node, isOnline: bool): bool = all {
    online' = online.set(node, isOnline),
    currentTerm' = currentTerm,
    leader' = leader,
    votedFor' = votedFor,
    log' = if (isOnline) log.set(node, committed) else log,
    commitIndex' = if (isOnline) commitIndex.set(node, globalCommitIndex) else commitIndex,
    committed' = committed,
    globalCommitIndex' = globalCommitIndex,
    nextLogIndex' = nextLogIndex,
    kvCell' = kvCell,
    leadersByTerm' = leadersByTerm,
  }

  action electLeader(candidate: Node): bool = {
    val votersOnline = onlineVoters(online)
    val electionTerm = currentTerm + 1
    all {
      online.get(candidate),
      candidate.in(Voters),
      votersOnline.hasQuorum(),
      electionTerm <= 4,
      commitIndex.get(candidate) == globalCommitIndex,
      logsMatchCommittedPrefix(candidate),
      online' = online,
      currentTerm' = electionTerm,
      leader' = Some(candidate),
      votedFor' = Nodes.mapBy(n =>
        if (votersOnline.contains(n)) Some(candidate) else votedFor.get(n)
      ),
      log' = log,
      commitIndex' = commitIndex,
      committed' = committed,
      globalCommitIndex' = globalCommitIndex,
      nextLogIndex' = nextLogIndex,
      kvCell' = kvCell,
      leadersByTerm' = leadersByTerm.set(electionTerm, Set(candidate)),
    }
  }

  action propose(command: KvCommand): bool = {
    val votersOnline = onlineVoters(online)
    val entry = { term: currentTerm, command: command }
    all {
      isSome(leader),
      online.get(unwrapOr(leader, N1)),
      votersOnline.hasQuorum(),
      nextLogIndex <= MaxLogIndex,
      // The model starts with catch-up before election, so the current leader is
      // never allowed to append on top of a stale committed prefix.
      commitIndex.get(unwrapOr(leader, N1)) == globalCommitIndex,
      logsMatchCommittedPrefix(unwrapOr(leader, N1)),
      online' = online,
      currentTerm' = currentTerm,
      leader' = leader,
      votedFor' = votedFor,
      log' = setEntryOnTargets(log, votersOnline, nextLogIndex, entry),
      commitIndex' = setCommitOnTargets(commitIndex, votersOnline, nextLogIndex),
      committed' = committed.set(nextLogIndex, Filled(entry)),
      globalCommitIndex' = nextLogIndex,
      nextLogIndex' = nextLogIndex + 1,
      kvCell' = applyCommand(kvCell, command),
      leadersByTerm' = leadersByTerm,
    }
  }

  action catchUp(node: Node): bool = all {
    online.get(node),
    log' = log.set(node, committed),
    commitIndex' = commitIndex.set(node, globalCommitIndex),
    online' = online,
    currentTerm' = currentTerm,
    leader' = leader,
    votedFor' = votedFor,
    committed' = committed,
    globalCommitIndex' = globalCommitIndex,
    nextLogIndex' = nextLogIndex,
    kvCell' = kvCell,
    leadersByTerm' = leadersByTerm,
  }

  action step = any {
    {
      nondet node = oneOf(Nodes)
      any {
        setNodeOnline(node, true),
        setNodeOnline(node, false),
        catchUp(node),
        electLeader(node),
      }
    },
    {
      nondet command = oneOf(Commands)
      propose(command)
    }
  }
}
