/// Bounded Raft-backed KV model for Sinomo's compact cluster control plane.
///
/// This small contract model verifies both the existing commit-prefix safety
/// skeleton and the public result semantics required by a reactive cluster:
/// - a local read returns one node's applied committed prefix and may be stale;
/// - an authoritative read succeeds only after a current-term quorum barrier;
/// - a new write succeeds only after quorum commit;
/// - a semantic no-effect write succeeds only with a quorum barrier or an exact
///   durable record of the same committed request; and
/// - committed nodes agree on one canonical prefix whose replay is the KV cell.
///
/// Daemon wake sources, maintenance timing, routing, retry retention, and the
/// full Raft protocol deliberately remain outside this bounded model.
module raft_kv {
  import common.* from "../../lib/common"

  type KvValue = V1 | V2
  type KvCommand = PutV1 | PutV2 | Delete
  type KvCell = NoCell | Cell(KvValue)
  type Entry = { term: int, command: KvCommand }
  type EntrySlot = Empty | Filled(Entry)

  type RequestId = R1 | R2
  type ReadKind = NoRead | LocalRead | AuthoritativeRead
  type WriteKind = NoWrite | QuorumWrite | SemanticNoEffectWrite | ExactCommittedRetry
  type WriteEvidence =
    | NoWriteEvidence
    | QuorumCommitEvidence
    | CurrentTermQuorumBarrierEvidence
    | ExactDurableCommittedRequestEvidence
  type LastOperation =
    | Initialized
    | NodeAvailabilityChanged
    | LeaderElected
    | NodeCaughtUp
    | LocalReadReturned
    | AuthoritativeReadReturned
    | QuorumWriteReturned
    | SemanticNoEffectWriteReturned
    | ExactCommittedRetryReturned
    | BrokenNoQuorumNoEffectReturned

  type ReadResult = {
    kind: ReadKind,
    node: Node,
    succeeded: bool,
    revision: int,
    barrierTerm: int,
    hadCurrentTermQuorumBarrier: bool,
    prefix: int -> EntrySlot,
    cell: KvCell,
  }

  type WriteResult = {
    kind: WriteKind,
    request: RequestId,
    command: KvCommand,
    succeeded: bool,
    revision: int,
    evidenceTerm: int,
    evidence: WriteEvidence,
    semanticNoEffect: bool,
  }

  /// The deliberately broken result is kept separate from the refined result
  /// state. `coverageStep` reaches it as an executable counterexample, while
  /// the production `step` relation contains only the refined transitions.
  type BrokenNoEffectResult = {
    attempted: bool,
    succeeded: bool,
    request: RequestId,
    command: KvCommand,
    semanticNoEffect: bool,
    hadCurrentTermQuorumBarrier: bool,
    hadExactDurableCommittedRequest: bool,
  }

  pure val MaxLogIndex: int = 3
  pure val LogIndexes: Set[int] = Set(1, 2, 3)
  pure val Commands: Set[KvCommand] = Set(PutV1, PutV2, Delete)
  pure val RequestIds: Set[RequestId] = Set(R1, R2)

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

  /// These maps abstract the durable idempotency evidence needed by this
  /// contract. Retention, uncertain outcomes, and response payloads are a
  /// separate algorithm and are intentionally not modeled here.
  var committedRequestCommands: RequestId -> Option[KvCommand]
  var committedRequestRevisions: RequestId -> int
  var committedRequestTerms: RequestId -> int

  var lastOperation: LastOperation
  var lastRead: ReadResult
  var lastWrite: WriteResult
  var brokenNoEffectResult: BrokenNoEffectResult

  pure val emptyLog: int -> EntrySlot =
    LogIndexes.mapBy(_ => Empty)

  pure val emptyLeaders: int -> Set[Node] =
    Set(1, 2, 3, 4).mapBy(_ => Set())

  pure val emptyRequestCommands: RequestId -> Option[KvCommand] =
    RequestIds.mapBy(_ => None)

  pure val emptyRequestNumbers: RequestId -> int =
    RequestIds.mapBy(_ => 0)

  pure val initialRead: ReadResult = {
    kind: NoRead,
    node: N1,
    succeeded: false,
    revision: -1,
    barrierTerm: 0,
    hadCurrentTermQuorumBarrier: false,
    prefix: emptyLog,
    cell: NoCell,
  }

  pure val initialWrite: WriteResult = {
    kind: NoWrite,
    request: R1,
    command: PutV1,
    succeeded: false,
    revision: -1,
    evidenceTerm: 0,
    evidence: NoWriteEvidence,
    semanticNoEffect: false,
  }

  pure val initialBrokenNoEffectResult: BrokenNoEffectResult = {
    attempted: false,
    succeeded: false,
    request: R1,
    command: Delete,
    semanticNoEffect: false,
    hadCurrentTermQuorumBarrier: false,
    hadExactDurableCommittedRequest: false,
  }

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

  def committedPrefixAt(index: int): int -> EntrySlot =
    LogIndexes.mapBy(i => if (i <= index) committed.get(i) else Empty)

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

  def currentTermQuorumBarrierAvailable: bool = {
    val currentLeader = unwrapOr(leader, N1)
    all {
      isSome(leader),
      leadersByTerm.get(currentTerm).contains(currentLeader),
      online.get(currentLeader),
      onlineVoters(online).hasQuorum(),
      commitIndex.get(currentLeader) == globalCommitIndex,
      logsMatchCommittedPrefix(currentLeader),
    }
  }

  def hasExactDurableCommittedRequest(request: RequestId, command: KvCommand): bool =
    match committedRequestCommands.get(request) {
      | Some(committedCommand) => all {
          committedCommand == command,
          committedRequestRevisions.get(request) > 0,
          committedRequestRevisions.get(request) <= globalCommitIndex,
          committedRequestTerms.get(request) > 0,
        }
      | None => false
    }

  def isSemanticNoEffect(command: KvCommand): bool =
    applyCommand(kvCell, command) == kvCell

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
    committedRequestCommands' = emptyRequestCommands,
    committedRequestRevisions' = emptyRequestNumbers,
    committedRequestTerms' = emptyRequestNumbers,
    lastOperation' = Initialized,
    lastRead' = initialRead,
    lastWrite' = initialWrite,
    brokenNoEffectResult' = initialBrokenNoEffectResult,
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
    committedRequestCommands' = committedRequestCommands,
    committedRequestRevisions' = committedRequestRevisions,
    committedRequestTerms' = committedRequestTerms,
    lastOperation' = NodeAvailabilityChanged,
    lastRead' = lastRead,
    lastWrite' = lastWrite,
    brokenNoEffectResult' = brokenNoEffectResult,
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
      committedRequestCommands' = committedRequestCommands,
      committedRequestRevisions' = committedRequestRevisions,
      committedRequestTerms' = committedRequestTerms,
      lastOperation' = LeaderElected,
      lastRead' = lastRead,
      lastWrite' = lastWrite,
      brokenNoEffectResult' = brokenNoEffectResult,
    }
  }

  /// A new effect-changing request publishes success only in the same
  /// transition that appends and commits its entry on an online voter quorum.
  action propose(request: RequestId, command: KvCommand): bool = {
    val votersOnline = onlineVoters(online)
    val entry = { term: currentTerm, command: command }
    val committedRevision = nextLogIndex
    all {
      currentTermQuorumBarrierAvailable,
      committedRequestCommands.get(request) == None,
      not(isSemanticNoEffect(command)),
      nextLogIndex <= MaxLogIndex,
      online' = online,
      currentTerm' = currentTerm,
      leader' = leader,
      votedFor' = votedFor,
      log' = setEntryOnTargets(log, votersOnline, committedRevision, entry),
      commitIndex' = setCommitOnTargets(commitIndex, votersOnline, committedRevision),
      committed' = committed.set(committedRevision, Filled(entry)),
      globalCommitIndex' = committedRevision,
      nextLogIndex' = committedRevision + 1,
      kvCell' = applyCommand(kvCell, command),
      leadersByTerm' = leadersByTerm,
      committedRequestCommands' = committedRequestCommands.set(request, Some(command)),
      committedRequestRevisions' = committedRequestRevisions.set(request, committedRevision),
      committedRequestTerms' = committedRequestTerms.set(request, currentTerm),
      lastOperation' = QuorumWriteReturned,
      lastRead' = lastRead,
      lastWrite' = {
        kind: QuorumWrite,
        request: request,
        command: command,
        succeeded: true,
        revision: committedRevision,
        evidenceTerm: currentTerm,
        evidence: QuorumCommitEvidence,
        semanticNoEffect: false,
      },
      brokenNoEffectResult' = brokenNoEffectResult,
    }
  }

  /// A locally detected no-effect request may avoid a new entry only after a
  /// current-term quorum barrier confirms that the compared state is canonical.
  action confirmSemanticNoEffect(request: RequestId, command: KvCommand): bool = all {
    currentTermQuorumBarrierAvailable,
    isSemanticNoEffect(command),
    not(hasExactDurableCommittedRequest(request, command)),
    online' = online,
    currentTerm' = currentTerm,
    leader' = leader,
    votedFor' = votedFor,
    log' = log,
    commitIndex' = commitIndex,
    committed' = committed,
    globalCommitIndex' = globalCommitIndex,
    nextLogIndex' = nextLogIndex,
    kvCell' = kvCell,
    leadersByTerm' = leadersByTerm,
    committedRequestCommands' = committedRequestCommands,
    committedRequestRevisions' = committedRequestRevisions,
    committedRequestTerms' = committedRequestTerms,
    lastOperation' = SemanticNoEffectWriteReturned,
    lastRead' = lastRead,
    lastWrite' = {
      kind: SemanticNoEffectWrite,
      request: request,
      command: command,
      succeeded: true,
      revision: globalCommitIndex,
      evidenceTerm: currentTerm,
      evidence: CurrentTermQuorumBarrierEvidence,
      semanticNoEffect: true,
    },
    brokenNoEffectResult' = brokenNoEffectResult,
  }

  /// An exact retry may reuse a prior response without live quorum only when a
  /// durable record binds the same request id and command to its committed
  /// revision and term.
  action returnExactCommittedRetry(request: RequestId, command: KvCommand): bool = all {
    hasExactDurableCommittedRequest(request, command),
    online' = online,
    currentTerm' = currentTerm,
    leader' = leader,
    votedFor' = votedFor,
    log' = log,
    commitIndex' = commitIndex,
    committed' = committed,
    globalCommitIndex' = globalCommitIndex,
    nextLogIndex' = nextLogIndex,
    kvCell' = kvCell,
    leadersByTerm' = leadersByTerm,
    committedRequestCommands' = committedRequestCommands,
    committedRequestRevisions' = committedRequestRevisions,
    committedRequestTerms' = committedRequestTerms,
    lastOperation' = ExactCommittedRetryReturned,
    lastRead' = lastRead,
    lastWrite' = {
      kind: ExactCommittedRetry,
      request: request,
      command: command,
      succeeded: true,
      revision: committedRequestRevisions.get(request),
      evidenceTerm: committedRequestTerms.get(request),
      evidence: ExactDurableCommittedRequestEvidence,
      semanticNoEffect: isSemanticNoEffect(command),
    },
    brokenNoEffectResult' = brokenNoEffectResult,
  }

  /// A local read never needs network quorum. Its revision and returned prefix
  /// are exactly the selected node's locally applied committed prefix.
  action readLocal(node: Node): bool = {
    val localRevision = commitIndex.get(node)
    all {
      online' = online,
      currentTerm' = currentTerm,
      leader' = leader,
      votedFor' = votedFor,
      log' = log,
      commitIndex' = commitIndex,
      committed' = committed,
      globalCommitIndex' = globalCommitIndex,
      nextLogIndex' = nextLogIndex,
      kvCell' = kvCell,
      leadersByTerm' = leadersByTerm,
      committedRequestCommands' = committedRequestCommands,
      committedRequestRevisions' = committedRequestRevisions,
      committedRequestTerms' = committedRequestTerms,
      lastOperation' = LocalReadReturned,
      lastRead' = {
        kind: LocalRead,
        node: node,
        succeeded: true,
        revision: localRevision,
        barrierTerm: 0,
        hadCurrentTermQuorumBarrier: false,
        prefix: committedPrefixAt(localRevision),
        cell: replayCommittedAt(localRevision),
      },
      lastWrite' = lastWrite,
      brokenNoEffectResult' = brokenNoEffectResult,
    }
  }

  /// Authoritative failure is explicit: it returns no revision or prefix and
  /// never silently substitutes the caller's local view.
  action readAuthoritative(node: Node): bool = {
    val barrierEstablished = currentTermQuorumBarrierAvailable
    all {
      online' = online,
      currentTerm' = currentTerm,
      leader' = leader,
      votedFor' = votedFor,
      log' = log,
      commitIndex' = commitIndex,
      committed' = committed,
      globalCommitIndex' = globalCommitIndex,
      nextLogIndex' = nextLogIndex,
      kvCell' = kvCell,
      leadersByTerm' = leadersByTerm,
      committedRequestCommands' = committedRequestCommands,
      committedRequestRevisions' = committedRequestRevisions,
      committedRequestTerms' = committedRequestTerms,
      lastOperation' = AuthoritativeReadReturned,
      lastRead' = if (barrierEstablished) {
        {
          kind: AuthoritativeRead,
          node: node,
          succeeded: true,
          revision: globalCommitIndex,
          barrierTerm: currentTerm,
          hadCurrentTermQuorumBarrier: true,
          prefix: committed,
          cell: kvCell,
        }
      } else {
        {
          kind: AuthoritativeRead,
          node: node,
          succeeded: false,
          revision: -1,
          barrierTerm: 0,
          hadCurrentTermQuorumBarrier: false,
          prefix: emptyLog,
          cell: NoCell,
        }
      },
      lastWrite' = lastWrite,
      brokenNoEffectResult' = brokenNoEffectResult,
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
    committedRequestCommands' = committedRequestCommands,
    committedRequestRevisions' = committedRequestRevisions,
    committedRequestTerms' = committedRequestTerms,
    lastOperation' = NodeCaughtUp,
    lastRead' = lastRead,
    lastWrite' = lastWrite,
    brokenNoEffectResult' = brokenNoEffectResult,
  }

  /// Deliberately broken legacy behavior: local equality is treated as success
  /// even though neither a current-term quorum barrier nor an exact durable
  /// committed-request record exists. This action is coverage-only and must not
  /// be added to `step`.
  action brokenNoQuorumNoEffect(request: RequestId, command: KvCommand): bool = all {
    not(currentTermQuorumBarrierAvailable),
    isSemanticNoEffect(command),
    not(hasExactDurableCommittedRequest(request, command)),
    online' = online,
    currentTerm' = currentTerm,
    leader' = leader,
    votedFor' = votedFor,
    log' = log,
    commitIndex' = commitIndex,
    committed' = committed,
    globalCommitIndex' = globalCommitIndex,
    nextLogIndex' = nextLogIndex,
    kvCell' = kvCell,
    leadersByTerm' = leadersByTerm,
    committedRequestCommands' = committedRequestCommands,
    committedRequestRevisions' = committedRequestRevisions,
    committedRequestTerms' = committedRequestTerms,
    lastOperation' = BrokenNoQuorumNoEffectReturned,
    lastRead' = lastRead,
    lastWrite' = lastWrite,
    brokenNoEffectResult' = {
      attempted: true,
      succeeded: true,
      request: request,
      command: command,
      semanticNoEffect: true,
      hadCurrentTermQuorumBarrier: false,
      hadExactDurableCommittedRequest: false,
    },
  }

  action step = any {
    {
      nondet node = oneOf(Nodes)
      any {
        setNodeOnline(node, true),
        setNodeOnline(node, false),
        catchUp(node),
        electLeader(node),
        readLocal(node),
        readAuthoritative(node),
      }
    },
    {
      nondet request = oneOf(RequestIds)
      nondet command = oneOf(Commands)
      any {
        propose(request, command),
        confirmSemanticNoEffect(request, command),
        returnExactCommittedRetry(request, command),
      }
    }
  }

  /// Deterministically makes the legacy counterexample reachable for witness
  /// coverage, then returns to the refined nondeterministic step relation.
  action coverageStep =
    if (not(brokenNoEffectResult.attempted)) {
      if (online.get(N2)) {
        setNodeOnline(N2, false)
      } else if (online.get(N3)) {
        setNodeOnline(N3, false)
      } else {
        brokenNoQuorumNoEffect(R1, Delete)
      }
    } else {
      step
    }
}
