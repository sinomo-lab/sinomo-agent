/// DFS routed namespace freshness.
///
/// This module refines `dfs_authority_routing` for the fragile namespace
/// response path. It models the requester-side catch-up contract after a routed
/// namespace mutation or distributed current lookup: a successful operation may
/// complete only after the requester has replayed enough primary-sourced
/// metadata journal, or after an explicit SyncPath response has installed the
/// authoritative path object. It intentionally does not model backend metadata
/// tables, object indexes, or read-snapshot caching.
module dfs_routed_namespace_freshness {
  import common.* from "../../lib/common"

  type NamespaceOperation =
    | RoutedCreate
    | RoutedDelete
    | DistributedLookup

  type RequesterRole = MetadataReplica | NonReplicaMirror
  type PathState = PathMissing | PathLive | PathTombstone
  type RequestPhase = Idle | CatchingUp | Terminal
  type TerminalOutcome = NoOutcome | CompletedFresh | FailedBounded
  type Decision =
    | NoDecision
    | NamespaceRequestStarted
    | JournalBatchApplied
    | SyncedPathApplied
    | LocalReplicaHitCompleted
    | FreshRequestCompleted
    | BoundedFailure
    | TerminalStutter

  pure val SeqValues: Set[int] = Set(0, 1, 2, 3)
  pure val StartSeqValues: Set[int] = Set(0, 1, 2)
  pure val NamespaceOperations: Set[NamespaceOperation] =
    Set(RoutedCreate, RoutedDelete, DistributedLookup)
  pure val RequesterRoles: Set[RequesterRole] =
    Set(MetadataReplica, NonReplicaMirror)
  pure val PathStates: Set[PathState] =
    Set(PathMissing, PathLive, PathTombstone)

  var phase: RequestPhase
  var operation: NamespaceOperation
  var requester: Node
  var currentPrimary: Node
  var requesterRole: RequesterRole
  var primaryReachable: bool
  var primarySeq: int
  var requesterSeq: int
  var targetSeq: int
  var primaryPathState: PathState
  var localPathState: PathState
  var returnedPathState: PathState
  var syncedPathApplied: bool
  var lastBatchLimitReached: bool
  var acceptedJournalBatchSources: Set[Node]
  var terminalOutcome: TerminalOutcome
  var lastDecision: Decision

  pure def mutationTargetState(op: NamespaceOperation): PathState =
    match op {
      | RoutedCreate => PathLive
      | RoutedDelete => PathTombstone
      | DistributedLookup => PathMissing
    }

  pure def advanceOne(current: int, target: int): int =
    if (current < target) { current + 1 } else { current }

  pure def requesterHasFreshPath(
    localState: PathState,
    primaryState: PathState,
    localSeq: int,
    target: int,
    synced: bool
  ): bool =
    all {
      localState == primaryState,
      any {
        localSeq >= target,
        synced,
      },
    }

  pure def localReplicaCanServeCurrentHit(
    role: RequesterRole,
    localState: PathState,
    primaryState: PathState,
    localSeq: int,
    target: int
  ): bool =
    all {
      role == MetadataReplica,
      localSeq >= target,
      localState == primaryState,
    }

  action init = all {
    phase' = Idle,
    operation' = DistributedLookup,
    requester' = N2,
    currentPrimary' = N1,
    requesterRole' = MetadataReplica,
    primaryReachable' = true,
    primarySeq' = 0,
    requesterSeq' = 0,
    targetSeq' = 0,
    primaryPathState' = PathMissing,
    localPathState' = PathMissing,
    returnedPathState' = PathMissing,
    syncedPathApplied' = false,
    lastBatchLimitReached' = false,
    acceptedJournalBatchSources' = Set(),
    terminalOutcome' = NoOutcome,
    lastDecision' = NoDecision,
  }

  /// Start either a routed mutation or a distributed current lookup. Mutations
  /// advance the primary journal by one sequence and set a new authoritative
  /// path state. Lookups observe an existing primary state at `primaryStart`.
  action startNamespaceRequest(
    op: NamespaceOperation,
    role: RequesterRole,
    req: Node,
    primary: Node,
    primaryStart: int,
    requesterStart: int,
    primaryState: PathState,
    localState: PathState,
    reachable: bool
  ): bool = {
    val isMutation = op == RoutedCreate or op == RoutedDelete
    val newPrimarySeq = if (isMutation) { primaryStart + 1 } else { primaryStart }
    val newPrimaryState = if (isMutation) { mutationTargetState(op) } else { primaryState }
    all {
      phase == Idle,
      req != primary,
      if (isMutation) { StartSeqValues.contains(primaryStart) } else {
        SeqValues.contains(primaryStart)
      },
      SeqValues.contains(requesterStart),
      requesterStart <= primaryStart,
      PathStates.contains(primaryState),
      PathStates.contains(localState),
      operation' = op,
      requester' = req,
      currentPrimary' = primary,
      requesterRole' = role,
      primaryReachable' = reachable,
      primarySeq' = newPrimarySeq,
      requesterSeq' = requesterStart,
      targetSeq' = newPrimarySeq,
      primaryPathState' = newPrimaryState,
      localPathState' = localState,
      returnedPathState' = PathMissing,
      syncedPathApplied' = false,
      lastBatchLimitReached' = false,
      acceptedJournalBatchSources' = Set(),
      terminalOutcome' = NoOutcome,
      phase' = CatchingUp,
      lastDecision' = NamespaceRequestStarted,
    }
  }

  /// Apply one bounded metadata journal batch from the current primary. If the
  /// batch does not reach the target sequence, the requester must keep syncing
  /// instead of treating the routed operation as complete.
  action applyPrimaryJournalBatch(source: Node): bool = {
    val nextSeq = advanceOne(requesterSeq, targetSeq)
    all {
      phase == CatchingUp,
      primaryReachable,
      source == currentPrimary,
      requesterSeq < targetSeq,
      requesterSeq' = nextSeq,
      localPathState' =
        if (nextSeq >= targetSeq) { primaryPathState } else { localPathState },
      lastBatchLimitReached' = nextSeq < targetSeq,
      acceptedJournalBatchSources' = acceptedJournalBatchSources.union(Set(source)),
      operation' = operation,
      requester' = requester,
      currentPrimary' = currentPrimary,
      requesterRole' = requesterRole,
      primaryReachable' = primaryReachable,
      primarySeq' = primarySeq,
      targetSeq' = targetSeq,
      primaryPathState' = primaryPathState,
      returnedPathState' = returnedPathState,
      syncedPathApplied' = syncedPathApplied,
      terminalOutcome' = terminalOutcome,
      phase' = phase,
      lastDecision' = JournalBatchApplied,
    }
  }

  /// SyncPath installs the authoritative path object directly. This is the
  /// abstraction for routed responses that carry `synced_path_object(s)` and for
  /// distributed non-replica lookup/listing catch-up.
  action applySyncedPathObject: bool = all {
    phase == CatchingUp,
    primaryReachable,
    localPathState' = primaryPathState,
    syncedPathApplied' = true,
    lastBatchLimitReached' = false,
    operation' = operation,
    requester' = requester,
    currentPrimary' = currentPrimary,
    requesterRole' = requesterRole,
    primaryReachable' = primaryReachable,
    primarySeq' = primarySeq,
    requesterSeq' = requesterSeq,
    targetSeq' = targetSeq,
    primaryPathState' = primaryPathState,
    returnedPathState' = returnedPathState,
    acceptedJournalBatchSources' = acceptedJournalBatchSources,
    terminalOutcome' = terminalOutcome,
    phase' = phase,
    lastDecision' = SyncedPathApplied,
  }

  /// A metadata replica may complete from its local view only when that local
  /// view is already known to be at the target journal sequence. Non-replica
  /// mirrors must use primary-sourced catch-up for distributed current reads.
  action completeLocalReplicaCurrentHit: bool = all {
    phase == CatchingUp,
    operation == DistributedLookup,
    localReplicaCanServeCurrentHit(
      requesterRole,
      localPathState,
      primaryPathState,
      requesterSeq,
      targetSeq
    ),
    returnedPathState' = localPathState,
    terminalOutcome' = CompletedFresh,
    operation' = operation,
    requester' = requester,
    currentPrimary' = currentPrimary,
    requesterRole' = requesterRole,
    primaryReachable' = primaryReachable,
    primarySeq' = primarySeq,
    requesterSeq' = requesterSeq,
    targetSeq' = targetSeq,
    primaryPathState' = primaryPathState,
    localPathState' = localPathState,
    syncedPathApplied' = syncedPathApplied,
    lastBatchLimitReached' = lastBatchLimitReached,
    acceptedJournalBatchSources' = acceptedJournalBatchSources,
    phase' = Terminal,
    lastDecision' = LocalReplicaHitCompleted,
  }

  action completeFreshRequest: bool = all {
    phase == CatchingUp,
    requesterHasFreshPath(
      localPathState,
      primaryPathState,
      requesterSeq,
      targetSeq,
      syncedPathApplied
    ),
    not(lastBatchLimitReached),
    returnedPathState' = localPathState,
    terminalOutcome' = CompletedFresh,
    operation' = operation,
    requester' = requester,
    currentPrimary' = currentPrimary,
    requesterRole' = requesterRole,
    primaryReachable' = primaryReachable,
    primarySeq' = primarySeq,
    requesterSeq' = requesterSeq,
    targetSeq' = targetSeq,
    primaryPathState' = primaryPathState,
    localPathState' = localPathState,
    syncedPathApplied' = syncedPathApplied,
    lastBatchLimitReached' = lastBatchLimitReached,
    acceptedJournalBatchSources' = acceptedJournalBatchSources,
    phase' = Terminal,
    lastDecision' = FreshRequestCompleted,
  }

  action failBounded: bool = all {
    phase == CatchingUp,
    any {
      not(primaryReachable),
      all {
        requesterSeq < targetSeq,
        not(syncedPathApplied),
      },
    },
    returnedPathState' = PathMissing,
    terminalOutcome' = FailedBounded,
    operation' = operation,
    requester' = requester,
    currentPrimary' = currentPrimary,
    requesterRole' = requesterRole,
    primaryReachable' = primaryReachable,
    primarySeq' = primarySeq,
    requesterSeq' = requesterSeq,
    targetSeq' = targetSeq,
    primaryPathState' = primaryPathState,
    localPathState' = localPathState,
    syncedPathApplied' = syncedPathApplied,
    lastBatchLimitReached' = lastBatchLimitReached,
    acceptedJournalBatchSources' = acceptedJournalBatchSources,
    phase' = Terminal,
    lastDecision' = BoundedFailure,
  }

  action stutterTerminal: bool = all {
    phase == Terminal,
    operation' = operation,
    requester' = requester,
    currentPrimary' = currentPrimary,
    requesterRole' = requesterRole,
    primaryReachable' = primaryReachable,
    primarySeq' = primarySeq,
    requesterSeq' = requesterSeq,
    targetSeq' = targetSeq,
    primaryPathState' = primaryPathState,
    localPathState' = localPathState,
    returnedPathState' = returnedPathState,
    syncedPathApplied' = syncedPathApplied,
    lastBatchLimitReached' = lastBatchLimitReached,
    acceptedJournalBatchSources' = acceptedJournalBatchSources,
    terminalOutcome' = terminalOutcome,
    phase' = phase,
    lastDecision' = TerminalStutter,
  }

  action step = any {
    {
      nondet op = oneOf(NamespaceOperations)
      nondet role = oneOf(RequesterRoles)
      nondet req = oneOf(Nodes)
      nondet primary = oneOf(Nodes)
      nondet primaryStart = oneOf(SeqValues)
      nondet requesterStart = oneOf(SeqValues)
      nondet primaryState = oneOf(PathStates)
      nondet localState = oneOf(PathStates)
      nondet reachable = oneOf(Set(true, false))
      startNamespaceRequest(
        op,
        role,
        req,
        primary,
        primaryStart,
        requesterStart,
        primaryState,
        localState,
        reachable
      )
    },
    {
      nondet source = oneOf(Nodes)
      applyPrimaryJournalBatch(source)
    },
    applySyncedPathObject,
    completeLocalReplicaCurrentHit,
    completeFreshRequest,
    failBounded,
    stutterTerminal,
  }
}
