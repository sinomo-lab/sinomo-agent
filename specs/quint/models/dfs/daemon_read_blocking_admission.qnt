/// Async admission for blocking daemon DFS read lanes.
///
/// The object-RPC transport is already asynchronous, but a read service lane
/// is protected by a blocking mutex. This model places the bounded read-slot
/// admission before blocking-worker dispatch: at most two reads own the two
/// modeled lanes, while a third read remains an async waiter with neither a
/// lane nor a blocking worker. An unrelated mutation uses an independent
/// blocking-worker path and therefore remains admissible when both read slots
/// are occupied.
///
/// Caller detachment is deliberately not work completion. A detached
/// spawn-blocking read retains its slot, lane, and worker until the blocking
/// closure returns; only that completion releases resources for the waiter.
module dfs_daemon_read_blocking_admission {
  import common.* from "../../lib/common"

  type ReadRequest = ReadOne | ReadTwo | ReadThree
  type ReadLane = ReadLaneOne | ReadLaneTwo
  type ReadAdmissionTransition =
    | ReadAdmissionInitialized
    | WorkloadSubmitted
    | ReadAdmittedToBlockingWorker
    | MutationAdmittedToBlockingWorker
    | ReadCallerDetached
    | AttachedReadFinished
    | DetachedReadFinished
    | MutationFinished
    | AdmissionTerminalStutter

  type DaemonReadBlockingAdmissionState = {
    submittedReads: Set[ReadRequest],
    activeReads: Set[ReadRequest],
    detachedReads: Set[ReadRequest],
    completedReads: Set[ReadRequest],
    detachedCompletedReads: Set[ReadRequest],
    laneByRead: ReadRequest -> Option[ReadLane],
    blockingWorkerByRead: ReadRequest -> bool,
    readSlotsInUse: int,
    mutationSubmitted: bool,
    mutationActive: bool,
    mutationCompleted: bool,
    lastAdmittedRead: Option[ReadRequest],
    lastAdmittedLane: Option[ReadLane],
    lastDetachedRead: Option[ReadRequest],
    lastReleasedRead: Option[ReadRequest],
    releasedLaneByRead: ReadRequest -> Option[ReadLane],
    lastTransition: ReadAdmissionTransition,
  }

  pure val Reads: Set[ReadRequest] = Set(ReadOne, ReadTwo, ReadThree)
  pure val ReadLanes: Set[ReadLane] = Set(ReadLaneOne, ReadLaneTwo)
  pure val ReadSlotCap: int = 2

  pure val EmptyReadLanes: ReadRequest -> Option[ReadLane] =
    Reads.mapBy(_ => None)
  pure val EmptyReadWorkers: ReadRequest -> bool =
    Reads.mapBy(_ => false)

  pure def readRank(request: ReadRequest): int =
    match request {
      | ReadOne => 1
      | ReadTwo => 2
      | ReadThree => 3
    }

  pure def pendingReads(
    state: DaemonReadBlockingAdmissionState
  ): Set[ReadRequest] =
    state.submittedReads.filter(request => all {
      not(state.activeReads.contains(request)),
      not(state.completedReads.contains(request)),
    })

  pure def laneIsFree(
    state: DaemonReadBlockingAdmissionState,
    lane: ReadLane,
  ): bool =
    state.activeReads.forall(request =>
      state.laneByRead.get(request) != Some(lane)
    )

  pure def noEarlierPendingRead(
    state: DaemonReadBlockingAdmissionState,
    request: ReadRequest,
  ): bool =
    pendingReads(state).forall(other =>
      other == request or readRank(other) >= readRank(request)
    )

  pure def canAdmitRead(
    state: DaemonReadBlockingAdmissionState,
    request: ReadRequest,
    lane: ReadLane,
  ): bool = all {
    pendingReads(state).contains(request),
    noEarlierPendingRead(state, request),
    state.readSlotsInUse < ReadSlotCap,
    laneIsFree(state, lane),
  }

  /// Mutation admission intentionally has no dependency on read-slot or
  /// read-lane occupancy. The scheduler may still apply its separate shard or
  /// global mutation rules outside this bounded read-pool slice.
  pure def canAdmitMutation(
    state: DaemonReadBlockingAdmissionState
  ): bool = all {
    state.mutationSubmitted,
    not(state.mutationActive),
    not(state.mutationCompleted),
  }

  pure val initialState: DaemonReadBlockingAdmissionState = {
    submittedReads: Set(),
    activeReads: Set(),
    detachedReads: Set(),
    completedReads: Set(),
    detachedCompletedReads: Set(),
    laneByRead: EmptyReadLanes,
    blockingWorkerByRead: EmptyReadWorkers,
    readSlotsInUse: 0,
    mutationSubmitted: false,
    mutationActive: false,
    mutationCompleted: false,
    lastAdmittedRead: None,
    lastAdmittedLane: None,
    lastDetachedRead: None,
    lastReleasedRead: None,
    releasedLaneByRead: EmptyReadLanes,
    lastTransition: ReadAdmissionInitialized,
  }

  var readAdmission: DaemonReadBlockingAdmissionState

  action init = readAdmission' = initialState

  /// Submit one bounded workload atomically so every witness describes the
  /// same arrival order: ReadOne and ReadTwo fill the pool before ReadThree,
  /// while the unrelated mutation is already eligible for its own worker.
  action submitWorkload: bool = all {
    readAdmission.submittedReads == Set(),
    not(readAdmission.mutationSubmitted),
    readAdmission' = readAdmission
      .with("submittedReads", Reads)
      .with("mutationSubmitted", true)
      .with("lastTransition", WorkloadSubmitted),
  }

  action admitRead(request: ReadRequest, lane: ReadLane): bool = all {
    canAdmitRead(readAdmission, request, lane),
    readAdmission' = readAdmission
      .with(
        "activeReads",
        readAdmission.activeReads.union(Set(request))
      )
      .with(
        "laneByRead",
        readAdmission.laneByRead.set(request, Some(lane))
      )
      .with(
        "blockingWorkerByRead",
        readAdmission.blockingWorkerByRead.set(request, true)
      )
      .with("readSlotsInUse", readAdmission.readSlotsInUse + 1)
      .with("lastAdmittedRead", Some(request))
      .with("lastAdmittedLane", Some(lane))
      .with("lastTransition", ReadAdmittedToBlockingWorker),
  }

  action admitMutation: bool = all {
    canAdmitMutation(readAdmission),
    readAdmission' = readAdmission
      .with("mutationActive", true)
      .with("lastTransition", MutationAdmittedToBlockingWorker),
  }

  /// Dropping the async caller does not cancel the spawn-blocking closure and
  /// therefore cannot release any admission resource.
  action detachReadCaller(request: ReadRequest): bool = all {
    readAdmission.activeReads.contains(request),
    not(readAdmission.detachedReads.contains(request)),
    readAdmission' = readAdmission
      .with(
        "detachedReads",
        readAdmission.detachedReads.union(Set(request))
      )
      .with("lastDetachedRead", Some(request))
      .with("lastTransition", ReadCallerDetached),
  }

  action finishAttachedRead(
    request: ReadRequest,
    lane: ReadLane,
  ): bool = all {
    readAdmission.activeReads.contains(request),
    not(readAdmission.detachedReads.contains(request)),
    readAdmission.laneByRead.get(request) == Some(lane),
    readAdmission' = readAdmission
      .with(
        "activeReads",
        readAdmission.activeReads.filter(active => active != request)
      )
      .with(
        "completedReads",
        readAdmission.completedReads.union(Set(request))
      )
      .with(
        "laneByRead",
        readAdmission.laneByRead.set(request, None)
      )
      .with(
        "blockingWorkerByRead",
        readAdmission.blockingWorkerByRead.set(request, false)
      )
      .with("readSlotsInUse", readAdmission.readSlotsInUse - 1)
      .with("lastReleasedRead", Some(request))
      .with(
        "releasedLaneByRead",
        readAdmission.releasedLaneByRead.set(request, Some(lane))
      )
      .with("lastTransition", AttachedReadFinished),
  }

  /// This is the only completion transition for detached work. It releases
  /// the slot, lane, and worker together after the blocking closure returns.
  action finishDetachedRead(
    request: ReadRequest,
    lane: ReadLane,
  ): bool = all {
    readAdmission.activeReads.contains(request),
    readAdmission.detachedReads.contains(request),
    readAdmission.laneByRead.get(request) == Some(lane),
    readAdmission' = readAdmission
      .with(
        "activeReads",
        readAdmission.activeReads.filter(active => active != request)
      )
      .with(
        "detachedReads",
        readAdmission.detachedReads.filter(detached => detached != request)
      )
      .with(
        "completedReads",
        readAdmission.completedReads.union(Set(request))
      )
      .with(
        "detachedCompletedReads",
        readAdmission.detachedCompletedReads.union(Set(request))
      )
      .with(
        "laneByRead",
        readAdmission.laneByRead.set(request, None)
      )
      .with(
        "blockingWorkerByRead",
        readAdmission.blockingWorkerByRead.set(request, false)
      )
      .with("readSlotsInUse", readAdmission.readSlotsInUse - 1)
      .with("lastReleasedRead", Some(request))
      .with(
        "releasedLaneByRead",
        readAdmission.releasedLaneByRead.set(request, Some(lane))
      )
      .with("lastTransition", DetachedReadFinished),
  }

  action finishMutation: bool = all {
    readAdmission.mutationActive,
    readAdmission' = readAdmission
      .with("mutationActive", false)
      .with("mutationCompleted", true)
      .with("lastTransition", MutationFinished),
  }

  action stutterTerminal: bool = all {
    readAdmission.completedReads == Reads,
    readAdmission.mutationCompleted,
    readAdmission' = readAdmission
      .with("lastTransition", AdmissionTerminalStutter),
  }

  action step = any {
    submitWorkload,
    {
      nondet request = oneOf(Reads)
      nondet lane = oneOf(ReadLanes)
      admitRead(request, lane)
    },
    admitMutation,
    {
      nondet request = oneOf(Reads)
      detachReadCaller(request)
    },
    {
      nondet request = oneOf(Reads)
      nondet lane = oneOf(ReadLanes)
      finishAttachedRead(request, lane)
    },
    {
      nondet request = oneOf(Reads)
      nondet lane = oneOf(ReadLanes)
      finishDetachedRead(request, lane)
    },
    finishMutation,
    stutterTerminal,
  }
}
