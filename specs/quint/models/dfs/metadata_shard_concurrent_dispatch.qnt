/// Concurrent daemon dispatch for independent DFS metadata shards.
///
/// The object-RPC router already admits concurrent requests. This model
/// isolates the scheduler that turns that transport concurrency into safe DFS
/// core concurrency: one ordered mutation owner per affected shard, atomic
/// acquisition for cross-shard operations, and an all-shard fallback for
/// legacy messages whose exact footprint is unavailable.
module dfs_metadata_shard_concurrent_dispatch {
  import common.* from "../../lib/common"

  type DispatchShard = RootShard | ServicesShard | UsersShard
  type DispatchCommand =
    | RootFirst
    | RootSecond
    | ServicesIndependent
    | UsersIndependent
    | CrossRootServices
    | GlobalUnknown
    | PostTopologyRoot
  type DispatchTransition =
    | DispatchInitialized
    | CommandSubmitted
    | ExactCommandWaitingOnGlobal
    | GlobalFenceQueued
    | CommandAdmitted
    | CommandCompleted
    | ActiveCallerDetached
    | DetachedCommandCompleted
    | TopologyAdvanced
    | TerminalStutter

  type MetadataShardConcurrentDispatchState = {
    generation: int,
    nextTicket: int,
    submitted: Set[DispatchCommand],
    waitingOnGlobal: Set[DispatchCommand],
    active: Set[DispatchCommand],
    detached: Set[DispatchCommand],
    completed: Set[DispatchCommand],
    cancelled: Set[DispatchCommand],
    ticketByCommand: DispatchCommand -> int,
    generationByCommand: DispatchCommand -> int,
    heldByShard: DispatchShard -> Option[DispatchCommand],
    reservedByShard: DispatchShard -> Option[DispatchCommand],
    globalFenceQueued: bool,
    lastAdmitted: Option[DispatchCommand],
    lastReleased: Option[DispatchCommand],
    topologyAdvanceObserved: bool,
    lastTransition: DispatchTransition,
  }

  pure val Shards: Set[DispatchShard] =
    Set(RootShard, ServicesShard, UsersShard)
  pure val Commands: Set[DispatchCommand] = Set(
    RootFirst,
    RootSecond,
    ServicesIndependent,
    UsersIndependent,
    CrossRootServices,
    GlobalUnknown,
    PostTopologyRoot,
  )

  pure val EmptyCommandNumbers: DispatchCommand -> int =
    Commands.mapBy(_ => 0)
  pure val EmptyShardHolders: DispatchShard -> Option[DispatchCommand] =
    Shards.mapBy(_ => None)

  pure def affectedShards(command: DispatchCommand): Set[DispatchShard] =
    match command {
      | RootFirst => Set(RootShard)
      | RootSecond => Set(RootShard)
      | ServicesIndependent => Set(ServicesShard)
      | UsersIndependent => Set(UsersShard)
      | CrossRootServices => Set(RootShard, ServicesShard)
      | GlobalUnknown => Shards
      | PostTopologyRoot => Set(RootShard)
    }

  pure def terminal(
    state: MetadataShardConcurrentDispatchState,
    command: DispatchCommand,
  ): bool = any {
    state.completed.contains(command),
    state.cancelled.contains(command),
  }

  pure def pendingCommands(
    state: MetadataShardConcurrentDispatchState
  ): Set[DispatchCommand] =
    state.submitted.filter(command => all {
      not(state.active.contains(command)),
      not(terminal(state, command)),
    })

  pure def footprintsOverlap(
    left: DispatchCommand,
    right: DispatchCommand,
  ): bool =
    affectedShards(left).intersect(affectedShards(right)).size() > 0

  pure def hasOlderOverlappingPending(
    state: MetadataShardConcurrentDispatchState,
    command: DispatchCommand,
  ): bool =
    pendingCommands(state).filter(other => all {
      other != command,
      state.ticketByCommand.get(other) < state.ticketByCommand.get(command),
      footprintsOverlap(other, command),
    }).size() > 0

  pure def canAdmit(
    state: MetadataShardConcurrentDispatchState,
    command: DispatchCommand,
  ): bool = if (command == GlobalUnknown) {
    all {
      pendingCommands(state).contains(command),
      state.generationByCommand.get(command) == state.generation,
      state.globalFenceQueued,
      state.active == Set(),
    }
  } else {
    all {
      pendingCommands(state).contains(command),
      state.generationByCommand.get(command) == state.generation,
      not(state.globalFenceQueued),
      not(hasOlderOverlappingPending(state, command)),
      affectedShards(command).forall(shard => all {
        state.heldByShard.get(shard) == None,
        any {
          state.reservedByShard.get(shard) == None,
          state.reservedByShard.get(shard) == Some(command),
        },
      }),
    }
  }

  pure val initialState: MetadataShardConcurrentDispatchState = {
    generation: 1,
    nextTicket: 1,
    submitted: Set(),
    waitingOnGlobal: Set(),
    active: Set(),
    detached: Set(),
    completed: Set(),
    cancelled: Set(),
    ticketByCommand: EmptyCommandNumbers,
    generationByCommand: EmptyCommandNumbers,
    heldByShard: EmptyShardHolders,
    reservedByShard: EmptyShardHolders,
    globalFenceQueued: false,
    lastAdmitted: None,
    lastReleased: None,
    topologyAdvanceObserved: false,
    lastTransition: DispatchInitialized,
  }

  var dispatch: MetadataShardConcurrentDispatchState

  action init = dispatch' = initialState

  action submit(command: DispatchCommand): bool = all {
    not(dispatch.submitted.contains(command)),
    not(terminal(dispatch, command)),
    dispatch' = dispatch
      .with("submitted", dispatch.submitted.union(Set(command)))
      .with(
        "ticketByCommand",
        dispatch.ticketByCommand.set(command, dispatch.nextTicket)
      )
      .with(
        "generationByCommand",
        dispatch.generationByCommand.set(command, dispatch.generation)
      )
      .with("nextTicket", dispatch.nextTicket + 1)
      .with("lastTransition", CommandSubmitted),
  }

  /// Exact work acquires its shard permit before awaiting the global read
  /// fence. If a global writer is already queued, the exact command may hold
  /// only its shard reservation while it waits; it never contributes a global
  /// reader that can extend the writer's wait through a same-shard backlog.
  action waitOnQueuedGlobal(command: DispatchCommand): bool = all {
    command != GlobalUnknown,
    pendingCommands(dispatch).contains(command),
    dispatch.generationByCommand.get(command) == dispatch.generation,
    dispatch.globalFenceQueued,
    not(dispatch.waitingOnGlobal.contains(command)),
    not(hasOlderOverlappingPending(dispatch, command)),
    affectedShards(command).forall(shard => all {
      dispatch.heldByShard.get(shard) == None,
      dispatch.reservedByShard.get(shard) == None,
    }),
    dispatch' = dispatch
      .with(
        "waitingOnGlobal",
        dispatch.waitingOnGlobal.union(Set(command))
      )
      .with(
        "reservedByShard",
        Shards.mapBy(shard =>
          if (affectedShards(command).contains(shard)) {
            Some(command)
          } else {
            dispatch.reservedByShard.get(shard)
          }
        )
      )
      .with("lastTransition", ExactCommandWaitingOnGlobal),
  }

  /// Once a global request queues for the write fence, new exact admissions
  /// cannot acquire a global read guard ahead of it. Existing active readers
  /// may finish, and shard-only reservations do not block the global writer.
  action queueGlobalFence: bool = all {
    pendingCommands(dispatch).contains(GlobalUnknown),
    dispatch.generationByCommand.get(GlobalUnknown) == dispatch.generation,
    not(dispatch.globalFenceQueued),
    dispatch' = dispatch
      .with("globalFenceQueued", true)
      .with("lastTransition", GlobalFenceQueued),
  }

  /// All shards in a command footprint are acquired in one transition. This
  /// is the target rule for atomic multi-shard admission; the current Rust
  /// refinement implements the exact single-shard and global cases only.
  action admit(command: DispatchCommand): bool = all {
    canAdmit(dispatch, command),
    dispatch' = dispatch
      .with(
        "waitingOnGlobal",
        dispatch.waitingOnGlobal.filter(waiting => waiting != command)
      )
      .with("active", dispatch.active.union(Set(command)))
      .with(
        "heldByShard",
        Shards.mapBy(shard =>
          if (affectedShards(command).contains(shard)) {
            Some(command)
          } else {
            dispatch.heldByShard.get(shard)
          }
        )
      )
      .with(
        "reservedByShard",
        Shards.mapBy(shard =>
          if (dispatch.reservedByShard.get(shard) == Some(command)) {
            None
          } else {
            dispatch.reservedByShard.get(shard)
          }
        )
      )
      .with(
        "globalFenceQueued",
        if (command == GlobalUnknown) { false } else { dispatch.globalFenceQueued }
      )
      .with("lastAdmitted", Some(command))
      .with("lastTransition", CommandAdmitted),
  }

  action complete(command: DispatchCommand): bool = all {
    dispatch.active.contains(command),
    not(dispatch.detached.contains(command)),
    dispatch' = dispatch
      .with("active", dispatch.active.filter(active => active != command))
      .with("completed", dispatch.completed.union(Set(command)))
      .with(
        "heldByShard",
        Shards.mapBy(shard =>
          if (dispatch.heldByShard.get(shard) == Some(command)) {
            None
          } else {
            dispatch.heldByShard.get(shard)
          }
        )
      )
      .with("lastReleased", Some(command))
      .with("lastTransition", CommandCompleted),
  }

  action cancelActive(command: DispatchCommand): bool = all {
    dispatch.active.contains(command),
    not(dispatch.detached.contains(command)),
    dispatch' = dispatch
      .with("detached", dispatch.detached.union(Set(command)))
      .with("lastTransition", ActiveCallerDetached),
  }

  /// Dropping the async RPC caller does not cancel spawn_blocking work. The
  /// detached operation retains its shard/global admission until the blocking
  /// closure actually returns, so a younger conflicting request cannot enter
  /// the core concurrently with work whose response is no longer observed.
  action completeDetached(command: DispatchCommand): bool = all {
    dispatch.active.contains(command),
    dispatch.detached.contains(command),
    dispatch' = dispatch
      .with("active", dispatch.active.filter(active => active != command))
      .with("detached", dispatch.detached.filter(detached => detached != command))
      .with("cancelled", dispatch.cancelled.union(Set(command)))
      .with(
        "heldByShard",
        Shards.mapBy(shard =>
          if (dispatch.heldByShard.get(shard) == Some(command)) {
            None
          } else {
            dispatch.heldByShard.get(shard)
          }
        )
      )
      .with("lastReleased", Some(command))
      .with("lastTransition", DetachedCommandCompleted),
  }

  /// Authority topology changes quiesce active mutation owners, fence every
  /// queued old-generation command, then admit only newly submitted work.
  action advanceTopology: bool = all {
    dispatch.active == Set(),
    pendingCommands(dispatch).size() > 0,
    dispatch' = dispatch
      .with("generation", dispatch.generation + 1)
      .with(
        "cancelled",
        dispatch.cancelled.union(pendingCommands(dispatch))
      )
      .with("waitingOnGlobal", Set())
      .with("reservedByShard", EmptyShardHolders)
      .with("globalFenceQueued", false)
      .with("topologyAdvanceObserved", true)
      .with("lastTransition", TopologyAdvanced),
  }

  action stutterTerminal: bool = all {
    dispatch.active == Set(),
    pendingCommands(dispatch) == Set(),
    dispatch.submitted.size() > 0,
    dispatch' = dispatch.with("lastTransition", TerminalStutter),
  }

  action step = any {
    {
      nondet command = oneOf(Commands)
      submit(command)
    },
    {
      nondet command = oneOf(Commands)
      waitOnQueuedGlobal(command)
    },
    queueGlobalFence,
    {
      nondet command = oneOf(Commands)
      admit(command)
    },
    {
      nondet command = oneOf(Commands)
      complete(command)
    },
    {
      nondet command = oneOf(Commands)
      cancelActive(command)
    },
    {
      nondet command = oneOf(Commands)
      completeDetached(command)
    },
    advanceTopology,
    stutterTerminal,
  }
}
