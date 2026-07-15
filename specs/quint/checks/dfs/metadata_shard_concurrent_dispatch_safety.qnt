/// Safety and bounded reachability for per-shard concurrent DFS dispatch.
module dfs_metadata_shard_concurrent_dispatch_safety {
  import common.* from "../../lib/common"
  import dfs_metadata_shard_concurrent_dispatch.* from "../../models/dfs/metadata_shard_concurrent_dispatch"

  def holdersAndActiveCommandsAgree: bool = all {
    dispatch.active.forall(command =>
      Shards.forall(shard =>
        if (affectedShards(command).contains(shard)) {
          dispatch.heldByShard.get(shard) == Some(command)
        } else {
          dispatch.heldByShard.get(shard) != Some(command)
        }
      )
    ),
    Shards.forall(shard =>
      match dispatch.heldByShard.get(shard) {
        | Some(command) => all {
            dispatch.active.contains(command),
            affectedShards(command).contains(shard),
          }
        | None => true
      }
    ),
  }

  def activeFootprintsNeverOverlap: bool =
    dispatch.active.forall(left =>
      dispatch.active.forall(right =>
        left == right or not(footprintsOverlap(left, right))
      )
    )

  def shardReservationsBelongOnlyToWaitingExactCommands: bool = all {
    dispatch.waitingOnGlobal.forall(command => all {
      command != GlobalUnknown,
      pendingCommands(dispatch).contains(command),
      affectedShards(command).forall(shard =>
        dispatch.reservedByShard.get(shard) == Some(command)
      ),
    }),
    Shards.forall(shard =>
      match dispatch.reservedByShard.get(shard) {
        | Some(command) => all {
            dispatch.waitingOnGlobal.contains(command),
            affectedShards(command).contains(shard),
          }
        | None => true
      }
    ),
  }

  def queuedGlobalFenceNamesPendingGlobalWork: bool =
    dispatch.globalFenceQueued implies all {
      pendingCommands(dispatch).contains(GlobalUnknown),
      not(dispatch.active.contains(GlobalUnknown)),
    }

  def activeCommandsUseCurrentTopology: bool =
    dispatch.active.forall(command =>
      dispatch.generationByCommand.get(command) == dispatch.generation
    )

  def terminalSetsStayDisjoint: bool = all {
    dispatch.detached.subseteq(dispatch.active),
    dispatch.active.intersect(dispatch.completed) == Set(),
    dispatch.active.intersect(dispatch.cancelled) == Set(),
    dispatch.completed.intersect(dispatch.cancelled) == Set(),
  }

  def globalFallbackExcludesEveryOtherMutation: bool =
    dispatch.active.contains(GlobalUnknown) implies
      dispatch.active == Set(GlobalUnknown)

  def admissionAcquiresExactFootprint: bool =
    dispatch.lastTransition == CommandAdmitted implies
      match dispatch.lastAdmitted {
        | Some(command) => Shards.forall(shard =>
            if (affectedShards(command).contains(shard)) {
              dispatch.heldByShard.get(shard) == Some(command)
            } else {
              dispatch.heldByShard.get(shard) != Some(command)
            }
          )
        | None => false
      }

  def releaseDropsExactOwnership: bool =
    any {
      dispatch.lastTransition == CommandCompleted,
      dispatch.lastTransition == DetachedCommandCompleted,
    } implies match dispatch.lastReleased {
      | Some(command) => Shards.forall(shard =>
          dispatch.heldByShard.get(shard) != Some(command)
        )
      | None => false
    }

  def safety: bool = all {
    holdersAndActiveCommandsAgree,
    activeFootprintsNeverOverlap,
    shardReservationsBelongOnlyToWaitingExactCommands,
    queuedGlobalFenceNamesPendingGlobalWork,
    activeCommandsUseCurrentTopology,
    terminalSetsStayDisjoint,
    globalFallbackExcludesEveryOtherMutation,
    admissionAcquiresExactFootprint,
    releaseDropsExactOwnership,
  }

  def witnessDisjointShardsRunConcurrently: bool =
    dispatch.active == Set(RootFirst, ServicesIndependent)

  def witnessSameShardFifoBlocksYounger: bool = all {
    dispatch.active == Set(RootFirst),
    pendingCommands(dispatch).contains(RootSecond),
    dispatch.ticketByCommand.get(RootFirst)
      < dispatch.ticketByCommand.get(RootSecond),
    not(canAdmit(dispatch, RootSecond)),
  }

  def witnessSameShardFifoAdvancesAfterRelease: bool = all {
    dispatch.completed.contains(RootFirst),
    dispatch.active.contains(RootSecond),
  }

  def witnessCrossShardAcquiresAtomically: bool = all {
    dispatch.active.contains(CrossRootServices),
    dispatch.heldByShard.get(RootShard) == Some(CrossRootServices),
    dispatch.heldByShard.get(ServicesShard) == Some(CrossRootServices),
  }

  def witnessUnrelatedShardOverlapsCrossShard: bool =
    dispatch.active == Set(CrossRootServices, UsersIndependent)

  def witnessGlobalFallbackRunsExclusively: bool =
    dispatch.active == Set(GlobalUnknown)

  def witnessQueuedGlobalBypassesShardOnlyWaiter: bool = all {
    dispatch.active == Set(GlobalUnknown),
    dispatch.waitingOnGlobal.contains(RootSecond),
    dispatch.reservedByShard.get(RootShard) == Some(RootSecond),
    not(dispatch.completed.contains(RootSecond)),
  }

  def witnessCancellationKeepsOwnershipUntilBlockingWorkFinishes: bool = all {
    dispatch.detached.contains(RootFirst),
    dispatch.active.contains(RootFirst),
    pendingCommands(dispatch).contains(RootSecond),
    dispatch.heldByShard.get(RootShard) == Some(RootFirst),
    not(canAdmit(dispatch, RootSecond)),
  }

  def witnessDetachedWorkReleaseAllowsNextOwner: bool = all {
    dispatch.cancelled.contains(RootFirst),
    dispatch.active.contains(RootSecond),
    dispatch.heldByShard.get(RootShard) == Some(RootSecond),
  }

  def witnessTopologyFencesQueuedOldGeneration: bool = all {
    dispatch.topologyAdvanceObserved,
    dispatch.cancelled.contains(RootFirst),
    dispatch.generationByCommand.get(RootFirst) < dispatch.generation,
    dispatch.active.contains(PostTopologyRoot),
    dispatch.generationByCommand.get(PostTopologyRoot) == dispatch.generation,
  }
}
