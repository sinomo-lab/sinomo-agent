/// Safety and bounded reachability for daemon DFS read blocking admission.
module dfs_daemon_read_blocking_admission_safety {
  import common.* from "../../lib/common"
  import dfs_daemon_read_blocking_admission.* from "../../models/dfs/daemon_read_blocking_admission"

  def readSetsStayWellFormed: bool = all {
    readAdmission.submittedReads.subseteq(Reads),
    readAdmission.activeReads.subseteq(readAdmission.submittedReads),
    readAdmission.detachedReads.subseteq(readAdmission.activeReads),
    readAdmission.completedReads.subseteq(readAdmission.submittedReads),
    readAdmission.detachedCompletedReads.subseteq(readAdmission.completedReads),
    readAdmission.activeReads.intersect(readAdmission.completedReads) == Set(),
    readAdmission.detachedReads.intersect(readAdmission.completedReads) == Set(),
  }

  /// Slot accounting is exact, and every active read owns exactly one lane and
  /// one blocking worker. No queued async waiter owns either resource.
  def resourcesBelongExactlyToActiveReads: bool = all {
    readAdmission.readSlotsInUse == readAdmission.activeReads.size(),
    readAdmission.readSlotsInUse >= 0,
    readAdmission.readSlotsInUse <= ReadSlotCap,
    Reads.forall(request =>
      if (readAdmission.activeReads.contains(request)) {
        all {
          readAdmission.blockingWorkerByRead.get(request),
          match readAdmission.laneByRead.get(request) {
            | Some(lane) => ReadLanes.contains(lane)
            | None => false
          },
        }
      } else {
        all {
          not(readAdmission.blockingWorkerByRead.get(request)),
          readAdmission.laneByRead.get(request) == None,
        }
      }
    ),
  }

  def activeReadLanesNeverCollide: bool =
    readAdmission.activeReads.forall(left =>
      readAdmission.activeReads.forall(right =>
        left == right
          or readAdmission.laneByRead.get(left)
            != readAdmission.laneByRead.get(right)
      )
    )

  /// Once both slots are occupied, every additional submitted read is still
  /// an async waiter rather than a task parked on a lane mutex in the blocking
  /// pool.
  def saturatedWaitersOwnNoBlockingResources: bool =
    readAdmission.readSlotsInUse == ReadSlotCap implies
      pendingReads(readAdmission).forall(request => all {
        not(readAdmission.blockingWorkerByRead.get(request)),
        readAdmission.laneByRead.get(request) == None,
      })

  /// Mutation admission is a separate capacity domain. Saturated read slots
  /// do not make an otherwise pending mutation inadmissible.
  def saturatedReadsCannotBlockMutationAdmission: bool =
    all {
      readAdmission.readSlotsInUse == ReadSlotCap,
      readAdmission.mutationSubmitted,
      not(readAdmission.mutationActive),
      not(readAdmission.mutationCompleted),
    } implies canAdmitMutation(readAdmission)

  def detachedReadsRetainEveryResource: bool =
    readAdmission.detachedReads.forall(request => all {
      readAdmission.activeReads.contains(request),
      readAdmission.blockingWorkerByRead.get(request),
      readAdmission.laneByRead.get(request) != None,
    })

  def detachTransitionCannotReleaseResources: bool =
    readAdmission.lastTransition == ReadCallerDetached implies
      match readAdmission.lastDetachedRead {
        | Some(request) => all {
            readAdmission.detachedReads.contains(request),
            readAdmission.activeReads.contains(request),
            readAdmission.blockingWorkerByRead.get(request),
            readAdmission.laneByRead.get(request) != None,
          }
        | None => false
      }

  def completionReleasesReadResources: bool =
    any {
      readAdmission.lastTransition == AttachedReadFinished,
      readAdmission.lastTransition == DetachedReadFinished,
    } implies match readAdmission.lastReleasedRead {
      | Some(request) => all {
          readAdmission.completedReads.contains(request),
          not(readAdmission.activeReads.contains(request)),
          not(readAdmission.detachedReads.contains(request)),
          not(readAdmission.blockingWorkerByRead.get(request)),
          readAdmission.laneByRead.get(request) == None,
          readAdmission.releasedLaneByRead.get(request) != None,
        }
      | None => false
    }

  def mutationDoesNotConsumeReadCapacity: bool =
    readAdmission.mutationActive implies
      readAdmission.readSlotsInUse == readAdmission.activeReads.size()

  def safety: bool = all {
    readSetsStayWellFormed,
    resourcesBelongExactlyToActiveReads,
    activeReadLanesNeverCollide,
    saturatedWaitersOwnNoBlockingResources,
    saturatedReadsCannotBlockMutationAdmission,
    detachedReadsRetainEveryResource,
    detachTransitionCannotReleaseResources,
    completionReleasesReadResources,
    mutationDoesNotConsumeReadCapacity,
  }

  def firstTwoReadsOwnDistinctLanes: bool = all {
    readAdmission.activeReads.contains(ReadOne),
    readAdmission.activeReads.contains(ReadTwo),
    readAdmission.laneByRead.get(ReadOne) != None,
    readAdmission.laneByRead.get(ReadTwo) != None,
    readAdmission.laneByRead.get(ReadOne)
      != readAdmission.laneByRead.get(ReadTwo),
  }

  def thirdReadIsAsyncWaiter: bool = all {
    pendingReads(readAdmission).contains(ReadThree),
    not(readAdmission.blockingWorkerByRead.get(ReadThree)),
    readAdmission.laneByRead.get(ReadThree) == None,
  }

  def witnessTwoReadSlotsSaturated: bool = all {
    readAdmission.activeReads == Set(ReadOne, ReadTwo),
    readAdmission.readSlotsInUse == ReadSlotCap,
    firstTwoReadsOwnDistinctLanes,
  }

  def witnessNoReadLaneCollisionAtSaturation: bool = all {
    witnessTwoReadSlotsSaturated,
    activeReadLanesNeverCollide,
  }

  def witnessThirdReadWaitsOutsideBlockingPool: bool = all {
    witnessTwoReadSlotsSaturated,
    thirdReadIsAsyncWaiter,
  }

  def witnessMutationProgressesWhileReadsSaturated: bool = all {
    witnessThirdReadWaitsOutsideBlockingPool,
    readAdmission.mutationActive,
  }

  def witnessCallerDetachmentRetainsSlotAndLane: bool = all {
    witnessThirdReadWaitsOutsideBlockingPool,
    readAdmission.detachedReads.contains(ReadOne),
    readAdmission.activeReads.contains(ReadOne),
    readAdmission.blockingWorkerByRead.get(ReadOne),
    readAdmission.laneByRead.get(ReadOne) != None,
  }

  def witnessDetachedCompletionLetsWaitingReadEnter: bool = all {
    readAdmission.detachedCompletedReads.contains(ReadOne),
    readAdmission.completedReads.contains(ReadOne),
    readAdmission.activeReads == Set(ReadTwo, ReadThree),
    readAdmission.readSlotsInUse == ReadSlotCap,
    readAdmission.laneByRead.get(ReadThree)
      == readAdmission.releasedLaneByRead.get(ReadOne),
    readAdmission.blockingWorkerByRead.get(ReadThree),
    activeReadLanesNeverCollide,
  }
}
