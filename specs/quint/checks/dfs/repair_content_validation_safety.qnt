/// Invariant checks for `dfs_repair_content_validation`.
module dfs_repair_content_validation_safety {
  import common.* from "../../lib/common"
  import dfs_repair_content_validation.* from "../../models/dfs/repair_content_validation"

  def durableReceiptsAreDesiredReplicas: bool =
    durableReceipts.subseteq(desiredReplicas)


  def activeIntentHasValidCommitThreshold: bool =
    not(pending) or validCommitThreshold(desiredReplicas, minCommitReplicas)


  def durableReceiptsHaveMatchingMetadataHash: bool =
    durableReceipts
      .filter(node => metadataReceiptHash.get(node) != Some(pendingHash))
      .size() == 0


  def lastAcceptedHashMatchesPending: bool =
    match lastAcceptedHash {
      | Some(hash) => hash == pendingHash
      | None => true
    }


  def currentLocationsAreDurable: bool =
    currentLocations.subseteq(durableReceipts)


  def currentLocationsOnlyForCurrentTargetHash: bool =
    currentLocations.size() == 0
      or all {
        targetVersionIsCurrent,
        currentObjectHash == pendingHash,
      }


  def repairJobsExactlyMissingDesiredReceipts: bool =
    if (pending) {
      repairJobs == missingDesiredReplicas(desiredReplicas, durableReceipts)
    } else {
      repairJobs == Set()
    }


  def wrongHashMetadataDoesNotSuppressRepair: bool =
    if (pending) {
      desiredReplicas
        .filter(node =>
          not(durableReceipts.contains(node))
            and metadataReceiptHash.get(node) == Some(OtherHash)
        )
        .subseteq(repairJobs)
    } else {
      true
    }


  def inflightTargetsAreDesiredReplicas: bool =
    all {
      Nodes
        .filter(node => isSome(stageInflight.get(node)))
        .subseteq(desiredReplicas),
      Nodes
        .filter(node => isSome(repairInflight.get(node)))
        .subseteq(desiredReplicas),
    }


  def stageFanoutIsBounded: bool =
    stageFanoutWithinCap


  def stageRetryStateIsTargetIsolated: bool = all {
    stageRetryDue.subseteq(desiredReplicas),
    stageRetryDue.intersect(activeStageTargets) == Set(),
    stageRetryDue.intersect(durableReceipts) == Set(),
    stageRetriedTargets.subseteq(desiredReplicas),
  }


  def stageOutcomeHistoryIsConsistent: bool = all {
    stageReceiptTargets.subseteq(durableReceipts),
    parallelStageSuccessTargets.subseteq(stageReceiptTargets),
    postCommitStageReceiptTargets.subseteq(stageReceiptTargets),
  }


  def visibleCommitHasStableWReceiptSnapshot: bool =
    if (visibleCommitted) {
      all {
        commitReceipts.subseteq(desiredReplicas),
        commitReceipts.subseteq(durableReceipts),
        commitReceipts.size() >= minCommitReplicas,
        committedTargetIdentityStable,
      }
    } else {
      all {
        commitReceipts == Set(),
        committedTargetIdentityStable,
      }
    }


  def lateStageReceiptsPreserveCommittedTarget: bool =
    postCommitStageReceiptTargets.size() == 0 or all {
      visibleCommitted,
      committedTargetIdentityStable,
      commitReceipts.size() >= minCommitReplicas,
      postCommitStageReceiptTargets
        .filter(node => not(commitReceipts.contains(node)))
        .size() > 0,
    }


  def stableTargetVersionHash: bool =
    targetVersionHashStable


  def stableTargetVersionIdentity: bool =
    targetVersionIdentityStable


  /// The cap is full with N2 and N3 active and neither target has responded.
  /// N1 remains missing but cannot consume a third slot.
  def witnessBoundedParallelN2N3StageFanout: bool = all {
    pending,
    desiredReplicas == Set(N1, N2, N3),
    activeStageTargets == Set(N2, N3),
    activeStageTargets.size() == StageFanoutCap,
    durableReceipts.intersect(Set(N2, N3)) == Set(),
    stageReceiptTargets.intersect(Set(N2, N3)) == Set(),
    not(durableReceipts.contains(N1)),
  }


  /// N3 completes first from a two-request fan-out while N2 remains active.
  def witnessOutOfOrderN3SuccessWhileN2Inflight: bool = all {
    parallelStageSuccessTargets.contains(N3),
    stageReceiptTargets.contains(N3),
    durableReceipts.contains(N3),
    stageInflight.get(N3) == None,
    isSome(stageInflight.get(N2)),
  }


  /// Rejecting N2 clears only N2 and leaves the concurrent N3 transfer active.
  def witnessN2FailureDoesNotCancelN3Inflight: bool = all {
    stageRetryDue.contains(N2),
    stageInflight.get(N2) == None,
    isSome(stageInflight.get(N3)),
    not(durableReceipts.contains(N2)),
    not(durableReceipts.contains(N3)),
  }


  /// Retrying N2 fills the free slot without restarting or cancelling N3.
  def witnessN2RetryDoesNotRestartN3Inflight: bool = all {
    stageRetriedTargets.contains(N2),
    not(stageRetriedTargets.contains(N3)),
    activeStageTargets == Set(N2, N3),
    not(stageRetryDue.contains(N2)),
  }


  /// W=1 publishes after N2 wins the response race. N3's already-issued
  /// request then records a late receipt without changing the target identity.
  def witnessLateN3ReceiptAfterWCommitPreservesTarget: bool = all {
    visibleCommitted,
    desiredReplicas.contains(N2),
    desiredReplicas.contains(N3),
    commitReceipts.contains(N2),
    not(commitReceipts.contains(N3)),
    commitReceipts.size() >= minCommitReplicas,
    parallelStageSuccessTargets.contains(N2),
    postCommitStageReceiptTargets.contains(N3),
    durableReceipts.contains(N2),
    durableReceipts.contains(N3),
    pendingHash == ExpectedHash,
    pendingVersion == Version1,
    committedTargetHash == Some(ExpectedHash),
    committedTargetVersion == Some(Version1),
    targetVersionIsCurrent,
    currentObjectHash == ExpectedHash,
    currentObjectVersion == Version1,
  }


  def safety: bool = all {
    durableReceiptsAreDesiredReplicas,
    activeIntentHasValidCommitThreshold,
    durableReceiptsHaveMatchingMetadataHash,
    lastAcceptedHashMatchesPending,
    currentLocationsAreDurable,
    currentLocationsOnlyForCurrentTargetHash,
    repairJobsExactlyMissingDesiredReceipts,
    wrongHashMetadataDoesNotSuppressRepair,
    inflightTargetsAreDesiredReplicas,
    stageFanoutIsBounded,
    stageRetryStateIsTargetIsolated,
    stageOutcomeHistoryIsConsistent,
    visibleCommitHasStableWReceiptSnapshot,
    lateStageReceiptsPreserveCommittedTarget,
    stableTargetVersionHash,
    stableTargetVersionIdentity,
  }
}
