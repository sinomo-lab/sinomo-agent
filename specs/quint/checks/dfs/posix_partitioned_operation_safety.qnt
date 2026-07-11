/// Safety checks and branch witnesses for canonical partitioned POSIX work.
module dfs_posix_partitioned_operation_safety {
  import dfs_posix_partitioned_operation.* from "../../models/dfs/posix_partitioned_operation"

  def versionsRemainOrderedAndBounded: bool = all {
    partitionBaseVersion >= 0,
    partitionBaseVersion <= canonicalVersion,
    canonicalVersion <= durableVersion,
    durableVersion <= MaxCanonicalVersion,
    quorumContentVersion >= 0,
    quorumContentVersion <= durableVersion,
  }

  def anEffectiveLockUsesTheCurrentEpoch: bool =
    lockOwner != NoLockOwner implies lockEpoch == authorityEpoch

  def holderContinuationRequiresCompleteStateContentAndPermit: bool =
    lastDecision == HolderCommittedDuringPartition implies all {
      holderCoverage == CompleteCoverage,
      holderHasRequiredContent,
      holderSyncEvidenceReady,
      leaseTicksRemaining > 0,
      authorityComponent == HolderComponent,
      lockOwner == OriginalHolder,
      lockEpoch == authorityEpoch,
      holderPhase == HolderCommitted,
    }

  def completeShardCoverageAloneNeverAuthorizesContinuation: bool =
    all {
      topology == Split,
      holderCoverage == CompleteCoverage,
      any {
        not(holderHasRequiredContent),
        leaseTicksRemaining == 0,
        authorityComponent != HolderComponent,
        lockOwner != OriginalHolder,
        lockEpoch != authorityEpoch,
      },
    } implies not(holderCanContinue)

  def quorumActivationWaitsForFencingBarrier: bool =
    lastDecision == QuorumEpochActivated implies all {
      authorityComponent == QuorumComponent,
      authorityEpoch == 2,
      leaseTicksRemaining == 0,
      lockOwner == NoLockOwner,
      repairPhase == PendingRepair,
      quorumPhase == QuorumRepairing,
    }

  def oldLockCannotBlockTheNewEpoch: bool =
    authorityComponent == QuorumComponent implies all {
      authorityEpoch == 2,
      lockOwner != OriginalHolder,
      lockEpoch != 1,
    }

  def staleHolderUnlockCannotReleaseCurrentLock: bool =
    lastDecision == StaleHolderUnlockRejected implies all {
      lockOwner == QuorumWaiter,
      lockEpoch == authorityEpoch,
      authorityEpoch == 2,
      quorumPhase == QuorumLockHeld,
    }

  def repairIsRequiredBeforeQuorumLockGrant: bool =
    all {
      authorityComponent == QuorumComponent,
      quorumPhase == QuorumLockHeld or quorumPhase == QuorumCompleted,
    } implies all {
      repairPhase == RepairComplete,
      quorumContentVersion == durableVersion,
    }

  def pendingRepairCannotGrantOrMutate: bool =
    repairPhase == PendingRepair implies all {
      quorumPhase == QuorumRepairing,
      lockOwner == NoLockOwner,
      quorumContentVersion <= durableVersion,
    }

  def aFencedDirtyHolderDidNotCommitItsDirtyVersion: bool =
    all {
      lastDecision == QuorumEpochActivated,
      holderPhase == HolderFenced,
    } implies all {
      canonicalVersion == partitionBaseVersion,
      durableVersion == partitionBaseVersion,
    }

  def successfulHolderSyncIsNeverRolledBack: bool =
    holderPhase == HolderCommitted implies all {
      canonicalVersion > partitionBaseVersion,
      durableVersion >= canonicalVersion,
    }

  def terminalStateHasNoLockOrRepairDeadlock: bool =
    protocolTerminal implies all {
      lockOwner == NoLockOwner,
      not(originalHolderTokenExists),
      not(originalHolderTokenEffective),
      quorumPhase == QuorumCompleted,
      repairPhase != PendingRepair,
    }

  /// This is a safety-shaped progress obligation. Within the checked bounds,
  /// Apalache deadlock detection and the deterministic scenarios rule out a
  /// terminal state that still owns an effective obsolete lock. Eventual
  /// scheduling and time advancement remain explicit liveness assumptions.
  def everyNonterminalProtocolCutHasARecoveryClass: bool =
    not(protocolTerminal) implies any {
      all { topology == Connected, holderPhase == HolderIdle },
      all { topology == Connected, holderPhase == HolderDirty },
      all {
        topology == Split,
        holderPhase == HolderDirty,
        holderHasCompleteTouchedState,
        leaseTicksRemaining > 0,
      },
      all {
        topology == Split,
        holderPhase == HolderDirty,
        not(holderHasCompleteTouchedState),
      },
      all { topology == Split, leaseTicksRemaining > 0 },
      all {
        topology == Split,
        authorityComponent == HolderComponent,
        leaseTicksRemaining == 0,
        quorumPhase == QuorumWaiting,
      },
      all {
        topology == Split,
        authorityComponent == QuorumComponent,
        repairPhase == PendingRepair,
      },
      all {
        topology == Split,
        authorityComponent == QuorumComponent,
        repairPhase == RepairComplete,
        quorumPhase == QuorumRepairing,
      },
      all { quorumPhase == QuorumLockHeld },
      all {
        topology == Split,
        holderPhase == HolderCommitted,
        lockOwner == OriginalHolder,
      },
      all {
        topology == Split,
        holderPhase == HolderCommitted,
        lockOwner == NoLockOwner,
        quorumPhase == QuorumWaiting,
      },
      all {
        topology == Rejoined,
        holderPhase == HolderCommitted,
        quorumPhase == QuorumWaiting,
        lockOwner == NoLockOwner,
      },
      all {
        topology == Split,
        authorityComponent == QuorumComponent,
        quorumPhase == QuorumCompleted,
        lockOwner == NoLockOwner,
      },
      all {
        topology == Rejoined,
        authorityComponent == QuorumComponent,
        holderWorkIsTerminal,
        quorumPhase == QuorumCompleted,
        lockOwner == NoLockOwner,
        originalHolderTokenExists,
        not(originalHolderTokenEffective),
      },
    }

  def witnessCompleteHolderContinuesBeforeFence: bool = all {
    lastDecision == HolderCommittedDuringPartition,
    topology == Split,
    holderCoverage == CompleteCoverage,
    holderHasRequiredContent,
    holderPhase == HolderCommitted,
    canonicalVersion == 1,
    durableVersion == 1,
  }

  def witnessHolderSyncEvidencePrepared: bool = all {
    lastDecision == HolderSyncEvidencePrepared,
    holderSyncEvidenceReady,
    holderCanContinue,
    canonicalVersion == partitionBaseVersion,
  }

  def witnessIncompleteHolderSuspends: bool = all {
    lastDecision == HolderOperationSuspended,
    topology == Split,
    not(holderHasCompleteTouchedState),
    holderPhase == HolderSuspended,
    canonicalVersion == partitionBaseVersion,
  }

  def witnessCompleteHolderCanStillBeFencedAfterExpiry: bool = all {
    lastDecision == QuorumEpochActivated,
    holderCoverage == CompleteCoverage,
    holderHasRequiredContent,
    holderPhase == HolderFenced,
    authorityComponent == QuorumComponent,
  }

  def witnessQuorumWinsAndWaitsForRepair: bool = all {
    lastDecision == QuorumEpochActivated,
    authorityComponent == QuorumComponent,
    repairPhase == PendingRepair,
    lockOwner == NoLockOwner,
  }

  def witnessStaleUnlockRejected: bool =
    lastDecision == StaleHolderUnlockRejected

  def witnessQuorumCompletesAfterRepair: bool = all {
    lastDecision == QuorumOperationCommitted,
    authorityComponent == QuorumComponent,
    repairPhase == RepairComplete,
    quorumPhase == QuorumCompleted,
    lockOwner == NoLockOwner,
  }

  def witnessQuorumCommitsWhileHolderOffline: bool = all {
    lastDecision == QuorumOperationCommitted,
    holderPhase == HolderFenced,
    originalHolderTokenExists,
    not(originalHolderTokenEffective),
    quorumPhase == QuorumCompleted,
  }

  def witnessFencedHolderSettlesAfterRejoin: bool = all {
    lastDecision == FencedHolderObservedAfterRejoin,
    topology == Rejoined,
    not(originalHolderTokenExists),
    not(originalHolderTokenEffective),
  }

  def witnessBothSidesReachCleanHolderWinnerEndState: bool = all {
    protocolTerminal,
    authorityComponent == HolderComponent,
    holderPhase == HolderCommitted,
    quorumPhase == QuorumCompleted,
    canonicalVersion == 2,
  }

  def witnessBothSidesReachCleanQuorumWinnerEndState: bool = all {
    protocolTerminal,
    authorityComponent == QuorumComponent,
    holderPhase == HolderFenced,
    quorumPhase == QuorumCompleted,
    canonicalVersion == 1,
  }

  def safety: bool = all {
    versionsRemainOrderedAndBounded,
    anEffectiveLockUsesTheCurrentEpoch,
    holderContinuationRequiresCompleteStateContentAndPermit,
    completeShardCoverageAloneNeverAuthorizesContinuation,
    quorumActivationWaitsForFencingBarrier,
    oldLockCannotBlockTheNewEpoch,
    staleHolderUnlockCannotReleaseCurrentLock,
    repairIsRequiredBeforeQuorumLockGrant,
    pendingRepairCannotGrantOrMutate,
    aFencedDirtyHolderDidNotCommitItsDirtyVersion,
    successfulHolderSyncIsNeverRolledBack,
    terminalStateHasNoLockOrRepairDeadlock,
    everyNonterminalProtocolCutHasARecoveryClass,
  }
}
