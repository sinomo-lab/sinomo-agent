/// Deterministic canonical DFS partition regressions.
///
/// The first module exercises the bounded complete-holder exception: the lock
/// holder retains all touched current shard state and content, completes fsync
/// before its strong permit expires, releases the lock, and both clients finish
/// after reconnect. The second module exercises the quorum-winner path: an
/// incomplete holder is suspended, its permit expires, the quorum activates a
/// higher epoch, repairs the last canonical checkpoint, and commits while the
/// old holder remains offline. After rejoin, the fenced holder observes its
/// terminal outcome and discards the already-ineffective token.
module dfs_posix_complete_holder_partition_scenario {
  import dfs_posix_partitioned_operation from "../../models/dfs/posix_partitioned_operation"

  type Phase =
    | Start
    | HolderDirty
    | PartitionedComplete
    | HolderCertified
    | HolderSynced
    | HolderReleased
    | Rejoined
    | QuorumLockHeld
    | BothCompleted

  var phase: Phase

  action init = all {
    dfs_posix_partitioned_operation::init,
    phase' = Start,
  }

  action step =
    if (phase == Start) {
      all {
        dfs_posix_partitioned_operation::holderAcquireLockAndWrite,
        phase' = HolderDirty,
      }
    } else if (phase == HolderDirty) {
      all {
        dfs_posix_partitioned_operation::partition(
          dfs_posix_partitioned_operation::CompleteCoverage,
          true
        ),
        phase' = PartitionedComplete,
      }
    } else if (phase == PartitionedComplete) {
      all {
        dfs_posix_partitioned_operation::prepareHolderSyncEvidence,
        phase' = HolderCertified,
      }
    } else if (phase == HolderCertified) {
      all {
        dfs_posix_partitioned_operation::holderCommitBeforeFence,
        phase' = HolderSynced,
      }
    } else if (phase == HolderSynced) {
      all {
        dfs_posix_partitioned_operation::releaseHolderLock,
        phase' = HolderReleased,
      }
    } else if (phase == HolderReleased) {
      all {
        dfs_posix_partitioned_operation::rejoinAfterHolderCommit,
        phase' = Rejoined,
      }
    } else if (phase == Rejoined) {
      all {
        dfs_posix_partitioned_operation::grantQuorumLock,
        phase' = QuorumLockHeld,
      }
    } else if (phase == QuorumLockHeld) {
      all {
        dfs_posix_partitioned_operation::completeQuorumOperation,
        phase' = BothCompleted,
      }
    } else {
      all {
        phase == BothCompleted,
        dfs_posix_partitioned_operation::stutterTerminal,
        phase' = phase,
      }
    }

  def safety: bool = all {
    dfs_posix_partitioned_operation::canonicalVersion
      <= dfs_posix_partitioned_operation::durableVersion,
    dfs_posix_partitioned_operation::lockOwner
      != dfs_posix_partitioned_operation::NoLockOwner implies
        dfs_posix_partitioned_operation::lockEpoch
          == dfs_posix_partitioned_operation::authorityEpoch,
    match phase {
      | Start => all {
          dfs_posix_partitioned_operation::topology
            == dfs_posix_partitioned_operation::Connected,
          dfs_posix_partitioned_operation::lockOwner
            == dfs_posix_partitioned_operation::NoLockOwner,
        }
      | HolderDirty => all {
          dfs_posix_partitioned_operation::holderPhase
            == dfs_posix_partitioned_operation::HolderDirty,
          dfs_posix_partitioned_operation::lockOwner
            == dfs_posix_partitioned_operation::OriginalHolder,
        }
      | PartitionedComplete => all {
          dfs_posix_partitioned_operation::topology
            == dfs_posix_partitioned_operation::Split,
          dfs_posix_partitioned_operation::holderHasCompleteTouchedState,
          dfs_posix_partitioned_operation::holderPermitValid,
          not(dfs_posix_partitioned_operation::holderSyncEvidenceReady),
        }
      | HolderCertified => all {
          dfs_posix_partitioned_operation::holderCanContinue,
          dfs_posix_partitioned_operation::holderSyncEvidenceReady,
        }
      | HolderSynced => all {
          dfs_posix_partitioned_operation::holderPhase
            == dfs_posix_partitioned_operation::HolderCommitted,
          dfs_posix_partitioned_operation::canonicalVersion == 1,
          dfs_posix_partitioned_operation::durableVersion == 1,
        }
      | HolderReleased =>
          dfs_posix_partitioned_operation::lockOwner
            == dfs_posix_partitioned_operation::NoLockOwner
      | Rejoined => all {
          dfs_posix_partitioned_operation::topology
            == dfs_posix_partitioned_operation::Rejoined,
          dfs_posix_partitioned_operation::quorumContentVersion == 1,
        }
      | QuorumLockHeld => all {
          dfs_posix_partitioned_operation::lockOwner
            == dfs_posix_partitioned_operation::QuorumWaiter,
          dfs_posix_partitioned_operation::lockEpoch
            == dfs_posix_partitioned_operation::authorityEpoch,
        }
      | BothCompleted => all {
          dfs_posix_partitioned_operation::protocolTerminal,
          dfs_posix_partitioned_operation::holderPhase
            == dfs_posix_partitioned_operation::HolderCommitted,
          dfs_posix_partitioned_operation::quorumPhase
            == dfs_posix_partitioned_operation::QuorumCompleted,
          dfs_posix_partitioned_operation::canonicalVersion == 2,
          dfs_posix_partitioned_operation::lockOwner
            == dfs_posix_partitioned_operation::NoLockOwner,
        }
    },
  }
}

module dfs_posix_quorum_repair_partition_scenario {
  import dfs_posix_partitioned_operation from "../../models/dfs/posix_partitioned_operation"

  type Phase =
    | Start
    | HolderDirty
    | PartitionedIncomplete
    | HolderSuspended
    | FirstPermitTick
    | PermitExpired
    | HolderFenced
    | Repaired
    | QuorumLockHeld
    | QuorumCompletedWhileHolderOffline
    | RejoinedWithStaleToken
    | BothSettled

  var phase: Phase

  action init = all {
    dfs_posix_partitioned_operation::init,
    phase' = Start,
  }

  action step =
    if (phase == Start) {
      all {
        dfs_posix_partitioned_operation::holderAcquireLockAndWrite,
        phase' = HolderDirty,
      }
    } else if (phase == HolderDirty) {
      all {
        dfs_posix_partitioned_operation::partition(
          dfs_posix_partitioned_operation::IncompleteCoverage,
          false
        ),
        phase' = PartitionedIncomplete,
      }
    } else if (phase == PartitionedIncomplete) {
      all {
        dfs_posix_partitioned_operation::suspendIncompleteHolder,
        phase' = HolderSuspended,
      }
    } else if (phase == HolderSuspended) {
      all {
        dfs_posix_partitioned_operation::tickPermit,
        phase' = FirstPermitTick,
      }
    } else if (phase == FirstPermitTick) {
      all {
        dfs_posix_partitioned_operation::tickPermit,
        phase' = PermitExpired,
      }
    } else if (phase == PermitExpired) {
      all {
        dfs_posix_partitioned_operation::activateQuorumEpoch,
        phase' = HolderFenced,
      }
    } else if (phase == HolderFenced) {
      all {
        dfs_posix_partitioned_operation::repairCanonicalCheckpoint,
        phase' = Repaired,
      }
    } else if (phase == Repaired) {
      all {
        dfs_posix_partitioned_operation::grantQuorumLock,
        phase' = QuorumLockHeld,
      }
    } else if (phase == QuorumLockHeld) {
      all {
        dfs_posix_partitioned_operation::completeQuorumOperation,
        phase' = QuorumCompletedWhileHolderOffline,
      }
    } else if (phase == QuorumCompletedWhileHolderOffline) {
      all {
        dfs_posix_partitioned_operation::rejoinAfterQuorumCommit,
        phase' = RejoinedWithStaleToken,
      }
    } else if (phase == RejoinedWithStaleToken) {
      all {
        dfs_posix_partitioned_operation::observeFencedHolderAfterRejoin,
        phase' = BothSettled,
      }
    } else {
      all {
        phase == BothSettled,
        dfs_posix_partitioned_operation::stutterTerminal,
        phase' = phase,
      }
    }

  def safety: bool = all {
    dfs_posix_partitioned_operation::canonicalVersion
      <= dfs_posix_partitioned_operation::durableVersion,
    dfs_posix_partitioned_operation::authorityComponent
      == dfs_posix_partitioned_operation::QuorumComponent implies
        dfs_posix_partitioned_operation::lockOwner
          != dfs_posix_partitioned_operation::OriginalHolder,
    match phase {
      | Start =>
          dfs_posix_partitioned_operation::canonicalVersion == 0
      | HolderDirty => all {
          dfs_posix_partitioned_operation::holderPhase
            == dfs_posix_partitioned_operation::HolderDirty,
          dfs_posix_partitioned_operation::originalHolderTokenExists,
        }
      | PartitionedIncomplete => all {
          dfs_posix_partitioned_operation::topology
            == dfs_posix_partitioned_operation::Split,
          not(dfs_posix_partitioned_operation::holderHasCompleteTouchedState),
          not(dfs_posix_partitioned_operation::holderCanContinue),
        }
      | HolderSuspended => all {
          dfs_posix_partitioned_operation::holderPhase
            == dfs_posix_partitioned_operation::HolderSuspended,
          dfs_posix_partitioned_operation::canonicalVersion == 0,
        }
      | FirstPermitTick =>
          dfs_posix_partitioned_operation::leaseTicksRemaining == 1
      | PermitExpired => all {
          dfs_posix_partitioned_operation::leaseTicksRemaining == 0,
          dfs_posix_partitioned_operation::authorityComponent
            == dfs_posix_partitioned_operation::HolderComponent,
        }
      | HolderFenced => all {
          dfs_posix_partitioned_operation::holderPhase
            == dfs_posix_partitioned_operation::HolderFenced,
          dfs_posix_partitioned_operation::authorityComponent
            == dfs_posix_partitioned_operation::QuorumComponent,
          dfs_posix_partitioned_operation::repairPhase
            == dfs_posix_partitioned_operation::PendingRepair,
          dfs_posix_partitioned_operation::lockOwner
            == dfs_posix_partitioned_operation::NoLockOwner,
        }
      | Repaired => all {
          dfs_posix_partitioned_operation::repairPhase
            == dfs_posix_partitioned_operation::RepairComplete,
          dfs_posix_partitioned_operation::quorumContentVersion
            == dfs_posix_partitioned_operation::durableVersion,
        }
      | QuorumLockHeld => all {
          dfs_posix_partitioned_operation::lockOwner
            == dfs_posix_partitioned_operation::QuorumWaiter,
          dfs_posix_partitioned_operation::lockEpoch == 2,
          dfs_posix_partitioned_operation::originalHolderTokenExists,
          not(dfs_posix_partitioned_operation::originalHolderTokenEffective),
        }
      | QuorumCompletedWhileHolderOffline => all {
          dfs_posix_partitioned_operation::lastDecision
            == dfs_posix_partitioned_operation::QuorumOperationCommitted,
          dfs_posix_partitioned_operation::quorumPhase
            == dfs_posix_partitioned_operation::QuorumCompleted,
          dfs_posix_partitioned_operation::canonicalVersion == 1,
          dfs_posix_partitioned_operation::lockOwner
            == dfs_posix_partitioned_operation::NoLockOwner,
          dfs_posix_partitioned_operation::originalHolderTokenExists,
          not(dfs_posix_partitioned_operation::originalHolderTokenEffective),
        }
      | RejoinedWithStaleToken => all {
          dfs_posix_partitioned_operation::topology
            == dfs_posix_partitioned_operation::Rejoined,
          dfs_posix_partitioned_operation::quorumPhase
            == dfs_posix_partitioned_operation::QuorumCompleted,
          not(dfs_posix_partitioned_operation::protocolTerminal),
          dfs_posix_partitioned_operation::lockOwner
            == dfs_posix_partitioned_operation::NoLockOwner,
          dfs_posix_partitioned_operation::originalHolderTokenExists,
          not(dfs_posix_partitioned_operation::originalHolderTokenEffective),
        }
      | BothSettled => all {
          dfs_posix_partitioned_operation::protocolTerminal,
          dfs_posix_partitioned_operation::lastDecision
              == dfs_posix_partitioned_operation::FencedHolderObservedAfterRejoin
            or dfs_posix_partitioned_operation::lastDecision
              == dfs_posix_partitioned_operation::TerminalStutter,
          dfs_posix_partitioned_operation::holderPhase
            == dfs_posix_partitioned_operation::HolderFenced,
          dfs_posix_partitioned_operation::quorumPhase
            == dfs_posix_partitioned_operation::QuorumCompleted,
          dfs_posix_partitioned_operation::lockOwner
            == dfs_posix_partitioned_operation::NoLockOwner,
          not(dfs_posix_partitioned_operation::originalHolderTokenExists),
        }
    },
  }
}
