/// Canonical POSIX operation ownership across a two-component partition.
///
/// Evidence class: target hardening. This slice consumes abstract guarantees
/// from the DFS authority, advisory-lock, and fsync layers. It deliberately
/// does not model local-first work. A dirty operation is either completed by
/// its original holder while a bounded strong permit remains valid, or it is
/// fenced and the quorum component recovers the last canonical checkpoint.
///
/// `CompleteCoverage` means that the holder component can prove it has every
/// metadata shard touched by the operation at the certified journal prefix.
/// It does not grant authority by itself. `holderHasRequiredContent` separately
/// proves that the content named by the operation is present and validated.
module dfs_posix_partitioned_operation {
  type Topology = Connected | Split | Rejoined
  type Component = HolderComponent | QuorumComponent
  type ShardCoverage = CompleteCoverage | IncompleteCoverage
  type LockOwner = NoLockOwner | OriginalHolder | QuorumWaiter
  type HolderPhase =
    | HolderIdle
    | HolderDirty
    | HolderSuspended
    | HolderCommitted
    | HolderFenced
  type QuorumPhase =
    | QuorumIdle
    | QuorumWaiting
    | QuorumRepairing
    | QuorumLockHeld
    | QuorumCompleted
  type RepairPhase = NoRepair | PendingRepair | RepairComplete
  type PartitionDecision =
    | NoDecision
    | HolderLockAndWriteStarted
    | ClusterPartitioned
    | HolderOperationSuspended
    | PermitTicked
    | HolderSyncEvidencePrepared
    | HolderCommittedDuringPartition
    | HolderLockReleased
    | QuorumEpochActivated
    | CanonicalCheckpointRepaired
    | QuorumLockGranted
    | StaleHolderUnlockRejected
    | QuorumOperationCommitted
    | ComponentsRejoined
    | FencedHolderObservedAfterRejoin
    | TerminalStutter

  pure val MaxLeaseTicks: int = 2
  pure val MaxCanonicalVersion: int = 2

  var topology: Topology
  var authorityComponent: Component
  var authorityEpoch: int
  var lockOwner: LockOwner
  var lockEpoch: int
  var originalHolderTokenExists: bool
  var leaseTicksRemaining: int
  var holderCoverage: ShardCoverage
  var holderHasRequiredContent: bool
  /// Abstract lower-layer proof that the configured content-receipt threshold,
  /// metadata prepare quorum, and recovery-set intersection needed by fsync are
  /// ready for the captured operation. The commit certificate is formed only
  /// by the later atomic publication action.
  var holderSyncEvidenceReady: bool
  var holderPhase: HolderPhase
  var quorumPhase: QuorumPhase
  var repairPhase: RepairPhase
  var canonicalVersion: int
  var durableVersion: int
  var partitionBaseVersion: int
  var quorumContentVersion: int
  var lastDecision: PartitionDecision

  pure def holderHasCompleteTouchedState: bool =
    holderCoverage == CompleteCoverage and holderHasRequiredContent

  /// A disconnected holder may finish only while all three facts remain true:
  /// complete touched-shard state, required content, and its old strong permit.
  pure def holderPermitValid: bool = all {
    topology == Split,
    authorityComponent == HolderComponent,
    authorityEpoch == 1,
    lockOwner == OriginalHolder,
    lockEpoch == authorityEpoch,
    leaseTicksRemaining > 0,
  }

  /// The client may physically retain an old token while offline. Epoch and
  /// owner fencing determine effectiveness; quorum progress never waits for
  /// that client to acknowledge loss or send an unlock.
  pure def originalHolderTokenEffective: bool = all {
    originalHolderTokenExists,
    authorityComponent == HolderComponent,
    lockOwner == OriginalHolder,
    lockEpoch == authorityEpoch,
    leaseTicksRemaining > 0,
  }

  pure def holderCanPrepareSync: bool = all {
    holderPhase == HolderDirty,
    holderHasCompleteTouchedState,
    holderPermitValid,
  }

  pure def holderCanContinue: bool = all {
    holderCanPrepareSync,
    holderSyncEvidenceReady,
  }

  pure def holderWorkIsTerminal: bool =
    holderPhase == HolderCommitted or holderPhase == HolderFenced

  pure def quorumWorkIsTerminal: bool =
    quorumPhase == QuorumCompleted

  /// Fully terminal means both the cluster work and the rejoined client's
  /// local view are settled. Quorum commit itself does not wait for the client,
  /// but after rejoin the client must consume the fence before terminal stutter
  /// becomes available.
  pure def protocolTerminal: bool = all {
    topology == Rejoined,
    holderWorkIsTerminal,
    quorumWorkIsTerminal,
    lockOwner == NoLockOwner,
    not(originalHolderTokenExists),
    not(originalHolderTokenEffective),
    repairPhase != PendingRepair,
  }

  action init = all {
    topology' = Connected,
    authorityComponent' = HolderComponent,
    authorityEpoch' = 1,
    lockOwner' = NoLockOwner,
    lockEpoch' = 0,
    originalHolderTokenExists' = false,
    leaseTicksRemaining' = 0,
    holderCoverage' = IncompleteCoverage,
    holderHasRequiredContent' = false,
    holderSyncEvidenceReady' = false,
    holderPhase' = HolderIdle,
    quorumPhase' = QuorumIdle,
    repairPhase' = NoRepair,
    canonicalVersion' = 0,
    durableVersion' = 0,
    partitionBaseVersion' = 0,
    quorumContentVersion' = 0,
    lastDecision' = NoDecision,
  }

  /// The advisory lock is authority-epoch scoped. It coordinates cooperating
  /// clients, but does not itself grant metadata authority.
  action holderAcquireLockAndWrite: bool = all {
    topology == Connected,
    authorityComponent == HolderComponent,
    holderPhase == HolderIdle,
    quorumPhase == QuorumIdle,
    lockOwner == NoLockOwner,
    lockOwner' = OriginalHolder,
    lockEpoch' = authorityEpoch,
    originalHolderTokenExists' = true,
    leaseTicksRemaining' = MaxLeaseTicks,
    holderPhase' = HolderDirty,
    topology' = topology,
    authorityComponent' = authorityComponent,
    authorityEpoch' = authorityEpoch,
    holderCoverage' = holderCoverage,
    holderHasRequiredContent' = holderHasRequiredContent,
    holderSyncEvidenceReady' = false,
    quorumPhase' = quorumPhase,
    repairPhase' = repairPhase,
    canonicalVersion' = canonicalVersion,
    durableVersion' = durableVersion,
    partitionBaseVersion' = partitionBaseVersion,
    quorumContentVersion' = quorumContentVersion,
    lastDecision' = HolderLockAndWriteStarted,
  }

  action partition(
    coverage: ShardCoverage,
    hasRequiredContent: bool
  ): bool = all {
    topology == Connected,
    holderPhase == HolderDirty,
    lockOwner == OriginalHolder,
    topology' = Split,
    holderCoverage' = coverage,
    holderHasRequiredContent' = hasRequiredContent,
    holderSyncEvidenceReady' = false,
    quorumPhase' = QuorumWaiting,
    partitionBaseVersion' = canonicalVersion,
    authorityComponent' = authorityComponent,
    authorityEpoch' = authorityEpoch,
    lockOwner' = lockOwner,
    lockEpoch' = lockEpoch,
    originalHolderTokenExists' = originalHolderTokenExists,
    leaseTicksRemaining' = leaseTicksRemaining,
    holderPhase' = holderPhase,
    repairPhase' = repairPhase,
    canonicalVersion' = canonicalVersion,
    durableVersion' = durableVersion,
    quorumContentVersion' = quorumContentVersion,
    lastDecision' = ClusterPartitioned,
  }

  /// Missing touched state or content suspends the dirty operation immediately.
  /// The old epoch token remains recorded until the bounded fencing barrier,
  /// but it cannot commit while the completeness predicate is false.
  action suspendIncompleteHolder: bool = all {
    topology == Split,
    holderPhase == HolderDirty,
    not(holderHasCompleteTouchedState),
    holderPhase' = HolderSuspended,
    topology' = topology,
    authorityComponent' = authorityComponent,
    authorityEpoch' = authorityEpoch,
    lockOwner' = lockOwner,
    lockEpoch' = lockEpoch,
    originalHolderTokenExists' = originalHolderTokenExists,
    leaseTicksRemaining' = leaseTicksRemaining,
    holderCoverage' = holderCoverage,
    holderHasRequiredContent' = holderHasRequiredContent,
    holderSyncEvidenceReady' = holderSyncEvidenceReady,
    quorumPhase' = quorumPhase,
    repairPhase' = repairPhase,
    canonicalVersion' = canonicalVersion,
    durableVersion' = durableVersion,
    partitionBaseVersion' = partitionBaseVersion,
    quorumContentVersion' = quorumContentVersion,
    lastDecision' = HolderOperationSuspended,
  }

  /// Logical time is explicit. A real lease implementation must state bounded
  /// clock-drift assumptions; a per-operation quorum certificate would instead
  /// make the holder stop as soon as it loses the certified commit path.
  action tickPermit: bool = all {
    topology == Split,
    leaseTicksRemaining > 0,
    leaseTicksRemaining' = leaseTicksRemaining - 1,
    topology' = topology,
    authorityComponent' = authorityComponent,
    authorityEpoch' = authorityEpoch,
    lockOwner' = lockOwner,
    lockEpoch' = lockEpoch,
    originalHolderTokenExists' = originalHolderTokenExists,
    holderCoverage' = holderCoverage,
    holderHasRequiredContent' = holderHasRequiredContent,
    holderSyncEvidenceReady' = holderSyncEvidenceReady,
    holderPhase' = holderPhase,
    quorumPhase' = quorumPhase,
    repairPhase' = repairPhase,
    canonicalVersion' = canonicalVersion,
    durableVersion' = durableVersion,
    partitionBaseVersion' = partitionBaseVersion,
    quorumContentVersion' = quorumContentVersion,
    lastDecision' = PermitTicked,
  }

  /// Consume the lower fsync prerequisites before publishing. Complete local
  /// state is not enough: the modeled receipt threshold, metadata prepare
  /// quorum, and recovery intersection must also exist. This action does not
  /// form the canonical commit certificate.
  action prepareHolderSyncEvidence: bool = all {
    holderCanPrepareSync,
    not(holderSyncEvidenceReady),
    holderSyncEvidenceReady' = true,
    topology' = topology,
    authorityComponent' = authorityComponent,
    authorityEpoch' = authorityEpoch,
    lockOwner' = lockOwner,
    lockEpoch' = lockEpoch,
    originalHolderTokenExists' = originalHolderTokenExists,
    leaseTicksRemaining' = leaseTicksRemaining,
    holderCoverage' = holderCoverage,
    holderHasRequiredContent' = holderHasRequiredContent,
    holderPhase' = holderPhase,
    quorumPhase' = quorumPhase,
    repairPhase' = repairPhase,
    canonicalVersion' = canonicalVersion,
    durableVersion' = durableVersion,
    partitionBaseVersion' = partitionBaseVersion,
    quorumContentVersion' = quorumContentVersion,
    lastDecision' = HolderSyncEvidencePrepared,
  }

  /// This action abstracts the atomic publication point of a successful file
  /// fsync, including formation of its failover-recoverable commit certificate.
  /// A local dirty buffer or complete shard set alone cannot take it.
  action holderCommitBeforeFence: bool = all {
    holderCanContinue,
    canonicalVersion == partitionBaseVersion,
    canonicalVersion < MaxCanonicalVersion,
    canonicalVersion' = canonicalVersion + 1,
    durableVersion' = canonicalVersion + 1,
    holderPhase' = HolderCommitted,
    topology' = topology,
    authorityComponent' = authorityComponent,
    authorityEpoch' = authorityEpoch,
    lockOwner' = lockOwner,
    lockEpoch' = lockEpoch,
    originalHolderTokenExists' = originalHolderTokenExists,
    leaseTicksRemaining' = leaseTicksRemaining,
    holderCoverage' = holderCoverage,
    holderHasRequiredContent' = holderHasRequiredContent,
    holderSyncEvidenceReady' = holderSyncEvidenceReady,
    quorumPhase' = quorumPhase,
    repairPhase' = repairPhase,
    partitionBaseVersion' = partitionBaseVersion,
    quorumContentVersion' = quorumContentVersion,
    lastDecision' = HolderCommittedDuringPartition,
  }

  action releaseHolderLock: bool = all {
    holderPhase == HolderCommitted,
    lockOwner == OriginalHolder,
    lockOwner' = NoLockOwner,
    lockEpoch' = 0,
    originalHolderTokenExists' = false,
    leaseTicksRemaining' = 0,
    topology' = topology,
    authorityComponent' = authorityComponent,
    authorityEpoch' = authorityEpoch,
    holderCoverage' = holderCoverage,
    holderHasRequiredContent' = holderHasRequiredContent,
    holderSyncEvidenceReady' = holderSyncEvidenceReady,
    holderPhase' = holderPhase,
    quorumPhase' = quorumPhase,
    repairPhase' = repairPhase,
    canonicalVersion' = canonicalVersion,
    durableVersion' = durableVersion,
    partitionBaseVersion' = partitionBaseVersion,
    quorumContentVersion' = quorumContentVersion,
    lastDecision' = HolderLockReleased,
  }

  /// The quorum side activates a higher epoch only after the old permit's
  /// fencing barrier. Shard count does not select the winner. The old holder's
  /// lock becomes ineffective atomically with epoch activation.
  action activateQuorumEpoch: bool = all {
    topology == Split,
    authorityComponent == HolderComponent,
    quorumPhase == QuorumWaiting,
    leaseTicksRemaining == 0,
    authorityComponent' = QuorumComponent,
    authorityEpoch' = authorityEpoch + 1,
    lockOwner' = NoLockOwner,
    lockEpoch' = 0,
    holderPhase' =
      if (holderPhase == HolderCommitted) { HolderCommitted } else { HolderFenced },
    quorumPhase' = QuorumRepairing,
    repairPhase' = PendingRepair,
    topology' = topology,
    originalHolderTokenExists' = originalHolderTokenExists,
    leaseTicksRemaining' = 0,
    holderCoverage' = holderCoverage,
    holderHasRequiredContent' = holderHasRequiredContent,
    holderSyncEvidenceReady' = holderSyncEvidenceReady,
    canonicalVersion' = canonicalVersion,
    durableVersion' = durableVersion,
    partitionBaseVersion' = partitionBaseVersion,
    quorumContentVersion' = quorumContentVersion,
    lastDecision' = QuorumEpochActivated,
  }

  /// Authority may move before bytes are available, but the winner remains in
  /// PendingRepair and cannot serve or mutate stale content. This action
  /// abstracts recovery from a source validated against the canonical commit.
  action repairCanonicalCheckpoint: bool = all {
    topology == Split,
    authorityComponent == QuorumComponent,
    quorumPhase == QuorumRepairing,
    repairPhase == PendingRepair,
    repairPhase' = RepairComplete,
    quorumContentVersion' = durableVersion,
    topology' = topology,
    authorityComponent' = authorityComponent,
    authorityEpoch' = authorityEpoch,
    lockOwner' = lockOwner,
    lockEpoch' = lockEpoch,
    originalHolderTokenExists' = originalHolderTokenExists,
    leaseTicksRemaining' = leaseTicksRemaining,
    holderCoverage' = holderCoverage,
    holderHasRequiredContent' = holderHasRequiredContent,
    holderSyncEvidenceReady' = holderSyncEvidenceReady,
    holderPhase' = holderPhase,
    quorumPhase' = quorumPhase,
    canonicalVersion' = canonicalVersion,
    durableVersion' = durableVersion,
    partitionBaseVersion' = partitionBaseVersion,
    lastDecision' = CanonicalCheckpointRepaired,
  }

  action grantQuorumLock: bool = all {
    topology == Split or topology == Rejoined,
    quorumPhase == QuorumWaiting or quorumPhase == QuorumRepairing,
    lockOwner == NoLockOwner,
    any {
      all {
        authorityComponent == QuorumComponent,
        repairPhase == RepairComplete,
        quorumContentVersion == durableVersion,
      },
      all {
        topology == Rejoined,
        authorityComponent == HolderComponent,
        holderPhase == HolderCommitted,
        repairPhase == NoRepair,
      },
    },
    lockOwner' = QuorumWaiter,
    lockEpoch' = authorityEpoch,
    quorumPhase' = QuorumLockHeld,
    topology' = topology,
    authorityComponent' = authorityComponent,
    authorityEpoch' = authorityEpoch,
    originalHolderTokenExists' = originalHolderTokenExists,
    leaseTicksRemaining' = leaseTicksRemaining,
    holderCoverage' = holderCoverage,
    holderHasRequiredContent' = holderHasRequiredContent,
    holderSyncEvidenceReady' = holderSyncEvidenceReady,
    holderPhase' = holderPhase,
    repairPhase' = repairPhase,
    canonicalVersion' = canonicalVersion,
    durableVersion' = durableVersion,
    partitionBaseVersion' = partitionBaseVersion,
    quorumContentVersion' = quorumContentVersion,
    lastDecision' = QuorumLockGranted,
  }

  /// A stale epoch token cannot release a current-epoch lock.
  action rejectStaleHolderUnlock: bool = all {
    originalHolderTokenExists,
    authorityEpoch > 1,
    lockOwner == QuorumWaiter,
    lockEpoch == authorityEpoch,
    topology' = topology,
    authorityComponent' = authorityComponent,
    authorityEpoch' = authorityEpoch,
    lockOwner' = lockOwner,
    lockEpoch' = lockEpoch,
    originalHolderTokenExists' = false,
    leaseTicksRemaining' = leaseTicksRemaining,
    holderCoverage' = holderCoverage,
    holderHasRequiredContent' = holderHasRequiredContent,
    holderSyncEvidenceReady' = holderSyncEvidenceReady,
    holderPhase' = holderPhase,
    quorumPhase' = quorumPhase,
    repairPhase' = repairPhase,
    canonicalVersion' = canonicalVersion,
    durableVersion' = durableVersion,
    partitionBaseVersion' = partitionBaseVersion,
    quorumContentVersion' = quorumContentVersion,
    lastDecision' = StaleHolderUnlockRejected,
  }

  action completeQuorumOperation: bool = all {
    quorumPhase == QuorumLockHeld,
    lockOwner == QuorumWaiter,
    lockEpoch == authorityEpoch,
    canonicalVersion < MaxCanonicalVersion,
    canonicalVersion' = canonicalVersion + 1,
    durableVersion' = canonicalVersion + 1,
    quorumContentVersion' = canonicalVersion + 1,
    quorumPhase' = QuorumCompleted,
    lockOwner' = NoLockOwner,
    lockEpoch' = 0,
    topology' = topology,
    authorityComponent' = authorityComponent,
    authorityEpoch' = authorityEpoch,
    originalHolderTokenExists' = originalHolderTokenExists,
    leaseTicksRemaining' = leaseTicksRemaining,
    holderCoverage' = holderCoverage,
    holderHasRequiredContent' = holderHasRequiredContent,
    holderSyncEvidenceReady' = holderSyncEvidenceReady,
    holderPhase' = holderPhase,
    repairPhase' = repairPhase,
    partitionBaseVersion' = partitionBaseVersion,
    lastDecision' = QuorumOperationCommitted,
  }

  action rejoinAfterHolderCommit: bool = all {
    topology == Split,
    authorityComponent == HolderComponent,
    holderPhase == HolderCommitted,
    quorumPhase == QuorumWaiting,
    lockOwner == NoLockOwner,
    topology' = Rejoined,
    authorityComponent' = authorityComponent,
    authorityEpoch' = authorityEpoch,
    lockOwner' = lockOwner,
    lockEpoch' = lockEpoch,
    originalHolderTokenExists' = originalHolderTokenExists,
    leaseTicksRemaining' = leaseTicksRemaining,
    holderCoverage' = holderCoverage,
    holderHasRequiredContent' = holderHasRequiredContent,
    holderSyncEvidenceReady' = holderSyncEvidenceReady,
    holderPhase' = holderPhase,
    quorumPhase' = quorumPhase,
    repairPhase' = repairPhase,
    canonicalVersion' = canonicalVersion,
    durableVersion' = durableVersion,
    partitionBaseVersion' = partitionBaseVersion,
    quorumContentVersion' = canonicalVersion,
    lastDecision' = ComponentsRejoined,
  }

  action rejoinAfterQuorumCommit: bool = all {
    topology == Split,
    authorityComponent == QuorumComponent,
    quorumPhase == QuorumCompleted,
    holderWorkIsTerminal,
    lockOwner == NoLockOwner,
    repairPhase == RepairComplete,
    topology' = Rejoined,
    authorityComponent' = authorityComponent,
    authorityEpoch' = authorityEpoch,
    lockOwner' = lockOwner,
    lockEpoch' = lockEpoch,
    originalHolderTokenExists' = originalHolderTokenExists,
    leaseTicksRemaining' = leaseTicksRemaining,
    holderCoverage' = holderCoverage,
    holderHasRequiredContent' = holderHasRequiredContent,
    holderSyncEvidenceReady' = holderSyncEvidenceReady,
    holderPhase' = holderPhase,
    quorumPhase' = quorumPhase,
    repairPhase' = repairPhase,
    canonicalVersion' = canonicalVersion,
    durableVersion' = durableVersion,
    partitionBaseVersion' = partitionBaseVersion,
    quorumContentVersion' = quorumContentVersion,
    lastDecision' = ComponentsRejoined,
  }

  /// Quorum recovery never waits for the partitioned client. When that client
  /// later rejoins, it observes the durable fenced outcome and discards the
  /// physically retained but already ineffective token.
  action observeFencedHolderAfterRejoin: bool = all {
    topology == Rejoined,
    authorityComponent == QuorumComponent,
    holderWorkIsTerminal,
    quorumPhase == QuorumCompleted,
    lockOwner == NoLockOwner,
    originalHolderTokenExists,
    originalHolderTokenExists' = false,
    topology' = topology,
    authorityComponent' = authorityComponent,
    authorityEpoch' = authorityEpoch,
    lockOwner' = lockOwner,
    lockEpoch' = lockEpoch,
    leaseTicksRemaining' = leaseTicksRemaining,
    holderCoverage' = holderCoverage,
    holderHasRequiredContent' = holderHasRequiredContent,
    holderSyncEvidenceReady' = holderSyncEvidenceReady,
    holderPhase' = holderPhase,
    quorumPhase' = quorumPhase,
    repairPhase' = repairPhase,
    canonicalVersion' = canonicalVersion,
    durableVersion' = durableVersion,
    partitionBaseVersion' = partitionBaseVersion,
    quorumContentVersion' = quorumContentVersion,
    lastDecision' = FencedHolderObservedAfterRejoin,
  }

  action stutterTerminal: bool = all {
    protocolTerminal,
    topology' = topology,
    authorityComponent' = authorityComponent,
    authorityEpoch' = authorityEpoch,
    lockOwner' = lockOwner,
    lockEpoch' = lockEpoch,
    originalHolderTokenExists' = originalHolderTokenExists,
    leaseTicksRemaining' = leaseTicksRemaining,
    holderCoverage' = holderCoverage,
    holderHasRequiredContent' = holderHasRequiredContent,
    holderSyncEvidenceReady' = holderSyncEvidenceReady,
    holderPhase' = holderPhase,
    quorumPhase' = quorumPhase,
    repairPhase' = repairPhase,
    canonicalVersion' = canonicalVersion,
    durableVersion' = durableVersion,
    partitionBaseVersion' = partitionBaseVersion,
    quorumContentVersion' = quorumContentVersion,
    lastDecision' = TerminalStutter,
  }

  action step = any {
    holderAcquireLockAndWrite,
    {
      nondet coverage = oneOf(Set(CompleteCoverage, IncompleteCoverage))
      nondet hasContent = oneOf(Set(true, false))
      partition(coverage, hasContent)
    },
    suspendIncompleteHolder,
    tickPermit,
    prepareHolderSyncEvidence,
    holderCommitBeforeFence,
    releaseHolderLock,
    activateQuorumEpoch,
    repairCanonicalCheckpoint,
    grantQuorumLock,
    rejectStaleHolderUnlock,
    completeQuorumOperation,
    rejoinAfterHolderCommit,
    rejoinAfterQuorumCommit,
    observeFencedHolderAfterRejoin,
    stutterTerminal,
  }
}
