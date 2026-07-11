/// Safety checks for bounded same-shard local-first namespace reconciliation.
module dfs_local_first_namespace_safety {
  import common.* from "../../lib/common"
  import dfs_local_first_projection.* from "../../contracts/dfs/local_first_projection"
  import dfs_local_first_namespace.* from "../../models/dfs/local_first_namespace"

  pure def namespaceStampTotal(
    namespaceMap: EntryKey -> EntryBinding
  ): int =
    EntryKeys.fold(0, (total, entry) =>
      total + namespaceMap.get(entry).stamp
    )

  pure def namespaceCanonicalProjection(
    namespaceMap: EntryKey -> EntryBinding,
    journalHead: int
  ): CanonicalProjection = {
    authorityEpoch: 1,
    journalHead: journalHead,
    objectRevision: 0,
    namespaceRevision: namespaceStampTotal(namespaceMap),
    visibleAuthorityCount: 1,
    visibleHeadCount: 1,
    canonicalConflictCount: 0,
  }

  pure def localStageDecision(decision: NamespaceDecision): bool = any {
    decision == LocalCreateStaged,
    decision == LocalMkdirStaged,
    decision == LocalUnlinkStaged,
    decision == LocalRmdirStaged,
    decision == LocalRenameStaged,
  }

  pure def canonicalStutterDecision(decision: NamespaceDecision): bool = any {
    decision == NoDecision,
    decision == BranchStarted,
    localStageDecision(decision),
    decision == BranchManifestSealed,
    decision == ReconcilePlanPrepared,
    decision == StalePlanRejected,
    decision == IdempotentRetry,
  }

  pure def strongConcurrentDecision(decision: NamespaceDecision): bool = any {
    decision == StrongUnrelatedCommitted,
    decision == StrongEquivalentCommitted,
    decision == StrongConflictingCommitted,
    decision == StrongSameObjectAbaCommitted,
    decision == StrongDirectoryChildCommitted,
  }

  pure def terminalCommitDecision(decision: NamespaceDecision): bool = any {
    decision == ApplicableCommitted,
    decision == ConvergedCommitted,
    decision == ConflictCommitted,
    decision == SupersededCommitted,
  }

  pure def namespaceProjectionStepKind(
    decision: NamespaceDecision
  ): ProjectionStepKind =
    match decision {
      | BranchManifestSealed => LocalDurabilityStep
      | ReconcilePlanPrepared => ReconcilePlanningStep
      | StalePlanRejected => ReconcilePlanningStep
      | IdempotentRetry => ReconcilePlanningStep
      | StrongUnrelatedCommitted => StrongCanonicalCommitStep
      | StrongEquivalentCommitted => StrongCanonicalCommitStep
      | StrongConflictingCommitted => StrongCanonicalCommitStep
      | StrongSameObjectAbaCommitted => StrongCanonicalCommitStep
      | StrongDirectoryChildCommitted => StrongCanonicalCommitStep
      | ApplicableCommitted => ReconcileCanonicalCommitStep
      | ConvergedCommitted => ReconcileCanonicalCommitStep
      | ConflictCommitted => ReconcileConflictCommitStep
      | SupersededCommitted => ReconcileCanonicalCommitStep
      | _ => LocalOverlayStep
    }

  def namespaceCommitEvidence: CanonicalCommitEvidence = {
    quorumAvailable: true,
    currentAuthority: true,
    epochMatches: true,
    planHeadMatches:
      not(terminalCommitDecision(lastDecision))
        or beforeCanonicalHead == planHead,
    contentPolicySatisfied: true,
    metadataPrepareSatisfied: true,
    atomicPublication: true,
  }

  pure def unchangedOutside(
    before: EntryKey -> EntryBinding,
    after: EntryKey -> EntryBinding,
    first: EntryKey,
    second: EntryKey
  ): bool =
    EntryKeys.forall(entry =>
      if (entry == first or entry == second) {
        true
      } else {
        before.get(entry) == after.get(entry)
      }
    )

  def canonicalAndLocalNamespacesStayWellFormed: bool = all {
    namespaceWellFormed(canonicalNamespace),
    namespaceWellFormed(localNamespace),
  }

  /// Branch creation, local syscalls, sealing, planning, stale rejection, and
  /// result retry all stutter under the canonical namespace projection.
  def localProtocolCannotMutateCanonicalState: bool =
    canonicalStutterDecision(lastDecision) implies all {
      canonicalNamespace == beforeCanonicalNamespace,
      canonicalHead == beforeCanonicalHead,
    }

  /// Namespace-local work and planning stutter under the shared canonical
  /// projection. Strong transactions and terminal reconciliation outcomes each
  /// consume one abstract authority journal record; only Applicable changes
  /// bounded names, while Conflict may additionally publish a disjoint
  /// local-first sidecar.
  def everyNamespaceStepRefinesCanonicalProjection: bool =
    lastDecision == NoDecision or
      projectionStepAllowed(
        namespaceProjectionStepKind(lastDecision),
        namespaceCanonicalProjection(
          beforeCanonicalNamespace,
          beforeCanonicalHead
        ),
        namespaceCanonicalProjection(canonicalNamespace, canonicalHead),
        namespaceCommitEvidence
      )

  def onlyConflictOutcomeGrowsLocalConflictSidecars: bool =
    if (branchPhase != BranchTerminal) {
      conflictBranches == Set()
    } else if (branchResult == Conflict) {
      conflictBranches == Set(LocalBranchOne)
    } else {
      conflictBranches == Set()
    }

  /// A concurrent quorum transaction is not allowed to rewrite the sealed
  /// branch view that the user saw and durably recorded.
  def strongProtocolCannotMutateLocalOverlay: bool =
    strongConcurrentDecision(lastDecision) implies
      localNamespace == beforeLocalNamespace

  def sealedOverlayIsImmutable: bool =
    not(localStageDecision(lastDecision)) implies
      localNamespace == beforeLocalNamespace

  def canonicalNamespaceChangesAreAuthorized: bool =
    canonicalNamespace != beforeCanonicalNamespace implies any {
      lastDecision == StrongEquivalentCommitted,
      lastDecision == StrongConflictingCommitted,
      lastDecision == StrongSameObjectAbaCommitted,
      lastDecision == StrongDirectoryChildCommitted,
      lastDecision == ApplicableCommitted,
    }

  def entryStampsNeverMoveBackward: bool = all {
    EntryKeys.forall(entry =>
      canonicalNamespace.get(entry).stamp
        >= beforeCanonicalNamespace.get(entry).stamp
    ),
    EntryKeys.forall(entry =>
      localNamespace.get(entry).stamp >= beforeLocalNamespace.get(entry).stamp
    ),
  }

  /// A plan is a classification against one exact canonical journal head.
  /// Once that head changes, the cached class is unusable until replanning.
  def freshPlanMatchesCurrentClassification: bool =
    all {
      branchPhase == BranchPlanned,
      canonicalHead == planHead,
    } implies planOutcome == classifyReconciliation

  def stalePlanCannotBecomeTerminal: bool =
    all {
      branchPhase == BranchPlanned,
      canonicalHead != planHead,
    } implies branchResult == Unclassified

  def stalePlanRejectionReturnsToSealedBranch: bool =
    lastDecision == StalePlanRejected implies all {
      branchPhase == BranchSealed,
      planOutcome == Unclassified,
      branchResult == Unclassified,
      canonicalNamespace == beforeCanonicalNamespace,
    }

  def applicableCommitUsedExactEntryStamps: bool =
    lastDecision == ApplicableCommitted implies
      baseStillExactIn(beforeCanonicalNamespace)

  /// Every same-shard syscall publishes all of its name changes in one
  /// authority transition. In particular rename has no source-only or
  /// destination-only committed phase.
  def applicableCommitIsAtomic: bool =
    lastDecision == ApplicableCommitted implies all {
      unchangedOutside(
        beforeCanonicalNamespace,
        canonicalNamespace,
        sourceEntry,
        destinationEntry
      ),
      match branchOperation {
        | CreateFile => all {
            sameBindingPayload(
              canonicalNamespace.get(destinationEntry),
              localNamespace.get(destinationEntry)
            ),
            canonicalNamespace.get(destinationEntry).stamp
              == beforeCanonicalNamespace.get(destinationEntry).stamp + 1,
          }
        | MakeDirectory => all {
            sameBindingPayload(
              canonicalNamespace.get(destinationEntry),
              localNamespace.get(destinationEntry)
            ),
            canonicalNamespace.get(destinationEntry).stamp
              == beforeCanonicalNamespace.get(destinationEntry).stamp + 1,
          }
        | UnlinkFile => all {
            isAbsent(canonicalNamespace.get(sourceEntry)),
            canonicalNamespace.get(sourceEntry).stamp
              == beforeCanonicalNamespace.get(sourceEntry).stamp + 1,
          }
        | RemoveDirectory => all {
            isAbsent(canonicalNamespace.get(sourceEntry)),
            not(hasLiveChild(beforeCanonicalNamespace, DirectoryObject)),
            canonicalNamespace.get(sourceEntry).stamp
              == beforeCanonicalNamespace.get(sourceEntry).stamp + 1,
          }
        | RenameEntry => all {
            isAbsent(canonicalNamespace.get(sourceEntry)),
            sameBindingPayload(
              canonicalNamespace.get(destinationEntry),
              baseSourceBinding
            ),
            canonicalNamespace.get(sourceEntry).stamp
              == beforeCanonicalNamespace.get(sourceEntry).stamp + 1,
            canonicalNamespace.get(destinationEntry).stamp
              == beforeCanonicalNamespace.get(destinationEntry).stamp + 1,
          }
        | NoNamespaceOperation => false
      },
    }

  /// Converged work was already visible and superseded work lost its exact
  /// precondition. Neither classification is permission to rewrite names.
  def nonApplyingOutcomesPreserveCanonicalWinner: bool =
    any {
      lastDecision == ConvergedCommitted,
      lastDecision == SupersededCommitted,
    } implies canonicalNamespace == beforeCanonicalNamespace

  /// An unlink whose entry stamp now names another object must never delete
  /// that replacement.
  def unlinkChangedBindingIsSuperseded: bool =
    all {
      branchPhase == BranchTerminal,
      branchOperation == UnlinkFile,
      branchResult == Superseded,
    } implies all {
      isLive(canonicalNamespace.get(sourceEntry)),
      canonicalNamespace.get(sourceEntry) != baseSourceBinding,
    }

  def childCreateRequiresItsExactStableParent: bool =
    all {
      lastDecision == ApplicableCommitted,
      branchOperation == CreateFile,
      destinationEntry == DirectoryChildEntry,
    } implies all {
      beforeCanonicalNamespace.get(RootDirectoryEntry) == baseSourceBinding,
      beforeCanonicalNamespace.get(RootDirectoryEntry).kind == EntryDirectory,
      beforeCanonicalNamespace.get(RootDirectoryEntry).object
        == Some(DirectoryObject),
    }

  def rmdirWithConcurrentChildBecomesConflict: bool =
    all {
      branchPhase == BranchTerminal,
      branchOperation == RemoveDirectory,
      branchResult == Conflict,
    } implies hasLiveChild(canonicalNamespace, DirectoryObject)

  /// A conflict is a sidecar result. The quorum namespace remains the winner,
  /// while the deterministic branch record retains every symbolic local object
  /// identity named by the operation. Concrete reachability and storage
  /// receipts are outside this slice.
  def conflictsPreserveLocalObjectsWithoutChangingNames: bool =
    lastDecision == ConflictCommitted implies all {
      canonicalNamespace == beforeCanonicalNamespace,
      conflictBranches.contains(LocalBranchOne),
      operationObjectsToPreserve.subseteq(preservedObjects),
    }

  def conflictIdentityIsDeterministic: bool = all {
    conflictBranches.subseteq(Set(LocalBranchOne)),
    not(conflictBranches.contains(LocalBranchReserved)),
  }

  def terminalResultIsDurable: bool =
    branchPhase == BranchTerminal implies all {
      branchResult != Unclassified,
      canonicalNamespace == terminalCanonicalNamespace,
      canonicalHead == terminalCanonicalHead,
      conflictBranches == terminalConflictBranches,
      preservedObjects == terminalPreservedObjects,
    }

  def terminalRetryIsIdempotent: bool =
    lastDecision == IdempotentRetry implies all {
      branchPhase == BranchTerminal,
      canonicalNamespace == terminalCanonicalNamespace,
      canonicalHead == terminalCanonicalHead,
      conflictBranches == terminalConflictBranches,
      preservedObjects == terminalPreservedObjects,
      canonicalNamespace == beforeCanonicalNamespace,
      canonicalHead == beforeCanonicalHead,
    }

  def terminalCommitAdvancesOneAuthorityRecord: bool =
    terminalCommitDecision(lastDecision) implies
      canonicalHead == beforeCanonicalHead + 1

  /// Directory children remain keyed by the directory object, not its old
  /// path. Moving the directory binding therefore does not rewrite child keys.
  def directoryRenameKeepsStableParentIdentity: bool =
    all {
      branchOperation == RenameEntry,
      baseSourceBinding.kind == EntryDirectory,
    } implies all {
      parentObject(DirectoryChildEntry) == DirectoryObject,
      entryName(DirectoryChildEntry) == ChildName,
    }

  def directoryRenamePreservesLiveChild: bool =
    all {
      lastDecision == ApplicableCommitted,
      branchOperation == RenameEntry,
      baseSourceBinding.kind == EntryDirectory,
      isLive(beforeCanonicalNamespace.get(DirectoryChildEntry)),
    } implies all {
      canonicalNamespace.get(DirectoryChildEntry)
        == beforeCanonicalNamespace.get(DirectoryChildEntry),
      canonicalNamespace.get(destinationEntry).object
        == baseSourceBinding.object,
      parentObject(DirectoryChildEntry)
        == unwrapOr(baseSourceBinding.object, DirectoryObject),
    }

  def safety: bool = all {
    canonicalAndLocalNamespacesStayWellFormed,
    localProtocolCannotMutateCanonicalState,
    everyNamespaceStepRefinesCanonicalProjection,
    onlyConflictOutcomeGrowsLocalConflictSidecars,
    strongProtocolCannotMutateLocalOverlay,
    sealedOverlayIsImmutable,
    canonicalNamespaceChangesAreAuthorized,
    entryStampsNeverMoveBackward,
    freshPlanMatchesCurrentClassification,
    stalePlanCannotBecomeTerminal,
    stalePlanRejectionReturnsToSealedBranch,
    applicableCommitUsedExactEntryStamps,
    applicableCommitIsAtomic,
    nonApplyingOutcomesPreserveCanonicalWinner,
    unlinkChangedBindingIsSuperseded,
    childCreateRequiresItsExactStableParent,
    rmdirWithConcurrentChildBecomesConflict,
    conflictsPreserveLocalObjectsWithoutChangingNames,
    conflictIdentityIsDeterministic,
    terminalResultIsDurable,
    terminalRetryIsIdempotent,
    terminalCommitAdvancesOneAuthorityRecord,
    directoryRenameKeepsStableParentIdentity,
    directoryRenamePreservesLiveChild,
  }

  def witnessApplicable: bool = all {
    branchPhase == BranchTerminal,
    branchResult == Applicable,
  }

  def witnessConverged: bool = all {
    branchPhase == BranchTerminal,
    branchResult == Converged,
  }

  def witnessConflictPreserved: bool = all {
    branchPhase == BranchTerminal,
    branchResult == Conflict,
    conflictBranches.contains(LocalBranchOne),
    operationObjectsToPreserve.subseteq(preservedObjects),
  }

  def witnessSuperseded: bool = all {
    branchPhase == BranchTerminal,
    branchResult == Superseded,
  }

  def witnessStalePlanRejected: bool = all {
    lastDecision == StalePlanRejected,
    branchPhase == BranchSealed,
    planOutcome == Unclassified,
  }

  def witnessSameObjectAbaSuperseded: bool = all {
    branchPhase == BranchTerminal,
    branchOperation == UnlinkFile,
    branchResult == Superseded,
    canonicalNamespace.get(sourceEntry).object == baseSourceBinding.object,
    canonicalNamespace.get(sourceEntry).stamp > baseSourceBinding.stamp,
  }

  def witnessChildParentChangeConflicts: bool = all {
    branchPhase == BranchTerminal,
    branchOperation == CreateFile,
    destinationEntry == DirectoryChildEntry,
    branchResult == Conflict,
    canonicalNamespace.get(RootDirectoryEntry).object
      == Some(StrongDirectoryObject),
    preservedObjects.contains(LocalFileObject),
  }

  def witnessDirectoryRenamePreservesConcurrentChild: bool = all {
    branchPhase == BranchTerminal,
    branchOperation == RenameEntry,
    branchResult == Applicable,
    baseSourceBinding.kind == EntryDirectory,
    canonicalNamespace.get(destinationEntry).object == Some(DirectoryObject),
    canonicalNamespace.get(DirectoryChildEntry).object == Some(StrongFileObject),
    parentObject(DirectoryChildEntry) == DirectoryObject,
  }

  def witnessRmdirConcurrentChildConflicts: bool = all {
    branchPhase == BranchTerminal,
    branchOperation == RemoveDirectory,
    branchResult == Conflict,
    canonicalNamespace.get(DirectoryChildEntry).object
      == Some(StrongFileObject),
  }

  /// Bounded full-causal driver for all four namespace classifications. A file
  /// rename deliberately invalidates an applicable plan before replanning as a
  /// conflict; a directory rename admits a concurrent child and then applies
  /// without rewriting it. Full-depth symbolic checks therefore consume the
  /// same aggregate `safety` predicate as the nondeterministic model.
  action namespaceReconcileFullSafetyStep: bool =
    if (branchPhase == NoBranch) {
      beginBranch
    } else if (branchPhase == BranchOpen) {
      any {
        stageMkdir(RootSpareEntry),
        stageCreate(RootSpareEntry),
        stageCreate(DirectoryChildEntry),
        stageRename(RootFileEntry, RootSpareEntry),
        stageRename(RootDirectoryEntry, RootSpareEntry),
        stageUnlink(RootFileEntry),
        stageRmdir(RootDirectoryEntry),
      }
    } else if (branchPhase == BranchDirty) {
      sealBranch
    } else if (branchPhase == BranchSealed) {
      if (all {
        branchOperation == CreateFile,
        destinationEntry == DirectoryChildEntry,
        canonicalHead == 0,
      }) {
        commitStrongConflict
      } else if (branchOperation == CreateFile and canonicalHead == 0) {
        commitStrongEquivalent
      } else if (branchOperation == UnlinkFile and canonicalHead == 0) {
        any {
          commitStrongConflict,
          commitStrongSameObjectAba,
        }
      } else if (all {
        branchOperation == RenameEntry,
        baseSourceBinding.kind == EntryDirectory,
        canonicalHead == 0,
      }) {
        commitStrongDirectoryChild
      } else if (branchOperation == RemoveDirectory and canonicalHead == 0) {
        any {
          commitStrongConflict,
          prepareReconcilePlan,
        }
      } else {
        prepareReconcilePlan
      }
    } else if (branchPhase == BranchPlanned) {
      if (canonicalHead != planHead) {
        rejectStalePlan
      } else if (all {
        branchOperation == RenameEntry,
        planOutcome == Applicable,
        canonicalHead == 0,
      }) {
        commitStrongConflict
      } else {
        commitReconcilePlan
      }
    } else if (branchPhase == BranchTerminal) {
      retryTerminalResult
    } else {
      all { false, beginBranch }
    }
}
