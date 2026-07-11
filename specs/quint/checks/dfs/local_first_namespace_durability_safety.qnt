/// Safety checks for local-first namespace directory durability.
module dfs_local_first_namespace_durability_safety {
  import common.* from "../../lib/common"
  import dfs_local_first_projection.* from "../../contracts/dfs/local_first_projection"
  import dfs_local_first_namespace_durability.* from "../../models/dfs/local_first_namespace_durability"

  pure def lfndCanonicalProjection(revision: int): CanonicalProjection = {
    authorityEpoch: 1,
    journalHead: revision,
    objectRevision: 0,
    namespaceRevision: revision,
    visibleAuthorityCount: 1,
    visibleHeadCount: 1,
    canonicalConflictCount: 0,
  }

  pure def lfndProjectionStepKind(
    decision: NamespaceDurabilityDecision
  ): ProjectionStepKind =
    match decision {
      | NamespaceDirectoryFsyncReturned => LocalDurabilityStep
      | NamespaceDurabilityBranchSealed => LocalDurabilityStep
      | _ => LocalOverlayStep
    }

  pure val lfndUnusedCommitEvidence: CanonicalCommitEvidence = {
    quorumAvailable: false,
    currentAuthority: false,
    epochMatches: false,
    planHeadMatches: false,
    contentPolicySatisfied: false,
    metadataPrepareSatisfied: false,
    atomicPublication: false,
  }

  def lfndStateIsWellFormed: bool = all {
    lfndCanonicalRevision == 0,
    lfndDirectorySyncGeneration >= 0,
    lfndDirectorySyncGeneration <= MaxDirectorySyncGeneration,
    lfndReturnCount >= 0,
    lfndReturnCount <= MaxNamespaceReturnCount,
    lfndMutationId == NamespaceMutation1,
    lfndPhase == NamespaceBranchAbsent implies all {
      lfndOverlay == NamespaceBase,
      lfndDurableOverlay == NamespaceBase,
      lfndSealedOverlay == None,
    },
  }

  /// No transition in this companion slice has canonical authority. The
  /// deliberately false commit evidence also demonstrates that local
  /// projection steps do not acquire authority by accident.
  def lfndEveryStepStuttersCanonicalProjection: bool =
    lfndLastDecision == NoNamespaceDurabilityDecision or
      projectionStepAllowed(
        lfndProjectionStepKind(lfndLastDecision),
        lfndCanonicalProjection(lfndLastAudit.canonicalRevisionBefore),
        lfndCanonicalProjection(lfndCanonicalRevision),
        lfndUnusedCommitEvidence
      )

  def lfndReturnedMutationIsInitiallyVolatile: bool =
    lfndLastDecision == NamespaceMutationReturned implies all {
      lfndOverlay == NamespaceEntryCreated,
      lfndDurableOverlay == lfndLastAudit.durableOverlayBefore,
      lfndCanonicalRevision == lfndLastAudit.canonicalRevisionBefore,
      lfndReturnCount == lfndLastAudit.returnCountBefore + 1,
    }

  def lfndDirectoryFsyncPersistsExactReturnedImage: bool =
    lfndLastDecision == NamespaceDirectoryFsyncReturned implies all {
      lfndLastAudit.overlayBefore == NamespaceEntryCreated,
      lfndDurableOverlay == lfndLastAudit.overlayBefore,
      lfndOverlay == lfndLastAudit.overlayBefore,
      lfndDirectorySyncGeneration
        == lfndLastAudit.directorySyncGenerationBefore + 1,
      lfndCanonicalRevision == lfndLastAudit.canonicalRevisionBefore,
    }

  def lfndCrashRestoresExactlyLastDurableImage: bool =
    lfndLastDecision == NamespaceProcessCrashed implies all {
      lfndOverlay == lfndLastAudit.durableOverlayBefore,
      lfndDurableOverlay == lfndLastAudit.durableOverlayBefore,
      lfndDirectorySyncGeneration
        == lfndLastAudit.directorySyncGenerationBefore,
      lfndCanonicalRevision == lfndLastAudit.canonicalRevisionBefore,
    }

  def lfndVolatileReturnMayBeLostOnCrash: bool =
    all {
      lfndLastDecision == NamespaceProcessCrashed,
      lfndLastAudit.overlayBefore == NamespaceEntryCreated,
      lfndLastAudit.durableOverlayBefore == NamespaceBase,
    } implies lfndOverlay == NamespaceBase

  def lfndDirectoryFsyncedReturnSurvivesCrash: bool =
    all {
      lfndLastDecision == NamespaceProcessCrashed,
      lfndLastAudit.durableOverlayBefore == NamespaceEntryCreated,
    } implies lfndOverlay == NamespaceEntryCreated

  /// Sealing consumes the exact locally durable namespace image. There is no
  /// action that can seal a merely returned volatile mutation.
  def lfndSealRequiresExactDirectoryDurability: bool =
    lfndPhase == NamespaceBranchSealed implies all {
      lfndOverlay == NamespaceEntryCreated,
      lfndDurableOverlay == NamespaceEntryCreated,
      lfndOverlay == lfndDurableOverlay,
      lfndDirectorySyncGeneration > 0,
      lfndSealedOverlay == Some(lfndDurableOverlay),
      lfndSealedGeneration == lfndDirectorySyncGeneration,
      lfndSealedMutationId == lfndMutationId,
    }

  def lfndSealTransitionCapturedDurablePreState: bool =
    lfndLastDecision == NamespaceDurabilityBranchSealed implies all {
      lfndLastAudit.overlayBefore == NamespaceEntryCreated,
      lfndLastAudit.overlayBefore == lfndLastAudit.durableOverlayBefore,
      lfndLastAudit.directorySyncGenerationBefore > 0,
      lfndSealedOverlay == Some(lfndLastAudit.durableOverlayBefore),
      lfndCanonicalRevision == lfndLastAudit.canonicalRevisionBefore,
    }

  def lfndSealedManifestIsImmutable: bool =
    isSome(lfndLastAudit.sealedOverlayBefore) implies all {
      lfndPhase == NamespaceBranchSealed,
      lfndSealedOverlay == lfndLastAudit.sealedOverlayBefore,
      lfndSealedOverlay == Some(NamespaceEntryCreated),
    }

  /// Deterministic crash/replay/fsync/recovery driver used by the symbolic
  /// harness to exercise the complete durability path against this module's
  /// aggregate `safety` predicate.
  action lfndRecoveryFullSafetyStep: bool =
    if (lfndPhase == NamespaceBranchAbsent) {
      beginNamespaceDurabilityBranch
    } else if (all {
      lfndPhase == NamespaceBranchOpen,
      lfndReturnCount == 0,
    }) {
      returnNamespaceMutation
    } else if (all {
      lfndPhase == NamespaceBranchOpen,
      lfndReturnCount == 1,
      lfndOverlay == NamespaceEntryCreated,
      lfndDurableOverlay == NamespaceBase,
    }) {
      crashNamespaceProcess
    } else if (all {
      lfndPhase == NamespaceBranchOpen,
      lfndReturnCount == 1,
      lfndOverlay == NamespaceBase,
      lfndDurableOverlay == NamespaceBase,
    }) {
      returnNamespaceMutation
    } else if (all {
      lfndPhase == NamespaceBranchOpen,
      lfndReturnCount == 2,
      lfndOverlay == NamespaceEntryCreated,
      lfndDurableOverlay == NamespaceBase,
    }) {
      fsyncLocalDirectory
    } else if (all {
      lfndPhase == NamespaceBranchOpen,
      lfndLastDecision == NamespaceDirectoryFsyncReturned,
      lfndOverlay == NamespaceEntryCreated,
      lfndDurableOverlay == NamespaceEntryCreated,
    }) {
      crashNamespaceProcess
    } else if (all {
      lfndPhase == NamespaceBranchOpen,
      lfndLastDecision == NamespaceProcessCrashed,
      lfndOverlay == NamespaceEntryCreated,
      lfndDurableOverlay == NamespaceEntryCreated,
    }) {
      sealDurableNamespaceBranch
    } else if (lfndPhase == NamespaceBranchSealed) {
      stutterSealedNamespaceBranch
    } else {
      all { false, beginNamespaceDurabilityBranch }
    }

  def safety: bool = all {
    lfndStateIsWellFormed,
    lfndEveryStepStuttersCanonicalProjection,
    lfndReturnedMutationIsInitiallyVolatile,
    lfndDirectoryFsyncPersistsExactReturnedImage,
    lfndCrashRestoresExactlyLastDurableImage,
    lfndVolatileReturnMayBeLostOnCrash,
    lfndDirectoryFsyncedReturnSurvivesCrash,
    lfndSealRequiresExactDirectoryDurability,
    lfndSealTransitionCapturedDurablePreState,
    lfndSealedManifestIsImmutable,
  }

  def witnessReturnedMutationIsVolatile: bool = all {
    lfndLastDecision == NamespaceMutationReturned,
    lfndOverlay == NamespaceEntryCreated,
    lfndDurableOverlay == NamespaceBase,
  }

  def witnessVolatileMutationLostOnCrash: bool = all {
    lfndLastDecision == NamespaceProcessCrashed,
    lfndLastAudit.overlayBefore == NamespaceEntryCreated,
    lfndLastAudit.durableOverlayBefore == NamespaceBase,
    lfndOverlay == NamespaceBase,
  }

  def witnessDirectoryFsyncMadeMutationDurable: bool = all {
    lfndLastDecision == NamespaceDirectoryFsyncReturned,
    lfndOverlay == NamespaceEntryCreated,
    lfndDurableOverlay == NamespaceEntryCreated,
  }

  def witnessDurableMutationSurvivesCrash: bool = all {
    lfndLastDecision == NamespaceProcessCrashed,
    lfndLastAudit.durableOverlayBefore == NamespaceEntryCreated,
    lfndOverlay == NamespaceEntryCreated,
  }

  def witnessSealedExactDurableNamespace: bool = all {
    lfndLastDecision == NamespaceDurabilityBranchSealed,
    lfndPhase == NamespaceBranchSealed,
    lfndSealedOverlay == Some(NamespaceEntryCreated),
  }
}
