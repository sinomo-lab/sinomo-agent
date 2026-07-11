/// Local directory durability for one local-first namespace mutation.
///
/// This companion slice stays below reconciliation. A returned namespace
/// mutation first changes only the volatile overlay. Directory fsync copies the
/// exact overlay into the locally durable image. Process crash restores that
/// durable image, and sealing is permitted only when the returned mutation is
/// present in both images. Every transition is local-only and leaves canonical
/// DFS state unchanged.
module dfs_local_first_namespace_durability {
  import common.* from "../../lib/common"

  type NamespaceDurabilityImage = NamespaceBase | NamespaceEntryCreated
  type NamespaceMutationId = NamespaceMutation1 | NamespaceMutationReserved
  type NamespaceDurabilityPhase =
    | NamespaceBranchAbsent
    | NamespaceBranchOpen
    | NamespaceBranchSealed
  type NamespaceDurabilityDecision =
    | NoNamespaceDurabilityDecision
    | NamespaceDurabilityBranchStarted
    | NamespaceMutationReturned
    | NamespaceDirectoryFsyncReturned
    | NamespaceProcessCrashed
    | NamespaceDurabilityBranchSealed
    | NamespaceDurabilityTerminalStutter

  type NamespaceDurabilityAudit = {
    canonicalRevisionBefore: int,
    overlayBefore: NamespaceDurabilityImage,
    durableOverlayBefore: NamespaceDurabilityImage,
    directorySyncGenerationBefore: int,
    sealedOverlayBefore: Option[NamespaceDurabilityImage],
    returnCountBefore: int,
  }

  pure val MaxNamespaceReturnCount: int = 2
  pure val MaxDirectorySyncGeneration: int = 1

  pure val initialNamespaceDurabilityAudit: NamespaceDurabilityAudit = {
    canonicalRevisionBefore: 0,
    overlayBefore: NamespaceBase,
    durableOverlayBefore: NamespaceBase,
    directorySyncGenerationBefore: 0,
    sealedOverlayBefore: None,
    returnCountBefore: 0,
  }

  var lfndPhase: NamespaceDurabilityPhase
  var lfndCanonicalRevision: int
  var lfndOverlay: NamespaceDurabilityImage
  var lfndDurableOverlay: NamespaceDurabilityImage
  var lfndDirectorySyncGeneration: int
  var lfndMutationId: NamespaceMutationId
  var lfndReturnCount: int
  var lfndSealedOverlay: Option[NamespaceDurabilityImage]
  var lfndSealedGeneration: int
  var lfndSealedMutationId: NamespaceMutationId
  var lfndLastDecision: NamespaceDurabilityDecision
  var lfndLastAudit: NamespaceDurabilityAudit

  def lfndCurrentAudit: NamespaceDurabilityAudit = {
    canonicalRevisionBefore: lfndCanonicalRevision,
    overlayBefore: lfndOverlay,
    durableOverlayBefore: lfndDurableOverlay,
    directorySyncGenerationBefore: lfndDirectorySyncGeneration,
    sealedOverlayBefore: lfndSealedOverlay,
    returnCountBefore: lfndReturnCount,
  }

  action init = all {
    lfndPhase' = NamespaceBranchAbsent,
    lfndCanonicalRevision' = 0,
    lfndOverlay' = NamespaceBase,
    lfndDurableOverlay' = NamespaceBase,
    lfndDirectorySyncGeneration' = 0,
    lfndMutationId' = NamespaceMutation1,
    lfndReturnCount' = 0,
    lfndSealedOverlay' = None,
    lfndSealedGeneration' = 0,
    lfndSealedMutationId' = NamespaceMutation1,
    lfndLastDecision' = NoNamespaceDurabilityDecision,
    lfndLastAudit' = initialNamespaceDurabilityAudit,
  }

  action beginNamespaceDurabilityBranch: bool = all {
    lfndPhase == NamespaceBranchAbsent,
    lfndPhase' = NamespaceBranchOpen,
    lfndCanonicalRevision' = lfndCanonicalRevision,
    lfndOverlay' = lfndOverlay,
    lfndDurableOverlay' = lfndDurableOverlay,
    lfndDirectorySyncGeneration' = lfndDirectorySyncGeneration,
    lfndMutationId' = lfndMutationId,
    lfndReturnCount' = lfndReturnCount,
    lfndSealedOverlay' = lfndSealedOverlay,
    lfndSealedGeneration' = lfndSealedGeneration,
    lfndSealedMutationId' = lfndSealedMutationId,
    lfndLastDecision' = NamespaceDurabilityBranchStarted,
    lfndLastAudit' = lfndCurrentAudit,
  }

  /// The syscall has returned to the local caller, but no directory durability
  /// claim is implied. Reissuing after a volatile crash uses the same mutation
  /// identity and recreates the same overlay image.
  action returnNamespaceMutation: bool = all {
    lfndPhase == NamespaceBranchOpen,
    lfndOverlay == NamespaceBase,
    lfndReturnCount < MaxNamespaceReturnCount,
    lfndPhase' = lfndPhase,
    lfndCanonicalRevision' = lfndCanonicalRevision,
    lfndOverlay' = NamespaceEntryCreated,
    lfndDurableOverlay' = lfndDurableOverlay,
    lfndDirectorySyncGeneration' = lfndDirectorySyncGeneration,
    lfndMutationId' = lfndMutationId,
    lfndReturnCount' = lfndReturnCount + 1,
    lfndSealedOverlay' = lfndSealedOverlay,
    lfndSealedGeneration' = lfndSealedGeneration,
    lfndSealedMutationId' = lfndSealedMutationId,
    lfndLastDecision' = NamespaceMutationReturned,
    lfndLastAudit' = lfndCurrentAudit,
  }

  action fsyncLocalDirectory: bool = all {
    lfndPhase == NamespaceBranchOpen,
    lfndOverlay == NamespaceEntryCreated,
    lfndDurableOverlay != lfndOverlay,
    lfndDirectorySyncGeneration < MaxDirectorySyncGeneration,
    lfndPhase' = lfndPhase,
    lfndCanonicalRevision' = lfndCanonicalRevision,
    lfndOverlay' = lfndOverlay,
    lfndDurableOverlay' = lfndOverlay,
    lfndDirectorySyncGeneration' = lfndDirectorySyncGeneration + 1,
    lfndMutationId' = lfndMutationId,
    lfndReturnCount' = lfndReturnCount,
    lfndSealedOverlay' = lfndSealedOverlay,
    lfndSealedGeneration' = lfndSealedGeneration,
    lfndSealedMutationId' = lfndSealedMutationId,
    lfndLastDecision' = NamespaceDirectoryFsyncReturned,
    lfndLastAudit' = lfndCurrentAudit,
  }

  action crashNamespaceProcess: bool = all {
    lfndPhase == NamespaceBranchOpen,
    lfndPhase' = lfndPhase,
    lfndCanonicalRevision' = lfndCanonicalRevision,
    lfndOverlay' = lfndDurableOverlay,
    lfndDurableOverlay' = lfndDurableOverlay,
    lfndDirectorySyncGeneration' = lfndDirectorySyncGeneration,
    lfndMutationId' = lfndMutationId,
    lfndReturnCount' = lfndReturnCount,
    lfndSealedOverlay' = lfndSealedOverlay,
    lfndSealedGeneration' = lfndSealedGeneration,
    lfndSealedMutationId' = lfndSealedMutationId,
    lfndLastDecision' = NamespaceProcessCrashed,
    lfndLastAudit' = lfndCurrentAudit,
  }

  action sealDurableNamespaceBranch: bool = all {
    lfndPhase == NamespaceBranchOpen,
    lfndOverlay == NamespaceEntryCreated,
    lfndOverlay == lfndDurableOverlay,
    lfndDirectorySyncGeneration > 0,
    lfndPhase' = NamespaceBranchSealed,
    lfndCanonicalRevision' = lfndCanonicalRevision,
    lfndOverlay' = lfndOverlay,
    lfndDurableOverlay' = lfndDurableOverlay,
    lfndDirectorySyncGeneration' = lfndDirectorySyncGeneration,
    lfndMutationId' = lfndMutationId,
    lfndReturnCount' = lfndReturnCount,
    lfndSealedOverlay' = Some(lfndDurableOverlay),
    lfndSealedGeneration' = lfndDirectorySyncGeneration,
    lfndSealedMutationId' = lfndMutationId,
    lfndLastDecision' = NamespaceDurabilityBranchSealed,
    lfndLastAudit' = lfndCurrentAudit,
  }

  action stutterSealedNamespaceBranch: bool = all {
    lfndPhase == NamespaceBranchSealed,
    lfndPhase' = lfndPhase,
    lfndCanonicalRevision' = lfndCanonicalRevision,
    lfndOverlay' = lfndOverlay,
    lfndDurableOverlay' = lfndDurableOverlay,
    lfndDirectorySyncGeneration' = lfndDirectorySyncGeneration,
    lfndMutationId' = lfndMutationId,
    lfndReturnCount' = lfndReturnCount,
    lfndSealedOverlay' = lfndSealedOverlay,
    lfndSealedGeneration' = lfndSealedGeneration,
    lfndSealedMutationId' = lfndSealedMutationId,
    lfndLastDecision' = NamespaceDurabilityTerminalStutter,
    lfndLastAudit' = lfndCurrentAudit,
  }

  action step = any {
    beginNamespaceDurabilityBranch,
    returnNamespaceMutation,
    fsyncLocalDirectory,
    crashNamespaceProcess,
    sealDurableNamespaceBranch,
    stutterSealedNamespaceBranch,
  }
}
