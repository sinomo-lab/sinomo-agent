/// Bounded local-first regular-file branch and reconciliation protocol.
///
/// The canonical file head remains single-valued and quorum-owned. An opted-in
/// disconnected client may write and explicitly sync a local overlay, but
/// those actions stutter at the canonical projection. Reconnection seals an
/// immutable branch payload, records its exact symbolic content in a retention
/// marker, and plans reconciliation against one exact canonical head. A
/// changed head invalidates the plan. A fresh plan either
/// applies one ordinary canonical version, observes convergence, or records a
/// sidecar conflict while leaving the canonical winner unchanged.
///
/// Durable reconciliation records are keyed by a complete request identity.
/// A lost reply can therefore be recovered exactly without repeating either a
/// canonical publication or conflict materialization.
module dfs_local_first_file_branch {
  import common.* from "../../lib/common"

  type FileRevision = FileBase | FileConcurrent | FileReconciled
  type FileContent =
    | BaseBytes
    | LocalWriteBytes
    | LocalTruncatedBytes
    | RemoteBytes

  type BranchPolicy = StrongOnly | LocalFirstAllowed
  type BranchPhase =
    | BranchAbsent
    | BranchOpen
    | BranchFrozen
    | BranchSealed
    | BranchPlanned
    | BranchTerminal

  type ReconcileId = Reconcile1 | Reconcile2
  type ConflictId = FileConflict1 | FileConflict2
  type PlanDisposition = PlanApply | PlanConverged | PlanConflict

  type FileHead = {
    revision: FileRevision,
    content: FileContent,
  }

  /// The base head and local sync generation make the sealed payload an exact
  /// request identity rather than a request id plus mutable ambient state.
  type BranchPayload = {
    baseHead: FileHead,
    content: FileContent,
    localSyncGeneration: int,
  }

  type ReconcilePlan = {
    requestId: ReconcileId,
    payload: BranchPayload,
    observedHead: FileHead,
    disposition: PlanDisposition,
  }

  type ReconcileResult =
    | AppliedResult(FileHead)
    | ConvergedResult(FileHead)
    | ConflictResult(ConflictId)

  type ReturnedReconcileResult = {
    requestId: ReconcileId,
    result: ReconcileResult,
  }

  type DurableResultRecord = {
    payload: BranchPayload,
    result: ReconcileResult,
  }

  type LocalFirstConflict = {
    id: ConflictId,
    requestId: ReconcileId,
    payload: BranchPayload,
    canonicalWinner: FileHead,
    retainedContent: FileContent,
  }

  type ReconcileResponse =
    | NoReconcileResponse
    | ReconcilePlanReady(ReconcileId)
    | ReconcilePlanStale(ReconcileId)
    | ReconcileResultReturned(ReturnedReconcileResult)
    | ReconcileOutcomeUnknown(ReconcileId)
    | ReconcilePayloadMismatch(ReconcileId)

  type FileBranchDecision =
    | NoFileBranchDecision
    | LocalBranchStarted
    | LocalWriteReturned
    | LocalTruncateReturned
    | LocalFsyncReturned
    | LocalProcessCrashed
    | LocalBranchSealed
    | AuthorityReturnSealedBranch
    | AuthorityReturnFrozeBranch
    | CleanFrozenBranchClosed
    | LocalAuthorityRouteLost
    | CanonicalQuorumLost
    | CanonicalQuorumRestored
    | SealedContentRetained
    | LocalAuthorityRestored
    | CanonicalConcurrentWrite
    | ReconcilePlanCreated
    | ReconcileGatewayProgressed
    | StaleReconcilePlanRejected
    | ReconcileApplied
    | ReconcileConverged
    | ReconcileConflictMaterialized
    | ReconcileProcessRestarted
    | ExactReconcileRetryReplayed
    | ChangedReconcileRetryRejected
    | FileBranchTerminalStutter

  /// Pre-state facts used to state transition-local refinement properties as
  /// ordinary state invariants.
  type FileBranchAudit = {
    canonicalBefore: FileHead,
    conflictBefore: Option[LocalFirstConflict],
    effectCountBefore: int,
    overlayBefore: FileContent,
    durableOverlayBefore: FileContent,
    resultRecordsBefore: ReconcileId -> Option[DurableResultRecord],
    decisionBefore: FileBranchDecision,
    phaseBefore: BranchPhase,
    authorityReachableBefore: bool,
    quorumAvailableBefore: bool,
  }

  pure val FileContents: Set[FileContent] =
    Set(BaseBytes, LocalWriteBytes, LocalTruncatedBytes, RemoteBytes)
  pure val ConcurrentContents: Set[FileContent] =
    Set(LocalWriteBytes, LocalTruncatedBytes, RemoteBytes)
  pure val ReconcileIds: Set[ReconcileId] =
    Set(Reconcile1, Reconcile2)
  pure val RetrySyncGenerations: Set[int] = Set(0, 1, 2)
  pure val MaxLocalSyncGeneration: int = 2

  pure val baseHead: FileHead = {
    revision: FileBase,
    content: BaseBytes,
  }

  pure val RetryBaseHeads: Set[FileHead] = Set(
    baseHead,
    { revision: FileConcurrent, content: LocalWriteBytes },
    { revision: FileConcurrent, content: LocalTruncatedBytes },
    { revision: FileConcurrent, content: RemoteBytes }
  )

  pure val defaultPayload: BranchPayload = {
    baseHead: baseHead,
    content: BaseBytes,
    localSyncGeneration: 0,
  }

  pure val defaultPlan: ReconcilePlan = {
    requestId: Reconcile1,
    payload: defaultPayload,
    observedHead: baseHead,
    disposition: PlanConverged,
  }

  pure val defaultConflict: LocalFirstConflict = {
    id: FileConflict1,
    requestId: Reconcile1,
    payload: defaultPayload,
    canonicalWinner: baseHead,
    retainedContent: BaseBytes,
  }

  pure val defaultResultRecord: DurableResultRecord = {
    payload: defaultPayload,
    result: ConvergedResult(baseHead),
  }

  pure val emptyResultRecords: ReconcileId -> Option[DurableResultRecord] =
    ReconcileIds.mapBy(_ => None)

  pure val defaultAudit: FileBranchAudit = {
    canonicalBefore: baseHead,
    conflictBefore: None,
    effectCountBefore: 0,
    overlayBefore: BaseBytes,
    durableOverlayBefore: BaseBytes,
    resultRecordsBefore: emptyResultRecords,
    decisionBefore: NoFileBranchDecision,
    phaseBefore: BranchAbsent,
    authorityReachableBefore: false,
    quorumAvailableBefore: true,
  }

  var lfPolicy: BranchPolicy
  var lfPhase: BranchPhase
  var lfLocalAuthorityReachable: bool
  var lfCanonicalQuorumAvailable: bool
  var lfCanonicalHead: FileHead
  var lfBranchBase: FileHead
  var lfOverlayContent: FileContent
  var lfDurableOverlayContent: FileContent
  var lfLocalSyncGeneration: int
  var lfSealedPayload: Option[BranchPayload]
  var lfRetainedContent: Option[FileContent]
  var lfPlan: Option[ReconcilePlan]
  var lfResultRecords: ReconcileId -> Option[DurableResultRecord]
  var lfConflictRecord: Option[LocalFirstConflict]
  var lfEffectCount: int
  var lfLastResponse: ReconcileResponse
  var lfLastDecision: FileBranchDecision
  var lfLastAudit: FileBranchAudit
  var lfLastReconcileId: Option[ReconcileId]
  var lfLastSuppliedPayload: Option[BranchPayload]

  pure def classifyPlan(
    payload: BranchPayload,
    observed: FileHead
  ): PlanDisposition =
    if (observed.content == payload.content) {
      PlanConverged
    } else if (observed == payload.baseHead) {
      PlanApply
    } else {
      PlanConflict
    }

  pure def reconciledHead(payload: BranchPayload): FileHead = {
    revision: FileReconciled,
    content: payload.content,
  }

  def currentAudit: FileBranchAudit = {
    canonicalBefore: lfCanonicalHead,
    conflictBefore: lfConflictRecord,
    effectCountBefore: lfEffectCount,
    overlayBefore: lfOverlayContent,
    durableOverlayBefore: lfDurableOverlayContent,
    resultRecordsBefore: lfResultRecords,
    decisionBefore: lfLastDecision,
    phaseBefore: lfPhase,
    authorityReachableBefore: lfLocalAuthorityReachable,
    quorumAvailableBefore: lfCanonicalQuorumAvailable,
  }

  pure def localOnlyDecision(decision: FileBranchDecision): bool =
    any {
      decision == LocalBranchStarted,
      decision == LocalWriteReturned,
      decision == LocalTruncateReturned,
      decision == LocalFsyncReturned,
      decision == LocalProcessCrashed,
      decision == LocalBranchSealed,
      decision == AuthorityReturnSealedBranch,
      decision == AuthorityReturnFrozeBranch,
      decision == CleanFrozenBranchClosed,
      decision == LocalAuthorityRouteLost,
      decision == CanonicalQuorumLost,
      decision == CanonicalQuorumRestored,
      decision == SealedContentRetained,
      decision == LocalAuthorityRestored,
      decision == ReconcileGatewayProgressed,
      decision == ReconcileProcessRestarted,
    }

  pure def recordedResultCount(
    records: ReconcileId -> Option[DurableResultRecord]
  ): int =
    ReconcileIds.filter(id => isSome(records.get(id))).size()

  action init = all {
    lfPolicy' = LocalFirstAllowed,
    lfPhase' = BranchAbsent,
    lfLocalAuthorityReachable' = false,
    lfCanonicalQuorumAvailable' = true,
    lfCanonicalHead' = baseHead,
    lfBranchBase' = baseHead,
    lfOverlayContent' = BaseBytes,
    lfDurableOverlayContent' = BaseBytes,
    lfLocalSyncGeneration' = 0,
    lfSealedPayload' = None,
    lfRetainedContent' = None,
    lfPlan' = None,
    lfResultRecords' = emptyResultRecords,
    lfConflictRecord' = None,
    lfEffectCount' = 0,
    lfLastResponse' = NoReconcileResponse,
    lfLastDecision' = NoFileBranchDecision,
    lfLastAudit' = defaultAudit,
    lfLastReconcileId' = None,
    lfLastSuppliedPayload' = None,
  }

  /// Entering degraded mode is explicitly policy-gated. Failure to reach the
  /// authority does not grant authority; it only permits a local overlay.
  action beginLocalBranch: bool = all {
    lfPhase == BranchAbsent,
    lfPolicy == LocalFirstAllowed,
    not(lfLocalAuthorityReachable),
    lfPhase' = BranchOpen,
    lfLocalAuthorityReachable' = lfLocalAuthorityReachable,
    lfCanonicalQuorumAvailable' = lfCanonicalQuorumAvailable,
    lfCanonicalHead' = lfCanonicalHead,
    lfBranchBase' = lfCanonicalHead,
    lfOverlayContent' = lfCanonicalHead.content,
    lfDurableOverlayContent' = lfCanonicalHead.content,
    lfLocalSyncGeneration' = 0,
    lfSealedPayload' = None,
    lfRetainedContent' = None,
    lfPlan' = None,
    lfResultRecords' = lfResultRecords,
    lfConflictRecord' = lfConflictRecord,
    lfEffectCount' = lfEffectCount,
    lfPolicy' = lfPolicy,
    lfLastResponse' = NoReconcileResponse,
    lfLastDecision' = LocalBranchStarted,
    lfLastAudit' = currentAudit,
    lfLastReconcileId' = None,
    lfLastSuppliedPayload' = None,
  }

  action localWrite: bool = all {
    lfPhase == BranchOpen,
    not(lfLocalAuthorityReachable),
    lfOverlayContent' = LocalWriteBytes,
    lfPolicy' = lfPolicy,
    lfPhase' = lfPhase,
    lfLocalAuthorityReachable' = lfLocalAuthorityReachable,
    lfCanonicalQuorumAvailable' = lfCanonicalQuorumAvailable,
    lfCanonicalHead' = lfCanonicalHead,
    lfBranchBase' = lfBranchBase,
    lfDurableOverlayContent' = lfDurableOverlayContent,
    lfLocalSyncGeneration' = lfLocalSyncGeneration,
    lfSealedPayload' = lfSealedPayload,
    lfRetainedContent' = lfRetainedContent,
    lfPlan' = lfPlan,
    lfResultRecords' = lfResultRecords,
    lfConflictRecord' = lfConflictRecord,
    lfEffectCount' = lfEffectCount,
    lfLastResponse' = NoReconcileResponse,
    lfLastDecision' = LocalWriteReturned,
    lfLastAudit' = currentAudit,
    lfLastReconcileId' = None,
    lfLastSuppliedPayload' = None,
  }

  action localTruncate: bool = all {
    lfPhase == BranchOpen,
    not(lfLocalAuthorityReachable),
    lfOverlayContent' = LocalTruncatedBytes,
    lfPolicy' = lfPolicy,
    lfPhase' = lfPhase,
    lfLocalAuthorityReachable' = lfLocalAuthorityReachable,
    lfCanonicalQuorumAvailable' = lfCanonicalQuorumAvailable,
    lfCanonicalHead' = lfCanonicalHead,
    lfBranchBase' = lfBranchBase,
    lfDurableOverlayContent' = lfDurableOverlayContent,
    lfLocalSyncGeneration' = lfLocalSyncGeneration,
    lfSealedPayload' = lfSealedPayload,
    lfRetainedContent' = lfRetainedContent,
    lfPlan' = lfPlan,
    lfResultRecords' = lfResultRecords,
    lfConflictRecord' = lfConflictRecord,
    lfEffectCount' = lfEffectCount,
    lfLastResponse' = NoReconcileResponse,
    lfLastDecision' = LocalTruncateReturned,
    lfLastAudit' = currentAudit,
    lfLastReconcileId' = None,
    lfLastSuppliedPayload' = None,
  }

  /// Local fsync certifies only the branch overlay. It neither advances nor
  /// manufactures a canonical quorum certificate.
  action localFsync: bool = all {
    any {
      lfPhase == BranchOpen,
      lfPhase == BranchFrozen,
    },
    lfLocalSyncGeneration < MaxLocalSyncGeneration,
    lfDurableOverlayContent' = lfOverlayContent,
    lfLocalSyncGeneration' = lfLocalSyncGeneration + 1,
    lfPolicy' = lfPolicy,
    lfPhase' = lfPhase,
    lfLocalAuthorityReachable' = lfLocalAuthorityReachable,
    lfCanonicalQuorumAvailable' = lfCanonicalQuorumAvailable,
    lfCanonicalHead' = lfCanonicalHead,
    lfBranchBase' = lfBranchBase,
    lfOverlayContent' = lfOverlayContent,
    lfSealedPayload' = lfSealedPayload,
    lfRetainedContent' = lfRetainedContent,
    lfPlan' = lfPlan,
    lfResultRecords' = lfResultRecords,
    lfConflictRecord' = lfConflictRecord,
    lfEffectCount' = lfEffectCount,
    lfLastResponse' = NoReconcileResponse,
    lfLastDecision' = LocalFsyncReturned,
    lfLastAudit' = currentAudit,
    lfLastReconcileId' = None,
    lfLastSuppliedPayload' = None,
  }

  /// A crash loses only the unsynced suffix and recovers the last local fsync
  /// image. Canonical state is outside this recovery domain.
  action crashLocalProcess: bool = all {
    any {
      lfPhase == BranchOpen,
      lfPhase == BranchFrozen,
    },
    lfOverlayContent' = lfDurableOverlayContent,
    lfPolicy' = lfPolicy,
    lfPhase' = lfPhase,
    lfLocalAuthorityReachable' = lfLocalAuthorityReachable,
    lfCanonicalQuorumAvailable' = lfCanonicalQuorumAvailable,
    lfCanonicalHead' = lfCanonicalHead,
    lfBranchBase' = lfBranchBase,
    lfDurableOverlayContent' = lfDurableOverlayContent,
    lfLocalSyncGeneration' = lfLocalSyncGeneration,
    lfSealedPayload' = lfSealedPayload,
    lfRetainedContent' = lfRetainedContent,
    lfPlan' = lfPlan,
    lfResultRecords' = lfResultRecords,
    lfConflictRecord' = lfConflictRecord,
    lfEffectCount' = lfEffectCount,
    lfLastResponse' = NoReconcileResponse,
    lfLastDecision' = LocalProcessCrashed,
    lfLastAudit' = currentAudit,
    lfLastReconcileId' = None,
    lfLastSuppliedPayload' = None,
  }

  action sealLocalBranch: bool = {
    val payload = {
      baseHead: lfBranchBase,
      content: lfOverlayContent,
      localSyncGeneration: lfLocalSyncGeneration,
    }
    all {
      any {
        lfPhase == BranchOpen,
        lfPhase == BranchFrozen,
      },
      lfOverlayContent == lfDurableOverlayContent,
      lfOverlayContent != lfBranchBase.content,
      lfLocalSyncGeneration > 0,
      lfPhase' = BranchSealed,
      lfSealedPayload' = Some(payload),
      lfPolicy' = lfPolicy,
      lfLocalAuthorityReachable' = lfLocalAuthorityReachable,
      lfCanonicalQuorumAvailable' = lfCanonicalQuorumAvailable,
      lfCanonicalHead' = lfCanonicalHead,
      lfBranchBase' = lfBranchBase,
      lfOverlayContent' = lfOverlayContent,
      lfDurableOverlayContent' = lfDurableOverlayContent,
      lfLocalSyncGeneration' = lfLocalSyncGeneration,
      lfRetainedContent' = None,
      lfPlan' = None,
      lfResultRecords' = lfResultRecords,
      lfConflictRecord' = lfConflictRecord,
      lfEffectCount' = lfEffectCount,
      lfLastResponse' = NoReconcileResponse,
      lfLastDecision' = LocalBranchSealed,
      lfLastAudit' = currentAudit,
      lfLastReconcileId' = None,
      lfLastSuppliedPayload' = None,
    }
  }

  /// Authority-route return is an independent environment event. A locally
  /// durable dirty branch seals atomically. Any clean or unsynced branch enters
  /// a permanently frozen phase: no later route flap can re-enable degraded
  /// mutation, though local fsync/crash and eventual seal remain possible.
  action sealBranchOnAuthorityReturn: bool = {
    val payload = {
      baseHead: lfBranchBase,
      content: lfOverlayContent,
      localSyncGeneration: lfLocalSyncGeneration,
    }
    val canSeal = all {
      lfOverlayContent == lfDurableOverlayContent,
      lfOverlayContent != lfBranchBase.content,
      lfLocalSyncGeneration > 0,
    }
    all {
      lfPhase == BranchOpen,
      not(lfLocalAuthorityReachable),
      lfPhase' = if (canSeal) { BranchSealed } else { BranchFrozen },
      lfLocalAuthorityReachable' = true,
      lfSealedPayload' = if (canSeal) { Some(payload) } else { None },
      lfPolicy' = lfPolicy,
      lfCanonicalQuorumAvailable' = lfCanonicalQuorumAvailable,
      lfCanonicalHead' = lfCanonicalHead,
      lfBranchBase' = lfBranchBase,
      lfOverlayContent' = lfOverlayContent,
      lfDurableOverlayContent' = lfDurableOverlayContent,
      lfLocalSyncGeneration' = lfLocalSyncGeneration,
      lfRetainedContent' = None,
      lfPlan' = None,
      lfResultRecords' = lfResultRecords,
      lfConflictRecord' = lfConflictRecord,
      lfEffectCount' = lfEffectCount,
      lfLastResponse' = NoReconcileResponse,
      lfLastDecision' =
        if (canSeal) {
          AuthorityReturnSealedBranch
        } else {
          AuthorityReturnFrozeBranch
        },
      lfLastAudit' = currentAudit,
      lfLastReconcileId' = None,
      lfLastSuppliedPayload' = None,
    }
  }

  action closeCleanFrozenBranch: bool = all {
    lfPhase == BranchFrozen,
    lfOverlayContent == lfBranchBase.content,
    lfDurableOverlayContent == lfOverlayContent,
    lfPolicy' = lfPolicy,
    lfPhase' = BranchAbsent,
    lfLocalAuthorityReachable' = lfLocalAuthorityReachable,
    lfCanonicalQuorumAvailable' = lfCanonicalQuorumAvailable,
    lfCanonicalHead' = lfCanonicalHead,
    lfBranchBase' = lfBranchBase,
    lfOverlayContent' = lfOverlayContent,
    lfDurableOverlayContent' = lfDurableOverlayContent,
    lfLocalSyncGeneration' = 0,
    lfSealedPayload' = None,
    lfRetainedContent' = None,
    lfPlan' = None,
    lfResultRecords' = lfResultRecords,
    lfConflictRecord' = lfConflictRecord,
    lfEffectCount' = lfEffectCount,
    lfLastResponse' = NoReconcileResponse,
    lfLastDecision' = CleanFrozenBranchClosed,
    lfLastAudit' = currentAudit,
    lfLastReconcileId' = None,
    lfLastSuppliedPayload' = None,
  }

  action loseLocalAuthorityRoute: bool = all {
    lfLocalAuthorityReachable,
    any {
      lfPhase == BranchFrozen,
      lfPhase == BranchSealed,
      lfPhase == BranchPlanned,
    },
    lfPolicy' = lfPolicy,
    lfPhase' = lfPhase,
    lfLocalAuthorityReachable' = false,
    lfCanonicalQuorumAvailable' = lfCanonicalQuorumAvailable,
    lfCanonicalHead' = lfCanonicalHead,
    lfBranchBase' = lfBranchBase,
    lfOverlayContent' = lfOverlayContent,
    lfDurableOverlayContent' = lfDurableOverlayContent,
    lfLocalSyncGeneration' = lfLocalSyncGeneration,
    lfSealedPayload' = lfSealedPayload,
    lfRetainedContent' = lfRetainedContent,
    lfPlan' = lfPlan,
    lfResultRecords' = lfResultRecords,
    lfConflictRecord' = lfConflictRecord,
    lfEffectCount' = lfEffectCount,
    lfLastResponse' = NoReconcileResponse,
    lfLastDecision' = LocalAuthorityRouteLost,
    lfLastAudit' = currentAudit,
    lfLastReconcileId' = None,
    lfLastSuppliedPayload' = None,
  }

  /// Global quorum availability is independent of the local route. Losing it
  /// blocks canonical progress but does not grant the branch any authority.
  action setCanonicalQuorum(available: bool): bool = all {
    available != lfCanonicalQuorumAvailable,
    lfPolicy' = lfPolicy,
    lfPhase' = lfPhase,
    lfLocalAuthorityReachable' = lfLocalAuthorityReachable,
    lfCanonicalQuorumAvailable' = available,
    lfCanonicalHead' = lfCanonicalHead,
    lfBranchBase' = lfBranchBase,
    lfOverlayContent' = lfOverlayContent,
    lfDurableOverlayContent' = lfDurableOverlayContent,
    lfLocalSyncGeneration' = lfLocalSyncGeneration,
    lfSealedPayload' = lfSealedPayload,
    lfRetainedContent' = lfRetainedContent,
    lfPlan' = lfPlan,
    lfResultRecords' = lfResultRecords,
    lfConflictRecord' = lfConflictRecord,
    lfEffectCount' = lfEffectCount,
    lfLastResponse' = lfLastResponse,
    lfLastDecision' =
      if (available) { CanonicalQuorumRestored } else { CanonicalQuorumLost },
    lfLastAudit' = currentAudit,
    lfLastReconcileId' = lfLastReconcileId,
    lfLastSuppliedPayload' = lfLastSuppliedPayload,
  }

  /// The bounded staging marker names the exact immutable symbolic content.
  /// Conflict publication is disabled until that marker exists. A concrete
  /// retained-content receipt and its storage quorum are outside this slice.
  action retainSealedContent: bool = {
    val payload = unwrapOr(lfSealedPayload, defaultPayload)
    all {
      lfPhase == BranchSealed,
      lfSealedPayload != None,
      lfRetainedContent' = Some(payload.content),
      lfPolicy' = lfPolicy,
      lfPhase' = lfPhase,
      lfLocalAuthorityReachable' = lfLocalAuthorityReachable,
      lfCanonicalQuorumAvailable' = lfCanonicalQuorumAvailable,
      lfCanonicalHead' = lfCanonicalHead,
      lfBranchBase' = lfBranchBase,
      lfOverlayContent' = lfOverlayContent,
      lfDurableOverlayContent' = lfDurableOverlayContent,
      lfLocalSyncGeneration' = lfLocalSyncGeneration,
      lfSealedPayload' = lfSealedPayload,
      lfPlan' = lfPlan,
      lfResultRecords' = lfResultRecords,
      lfConflictRecord' = lfConflictRecord,
      lfEffectCount' = lfEffectCount,
      lfLastResponse' = NoReconcileResponse,
      lfLastDecision' = SealedContentRetained,
      lfLastAudit' = currentAudit,
      lfLastReconcileId' = None,
      lfLastSuppliedPayload' = None,
    }
  }

  action restoreLocalAuthority: bool = all {
    any {
      lfPhase == BranchFrozen,
      lfPhase == BranchSealed,
      lfPhase == BranchPlanned,
    },
    not(lfLocalAuthorityReachable),
    lfLocalAuthorityReachable' = true,
    lfPolicy' = lfPolicy,
    lfPhase' = lfPhase,
    lfCanonicalQuorumAvailable' = lfCanonicalQuorumAvailable,
    lfCanonicalHead' = lfCanonicalHead,
    lfBranchBase' = lfBranchBase,
    lfOverlayContent' = lfOverlayContent,
    lfDurableOverlayContent' = lfDurableOverlayContent,
    lfLocalSyncGeneration' = lfLocalSyncGeneration,
    lfSealedPayload' = lfSealedPayload,
    lfRetainedContent' = lfRetainedContent,
    lfPlan' = lfPlan,
    lfResultRecords' = lfResultRecords,
    lfConflictRecord' = lfConflictRecord,
    lfEffectCount' = lfEffectCount,
    lfLastResponse' = NoReconcileResponse,
    lfLastDecision' = LocalAuthorityRestored,
    lfLastAudit' = currentAudit,
    lfLastReconcileId' = None,
    lfLastSuppliedPayload' = None,
  }

  /// This represents legal progress by the remote quorum side. It may happen
  /// while the branch is open or after a local plan was produced.
  action canonicalConcurrentWrite(content: FileContent): bool = all {
    lfCanonicalQuorumAvailable,
    ConcurrentContents.contains(content),
    lfCanonicalHead.revision == FileBase,
    lfPhase != BranchTerminal,
    lfCanonicalHead' = {
      revision: FileConcurrent,
      content: content,
    },
    lfPolicy' = lfPolicy,
    lfPhase' = lfPhase,
    lfLocalAuthorityReachable' = lfLocalAuthorityReachable,
    lfCanonicalQuorumAvailable' = lfCanonicalQuorumAvailable,
    lfBranchBase' = lfBranchBase,
    lfOverlayContent' = lfOverlayContent,
    lfDurableOverlayContent' = lfDurableOverlayContent,
    lfLocalSyncGeneration' = lfLocalSyncGeneration,
    lfSealedPayload' = lfSealedPayload,
    lfRetainedContent' = lfRetainedContent,
    lfPlan' = lfPlan,
    lfResultRecords' = lfResultRecords,
    lfConflictRecord' = lfConflictRecord,
    lfEffectCount' = lfEffectCount + 1,
    lfLastResponse' = NoReconcileResponse,
    lfLastDecision' = CanonicalConcurrentWrite,
    lfLastAudit' = currentAudit,
    lfLastReconcileId' = None,
    lfLastSuppliedPayload' = None,
  }

  action planReconciliation(requestId: ReconcileId): bool = {
    val payload = unwrapOr(lfSealedPayload, defaultPayload)
    val plan = {
      requestId: requestId,
      payload: payload,
      observedHead: lfCanonicalHead,
      disposition: classifyPlan(payload, lfCanonicalHead),
    }
    all {
      lfPhase == BranchSealed,
      lfLocalAuthorityReachable,
      lfCanonicalQuorumAvailable,
      lfSealedPayload != None,
      lfRetainedContent == Some(payload.content),
      lfResultRecords.get(requestId) == None,
      lfPhase' = BranchPlanned,
      lfPlan' = Some(plan),
      lfPolicy' = lfPolicy,
      lfLocalAuthorityReachable' = lfLocalAuthorityReachable,
      lfCanonicalQuorumAvailable' = lfCanonicalQuorumAvailable,
      lfCanonicalHead' = lfCanonicalHead,
      lfBranchBase' = lfBranchBase,
      lfOverlayContent' = lfOverlayContent,
      lfDurableOverlayContent' = lfDurableOverlayContent,
      lfLocalSyncGeneration' = lfLocalSyncGeneration,
      lfSealedPayload' = lfSealedPayload,
      lfRetainedContent' = lfRetainedContent,
      lfResultRecords' = lfResultRecords,
      lfConflictRecord' = lfConflictRecord,
      lfEffectCount' = lfEffectCount,
      lfLastResponse' = ReconcilePlanReady(requestId),
      lfLastDecision' = ReconcilePlanCreated,
      lfLastAudit' = currentAudit,
      lfLastReconcileId' = Some(requestId),
      lfLastSuppliedPayload' = Some(payload),
    }
  }

  action rejectStalePlan: bool = {
    val plan = unwrapOr(lfPlan, defaultPlan)
    all {
      lfPhase == BranchPlanned,
      lfPlan != None,
      lfCanonicalHead != plan.observedHead,
      lfPhase' = BranchSealed,
      lfPlan' = None,
      lfPolicy' = lfPolicy,
      lfLocalAuthorityReachable' = lfLocalAuthorityReachable,
      lfCanonicalQuorumAvailable' = lfCanonicalQuorumAvailable,
      lfCanonicalHead' = lfCanonicalHead,
      lfBranchBase' = lfBranchBase,
      lfOverlayContent' = lfOverlayContent,
      lfDurableOverlayContent' = lfDurableOverlayContent,
      lfLocalSyncGeneration' = lfLocalSyncGeneration,
      lfSealedPayload' = lfSealedPayload,
      lfRetainedContent' = lfRetainedContent,
      lfResultRecords' = lfResultRecords,
      lfConflictRecord' = lfConflictRecord,
      lfEffectCount' = lfEffectCount,
      lfLastResponse' = ReconcilePlanStale(plan.requestId),
      lfLastDecision' = StaleReconcilePlanRejected,
      lfLastAudit' = currentAudit,
      lfLastReconcileId' = Some(plan.requestId),
      lfLastSuppliedPayload' = Some(plan.payload),
    }
  }

  /// The composed gateway owns publication progress between branch planning
  /// and commit. These steps must not terminalize the branch or mutate either
  /// its canonical head or durable result maps.
  action awaitReconcileGateway: bool = all {
    lfPhase == BranchPlanned,
    lfPolicy' = lfPolicy,
    lfPhase' = lfPhase,
    lfLocalAuthorityReachable' = lfLocalAuthorityReachable,
    lfCanonicalQuorumAvailable' = lfCanonicalQuorumAvailable,
    lfCanonicalHead' = lfCanonicalHead,
    lfBranchBase' = lfBranchBase,
    lfOverlayContent' = lfOverlayContent,
    lfDurableOverlayContent' = lfDurableOverlayContent,
    lfLocalSyncGeneration' = lfLocalSyncGeneration,
    lfSealedPayload' = lfSealedPayload,
    lfRetainedContent' = lfRetainedContent,
    lfPlan' = lfPlan,
    lfResultRecords' = lfResultRecords,
    lfConflictRecord' = lfConflictRecord,
    lfEffectCount' = lfEffectCount,
    lfLastResponse' = lfLastResponse,
    lfLastDecision' = ReconcileGatewayProgressed,
    lfLastAudit' = currentAudit,
    lfLastReconcileId' = lfLastReconcileId,
    lfLastSuppliedPayload' = lfLastSuppliedPayload,
  }

  /// The only local-first transition that changes the canonical projection.
  /// It is one fresh, quorum-authorized canonical publication.
  action applyFreshPlan(deliverReply: bool): bool = {
    val plan = unwrapOr(lfPlan, defaultPlan)
    val publishedHead = reconciledHead(plan.payload)
    val result = AppliedResult(publishedHead)
    val record = { payload: plan.payload, result: result }
    all {
      lfPhase == BranchPlanned,
      lfPlan != None,
      lfLocalAuthorityReachable,
      lfCanonicalQuorumAvailable,
      lfCanonicalHead == plan.observedHead,
      plan.disposition == PlanApply,
      classifyPlan(plan.payload, lfCanonicalHead) == PlanApply,
      lfRetainedContent == Some(plan.payload.content),
      lfResultRecords.get(plan.requestId) == None,
      lfCanonicalHead' = publishedHead,
      lfResultRecords' = lfResultRecords.set(plan.requestId, Some(record)),
      lfPhase' = BranchTerminal,
      lfPolicy' = lfPolicy,
      lfLocalAuthorityReachable' = lfLocalAuthorityReachable,
      lfCanonicalQuorumAvailable' = lfCanonicalQuorumAvailable,
      lfBranchBase' = lfBranchBase,
      lfOverlayContent' = lfOverlayContent,
      lfDurableOverlayContent' = lfDurableOverlayContent,
      lfLocalSyncGeneration' = lfLocalSyncGeneration,
      lfSealedPayload' = lfSealedPayload,
      lfRetainedContent' = lfRetainedContent,
      lfPlan' = lfPlan,
      lfConflictRecord' = lfConflictRecord,
      lfEffectCount' = lfEffectCount + 1,
      lfLastResponse' =
        if (deliverReply) {
          ReconcileResultReturned({
            requestId: plan.requestId,
            result: result,
          })
        } else {
          ReconcileOutcomeUnknown(plan.requestId)
        },
      lfLastDecision' = ReconcileApplied,
      lfLastAudit' = currentAudit,
      lfLastReconcileId' = Some(plan.requestId),
      lfLastSuppliedPayload' = Some(plan.payload),
    }
  }

  action convergeFreshPlan(deliverReply: bool): bool = {
    val plan = unwrapOr(lfPlan, defaultPlan)
    val result = ConvergedResult(lfCanonicalHead)
    val record = { payload: plan.payload, result: result }
    all {
      lfPhase == BranchPlanned,
      lfPlan != None,
      lfLocalAuthorityReachable,
      lfCanonicalQuorumAvailable,
      lfCanonicalHead == plan.observedHead,
      plan.disposition == PlanConverged,
      classifyPlan(plan.payload, lfCanonicalHead) == PlanConverged,
      lfRetainedContent == Some(plan.payload.content),
      lfResultRecords.get(plan.requestId) == None,
      lfResultRecords' = lfResultRecords.set(plan.requestId, Some(record)),
      lfPhase' = BranchTerminal,
      lfPolicy' = lfPolicy,
      lfLocalAuthorityReachable' = lfLocalAuthorityReachable,
      lfCanonicalQuorumAvailable' = lfCanonicalQuorumAvailable,
      lfCanonicalHead' = lfCanonicalHead,
      lfBranchBase' = lfBranchBase,
      lfOverlayContent' = lfOverlayContent,
      lfDurableOverlayContent' = lfDurableOverlayContent,
      lfLocalSyncGeneration' = lfLocalSyncGeneration,
      lfSealedPayload' = lfSealedPayload,
      lfRetainedContent' = lfRetainedContent,
      lfPlan' = lfPlan,
      lfConflictRecord' = lfConflictRecord,
      lfEffectCount' = lfEffectCount + 1,
      lfLastResponse' =
        if (deliverReply) {
          ReconcileResultReturned({
            requestId: plan.requestId,
            result: result,
          })
        } else {
          ReconcileOutcomeUnknown(plan.requestId)
        },
      lfLastDecision' = ReconcileConverged,
      lfLastAudit' = currentAudit,
      lfLastReconcileId' = Some(plan.requestId),
      lfLastSuppliedPayload' = Some(plan.payload),
    }
  }

  action conflictFreshPlan(deliverReply: bool): bool = {
    val plan = unwrapOr(lfPlan, defaultPlan)
    val result = ConflictResult(FileConflict1)
    val record = { payload: plan.payload, result: result }
    val conflict = {
      id: FileConflict1,
      requestId: plan.requestId,
      payload: plan.payload,
      canonicalWinner: lfCanonicalHead,
      retainedContent: plan.payload.content,
    }
    all {
      lfPhase == BranchPlanned,
      lfPlan != None,
      lfLocalAuthorityReachable,
      lfCanonicalQuorumAvailable,
      lfCanonicalHead == plan.observedHead,
      plan.disposition == PlanConflict,
      classifyPlan(plan.payload, lfCanonicalHead) == PlanConflict,
      lfRetainedContent == Some(plan.payload.content),
      lfResultRecords.get(plan.requestId) == None,
      lfConflictRecord == None,
      lfResultRecords' = lfResultRecords.set(plan.requestId, Some(record)),
      lfConflictRecord' = Some(conflict),
      lfPhase' = BranchTerminal,
      lfPolicy' = lfPolicy,
      lfLocalAuthorityReachable' = lfLocalAuthorityReachable,
      lfCanonicalQuorumAvailable' = lfCanonicalQuorumAvailable,
      lfCanonicalHead' = lfCanonicalHead,
      lfBranchBase' = lfBranchBase,
      lfOverlayContent' = lfOverlayContent,
      lfDurableOverlayContent' = lfDurableOverlayContent,
      lfLocalSyncGeneration' = lfLocalSyncGeneration,
      lfSealedPayload' = lfSealedPayload,
      lfRetainedContent' = lfRetainedContent,
      lfPlan' = lfPlan,
      lfEffectCount' = lfEffectCount + 1,
      lfLastResponse' =
        if (deliverReply) {
          ReconcileResultReturned({
            requestId: plan.requestId,
            result: result,
          })
        } else {
          ReconcileOutcomeUnknown(plan.requestId)
        },
      lfLastDecision' = ReconcileConflictMaterialized,
      lfLastAudit' = currentAudit,
      lfLastReconcileId' = Some(plan.requestId),
      lfLastSuppliedPayload' = Some(plan.payload),
    }
  }

  /// Exact retry replays the durable result, including a result whose original
  /// reply was lost. It does not repeat canonical or sidecar effects.
  action retryExactResult(
    requestId: ReconcileId,
    supplied: BranchPayload
  ): bool = {
    val record = unwrapOr(
      lfResultRecords.get(requestId),
      defaultResultRecord
    )
    all {
      lfPhase == BranchTerminal,
      lfResultRecords.get(requestId) != None,
      supplied == record.payload,
      lfPolicy' = lfPolicy,
      lfPhase' = lfPhase,
      lfLocalAuthorityReachable' = lfLocalAuthorityReachable,
      lfCanonicalQuorumAvailable' = lfCanonicalQuorumAvailable,
      lfCanonicalHead' = lfCanonicalHead,
      lfBranchBase' = lfBranchBase,
      lfOverlayContent' = lfOverlayContent,
      lfDurableOverlayContent' = lfDurableOverlayContent,
      lfLocalSyncGeneration' = lfLocalSyncGeneration,
      lfSealedPayload' = lfSealedPayload,
      lfRetainedContent' = lfRetainedContent,
      lfPlan' = lfPlan,
      lfResultRecords' = lfResultRecords,
      lfConflictRecord' = lfConflictRecord,
      lfEffectCount' = lfEffectCount,
      lfLastResponse' = ReconcileResultReturned({
        requestId: requestId,
        result: record.result,
      }),
      lfLastDecision' = ExactReconcileRetryReplayed,
      lfLastAudit' = currentAudit,
      lfLastReconcileId' = Some(requestId),
      lfLastSuppliedPayload' = Some(supplied),
    }
  }

  action rejectChangedRetry(
    requestId: ReconcileId,
    supplied: BranchPayload
  ): bool = {
    val record = unwrapOr(
      lfResultRecords.get(requestId),
      defaultResultRecord
    )
    all {
      lfPhase == BranchTerminal,
      lfResultRecords.get(requestId) != None,
      supplied != record.payload,
      lfPolicy' = lfPolicy,
      lfPhase' = lfPhase,
      lfLocalAuthorityReachable' = lfLocalAuthorityReachable,
      lfCanonicalQuorumAvailable' = lfCanonicalQuorumAvailable,
      lfCanonicalHead' = lfCanonicalHead,
      lfBranchBase' = lfBranchBase,
      lfOverlayContent' = lfOverlayContent,
      lfDurableOverlayContent' = lfDurableOverlayContent,
      lfLocalSyncGeneration' = lfLocalSyncGeneration,
      lfSealedPayload' = lfSealedPayload,
      lfRetainedContent' = lfRetainedContent,
      lfPlan' = lfPlan,
      lfResultRecords' = lfResultRecords,
      lfConflictRecord' = lfConflictRecord,
      lfEffectCount' = lfEffectCount,
      lfLastResponse' = ReconcilePayloadMismatch(requestId),
      lfLastDecision' = ChangedReconcileRetryRejected,
      lfLastAudit' = currentAudit,
      lfLastReconcileId' = Some(requestId),
      lfLastSuppliedPayload' = Some(supplied),
    }
  }

  /// A process restart drops only the transient response. The terminal result
  /// record and any conflict sidecar are modeled as durable local metadata and
  /// remain available for an exact retry after recovery.
  action restartAfterReconciliation: bool = all {
    lfPhase == BranchTerminal,
    recordedResultCount(lfResultRecords) > 0,
    lfPolicy' = lfPolicy,
    lfPhase' = lfPhase,
    lfLocalAuthorityReachable' = lfLocalAuthorityReachable,
    lfCanonicalQuorumAvailable' = lfCanonicalQuorumAvailable,
    lfCanonicalHead' = lfCanonicalHead,
    lfBranchBase' = lfBranchBase,
    lfOverlayContent' = lfOverlayContent,
    lfDurableOverlayContent' = lfDurableOverlayContent,
    lfLocalSyncGeneration' = lfLocalSyncGeneration,
    lfSealedPayload' = lfSealedPayload,
    lfRetainedContent' = lfRetainedContent,
    lfPlan' = lfPlan,
    lfResultRecords' = lfResultRecords,
    lfConflictRecord' = lfConflictRecord,
    lfEffectCount' = lfEffectCount,
    lfLastResponse' = NoReconcileResponse,
    lfLastDecision' = ReconcileProcessRestarted,
    lfLastAudit' = currentAudit,
    lfLastReconcileId' = None,
    lfLastSuppliedPayload' = None,
  }

  action stutterTerminal: bool = all {
    lfPhase == BranchTerminal,
    lfPolicy' = lfPolicy,
    lfPhase' = lfPhase,
    lfLocalAuthorityReachable' = lfLocalAuthorityReachable,
    lfCanonicalQuorumAvailable' = lfCanonicalQuorumAvailable,
    lfCanonicalHead' = lfCanonicalHead,
    lfBranchBase' = lfBranchBase,
    lfOverlayContent' = lfOverlayContent,
    lfDurableOverlayContent' = lfDurableOverlayContent,
    lfLocalSyncGeneration' = lfLocalSyncGeneration,
    lfSealedPayload' = lfSealedPayload,
    lfRetainedContent' = lfRetainedContent,
    lfPlan' = lfPlan,
    lfResultRecords' = lfResultRecords,
    lfConflictRecord' = lfConflictRecord,
    lfEffectCount' = lfEffectCount,
    lfLastResponse' = lfLastResponse,
    lfLastDecision' = FileBranchTerminalStutter,
    lfLastAudit' = currentAudit,
    lfLastReconcileId' = lfLastReconcileId,
    lfLastSuppliedPayload' = lfLastSuppliedPayload,
  }

  action step = any {
    beginLocalBranch,
    localWrite,
    localTruncate,
    localFsync,
    crashLocalProcess,
    sealLocalBranch,
    sealBranchOnAuthorityReturn,
    closeCleanFrozenBranch,
    loseLocalAuthorityRoute,
    setCanonicalQuorum(false),
    setCanonicalQuorum(true),
    retainSealedContent,
    restoreLocalAuthority,
    {
      nondet content = oneOf(ConcurrentContents)
      canonicalConcurrentWrite(content)
    },
    {
      nondet requestId = oneOf(ReconcileIds)
      planReconciliation(requestId)
    },
    rejectStalePlan,
    {
      nondet deliverReply = oneOf(Set(true, false))
      applyFreshPlan(deliverReply)
    },
    {
      nondet deliverReply = oneOf(Set(true, false))
      convergeFreshPlan(deliverReply)
    },
    {
      nondet deliverReply = oneOf(Set(true, false))
      conflictFreshPlan(deliverReply)
    },
    {
      nondet requestId = oneOf(ReconcileIds)
      nondet base = oneOf(RetryBaseHeads)
      nondet content = oneOf(FileContents)
      nondet generation = oneOf(RetrySyncGenerations)
      retryExactResult(requestId, {
        baseHead: base,
        content: content,
        localSyncGeneration: generation,
      })
    },
    {
      nondet requestId = oneOf(ReconcileIds)
      nondet base = oneOf(RetryBaseHeads)
      nondet content = oneOf(FileContents)
      nondet generation = oneOf(RetrySyncGenerations)
      rejectChangedRetry(requestId, {
        baseHead: base,
        content: content,
        localSyncGeneration: generation,
      })
    },
    restartAfterReconciliation,
    stutterTerminal,
  }
}
