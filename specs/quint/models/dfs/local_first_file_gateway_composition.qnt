/// Composition of the bounded local-first file branch with the typed
/// reconciliation publication gateway.
///
/// The branch may plan and wait, but none of its three terminal actions is
/// exposed independently. A terminal branch result is created only in the same
/// transition that commits the matching gateway journal envelope.
module dfs_local_first_file_gateway_composition {
  import common.* from "../../lib/common"
  import dfs_local_first_file_branch from "local_first_file_branch"
  import dfs_local_first_reconcile_gateway from "local_first_reconcile_gateway"
  import dfs_local_first_reconcile_permit from "../../contracts/dfs/local_first_reconcile_permit"

  pure def fileGatewayOutcome(
    disposition: dfs_local_first_file_branch::PlanDisposition
  ): dfs_local_first_reconcile_permit::ReconcileOutcome =
    match disposition {
      | PlanApply =>
          dfs_local_first_reconcile_permit::PublishApply
      | PlanConverged =>
          dfs_local_first_reconcile_permit::PublishConverged
      | PlanConflict =>
          dfs_local_first_reconcile_permit::PublishConflict
    }

  /// The bridge admits two exact one-sync manifests. The generic file model
  /// remains wider, but no other payload can enter this composition.
  pure def fileGatewayPayloadAdmitted(
    payload: dfs_local_first_file_branch::BranchPayload
  ): bool = all {
    payload.baseHead == dfs_local_first_file_branch::baseHead,
    payload.localSyncGeneration == 1,
    any {
      payload.content == dfs_local_first_file_branch::LocalWriteBytes,
      payload.content == dfs_local_first_file_branch::LocalTruncatedBytes,
    },
  }

  pure def fileGatewayPayloadDigest(
    payload: dfs_local_first_file_branch::BranchPayload
  ): dfs_local_first_reconcile_permit::ReconcilePayloadDigest =
    if (payload.content == dfs_local_first_file_branch::LocalWriteBytes) {
      dfs_local_first_reconcile_permit::PayloadA
    } else {
      dfs_local_first_reconcile_permit::PayloadB
    }

  pure def changedFileGatewayPayload(
    payload: dfs_local_first_file_branch::BranchPayload
  ): dfs_local_first_file_branch::BranchPayload = {
    baseHead: payload.baseHead,
    content:
      if (payload.content
        == dfs_local_first_file_branch::LocalWriteBytes) {
        dfs_local_first_file_branch::LocalTruncatedBytes
      } else {
        dfs_local_first_file_branch::LocalWriteBytes
      },
    localSyncGeneration: payload.localSyncGeneration,
  }

  pure def fileHeadObjectRevision(
    fileHead: dfs_local_first_file_branch::FileHead
  ): int =
    match fileHead.revision {
      | FileBase => 0
      | FileConcurrent => 1
      | FileReconciled => 1
    }

  def composedFilePlan: dfs_local_first_file_branch::ReconcilePlan =
    unwrapOr(
      dfs_local_first_file_branch::lfPlan,
      dfs_local_first_file_branch::defaultPlan
    )

  def composedFilePayload: dfs_local_first_file_branch::BranchPayload =
    unwrapOr(
      dfs_local_first_file_branch::lfSealedPayload,
      dfs_local_first_file_branch::defaultPayload
    )

  action init = all {
    dfs_local_first_file_branch::init,
    dfs_local_first_reconcile_gateway::init,
  }

  action beginComposedFileBranch: bool = all {
    dfs_local_first_file_branch::lfCanonicalHead
      == dfs_local_first_file_branch::baseHead,
    dfs_local_first_file_branch::beginLocalBranch,
    dfs_local_first_reconcile_gateway::stutterGateway,
  }

  action writeComposedFileBranch: bool = all {
    dfs_local_first_file_branch::lfLocalSyncGeneration == 0,
    dfs_local_first_file_branch::localWrite,
    dfs_local_first_reconcile_gateway::stutterGateway,
  }

  action truncateComposedFileBranch: bool = all {
    dfs_local_first_file_branch::lfLocalSyncGeneration == 0,
    dfs_local_first_file_branch::localTruncate,
    dfs_local_first_reconcile_gateway::stutterGateway,
  }

  action syncComposedFileBranch: bool = all {
    dfs_local_first_file_branch::lfLocalSyncGeneration == 0,
    dfs_local_first_file_branch::lfOverlayContent
      != dfs_local_first_file_branch::lfBranchBase.content,
    dfs_local_first_file_branch::localFsync,
    dfs_local_first_reconcile_gateway::stutterGateway,
  }

  action sealComposedFileBranch: bool = all {
    dfs_local_first_file_branch::sealLocalBranch,
    dfs_local_first_reconcile_gateway::stutterGateway,
  }

  action retainComposedFileContent: bool = all {
    dfs_local_first_file_branch::retainSealedContent,
    dfs_local_first_reconcile_gateway::stutterGateway,
  }

  action restoreComposedFileAuthority: bool = all {
    dfs_local_first_file_branch::restoreLocalAuthority,
    dfs_local_first_reconcile_gateway::stutterGateway,
  }

  action advanceComposedFileCanonical(
    content: dfs_local_first_file_branch::FileContent
  ): bool = all {
    dfs_local_first_file_branch::lfPhase
      != dfs_local_first_file_branch::BranchAbsent,
    dfs_local_first_file_branch::canonicalConcurrentWrite(content),
    dfs_local_first_reconcile_gateway::advanceStrongCanonicalHead,
  }

  action planComposedFileGateway: bool = {
    val payload = composedFilePayload
    val disposition = dfs_local_first_file_branch::classifyPlan(
      payload,
      dfs_local_first_file_branch::lfCanonicalHead
    )
    all {
      fileGatewayPayloadAdmitted(payload),
      dfs_local_first_file_branch::planReconciliation(
        dfs_local_first_file_branch::Reconcile1
      ),
      dfs_local_first_reconcile_gateway::planReconcilePublication(
        fileGatewayPayloadDigest(payload),
        fileGatewayOutcome(disposition)
      ),
    }
  }

  /// Re-create only the volatile gateway plan after a precommit crash. The
  /// branch's immutable plan remains the source of the outcome and payload.
  action resumeComposedFileGatewayPlan: bool = {
    val plan = composedFilePlan
    all {
      dfs_local_first_file_branch::lfPhase
        == dfs_local_first_file_branch::BranchPlanned,
      dfs_local_first_reconcile_gateway::gatewayPhase
        == dfs_local_first_reconcile_gateway::GatewayIdle,
      dfs_local_first_file_branch::lfCanonicalHead == plan.observedHead,
      dfs_local_first_file_branch::awaitReconcileGateway,
      dfs_local_first_reconcile_gateway::planReconcilePublication(
        fileGatewayPayloadDigest(plan.payload),
        fileGatewayOutcome(plan.disposition)
      ),
    }
  }

  action collectComposedFileGatewayEvidence: bool = all {
    dfs_local_first_file_branch::awaitReconcileGateway,
    dfs_local_first_reconcile_gateway::collectAllGatewayEvidence,
  }

  action buildComposedFileGatewayPermit: bool = all {
    dfs_local_first_file_branch::awaitReconcileGateway,
    dfs_local_first_reconcile_gateway::buildReconcilePermit,
  }

  action appendComposedFileGatewayEntry: bool = all {
    dfs_local_first_file_branch::awaitReconcileGateway,
    dfs_local_first_reconcile_gateway::appendReconcileEntry,
  }

  /// This is the only composed file terminal transition.
  action commitComposedFileTerminal(deliverReply: bool): bool = {
    val plan = composedFilePlan
    all {
      match plan.disposition {
        | PlanApply =>
            dfs_local_first_file_branch::applyFreshPlan(deliverReply)
        | PlanConverged =>
            dfs_local_first_file_branch::convergeFreshPlan(deliverReply)
        | PlanConflict =>
            dfs_local_first_file_branch::conflictFreshPlan(deliverReply)
      },
      dfs_local_first_reconcile_gateway::commitReconcileEntry(deliverReply),
    }
  }

  action rejectComposedFileStalePlan: bool = all {
    dfs_local_first_file_branch::rejectStalePlan,
    dfs_local_first_reconcile_gateway::rejectStaleGatewayPublication,
  }

  /// A precommit crash may discard the volatile gateway plan before an
  /// unrelated strong write makes the retained branch plan stale.
  action rejectComposedFileStalePlanWithoutGateway: bool = all {
    dfs_local_first_reconcile_gateway::gatewayPhase
      == dfs_local_first_reconcile_gateway::GatewayIdle,
    dfs_local_first_file_branch::rejectStalePlan,
    dfs_local_first_reconcile_gateway::stutterGateway,
  }

  action crashComposedFileGatewayBeforeCommit: bool = all {
    dfs_local_first_file_branch::awaitReconcileGateway,
    dfs_local_first_reconcile_gateway::crashGatewayBeforeCommit,
  }

  action restartComposedFileGatewayBeforeCommit: bool = all {
    dfs_local_first_file_branch::awaitReconcileGateway,
    dfs_local_first_reconcile_gateway::restartGatewayProcess,
  }

  action crashComposedFileGatewayAfterCommit: bool = all {
    dfs_local_first_file_branch::stutterTerminal,
    dfs_local_first_reconcile_gateway::crashGatewayAfterCommit,
  }

  action restartComposedFileGatewayAfterCommit: bool = all {
    dfs_local_first_file_branch::stutterTerminal,
    dfs_local_first_reconcile_gateway::restartGatewayProcess,
  }

  action materializeComposedFileGateway: bool = all {
    dfs_local_first_file_branch::stutterTerminal,
    dfs_local_first_reconcile_gateway::materializeCommittedReconcileEntry,
  }

  action retryExactComposedFileResult: bool = {
    val payload = composedFilePayload
    all {
      dfs_local_first_file_branch::retryExactResult(
        dfs_local_first_file_branch::Reconcile1,
        payload
      ),
      dfs_local_first_reconcile_gateway::retryCommittedResult(
        fileGatewayPayloadDigest(payload)
      ),
    }
  }

  action rejectChangedComposedFileResult: bool = {
    val payload = composedFilePayload
    val changedPayload = changedFileGatewayPayload(payload)
    all {
      dfs_local_first_file_branch::rejectChangedRetry(
        dfs_local_first_file_branch::Reconcile1,
        changedPayload
      ),
      dfs_local_first_reconcile_gateway::retryCommittedResult(
        fileGatewayPayloadDigest(changedPayload)
      ),
    }
  }

  action stutterComposedFileTerminal: bool = all {
    dfs_local_first_file_branch::stutterTerminal,
    dfs_local_first_reconcile_gateway::stutterGateway,
  }

  action step = any {
    beginComposedFileBranch,
    writeComposedFileBranch,
    truncateComposedFileBranch,
    syncComposedFileBranch,
    sealComposedFileBranch,
    retainComposedFileContent,
    restoreComposedFileAuthority,
    {
      nondet content = oneOf(
        dfs_local_first_file_branch::ConcurrentContents
      )
      advanceComposedFileCanonical(content)
    },
    planComposedFileGateway,
    resumeComposedFileGatewayPlan,
    collectComposedFileGatewayEvidence,
    buildComposedFileGatewayPermit,
    appendComposedFileGatewayEntry,
    {
      nondet deliverReply = oneOf(Set(true, false))
      commitComposedFileTerminal(deliverReply)
    },
    rejectComposedFileStalePlan,
    rejectComposedFileStalePlanWithoutGateway,
    crashComposedFileGatewayBeforeCommit,
    restartComposedFileGatewayBeforeCommit,
    crashComposedFileGatewayAfterCommit,
    restartComposedFileGatewayAfterCommit,
    materializeComposedFileGateway,
    retryExactComposedFileResult,
    rejectChangedComposedFileResult,
    stutterComposedFileTerminal,
  }
}
