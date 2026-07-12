/// Deterministic file composition trace across both gateway crash boundaries.
module dfs_local_first_file_gateway_composition_scenario {
  import common.* from "../../lib/common"
  import dfs_local_first_file_branch from "../../models/dfs/local_first_file_branch"
  import dfs_local_first_reconcile_gateway from "../../models/dfs/local_first_reconcile_gateway"
  import dfs_local_first_file_gateway_composition from "../../models/dfs/local_first_file_gateway_composition"
  import dfs_local_first_file_gateway_composition_safety from "../../checks/dfs/local_first_file_gateway_composition_safety"

  type Phase =
    | FileCompositionStart
    | FileBranchStarted
    | FileWriteStaged
    | FileWriteSynced
    | FileBranchSealed
    | FileContentRetained
    | FileAuthorityRestored
    | FilePlanCreated
    | FileEvidenceCollected
    | FilePermitBuilt
    | FileEntryAppended
    | FilePrecommitCrashed
    | FilePrecommitRestarted
    | FilePlanResumed
    | FileEvidenceRecollected
    | FilePermitRebuilt
    | FileEntryReappended
    | FileCommittedUnknown
    | FilePostcommitCrashed
    | FilePostcommitRestarted
    | FileMaterialized
    | FileExactRetry
    | FileChangedRetry

  var phase: Phase

  action init = all {
    dfs_local_first_file_gateway_composition::init,
    phase' = FileCompositionStart,
  }

  action step =
    if (phase == FileCompositionStart) {
      all {
        dfs_local_first_file_gateway_composition::beginComposedFileBranch,
        phase' = FileBranchStarted,
      }
    } else if (phase == FileBranchStarted) {
      all {
        dfs_local_first_file_gateway_composition::writeComposedFileBranch,
        phase' = FileWriteStaged,
      }
    } else if (phase == FileWriteStaged) {
      all {
        dfs_local_first_file_gateway_composition::syncComposedFileBranch,
        phase' = FileWriteSynced,
      }
    } else if (phase == FileWriteSynced) {
      all {
        dfs_local_first_file_gateway_composition::sealComposedFileBranch,
        phase' = FileBranchSealed,
      }
    } else if (phase == FileBranchSealed) {
      all {
        dfs_local_first_file_gateway_composition::retainComposedFileContent,
        phase' = FileContentRetained,
      }
    } else if (phase == FileContentRetained) {
      all {
        dfs_local_first_file_gateway_composition::restoreComposedFileAuthority,
        phase' = FileAuthorityRestored,
      }
    } else if (phase == FileAuthorityRestored) {
      all {
        dfs_local_first_file_gateway_composition::planComposedFileGateway,
        phase' = FilePlanCreated,
      }
    } else if (phase == FilePlanCreated) {
      all {
        dfs_local_first_file_gateway_composition::collectComposedFileGatewayEvidence,
        phase' = FileEvidenceCollected,
      }
    } else if (phase == FileEvidenceCollected) {
      all {
        dfs_local_first_file_gateway_composition::buildComposedFileGatewayPermit,
        phase' = FilePermitBuilt,
      }
    } else if (phase == FilePermitBuilt) {
      all {
        dfs_local_first_file_gateway_composition::appendComposedFileGatewayEntry,
        phase' = FileEntryAppended,
      }
    } else if (phase == FileEntryAppended) {
      all {
        dfs_local_first_file_gateway_composition::crashComposedFileGatewayBeforeCommit,
        phase' = FilePrecommitCrashed,
      }
    } else if (phase == FilePrecommitCrashed) {
      all {
        dfs_local_first_file_gateway_composition::restartComposedFileGatewayBeforeCommit,
        phase' = FilePrecommitRestarted,
      }
    } else if (phase == FilePrecommitRestarted) {
      all {
        dfs_local_first_file_gateway_composition::resumeComposedFileGatewayPlan,
        phase' = FilePlanResumed,
      }
    } else if (phase == FilePlanResumed) {
      all {
        dfs_local_first_file_gateway_composition::collectComposedFileGatewayEvidence,
        phase' = FileEvidenceRecollected,
      }
    } else if (phase == FileEvidenceRecollected) {
      all {
        dfs_local_first_file_gateway_composition::buildComposedFileGatewayPermit,
        phase' = FilePermitRebuilt,
      }
    } else if (phase == FilePermitRebuilt) {
      all {
        dfs_local_first_file_gateway_composition::appendComposedFileGatewayEntry,
        phase' = FileEntryReappended,
      }
    } else if (phase == FileEntryReappended) {
      all {
        dfs_local_first_file_gateway_composition::commitComposedFileTerminal(false),
        phase' = FileCommittedUnknown,
      }
    } else if (phase == FileCommittedUnknown) {
      all {
        dfs_local_first_file_gateway_composition::crashComposedFileGatewayAfterCommit,
        phase' = FilePostcommitCrashed,
      }
    } else if (phase == FilePostcommitCrashed) {
      all {
        dfs_local_first_file_gateway_composition::restartComposedFileGatewayAfterCommit,
        phase' = FilePostcommitRestarted,
      }
    } else if (phase == FilePostcommitRestarted) {
      all {
        dfs_local_first_file_gateway_composition::materializeComposedFileGateway,
        phase' = FileMaterialized,
      }
    } else if (phase == FileMaterialized) {
      all {
        dfs_local_first_file_gateway_composition::retryExactComposedFileResult,
        phase' = FileExactRetry,
      }
    } else if (phase == FileExactRetry) {
      all {
        dfs_local_first_file_gateway_composition::rejectChangedComposedFileResult,
        phase' = FileChangedRetry,
      }
    } else {
      all {
        phase == FileChangedRetry,
        dfs_local_first_file_gateway_composition::stutterComposedFileTerminal,
        phase' = phase,
      }
    }

  def safety: bool = all {
    dfs_local_first_file_gateway_composition_safety::safety,
    match phase {
      | FilePrecommitCrashed => all {
          dfs_local_first_file_branch::lfPhase
            == dfs_local_first_file_branch::BranchPlanned,
          not(dfs_local_first_reconcile_gateway::gatewayJournal.committed),
        }
      | FileCommittedUnknown => all {
          dfs_local_first_file_branch::lfPhase
            == dfs_local_first_file_branch::BranchTerminal,
          dfs_local_first_reconcile_gateway::gatewayJournal.committed,
          dfs_local_first_reconcile_gateway::gatewayLastResponse
            == dfs_local_first_reconcile_gateway::GatewayOutcomeUnknown,
        }
      | FileMaterialized =>
          dfs_local_first_reconcile_gateway::gatewayMaterialized.resultRecord
            != None
      | FileExactRetry =>
          dfs_local_first_reconcile_gateway::gatewayLastDecision
            == dfs_local_first_reconcile_gateway::GatewayExactRetryReplayed
      | FileChangedRetry =>
          any {
            dfs_local_first_reconcile_gateway::gatewayLastDecision
              == dfs_local_first_reconcile_gateway::GatewayChangedRetryRejected,
            dfs_local_first_reconcile_gateway::gatewayLastDecision
              == dfs_local_first_reconcile_gateway::GatewayTerminalStutter,
          }
      | _ => true
    },
  }
}

/// Four bounded namespace classifications through the composed terminal
/// action. The selected operation is immutable for the whole trace.
module dfs_local_first_namespace_gateway_composition_scenario {
  import common.* from "../../lib/common"
  import dfs_local_first_namespace from "../../models/dfs/local_first_namespace"
  import dfs_local_first_reconcile_gateway from "../../models/dfs/local_first_reconcile_gateway"
  import dfs_local_first_namespace_gateway_composition from "../../models/dfs/local_first_namespace_gateway_composition"
  import dfs_local_first_namespace_gateway_composition_safety from "../../checks/dfs/local_first_namespace_gateway_composition_safety"

  type SelectedOutcome =
    | SelectApplicable
    | SelectConverged
    | SelectConflict
    | SelectSuperseded

  type Phase =
    | NamespaceCompositionStart
    | NamespaceBranchStarted
    | NamespaceOperationStaged
    | NamespaceBranchSealed
    | NamespaceConcurrentCommitted
    | NamespacePlanCreated
    | NamespaceEvidenceCollected
    | NamespacePermitBuilt
    | NamespaceEntryAppended
    | NamespaceCommittedUnknown
    | NamespaceMaterialized
    | NamespaceExactRetry

  var selectedOutcome: SelectedOutcome
  var phase: Phase

  action init = {
    nondet selected = oneOf(Set(
      SelectApplicable,
      SelectConverged,
      SelectConflict,
      SelectSuperseded
    ))
    all {
      dfs_local_first_namespace_gateway_composition::init,
      selectedOutcome' = selected,
      phase' = NamespaceCompositionStart,
    }
  }

  action step =
    if (phase == NamespaceCompositionStart) {
      all {
        dfs_local_first_namespace_gateway_composition::beginComposedNamespaceBranch,
        selectedOutcome' = selectedOutcome,
        phase' = NamespaceBranchStarted,
      }
    } else if (phase == NamespaceBranchStarted) {
      all {
        if (selectedOutcome == SelectSuperseded) {
          dfs_local_first_namespace_gateway_composition::stageComposedNamespaceUnlink
        } else {
          dfs_local_first_namespace_gateway_composition::stageComposedNamespaceCreate(
            dfs_local_first_namespace::RootSpareEntry
          )
        },
        selectedOutcome' = selectedOutcome,
        phase' = NamespaceOperationStaged,
      }
    } else if (phase == NamespaceOperationStaged) {
      all {
        dfs_local_first_namespace_gateway_composition::sealComposedNamespaceBranch,
        selectedOutcome' = selectedOutcome,
        phase' = NamespaceBranchSealed,
      }
    } else if (phase == NamespaceBranchSealed) {
      if (selectedOutcome == SelectApplicable) {
        all {
          dfs_local_first_namespace_gateway_composition::planComposedNamespaceGateway,
          selectedOutcome' = selectedOutcome,
          phase' = NamespacePlanCreated,
        }
      } else {
        all {
          if (selectedOutcome == SelectConverged) {
            dfs_local_first_namespace_gateway_composition::commitComposedNamespaceStrongEquivalent
          } else {
            dfs_local_first_namespace_gateway_composition::commitComposedNamespaceStrongConflict
          },
          selectedOutcome' = selectedOutcome,
          phase' = NamespaceConcurrentCommitted,
        }
      }
    } else if (phase == NamespaceConcurrentCommitted) {
      all {
        dfs_local_first_namespace_gateway_composition::planComposedNamespaceGateway,
        selectedOutcome' = selectedOutcome,
        phase' = NamespacePlanCreated,
      }
    } else if (phase == NamespacePlanCreated) {
      all {
        dfs_local_first_namespace_gateway_composition::collectComposedNamespaceGatewayEvidence,
        selectedOutcome' = selectedOutcome,
        phase' = NamespaceEvidenceCollected,
      }
    } else if (phase == NamespaceEvidenceCollected) {
      all {
        dfs_local_first_namespace_gateway_composition::buildComposedNamespaceGatewayPermit,
        selectedOutcome' = selectedOutcome,
        phase' = NamespacePermitBuilt,
      }
    } else if (phase == NamespacePermitBuilt) {
      all {
        dfs_local_first_namespace_gateway_composition::appendComposedNamespaceGatewayEntry,
        selectedOutcome' = selectedOutcome,
        phase' = NamespaceEntryAppended,
      }
    } else if (phase == NamespaceEntryAppended) {
      all {
        dfs_local_first_namespace_gateway_composition::commitComposedNamespaceTerminal(false),
        selectedOutcome' = selectedOutcome,
        phase' = NamespaceCommittedUnknown,
      }
    } else if (phase == NamespaceCommittedUnknown) {
      all {
        dfs_local_first_namespace_gateway_composition::materializeComposedNamespaceGateway,
        selectedOutcome' = selectedOutcome,
        phase' = NamespaceMaterialized,
      }
    } else if (phase == NamespaceMaterialized) {
      all {
        dfs_local_first_namespace_gateway_composition::retryExactComposedNamespaceResult,
        selectedOutcome' = selectedOutcome,
        phase' = NamespaceExactRetry,
      }
    } else {
      all {
        phase == NamespaceExactRetry,
        dfs_local_first_namespace_gateway_composition::stutterComposedNamespaceTerminal,
        selectedOutcome' = selectedOutcome,
        phase' = phase,
      }
    }

  pure def expectedOutcome(
    selected: SelectedOutcome
  ): dfs_local_first_namespace::ReconcileOutcome =
    match selected {
      | SelectApplicable => dfs_local_first_namespace::Applicable
      | SelectConverged => dfs_local_first_namespace::Converged
      | SelectConflict => dfs_local_first_namespace::Conflict
      | SelectSuperseded => dfs_local_first_namespace::Superseded
    }

  def safety: bool = all {
    dfs_local_first_namespace_gateway_composition_safety::safety,
    any {
      phase == NamespaceCompositionStart,
      phase == NamespaceBranchStarted,
      phase == NamespaceOperationStaged,
      phase == NamespaceBranchSealed,
      phase == NamespaceConcurrentCommitted,
      phase == NamespacePlanCreated,
      phase == NamespaceEvidenceCollected,
      phase == NamespacePermitBuilt,
      phase == NamespaceEntryAppended,
    } or all {
      dfs_local_first_namespace::branchPhase
        == dfs_local_first_namespace::BranchTerminal,
      dfs_local_first_namespace::branchResult
        == expectedOutcome(selectedOutcome),
      dfs_local_first_reconcile_gateway::gatewayJournal.committed,
    },
  }
}
