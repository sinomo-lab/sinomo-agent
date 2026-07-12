/// Safety of the bounded namespace-branch to typed-gateway composition.
module dfs_local_first_namespace_gateway_composition_safety {
  import common.* from "../../lib/common"
  import dfs_local_first_namespace_gateway_composition.* from "../../models/dfs/local_first_namespace_gateway_composition"
  import dfs_local_first_namespace from "../../models/dfs/local_first_namespace"
  import dfs_local_first_reconcile_gateway from "../../models/dfs/local_first_reconcile_gateway"
  import dfs_local_first_reconcile_permit from "../../contracts/dfs/local_first_reconcile_permit"

  type NamespaceGatewayDriverManifest =
    | DriverCreateChild
    | DriverMkdir
    | DriverRmdir
    | DriverRenameFile
    | DriverRenameDirectory

  pure def namespaceTerminalDecision(
    decision: dfs_local_first_namespace::NamespaceDecision
  ): bool = any {
    decision == dfs_local_first_namespace::ApplicableCommitted,
    decision == dfs_local_first_namespace::ConvergedCommitted,
    decision == dfs_local_first_namespace::ConflictCommitted,
    decision == dfs_local_first_namespace::SupersededCommitted,
  }

  pure def gatewayCommittedDecision(
    decision: dfs_local_first_reconcile_gateway::GatewayDecision
  ): bool = any {
    decision
      == dfs_local_first_reconcile_gateway::GatewayReconcileCommittedWithReply,
    decision
      == dfs_local_first_reconcile_gateway::GatewayReconcileCommittedReplyLost,
  }

  pure def resultDigestForNamespaceOutcome(
    outcome: dfs_local_first_namespace::ReconcileOutcome
  ): dfs_local_first_reconcile_permit::ReconcileResultDigest =
    match outcome {
      | Applicable => dfs_local_first_reconcile_permit::ResultApplied
      | Converged => dfs_local_first_reconcile_permit::ResultConverged
      | Conflict => dfs_local_first_reconcile_permit::ResultConflict
      | Superseded => dfs_local_first_reconcile_permit::ResultSuperseded
      | Unclassified => dfs_local_first_reconcile_permit::ResultSuperseded
    }

  def namespaceGatewayEntry:
    dfs_local_first_reconcile_gateway::GatewayJournalEntry =
    dfs_local_first_reconcile_gateway::unwrapGatewayEntry(
      dfs_local_first_reconcile_gateway::gatewayJournal.entry
    )

  def namespaceGatewayCanonicalStateIsAligned: bool = all {
    dfs_local_first_reconcile_gateway::gatewayCanonical.journalHead
      == dfs_local_first_namespace::canonicalHead,
    dfs_local_first_reconcile_gateway::gatewayCanonical.objectRevision
      == namespaceGatewayMutationRevision,
    dfs_local_first_reconcile_gateway::gatewayCanonical
      .canonicalConflictCount == 0,
  }

  def namespaceGatewayKeepsTerminalSlotAvailable: bool =
    dfs_local_first_namespace::branchPhase
      != dfs_local_first_namespace::BranchTerminal
      implies dfs_local_first_reconcile_gateway::gatewayCanonical.journalHead
        < dfs_local_first_reconcile_gateway::MaxGatewayJournalHead

  /// A live gateway plan or appended envelope names exactly the one-operation
  /// namespace manifest and classification frozen by the branch plan.
  def namespaceGatewayPlanMatchesBranchPlan: bool = all {
    match dfs_local_first_reconcile_gateway::gatewayPlan {
      | Some(gatewayPlan) => all {
          dfs_local_first_namespace::branchPhase
            == dfs_local_first_namespace::BranchPlanned,
          dfs_local_first_namespace::planOutcome
            != dfs_local_first_namespace::Unclassified,
          namespaceGatewayManifestAdmitted(
            dfs_local_first_namespace::branchOperation,
            dfs_local_first_namespace::sourceEntry,
            dfs_local_first_namespace::destinationEntry
          ),
          gatewayPlan.requestId
            == dfs_local_first_reconcile_permit::GatewayRequest,
          gatewayPlan.payloadDigest == currentNamespaceGatewayPayloadDigest,
          gatewayPlan.outcome == namespaceGatewayOutcome(
            dfs_local_first_namespace::planOutcome
          ),
          gatewayPlan.journalHead
            == dfs_local_first_namespace::planHead,
          if (dfs_local_first_namespace::canonicalHead
            == dfs_local_first_namespace::planHead) {
            gatewayPlan.objectRevision == namespaceGatewayMutationRevision
          } else {
            gatewayPlan.objectRevision <= namespaceGatewayMutationRevision
          },
          gatewayPlan.retainedArtifact
            == dfs_local_first_reconcile_permit::ArtifactB,
        }
      | None => true
    },
    match dfs_local_first_reconcile_gateway::gatewayJournal.entry {
      | Some(entry) => all {
          dfs_local_first_namespace::planOutcome
            != dfs_local_first_namespace::Unclassified,
          namespaceGatewayManifestAdmitted(
            dfs_local_first_namespace::branchOperation,
            dfs_local_first_namespace::sourceEntry,
            dfs_local_first_namespace::destinationEntry
          ),
          entry.permit.plan.requestId
            == dfs_local_first_reconcile_permit::GatewayRequest,
          entry.permit.plan.payloadDigest
            == currentNamespaceGatewayPayloadDigest,
          entry.permit.plan.outcome == namespaceGatewayOutcome(
            dfs_local_first_namespace::planOutcome
          ),
          entry.permit.plan.journalHead
            == dfs_local_first_namespace::planHead,
          entry.permit.plan.retainedArtifact
            == dfs_local_first_reconcile_permit::ArtifactB,
        }
      | None => true
    },
  }

  /// This is the composition firewall. The namespace cannot expose a terminal
  /// classification before the matching gateway entry is durably committed.
  def namespaceTerminalStateEqualsGatewayCommit: bool =
    (dfs_local_first_namespace::branchPhase
      == dfs_local_first_namespace::BranchTerminal)
      == dfs_local_first_reconcile_gateway::gatewayJournal.committed

  def namespaceTerminalTransitionEqualsGatewayCommitTransition: bool = all {
    namespaceTerminalDecision(
      dfs_local_first_namespace::lastDecision
    ) == gatewayCommittedDecision(
      dfs_local_first_reconcile_gateway::gatewayLastDecision
    ),
    namespaceTerminalDecision(
      dfs_local_first_namespace::lastDecision
    ) implies dfs_local_first_reconcile_gateway::gatewayAudit.phaseBefore
      == dfs_local_first_reconcile_gateway::GatewayEntryAppended,
  }

  def committedNamespaceResultMatchesGatewayEnvelope: bool =
    dfs_local_first_reconcile_gateway::gatewayJournal.committed implies {
      val publication = namespaceGatewayEntry.permit.publication
      all {
        dfs_local_first_namespace::branchPhase
          == dfs_local_first_namespace::BranchTerminal,
        publication.requestId
          == dfs_local_first_reconcile_permit::GatewayRequest,
        publication.payloadDigest == currentNamespaceGatewayPayloadDigest,
        publication.outcome == namespaceGatewayOutcome(
          dfs_local_first_namespace::branchResult
        ),
        publication.resultDigest == resultDigestForNamespaceOutcome(
          dfs_local_first_namespace::branchResult
        ),
        publication.nextJournalHead
          == dfs_local_first_namespace::canonicalHead,
        publication.nextObjectRevision == namespaceGatewayMutationRevision,
        if (dfs_local_first_namespace::branchResult
          == dfs_local_first_namespace::Conflict) {
          all {
            dfs_local_first_namespace::conflictBranches
              == Set(dfs_local_first_namespace::LocalBranchOne),
            publication.conflictArtifact
              == Some(dfs_local_first_reconcile_permit::ArtifactB),
          }
        } else {
          all {
            dfs_local_first_namespace::conflictBranches == Set(),
            publication.conflictArtifact == None,
          }
        },
      }
    }

  def namespaceMutationRevisionMatchesPublishedEffect: bool = all {
    dfs_local_first_namespace::lastDecision
      == dfs_local_first_namespace::StrongUnrelatedCommitted
      implies all {
        dfs_local_first_reconcile_gateway::gatewayLastDecision
          == dfs_local_first_reconcile_gateway::GatewayStrongHeadOnlyAdvanced,
        namespaceGatewayMutationRevision
          == dfs_local_first_reconcile_gateway::gatewayAudit
            .canonicalBefore.objectRevision,
      },
    any {
      dfs_local_first_namespace::lastDecision
        == dfs_local_first_namespace::StrongEquivalentCommitted,
      dfs_local_first_namespace::lastDecision
        == dfs_local_first_namespace::StrongConflictingCommitted,
      dfs_local_first_namespace::lastDecision
        == dfs_local_first_namespace::StrongSameObjectAbaCommitted,
      dfs_local_first_namespace::lastDecision
        == dfs_local_first_namespace::StrongDirectoryChildCommitted,
    } implies all {
      dfs_local_first_reconcile_gateway::gatewayLastDecision
        == dfs_local_first_reconcile_gateway::GatewayStrongHeadAdvanced,
      namespaceGatewayMutationRevision
        == dfs_local_first_reconcile_gateway::gatewayAudit
          .canonicalBefore.objectRevision + 1,
    },
    namespaceTerminalDecision(
      dfs_local_first_namespace::lastDecision
    ) implies namespaceGatewayMutationRevision
      == dfs_local_first_reconcile_gateway::gatewayAudit
        .canonicalBefore.objectRevision
        + if (dfs_local_first_namespace::lastDecision
          == dfs_local_first_namespace::ApplicableCommitted) { 1 } else { 0 },
  }

  def namespacePrecommitCrashCannotTerminalizeBranch: bool =
    dfs_local_first_reconcile_gateway::gatewayLastDecision
      == dfs_local_first_reconcile_gateway::GatewayProcessCrashedBeforeCommit
      implies all {
        dfs_local_first_namespace::branchPhase
          == dfs_local_first_namespace::BranchPlanned,
        dfs_local_first_namespace::branchResult
          == dfs_local_first_namespace::Unclassified,
        dfs_local_first_namespace::conflictBranches == Set(),
      }

  def namespaceStaleRejectionIsCoupled: bool =
    dfs_local_first_namespace::lastDecision
      == dfs_local_first_namespace::StalePlanRejected
      implies all {
        any {
          dfs_local_first_reconcile_gateway::gatewayLastDecision
            == dfs_local_first_reconcile_gateway::GatewayStalePublicationRejected,
          dfs_local_first_reconcile_gateway::gatewayLastDecision
            == dfs_local_first_reconcile_gateway::GatewayTerminalStutter,
        },
        dfs_local_first_namespace::branchPhase
          == dfs_local_first_namespace::BranchSealed,
        not(dfs_local_first_reconcile_gateway::gatewayJournal.committed),
      }

  def namespaceRetryIsCoupledWithoutEffects: bool =
    dfs_local_first_namespace::lastDecision
      == dfs_local_first_namespace::IdempotentRetry
      implies dfs_local_first_reconcile_gateway::gatewayLastDecision
        == dfs_local_first_reconcile_gateway::GatewayExactRetryReplayed

  def witnessComposedNamespaceApplicable: bool = all {
    dfs_local_first_namespace::lastDecision
      == dfs_local_first_namespace::ApplicableCommitted,
    dfs_local_first_reconcile_gateway::gatewayJournal.committed,
  }

  /// The deterministic Applicable driver proves terminal publication. This
  /// shorter witness keeps randomized coverage focused on classification and
  /// exact plan binding rather than a low-probability uninterrupted pipeline.
  def witnessComposedNamespaceApplicablePlanned: bool = all {
    dfs_local_first_namespace::branchPhase
      == dfs_local_first_namespace::BranchPlanned,
    dfs_local_first_namespace::planOutcome
      == dfs_local_first_namespace::Applicable,
    dfs_local_first_reconcile_gateway::gatewayPlan != None,
  }

  def witnessComposedNamespaceConverged: bool = all {
    dfs_local_first_namespace::lastDecision
      == dfs_local_first_namespace::ConvergedCommitted,
    dfs_local_first_reconcile_gateway::gatewayJournal.committed,
  }

  def witnessComposedNamespaceConflict: bool = all {
    dfs_local_first_namespace::lastDecision
      == dfs_local_first_namespace::ConflictCommitted,
    dfs_local_first_reconcile_gateway::gatewayJournal.committed,
  }

  def witnessComposedNamespaceSuperseded: bool = all {
    dfs_local_first_namespace::lastDecision
      == dfs_local_first_namespace::SupersededCommitted,
    dfs_local_first_reconcile_gateway::gatewayJournal.committed,
  }

  def witnessComposedNamespacePrecommitCrash: bool =
    dfs_local_first_reconcile_gateway::gatewayLastDecision
      == dfs_local_first_reconcile_gateway::GatewayProcessCrashedBeforeCommit

  def witnessComposedNamespacePostcommitRecovery: bool =
    dfs_local_first_reconcile_gateway::gatewayLastDecision
      == dfs_local_first_reconcile_gateway::GatewayCommittedEntryMaterialized

  def witnessComposedNamespaceExactRetry: bool =
    dfs_local_first_namespace::lastDecision
      == dfs_local_first_namespace::IdempotentRetry

  action namespaceGatewayApplicableFullSafetyStep: bool =
    namespaceGatewayFullSafetyStep(false, false, true)

  action namespaceGatewayConvergedFullSafetyStep: bool =
    namespaceGatewayFullSafetyStep(true, false, true)

  action namespaceGatewayConflictFullSafetyStep: bool =
    namespaceGatewayFullSafetyStep(false, true, true)

  action namespaceGatewaySupersededFullSafetyStep: bool =
    namespaceGatewayFullSafetyStep(false, true, false)

  action namespaceGatewayCreateChildFullSafetyStep: bool =
    namespaceGatewayManifestFullSafetyStep(DriverCreateChild)

  action namespaceGatewayMkdirFullSafetyStep: bool =
    namespaceGatewayManifestFullSafetyStep(DriverMkdir)

  action namespaceGatewayRmdirFullSafetyStep: bool =
    namespaceGatewayManifestFullSafetyStep(DriverRmdir)

  action namespaceGatewayRenameFileFullSafetyStep: bool =
    namespaceGatewayManifestFullSafetyStep(DriverRenameFile)

  action namespaceGatewayRenameDirectoryFullSafetyStep: bool =
    namespaceGatewayManifestFullSafetyStep(DriverRenameDirectory)

  /// Each remaining finite manifest is driven through an Applicable terminal
  /// commit. Create-root and unlink are already covered by the four-outcome
  /// driver above.
  action namespaceGatewayManifestFullSafetyStep(
    manifest: NamespaceGatewayDriverManifest
  ): bool =
    if (dfs_local_first_namespace::branchPhase
      == dfs_local_first_namespace::NoBranch) {
      beginComposedNamespaceBranch
    } else if (dfs_local_first_namespace::branchPhase
      == dfs_local_first_namespace::BranchOpen) {
      match manifest {
        | DriverCreateChild => stageComposedNamespaceCreate(
            dfs_local_first_namespace::DirectoryChildEntry
          )
        | DriverMkdir => stageComposedNamespaceMkdir
        | DriverRmdir => stageComposedNamespaceRmdir
        | DriverRenameFile => stageComposedNamespaceRename(
            dfs_local_first_namespace::RootFileEntry
          )
        | DriverRenameDirectory => stageComposedNamespaceRename(
            dfs_local_first_namespace::RootDirectoryEntry
          )
      }
    } else if (dfs_local_first_namespace::branchPhase
      == dfs_local_first_namespace::BranchDirty) {
      sealComposedNamespaceBranch
    } else if (dfs_local_first_namespace::branchPhase
      == dfs_local_first_namespace::BranchSealed) {
      planComposedNamespaceGateway
    } else if (dfs_local_first_namespace::branchPhase
      == dfs_local_first_namespace::BranchPlanned) {
      if (dfs_local_first_reconcile_gateway::gatewayPhase
        == dfs_local_first_reconcile_gateway::GatewayPlanned) {
        if (dfs_local_first_reconcile_gateway::gatewayLastDecision
          == dfs_local_first_reconcile_gateway::GatewayPlanCreated) {
          collectComposedNamespaceGatewayEvidence
        } else {
          buildComposedNamespaceGatewayPermit
        }
      } else if (dfs_local_first_reconcile_gateway::gatewayPhase
        == dfs_local_first_reconcile_gateway::GatewayPermitReady) {
        appendComposedNamespaceGatewayEntry
      } else {
        commitComposedNamespaceTerminal(false)
      }
    } else if (dfs_local_first_reconcile_gateway::gatewayPhase
      == dfs_local_first_reconcile_gateway::GatewayCommitted) {
      materializeComposedNamespaceGateway
    } else if (dfs_local_first_reconcile_gateway::gatewayLastDecision
      == dfs_local_first_reconcile_gateway::GatewayCommittedEntryMaterialized) {
      retryExactComposedNamespaceResult
    } else {
      stutterComposedNamespaceTerminal
    }

  /// Deterministic four-outcome driver. `createOperation = false` selects the
  /// unlink/replacement path, whose conflicting strong write is Superseded.
  action namespaceGatewayFullSafetyStep(
    makeEquivalent: bool,
    makeConflict: bool,
    createOperation: bool
  ): bool =
    if (dfs_local_first_namespace::branchPhase
      == dfs_local_first_namespace::NoBranch) {
      beginComposedNamespaceBranch
    } else if (dfs_local_first_namespace::branchPhase
      == dfs_local_first_namespace::BranchOpen) {
      if (createOperation) {
        stageComposedNamespaceCreate(
          dfs_local_first_namespace::RootSpareEntry
        )
      } else {
        stageComposedNamespaceUnlink
      }
    } else if (dfs_local_first_namespace::branchPhase
      == dfs_local_first_namespace::BranchDirty) {
      sealComposedNamespaceBranch
    } else if (dfs_local_first_namespace::branchPhase
      == dfs_local_first_namespace::BranchSealed) {
      if (makeEquivalent and dfs_local_first_namespace::canonicalHead == 0) {
        commitComposedNamespaceStrongEquivalent
      } else if (makeConflict
        and dfs_local_first_namespace::canonicalHead == 0) {
        commitComposedNamespaceStrongConflict
      } else {
        planComposedNamespaceGateway
      }
    } else if (dfs_local_first_namespace::branchPhase
      == dfs_local_first_namespace::BranchPlanned) {
      if (dfs_local_first_reconcile_gateway::gatewayPhase
        == dfs_local_first_reconcile_gateway::GatewayPlanned) {
        if (dfs_local_first_reconcile_gateway::gatewayLastDecision
          == dfs_local_first_reconcile_gateway::GatewayPlanCreated) {
          collectComposedNamespaceGatewayEvidence
        } else {
          buildComposedNamespaceGatewayPermit
        }
      } else if (dfs_local_first_reconcile_gateway::gatewayPhase
        == dfs_local_first_reconcile_gateway::GatewayPermitReady) {
        appendComposedNamespaceGatewayEntry
      } else {
        commitComposedNamespaceTerminal(false)
      }
    } else if (dfs_local_first_reconcile_gateway::gatewayPhase
      == dfs_local_first_reconcile_gateway::GatewayCommitted) {
      materializeComposedNamespaceGateway
    } else if (dfs_local_first_reconcile_gateway::gatewayLastDecision
      == dfs_local_first_reconcile_gateway::GatewayCommittedEntryMaterialized) {
      retryExactComposedNamespaceResult
    } else {
      stutterComposedNamespaceTerminal
    }

  def safety: bool = all {
    namespaceGatewayCanonicalStateIsAligned,
    namespaceGatewayKeepsTerminalSlotAvailable,
    namespaceGatewayPlanMatchesBranchPlan,
    namespaceTerminalStateEqualsGatewayCommit,
    namespaceTerminalTransitionEqualsGatewayCommitTransition,
    committedNamespaceResultMatchesGatewayEnvelope,
    namespaceMutationRevisionMatchesPublishedEffect,
    namespacePrecommitCrashCannotTerminalizeBranch,
    namespaceStaleRejectionIsCoupled,
    namespaceRetryIsCoupledWithoutEffects,
  }
}
