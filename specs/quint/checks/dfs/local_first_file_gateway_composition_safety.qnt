/// Safety of the bounded file-branch to typed-gateway composition.
module dfs_local_first_file_gateway_composition_safety {
  import common.* from "../../lib/common"
  import dfs_local_first_file_gateway_composition.* from "../../models/dfs/local_first_file_gateway_composition"
  import dfs_local_first_file_branch from "../../models/dfs/local_first_file_branch"
  import dfs_local_first_reconcile_gateway from "../../models/dfs/local_first_reconcile_gateway"
  import dfs_local_first_reconcile_permit from "../../contracts/dfs/local_first_reconcile_permit"

  pure def fileTerminalDecision(
    decision: dfs_local_first_file_branch::FileBranchDecision
  ): bool = any {
    decision == dfs_local_first_file_branch::ReconcileApplied,
    decision == dfs_local_first_file_branch::ReconcileConverged,
    decision == dfs_local_first_file_branch::ReconcileConflictMaterialized,
  }

  pure def gatewayCommittedDecision(
    decision: dfs_local_first_reconcile_gateway::GatewayDecision
  ): bool = any {
    decision
      == dfs_local_first_reconcile_gateway::GatewayReconcileCommittedWithReply,
    decision
      == dfs_local_first_reconcile_gateway::GatewayReconcileCommittedReplyLost,
  }

  pure def resultDigestForFileResult(
    result: dfs_local_first_file_branch::ReconcileResult
  ): dfs_local_first_reconcile_permit::ReconcileResultDigest =
    match result {
      | AppliedResult(_) => dfs_local_first_reconcile_permit::ResultApplied
      | ConvergedResult(_) => dfs_local_first_reconcile_permit::ResultConverged
      | ConflictResult(_) => dfs_local_first_reconcile_permit::ResultConflict
    }

  def fileGatewayEntry:
    dfs_local_first_reconcile_gateway::GatewayJournalEntry =
    dfs_local_first_reconcile_gateway::unwrapGatewayEntry(
      dfs_local_first_reconcile_gateway::gatewayJournal.entry
    )

  def fileGatewayCanonicalStateIsAligned: bool = all {
    dfs_local_first_reconcile_gateway::gatewayCanonical.journalHead
      == dfs_local_first_file_branch::lfEffectCount,
    dfs_local_first_reconcile_gateway::gatewayCanonical.objectRevision
      == fileHeadObjectRevision(
        dfs_local_first_file_branch::lfCanonicalHead
      ),
    dfs_local_first_reconcile_gateway::gatewayCanonical
      .canonicalConflictCount == 0,
  }

  /// The composed action surface cannot leave the exact two-manifest slice:
  /// strong writes are unavailable before branch creation, and after the sole
  /// local sync only sealing remains enabled.
  def fileCompositionManifestDomainIsClosed: bool =
    if (dfs_local_first_file_branch::lfPhase
      == dfs_local_first_file_branch::BranchAbsent) {
      all {
        dfs_local_first_file_branch::lfCanonicalHead
          == dfs_local_first_file_branch::baseHead,
        dfs_local_first_file_branch::lfEffectCount == 0,
      }
    } else if (dfs_local_first_file_branch::lfPhase
      == dfs_local_first_file_branch::BranchOpen) {
      all {
        dfs_local_first_file_branch::lfBranchBase
          == dfs_local_first_file_branch::baseHead,
        dfs_local_first_file_branch::lfLocalSyncGeneration >= 0,
        dfs_local_first_file_branch::lfLocalSyncGeneration <= 1,
        dfs_local_first_file_branch::lfLocalSyncGeneration == 1 implies all {
          dfs_local_first_file_branch::lfOverlayContent
            == dfs_local_first_file_branch::lfDurableOverlayContent,
          dfs_local_first_file_branch::lfOverlayContent
            != dfs_local_first_file_branch::lfBranchBase.content,
        },
      }
    } else if (dfs_local_first_file_branch::lfPhase
      == dfs_local_first_file_branch::BranchFrozen) {
      false
    } else {
      all {
        dfs_local_first_file_branch::lfSealedPayload != None,
        fileGatewayPayloadAdmitted(composedFilePayload),
      }
    }

  /// The exact file manifest admitted by this bridge is frozen before either
  /// a gateway plan or journal entry can exist.
  def fileGatewayPlanMatchesBranchPlan: bool = all {
    match dfs_local_first_reconcile_gateway::gatewayPlan {
      | Some(gatewayPlan) => all {
          dfs_local_first_file_branch::lfPlan != None,
          fileGatewayPayloadAdmitted(composedFilePlan.payload),
          gatewayPlan.requestId
            == dfs_local_first_reconcile_permit::GatewayRequest,
          composedFilePlan.requestId
            == dfs_local_first_file_branch::Reconcile1,
          gatewayPlan.payloadDigest
            == fileGatewayPayloadDigest(composedFilePlan.payload),
          gatewayPlan.outcome
            == fileGatewayOutcome(composedFilePlan.disposition),
          gatewayPlan.journalHead
            == fileHeadObjectRevision(composedFilePlan.observedHead),
          gatewayPlan.objectRevision
            == fileHeadObjectRevision(composedFilePlan.observedHead),
        }
      | None => true
    },
    match dfs_local_first_reconcile_gateway::gatewayJournal.entry {
      | Some(entry) => all {
          dfs_local_first_file_branch::lfPlan != None,
          entry.permit.plan.payloadDigest
            == fileGatewayPayloadDigest(composedFilePlan.payload),
          entry.permit.plan.outcome
            == fileGatewayOutcome(composedFilePlan.disposition),
          entry.permit.plan.requestId
            == dfs_local_first_reconcile_permit::GatewayRequest,
        }
      | None => true
    },
  }

  /// This is the composition firewall: neither state owner can reach a
  /// terminal/committed state without the other in the same transition.
  def fileTerminalStateEqualsGatewayCommit: bool =
    (dfs_local_first_file_branch::lfPhase
      == dfs_local_first_file_branch::BranchTerminal)
      == dfs_local_first_reconcile_gateway::gatewayJournal.committed

  def fileTerminalTransitionEqualsGatewayCommitTransition: bool = all {
    fileTerminalDecision(dfs_local_first_file_branch::lfLastDecision)
      == gatewayCommittedDecision(
        dfs_local_first_reconcile_gateway::gatewayLastDecision
      ),
    fileTerminalDecision(dfs_local_first_file_branch::lfLastDecision) implies
      dfs_local_first_reconcile_gateway::gatewayAudit.phaseBefore
        == dfs_local_first_reconcile_gateway::GatewayEntryAppended,
  }

  def committedFileResultMatchesGatewayEnvelope: bool =
    dfs_local_first_reconcile_gateway::gatewayJournal.committed implies {
      val publication = fileGatewayEntry.permit.publication
      val record = unwrapOr(
        dfs_local_first_file_branch::lfResultRecords.get(
          dfs_local_first_file_branch::Reconcile1
        ),
        dfs_local_first_file_branch::defaultResultRecord
      )
      all {
        dfs_local_first_file_branch::lfResultRecords.get(
          dfs_local_first_file_branch::Reconcile1
        ) != None,
        publication.payloadDigest
          == fileGatewayPayloadDigest(record.payload),
        publication.outcome
          == fileGatewayOutcome(composedFilePlan.disposition),
        publication.resultDigest == resultDigestForFileResult(record.result),
        if (publication.outcome
          == dfs_local_first_reconcile_permit::PublishConflict) {
          all {
            dfs_local_first_file_branch::lfConflictRecord != None,
            publication.conflictArtifact
              == Some(dfs_local_first_reconcile_permit::ArtifactA),
          }
        } else {
          all {
            dfs_local_first_file_branch::lfConflictRecord == None,
            publication.conflictArtifact == None,
          }
        },
      }
    }

  def filePrecommitCrashCannotTerminalizeBranch: bool =
    dfs_local_first_reconcile_gateway::gatewayLastDecision
      == dfs_local_first_reconcile_gateway::GatewayProcessCrashedBeforeCommit
      implies all {
        dfs_local_first_file_branch::lfPhase
          == dfs_local_first_file_branch::BranchPlanned,
        dfs_local_first_file_branch::recordedResultCount(
          dfs_local_first_file_branch::lfResultRecords
        ) == 0,
        dfs_local_first_file_branch::lfConflictRecord == None,
      }

  def fileStaleRejectionIsCoupled: bool =
    dfs_local_first_file_branch::lfLastDecision
      == dfs_local_first_file_branch::StaleReconcilePlanRejected
      implies all {
        any {
          dfs_local_first_reconcile_gateway::gatewayLastDecision
            == dfs_local_first_reconcile_gateway::GatewayStalePublicationRejected,
          dfs_local_first_reconcile_gateway::gatewayLastDecision
            == dfs_local_first_reconcile_gateway::GatewayTerminalStutter,
        },
        dfs_local_first_file_branch::lfPhase
          == dfs_local_first_file_branch::BranchSealed,
        not(dfs_local_first_reconcile_gateway::gatewayJournal.committed),
      }

  def fileRetriesAreCoupledWithoutEffects: bool = all {
    dfs_local_first_file_branch::lfLastDecision
      == dfs_local_first_file_branch::ExactReconcileRetryReplayed
      implies dfs_local_first_reconcile_gateway::gatewayLastDecision
        == dfs_local_first_reconcile_gateway::GatewayExactRetryReplayed,
    dfs_local_first_file_branch::lfLastDecision
      == dfs_local_first_file_branch::ChangedReconcileRetryRejected
      implies {
        val supplied = unwrapOr(
          dfs_local_first_file_branch::lfLastSuppliedPayload,
          dfs_local_first_file_branch::defaultPayload
        )
        all {
          supplied == changedFileGatewayPayload(composedFilePayload),
          dfs_local_first_reconcile_gateway::gatewayLastDecision
            == dfs_local_first_reconcile_gateway::GatewayChangedRetryRejected,
          dfs_local_first_reconcile_gateway::gatewayLastSuppliedPayload
            == Some(fileGatewayPayloadDigest(supplied)),
        }
      },
  }

  def witnessComposedFileApplied: bool = all {
    dfs_local_first_file_branch::lfLastDecision
      == dfs_local_first_file_branch::ReconcileApplied,
    dfs_local_first_reconcile_gateway::gatewayJournal.committed,
  }

  /// Random exploration reaches planning far more reliably than the complete
  /// straight-through apply path. Terminal Apply is covered by the dedicated
  /// full-causal driver below.
  def witnessComposedFileApplyPlanned: bool = all {
    dfs_local_first_file_branch::lfPhase
      == dfs_local_first_file_branch::BranchPlanned,
    composedFilePlan.disposition
      == dfs_local_first_file_branch::PlanApply,
    dfs_local_first_reconcile_gateway::gatewayPlan != None,
  }

  def witnessComposedFileConverged: bool = all {
    dfs_local_first_file_branch::lfLastDecision
      == dfs_local_first_file_branch::ReconcileConverged,
    dfs_local_first_reconcile_gateway::gatewayJournal.committed,
  }

  def witnessComposedFileConflict: bool = all {
    dfs_local_first_file_branch::lfLastDecision
      == dfs_local_first_file_branch::ReconcileConflictMaterialized,
    dfs_local_first_reconcile_gateway::gatewayJournal.committed,
  }

  def witnessComposedFilePrecommitCrash: bool =
    dfs_local_first_reconcile_gateway::gatewayLastDecision
      == dfs_local_first_reconcile_gateway::GatewayProcessCrashedBeforeCommit

  def witnessComposedFilePostcommitRecovery: bool =
    dfs_local_first_reconcile_gateway::gatewayLastDecision
      == dfs_local_first_reconcile_gateway::GatewayCommittedEntryMaterialized

  def witnessComposedFileExactRetry: bool =
    dfs_local_first_file_branch::lfLastDecision
      == dfs_local_first_file_branch::ExactReconcileRetryReplayed

  def witnessComposedFileChangedRetry: bool =
    dfs_local_first_file_branch::lfLastDecision
      == dfs_local_first_file_branch::ChangedReconcileRetryRejected

  /// Apply path through commit, lost reply, postcommit crash, recovery,
  /// materialization, exact retry, and changed-payload rejection.
  action fileGatewayApplyFullSafetyStep: bool =
    fileGatewayApplyManifestFullSafetyStep(false)

  /// The second admitted file manifest follows the same full lifecycle with a
  /// locally synced truncate rather than a write.
  action fileGatewayTruncateApplyFullSafetyStep: bool =
    fileGatewayApplyManifestFullSafetyStep(true)

  action fileGatewayApplyManifestFullSafetyStep(
    truncateManifest: bool
  ): bool =
    if (dfs_local_first_file_branch::lfPhase
      == dfs_local_first_file_branch::BranchAbsent) {
      beginComposedFileBranch
    } else if (dfs_local_first_file_branch::lfPhase
      == dfs_local_first_file_branch::BranchOpen) {
      if (dfs_local_first_file_branch::lfLastDecision
        == dfs_local_first_file_branch::LocalBranchStarted) {
        if (truncateManifest) {
          truncateComposedFileBranch
        } else {
          writeComposedFileBranch
        }
      } else if (any {
        dfs_local_first_file_branch::lfLastDecision
          == dfs_local_first_file_branch::LocalWriteReturned,
        dfs_local_first_file_branch::lfLastDecision
          == dfs_local_first_file_branch::LocalTruncateReturned,
      }) {
        syncComposedFileBranch
      } else {
        sealComposedFileBranch
      }
    } else if (dfs_local_first_file_branch::lfPhase
      == dfs_local_first_file_branch::BranchSealed) {
      if (dfs_local_first_file_branch::lfRetainedContent == None) {
        retainComposedFileContent
      } else if (not(dfs_local_first_file_branch::lfLocalAuthorityReachable)) {
        restoreComposedFileAuthority
      } else {
        planComposedFileGateway
      }
    } else if (dfs_local_first_file_branch::lfPhase
      == dfs_local_first_file_branch::BranchPlanned) {
      if (dfs_local_first_reconcile_gateway::gatewayPhase
        == dfs_local_first_reconcile_gateway::GatewayPlanned) {
        if (dfs_local_first_reconcile_gateway::gatewayLastDecision
          == dfs_local_first_reconcile_gateway::GatewayPlanCreated) {
          collectComposedFileGatewayEvidence
        } else {
          buildComposedFileGatewayPermit
        }
      } else if (dfs_local_first_reconcile_gateway::gatewayPhase
        == dfs_local_first_reconcile_gateway::GatewayPermitReady) {
        appendComposedFileGatewayEntry
      } else {
        commitComposedFileTerminal(false)
      }
    } else if (dfs_local_first_reconcile_gateway::gatewayPhase
      == dfs_local_first_reconcile_gateway::GatewayCommitted) {
      if (dfs_local_first_reconcile_gateway::gatewayLastDecision
        == dfs_local_first_reconcile_gateway::GatewayReconcileCommittedReplyLost) {
        crashComposedFileGatewayAfterCommit
      } else {
        materializeComposedFileGateway
      }
    } else if (dfs_local_first_reconcile_gateway::gatewayPhase
      == dfs_local_first_reconcile_gateway::GatewayStopped) {
      restartComposedFileGatewayAfterCommit
    } else if (dfs_local_first_reconcile_gateway::gatewayLastDecision
      == dfs_local_first_reconcile_gateway::GatewayCommittedEntryMaterialized) {
      retryExactComposedFileResult
    } else if (dfs_local_first_file_branch::lfLastDecision
      == dfs_local_first_file_branch::ExactReconcileRetryReplayed) {
      rejectChangedComposedFileResult
    } else {
      stutterComposedFileTerminal
    }

  pure def convergedDriverContent:
    dfs_local_first_file_branch::FileContent =
    dfs_local_first_file_branch::LocalWriteBytes

  pure def conflictDriverContent:
    dfs_local_first_file_branch::FileContent =
    dfs_local_first_file_branch::RemoteBytes

  action fileGatewayConvergeFullSafetyStep: bool =
    if (dfs_local_first_file_branch::lfPhase
      == dfs_local_first_file_branch::BranchAbsent) {
      beginComposedFileBranch
    } else if (dfs_local_first_file_branch::lfPhase
      == dfs_local_first_file_branch::BranchOpen) {
      if (dfs_local_first_file_branch::lfLastDecision
        == dfs_local_first_file_branch::LocalBranchStarted) {
        writeComposedFileBranch
      } else if (dfs_local_first_file_branch::lfLastDecision
        == dfs_local_first_file_branch::LocalWriteReturned) {
        syncComposedFileBranch
      } else {
        sealComposedFileBranch
      }
    } else if (dfs_local_first_file_branch::lfPhase
      == dfs_local_first_file_branch::BranchSealed) {
      if (dfs_local_first_file_branch::lfRetainedContent == None) {
        retainComposedFileContent
      } else if (dfs_local_first_file_branch::lfCanonicalHead.revision
        == dfs_local_first_file_branch::FileBase) {
        advanceComposedFileCanonical(convergedDriverContent)
      } else if (not(dfs_local_first_file_branch::lfLocalAuthorityReachable)) {
        restoreComposedFileAuthority
      } else {
        planComposedFileGateway
      }
    } else if (dfs_local_first_file_branch::lfPhase
      == dfs_local_first_file_branch::BranchPlanned) {
      if (dfs_local_first_reconcile_gateway::gatewayPhase
        == dfs_local_first_reconcile_gateway::GatewayPlanned) {
        if (dfs_local_first_reconcile_gateway::gatewayLastDecision
          == dfs_local_first_reconcile_gateway::GatewayPlanCreated) {
          collectComposedFileGatewayEvidence
        } else {
          buildComposedFileGatewayPermit
        }
      } else if (dfs_local_first_reconcile_gateway::gatewayPhase
        == dfs_local_first_reconcile_gateway::GatewayPermitReady) {
        appendComposedFileGatewayEntry
      } else {
        commitComposedFileTerminal(true)
      }
    } else {
      stutterComposedFileTerminal
    }

  action fileGatewayConflictFullSafetyStep: bool =
    if (dfs_local_first_file_branch::lfPhase
      == dfs_local_first_file_branch::BranchAbsent) {
      beginComposedFileBranch
    } else if (dfs_local_first_file_branch::lfPhase
      == dfs_local_first_file_branch::BranchOpen) {
      if (dfs_local_first_file_branch::lfLastDecision
        == dfs_local_first_file_branch::LocalBranchStarted) {
        writeComposedFileBranch
      } else if (dfs_local_first_file_branch::lfLastDecision
        == dfs_local_first_file_branch::LocalWriteReturned) {
        syncComposedFileBranch
      } else {
        sealComposedFileBranch
      }
    } else if (dfs_local_first_file_branch::lfPhase
      == dfs_local_first_file_branch::BranchSealed) {
      if (dfs_local_first_file_branch::lfRetainedContent == None) {
        retainComposedFileContent
      } else if (dfs_local_first_file_branch::lfCanonicalHead.revision
        == dfs_local_first_file_branch::FileBase) {
        advanceComposedFileCanonical(conflictDriverContent)
      } else if (not(dfs_local_first_file_branch::lfLocalAuthorityReachable)) {
        restoreComposedFileAuthority
      } else {
        planComposedFileGateway
      }
    } else if (dfs_local_first_file_branch::lfPhase
      == dfs_local_first_file_branch::BranchPlanned) {
      if (dfs_local_first_reconcile_gateway::gatewayPhase
        == dfs_local_first_reconcile_gateway::GatewayPlanned) {
        if (dfs_local_first_reconcile_gateway::gatewayLastDecision
          == dfs_local_first_reconcile_gateway::GatewayPlanCreated) {
          collectComposedFileGatewayEvidence
        } else {
          buildComposedFileGatewayPermit
        }
      } else if (dfs_local_first_reconcile_gateway::gatewayPhase
        == dfs_local_first_reconcile_gateway::GatewayPermitReady) {
        appendComposedFileGatewayEntry
      } else {
        commitComposedFileTerminal(true)
      }
    } else {
      stutterComposedFileTerminal
    }

  def safety: bool = all {
    fileGatewayCanonicalStateIsAligned,
    fileCompositionManifestDomainIsClosed,
    fileGatewayPlanMatchesBranchPlan,
    fileTerminalStateEqualsGatewayCommit,
    fileTerminalTransitionEqualsGatewayCommitTransition,
    committedFileResultMatchesGatewayEnvelope,
    filePrecommitCrashCannotTerminalizeBranch,
    fileStaleRejectionIsCoupled,
    fileRetriesAreCoupledWithoutEffects,
  }
}
