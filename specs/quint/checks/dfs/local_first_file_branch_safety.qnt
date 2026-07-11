/// Safety properties and coverage witnesses for the bounded local-first file
/// branch protocol.
module dfs_local_first_file_branch_safety {
  import common.* from "../../lib/common"
  import dfs_local_first_projection.* from "../../contracts/dfs/local_first_projection"
  import dfs_local_first_file_branch.* from "../../models/dfs/local_first_file_branch"

  pure def projectedFileRevision(
    fileHead: FileHead,
    effectCount: int,
    conflictCount: int
  ): int =
    match fileHead.revision {
      | FileBase => 0
      | FileConcurrent => 1
      | FileReconciled => effectCount - conflictCount
    }

  pure def fileCanonicalProjection(
    fileHead: FileHead,
    effectCount: int,
    conflictCount: int
  ): CanonicalProjection = {
    authorityEpoch: 1,
    journalHead: effectCount,
    objectRevision:
      projectedFileRevision(fileHead, effectCount, conflictCount),
    namespaceRevision: 0,
    visibleAuthorityCount: 1,
    visibleHeadCount: 1,
    canonicalConflictCount: 0,
  }


  pure def fileConflictCount(
    conflict: Option[LocalFirstConflict]
  ): int = if (isSome(conflict)) { 1 } else { 0 }


  pure def fileProjectionStepKind(
    decision: FileBranchDecision
  ): ProjectionStepKind =
    match decision {
      | LocalFsyncReturned => LocalDurabilityStep
      | ReconcileConflictMaterialized => ReconcileConflictCommitStep
      | CanonicalConcurrentWrite => StrongCanonicalCommitStep
      | ReconcileApplied => ReconcileCanonicalCommitStep
      | ReconcilePlanCreated => ReconcilePlanningStep
      | StaleReconcilePlanRejected => ReconcilePlanningStep
      | ReconcileConverged => ReconcilePlanningStep
      | ExactReconcileRetryReplayed => ReconcilePlanningStep
      | ChangedReconcileRetryRejected => ReconcilePlanningStep
      | ReconcileProcessRestarted => LocalDurabilityStep
      | FileBranchTerminalStutter => ReconcilePlanningStep
      | _ => LocalOverlayStep
    }


  def fileCommitEvidence: CanonicalCommitEvidence = {
    val plan = unwrapOr(lfPlan, defaultPlan)
    val reconciliationPublishes = any {
      lfLastDecision == ReconcileApplied,
      lfLastDecision == ReconcileConflictMaterialized,
    }
    {
      quorumAvailable: lfCanonicalQuorumAvailable,
      currentAuthority:
        lfLastDecision == CanonicalConcurrentWrite or lfLocalAuthorityReachable,
      epochMatches: true,
      planHeadMatches:
        not(reconciliationPublishes)
          or lfLastAudit.canonicalBefore == plan.observedHead,
      contentPolicySatisfied:
        not(reconciliationPublishes)
          or lfRetainedContent == Some(plan.payload.content),
      metadataPrepareSatisfied: true,
      atomicPublication: true,
    }
  }

  def degradedBranchRequiresOptIn: bool =
    lfPhase != BranchAbsent implies lfPolicy == LocalFirstAllowed


  def branchLocalStateIsWellFormed: bool = all {
    lfLocalSyncGeneration >= 0,
    lfLocalSyncGeneration <= MaxLocalSyncGeneration,
    lfPhase == BranchAbsent implies all {
      lfSealedPayload == None,
      lfPlan == None,
    },
    any {
      lfPhase == BranchAbsent,
      lfPhase == BranchOpen,
      lfPhase == BranchFrozen,
    } implies lfSealedPayload == None,
  }


  def sealedPayloadIsImmutableLocalDurabilityImage: bool =
    match lfSealedPayload {
      | Some(payload) => all {
          lfPhase != BranchAbsent,
          lfPhase != BranchOpen,
          lfPhase != BranchFrozen,
          payload.baseHead == lfBranchBase,
          payload.content == lfOverlayContent,
          payload.content == lfDurableOverlayContent,
          payload.localSyncGeneration == lfLocalSyncGeneration,
          payload.localSyncGeneration > 0,
        }
      | None => any {
          lfPhase == BranchAbsent,
          lfPhase == BranchOpen,
          lfPhase == BranchFrozen,
        }
    }


  def retentionMarkerNamesExactSealedContent: bool =
    match lfRetainedContent {
      | Some(content) =>
          match lfSealedPayload {
            | Some(payload) => content == payload.content
            | None => false
          }
      | None => true
    }


  def planFreezesPayloadHeadAndClassification: bool =
    match lfPlan {
      | Some(plan) => all {
          lfSealedPayload == Some(plan.payload),
          plan.disposition == classifyPlan(plan.payload, plan.observedHead),
          lfRetainedContent == Some(plan.payload.content),
        }
      | None => lfPhase != BranchPlanned
    }


  /// Local operation, local durability, crash, seal, and reconnect transitions
  /// all stutter under the projection that retains only canonical DFS state.
  def localTransitionsDoNotChangeCanonicalProjection: bool =
    localOnlyDecision(lfLastDecision) implies all {
      lfCanonicalHead == lfLastAudit.canonicalBefore,
      lfConflictRecord == lfLastAudit.conflictBefore,
      lfEffectCount == lfLastAudit.effectCountBefore,
    }


  /// This consumes the shared extension contract directly: every local,
  /// durability, planning, and conflict-sidecar action stutters on canonical
  /// state, while the two publishing actions refine one authorized journal
  /// step with a single visible authority and head.
  def everyStepRefinesCanonicalProjection: bool =
    lfLastDecision == NoFileBranchDecision or
      projectionStepAllowed(
        fileProjectionStepKind(lfLastDecision),
        fileCanonicalProjection(
          lfLastAudit.canonicalBefore,
          lfLastAudit.effectCountBefore,
          fileConflictCount(lfLastAudit.conflictBefore)
        ),
        fileCanonicalProjection(
          lfCanonicalHead,
          lfEffectCount,
          fileConflictCount(lfConflictRecord)
        ),
        fileCommitEvidence
      )


  def onlyConflictSidecarStepsGrowLocalConflicts: bool =
    lfLastDecision == NoFileBranchDecision or
      localConflictCountAllowed(
        fileProjectionStepKind(lfLastDecision),
        fileConflictCount(lfLastAudit.conflictBefore),
        fileConflictCount(lfConflictRecord)
      )


  def localFsyncOnlyAdvancesLocalDurability: bool =
    lfLastDecision == LocalFsyncReturned implies all {
      lfDurableOverlayContent == lfLastAudit.overlayBefore,
      lfCanonicalHead == lfLastAudit.canonicalBefore,
      lfEffectCount == lfLastAudit.effectCountBefore,
    }

  def authorityReturnSealsAndStopsLocalMutation: bool =
    lfLastDecision == AuthorityReturnSealedBranch implies all {
      lfPhase == BranchSealed,
      lfLocalAuthorityReachable,
      lfSealedPayload != None,
      lfCanonicalHead == lfLastAudit.canonicalBefore,
    }

  def authorityReturnFreezesOrSealsEveryOpenBranch: bool =
    any {
      lfLastDecision == AuthorityReturnSealedBranch,
      lfLastDecision == AuthorityReturnFrozeBranch,
    } implies all {
      lfLastAudit.phaseBefore == BranchOpen,
      not(lfLastAudit.authorityReachableBefore),
      lfLocalAuthorityReachable,
      if (lfLastDecision == AuthorityReturnSealedBranch) {
        all {
          lfPhase == BranchSealed,
          lfSealedPayload != None,
        }
      } else {
        all {
          lfPhase == BranchFrozen,
          lfSealedPayload == None,
          lfOverlayContent == lfLastAudit.overlayBefore,
          lfDurableOverlayContent == lfLastAudit.durableOverlayBefore,
        }
      },
    }

  def frozenBranchCannotResumeDegradedMutation: bool =
    lfPhase == BranchFrozen implies all {
      lfLastDecision != LocalWriteReturned,
      lfLastDecision != LocalTruncateReturned,
    }

  def localRouteLossPreservesBranchPhase: bool =
    lfLastDecision == LocalAuthorityRouteLost implies all {
      lfLastAudit.authorityReachableBefore,
      not(lfLocalAuthorityReachable),
      lfPhase == lfLastAudit.phaseBefore,
      any {
        lfPhase == BranchFrozen,
        lfPhase == BranchSealed,
        lfPhase == BranchPlanned,
      },
    }

  def authorityRouteRestoreIsIndependentOfQuorum: bool =
    lfLastDecision == LocalAuthorityRestored implies all {
      not(lfLastAudit.authorityReachableBefore),
      lfLocalAuthorityReachable,
      lfPhase == lfLastAudit.phaseBefore,
      lfCanonicalQuorumAvailable == lfLastAudit.quorumAvailableBefore,
    }

  def quorumAvailabilityIsIndependentEnvironmentState: bool =
    if (lfLastDecision == CanonicalQuorumLost) {
      all {
        lfLastAudit.quorumAvailableBefore,
        not(lfCanonicalQuorumAvailable),
      }
    } else if (lfLastDecision == CanonicalQuorumRestored) {
      all {
        not(lfLastAudit.quorumAvailableBefore),
        lfCanonicalQuorumAvailable,
      }
    } else {
      true
    }


  def crashRecoversLastLocallySyncedImage: bool =
    lfLastDecision == LocalProcessCrashed implies all {
      lfOverlayContent == lfLastAudit.durableOverlayBefore,
      lfDurableOverlayContent == lfLastAudit.durableOverlayBefore,
      lfCanonicalHead == lfLastAudit.canonicalBefore,
    }


  def canonicalHeadHasOneLegalShape: bool =
    match lfCanonicalHead.revision {
      | FileBase => lfCanonicalHead.content == BaseBytes
      | FileConcurrent => ConcurrentContents.contains(lfCanonicalHead.content)
      | FileReconciled =>
          match lfSealedPayload {
            | Some(payload) => lfCanonicalHead.content == payload.content
            | None => false
          }
    }


  def canonicalChangesOnlyThroughStrongTransitions: bool =
    lfCanonicalHead != lfLastAudit.canonicalBefore implies all {
      lfCanonicalQuorumAvailable,
      any {
        lfLastDecision == CanonicalConcurrentWrite,
        lfLastDecision == ReconcileApplied,
      },
    }


  def stalePlanCannotCommit: bool =
    lfLastDecision == StaleReconcilePlanRejected implies all {
      lfPhase == BranchSealed,
      lfPlan == None,
      lfCanonicalHead == lfLastAudit.canonicalBefore,
      lfConflictRecord == lfLastAudit.conflictBefore,
      lfEffectCount == lfLastAudit.effectCountBefore,
    }


  def appliedPlanIsFreshOrdinaryCanonicalPublication: bool =
    lfLastDecision == ReconcileApplied implies {
      val plan = unwrapOr(lfPlan, defaultPlan)
      all {
        lfPlan != None,
        lfLastAudit.canonicalBefore == plan.observedHead,
        plan.disposition == PlanApply,
        classifyPlan(plan.payload, plan.observedHead) == PlanApply,
        lfCanonicalHead == reconciledHead(plan.payload),
        lfConflictRecord == lfLastAudit.conflictBefore,
        lfEffectCount == lfLastAudit.effectCountBefore + 1,
      }
    }


  def convergedPlanDoesNotPublishAnotherVersion: bool =
    lfLastDecision == ReconcileConverged implies {
      val plan = unwrapOr(lfPlan, defaultPlan)
      all {
        lfPlan != None,
        plan.disposition == PlanConverged,
        lfCanonicalHead == plan.observedHead,
        lfCanonicalHead == lfLastAudit.canonicalBefore,
        lfConflictRecord == lfLastAudit.conflictBefore,
        lfEffectCount == lfLastAudit.effectCountBefore,
      }
    }


  def conflictsPreserveCanonicalWinnerAndLocalBytes: bool =
    match lfConflictRecord {
      | Some(conflict) => all {
          lfCanonicalHead == conflict.canonicalWinner,
          conflict.payload.content == conflict.retainedContent,
          lfRetainedContent == Some(conflict.retainedContent),
          conflict.canonicalWinner != conflict.payload.baseHead,
          conflict.canonicalWinner.content != conflict.payload.content,
        }
      | None => true
    }


  def conflictTransitionDoesNotReplaceCanonicalHead: bool =
    lfLastDecision == ReconcileConflictMaterialized implies all {
      lfCanonicalHead == lfLastAudit.canonicalBefore,
      lfLastAudit.conflictBefore == None,
      lfConflictRecord != None,
      lfEffectCount == lfLastAudit.effectCountBefore + 1,
    }


  def resultRecordMatchesTerminalState(
    requestId: ReconcileId,
    record: DurableResultRecord
  ): bool = all {
    lfSealedPayload == Some(record.payload),
    match record.result {
      | AppliedResult(publishedHead) => all {
          publishedHead == reconciledHead(record.payload),
          lfCanonicalHead == publishedHead,
          lfConflictRecord == None,
        }
      | ConvergedResult(convergedHead) => all {
          convergedHead == lfCanonicalHead,
          convergedHead.content == record.payload.content,
          lfConflictRecord == None,
        }
      | ConflictResult(conflictId) =>
          match lfConflictRecord {
            | Some(conflict) => all {
                conflict.id == conflictId,
                conflict.requestId == requestId,
                conflict.payload == record.payload,
                conflict.canonicalWinner == lfCanonicalHead,
              }
            | None => false
          }
    },
  }


  def durableResultsAreUniqueAndConsistent: bool = all {
    recordedResultCount(lfResultRecords) <= 1,
    ReconcileIds.forall(requestId =>
      match lfResultRecords.get(requestId) {
        | Some(record) => all {
            lfPhase == BranchTerminal,
            resultRecordMatchesTerminalState(requestId, record),
          }
        | None => true
      }
    ),
    lfPhase == BranchTerminal implies recordedResultCount(lfResultRecords) == 1,
  }

  /// Publication, conflict-sidecar creation, and the durable result record are
  /// one abstract metadata transaction. There is deliberately no state in
  /// which a reconciliation effect exists without its replay record.
  def reconciliationPublicationAtomicallyRecordsResult: bool =
    any {
      lfLastDecision == ReconcileApplied,
      lfLastDecision == ReconcileConverged,
      lfLastDecision == ReconcileConflictMaterialized,
    } implies {
      val requestId = unwrapOr(lfLastReconcileId, Reconcile1)
      all {
        lfLastReconcileId != None,
        lfResultRecords.get(requestId) != None,
        match lfResultRecords.get(requestId) {
          | Some(record) => all {
              lfLastSuppliedPayload == Some(record.payload),
              resultRecordMatchesTerminalState(requestId, record),
            }
          | None => false
        },
      }
    }


  def exactRetryReturnsStoredResultWithoutEffects: bool =
    lfLastDecision == ExactReconcileRetryReplayed implies {
      val requestId = unwrapOr(lfLastReconcileId, Reconcile1)
      val record = unwrapOr(
        lfResultRecords.get(requestId),
        defaultResultRecord
      )
      all {
        lfLastReconcileId != None,
        lfResultRecords.get(requestId) != None,
        lfLastSuppliedPayload == Some(record.payload),
        lfLastResponse == ReconcileResultReturned({
          requestId: requestId,
          result: record.result,
        }),
        lfCanonicalHead == lfLastAudit.canonicalBefore,
        lfConflictRecord == lfLastAudit.conflictBefore,
        lfEffectCount == lfLastAudit.effectCountBefore,
      }
    }

  def reconciliationRestartPreservesDurableRecords: bool =
    lfLastDecision == ReconcileProcessRestarted implies all {
      lfPhase == BranchTerminal,
      lfResultRecords == lfLastAudit.resultRecordsBefore,
      recordedResultCount(lfResultRecords) > 0,
      lfConflictRecord == lfLastAudit.conflictBefore,
      lfCanonicalHead == lfLastAudit.canonicalBefore,
      lfEffectCount == lfLastAudit.effectCountBefore,
      lfLastResponse == NoReconcileResponse,
    }


  def changedRetryIsRejectedWithoutEffects: bool =
    lfLastDecision == ChangedReconcileRetryRejected implies {
      val requestId = unwrapOr(lfLastReconcileId, Reconcile1)
      val record = unwrapOr(
        lfResultRecords.get(requestId),
        defaultResultRecord
      )
      val supplied = unwrapOr(lfLastSuppliedPayload, defaultPayload)
      all {
        lfResultRecords.get(requestId) != None,
        supplied != record.payload,
        lfLastResponse == ReconcilePayloadMismatch(requestId),
        lfCanonicalHead == lfLastAudit.canonicalBefore,
        lfConflictRecord == lfLastAudit.conflictBefore,
        lfEffectCount == lfLastAudit.effectCountBefore,
      }
    }


  def countersStayInsideBounds: bool = all {
    lfEffectCount >= 0,
    lfEffectCount <= 2,
    recordedResultCount(lfResultRecords) >= 0,
    recordedResultCount(lfResultRecords) <= 1,
  }


  def witnessVolatileWriteLostOnCrash: bool = all {
    lfLastDecision == LocalProcessCrashed,
    lfLastAudit.overlayBefore == LocalWriteBytes,
    lfLastAudit.durableOverlayBefore == BaseBytes,
    lfOverlayContent == BaseBytes,
  }


  def witnessLocallySyncedTruncateSurvivesCrash: bool = all {
    lfLastDecision == LocalProcessCrashed,
    lfLastAudit.overlayBefore == LocalTruncatedBytes,
    lfLastAudit.durableOverlayBefore == LocalTruncatedBytes,
    lfOverlayContent == LocalTruncatedBytes,
  }


  def witnessStalePlanRejected: bool =
    lfLastDecision == StaleReconcilePlanRejected


  def witnessLocalBranchApplied: bool =
    lfLastDecision == ReconcileApplied


  def witnessLocalBranchConverged: bool =
    lfLastDecision == ReconcileConverged


  def witnessLocalBranchConflictRetained: bool = all {
    lfLastDecision == ReconcileConflictMaterialized,
    lfConflictRecord != None,
  }


  def witnessLostReplyExactRetry: bool = all {
    lfLastDecision == ExactReconcileRetryReplayed,
    match lfLastReconcileId {
      | Some(requestId) =>
          match lfResultRecords.get(requestId) {
            | Some(record) =>
                lfLastResponse == ReconcileResultReturned({
                  requestId: requestId,
                  result: record.result,
                })
            | None => false
          }
      | None => false
    },
  }


  def witnessChangedRetryRejected: bool =
    lfLastDecision == ChangedReconcileRetryRejected

  def witnessChangedRetryBaseRejected: bool = {
      val requestId = unwrapOr(lfLastReconcileId, Reconcile1)
      val record = unwrapOr(lfResultRecords.get(requestId), defaultResultRecord)
      val supplied = unwrapOr(lfLastSuppliedPayload, defaultPayload)
      all {
        lfLastDecision == ChangedReconcileRetryRejected,
        supplied.baseHead != record.payload.baseHead,
      }
    }

  def witnessChangedRetryGenerationRejected: bool = {
      val requestId = unwrapOr(lfLastReconcileId, Reconcile1)
      val record = unwrapOr(lfResultRecords.get(requestId), defaultResultRecord)
      val supplied = unwrapOr(lfLastSuppliedPayload, defaultPayload)
      all {
        lfLastDecision == ChangedReconcileRetryRejected,
        supplied.localSyncGeneration != record.payload.localSyncGeneration,
      }
    }

  def witnessAuthorityReturnSealsDirtyBranch: bool =
    lfLastDecision == AuthorityReturnSealedBranch

  def witnessAuthorityReturnFreezesUnsyncedBranch: bool = all {
    lfLastDecision == AuthorityReturnFrozeBranch,
    lfLastAudit.overlayBefore != lfLastAudit.durableOverlayBefore,
    lfPhase == BranchFrozen,
  }

  def witnessFrozenBranchSurvivesRouteLoss: bool = all {
    lfLastDecision == LocalAuthorityRouteLost,
    lfPhase == BranchFrozen,
    not(lfLocalAuthorityReachable),
  }

  def witnessCleanFrozenBranchClosed: bool = all {
    lfLastDecision == CleanFrozenBranchClosed,
    lfPhase == BranchAbsent,
  }

  def witnessCanonicalQuorumLost: bool =
    lfLastDecision == CanonicalQuorumLost

  def witnessCanonicalQuorumRestored: bool =
    lfLastDecision == CanonicalQuorumRestored

  def witnessRouteRestoredWithoutQuorum: bool = all {
    lfLastDecision == LocalAuthorityRestored,
    not(lfCanonicalQuorumAvailable),
  }

  def witnessExactRetryAfterProcessRestart: bool = all {
    lfLastDecision == ExactReconcileRetryReplayed,
    lfLastAudit.decisionBefore == ReconcileProcessRestarted,
  }

  def witnessExactRetryWithConcurrentBase: bool = all {
    lfLastDecision == ExactReconcileRetryReplayed,
    match lfLastSuppliedPayload {
      | Some(payload) => payload.baseHead.revision == FileConcurrent
      | None => false
    },
  }


  /// Deterministic full-causal driver used by the harness to check this entire
  /// safety suite, not only scenario-local assertions, through local crash
  /// recovery, stale-plan rejection, conflict publication, process restart,
  /// and exact retry.
  action fileReconcileFullSafetyStep: bool =
    if (lfPhase == BranchAbsent) {
      beginLocalBranch
    } else if (lfPhase == BranchOpen) {
      if (lfLastDecision == LocalBranchStarted) {
        localWrite
      } else if (lfLastDecision == LocalWriteReturned) {
        crashLocalProcess
      } else if (all {
        lfLastDecision == LocalProcessCrashed,
        lfLocalSyncGeneration == 0,
      }) {
        localTruncate
      } else if (lfLastDecision == LocalTruncateReturned) {
        localFsync
      } else if (lfLastDecision == LocalFsyncReturned) {
        crashLocalProcess
      } else if (all {
        lfLastDecision == LocalProcessCrashed,
        lfLocalSyncGeneration > 0,
      }) {
        sealBranchOnAuthorityReturn
      } else {
        all { false, beginLocalBranch }
      }
    } else if (lfPhase == BranchSealed) {
      if (lfRetainedContent == None) {
        retainSealedContent
      } else if (not(lfLocalAuthorityReachable)) {
        restoreLocalAuthority
      } else {
        planReconciliation(Reconcile1)
      }
    } else if (lfPhase == BranchPlanned) {
      match lfPlan {
        | Some(plan) =>
            if (lfCanonicalHead != plan.observedHead) {
              rejectStalePlan
            } else if (plan.disposition == PlanApply) {
              canonicalConcurrentWrite(RemoteBytes)
            } else {
              conflictFreshPlan(false)
            }
        | None => all { false, beginLocalBranch }
      }
    } else if (lfPhase == BranchTerminal) {
      if (lfLastDecision == ReconcileConflictMaterialized) {
        restartAfterReconciliation
      } else if (lfLastDecision == ReconcileProcessRestarted) {
        retryExactResult(
          Reconcile1,
          unwrapOr(lfSealedPayload, defaultPayload)
        )
      } else if (lfLastDecision == ExactReconcileRetryReplayed) {
        rejectChangedRetry(Reconcile1, {
          baseHead: { revision: FileConcurrent, content: RemoteBytes },
          content: lfOverlayContent,
          localSyncGeneration: lfLocalSyncGeneration,
        })
      } else if (all {
        lfLastDecision == ChangedReconcileRetryRejected,
        unwrapOr(lfLastSuppliedPayload, defaultPayload).baseHead != lfBranchBase,
      }) {
        rejectChangedRetry(Reconcile1, {
          baseHead: lfBranchBase,
          content: lfOverlayContent,
          localSyncGeneration: 0,
        })
      } else {
        stutterTerminal
      }
    } else {
      all { false, beginLocalBranch }
    }

  /// Full-causal apply path through local durability, authority return,
  /// canonical publication, restart, and exact replay.
  action fileApplyFullSafetyStep: bool =
    if (lfPhase == BranchAbsent) {
      if (lfCanonicalHead.revision == FileBase) {
        canonicalConcurrentWrite(LocalWriteBytes)
      } else {
        beginLocalBranch
      }
    } else if (lfPhase == BranchOpen) {
      if (lfLastDecision == LocalBranchStarted) {
        localTruncate
      } else if (lfLastDecision == LocalTruncateReturned) {
        localFsync
      } else if (lfLastDecision == LocalFsyncReturned) {
        sealBranchOnAuthorityReturn
      } else {
        all { false, beginLocalBranch }
      }
    } else if (lfPhase == BranchSealed) {
      if (lfRetainedContent == None) {
        retainSealedContent
      } else {
        planReconciliation(Reconcile1)
      }
    } else if (lfPhase == BranchPlanned) {
      if (lfLastDecision == ReconcilePlanCreated) {
        loseLocalAuthorityRoute
      } else if (lfLastDecision == LocalAuthorityRouteLost) {
        setCanonicalQuorum(false)
      } else if (lfLastDecision == CanonicalQuorumLost) {
        restoreLocalAuthority
      } else if (all {
        lfLastDecision == LocalAuthorityRestored,
        not(lfCanonicalQuorumAvailable),
      }) {
        setCanonicalQuorum(true)
      } else {
        applyFreshPlan(false)
      }
    } else if (lfPhase == BranchTerminal) {
      if (lfLastDecision == ReconcileApplied) {
        restartAfterReconciliation
      } else if (lfLastDecision == ReconcileProcessRestarted) {
        retryExactResult(
          Reconcile1,
          unwrapOr(lfSealedPayload, defaultPayload)
        )
      } else {
        stutterTerminal
      }
    } else {
      all { false, beginLocalBranch }
    }

  /// Full-causal convergence path: the authority publishes the same bytes
  /// first, so reconciliation records a no-op outcome and still survives
  /// process restart and exact replay.
  action fileConvergeFullSafetyStep: bool =
    if (lfPhase == BranchAbsent) {
      beginLocalBranch
    } else if (lfPhase == BranchOpen) {
      if (lfLastDecision == LocalBranchStarted) {
        localTruncate
      } else if (lfLastDecision == LocalTruncateReturned) {
        localFsync
      } else if (lfLastDecision == LocalFsyncReturned) {
        sealBranchOnAuthorityReturn
      } else {
        all { false, beginLocalBranch }
      }
    } else if (lfPhase == BranchSealed) {
      if (lfRetainedContent == None) {
        retainSealedContent
      } else if (lfCanonicalHead.revision == FileBase) {
        canonicalConcurrentWrite(LocalTruncatedBytes)
      } else {
        planReconciliation(Reconcile1)
      }
    } else if (lfPhase == BranchPlanned) {
      convergeFreshPlan(false)
    } else if (lfPhase == BranchTerminal) {
      if (lfLastDecision == ReconcileConverged) {
        restartAfterReconciliation
      } else if (lfLastDecision == ReconcileProcessRestarted) {
        retryExactResult(
          Reconcile1,
          unwrapOr(lfSealedPayload, defaultPayload)
        )
      } else {
        stutterTerminal
      }
    } else {
      all { false, beginLocalBranch }
    }


  def safety: bool = all {
    degradedBranchRequiresOptIn,
    branchLocalStateIsWellFormed,
    sealedPayloadIsImmutableLocalDurabilityImage,
    retentionMarkerNamesExactSealedContent,
    planFreezesPayloadHeadAndClassification,
    localTransitionsDoNotChangeCanonicalProjection,
    everyStepRefinesCanonicalProjection,
    onlyConflictSidecarStepsGrowLocalConflicts,
    localFsyncOnlyAdvancesLocalDurability,
    authorityReturnSealsAndStopsLocalMutation,
    authorityReturnFreezesOrSealsEveryOpenBranch,
    frozenBranchCannotResumeDegradedMutation,
    localRouteLossPreservesBranchPhase,
    authorityRouteRestoreIsIndependentOfQuorum,
    quorumAvailabilityIsIndependentEnvironmentState,
    crashRecoversLastLocallySyncedImage,
    canonicalHeadHasOneLegalShape,
    canonicalChangesOnlyThroughStrongTransitions,
    stalePlanCannotCommit,
    appliedPlanIsFreshOrdinaryCanonicalPublication,
    convergedPlanDoesNotPublishAnotherVersion,
    conflictsPreserveCanonicalWinnerAndLocalBytes,
    conflictTransitionDoesNotReplaceCanonicalHead,
    durableResultsAreUniqueAndConsistent,
    reconciliationPublicationAtomicallyRecordsResult,
    reconciliationRestartPreservesDurableRecords,
    exactRetryReturnsStoredResultWithoutEffects,
    changedRetryIsRejectedWithoutEffects,
    countersStayInsideBounds,
  }
}
