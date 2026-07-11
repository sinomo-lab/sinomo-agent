/// Safety properties and causal proof drivers for the typed local-first
/// reconciliation publication gateway.
module dfs_local_first_reconcile_gateway_safety {
  import common.* from "../../lib/common"
  import dfs_local_first_projection.* from "../../contracts/dfs/local_first_projection"
  import dfs_local_first_reconcile_permit.* from "../../contracts/dfs/local_first_reconcile_permit"
  import dfs_local_first_reconcile_gateway.* from "../../models/dfs/local_first_reconcile_gateway"

  pure def gatewayProjection(
    state: GatewayCanonicalState
  ): CanonicalProjection = {
    authorityEpoch: state.authorityEpoch,
    journalHead: state.journalHead,
    objectRevision: state.objectRevision,
    namespaceRevision: 0,
    visibleAuthorityCount: 1,
    visibleHeadCount: 1,
    canonicalConflictCount: state.canonicalConflictCount,
  }

  pure def committedGatewayDecision(decision: GatewayDecision): bool = any {
    decision == GatewayReconcileCommittedWithReply,
    decision == GatewayReconcileCommittedReplyLost,
  }

  pure def gatewayCommitKind(
    publication: AtomicReconcilePublication
  ): ProjectionStepKind =
    if (publication.outcome == PublishConflict) {
      ReconcileConflictCommitStep
    } else {
      ReconcileCanonicalCommitStep
    }

  def auditEntry: GatewayJournalEntry =
    unwrapGatewayEntry(gatewayAudit.journalBefore.entry)

  def currentEntry: GatewayJournalEntry =
    unwrapGatewayEntry(gatewayJournal.entry)

  def auditPermitEvidence: CanonicalCommitEvidence =
    canonicalEvidenceFromPermit(
      auditEntry.permit,
      gatewayAudit.canonicalBefore.primary,
      gatewayAudit.canonicalBefore.authorityEpoch,
      gatewayAudit.canonicalBefore.configRevision,
      gatewayAudit.canonicalBefore.journalHead,
      gatewayAudit.canonicalBefore.objectRevision
    )

  def gatewayStateIsWellFormed: bool = all {
    gatewayOnline.subseteq(Nodes),
    gatewayCanonical.authorityEpoch > 0,
    gatewayCanonical.authorityEpoch <= MaxGatewayEpoch,
    gatewayCanonical.configRevision == 1,
    gatewayCanonical.journalHead >= 0,
    gatewayCanonical.journalHead <= MaxGatewayJournalHead,
    gatewayCanonical.objectRevision >= 0,
    gatewayCanonical.objectRevision <= MaxGatewayObjectRevision,
    gatewayCanonical.objectRevision <= gatewayCanonical.journalHead,
    gatewayCanonical.canonicalConflictCount == 0,
    canonicalProjectionWellFormed(gatewayProjection(gatewayCanonical)),
    gatewayMaterialized.objectRevision >= 0,
    gatewayMaterialized.objectRevision <= gatewayCanonical.objectRevision,
    gatewayJournal.entry == None implies not(gatewayJournal.committed),
    gatewayPhase == GatewayIdle implies all {
      gatewayPlan == None,
      gatewayPermit == None,
      gatewayJournal.entry == None,
      not(gatewayJournal.committed),
    },
    gatewayPhase == GatewayPlanned implies all {
      gatewayPlan != None,
      gatewayPermit == None,
      gatewayJournal.entry == None,
    },
    gatewayPhase == GatewayPermitReady implies all {
      gatewayPlan != None,
      gatewayPermit != None,
      gatewayJournal.entry == None,
    },
    gatewayPhase == GatewayEntryAppended implies all {
      gatewayPlan != None,
      gatewayPermit != None,
      gatewayJournal.entry != None,
      not(gatewayJournal.committed),
    },
    any {
      gatewayPhase == GatewayCommitted,
      gatewayPhase == GatewayMaterialized,
    } implies all {
      gatewayJournal.entry != None,
      gatewayJournal.committed,
      gatewayPlan == None,
      gatewayPermit == None,
    },
  }

  /// A stored entry is internally self-contained. Once committed, its frozen
  /// predecessor state reconstructs a valid permit even though the live head
  /// has already advanced to the successor slot.
  def journalEntryIsSelfConsistent: bool =
    match gatewayJournal.entry {
      | Some(entry) => {
          val permit = entry.permit
          all {
            entry.index == permit.publication.nextJournalHead,
            permit.publication == publicationForPlan(permit.plan),
            reconcilePermitReady(
              permit,
              permit.plan.primary,
              permit.plan.authorityEpoch,
              permit.plan.configRevision,
              permit.plan.journalHead,
              permit.plan.objectRevision
            ),
            if (gatewayJournal.committed) {
              all {
                gatewayCanonical.journalHead
                  == permit.publication.nextJournalHead,
                gatewayCanonical.objectRevision
                  == permit.publication.nextObjectRevision,
              }
            } else {
              entry.index <= gatewayCanonical.journalHead + 1
            },
          }
        }
      | None => not(gatewayJournal.committed)
    }

  /// Local planning, evidence, append, crash/restart, retry, and recovery
  /// materialization stutter on the logical canonical projection. The only
  /// extension-layer advance is commit of the exact typed permit.
  def everyGatewayStepRefinesCanonicalProjection: bool =
    if (committedGatewayDecision(gatewayLastDecision)) {
      projectionStepAllowed(
        gatewayCommitKind(currentEntry.permit.publication),
        gatewayProjection(gatewayAudit.canonicalBefore),
        gatewayProjection(gatewayCanonical),
        auditPermitEvidence
      )
    } else if (gatewayLastDecision == GatewayStrongHeadAdvanced) {
      all {
        hasQuorum(gatewayAudit.onlineBefore),
        gatewayCanonical.primary == gatewayAudit.canonicalBefore.primary,
        gatewayCanonical.authorityEpoch
          == gatewayAudit.canonicalBefore.authorityEpoch,
        gatewayCanonical.configRevision
          == gatewayAudit.canonicalBefore.configRevision,
        gatewayCanonical.journalHead
          == gatewayAudit.canonicalBefore.journalHead + 1,
        gatewayCanonical.objectRevision
          == gatewayAudit.canonicalBefore.objectRevision + 1,
        gatewayCanonical.canonicalConflictCount
          == gatewayAudit.canonicalBefore.canonicalConflictCount,
      }
    } else if (gatewayLastDecision == GatewayAuthorityEpochAdvanced) {
      all {
        hasQuorum(gatewayAudit.onlineBefore),
        gatewayCanonical.primary != gatewayAudit.canonicalBefore.primary,
        gatewayCanonical.authorityEpoch
          == gatewayAudit.canonicalBefore.authorityEpoch + 1,
        gatewayCanonical.configRevision
          == gatewayAudit.canonicalBefore.configRevision,
        gatewayCanonical.journalHead
          == gatewayAudit.canonicalBefore.journalHead + 1,
        gatewayCanonical.objectRevision
          == gatewayAudit.canonicalBefore.objectRevision,
        gatewayCanonical.canonicalConflictCount
          == gatewayAudit.canonicalBefore.canonicalConflictCount,
      }
    } else {
      gatewayCanonical == gatewayAudit.canonicalBefore
    }

  /// This transition-local guard is the direct replacement for hard-coded
  /// publication booleans in the focused local-first file/namespace slices.
  def commitUsedFreshTypedPermit: bool =
    committedGatewayDecision(gatewayLastDecision) implies all {
      gatewayAudit.phaseBefore == GatewayEntryAppended,
      gatewayAudit.journalBefore.entry != None,
      not(gatewayAudit.journalBefore.committed),
      reconcilePermitReady(
        auditEntry.permit,
        gatewayAudit.canonicalBefore.primary,
        gatewayAudit.canonicalBefore.authorityEpoch,
        gatewayAudit.canonicalBefore.configRevision,
        gatewayAudit.canonicalBefore.journalHead,
        gatewayAudit.canonicalBefore.objectRevision
      ),
      canonicalCommitEvidenceReady(auditPermitEvidence),
      gatewayJournal.entry == gatewayAudit.journalBefore.entry,
      gatewayJournal.committed,
      gatewayCanonical.journalHead == auditEntry.index,
    }

  def stalePermitCannotAuthorizeCurrentState: bool =
    match gatewayPermit {
      | Some(permit) => {
          val plan = permit.plan
          if (any {
            plan.primary != gatewayCanonical.primary,
            plan.authorityEpoch != gatewayCanonical.authorityEpoch,
            plan.configRevision != gatewayCanonical.configRevision,
            plan.journalHead != gatewayCanonical.journalHead,
            plan.objectRevision != gatewayCanonical.objectRevision,
          }) {
            not(currentPermitReady(permit))
          } else {
            true
          }
        }
      | None => true
    }

  def appendNeverPublishesCanonicalState: bool =
    gatewayLastDecision == GatewayEntryWasAppended implies all {
      gatewayCanonical == gatewayAudit.canonicalBefore,
      not(gatewayJournal.committed),
      gatewayMaterialized == gatewayAudit.materializedBefore,
    }

  def precommitCrashDiscardsOnlyVolatileState: bool =
    gatewayLastDecision == GatewayProcessCrashedBeforeCommit implies all {
      not(gatewayAudit.journalBefore.committed),
      gatewayJournal == emptyJournalState,
      gatewayCanonical == gatewayAudit.canonicalBefore,
      gatewayMaterialized == gatewayAudit.materializedBefore,
      gatewayPlan == None,
      gatewayPermit == None,
      gatewayPhase == GatewayStopped,
    }

  def postcommitCrashPreservesDurableEntryAndResultEnvelope: bool =
    gatewayLastDecision == GatewayProcessCrashedAfterCommit implies all {
      gatewayAudit.journalBefore.committed,
      gatewayJournal == gatewayAudit.journalBefore,
      gatewayCanonical == gatewayAudit.canonicalBefore,
      gatewayMaterialized == gatewayAudit.materializedBefore,
      gatewayPhase == GatewayStopped,
    }

  def restartPreservesEveryDurableComponent: bool =
    gatewayLastDecision == GatewayProcessRestarted implies all {
      gatewayJournal == gatewayAudit.journalBefore,
      gatewayCanonical == gatewayAudit.canonicalBefore,
      gatewayMaterialized == gatewayAudit.materializedBefore,
      gatewayPhase != GatewayStopped,
    }

  /// The materialized tables are projections of the committed journal entry,
  /// never an independent source of a terminal reconciliation result.
  def onlyCommittedJournalCanMaterialize: bool =
    match gatewayMaterialized.resultRecord {
      | Some(record) => all {
          gatewayJournal.committed,
          gatewayJournal.entry != None,
          record == currentEntry.permit.publication,
          gatewayMaterialized.objectRevision == record.nextObjectRevision,
          gatewayMaterialized.conflictArtifact == record.conflictArtifact,
        }
      | None => gatewayMaterialized.conflictArtifact == None
    }

  def materializationReplaysExactCommittedEnvelope: bool =
    gatewayLastDecision == GatewayCommittedEntryMaterialized implies all {
      gatewayAudit.journalBefore.committed,
      gatewayJournal == gatewayAudit.journalBefore,
      gatewayCanonical == gatewayAudit.canonicalBefore,
      gatewayMaterialized.resultRecord
        == Some(currentEntry.permit.publication),
      gatewayMaterialized.objectRevision
        == currentEntry.permit.publication.nextObjectRevision,
      gatewayMaterialized.conflictArtifact
        == currentEntry.permit.publication.conflictArtifact,
    }

  def publicationOutcomeHasExactCanonicalSemantics: bool =
    match gatewayJournal.entry {
      | Some(entry) => {
          val publication = entry.permit.publication
          all {
            if (publication.outcome == PublishApply) {
              publication.nextObjectRevision
                == publication.previousObjectRevision + 1
            } else {
              publication.nextObjectRevision
                == publication.previousObjectRevision
            },
            if (publication.outcome == PublishConflict) {
              publication.conflictArtifact
                == Some(entry.permit.plan.retainedArtifact)
            } else {
              publication.conflictArtifact == None
            },
            publication.resultDigest == resultDigestFor(publication.outcome),
          }
        }
      | None => true
    }

  /// In a three-node slice, any two-of-three content and prepare sets overlap.
  /// The intersection is useful for recovery, while still falling short of a
  /// concrete proof that bytes at those holders match the symbolic digest.
  def committedContentAndPrepareEvidenceIntersect: bool =
    gatewayJournal.committed implies
      currentEntry.permit.content.holders
        .intersect(currentEntry.permit.prepare.voters)
        .size() > 0

  def exactRetryReturnsJournalResultWithoutNewEffects: bool =
    gatewayLastDecision == GatewayExactRetryReplayed implies {
      val publication = currentEntry.permit.publication
      all {
        gatewayJournal.committed,
        gatewayLastSuppliedPayload == Some(publication.payloadDigest),
        gatewayLastResponse
          == GatewayResultReturned(publication.resultDigest),
        gatewayCanonical == gatewayAudit.canonicalBefore,
        gatewayJournal == gatewayAudit.journalBefore,
        gatewayMaterialized == gatewayAudit.materializedBefore,
      }
    }

  def changedPayloadRetryIsRejectedWithoutNewEffects: bool =
    gatewayLastDecision == GatewayChangedRetryRejected implies {
      val publication = currentEntry.permit.publication
      all {
        gatewayJournal.committed,
        gatewayLastSuppliedPayload != Some(publication.payloadDigest),
        gatewayLastResponse == GatewayPayloadMismatch,
        gatewayCanonical == gatewayAudit.canonicalBefore,
        gatewayJournal == gatewayAudit.journalBefore,
        gatewayMaterialized == gatewayAudit.materializedBefore,
      }
    }

  def staleAndInsufficientRejectionsCannotPublish: bool =
    any {
      gatewayLastDecision == GatewayStalePublicationRejected,
      gatewayLastDecision == GatewayInsufficientEvidenceRejected,
    } implies all {
      gatewayCanonical == gatewayAudit.canonicalBefore,
      gatewayMaterialized == gatewayAudit.materializedBefore,
      gatewayJournal == emptyJournalState,
      gatewayPhase == GatewayIdle,
    }

  def strongHeadMaterializationMovesWithEnvironmentCommit: bool =
    gatewayLastDecision == GatewayStrongHeadAdvanced implies all {
      gatewayMaterialized.objectRevision
        == gatewayAudit.materializedBefore.objectRevision + 1,
      gatewayMaterialized.resultRecord
        == gatewayAudit.materializedBefore.resultRecord,
      gatewayMaterialized.conflictArtifact
        == gatewayAudit.materializedBefore.conflictArtifact,
    }

  def witnessPermitBuilt: bool =
    gatewayLastDecision == GatewayPermitBuilt

  def witnessApplyCommitted: bool = all {
    committedGatewayDecision(gatewayLastDecision),
    currentEntry.permit.publication.outcome == PublishApply,
  }

  def witnessConvergedCommitted: bool = all {
    committedGatewayDecision(gatewayLastDecision),
    currentEntry.permit.publication.outcome == PublishConverged,
  }

  def witnessConflictCommitted: bool = all {
    committedGatewayDecision(gatewayLastDecision),
    currentEntry.permit.publication.outcome == PublishConflict,
    currentEntry.permit.publication.conflictArtifact != None,
  }

  def witnessUncommittedAppendDiscardedByCrash: bool = all {
    gatewayLastDecision == GatewayProcessCrashedBeforeCommit,
    gatewayAudit.journalBefore.entry != None,
    not(gatewayAudit.journalBefore.committed),
    gatewayJournal == emptyJournalState,
  }

  def witnessCommittedEntrySurvivesCrash: bool = all {
    gatewayLastDecision == GatewayProcessCrashedAfterCommit,
    gatewayJournal.committed,
    gatewayJournal.entry != None,
  }

  def witnessCommittedEntryMaterialized: bool =
    gatewayLastDecision == GatewayCommittedEntryMaterialized

  def witnessExactRetryFromJournal: bool =
    gatewayLastDecision == GatewayExactRetryReplayed

  def witnessChangedPayloadRetryRejected: bool =
    gatewayLastDecision == GatewayChangedRetryRejected

  def witnessStaleHeadRejected: bool = all {
    gatewayLastDecision == GatewayStalePublicationRejected,
    unwrapGatewayPlan(gatewayAudit.planBefore).journalHead
      != gatewayAudit.canonicalBefore.journalHead,
  }

  def witnessStaleEpochRejected: bool = all {
    gatewayLastDecision == GatewayStalePublicationRejected,
    unwrapGatewayPlan(gatewayAudit.planBefore).authorityEpoch
      != gatewayAudit.canonicalBefore.authorityEpoch,
  }

  def witnessInsufficientEvidenceRejected: bool =
    gatewayLastDecision == GatewayInsufficientEvidenceRejected

  /// Deterministic full-safety path for an uncommitted append lost on crash.
  action gatewayPrecommitCrashFullSafetyStep: bool =
    if (gatewayPhase == GatewayIdle) {
      if (gatewayLastDecision == GatewayProcessRestarted) {
        stutterGateway
      } else {
        planReconcilePublication(PayloadA, PublishApply)
      }
    } else if (gatewayPhase == GatewayPlanned) {
      if (gatewayLastDecision == GatewayPlanCreated) {
        collectGatewayQuorumAcks
      } else if (gatewayLastDecision == GatewayQuorumAcksCollected) {
        collectGatewayContentReceipts
      } else if (gatewayLastDecision == GatewayContentReceiptsCollected) {
        collectGatewayPrepareAcks
      } else {
        buildReconcilePermit
      }
    } else if (gatewayPhase == GatewayPermitReady) {
      appendReconcileEntry
    } else if (gatewayPhase == GatewayEntryAppended) {
      crashGatewayBeforeCommit
    } else if (gatewayPhase == GatewayStopped) {
      restartGatewayProcess
    } else {
      stutterGateway
    }

  /// Deterministic full-safety path for commit with a lost reply, crash before
  /// table materialization, journal recovery, exact retry, and changed retry.
  action gatewayPostcommitRecoveryFullSafetyStep: bool =
    if (gatewayPhase == GatewayIdle) {
      planReconcilePublication(PayloadA, PublishConflict)
    } else if (gatewayPhase == GatewayPlanned) {
      if (gatewayLastDecision == GatewayPlanCreated) {
        collectGatewayQuorumAcks
      } else if (gatewayLastDecision == GatewayQuorumAcksCollected) {
        collectGatewayContentReceipts
      } else if (gatewayLastDecision == GatewayContentReceiptsCollected) {
        collectGatewayPrepareAcks
      } else {
        buildReconcilePermit
      }
    } else if (gatewayPhase == GatewayPermitReady) {
      appendReconcileEntry
    } else if (gatewayPhase == GatewayEntryAppended) {
      commitReconcileEntry(false)
    } else if (all {
      gatewayPhase == GatewayCommitted,
      gatewayLastDecision == GatewayReconcileCommittedReplyLost,
    }) {
      crashGatewayAfterCommit
    } else if (gatewayPhase == GatewayStopped) {
      restartGatewayProcess
    } else if (gatewayPhase == GatewayCommitted) {
      materializeCommittedReconcileEntry
    } else if (all {
      gatewayPhase == GatewayMaterialized,
      gatewayLastDecision == GatewayCommittedEntryMaterialized,
    }) {
      retryCommittedResult(PayloadA)
    } else if (gatewayLastDecision == GatewayExactRetryReplayed) {
      retryCommittedResult(PayloadB)
    } else {
      stutterGateway
    }

  /// Two stale-entry paths: an unrelated canonical commit first changes the
  /// predecessor head; after replanning, a committed authority movement
  /// changes both epoch and head. Neither old append can commit.
  action gatewayFencingFullSafetyStep: bool =
    if (gatewayPhase == GatewayIdle) {
      if (gatewayCanonical.authorityEpoch == 1) {
        planReconcilePublication(PayloadA, PublishApply)
      } else {
        stutterGateway
      }
    } else if (gatewayPhase == GatewayPlanned) {
      if (gatewayLastDecision == GatewayPlanCreated) {
        collectGatewayQuorumAcks
      } else if (gatewayLastDecision == GatewayQuorumAcksCollected) {
        collectGatewayContentReceipts
      } else if (gatewayLastDecision == GatewayContentReceiptsCollected) {
        collectGatewayPrepareAcks
      } else {
        buildReconcilePermit
      }
    } else if (gatewayPhase == GatewayPermitReady) {
      appendReconcileEntry
    } else if (gatewayPhase == GatewayEntryAppended) {
      if (any {
        gatewayLastDecision == GatewayStrongHeadAdvanced,
        gatewayLastDecision == GatewayAuthorityEpochAdvanced,
      }) {
        rejectStaleGatewayPublication
      } else if (gatewayCanonical.journalHead == 0) {
        advanceStrongCanonicalHead
      } else if (gatewayCanonical.authorityEpoch == 1) {
        advanceGatewayAuthority(N2)
      } else {
        rejectStaleGatewayPublication
      }
    } else {
      stutterGateway
    }

  def safety: bool = all {
    gatewayStateIsWellFormed,
    journalEntryIsSelfConsistent,
    everyGatewayStepRefinesCanonicalProjection,
    commitUsedFreshTypedPermit,
    stalePermitCannotAuthorizeCurrentState,
    appendNeverPublishesCanonicalState,
    precommitCrashDiscardsOnlyVolatileState,
    postcommitCrashPreservesDurableEntryAndResultEnvelope,
    restartPreservesEveryDurableComponent,
    onlyCommittedJournalCanMaterialize,
    materializationReplaysExactCommittedEnvelope,
    publicationOutcomeHasExactCanonicalSemantics,
    committedContentAndPrepareEvidenceIntersect,
    exactRetryReturnsJournalResultWithoutNewEffects,
    changedPayloadRetryIsRejectedWithoutNewEffects,
    staleAndInsufficientRejectionsCannotPublish,
    strongHeadMaterializationMovesWithEnvironmentCommit,
  }
}
