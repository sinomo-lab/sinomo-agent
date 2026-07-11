/// Deterministic crash-boundary trace for the typed reconciliation gateway.
/// It first loses an uncommitted append, then commits a conflict with a lost
/// reply, crashes before table materialization, recovers from the journal, and
/// exercises exact and changed-payload retries.
module dfs_local_first_reconcile_gateway_crash_recovery_scenario {
  import common.* from "../../lib/common"
  import dfs_local_first_reconcile_permit from "../../contracts/dfs/local_first_reconcile_permit"
  import dfs_local_first_reconcile_gateway from "../../models/dfs/local_first_reconcile_gateway"

  type Phase =
    | RecoveryStart
    | FirstPlan
    | FirstQuorum
    | FirstContent
    | FirstPrepare
    | FirstPermit
    | FirstAppend
    | PrecommitCrash
    | PrecommitRestart
    | SecondPlan
    | SecondQuorum
    | SecondContent
    | SecondPrepare
    | SecondPermit
    | SecondAppend
    | CommittedUnknown
    | PostcommitCrash
    | PostcommitRestart
    | RecoveredMaterialization
    | ExactRetry
    | ChangedRetry

  var phase: Phase

  action init = all {
    dfs_local_first_reconcile_gateway::init,
    phase' = RecoveryStart,
  }

  action step =
    if (phase == RecoveryStart) {
      all {
        dfs_local_first_reconcile_gateway::planReconcilePublication(
          dfs_local_first_reconcile_permit::PayloadA,
          dfs_local_first_reconcile_permit::PublishApply
        ),
        phase' = FirstPlan,
      }
    } else if (phase == FirstPlan) {
      all {
        dfs_local_first_reconcile_gateway::collectGatewayQuorumAcks,
        phase' = FirstQuorum,
      }
    } else if (phase == FirstQuorum) {
      all {
        dfs_local_first_reconcile_gateway::collectGatewayContentReceipts,
        phase' = FirstContent,
      }
    } else if (phase == FirstContent) {
      all {
        dfs_local_first_reconcile_gateway::collectGatewayPrepareAcks,
        phase' = FirstPrepare,
      }
    } else if (phase == FirstPrepare) {
      all {
        dfs_local_first_reconcile_gateway::buildReconcilePermit,
        phase' = FirstPermit,
      }
    } else if (phase == FirstPermit) {
      all {
        dfs_local_first_reconcile_gateway::appendReconcileEntry,
        phase' = FirstAppend,
      }
    } else if (phase == FirstAppend) {
      all {
        dfs_local_first_reconcile_gateway::crashGatewayBeforeCommit,
        phase' = PrecommitCrash,
      }
    } else if (phase == PrecommitCrash) {
      all {
        dfs_local_first_reconcile_gateway::restartGatewayProcess,
        phase' = PrecommitRestart,
      }
    } else if (phase == PrecommitRestart) {
      all {
        dfs_local_first_reconcile_gateway::planReconcilePublication(
          dfs_local_first_reconcile_permit::PayloadA,
          dfs_local_first_reconcile_permit::PublishConflict
        ),
        phase' = SecondPlan,
      }
    } else if (phase == SecondPlan) {
      all {
        dfs_local_first_reconcile_gateway::collectGatewayQuorumAcks,
        phase' = SecondQuorum,
      }
    } else if (phase == SecondQuorum) {
      all {
        dfs_local_first_reconcile_gateway::collectGatewayContentReceipts,
        phase' = SecondContent,
      }
    } else if (phase == SecondContent) {
      all {
        dfs_local_first_reconcile_gateway::collectGatewayPrepareAcks,
        phase' = SecondPrepare,
      }
    } else if (phase == SecondPrepare) {
      all {
        dfs_local_first_reconcile_gateway::buildReconcilePermit,
        phase' = SecondPermit,
      }
    } else if (phase == SecondPermit) {
      all {
        dfs_local_first_reconcile_gateway::appendReconcileEntry,
        phase' = SecondAppend,
      }
    } else if (phase == SecondAppend) {
      all {
        dfs_local_first_reconcile_gateway::commitReconcileEntry(false),
        phase' = CommittedUnknown,
      }
    } else if (phase == CommittedUnknown) {
      all {
        dfs_local_first_reconcile_gateway::crashGatewayAfterCommit,
        phase' = PostcommitCrash,
      }
    } else if (phase == PostcommitCrash) {
      all {
        dfs_local_first_reconcile_gateway::restartGatewayProcess,
        phase' = PostcommitRestart,
      }
    } else if (phase == PostcommitRestart) {
      all {
        dfs_local_first_reconcile_gateway::materializeCommittedReconcileEntry,
        phase' = RecoveredMaterialization,
      }
    } else if (phase == RecoveredMaterialization) {
      all {
        dfs_local_first_reconcile_gateway::retryCommittedResult(
          dfs_local_first_reconcile_permit::PayloadA
        ),
        phase' = ExactRetry,
      }
    } else if (phase == ExactRetry) {
      all {
        dfs_local_first_reconcile_gateway::retryCommittedResult(
          dfs_local_first_reconcile_permit::PayloadB
        ),
        phase' = ChangedRetry,
      }
    } else {
      all {
        phase == ChangedRetry,
        dfs_local_first_reconcile_gateway::stutterGateway,
        phase' = phase,
      }
    }

  def safety: bool = all {
    dfs_local_first_reconcile_gateway::gatewayCanonical
      .canonicalConflictCount == 0,
    match phase {
      | FirstAppend => all {
          not(dfs_local_first_reconcile_gateway::gatewayJournal.committed),
          dfs_local_first_reconcile_gateway::gatewayCanonical.journalHead == 0,
        }
      | PrecommitCrash => all {
          dfs_local_first_reconcile_gateway::gatewayJournal
            == dfs_local_first_reconcile_gateway::emptyJournalState,
          dfs_local_first_reconcile_gateway::gatewayCanonical.journalHead == 0,
          dfs_local_first_reconcile_gateway::gatewayMaterialized.resultRecord
            == None,
        }
      | CommittedUnknown => all {
          dfs_local_first_reconcile_gateway::gatewayJournal.committed,
          dfs_local_first_reconcile_gateway::gatewayCanonical.journalHead == 1,
          dfs_local_first_reconcile_gateway::gatewayMaterialized.resultRecord
            == None,
          dfs_local_first_reconcile_gateway::gatewayLastResponse
            == dfs_local_first_reconcile_gateway::GatewayOutcomeUnknown,
        }
      | PostcommitCrash => all {
          dfs_local_first_reconcile_gateway::gatewayJournal.committed,
          dfs_local_first_reconcile_gateway::gatewayJournal.entry != None,
          dfs_local_first_reconcile_gateway::gatewayMaterialized.resultRecord
            == None,
        }
      | RecoveredMaterialization => all {
          dfs_local_first_reconcile_gateway::gatewayJournal.committed,
          dfs_local_first_reconcile_gateway::gatewayMaterialized.resultRecord
            != None,
          dfs_local_first_reconcile_gateway::gatewayMaterialized.conflictArtifact
            == Some(dfs_local_first_reconcile_permit::ArtifactA),
          dfs_local_first_reconcile_gateway::gatewayCanonical.objectRevision == 0,
        }
      | ExactRetry =>
          dfs_local_first_reconcile_gateway::gatewayLastResponse
            == dfs_local_first_reconcile_gateway::GatewayResultReturned(
              dfs_local_first_reconcile_permit::ResultConflict
            )
      | ChangedRetry =>
          dfs_local_first_reconcile_gateway::gatewayLastResponse
            == dfs_local_first_reconcile_gateway::GatewayPayloadMismatch
      | _ => true
    },
  }
}

/// Deterministic stale-head and stale-epoch fencing trace. Each stale append is
/// explicitly rejected before a new plan is created.
module dfs_local_first_reconcile_gateway_fencing_scenario {
  import common.* from "../../lib/common"
  import dfs_local_first_reconcile_permit from "../../contracts/dfs/local_first_reconcile_permit"
  import dfs_local_first_reconcile_gateway from "../../models/dfs/local_first_reconcile_gateway"

  type Phase =
    | FenceStart
    | HeadPlan
    | HeadQuorum
    | HeadContent
    | HeadPrepare
    | HeadPermit
    | HeadAppend
    | HeadAdvanced
    | HeadRejected
    | EpochPlan
    | EpochQuorum
    | EpochContent
    | EpochPrepare
    | EpochPermit
    | EpochAppend
    | EpochAdvanced
    | EpochRejected

  var phase: Phase

  action init = all {
    dfs_local_first_reconcile_gateway::init,
    phase' = FenceStart,
  }

  action step =
    if (phase == FenceStart) {
      all {
        dfs_local_first_reconcile_gateway::planReconcilePublication(
          dfs_local_first_reconcile_permit::PayloadA,
          dfs_local_first_reconcile_permit::PublishApply
        ),
        phase' = HeadPlan,
      }
    } else if (phase == HeadPlan) {
      all {
        dfs_local_first_reconcile_gateway::collectGatewayQuorumAcks,
        phase' = HeadQuorum,
      }
    } else if (phase == HeadQuorum) {
      all {
        dfs_local_first_reconcile_gateway::collectGatewayContentReceipts,
        phase' = HeadContent,
      }
    } else if (phase == HeadContent) {
      all {
        dfs_local_first_reconcile_gateway::collectGatewayPrepareAcks,
        phase' = HeadPrepare,
      }
    } else if (phase == HeadPrepare) {
      all {
        dfs_local_first_reconcile_gateway::buildReconcilePermit,
        phase' = HeadPermit,
      }
    } else if (phase == HeadPermit) {
      all {
        dfs_local_first_reconcile_gateway::appendReconcileEntry,
        phase' = HeadAppend,
      }
    } else if (phase == HeadAppend) {
      all {
        dfs_local_first_reconcile_gateway::advanceStrongCanonicalHead,
        phase' = HeadAdvanced,
      }
    } else if (phase == HeadAdvanced) {
      all {
        dfs_local_first_reconcile_gateway::rejectStaleGatewayPublication,
        phase' = HeadRejected,
      }
    } else if (phase == HeadRejected) {
      all {
        dfs_local_first_reconcile_gateway::planReconcilePublication(
          dfs_local_first_reconcile_permit::PayloadA,
          dfs_local_first_reconcile_permit::PublishApply
        ),
        phase' = EpochPlan,
      }
    } else if (phase == EpochPlan) {
      all {
        dfs_local_first_reconcile_gateway::collectGatewayQuorumAcks,
        phase' = EpochQuorum,
      }
    } else if (phase == EpochQuorum) {
      all {
        dfs_local_first_reconcile_gateway::collectGatewayContentReceipts,
        phase' = EpochContent,
      }
    } else if (phase == EpochContent) {
      all {
        dfs_local_first_reconcile_gateway::collectGatewayPrepareAcks,
        phase' = EpochPrepare,
      }
    } else if (phase == EpochPrepare) {
      all {
        dfs_local_first_reconcile_gateway::buildReconcilePermit,
        phase' = EpochPermit,
      }
    } else if (phase == EpochPermit) {
      all {
        dfs_local_first_reconcile_gateway::appendReconcileEntry,
        phase' = EpochAppend,
      }
    } else if (phase == EpochAppend) {
      all {
        dfs_local_first_reconcile_gateway::advanceGatewayAuthority(N2),
        phase' = EpochAdvanced,
      }
    } else if (phase == EpochAdvanced) {
      all {
        dfs_local_first_reconcile_gateway::rejectStaleGatewayPublication,
        phase' = EpochRejected,
      }
    } else {
      all {
        phase == EpochRejected,
        dfs_local_first_reconcile_gateway::stutterGateway,
        phase' = phase,
      }
    }

  def safety: bool = all {
    dfs_local_first_reconcile_gateway::gatewayCanonical
      .canonicalConflictCount == 0,
    match phase {
      | HeadAdvanced => all {
          dfs_local_first_reconcile_gateway::gatewayCanonical.journalHead == 1,
          dfs_local_first_reconcile_gateway::gatewayJournal.entry != None,
          not(dfs_local_first_reconcile_gateway::gatewayJournal.committed),
          not(dfs_local_first_reconcile_gateway::currentPermitReady(
            dfs_local_first_reconcile_gateway::unwrapGatewayPermit(
              dfs_local_first_reconcile_gateway::gatewayPermit
            )
          )),
        }
      | HeadRejected => all {
          dfs_local_first_reconcile_gateway::gatewayJournal
            == dfs_local_first_reconcile_gateway::emptyJournalState,
          dfs_local_first_reconcile_gateway::gatewayCanonical.journalHead == 1,
        }
      | EpochAdvanced => all {
          dfs_local_first_reconcile_gateway::gatewayCanonical.authorityEpoch == 2,
          dfs_local_first_reconcile_gateway::gatewayCanonical.primary == N2,
          dfs_local_first_reconcile_gateway::gatewayCanonical.journalHead == 2,
          dfs_local_first_reconcile_gateway::gatewayJournal.entry != None,
          not(dfs_local_first_reconcile_gateway::gatewayJournal.committed),
          not(dfs_local_first_reconcile_gateway::currentPermitReady(
            dfs_local_first_reconcile_gateway::unwrapGatewayPermit(
              dfs_local_first_reconcile_gateway::gatewayPermit
            )
          )),
        }
      | EpochRejected => all {
          dfs_local_first_reconcile_gateway::gatewayCanonical.authorityEpoch == 2,
          dfs_local_first_reconcile_gateway::gatewayJournal
            == dfs_local_first_reconcile_gateway::emptyJournalState,
          dfs_local_first_reconcile_gateway::gatewayMaterialized.resultRecord
            == None,
        }
      | _ => true
    },
  }
}
