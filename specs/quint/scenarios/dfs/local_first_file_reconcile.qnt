/// Deterministic degraded-write, stale-plan, conflict, and lost-reply recovery
/// trace for the regular-file local-first protocol.
module dfs_local_first_file_reconcile_scenario {
  import common.* from "../../lib/common"
  import dfs_local_first_file_branch from "../../models/dfs/local_first_file_branch"

  type LocalFirstFileScenarioPhase =
    | ScenarioStart
    | ScenarioBranchOpen
    | ScenarioVolatileWrite
    | ScenarioVolatileCrashRecovered
    | ScenarioRewritten
    | ScenarioTruncated
    | ScenarioLocallySynced
    | ScenarioDurableCrashRecovered
    | ScenarioSealed
    | ScenarioContentRetained
    | ScenarioAuthorityRestored
    | ScenarioPlanAtBase
    | ScenarioCanonicalAdvanced
    | ScenarioStalePlanRejected
    | ScenarioConflictReplanned
    | ScenarioConflictCommittedUnknown
    | ScenarioReconcileProcessRestarted
    | ScenarioExactRetryConfirmed

  var lfsPhase: LocalFirstFileScenarioPhase

  action init = all {
    dfs_local_first_file_branch::init,
    lfsPhase' = ScenarioStart,
  }

  action step =
    if (lfsPhase == ScenarioStart) {
      all {
        dfs_local_first_file_branch::beginLocalBranch,
        lfsPhase' = ScenarioBranchOpen,
      }
    } else if (lfsPhase == ScenarioBranchOpen) {
      all {
        dfs_local_first_file_branch::localWrite,
        lfsPhase' = ScenarioVolatileWrite,
      }
    } else if (lfsPhase == ScenarioVolatileWrite) {
      all {
        dfs_local_first_file_branch::crashLocalProcess,
        lfsPhase' = ScenarioVolatileCrashRecovered,
      }
    } else if (lfsPhase == ScenarioVolatileCrashRecovered) {
      all {
        dfs_local_first_file_branch::localWrite,
        lfsPhase' = ScenarioRewritten,
      }
    } else if (lfsPhase == ScenarioRewritten) {
      all {
        dfs_local_first_file_branch::localTruncate,
        lfsPhase' = ScenarioTruncated,
      }
    } else if (lfsPhase == ScenarioTruncated) {
      all {
        dfs_local_first_file_branch::localFsync,
        lfsPhase' = ScenarioLocallySynced,
      }
    } else if (lfsPhase == ScenarioLocallySynced) {
      all {
        dfs_local_first_file_branch::crashLocalProcess,
        lfsPhase' = ScenarioDurableCrashRecovered,
      }
    } else if (lfsPhase == ScenarioDurableCrashRecovered) {
      all {
        dfs_local_first_file_branch::sealLocalBranch,
        lfsPhase' = ScenarioSealed,
      }
    } else if (lfsPhase == ScenarioSealed) {
      all {
        dfs_local_first_file_branch::retainSealedContent,
        lfsPhase' = ScenarioContentRetained,
      }
    } else if (lfsPhase == ScenarioContentRetained) {
      all {
        dfs_local_first_file_branch::restoreLocalAuthority,
        lfsPhase' = ScenarioAuthorityRestored,
      }
    } else if (lfsPhase == ScenarioAuthorityRestored) {
      all {
        dfs_local_first_file_branch::planReconciliation(
          dfs_local_first_file_branch::Reconcile1
        ),
        lfsPhase' = ScenarioPlanAtBase,
      }
    } else if (lfsPhase == ScenarioPlanAtBase) {
      all {
        dfs_local_first_file_branch::canonicalConcurrentWrite(
          dfs_local_first_file_branch::RemoteBytes
        ),
        lfsPhase' = ScenarioCanonicalAdvanced,
      }
    } else if (lfsPhase == ScenarioCanonicalAdvanced) {
      all {
        dfs_local_first_file_branch::rejectStalePlan,
        lfsPhase' = ScenarioStalePlanRejected,
      }
    } else if (lfsPhase == ScenarioStalePlanRejected) {
      all {
        dfs_local_first_file_branch::planReconciliation(
          dfs_local_first_file_branch::Reconcile1
        ),
        lfsPhase' = ScenarioConflictReplanned,
      }
    } else if (lfsPhase == ScenarioConflictReplanned) {
      all {
        dfs_local_first_file_branch::conflictFreshPlan(false),
        lfsPhase' = ScenarioConflictCommittedUnknown,
      }
    } else if (lfsPhase == ScenarioConflictCommittedUnknown) {
      all {
        dfs_local_first_file_branch::restartAfterReconciliation,
        lfsPhase' = ScenarioReconcileProcessRestarted,
      }
    } else if (lfsPhase == ScenarioReconcileProcessRestarted) {
      all {
        dfs_local_first_file_branch::retryExactResult(
          dfs_local_first_file_branch::Reconcile1,
          unwrapOr(
            dfs_local_first_file_branch::lfSealedPayload,
            dfs_local_first_file_branch::defaultPayload
          )
        ),
        lfsPhase' = ScenarioExactRetryConfirmed,
      }
    } else {
      all {
        lfsPhase == ScenarioExactRetryConfirmed,
        dfs_local_first_file_branch::stutterTerminal,
        lfsPhase' = lfsPhase,
      }
    }

  def terminalConflictWasRetainedAndReplayed: bool =
    lfsPhase == ScenarioExactRetryConfirmed implies all {
      dfs_local_first_file_branch::lfCanonicalHead == {
        revision: dfs_local_first_file_branch::FileConcurrent,
        content: dfs_local_first_file_branch::RemoteBytes,
      },
      match dfs_local_first_file_branch::lfConflictRecord {
        | Some(conflict) => all {
            conflict.canonicalWinner
              == dfs_local_first_file_branch::lfCanonicalHead,
            conflict.retainedContent
              == dfs_local_first_file_branch::LocalTruncatedBytes,
          }
        | None => false
      },
      match dfs_local_first_file_branch::lfResultRecords.get(
        dfs_local_first_file_branch::Reconcile1
      ) {
        | Some(record) =>
            dfs_local_first_file_branch::lfLastResponse
              == dfs_local_first_file_branch::ReconcileResultReturned({
                requestId: dfs_local_first_file_branch::Reconcile1,
                result: record.result,
              })
        | None => false
      },
    }

  def crashPhasesHaveExpectedLocalImages: bool =
    match lfsPhase {
      | ScenarioVolatileCrashRecovered =>
          dfs_local_first_file_branch::lfOverlayContent
            == dfs_local_first_file_branch::BaseBytes
      | ScenarioDurableCrashRecovered =>
          dfs_local_first_file_branch::lfOverlayContent
            == dfs_local_first_file_branch::LocalTruncatedBytes
      | _ => true
    }

  def restartPreservedTerminalResult: bool =
    lfsPhase == ScenarioReconcileProcessRestarted implies all {
      dfs_local_first_file_branch::lfLastDecision
        == dfs_local_first_file_branch::ReconcileProcessRestarted,
      dfs_local_first_file_branch::lfLastResponse
        == dfs_local_first_file_branch::NoReconcileResponse,
      isSome(dfs_local_first_file_branch::lfResultRecords.get(
        dfs_local_first_file_branch::Reconcile1
      )),
    }

  def safety: bool = all {
    crashPhasesHaveExpectedLocalImages,
    restartPreservedTerminalResult,
    terminalConflictWasRetainedAndReplayed,
  }
}
