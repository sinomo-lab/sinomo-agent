/// Bounded reconciliation traces for same-shard local-first namespace work.
///
/// The selected trace demonstrates one of the four terminal classifications.
/// The conflict trace additionally prepares an applicable plan, advances the
/// canonical head, rejects that stale plan, replans, records the canonical
/// rename winner, preserves the local object, and retries the exact result.
module dfs_local_first_namespace_reconcile_scenario {
  import common.* from "../../lib/common"
  import dfs_local_first_namespace from "../../models/dfs/local_first_namespace"

  type NamespaceScenario =
    | ApplicableScenario
    | ConvergedScenario
    | ConflictScenario
    | SupersededScenario

  type NamespaceScenarioPhase =
    | ScenarioStart
    | ScenarioBranchOpen
    | ScenarioLocalStaged
    | ScenarioSealed
    | ScenarioPlanBeforeConflict
    | ScenarioPlanBecameStale
    | ScenarioReadyToReplan
    | ScenarioConcurrentCommitted
    | ScenarioFreshPlan
    | ScenarioCompleted
    | ScenarioRetried

  pure val NamespaceScenarios: Set[NamespaceScenario] = Set(
    ApplicableScenario,
    ConvergedScenario,
    ConflictScenario,
    SupersededScenario
  )

  var namespaceScenario: NamespaceScenario
  var namespaceScenarioPhase: NamespaceScenarioPhase

  action init = all {
    dfs_local_first_namespace::init,
    namespaceScenario' = ApplicableScenario,
    namespaceScenarioPhase' = ScenarioStart,
  }

  action step =
    if (namespaceScenarioPhase == ScenarioStart) {
      nondet selectedScenario = oneOf(NamespaceScenarios)
      all {
        dfs_local_first_namespace::beginBranch,
        namespaceScenario' = selectedScenario,
        namespaceScenarioPhase' = ScenarioBranchOpen,
      }
    } else if (namespaceScenarioPhase == ScenarioBranchOpen) {
      all {
        if (namespaceScenario == ApplicableScenario) {
          dfs_local_first_namespace::stageMkdir(
            dfs_local_first_namespace::RootSpareEntry
          )
        } else if (namespaceScenario == ConvergedScenario) {
          dfs_local_first_namespace::stageCreate(
            dfs_local_first_namespace::RootSpareEntry
          )
        } else if (namespaceScenario == ConflictScenario) {
          dfs_local_first_namespace::stageRename(
            dfs_local_first_namespace::RootFileEntry,
            dfs_local_first_namespace::RootSpareEntry
          )
        } else {
          dfs_local_first_namespace::stageUnlink(
            dfs_local_first_namespace::RootFileEntry
          )
        },
        namespaceScenario' = namespaceScenario,
        namespaceScenarioPhase' = ScenarioLocalStaged,
      }
    } else if (namespaceScenarioPhase == ScenarioLocalStaged) {
      all {
        dfs_local_first_namespace::sealBranch,
        namespaceScenario' = namespaceScenario,
        namespaceScenarioPhase' = ScenarioSealed,
      }
    } else if (namespaceScenarioPhase == ScenarioSealed) {
      if (namespaceScenario == ConflictScenario) {
        all {
          dfs_local_first_namespace::prepareReconcilePlan,
          namespaceScenario' = namespaceScenario,
          namespaceScenarioPhase' = ScenarioPlanBeforeConflict,
        }
      } else if (namespaceScenario == ConvergedScenario) {
        all {
          dfs_local_first_namespace::commitStrongEquivalent,
          namespaceScenario' = namespaceScenario,
          namespaceScenarioPhase' = ScenarioConcurrentCommitted,
        }
      } else if (namespaceScenario == SupersededScenario) {
        all {
          dfs_local_first_namespace::commitStrongConflict,
          namespaceScenario' = namespaceScenario,
          namespaceScenarioPhase' = ScenarioConcurrentCommitted,
        }
      } else {
        all {
          namespaceScenario == ApplicableScenario,
          dfs_local_first_namespace::prepareReconcilePlan,
          namespaceScenario' = namespaceScenario,
          namespaceScenarioPhase' = ScenarioFreshPlan,
        }
      }
    } else if (namespaceScenarioPhase == ScenarioPlanBeforeConflict) {
      all {
        namespaceScenario == ConflictScenario,
        dfs_local_first_namespace::commitStrongConflict,
        namespaceScenario' = namespaceScenario,
        namespaceScenarioPhase' = ScenarioPlanBecameStale,
      }
    } else if (namespaceScenarioPhase == ScenarioPlanBecameStale) {
      all {
        namespaceScenario == ConflictScenario,
        dfs_local_first_namespace::rejectStalePlan,
        namespaceScenario' = namespaceScenario,
        namespaceScenarioPhase' = ScenarioReadyToReplan,
      }
    } else if (namespaceScenarioPhase == ScenarioReadyToReplan) {
      all {
        namespaceScenario == ConflictScenario,
        dfs_local_first_namespace::prepareReconcilePlan,
        namespaceScenario' = namespaceScenario,
        namespaceScenarioPhase' = ScenarioFreshPlan,
      }
    } else if (namespaceScenarioPhase == ScenarioConcurrentCommitted) {
      all {
        namespaceScenario != ApplicableScenario,
        namespaceScenario != ConflictScenario,
        dfs_local_first_namespace::prepareReconcilePlan,
        namespaceScenario' = namespaceScenario,
        namespaceScenarioPhase' = ScenarioFreshPlan,
      }
    } else if (namespaceScenarioPhase == ScenarioFreshPlan) {
      all {
        dfs_local_first_namespace::commitReconcilePlan,
        namespaceScenario' = namespaceScenario,
        namespaceScenarioPhase' = ScenarioCompleted,
      }
    } else if (namespaceScenarioPhase == ScenarioCompleted) {
      all {
        dfs_local_first_namespace::retryTerminalResult,
        namespaceScenario' = namespaceScenario,
        namespaceScenarioPhase' = ScenarioRetried,
      }
    } else {
      all {
        namespaceScenarioPhase == ScenarioRetried,
        dfs_local_first_namespace::retryTerminalResult,
        namespaceScenario' = namespaceScenario,
        namespaceScenarioPhase' = namespaceScenarioPhase,
      }
    }

  def selectedOutcomeMatches: bool =
    match namespaceScenario {
      | ApplicableScenario =>
          dfs_local_first_namespace::branchResult
            == dfs_local_first_namespace::Applicable
      | ConvergedScenario =>
          dfs_local_first_namespace::branchResult
            == dfs_local_first_namespace::Converged
      | ConflictScenario =>
          dfs_local_first_namespace::branchResult
            == dfs_local_first_namespace::Conflict
      | SupersededScenario =>
          dfs_local_first_namespace::branchResult
            == dfs_local_first_namespace::Superseded
    }

  def freshPlanHasSelectedClassification: bool =
    namespaceScenarioPhase == ScenarioFreshPlan implies all {
      dfs_local_first_namespace::branchPhase
        == dfs_local_first_namespace::BranchPlanned,
      dfs_local_first_namespace::canonicalHead
        == dfs_local_first_namespace::planHead,
      match namespaceScenario {
        | ApplicableScenario =>
            dfs_local_first_namespace::planOutcome
              == dfs_local_first_namespace::Applicable
        | ConvergedScenario =>
            dfs_local_first_namespace::planOutcome
              == dfs_local_first_namespace::Converged
        | ConflictScenario =>
            dfs_local_first_namespace::planOutcome
              == dfs_local_first_namespace::Conflict
        | SupersededScenario =>
            dfs_local_first_namespace::planOutcome
              == dfs_local_first_namespace::Superseded
      },
    }

  def conflictPlanIsRejectedAndRecomputed: bool =
    match namespaceScenarioPhase {
      | ScenarioPlanBeforeConflict => all {
          namespaceScenario == ConflictScenario,
          dfs_local_first_namespace::branchPhase
            == dfs_local_first_namespace::BranchPlanned,
          dfs_local_first_namespace::planOutcome
            == dfs_local_first_namespace::Applicable,
          dfs_local_first_namespace::canonicalHead
            == dfs_local_first_namespace::planHead,
        }
      | ScenarioPlanBecameStale => all {
          namespaceScenario == ConflictScenario,
          dfs_local_first_namespace::branchPhase
            == dfs_local_first_namespace::BranchPlanned,
          dfs_local_first_namespace::canonicalHead
            != dfs_local_first_namespace::planHead,
        }
      | ScenarioReadyToReplan => all {
          namespaceScenario == ConflictScenario,
          dfs_local_first_namespace::branchPhase
            == dfs_local_first_namespace::BranchSealed,
          dfs_local_first_namespace::planOutcome
            == dfs_local_first_namespace::Unclassified,
        }
      | _ => true
    }

  def terminalScenarioStateIsExact: bool =
    any {
      namespaceScenarioPhase == ScenarioCompleted,
      namespaceScenarioPhase == ScenarioRetried,
    } implies all {
      dfs_local_first_namespace::branchPhase
        == dfs_local_first_namespace::BranchTerminal,
      selectedOutcomeMatches,
      if (namespaceScenario == ApplicableScenario) {
        all {
          dfs_local_first_namespace::canonicalNamespace.get(
            dfs_local_first_namespace::RootSpareEntry
          ).kind == dfs_local_first_namespace::EntryDirectory,
          dfs_local_first_namespace::canonicalNamespace.get(
            dfs_local_first_namespace::RootSpareEntry
          ).object == Some(dfs_local_first_namespace::LocalDirectoryObject),
        }
      } else if (namespaceScenario == ConvergedScenario) {
        all {
          dfs_local_first_namespace::canonicalNamespace.get(
            dfs_local_first_namespace::RootSpareEntry
          ).kind == dfs_local_first_namespace::EntryFile,
          dfs_local_first_namespace::canonicalNamespace.get(
            dfs_local_first_namespace::RootSpareEntry
          ).object == Some(dfs_local_first_namespace::LocalFileObject),
        }
      } else if (namespaceScenario == ConflictScenario) {
        all {
          dfs_local_first_namespace::canonicalNamespace.get(
            dfs_local_first_namespace::RootFileEntry
          ).object == Some(dfs_local_first_namespace::FileObject),
          dfs_local_first_namespace::canonicalNamespace.get(
            dfs_local_first_namespace::RootSpareEntry
          ).object == Some(dfs_local_first_namespace::StrongSpareObject),
          dfs_local_first_namespace::conflictBranches.contains(
            dfs_local_first_namespace::LocalBranchOne
          ),
          dfs_local_first_namespace::preservedObjects.contains(
            dfs_local_first_namespace::FileObject
          ),
        }
      } else {
        all {
          namespaceScenario == SupersededScenario,
          dfs_local_first_namespace::canonicalNamespace.get(
            dfs_local_first_namespace::RootFileEntry
          ).object == Some(dfs_local_first_namespace::StrongFileObject),
        }
      },
    }

  def safety: bool = all {
    freshPlanHasSelectedClassification,
    conflictPlanIsRejectedAndRecomputed,
    terminalScenarioStateIsExact,
  }
}
