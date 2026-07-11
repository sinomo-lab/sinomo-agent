/// Deterministic local namespace durability and crash-recovery trace.
///
/// The same returned mutation is first lost because the containing directory
/// was not locally fsynced. It is then replayed, directory-fsynced, recovered
/// after a second crash, and sealed from that exact durable image.
module dfs_local_first_namespace_durability_recovery_scenario {
  import common.* from "../../lib/common"
  import dfs_local_first_namespace_durability from "../../models/dfs/local_first_namespace_durability"

  type NamespaceDurabilityScenarioPhase =
    | DurabilityScenarioStart
    | DurabilityScenarioBranchOpen
    | DurabilityScenarioVolatileReturned
    | DurabilityScenarioVolatileLost
    | DurabilityScenarioMutationReplayed
    | DurabilityScenarioDirectorySynced
    | DurabilityScenarioDurableRecovered
    | DurabilityScenarioSealed

  var lfndsPhase: NamespaceDurabilityScenarioPhase

  action init = all {
    dfs_local_first_namespace_durability::init,
    lfndsPhase' = DurabilityScenarioStart,
  }

  action step =
    if (lfndsPhase == DurabilityScenarioStart) {
      all {
        dfs_local_first_namespace_durability::beginNamespaceDurabilityBranch,
        lfndsPhase' = DurabilityScenarioBranchOpen,
      }
    } else if (lfndsPhase == DurabilityScenarioBranchOpen) {
      all {
        dfs_local_first_namespace_durability::returnNamespaceMutation,
        lfndsPhase' = DurabilityScenarioVolatileReturned,
      }
    } else if (lfndsPhase == DurabilityScenarioVolatileReturned) {
      all {
        dfs_local_first_namespace_durability::crashNamespaceProcess,
        lfndsPhase' = DurabilityScenarioVolatileLost,
      }
    } else if (lfndsPhase == DurabilityScenarioVolatileLost) {
      all {
        dfs_local_first_namespace_durability::returnNamespaceMutation,
        lfndsPhase' = DurabilityScenarioMutationReplayed,
      }
    } else if (lfndsPhase == DurabilityScenarioMutationReplayed) {
      all {
        dfs_local_first_namespace_durability::fsyncLocalDirectory,
        lfndsPhase' = DurabilityScenarioDirectorySynced,
      }
    } else if (lfndsPhase == DurabilityScenarioDirectorySynced) {
      all {
        dfs_local_first_namespace_durability::crashNamespaceProcess,
        lfndsPhase' = DurabilityScenarioDurableRecovered,
      }
    } else if (lfndsPhase == DurabilityScenarioDurableRecovered) {
      all {
        dfs_local_first_namespace_durability::sealDurableNamespaceBranch,
        lfndsPhase' = DurabilityScenarioSealed,
      }
    } else {
      all {
        lfndsPhase == DurabilityScenarioSealed,
        dfs_local_first_namespace_durability::stutterSealedNamespaceBranch,
        lfndsPhase' = lfndsPhase,
      }
    }

  def lfndsPhaseHasExactDurabilityImage: bool =
    match lfndsPhase {
      | DurabilityScenarioStart => all {
          dfs_local_first_namespace_durability::lfndPhase
            == dfs_local_first_namespace_durability::NamespaceBranchAbsent,
          dfs_local_first_namespace_durability::lfndOverlay
            == dfs_local_first_namespace_durability::NamespaceBase,
        }
      | DurabilityScenarioBranchOpen =>
          dfs_local_first_namespace_durability::lfndPhase
            == dfs_local_first_namespace_durability::NamespaceBranchOpen
      | DurabilityScenarioVolatileReturned => all {
          dfs_local_first_namespace_durability::lfndOverlay
            == dfs_local_first_namespace_durability::NamespaceEntryCreated,
          dfs_local_first_namespace_durability::lfndDurableOverlay
            == dfs_local_first_namespace_durability::NamespaceBase,
          dfs_local_first_namespace_durability::lfndReturnCount == 1,
        }
      | DurabilityScenarioVolatileLost => all {
          dfs_local_first_namespace_durability::lfndLastDecision
            == dfs_local_first_namespace_durability::NamespaceProcessCrashed,
          dfs_local_first_namespace_durability::lfndOverlay
            == dfs_local_first_namespace_durability::NamespaceBase,
          dfs_local_first_namespace_durability::lfndDurableOverlay
            == dfs_local_first_namespace_durability::NamespaceBase,
        }
      | DurabilityScenarioMutationReplayed => all {
          dfs_local_first_namespace_durability::lfndOverlay
            == dfs_local_first_namespace_durability::NamespaceEntryCreated,
          dfs_local_first_namespace_durability::lfndDurableOverlay
            == dfs_local_first_namespace_durability::NamespaceBase,
          dfs_local_first_namespace_durability::lfndReturnCount == 2,
          dfs_local_first_namespace_durability::lfndMutationId
            == dfs_local_first_namespace_durability::NamespaceMutation1,
        }
      | DurabilityScenarioDirectorySynced => all {
          dfs_local_first_namespace_durability::lfndLastDecision
            == dfs_local_first_namespace_durability::NamespaceDirectoryFsyncReturned,
          dfs_local_first_namespace_durability::lfndOverlay
            == dfs_local_first_namespace_durability::NamespaceEntryCreated,
          dfs_local_first_namespace_durability::lfndDurableOverlay
            == dfs_local_first_namespace_durability::NamespaceEntryCreated,
          dfs_local_first_namespace_durability::lfndDirectorySyncGeneration == 1,
        }
      | DurabilityScenarioDurableRecovered => all {
          dfs_local_first_namespace_durability::lfndLastDecision
            == dfs_local_first_namespace_durability::NamespaceProcessCrashed,
          dfs_local_first_namespace_durability::lfndOverlay
            == dfs_local_first_namespace_durability::NamespaceEntryCreated,
          dfs_local_first_namespace_durability::lfndDurableOverlay
            == dfs_local_first_namespace_durability::NamespaceEntryCreated,
        }
      | DurabilityScenarioSealed => all {
          dfs_local_first_namespace_durability::lfndPhase
            == dfs_local_first_namespace_durability::NamespaceBranchSealed,
          dfs_local_first_namespace_durability::lfndSealedOverlay
            == Some(dfs_local_first_namespace_durability::NamespaceEntryCreated),
          dfs_local_first_namespace_durability::lfndSealedGeneration == 1,
          dfs_local_first_namespace_durability::lfndCanonicalRevision == 0,
        }
    }

  def safety: bool = lfndsPhaseHasExactDurabilityImage
}
