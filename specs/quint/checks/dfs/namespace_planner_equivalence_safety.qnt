/// Safety and equivalence checks for the two-object namespace planner.
module dfs_namespace_planner_equivalence_safety {
  import common.* from "../../lib/common"
  import dfs_namespace_planner_equivalence.* from "../../models/dfs/namespace_planner_equivalence"

  def preparedPlansAreEquivalent: bool =
    phase == Compared implies all {
      referenceResult.firstPlan == optimizedResult.firstPlan,
      referenceResult.secondPlan == optimizedResult.secondPlan,
    }

  def planningStatesAreEquivalent: bool =
    phase == Compared implies
      referenceResult.planningTerminal == optimizedResult.planningTerminal

  def terminalStatesAreEquivalent: bool =
    phase == Compared implies referenceResult.terminal == optimizedResult.terminal

  def dependentSecondPlanObservedTheFirstPreapply: bool =
    phase == Compared implies all {
      referenceResult.secondPlan.dependencyReady,
      optimizedResult.secondPlan.dependencyReady,
    }

  def optimizationEliminatesCloneAndReplayWork: bool =
    phase == Compared implies all {
      referenceResult.tableClones == 1,
      optimizedResult.tableClones == 0,
      referenceResult.planningApplications == 3,
      referenceResult.replayApplications == 3,
      optimizedResult.planningApplications == 3,
      optimizedResult.replayApplications == 0,
    }

  def successfulAppendHasOneJournalEntryAndOneTableBatch: bool =
    selectedOutcome == Some(CommitSucceeds) implies all {
      referenceResult.terminal.transactionRecorded,
      optimizedResult.terminal.transactionRecorded,
      referenceResult.terminal.journalEntries == 1,
      optimizedResult.terminal.journalEntries == 1,
      referenceResult.terminal.tableBatches == 1,
      optimizedResult.terminal.tableBatches == 1,
    }

  def failedAppendRestoresOriginalTables: bool =
    selectedOutcome == Some(TableBatchAppendFails) implies all {
      not(referenceResult.terminal.transactionRecorded),
      not(optimizedResult.terminal.transactionRecorded),
      referenceResult.terminal.objectRows == initialTables.objectRows,
      optimizedResult.terminal.objectRows == initialTables.objectRows,
      referenceResult.terminal.pathRows == initialTables.pathRows,
      optimizedResult.terminal.pathRows == initialTables.pathRows,
      referenceResult.terminal.dentryRows == initialTables.dentryRows,
      optimizedResult.terminal.dentryRows == initialTables.dentryRows,
      referenceResult.terminal.journalEntries == 1,
      optimizedResult.terminal.journalEntries == 1,
      referenceResult.terminal.tableBatches == 0,
      optimizedResult.terminal.tableBatches == 0,
    }

  def failedAppendHasNoPartialParentChildVisibility: bool =
    selectedOutcome == Some(TableBatchAppendFails) implies all {
      optimizedResult.terminal.objectRows.get(Parent)
        == { kind: Live, path: ParentOld },
      optimizedResult.terminal.objectRows.get(Child)
        == { kind: Live, path: ChildOld },
      optimizedResult.terminal.pathRows.get(ParentOld) == Some(Parent),
      optimizedResult.terminal.pathRows.get(ChildOld) == Some(Child),
      optimizedResult.terminal.pathRows.get(ParentNew) == None,
      optimizedResult.terminal.pathRows.get(ChildNew) == None,
      optimizedResult.terminal.dentryRows.get(ParentOld) == Some(Parent),
      optimizedResult.terminal.dentryRows.get(ChildOld) == Some(Child),
      optimizedResult.terminal.dentryRows.get(ParentNew) == None,
      optimizedResult.terminal.dentryRows.get(ChildNew) == None,
    }

  def moveTerminalIsComplete: bool =
    all {
      selectedScenario == Some(MoveTree),
      selectedOutcome == Some(CommitSucceeds),
    } implies all {
      optimizedResult.terminal.objectRows.get(Parent)
        == { kind: Live, path: ParentNew },
      optimizedResult.terminal.objectRows.get(Child)
        == { kind: Live, path: ChildNew },
      optimizedResult.terminal.pathRows.get(ParentOld) == None,
      optimizedResult.terminal.pathRows.get(ChildOld) == None,
      optimizedResult.terminal.pathRows.get(ParentNew) == Some(Parent),
      optimizedResult.terminal.pathRows.get(ChildNew) == Some(Child),
      optimizedResult.terminal.dentryRows.get(ParentOld) == None,
      optimizedResult.terminal.dentryRows.get(ChildOld) == None,
      optimizedResult.terminal.dentryRows.get(ParentNew) == Some(Parent),
      optimizedResult.terminal.dentryRows.get(ChildNew) == Some(Child),
    }

  def deleteTerminalIsComplete: bool =
    all {
      selectedScenario == Some(DeleteTree),
      selectedOutcome == Some(CommitSucceeds),
    } implies all {
      optimizedResult.terminal.objectRows.get(Parent)
        == { kind: Tombstone, path: ParentOld },
      optimizedResult.terminal.objectRows.get(Child)
        == { kind: Tombstone, path: ChildOld },
      optimizedResult.terminal.pathRows.get(ParentOld) == Some(Parent),
      optimizedResult.terminal.pathRows.get(ChildOld) == Some(Child),
      optimizedResult.terminal.pathRows.get(ParentNew) == None,
      optimizedResult.terminal.pathRows.get(ChildNew) == None,
      optimizedResult.terminal.dentryRows.get(ParentOld) == None,
      optimizedResult.terminal.dentryRows.get(ChildOld) == None,
      optimizedResult.terminal.dentryRows.get(ParentNew) == None,
      optimizedResult.terminal.dentryRows.get(ChildNew) == None,
    }

  def witnessMoveTreeEquivalent: bool = all {
    selectedScenario == Some(MoveTree),
    selectedOutcome == Some(CommitSucceeds),
    terminalStatesAreEquivalent,
    optimizedResult.secondPlan.dependencyReady,
  }

  def witnessDeleteTreeEquivalent: bool = all {
    selectedScenario == Some(DeleteTree),
    selectedOutcome == Some(CommitSucceeds),
    terminalStatesAreEquivalent,
    optimizedResult.secondPlan.dependencyReady,
  }

  def witnessFailedAppendRollbackEquivalent: bool = all {
    selectedOutcome == Some(TableBatchAppendFails),
    terminalStatesAreEquivalent,
    failedAppendRestoresOriginalTables,
    failedAppendHasNoPartialParentChildVisibility,
  }

  def safety: bool = all {
    preparedPlansAreEquivalent,
    planningStatesAreEquivalent,
    terminalStatesAreEquivalent,
    dependentSecondPlanObservedTheFirstPreapply,
    optimizationEliminatesCloneAndReplayWork,
    successfulAppendHasOneJournalEntryAndOneTableBatch,
    failedAppendRestoresOriginalTables,
    failedAppendHasNoPartialParentChildVisibility,
    moveTerminalIsComplete,
    deleteTerminalIsComplete,
  }
}
