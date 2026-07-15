/// DFS two-object namespace planner equivalence.
///
/// Evidence class: target hardening for the metadata hot-path optimization
/// that replaces a full-table clone plus generated-op replay with planning
/// directly against the live in-memory tables while recording rollback.
///
/// The bounded transaction contains one parent directory and one child. A
/// move plans the parent before the child, so the child observes the new
/// parent row. A recursive delete plans the child before the parent, so the
/// parent observes that the child dentry is gone. These are the smallest
/// dependency-bearing cases that distinguish a genuinely preapplied planner
/// from planning both objects against one stale input snapshot.
///
/// The reference algorithm clones the table state, records the transaction,
/// plans and applies both object updates to that scratch state, then replays
/// the prepared plans against the original state. The optimized algorithm
/// records and preapplies each plan in place and durably appends the already
/// applied batch once. Both must produce the same plans and terminal tables.
/// If the journal append is durable but the table-batch append fails, the
/// optimized rollback must restore the original table rows just as the
/// reference path leaves them untouched. Recovery from that journal-only cut,
/// versions, advisory locks, and wider transactions remain outside this slice.
module dfs_namespace_planner_equivalence {
  import common.* from "../../lib/common"

  type TreeObject = Parent | Child
  type ObjectKind = Live | Tombstone
  type TreePath = ParentOld | ParentNew | ChildOld | ChildNew
  type PlannerScenario = MoveTree | DeleteTree
  type PlannerOutcome = CommitSucceeds | TableBatchAppendFails
  type PlannerPhase = Ready | Compared

  type ObjectRow = {
    kind: ObjectKind,
    path: TreePath,
  }

  type ObjectPlan = {
    object: TreeObject,
    previousPath: TreePath,
    targetRow: ObjectRow,
    removePreviousPath: bool,
    removePreviousDentry: bool,
    dependencyReady: bool,
  }

  type TableState = {
    transactionRecorded: bool,
    objectRows: TreeObject -> ObjectRow,
    pathRows: TreePath -> Option[TreeObject],
    dentryRows: TreePath -> Option[TreeObject],
    journalEntries: int,
    tableBatches: int,
  }

  type PlannerResult = {
    firstPlan: ObjectPlan,
    secondPlan: ObjectPlan,
    planningTerminal: TableState,
    terminal: TableState,
    tableClones: int,
    planningApplications: int,
    replayApplications: int,
  }

  pure val TreeObjects: Set[TreeObject] = Set(Parent, Child)
  pure val TreePaths: Set[TreePath] =
    Set(ParentOld, ParentNew, ChildOld, ChildNew)
  pure val PlannerScenarios: Set[PlannerScenario] = Set(MoveTree, DeleteTree)
  pure val PlannerOutcomes: Set[PlannerOutcome] =
    Set(CommitSucceeds, TableBatchAppendFails)

  pure val initialObjectRows: TreeObject -> ObjectRow =
    TreeObjects.mapBy(object =>
      if (object == Parent) {
        { kind: Live, path: ParentOld }
      } else {
        { kind: Live, path: ChildOld }
      }
    )

  pure val emptyPathRows: TreePath -> Option[TreeObject] =
    TreePaths.mapBy(_ => None)

  pure val initialPathRows: TreePath -> Option[TreeObject] =
    emptyPathRows
      .set(ParentOld, Some(Parent))
      .set(ChildOld, Some(Child))

  pure val initialDentryRows: TreePath -> Option[TreeObject] =
    emptyPathRows
      .set(ParentOld, Some(Parent))
      .set(ChildOld, Some(Child))

  pure val initialTables: TableState = {
    transactionRecorded: false,
    objectRows: initialObjectRows,
    pathRows: initialPathRows,
    dentryRows: initialDentryRows,
    journalEntries: 0,
    tableBatches: 0,
  }

  pure val emptyPlan: ObjectPlan = {
    object: Parent,
    previousPath: ParentOld,
    targetRow: { kind: Live, path: ParentOld },
    removePreviousPath: false,
    removePreviousDentry: false,
    dependencyReady: true,
  }

  pure val emptyResult: PlannerResult = {
    firstPlan: emptyPlan,
    secondPlan: emptyPlan,
    planningTerminal: initialTables,
    terminal: initialTables,
    tableClones: 0,
    planningApplications: 0,
    replayApplications: 0,
  }

  var phase: PlannerPhase
  var selectedScenario: Option[PlannerScenario]
  var selectedOutcome: Option[PlannerOutcome]
  var referenceResult: PlannerResult
  var optimizedResult: PlannerResult

  pure def firstObject(scenario: PlannerScenario): TreeObject =
    if (scenario == MoveTree) Parent else Child

  pure def secondObject(scenario: PlannerScenario): TreeObject =
    if (scenario == MoveTree) Child else Parent

  pure def targetRow(scenario: PlannerScenario, object: TreeObject): ObjectRow =
    match scenario {
      | MoveTree =>
          if (object == Parent) {
            { kind: Live, path: ParentNew }
          } else {
            { kind: Live, path: ChildNew }
          }
      | DeleteTree =>
          if (object == Parent) {
            { kind: Tombstone, path: ParentOld }
          } else {
            { kind: Tombstone, path: ChildOld }
          }
    }

  /// The dependency bit is observational. It exposes the state dependency
  /// that a stale, plan-everything-first optimization would miss.
  pure def dependencyIsReady(
    tables: TableState,
    scenario: PlannerScenario,
    object: TreeObject,
  ): bool =
    if (scenario == MoveTree and object == Child) {
      tables.pathRows.get(ParentNew) == Some(Parent)
        and tables.objectRows.get(Parent) == { kind: Live, path: ParentNew }
    } else if (scenario == DeleteTree and object == Parent) {
      tables.dentryRows.get(ChildOld) == None
        and tables.objectRows.get(Child) == { kind: Tombstone, path: ChildOld }
    } else {
      true
    }

  pure def planObject(
    tables: TableState,
    scenario: PlannerScenario,
    object: TreeObject,
  ): ObjectPlan = {
    val previous = tables.objectRows.get(object)
    val target = targetRow(scenario, object)
    {
      object: object,
      previousPath: previous.path,
      targetRow: target,
      removePreviousPath: previous.path != target.path,
      removePreviousDentry:
        previous.kind == Live
          and (target.kind == Tombstone or previous.path != target.path),
      dependencyReady: dependencyIsReady(tables, scenario, object),
    }
  }

  pure def recordTransaction(tables: TableState): TableState = {
    transactionRecorded: true,
    objectRows: tables.objectRows,
    pathRows: tables.pathRows,
    dentryRows: tables.dentryRows,
    journalEntries: tables.journalEntries,
    tableBatches: tables.tableBatches,
  }

  /// Apply a prepared operation group without re-planning it. The reference
  /// replay path and optimized preapply path deliberately share only this
  /// table-operation semantics, just as both Rust paths consume the same
  /// `DfsMetadataTableOp` vocabulary.
  pure def applyPreparedObjectPlan(
    tables: TableState,
    plan: ObjectPlan,
  ): TableState = {
    val pathsAfterRemoval =
      if (plan.removePreviousPath) {
        tables.pathRows.set(plan.previousPath, None)
      } else {
        tables.pathRows
      }
    val dentriesAfterRemoval =
      if (plan.removePreviousDentry) {
        tables.dentryRows.set(plan.previousPath, None)
      } else {
        tables.dentryRows
      }
    val pathsAfterPut = pathsAfterRemoval.set(
      plan.targetRow.path,
      Some(plan.object)
    )
    val dentriesAfterPut =
      if (plan.targetRow.kind == Live) {
        dentriesAfterRemoval.set(plan.targetRow.path, Some(plan.object))
      } else {
        dentriesAfterRemoval.set(plan.targetRow.path, None)
      }
    {
      transactionRecorded: tables.transactionRecorded,
      objectRows: tables.objectRows.set(plan.object, plan.targetRow),
      pathRows: pathsAfterPut,
      dentryRows: dentriesAfterPut,
      journalEntries: tables.journalEntries,
      tableBatches: tables.tableBatches,
    }
  }

  pure def commitPreparedBatch(tables: TableState): TableState = {
    transactionRecorded: tables.transactionRecorded,
    objectRows: tables.objectRows,
    pathRows: tables.pathRows,
    dentryRows: tables.dentryRows,
    journalEntries: tables.journalEntries + 1,
    tableBatches: tables.tableBatches + 1,
  }

  /// Reference failure cut: the journal mutation is durable, but no live
  /// table rows were changed before the table-batch append failed.
  pure def failReferenceTableBatchAppend(tables: TableState): TableState = {
    transactionRecorded: tables.transactionRecorded,
    objectRows: tables.objectRows,
    pathRows: tables.pathRows,
    dentryRows: tables.dentryRows,
    journalEntries: tables.journalEntries + 1,
    tableBatches: tables.tableBatches,
  }

  /// Optimized failure cut: planning already mutated the in-memory tables, so
  /// the captured rollback restores every table family to its pre-plan value.
  /// The durable journal mutation remains and the failed table batch does not.
  pure def rollbackFailedPreappliedBatch(
    preapplied: TableState,
    before: TableState,
  ): TableState = {
    transactionRecorded: before.transactionRecorded,
    objectRows: before.objectRows,
    pathRows: before.pathRows,
    dentryRows: before.dentryRows,
    journalEntries: preapplied.journalEntries + 1,
    tableBatches: before.tableBatches,
  }

  /// Reference: clone, plan while replaying into scratch, then replay the
  /// prepared operation groups against the original tables.
  pure def runReferencePlanner(
    scenario: PlannerScenario,
    outcome: PlannerOutcome,
  ): PlannerResult = {
    val scratchWithTransaction = recordTransaction(initialTables)
    val first = planObject(scratchWithTransaction, scenario, firstObject(scenario))
    val scratchAfterFirst = applyPreparedObjectPlan(scratchWithTransaction, first)
    val second = planObject(scratchAfterFirst, scenario, secondObject(scenario))
    val scratchTerminal = applyPreparedObjectPlan(scratchAfterFirst, second)
    val replayWithTransaction = recordTransaction(initialTables)
    val replayAfterFirst = applyPreparedObjectPlan(replayWithTransaction, first)
    val replayTerminal = applyPreparedObjectPlan(replayAfterFirst, second)
    {
      firstPlan: first,
      secondPlan: second,
      planningTerminal: scratchTerminal,
      terminal:
        if (outcome == CommitSucceeds) {
          commitPreparedBatch(replayTerminal)
        } else {
          failReferenceTableBatchAppend(initialTables)
        },
      tableClones: 1,
      planningApplications: 3,
      replayApplications: 3,
    }
  }

  /// Optimized: plan each object against the state produced by the preceding
  /// preapply, retaining one operation batch for durable append and rollback.
  pure def runOptimizedPlanner(
    scenario: PlannerScenario,
    outcome: PlannerOutcome,
  ): PlannerResult = {
    val preappliedTransaction = recordTransaction(initialTables)
    val first = planObject(preappliedTransaction, scenario, firstObject(scenario))
    val afterFirst = applyPreparedObjectPlan(preappliedTransaction, first)
    val second = planObject(afterFirst, scenario, secondObject(scenario))
    val preappliedTerminal = applyPreparedObjectPlan(afterFirst, second)
    {
      firstPlan: first,
      secondPlan: second,
      planningTerminal: preappliedTerminal,
      terminal:
        if (outcome == CommitSucceeds) {
          commitPreparedBatch(preappliedTerminal)
        } else {
          rollbackFailedPreappliedBatch(preappliedTerminal, initialTables)
        },
      tableClones: 0,
      planningApplications: 3,
      replayApplications: 0,
    }
  }

  action init = all {
    phase' = Ready,
    selectedScenario' = None,
    selectedOutcome' = None,
    referenceResult' = emptyResult,
    optimizedResult' = emptyResult,
  }

  action comparePlanners(
    scenario: PlannerScenario,
    outcome: PlannerOutcome,
  ): bool = all {
    phase == Ready,
    PlannerScenarios.contains(scenario),
    PlannerOutcomes.contains(outcome),
    phase' = Compared,
    selectedScenario' = Some(scenario),
    selectedOutcome' = Some(outcome),
    referenceResult' = runReferencePlanner(scenario, outcome),
    optimizedResult' = runOptimizedPlanner(scenario, outcome),
  }

  action stutter: bool = all {
    phase == Compared,
    phase' = phase,
    selectedScenario' = selectedScenario,
    selectedOutcome' = selectedOutcome,
    referenceResult' = referenceResult,
    optimizedResult' = optimizedResult,
  }

  action step = any {
    {
      nondet scenario = oneOf(PlannerScenarios)
      nondet outcome = oneOf(PlannerOutcomes)
      comparePlanners(scenario, outcome)
    },
    stutter,
  }
}
