/// Safety and bounded conflict reachability for namespace expectation snapshots.
module dfs_namespace_expectation_snapshot_validation_safety {
  import dfs_namespace_expectation_snapshot_validation.* from "../../models/dfs/namespace_expectation_snapshot_validation"

  def expectationsComeFromOneImmutableSnapshot: bool =
    expectationSnapshot.phase != BeforeSnapshot implies all {
      expectationSnapshot.expectedObjectVersion
        == expectationSnapshot.capturedSnapshot.objectVersion,
      expectationSnapshot.expectedPathOccupant
        == expectationSnapshot.capturedSnapshot.pathOccupant,
      expectationSnapshot.capturedSnapshot == initialTables,
    }

  def resolvedDecisionUsesExactCommitTimeInput: bool =
    expectationSnapshot.phase == Resolved implies all {
      expectationSnapshot.lastTransition == CommitValidated,
      expectationSnapshot.validationInput == expectationSnapshot.tables,
      expectationSnapshot.decision == validationDecision(
        expectationSnapshot.expectedObjectVersion,
        expectationSnapshot.expectedPathOccupant,
        expectationSnapshot.validationInput
      ),
    }

  def acceptedCommitRequiresBothExpectationsUnchanged: bool =
    expectationSnapshot.decision == CommitAccepted implies all {
      expectationSnapshot.phase == Resolved,
      expectationSnapshot.validationInput.objectVersion
        == expectationSnapshot.expectedObjectVersion,
      expectationSnapshot.validationInput.pathOccupant
        == expectationSnapshot.expectedPathOccupant,
      not(expectationSnapshot.objectChangedAfterSnapshot),
      not(expectationSnapshot.pathChangedAfterSnapshot),
      expectationSnapshot.commitVisible,
    }

  def objectMismatchCannotCommit: bool =
    all {
      expectationSnapshot.phase == Resolved,
      expectationSnapshot.validationInput.objectVersion
        != expectationSnapshot.expectedObjectVersion,
    } implies all {
      expectationSnapshot.decision == ObjectConflict,
      not(expectationSnapshot.commitVisible),
    }

  def pathMismatchCannotCommit: bool =
    all {
      expectationSnapshot.phase == Resolved,
      expectationSnapshot.validationInput.pathOccupant
        != expectationSnapshot.expectedPathOccupant,
    } implies all {
      expectationSnapshot.decision != CommitAccepted,
      not(expectationSnapshot.commitVisible),
    }

  def exactRelevantSnapshotCannotBeFalselyRejected: bool =
    all {
      expectationSnapshot.phase == Resolved,
      expectationSnapshot.validationInput.objectVersion
        == expectationSnapshot.expectedObjectVersion,
      expectationSnapshot.validationInput.pathOccupant
        == expectationSnapshot.expectedPathOccupant,
    } implies all {
      expectationSnapshot.decision == CommitAccepted,
      expectationSnapshot.commitVisible,
    }

  def rejectedDecisionHasNoVisibleCommit: bool =
    any {
      expectationSnapshot.decision == ObjectConflict,
      expectationSnapshot.decision == PathConflict,
    } implies not(expectationSnapshot.commitVisible)

  def witnessUnchangedSnapshotFastPathAccepted: bool = all {
    expectationSnapshot.lastTransition == CommitValidated,
    expectationSnapshot.decision == CommitAccepted,
    expectationSnapshot.unrelatedRevision == 0,
    acceptedCommitRequiresBothExpectationsUnchanged,
  }

  def witnessUnrelatedMutationDoesNotBlockCommit: bool = all {
    expectationSnapshot.lastTransition == CommitValidated,
    expectationSnapshot.unrelatedRevision == 1,
    expectationSnapshot.decision == CommitAccepted,
    exactRelevantSnapshotCannotBeFalselyRejected,
  }

  def witnessObjectVersionConflictRejected: bool = all {
    expectationSnapshot.lastTransition == CommitValidated,
    expectationSnapshot.objectChangedAfterSnapshot,
    not(expectationSnapshot.pathChangedAfterSnapshot),
    expectationSnapshot.decision == ObjectConflict,
    objectMismatchCannotCommit,
  }

  def witnessPathOccupantConflictRejected: bool = all {
    expectationSnapshot.lastTransition == CommitValidated,
    not(expectationSnapshot.objectChangedAfterSnapshot),
    expectationSnapshot.pathChangedAfterSnapshot,
    expectationSnapshot.decision == PathConflict,
    pathMismatchCannotCommit,
  }

  def witnessBothRelevantConflictsRejected: bool = all {
    expectationSnapshot.lastTransition == CommitValidated,
    expectationSnapshot.objectChangedAfterSnapshot,
    expectationSnapshot.pathChangedAfterSnapshot,
    expectationSnapshot.decision != CommitAccepted,
    rejectedDecisionHasNoVisibleCommit,
  }

  def safety: bool = all {
    expectationsComeFromOneImmutableSnapshot,
    resolvedDecisionUsesExactCommitTimeInput,
    acceptedCommitRequiresBothExpectationsUnchanged,
    objectMismatchCannotCommit,
    pathMismatchCannotCommit,
    exactRelevantSnapshotCannotBeFalselyRejected,
    rejectedDecisionHasNoVisibleCommit,
  }
}
