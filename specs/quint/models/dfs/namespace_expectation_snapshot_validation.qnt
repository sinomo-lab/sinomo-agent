/// DFS namespace expectation construction from one immutable table snapshot.
///
/// Evidence class: target hardening for replacing repeated backend point reads
/// in `namespace_prepare_expectations` with one immutable shard-table snapshot.
/// The snapshot projects exactly the object-version and path-occupant rows that
/// `validate_namespace_prepare_expectations` rechecks under the metadata write
/// lock before a Prepared -> Committed transition becomes durable.
///
/// The bounded footprint contains one object and one path. Either relevant row
/// may change after expectation construction, while an unrelated table revision
/// may also advance. Commit accepts only when both relevant values still equal
/// the captured snapshot. Relevant changes use distinct values and are not
/// restored, so ABA, wider transactions, prepare overlap, owner-epoch fencing,
/// journal durability, and transaction materialization remain outside this
/// focused validation slice.
module dfs_namespace_expectation_snapshot_validation {
  type ObjectVersion = VersionOne | VersionTwo
  type PathOccupant = PreparedObject | CompetingObject
  type SnapshotPhase = BeforeSnapshot | Prepared | Resolved
  type CommitDecision = Undecided | CommitAccepted | ObjectConflict | PathConflict
  type SnapshotTransition =
    | SnapshotInitialized
    | ExpectationsCaptured
    | ObjectVersionChanged
    | PathOccupantChanged
    | UnrelatedTableChanged
    | CommitValidated

  type RelevantTables = {
    objectVersion: ObjectVersion,
    pathOccupant: PathOccupant,
  }

  type ExpectationSnapshotState = {
    phase: SnapshotPhase,
    tables: RelevantTables,
    capturedSnapshot: RelevantTables,
    expectedObjectVersion: ObjectVersion,
    expectedPathOccupant: PathOccupant,
    validationInput: RelevantTables,
    objectChangedAfterSnapshot: bool,
    pathChangedAfterSnapshot: bool,
    unrelatedRevision: int,
    decision: CommitDecision,
    commitVisible: bool,
    lastTransition: SnapshotTransition,
  }

  pure val initialTables: RelevantTables = {
    objectVersion: VersionOne,
    pathOccupant: PreparedObject,
  }

  pure val initialState: ExpectationSnapshotState = {
    phase: BeforeSnapshot,
    tables: initialTables,
    capturedSnapshot: initialTables,
    expectedObjectVersion: VersionOne,
    expectedPathOccupant: PreparedObject,
    validationInput: initialTables,
    objectChangedAfterSnapshot: false,
    pathChangedAfterSnapshot: false,
    unrelatedRevision: 0,
    decision: Undecided,
    commitVisible: false,
    lastTransition: SnapshotInitialized,
  }

  var expectationSnapshot: ExpectationSnapshotState

  pure def validationDecision(
    expectedObjectVersion: ObjectVersion,
    expectedPathOccupant: PathOccupant,
    current: RelevantTables,
  ): CommitDecision =
    if (current.objectVersion != expectedObjectVersion) {
      ObjectConflict
    } else if (current.pathOccupant != expectedPathOccupant) {
      PathConflict
    } else {
      CommitAccepted
    }

  action init = expectationSnapshot' = initialState

  /// One immutable shard-table load supplies both expectation families. Later
  /// transitions mutate only `tables`; the captured projection is retained in
  /// the durable prepare record.
  action captureExpectationsFromOneSnapshot = all {
    expectationSnapshot.phase == BeforeSnapshot,
    expectationSnapshot' = expectationSnapshot
      .with("phase", Prepared)
      .with("capturedSnapshot", expectationSnapshot.tables)
      .with("expectedObjectVersion", expectationSnapshot.tables.objectVersion)
      .with("expectedPathOccupant", expectationSnapshot.tables.pathOccupant)
      .with("lastTransition", ExpectationsCaptured),
  }

  action changeRelevantObjectVersion = all {
    expectationSnapshot.phase == Prepared,
    not(expectationSnapshot.objectChangedAfterSnapshot),
    expectationSnapshot' = expectationSnapshot
      .with(
        "tables",
        expectationSnapshot.tables.with("objectVersion", VersionTwo)
      )
      .with("objectChangedAfterSnapshot", true)
      .with("lastTransition", ObjectVersionChanged),
  }

  action changeRelevantPathOccupant = all {
    expectationSnapshot.phase == Prepared,
    not(expectationSnapshot.pathChangedAfterSnapshot),
    expectationSnapshot' = expectationSnapshot
      .with(
        "tables",
        expectationSnapshot.tables.with("pathOccupant", CompetingObject)
      )
      .with("pathChangedAfterSnapshot", true)
      .with("lastTransition", PathOccupantChanged),
  }

  /// An unrelated table revision is intentionally outside the expectation
  /// footprint and therefore must not serialize an otherwise safe commit.
  action changeUnrelatedTable = all {
    expectationSnapshot.phase == Prepared,
    expectationSnapshot.unrelatedRevision == 0,
    expectationSnapshot' = expectationSnapshot
      .with("unrelatedRevision", 1)
      .with("lastTransition", UnrelatedTableChanged),
  }

  /// This is the commit-time check under the shard metadata write lock. The
  /// validator records the exact input it compared so accepted and rejected
  /// decisions remain explicit invariant obligations.
  action validateAndResolveCommit = {
    val decision = validationDecision(
      expectationSnapshot.expectedObjectVersion,
      expectationSnapshot.expectedPathOccupant,
      expectationSnapshot.tables
    )
    all {
      expectationSnapshot.phase == Prepared,
      expectationSnapshot' = expectationSnapshot
        .with("phase", Resolved)
        .with("validationInput", expectationSnapshot.tables)
        .with("decision", decision)
        .with("commitVisible", decision == CommitAccepted)
        .with("lastTransition", CommitValidated),
    }
  }

  action stutter = expectationSnapshot' = expectationSnapshot

  action step = any {
    captureExpectationsFromOneSnapshot,
    changeRelevantObjectVersion,
    changeRelevantPathOccupant,
    changeUnrelatedTable,
    validateAndResolveCommit,
    stutter,
  }

  /// Coverage-only schedules call the same ordinary actions. Apalache verifies
  /// the unrestricted `step` relation.
  action unchangedCoverageStep =
    if (expectationSnapshot.phase == BeforeSnapshot) {
      captureExpectationsFromOneSnapshot
    } else if (expectationSnapshot.phase == Prepared) {
      validateAndResolveCommit
    } else {
      stutter
    }

  action objectConflictCoverageStep =
    if (expectationSnapshot.phase == BeforeSnapshot) {
      captureExpectationsFromOneSnapshot
    } else if (
      expectationSnapshot.phase == Prepared
        and not(expectationSnapshot.objectChangedAfterSnapshot)
    ) {
      changeRelevantObjectVersion
    } else if (expectationSnapshot.phase == Prepared) {
      validateAndResolveCommit
    } else {
      stutter
    }

  action pathConflictCoverageStep =
    if (expectationSnapshot.phase == BeforeSnapshot) {
      captureExpectationsFromOneSnapshot
    } else if (
      expectationSnapshot.phase == Prepared
        and not(expectationSnapshot.pathChangedAfterSnapshot)
    ) {
      changeRelevantPathOccupant
    } else if (expectationSnapshot.phase == Prepared) {
      validateAndResolveCommit
    } else {
      stutter
    }

  action bothConflictsCoverageStep =
    if (expectationSnapshot.phase == BeforeSnapshot) {
      captureExpectationsFromOneSnapshot
    } else if (
      expectationSnapshot.phase == Prepared
        and not(expectationSnapshot.objectChangedAfterSnapshot)
    ) {
      changeRelevantObjectVersion
    } else if (
      expectationSnapshot.phase == Prepared
        and not(expectationSnapshot.pathChangedAfterSnapshot)
    ) {
      changeRelevantPathOccupant
    } else if (expectationSnapshot.phase == Prepared) {
      validateAndResolveCommit
    } else {
      stutter
    }

  action unrelatedChangeCoverageStep =
    if (expectationSnapshot.phase == BeforeSnapshot) {
      captureExpectationsFromOneSnapshot
    } else if (
      expectationSnapshot.phase == Prepared
        and expectationSnapshot.unrelatedRevision == 0
    ) {
      changeUnrelatedTable
    } else if (expectationSnapshot.phase == Prepared) {
      validateAndResolveCommit
    } else {
      stutter
    }

  action coverageStep = any {
    step,
    unchangedCoverageStep,
    objectConflictCoverageStep,
    pathConflictCoverageStep,
    bothConflictsCoverageStep,
    unrelatedChangeCoverageStep,
  }
}
