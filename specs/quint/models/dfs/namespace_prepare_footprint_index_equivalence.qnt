/// DFS active namespace-prepare footprint-index equivalence.
///
/// Evidence class: target hardening for replacing the historical scan in
/// `validate_namespace_prepare_does_not_overlap` with direct active-owner
/// lookups. The bounded footprint contains one object and one normalized path;
/// the four profiles cover object-only overlap, path-only overlap, complete
/// overlap, and complete separation between two operations.
///
/// The reference algorithm scans every retained prepare row and considers only
/// other operations in `Prepared` state for the same filesystem. The indexed
/// algorithm performs two filesystem-qualified lookups. Exact retries ignore
/// their own ownership. Committed and aborted rows remain in history but release
/// both index entries. Recovery atomically derives the sparse index from the
/// retained history, matching a restart-time index rebuild.
///
/// Payload equality, expectation validation, multi-object/multi-path set
/// construction, persistence failure, owner-epoch fencing, and local-first
/// behavior remain outside this algorithm-only refinement slice.
module dfs_namespace_prepare_footprint_index_equivalence {
  import common.* from "../../lib/common"

  type FileSystem = FsA | FsB
  type OperationId = OperationA | OperationB
  type ObjectId = SharedObject | OtherObject
  type NamespacePath = SharedPath | OtherPath
  type FootprintProfile = SharedObjectSharedPath
    | SharedObjectOtherPath
    | OtherObjectSharedPath
    | OtherObjectOtherPath
  type PrepareStatus = Prepared | Committed | Aborted
  type ValidationDecision = Accept | ObjectConflict | PathConflict
  type FootprintTransition = Initialized
    | PrepareAccepted
    | PrepareRejected
    | RetryAccepted
    | PrepareCommitted
    | PrepareAborted
    | IndexRecovered

  type PrepareRecord = {
    fileSystem: FileSystem,
    object: ObjectId,
    path: NamespacePath,
    status: PrepareStatus,
  }

  pure val FileSystems: Set[FileSystem] = Set(FsA, FsB)
  pure val OperationIds: Set[OperationId] = Set(OperationA, OperationB)
  pure val ObjectIds: Set[ObjectId] = Set(SharedObject, OtherObject)
  pure val NamespacePaths: Set[NamespacePath] = Set(SharedPath, OtherPath)
  pure val FootprintProfiles: Set[FootprintProfile] = Set(
    SharedObjectSharedPath,
    SharedObjectOtherPath,
    OtherObjectSharedPath,
    OtherObjectOtherPath
  )
  pure val TerminalStatuses: Set[PrepareStatus] = Set(Committed, Aborted)

  pure val emptyPrepares: OperationId -> Option[PrepareRecord] =
    OperationIds.mapBy(_ => None)

  pure val emptyObjectOwnerRow: ObjectId -> Option[OperationId] =
    ObjectIds.mapBy(_ => None)

  pure val emptyObjectOwners:
    FileSystem -> (ObjectId -> Option[OperationId]) =
      FileSystems.mapBy(_ => emptyObjectOwnerRow)

  pure val emptyPathOwnerRow: NamespacePath -> Option[OperationId] =
    NamespacePaths.mapBy(_ => None)

  pure val emptyPathOwners:
    FileSystem -> (NamespacePath -> Option[OperationId]) =
      FileSystems.mapBy(_ => emptyPathOwnerRow)

  var prepares: OperationId -> Option[PrepareRecord]
  var objectOwners: FileSystem -> (ObjectId -> Option[OperationId])
  var pathOwners: FileSystem -> (NamespacePath -> Option[OperationId])

  /// Pre-transition snapshots make accepted, rejected, retry, terminal, and
  /// recovery maintenance obligations independently checkable.
  var beforePrepares: OperationId -> Option[PrepareRecord]
  var beforeObjectOwners: FileSystem -> (ObjectId -> Option[OperationId])
  var beforePathOwners:
    FileSystem -> (NamespacePath -> Option[OperationId])
  var lastTransition: FootprintTransition
  var lastOperation: OperationId
  var lastCandidate: PrepareRecord
  var lastReferenceDecision: ValidationDecision
  var lastIndexedDecision: ValidationDecision
  var lastTerminalStatus: PrepareStatus

  pure def objectFor(profile: FootprintProfile): ObjectId =
    match profile {
      | SharedObjectSharedPath => SharedObject
      | SharedObjectOtherPath => SharedObject
      | OtherObjectSharedPath => OtherObject
      | OtherObjectOtherPath => OtherObject
    }

  pure def pathFor(profile: FootprintProfile): NamespacePath =
    match profile {
      | SharedObjectSharedPath => SharedPath
      | SharedObjectOtherPath => OtherPath
      | OtherObjectSharedPath => SharedPath
      | OtherObjectOtherPath => OtherPath
    }

  pure def prepareFor(
    fileSystem: FileSystem,
    profile: FootprintProfile,
    status: PrepareStatus,
  ): PrepareRecord = {
    fileSystem: fileSystem,
    object: objectFor(profile),
    path: pathFor(profile),
    status: status,
  }

  pure def withStatus(
    prepare: PrepareRecord,
    status: PrepareStatus,
  ): PrepareRecord = {
    fileSystem: prepare.fileSystem,
    object: prepare.object,
    path: prepare.path,
    status: status,
  }

  pure def unwrapPrepare(value: Option[PrepareRecord]): PrepareRecord =
    match value {
      | Some(prepare) => prepare
      | None => prepareFor(FsA, SharedObjectSharedPath, Aborted)
    }

  pure def isActiveOwnerAt(
    source: OperationId -> Option[PrepareRecord],
    operation: OperationId,
    fileSystem: FileSystem,
    object: ObjectId,
  ): bool =
    match source.get(operation) {
      | Some(prepare) => all {
          prepare.status == Prepared,
          prepare.fileSystem == fileSystem,
          prepare.object == object,
        }
      | None => false
    }

  pure def isActivePathOwnerAt(
    source: OperationId -> Option[PrepareRecord],
    operation: OperationId,
    fileSystem: FileSystem,
    path: NamespacePath,
  ): bool =
    match source.get(operation) {
      | Some(prepare) => all {
          prepare.status == Prepared,
          prepare.fileSystem == fileSystem,
          prepare.path == path,
        }
      | None => false
    }

  /// The admission invariant permits at most one active owner per qualified
  /// key, so this deterministic bounded projection is the restart-time index.
  pure def referenceObjectOwnerAt(
    source: OperationId -> Option[PrepareRecord],
    fileSystem: FileSystem,
    object: ObjectId,
  ): Option[OperationId] =
    if (isActiveOwnerAt(source, OperationA, fileSystem, object)) {
      Some(OperationA)
    } else if (isActiveOwnerAt(source, OperationB, fileSystem, object)) {
      Some(OperationB)
    } else {
      None
    }

  pure def referencePathOwnerAt(
    source: OperationId -> Option[PrepareRecord],
    fileSystem: FileSystem,
    path: NamespacePath,
  ): Option[OperationId] =
    if (isActivePathOwnerAt(source, OperationA, fileSystem, path)) {
      Some(OperationA)
    } else if (isActivePathOwnerAt(source, OperationB, fileSystem, path)) {
      Some(OperationB)
    } else {
      None
    }

  pure def rebuildObjectOwners(
    source: OperationId -> Option[PrepareRecord],
  ): FileSystem -> (ObjectId -> Option[OperationId]) =
    FileSystems.mapBy(fileSystem =>
      ObjectIds.mapBy(object =>
        referenceObjectOwnerAt(source, fileSystem, object)
      )
    )

  pure def rebuildPathOwners(
    source: OperationId -> Option[PrepareRecord],
  ): FileSystem -> (NamespacePath -> Option[OperationId]) =
    FileSystems.mapBy(fileSystem =>
      NamespacePaths.mapBy(path =>
        referencePathOwnerAt(source, fileSystem, path)
      )
    )

  pure def referenceHasObjectConflictAt(
    source: OperationId -> Option[PrepareRecord],
    operation: OperationId,
    candidate: PrepareRecord,
  ): bool =
    OperationIds.exists(existingOperation =>
      existingOperation != operation and
        match source.get(existingOperation) {
          | Some(existing) => all {
              existing.status == Prepared,
              existing.fileSystem == candidate.fileSystem,
              existing.object == candidate.object,
            }
          | None => false
        }
    )

  pure def referenceHasPathConflictAt(
    source: OperationId -> Option[PrepareRecord],
    operation: OperationId,
    candidate: PrepareRecord,
  ): bool =
    OperationIds.exists(existingOperation =>
      existingOperation != operation and
        match source.get(existingOperation) {
          | Some(existing) => all {
              existing.status == Prepared,
              existing.fileSystem == candidate.fileSystem,
              existing.path == candidate.path,
            }
          | None => false
        }
    )

  pure def referenceValidationAt(
    source: OperationId -> Option[PrepareRecord],
    operation: OperationId,
    candidate: PrepareRecord,
  ): ValidationDecision =
    if (referenceHasObjectConflictAt(source, operation, candidate)) {
      ObjectConflict
    } else if (referenceHasPathConflictAt(source, operation, candidate)) {
      PathConflict
    } else {
      Accept
    }

  pure def indexedValidationAt(
    indexedObjects: FileSystem -> (ObjectId -> Option[OperationId]),
    indexedPaths: FileSystem -> (NamespacePath -> Option[OperationId]),
    operation: OperationId,
    candidate: PrepareRecord,
  ): ValidationDecision = {
    val objectOwner = indexedObjects
      .get(candidate.fileSystem)
      .get(candidate.object)
    val pathOwner = indexedPaths
      .get(candidate.fileSystem)
      .get(candidate.path)
    if (isSome(objectOwner) and objectOwner != Some(operation)) {
      ObjectConflict
    } else if (isSome(pathOwner) and pathOwner != Some(operation)) {
      PathConflict
    } else {
      Accept
    }
  }

  def referenceValidation(
    operation: OperationId,
    candidate: PrepareRecord,
  ): ValidationDecision =
    referenceValidationAt(prepares, operation, candidate)

  def indexedValidation(
    operation: OperationId,
    candidate: PrepareRecord,
  ): ValidationDecision =
    indexedValidationAt(objectOwners, pathOwners, operation, candidate)

  pure def putPrepare(
    source: OperationId -> Option[PrepareRecord],
    operation: OperationId,
    prepare: PrepareRecord,
  ): OperationId -> Option[PrepareRecord] =
    source.set(operation, Some(prepare))

  pure def putObjectOwner(
    source: FileSystem -> (ObjectId -> Option[OperationId]),
    operation: OperationId,
    prepare: PrepareRecord,
  ): FileSystem -> (ObjectId -> Option[OperationId]) =
    source.set(
      prepare.fileSystem,
      source.get(prepare.fileSystem).set(prepare.object, Some(operation))
    )

  pure def putPathOwner(
    source: FileSystem -> (NamespacePath -> Option[OperationId]),
    operation: OperationId,
    prepare: PrepareRecord,
  ): FileSystem -> (NamespacePath -> Option[OperationId]) =
    source.set(
      prepare.fileSystem,
      source.get(prepare.fileSystem).set(prepare.path, Some(operation))
    )

  pure def removeObjectOwner(
    source: FileSystem -> (ObjectId -> Option[OperationId]),
    operation: OperationId,
    prepare: PrepareRecord,
  ): FileSystem -> (ObjectId -> Option[OperationId]) =
    if (source.get(prepare.fileSystem).get(prepare.object) == Some(operation)) {
      source.set(
        prepare.fileSystem,
        source.get(prepare.fileSystem).set(prepare.object, None)
      )
    } else {
      source
    }

  pure def removePathOwner(
    source: FileSystem -> (NamespacePath -> Option[OperationId]),
    operation: OperationId,
    prepare: PrepareRecord,
  ): FileSystem -> (NamespacePath -> Option[OperationId]) =
    if (source.get(prepare.fileSystem).get(prepare.path) == Some(operation)) {
      source.set(
        prepare.fileSystem,
        source.get(prepare.fileSystem).set(prepare.path, None)
      )
    } else {
      source
    }

  pure def activePrepareCountAt(
    source: OperationId -> Option[PrepareRecord],
  ): int =
    OperationIds.filter(operation =>
      match source.get(operation) {
        | Some(prepare) => prepare.status == Prepared
        | None => false
      }
    ).size()

  /// Worst-case reference work inspects object and path ownership on every
  /// retained history row. The sparse path performs exactly two lookups.
  pure val referenceHistoryChecks: int = 4
  pure val indexedOwnerLookups: int = 2

  action init = all {
    prepares' = emptyPrepares,
    objectOwners' = emptyObjectOwners,
    pathOwners' = emptyPathOwners,
    beforePrepares' = emptyPrepares,
    beforeObjectOwners' = emptyObjectOwners,
    beforePathOwners' = emptyPathOwners,
    lastTransition' = Initialized,
    lastOperation' = OperationA,
    lastCandidate' = prepareFor(
      FsA,
      SharedObjectSharedPath,
      Prepared
    ),
    lastReferenceDecision' = Accept,
    lastIndexedDecision' = Accept,
    lastTerminalStatus' = Aborted,
  }

  /// New operations are admitted by the historical reference decision while
  /// the indexed decision is recorded independently for refinement checking.
  /// A rejected attempt is an observation and mutates neither representation.
  action attemptPrepare(
    operation: OperationId,
    fileSystem: FileSystem,
    profile: FootprintProfile,
  ): bool = {
    val candidate = prepareFor(fileSystem, profile, Prepared)
    val referenceDecision = referenceValidation(operation, candidate)
    val indexedDecision = indexedValidation(operation, candidate)
    all {
      prepares.get(operation) == None,
      beforePrepares' = prepares,
      beforeObjectOwners' = objectOwners,
      beforePathOwners' = pathOwners,
      lastOperation' = operation,
      lastCandidate' = candidate,
      lastReferenceDecision' = referenceDecision,
      lastIndexedDecision' = indexedDecision,
      lastTerminalStatus' = Aborted,
      if (referenceDecision == Accept) {
        all {
          prepares' = putPrepare(prepares, operation, candidate),
          objectOwners' = putObjectOwner(
            objectOwners,
            operation,
            candidate
          ),
          pathOwners' = putPathOwner(pathOwners, operation, candidate),
          lastTransition' = PrepareAccepted,
        }
      } else {
        all {
          prepares' = prepares,
          objectOwners' = objectOwners,
          pathOwners' = pathOwners,
          lastTransition' = PrepareRejected,
        }
      },
    }
  }

  /// Exact payload retry is the only same-id prepare path in this slice. The
  /// reference scan ignores the same operation, and the indexed lookup treats
  /// ownership by that same operation as non-conflicting.
  action retryPrepared(operation: OperationId): bool = {
    val candidate = unwrapPrepare(prepares.get(operation))
    val referenceDecision = referenceValidation(operation, candidate)
    val indexedDecision = indexedValidation(operation, candidate)
    all {
      isSome(prepares.get(operation)),
      candidate.status == Prepared,
      beforePrepares' = prepares,
      beforeObjectOwners' = objectOwners,
      beforePathOwners' = pathOwners,
      prepares' = prepares,
      objectOwners' = objectOwners,
      pathOwners' = pathOwners,
      lastTransition' = RetryAccepted,
      lastOperation' = operation,
      lastCandidate' = candidate,
      lastReferenceDecision' = referenceDecision,
      lastIndexedDecision' = indexedDecision,
      lastTerminalStatus' = Aborted,
    }
  }

  /// Both terminal states retain the durable row but release the exact active
  /// object and path ownership entries.
  action resolvePrepare(
    operation: OperationId,
    terminalStatus: PrepareStatus,
  ): bool = {
    val current = unwrapPrepare(prepares.get(operation))
    val terminal = withStatus(current, terminalStatus)
    all {
      isSome(prepares.get(operation)),
      current.status == Prepared,
      TerminalStatuses.contains(terminalStatus),
      beforePrepares' = prepares,
      beforeObjectOwners' = objectOwners,
      beforePathOwners' = pathOwners,
      prepares' = putPrepare(prepares, operation, terminal),
      objectOwners' = removeObjectOwner(
        objectOwners,
        operation,
        current
      ),
      pathOwners' = removePathOwner(pathOwners, operation, current),
      lastTransition' =
        if (terminalStatus == Committed) PrepareCommitted else PrepareAborted,
      lastOperation' = operation,
      lastCandidate' = current,
      lastReferenceDecision' = Accept,
      lastIndexedDecision' = Accept,
      lastTerminalStatus' = terminalStatus,
    }
  }

  /// Restart discards no durable history. It atomically regenerates only the
  /// active sparse projection, so terminal rows never regain ownership.
  action recoverIndex = all {
    beforePrepares' = prepares,
    beforeObjectOwners' = objectOwners,
    beforePathOwners' = pathOwners,
    prepares' = prepares,
    objectOwners' = rebuildObjectOwners(prepares),
    pathOwners' = rebuildPathOwners(prepares),
    lastTransition' = IndexRecovered,
    lastOperation' = lastOperation,
    lastCandidate' = lastCandidate,
    lastReferenceDecision' = lastReferenceDecision,
    lastIndexedDecision' = lastIndexedDecision,
    lastTerminalStatus' = lastTerminalStatus,
  }

  action step = any {
    {
      nondet operation = oneOf(OperationIds)
      nondet fileSystem = oneOf(FileSystems)
      nondet profile = oneOf(FootprintProfiles)
      attemptPrepare(operation, fileSystem, profile)
    },
    {
      nondet operation = oneOf(OperationIds)
      retryPrepared(operation)
    },
    {
      nondet operation = oneOf(OperationIds)
      nondet terminalStatus = oneOf(TerminalStatuses)
      resolvePrepare(operation, terminalStatus)
    },
    recoverIndex,
  }
}
