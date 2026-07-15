/// Safety and refinement checks for the active namespace-prepare footprint index.
module dfs_namespace_prepare_footprint_index_equivalence_safety {
  import common.* from "../../lib/common"
  import dfs_namespace_prepare_footprint_index_equivalence.* from "../../models/dfs/namespace_prepare_footprint_index_equivalence"

  def objectIndexExactlyMatchesActiveHistory: bool =
    objectOwners == rebuildObjectOwners(prepares)

  def pathIndexExactlyMatchesActiveHistory: bool =
    pathOwners == rebuildPathOwners(prepares)

  def allCandidateDecisionsAreEquivalent: bool =
    OperationIds.forall(operation =>
      FileSystems.forall(fileSystem =>
        FootprintProfiles.forall(profile => {
          val candidate = prepareFor(fileSystem, profile, Prepared)
          referenceValidation(operation, candidate)
            == indexedValidation(operation, candidate)
        })
      )
    )

  def activeObjectOwnershipIsUnique: bool =
    FileSystems.forall(fileSystem =>
      ObjectIds.forall(object =>
        not(all {
          isActiveOwnerAt(prepares, OperationA, fileSystem, object),
          isActiveOwnerAt(prepares, OperationB, fileSystem, object),
        })
      )
    )

  def activePathOwnershipIsUnique: bool =
    FileSystems.forall(fileSystem =>
      NamespacePaths.forall(path =>
        not(all {
          isActivePathOwnerAt(prepares, OperationA, fileSystem, path),
          isActivePathOwnerAt(prepares, OperationB, fileSystem, path),
        })
      )
    )

  def everyIndexedOwnerIsExactAndActive: bool = all {
    FileSystems.forall(fileSystem =>
      ObjectIds.forall(object =>
        match objectOwners.get(fileSystem).get(object) {
          | Some(operation) =>
              isActiveOwnerAt(prepares, operation, fileSystem, object)
          | None => true
        }
      )
    ),
    FileSystems.forall(fileSystem =>
      NamespacePaths.forall(path =>
        match pathOwners.get(fileSystem).get(path) {
          | Some(operation) =>
              isActivePathOwnerAt(prepares, operation, fileSystem, path)
          | None => true
        }
      )
    ),
  }

  def terminalHistoryOwnsNoFootprint: bool =
    OperationIds.forall(operation =>
      match prepares.get(operation) {
        | Some(prepare) =>
            prepare.status != Prepared implies all {
              objectOwners.get(prepare.fileSystem).get(prepare.object)
                != Some(operation),
              pathOwners.get(prepare.fileSystem).get(prepare.path)
                != Some(operation),
            }
        | None => true
      }
    )

  def acceptedPrepareUpdatedEveryView: bool =
    lastTransition == PrepareAccepted implies all {
      prepares == putPrepare(
        beforePrepares,
        lastOperation,
        lastCandidate
      ),
      objectOwners == putObjectOwner(
        beforeObjectOwners,
        lastOperation,
        lastCandidate
      ),
      pathOwners == putPathOwner(
        beforePathOwners,
        lastOperation,
        lastCandidate
      ),
      lastReferenceDecision == Accept,
      lastIndexedDecision == Accept,
    }

  def rejectedPrepareMutatedNoView: bool =
    lastTransition == PrepareRejected implies all {
      prepares == beforePrepares,
      objectOwners == beforeObjectOwners,
      pathOwners == beforePathOwners,
      lastReferenceDecision != Accept,
      lastReferenceDecision == lastIndexedDecision,
    }

  def exactRetryIsAnAcceptedStutter: bool =
    lastTransition == RetryAccepted implies all {
      prepares == beforePrepares,
      objectOwners == beforeObjectOwners,
      pathOwners == beforePathOwners,
      beforePrepares.get(lastOperation) == Some(lastCandidate),
      lastCandidate.status == Prepared,
      lastReferenceDecision == Accept,
      lastIndexedDecision == Accept,
    }

  def terminalTransitionReleasedExactFootprint: bool =
    any {
      lastTransition == PrepareCommitted,
      lastTransition == PrepareAborted,
    } implies all {
      prepares == putPrepare(
        beforePrepares,
        lastOperation,
        withStatus(lastCandidate, lastTerminalStatus)
      ),
      objectOwners == removeObjectOwner(
        beforeObjectOwners,
        lastOperation,
        lastCandidate
      ),
      pathOwners == removePathOwner(
        beforePathOwners,
        lastOperation,
        lastCandidate
      ),
      lastCandidate.status == Prepared,
      TerminalStatuses.contains(lastTerminalStatus),
    }

  def recoveryRebuiltExactActiveProjection: bool =
    lastTransition == IndexRecovered implies all {
      prepares == beforePrepares,
      objectOwners == rebuildObjectOwners(beforePrepares),
      pathOwners == rebuildPathOwners(beforePrepares),
    }

  def indexedCostIsHistoryIndependent: bool = all {
    referenceHistoryChecks == 4,
    indexedOwnerLookups == 2,
    indexedOwnerLookups < referenceHistoryChecks,
  }

  def witnessNonOverlapAccepted: bool = all {
    lastTransition == PrepareAccepted,
    OperationIds.exists(existingOperation =>
      existingOperation != lastOperation and
        match beforePrepares.get(existingOperation) {
          | Some(existing) => all {
              existing.status == Prepared,
              existing.fileSystem == lastCandidate.fileSystem,
              existing.object != lastCandidate.object,
              existing.path != lastCandidate.path,
            }
          | None => false
        }
    ),
    acceptedPrepareUpdatedEveryView,
  }

  def witnessObjectConflictRejected: bool = all {
    lastTransition == PrepareRejected,
    lastReferenceDecision == ObjectConflict,
    lastIndexedDecision == ObjectConflict,
    rejectedPrepareMutatedNoView,
  }

  def witnessPathConflictRejected: bool = all {
    lastTransition == PrepareRejected,
    lastReferenceDecision == PathConflict,
    lastIndexedDecision == PathConflict,
    rejectedPrepareMutatedNoView,
  }

  def witnessTerminalHistoryIgnored: bool = all {
    lastTransition == PrepareAccepted,
    OperationIds.exists(existingOperation =>
      existingOperation != lastOperation and
        match beforePrepares.get(existingOperation) {
          | Some(existing) => all {
              existing.status != Prepared,
              existing.fileSystem == lastCandidate.fileSystem,
              existing.object == lastCandidate.object,
              existing.path == lastCandidate.path,
            }
          | None => false
        }
    ),
    lastReferenceDecision == Accept,
    lastIndexedDecision == Accept,
    acceptedPrepareUpdatedEveryView,
  }

  def witnessSameOperationRetryIgnored: bool = all {
    lastTransition == RetryAccepted,
    exactRetryIsAnAcceptedStutter,
  }

  def witnessIndexReferenceEquivalent: bool = all {
    activePrepareCountAt(prepares) > 0,
    objectIndexExactlyMatchesActiveHistory,
    pathIndexExactlyMatchesActiveHistory,
    allCandidateDecisionsAreEquivalent,
  }

  def witnessCrossFileSystemOwnershipIsolated: bool = all {
    lastTransition == PrepareAccepted,
    OperationIds.exists(existingOperation =>
      existingOperation != lastOperation and
        match beforePrepares.get(existingOperation) {
          | Some(existing) => all {
              existing.status == Prepared,
              existing.fileSystem != lastCandidate.fileSystem,
              existing.object == lastCandidate.object,
              existing.path == lastCandidate.path,
            }
          | None => false
        }
    ),
    lastReferenceDecision == Accept,
    lastIndexedDecision == Accept,
  }

  def witnessRecoveryRebuildsActiveOnly: bool = all {
    lastTransition == IndexRecovered,
    activePrepareCountAt(prepares) > 0,
    recoveryRebuiltExactActiveProjection,
    terminalHistoryOwnsNoFootprint,
  }

  def safety: bool = all {
    objectIndexExactlyMatchesActiveHistory,
    pathIndexExactlyMatchesActiveHistory,
    allCandidateDecisionsAreEquivalent,
    activeObjectOwnershipIsUnique,
    activePathOwnershipIsUnique,
    everyIndexedOwnerIsExactAndActive,
    terminalHistoryOwnsNoFootprint,
    acceptedPrepareUpdatedEveryView,
    rejectedPrepareMutatedNoView,
    exactRetryIsAnAcceptedStutter,
    terminalTransitionReleasedExactFootprint,
    recoveryRebuiltExactActiveProjection,
    indexedCostIsHistoryIndependent,
  }
}
