/// Safety and refinement checks for the directory-tombstone child overlay.
module dfs_namespace_tombstone_overlay_equivalence_safety {
  import common.* from "../../lib/common"
  import dfs_namespace_tombstone_overlay_equivalence.* from "../../models/dfs/namespace_tombstone_overlay_equivalence"

  def terminalDirectChildrenAreEquivalent: bool =
    phase == Compared implies all {
      referenceResult.terminalChildrenA == overlayResult.terminalChildrenA,
      referenceResult.terminalChildrenB == overlayResult.terminalChildrenB,
    }

  def candidatePathOwnersAreEquivalent: bool =
    phase == Compared implies
      CandidatePlacements.forall(placement =>
        referenceResult.terminalOwners.get(placement)
          == overlayResult.terminalOwners.get(placement)
      )

  def validationOutcomesAreEquivalent: bool =
    phase == Compared implies referenceResult.outcome == overlayResult.outcome

  def replayedObjectsAndOrderAreEquivalent: bool =
    phase == Compared implies all {
      referenceResult.appliedOrdinal == overlayResult.appliedOrdinal,
      referenceResult.appliedCount == overlayResult.appliedCount,
      referenceResult.pointLookups == overlayResult.pointLookups,
      referenceResult.terminalCompactedOwners
        == overlayResult.terminalCompactedOwners,
    }

  def objectOwnsAtMostOneCandidateSlot(
    owners: Placement -> Option[MetadataObject],
  ): bool =
    MetadataObjects.forall(object =>
      CandidatePlacements
        .filter(placement => owners.get(placement) == Some(object))
        .size() <= 1
    )

  def terminalCandidateOwnershipIsFunctional: bool =
    phase == Compared implies all {
      objectOwnsAtMostOneCandidateSlot(referenceResult.terminalOwners),
      objectOwnsAtMostOneCandidateSlot(overlayResult.terminalOwners),
    }

  def plannerOrderFor(
    ordinals: MetadataObject -> int,
    counts: MetadataObject -> int,
  ): bool = all {
    selectedScenario == Some(ParentOnlyDelete) implies all {
      ordinals.get(CandidateDirectoryA) == 1,
      counts.get(CandidateDirectoryA) == 1,
    },
    selectedScenario == Some(ChildAndParentDelete) implies all {
      ordinals.get(ExistingChildA) == 1,
      ordinals.get(CandidateDirectoryA) == 2,
    },
    selectedScenario == Some(ChildMoveOutAndParentDelete) implies all {
      ordinals.get(CandidateDirectoryA) == 1,
      ordinals.get(ExistingChildA) == 2,
    },
    selectedScenario == Some(ChildLiveInPlaceAndParentDelete) implies all {
      ordinals.get(CandidateDirectoryA) == 1,
      ordinals.get(ExistingChildA) == 2,
    },
    selectedScenario == Some(ChildMoveIntoDeletedParent) implies all {
      ordinals.get(CandidateDirectoryA) == 1,
      ordinals.get(ExistingChildA) == 2,
    },
    selectedScenario == Some(NewChildIntoDeletedParent) implies all {
      ordinals.get(CandidateDirectoryA) == 1,
      ordinals.get(NewChild) == 2,
    },
    selectedScenario == Some(PathReplacementAndParentDelete) implies all {
      ordinals.get(ExistingChildA) == 1,
      ordinals.get(CandidateDirectoryA) == 2,
      ordinals.get(ReplacementChild) == 3,
    },
    selectedScenario == Some(CompactedChildDeleteAndParentDelete) implies all {
      ordinals.get(ExistingChildA) == 0,
      counts.get(ExistingChildA) == 0,
      ordinals.get(CandidateDirectoryA) == 1,
    },
    selectedScenario == Some(NewIdTombstoneOccupiedPath) implies all {
      ordinals.get(NewChild) == 1,
      ordinals.get(CandidateDirectoryA) == 2,
    },
    selectedScenario == Some(NewIdLiveReplacement) implies all {
      ordinals.get(CandidateDirectoryA) == 1,
      ordinals.get(ReplacementChild) == 2,
    },
    selectedScenario == Some(DuplicateIdTombstoneThenLive) implies all {
      ordinals.get(CandidateDirectoryA) == 2,
      ordinals.get(ExistingChildA) == 3,
      counts.get(ExistingChildA) == 2,
    },
    selectedScenario == Some(TwoCandidateDirectoriesFirstFailure) implies all {
      ordinals.get(CandidateDirectoryB) == 1,
      ordinals.get(CandidateDirectoryA) == 2,
    },
    selectedScenario == Some(CompactedMarkerRemovedBeforeLaterTombstone) implies all {
      ordinals.get(NewChild) == 1,
      ordinals.get(ReplacementChild) == 2,
      ordinals.get(CandidateDirectoryA) == 3,
    },
  }

  def exactPlannerOrderIsPreserved: bool =
    phase == Compared implies all {
      plannerOrderFor(
        referenceResult.appliedOrdinal,
        referenceResult.appliedCount,
      ),
      plannerOrderFor(
        overlayResult.appliedOrdinal,
        overlayResult.appliedCount,
      ),
    }

  def overlaySeedsExactlyCurrentCandidateChildren: bool =
    OverlayScenarios.forall(scenario =>
      selectedScenario == Some(scenario) implies
        overlayResult.scannedDentryObjects
          == initialScannedDentryObjects(scenario)
    )

  def overlayNeverScansUnrelatedMetadata: bool =
    phase == Compared implies all {
      overlayResult.fullTableClones == 0,
      overlayResult.scannedDentryObjects
        .intersect(UnrelatedMetadata)
        .size() == 0,
      overlayResult.pointLookups
        .intersect(UnrelatedMetadata)
        .size() == 0,
    }

  def referenceCostIncludesWholeMaterializedTable: bool =
    OverlayScenarios.forall(scenario =>
      selectedScenario == Some(scenario) implies all {
        referenceResult.fullTableClones == 1,
        referenceResult.scannedMetadata
          == materializedObjects(initialRows(scenario)),
        UnrelatedMetadata.subseteq(referenceResult.scannedMetadata),
        overlayResult.scannedDentryObjects.size()
          < referenceResult.scannedMetadata.size(),
      }
    )

  def expectedScenarioOutcome: bool = all {
    selectedScenario == Some(ParentOnlyDelete) implies
      overlayResult.outcome == CandidateANotEmpty,
    selectedScenario == Some(ChildAndParentDelete) implies
      overlayResult.outcome == Accept,
    selectedScenario == Some(ChildMoveOutAndParentDelete) implies
      overlayResult.outcome == Accept,
    selectedScenario == Some(ChildLiveInPlaceAndParentDelete) implies
      overlayResult.outcome == CandidateANotEmpty,
    selectedScenario == Some(ChildMoveIntoDeletedParent) implies
      overlayResult.outcome == CandidateANotEmpty,
    selectedScenario == Some(NewChildIntoDeletedParent) implies
      overlayResult.outcome == CandidateANotEmpty,
    selectedScenario == Some(PathReplacementAndParentDelete) implies
      overlayResult.outcome == CandidateANotEmpty,
    selectedScenario == Some(CompactedChildDeleteAndParentDelete) implies
      overlayResult.outcome == CandidateANotEmpty,
    selectedScenario == Some(NewIdTombstoneOccupiedPath) implies
      overlayResult.outcome == Accept,
    selectedScenario == Some(NewIdLiveReplacement) implies
      overlayResult.outcome == CandidateANotEmpty,
    selectedScenario == Some(DuplicateIdTombstoneThenLive) implies
      overlayResult.outcome == CandidateANotEmpty,
    selectedScenario == Some(TwoCandidateDirectoriesFirstFailure) implies
      overlayResult.outcome == CandidateBNotEmpty,
    selectedScenario == Some(CompactedMarkerRemovedBeforeLaterTombstone) implies
      overlayResult.outcome == Accept,
  }

  def witnessParentOnlyDeleteRejects: bool = all {
    selectedScenario == Some(ParentOnlyDelete),
    referenceResult.outcome == CandidateANotEmpty,
    overlayResult.outcome == CandidateANotEmpty,
    overlayResult.terminalChildrenA == Set(ExistingChildA),
  }

  def witnessChildAndParentDeleteAccepts: bool = all {
    selectedScenario == Some(ChildAndParentDelete),
    referenceResult.outcome == Accept,
    overlayResult.outcome == Accept,
    overlayResult.terminalChildrenA.size() == 0,
  }

  def witnessChildMoveOutAndParentDeleteAccepts: bool = all {
    selectedScenario == Some(ChildMoveOutAndParentDelete),
    referenceResult.outcome == Accept,
    overlayResult.outcome == Accept,
    referenceResult.terminalOwners.get(OutsideCandidate)
      == Some(ExistingChildA),
    overlayResult.terminalChildrenA.size() == 0,
  }

  def witnessChildLiveInPlaceRejects: bool = all {
    selectedScenario == Some(ChildLiveInPlaceAndParentDelete),
    referenceResult.outcome == CandidateANotEmpty,
    overlayResult.outcome == CandidateANotEmpty,
    overlayResult.terminalOwners.get(CandidateASlotA)
      == Some(ExistingChildA),
  }

  def witnessMoveChildIntoDeletedParentRejects: bool = all {
    selectedScenario == Some(ChildMoveIntoDeletedParent),
    initialScannedDentryObjects(ChildMoveIntoDeletedParent).size() == 0,
    referenceResult.outcome == CandidateANotEmpty,
    overlayResult.outcome == CandidateANotEmpty,
    overlayResult.terminalChildrenA == Set(ExistingChildA),
  }

  def witnessNewChildIntoDeletedParentRejects: bool = all {
    selectedScenario == Some(NewChildIntoDeletedParent),
    initialScannedDentryObjects(NewChildIntoDeletedParent).size() == 0,
    referenceResult.outcome == CandidateANotEmpty,
    overlayResult.outcome == CandidateANotEmpty,
    overlayResult.terminalChildrenA == Set(NewChild),
  }

  def witnessPathReplacementRemainsNonempty: bool = all {
    selectedScenario == Some(PathReplacementAndParentDelete),
    firstUpdate(PathReplacementAndParentDelete).placement
      == secondUpdate(PathReplacementAndParentDelete).placement,
    referenceResult.outcome == CandidateANotEmpty,
    overlayResult.outcome == CandidateANotEmpty,
    overlayResult.terminalChildrenA == Set(ReplacementChild),
    overlayResult.terminalOwners.get(CandidateASlotA)
      == Some(ReplacementChild),
  }

  def witnessCompactedMatchingTombstoneSkipped: bool = all {
    selectedScenario == Some(CompactedChildDeleteAndParentDelete),
    referenceResult.appliedCount.get(ExistingChildA) == 0,
    overlayResult.appliedCount.get(ExistingChildA) == 0,
    overlayResult.terminalCompactedOwners.get(CandidateASlotA)
      == Some(ExistingChildA),
    overlayResult.terminalChildrenA == Set(ExistingChildA),
    overlayResult.outcome == CandidateANotEmpty,
  }

  def witnessNewIdTombstoneEvictsOccupiedPath: bool = all {
    selectedScenario == Some(NewIdTombstoneOccupiedPath),
    firstUpdate(NewIdTombstoneOccupiedPath).object == NewChild,
    initialReferenceOwners(NewIdTombstoneOccupiedPath).get(CandidateASlotA)
      == Some(ExistingChildA),
    overlayResult.terminalOwners.get(CandidateASlotA) == None,
    overlayResult.terminalChildrenA.size() == 0,
    overlayResult.outcome == Accept,
  }

  def witnessNewIdLiveReplacementOwnsPath: bool = all {
    selectedScenario == Some(NewIdLiveReplacement),
    firstUpdate(NewIdLiveReplacement).object == ReplacementChild,
    initialReferenceOwners(NewIdLiveReplacement).get(CandidateASlotA)
      == Some(ExistingChildA),
    overlayResult.terminalOwners.get(CandidateASlotA)
      == Some(ReplacementChild),
    overlayResult.terminalChildrenA == Set(ReplacementChild),
    overlayResult.outcome == CandidateANotEmpty,
  }

  def witnessDuplicateIdTombstoneThenLive: bool = all {
    selectedScenario == Some(DuplicateIdTombstoneThenLive),
    firstUpdate(DuplicateIdTombstoneThenLive).object == ExistingChildA,
    secondUpdate(DuplicateIdTombstoneThenLive).object == ExistingChildA,
    overlayResult.appliedCount.get(ExistingChildA) == 2,
    overlayResult.appliedOrdinal.get(ExistingChildA) == 3,
    overlayResult.terminalOwners.get(CandidateASlotA) == None,
    overlayResult.terminalOwners.get(CandidateASlotB)
      == Some(ExistingChildA),
    overlayResult.outcome == CandidateANotEmpty,
  }

  def witnessTwoCandidatesReportFirstFailure: bool = all {
    selectedScenario == Some(TwoCandidateDirectoriesFirstFailure),
    firstUpdate(TwoCandidateDirectoriesFirstFailure).object
      == CandidateDirectoryB,
    secondUpdate(TwoCandidateDirectoriesFirstFailure).object
      == CandidateDirectoryA,
    overlayResult.terminalChildrenA == Set(ExistingChildA),
    overlayResult.terminalChildrenB == Set(ExistingChildB),
    overlayResult.outcome == CandidateBNotEmpty,
  }

  def witnessCompactedMarkerRemovalReplaysLaterTombstone: bool = all {
    selectedScenario == Some(CompactedMarkerRemovedBeforeLaterTombstone),
    initialCompactedOwners(CompactedMarkerRemovedBeforeLaterTombstone)
      .get(CandidateASlotB) == Some(ReplacementChild),
    firstUpdate(CompactedMarkerRemovedBeforeLaterTombstone).object == NewChild,
    secondUpdate(CompactedMarkerRemovedBeforeLaterTombstone).object
      == ReplacementChild,
    overlayResult.appliedOrdinal.get(NewChild) == 1,
    overlayResult.appliedOrdinal.get(ReplacementChild) == 2,
    overlayResult.terminalCompactedOwners.get(CandidateASlotB) == None,
    overlayResult.terminalChildrenA.size() == 0,
    overlayResult.outcome == Accept,
  }

  def safety: bool = all {
    terminalDirectChildrenAreEquivalent,
    candidatePathOwnersAreEquivalent,
    validationOutcomesAreEquivalent,
    replayedObjectsAndOrderAreEquivalent,
    terminalCandidateOwnershipIsFunctional,
    exactPlannerOrderIsPreserved,
    overlaySeedsExactlyCurrentCandidateChildren,
    overlayNeverScansUnrelatedMetadata,
    referenceCostIncludesWholeMaterializedTable,
    expectedScenarioOutcome,
  }
}
