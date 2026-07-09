/// Invariant checks for `dfs_anti_entropy`.
module dfs_anti_entropy_safety {
  import common.* from "../../lib/common"
  import dfs_anti_entropy.* from "../../models/dfs/anti_entropy"

  def noBackendRunDoesNotScanOrReport: bool =
    all {
      phase != Idle,
      not(backendConfigured),
    } implies all {
      scannedObjects == Set(),
      checkedObjects == Set(),
      issueObjects == Set(),
      repairedObjects == Set(),
      transientPrunedObjects == Set(),
      storedIssueObjects == Set(),
      not(maxIssuesReached),
      backendLocations == initialBackendLocations,
      versionLocations == initialVersionLocations,
    }


  def completedBackendRunScansEveryObject: bool =
    all {
      phase == Complete,
      backendConfigured,
    } implies scannedObjects == Objects


  def checkedObjectsWereScannedAndEligible: bool =
    all {
      checkedObjects.subseteq(scannedObjects),
      checkedObjects.forall(object => objectWasInitiallyEligible(object)),
    }


  def issuesAreOnlyForStaleCheckedContentObjects: bool =
    all {
      issueObjects.subseteq(checkedObjects),
      issueObjects.forall(object => objectHadInitialIssue(object)),
    }


  def hashAddressedBlobFallbackIsHealthy: bool =
    Objects
      .filter(object => contentHealth.get(object) == HashAddressedBlobAvailable)
      .intersect(issueObjects) == Set()


  def repairedObjectsMatchRepairOption: bool =
    if (repairMetadata) {
      repairedObjects == issueObjects
    } else {
      repairedObjects == Set()
    }


  def transientPruneMatchesReportedRepair: bool =
    transientPrunedObjects == repairedObjects


  def storedIssuesAreCapped: bool =
    all {
      storedIssueObjects.subseteq(issueObjects),
      storedIssueObjects.size() <= maxIssues,
      maxIssuesReached == (issueObjects.size() > maxIssues),
    }


  def noRepairLeavesDurableLocationsUnchanged: bool =
    not(repairMetadata) implies all {
      backendLocations == initialBackendLocations,
      versionLocations == initialVersionLocations,
    }


  def durablePruneOnlyForPersistableReportedRepair: bool =
    Objects.forall(object =>
      all {
        initialBackendLocations.get(object).contains(LocalNode),
        not(backendLocations.get(object).contains(LocalNode)),
      } implies all {
        repairedObjects.contains(object),
        persistAllowed(object),
      }
    )


  def nonPersistableRepairsDoNotClaimDurableBackendPrune: bool =
    Objects.forall(object =>
      all {
        repairedObjects.contains(object),
        not(persistAllowed(object)),
        initialBackendLocations.get(object).contains(LocalNode),
      } implies backendLocations.get(object).contains(LocalNode)
    )


  def completedRepairPrunesPersistableStaleLocations: bool =
    all {
      phase == Complete,
      backendConfigured,
      repairMetadata,
    } implies Objects.forall(object =>
      all {
        objectHadInitialIssue(object),
        persistAllowed(object),
      } implies all {
        not(backendLocations.get(object).contains(LocalNode)),
        not(versionLocations.get(object).contains(LocalNode)),
      }
    )


  def safety: bool = all {
    noBackendRunDoesNotScanOrReport,
    completedBackendRunScansEveryObject,
    checkedObjectsWereScannedAndEligible,
    issuesAreOnlyForStaleCheckedContentObjects,
    hashAddressedBlobFallbackIsHealthy,
    repairedObjectsMatchRepairOption,
    transientPruneMatchesReportedRepair,
    storedIssuesAreCapped,
    noRepairLeavesDurableLocationsUnchanged,
    durablePruneOnlyForPersistableReportedRepair,
    nonPersistableRepairsDoNotClaimDurableBackendPrune,
    completedRepairPrunesPersistableStaleLocations,
  }
}
