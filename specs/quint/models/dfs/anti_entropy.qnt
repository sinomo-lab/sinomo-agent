/// DFS local content anti-entropy.
///
/// This module models the narrow maintenance path around:
/// - `run_local_content_anti_entropy`
/// - `local_content_anti_entropy_issue_reason`
/// - `record_anti_entropy_issue`
/// - `mark_remote_read_location_missing`
///
/// It intentionally consumes lower-level DFS facts: content hashes are abstract
/// values, request routing is outside the model, and repair transfer correctness
/// is covered by the repair/content model and the network session contract.
module dfs_anti_entropy {
  import common.* from "../../lib/common"

  type DfsObject = ObjA | ObjB
  type DfsObjectKind = File | Symlink | Directory
  type ContentHealth =
    HealthyManifest
    | HashAddressedBlobAvailable
    | MissingManifest
    | HashMismatch
    | SizeMismatch
    | MissingContent
  type ObjectShape =
    HealthyManifestLocal
    | HashAddressedBlobLocal
    | StaleManifestMissingLocal
    | StaleSymlinkLocal
    | DirectoryLocal
    | RemoteOnlyFile
    | MissingHashLocal
  type RunPhase = Idle | Running | Complete
  type Decision =
    NoDecision
    | RunStarted
    | ObjectSkipped
    | LocalLocationHealthy
    | IssueRecorded
    | MetadataRepairRecorded
    | RunCompleted

  pure val LocalNode: Node = N1
  pure val Objects: Set[DfsObject] = Set(ObjA, ObjB)
  pure val ObjectShapes: Set[ObjectShape] = Set(
    HealthyManifestLocal,
    HashAddressedBlobLocal,
    StaleManifestMissingLocal,
    StaleSymlinkLocal,
    DirectoryLocal,
    RemoteOnlyFile,
    MissingHashLocal
  )
  pure val IssueLimits: Set[int] = Set(0, 1, 2)

  var phase: RunPhase
  var backendConfigured: bool
  var repairMetadata: bool
  var controlPlaneConfigured: bool
  var localPrimaryForObject: DfsObject -> bool
  var objectKind: DfsObject -> DfsObjectKind
  var contentHashPresent: DfsObject -> bool
  var contentHealth: DfsObject -> ContentHealth
  var initialBackendLocations: DfsObject -> Set[Node]
  var initialVersionLocations: DfsObject -> Set[Node]
  var backendLocations: DfsObject -> Set[Node]
  var versionLocations: DfsObject -> Set[Node]
  var scannedObjects: Set[DfsObject]
  var checkedObjects: Set[DfsObject]
  var issueObjects: Set[DfsObject]
  var repairedObjects: Set[DfsObject]
  var transientPrunedObjects: Set[DfsObject]
  var storedIssueObjects: Set[DfsObject]
  var maxIssues: int
  var maxIssuesReached: bool
  var lastDecision: Decision

  pure val emptyKindMap: DfsObject -> DfsObjectKind =
    Objects.mapBy(_ => Directory)

  pure val emptyBoolMap: DfsObject -> bool =
    Objects.mapBy(_ => false)

  pure val emptyHealthMap: DfsObject -> ContentHealth =
    Objects.mapBy(_ => HealthyManifest)

  pure val emptyLocationMap: DfsObject -> Set[Node] =
    Objects.mapBy(_ => Set())

  pure def removeLocal(locations: Set[Node]): Set[Node] =
    locations.filter(node => node != LocalNode)

  pure def isContentObject(kind: DfsObjectKind): bool =
    kind == File or kind == Symlink

  pure def contentIsHealthy(health: ContentHealth): bool =
    any {
      health == HealthyManifest,
      health == HashAddressedBlobAvailable,
    }

  pure def shapeKind(shape: ObjectShape): DfsObjectKind =
    if (shape == StaleSymlinkLocal) {
      Symlink
    } else if (shape == DirectoryLocal) {
      Directory
    } else {
      File
    }

  pure def shapeContentHashPresent(shape: ObjectShape): bool =
    shape != MissingHashLocal

  pure def shapeContentHealth(shape: ObjectShape): ContentHealth =
    if (shape == HealthyManifestLocal) {
      HealthyManifest
    } else if (shape == HashAddressedBlobLocal) {
      HashAddressedBlobAvailable
    } else if (shape == StaleSymlinkLocal) {
      MissingContent
    } else if (shape == DirectoryLocal) {
      MissingContent
    } else if (shape == RemoteOnlyFile) {
      MissingContent
    } else if (shape == MissingHashLocal) {
      MissingContent
    } else {
      MissingManifest
    }

  pure def shapeBackendLocations(shape: ObjectShape): Set[Node] =
    if (shape == RemoteOnlyFile) {
      Set(N2)
    } else if (shape == HealthyManifestLocal or shape == StaleManifestMissingLocal) {
      Set(N1, N2)
    } else {
      Set(N1)
    }

  pure def shapeVersionLocations(shape: ObjectShape): Set[Node] =
    if (shape == RemoteOnlyFile) {
      Set(N2)
    } else if (shape == HealthyManifestLocal or shape == StaleManifestMissingLocal) {
      Set(N1, N2)
    } else {
      Set(N1)
    }

  def objectIsScanEligible(object: DfsObject): bool =
    all {
      backendConfigured,
      isContentObject(objectKind.get(object)),
      backendLocations.get(object).contains(LocalNode),
      contentHashPresent.get(object),
    }

  def objectWasInitiallyEligible(object: DfsObject): bool =
    all {
      backendConfigured,
      isContentObject(objectKind.get(object)),
      initialBackendLocations.get(object).contains(LocalNode),
      contentHashPresent.get(object),
    }

  def objectHadInitialIssue(object: DfsObject): bool =
    all {
      objectWasInitiallyEligible(object),
      not(contentIsHealthy(contentHealth.get(object))),
    }

  def persistAllowed(object: DfsObject): bool =
    not(controlPlaneConfigured) or localPrimaryForObject.get(object)

  action init = all {
    phase' = Idle,
    backendConfigured' = false,
    repairMetadata' = false,
    controlPlaneConfigured' = false,
    localPrimaryForObject' = emptyBoolMap,
    objectKind' = emptyKindMap,
    contentHashPresent' = emptyBoolMap,
    contentHealth' = emptyHealthMap,
    initialBackendLocations' = emptyLocationMap,
    initialVersionLocations' = emptyLocationMap,
    backendLocations' = emptyLocationMap,
    versionLocations' = emptyLocationMap,
    scannedObjects' = Set(),
    checkedObjects' = Set(),
    issueObjects' = Set(),
    repairedObjects' = Set(),
    transientPrunedObjects' = Set(),
    storedIssueObjects' = Set(),
    maxIssues' = 0,
    maxIssuesReached' = false,
    lastDecision' = NoDecision,
  }

  action beginRun(
    backendConfiguredValue: bool,
    repairMetadataValue: bool,
    controlPlaneConfiguredValue: bool,
    shapeA: ObjectShape,
    shapeB: ObjectShape,
    primaryA: bool,
    primaryB: bool,
    maxIssueLimit: int
  ): bool = {
    val backendLocationMap = Objects.mapBy(object =>
      if (object == ObjA) { shapeBackendLocations(shapeA) } else { shapeBackendLocations(shapeB) }
    )
    val versionLocationMap = Objects.mapBy(object =>
      if (object == ObjA) { shapeVersionLocations(shapeA) } else { shapeVersionLocations(shapeB) }
    )
    all {
      phase == Idle,
      phase' = Running,
      backendConfigured' = backendConfiguredValue,
      repairMetadata' = repairMetadataValue,
      controlPlaneConfigured' = controlPlaneConfiguredValue,
      localPrimaryForObject' = Objects.mapBy(object =>
        if (object == ObjA) { primaryA } else { primaryB }
      ),
      objectKind' = Objects.mapBy(object =>
        if (object == ObjA) { shapeKind(shapeA) } else { shapeKind(shapeB) }
      ),
      contentHashPresent' = Objects.mapBy(object =>
        if (object == ObjA) { shapeContentHashPresent(shapeA) } else { shapeContentHashPresent(shapeB) }
      ),
      contentHealth' = Objects.mapBy(object =>
        if (object == ObjA) { shapeContentHealth(shapeA) } else { shapeContentHealth(shapeB) }
      ),
      initialBackendLocations' = backendLocationMap,
      initialVersionLocations' = versionLocationMap,
      backendLocations' = backendLocationMap,
      versionLocations' = versionLocationMap,
      scannedObjects' = Set(),
      checkedObjects' = Set(),
      issueObjects' = Set(),
      repairedObjects' = Set(),
      transientPrunedObjects' = Set(),
      storedIssueObjects' = Set(),
      maxIssues' = maxIssueLimit,
      maxIssuesReached' = false,
      lastDecision' = RunStarted,
    }
  }

  action scanObject(object: DfsObject): bool = {
    val eligible = objectIsScanEligible(object)
    val hasIssue = eligible and not(contentIsHealthy(contentHealth.get(object)))
    val shouldRepair = hasIssue and repairMetadata
    val shouldPersist = shouldRepair and persistAllowed(object)
    all {
      phase == Running,
      backendConfigured,
      not(scannedObjects.contains(object)),
      phase' = phase,
      backendConfigured' = backendConfigured,
      repairMetadata' = repairMetadata,
      controlPlaneConfigured' = controlPlaneConfigured,
      localPrimaryForObject' = localPrimaryForObject,
      objectKind' = objectKind,
      contentHashPresent' = contentHashPresent,
      contentHealth' = contentHealth,
      initialBackendLocations' = initialBackendLocations,
      initialVersionLocations' = initialVersionLocations,
      backendLocations' =
        if (shouldPersist) {
          backendLocations.set(object, removeLocal(backendLocations.get(object)))
        } else {
          backendLocations
        },
      versionLocations' =
        if (shouldPersist) {
          versionLocations.set(object, removeLocal(versionLocations.get(object)))
        } else {
          versionLocations
        },
      scannedObjects' = scannedObjects.union(Set(object)),
      checkedObjects' =
        if (eligible) { checkedObjects.union(Set(object)) } else { checkedObjects },
      issueObjects' =
        if (hasIssue) { issueObjects.union(Set(object)) } else { issueObjects },
      repairedObjects' =
        if (shouldRepair) { repairedObjects.union(Set(object)) } else { repairedObjects },
      transientPrunedObjects' =
        if (shouldRepair) { transientPrunedObjects.union(Set(object)) } else { transientPrunedObjects },
      storedIssueObjects' =
        if (hasIssue and storedIssueObjects.size() < maxIssues) {
          storedIssueObjects.union(Set(object))
        } else {
          storedIssueObjects
        },
      maxIssues' = maxIssues,
      maxIssuesReached' =
        if (hasIssue) {
          if (storedIssueObjects.size() < maxIssues) {
            maxIssuesReached
          } else {
            true
          }
        } else {
          maxIssuesReached
        },
      lastDecision' =
        if (shouldRepair) {
          MetadataRepairRecorded
        } else if (hasIssue) {
          IssueRecorded
        } else if (eligible) {
          LocalLocationHealthy
        } else {
          ObjectSkipped
        },
    }
  }

  action completeRun: bool = all {
    phase == Running,
    any {
      not(backendConfigured),
      scannedObjects == Objects,
    },
    phase' = Complete,
    backendConfigured' = backendConfigured,
    repairMetadata' = repairMetadata,
    controlPlaneConfigured' = controlPlaneConfigured,
    localPrimaryForObject' = localPrimaryForObject,
    objectKind' = objectKind,
    contentHashPresent' = contentHashPresent,
    contentHealth' = contentHealth,
    initialBackendLocations' = initialBackendLocations,
    initialVersionLocations' = initialVersionLocations,
    backendLocations' = backendLocations,
    versionLocations' = versionLocations,
    scannedObjects' = scannedObjects,
    checkedObjects' = checkedObjects,
    issueObjects' = issueObjects,
    repairedObjects' = repairedObjects,
    transientPrunedObjects' = transientPrunedObjects,
    storedIssueObjects' = storedIssueObjects,
    maxIssues' = maxIssues,
    maxIssuesReached' = maxIssuesReached,
    lastDecision' = RunCompleted,
  }

  action stutterComplete: bool = all {
    phase == Complete,
    phase' = phase,
    backendConfigured' = backendConfigured,
    repairMetadata' = repairMetadata,
    controlPlaneConfigured' = controlPlaneConfigured,
    localPrimaryForObject' = localPrimaryForObject,
    objectKind' = objectKind,
    contentHashPresent' = contentHashPresent,
    contentHealth' = contentHealth,
    initialBackendLocations' = initialBackendLocations,
    initialVersionLocations' = initialVersionLocations,
    backendLocations' = backendLocations,
    versionLocations' = versionLocations,
    scannedObjects' = scannedObjects,
    checkedObjects' = checkedObjects,
    issueObjects' = issueObjects,
    repairedObjects' = repairedObjects,
    transientPrunedObjects' = transientPrunedObjects,
    storedIssueObjects' = storedIssueObjects,
    maxIssues' = maxIssues,
    maxIssuesReached' = maxIssuesReached,
    lastDecision' = lastDecision,
  }

  action step = any {
    {
      nondet backendConfiguredValue = oneOf(Set(true, false))
      nondet repairMetadataValue = oneOf(Set(true, false))
      nondet controlPlaneConfiguredValue = oneOf(Set(true, false))
      nondet shapeA = oneOf(ObjectShapes)
      nondet shapeB = oneOf(ObjectShapes)
      nondet primaryA = oneOf(Set(true, false))
      nondet primaryB = oneOf(Set(true, false))
      nondet maxIssueLimit = oneOf(IssueLimits)
      beginRun(
        backendConfiguredValue,
        repairMetadataValue,
        controlPlaneConfiguredValue,
        shapeA,
        shapeB,
        primaryA,
        primaryB,
        maxIssueLimit
      )
    },
    {
      nondet object = oneOf(Objects)
      scanObject(object)
    },
    completeRun,
    stutterComplete,
  }
}
