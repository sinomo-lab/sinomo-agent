/// DFS directory-tombstone precondition overlay equivalence.
///
/// Evidence class: target hardening for replacing the full
/// `DfsMetadataTables` clone in
/// `validate_namespace_transaction_preconditions` with sparse direct-child
/// ownership for only the directories being tombstoned.
///
/// The reference algorithm clones every materialized row and path owner, then
/// replays transaction objects in planner order: tombstones first, live objects
/// second, with caller order retained inside each partition. Every replayed
/// object first loses its prior placement and evicts the current owner of its
/// target slot. A live row then owns the target; a tombstone leaves it empty.
/// Matching compacted tombstones are skipped until another replayed object
/// removes their path marker.
///
/// The overlay tracks the same ownership transitions only for slots directly
/// below candidate directories. Three explicit transaction slots cover parent
/// deletion, recursive deletion, moves, new children, same-path replacement,
/// new-id tombstones, duplicate ids across planner partitions, two candidate
/// directories with deterministic first-error order, and dynamic compacted
/// marker removal. Byte-level dentry-key collisions and arbitrary path
/// normalization remain outside this bounded slice and require Rust
/// differential coverage or an enforced canonical-path invariant.
module dfs_namespace_tombstone_overlay_equivalence {
  import common.* from "../../lib/common"

  type MetadataObject = CandidateDirectoryA | CandidateDirectoryB
    | ExistingChildA | ExistingChildB | NewChild | ReplacementChild
    | UnrelatedA | UnrelatedB
  type ObjectKind = Directory | File | Tombstone | Missing
  type PlannerPartition = TombstonePartition | LivePartition
  type Placement = CandidateASlotA | CandidateASlotB
    | CandidateBSlotA | CandidateBSlotB | OutsideCandidate | NoPlacement
  type ValidationOutcome = Accept | CandidateANotEmpty | CandidateBNotEmpty
  type OverlayScenario = ParentOnlyDelete | ChildAndParentDelete
    | ChildMoveOutAndParentDelete | ChildLiveInPlaceAndParentDelete
    | ChildMoveIntoDeletedParent | NewChildIntoDeletedParent
    | PathReplacementAndParentDelete | CompactedChildDeleteAndParentDelete
    | NewIdTombstoneOccupiedPath | NewIdLiveReplacement
    | DuplicateIdTombstoneThenLive | TwoCandidateDirectoriesFirstFailure
    | CompactedMarkerRemovedBeforeLaterTombstone
  type ComparisonPhase = Ready | Compared

  type ObjectRow = {
    kind: ObjectKind,
    placement: Placement,
  }

  type TransactionUpdate = {
    present: bool,
    object: MetadataObject,
    kind: ObjectKind,
    placement: Placement,
  }

  type ReferenceReplay = {
    rows: MetadataObject -> ObjectRow,
    owners: Placement -> Option[MetadataObject],
    compactedOwners: Placement -> Option[MetadataObject],
    nextOrdinal: int,
    appliedOrdinal: MetadataObject -> int,
    appliedCount: MetadataObject -> int,
    pointLookups: Set[MetadataObject],
  }

  type OverlayReplay = {
    owners: Placement -> Option[MetadataObject],
    compactedOwners: Placement -> Option[MetadataObject],
    nextOrdinal: int,
    appliedOrdinal: MetadataObject -> int,
    appliedCount: MetadataObject -> int,
    pointLookups: Set[MetadataObject],
  }

  type ReferenceResult = {
    terminalRows: MetadataObject -> ObjectRow,
    terminalOwners: Placement -> Option[MetadataObject],
    terminalCompactedOwners: Placement -> Option[MetadataObject],
    terminalChildrenA: Set[MetadataObject],
    terminalChildrenB: Set[MetadataObject],
    outcome: ValidationOutcome,
    fullTableClones: int,
    scannedMetadata: Set[MetadataObject],
    appliedOrdinal: MetadataObject -> int,
    appliedCount: MetadataObject -> int,
    pointLookups: Set[MetadataObject],
  }

  type OverlayResult = {
    terminalOwners: Placement -> Option[MetadataObject],
    terminalCompactedOwners: Placement -> Option[MetadataObject],
    terminalChildrenA: Set[MetadataObject],
    terminalChildrenB: Set[MetadataObject],
    outcome: ValidationOutcome,
    fullTableClones: int,
    scannedDentryObjects: Set[MetadataObject],
    appliedOrdinal: MetadataObject -> int,
    appliedCount: MetadataObject -> int,
    pointLookups: Set[MetadataObject],
  }

  pure val MetadataObjects: Set[MetadataObject] = Set(
    CandidateDirectoryA,
    CandidateDirectoryB,
    ExistingChildA,
    ExistingChildB,
    NewChild,
    ReplacementChild,
    UnrelatedA,
    UnrelatedB
  )
  pure val Placements: Set[Placement] = Set(
    CandidateASlotA,
    CandidateASlotB,
    CandidateBSlotA,
    CandidateBSlotB,
    OutsideCandidate,
    NoPlacement
  )
  pure val OverlayScenarios: Set[OverlayScenario] = Set(
    ParentOnlyDelete,
    ChildAndParentDelete,
    ChildMoveOutAndParentDelete,
    ChildLiveInPlaceAndParentDelete,
    ChildMoveIntoDeletedParent,
    NewChildIntoDeletedParent,
    PathReplacementAndParentDelete,
    CompactedChildDeleteAndParentDelete,
    NewIdTombstoneOccupiedPath,
    NewIdLiveReplacement,
    DuplicateIdTombstoneThenLive,
    TwoCandidateDirectoriesFirstFailure,
    CompactedMarkerRemovedBeforeLaterTombstone
  )
  pure val CandidateAPlacements: Set[Placement] =
    Set(CandidateASlotA, CandidateASlotB)
  pure val CandidateBPlacements: Set[Placement] =
    Set(CandidateBSlotA, CandidateBSlotB)
  pure val CandidatePlacements: Set[Placement] =
    CandidateAPlacements.union(CandidateBPlacements)
  pure val OwnedPlacements: Set[Placement] =
    CandidatePlacements.union(Set(OutsideCandidate))
  pure val UnrelatedMetadata: Set[MetadataObject] = Set(UnrelatedA, UnrelatedB)

  pure val missingRow: ObjectRow = {
    kind: Missing,
    placement: NoPlacement,
  }

  pure val baseRows: MetadataObject -> ObjectRow =
    MetadataObjects
      .mapBy(_ => missingRow)
      .set(CandidateDirectoryA, {
        kind: Directory,
        placement: NoPlacement,
      })
      .set(CandidateDirectoryB, {
        kind: Directory,
        placement: NoPlacement,
      })
      .set(UnrelatedA, {
        kind: File,
        placement: OutsideCandidate,
      })
      .set(UnrelatedB, {
        kind: Directory,
        placement: OutsideCandidate,
      })

  pure val emptyOwners: Placement -> Option[MetadataObject] =
    Placements.mapBy(_ => None)
  pure val emptyOrdinals: MetadataObject -> int =
    MetadataObjects.mapBy(_ => 0)
  pure val emptyCounts: MetadataObject -> int =
    MetadataObjects.mapBy(_ => 0)

  pure val emptyUpdate: TransactionUpdate = {
    present: false,
    object: CandidateDirectoryA,
    kind: Missing,
    placement: NoPlacement,
  }

  pure def liveRow(placement: Placement): ObjectRow = {
    kind: File,
    placement: placement,
  }

  pure def update(
    object: MetadataObject,
    kind: ObjectKind,
    placement: Placement,
  ): TransactionUpdate = {
    present: true,
    object: object,
    kind: kind,
    placement: placement,
  }

  pure def parentATombstone: TransactionUpdate =
    update(CandidateDirectoryA, Tombstone, NoPlacement)

  pure def parentBTombstone: TransactionUpdate =
    update(CandidateDirectoryB, Tombstone, NoPlacement)

  pure def initialRows(
    scenario: OverlayScenario,
  ): MetadataObject -> ObjectRow =
    match scenario {
      | ChildMoveIntoDeletedParent =>
          baseRows.set(ExistingChildA, liveRow(OutsideCandidate))
      | NewChildIntoDeletedParent => baseRows
      | TwoCandidateDirectoriesFirstFailure =>
          baseRows
            .set(ExistingChildA, liveRow(CandidateASlotA))
            .set(ExistingChildB, liveRow(CandidateBSlotA))
      | CompactedMarkerRemovedBeforeLaterTombstone =>
          baseRows.set(ReplacementChild, liveRow(CandidateASlotA))
      | _ => baseRows.set(ExistingChildA, liveRow(CandidateASlotA))
    }

  pure def initialReferenceOwners(
    scenario: OverlayScenario,
  ): Placement -> Option[MetadataObject] =
    match scenario {
      | ChildMoveIntoDeletedParent =>
          emptyOwners.set(OutsideCandidate, Some(ExistingChildA))
      | NewChildIntoDeletedParent => emptyOwners
      | TwoCandidateDirectoriesFirstFailure =>
          emptyOwners
            .set(CandidateASlotA, Some(ExistingChildA))
            .set(CandidateBSlotA, Some(ExistingChildB))
      | CompactedMarkerRemovedBeforeLaterTombstone =>
          emptyOwners.set(CandidateASlotA, Some(ReplacementChild))
      | _ => emptyOwners.set(CandidateASlotA, Some(ExistingChildA))
    }

  pure def validatesCandidateB(scenario: OverlayScenario): bool =
    scenario == TwoCandidateDirectoriesFirstFailure

  pure def initialOverlayOwners(
    scenario: OverlayScenario,
  ): Placement -> Option[MetadataObject] = {
    val referenceOwners = initialReferenceOwners(scenario)
    Placements.mapBy(placement =>
      if (CandidateAPlacements.contains(placement)
          or (validatesCandidateB(scenario)
            and CandidateBPlacements.contains(placement))) {
        referenceOwners.get(placement)
      } else {
        None
      }
    )
  }

  pure def initialCompactedOwners(
    scenario: OverlayScenario,
  ): Placement -> Option[MetadataObject] =
    match scenario {
      | CompactedChildDeleteAndParentDelete =>
          emptyOwners.set(CandidateASlotA, Some(ExistingChildA))
      | CompactedMarkerRemovedBeforeLaterTombstone =>
          emptyOwners.set(CandidateASlotB, Some(ReplacementChild))
      | _ => emptyOwners
    }

  /// Caller transaction order. Replay scans all three slots once for
  /// tombstones, then once for live rows.
  pure def firstUpdate(scenario: OverlayScenario): TransactionUpdate =
    match scenario {
      | ParentOnlyDelete => parentATombstone
      | ChildAndParentDelete =>
          update(ExistingChildA, Tombstone, CandidateASlotA)
      | ChildMoveOutAndParentDelete =>
          update(ExistingChildA, File, OutsideCandidate)
      | ChildLiveInPlaceAndParentDelete =>
          update(ExistingChildA, File, CandidateASlotA)
      | ChildMoveIntoDeletedParent =>
          update(ExistingChildA, File, CandidateASlotA)
      | NewChildIntoDeletedParent =>
          update(NewChild, File, CandidateASlotB)
      | PathReplacementAndParentDelete =>
          update(ReplacementChild, File, CandidateASlotA)
      | CompactedChildDeleteAndParentDelete =>
          update(ExistingChildA, Tombstone, CandidateASlotA)
      | NewIdTombstoneOccupiedPath =>
          update(NewChild, Tombstone, CandidateASlotA)
      | NewIdLiveReplacement =>
          update(ReplacementChild, File, CandidateASlotA)
      | DuplicateIdTombstoneThenLive =>
          update(ExistingChildA, File, CandidateASlotB)
      | TwoCandidateDirectoriesFirstFailure => parentBTombstone
      | CompactedMarkerRemovedBeforeLaterTombstone =>
          update(NewChild, Tombstone, CandidateASlotB)
    }

  pure def secondUpdate(scenario: OverlayScenario): TransactionUpdate =
    match scenario {
      | ParentOnlyDelete => emptyUpdate
      | PathReplacementAndParentDelete =>
          update(ExistingChildA, Tombstone, CandidateASlotA)
      | DuplicateIdTombstoneThenLive =>
          update(ExistingChildA, Tombstone, CandidateASlotA)
      | TwoCandidateDirectoriesFirstFailure => parentATombstone
      | CompactedMarkerRemovedBeforeLaterTombstone =>
          update(ReplacementChild, Tombstone, CandidateASlotB)
      | _ => parentATombstone
    }

  pure def thirdUpdate(scenario: OverlayScenario): TransactionUpdate =
    match scenario {
      | PathReplacementAndParentDelete => parentATombstone
      | DuplicateIdTombstoneThenLive => parentATombstone
      | CompactedMarkerRemovedBeforeLaterTombstone => parentATombstone
      | _ => emptyUpdate
    }

  pure def isLiveKind(kind: ObjectKind): bool =
    any { kind == Directory, kind == File }

  pure def ownersWithoutObject(
    owners: Placement -> Option[MetadataObject],
    object: MetadataObject,
  ): Placement -> Option[MetadataObject] =
    Placements.mapBy(placement =>
      if (owners.get(placement) == Some(object)) None
      else owners.get(placement)
    )

  /// Ownership replay shared by the reference and overlay. `trackedPlacements`
  /// is all materialized slots for the reference and only candidate-directory
  /// slots for the sparse overlay.
  pure def applyOwnershipUpdate(
    owners: Placement -> Option[MetadataObject],
    candidate: TransactionUpdate,
    trackedPlacements: Set[Placement],
  ): Placement -> Option[MetadataObject] = {
    val withoutPriorObject = ownersWithoutObject(owners, candidate.object)
    val afterTargetEviction =
      if (trackedPlacements.contains(candidate.placement)) {
        withoutPriorObject.set(candidate.placement, None)
      } else {
        withoutPriorObject
      }
    if (isLiveKind(candidate.kind)
        and trackedPlacements.contains(candidate.placement)) {
      afterTargetEviction.set(candidate.placement, Some(candidate.object))
    } else {
      afterTargetEviction
    }
  }

  pure def removeCompactedMarkerAtTarget(
    compactedOwners: Placement -> Option[MetadataObject],
    candidate: TransactionUpdate,
  ): Placement -> Option[MetadataObject] =
    if (OwnedPlacements.contains(candidate.placement)) {
      compactedOwners.set(candidate.placement, None)
    } else {
      compactedOwners
    }

  pure def compactedTombstoneMatches(
    compactedOwners: Placement -> Option[MetadataObject],
    candidate: TransactionUpdate,
  ): bool = all {
    candidate.kind == Tombstone,
    compactedOwners.get(candidate.placement) == Some(candidate.object),
  }

  pure def shouldReplay(
    candidate: TransactionUpdate,
    plannerPartition: PlannerPartition,
    compactedOwners: Placement -> Option[MetadataObject],
  ): bool = all {
    candidate.present,
    if (plannerPartition == TombstonePartition) {
      candidate.kind == Tombstone
    } else {
      candidate.kind != Tombstone and candidate.kind != Missing
    },
    not(compactedTombstoneMatches(compactedOwners, candidate)),
  }

  pure def applyReferenceUpdate(
    replay: ReferenceReplay,
    candidate: TransactionUpdate,
    plannerPartition: PlannerPartition,
  ): ReferenceReplay =
    if (shouldReplay(candidate, plannerPartition, replay.compactedOwners)) {
      {
        rows: replay.rows.set(candidate.object, {
          kind: candidate.kind,
          placement: candidate.placement,
        }),
        owners: applyOwnershipUpdate(
          replay.owners,
          candidate,
          OwnedPlacements,
        ),
        compactedOwners:
          removeCompactedMarkerAtTarget(replay.compactedOwners, candidate),
        nextOrdinal: replay.nextOrdinal + 1,
        appliedOrdinal:
          replay.appliedOrdinal.set(candidate.object, replay.nextOrdinal),
        appliedCount: replay.appliedCount
          .set(candidate.object, replay.appliedCount.get(candidate.object) + 1),
        pointLookups: replay.pointLookups.union(Set(candidate.object)),
      }
    } else {
      replay
    }

  pure def applyOverlayUpdate(
    replay: OverlayReplay,
    candidate: TransactionUpdate,
    plannerPartition: PlannerPartition,
  ): OverlayReplay =
    if (shouldReplay(candidate, plannerPartition, replay.compactedOwners)) {
      {
        owners: applyOwnershipUpdate(
          replay.owners,
          candidate,
          CandidatePlacements,
        ),
        compactedOwners:
          removeCompactedMarkerAtTarget(replay.compactedOwners, candidate),
        nextOrdinal: replay.nextOrdinal + 1,
        appliedOrdinal:
          replay.appliedOrdinal.set(candidate.object, replay.nextOrdinal),
        appliedCount: replay.appliedCount
          .set(candidate.object, replay.appliedCount.get(candidate.object) + 1),
        pointLookups: replay.pointLookups.union(Set(candidate.object)),
      }
    } else {
      replay
    }

  pure def directChildrenFor(
    owners: Placement -> Option[MetadataObject],
    candidatePlacements: Set[Placement],
  ): Set[MetadataObject] =
    MetadataObjects.filter(object =>
      candidatePlacements.exists(placement =>
        owners.get(placement) == Some(object)
      )
    )

  pure def directChildrenA(
    owners: Placement -> Option[MetadataObject],
  ): Set[MetadataObject] = directChildrenFor(owners, CandidateAPlacements)

  pure def directChildrenB(
    owners: Placement -> Option[MetadataObject],
  ): Set[MetadataObject] = directChildrenFor(owners, CandidateBPlacements)

  pure def materializedObjects(
    rows: MetadataObject -> ObjectRow,
  ): Set[MetadataObject] =
    MetadataObjects.filter(object => rows.get(object).kind != Missing)

  pure def validationAfterCandidate(
    current: ValidationOutcome,
    candidate: TransactionUpdate,
    childrenA: Set[MetadataObject],
    childrenB: Set[MetadataObject],
  ): ValidationOutcome =
    if (current != Accept) {
      current
    } else if (candidate.present
        and candidate.kind == Tombstone
        and candidate.object == CandidateDirectoryA
        and childrenA.size() > 0) {
      CandidateANotEmpty
    } else if (candidate.present
        and candidate.kind == Tombstone
        and candidate.object == CandidateDirectoryB
        and childrenB.size() > 0) {
      CandidateBNotEmpty
    } else {
      Accept
    }

  pure def validationOutcome(
    scenario: OverlayScenario,
    owners: Placement -> Option[MetadataObject],
  ): ValidationOutcome = {
    val childrenA = directChildrenA(owners)
    val childrenB = directChildrenB(owners)
    val afterFirst = validationAfterCandidate(
      Accept,
      firstUpdate(scenario),
      childrenA,
      childrenB,
    )
    val afterSecond = validationAfterCandidate(
      afterFirst,
      secondUpdate(scenario),
      childrenA,
      childrenB,
    )
    validationAfterCandidate(
      afterSecond,
      thirdUpdate(scenario),
      childrenA,
      childrenB,
    )
  }

  /// Exact planner order: input order inside the tombstone partition, followed
  /// by input order inside the live partition.
  pure def replayReference(
    scenario: OverlayScenario,
  ): ReferenceReplay = {
    val initial = {
      rows: initialRows(scenario),
      owners: initialReferenceOwners(scenario),
      compactedOwners: initialCompactedOwners(scenario),
      nextOrdinal: 1,
      appliedOrdinal: emptyOrdinals,
      appliedCount: emptyCounts,
      pointLookups: Set(),
    }
    val tombstone1 =
      applyReferenceUpdate(initial, firstUpdate(scenario), TombstonePartition)
    val tombstone2 =
      applyReferenceUpdate(tombstone1, secondUpdate(scenario), TombstonePartition)
    val tombstone3 =
      applyReferenceUpdate(tombstone2, thirdUpdate(scenario), TombstonePartition)
    val live1 =
      applyReferenceUpdate(tombstone3, firstUpdate(scenario), LivePartition)
    val live2 =
      applyReferenceUpdate(live1, secondUpdate(scenario), LivePartition)
    applyReferenceUpdate(live2, thirdUpdate(scenario), LivePartition)
  }

  pure def replayOverlay(
    scenario: OverlayScenario,
  ): OverlayReplay = {
    val initial = {
      owners: initialOverlayOwners(scenario),
      compactedOwners: initialCompactedOwners(scenario),
      nextOrdinal: 1,
      appliedOrdinal: emptyOrdinals,
      appliedCount: emptyCounts,
      pointLookups: Set(),
    }
    val tombstone1 =
      applyOverlayUpdate(initial, firstUpdate(scenario), TombstonePartition)
    val tombstone2 =
      applyOverlayUpdate(tombstone1, secondUpdate(scenario), TombstonePartition)
    val tombstone3 =
      applyOverlayUpdate(tombstone2, thirdUpdate(scenario), TombstonePartition)
    val live1 =
      applyOverlayUpdate(tombstone3, firstUpdate(scenario), LivePartition)
    val live2 =
      applyOverlayUpdate(live1, secondUpdate(scenario), LivePartition)
    applyOverlayUpdate(live2, thirdUpdate(scenario), LivePartition)
  }

  pure def initialScannedDentryObjects(
    scenario: OverlayScenario,
  ): Set[MetadataObject] = {
    val owners = initialReferenceOwners(scenario)
    val childrenA = directChildrenA(owners)
    if (validatesCandidateB(scenario)) {
      childrenA.union(directChildrenB(owners))
    } else {
      childrenA
    }
  }

  pure def runReference(scenario: OverlayScenario): ReferenceResult = {
    val replay = replayReference(scenario)
    val childrenA = directChildrenA(replay.owners)
    val childrenB = directChildrenB(replay.owners)
    {
      terminalRows: replay.rows,
      terminalOwners: replay.owners,
      terminalCompactedOwners: replay.compactedOwners,
      terminalChildrenA: childrenA,
      terminalChildrenB: childrenB,
      outcome: validationOutcome(scenario, replay.owners),
      fullTableClones: 1,
      scannedMetadata: materializedObjects(initialRows(scenario)),
      appliedOrdinal: replay.appliedOrdinal,
      appliedCount: replay.appliedCount,
      pointLookups: replay.pointLookups,
    }
  }

  pure def runOverlay(scenario: OverlayScenario): OverlayResult = {
    val replay = replayOverlay(scenario)
    val childrenA = directChildrenA(replay.owners)
    val childrenB = directChildrenB(replay.owners)
    {
      terminalOwners: replay.owners,
      terminalCompactedOwners: replay.compactedOwners,
      terminalChildrenA: childrenA,
      terminalChildrenB: childrenB,
      outcome: validationOutcome(scenario, replay.owners),
      fullTableClones: 0,
      scannedDentryObjects: initialScannedDentryObjects(scenario),
      appliedOrdinal: replay.appliedOrdinal,
      appliedCount: replay.appliedCount,
      pointLookups: replay.pointLookups,
    }
  }

  pure val emptyReferenceResult: ReferenceResult = {
    terminalRows: baseRows,
    terminalOwners: emptyOwners,
    terminalCompactedOwners: emptyOwners,
    terminalChildrenA: Set(),
    terminalChildrenB: Set(),
    outcome: Accept,
    fullTableClones: 0,
    scannedMetadata: Set(),
    appliedOrdinal: emptyOrdinals,
    appliedCount: emptyCounts,
    pointLookups: Set(),
  }

  pure val emptyOverlayResult: OverlayResult = {
    terminalOwners: emptyOwners,
    terminalCompactedOwners: emptyOwners,
    terminalChildrenA: Set(),
    terminalChildrenB: Set(),
    outcome: Accept,
    fullTableClones: 0,
    scannedDentryObjects: Set(),
    appliedOrdinal: emptyOrdinals,
    appliedCount: emptyCounts,
    pointLookups: Set(),
  }

  var phase: ComparisonPhase
  var selectedScenario: Option[OverlayScenario]
  var referenceResult: ReferenceResult
  var overlayResult: OverlayResult

  action init = all {
    phase' = Ready,
    selectedScenario' = None,
    referenceResult' = emptyReferenceResult,
    overlayResult' = emptyOverlayResult,
  }

  action compare(scenario: OverlayScenario): bool = all {
    phase == Ready,
    OverlayScenarios.contains(scenario),
    phase' = Compared,
    selectedScenario' = Some(scenario),
    referenceResult' = runReference(scenario),
    overlayResult' = runOverlay(scenario),
  }

  action stutter: bool = all {
    phase == Compared,
    phase' = phase,
    selectedScenario' = selectedScenario,
    referenceResult' = referenceResult,
    overlayResult' = overlayResult,
  }

  action step = any {
    {
      nondet scenario = oneOf(OverlayScenarios)
      compare(scenario)
    },
    stutter,
  }
}
