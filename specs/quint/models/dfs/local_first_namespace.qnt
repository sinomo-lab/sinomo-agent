/// Bounded same-shard local-first DFS namespace reconciliation.
///
/// Canonical metadata remains a single quorum-owned map. An opt-in client may
/// branch from that map, perform exactly one atomic namespace syscall in its
/// local overlay, seal the branch, and later ask the current authority to
/// classify and commit it. The separate namespace-durability companion models
/// crash persistence and durable-only sealing. Local actions never mutate
/// canonical metadata.
///
/// Names are represented by stable `(parent object, name)` keys. Bindings carry
/// exact entry stamps so delete/recreate ABA cycles cannot satisfy an old
/// unlink or rename precondition. The slice deliberately stays on one metadata
/// shard and excludes hard links, symlinks, cross-shard moves, and open-unlinked
/// lifetime.
module dfs_local_first_namespace {
  import common.* from "../../lib/common"

  type NamespaceObject =
    | RootObject
    | FileObject
    | DirectoryObject
    | LocalFileObject
    | LocalDirectoryObject
    | StrongFileObject
    | StrongDirectoryObject
    | StrongSpareObject

  type EntryName = FileName | DirectoryName | SpareName | ChildName
  type EntryKey =
    | RootFileEntry
    | RootDirectoryEntry
    | RootSpareEntry
    | DirectoryChildEntry

  type EntryKind = EntryMissing | EntryFile | EntryDirectory | EntryTombstone
  type EntryBinding = {
    kind: EntryKind,
    object: Option[NamespaceObject],
    stamp: int,
  }

  type NamespaceOperation =
    | NoNamespaceOperation
    | CreateFile
    | MakeDirectory
    | UnlinkFile
    | RemoveDirectory
    | RenameEntry

  type LocalBranchPhase =
    | NoBranch
    | BranchOpen
    | BranchDirty
    | BranchSealed
    | BranchPlanned
    | BranchTerminal

  type ReconcileOutcome =
    | Unclassified
    | Applicable
    | Converged
    | Conflict
    | Superseded

  /// The reserved second value keeps this a finite sum type while the bounded
  /// slice exercises one concrete branch identity.
  type LocalBranchId = LocalBranchOne | LocalBranchReserved
  type NamespaceDecision =
    | NoDecision
    | BranchStarted
    | LocalCreateStaged
    | LocalMkdirStaged
    | LocalUnlinkStaged
    | LocalRmdirStaged
    | LocalRenameStaged
    | BranchManifestSealed
    | StrongUnrelatedCommitted
    | StrongEquivalentCommitted
    | StrongConflictingCommitted
    | StrongSameObjectAbaCommitted
    | StrongDirectoryChildCommitted
    | ReconcilePlanPrepared
    | ReconcileGatewayProgressed
    | StalePlanRejected
    | ApplicableCommitted
    | ConvergedCommitted
    | ConflictCommitted
    | SupersededCommitted
    | IdempotentRetry

  pure val EntryKeys: Set[EntryKey] = Set(
    RootFileEntry,
    RootDirectoryEntry,
    RootSpareEntry,
    DirectoryChildEntry
  )
  pure val CreatableEntries: Set[EntryKey] =
    Set(RootSpareEntry, DirectoryChildEntry)
  pure val RenameSources: Set[EntryKey] =
    Set(RootFileEntry, RootDirectoryEntry)
  pure val MaxCanonicalHead: int = 6

  var canonicalNamespace: EntryKey -> EntryBinding
  var localNamespace: EntryKey -> EntryBinding
  var beforeCanonicalNamespace: EntryKey -> EntryBinding
  var beforeLocalNamespace: EntryKey -> EntryBinding
  var branchPhase: LocalBranchPhase
  var branchOperation: NamespaceOperation
  var sourceEntry: EntryKey
  var destinationEntry: EntryKey
  var baseSourceBinding: EntryBinding
  var baseDestinationBinding: EntryBinding
  var baseChildBinding: EntryBinding
  var canonicalHead: int
  var beforeCanonicalHead: int
  var planHead: int
  var planOutcome: ReconcileOutcome
  var branchResult: ReconcileOutcome
  var conflictBranches: Set[LocalBranchId]
  var preservedObjects: Set[NamespaceObject]
  var terminalCanonicalNamespace: EntryKey -> EntryBinding
  var terminalCanonicalHead: int
  var terminalConflictBranches: Set[LocalBranchId]
  var terminalPreservedObjects: Set[NamespaceObject]
  var lastDecision: NamespaceDecision

  pure def missingBinding(stamp: int): EntryBinding = {
    kind: EntryMissing,
    object: None,
    stamp: stamp,
  }

  pure def tombstoneBinding(stamp: int): EntryBinding = {
    kind: EntryTombstone,
    object: None,
    stamp: stamp,
  }

  pure def liveBinding(
    kind: EntryKind,
    object: NamespaceObject,
    stamp: int
  ): EntryBinding = {
    kind: kind,
    object: Some(object),
    stamp: stamp,
  }

  pure val initialCanonicalNamespace: EntryKey -> EntryBinding =
    EntryKeys.mapBy(entry =>
      if (entry == RootFileEntry) {
        liveBinding(EntryFile, FileObject, 1)
      } else if (entry == RootDirectoryEntry) {
        liveBinding(EntryDirectory, DirectoryObject, 1)
      } else {
        missingBinding(0)
      }
    )

  pure def parentObject(entry: EntryKey): NamespaceObject =
    match entry {
      | RootFileEntry => RootObject
      | RootDirectoryEntry => RootObject
      | RootSpareEntry => RootObject
      | DirectoryChildEntry => DirectoryObject
    }

  pure def entryName(entry: EntryKey): EntryName =
    match entry {
      | RootFileEntry => FileName
      | RootDirectoryEntry => DirectoryName
      | RootSpareEntry => SpareName
      | DirectoryChildEntry => ChildName
    }

  pure def isAbsent(binding: EntryBinding): bool =
    binding.kind == EntryMissing or binding.kind == EntryTombstone

  pure def isLive(binding: EntryBinding): bool = not(isAbsent(binding))

  pure def bindingShapeValid(binding: EntryBinding): bool =
    if (isAbsent(binding)) {
      binding.object == None
    } else {
      any {
        binding.kind == EntryFile,
        binding.kind == EntryDirectory,
      } and isSome(binding.object)
    }

  /// Payload equality deliberately ignores the authority-assigned entry stamp.
  /// It recognizes a lost-reply retry that already published the same object.
  pure def sameBindingPayload(left: EntryBinding, right: EntryBinding): bool =
    left.kind == right.kind and left.object == right.object

  pure def objectSet(binding: EntryBinding): Set[NamespaceObject] =
    match binding.object {
      | Some(object) => Set(object)
      | None => Set()
    }

  pure def parentDirectoryExists(
    namespace: EntryKey -> EntryBinding,
    entry: EntryKey
  ): bool =
    if (parentObject(entry) == RootObject) {
      true
    } else {
      EntryKeys.exists(candidate => all {
        namespace.get(candidate).kind == EntryDirectory,
        namespace.get(candidate).object == Some(parentObject(entry)),
      })
    }

  pure def hasLiveChild(
    namespace: EntryKey -> EntryBinding,
    directory: NamespaceObject
  ): bool =
    EntryKeys.exists(entry =>
      parentObject(entry) == directory and isLive(namespace.get(entry))
    )

  pure def namespaceWellFormed(
    namespace: EntryKey -> EntryBinding
  ): bool = all {
    EntryKeys.forall(entry => all {
      bindingShapeValid(namespace.get(entry)),
      namespace.get(entry).stamp >= 0,
      not(isLive(namespace.get(entry))) or parentDirectoryExists(namespace, entry),
    }),
    EntryKeys.forall(left =>
      EntryKeys.forall(right =>
        left == right or
          not(isLive(namespace.get(left))) or
          not(isLive(namespace.get(right))) or
          namespace.get(left).object != namespace.get(right).object
      )
    ),
  }

  pure def nextStamp(binding: EntryBinding): int = binding.stamp + 1

  def localDesiredBinding: EntryBinding =
    localNamespace.get(destinationEntry)

  def baseStillExactIn(
    namespace: EntryKey -> EntryBinding
  ): bool =
    match branchOperation {
      | CreateFile => namespace.get(destinationEntry) == baseDestinationBinding
        and if (parentObject(destinationEntry) == RootObject) {
          true
        } else {
          namespace.get(RootDirectoryEntry) == baseSourceBinding
        }
      | MakeDirectory => namespace.get(destinationEntry) == baseDestinationBinding
      | UnlinkFile => namespace.get(sourceEntry) == baseSourceBinding
      | RemoveDirectory => all {
          namespace.get(sourceEntry) == baseSourceBinding,
          namespace.get(DirectoryChildEntry) == baseChildBinding,
        }
      | RenameEntry => all {
          namespace.get(sourceEntry) == baseSourceBinding,
          namespace.get(destinationEntry) == baseDestinationBinding,
        }
      | NoNamespaceOperation => false
    }

  def namespaceAfterBranchOperation(
    namespace: EntryKey -> EntryBinding
  ): EntryKey -> EntryBinding =
    match branchOperation {
      | CreateFile => namespace.set(destinationEntry, liveBinding(
          EntryFile,
          unwrapOr(localDesiredBinding.object, LocalFileObject),
          nextStamp(namespace.get(destinationEntry))
        ))
      | MakeDirectory => namespace.set(destinationEntry, liveBinding(
          EntryDirectory,
          unwrapOr(localDesiredBinding.object, LocalDirectoryObject),
          nextStamp(namespace.get(destinationEntry))
        ))
      | UnlinkFile => namespace.set(
          sourceEntry,
          tombstoneBinding(nextStamp(namespace.get(sourceEntry)))
        )
      | RemoveDirectory => namespace.set(
          sourceEntry,
          tombstoneBinding(nextStamp(namespace.get(sourceEntry)))
        )
      | RenameEntry => namespace
          .set(sourceEntry, tombstoneBinding(nextStamp(namespace.get(sourceEntry))))
          .set(destinationEntry, liveBinding(
            baseSourceBinding.kind,
            unwrapOr(baseSourceBinding.object, FileObject),
            nextStamp(namespace.get(destinationEntry))
          ))
      | NoNamespaceOperation => namespace
    }

  def namespaceAfterStrongConflict(
    namespace: EntryKey -> EntryBinding
  ): EntryKey -> EntryBinding =
    match branchOperation {
      | CreateFile =>
          if (destinationEntry == DirectoryChildEntry) {
            namespace.set(RootDirectoryEntry, liveBinding(
              EntryDirectory,
              StrongDirectoryObject,
              nextStamp(namespace.get(RootDirectoryEntry))
            ))
          } else {
            namespace.set(destinationEntry, liveBinding(
              EntryFile,
              StrongFileObject,
              nextStamp(namespace.get(destinationEntry))
            ))
          }
      | MakeDirectory => namespace.set(destinationEntry, liveBinding(
          EntryDirectory,
          StrongDirectoryObject,
          nextStamp(namespace.get(destinationEntry))
        ))
      | UnlinkFile => namespace.set(sourceEntry, liveBinding(
          EntryFile,
          StrongFileObject,
          nextStamp(namespace.get(sourceEntry))
        ))
      | RemoveDirectory => namespace.set(DirectoryChildEntry, liveBinding(
          EntryFile,
          StrongFileObject,
          nextStamp(namespace.get(DirectoryChildEntry))
        ))
      | RenameEntry => namespace.set(destinationEntry, liveBinding(
          EntryFile,
          StrongSpareObject,
          nextStamp(namespace.get(destinationEntry))
        ))
      | NoNamespaceOperation => namespace
    }

  def classifyReconciliation: ReconcileOutcome =
    match branchOperation {
      | CreateFile =>
          if (baseStillExactIn(canonicalNamespace)) {
            Applicable
          } else if (sameBindingPayload(
            canonicalNamespace.get(destinationEntry),
            localDesiredBinding
          )) {
            Converged
          } else {
            Conflict
          }
      | MakeDirectory =>
          if (canonicalNamespace.get(destinationEntry) == baseDestinationBinding) {
            Applicable
          } else if (sameBindingPayload(
            canonicalNamespace.get(destinationEntry),
            localDesiredBinding
          )) {
            Converged
          } else {
            Conflict
          }
      | UnlinkFile =>
          if (canonicalNamespace.get(sourceEntry) == baseSourceBinding) {
            Applicable
          } else if (isAbsent(canonicalNamespace.get(sourceEntry))) {
            Converged
          } else {
            Superseded
          }
      | RemoveDirectory =>
          if (all {
            canonicalNamespace.get(sourceEntry) == baseSourceBinding,
            canonicalNamespace.get(DirectoryChildEntry) == baseChildBinding,
          }) {
            Applicable
          } else if (isAbsent(canonicalNamespace.get(sourceEntry))) {
            Converged
          } else if (
            canonicalNamespace.get(sourceEntry).object != baseSourceBinding.object
          ) {
            Superseded
          } else if (hasLiveChild(canonicalNamespace, DirectoryObject)) {
            Conflict
          } else {
            Superseded
          }
      | RenameEntry =>
          if (all {
            isAbsent(canonicalNamespace.get(sourceEntry)),
            sameBindingPayload(
              canonicalNamespace.get(destinationEntry),
              localDesiredBinding
            ),
          }) {
            Converged
          } else if (all {
            canonicalNamespace.get(sourceEntry) == baseSourceBinding,
            canonicalNamespace.get(destinationEntry) == baseDestinationBinding,
          }) {
            Applicable
          } else {
            Conflict
          }
      | NoNamespaceOperation => Unclassified
    }

  def operationObjectsToPreserve: Set[NamespaceObject] =
    match branchOperation {
      | CreateFile => objectSet(localNamespace.get(destinationEntry))
      | MakeDirectory => objectSet(localNamespace.get(destinationEntry))
      | UnlinkFile => objectSet(baseSourceBinding)
      | RemoveDirectory => objectSet(baseSourceBinding)
      | RenameEntry => objectSet(localNamespace.get(destinationEntry))
      | NoNamespaceOperation => Set()
    }

  pure def decisionForOutcome(outcome: ReconcileOutcome): NamespaceDecision =
    match outcome {
      | Applicable => ApplicableCommitted
      | Converged => ConvergedCommitted
      | Conflict => ConflictCommitted
      | Superseded => SupersededCommitted
      | Unclassified => NoDecision
    }

  def nonTerminalBranch: bool = any {
    branchPhase == BranchDirty,
    branchPhase == BranchSealed,
    branchPhase == BranchPlanned,
  }

  action init = all {
    canonicalNamespace' = initialCanonicalNamespace,
    localNamespace' = initialCanonicalNamespace,
    beforeCanonicalNamespace' = initialCanonicalNamespace,
    beforeLocalNamespace' = initialCanonicalNamespace,
    branchPhase' = NoBranch,
    branchOperation' = NoNamespaceOperation,
    sourceEntry' = RootFileEntry,
    destinationEntry' = RootSpareEntry,
    baseSourceBinding' = initialCanonicalNamespace.get(RootFileEntry),
    baseDestinationBinding' = initialCanonicalNamespace.get(RootSpareEntry),
    baseChildBinding' = initialCanonicalNamespace.get(DirectoryChildEntry),
    canonicalHead' = 0,
    beforeCanonicalHead' = 0,
    planHead' = 0,
    planOutcome' = Unclassified,
    branchResult' = Unclassified,
    conflictBranches' = Set(),
    preservedObjects' = Set(),
    terminalCanonicalNamespace' = initialCanonicalNamespace,
    terminalCanonicalHead' = 0,
    terminalConflictBranches' = Set(),
    terminalPreservedObjects' = Set(),
    lastDecision' = NoDecision,
  }

  action beginBranch: bool = all {
    branchPhase == NoBranch,
    canonicalNamespace' = canonicalNamespace,
    localNamespace' = canonicalNamespace,
    beforeCanonicalNamespace' = canonicalNamespace,
    beforeLocalNamespace' = localNamespace,
    branchPhase' = BranchOpen,
    branchOperation' = NoNamespaceOperation,
    sourceEntry' = sourceEntry,
    destinationEntry' = destinationEntry,
    baseSourceBinding' = baseSourceBinding,
    baseDestinationBinding' = baseDestinationBinding,
    baseChildBinding' = baseChildBinding,
    canonicalHead' = canonicalHead,
    beforeCanonicalHead' = canonicalHead,
    planHead' = canonicalHead,
    planOutcome' = Unclassified,
    branchResult' = Unclassified,
    conflictBranches' = conflictBranches,
    preservedObjects' = preservedObjects,
    terminalCanonicalNamespace' = terminalCanonicalNamespace,
    terminalCanonicalHead' = terminalCanonicalHead,
    terminalConflictBranches' = terminalConflictBranches,
    terminalPreservedObjects' = terminalPreservedObjects,
    lastDecision' = BranchStarted,
  }

  action stageCreate(entry: EntryKey): bool = {
    val desired = liveBinding(
      EntryFile,
      LocalFileObject,
      nextStamp(localNamespace.get(entry))
    )
    all {
      branchPhase == BranchOpen,
      CreatableEntries.contains(entry),
      isAbsent(localNamespace.get(entry)),
      parentDirectoryExists(localNamespace, entry),
      canonicalNamespace' = canonicalNamespace,
      localNamespace' = localNamespace.set(entry, desired),
      beforeCanonicalNamespace' = canonicalNamespace,
      beforeLocalNamespace' = localNamespace,
      branchPhase' = BranchDirty,
      branchOperation' = CreateFile,
      sourceEntry' = entry,
      destinationEntry' = entry,
      baseSourceBinding' =
        if (parentObject(entry) == RootObject) {
          localNamespace.get(entry)
        } else {
          localNamespace.get(RootDirectoryEntry)
        },
      baseDestinationBinding' = localNamespace.get(entry),
      baseChildBinding' = localNamespace.get(DirectoryChildEntry),
      canonicalHead' = canonicalHead,
      beforeCanonicalHead' = canonicalHead,
      planHead' = canonicalHead,
      planOutcome' = Unclassified,
      branchResult' = Unclassified,
      conflictBranches' = conflictBranches,
      preservedObjects' = preservedObjects,
      terminalCanonicalNamespace' = terminalCanonicalNamespace,
      terminalCanonicalHead' = terminalCanonicalHead,
      terminalConflictBranches' = terminalConflictBranches,
      terminalPreservedObjects' = terminalPreservedObjects,
      lastDecision' = LocalCreateStaged,
    }
  }

  action stageMkdir(entry: EntryKey): bool = {
    val desired = liveBinding(
      EntryDirectory,
      LocalDirectoryObject,
      nextStamp(localNamespace.get(entry))
    )
    all {
      branchPhase == BranchOpen,
      entry == RootSpareEntry,
      isAbsent(localNamespace.get(entry)),
      parentDirectoryExists(localNamespace, entry),
      canonicalNamespace' = canonicalNamespace,
      localNamespace' = localNamespace.set(entry, desired),
      beforeCanonicalNamespace' = canonicalNamespace,
      beforeLocalNamespace' = localNamespace,
      branchPhase' = BranchDirty,
      branchOperation' = MakeDirectory,
      sourceEntry' = entry,
      destinationEntry' = entry,
      baseSourceBinding' = localNamespace.get(entry),
      baseDestinationBinding' = localNamespace.get(entry),
      baseChildBinding' = localNamespace.get(DirectoryChildEntry),
      canonicalHead' = canonicalHead,
      beforeCanonicalHead' = canonicalHead,
      planHead' = canonicalHead,
      planOutcome' = Unclassified,
      branchResult' = Unclassified,
      conflictBranches' = conflictBranches,
      preservedObjects' = preservedObjects,
      terminalCanonicalNamespace' = terminalCanonicalNamespace,
      terminalCanonicalHead' = terminalCanonicalHead,
      terminalConflictBranches' = terminalConflictBranches,
      terminalPreservedObjects' = terminalPreservedObjects,
      lastDecision' = LocalMkdirStaged,
    }
  }

  action stageUnlink(entry: EntryKey): bool = all {
    branchPhase == BranchOpen,
    entry == RootFileEntry,
    localNamespace.get(entry).kind == EntryFile,
    canonicalNamespace' = canonicalNamespace,
    localNamespace' = localNamespace.set(
      entry,
      tombstoneBinding(nextStamp(localNamespace.get(entry)))
    ),
    beforeCanonicalNamespace' = canonicalNamespace,
    beforeLocalNamespace' = localNamespace,
    branchPhase' = BranchDirty,
    branchOperation' = UnlinkFile,
    sourceEntry' = entry,
    destinationEntry' = entry,
    baseSourceBinding' = localNamespace.get(entry),
    baseDestinationBinding' = localNamespace.get(entry),
    baseChildBinding' = localNamespace.get(DirectoryChildEntry),
    canonicalHead' = canonicalHead,
    beforeCanonicalHead' = canonicalHead,
    planHead' = canonicalHead,
    planOutcome' = Unclassified,
    branchResult' = Unclassified,
    conflictBranches' = conflictBranches,
    preservedObjects' = preservedObjects,
    terminalCanonicalNamespace' = terminalCanonicalNamespace,
    terminalCanonicalHead' = terminalCanonicalHead,
    terminalConflictBranches' = terminalConflictBranches,
    terminalPreservedObjects' = terminalPreservedObjects,
    lastDecision' = LocalUnlinkStaged,
  }

  action stageRmdir(entry: EntryKey): bool = all {
    branchPhase == BranchOpen,
    entry == RootDirectoryEntry,
    localNamespace.get(entry).kind == EntryDirectory,
    localNamespace.get(entry).object == Some(DirectoryObject),
    not(hasLiveChild(localNamespace, DirectoryObject)),
    canonicalNamespace' = canonicalNamespace,
    localNamespace' = localNamespace.set(
      entry,
      tombstoneBinding(nextStamp(localNamespace.get(entry)))
    ),
    beforeCanonicalNamespace' = canonicalNamespace,
    beforeLocalNamespace' = localNamespace,
    branchPhase' = BranchDirty,
    branchOperation' = RemoveDirectory,
    sourceEntry' = entry,
    destinationEntry' = entry,
    baseSourceBinding' = localNamespace.get(entry),
    baseDestinationBinding' = localNamespace.get(entry),
    baseChildBinding' = localNamespace.get(DirectoryChildEntry),
    canonicalHead' = canonicalHead,
    beforeCanonicalHead' = canonicalHead,
    planHead' = canonicalHead,
    planOutcome' = Unclassified,
    branchResult' = Unclassified,
    conflictBranches' = conflictBranches,
    preservedObjects' = preservedObjects,
    terminalCanonicalNamespace' = terminalCanonicalNamespace,
    terminalCanonicalHead' = terminalCanonicalHead,
    terminalConflictBranches' = terminalConflictBranches,
    terminalPreservedObjects' = terminalPreservedObjects,
    lastDecision' = LocalRmdirStaged,
  }

  action stageRename(source: EntryKey, destination: EntryKey): bool = all {
    branchPhase == BranchOpen,
    RenameSources.contains(source),
    destination == RootSpareEntry,
    source != destination,
    isLive(localNamespace.get(source)),
    isAbsent(localNamespace.get(destination)),
    parentDirectoryExists(localNamespace, destination),
    canonicalNamespace' = canonicalNamespace,
    localNamespace' = localNamespace
      .set(source, tombstoneBinding(nextStamp(localNamespace.get(source))))
      .set(destination, liveBinding(
        localNamespace.get(source).kind,
        unwrapOr(localNamespace.get(source).object, FileObject),
        nextStamp(localNamespace.get(destination))
      )),
    beforeCanonicalNamespace' = canonicalNamespace,
    beforeLocalNamespace' = localNamespace,
    branchPhase' = BranchDirty,
    branchOperation' = RenameEntry,
    sourceEntry' = source,
    destinationEntry' = destination,
    baseSourceBinding' = localNamespace.get(source),
    baseDestinationBinding' = localNamespace.get(destination),
    baseChildBinding' = localNamespace.get(DirectoryChildEntry),
    canonicalHead' = canonicalHead,
    beforeCanonicalHead' = canonicalHead,
    planHead' = canonicalHead,
    planOutcome' = Unclassified,
    branchResult' = Unclassified,
    conflictBranches' = conflictBranches,
    preservedObjects' = preservedObjects,
    terminalCanonicalNamespace' = terminalCanonicalNamespace,
    terminalCanonicalHead' = terminalCanonicalHead,
    terminalConflictBranches' = terminalConflictBranches,
    terminalPreservedObjects' = terminalPreservedObjects,
    lastDecision' = LocalRenameStaged,
  }

  action sealBranch: bool = all {
    branchPhase == BranchDirty,
    canonicalNamespace' = canonicalNamespace,
    localNamespace' = localNamespace,
    beforeCanonicalNamespace' = canonicalNamespace,
    beforeLocalNamespace' = localNamespace,
    branchPhase' = BranchSealed,
    branchOperation' = branchOperation,
    sourceEntry' = sourceEntry,
    destinationEntry' = destinationEntry,
    baseSourceBinding' = baseSourceBinding,
    baseDestinationBinding' = baseDestinationBinding,
    baseChildBinding' = baseChildBinding,
    canonicalHead' = canonicalHead,
    beforeCanonicalHead' = canonicalHead,
    planHead' = planHead,
    planOutcome' = Unclassified,
    branchResult' = branchResult,
    conflictBranches' = conflictBranches,
    preservedObjects' = preservedObjects,
    terminalCanonicalNamespace' = terminalCanonicalNamespace,
    terminalCanonicalHead' = terminalCanonicalHead,
    terminalConflictBranches' = terminalConflictBranches,
    terminalPreservedObjects' = terminalPreservedObjects,
    lastDecision' = BranchManifestSealed,
  }

  /// An unrelated canonical transaction invalidates a prepared plan even when
  /// none of the bounded entries in this slice changed.
  action commitStrongUnrelated: bool = all {
    nonTerminalBranch,
    canonicalHead < MaxCanonicalHead,
    canonicalNamespace' = canonicalNamespace,
    localNamespace' = localNamespace,
    beforeCanonicalNamespace' = canonicalNamespace,
    beforeLocalNamespace' = localNamespace,
    branchPhase' = branchPhase,
    branchOperation' = branchOperation,
    sourceEntry' = sourceEntry,
    destinationEntry' = destinationEntry,
    baseSourceBinding' = baseSourceBinding,
    baseDestinationBinding' = baseDestinationBinding,
    baseChildBinding' = baseChildBinding,
    canonicalHead' = canonicalHead + 1,
    beforeCanonicalHead' = canonicalHead,
    planHead' = planHead,
    planOutcome' = planOutcome,
    branchResult' = branchResult,
    conflictBranches' = conflictBranches,
    preservedObjects' = preservedObjects,
    terminalCanonicalNamespace' = terminalCanonicalNamespace,
    terminalCanonicalHead' = terminalCanonicalHead,
    terminalConflictBranches' = terminalConflictBranches,
    terminalPreservedObjects' = terminalPreservedObjects,
    lastDecision' = StrongUnrelatedCommitted,
  }

  /// Abstracts a lost reply: the same operation is already canonical. Stable
  /// object IDs let reconciliation classify the retry as converged.
  action commitStrongEquivalent: bool = all {
    nonTerminalBranch,
    baseStillExactIn(canonicalNamespace),
    canonicalHead < MaxCanonicalHead,
    canonicalNamespace' = namespaceAfterBranchOperation(canonicalNamespace),
    localNamespace' = localNamespace,
    beforeCanonicalNamespace' = canonicalNamespace,
    beforeLocalNamespace' = localNamespace,
    branchPhase' = branchPhase,
    branchOperation' = branchOperation,
    sourceEntry' = sourceEntry,
    destinationEntry' = destinationEntry,
    baseSourceBinding' = baseSourceBinding,
    baseDestinationBinding' = baseDestinationBinding,
    baseChildBinding' = baseChildBinding,
    canonicalHead' = canonicalHead + 1,
    beforeCanonicalHead' = canonicalHead,
    planHead' = planHead,
    planOutcome' = planOutcome,
    branchResult' = branchResult,
    conflictBranches' = conflictBranches,
    preservedObjects' = preservedObjects,
    terminalCanonicalNamespace' = terminalCanonicalNamespace,
    terminalCanonicalHead' = terminalCanonicalHead,
    terminalConflictBranches' = terminalConflictBranches,
    terminalPreservedObjects' = terminalPreservedObjects,
    lastDecision' = StrongEquivalentCommitted,
  }

  /// Produces the operation-specific concurrent change: name occupation for
  /// create/mkdir/rename, replacement for unlink, or a new child for rmdir.
  action commitStrongConflict: bool = all {
    nonTerminalBranch,
    baseStillExactIn(canonicalNamespace),
    canonicalHead < MaxCanonicalHead,
    canonicalNamespace' = namespaceAfterStrongConflict(canonicalNamespace),
    localNamespace' = localNamespace,
    beforeCanonicalNamespace' = canonicalNamespace,
    beforeLocalNamespace' = localNamespace,
    branchPhase' = branchPhase,
    branchOperation' = branchOperation,
    sourceEntry' = sourceEntry,
    destinationEntry' = destinationEntry,
    baseSourceBinding' = baseSourceBinding,
    baseDestinationBinding' = baseDestinationBinding,
    baseChildBinding' = baseChildBinding,
    canonicalHead' = canonicalHead + 1,
    beforeCanonicalHead' = canonicalHead,
    planHead' = planHead,
    planOutcome' = planOutcome,
    branchResult' = branchResult,
    conflictBranches' = conflictBranches,
    preservedObjects' = preservedObjects,
    terminalCanonicalNamespace' = terminalCanonicalNamespace,
    terminalCanonicalHead' = terminalCanonicalHead,
    terminalConflictBranches' = terminalConflictBranches,
    terminalPreservedObjects' = terminalPreservedObjects,
    lastDecision' = StrongConflictingCommitted,
  }

  /// Delete/recreate of the same object still changes the exact entry stamp.
  /// This reaches the ABA case that object-identity-only validation would miss.
  action commitStrongSameObjectAba: bool = all {
    nonTerminalBranch,
    branchOperation == UnlinkFile,
    baseStillExactIn(canonicalNamespace),
    canonicalHead < MaxCanonicalHead,
    canonicalNamespace' = canonicalNamespace.set(
      sourceEntry,
      liveBinding(
        baseSourceBinding.kind,
        unwrapOr(baseSourceBinding.object, FileObject),
        nextStamp(canonicalNamespace.get(sourceEntry))
      )
    ),
    localNamespace' = localNamespace,
    beforeCanonicalNamespace' = canonicalNamespace,
    beforeLocalNamespace' = localNamespace,
    branchPhase' = branchPhase,
    branchOperation' = branchOperation,
    sourceEntry' = sourceEntry,
    destinationEntry' = destinationEntry,
    baseSourceBinding' = baseSourceBinding,
    baseDestinationBinding' = baseDestinationBinding,
    baseChildBinding' = baseChildBinding,
    canonicalHead' = canonicalHead + 1,
    beforeCanonicalHead' = canonicalHead,
    planHead' = planHead,
    planOutcome' = planOutcome,
    branchResult' = branchResult,
    conflictBranches' = conflictBranches,
    preservedObjects' = preservedObjects,
    terminalCanonicalNamespace' = terminalCanonicalNamespace,
    terminalCanonicalHead' = terminalCanonicalHead,
    terminalConflictBranches' = terminalConflictBranches,
    terminalPreservedObjects' = terminalPreservedObjects,
    lastDecision' = StrongSameObjectAbaCommitted,
  }

  /// A canonical writer may add a child while a disconnected branch has
  /// renamed the directory locally. The child is keyed by DirectoryObject, so
  /// later reconciliation may move the directory binding without rewriting or
  /// orphaning the child.
  action commitStrongDirectoryChild: bool = all {
    nonTerminalBranch,
    branchOperation == RenameEntry,
    baseSourceBinding.kind == EntryDirectory,
    baseStillExactIn(canonicalNamespace),
    isAbsent(canonicalNamespace.get(DirectoryChildEntry)),
    canonicalHead < MaxCanonicalHead,
    canonicalNamespace' = canonicalNamespace.set(
      DirectoryChildEntry,
      liveBinding(
        EntryFile,
        StrongFileObject,
        nextStamp(canonicalNamespace.get(DirectoryChildEntry))
      )
    ),
    localNamespace' = localNamespace,
    beforeCanonicalNamespace' = canonicalNamespace,
    beforeLocalNamespace' = localNamespace,
    branchPhase' = branchPhase,
    branchOperation' = branchOperation,
    sourceEntry' = sourceEntry,
    destinationEntry' = destinationEntry,
    baseSourceBinding' = baseSourceBinding,
    baseDestinationBinding' = baseDestinationBinding,
    baseChildBinding' = baseChildBinding,
    canonicalHead' = canonicalHead + 1,
    beforeCanonicalHead' = canonicalHead,
    planHead' = planHead,
    planOutcome' = planOutcome,
    branchResult' = branchResult,
    conflictBranches' = conflictBranches,
    preservedObjects' = preservedObjects,
    terminalCanonicalNamespace' = terminalCanonicalNamespace,
    terminalCanonicalHead' = terminalCanonicalHead,
    terminalConflictBranches' = terminalConflictBranches,
    terminalPreservedObjects' = terminalPreservedObjects,
    lastDecision' = StrongDirectoryChildCommitted,
  }

  action prepareReconcilePlan: bool = all {
    branchPhase == BranchSealed,
    canonicalNamespace' = canonicalNamespace,
    localNamespace' = localNamespace,
    beforeCanonicalNamespace' = canonicalNamespace,
    beforeLocalNamespace' = localNamespace,
    branchPhase' = BranchPlanned,
    branchOperation' = branchOperation,
    sourceEntry' = sourceEntry,
    destinationEntry' = destinationEntry,
    baseSourceBinding' = baseSourceBinding,
    baseDestinationBinding' = baseDestinationBinding,
    baseChildBinding' = baseChildBinding,
    canonicalHead' = canonicalHead,
    beforeCanonicalHead' = canonicalHead,
    planHead' = canonicalHead,
    planOutcome' = classifyReconciliation,
    branchResult' = Unclassified,
    conflictBranches' = conflictBranches,
    preservedObjects' = preservedObjects,
    terminalCanonicalNamespace' = terminalCanonicalNamespace,
    terminalCanonicalHead' = terminalCanonicalHead,
    terminalConflictBranches' = terminalConflictBranches,
    terminalPreservedObjects' = terminalPreservedObjects,
    lastDecision' = ReconcilePlanPrepared,
  }

  action rejectStalePlan: bool = all {
    branchPhase == BranchPlanned,
    canonicalHead != planHead,
    canonicalNamespace' = canonicalNamespace,
    localNamespace' = localNamespace,
    beforeCanonicalNamespace' = canonicalNamespace,
    beforeLocalNamespace' = localNamespace,
    branchPhase' = BranchSealed,
    branchOperation' = branchOperation,
    sourceEntry' = sourceEntry,
    destinationEntry' = destinationEntry,
    baseSourceBinding' = baseSourceBinding,
    baseDestinationBinding' = baseDestinationBinding,
    baseChildBinding' = baseChildBinding,
    canonicalHead' = canonicalHead,
    beforeCanonicalHead' = canonicalHead,
    planHead' = canonicalHead,
    planOutcome' = Unclassified,
    branchResult' = Unclassified,
    conflictBranches' = conflictBranches,
    preservedObjects' = preservedObjects,
    terminalCanonicalNamespace' = terminalCanonicalNamespace,
    terminalCanonicalHead' = terminalCanonicalHead,
    terminalConflictBranches' = terminalConflictBranches,
    terminalPreservedObjects' = terminalPreservedObjects,
    lastDecision' = StalePlanRejected,
  }

  /// The composed typed gateway may collect evidence, append, crash, restart,
  /// or materialize while the namespace branch waits. Neither a planned nor a
  /// terminal branch may gain another namespace effect from those steps.
  action awaitReconcileGateway: bool = all {
    any {
      branchPhase == BranchPlanned,
      branchPhase == BranchTerminal,
    },
    canonicalNamespace' = canonicalNamespace,
    localNamespace' = localNamespace,
    beforeCanonicalNamespace' = canonicalNamespace,
    beforeLocalNamespace' = localNamespace,
    branchPhase' = branchPhase,
    branchOperation' = branchOperation,
    sourceEntry' = sourceEntry,
    destinationEntry' = destinationEntry,
    baseSourceBinding' = baseSourceBinding,
    baseDestinationBinding' = baseDestinationBinding,
    baseChildBinding' = baseChildBinding,
    canonicalHead' = canonicalHead,
    beforeCanonicalHead' = canonicalHead,
    planHead' = planHead,
    planOutcome' = planOutcome,
    branchResult' = branchResult,
    conflictBranches' = conflictBranches,
    preservedObjects' = preservedObjects,
    terminalCanonicalNamespace' = terminalCanonicalNamespace,
    terminalCanonicalHead' = terminalCanonicalHead,
    terminalConflictBranches' = terminalConflictBranches,
    terminalPreservedObjects' = terminalPreservedObjects,
    lastDecision' = ReconcileGatewayProgressed,
  }

  action commitReconcilePlan: bool = {
    val committedNamespace =
      if (planOutcome == Applicable) {
        namespaceAfterBranchOperation(canonicalNamespace)
      } else {
        canonicalNamespace
      }
    val committedConflicts =
      if (planOutcome == Conflict) {
        conflictBranches.union(Set(LocalBranchOne))
      } else {
        conflictBranches
      }
    val committedPreservedObjects =
      if (planOutcome == Conflict) {
        preservedObjects.union(operationObjectsToPreserve)
      } else {
        preservedObjects
      }
    all {
      branchPhase == BranchPlanned,
      canonicalHead == planHead,
      planOutcome != Unclassified,
      canonicalHead < MaxCanonicalHead,
      canonicalNamespace' = committedNamespace,
      localNamespace' = localNamespace,
      beforeCanonicalNamespace' = canonicalNamespace,
      beforeLocalNamespace' = localNamespace,
      branchPhase' = BranchTerminal,
      branchOperation' = branchOperation,
      sourceEntry' = sourceEntry,
      destinationEntry' = destinationEntry,
      baseSourceBinding' = baseSourceBinding,
      baseDestinationBinding' = baseDestinationBinding,
      baseChildBinding' = baseChildBinding,
      canonicalHead' = canonicalHead + 1,
      beforeCanonicalHead' = canonicalHead,
      planHead' = planHead,
      planOutcome' = planOutcome,
      branchResult' = planOutcome,
      conflictBranches' = committedConflicts,
      preservedObjects' = committedPreservedObjects,
      terminalCanonicalNamespace' = committedNamespace,
      terminalCanonicalHead' = canonicalHead + 1,
      terminalConflictBranches' = committedConflicts,
      terminalPreservedObjects' = committedPreservedObjects,
      lastDecision' = decisionForOutcome(planOutcome),
    }
  }

  /// The deterministic branch ID makes a lost reconciliation reply a set
  /// lookup. Retrying cannot advance the head, reapply rename, or duplicate a
  /// conflict sidecar.
  action retryTerminalResult: bool = all {
    branchPhase == BranchTerminal,
    canonicalNamespace' = canonicalNamespace,
    localNamespace' = localNamespace,
    beforeCanonicalNamespace' = canonicalNamespace,
    beforeLocalNamespace' = localNamespace,
    branchPhase' = branchPhase,
    branchOperation' = branchOperation,
    sourceEntry' = sourceEntry,
    destinationEntry' = destinationEntry,
    baseSourceBinding' = baseSourceBinding,
    baseDestinationBinding' = baseDestinationBinding,
    baseChildBinding' = baseChildBinding,
    canonicalHead' = canonicalHead,
    beforeCanonicalHead' = canonicalHead,
    planHead' = planHead,
    planOutcome' = planOutcome,
    branchResult' = branchResult,
    conflictBranches' = conflictBranches,
    preservedObjects' = preservedObjects,
    terminalCanonicalNamespace' = terminalCanonicalNamespace,
    terminalCanonicalHead' = terminalCanonicalHead,
    terminalConflictBranches' = terminalConflictBranches,
    terminalPreservedObjects' = terminalPreservedObjects,
    lastDecision' = IdempotentRetry,
  }

  action step = any {
    beginBranch,
    {
      nondet entry = oneOf(CreatableEntries)
      stageCreate(entry)
    },
    stageMkdir(RootSpareEntry),
    stageUnlink(RootFileEntry),
    stageRmdir(RootDirectoryEntry),
    {
      nondet source = oneOf(RenameSources)
      stageRename(source, RootSpareEntry)
    },
    sealBranch,
    commitStrongUnrelated,
    commitStrongEquivalent,
    commitStrongConflict,
    commitStrongSameObjectAba,
    commitStrongDirectoryChild,
    prepareReconcilePlan,
    rejectStalePlan,
    commitReconcilePlan,
    retryTerminalResult,
  }
}
