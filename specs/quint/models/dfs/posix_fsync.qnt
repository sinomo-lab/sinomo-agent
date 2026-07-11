/// DFS POSIX file and directory synchronization semantics.
///
/// This target-hardening model separates four facts that a filesystem client
/// must not conflate:
/// - a write syscall returned to the caller;
/// - the current authority exposes that write;
/// - enough content replicas durably stored the write;
/// - durable metadata authority certified the write as canonical.
///
/// `fsync` and `fdatasync` snapshot the returned-write prefix at call time.
/// A successful response certifies that whole prefix even when a later write
/// returned while the sync was in flight. File sync never claims that the
/// parent directory entry is durable; directory fsync is a separate request.
///
/// Sync ids are durable idempotency keys. A committed sync can lose its reply
/// and is rediscovered by retry. Reusing an id with another payload is rejected.
/// An authority fence terminates an uncommitted sync, rolls volatile visibility
/// back to the certified prefix, and never rolls a committed prefix back.
module dfs_posix_fsync {
  import common.* from "../../lib/common"

  type FileVersion = BaseVersion | FirstWrite | SecondWrite
  type FileContent = BaseContent | FirstContent | SecondContent
  type DirectoryRevision =
    | DirectoryBase
    | EntryCreated
    | EntryRenamed
    | EntryUnlinked
  type AuthorityEpoch = Epoch1 | Epoch2
  type SyncId = Sync1 | Sync2
  type SyncKind = FileFsync | FileDataSync | DirectoryFsync
  type SyncStatus = SyncInFlight | SyncCommitted | SyncFenced

  type SyncPayload = {
    kind: SyncKind,
    // Highest write that had returned when file sync began.
    targetVersion: FileVersion,
    // Ancillary metadata target. `BaseVersion` means the operation makes no
    // ancillary-metadata durability claim.
    targetFileMetadata: FileVersion,
    // Namespace revision captured by a directory sync. `DirectoryBase` means
    // that a file sync makes no parent-directory durability claim.
    targetDirectoryRevision: DirectoryRevision,
  }

  type SyncRecord = {
    payload: SyncPayload,
    status: SyncStatus,
    epoch: AuthorityEpoch,
    // Durably recorded metadata-prepare voters. This evidence is abortable
    // while the record is in flight and joins the commit certificate only at
    // the atomic canonical-publication transition.
    prepareVoters: Set[Node],
  }

  type SyncResponse =
    | NoSyncResponse
    | SyncPending(SyncId)
    | SyncSuccess(SyncId)
    | SyncOutcomeUnknown(SyncId)
    | SyncNotCommitted(SyncId)
    | SyncPayloadMismatch(SyncId)

  type FsyncDecision =
    | NoFsyncDecision
    | WriteReturned
    | DirectoryMutationReturned
    | DataReceiptRecorded
    | SyncStarted
    | MetadataPrepareQuorumFormed
    | SyncCommittedWithReply
    | SyncCommittedReplyLost
    | SyncTimedOut
    | SyncRetryPending
    | SyncRetryDeduplicated
    | SyncRetryFenced
    | SyncRetryPayloadRejected
    | AuthorityFenced
    | CanonicalReplicaRepaired
    | TerminalStutter

  pure val FileVersions: Set[FileVersion] =
    Set(BaseVersion, FirstWrite, SecondWrite)
  pure val DirectoryRevisions: Set[DirectoryRevision] =
    Set(DirectoryBase, EntryCreated, EntryRenamed, EntryUnlinked)
  pure val SyncIds: Set[SyncId] = Set(Sync1, Sync2)
  pure val SyncKinds: Set[SyncKind] =
    Set(FileFsync, FileDataSync, DirectoryFsync)

  /// A successful file sync needs two durable content receipts in the bounded
  /// three-replica model. Metadata certification is modeled separately below.
  pure val dataReceiptThreshold: int = 2

  pure val emptyPayload: SyncPayload = {
    kind: FileFsync,
    targetVersion: BaseVersion,
    targetFileMetadata: BaseVersion,
    targetDirectoryRevision: DirectoryBase,
  }

  pure val emptyRecord: SyncRecord = {
    payload: emptyPayload,
    status: SyncFenced,
    epoch: Epoch1,
    prepareVoters: Set(),
  }

  var authorityEpoch: AuthorityEpoch
  var primary: Node

  /// Current-authority write/visibility frontier. It may roll back to the
  /// canonical prefix when the authority is fenced.
  var returnedVersion: FileVersion
  var visibleVersion: FileVersion

  /// Receipt durability and certified canonical visibility are distinct.
  var durableVersion: FileVersion
  var canonicalVersion: FileVersion
  var metadataCertifiedVersion: FileVersion
  var dataReceipts: Node -> FileVersion

  /// Size/allocation metadata is required by both fdatasync and fsync.
  var essentialMetadataVisibleVersion: FileVersion
  var essentialMetadataDurableVersion: FileVersion

  /// Ancillary metadata is required by fsync but deliberately not promised by
  /// fdatasync. It can therefore trail the canonical content prefix.
  var fullMetadataVisibleVersion: FileVersion
  var fullMetadataDurableVersion: FileVersion

  /// Namespace mutations have an independent visibility/durability frontier.
  /// File sync never advances it; directory sync captures and certifies it.
  var directoryVisibleRevision: DirectoryRevision
  var directoryDurableRevision: DirectoryRevision
  var directoryCertifiedRevision: DirectoryRevision

  var syncRecords: SyncId -> Option[SyncRecord]
  var pendingSync: Option[SyncId]
  var lastResponse: SyncResponse
  var lastSuppliedPayload: Option[SyncPayload]
  var lastDecision: FsyncDecision
  var lastSyncId: Option[SyncId]
  var lastReplica: Option[Node]

  pure val emptySyncRecords: SyncId -> Option[SyncRecord] =
    SyncIds.mapBy(_ => None)

  pure val baseReceipts: Node -> FileVersion =
    Nodes.mapBy(_ => BaseVersion)

  pure def versionRank(version: FileVersion): int =
    match version {
      | BaseVersion => 0
      | FirstWrite => 1
      | SecondWrite => 2
    }

  pure def versionAtLeast(left: FileVersion, right: FileVersion): bool =
    versionRank(left) >= versionRank(right)

  pure def laterVersion(left: FileVersion, right: FileVersion): FileVersion =
    if (versionAtLeast(left, right)) { left } else { right }

  pure def contentAt(version: FileVersion): FileContent =
    match version {
      | BaseVersion => BaseContent
      | FirstWrite => FirstContent
      | SecondWrite => SecondContent
    }

  pure def nextWriteAfter(version: FileVersion): FileVersion =
    match version {
      | BaseVersion => FirstWrite
      | FirstWrite => SecondWrite
      | SecondWrite => SecondWrite
    }

  pure def directoryRevisionRank(revision: DirectoryRevision): int =
    match revision {
      | DirectoryBase => 0
      | EntryCreated => 1
      | EntryRenamed => 2
      | EntryUnlinked => 3
    }

  pure def directoryRevisionAtLeast(
    left: DirectoryRevision,
    right: DirectoryRevision
  ): bool =
    directoryRevisionRank(left) >= directoryRevisionRank(right)

  pure def laterDirectoryRevision(
    left: DirectoryRevision,
    right: DirectoryRevision
  ): DirectoryRevision =
    if (directoryRevisionAtLeast(left, right)) { left } else { right }

  pure def nextDirectoryRevisionAfter(
    revision: DirectoryRevision
  ): DirectoryRevision =
    match revision {
      | DirectoryBase => EntryCreated
      | EntryCreated => EntryRenamed
      | EntryRenamed => EntryUnlinked
      | EntryUnlinked => EntryUnlinked
    }

  pure def receiptCountAtLeast(
    receipts: Node -> FileVersion,
    target: FileVersion
  ): int =
    receiptHoldersAt(receipts, target).size()

  pure def receiptHoldersAt(
    receipts: Node -> FileVersion,
    target: FileVersion
  ): Set[Node] =
    Nodes.filter(node => versionAtLeast(receipts.get(node), target))

  pure def receiptThresholdReached(
    receipts: Node -> FileVersion,
    target: FileVersion
  ): bool =
    target == BaseVersion
      or receiptCountAtLeast(receipts, target) >= dataReceiptThreshold

  pure def highestDurableVersion(
    receipts: Node -> FileVersion
  ): FileVersion =
    if (receiptThresholdReached(receipts, SecondWrite)) {
      SecondWrite
    } else if (receiptThresholdReached(receipts, FirstWrite)) {
      FirstWrite
    } else {
      BaseVersion
    }

  def payloadAtCall(kind: SyncKind): SyncPayload =
    match kind {
      | FileFsync => {
          kind: FileFsync,
          targetVersion: returnedVersion,
          targetFileMetadata: fullMetadataVisibleVersion,
          targetDirectoryRevision: DirectoryBase,
        }
      | FileDataSync => {
          kind: FileDataSync,
          targetVersion: returnedVersion,
          targetFileMetadata: BaseVersion,
          targetDirectoryRevision: DirectoryBase,
        }
      | DirectoryFsync => {
          kind: DirectoryFsync,
          targetVersion: BaseVersion,
          targetFileMetadata: BaseVersion,
          targetDirectoryRevision: directoryVisibleRevision,
        }
    }

  pure def fileSyncKind(kind: SyncKind): bool =
    kind == FileFsync or kind == FileDataSync

  pure def fencedRecord(record: SyncRecord): SyncRecord = {
    payload: record.payload,
    status: SyncFenced,
    epoch: record.epoch,
    prepareVoters: record.prepareVoters,
  }

  action init = all {
    authorityEpoch' = Epoch1,
    primary' = N1,
    returnedVersion' = BaseVersion,
    visibleVersion' = BaseVersion,
    durableVersion' = BaseVersion,
    canonicalVersion' = BaseVersion,
    metadataCertifiedVersion' = BaseVersion,
    dataReceipts' = baseReceipts,
    essentialMetadataVisibleVersion' = BaseVersion,
    essentialMetadataDurableVersion' = BaseVersion,
    fullMetadataVisibleVersion' = BaseVersion,
    fullMetadataDurableVersion' = BaseVersion,
    directoryVisibleRevision' = DirectoryBase,
    directoryDurableRevision' = DirectoryBase,
    directoryCertifiedRevision' = DirectoryBase,
    syncRecords' = emptySyncRecords,
    pendingSync' = None,
    lastResponse' = NoSyncResponse,
    lastSuppliedPayload' = None,
    lastDecision' = NoFsyncDecision,
    lastSyncId' = None,
    lastReplica' = None,
  }

  /// A completed write is visible at the current authority but is not yet a
  /// crash/failover-stable canonical promise. Writes after a sync starts do not
  /// change that sync's captured target.
  action returnWrite: bool = {
    val nextVersion = nextWriteAfter(returnedVersion)
    all {
      authorityEpoch == Epoch1,
      returnedVersion != SecondWrite,
      returnedVersion == visibleVersion,
      authorityEpoch' = authorityEpoch,
      primary' = primary,
      returnedVersion' = nextVersion,
      visibleVersion' = nextVersion,
      durableVersion' = durableVersion,
      canonicalVersion' = canonicalVersion,
      metadataCertifiedVersion' = metadataCertifiedVersion,
      dataReceipts' = dataReceipts,
      essentialMetadataVisibleVersion' = nextVersion,
      essentialMetadataDurableVersion' = essentialMetadataDurableVersion,
      fullMetadataVisibleVersion' = nextVersion,
      fullMetadataDurableVersion' = fullMetadataDurableVersion,
      directoryVisibleRevision' = directoryVisibleRevision,
      directoryDurableRevision' = directoryDurableRevision,
      directoryCertifiedRevision' = directoryCertifiedRevision,
      syncRecords' = syncRecords,
      pendingSync' = pendingSync,
      lastResponse' = NoSyncResponse,
      lastSuppliedPayload' = None,
      lastDecision' = WriteReturned,
      lastSyncId' = None,
      lastReplica' = None,
    }
  }

  /// Abstract successive create, rename, and unlink namespace generations.
  /// They are visible through the current authority but remain crash-volatile
  /// until a directory fsync captures and certifies the generation.
  action returnDirectoryMutation: bool = {
    val nextRevision = nextDirectoryRevisionAfter(directoryVisibleRevision)
    all {
      authorityEpoch == Epoch1,
      directoryVisibleRevision != EntryUnlinked,
      directoryVisibleRevision' = nextRevision,
      directoryDurableRevision' = directoryDurableRevision,
      directoryCertifiedRevision' = directoryCertifiedRevision,
      authorityEpoch' = authorityEpoch,
      primary' = primary,
      returnedVersion' = returnedVersion,
      visibleVersion' = visibleVersion,
      durableVersion' = durableVersion,
      canonicalVersion' = canonicalVersion,
      metadataCertifiedVersion' = metadataCertifiedVersion,
      dataReceipts' = dataReceipts,
      essentialMetadataVisibleVersion' = essentialMetadataVisibleVersion,
      essentialMetadataDurableVersion' = essentialMetadataDurableVersion,
      fullMetadataVisibleVersion' = fullMetadataVisibleVersion,
      fullMetadataDurableVersion' = fullMetadataDurableVersion,
      syncRecords' = syncRecords,
      pendingSync' = pendingSync,
      lastResponse' = NoSyncResponse,
      lastSuppliedPayload' = None,
      lastDecision' = DirectoryMutationReturned,
      lastSyncId' = None,
      lastReplica' = None,
    }
  }

  action recordDataReceipt(replica: Node, target: FileVersion): bool = {
    val newReceipts = dataReceipts.set(replica, target)
    all {
      authorityEpoch == Epoch1,
      target != BaseVersion,
      versionAtLeast(visibleVersion, target),
      versionRank(target) > versionRank(dataReceipts.get(replica)),
      authorityEpoch' = authorityEpoch,
      primary' = primary,
      returnedVersion' = returnedVersion,
      visibleVersion' = visibleVersion,
      durableVersion' = highestDurableVersion(newReceipts),
      canonicalVersion' = canonicalVersion,
      metadataCertifiedVersion' = metadataCertifiedVersion,
      dataReceipts' = newReceipts,
      essentialMetadataVisibleVersion' = essentialMetadataVisibleVersion,
      essentialMetadataDurableVersion' = essentialMetadataDurableVersion,
      fullMetadataVisibleVersion' = fullMetadataVisibleVersion,
      fullMetadataDurableVersion' = fullMetadataDurableVersion,
      directoryVisibleRevision' = directoryVisibleRevision,
      directoryDurableRevision' = directoryDurableRevision,
      directoryCertifiedRevision' = directoryCertifiedRevision,
      syncRecords' = syncRecords,
      pendingSync' = pendingSync,
      lastResponse' = NoSyncResponse,
      lastSuppliedPayload' = None,
      lastDecision' = DataReceiptRecorded,
      lastSyncId' = None,
      lastReplica' = Some(replica),
    }
  }

  action beginSync(syncId: SyncId, kind: SyncKind): bool = {
    val payload = payloadAtCall(kind)
    all {
      syncRecords.get(syncId) == None,
      pendingSync == None,
      authorityEpoch' = authorityEpoch,
      primary' = primary,
      returnedVersion' = returnedVersion,
      visibleVersion' = visibleVersion,
      durableVersion' = durableVersion,
      canonicalVersion' = canonicalVersion,
      metadataCertifiedVersion' = metadataCertifiedVersion,
      dataReceipts' = dataReceipts,
      essentialMetadataVisibleVersion' = essentialMetadataVisibleVersion,
      essentialMetadataDurableVersion' = essentialMetadataDurableVersion,
      fullMetadataVisibleVersion' = fullMetadataVisibleVersion,
      fullMetadataDurableVersion' = fullMetadataDurableVersion,
      directoryVisibleRevision' = directoryVisibleRevision,
      directoryDurableRevision' = directoryDurableRevision,
      directoryCertifiedRevision' = directoryCertifiedRevision,
      syncRecords' = syncRecords.set(syncId, Some({
        payload: payload,
        status: SyncInFlight,
        epoch: authorityEpoch,
        prepareVoters: Set(),
      })),
      pendingSync' = Some(syncId),
      lastResponse' = SyncPending(syncId),
      lastSuppliedPayload' = Some(payload),
      lastDecision' = SyncStarted,
      lastSyncId' = Some(syncId),
      lastReplica' = None,
    }
  }

  /// Abstract the pre-commit metadata-journal acknowledgement step as a durable
  /// prepare quorum. This evidence authorizes the following atomic commit but
  /// is not itself a failover-recoverable commit certificate. A concrete
  /// refinement may collect these acknowledgements one by one.
  action formMetadataPrepareQuorum(syncId: SyncId): bool = {
    val record = unwrapOr(syncRecords.get(syncId), emptyRecord)
    all {
      pendingSync == Some(syncId),
      record.status == SyncInFlight,
      record.epoch == authorityEpoch,
      not(hasQuorum(record.prepareVoters)),
      authorityEpoch' = authorityEpoch,
      primary' = primary,
      returnedVersion' = returnedVersion,
      visibleVersion' = visibleVersion,
      durableVersion' = durableVersion,
      canonicalVersion' = canonicalVersion,
      metadataCertifiedVersion' = metadataCertifiedVersion,
      dataReceipts' = dataReceipts,
      essentialMetadataVisibleVersion' = essentialMetadataVisibleVersion,
      essentialMetadataDurableVersion' = essentialMetadataDurableVersion,
      fullMetadataVisibleVersion' = fullMetadataVisibleVersion,
      fullMetadataDurableVersion' = fullMetadataDurableVersion,
      directoryVisibleRevision' = directoryVisibleRevision,
      directoryDurableRevision' = directoryDurableRevision,
      directoryCertifiedRevision' = directoryCertifiedRevision,
      syncRecords' = syncRecords.set(syncId, Some({
        payload: record.payload,
        status: record.status,
        epoch: record.epoch,
        prepareVoters: Set(N1, N2),
      })),
      pendingSync' = pendingSync,
      lastResponse' = SyncPending(syncId),
      lastSuppliedPayload' = Some(record.payload),
      lastDecision' = MetadataPrepareQuorumFormed,
      lastSyncId' = Some(syncId),
      lastReplica' = None,
    }
  }

  /// Atomic sync commit: the receipt threshold and prepare quorum are checked
  /// before the failover-recoverable metadata certificate and canonical prefix
  /// advance. `deliverReply` separates commit from result transport.
  action commitSync(syncId: SyncId, deliverReply: bool): bool = {
    val record = unwrapOr(syncRecords.get(syncId), emptyRecord)
    val payload = record.payload
    val fileSync = fileSyncKind(payload.kind)
    all {
      pendingSync == Some(syncId),
      record.status == SyncInFlight,
      record.epoch == authorityEpoch,
      hasQuorum(record.prepareVoters),
      not(fileSync) or versionAtLeast(durableVersion, payload.targetVersion),
      authorityEpoch' = authorityEpoch,
      primary' = primary,
      returnedVersion' = returnedVersion,
      visibleVersion' = visibleVersion,
      durableVersion' = durableVersion,
      canonicalVersion' =
        if (fileSync) {
          laterVersion(canonicalVersion, payload.targetVersion)
        } else {
          canonicalVersion
        },
      metadataCertifiedVersion' =
        if (fileSync) {
          laterVersion(metadataCertifiedVersion, payload.targetVersion)
        } else {
          metadataCertifiedVersion
        },
      dataReceipts' = dataReceipts,
      essentialMetadataVisibleVersion' = essentialMetadataVisibleVersion,
      essentialMetadataDurableVersion' =
        if (fileSync) {
          laterVersion(essentialMetadataDurableVersion, payload.targetVersion)
        } else {
          essentialMetadataDurableVersion
        },
      fullMetadataVisibleVersion' = fullMetadataVisibleVersion,
      fullMetadataDurableVersion' =
        if (payload.kind == FileFsync) {
          laterVersion(fullMetadataDurableVersion, payload.targetFileMetadata)
        } else {
          fullMetadataDurableVersion
        },
      directoryVisibleRevision' = directoryVisibleRevision,
      directoryDurableRevision' =
        if (payload.kind == DirectoryFsync) {
          laterDirectoryRevision(
            directoryDurableRevision,
            payload.targetDirectoryRevision
          )
        } else {
          directoryDurableRevision
        },
      directoryCertifiedRevision' =
        if (payload.kind == DirectoryFsync) {
          laterDirectoryRevision(
            directoryCertifiedRevision,
            payload.targetDirectoryRevision
          )
        } else {
          directoryCertifiedRevision
        },
      syncRecords' = syncRecords.set(syncId, Some({
        payload: payload,
        status: SyncCommitted,
        epoch: record.epoch,
        prepareVoters: record.prepareVoters,
      })),
      pendingSync' = None,
      lastResponse' =
        if (deliverReply) {
          SyncSuccess(syncId)
        } else {
          SyncOutcomeUnknown(syncId)
        },
      lastSuppliedPayload' = Some(payload),
      lastDecision' =
        if (deliverReply) {
          SyncCommittedWithReply
        } else {
          SyncCommittedReplyLost
        },
      lastSyncId' = Some(syncId),
      lastReplica' = None,
    }
  }

  action timeoutPendingSync(syncId: SyncId): bool = {
    val record = unwrapOr(syncRecords.get(syncId), emptyRecord)
    all {
      pendingSync == Some(syncId),
      record.status == SyncInFlight,
      authorityEpoch' = authorityEpoch,
      primary' = primary,
      returnedVersion' = returnedVersion,
      visibleVersion' = visibleVersion,
      durableVersion' = durableVersion,
      canonicalVersion' = canonicalVersion,
      metadataCertifiedVersion' = metadataCertifiedVersion,
      dataReceipts' = dataReceipts,
      essentialMetadataVisibleVersion' = essentialMetadataVisibleVersion,
      essentialMetadataDurableVersion' = essentialMetadataDurableVersion,
      fullMetadataVisibleVersion' = fullMetadataVisibleVersion,
      fullMetadataDurableVersion' = fullMetadataDurableVersion,
      directoryVisibleRevision' = directoryVisibleRevision,
      directoryDurableRevision' = directoryDurableRevision,
      directoryCertifiedRevision' = directoryCertifiedRevision,
      syncRecords' = syncRecords,
      pendingSync' = pendingSync,
      lastResponse' = SyncOutcomeUnknown(syncId),
      lastSuppliedPayload' = Some(record.payload),
      lastDecision' = SyncTimedOut,
      lastSyncId' = Some(syncId),
      lastReplica' = None,
    }
  }

  /// A retry carries the complete original payload. The stored payload is
  /// immutable; an id/payload mismatch is terminal for this request and cannot
  /// mutate either the sync record or the filesystem.
  action retrySync(
    syncId: SyncId,
    kind: SyncKind,
    target: FileVersion,
    metadataTarget: FileVersion,
    directoryTarget: DirectoryRevision
  ): bool = {
    val record = unwrapOr(syncRecords.get(syncId), emptyRecord)
    val supplied = {
      kind: kind,
      targetVersion: target,
      targetFileMetadata: metadataTarget,
      targetDirectoryRevision: directoryTarget,
    }
    val matches = syncRecords.get(syncId) != None and record.payload == supplied
    all {
      syncRecords.get(syncId) != None,
      authorityEpoch' = authorityEpoch,
      primary' = primary,
      returnedVersion' = returnedVersion,
      visibleVersion' = visibleVersion,
      durableVersion' = durableVersion,
      canonicalVersion' = canonicalVersion,
      metadataCertifiedVersion' = metadataCertifiedVersion,
      dataReceipts' = dataReceipts,
      essentialMetadataVisibleVersion' = essentialMetadataVisibleVersion,
      essentialMetadataDurableVersion' = essentialMetadataDurableVersion,
      fullMetadataVisibleVersion' = fullMetadataVisibleVersion,
      fullMetadataDurableVersion' = fullMetadataDurableVersion,
      directoryVisibleRevision' = directoryVisibleRevision,
      directoryDurableRevision' = directoryDurableRevision,
      directoryCertifiedRevision' = directoryCertifiedRevision,
      syncRecords' = syncRecords,
      pendingSync' = pendingSync,
      lastResponse' =
        if (not(matches)) {
          SyncPayloadMismatch(syncId)
        } else if (record.status == SyncCommitted) {
          SyncSuccess(syncId)
        } else if (record.status == SyncFenced) {
          SyncNotCommitted(syncId)
        } else {
          SyncPending(syncId)
        },
      lastSuppliedPayload' = Some(supplied),
      lastDecision' =
        if (not(matches)) {
          SyncRetryPayloadRejected
        } else if (record.status == SyncCommitted) {
          SyncRetryDeduplicated
        } else if (record.status == SyncFenced) {
          SyncRetryFenced
        } else {
          SyncRetryPending
        },
      lastSyncId' = Some(syncId),
      lastReplica' = None,
    }
  }

  /// A new quorum authority wins over an uncertified old-primary tail. Any
  /// active sync is made terminal, so it cannot wait forever or commit under
  /// its stale epoch. Certified data and committed sync records survive.
  action fenceAuthority: bool = {
    val activeId = unwrapOr(pendingSync, Sync1)
    val activeRecord = unwrapOr(syncRecords.get(activeId), emptyRecord)
    all {
      authorityEpoch == Epoch1,
      authorityEpoch' = Epoch2,
      primary' = N2,
      returnedVersion' = canonicalVersion,
      visibleVersion' = canonicalVersion,
      durableVersion' = durableVersion,
      canonicalVersion' = canonicalVersion,
      metadataCertifiedVersion' = metadataCertifiedVersion,
      dataReceipts' = dataReceipts,
      essentialMetadataVisibleVersion' = essentialMetadataDurableVersion,
      essentialMetadataDurableVersion' = essentialMetadataDurableVersion,
      fullMetadataVisibleVersion' = fullMetadataDurableVersion,
      fullMetadataDurableVersion' = fullMetadataDurableVersion,
      directoryVisibleRevision' = directoryDurableRevision,
      directoryDurableRevision' = directoryDurableRevision,
      directoryCertifiedRevision' = directoryCertifiedRevision,
      syncRecords' =
        match pendingSync {
          | Some(syncId) => syncRecords.set(syncId, Some(fencedRecord(activeRecord)))
          | None => syncRecords
        },
      pendingSync' = None,
      lastResponse' =
        match pendingSync {
          | Some(syncId) => SyncOutcomeUnknown(syncId)
          | None => NoSyncResponse
        },
      lastSuppliedPayload' =
        match pendingSync {
          | Some(_) => Some(activeRecord.payload)
          | None => None
        },
      lastDecision' = AuthorityFenced,
      lastSyncId' = pendingSync,
      lastReplica' = None,
    }
  }

  /// Recovery/repair copies only the certified canonical prefix. Orphaned
  /// durable tails never become visible merely because a replica has them.
  action repairCanonicalReplica(replica: Node): bool = all {
    authorityEpoch == Epoch2,
    canonicalVersion != BaseVersion,
    versionRank(dataReceipts.get(replica)) < versionRank(canonicalVersion),
    authorityEpoch' = authorityEpoch,
    primary' = primary,
    returnedVersion' = returnedVersion,
    visibleVersion' = visibleVersion,
    durableVersion' = highestDurableVersion(
      dataReceipts.set(replica, canonicalVersion)
    ),
    canonicalVersion' = canonicalVersion,
    metadataCertifiedVersion' = metadataCertifiedVersion,
    dataReceipts' = dataReceipts.set(replica, canonicalVersion),
    essentialMetadataVisibleVersion' = essentialMetadataVisibleVersion,
    essentialMetadataDurableVersion' = essentialMetadataDurableVersion,
    fullMetadataVisibleVersion' = fullMetadataVisibleVersion,
    fullMetadataDurableVersion' = fullMetadataDurableVersion,
    directoryVisibleRevision' = directoryVisibleRevision,
    directoryDurableRevision' = directoryDurableRevision,
    directoryCertifiedRevision' = directoryCertifiedRevision,
    syncRecords' = syncRecords,
    pendingSync' = pendingSync,
    lastResponse' = NoSyncResponse,
    lastSuppliedPayload' = None,
    lastDecision' = CanonicalReplicaRepaired,
    lastSyncId' = None,
    lastReplica' = Some(replica),
  }

  action stutter: bool = all {
    authorityEpoch' = authorityEpoch,
    primary' = primary,
    returnedVersion' = returnedVersion,
    visibleVersion' = visibleVersion,
    durableVersion' = durableVersion,
    canonicalVersion' = canonicalVersion,
    metadataCertifiedVersion' = metadataCertifiedVersion,
    dataReceipts' = dataReceipts,
    essentialMetadataVisibleVersion' = essentialMetadataVisibleVersion,
    essentialMetadataDurableVersion' = essentialMetadataDurableVersion,
    fullMetadataVisibleVersion' = fullMetadataVisibleVersion,
    fullMetadataDurableVersion' = fullMetadataDurableVersion,
    directoryVisibleRevision' = directoryVisibleRevision,
    directoryDurableRevision' = directoryDurableRevision,
    directoryCertifiedRevision' = directoryCertifiedRevision,
    syncRecords' = syncRecords,
    pendingSync' = pendingSync,
    lastResponse' = lastResponse,
    lastSuppliedPayload' = lastSuppliedPayload,
    lastDecision' = TerminalStutter,
    lastSyncId' = lastSyncId,
    lastReplica' = lastReplica,
  }

  action step = any {
    returnWrite,
    returnDirectoryMutation,
    {
      nondet replica = oneOf(Nodes)
      nondet target = oneOf(FileVersions)
      recordDataReceipt(replica, target)
    },
    {
      nondet syncId = oneOf(SyncIds)
      nondet kind = oneOf(SyncKinds)
      beginSync(syncId, kind)
    },
    {
      nondet syncId = oneOf(SyncIds)
      formMetadataPrepareQuorum(syncId)
    },
    {
      nondet syncId = oneOf(SyncIds)
      nondet deliverReply = oneOf(Set(true, false))
      commitSync(syncId, deliverReply)
    },
    {
      nondet syncId = oneOf(SyncIds)
      timeoutPendingSync(syncId)
    },
    {
      nondet syncId = oneOf(SyncIds)
      nondet kind = oneOf(SyncKinds)
      nondet target = oneOf(FileVersions)
      nondet metadataTarget = oneOf(FileVersions)
      nondet directoryTarget = oneOf(DirectoryRevisions)
      retrySync(syncId, kind, target, metadataTarget, directoryTarget)
    },
    fenceAuthority,
    {
      nondet replica = oneOf(Nodes)
      repairCanonicalReplica(replica)
    },
    stutter,
  }
}
