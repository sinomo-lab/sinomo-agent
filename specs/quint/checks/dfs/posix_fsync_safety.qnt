/// Safety invariants and coverage witnesses for `dfs_posix_fsync`.
module dfs_posix_fsync_safety {
  import common.* from "../../lib/common"
  import dfs_posix_fsync.* from "../../models/dfs/posix_fsync"

  def receiptsDefineDurableVersion: bool =
    durableVersion == highestDurableVersion(dataReceipts)


  def canonicalDataHasReceiptThreshold: bool =
    versionAtLeast(durableVersion, canonicalVersion)


  def canonicalDataHasMetadataCertificate: bool =
    versionAtLeast(metadataCertifiedVersion, canonicalVersion)


  def certifiedMetadataNeverRunsAheadOfCanonicalData: bool =
    versionAtLeast(canonicalVersion, metadataCertifiedVersion)


  def currentAuthorityNeverExposesBeforeCanonical: bool =
    versionAtLeast(visibleVersion, canonicalVersion)


  def returnedWritesMatchCurrentAuthorityVisibility: bool =
    returnedVersion == visibleVersion


  def durableMetadataIsOrdered: bool = all {
    versionAtLeast(canonicalVersion, essentialMetadataDurableVersion),
    versionAtLeast(essentialMetadataDurableVersion, fullMetadataDurableVersion),
  }


  def directoryDurabilityHasCertificate: bool = all {
    directoryRevisionAtLeast(
      directoryVisibleRevision,
      directoryDurableRevision
    ),
    directoryRevisionAtLeast(
      directoryCertifiedRevision,
      directoryDurableRevision
    ),
    directoryRevisionAtLeast(
      directoryDurableRevision,
      directoryCertifiedRevision
    ),
  }


  def pendingSyncNamesAnInflightRecord: bool =
    match pendingSync {
      | Some(syncId) =>
          match syncRecords.get(syncId) {
            | Some(record) => record.status == SyncInFlight
            | None => false
          }
      | None => true
    }


  def committedSyncHasQuorumMetadataCertificate: bool =
    SyncIds.forall(syncId =>
      match syncRecords.get(syncId) {
        | Some(record) =>
            record.status != SyncCommitted or hasQuorum(record.prepareVoters)
        | None => true
      }
    )


  /// In the bounded three-node policy, every committed file-sync target has a
  /// durable content holder in common with its metadata prepare quorum. This
  /// is the recovery-set intersection consumed abstractly by the combined
  /// partition model.
  def committedFileSyncRecoverySetsIntersect: bool =
    SyncIds.forall(syncId =>
      match syncRecords.get(syncId) {
        | Some(record) =>
            if (record.status == SyncCommitted and fileSyncKind(record.payload.kind)) {
              record.prepareVoters
                .intersect(receiptHoldersAt(dataReceipts, record.payload.targetVersion))
                .size() > 0
            } else {
              true
            }
        | None => true
      }
    )


  /// This is the central fsync/fdatasync contract: a committed record's frozen
  /// target, not the possibly newer returned frontier, is fully durable and
  /// canonical. Success responses and idempotent retries both rely on it.
  def committedFileSyncCoversPreCallWrites: bool =
    SyncIds.forall(syncId =>
      match syncRecords.get(syncId) {
        | Some(record) =>
            if (record.status == SyncCommitted and fileSyncKind(record.payload.kind)) {
              all {
                versionAtLeast(durableVersion, record.payload.targetVersion),
                versionAtLeast(canonicalVersion, record.payload.targetVersion),
                versionAtLeast(
                  metadataCertifiedVersion,
                  record.payload.targetVersion
                ),
                versionAtLeast(
                  essentialMetadataDurableVersion,
                  record.payload.targetVersion
                ),
              }
            } else {
              true
            }
        | None => true
      }
    )


  def committedFullFsyncCoversAncillaryMetadata: bool =
    SyncIds.forall(syncId =>
      match syncRecords.get(syncId) {
        | Some(record) =>
            if (record.status == SyncCommitted and record.payload.kind == FileFsync) {
              versionAtLeast(
                fullMetadataDurableVersion,
                record.payload.targetFileMetadata
              )
            } else {
              true
            }
        | None => true
      }
    )


  def fileSyncPayloadMakesNoDirectoryClaim: bool =
    SyncIds.forall(syncId =>
      match syncRecords.get(syncId) {
        | Some(record) =>
            not(fileSyncKind(record.payload.kind))
              or record.payload.targetDirectoryRevision == DirectoryBase
        | None => true
      }
    )


  def fdatasyncPayloadMakesNoAncillaryMetadataClaim: bool =
    SyncIds.forall(syncId =>
      match syncRecords.get(syncId) {
        | Some(record) =>
            record.payload.kind != FileDataSync
              or record.payload.targetFileMetadata == BaseVersion
        | None => true
      }
    )


  def directorySyncPayloadMakesNoFileDataClaim: bool =
    SyncIds.forall(syncId =>
      match syncRecords.get(syncId) {
        | Some(record) =>
            record.payload.kind != DirectoryFsync
              or all {
                record.payload.targetVersion == BaseVersion,
                record.payload.targetFileMetadata == BaseVersion,
              }
        | None => true
      }
    )


  def committedDirectorySyncCoversCapturedEntry: bool =
    SyncIds.forall(syncId =>
      match syncRecords.get(syncId) {
        | Some(record) =>
            if (record.status == SyncCommitted
              and record.payload.kind == DirectoryFsync
              and record.payload.targetDirectoryRevision != DirectoryBase) {
              all {
                directoryRevisionAtLeast(
                  directoryDurableRevision,
                  record.payload.targetDirectoryRevision
                ),
                directoryRevisionAtLeast(
                  directoryCertifiedRevision,
                  record.payload.targetDirectoryRevision
                ),
              }
            } else {
              true
            }
        | None => true
      }
    )


  def successNamesCommittedRecord: bool =
    match lastResponse {
      | SyncSuccess(syncId) =>
          match syncRecords.get(syncId) {
            | Some(record) => record.status == SyncCommitted
            | None => false
          }
      | _ => true
    }


  def payloadMismatchIsAuditable: bool =
    match lastResponse {
      | SyncPayloadMismatch(syncId) =>
          match syncRecords.get(syncId) {
            | Some(record) =>
                match lastSuppliedPayload {
                  | Some(supplied) => record.payload != supplied
                  | None => false
                }
            | None => false
          }
      | _ => true
    }


  def deduplicatedRetryNamesCommittedRecord: bool =
    if (lastDecision == SyncRetryDeduplicated) {
      match lastSyncId {
        | Some(syncId) =>
            match syncRecords.get(syncId) {
              | Some(record) => all {
                  record.status == SyncCommitted,
                  lastResponse == SyncSuccess(syncId),
                }
              | None => false
            }
        | None => false
      }
    } else {
      true
    }


  def fencedSyncCannotReportSuccess: bool =
    match lastResponse {
      | SyncSuccess(syncId) =>
          match syncRecords.get(syncId) {
            | Some(record) => record.status != SyncFenced
            | None => false
          }
      | _ => true
    }


  def fencedInflightSyncIsTerminal: bool =
    SyncIds.forall(syncId =>
      match syncRecords.get(syncId) {
        | Some(record) =>
            record.status != SyncFenced or record.epoch != authorityEpoch
        | None => true
      }
    )


  def authorityFenceExposesOnlyCertifiedState: bool =
    authorityEpoch != Epoch2 or all {
      visibleVersion == canonicalVersion,
      returnedVersion == canonicalVersion,
      primary == N2,
    }


  def repairNeverPromotesUncertifiedData: bool =
    if (lastDecision == CanonicalReplicaRepaired) {
      match lastReplica {
        | Some(replica) => all {
            versionAtLeast(dataReceipts.get(replica), canonicalVersion),
            versionAtLeast(metadataCertifiedVersion, canonicalVersion),
          }
        | None => false
      }
    } else {
      true
    }


  /// Coverage witnesses are excluded from `safety`; the harness asks Quint to
  /// report that exploration reached each semantically important outcome.
  def fileFsyncSuccessWithoutDirectoryDurabilityWitness: bool =
    match lastResponse {
      | SyncSuccess(syncId) =>
          match syncRecords.get(syncId) {
            | Some(record) => all {
                record.payload.kind == FileFsync,
                record.payload.targetVersion != BaseVersion,
                directoryDurableRevision == DirectoryBase,
              }
            | None => false
          }
      | _ => false
    }


  def fdatasyncSuccessWithoutAncillaryMetadataWitness: bool =
    match lastResponse {
      | SyncSuccess(syncId) =>
          match syncRecords.get(syncId) {
            | Some(record) => all {
                record.payload.kind == FileDataSync,
                record.payload.targetVersion != BaseVersion,
                versionAtLeast(
                  essentialMetadataDurableVersion,
                  record.payload.targetVersion
                ),
                fullMetadataDurableVersion == BaseVersion,
              }
            | None => false
          }
      | _ => false
    }


  def directoryFsyncDoesNotSyncFileDataWitness: bool =
    match lastResponse {
      | SyncSuccess(syncId) =>
          match syncRecords.get(syncId) {
            | Some(record) => all {
                record.payload.kind == DirectoryFsync,
                record.payload.targetDirectoryRevision != DirectoryBase,
                directoryRevisionAtLeast(
                  directoryDurableRevision,
                  record.payload.targetDirectoryRevision
                ),
                canonicalVersion == BaseVersion,
              }
            | None => false
          }
      | _ => false
    }


  def laterWriteNotCoveredByEarlierFsyncWitness: bool =
    match lastResponse {
      | SyncSuccess(syncId) =>
          match syncRecords.get(syncId) {
            | Some(record) => all {
                record.payload.kind == FileFsync,
                record.payload.targetVersion == FirstWrite,
                returnedVersion == SecondWrite,
                canonicalVersion == FirstWrite,
              }
            | None => false
          }
      | _ => false
    }


  def committedUnknownOutcomeDeduplicatesWitness: bool =
    lastDecision == SyncRetryDeduplicated


  def payloadMismatchRejectedWitness: bool =
    lastDecision == SyncRetryPayloadRejected


  def inflightSyncFencedWithoutStaleCommitWitness: bool = all {
    lastDecision == AuthorityFenced,
    match lastSyncId {
      | Some(syncId) =>
          match syncRecords.get(syncId) {
            | Some(record) => all {
                record.status == SyncFenced,
                lastResponse == SyncOutcomeUnknown(syncId),
              }
            | None => false
          }
      | None => false
    },
  }


  def successfulSyncSurvivesAuthorityChangeWitness: bool = all {
    authorityEpoch == Epoch2,
    SyncIds.exists(syncId =>
      match syncRecords.get(syncId) {
        | Some(record) => all {
            record.status == SyncCommitted,
            fileSyncKind(record.payload.kind),
            record.payload.targetVersion != BaseVersion,
            versionAtLeast(canonicalVersion, record.payload.targetVersion),
          }
        | None => false
      }
    ),
  }


  def committedPrefixRepairWitness: bool = all {
    lastDecision == CanonicalReplicaRepaired,
    canonicalVersion != BaseVersion,
    Nodes.forall(replica =>
      versionAtLeast(dataReceipts.get(replica), canonicalVersion)
    ),
  }


  def safety: bool = all {
    receiptsDefineDurableVersion,
    canonicalDataHasReceiptThreshold,
    canonicalDataHasMetadataCertificate,
    certifiedMetadataNeverRunsAheadOfCanonicalData,
    currentAuthorityNeverExposesBeforeCanonical,
    returnedWritesMatchCurrentAuthorityVisibility,
    durableMetadataIsOrdered,
    directoryDurabilityHasCertificate,
    pendingSyncNamesAnInflightRecord,
    committedSyncHasQuorumMetadataCertificate,
    committedFileSyncRecoverySetsIntersect,
    committedFileSyncCoversPreCallWrites,
    committedFullFsyncCoversAncillaryMetadata,
    fileSyncPayloadMakesNoDirectoryClaim,
    fdatasyncPayloadMakesNoAncillaryMetadataClaim,
    directorySyncPayloadMakesNoFileDataClaim,
    committedDirectorySyncCoversCapturedEntry,
    successNamesCommittedRecord,
    payloadMismatchIsAuditable,
    deduplicatedRetryNamesCommittedRecord,
    fencedSyncCannotReportSuccess,
    fencedInflightSyncIsTerminal,
    authorityFenceExposesOnlyCertifiedState,
    repairNeverPromotesUncertifiedData,
  }
}
