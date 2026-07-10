/// Deterministic file-fsync recovery and directory-generation barriers.
module dfs_posix_fsync_unknown_recovery_scenario {
  import common.* from "../../lib/common"
  import dfs_posix_fsync from "../../models/dfs/posix_fsync"

  type Phase =
    | Start
    | WriteVisible
    | FirstReceipt
    | DataDurable
    | SyncPending
    | PrepareQuorum
    | CommittedUnknown
    | AuthorityChanged
    | ReplicaRepaired
    | RetryConfirmed

  var phase: Phase

  action init = all {
    dfs_posix_fsync::init,
    phase' = Start,
  }

  action step =
    if (phase == Start) {
      all {
        dfs_posix_fsync::returnWrite,
        phase' = WriteVisible,
      }
    } else if (phase == WriteVisible) {
      all {
        dfs_posix_fsync::recordDataReceipt(
          N2,
          dfs_posix_fsync::FirstWrite
        ),
        phase' = FirstReceipt,
      }
    } else if (phase == FirstReceipt) {
      all {
        dfs_posix_fsync::recordDataReceipt(
          N3,
          dfs_posix_fsync::FirstWrite
        ),
        phase' = DataDurable,
      }
    } else if (phase == DataDurable) {
      all {
        dfs_posix_fsync::beginSync(
          dfs_posix_fsync::Sync1,
          dfs_posix_fsync::FileFsync
        ),
        phase' = SyncPending,
      }
    } else if (phase == SyncPending) {
      all {
        dfs_posix_fsync::formMetadataPrepareQuorum(
          dfs_posix_fsync::Sync1
        ),
        phase' = PrepareQuorum,
      }
    } else if (phase == PrepareQuorum) {
      all {
        dfs_posix_fsync::commitSync(dfs_posix_fsync::Sync1, false),
        phase' = CommittedUnknown,
      }
    } else if (phase == CommittedUnknown) {
      all {
        dfs_posix_fsync::fenceAuthority,
        phase' = AuthorityChanged,
      }
    } else if (phase == AuthorityChanged) {
      all {
        dfs_posix_fsync::repairCanonicalReplica(
          N1
        ),
        phase' = ReplicaRepaired,
      }
    } else if (phase == ReplicaRepaired) {
      all {
        dfs_posix_fsync::retrySync(
          dfs_posix_fsync::Sync1,
          dfs_posix_fsync::FileFsync,
          dfs_posix_fsync::FirstWrite,
          dfs_posix_fsync::FirstWrite,
          dfs_posix_fsync::DirectoryBase
        ),
        phase' = RetryConfirmed,
      }
    } else {
      all {
        phase == RetryConfirmed,
        dfs_posix_fsync::stutter,
        phase' = phase,
      }
    }

  /// The deterministic trace deliberately uses data holders N2/N3 and metadata
  /// prepare voters N1/N2, so the non-base commit exercises a one-node recovery
  /// intersection rather than reusing identical quorum sets.
  def committedFileSyncRecoverySetsIntersect: bool =
    dfs_posix_fsync::SyncIds.forall(syncId =>
      match dfs_posix_fsync::syncRecords.get(syncId) {
        | Some(record) =>
            if (all {
              record.status == dfs_posix_fsync::SyncCommitted,
              dfs_posix_fsync::fileSyncKind(record.payload.kind),
            }) {
              record.prepareVoters
                .intersect(dfs_posix_fsync::receiptHoldersAt(
                  dfs_posix_fsync::dataReceipts,
                  record.payload.targetVersion
                ))
                .size() > 0
            } else {
              true
            }
        | None => true
      }
    )

  def safety: bool = all {
    dfs_posix_fsync::durableVersion
      == dfs_posix_fsync::highestDurableVersion(
        dfs_posix_fsync::dataReceipts
      ),
    dfs_posix_fsync::versionAtLeast(
      dfs_posix_fsync::durableVersion,
      dfs_posix_fsync::canonicalVersion
    ),
    dfs_posix_fsync::metadataCertifiedVersion
      == dfs_posix_fsync::canonicalVersion,
    dfs_posix_fsync::directoryRevisionAtLeast(
      dfs_posix_fsync::directoryVisibleRevision,
      dfs_posix_fsync::directoryDurableRevision
    ),
    committedFileSyncRecoverySetsIntersect,
    match phase {
      | Start => all {
          dfs_posix_fsync::returnedVersion == dfs_posix_fsync::BaseVersion,
          dfs_posix_fsync::canonicalVersion == dfs_posix_fsync::BaseVersion,
        }
      | WriteVisible => all {
          dfs_posix_fsync::visibleVersion == dfs_posix_fsync::FirstWrite,
          dfs_posix_fsync::canonicalVersion == dfs_posix_fsync::BaseVersion,
        }
      | FirstReceipt =>
          dfs_posix_fsync::durableVersion == dfs_posix_fsync::BaseVersion
      | DataDurable =>
          dfs_posix_fsync::durableVersion == dfs_posix_fsync::FirstWrite
      | SyncPending => all {
          dfs_posix_fsync::pendingSync == Some(dfs_posix_fsync::Sync1),
          match dfs_posix_fsync::syncRecords.get(dfs_posix_fsync::Sync1) {
            | Some(record) => not(hasQuorum(record.prepareVoters))
            | None => false
          },
        }
      | PrepareQuorum => all {
          dfs_posix_fsync::receiptHoldersAt(
            dfs_posix_fsync::dataReceipts,
            dfs_posix_fsync::FirstWrite
          ) == Set(N2, N3),
          match dfs_posix_fsync::syncRecords.get(dfs_posix_fsync::Sync1) {
            | Some(record) => all {
                record.prepareVoters == Set(N1, N2),
                hasQuorum(record.prepareVoters),
                record.prepareVoters
                  .intersect(dfs_posix_fsync::receiptHoldersAt(
                    dfs_posix_fsync::dataReceipts,
                    dfs_posix_fsync::FirstWrite
                  )) == Set(N2),
              }
            | None => false
          },
        }
      | CommittedUnknown => all {
          dfs_posix_fsync::canonicalVersion == dfs_posix_fsync::FirstWrite,
          dfs_posix_fsync::lastResponse
            == dfs_posix_fsync::SyncOutcomeUnknown(dfs_posix_fsync::Sync1),
        }
      | AuthorityChanged => all {
          dfs_posix_fsync::authorityEpoch == dfs_posix_fsync::Epoch2,
          dfs_posix_fsync::visibleVersion == dfs_posix_fsync::FirstWrite,
          match dfs_posix_fsync::syncRecords.get(dfs_posix_fsync::Sync1) {
            | Some(record) => record.status == dfs_posix_fsync::SyncCommitted
            | None => false
          },
        }
      | ReplicaRepaired => all {
          Nodes.forall(replica =>
            dfs_posix_fsync::versionAtLeast(
              dfs_posix_fsync::dataReceipts.get(replica),
              dfs_posix_fsync::FirstWrite
            )
          ),
          dfs_posix_fsync::canonicalVersion == dfs_posix_fsync::FirstWrite,
        }
      | RetryConfirmed => all {
          dfs_posix_fsync::lastResponse
            == dfs_posix_fsync::SyncSuccess(dfs_posix_fsync::Sync1),
          any {
            dfs_posix_fsync::lastDecision
              == dfs_posix_fsync::SyncRetryDeduplicated,
            dfs_posix_fsync::lastDecision
              == dfs_posix_fsync::TerminalStutter,
          },
        }
    },
  }
}

module dfs_posix_directory_fsync_generation_scenario {
  import common.* from "../../lib/common"
  import dfs_posix_fsync from "../../models/dfs/posix_fsync"

  type Phase =
    | Start
    | CreateVisible
    | FirstSyncPending
    | RenameVisible
    | FirstPrepareQuorum
    | CreateDurable
    | SecondSyncPending
    | SecondPrepareQuorum
    | RenameDurable

  var phase: Phase

  action init = all {
    dfs_posix_fsync::init,
    phase' = Start,
  }

  action step =
    if (phase == Start) {
      all {
        dfs_posix_fsync::returnDirectoryMutation,
        phase' = CreateVisible,
      }
    } else if (phase == CreateVisible) {
      all {
        dfs_posix_fsync::beginSync(
          dfs_posix_fsync::Sync1,
          dfs_posix_fsync::DirectoryFsync
        ),
        phase' = FirstSyncPending,
      }
    } else if (phase == FirstSyncPending) {
      all {
        dfs_posix_fsync::returnDirectoryMutation,
        phase' = RenameVisible,
      }
    } else if (phase == RenameVisible) {
      all {
        dfs_posix_fsync::formMetadataPrepareQuorum(
          dfs_posix_fsync::Sync1
        ),
        phase' = FirstPrepareQuorum,
      }
    } else if (phase == FirstPrepareQuorum) {
      all {
        dfs_posix_fsync::commitSync(dfs_posix_fsync::Sync1, true),
        phase' = CreateDurable,
      }
    } else if (phase == CreateDurable) {
      all {
        dfs_posix_fsync::beginSync(
          dfs_posix_fsync::Sync2,
          dfs_posix_fsync::DirectoryFsync
        ),
        phase' = SecondSyncPending,
      }
    } else if (phase == SecondSyncPending) {
      all {
        dfs_posix_fsync::formMetadataPrepareQuorum(
          dfs_posix_fsync::Sync2
        ),
        phase' = SecondPrepareQuorum,
      }
    } else if (phase == SecondPrepareQuorum) {
      all {
        dfs_posix_fsync::commitSync(dfs_posix_fsync::Sync2, true),
        phase' = RenameDurable,
      }
    } else {
      all {
        phase == RenameDurable,
        dfs_posix_fsync::stutter,
        phase' = phase,
      }
    }

  def safety: bool = all {
    dfs_posix_fsync::directoryRevisionAtLeast(
      dfs_posix_fsync::directoryVisibleRevision,
      dfs_posix_fsync::directoryDurableRevision
    ),
    dfs_posix_fsync::directoryDurableRevision
      == dfs_posix_fsync::directoryCertifiedRevision,
    dfs_posix_fsync::canonicalVersion == dfs_posix_fsync::BaseVersion,
    match phase {
      | Start => all {
          dfs_posix_fsync::directoryVisibleRevision
            == dfs_posix_fsync::DirectoryBase,
          dfs_posix_fsync::directoryDurableRevision
            == dfs_posix_fsync::DirectoryBase,
        }
      | CreateVisible => all {
          dfs_posix_fsync::directoryVisibleRevision
            == dfs_posix_fsync::EntryCreated,
          dfs_posix_fsync::directoryDurableRevision
            == dfs_posix_fsync::DirectoryBase,
        }
      | FirstSyncPending =>
          match dfs_posix_fsync::syncRecords.get(dfs_posix_fsync::Sync1) {
            | Some(record) =>
                record.payload.targetDirectoryRevision
                  == dfs_posix_fsync::EntryCreated
            | None => false
          }
      | RenameVisible => all {
          dfs_posix_fsync::directoryVisibleRevision
            == dfs_posix_fsync::EntryRenamed,
          dfs_posix_fsync::directoryDurableRevision
            == dfs_posix_fsync::DirectoryBase,
        }
      | FirstPrepareQuorum =>
          match dfs_posix_fsync::syncRecords.get(dfs_posix_fsync::Sync1) {
            | Some(record) => hasQuorum(record.prepareVoters)
            | None => false
          }
      | CreateDurable => all {
          dfs_posix_fsync::directoryVisibleRevision
            == dfs_posix_fsync::EntryRenamed,
          dfs_posix_fsync::directoryDurableRevision
            == dfs_posix_fsync::EntryCreated,
          dfs_posix_fsync::directoryCertifiedRevision
            == dfs_posix_fsync::EntryCreated,
        }
      | SecondSyncPending =>
          match dfs_posix_fsync::syncRecords.get(dfs_posix_fsync::Sync2) {
            | Some(record) =>
                record.payload.targetDirectoryRevision
                  == dfs_posix_fsync::EntryRenamed
            | None => false
          }
      | SecondPrepareQuorum =>
          match dfs_posix_fsync::syncRecords.get(dfs_posix_fsync::Sync2) {
            | Some(record) => hasQuorum(record.prepareVoters)
            | None => false
          }
      | RenameDurable => all {
          dfs_posix_fsync::directoryDurableRevision
            == dfs_posix_fsync::EntryRenamed,
          dfs_posix_fsync::directoryCertifiedRevision
            == dfs_posix_fsync::EntryRenamed,
          dfs_posix_fsync::canonicalVersion == dfs_posix_fsync::BaseVersion,
        }
    },
  }
}
