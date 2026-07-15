/// Safety and bounded convergence for replica ingest on a retired primary.
module dfs_metadata_retiring_lane_replica_ingest_safety {
  import dfs_metadata_retiring_lane_replica_ingest.* from "../../models/dfs/metadata_retiring_lane_replica_ingest"

  def activatedRetiringLaneRemainsAuthorFenced: bool = all {
    retiringLane.handoffActivated,
    retiringLane.retiringFileSystem == TargetFileSystem,
    retiringLane.retiringShard == TargetShard,
    retiringLane.retiringNode == RetiringNode,
    retiringLane.retiringEpoch == RetiringEpoch,
    retiringLane.markerSuccessor == MarkerSuccessor,
    retiringLane.markerEpoch == MarkerEpoch,
    retiringLane.activeSuccessor == ActiveSuccessor,
    retiringLane.activeEpoch == ActiveEpoch,
    retiringLane.markerEpoch <= retiringLane.activeEpoch,
    retiringLane.activeEpochCommitted,
    retiringLane.retiringConfiguredReplica,
    retiringLane.retiringAuthorFenced,
    not(retiringLane.retiringAuthorBound),
    not(retiringLane.authorWriteAccepted),
  }

  def retiringAuthorAttemptCannotAdvancePrefix: bool =
    retiringLane.lastTransition == RetiringAuthorWriteAttemptRejected implies all {
      retiringLane.authorWriteOutcome == AuthorWriteRejected,
      not(retiringLane.authorWriteAccepted),
      retiringLane.retiringSequence == retiringLane.authorSequenceBeforeAttempt,
      retiringLane.retiringAuthorFenced,
      not(retiringLane.retiringAuthorBound),
    }

  def acceptedReplicaBatchRequiresExactSuccessorAndContiguity: bool =
    retiringLane.lastTransition == ReplicaBatchAccepted implies all {
      retiringLane.lastIngestKind == JournalBatchIngest,
      retiringLane.lastIngestOutcome == IngestAccepted,
      retiringLane.lastPermitFileSystem == retiringLane.retiringFileSystem,
      retiringLane.lastPermitShard == retiringLane.retiringShard,
      retiringLane.lastPayloadFileSystems == Set(retiringLane.retiringFileSystem),
      retiringLane.lastMarkerEvidence == MarkerActivated,
      retiringLane.lastReplicaMembershipEvidence == ReplicaConfigured,
      retiringLane.lastSource == ActiveSuccessor,
      retiringLane.lastSourceEpoch == ActiveEpoch,
      retiringLane.lastBatchBegin == retiringLane.replicaSequenceBeforeIngest + 1,
      retiringLane.lastBatchEnd >= retiringLane.lastBatchBegin,
      retiringLane.lastBatchEnd <= retiringLane.successorSequence,
      retiringLane.retiringSequence == retiringLane.lastBatchEnd,
      retiringLane.retiringAuthorFenced,
      not(retiringLane.retiringAuthorBound),
    }

  def wrongFileSystemReplicaBatchCannotAdvancePrefix: bool =
    all {
      retiringLane.lastIngestKind == JournalBatchIngest,
      any {
        retiringLane.lastPermitFileSystem != retiringLane.retiringFileSystem,
        retiringLane.lastPayloadFileSystems != Set(retiringLane.retiringFileSystem),
      },
    } implies all {
      retiringLane.lastIngestOutcome == IngestRejected,
      retiringLane.retiringSequence == retiringLane.replicaSequenceBeforeIngest,
      retiringLane.retiringAuthorFenced,
      not(retiringLane.retiringAuthorBound),
    }

  def wrongShardReplicaBatchCannotAdvancePrefix: bool =
    all {
      retiringLane.lastIngestKind == JournalBatchIngest,
      retiringLane.lastPermitShard != retiringLane.retiringShard,
    } implies all {
      retiringLane.lastIngestOutcome == IngestRejected,
      retiringLane.retiringSequence == retiringLane.replicaSequenceBeforeIngest,
      retiringLane.retiringAuthorFenced,
      not(retiringLane.retiringAuthorBound),
    }

  def rejectedReplicaBatchCannotAdvancePrefix: bool =
    retiringLane.lastTransition == ReplicaBatchRejected implies all {
      retiringLane.lastIngestKind == JournalBatchIngest,
      retiringLane.lastIngestOutcome == IngestRejected,
      retiringLane.retiringSequence == retiringLane.replicaSequenceBeforeIngest,
      retiringLane.retiringAuthorFenced,
      not(retiringLane.retiringAuthorBound),
    }

  def invalidMarkerOrMembershipBatchCannotAdvancePrefix: bool =
    all {
      retiringLane.lastIngestKind == JournalBatchIngest,
      any {
        retiringLane.lastMarkerEvidence != MarkerActivated,
        retiringLane.lastReplicaMembershipEvidence != ReplicaConfigured,
      },
    } implies all {
      retiringLane.lastIngestOutcome == IngestRejected,
      retiringLane.retiringSequence == retiringLane.replicaSequenceBeforeIngest,
      retiringLane.retiringAuthorFenced,
      not(retiringLane.retiringAuthorBound),
    }

  def acceptedSnapshotRequiresExactSuccessorAndAuthoritativeState: bool =
    retiringLane.lastTransition == AuthoritativeSnapshotAccepted implies all {
      retiringLane.lastIngestKind == SnapshotIngest,
      retiringLane.lastIngestOutcome == IngestAccepted,
      retiringLane.lastPermitFileSystem == retiringLane.retiringFileSystem,
      retiringLane.lastPermitShard == retiringLane.retiringShard,
      retiringLane.lastPayloadFileSystems == Set(retiringLane.retiringFileSystem),
      retiringLane.lastMarkerEvidence == MarkerActivated,
      retiringLane.lastReplicaMembershipEvidence == ReplicaConfigured,
      retiringLane.lastSource == ActiveSuccessor,
      retiringLane.lastSourceEpoch == ActiveEpoch,
      retiringLane.lastSnapshotEvidence == SnapshotExactAuthoritative,
      retiringLane.lastSnapshotSequence == retiringLane.successorSequence,
      retiringLane.retiringSequence == retiringLane.lastSnapshotSequence,
      retiringLane.retiringAuthorFenced,
      not(retiringLane.retiringAuthorBound),
    }

  def wrongFileSystemSnapshotCannotAdvanceOrBindAuthority: bool =
    all {
      retiringLane.lastIngestKind == SnapshotIngest,
      any {
        retiringLane.lastPermitFileSystem != retiringLane.retiringFileSystem,
        retiringLane.lastPayloadFileSystems != Set(retiringLane.retiringFileSystem),
      },
    } implies all {
      retiringLane.lastIngestOutcome == IngestRejected,
      retiringLane.retiringSequence == retiringLane.replicaSequenceBeforeIngest,
      retiringLane.retiringAuthorFenced,
      not(retiringLane.retiringAuthorBound),
    }

  def wrongShardSnapshotCannotAdvanceOrBindAuthority: bool =
    all {
      retiringLane.lastIngestKind == SnapshotIngest,
      retiringLane.lastPermitShard != retiringLane.retiringShard,
    } implies all {
      retiringLane.lastIngestOutcome == IngestRejected,
      retiringLane.retiringSequence == retiringLane.replicaSequenceBeforeIngest,
      retiringLane.retiringAuthorFenced,
      not(retiringLane.retiringAuthorBound),
    }

  def rejectedSnapshotCannotAdvanceOrBindAuthority: bool =
    retiringLane.lastTransition == AuthoritativeSnapshotRejected implies all {
      retiringLane.lastIngestKind == SnapshotIngest,
      retiringLane.lastIngestOutcome == IngestRejected,
      retiringLane.retiringSequence == retiringLane.replicaSequenceBeforeIngest,
      retiringLane.retiringAuthorFenced,
      not(retiringLane.retiringAuthorBound),
    }

  def invalidMarkerOrMembershipSnapshotCannotAdvanceOrBindAuthority: bool =
    all {
      retiringLane.lastIngestKind == SnapshotIngest,
      any {
        retiringLane.lastMarkerEvidence != MarkerActivated,
        retiringLane.lastReplicaMembershipEvidence != ReplicaConfigured,
      },
    } implies all {
      retiringLane.lastIngestOutcome == IngestRejected,
      retiringLane.retiringSequence == retiringLane.replicaSequenceBeforeIngest,
      retiringLane.retiringAuthorFenced,
      not(retiringLane.retiringAuthorBound),
    }

  def retiringReplicaNeverOutrunsActiveSuccessor: bool =
    retiringLane.retiringSequence <= retiringLane.successorSequence

  def witnessRetiringAuthorWriteRejected: bool = all {
    retiringLane.lastTransition == RetiringAuthorWriteAttemptRejected,
    retiringAuthorAttemptCannotAdvancePrefix,
  }

  /// The supplied batch is otherwise a valid contiguous suffix; only N1's
  /// forged source identity prevents admission.
  def witnessForgedSourceReplicationRejected: bool = all {
    retiringLane.lastTransition == ReplicaBatchRejected,
    retiringLane.lastPermitFileSystem == TargetFileSystem,
    retiringLane.lastPayloadFileSystems == TargetPayloadFileSystems,
    retiringLane.lastMarkerEvidence == MarkerActivated,
    retiringLane.lastReplicaMembershipEvidence == ReplicaConfigured,
    retiringLane.lastSource == ForgedSource,
    retiringLane.lastSourceEpoch == ActiveEpoch,
    retiringLane.lastBatchBegin == retiringLane.replicaSequenceBeforeIngest + 1,
    retiringLane.lastBatchEnd == retiringLane.successorSequence,
    retiringLane.lastBatchEnd >= retiringLane.lastBatchBegin,
    rejectedReplicaBatchCannotAdvancePrefix,
  }

  /// N3 is the correct node, but epoch 3 belongs to the durable marker rather
  /// than the current committed epoch-4 assignment.
  def witnessStaleSuccessorReplicationRejected: bool = all {
    retiringLane.lastTransition == ReplicaBatchRejected,
    retiringLane.lastPermitFileSystem == TargetFileSystem,
    retiringLane.lastPayloadFileSystems == TargetPayloadFileSystems,
    retiringLane.lastMarkerEvidence == MarkerActivated,
    retiringLane.lastReplicaMembershipEvidence == ReplicaConfigured,
    retiringLane.lastSource == ActiveSuccessor,
    retiringLane.lastSourceEpoch == MarkerEpoch,
    retiringLane.lastBatchBegin == retiringLane.replicaSequenceBeforeIngest + 1,
    retiringLane.lastBatchEnd == retiringLane.successorSequence,
    retiringLane.lastBatchEnd >= retiringLane.lastBatchBegin,
    rejectedReplicaBatchCannotAdvancePrefix,
  }

  def witnessRetiringEpochRollbackBatchRejected: bool = all {
    retiringLane.lastTransition == ReplicaBatchRejected,
    retiringLane.lastPermitFileSystem == TargetFileSystem,
    retiringLane.lastPermitShard == TargetShard,
    retiringLane.lastPayloadFileSystems == TargetPayloadFileSystems,
    retiringLane.lastMarkerEvidence == MarkerActivated,
    retiringLane.lastReplicaMembershipEvidence == ReplicaConfigured,
    retiringLane.lastSource == ActiveSuccessor,
    retiringLane.lastSourceEpoch == RetiringEpoch,
    retiringLane.lastSourceEpoch < retiringLane.markerEpoch,
    retiringLane.lastBatchBegin == retiringLane.replicaSequenceBeforeIngest + 1,
    retiringLane.lastBatchEnd == retiringLane.successorSequence,
    retiringLane.lastBatchEnd >= retiringLane.lastBatchBegin,
    rejectedReplicaBatchCannotAdvancePrefix,
  }

  def witnessFutureUncommittedEpochReplicationRejected: bool = all {
    retiringLane.lastTransition == ReplicaBatchRejected,
    retiringLane.lastPermitFileSystem == TargetFileSystem,
    retiringLane.lastPayloadFileSystems == TargetPayloadFileSystems,
    retiringLane.lastMarkerEvidence == MarkerActivated,
    retiringLane.lastReplicaMembershipEvidence == ReplicaConfigured,
    retiringLane.lastSource == ActiveSuccessor,
    retiringLane.lastSourceEpoch == FutureUncommittedEpoch,
    retiringLane.lastSourceEpoch > retiringLane.activeEpoch,
    retiringLane.lastBatchBegin == retiringLane.replicaSequenceBeforeIngest + 1,
    retiringLane.lastBatchEnd == retiringLane.successorSequence,
    retiringLane.lastBatchEnd >= retiringLane.lastBatchBegin,
    rejectedReplicaBatchCannotAdvancePrefix,
  }

  def witnessPendingMarkerBatchRejected: bool = all {
    retiringLane.lastTransition == ReplicaBatchRejected,
    retiringLane.lastPermitFileSystem == TargetFileSystem,
    retiringLane.lastPayloadFileSystems == TargetPayloadFileSystems,
    retiringLane.lastMarkerEvidence == MarkerPending,
    retiringLane.lastReplicaMembershipEvidence == ReplicaConfigured,
    retiringLane.lastSource == ActiveSuccessor,
    retiringLane.lastSourceEpoch == ActiveEpoch,
    retiringLane.lastBatchBegin == retiringLane.replicaSequenceBeforeIngest + 1,
    retiringLane.lastBatchEnd == retiringLane.successorSequence,
    retiringLane.lastBatchEnd >= retiringLane.lastBatchBegin,
    invalidMarkerOrMembershipBatchCannotAdvancePrefix,
  }

  def witnessRemovedReplicaBatchRejected: bool = all {
    retiringLane.lastTransition == ReplicaBatchRejected,
    retiringLane.lastPermitFileSystem == TargetFileSystem,
    retiringLane.lastPayloadFileSystems == TargetPayloadFileSystems,
    retiringLane.lastMarkerEvidence == MarkerActivated,
    retiringLane.lastReplicaMembershipEvidence == ReplicaRemoved,
    retiringLane.lastSource == ActiveSuccessor,
    retiringLane.lastSourceEpoch == ActiveEpoch,
    retiringLane.lastBatchBegin == retiringLane.replicaSequenceBeforeIngest + 1,
    retiringLane.lastBatchEnd == retiringLane.successorSequence,
    retiringLane.lastBatchEnd >= retiringLane.lastBatchBegin,
    invalidMarkerOrMembershipBatchCannotAdvancePrefix,
  }

  def witnessForeignShardBatchRejected: bool = all {
    retiringLane.lastTransition == ReplicaBatchRejected,
    retiringLane.lastPermitFileSystem == TargetFileSystem,
    retiringLane.lastPermitShard == ForeignShard,
    retiringLane.lastPayloadFileSystems == TargetPayloadFileSystems,
    retiringLane.lastMarkerEvidence == MarkerActivated,
    retiringLane.lastReplicaMembershipEvidence == ReplicaConfigured,
    retiringLane.lastSource == ActiveSuccessor,
    retiringLane.lastSourceEpoch == ActiveEpoch,
    retiringLane.lastBatchBegin == retiringLane.replicaSequenceBeforeIngest + 1,
    retiringLane.lastBatchEnd == retiringLane.successorSequence,
    retiringLane.lastBatchEnd >= retiringLane.lastBatchBegin,
    wrongShardReplicaBatchCannotAdvancePrefix,
  }

  def witnessNoncontiguousReplicaBatchRejected: bool = all {
    retiringLane.lastTransition == ReplicaBatchRejected,
    retiringLane.lastPermitFileSystem == TargetFileSystem,
    retiringLane.lastPayloadFileSystems == TargetPayloadFileSystems,
    retiringLane.lastMarkerEvidence == MarkerActivated,
    retiringLane.lastReplicaMembershipEvidence == ReplicaConfigured,
    retiringLane.lastSource == ActiveSuccessor,
    retiringLane.lastSourceEpoch == ActiveEpoch,
    retiringLane.lastBatchBegin == retiringLane.replicaSequenceBeforeIngest + 2,
    retiringLane.lastBatchEnd == retiringLane.successorSequence,
    rejectedReplicaBatchCannotAdvancePrefix,
  }

  /// Source, epoch, suffix, and payload agree with the foreign permit, but the
  /// permit cannot authorize writes into the target filesystem's lane.
  def witnessForeignFileSystemPermitBatchRejected: bool = all {
    retiringLane.lastTransition == ReplicaBatchRejected,
    retiringLane.lastPermitFileSystem == ForeignFileSystem,
    retiringLane.lastPayloadFileSystems == ForeignPayloadFileSystems,
    retiringLane.lastMarkerEvidence == MarkerActivated,
    retiringLane.lastReplicaMembershipEvidence == ReplicaConfigured,
    retiringLane.lastSource == ActiveSuccessor,
    retiringLane.lastSourceEpoch == ActiveEpoch,
    retiringLane.lastBatchBegin == retiringLane.replicaSequenceBeforeIngest + 1,
    retiringLane.lastBatchEnd == retiringLane.successorSequence,
    retiringLane.lastBatchEnd >= retiringLane.lastBatchBegin,
    wrongFileSystemReplicaBatchCannotAdvancePrefix,
  }

  /// The permit, source, epoch, and suffix are exact. The payload nevertheless
  /// contains a record for another filesystem, so no part of the batch lands.
  def witnessMixedFileSystemReplicaBatchRejected: bool = all {
    retiringLane.lastTransition == ReplicaBatchRejected,
    retiringLane.lastPermitFileSystem == TargetFileSystem,
    retiringLane.lastPayloadFileSystems == MixedPayloadFileSystems,
    retiringLane.lastMarkerEvidence == MarkerActivated,
    retiringLane.lastReplicaMembershipEvidence == ReplicaConfigured,
    retiringLane.lastSource == ActiveSuccessor,
    retiringLane.lastSourceEpoch == ActiveEpoch,
    retiringLane.lastBatchBegin == retiringLane.replicaSequenceBeforeIngest + 1,
    retiringLane.lastBatchEnd == retiringLane.successorSequence,
    retiringLane.lastBatchEnd >= retiringLane.lastBatchBegin,
    wrongFileSystemReplicaBatchCannotAdvancePrefix,
  }

  def witnessExactSuccessorReplicationAcceptedWhileAuthorFenced: bool = all {
    retiringLane.lastTransition == ReplicaBatchAccepted,
    retiringLane.markerEpoch == MarkerEpoch,
    retiringLane.activeEpoch == ActiveEpoch,
    retiringLane.markerEpoch <= retiringLane.activeEpoch,
    retiringLane.retiringConfiguredReplica,
    acceptedReplicaBatchRequiresExactSuccessorAndContiguity,
    retiringLane.retiringAuthorFenced,
    not(retiringLane.retiringAuthorBound),
  }

  def witnessConvergenceAfterSuccessorAppend: bool = all {
    retiringLane.lastTransition == ReplicaBatchAccepted,
    retiringLane.successorAppendCommitted,
    retiringLane.retiringSequence == retiringLane.successorSequence,
    acceptedReplicaBatchRequiresExactSuccessorAndContiguity,
  }

  /// The source is exact and the advertised height is current, leaving the
  /// arbitrary snapshot evidence as the only rejection reason.
  def witnessArbitrarySnapshotRejected: bool = all {
    retiringLane.lastTransition == AuthoritativeSnapshotRejected,
    retiringLane.lastPermitFileSystem == TargetFileSystem,
    retiringLane.lastPayloadFileSystems == TargetPayloadFileSystems,
    retiringLane.lastMarkerEvidence == MarkerActivated,
    retiringLane.lastReplicaMembershipEvidence == ReplicaConfigured,
    retiringLane.lastSource == ActiveSuccessor,
    retiringLane.lastSourceEpoch == ActiveEpoch,
    retiringLane.lastSnapshotEvidence == SnapshotArbitrary,
    retiringLane.lastSnapshotSequence == retiringLane.successorSequence,
    rejectedSnapshotCannotAdvanceOrBindAuthority,
  }

  def witnessForgedSnapshotRejected: bool = all {
    retiringLane.lastTransition == AuthoritativeSnapshotRejected,
    retiringLane.lastPermitFileSystem == TargetFileSystem,
    retiringLane.lastPayloadFileSystems == TargetPayloadFileSystems,
    retiringLane.lastMarkerEvidence == MarkerActivated,
    retiringLane.lastReplicaMembershipEvidence == ReplicaConfigured,
    retiringLane.lastSource == ForgedSource,
    retiringLane.lastSourceEpoch == ActiveEpoch,
    retiringLane.lastSnapshotEvidence == SnapshotExactAuthoritative,
    retiringLane.lastSnapshotSequence == retiringLane.successorSequence,
    rejectedSnapshotCannotAdvanceOrBindAuthority,
  }

  def witnessStaleEpochSnapshotRejected: bool = all {
    retiringLane.lastTransition == AuthoritativeSnapshotRejected,
    retiringLane.lastPermitFileSystem == TargetFileSystem,
    retiringLane.lastPayloadFileSystems == TargetPayloadFileSystems,
    retiringLane.lastMarkerEvidence == MarkerActivated,
    retiringLane.lastReplicaMembershipEvidence == ReplicaConfigured,
    retiringLane.lastSource == ActiveSuccessor,
    retiringLane.lastSourceEpoch == MarkerEpoch,
    retiringLane.lastSnapshotEvidence == SnapshotExactAuthoritative,
    retiringLane.lastSnapshotSequence == retiringLane.successorSequence,
    rejectedSnapshotCannotAdvanceOrBindAuthority,
  }

  def witnessRetiringEpochRollbackSnapshotRejected: bool = all {
    retiringLane.lastTransition == AuthoritativeSnapshotRejected,
    retiringLane.lastPermitFileSystem == TargetFileSystem,
    retiringLane.lastPermitShard == TargetShard,
    retiringLane.lastPayloadFileSystems == TargetPayloadFileSystems,
    retiringLane.lastMarkerEvidence == MarkerActivated,
    retiringLane.lastReplicaMembershipEvidence == ReplicaConfigured,
    retiringLane.lastSource == ActiveSuccessor,
    retiringLane.lastSourceEpoch == RetiringEpoch,
    retiringLane.lastSourceEpoch < retiringLane.markerEpoch,
    retiringLane.lastSnapshotEvidence == SnapshotExactAuthoritative,
    retiringLane.lastSnapshotSequence == retiringLane.successorSequence,
    rejectedSnapshotCannotAdvanceOrBindAuthority,
  }

  def witnessFutureUncommittedEpochSnapshotRejected: bool = all {
    retiringLane.lastTransition == AuthoritativeSnapshotRejected,
    retiringLane.lastPermitFileSystem == TargetFileSystem,
    retiringLane.lastPayloadFileSystems == TargetPayloadFileSystems,
    retiringLane.lastMarkerEvidence == MarkerActivated,
    retiringLane.lastReplicaMembershipEvidence == ReplicaConfigured,
    retiringLane.lastSource == ActiveSuccessor,
    retiringLane.lastSourceEpoch == FutureUncommittedEpoch,
    retiringLane.lastSourceEpoch > retiringLane.activeEpoch,
    retiringLane.lastSnapshotEvidence == SnapshotExactAuthoritative,
    retiringLane.lastSnapshotSequence == retiringLane.successorSequence,
    rejectedSnapshotCannotAdvanceOrBindAuthority,
  }

  def witnessPendingMarkerSnapshotRejected: bool = all {
    retiringLane.lastTransition == AuthoritativeSnapshotRejected,
    retiringLane.lastPermitFileSystem == TargetFileSystem,
    retiringLane.lastPayloadFileSystems == TargetPayloadFileSystems,
    retiringLane.lastMarkerEvidence == MarkerPending,
    retiringLane.lastReplicaMembershipEvidence == ReplicaConfigured,
    retiringLane.lastSource == ActiveSuccessor,
    retiringLane.lastSourceEpoch == ActiveEpoch,
    retiringLane.lastSnapshotEvidence == SnapshotExactAuthoritative,
    retiringLane.lastSnapshotSequence == retiringLane.successorSequence,
    invalidMarkerOrMembershipSnapshotCannotAdvanceOrBindAuthority,
  }

  def witnessRemovedReplicaSnapshotRejected: bool = all {
    retiringLane.lastTransition == AuthoritativeSnapshotRejected,
    retiringLane.lastPermitFileSystem == TargetFileSystem,
    retiringLane.lastPayloadFileSystems == TargetPayloadFileSystems,
    retiringLane.lastMarkerEvidence == MarkerActivated,
    retiringLane.lastReplicaMembershipEvidence == ReplicaRemoved,
    retiringLane.lastSource == ActiveSuccessor,
    retiringLane.lastSourceEpoch == ActiveEpoch,
    retiringLane.lastSnapshotEvidence == SnapshotExactAuthoritative,
    retiringLane.lastSnapshotSequence == retiringLane.successorSequence,
    invalidMarkerOrMembershipSnapshotCannotAdvanceOrBindAuthority,
  }

  def witnessForeignShardSnapshotRejected: bool = all {
    retiringLane.lastTransition == AuthoritativeSnapshotRejected,
    retiringLane.lastPermitFileSystem == TargetFileSystem,
    retiringLane.lastPermitShard == ForeignShard,
    retiringLane.lastPayloadFileSystems == TargetPayloadFileSystems,
    retiringLane.lastMarkerEvidence == MarkerActivated,
    retiringLane.lastReplicaMembershipEvidence == ReplicaConfigured,
    retiringLane.lastSource == ActiveSuccessor,
    retiringLane.lastSourceEpoch == ActiveEpoch,
    retiringLane.lastSnapshotEvidence == SnapshotExactAuthoritative,
    retiringLane.lastSnapshotSequence == retiringLane.successorSequence,
    wrongShardSnapshotCannotAdvanceOrBindAuthority,
  }

  /// Exact authoritative evidence for another filesystem cannot replace the
  /// target filesystem's retiring replica state.
  def witnessForeignFileSystemPermitSnapshotRejected: bool = all {
    retiringLane.lastTransition == AuthoritativeSnapshotRejected,
    retiringLane.lastPermitFileSystem == ForeignFileSystem,
    retiringLane.lastPayloadFileSystems == ForeignPayloadFileSystems,
    retiringLane.lastMarkerEvidence == MarkerActivated,
    retiringLane.lastReplicaMembershipEvidence == ReplicaConfigured,
    retiringLane.lastSource == ActiveSuccessor,
    retiringLane.lastSourceEpoch == ActiveEpoch,
    retiringLane.lastSnapshotEvidence == SnapshotExactAuthoritative,
    retiringLane.lastSnapshotSequence == retiringLane.successorSequence,
    wrongFileSystemSnapshotCannotAdvanceOrBindAuthority,
  }

  /// The permit and authoritative-state evidence are exact. A foreign
  /// filesystem record family inside the snapshot makes replacement atomic
  /// rejection rather than partial installation.
  def witnessMixedFileSystemSnapshotRejected: bool = all {
    retiringLane.lastTransition == AuthoritativeSnapshotRejected,
    retiringLane.lastPermitFileSystem == TargetFileSystem,
    retiringLane.lastPayloadFileSystems == MixedPayloadFileSystems,
    retiringLane.lastMarkerEvidence == MarkerActivated,
    retiringLane.lastReplicaMembershipEvidence == ReplicaConfigured,
    retiringLane.lastSource == ActiveSuccessor,
    retiringLane.lastSourceEpoch == ActiveEpoch,
    retiringLane.lastSnapshotEvidence == SnapshotExactAuthoritative,
    retiringLane.lastSnapshotSequence == retiringLane.successorSequence,
    wrongFileSystemSnapshotCannotAdvanceOrBindAuthority,
  }

  def witnessExactAuthoritativeSnapshotAcceptedWhileAuthorFenced: bool = all {
    retiringLane.lastTransition == AuthoritativeSnapshotAccepted,
    retiringLane.successorAppendCommitted,
    retiringLane.markerEpoch == MarkerEpoch,
    retiringLane.activeEpoch == ActiveEpoch,
    retiringLane.markerEpoch <= retiringLane.activeEpoch,
    retiringLane.retiringConfiguredReplica,
    retiringLane.retiringSequence == retiringLane.successorSequence,
    acceptedSnapshotRequiresExactSuccessorAndAuthoritativeState,
    retiringLane.retiringAuthorFenced,
    not(retiringLane.retiringAuthorBound),
  }

  def safety: bool = all {
    activatedRetiringLaneRemainsAuthorFenced,
    retiringAuthorAttemptCannotAdvancePrefix,
    acceptedReplicaBatchRequiresExactSuccessorAndContiguity,
    rejectedReplicaBatchCannotAdvancePrefix,
    wrongFileSystemReplicaBatchCannotAdvancePrefix,
    wrongShardReplicaBatchCannotAdvancePrefix,
    invalidMarkerOrMembershipBatchCannotAdvancePrefix,
    acceptedSnapshotRequiresExactSuccessorAndAuthoritativeState,
    rejectedSnapshotCannotAdvanceOrBindAuthority,
    wrongFileSystemSnapshotCannotAdvanceOrBindAuthority,
    wrongShardSnapshotCannotAdvanceOrBindAuthority,
    invalidMarkerOrMembershipSnapshotCannotAdvanceOrBindAuthority,
    retiringReplicaNeverOutrunsActiveSuccessor,
  }
}
