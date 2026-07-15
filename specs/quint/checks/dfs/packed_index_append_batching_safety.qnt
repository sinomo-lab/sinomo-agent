/// Safety and bounded reachability for byte-identical packed-index batching.
module dfs_packed_index_append_batching_safety {
  import dfs_packed_index_append_batching.* from "../../models/dfs/packed_index_append_batching"

  def countersStayBounded: bool = all {
    packedIndexAppend.crashCut >= 0,
    packedIndexAppend.crashCut <= FullStreamBytes,
    packedIndexAppend.legacySerializedBytes >= 0,
    packedIndexAppend.legacySerializedBytes <= FullStreamBytes,
    packedIndexAppend.batchedSerializedBytes >= 0,
    packedIndexAppend.batchedSerializedBytes <= FullStreamBytes,
    packedIndexAppend.legacyDurableBytes >= 0,
    packedIndexAppend.legacyDurableBytes <= FullStreamBytes,
    packedIndexAppend.batchedDurableBytes >= 0,
    packedIndexAppend.batchedDurableBytes <= FullStreamBytes,
    packedIndexAppend.legacyRecoveredRecords >= 0,
    packedIndexAppend.legacyRecoveredRecords <= RecordCount,
    packedIndexAppend.batchedRecoveredRecords >= 0,
    packedIndexAppend.batchedRecoveredRecords <= RecordCount,
    packedIndexAppend.legacyTruncatedBytes >= 0,
    packedIndexAppend.legacyTruncatedBytes <= FullStreamBytes,
    packedIndexAppend.batchedTruncatedBytes >= 0,
    packedIndexAppend.batchedTruncatedBytes <= FullStreamBytes,
  }

  /// The in-memory optimization preserves the exact four-byte length fields,
  /// record bodies, order, and total byte count.
  def serializedByteStreamIsIdentical: bool = all {
    BytePositions.forall(position =>
      legacyByteAt(position) == batchedByteAt(position)
    ),
    packedIndexAppend.phase != AppendReady implies all {
      packedIndexAppend.legacySerializedBytes == FullStreamBytes,
      packedIndexAppend.batchedSerializedBytes == FullStreamBytes,
    },
  }

  /// Coupled reference and optimized executions expose the same durable prefix
  /// at every selected cut; no atomicity assumption is introduced by batching.
  def everyCrashCutPreservesTheSamePrefix: bool =
    packedIndexAppend.phase != AppendReady implies all {
      CrashCuts.contains(packedIndexAppend.crashCut),
      packedIndexAppend.legacyDurableBytes == packedIndexAppend.crashCut,
      packedIndexAppend.batchedDurableBytes == packedIndexAppend.crashCut,
      packedIndexAppend.legacyDurableBytes
        == packedIndexAppend.batchedDurableBytes,
      BytePositions
        .filter(position => position < packedIndexAppend.crashCut)
        .forall(position => legacyByteAt(position) == batchedByteAt(position)),
    }

  /// Replay never accepts an incomplete length or body and truncates only the
  /// final partial record. The retained prefix is exactly zero, one, or two
  /// complete records.
  def recoveryKeepsOnlyCompleteCommittedRecords: bool =
    packedIndexAppend.phase == PrefixRecovered implies all {
      packedIndexAppend.legacyRecoveredRecords
        == recoveredRecordCount(packedIndexAppend.legacyDurableBytes),
      packedIndexAppend.batchedRecoveredRecords
        == recoveredRecordCount(packedIndexAppend.batchedDurableBytes),
      packedIndexAppend.legacyTruncatedBytes
        == packedIndexAppend.legacyRecoveredRecords * RecordBytes,
      packedIndexAppend.batchedTruncatedBytes
        == packedIndexAppend.batchedRecoveredRecords * RecordBytes,
      packedIndexAppend.legacyTruncatedBytes
        <= packedIndexAppend.legacyDurableBytes,
      packedIndexAppend.batchedTruncatedBytes
        <= packedIndexAppend.batchedDurableBytes,
      packedIndexAppend.legacyDurableBytes
          - packedIndexAppend.legacyTruncatedBytes
        < RecordBytes,
      packedIndexAppend.batchedDurableBytes
          - packedIndexAppend.batchedTruncatedBytes
        < RecordBytes,
    }

  /// Equal byte cuts yield equal committed record sets and truncation offsets.
  /// This is the reader-visible refinement relation between the two writers.
  def committedPrefixRecoveryIsEquivalent: bool =
    packedIndexAppend.phase == PrefixRecovered implies all {
      packedIndexAppend.legacyRecoveredRecords
        == packedIndexAppend.batchedRecoveredRecords,
      packedIndexAppend.legacyTruncatedBytes
        == packedIndexAppend.batchedTruncatedBytes,
      packedIndexAppend.legacyTruncatedBytes
        == recoveredPrefixBytes(packedIndexAppend.crashCut),
    }

  /// The optimization removes logical append calls without treating the single
  /// `write_all` as one guaranteed kernel syscall.
  def batchingDoesNotIncreaseLogicalAppendCalls: bool =
    packedIndexAppend.phase != AppendReady implies all {
      packedIndexAppend.legacyLogicalAppendCalls
        == legacyAppendCallsForCut(packedIndexAppend.crashCut),
      packedIndexAppend.batchedLogicalAppendCalls
        == batchedAppendCallsForCut(packedIndexAppend.crashCut),
      packedIndexAppend.batchedLogicalAppendCalls
        <= packedIndexAppend.legacyLogicalAppendCalls,
      packedIndexAppend.crashCut == FullStreamBytes implies all {
        packedIndexAppend.legacyLogicalAppendCalls == 4,
        packedIndexAppend.batchedLogicalAppendCalls == 1,
      },
    }

  def witnessHealthyFullAppendIsByteIdentical: bool = all {
    packedIndexAppend.lastTransition == DurablePrefixRecovered,
    packedIndexAppend.crashCut == FullStreamBytes,
    packedIndexAppend.legacyRecoveredRecords == 2,
    packedIndexAppend.batchedRecoveredRecords == 2,
    packedIndexAppend.legacyTruncatedBytes == FullStreamBytes,
    packedIndexAppend.batchedTruncatedBytes == FullStreamBytes,
    packedIndexAppend.legacyLogicalAppendCalls == 4,
    packedIndexAppend.batchedLogicalAppendCalls == 1,
    serializedByteStreamIsIdentical,
    committedPrefixRecoveryIsEquivalent,
  }

  def witnessTornFirstLengthRecoversNothing: bool = all {
    packedIndexAppend.lastTransition == DurablePrefixRecovered,
    packedIndexAppend.crashCut == 2,
    packedIndexAppend.legacyRecoveredRecords == 0,
    packedIndexAppend.batchedRecoveredRecords == 0,
    packedIndexAppend.legacyTruncatedBytes == 0,
    packedIndexAppend.batchedTruncatedBytes == 0,
  }

  def witnessTornFirstBodyRecoversNothing: bool = all {
    packedIndexAppend.lastTransition == DurablePrefixRecovered,
    packedIndexAppend.crashCut == 5,
    packedIndexAppend.legacyRecoveredRecords == 0,
    packedIndexAppend.batchedRecoveredRecords == 0,
    packedIndexAppend.legacyTruncatedBytes == 0,
    packedIndexAppend.batchedTruncatedBytes == 0,
  }

  def witnessExactFirstRecordBoundaryIsCommitted: bool = all {
    packedIndexAppend.lastTransition == DurablePrefixRecovered,
    packedIndexAppend.crashCut == RecordBytes,
    packedIndexAppend.legacyRecoveredRecords == 1,
    packedIndexAppend.batchedRecoveredRecords == 1,
    packedIndexAppend.legacyTruncatedBytes == RecordBytes,
    packedIndexAppend.batchedTruncatedBytes == RecordBytes,
  }

  def witnessTornSecondLengthKeepsFirstRecord: bool = all {
    packedIndexAppend.lastTransition == DurablePrefixRecovered,
    packedIndexAppend.crashCut == 8,
    packedIndexAppend.legacyRecoveredRecords == 1,
    packedIndexAppend.batchedRecoveredRecords == 1,
    packedIndexAppend.legacyTruncatedBytes == RecordBytes,
    packedIndexAppend.batchedTruncatedBytes == RecordBytes,
  }

  def witnessTornSecondBodyKeepsFirstRecord: bool = all {
    packedIndexAppend.lastTransition == DurablePrefixRecovered,
    packedIndexAppend.crashCut == 11,
    packedIndexAppend.legacyRecoveredRecords == 1,
    packedIndexAppend.batchedRecoveredRecords == 1,
    packedIndexAppend.legacyTruncatedBytes == RecordBytes,
    packedIndexAppend.batchedTruncatedBytes == RecordBytes,
  }

  def safety: bool = all {
    countersStayBounded,
    serializedByteStreamIsIdentical,
    everyCrashCutPreservesTheSamePrefix,
    recoveryKeepsOnlyCompleteCommittedRecords,
    committedPrefixRecoveryIsEquivalent,
    batchingDoesNotIncreaseLogicalAppendCalls,
  }
}
