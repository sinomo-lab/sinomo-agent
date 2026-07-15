/// Packed-blob index append batching with byte-identical torn-tail recovery.
///
/// Evidence class: core-algorithm target hardening for replacing the current
/// per-record `write_all(length); write_all(body)` loop with one contiguous
/// `write_all` over the same length-prefixed byte stream.
///
/// The bounded stream contains two records. Each record has the concrete
/// four-byte little-endian length field used by the Rust log and a symbolic
/// two-byte body. Every crash cut from zero through the complete twelve-byte
/// stream is selected nondeterministically. The legacy and batched encoders
/// must expose the same byte at every position, persist the same prefix for the
/// same cut, replay only complete records, and truncate the same torn suffix.
///
/// A `write_all` may itself issue more than one operating-system write after a
/// partial write. This model counts logical append invocations, not kernel
/// syscalls or atomic-write guarantees. Variable record sizes, concurrent
/// appenders, filesystem ordering, sync failure, packed-segment durability,
/// and corruption before the final incomplete record remain outside this
/// bounded refinement.
module dfs_packed_index_append_batching {
  type PackedIndexByte =
    | LengthA0
    | LengthA1
    | LengthA2
    | LengthA3
    | PayloadA0
    | PayloadA1
    | LengthB0
    | LengthB1
    | LengthB2
    | LengthB3
    | PayloadB0
    | PayloadB1

  type PackedIndexAppendPhase =
    | AppendReady
    | DurablePrefixSelected
    | PrefixRecovered

  type PackedIndexAppendTransition =
    | AppendBatchingInitialized
    | SameCrashCutPersisted
    | DurablePrefixRecovered
    | AppendBatchingStuttered

  type PackedIndexAppendState = {
    phase: PackedIndexAppendPhase,
    crashCut: int,
    legacySerializedBytes: int,
    batchedSerializedBytes: int,
    legacyDurableBytes: int,
    batchedDurableBytes: int,
    legacyLogicalAppendCalls: int,
    batchedLogicalAppendCalls: int,
    legacyRecoveredRecords: int,
    batchedRecoveredRecords: int,
    legacyTruncatedBytes: int,
    batchedTruncatedBytes: int,
    lastTransition: PackedIndexAppendTransition,
  }

  pure val LengthFieldBytes: int = 4
  pure val RecordPayloadBytes: int = 2
  pure val RecordBytes: int = LengthFieldBytes + RecordPayloadBytes
  pure val RecordCount: int = 2
  pure val FullStreamBytes: int = RecordBytes * RecordCount
  pure val BytePositions: Set[int] =
    Set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
  pure val CrashCuts: Set[int] =
    Set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)

  /// The reference layout emitted by the existing two-write-per-record loop.
  pure def legacyByteAt(position: int): PackedIndexByte =
    if (position == 0) LengthA0
    else if (position == 1) LengthA1
    else if (position == 2) LengthA2
    else if (position == 3) LengthA3
    else if (position == 4) PayloadA0
    else if (position == 5) PayloadA1
    else if (position == 6) LengthB0
    else if (position == 7) LengthB1
    else if (position == 8) LengthB2
    else if (position == 9) LengthB3
    else if (position == 10) PayloadB0
    else PayloadB1

  /// The optimized encoder first concatenates every prefix and body in memory.
  /// It is deliberately stated independently from `legacyByteAt` so the check
  /// module proves the refinement's byte-for-byte relation explicitly.
  pure def batchedByteAt(position: int): PackedIndexByte =
    if (position == 0) LengthA0
    else if (position == 1) LengthA1
    else if (position == 2) LengthA2
    else if (position == 3) LengthA3
    else if (position == 4) PayloadA0
    else if (position == 5) PayloadA1
    else if (position == 6) LengthB0
    else if (position == 7) LengthB1
    else if (position == 8) LengthB2
    else if (position == 9) LengthB3
    else if (position == 10) PayloadB0
    else PayloadB1

  /// The reader accepts only a complete length field and complete body. Its
  /// returned offset is therefore the longest complete-record prefix.
  pure def recoveredRecordCount(durableBytes: int): int =
    if (durableBytes < RecordBytes) 0
    else if (durableBytes < FullStreamBytes) 1
    else RecordCount

  pure def recoveredPrefixBytes(durableBytes: int): int =
    recoveredRecordCount(durableBytes) * RecordBytes

  /// The legacy implementation starts one logical write for each length and
  /// each body reached by the crash cut.
  pure def legacyAppendCallsForCut(cut: int): int =
    if (cut <= 0) 0
    else if (cut <= LengthFieldBytes) 1
    else if (cut <= RecordBytes) 2
    else if (cut <= RecordBytes + LengthFieldBytes) 3
    else 4

  /// The optimized implementation starts at most one logical contiguous write.
  pure def batchedAppendCallsForCut(cut: int): int =
    if (cut <= 0) 0 else 1

  pure val initialState: PackedIndexAppendState = {
    phase: AppendReady,
    crashCut: 0,
    legacySerializedBytes: 0,
    batchedSerializedBytes: 0,
    legacyDurableBytes: 0,
    batchedDurableBytes: 0,
    legacyLogicalAppendCalls: 0,
    batchedLogicalAppendCalls: 0,
    legacyRecoveredRecords: 0,
    batchedRecoveredRecords: 0,
    legacyTruncatedBytes: 0,
    batchedTruncatedBytes: 0,
    lastTransition: AppendBatchingInitialized,
  }

  var packedIndexAppend: PackedIndexAppendState

  action init = packedIndexAppend' = initialState

  /// Couple the reference and optimized executions at the same durable byte
  /// cut. Since `write_all` is not assumed atomic, every byte prefix is present
  /// in `CrashCuts`, including cuts inside either four-byte length field and
  /// either record body.
  action persistSameCrashCut(cut: int): bool = all {
    packedIndexAppend.phase == AppendReady,
    CrashCuts.contains(cut),
    packedIndexAppend' = packedIndexAppend
      .with("phase", DurablePrefixSelected)
      .with("crashCut", cut)
      .with("legacySerializedBytes", FullStreamBytes)
      .with("batchedSerializedBytes", FullStreamBytes)
      .with("legacyDurableBytes", cut)
      .with("batchedDurableBytes", cut)
      .with("legacyLogicalAppendCalls", legacyAppendCallsForCut(cut))
      .with("batchedLogicalAppendCalls", batchedAppendCallsForCut(cut))
      .with("lastTransition", SameCrashCutPersisted),
  }

  /// Reopen/replay projects both byte-identical prefixes to the longest complete
  /// record boundary and drops any final incomplete length or body bytes.
  action recoverDurablePrefix = all {
    packedIndexAppend.phase == DurablePrefixSelected,
    packedIndexAppend' = packedIndexAppend
      .with("phase", PrefixRecovered)
      .with(
        "legacyRecoveredRecords",
        recoveredRecordCount(packedIndexAppend.legacyDurableBytes)
      )
      .with(
        "batchedRecoveredRecords",
        recoveredRecordCount(packedIndexAppend.batchedDurableBytes)
      )
      .with(
        "legacyTruncatedBytes",
        recoveredPrefixBytes(packedIndexAppend.legacyDurableBytes)
      )
      .with(
        "batchedTruncatedBytes",
        recoveredPrefixBytes(packedIndexAppend.batchedDurableBytes)
      )
      .with("lastTransition", DurablePrefixRecovered),
  }

  action stutter =
    packedIndexAppend' = packedIndexAppend
      .with("lastTransition", AppendBatchingStuttered)

  action step = any {
    {
      nondet cut = oneOf(CrashCuts)
      persistSameCrashCut(cut)
    },
    recoverDurablePrefix,
    stutter,
  }
}
