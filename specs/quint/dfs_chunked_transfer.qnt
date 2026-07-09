/// DFS chunked transfer window and receiver assembly safety.
///
/// This module models the shared chunk-transfer surface used by stage, repair,
/// fetch, and read paths:
/// - outbound windows send a bounded prefix of chunks and report the next chunk
///   index to retry;
/// - receiver-side chunk validation rejects malformed shapes and bad per-chunk
///   hashes;
/// - duplicate chunks are accepted only if their payload is identical;
/// - a durable/committed response can be produced only after all chunks are
///   received and the assembled full-object hash matches the expected hash.
module dfs_chunked_transfer {
  import common.* from "./common"

  type ChunkPayload = CorrectPayload | CorruptPayload
  type ChunkLength = FullLen | TailLen | WrongLen
  type ChunkHashStatus = MissingHash | MatchesPayload | MismatchesPayload
  type TransferPhase = Idle | Receiving | Committed | Rejected
  type ContentHash = ExpectedContentHash | OtherContentHash
  type Decision =
    NoDecision
    | SenderWindowSent
    | SenderWindowStopped
    | SenderWindowComplete
    | PartialAccepted
    | DuplicateAccepted
    | CommitRecorded
    | ShapeRejected
    | ChunkHashRejected
    | ConflictingDuplicateRejected
    | AssembledHashRejected

  pure val ChunkIndexes: Set[int] = Set(0, 1, 2)
  pure val DeclaredTotals: Set[int] = Set(0, 1, 2, 3)
  pure val TotalChunks: Set[int] = Set(1, 2, 3)
  pure val WindowSizes: Set[int] = Set(0, 1, 2, 3)
  pure val ChunkPayloads: Set[ChunkPayload] = Set(CorrectPayload, CorruptPayload)
  pure val ChunkLengths: Set[ChunkLength] = Set(FullLen, TailLen, WrongLen)
  pure val ChunkHashStatuses: Set[ChunkHashStatus] =
    Set(MissingHash, MatchesPayload, MismatchesPayload)
  pure val MaybeFailAt: Set[Option[int]] =
    Set(None, Some(0), Some(1), Some(2))

  var phase: TransferPhase
  var totalChunks: int
  var lastChunkLen: ChunkLength
  var receivedChunks: Set[int]
  var receivedPayloads: int -> Option[ChunkPayload]
  var durableResponseSent: bool
  var durableResponseHash: Option[ContentHash]
  var senderTotalChunks: int
  var senderNextChunkIndex: int
  var senderLastWindow: Set[int]
  var senderComplete: bool
  var senderStopped: bool
  var lastDecision: Decision

  pure val emptyPayloadMap: int -> Option[ChunkPayload] =
    ChunkIndexes.mapBy(_ => None)

  pure def validIndexes(total: int): Set[int] =
    ChunkIndexes.filter(index => 0 <= index and index < total)

  pure def effectiveWindow(maxChunks: int): int =
    if (maxChunks <= 0) { 1 } else { maxChunks }

  pure def windowEnd(startIndex: int, maxChunks: int, total: int): int =
    if (startIndex >= total) {
      total
    } else if (startIndex + effectiveWindow(maxChunks) > total) {
      total
    } else {
      startIndex + effectiveWindow(maxChunks)
    }

  pure def windowIndexes(startIndex: int, endIndex: int): Set[int] =
    ChunkIndexes.filter(index => startIndex <= index and index < endIndex)

  pure def failInsideWindow(failAt: Option[int], startIndex: int, endIndex: int): bool =
    match failAt {
      | Some(index) => startIndex <= index and index < endIndex
      | None => false
    }

  pure def failureIndexOrEnd(failAt: Option[int], endIndex: int): int =
    match failAt {
      | Some(index) => index
      | None => endIndex
    }

  pure def chunkHashValid(status: ChunkHashStatus): bool =
    status != MismatchesPayload

  def chunkLengthValid(chunkIndex: int, declaredTotalChunks: int, chunkLen: ChunkLength): bool =
    if (chunkIndex + 1 == declaredTotalChunks) {
      chunkLen == lastChunkLen
    } else {
      chunkLen == FullLen
    }

  def chunkShapeValid(
    chunkIndex: int,
    declaredTotalChunks: int,
    chunkLen: ChunkLength
  ): bool =
    all {
      phase == Receiving,
      declaredTotalChunks == totalChunks,
      validIndexes(totalChunks).contains(chunkIndex),
      chunkLengthValid(chunkIndex, declaredTotalChunks, chunkLen),
    }

  pure def allChunksReceivedFor(received: Set[int], total: int): bool =
    validIndexes(total).subseteq(received)

  pure def assembledHashMatches(
    payloads: int -> Option[ChunkPayload],
    total: int
  ): bool =
    validIndexes(total)
      .filter(index => payloads.get(index) != Some(CorrectPayload))
      .size() == 0

  pure def contiguousReceivedCount(received: Set[int]): int =
    if (not(received.contains(0))) {
      0
    } else if (not(received.contains(1))) {
      1
    } else if (not(received.contains(2))) {
      2
    } else {
      3
    }

  action init = all {
    phase' = Idle,
    totalChunks' = 1,
    lastChunkLen' = TailLen,
    receivedChunks' = Set(),
    receivedPayloads' = emptyPayloadMap,
    durableResponseSent' = false,
    durableResponseHash' = None,
    senderTotalChunks' = 1,
    senderNextChunkIndex' = 0,
    senderLastWindow' = Set(),
    senderComplete' = false,
    senderStopped' = false,
    lastDecision' = NoDecision,
  }

  action beginReceive(total: int, finalLen: ChunkLength): bool = all {
    phase == Idle,
    TotalChunks.contains(total),
    finalLen != WrongLen,
    phase' = Receiving,
    totalChunks' = total,
    lastChunkLen' = finalLen,
    receivedChunks' = Set(),
    receivedPayloads' = emptyPayloadMap,
    durableResponseSent' = false,
    durableResponseHash' = None,
    senderTotalChunks' = senderTotalChunks,
    senderNextChunkIndex' = senderNextChunkIndex,
    senderLastWindow' = senderLastWindow,
    senderComplete' = senderComplete,
    senderStopped' = senderStopped,
    lastDecision' = NoDecision,
  }

  action sendWindow(
    total: int,
    startIndex: int,
    maxChunks: int,
    failAt: Option[int]
  ): bool = {
    val endIndex = windowEnd(startIndex, maxChunks, total)
    val failed = failInsideWindow(failAt, startIndex, endIndex)
    val stopIndex = failureIndexOrEnd(failAt, endIndex)
    val windowComplete = startIndex >= total or (not(failed) and endIndex >= total)
    all {
      TotalChunks.contains(total),
      ChunkIndexes.contains(startIndex) or startIndex == total,
      WindowSizes.contains(maxChunks),
      phase' = phase,
      totalChunks' = totalChunks,
      lastChunkLen' = lastChunkLen,
      receivedChunks' = receivedChunks,
      receivedPayloads' = receivedPayloads,
      durableResponseSent' = durableResponseSent,
      durableResponseHash' = durableResponseHash,
      senderTotalChunks' = total,
      senderNextChunkIndex' =
        if (startIndex >= total) {
          total
        } else if (failed) {
          stopIndex
        } else {
          endIndex
        },
      senderLastWindow' =
        if (startIndex >= total) {
          Set()
        } else if (failed) {
          windowIndexes(startIndex, stopIndex)
        } else {
          windowIndexes(startIndex, endIndex)
        },
      senderComplete' = windowComplete,
      senderStopped' = failed,
      lastDecision' =
        if (windowComplete) {
          SenderWindowComplete
        } else if (failed) {
          SenderWindowStopped
        } else {
          SenderWindowSent
        },
    }
  }

  action receiveChunk(
    chunkIndex: int,
    declaredTotalChunks: int,
    chunkLen: ChunkLength,
    payload: ChunkPayload,
    hashStatus: ChunkHashStatus
  ): bool = all {
    phase == Receiving,
    if (not(chunkShapeValid(chunkIndex, declaredTotalChunks, chunkLen))) {
      all {
        phase' = Rejected,
        totalChunks' = totalChunks,
        lastChunkLen' = lastChunkLen,
        receivedChunks' = receivedChunks,
        receivedPayloads' = receivedPayloads,
        durableResponseSent' = durableResponseSent,
        durableResponseHash' = durableResponseHash,
        senderTotalChunks' = senderTotalChunks,
        senderNextChunkIndex' = senderNextChunkIndex,
        senderLastWindow' = senderLastWindow,
        senderComplete' = senderComplete,
        senderStopped' = senderStopped,
        lastDecision' = ShapeRejected,
      }
    } else if (not(chunkHashValid(hashStatus))) {
      all {
        phase' = Rejected,
        totalChunks' = totalChunks,
        lastChunkLen' = lastChunkLen,
        receivedChunks' = receivedChunks,
        receivedPayloads' = receivedPayloads,
        durableResponseSent' = durableResponseSent,
        durableResponseHash' = durableResponseHash,
        senderTotalChunks' = senderTotalChunks,
        senderNextChunkIndex' = senderNextChunkIndex,
        senderLastWindow' = senderLastWindow,
        senderComplete' = senderComplete,
        senderStopped' = senderStopped,
        lastDecision' = ChunkHashRejected,
      }
    } else {
      match receivedPayloads.get(chunkIndex) {
        | Some(existingPayload) =>
            if (existingPayload == payload) {
              all {
                phase' = phase,
                totalChunks' = totalChunks,
                lastChunkLen' = lastChunkLen,
                receivedChunks' = receivedChunks,
                receivedPayloads' = receivedPayloads,
                durableResponseSent' = durableResponseSent,
                durableResponseHash' = durableResponseHash,
                senderTotalChunks' = senderTotalChunks,
                senderNextChunkIndex' = senderNextChunkIndex,
                senderLastWindow' = senderLastWindow,
                senderComplete' = senderComplete,
                senderStopped' = senderStopped,
                lastDecision' = DuplicateAccepted,
              }
            } else {
              all {
                phase' = Rejected,
                totalChunks' = totalChunks,
                lastChunkLen' = lastChunkLen,
                receivedChunks' = receivedChunks,
                receivedPayloads' = receivedPayloads,
                durableResponseSent' = durableResponseSent,
                durableResponseHash' = durableResponseHash,
                senderTotalChunks' = senderTotalChunks,
                senderNextChunkIndex' = senderNextChunkIndex,
                senderLastWindow' = senderLastWindow,
                senderComplete' = senderComplete,
                senderStopped' = senderStopped,
                lastDecision' = ConflictingDuplicateRejected,
              }
            }
        | None => {
            val nextReceived = receivedChunks.union(Set(chunkIndex))
            val nextPayloads = receivedPayloads.set(chunkIndex, Some(payload))
            if (allChunksReceivedFor(nextReceived, totalChunks)) {
              if (assembledHashMatches(nextPayloads, totalChunks)) {
                all {
                  phase' = Committed,
                  totalChunks' = totalChunks,
                  lastChunkLen' = lastChunkLen,
                  receivedChunks' = nextReceived,
                  receivedPayloads' = nextPayloads,
                  durableResponseSent' = true,
                  durableResponseHash' = Some(ExpectedContentHash),
                  senderTotalChunks' = senderTotalChunks,
                  senderNextChunkIndex' = senderNextChunkIndex,
                  senderLastWindow' = senderLastWindow,
                  senderComplete' = senderComplete,
                  senderStopped' = senderStopped,
                  lastDecision' = CommitRecorded,
                }
              } else {
                all {
                  phase' = Rejected,
                  totalChunks' = totalChunks,
                  lastChunkLen' = lastChunkLen,
                  receivedChunks' = nextReceived,
                  receivedPayloads' = nextPayloads,
                  durableResponseSent' = false,
                  durableResponseHash' = None,
                  senderTotalChunks' = senderTotalChunks,
                  senderNextChunkIndex' = senderNextChunkIndex,
                  senderLastWindow' = senderLastWindow,
                  senderComplete' = senderComplete,
                  senderStopped' = senderStopped,
                  lastDecision' = AssembledHashRejected,
                }
              }
            } else {
              all {
                phase' = Receiving,
                totalChunks' = totalChunks,
                lastChunkLen' = lastChunkLen,
                receivedChunks' = nextReceived,
                receivedPayloads' = nextPayloads,
                durableResponseSent' = false,
                durableResponseHash' = None,
                senderTotalChunks' = senderTotalChunks,
                senderNextChunkIndex' = senderNextChunkIndex,
                senderLastWindow' = senderLastWindow,
                senderComplete' = senderComplete,
                senderStopped' = senderStopped,
                lastDecision' = PartialAccepted,
              }
            }
          }
      }
    }
  }

  action step = any {
    {
      nondet total = oneOf(TotalChunks)
      nondet finalLen = oneOf(ChunkLengths)
      beginReceive(total, finalLen)
    },
    {
      nondet total = oneOf(TotalChunks)
      nondet startIndex = oneOf(DeclaredTotals)
      nondet maxChunks = oneOf(WindowSizes)
      nondet failAt = oneOf(MaybeFailAt)
      sendWindow(total, startIndex, maxChunks, failAt)
    },
    {
      nondet chunkIndex = oneOf(ChunkIndexes)
      nondet declaredTotalChunks = oneOf(DeclaredTotals)
      nondet chunkLen = oneOf(ChunkLengths)
      nondet payload = oneOf(ChunkPayloads)
      nondet hashStatus = oneOf(ChunkHashStatuses)
      receiveChunk(chunkIndex, declaredTotalChunks, chunkLen, payload, hashStatus)
    },
  }

  def receivedChunksAreInRange: bool =
    receivedChunks.subseteq(validIndexes(totalChunks))

  def receivedChunksHavePayloads: bool =
    receivedChunks
      .filter(index => not(isSome(receivedPayloads.get(index))))
      .size() == 0

  def payloadsOnlyForReceivedChunks: bool =
    ChunkIndexes
      .filter(index =>
        isSome(receivedPayloads.get(index)) and not(receivedChunks.contains(index))
      )
      .size() == 0

  def durableResponseRequiresCommit: bool =
    not(durableResponseSent) or phase == Committed

  def durableResponseHashIsExpected: bool =
    match durableResponseHash {
      | Some(hash) => hash == ExpectedContentHash
      | None => true
    }

  def committedOnlyAfterEveryChunkArrived: bool =
    phase != Committed or allChunksReceivedFor(receivedChunks, totalChunks)

  def committedOnlyWithExpectedAssembledHash: bool =
    phase != Committed or assembledHashMatches(receivedPayloads, totalChunks)

  def rejectedTransferNeverSendsDurableResponse: bool =
    phase != Rejected or not(durableResponseSent)

  def malformedOrConflictingChunksDoNotCommit: bool =
    all {
      lastDecision == ShapeRejected
        or lastDecision == ChunkHashRejected
        or lastDecision == ConflictingDuplicateRejected
        or lastDecision == AssembledHashRejected,
    } implies not(durableResponseSent)

  def receiverResumeIndexWithinBounds: bool =
    0 <= contiguousReceivedCount(receivedChunks)
      and contiguousReceivedCount(receivedChunks) <= totalChunks

  def receiverResumeIndexIsContiguousPrefix: bool =
    validIndexes(contiguousReceivedCount(receivedChunks)).subseteq(receivedChunks)

  def senderWindowChunksAreValid: bool =
    senderLastWindow.subseteq(validIndexes(senderTotalChunks))

  def senderNextChunkIndexInRange: bool =
    0 <= senderNextChunkIndex and senderNextChunkIndex <= senderTotalChunks

  def senderCompleteMeansNextIsEnd: bool =
    not(senderComplete) or senderNextChunkIndex == senderTotalChunks

  def senderStoppedBeforeCompletion: bool =
    not(senderStopped) or all {
      not(senderComplete),
      senderNextChunkIndex < senderTotalChunks,
    }

  def senderWindowIsBeforeNextIndex: bool =
    senderLastWindow
      .filter(index => senderNextChunkIndex <= index)
      .size() == 0

  def safety: bool = all {
    receivedChunksAreInRange,
    receivedChunksHavePayloads,
    payloadsOnlyForReceivedChunks,
    durableResponseRequiresCommit,
    durableResponseHashIsExpected,
    committedOnlyAfterEveryChunkArrived,
    committedOnlyWithExpectedAssembledHash,
    rejectedTransferNeverSendsDurableResponse,
    malformedOrConflictingChunksDoNotCommit,
    receiverResumeIndexWithinBounds,
    receiverResumeIndexIsContiguousPrefix,
    senderWindowChunksAreValid,
    senderNextChunkIndexInRange,
    senderCompleteMeansNextIsEnd,
    senderStoppedBeforeCompletion,
    senderWindowIsBeforeNextIndex,
  }
}
