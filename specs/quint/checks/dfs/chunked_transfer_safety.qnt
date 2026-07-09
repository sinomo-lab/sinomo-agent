/// Invariant checks for `dfs_chunked_transfer`.
module dfs_chunked_transfer_safety {
  import common.* from "../../lib/common"
  import dfs_chunked_transfer.* from "../../models/dfs/chunked_transfer"

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
