/// Invariant checks for `dfs_posix_read_write_visibility`.
module dfs_posix_read_write_visibility_safety {
  import common.* from "../../lib/common"
  import dfs_posix_read_write_visibility.* from "../../models/dfs/posix_read_write_visibility"

  def selectedReadMatchesAlgorithm: bool =
    phaseEvaluated implies
      readResult
        == selectedReadResult(
          readMode,
          pathState,
          currentContent,
          localContent,
          authorityReachable,
          contentReachable
        )


  def blockingWriteThenDistributedReadReturnsWrittenContent: bool =
    all {
      lastDecision == BlockingWriteThenRead,
      authorityReachable,
      contentReachable,
    } implies all {
      readResult == ReadOk,
      returnedContent == NewContent,
    }


  def staleContentRequiresLocalMirrorRead: bool =
    all {
      readResult == ReadOk,
      returnedContent == OldContent,
      currentContent == NewContent,
    } implies readMode == LocalMirror


  def distributedTombstoneReadsNotFound: bool =
    all {
      readMode == DistributedCurrent,
      authorityReachable,
      pathState == TombstonePath,
    } implies all {
      readResult == ReadNotFound,
      returnedContent == NoContent,
    }


  def rangeReadsAreSlicesOfFullDistributedRead: bool =
    all {
      lastDecision == RangeReadEvaluated,
      rangeResult == ReadOk,
    } implies all {
      rangeSize <= fullSize,
      if (rangeRequest == EmptyRange) {
        rangeContent == NoContent and rangeSize == 0
      } else {
        rangeContent == returnedContent
      },
    }


  def statAgreesWithDistributedReturnedContent: bool =
    all {
      readMode == DistributedCurrent,
      readResult == ReadOk,
    } implies all {
      statContent == returnedContent,
      fullSize == contentSize(statContent),
    }


  def noContentHasZeroSize: bool =
    all {
      fullSize == contentSize(returnedContent),
      rangeSize == rangeSizeFor(returnedContent, rangeRequest)
        or lastDecision != RangeReadEvaluated,
    }


  def safety: bool = all {
    selectedReadMatchesAlgorithm,
    blockingWriteThenDistributedReadReturnsWrittenContent,
    staleContentRequiresLocalMirrorRead,
    distributedTombstoneReadsNotFound,
    rangeReadsAreSlicesOfFullDistributedRead,
    statAgreesWithDistributedReturnedContent,
    noContentHasZeroSize,
  }
}
