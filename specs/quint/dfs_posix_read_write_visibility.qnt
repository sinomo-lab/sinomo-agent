/// DFS POSIX read/write visibility.
///
/// This module states the POSIX facade guarantee above the lower DFS read
/// freshness and routed namespace specs. It abstracts file bytes to symbolic
/// content values and checks the user-visible contract for blocking writes,
/// distributed current reads, local mirror reads, tombstones, stat metadata,
/// and range reads.
module dfs_posix_read_write_visibility {
  type PathState = MissingPath | LiveFile | TombstonePath
  type Content = NoContent | OldContent | NewContent
  type ReadMode = DistributedCurrent | LocalMirror
  type RangeRequest = WholeFile | PrefixRange | EmptyRange
  type ReadResult = NoResult | ReadOk | ReadNotFound | ReadPending
  type VisibilityDecision =
    | NoDecision
    | BlockingWriteThenRead
    | ReadEvaluated
    | RangeReadEvaluated
    | TerminalStutter

  pure val PathStates: Set[PathState] =
    Set(MissingPath, LiveFile, TombstonePath)
  pure val Contents: Set[Content] =
    Set(NoContent, OldContent, NewContent)
  pure val ReadModes: Set[ReadMode] =
    Set(DistributedCurrent, LocalMirror)
  pure val RangeRequests: Set[RangeRequest] =
    Set(WholeFile, PrefixRange, EmptyRange)

  var phaseEvaluated: bool
  var pathState: PathState
  var readMode: ReadMode
  var rangeRequest: RangeRequest
  var authorityReachable: bool
  var contentReachable: bool
  var currentContent: Content
  var localContent: Content
  var returnedContent: Content
  var statContent: Content
  var rangeContent: Content
  var readResult: ReadResult
  var rangeResult: ReadResult
  var fullSize: int
  var rangeSize: int
  var lastDecision: VisibilityDecision

  pure def contentSize(content: Content): int =
    match content {
      | NoContent => 0
      | OldContent => 1
      | NewContent => 2
    }

  pure def contentForLivePath(state: PathState, content: Content): Content =
    if (state == LiveFile) { content } else { NoContent }

  pure def selectedReadResult(
    mode: ReadMode,
    state: PathState,
    current: Content,
    local: Content,
    authorityUp: bool,
    contentUp: bool
  ): ReadResult =
    if (mode == LocalMirror) {
      if (local == NoContent) { ReadNotFound } else { ReadOk }
    } else if (not(authorityUp)) {
      ReadPending
    } else if (state == TombstonePath or state == MissingPath or current == NoContent) {
      ReadNotFound
    } else if (local == current or contentUp) {
      ReadOk
    } else {
      ReadPending
    }

  pure def selectedReadContent(
    mode: ReadMode,
    state: PathState,
    current: Content,
    local: Content,
    authorityUp: bool,
    contentUp: bool
  ): Content = {
    val result = selectedReadResult(mode, state, current, local, authorityUp, contentUp)
    if (result != ReadOk) {
      NoContent
    } else if (mode == LocalMirror) {
      local
    } else {
      current
    }
  }

  pure def selectedStatContent(state: PathState, current: Content, authorityUp: bool): Content =
    if (authorityUp and state == LiveFile) { current } else { NoContent }

  pure def rangeResultFor(fullResult: ReadResult): ReadResult =
    if (fullResult == ReadOk) { ReadOk } else { fullResult }

  pure def rangeContentFor(content: Content, request: RangeRequest): Content =
    if (request == EmptyRange) { NoContent } else { content }

  pure def rangeSizeFor(content: Content, request: RangeRequest): int =
    if (request == EmptyRange) {
      0
    } else if (request == PrefixRange and contentSize(content) > 0) {
      1
    } else {
      contentSize(content)
    }

  action init = all {
    phaseEvaluated' = false,
    pathState' = MissingPath,
    readMode' = DistributedCurrent,
    rangeRequest' = WholeFile,
    authorityReachable' = false,
    contentReachable' = false,
    currentContent' = NoContent,
    localContent' = NoContent,
    returnedContent' = NoContent,
    statContent' = NoContent,
    rangeContent' = NoContent,
    readResult' = NoResult,
    rangeResult' = NoResult,
    fullSize' = 0,
    rangeSize' = 0,
    lastDecision' = NoDecision,
  }

  action blockingWriteThenDistributedRead(
    authorityUp: bool,
    contentUp: bool,
    localStartsStale: bool
  ): bool = {
    val local = if (localStartsStale) { OldContent } else { NewContent }
    val result =
      selectedReadResult(
        DistributedCurrent,
        LiveFile,
        NewContent,
        local,
        authorityUp,
        contentUp
      )
    val returned =
      selectedReadContent(
        DistributedCurrent,
        LiveFile,
        NewContent,
        local,
        authorityUp,
        contentUp
      )
    all {
      pathState' = LiveFile,
      readMode' = DistributedCurrent,
      rangeRequest' = WholeFile,
      authorityReachable' = authorityUp,
      contentReachable' = contentUp,
      currentContent' = NewContent,
      localContent' = local,
      returnedContent' = returned,
      statContent' = selectedStatContent(LiveFile, NewContent, authorityUp),
      rangeContent' = NoContent,
      readResult' = result,
      rangeResult' = NoResult,
      fullSize' = contentSize(returned),
      rangeSize' = 0,
      phaseEvaluated' = true,
      lastDecision' = BlockingWriteThenRead,
    }
  }

  action evaluateRead(
    mode: ReadMode,
    state: PathState,
    current: Content,
    local: Content,
    authorityUp: bool,
    contentUp: bool
  ): bool = {
    val result = selectedReadResult(mode, state, current, local, authorityUp, contentUp)
    val returned = selectedReadContent(mode, state, current, local, authorityUp, contentUp)
    all {
      pathState' = state,
      readMode' = mode,
      rangeRequest' = WholeFile,
      authorityReachable' = authorityUp,
      contentReachable' = contentUp,
      currentContent' = contentForLivePath(state, current),
      localContent' = local,
      returnedContent' = returned,
      statContent' = selectedStatContent(state, current, authorityUp),
      rangeContent' = NoContent,
      readResult' = result,
      rangeResult' = NoResult,
      fullSize' = contentSize(returned),
      rangeSize' = 0,
      phaseEvaluated' = true,
      lastDecision' = ReadEvaluated,
    }
  }

  action evaluateRangeRead(
    state: PathState,
    current: Content,
    local: Content,
    request: RangeRequest,
    authorityUp: bool,
    contentUp: bool
  ): bool = {
    val fullResult =
      selectedReadResult(
        DistributedCurrent,
        state,
        current,
        local,
        authorityUp,
        contentUp
      )
    val fullContent =
      selectedReadContent(
        DistributedCurrent,
        state,
        current,
        local,
        authorityUp,
        contentUp
      )
    val ranged = rangeContentFor(fullContent, request)
    all {
      pathState' = state,
      readMode' = DistributedCurrent,
      rangeRequest' = request,
      authorityReachable' = authorityUp,
      contentReachable' = contentUp,
      currentContent' = contentForLivePath(state, current),
      localContent' = local,
      returnedContent' = fullContent,
      statContent' = selectedStatContent(state, current, authorityUp),
      rangeContent' = ranged,
      readResult' = fullResult,
      rangeResult' = rangeResultFor(fullResult),
      fullSize' = contentSize(fullContent),
      rangeSize' = rangeSizeFor(fullContent, request),
      phaseEvaluated' = true,
      lastDecision' = RangeReadEvaluated,
    }
  }

  action stutterEvaluated: bool = all {
    phaseEvaluated,
    pathState' = pathState,
    readMode' = readMode,
    rangeRequest' = rangeRequest,
    authorityReachable' = authorityReachable,
    contentReachable' = contentReachable,
    currentContent' = currentContent,
    localContent' = localContent,
    returnedContent' = returnedContent,
    statContent' = statContent,
    rangeContent' = rangeContent,
    readResult' = readResult,
    rangeResult' = rangeResult,
    fullSize' = fullSize,
    rangeSize' = rangeSize,
    phaseEvaluated' = phaseEvaluated,
    lastDecision' = TerminalStutter,
  }

  action step = any {
    {
      nondet authorityUp = oneOf(Set(true, false))
      nondet contentUp = oneOf(Set(true, false))
      nondet localStartsStale = oneOf(Set(true, false))
      blockingWriteThenDistributedRead(authorityUp, contentUp, localStartsStale)
    },
    {
      nondet mode = oneOf(ReadModes)
      nondet state = oneOf(PathStates)
      nondet current = oneOf(Contents)
      nondet local = oneOf(Contents)
      nondet authorityUp = oneOf(Set(true, false))
      nondet contentUp = oneOf(Set(true, false))
      evaluateRead(mode, state, current, local, authorityUp, contentUp)
    },
    {
      nondet state = oneOf(PathStates)
      nondet current = oneOf(Contents)
      nondet local = oneOf(Contents)
      nondet request = oneOf(RangeRequests)
      nondet authorityUp = oneOf(Set(true, false))
      nondet contentUp = oneOf(Set(true, false))
      evaluateRangeRead(state, current, local, request, authorityUp, contentUp)
    },
    stutterEvaluated,
  }

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
