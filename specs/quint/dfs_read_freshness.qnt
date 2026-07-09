/// DFS current-read freshness versus explicit local-mirror reads.
///
/// This model captures the algorithmic boundary behind the SIFS stale-cache
/// fixes. A distributed current read must refresh to the authoritative visible
/// version when that metadata is available. A local mirror read may intentionally
/// serve local cached content, including stale content, because that mode is an
/// explicit availability tradeoff for catalog/mounted reads.
module dfs_read_freshness {
  type ReadMode = DistributedCurrent | LocalMirror
  type ReadOutcome = ReadPending | ReadMissing | ReturnedV1 | ReturnedV2
  type ReadPhase = Idle | ReadEvaluated
  type Decision = NoDecision | ReadStarted | TerminalStutter

  pure val Versions: Set[int] = Set(0, 1, 2)
  pure val ReadModes: Set[ReadMode] = Set(DistributedCurrent, LocalMirror)

  var phase: ReadPhase
  var readMode: ReadMode
  var currentVisibleVersion: int
  var pathCacheVersion: int
  var localContentVersion: int
  var metadataRefreshAvailable: bool
  var remoteCurrentContentAvailable: bool
  var readOutcome: ReadOutcome
  var lastDecision: Decision

  pure def returnedVersion(outcome: ReadOutcome): int =
    match outcome {
      | ReturnedV1 => 1
      | ReturnedV2 => 2
      | _ => 0
    }

  pure def returnForVersion(version: int): ReadOutcome =
    if (version == 1) {
      ReturnedV1
    } else if (version == 2) {
      ReturnedV2
    } else {
      ReadMissing
    }

  pure def effectiveCurrentVersion(
    current: int,
    cached: int,
    refreshAvailable: bool
  ): int =
    if (refreshAvailable) { current } else { cached }

  pure def selectedReadOutcome(
    mode: ReadMode,
    current: int,
    cached: int,
    localContent: int,
    refreshAvailable: bool,
    remoteAvailable: bool
  ): ReadOutcome = {
    val effective = effectiveCurrentVersion(current, cached, refreshAvailable)
    if (mode == LocalMirror) {
      if (localContent == 0) { ReadPending } else { returnForVersion(localContent) }
    } else if (effective == 0) {
      ReadMissing
    } else if (localContent == effective) {
      returnForVersion(effective)
    } else if (remoteAvailable and effective == current) {
      returnForVersion(effective)
    } else {
      ReadPending
    }
  }

  action init = all {
    phase' = Idle,
    readMode' = DistributedCurrent,
    currentVisibleVersion' = 0,
    pathCacheVersion' = 0,
    localContentVersion' = 0,
    metadataRefreshAvailable' = false,
    remoteCurrentContentAvailable' = false,
    readOutcome' = ReadMissing,
    lastDecision' = NoDecision,
  }

  action startRead(
    mode: ReadMode,
    current: int,
    cached: int,
    localContent: int,
    refreshAvailable: bool,
    remoteAvailable: bool
  ): bool = all {
    phase == Idle,
    Versions.contains(current),
    Versions.contains(cached),
    Versions.contains(localContent),
    cached <= current,
    localContent <= current,
    readMode' = mode,
    currentVisibleVersion' = current,
    pathCacheVersion' = cached,
    localContentVersion' = localContent,
    metadataRefreshAvailable' = refreshAvailable,
    remoteCurrentContentAvailable' = remoteAvailable,
    readOutcome' =
      selectedReadOutcome(
        mode,
        current,
        cached,
        localContent,
        refreshAvailable,
        remoteAvailable
      ),
    phase' = ReadEvaluated,
    lastDecision' = ReadStarted,
  }

  action stutterEvaluated: bool = all {
    phase == ReadEvaluated,
    readMode' = readMode,
    currentVisibleVersion' = currentVisibleVersion,
    pathCacheVersion' = pathCacheVersion,
    localContentVersion' = localContentVersion,
    metadataRefreshAvailable' = metadataRefreshAvailable,
    remoteCurrentContentAvailable' = remoteCurrentContentAvailable,
    readOutcome' = readOutcome,
    phase' = phase,
    lastDecision' = TerminalStutter,
  }

  action step = any {
    {
      nondet mode = oneOf(ReadModes)
      nondet current = oneOf(Versions)
      nondet cached = oneOf(Versions)
      nondet localContent = oneOf(Versions)
      nondet refreshAvailable = oneOf(Set(true, false))
      nondet remoteAvailable = oneOf(Set(true, false))
      startRead(mode, current, cached, localContent, refreshAvailable, remoteAvailable)
    },
    stutterEvaluated,
  }

  def readOutcomeMatchesAlgorithm: bool =
    readOutcome
      == selectedReadOutcome(
        readMode,
        currentVisibleVersion,
        pathCacheVersion,
        localContentVersion,
        metadataRefreshAvailable,
        remoteCurrentContentAvailable
      )

  def distributedCurrentDoesNotReturnStaleAfterRefresh: bool =
    not(all {
      readMode == DistributedCurrent,
      metadataRefreshAvailable,
      returnedVersion(readOutcome) > 0,
      returnedVersion(readOutcome) < currentVisibleVersion,
    })

  def staleReturnedContentRequiresLocalMirrorOrNoRefresh: bool =
    all {
      returnedVersion(readOutcome) > 0,
      returnedVersion(readOutcome) < currentVisibleVersion,
    } implies any {
      readMode == LocalMirror,
      not(metadataRefreshAvailable),
    }

  def distributedCurrentWaitsForMissingCurrentContent: bool =
    all {
      readMode == DistributedCurrent,
      metadataRefreshAvailable,
      currentVisibleVersion > 0,
      localContentVersion != currentVisibleVersion,
      not(remoteCurrentContentAvailable),
    } implies readOutcome == ReadPending

  def readOwnCurrentLocalContent: bool =
    all {
      localContentVersion == currentVisibleVersion,
      currentVisibleVersion > 0,
      any {
        readMode == LocalMirror,
        metadataRefreshAvailable,
        pathCacheVersion == currentVisibleVersion,
      },
    } implies returnedVersion(readOutcome) == currentVisibleVersion

  def safety: bool = all {
    readOutcomeMatchesAlgorithm,
    distributedCurrentDoesNotReturnStaleAfterRefresh,
    staleReturnedContentRequiresLocalMirrorOrNoRefresh,
    distributedCurrentWaitsForMissingCurrentContent,
    readOwnCurrentLocalContent,
  }
}
