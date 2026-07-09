/// Invariant checks for `dfs_read_freshness`.
module dfs_read_freshness_safety {
  import common.* from "../../lib/common"
  import dfs_read_freshness.* from "../../models/dfs/read_freshness"

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
