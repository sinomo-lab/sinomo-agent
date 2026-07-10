/// Invariant checks for `dfs_posix_links`.
module dfs_posix_links_safety {
  import common.* from "../../lib/common"
  import dfs_posix_links.* from "../../models/dfs/posix_links"

  def hardlinksReferenceLiveAnchors: bool =
    Paths.forall(path =>
      kindAt.get(path) == Hardlink implies
        match objectAt.get(path) {
          | Some(objectId) => liveAnchorForObject(kindAt, objectAt, objectId)
          | None => false
        }
    )


  def failedOperationsDoNotMutateLinks: bool =
    all {
      lastResult != NoResult,
      lastResult != Ok,
    } implies all {
      kindAt == beforeKindAt,
      objectAt == beforeObjectAt,
      symlinkTarget == beforeSymlinkTarget,
    }


  def hardlinkToDirectoryRejected: bool =
    all {
      lastDecision == HardlinkOp,
      lastLinkPath != Root,
      beforeKindAt.get(lastExisting) == Directory,
    } implies lastResult == IsDirectory


  def hardlinkToMissingOrTombstoneRejected: bool =
    all {
      lastDecision == HardlinkOp,
      lastLinkPath != Root,
      beforeKindAt.get(lastExisting) == Missing
        or beforeKindAt.get(lastExisting) == Tombstone,
    } implies lastResult == NotFound


  def unlinkingHardlinkPreservesTargetContent: bool =
    all {
      lastDecision == UnlinkOp,
      lastResult == Ok,
      beforeKindAt.get(lastPath) == Hardlink,
    } implies
      match beforeObjectAt.get(lastPath) {
        | Some(objectId) => liveAnchorForObject(kindAt, objectAt, objectId)
        | None => false
      }


  def reportedLinkCountMatchesAliases: bool =
    all {
      lastDecision == StatOp or lastDecision == LstatOp,
      lastResult == Ok,
      reportedKind == File or reportedKind == Hardlink or reportedKind == Symlink,
    } implies
      match reportedObject {
        | Some(objectId) =>
            reportedLinkCount == linkCountForObject(kindAt, objectAt, objectId)
        | None => false
      }


  def readlinkOnlySucceedsForSymlinks: bool =
    lastDecision == ReadlinkOp implies
      (lastResult == Ok) == (beforeKindAt.get(lastPath) == Symlink)


  def lstatSeesSymlinkItself: bool =
    all {
      lastDecision == LstatOp,
      beforeKindAt.get(lastPath) == Symlink,
    } implies all {
      lastResult == Ok,
      reportedKind == Symlink,
    }


  def statRejectsDetectedSymlinkLoops: bool =
    all {
      lastDecision == StatOp,
      symlinkCycle(beforeKindAt, beforeSymlinkTarget, lastPath),
    } implies lastResult == TooManySymlinks


  def safety: bool = all {
    hardlinksReferenceLiveAnchors,
    failedOperationsDoNotMutateLinks,
    hardlinkToDirectoryRejected,
    hardlinkToMissingOrTombstoneRejected,
    unlinkingHardlinkPreservesTargetContent,
    reportedLinkCountMatchesAliases,
    readlinkOnlySucceedsForSymlinks,
    lstatSeesSymlinkItself,
    statRejectsDetectedSymlinkLoops,
  }
}
