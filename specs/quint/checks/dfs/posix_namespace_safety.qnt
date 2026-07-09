/// Invariant checks for `dfs_posix_namespace`.
module dfs_posix_namespace_safety {
  import common.* from "../../lib/common"
  import dfs_posix_namespace.* from "../../models/dfs/posix_namespace"

  def rootIsAlwaysDirectory: bool =
    namespace.get(Root) == Directory


  def liveNonRootPathsHaveDirectoryParents: bool =
    NonRootPaths.forall(path =>
      isLive(namespace.get(path)) implies namespace.get(parent(path)) == Directory
    )


  def failedOperationsDoNotMutateNamespace: bool =
    all {
      lastResult != NoResult,
      lastResult != Ok,
    } implies namespace == beforeNamespace


  def mkdirRequiresDirectoryParentAndEmptyTarget: bool =
    all {
      lastDecision == Mkdir,
      lastResult == Ok,
    } implies all {
      beforeNamespace.get(parent(lastPath)) == Directory,
      not(isLive(beforeNamespace.get(lastPath))),
      namespace.get(lastPath) == Directory,
    }


  def mkdirAllCreatesOnlyMissingPrefixes: bool =
    all {
      lastDecision == MkdirAll,
      lastResult == Ok,
    } implies Paths.forall(path =>
      if (namespace.get(path) != beforeNamespace.get(path)) {
        all {
          mkdirAllPrefix(lastPath, path),
          not(isLive(beforeNamespace.get(path))),
          namespace.get(path) == Directory,
        }
      } else {
        true
      }
    )


  def unlinkRejectsDirectories: bool =
    all {
      lastDecision == Unlink,
      lastPath != Root,
      beforeNamespace.get(lastPath) == Directory,
    } implies lastResult == IsDirectory


  def rmdirRejectsNonEmptyDirectories: bool =
    all {
      lastDecision == Rmdir,
      lastPath != Root,
      beforeNamespace.get(lastPath) == Directory,
      hasLiveChild(beforeNamespace, lastPath),
    } implies lastResult == DirectoryNotEmpty


  def renameRejectsDirectoryIntoDescendant: bool =
    all {
      lastDecision == Rename,
      beforeNamespace.get(lastFrom) == Directory,
      strictDescendantOf(lastFrom, lastTo),
    } implies lastResult == InvalidPath


  def renameRejectsUnrepresentableDirectoryMove: bool =
    all {
      lastDecision == Rename,
      lastFrom != lastTo,
      beforeNamespace.get(lastFrom) == Directory,
      not(directoryMoveRepresentable(beforeNamespace, lastFrom, lastTo)),
    } implies lastResult == InvalidPath


  def renameSuccessMovesSinglePath: bool =
    all {
      lastDecision == Rename,
      lastResult == Ok,
      lastFrom != lastTo,
    } implies all {
      namespace.get(lastFrom) == Tombstone,
      namespace.get(lastTo) == beforeNamespace.get(lastFrom),
      isLive(namespace.get(lastTo)),
    }


  def directoryRenameMovesChildren: bool =
    all {
      lastDecision == Rename,
      lastResult == Ok,
      beforeNamespace.get(lastFrom) == Directory,
      lastFrom == A,
      lastTo == X,
    } implies all {
      namespace.get(XB) == beforeNamespace.get(AB),
      namespace.get(XC) == beforeNamespace.get(AC),
      namespace.get(AB) == Tombstone,
      namespace.get(AC) == Tombstone,
    }


  def statAndListAgree: bool = all {
    lastDecision == ListDir,
    lastResult == Ok,
  } implies all {
    lastListed == liveChildren(namespace, lastPath),
    lastListed.forall(path =>
      directChild(lastPath, path) and isLive(namespace.get(path))
    ),
  }


  def safety: bool = all {
    rootIsAlwaysDirectory,
    liveNonRootPathsHaveDirectoryParents,
    failedOperationsDoNotMutateNamespace,
    mkdirRequiresDirectoryParentAndEmptyTarget,
    mkdirAllCreatesOnlyMissingPrefixes,
    unlinkRejectsDirectories,
    rmdirRejectsNonEmptyDirectories,
    renameRejectsDirectoryIntoDescendant,
    renameRejectsUnrepresentableDirectoryMove,
    renameSuccessMovesSinglePath,
    directoryRenameMovesChildren,
    statAndListAgree,
  }
}
