/// DFS POSIX namespace correctness.
///
/// This module is the POSIX facade layer above DFS metadata correctness. It
/// uses a tiny finite path tree and treats metadata transactions, routing, and
/// freshness as lower-layer guarantees. The goal is to state the namespace
/// behavior that `posix_mkdir`, `posix_mkdir_all`, `posix_unlink`,
/// `posix_rmdir`, `posix_rename`, `posix_stat`, and `posix_list_dir` must
/// refine.
module dfs_posix_namespace {
  type PosixPath = Root | A | AB | AC | X | XB | XC
  type PosixKind = Missing | Directory | File | Symlink | Hardlink | Tombstone
  type PosixResult =
    | NoResult
    | Ok
    | NotFound
    | AlreadyExists
    | NotDirectory
    | IsDirectory
    | MissingParent
    | DirectoryNotEmpty
    | InvalidPath
  type PosixDecision =
    | NoDecision
    | SeedFile
    | Mkdir
    | MkdirAll
    | Unlink
    | Rmdir
    | Rename
    | Stat
    | ListDir

  pure val Paths: Set[PosixPath] = Set(Root, A, AB, AC, X, XB, XC)
  pure val NonRootPaths: Set[PosixPath] = Set(A, AB, AC, X, XB, XC)
  pure val PosixKinds: Set[PosixKind] =
    Set(Missing, Directory, File, Symlink, Hardlink, Tombstone)

  var namespace: PosixPath -> PosixKind
  var beforeNamespace: PosixPath -> PosixKind
  var lastDecision: PosixDecision
  var lastResult: PosixResult
  var lastPath: PosixPath
  var lastFrom: PosixPath
  var lastTo: PosixPath
  var lastListed: Set[PosixPath]

  pure val emptyNamespace: PosixPath -> PosixKind =
    Paths.mapBy(path => if (path == Root) { Directory } else { Missing })

  pure def isLive(kind: PosixKind): bool =
    kind != Missing and kind != Tombstone

  pure def parent(path: PosixPath): PosixPath =
    match path {
      | Root => Root
      | A => Root
      | X => Root
      | AB => A
      | AC => A
      | XB => X
      | XC => X
    }

  pure def directChild(parentPath: PosixPath, childPath: PosixPath): bool =
    any {
      parentPath == Root and (childPath == A or childPath == X),
      parentPath == A and (childPath == AB or childPath == AC),
      parentPath == X and (childPath == XB or childPath == XC),
    }

  pure def strictDescendantOf(ancestor: PosixPath, candidate: PosixPath): bool =
    any {
      ancestor == Root and candidate != Root,
      ancestor == A and (candidate == AB or candidate == AC),
      ancestor == X and (candidate == XB or candidate == XC),
    }

  pure def hasLiveChild(ns: PosixPath -> PosixKind, path: PosixPath): bool =
    Paths.exists(child => directChild(path, child) and isLive(ns.get(child)))

  pure def liveChildren(ns: PosixPath -> PosixKind, path: PosixPath): Set[PosixPath] =
    Paths.filter(child => directChild(path, child) and isLive(ns.get(child)))

  pure def mkdirAllPrefix(target: PosixPath, path: PosixPath): bool =
    any {
      target == path and target != Root,
      parent(target) == path and path != Root,
    }

  pure def directoryMoveRepresentable(
    ns: PosixPath -> PosixKind,
    from: PosixPath,
    dest: PosixPath
  ): bool =
    any {
      ns.get(from) != Directory,
      not(hasLiveChild(ns, from)),
      from == A and dest == X,
      from == X and dest == A,
    }

  pure def mkdirResult(ns: PosixPath -> PosixKind, path: PosixPath): PosixResult =
    if (path == Root) {
      InvalidPath
    } else if (isLive(ns.get(path))) {
      AlreadyExists
    } else if (ns.get(parent(path)) == Missing or ns.get(parent(path)) == Tombstone) {
      MissingParent
    } else if (ns.get(parent(path)) != Directory) {
      NotDirectory
    } else {
      Ok
    }

  pure def seedFileResult(ns: PosixPath -> PosixKind, path: PosixPath): PosixResult =
    if (path == Root) {
      InvalidPath
    } else if (isLive(ns.get(path))) {
      AlreadyExists
    } else if (ns.get(parent(path)) != Directory) {
      MissingParent
    } else {
      Ok
    }

  pure def mkdirAllResult(ns: PosixPath -> PosixKind, path: PosixPath): PosixResult =
    if (path == Root) {
      Ok
    } else if (isLive(ns.get(path)) and ns.get(path) != Directory) {
      NotDirectory
    } else if (parent(path) != Root and isLive(ns.get(parent(path))) and ns.get(parent(path)) != Directory) {
      NotDirectory
    } else {
      Ok
    }

  pure def unlinkResult(ns: PosixPath -> PosixKind, path: PosixPath): PosixResult =
    if (path == Root) {
      InvalidPath
    } else if (ns.get(path) == Missing or ns.get(path) == Tombstone) {
      NotFound
    } else if (ns.get(path) == Directory) {
      IsDirectory
    } else {
      Ok
    }

  pure def rmdirResult(ns: PosixPath -> PosixKind, path: PosixPath): PosixResult =
    if (path == Root) {
      InvalidPath
    } else if (ns.get(path) == Missing or ns.get(path) == Tombstone) {
      NotFound
    } else if (ns.get(path) != Directory) {
      NotDirectory
    } else if (hasLiveChild(ns, path)) {
      DirectoryNotEmpty
    } else {
      Ok
    }

  pure def renameResult(
    ns: PosixPath -> PosixKind,
    from: PosixPath,
    dest: PosixPath
  ): PosixResult =
    if (from == Root or dest == Root) {
      InvalidPath
    } else if (ns.get(from) == Missing or ns.get(from) == Tombstone) {
      NotFound
    } else if (from == dest) {
      Ok
    } else if (ns.get(from) == Directory and strictDescendantOf(from, dest)) {
      InvalidPath
    } else if (not(directoryMoveRepresentable(ns, from, dest))) {
      InvalidPath
    } else if (ns.get(parent(dest)) == Missing or ns.get(parent(dest)) == Tombstone) {
      MissingParent
    } else if (ns.get(parent(dest)) != Directory) {
      NotDirectory
    } else if (ns.get(from) == Directory and isLive(ns.get(dest)) and ns.get(dest) != Directory) {
      NotDirectory
    } else if (ns.get(from) != Directory and ns.get(dest) == Directory) {
      IsDirectory
    } else if (ns.get(dest) == Directory and hasLiveChild(ns, dest)) {
      DirectoryNotEmpty
    } else {
      Ok
    }

  pure def namespaceAfterMkdir(
    ns: PosixPath -> PosixKind,
    path: PosixPath
  ): PosixPath -> PosixKind =
    if (mkdirResult(ns, path) == Ok) { ns.set(path, Directory) } else { ns }

  pure def namespaceAfterSeedFile(
    ns: PosixPath -> PosixKind,
    path: PosixPath
  ): PosixPath -> PosixKind =
    if (seedFileResult(ns, path) == Ok) { ns.set(path, File) } else { ns }

  pure def namespaceAfterMkdirAll(
    ns: PosixPath -> PosixKind,
    path: PosixPath
  ): PosixPath -> PosixKind =
    if (mkdirAllResult(ns, path) != Ok) {
      ns
    } else {
      Paths.fold(ns, (acc, candidate) =>
        if (mkdirAllPrefix(path, candidate) and not(isLive(acc.get(candidate)))) {
          acc.set(candidate, Directory)
        } else {
          acc
        }
      )
    }

  pure def namespaceAfterUnlinkOrRmdir(
    ns: PosixPath -> PosixKind,
    path: PosixPath,
    result: PosixResult
  ): PosixPath -> PosixKind =
    if (result == Ok) { ns.set(path, Tombstone) } else { ns }

  pure def namespaceAfterRename(
    ns: PosixPath -> PosixKind,
    from: PosixPath,
    dest: PosixPath
  ): PosixPath -> PosixKind =
    if (renameResult(ns, from, dest) != Ok or from == dest) {
      ns
    } else if (ns.get(from) == Directory and from == A and dest == X) {
      ns
        .set(A, Tombstone)
        .set(X, Directory)
        .set(AB, Tombstone)
        .set(AC, Tombstone)
        .set(XB, ns.get(AB))
        .set(XC, ns.get(AC))
    } else if (ns.get(from) == Directory and from == X and dest == A) {
      ns
        .set(X, Tombstone)
        .set(A, Directory)
        .set(XB, Tombstone)
        .set(XC, Tombstone)
        .set(AB, ns.get(XB))
        .set(AC, ns.get(XC))
    } else {
      ns.set(from, Tombstone).set(dest, ns.get(from))
    }

  action init = all {
    namespace' = emptyNamespace,
    beforeNamespace' = emptyNamespace,
    lastDecision' = NoDecision,
    lastResult' = NoResult,
    lastPath' = Root,
    lastFrom' = Root,
    lastTo' = Root,
    lastListed' = Set(),
  }

  action seedFile(path: PosixPath): bool = {
    val result = seedFileResult(namespace, path)
    all {
      beforeNamespace' = namespace,
      namespace' = namespaceAfterSeedFile(namespace, path),
      lastDecision' = SeedFile,
      lastResult' = result,
      lastPath' = path,
      lastFrom' = path,
      lastTo' = path,
      lastListed' = Set(),
    }
  }

  action mkdir(path: PosixPath): bool = {
    val result = mkdirResult(namespace, path)
    all {
      beforeNamespace' = namespace,
      namespace' = namespaceAfterMkdir(namespace, path),
      lastDecision' = Mkdir,
      lastResult' = result,
      lastPath' = path,
      lastFrom' = path,
      lastTo' = path,
      lastListed' = Set(),
    }
  }

  action mkdirAll(path: PosixPath): bool = {
    val result = mkdirAllResult(namespace, path)
    all {
      beforeNamespace' = namespace,
      namespace' = namespaceAfterMkdirAll(namespace, path),
      lastDecision' = MkdirAll,
      lastResult' = result,
      lastPath' = path,
      lastFrom' = path,
      lastTo' = path,
      lastListed' = Set(),
    }
  }

  action unlink(path: PosixPath): bool = {
    val result = unlinkResult(namespace, path)
    all {
      beforeNamespace' = namespace,
      namespace' = namespaceAfterUnlinkOrRmdir(namespace, path, result),
      lastDecision' = Unlink,
      lastResult' = result,
      lastPath' = path,
      lastFrom' = path,
      lastTo' = path,
      lastListed' = Set(),
    }
  }

  action rmdir(path: PosixPath): bool = {
    val result = rmdirResult(namespace, path)
    all {
      beforeNamespace' = namespace,
      namespace' = namespaceAfterUnlinkOrRmdir(namespace, path, result),
      lastDecision' = Rmdir,
      lastResult' = result,
      lastPath' = path,
      lastFrom' = path,
      lastTo' = path,
      lastListed' = Set(),
    }
  }

  action rename(from: PosixPath, dest: PosixPath): bool = {
    val result = renameResult(namespace, from, dest)
    all {
      beforeNamespace' = namespace,
      namespace' = namespaceAfterRename(namespace, from, dest),
      lastDecision' = Rename,
      lastResult' = result,
      lastPath' = dest,
      lastFrom' = from,
      lastTo' = dest,
      lastListed' = Set(),
    }
  }

  action stat(path: PosixPath): bool = all {
    beforeNamespace' = namespace,
    namespace' = namespace,
    lastDecision' = Stat,
    lastResult' = if (isLive(namespace.get(path))) { Ok } else { NotFound },
    lastPath' = path,
    lastFrom' = path,
    lastTo' = path,
    lastListed' = Set(),
  }

  action listDir(path: PosixPath): bool = all {
    beforeNamespace' = namespace,
    namespace' = namespace,
    lastDecision' = ListDir,
    lastResult' = if (namespace.get(path) == Directory) { Ok } else { NotDirectory },
    lastPath' = path,
    lastFrom' = path,
    lastTo' = path,
    lastListed' =
      if (namespace.get(path) == Directory) { liveChildren(namespace, path) } else { Set() },
  }

  action step = any {
    {
      nondet path = oneOf(NonRootPaths)
      seedFile(path)
    },
    {
      nondet path = oneOf(Paths)
      any {
        mkdir(path),
        mkdirAll(path),
        unlink(path),
        rmdir(path),
        stat(path),
        listDir(path),
      }
    },
    {
      nondet from = oneOf(Paths)
      nondet dest = oneOf(Paths)
      rename(from, dest)
    },
  }

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
