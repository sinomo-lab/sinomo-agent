/// DFS POSIX link correctness.
///
/// This module refines the POSIX namespace model for hardlink and symlink
/// behavior. It abstracts file content to object ids: hardlinks point at a file
/// object, symlinks point at a path, and stat/lstat/readlink report the POSIX
/// view without modeling raw bytes or metadata table layout.
module dfs_posix_links {
  import common.* from "../../lib/common"

  type LinkPath = Root | D | DA | DB | X
  type LinkObject = O1 | O2 | O3 | O4 | O5
  type LinkKind = Missing | Directory | File | Symlink | Hardlink | Tombstone
  type LinkResult =
    | NoResult
    | Ok
    | NotFound
    | AlreadyExists
    | NotDirectory
    | IsDirectory
    | NotSymlink
    | MissingParent
    | InvalidPath
    | TooManySymlinks
  type LinkDecision =
    | NoDecision
    | HardlinkOp
    | SymlinkOp
    | UnlinkOp
    | StatOp
    | LstatOp
    | ReadlinkOp

  pure val Paths: Set[LinkPath] = Set(Root, D, DA, DB, X)
  pure val NonRootPaths: Set[LinkPath] = Set(D, DA, DB, X)

  var kindAt: LinkPath -> LinkKind
  var objectAt: LinkPath -> Option[LinkObject]
  var symlinkTarget: LinkPath -> Option[LinkPath]
  var beforeKindAt: LinkPath -> LinkKind
  var beforeObjectAt: LinkPath -> Option[LinkObject]
  var beforeSymlinkTarget: LinkPath -> Option[LinkPath]
  var lastDecision: LinkDecision
  var lastResult: LinkResult
  var lastPath: LinkPath
  var lastExisting: LinkPath
  var lastLinkPath: LinkPath
  var reportedKind: LinkKind
  var reportedObject: Option[LinkObject]
  var reportedLinkCount: int

  pure val initialKinds: LinkPath -> LinkKind =
    Paths.mapBy(path =>
      if (path == Root or path == D) { Directory }
      else if (path == DA) { File }
      else if (path == DB) { Hardlink }
      else { Missing }
    )

  pure val initialObjects: LinkPath -> Option[LinkObject] =
    Paths.mapBy(path =>
      if (path == DA or path == DB) { Some(O1) } else { None }
    )

  pure val emptySymlinkTargets: LinkPath -> Option[LinkPath] =
    Paths.mapBy(_ => None)

  pure def isLive(kind: LinkKind): bool =
    kind != Missing and kind != Tombstone

  pure def parent(path: LinkPath): LinkPath =
    match path {
      | Root => Root
      | D => Root
      | X => Root
      | DA => D
      | DB => D
    }

  pure def symlinkObjectFor(path: LinkPath): LinkObject =
    match path {
      | Root => O5
      | D => O5
      | DA => O2
      | DB => O3
      | X => O4
    }

  pure def liveAnchorForObject(
    kinds: LinkPath -> LinkKind,
    objects: LinkPath -> Option[LinkObject],
    object: LinkObject
  ): bool =
    Paths.exists(path =>
      (kinds.get(path) == File or kinds.get(path) == Symlink)
        and objects.get(path) == Some(object)
    )

  pure def linkCountForObject(
    kinds: LinkPath -> LinkKind,
    objects: LinkPath -> Option[LinkObject],
    object: LinkObject
  ): int =
    Paths.filter(path =>
      (kinds.get(path) == File
        or kinds.get(path) == Hardlink
        or kinds.get(path) == Symlink)
        and objects.get(path) == Some(object)
    ).size()

  pure def firstHardlinkAlias(
    kinds: LinkPath -> LinkKind,
    objects: LinkPath -> Option[LinkObject],
    object: LinkObject,
    removed: LinkPath
  ): Option[LinkPath] =
    if (DB != removed and kinds.get(DB) == Hardlink and objects.get(DB) == Some(object)) {
      Some(DB)
    } else if (DA != removed and kinds.get(DA) == Hardlink and objects.get(DA) == Some(object)) {
      Some(DA)
    } else if (X != removed and kinds.get(X) == Hardlink and objects.get(X) == Some(object)) {
      Some(X)
    } else {
      None
    }

  pure def effectiveObject(
    objects: LinkPath -> Option[LinkObject],
    path: LinkPath
  ): Option[LinkObject] =
    objects.get(path)

  pure def symlinkCycle(
    kinds: LinkPath -> LinkKind,
    targets: LinkPath -> Option[LinkPath],
    path: LinkPath
  ): bool =
    if (kinds.get(path) != Symlink) {
      false
    } else {
      match targets.get(path) {
        | Some(first) =>
            any {
              first == path,
              all {
                kinds.get(first) == Symlink,
                targets.get(first) == Some(path),
              },
            }
        | None => false
      }
    }

  pure def resolvedStatKind(
    kinds: LinkPath -> LinkKind,
    targets: LinkPath -> Option[LinkPath],
    path: LinkPath
  ): LinkKind =
    if (kinds.get(path) == Symlink) {
      match targets.get(path) {
        | Some(first) =>
            if (kinds.get(first) == Symlink) {
              match targets.get(first) {
                | Some(second) => kinds.get(second)
                | None => Missing
              }
            } else {
              kinds.get(first)
            }
        | None => Missing
      }
    } else {
      kinds.get(path)
    }

  pure def resolvedStatObject(
    kinds: LinkPath -> LinkKind,
    objects: LinkPath -> Option[LinkObject],
    targets: LinkPath -> Option[LinkPath],
    path: LinkPath
  ): Option[LinkObject] =
    if (kinds.get(path) == Symlink) {
      match targets.get(path) {
        | Some(first) =>
            if (kinds.get(first) == Symlink) {
              match targets.get(first) {
                | Some(second) => objects.get(second)
                | None => None
              }
            } else {
              objects.get(first)
            }
        | None => None
      }
    } else {
      objects.get(path)
    }

  pure def hardlinkResult(
    kinds: LinkPath -> LinkKind,
    objects: LinkPath -> Option[LinkObject],
    existing: LinkPath,
    linkPath: LinkPath
  ): LinkResult =
    if (linkPath == Root) {
      InvalidPath
    } else if (kinds.get(existing) == Missing or kinds.get(existing) == Tombstone) {
      NotFound
    } else if (kinds.get(existing) == Directory) {
      IsDirectory
    } else if (objects.get(existing) == None) {
      NotFound
    } else if (isLive(kinds.get(linkPath))) {
      AlreadyExists
    } else if (kinds.get(parent(linkPath)) == Missing or kinds.get(parent(linkPath)) == Tombstone) {
      MissingParent
    } else if (kinds.get(parent(linkPath)) != Directory) {
      NotDirectory
    } else {
      Ok
    }

  pure def symlinkResult(
    kinds: LinkPath -> LinkKind,
    linkPath: LinkPath
  ): LinkResult =
    if (linkPath == Root) {
      InvalidPath
    } else if (isLive(kinds.get(linkPath))) {
      AlreadyExists
    } else if (kinds.get(parent(linkPath)) == Missing or kinds.get(parent(linkPath)) == Tombstone) {
      MissingParent
    } else if (kinds.get(parent(linkPath)) != Directory) {
      NotDirectory
    } else {
      Ok
    }

  pure def unlinkResult(kinds: LinkPath -> LinkKind, path: LinkPath): LinkResult =
    if (path == Root) {
      InvalidPath
    } else if (kinds.get(path) == Missing or kinds.get(path) == Tombstone) {
      NotFound
    } else if (kinds.get(path) == Directory) {
      IsDirectory
    } else {
      Ok
    }

  action init = all {
    kindAt' = initialKinds,
    objectAt' = initialObjects,
    symlinkTarget' = emptySymlinkTargets.set(X, Some(DA)),
    beforeKindAt' = initialKinds,
    beforeObjectAt' = initialObjects,
    beforeSymlinkTarget' = emptySymlinkTargets.set(X, Some(DA)),
    lastDecision' = NoDecision,
    lastResult' = NoResult,
    lastPath' = Root,
    lastExisting' = Root,
    lastLinkPath' = Root,
    reportedKind' = Missing,
    reportedObject' = None,
    reportedLinkCount' = 0,
  }

  action hardlink(existing: LinkPath, linkPath: LinkPath): bool = {
    val result = hardlinkResult(kindAt, objectAt, existing, linkPath)
    all {
      beforeKindAt' = kindAt,
      beforeObjectAt' = objectAt,
      beforeSymlinkTarget' = symlinkTarget,
      kindAt' = if (result == Ok) { kindAt.set(linkPath, Hardlink) } else { kindAt },
      objectAt' =
        if (result == Ok) { objectAt.set(linkPath, effectiveObject(objectAt, existing)) }
        else { objectAt },
      symlinkTarget' = if (result == Ok) { symlinkTarget.set(linkPath, None) } else { symlinkTarget },
      lastDecision' = HardlinkOp,
      lastResult' = result,
      lastPath' = linkPath,
      lastExisting' = existing,
      lastLinkPath' = linkPath,
      reportedKind' = Missing,
      reportedObject' = None,
      reportedLinkCount' = 0,
    }
  }

  action symlink(path: LinkPath, targetPath: LinkPath): bool = {
    val result = symlinkResult(kindAt, path)
    all {
      beforeKindAt' = kindAt,
      beforeObjectAt' = objectAt,
      beforeSymlinkTarget' = symlinkTarget,
      kindAt' = if (result == Ok) { kindAt.set(path, Symlink) } else { kindAt },
      objectAt' =
        if (result == Ok) { objectAt.set(path, Some(symlinkObjectFor(path))) }
        else { objectAt },
      symlinkTarget' = if (result == Ok) { symlinkTarget.set(path, Some(targetPath)) } else { symlinkTarget },
      lastDecision' = SymlinkOp,
      lastResult' = result,
      lastPath' = path,
      lastExisting' = targetPath,
      lastLinkPath' = path,
      reportedKind' = Missing,
      reportedObject' = None,
      reportedLinkCount' = 0,
    }
  }

  action unlink(path: LinkPath): bool = {
    val result = unlinkResult(kindAt, path)
    val object = unwrapOr(objectAt.get(path), O1)
    val alias = firstHardlinkAlias(kindAt, objectAt, object, path)
    val promote =
      all {
        result == Ok,
        kindAt.get(path) == File or kindAt.get(path) == Symlink,
        linkCountForObject(kindAt, objectAt, object) > 1,
      }
    all {
      beforeKindAt' = kindAt,
      beforeObjectAt' = objectAt,
      beforeSymlinkTarget' = symlinkTarget,
      kindAt' =
        if (not(promote) and result == Ok) {
          kindAt.set(path, Tombstone)
        } else if (promote and alias == Some(DB)) {
          kindAt.set(path, Tombstone).set(DB, kindAt.get(path))
        } else if (promote and alias == Some(DA)) {
          kindAt.set(path, Tombstone).set(DA, kindAt.get(path))
        } else if (promote and alias == Some(X)) {
          kindAt.set(path, Tombstone).set(X, kindAt.get(path))
        } else {
          kindAt
        },
      objectAt' = if (result == Ok) { objectAt.set(path, None) } else { objectAt },
      symlinkTarget' =
        if (result != Ok) {
          symlinkTarget
        } else if (promote and kindAt.get(path) == Symlink and alias == Some(DB)) {
          symlinkTarget.set(path, None).set(DB, symlinkTarget.get(path))
        } else if (promote and kindAt.get(path) == Symlink and alias == Some(DA)) {
          symlinkTarget.set(path, None).set(DA, symlinkTarget.get(path))
        } else if (promote and kindAt.get(path) == Symlink and alias == Some(X)) {
          symlinkTarget.set(path, None).set(X, symlinkTarget.get(path))
        } else {
          symlinkTarget.set(path, None)
        },
      lastDecision' = UnlinkOp,
      lastResult' = result,
      lastPath' = path,
      lastExisting' = path,
      lastLinkPath' = path,
      reportedKind' = Missing,
      reportedObject' = None,
      reportedLinkCount' = 0,
    }
  }

  action lstat(path: LinkPath): bool = {
    val kind = kindAt.get(path)
    val object = objectAt.get(path)
    val count = match object {
      | Some(objectId) => linkCountForObject(kindAt, objectAt, objectId)
      | None => 0
    }
    all {
      beforeKindAt' = kindAt,
      beforeObjectAt' = objectAt,
      beforeSymlinkTarget' = symlinkTarget,
      kindAt' = kindAt,
      objectAt' = objectAt,
      symlinkTarget' = symlinkTarget,
      lastDecision' = LstatOp,
      lastResult' = if (isLive(kind)) { Ok } else { NotFound },
      lastPath' = path,
      lastExisting' = path,
      lastLinkPath' = path,
      reportedKind' = kind,
      reportedObject' = object,
      reportedLinkCount' = count,
    }
  }

  action stat(path: LinkPath): bool = {
    val resolvedKind = resolvedStatKind(kindAt, symlinkTarget, path)
    val object = resolvedStatObject(kindAt, objectAt, symlinkTarget, path)
    val count = match object {
      | Some(objectId) => linkCountForObject(kindAt, objectAt, objectId)
      | None => 0
    }
    all {
      beforeKindAt' = kindAt,
      beforeObjectAt' = objectAt,
      beforeSymlinkTarget' = symlinkTarget,
      kindAt' = kindAt,
      objectAt' = objectAt,
      symlinkTarget' = symlinkTarget,
      lastDecision' = StatOp,
      lastResult' =
        if (symlinkCycle(kindAt, symlinkTarget, path)) { TooManySymlinks }
        else if (isLive(resolvedKind)) { Ok }
        else { NotFound },
      lastPath' = path,
      lastExisting' = path,
      lastLinkPath' = path,
      reportedKind' = resolvedKind,
      reportedObject' = object,
      reportedLinkCount' = count,
    }
  }

  action readlink(path: LinkPath): bool = all {
    beforeKindAt' = kindAt,
    beforeObjectAt' = objectAt,
    beforeSymlinkTarget' = symlinkTarget,
    kindAt' = kindAt,
    objectAt' = objectAt,
    symlinkTarget' = symlinkTarget,
    lastDecision' = ReadlinkOp,
    lastResult' = if (kindAt.get(path) == Symlink) { Ok } else { NotSymlink },
    lastPath' = path,
    lastExisting' = path,
    lastLinkPath' = path,
    reportedKind' = kindAt.get(path),
    reportedObject' = objectAt.get(path),
    reportedLinkCount' = 0,
  }

  action step = any {
    {
      nondet existing = oneOf(Paths)
      nondet linkPath = oneOf(Paths)
      hardlink(existing, linkPath)
    },
    {
      nondet linkPath = oneOf(Paths)
      nondet targetPath = oneOf(Paths)
      symlink(linkPath, targetPath)
    },
    {
      nondet path = oneOf(Paths)
      any {
        unlink(path),
        stat(path),
        lstat(path),
        readlink(path),
      }
    },
  }
}
