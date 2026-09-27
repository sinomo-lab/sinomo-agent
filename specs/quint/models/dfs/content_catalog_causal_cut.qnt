/// Target refinement of local reservation/cut ordering for content GC.
///
/// One durable reservation can be renewed once while a root catalog is in
/// flight. A durable sentinel frontier is advanced before its first page;
/// reservation writes share that lock and record its current epoch. A later
/// install cannot move the catalog's causal fence. Wall
/// clocks may move backward and forward; remote clocks never order local
/// reservations. Positive historical coverage remains usable for legacy
/// catalogs, while missing causal evidence cannot authorize negative release.
///
/// This slice excludes blob bytes, metadata authority/quorum, actual GC
/// deletion, and real-time grace measurement. Those are separate contracts.
/// It verifies which reservation generation an absence observation may reject.
module dfs_content_catalog_causal_cut {
  type Phase = Idle | Reserved | Scanning | Captured | Installed | Finished
  type Release = NoneReleased | PositiveRelease | NegativeRelease
  type State = {
    phase: Phase, wall: int, remoteOffset: int, frontier: int,
    reservation: bool, revision: int, stamp: int, legacyStamp: int,
    reservationEpochKnown: bool, matchingIncarnation: bool, oldWriterRejected: bool,
    duplicate: bool, duplicateStamp: int, duplicateWall: int,
    duplicateKnown: bool, duplicateForeign: bool, duplicateWallBugObserved: bool,
    canonicalRoot: bool, cutRevision: int, cutFence: int, cutStartWall: int,
    cutContainsRoot: bool, hasFence: bool, installWall: int,
    continued: bool, relabelRejected: bool, restarted: bool,
    stale: bool, graceElapsed: bool, release: Release,
    releasedRevision: int, failedCutSync: bool,
    legacyInstallBugObserved: bool, timestampOnlyBugObserved: bool,
  }
  pure val initial: State = {
    phase: Idle, wall: 1, remoteOffset: 0, frontier: 0,
    reservation: false, revision: 0, stamp: 0, legacyStamp: 0,
    reservationEpochKnown: false, matchingIncarnation: true, oldWriterRejected: false,
    duplicate: false, duplicateStamp: 0, duplicateWall: 0,
    duplicateKnown: true, duplicateForeign: false, duplicateWallBugObserved: false,
    canonicalRoot: false, cutRevision: 0, cutFence: 0, cutStartWall: 0,
    cutContainsRoot: false, hasFence: false, installWall: 0,
    continued: false, relabelRejected: false, restarted: false,
    stale: false, graceElapsed: true, release: NoneReleased,
    releasedRevision: 0, failedCutSync: false,
    legacyInstallBugObserved: false, timestampOnlyBugObserved: false,
  }
  var s: State
  pure def max2(a: int, b: int): int = if (a > b) { a } else { b }
  def postCutRenewal: bool = s.revision > s.cutRevision
  def allCopiesKnown: bool = all {
    s.reservationEpochKnown,
    not(s.duplicate) or (s.duplicateKnown and not(s.duplicateForeign)),
  }
  def aggregateStamp: int = if (s.duplicate) { max2(s.stamp, s.duplicateStamp) } else { s.stamp }
  def duplicateWallWouldDiscardNewer: bool = all {
    s.phase == Installed, s.duplicate, s.hasFence, s.legacyStamp > s.duplicateWall,
    s.stamp < s.cutFence, s.duplicateStamp >= s.cutFence,
  }
  def localFreshnessValid: bool = all {
    not(s.stale), s.cutStartWall <= s.installWall, s.installWall <= s.wall,
  }
  def legacyInstallWouldRejectNewer: bool = all {
    s.phase == Installed, postCutRenewal, not(s.cutContainsRoot),
    localFreshnessValid, s.installWall > s.legacyStamp,
  }
  def timestampOnlyWouldRejectNewer: bool = all {
    s.phase == Installed, postCutRenewal, not(s.cutContainsRoot),
    localFreshnessValid, s.cutStartWall > s.legacyStamp,
  }
  action init = s' = initial
  action reserve = all {
    s.phase == Idle,
    s' = s.with("phase", Reserved).with("reservation", true).with("revision", 1)
      .with("stamp", s.frontier).with("reservationEpochKnown", true)
      .with("legacyStamp", max2(s.wall, 1)),
  }
  action reserveBeforeUpgrade = all {
    s.phase == Idle,
    s' = s.with("phase", Reserved).with("reservation", true).with("revision", 1)
      .with("legacyStamp", max2(s.wall, 1)),
  }
  action moveLocalClock(now: int): bool = all {
    Set(1, 2, 3, 4).contains(now), s.phase != Finished,
    s' = s.with("wall", now),
  }
  action moveRemoteClock(offset: int): bool = all {
    Set(-2, 0, 2).contains(offset), s.phase != Finished,
    s' = s.with("remoteOffset", offset),
  }
  action beginScan = all {
    s.phase == Reserved,
    val fence = s.frontier + 1
    s' = s.with("phase", Scanning).with("frontier", fence).with("cutFence", fence)
      .with("cutStartWall", s.wall).with("cutRevision", s.revision).with("hasFence", true),
  }
  action failedBeginScan = all {
    s.phase == Reserved, not(s.failedCutSync),
    s' = s.with("failedCutSync", true),
  }
  action captureFreshFirstPage = all {
    s.phase == Scanning,
    s' = s.with("phase", Captured).with("cutContainsRoot", s.canonicalRoot),
  }
  action commitAndRenew = all {
    s.reservation, s.revision == 1, s.phase != Finished,
    s' = s.with("canonicalRoot", true).with("revision", 2)
      .with("stamp", s.frontier).with("reservationEpochKnown", true)
      .with("legacyStamp", max2(s.wall, s.legacyStamp + 1)),
  }
  action renewAsDuplicate = all {
    s.reservation, s.revision == 1, not(s.duplicate), s.phase != Finished,
    s' = s.with("duplicate", true).with("duplicateStamp", s.frontier)
      .with("duplicateWall", s.wall).with("revision", 2).with("canonicalRoot", true),
  }
  action addUncertainDuplicate(foreign: bool): bool = all {
    s.reservation, not(s.duplicate), s.phase != Finished,
    s' = s.with("duplicate", true).with("duplicateKnown", foreign)
      .with("duplicateForeign", foreign),
  }
  action continuation = all {
    s.phase == Captured, not(s.continued),
    s' = s.with("continued", true),
  }
  /// A cached continuation cannot be relabeled as a later observation.
  action rejectRelabeledContinuation = all {
    s.phase == Captured, not(s.relabelRejected),
    s' = s.with("relabelRejected", true),
  }
  action install = all {
    s.phase == Captured,
    s' = s.with("phase", Installed).with("installWall", s.wall),
  }
  action loadLegacy(positive: bool): bool = all {
    s.phase == Reserved,
    s' = s.with("phase", Installed).with("hasFence", false)
      .with("cutStartWall", s.wall).with("installWall", s.wall)
      .with("cutRevision", s.revision).with("cutContainsRoot", positive),
  }
  action restart = all {
    s.phase == Installed, not(s.restarted),
    s' = s.with("restarted", true),
  }
  action rejectOldWriter = all {
    s.hasFence, not(s.oldWriterRejected),
    s' = s.with("oldWriterRejected", true),
  }
  action replaceCatalogIncarnation = all {
    s.phase == Installed, s.matchingIncarnation,
    s' = s.with("matchingIncarnation", false),
  }
  action expire = all {
    s.phase == Installed, not(s.stale),
    s' = s.with("stale", true),
  }
  action waitForGrace = all {
    s.phase == Installed, s.graceElapsed,
    s' = s.with("graceElapsed", false),
  }
  action reconcile = all {
    s.phase == Installed,
    val negative = all {
      not(s.cutContainsRoot), s.hasFence, allCopiesKnown,
      s.matchingIncarnation, localFreshnessValid, s.graceElapsed,
      aggregateStamp < s.cutFence,
    }
    val release = if (s.cutContainsRoot) { PositiveRelease }
      else if (negative) { NegativeRelease } else { NoneReleased }
    s' = s.with("phase", Finished).with("release", release)
      .with("reservation", release == NoneReleased)
      .with("duplicate", s.duplicate and release == NoneReleased)
      .with("releasedRevision", if (release == NoneReleased) { 0 } else { s.revision })
      .with("legacyInstallBugObserved", legacyInstallWouldRejectNewer)
      .with("timestampOnlyBugObserved", timestampOnlyWouldRejectNewer)
      .with("duplicateWallBugObserved", duplicateWallWouldDiscardNewer),
  }
  action stutter = s' = s
  action step = {
    nondet now = Set(1, 2, 3, 4).oneOf()
    nondet offset = Set(-2, 0, 2).oneOf()
    nondet positive = Set(false, true).oneOf()
    any {
      reserve, reserveBeforeUpgrade, moveLocalClock(now), moveRemoteClock(offset), beginScan, failedBeginScan,
      captureFreshFirstPage, commitAndRenew, renewAsDuplicate, addUncertainDuplicate(positive),
      continuation, rejectRelabeledContinuation,
      install, loadLegacy(positive), restart, rejectOldWriter, replaceCatalogIncarnation,
      expire, waitForGrace, reconcile, stutter,
    }
  }

  /// Deterministic schedules choose only ordinary protocol actions; symbolic
  /// verification continues to use the unrestricted transition relation.
  action delayedInstallCoverageStep =
    if (s.phase == Idle) { reserve }
    else if (s.phase == Reserved) { beginScan }
    else if (s.phase == Scanning) { captureFreshFirstPage }
    else if (s.phase == Captured and s.revision == 1) { commitAndRenew }
    else if (s.phase == Captured and s.wall != 4) { moveLocalClock(4) }
    else if (s.phase == Captured) { install }
    else if (s.phase == Installed) { reconcile }
    else { stutter }
  action clockRollbackCoverageStep =
    if (s.phase == Idle) { reserve }
    else if (s.phase == Reserved and s.wall != 3) { moveLocalClock(3) }
    else if (s.phase == Reserved) { beginScan }
    else if (s.phase == Scanning) { captureFreshFirstPage }
    else if (s.phase == Captured and s.revision == 1 and s.wall != 1) { moveLocalClock(1) }
    else if (s.phase == Captured and s.revision == 1) { commitAndRenew }
    else if (s.phase == Captured and s.wall != 4) { moveLocalClock(4) }
    else if (s.phase == Captured) { install }
    else if (s.phase == Installed) { reconcile }
    else { stutter }
  action duplicateRollbackCoverageStep =
    if (s.phase == Idle and s.wall != 3) { moveLocalClock(3) }
    else if (s.phase == Idle) { reserve }
    else if (s.phase == Reserved) { beginScan }
    else if (s.phase == Scanning) { captureFreshFirstPage }
    else if (s.phase == Captured and s.revision == 1 and s.wall != 1) { moveLocalClock(1) }
    else if (s.phase == Captured and s.revision == 1) { renewAsDuplicate }
    else if (s.phase == Captured and s.wall != 4) { moveLocalClock(4) }
    else if (s.phase == Captured) { install }
    else if (s.phase == Installed) { reconcile }
    else { stutter }
  action coverageStep = any {
    step, delayedInstallCoverageStep, clockRollbackCoverageStep, duplicateRollbackCoverageStep,
  }
}
