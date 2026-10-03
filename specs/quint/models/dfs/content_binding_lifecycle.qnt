/// Generation ownership for the complementary content_binding_publication
/// model. A Binding is an opaque, immutable, already-certified exact tuple +
/// payload + manifest + causal-root identity. This slice never fabricates a
/// successful content receipt: publication certification is its interface.
///
/// Migration closes admission, drains admitted legacy writers AND GC, then
/// verifies the copied baseline again. Old handles remain alive; mandatory
/// layout/generation checks reject them after activation. Merely writing a
/// sentinel without this owner-level drain is not an implementation.
///
/// Compaction copies roots (including retention/pin roots supplied by DFS),
/// checks the source stamp at publication, synchronizes the replacement, then
/// publishes its pointer. New acknowledgements wait for directory durability.
/// A held stream keeps its old generation until release. Domain-authorized
/// deletion and exact certified appends are explicit companion-model inputs.
module dfs_content_binding_lifecycle {
  type Binding = (int, int)
  pure val first: Binding = (0, 0)
  pure val second: Binding = (1, 1)
  type State = {
    layout: int, generation: int, admission: bool, parentDurable: bool,
    active: Set[Binding], roots: Set[Binding], stamp: int,
    oldWriter: bool, oldGc: bool, legacyWrites: int,
    activationRequested: bool, migrationPrepared: bool,
    copied: Set[Binding], earlyActivationRejected: bool, staleHandleRejected: bool,
    compacting: bool, candidateSynced: bool, candidate: Set[Binding], candidateStamp: int,
    retired: Set[Binding], retiredRoots: Set[Binding], retiredGeneration: int, compacted: bool, reclaimed: bool,
    reader: bool, readerOpened: bool, readerGeneration: int, readerBinding: Binding,
    readerReleased: bool, reopened: bool, rolledBackCompaction: bool,
    postActivationAppend: bool, deletePermit: bool,
  }
  pure val initial: State = {
    layout: 0, generation: 0, admission: true, parentDurable: true,
    active: Set(first), roots: Set(first), stamp: 0,
    oldWriter: false, oldGc: false, legacyWrites: 0,
    activationRequested: false, migrationPrepared: false,
    copied: Set(), earlyActivationRejected: false, staleHandleRejected: false,
    compacting: false, candidateSynced: false, candidate: Set(), candidateStamp: -1,
    retired: Set(), retiredRoots: Set(), retiredGeneration: -1, compacted: false, reclaimed: false,
    reader: false, readerOpened: false, readerGeneration: -1, readerBinding: first,
    readerReleased: false, reopened: false, rolledBackCompaction: false,
    postActivationAppend: false, deletePermit: false,
  }
  var s: State
  def writable: bool = all { s.layout == 1, s.admission, s.parentDurable }
  action init = s' = initial
  action admitOldWriter = all {
    s.layout == 0, s.admission, not(s.oldWriter), s.legacyWrites == 0,
    s' = s.with("oldWriter", true),
  }
  action finishOldWriter = all {
    s.oldWriter,
    s' = s.with("oldWriter", false).with("legacyWrites", 1)
      .with("active", s.active.union(Set(second))).with("roots", s.roots.union(Set(second)))
      .with("stamp", s.stamp + 1),
  }
  action admitOldGc = all { s.layout == 0, s.admission, not(s.oldGc), s' = s.with("oldGc", true) }
  action finishOldGc = all { s.oldGc, s' = s.with("oldGc", false) }
  action closeAdmission = all {
    s.layout == 0, s.admission,
    s' = s.with("admission", false).with("activationRequested", true),
  }
  action prepareMigration = all {
    s.layout == 0, s.activationRequested,
    // This models a synced, fully validated copy, not just an index snapshot.
    s' = s.with("migrationPrepared", true).with("copied", s.active),
  }
  action rejectEarlyActivation = all {
    s.activationRequested, s.layout == 0, s.oldWriter or s.oldGc,
    s' = s.with("earlyActivationRejected", true),
  }
  action activate = all {
    s.layout == 0, s.activationRequested, s.migrationPrepared,
    not(s.oldWriter), not(s.oldGc), s.copied == s.active,
    // Includes mandatory layout-header and parent sync before routes reopen.
    s' = s.with("layout", 1).with("generation", 1).with("admission", true),
  }
  action rejectStaleHandle = all {
    s.layout == 1,
    // Generation-zero handle still exists, but every mutation checks this
    // mandatory header/generation under current data-owner admission.
    s' = s.with("staleHandleRejected", true),
  }
  action appendCertifiedBinding = all {
    writable, not(s.postActivationAppend),
    // Companion publication model supplies this exact synced binding.
    s' = s.with("active", s.active.union(Set(second))).with("roots", s.roots.union(Set(second)))
      .with("stamp", s.stamp + 1).with("postActivationAppend", true),
  }
  action authorizeVersionDeletion = all { writable, s' = s.with("deletePermit", true) }
  action deleteVersion = all {
    writable, s.deletePermit, s.roots.contains(first),
    // All canonical/retained/pinned/reservation roots have released permission;
    // the physical generation remains until compaction and reader release.
    s' = s.with("roots", s.roots.exclude(Set(first))).with("stamp", s.stamp + 1)
      .with("deletePermit", false),
  }
  action acquireReader = all {
    writable, not(s.readerOpened), s.roots.contains(first),
    s' = s.with("reader", true).with("readerOpened", true)
      .with("readerGeneration", s.generation).with("readerBinding", first),
  }
  action releaseReader = all {
    s.reader,
    s' = s.with("reader", false).with("readerReleased", true),
  }
  action prepareCompaction = all {
    writable, not(s.compacted),
    s' = s.with("compacting", true).with("candidate", s.roots)
      .with("candidateStamp", s.stamp).with("candidateSynced", false),
  }
  action syncCandidate = all { s.compacting, s' = s.with("candidateSynced", true) }
  action publishCompaction = all {
    writable, s.compacting, s.candidateSynced, s.candidateStamp == s.stamp,
    s.candidate == s.roots,
    // The replacement also reproduces the exact logical replay result;
    // deletion tombstones/fallback fences cannot disappear with dead bytes.
    s' = s.with("retired", s.active).with("retiredRoots", s.roots).with("retiredGeneration", s.generation)
      .with("active", s.candidate).with("generation", s.generation + 1)
      .with("parentDurable", false).with("compacting", false).with("compacted", true),
  }
  action syncDirectory = all { s.compacted, not(s.parentDurable), s' = s.with("parentDurable", true) }
  action reclaimRetired = all {
    s.compacted, s.parentDurable, s.retiredGeneration >= 0,
    not(s.reader) or s.readerGeneration != s.retiredGeneration,
    s' = s.with("retired", Set()).with("retiredRoots", Set()).with("retiredGeneration", -1).with("reclaimed", true),
  }
  action crashBeforeDirectorySync(keepNew: bool): bool = all {
    s.compacted, not(s.parentDurable), not(s.reopened),
    // Both pointer choices were previously fully synced and contain every
    // root. Recovery verifies and syncs the chosen pointer before admission.
    s' = s.with("active", if (keepNew) { s.active } else { s.retired })
      .with("roots", if (keepNew) { s.roots } else { s.retiredRoots })
      .with("generation", if (keepNew) { s.generation } else { s.retiredGeneration })
      .with("parentDurable", true).with("reader", false).with("reopened", true)
      .with("retired", if (keepNew) { s.retired } else { Set() })
      .with("retiredRoots", if (keepNew) { s.retiredRoots } else { Set() })
      .with("retiredGeneration", if (keepNew) { s.retiredGeneration } else { -1 })
      .with("rolledBackCompaction", not(keepNew)),
  }
  action step = {
    nondet keepNew = Set(false, true).oneOf()
    any {
      admitOldWriter, finishOldWriter, admitOldGc, finishOldGc,
      closeAdmission, prepareMigration, rejectEarlyActivation, activate,
      rejectStaleHandle, appendCertifiedBinding, authorizeVersionDeletion,
      deleteVersion, acquireReader, releaseReader, prepareCompaction,
      syncCandidate, publishCompaction, syncDirectory, reclaimRetired,
      crashBeforeDirectorySync(keepNew), s' = s,
    }
  }
  action lifecycleCoverageStep =
    if (s.layout == 0 and s.admission and s.legacyWrites == 0 and not(s.oldWriter)) { admitOldWriter }
    else if (s.layout == 0 and s.admission) { closeAdmission }
    else if (s.oldWriter and not(s.earlyActivationRejected)) { rejectEarlyActivation }
    else if (s.oldWriter) { finishOldWriter }
    else if (s.layout == 0 and (not(s.migrationPrepared) or s.copied != s.active)) { prepareMigration }
    else if (s.layout == 0) { activate }
    else if (not(s.staleHandleRejected)) { rejectStaleHandle }
    else if (not(s.readerOpened)) { acquireReader }
    else if (not(s.compacted) and not(s.compacting)) { prepareCompaction }
    else if (s.compacting and not(s.candidateSynced)) { syncCandidate }
    else if (s.compacting and s.candidateStamp != s.stamp) { prepareCompaction }
    else if (s.compacting) { publishCompaction }
    else if (not(s.parentDurable)) { syncDirectory }
    else if (s.reader) { releaseReader }
    else if (not(s.reclaimed) and s.retiredGeneration >= 0) { reclaimRetired }
    else { s' = s }
  action coverageStep = any { step, lifecycleCoverageStep }
}
