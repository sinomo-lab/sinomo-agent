module dfs_content_binding_lifecycle_safety {
  import dfs_content_binding_lifecycle.* from "../../models/dfs/content_binding_lifecycle"
  def heldReaderExists: bool = not(s.reader) or any {
    s.readerGeneration == s.generation and s.active.contains(s.readerBinding),
    s.readerGeneration == s.retiredGeneration and s.retired.contains(s.readerBinding),
  }
  def safety: bool = all {
    s.roots.subseteq(s.active), heldReaderExists,
    s.layout == 1 implies all { not(s.oldWriter), not(s.oldGc), s.generation >= 1 },
    s.layout == 0 implies s.generation == 0,
    not(s.parentDurable) implies not(writable),
    s.reclaimed implies s.retired == Set(),
    s.compacted and not(s.parentDurable) implies s.roots.subseteq(s.retired),
    s.compacted and not(s.parentDurable) implies s.roots == s.retiredRoots,
    // Every copied binding keeps its exact opaque identity through activation
    // and compaction; no tuple/hash-only re-association exists in this model.
    s.active.subseteq(Set(first, second)),
  }
  def witnessActivationWaitsForOldOperation: bool = all {
    s.earlyActivationRejected, s.layout == 0, s.oldWriter or s.oldGc,
  }
  def witnessActivationCarriesDrainedLegacyBaseline: bool = all {
    s.layout == 1, s.legacyWrites == 1, s.active.contains(second), s.copied.contains(second),
  }
  def witnessStaleHandleCannotWriteNewLayout: bool = all { s.layout == 1, s.staleHandleRejected }
  def witnessCompactionKeepsBindingAndReader: bool = all {
    s.compacted, s.reader, s.readerGeneration == s.retiredGeneration,
    s.retired.contains(s.readerBinding), s.roots.subseteq(s.active),
  }
  def witnessCompactionReclaimsAfterReaderRelease: bool = all {
    s.compacted, s.reclaimed, s.readerOpened, s.readerReleased, not(s.reader),
  }
  def witnessCrashBeforeDirectorySyncCanSelectOldGeneration: bool = all {
    s.reopened, s.rolledBackCompaction, s.parentDurable, s.roots.subseteq(s.active),
  }
}
