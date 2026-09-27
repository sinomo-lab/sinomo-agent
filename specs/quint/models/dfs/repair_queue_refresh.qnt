/// Target refinement for repair-queue refresh without unconditional SOBJ replay.
/// A fingerprint names the exact snapshot/log byte generation, not a timestamp
/// or numeric queue head. Hash collision resistance is abstracted. Supported
/// owned mutations serialize with refresh; arbitrary simultaneous foreign
/// writers are NOT made safe by the existing process-local mutex. An unstable
/// fingerprint read is rejected instead of granting a cached-view proof.
/// This slice models two durable repair identities and one retry counter. It
/// excludes queue scheduling fairness, networking and filesystem syscalls.
module dfs_repair_queue_refresh {
  type Queue = { a: bool, b: bool, retries: int }
  type State = {
    durable: Queue, visible: Queue, cache: Queue,
    diskHead: int, visibleHead: int, cacheHead: int,
    fingerprint: int, cachedFingerprint: int, revision: int,
    loaded: bool, refreshRequested: bool, uncertain: bool,
    corrupt: bool, torn: bool, unsupported: bool,
    viewReady: bool, viewRevision: int, viewFingerprint: int, viewHead: int,
    failed: bool, rejected: bool, fast: bool, replayed: bool, resynced: bool,
    externalAppendSeen: bool, replacementSeen: bool, truncationSeen: bool,
    corruptionSeen: bool, unstableSeen: bool, restarted: bool,
    staleRejected: bool, retryPreserved: bool, noOpAcknowledged: bool,
    sameHeadReplacement: bool, unsupportedReplayed: bool, reconcileAccepted: bool, abaRejected: bool,
  }
  pure val empty: Queue = { a: false, b: false, retries: 0 }
  pure val initial: State = {
    durable: empty, visible: empty, cache: empty,
    diskHead: 0, visibleHead: 0, cacheHead: 0,
    fingerprint: 0, cachedFingerprint: -1, revision: 0,
    loaded: false, refreshRequested: true, uncertain: false,
    corrupt: false, torn: false, unsupported: false,
    viewReady: false, viewRevision: -1, viewFingerprint: -1, viewHead: -1,
    failed: false, rejected: false, fast: false, replayed: false, resynced: false,
    externalAppendSeen: false, replacementSeen: false, truncationSeen: false,
    corruptionSeen: false, unstableSeen: false, restarted: false,
    staleRejected: false, retryPreserved: false, noOpAcknowledged: false,
    sameHeadReplacement: false, unsupportedReplayed: false, reconcileAccepted: false, abaRejected: false,
  }
  var s: State
  def exactCache: bool = all {
    s.loaded, not(s.uncertain), not(s.corrupt), not(s.torn),
    s.cachedFingerprint == s.fingerprint,
  }
  def budget: bool = s.fingerprint < 7 and s.revision < 9
  action init = s' = initial
  action requestRefresh = all {
    not(s.refreshRequested), s' = s.with("refreshRequested", true),
  }
  action unchangedRefresh = all {
    s.refreshRequested, exactCache, not(s.unsupported),
    s' = s.with("refreshRequested", false).with("fast", true),
  }
  action replayRefresh = all {
    s.refreshRequested, not(s.corrupt), not(s.uncertain), budget,
    not(s.loaded) or s.cachedFingerprint != s.fingerprint or s.unsupported or s.torn,
    s' = s.with("cache", s.durable).with("cacheHead", s.diskHead)
      .with("cachedFingerprint", s.fingerprint).with("loaded", true)
      .with("refreshRequested", false).with("revision", s.revision + 1)
      .with("replayed", true).with("unsupportedReplayed", s.unsupportedReplayed or s.unsupported),
  }
  action rejectCorruptRefresh = all {
    s.refreshRequested, s.corrupt,
    s' = s.with("loaded", false).with("refreshRequested", false)
      .with("rejected", true).with("corruptionSeen", true),
  }
  action rejectUnstableFingerprintRead = all {
    s.refreshRequested, not(s.unstableSeen),
    s' = s.with("loaded", false).with("unstableSeen", true).with("rejected", true),
  }
  /// A temporary B image may be decoded between two observations of A.
  /// The replay candidate carries B's consumed-byte fingerprint, so comparing
  /// that proof with restored A rejects it; before/after-A equality is useless.
  action rejectAbaSubstitution = all {
    s.refreshRequested, not(s.uncertain), not(s.corrupt), not(s.abaRejected),
    val consumedFingerprint = s.fingerprint + 100
    consumedFingerprint != s.fingerprint,
    s' = s.with("loaded", false).with("rejected", true).with("abaRejected", true),
  }
  action ownedEnqueue = all {
    exactCache, not(s.durable.a), budget,
    val updated = s.durable.with("a", true)
    s' = s.with("durable", updated).with("visible", updated).with("cache", updated)
      .with("diskHead", s.diskHead + 1).with("visibleHead", s.diskHead + 1)
      .with("cacheHead", s.diskHead + 1).with("fingerprint", s.fingerprint + 1)
      .with("cachedFingerprint", s.fingerprint + 1).with("revision", s.revision + 1),
  }
  action ownedRetryUpdate = all {
    exactCache, s.durable.a, s.durable.retries == 0, budget,
    val updated = s.durable.with("retries", 1)
    s' = s.with("durable", updated).with("visible", updated).with("cache", updated)
      .with("diskHead", s.diskHead + 1).with("visibleHead", s.diskHead + 1)
      .with("cacheHead", s.diskHead + 1).with("fingerprint", s.fingerprint + 1)
      .with("cachedFingerprint", s.fingerprint + 1).with("revision", s.revision + 1),
  }
  action duplicateEnqueueNoOp = all {
    exactCache, s.durable.a,
    s' = s.with("noOpAcknowledged", true)
      .with("retryPreserved", s.retryPreserved or s.cache.retries == 1),
  }
  action failedCompleteAppendSync = all {
    exactCache, not(s.durable.a), not(s.failed), budget,
    s' = s.with("visible", s.durable.with("a", true))
      .with("visibleHead", s.diskHead + 1).with("fingerprint", s.fingerprint + 1)
      .with("loaded", false).with("uncertain", true).with("failed", true)
      .with("refreshRequested", true),
  }
  action failedTornAppendSync = all {
    exactCache, not(s.failed), budget,
    s' = s.with("fingerprint", s.fingerprint + 1).with("loaded", false)
      .with("uncertain", true).with("failed", true).with("torn", true)
      .with("refreshRequested", true),
  }
  action resyncUncertainCompleteFrame = all {
    s.uncertain, not(s.corrupt),
    s' = s.with("durable", s.visible).with("diskHead", s.visibleHead)
      .with("uncertain", false).with("resynced", true).with("refreshRequested", true)
      .with("torn", false).with("truncationSeen", s.truncationSeen or s.torn),
  }
  action externalAppend = all {
    not(s.corrupt), not(s.uncertain), not(s.durable.b), budget,
    val updated = s.durable.with("b", true)
    s' = s.with("durable", updated).with("visible", updated)
      .with("diskHead", s.diskHead + 1).with("visibleHead", s.diskHead + 1)
      .with("fingerprint", s.fingerprint + 1).with("externalAppendSeen", true),
  }
  /// Snapshot/rotation replacement retains the semantic queue and numeric head
  /// but replaces physical history. It invalidates stale maintenance tokens.
  action externalReplacement = all {
    not(s.uncertain), not(s.replacementSeen), budget,
    val updated = if (s.durable.a) { s.durable.with("retries", 1) } else { s.durable }
    s' = s.with("durable", updated).with("visible", updated)
      .with("fingerprint", s.fingerprint + 1).with("replacementSeen", true)
      .with("sameHeadReplacement", s.viewReady and s.viewHead == s.diskHead),
  }
  action externalTornSuffix = all {
    not(s.corrupt), not(s.uncertain), not(s.truncationSeen), budget,
    s' = s.with("fingerprint", s.fingerprint + 1).with("torn", true),
  }
  action truncateTornSuffix = all {
    s.torn, not(s.corrupt), budget,
    s' = s.with("fingerprint", s.fingerprint + 1).with("torn", false)
      .with("loaded", false).with("refreshRequested", true).with("truncationSeen", true),
  }
  action externalCorruption = all {
    not(s.corrupt), not(s.uncertain), budget,
    s' = s.with("corrupt", true).with("fingerprint", s.fingerprint + 1),
  }
  action unsupportedFingerprint = all {
    not(s.unsupported), s' = s.with("unsupported", true).with("refreshRequested", true),
  }
  action captureMaintenanceView = all {
    exactCache, not(s.refreshRequested),
    s' = s.with("viewReady", true).with("viewRevision", s.revision)
      .with("viewFingerprint", s.fingerprint).with("viewHead", s.diskHead),
  }
  action acceptMaintenanceRetry = all {
    s.viewReady, exactCache, s.viewRevision == s.revision,
    s.viewFingerprint == s.fingerprint, s.viewHead == s.diskHead,
    s.durable.a, s.durable.retries == 0, budget,
    val updated = s.durable.with("retries", 1)
    s' = s.with("durable", updated).with("visible", updated).with("cache", updated)
      .with("diskHead", s.diskHead + 1).with("visibleHead", s.diskHead + 1)
      .with("cacheHead", s.diskHead + 1).with("fingerprint", s.fingerprint + 1)
      .with("cachedFingerprint", s.fingerprint + 1).with("revision", s.revision + 1)
      .with("viewReady", false).with("reconcileAccepted", true),
  }
  action rejectStaleMaintenance = all {
    s.viewReady, any {
      not(exactCache), s.viewRevision != s.revision,
      s.viewFingerprint != s.fingerprint, s.viewHead != s.diskHead,
    },
    s' = s.with("viewReady", false).with("staleRejected", true),
  }
  action restart = all {
    not(s.restarted),
    s' = s.with("loaded", false).with("viewReady", false)
      .with("refreshRequested", true).with("restarted", true),
  }
  action step = any {
    requestRefresh, unchangedRefresh, replayRefresh, rejectCorruptRefresh, rejectAbaSubstitution,
    rejectUnstableFingerprintRead, ownedEnqueue, ownedRetryUpdate,
    duplicateEnqueueNoOp, failedCompleteAppendSync, failedTornAppendSync, resyncUncertainCompleteFrame,
    externalAppend, externalReplacement, externalTornSuffix, truncateTornSuffix,
    externalCorruption, unsupportedFingerprint, captureMaintenanceView,
    acceptMaintenanceRetry, rejectStaleMaintenance, restart,
  }
}
