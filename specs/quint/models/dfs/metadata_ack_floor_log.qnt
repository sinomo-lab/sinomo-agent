/// Target hardening: durable, local ACK-floor append storage.
///
/// One instance models either the prefix floor or the authority floor. They
/// remain independent logical records and files. Floor payload verification
/// (exact prefix, operation identity, fingerprint enrichment and legacy None)
/// is an unchanged prerequisite; a floor value here is an opaque, validated
/// record at one of three ordered journal positions. This model isolates the
/// storage cuts, migration, cache fencing and durability-before-ACK contract.
/// Incomplete EOF is an unacknowledged partial append. A complete malformed
/// record, wrong-kind record, or missing migrated log is a fail-closed fault.
module dfs_metadata_ack_floor_log {
  type FloorKind = PrefixFloor | AuthorityFloor
  type Layout = LegacySidecar | MigrationPrepared | AppendLog
  type Tail = CleanTail | IncompleteTail | CorruptTail
  type Event =
    | Initialized | MigrationCheckpointSynced | LegacyAdvanced
    | MigrationBaselineMerged | SentinelSynced | CompleteAppendVisible
    | IncompleteAppendVisible | AppendSyncFailed | UncertainTailObserved
    | TailSynced | AckReturned | IncompleteTailTruncated
    | OtherHandleAdvanced | CheckpointPrepared | CheckpointSynced
    | CheckpointInstalled | CheckpointParentSynced | ProcessRestarted
    | MigratedLogLost | CompleteRecordCorrupt | WrongKindRecord
    | LegacyReaderRejected

  type FloorState = {
    kind: FloorKind, layout: Layout, legacy: int,
    visible: int, durable: int, cached: int, acked: int,
    prefixAcked: int, authorityAcked: int,
    stamp: int, cacheStamp: int, certifiedStamp: int,
    exists: bool, tail: Tail, blocked: bool, parentDurable: bool,
    syncFailed: bool, uncertainObserved: bool, externalAdvance: bool,
    rotating: bool, checkpoint: int, checkpointSynced: bool, rotated: bool,
    restarted: bool, oldReaderRejected: bool, last: Event,
  }

  pure def initial(kind: FloorKind, baseline: int): FloorState = {
    kind: kind, layout: LegacySidecar, legacy: baseline,
    visible: 0, durable: 0, cached: 0, acked: baseline,
    prefixAcked: if (kind == PrefixFloor) baseline else 0,
    authorityAcked: if (kind == AuthorityFloor) baseline else 0,
    stamp: 0, cacheStamp: -1, certifiedStamp: -1,
    exists: false, tail: CleanTail, blocked: false, parentDurable: false,
    syncFailed: false, uncertainObserved: false, externalAdvance: false,
    rotating: false, checkpoint: 0, checkpointSynced: false, rotated: false,
    restarted: false, oldReaderRejected: false, last: Initialized,
  }
  var s: FloorState

  def ackAllowed: bool = all {
    s.layout == AppendLog, s.exists, not(s.blocked), s.tail == CleanTail,
    s.parentDurable, not(s.rotating), s.visible == 2,
    s.cached == s.visible, s.cacheStamp == s.stamp,
    s.certifiedStamp == s.stamp, s.durable >= s.cached,
  }

  action init = {
    nondet kind = Set(PrefixFloor, AuthorityFloor).oneOf()
    nondet baseline = Set(0, 1).oneOf()
    s' = initial(kind, baseline)
  }

  /// File and parent directory are synced before the new log becomes a
  /// readable migration checkpoint. No optimized ACK precedes the sentinel.
  action prepareMigration = all {
    s.layout == LegacySidecar,
    s' = s.with("layout", MigrationPrepared).with("exists", true)
      .with("visible", s.legacy).with("durable", s.legacy)
      .with("cached", s.legacy).with("stamp", 1)
      .with("cacheStamp", 1).with("certifiedStamp", 1)
      .with("parentDurable", true).with("last", MigrationCheckpointSynced),
  }

  /// A crash before sentinel publication can let an old writer advance its
  /// still-legacy sidecar. Migration must merge that newer durable baseline.
  action advanceLegacy = all {
    s.layout == MigrationPrepared, s.legacy < 2,
    s' = s.with("legacy", 2).with("acked", 2)
      .with("prefixAcked", if (s.kind == PrefixFloor) 2 else s.prefixAcked)
      .with("authorityAcked", if (s.kind == AuthorityFloor) 2 else s.authorityAcked)
      .with("last", LegacyAdvanced),
  }
  action mergeLegacy = all {
    s.layout == MigrationPrepared, s.durable < s.legacy,
    s' = s.with("visible", s.legacy).with("durable", s.legacy)
      .with("cached", s.legacy).with("stamp", s.stamp + 1)
      .with("cacheStamp", s.stamp + 1).with("certifiedStamp", s.stamp + 1)
      .with("last", MigrationBaselineMerged),
  }
  action publishSentinel = all {
    s.layout == MigrationPrepared, s.exists, s.parentDurable,
    s.tail == CleanTail, s.durable >= s.legacy,
    s' = s.with("layout", AppendLog).with("last", SentinelSynced),
  }

  action appendComplete = all {
    s.layout == AppendLog, s.exists, not(s.blocked), not(s.rotating),
    s.parentDurable, s.tail == CleanTail, s.visible < 2,
    s' = s.with("visible", 2).with("stamp", s.stamp + 1)
      .with("last", CompleteAppendVisible),
  }
  action appendIncomplete = all {
    s.layout == AppendLog, s.exists, not(s.blocked), not(s.rotating),
    s.parentDurable, s.tail == CleanTail, s.visible < 2,
    s' = s.with("tail", IncompleteTail).with("stamp", s.stamp + 1)
      .with("cacheStamp", -1).with("certifiedStamp", -1)
      .with("last", IncompleteAppendVisible),
  }
  action failSync = all {
    s.layout == AppendLog, not(s.blocked), s.tail == CleanTail,
    s.visible > s.durable, not(s.syncFailed),
    s' = s.with("syncFailed", true).with("cacheStamp", -1)
      .with("certifiedStamp", -1).with("last", AppendSyncFailed),
  }
  /// Merely decoding the complete record after failed fsync is not durable
  /// evidence. A cache refresh must not turn an idempotent retry into an ACK.
  action observeUncertain = all {
    s.syncFailed, not(s.uncertainObserved), s.visible > s.durable,
    not(s.blocked), s.tail == CleanTail,
    s' = s.with("cached", s.visible).with("cacheStamp", s.stamp)
      .with("uncertainObserved", true).with("last", UncertainTailObserved),
  }
  action syncTail = all {
    s.layout == AppendLog, s.exists, not(s.blocked), not(s.rotating),
    s.tail == CleanTail, s.parentDurable,
    any { s.certifiedStamp != s.stamp, s.cacheStamp != s.stamp },
    s' = s.with("durable", s.visible).with("cached", s.visible)
      .with("certifiedStamp", s.stamp).with("cacheStamp", s.stamp)
      .with("last", TailSynced),
  }
  action acknowledge = all {
    ackAllowed,
    s' = s.with("acked", s.cached)
      .with("prefixAcked", if (s.kind == PrefixFloor) s.cached else s.prefixAcked)
      .with("authorityAcked", if (s.kind == AuthorityFloor) s.cached else s.authorityAcked)
      .with("last", AckReturned),
  }
  action truncateIncomplete = all {
    s.tail == IncompleteTail, not(s.blocked),
    s' = s.with("tail", CleanTail).with("stamp", s.stamp + 1)
      .with("cacheStamp", -1).with("certifiedStamp", -1)
      .with("last", IncompleteTailTruncated),
  }
  /// A second handle holds the common metadata write lock while appending and
  /// syncing. The first handle's cached file identity/length is now obsolete.
  action externalAppend = all {
    s.layout == AppendLog, s.exists, not(s.blocked), not(s.rotating),
    s.parentDurable, s.tail == CleanTail, s.visible < 2,
    s' = s.with("visible", 2).with("durable", 2)
      .with("stamp", s.stamp + 1).with("certifiedStamp", s.stamp + 1)
      .with("externalAdvance", true).with("last", OtherHandleAdvanced),
  }

  action prepareCheckpoint = all {
    s.layout == AppendLog, not(s.blocked), s.exists,
    s.acked == 2, s.visible == s.durable, s.tail == CleanTail,
    not(s.rotating), not(s.rotated),
    s' = s.with("rotating", true).with("checkpoint", s.durable)
      .with("last", CheckpointPrepared),
  }
  action syncCheckpoint = all {
    s.rotating, not(s.checkpointSynced), not(s.blocked),
    s' = s.with("checkpointSynced", true).with("last", CheckpointSynced),
  }
  action installCheckpoint = all {
    s.rotating, s.checkpointSynced, not(s.blocked),
    s' = s.with("visible", s.checkpoint).with("durable", s.checkpoint)
      .with("stamp", s.stamp + 1).with("cacheStamp", -1).with("certifiedStamp", -1)
      .with("parentDurable", false).with("rotating", false).with("rotated", true)
      .with("last", CheckpointInstalled),
  }
  action syncCheckpointParent = all {
    s.rotated, not(s.parentDurable), not(s.blocked),
    s' = s.with("parentDurable", true).with("cached", s.visible)
      .with("cacheStamp", s.stamp).with("certifiedStamp", s.stamp)
      .with("last", CheckpointParentSynced),
  }

  /// Only unacknowledged unsynced bytes may disappear. Both sides of a
  /// checkpoint rename already contain the complete previously ACKed floor.
  action restart = all {
    s.layout == AppendLog, s.exists, not(s.blocked), not(s.restarted),
    s' = s.with("visible", s.durable).with("cached", 0)
      .with("cacheStamp", -1).with("certifiedStamp", -1)
      .with("rotating", false).with("parentDurable", true)
      .with("restarted", true).with("last", ProcessRestarted),
  }
  action loseMigratedLog = all {
    s.layout == AppendLog, s.exists, not(s.blocked),
    s' = s.with("exists", false).with("visible", 0).with("durable", 0)
      .with("blocked", true).with("cacheStamp", -1)
      .with("last", MigratedLogLost),
  }
  action corruptRecord = all {
    s.layout == AppendLog, s.exists, not(s.blocked),
    s' = s.with("tail", CorruptTail).with("blocked", true)
      .with("cacheStamp", -1).with("last", CompleteRecordCorrupt),
  }
  action wrongKind = all {
    s.layout == AppendLog, s.exists, not(s.blocked),
    s' = s.with("tail", CorruptTail).with("blocked", true)
      .with("cacheStamp", -1).with("last", WrongKindRecord),
  }
  action oldReader = all {
    s.layout == AppendLog, not(s.oldReaderRejected),
    s' = s.with("oldReaderRejected", true).with("last", LegacyReaderRejected),
  }
  action step = any {
    prepareMigration, advanceLegacy, mergeLegacy, publishSentinel,
    appendComplete, appendIncomplete, failSync, observeUncertain, syncTail,
    acknowledge, truncateIncomplete, externalAppend, prepareCheckpoint,
    syncCheckpoint, installCheckpoint, syncCheckpointParent, restart,
    loseMigratedLog, corruptRecord, wrongKind, oldReader, s' = s,
  }

  /// Coverage follows existing actions through persistence cuts instead of
  /// letting an early corruption sink hide the longer retry/rotation paths.
  action coverageStep =
    if (s.layout == LegacySidecar) prepareMigration
    else if (s.layout == MigrationPrepared) any { advanceLegacy, mergeLegacy, publishSentinel }
    else if (s.blocked) s' = s
    else if (s.rotating) {
      if (s.checkpointSynced) installCheckpoint else syncCheckpoint
    } else if (s.rotated and not(s.parentDurable)) syncCheckpointParent
    else if (s.tail == IncompleteTail) truncateIncomplete
    else if (s.last == SentinelSynced) any { appendComplete, appendIncomplete, externalAppend, oldReader }
    else if (s.visible > s.durable and s.syncFailed and not(s.uncertainObserved)) observeUncertain
    else if (s.visible > s.durable) any { failSync, syncTail }
    else if (s.cacheStamp != s.stamp or s.certifiedStamp != s.stamp) syncTail
    else if (s.visible < 2) appendComplete
    else if (s.acked < 2) acknowledge
    else any { prepareCheckpoint, restart, loseMigratedLog, corruptRecord, wrongKind, oldReader, s' = s }
}
