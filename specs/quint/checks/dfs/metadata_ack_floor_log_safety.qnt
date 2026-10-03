/// Bounded storage refinement; does not model payload validation or real IO.
module dfs_metadata_ack_floor_log_safety {
  import dfs_metadata_ack_floor_log.* from "../../models/dfs/metadata_ack_floor_log"

  def ackHasSyncedCurrentEvidence: bool = s.last == AckReturned implies all {
    s.exists, not(s.blocked), s.tail == CleanTail, s.parentDurable,
    s.layout == AppendLog, s.cacheStamp == s.stamp,
    s.certifiedStamp == s.stamp, s.acked == s.cached, s.durable >= s.acked,
  }
  def migratedProtectionNeverFallsBack: bool =
    s.layout == AppendLog and not(s.blocked) implies all {
      s.exists, s.durable >= s.acked, s.durable >= s.legacy,
    }
  def incompleteTailCannotContainAnAck: bool =
    s.tail == IncompleteTail and not(s.blocked) implies all {
      s.visible == s.durable, s.durable >= s.acked, not(ackAllowed),
    }
  def invalidRecordFailsClosed: bool = all {
    s.tail == CorruptTail implies s.blocked,
    s.layout == AppendLog and not(s.exists) implies s.blocked,
    s.blocked implies not(ackAllowed),
  }
  def logicalFloorsStaySeparate: bool = all {
    s.kind == PrefixFloor implies all { s.authorityAcked == 0, s.prefixAcked == s.acked },
    s.kind == AuthorityFloor implies all { s.prefixAcked == 0, s.authorityAcked == s.acked },
  }
  def cachedBytesAloneCannotAcknowledgeFailedSync: bool =
    s.last == UncertainTailObserved implies all {
      s.cached > s.durable, s.cacheStamp == s.stamp,
      s.certifiedStamp != s.stamp, not(ackAllowed),
    }
  def obsoleteHandleCannotAcknowledge: bool =
    s.last == OtherHandleAdvanced implies all { s.cacheStamp != s.stamp, not(ackAllowed) }
  def rotationKeepsTheAcknowledgedFloor: bool =
    s.last == CheckpointInstalled implies all {
      s.checkpointSynced, s.durable >= s.acked, not(s.parentDurable), not(ackAllowed),
    }

  def witnessPrefixAckAfterSyncedAppend: bool = s.kind == PrefixFloor and s.last == AckReturned
  def witnessAuthorityAckAfterSyncedAppend: bool = s.kind == AuthorityFloor and s.last == AckReturned
  def witnessLegacyProgressMergedBeforeSentinel: bool = s.legacy == 2 and s.last == SentinelSynced
  def witnessCompleteUnsyncedRetryCannotAck: bool = s.last == UncertainTailObserved
  def witnessFailedSyncRetryRequiresResync: bool = all {
    s.last == AckReturned, s.syncFailed, s.uncertainObserved, s.durable == 2,
  }
  def witnessPartialTailRecovered: bool = s.last == IncompleteTailTruncated and not(s.blocked)
  def witnessOtherHandleInvalidatesCache: bool = s.last == OtherHandleAdvanced
  def witnessRotationPreservesAck: bool = s.last == CheckpointParentSynced and s.acked == 2
  def witnessRestartRetainsAck: bool = s.last == ProcessRestarted and s.acked == 2
  def witnessMissingMigratedLogRejects: bool = s.last == MigratedLogLost and s.acked == 2 and s.blocked
  def witnessMalformedCompleteRecordRejects: bool = s.last == CompleteRecordCorrupt and s.blocked
  def witnessWrongKindRecordRejects: bool = s.last == WrongKindRecord and s.blocked
  def witnessLegacyReaderFailsClosed: bool = s.last == LegacyReaderRejected and s.oldReaderRejected

  def safety: bool = all {
    ackHasSyncedCurrentEvidence, migratedProtectionNeverFallsBack,
    incompleteTailCannotContainAnAck, invalidRecordFailsClosed, logicalFloorsStaySeparate,
    cachedBytesAloneCannotAcknowledgeFailedSync, obsoleteHandleCannotAcknowledge,
    rotationKeepsTheAcknowledgedFloor,
  }
}
