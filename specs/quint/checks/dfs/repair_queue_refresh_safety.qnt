module dfs_repair_queue_refresh_safety {
  import dfs_repair_queue_refresh.* from "../../models/dfs/repair_queue_refresh"
  def safety: bool = all {
    exactCache implies s.cache == s.durable and s.cacheHead == s.diskHead,
    s.uncertain implies not(s.loaded),
    s.cache.retries >= 0, s.cache.retries <= 1,
    s.retryPreserved implies s.noOpAcknowledged,
    s.fingerprint >= 0, s.fingerprint <= 7,
    s.revision >= 0, s.revision <= 9,
  }
  def witnessDecodedAbaImageIsRejected: bool = s.abaRejected and s.rejected
  def witnessUnchangedGenerationSkipsReplay: bool = s.fast
  def witnessOwnedAppendKeepsHotProof: bool = s.fast and s.durable.a
  def witnessExternalAppendRefreshesPendingRecord: bool = all {
    s.externalAppendSeen, exactCache, s.cache.b, s.replayed,
  }
  def witnessSnapshotReplacementRefreshes: bool = s.replacementSeen and exactCache and s.replayed
  def witnessSameHeadReplacementRejectsStaleMaintenance: bool = s.sameHeadReplacement and s.staleRejected
  def witnessTornSuffixPreservesPending: bool = s.truncationSeen and exactCache and s.cache.a
  def witnessCorruptionDoesNotReuseHotView: bool = s.corruptionSeen and s.rejected and not(s.loaded)
  def witnessUnstableReadCannotPublishProof: bool = s.unstableSeen and s.rejected
  def witnessSyncFailureCannotAcknowledgeNoop: bool = s.uncertain and not(s.noOpAcknowledged)
  def witnessRetryResyncsCompleteUncertainFrame: bool = s.failed and s.resynced and s.noOpAcknowledged
  def witnessRestartRebuildsQueue: bool = s.restarted and s.replayed and exactCache and s.cache.a
  def witnessDuplicateEnqueuePreservesBackoff: bool = s.retryPreserved
  def witnessUnsupportedFingerprintFallsBack: bool = s.unsupportedReplayed
  def witnessFreshMaintenancePreservesPending: bool = s.reconcileAccepted and s.durable.a and s.durable.retries == 1
  def witnessFailedTornAppendRepairsBeforeReuse: bool = s.failed and s.resynced and s.truncationSeen and exactCache
  def witnessSameHeadReplacementPreservesChangedBackoff: bool = s.replacementSeen and exactCache and s.cache.a and s.cache.retries == 1
  def witnessConcurrentEnqueueRejectsStaleMaintenance: bool = s.externalAppendSeen and s.staleRejected
}
