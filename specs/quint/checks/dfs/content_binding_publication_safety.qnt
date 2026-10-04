module dfs_content_binding_publication_safety {
  import dfs_content_binding_publication.* from "../../models/dfs/content_binding_publication"
  def acknowledgedBindingStillReconstructs: bool = s.acked.forall(binding =>
    s.deleted.contains(binding) or all {
      s.durable.present, s.durable.manifest, s.durable.bytes,
      s.durable.tuple == binding._1, s.durable.payload == binding._2,
      s.durable.manifestPayload == binding._2,
    })
  def safety: bool = all {
    valid(s.durable), acknowledgedBindingStillReconstructs,
    s.certifiedStamp <= s.durableStamp,
    (s.cacheStamp == s.durableStamp and not(s.pending)) implies s.cache == s.durable,
    (s.blocked or s.malformed or s.torn) implies not(receiptAllowed),
    receiptAllowed implies all { s.durable.manifest, s.durable.reservation, s.durable.bytes },
    s.acked.filter(binding => not(s.deleted.contains(binding))).size() <= 1,
  }
  def witnessOneSyncExactPublication: bool = all { s.acked.size() == 1, s.stamp == 1, s.crashes == 0, not(s.syncFailed) }
  def witnessLostReplyCrashReconstructsBinding: bool = all { s.crashedAfterLostReply, s.opened, s.durable.manifest, s.durable.reservation }
  def witnessIndexLossRebuildsManifestAndRoot: bool = all { s.indexLoss, s.cacheStamp == s.durableStamp, s.acked.size() > 0, s.durable.reservation }
  def witnessSyncFailureCacheDoesNotAuthorizeReceipt: bool = all { s.syncFailed, s.uncertainObserved, s.certifiedStamp == -1, s.acked == Set() }
  def witnessRetrySynchronizesUncertainRecord: bool = all { s.retriedSync, s.acked.size() > 0 }
  def witnessTornTailReopenPreservesAcknowledgedPrefix: bool = all { s.tornObserved, s.crashes == 1, s.opened, not(s.torn), s.acked.size() > 0 }
  def witnessCompleteMalformedRecordFailsClosed: bool = all { s.blocked, s.malformed }
  def witnessConflictingTupleRejected: bool = s.conflictRejected
  def witnessQuotaRejectsSecondLiveTuple: bool = all { s.quotaRejected, s.durable.manifest }
  def witnessPostCutRenewalBlocksNegativeRelease: bool = all { s.negativeBlocked, s.cutPresent, s.renewalCount > 0, s.cutIncarnation == 1, s.durable.reservation, s.durable.epoch == s.cutEpoch }
  def witnessForeignCutBlocksNegativeRelease: bool = all { s.negativeBlocked, s.cutIncarnation == 2 }
  def witnessOlderReservationCanRelease: bool = s.negativeReleased
  def witnessPositiveReleaseKeepsCanonicalManifest: bool = all { s.positiveReleased, s.durable.manifest, s.durable.bytes }
}
