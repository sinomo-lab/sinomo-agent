/// Invariant checks for `dfs_repair_lost_authority`.
module dfs_repair_lost_authority_safety {
  import common.* from "../../lib/common"
  import dfs_repair_lost_authority.* from "../../models/dfs/repair_lost_authority"

  def pendingRemovedClearsPendingState: bool =
    pendingExists or all {
      not(intentCommitted),
      repairJobs == Set(),
      knownRetryReplicas == Set(),
      suppressedReplicas == Set(),
      repairRequestIds == Set(),
      repairResumeReplicas == Set(),
      retryDueReplicas == Set(),
      fetchInflightSources == Set(),
      fetchRequestIds == Set(),
      fetchResumeSources == Set(),
      fetchFailedSources == Set(),
      incomingFetchTransfers == Set(),
      persistedLostAuthorityRecords == Set(),
    }


  def suppressedRepairsAreNotActiveOrDue: bool =
    all {
      suppressedReplicas.intersect(repairRequestIds) == Set(),
      suppressedReplicas.intersect(repairResumeReplicas) == Set(),
      suppressedReplicas.intersect(retryDueReplicas) == Set(),
    }


  def persistedLostAuthorityRecordsStaySuppressed: bool =
    persistedLostAuthorityRecords.subseteq(suppressedReplicas)


  def lostAuthorityClearsFetchState: bool =
    all {
      lastDecision == LostAuthoritySuppressed
        or lastDecision == PendingDroppedForLostAuthority,
    } implies all {
      fetchInflightSources == Set(),
      fetchRequestIds == Set(),
      fetchResumeSources == Set(),
      fetchFailedSources == Set(),
      incomingFetchTransfers == Set(),
    }


  def lostAuthorityResponseSuppressesObservedReplica: bool =
    match lastReplica {
      | Some(replica) =>
          lastDecision == LostAuthoritySuppressed implies suppressedReplicas.contains(replica)
      | None => true
    }


  def lostAuthorityMaintenanceSuppressesRepairBacklog: bool =
    all {
      lastDecision == LostAuthoritySuppressed,
      lastReplica == None,
    } implies repairJobs.subseteq(suppressedReplicas)


  def droppedPendingIsRecordedAsCommittedDrop: bool =
    lastDecision == PendingDroppedForLostAuthority implies all {
      lastDroppedCommittedPending,
      not(pendingExists),
    }


  def staleBaseRejectionRemainsRetryable: bool =
    match lastReplica {
      | Some(replica) =>
          lastDecision == StaleBaseRetryScheduled implies all {
            pendingExists,
            not(suppressedReplicas.contains(replica)),
            not(repairRequestIds.contains(replica)),
            not(repairResumeReplicas.contains(replica)),
          }
      | None => true
    }


  def staleChunkResumeResetsWithoutSuppressing: bool =
    match lastReplica {
      | Some(replica) =>
          lastDecision == StaleChunkResumeReset implies all {
            pendingExists,
            not(suppressedReplicas.contains(replica)),
            not(repairRequestIds.contains(replica)),
            not(repairResumeReplicas.contains(replica)),
            retryDueReplicas.contains(replica),
          }
      | None => true
    }


  def transientRouteFailureRemainsRetryable: bool =
    match lastReplica {
      | Some(replica) =>
          lastDecision == TransientRouteRetryScheduled implies all {
            pendingExists,
            not(suppressedReplicas.contains(replica)),
          }
      | None => true
    }


  def retryDueReplicasAreKnownAndNotSuppressed: bool =
    all {
      retryDueReplicas.subseteq(knownRetryReplicas.union(repairJobs)),
      retryDueReplicas.intersect(suppressedReplicas) == Set(),
    }


  def repairRequestReplicasAreKnownAndNotSuppressed: bool =
    all {
      repairRequestIds.subseteq(knownRetryReplicas.union(repairJobs)),
      repairRequestIds.intersect(suppressedReplicas) == Set(),
    }


  def fetchRequestStateIsConsistent: bool =
    all {
      fetchRequestIds.subseteq(fetchInflightSources),
      fetchResumeSources.subseteq(fetchInflightSources),
      incomingFetchTransfers.subseteq(fetchRequestIds.union(fetchInflightSources)),
    }


  def safety: bool = all {
    pendingRemovedClearsPendingState,
    suppressedRepairsAreNotActiveOrDue,
    persistedLostAuthorityRecordsStaySuppressed,
    lostAuthorityClearsFetchState,
    lostAuthorityResponseSuppressesObservedReplica,
    lostAuthorityMaintenanceSuppressesRepairBacklog,
    droppedPendingIsRecordedAsCommittedDrop,
    staleBaseRejectionRemainsRetryable,
    staleChunkResumeResetsWithoutSuppressing,
    transientRouteFailureRemainsRetryable,
    retryDueReplicasAreKnownAndNotSuppressed,
    repairRequestReplicasAreKnownAndNotSuppressed,
    fetchRequestStateIsConsistent,
  }
}
