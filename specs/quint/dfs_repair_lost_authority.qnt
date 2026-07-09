/// DFS repair lost-authority suppression and retry classification.
///
/// This module models the retry/suppression boundary around:
/// - `repair_rejection_indicates_lost_authority`
/// - `repair_rejection_indicates_stale_replica_base`
/// - `repair_rejection_indicates_stale_chunk_resume`
/// - `suppress_repair_retries_for_lost_authority`
/// - `restore_repair_attempt_records`
///
/// It deliberately consumes lower-layer guarantees: repair responses are
/// assumed to be request-matched by `dfs_pending_request_lifecycle`, and
/// content identity is assumed to be checked by `dfs_repair_content_validation`.
module dfs_repair_lost_authority {
  import common.* from "./common"

  type RepairReason =
    LostAuthority
    | StaleReplicaBase
    | StaleChunkResume
    | TransientRouteFailure
    | OtherRejection

  type ContentCase =
    CurrentHashMatchesPending
    | CurrentObjectUnknown
    | SameVersionHashMismatch
    | CurrentIsBaseVersion
    | JobMetadataMismatch
    | OtherCurrentVersion

  type Decision =
    NoDecision
    | RepairRequestRecorded
    | PartialWindowRecorded
    | LostAuthoritySuppressed
    | PendingDroppedForLostAuthority
    | StaleBaseRetryScheduled
    | StaleChunkResumeReset
    | TransientRouteRetryScheduled
    | GenericRetryScheduled
    | RetryAllowed
    | RestoreIgnored

  pure val RepairReasons: Set[RepairReason] = Set(
    LostAuthority,
    StaleReplicaBase,
    StaleChunkResume,
    TransientRouteFailure,
    OtherRejection
  )

  pure val ContentCases: Set[ContentCase] = Set(
    CurrentHashMatchesPending,
    CurrentObjectUnknown,
    SameVersionHashMismatch,
    CurrentIsBaseVersion,
    JobMetadataMismatch,
    OtherCurrentVersion
  )

  pure val ReplicaSets: Set[Set[Node]] = Set(
    Set(N1),
    Set(N2),
    Set(N3),
    Set(N1, N2),
    Set(N1, N3),
    Set(N2, N3),
    Set(N1, N2, N3)
  )

  var pendingExists: bool
  var intentCommitted: bool
  var repairJobs: Set[Node]
  var knownRetryReplicas: Set[Node]
  var suppressedReplicas: Set[Node]
  var repairRequestIds: Set[Node]
  var repairResumeReplicas: Set[Node]
  var retryDueReplicas: Set[Node]
  var fetchInflightSources: Set[Node]
  var fetchRequestIds: Set[Node]
  var fetchResumeSources: Set[Node]
  var fetchFailedSources: Set[Node]
  var incomingFetchTransfers: Set[Node]
  var persistedLostAuthorityRecords: Set[Node]
  var lastDecision: Decision
  var lastReplica: Option[Node]
  var lastDroppedCommittedPending: bool

  pure def reasonIndicatesLostAuthority(reason: RepairReason): bool =
    reason == LostAuthority

  pure def reasonIndicatesStaleBase(reason: RepairReason): bool =
    reason == StaleReplicaBase

  pure def reasonIndicatesStaleChunkResume(reason: RepairReason): bool =
    reason == StaleChunkResume

  pure def reasonIndicatesTransientRouteFailure(reason: RepairReason): bool =
    reason == TransientRouteFailure

  pure def pendingRepairTargetsCurrentContent(contentCase: ContentCase): bool =
    any {
      contentCase == CurrentHashMatchesPending,
      contentCase == CurrentObjectUnknown,
      contentCase == CurrentIsBaseVersion,
    }

  pure def removeNode(nodes: Set[Node], replica: Node): Set[Node] =
    nodes.filter(node => node != replica)

  action init = all {
    pendingExists' = false,
    intentCommitted' = false,
    repairJobs' = Set(),
    knownRetryReplicas' = Set(),
    suppressedReplicas' = Set(),
    repairRequestIds' = Set(),
    repairResumeReplicas' = Set(),
    retryDueReplicas' = Set(),
    fetchInflightSources' = Set(),
    fetchRequestIds' = Set(),
    fetchResumeSources' = Set(),
    fetchFailedSources' = Set(),
    incomingFetchTransfers' = Set(),
    persistedLostAuthorityRecords' = Set(),
    lastDecision' = NoDecision,
    lastReplica' = None,
    lastDroppedCommittedPending' = false,
  }

  action beginPending(jobs: Set[Node], committed: bool): bool = all {
    not(pendingExists),
    jobs.size() > 0,
    pendingExists' = true,
    intentCommitted' = committed,
    repairJobs' = jobs,
    knownRetryReplicas' = jobs,
    suppressedReplicas' = Set(),
    repairRequestIds' = Set(),
    repairResumeReplicas' = Set(),
    retryDueReplicas' = jobs,
    fetchInflightSources' = Set(),
    fetchRequestIds' = Set(),
    fetchResumeSources' = Set(),
    fetchFailedSources' = Set(),
    incomingFetchTransfers' = Set(),
    persistedLostAuthorityRecords' = Set(),
    lastDecision' = NoDecision,
    lastReplica' = None,
    lastDroppedCommittedPending' = false,
  }

  action applyLostAuthoritySuppression(
    extraReplicas: Set[Node],
    dropCommittedPendingWrite: bool,
    observedReplica: Option[Node]
  ): bool = {
    val affectedReplicas = repairJobs.union(knownRetryReplicas).union(extraReplicas)
    val removePending = dropCommittedPendingWrite and intentCommitted
    all {
      pendingExists,
      pendingExists' = if (removePending) { false } else { pendingExists },
      intentCommitted' = if (removePending) { false } else { intentCommitted },
      repairJobs' = if (removePending) { Set() } else { repairJobs },
      knownRetryReplicas' =
        if (removePending) { Set() } else { knownRetryReplicas.union(affectedReplicas) },
      suppressedReplicas' =
        if (removePending) { Set() } else { suppressedReplicas.union(affectedReplicas) },
      repairRequestIds' =
        if (removePending) { Set() } else { repairRequestIds.filter(node => not(affectedReplicas.contains(node))) },
      repairResumeReplicas' =
        if (removePending) { Set() } else { repairResumeReplicas.filter(node => not(affectedReplicas.contains(node))) },
      retryDueReplicas' =
        if (removePending) { Set() } else { retryDueReplicas.filter(node => not(affectedReplicas.contains(node))) },
      fetchInflightSources' = Set(),
      fetchRequestIds' = Set(),
      fetchResumeSources' = Set(),
      fetchFailedSources' = Set(),
      incomingFetchTransfers' = Set(),
      persistedLostAuthorityRecords' =
        if (removePending) { Set() } else { persistedLostAuthorityRecords.union(affectedReplicas) },
      lastDecision' =
        if (removePending) { PendingDroppedForLostAuthority } else { LostAuthoritySuppressed },
      lastReplica' = observedReplica,
      lastDroppedCommittedPending' = removePending,
    }
  }

  action recordRepairRequest(replica: Node): bool = all {
    pendingExists,
    repairJobs.contains(replica),
    not(suppressedReplicas.contains(replica)),
    pendingExists' = pendingExists,
    intentCommitted' = intentCommitted,
    repairJobs' = repairJobs,
    knownRetryReplicas' = knownRetryReplicas.union(Set(replica)),
    suppressedReplicas' = suppressedReplicas,
    repairRequestIds' = repairRequestIds.union(Set(replica)),
    repairResumeReplicas' = repairResumeReplicas,
    retryDueReplicas' = removeNode(retryDueReplicas, replica),
    fetchInflightSources' = fetchInflightSources,
    fetchRequestIds' = fetchRequestIds,
    fetchResumeSources' = fetchResumeSources,
    fetchFailedSources' = fetchFailedSources,
    incomingFetchTransfers' = incomingFetchTransfers,
    persistedLostAuthorityRecords' = persistedLostAuthorityRecords,
    lastDecision' = RepairRequestRecorded,
    lastReplica' = Some(replica),
    lastDroppedCommittedPending' = false,
  }

  action recordPartialRepairWindow(replica: Node): bool = all {
    pendingExists,
    repairJobs.contains(replica),
    not(suppressedReplicas.contains(replica)),
    pendingExists' = pendingExists,
    intentCommitted' = intentCommitted,
    repairJobs' = repairJobs,
    knownRetryReplicas' = knownRetryReplicas.union(Set(replica)),
    suppressedReplicas' = suppressedReplicas,
    repairRequestIds' = repairRequestIds.union(Set(replica)),
    repairResumeReplicas' = repairResumeReplicas.union(Set(replica)),
    retryDueReplicas' = retryDueReplicas.union(Set(replica)),
    fetchInflightSources' = fetchInflightSources,
    fetchRequestIds' = fetchRequestIds,
    fetchResumeSources' = fetchResumeSources,
    fetchFailedSources' = fetchFailedSources,
    incomingFetchTransfers' = incomingFetchTransfers,
    persistedLostAuthorityRecords' = persistedLostAuthorityRecords,
    lastDecision' = PartialWindowRecorded,
    lastReplica' = Some(replica),
    lastDroppedCommittedPending' = false,
  }

  action startFetchFromAlternate(source: Node): bool = all {
    pendingExists,
    not(suppressedReplicas.contains(source)),
    pendingExists' = pendingExists,
    intentCommitted' = intentCommitted,
    repairJobs' = repairJobs,
    knownRetryReplicas' = knownRetryReplicas,
    suppressedReplicas' = suppressedReplicas,
    repairRequestIds' = repairRequestIds,
    repairResumeReplicas' = repairResumeReplicas,
    retryDueReplicas' = retryDueReplicas,
    fetchInflightSources' = fetchInflightSources.union(Set(source)),
    fetchRequestIds' = fetchRequestIds.union(Set(source)),
    fetchResumeSources' = fetchResumeSources.union(Set(source)),
    fetchFailedSources' = fetchFailedSources,
    incomingFetchTransfers' = incomingFetchTransfers.union(Set(source)),
    persistedLostAuthorityRecords' = persistedLostAuthorityRecords,
    lastDecision' = NoDecision,
    lastReplica' = Some(source),
    lastDroppedCommittedPending' = false,
  }

  action handleRepairRejected(replica: Node, reason: RepairReason): bool =
    if (reasonIndicatesLostAuthority(reason)) {
      applyLostAuthoritySuppression(Set(replica), false, Some(replica))
    } else if (reasonIndicatesStaleBase(reason)) {
      all {
        pendingExists,
        not(suppressedReplicas.contains(replica)),
        pendingExists' = pendingExists,
        intentCommitted' = intentCommitted,
        repairJobs' = repairJobs,
        knownRetryReplicas' = knownRetryReplicas.union(Set(replica)),
        suppressedReplicas' = suppressedReplicas,
        repairRequestIds' = removeNode(repairRequestIds, replica),
        repairResumeReplicas' = removeNode(repairResumeReplicas, replica),
        retryDueReplicas' = removeNode(retryDueReplicas, replica),
        fetchInflightSources' = fetchInflightSources,
        fetchRequestIds' = fetchRequestIds,
        fetchResumeSources' = fetchResumeSources,
        fetchFailedSources' = fetchFailedSources,
        incomingFetchTransfers' = incomingFetchTransfers,
        persistedLostAuthorityRecords' = persistedLostAuthorityRecords,
        lastDecision' = StaleBaseRetryScheduled,
        lastReplica' = Some(replica),
        lastDroppedCommittedPending' = false,
      }
    } else if (reasonIndicatesStaleChunkResume(reason)) {
      all {
        pendingExists,
        not(suppressedReplicas.contains(replica)),
        pendingExists' = pendingExists,
        intentCommitted' = intentCommitted,
        repairJobs' = repairJobs,
        knownRetryReplicas' = knownRetryReplicas.union(Set(replica)),
        suppressedReplicas' = suppressedReplicas,
        repairRequestIds' = removeNode(repairRequestIds, replica),
        repairResumeReplicas' = removeNode(repairResumeReplicas, replica),
        retryDueReplicas' = retryDueReplicas.union(Set(replica)),
        fetchInflightSources' = fetchInflightSources,
        fetchRequestIds' = fetchRequestIds,
        fetchResumeSources' = fetchResumeSources,
        fetchFailedSources' = fetchFailedSources,
        incomingFetchTransfers' = incomingFetchTransfers,
        persistedLostAuthorityRecords' = persistedLostAuthorityRecords,
        lastDecision' = StaleChunkResumeReset,
        lastReplica' = Some(replica),
        lastDroppedCommittedPending' = false,
      }
    } else {
      all {
        pendingExists,
        not(suppressedReplicas.contains(replica)),
        pendingExists' = pendingExists,
        intentCommitted' = intentCommitted,
        repairJobs' = repairJobs,
        knownRetryReplicas' = knownRetryReplicas.union(Set(replica)),
        suppressedReplicas' = suppressedReplicas,
        repairRequestIds' = removeNode(repairRequestIds, replica),
        repairResumeReplicas' = removeNode(repairResumeReplicas, replica),
        retryDueReplicas' = removeNode(retryDueReplicas, replica),
        fetchInflightSources' = fetchInflightSources,
        fetchRequestIds' = fetchRequestIds,
        fetchResumeSources' = fetchResumeSources,
        fetchFailedSources' = fetchFailedSources,
        incomingFetchTransfers' = incomingFetchTransfers,
        persistedLostAuthorityRecords' = persistedLostAuthorityRecords,
        lastDecision' = GenericRetryScheduled,
        lastReplica' = Some(replica),
        lastDroppedCommittedPending' = false,
      }
    }

  action recordRepairSendFailure(replica: Node, reason: RepairReason): bool = all {
    pendingExists,
    not(suppressedReplicas.contains(replica)),
    pendingExists' = pendingExists,
    intentCommitted' = intentCommitted,
    repairJobs' = repairJobs,
    knownRetryReplicas' = knownRetryReplicas.union(Set(replica)),
    suppressedReplicas' = suppressedReplicas,
    repairRequestIds' = removeNode(repairRequestIds, replica),
    repairResumeReplicas' = removeNode(repairResumeReplicas, replica),
    retryDueReplicas' = removeNode(retryDueReplicas, replica),
    fetchInflightSources' = fetchInflightSources,
    fetchRequestIds' = fetchRequestIds,
    fetchResumeSources' = fetchResumeSources,
    fetchFailedSources' = fetchFailedSources,
    incomingFetchTransfers' = incomingFetchTransfers,
    persistedLostAuthorityRecords' = persistedLostAuthorityRecords,
    lastDecision' =
      if (reasonIndicatesTransientRouteFailure(reason)) {
        TransientRouteRetryScheduled
      } else {
        GenericRetryScheduled
      },
    lastReplica' = Some(replica),
    lastDroppedCommittedPending' = false,
  }

  action wakeRepairRetry(replica: Node): bool = all {
    pendingExists,
    knownRetryReplicas.union(repairJobs).contains(replica),
    pendingExists' = pendingExists,
    intentCommitted' = intentCommitted,
    repairJobs' = repairJobs,
    knownRetryReplicas' = knownRetryReplicas,
    suppressedReplicas' = suppressedReplicas,
    repairRequestIds' = repairRequestIds,
    repairResumeReplicas' = repairResumeReplicas,
    retryDueReplicas' =
      if (suppressedReplicas.contains(replica)) {
        retryDueReplicas
      } else {
        retryDueReplicas.union(Set(replica))
      },
    fetchInflightSources' = fetchInflightSources,
    fetchRequestIds' = fetchRequestIds,
    fetchResumeSources' = fetchResumeSources,
    fetchFailedSources' = fetchFailedSources,
    incomingFetchTransfers' = incomingFetchTransfers,
    persistedLostAuthorityRecords' = persistedLostAuthorityRecords,
    lastDecision' = NoDecision,
    lastReplica' = Some(replica),
    lastDroppedCommittedPending' = false,
  }

  action restoreAttemptRecord(replica: Node, reason: RepairReason): bool =
    if (reasonIndicatesLostAuthority(reason)) {
      applyLostAuthoritySuppression(Set(replica), false, Some(replica))
    } else if (suppressedReplicas.contains(replica)) {
      all {
        pendingExists,
        pendingExists' = pendingExists,
        intentCommitted' = intentCommitted,
        repairJobs' = repairJobs,
        knownRetryReplicas' = knownRetryReplicas,
        suppressedReplicas' = suppressedReplicas,
        repairRequestIds' = removeNode(repairRequestIds, replica),
        repairResumeReplicas' = removeNode(repairResumeReplicas, replica),
        retryDueReplicas' = removeNode(retryDueReplicas, replica),
        fetchInflightSources' = fetchInflightSources,
        fetchRequestIds' = fetchRequestIds,
        fetchResumeSources' = fetchResumeSources,
        fetchFailedSources' = fetchFailedSources,
        incomingFetchTransfers' = incomingFetchTransfers,
        persistedLostAuthorityRecords' = persistedLostAuthorityRecords,
        lastDecision' = RestoreIgnored,
        lastReplica' = Some(replica),
        lastDroppedCommittedPending' = false,
      }
    } else {
      all {
        pendingExists,
        pendingExists' = pendingExists,
        intentCommitted' = intentCommitted,
        repairJobs' = repairJobs,
        knownRetryReplicas' = knownRetryReplicas.union(Set(replica)),
        suppressedReplicas' = suppressedReplicas,
        repairRequestIds' = removeNode(repairRequestIds, replica),
        repairResumeReplicas' = removeNode(repairResumeReplicas, replica),
        retryDueReplicas' =
          if (suppressedReplicas.contains(replica)) {
            retryDueReplicas
          } else {
            retryDueReplicas.union(Set(replica))
          },
        fetchInflightSources' = fetchInflightSources,
        fetchRequestIds' = fetchRequestIds,
        fetchResumeSources' = fetchResumeSources,
        fetchFailedSources' = fetchFailedSources,
        incomingFetchTransfers' = incomingFetchTransfers,
        persistedLostAuthorityRecords' = persistedLostAuthorityRecords,
        lastDecision' =
          if (reasonIndicatesTransientRouteFailure(reason)) {
            TransientRouteRetryScheduled
          } else {
            RestoreIgnored
          },
        lastReplica' = Some(replica),
        lastDroppedCommittedPending' = false,
      }
    }

  action maintenanceCheck(localAuthoritative: bool, contentCase: ContentCase): bool =
    if (not(localAuthoritative) or not(pendingRepairTargetsCurrentContent(contentCase))) {
      all {
        pendingExists,
        applyLostAuthoritySuppression(Set(), true, None),
      }
    } else {
      all {
        pendingExists,
        pendingExists' = pendingExists,
        intentCommitted' = intentCommitted,
        repairJobs' = repairJobs,
        knownRetryReplicas' = knownRetryReplicas,
        suppressedReplicas' = suppressedReplicas,
        repairRequestIds' = repairRequestIds,
        repairResumeReplicas' = repairResumeReplicas,
        retryDueReplicas' = retryDueReplicas.filter(node => not(suppressedReplicas.contains(node))),
        fetchInflightSources' = fetchInflightSources,
        fetchRequestIds' = fetchRequestIds,
        fetchResumeSources' = fetchResumeSources,
        fetchFailedSources' = fetchFailedSources,
        incomingFetchTransfers' = incomingFetchTransfers,
        persistedLostAuthorityRecords' = persistedLostAuthorityRecords,
        lastDecision' = RetryAllowed,
        lastReplica' = None,
        lastDroppedCommittedPending' = false,
      }
    }

  action step = any {
    {
      nondet jobs = oneOf(ReplicaSets)
      nondet committed = oneOf(Set(true, false))
      beginPending(jobs, committed)
    },
    {
      nondet replica = oneOf(Nodes)
      any {
        recordRepairRequest(replica),
        recordPartialRepairWindow(replica),
        startFetchFromAlternate(replica),
        wakeRepairRetry(replica),
      }
    },
    {
      nondet replica = oneOf(Nodes)
      nondet reason = oneOf(RepairReasons)
      any {
        handleRepairRejected(replica, reason),
        recordRepairSendFailure(replica, reason),
        restoreAttemptRecord(replica, reason),
      }
    },
    {
      nondet localAuthoritative = oneOf(Set(true, false))
      nondet contentCase = oneOf(ContentCases)
      maintenanceCheck(localAuthoritative, contentCase)
    },
  }

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
