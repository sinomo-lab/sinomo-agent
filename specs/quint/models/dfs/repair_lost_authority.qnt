/// DFS repair lost-authority suppression and retry classification.
///
/// This module models the retry/suppression boundary around:
/// - `repair_rejection_indicates_lost_authority`
/// - `repair_rejection_indicates_stale_replica_base`
/// - `suppress_repair_retries_for_lost_authority`
/// - `restore_repair_attempt_records`
///
/// It deliberately consumes lower-layer guarantees: repair responses are
/// assumed to be request-matched by `dfs_pending_request_lifecycle`, and
/// content identity is assumed to be checked by `dfs_repair_content_validation`.
module dfs_repair_lost_authority {
  import common.* from "../../lib/common"

  type RepairReason =
    LostAuthority
    | StaleReplicaBase
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
    | LostAuthoritySuppressed
    | PendingDroppedForLostAuthority
    | StaleBaseRetryScheduled
    | TransientRouteRetryScheduled
    | GenericRetryScheduled
    | RetryAllowed
    | RestoreIgnored

  pure val RepairReasons: Set[RepairReason] = Set(
    LostAuthority,
    StaleReplicaBase,
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
  var retryDueReplicas: Set[Node]
  var fetchInflightSources: Set[Node]
  var fetchRequestIds: Set[Node]
  var fetchFailedSources: Set[Node]
  var persistedLostAuthorityRecords: Set[Node]
  var lastDecision: Decision
  var lastReplica: Option[Node]
  var lastDroppedCommittedPending: bool

  pure def reasonIndicatesLostAuthority(reason: RepairReason): bool =
    reason == LostAuthority

  pure def reasonIndicatesStaleBase(reason: RepairReason): bool =
    reason == StaleReplicaBase

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
    retryDueReplicas' = Set(),
    fetchInflightSources' = Set(),
    fetchRequestIds' = Set(),
    fetchFailedSources' = Set(),
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
    retryDueReplicas' = jobs,
    fetchInflightSources' = Set(),
    fetchRequestIds' = Set(),
    fetchFailedSources' = Set(),
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
      retryDueReplicas' =
        if (removePending) { Set() } else { retryDueReplicas.filter(node => not(affectedReplicas.contains(node))) },
      fetchInflightSources' = Set(),
      fetchRequestIds' = Set(),
      fetchFailedSources' = Set(),
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
    retryDueReplicas' = removeNode(retryDueReplicas, replica),
    fetchInflightSources' = fetchInflightSources,
    fetchRequestIds' = fetchRequestIds,
    fetchFailedSources' = fetchFailedSources,
    persistedLostAuthorityRecords' = persistedLostAuthorityRecords,
    lastDecision' = RepairRequestRecorded,
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
    retryDueReplicas' = retryDueReplicas,
    fetchInflightSources' = fetchInflightSources.union(Set(source)),
    fetchRequestIds' = fetchRequestIds.union(Set(source)),
    fetchFailedSources' = fetchFailedSources,
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
        retryDueReplicas' = removeNode(retryDueReplicas, replica),
        fetchInflightSources' = fetchInflightSources,
        fetchRequestIds' = fetchRequestIds,
        fetchFailedSources' = fetchFailedSources,
        persistedLostAuthorityRecords' = persistedLostAuthorityRecords,
        lastDecision' = StaleBaseRetryScheduled,
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
        retryDueReplicas' = removeNode(retryDueReplicas, replica),
        fetchInflightSources' = fetchInflightSources,
        fetchRequestIds' = fetchRequestIds,
        fetchFailedSources' = fetchFailedSources,
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
    retryDueReplicas' = removeNode(retryDueReplicas, replica),
    fetchInflightSources' = fetchInflightSources,
    fetchRequestIds' = fetchRequestIds,
    fetchFailedSources' = fetchFailedSources,
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
    retryDueReplicas' =
      if (suppressedReplicas.contains(replica)) {
        retryDueReplicas
      } else {
        retryDueReplicas.union(Set(replica))
      },
    fetchInflightSources' = fetchInflightSources,
    fetchRequestIds' = fetchRequestIds,
    fetchFailedSources' = fetchFailedSources,
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
        retryDueReplicas' = removeNode(retryDueReplicas, replica),
        fetchInflightSources' = fetchInflightSources,
        fetchRequestIds' = fetchRequestIds,
        fetchFailedSources' = fetchFailedSources,
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
        retryDueReplicas' =
          if (suppressedReplicas.contains(replica)) {
            retryDueReplicas
          } else {
            retryDueReplicas.union(Set(replica))
          },
        fetchInflightSources' = fetchInflightSources,
        fetchRequestIds' = fetchRequestIds,
        fetchFailedSources' = fetchFailedSources,
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
        retryDueReplicas' = retryDueReplicas.filter(node => not(suppressedReplicas.contains(node))),
        fetchInflightSources' = fetchInflightSources,
        fetchRequestIds' = fetchRequestIds,
        fetchFailedSources' = fetchFailedSources,
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
}
