/// DFS repair/stage content validation before durable receipt acceptance.
///
/// This module models the narrow safety boundary around
/// `put_metadata_receipt`, `record_local_pending_write_receipt_if_desired`,
/// stage responses, and repair responses:
/// - local manifest receipts require content addressed by the pending hash;
/// - stage and repair responses require matching request/source ownership;
/// - restored repair responses may be accepted only while a repair job still
///   exists for that replica;
/// - existing conflicting metadata receipts do not become durable receipts;
/// - durable receipts and current-location updates only use the pending hash;
/// - replica staging is work-conserving up to a bounded fan-out cap;
/// - one target's stage failure/retry does not cancel another target's request;
/// - a matching stage receipt may arrive after the W commit without changing
///   the committed target hash or version.
module dfs_repair_content_validation {
  import common.* from "../../lib/common"

  type RequestId = Req1 | Req2
  type ContentHash = ExpectedHash | OtherHash
  type ObjectVersion = Version1 | Version2
  type Decision =
    NoDecision
    | ReceiptRecorded
    | ExistingReceiptHydrated
    | ResponseRejected
    | ResponseDiscarded
    | RetryScheduled
    | LocalManifestIgnored

  pure val RequestIds: Set[RequestId] = Set(Req1, Req2)
  pure val ContentHashes: Set[ContentHash] = Set(ExpectedHash, OtherHash)
  pure val CommitThresholds: Set[int] = Set(1, 2, 3)
  pure val StageFanoutCap: int = 2
  pure val ReplicaSets: Set[Set[Node]] = Set(
    Set(N1),
    Set(N2),
    Set(N3),
    Set(N1, N2),
    Set(N1, N3),
    Set(N2, N3),
    Set(N1, N2, N3)
  )

  var pending: bool
  var pendingHash: ContentHash
  var pendingVersion: ObjectVersion
  var desiredReplicas: Set[Node]
  var minCommitReplicas: int
  var visibleCommitted: bool
  var commitReceipts: Set[Node]
  var committedTargetHash: Option[ContentHash]
  var committedTargetVersion: Option[ObjectVersion]
  var targetVersionIsCurrent: bool
  var currentObjectHash: ContentHash
  var currentObjectVersion: ObjectVersion
  var metadataReceiptHash: Node -> Option[ContentHash]
  var durableReceipts: Set[Node]
  var currentLocations: Set[Node]
  var repairJobs: Set[Node]
  var stageInflight: Node -> Option[RequestId]
  var stageRetryDue: Set[Node]
  var stageRetriedTargets: Set[Node]
  var stageReceiptTargets: Set[Node]
  var parallelStageSuccessTargets: Set[Node]
  var postCommitStageReceiptTargets: Set[Node]
  var repairInflight: Node -> Option[RequestId]
  var localManifestHash: Node -> Option[ContentHash]
  var lastAcceptedHash: Option[ContentHash]
  var lastDecision: Decision

  pure val emptyHashMap: Node -> Option[ContentHash] =
    Nodes.mapBy(_ => None)

  pure val emptyRequestMap: Node -> Option[RequestId] =
    Nodes.mapBy(_ => None)

  pure def missingDesiredReplicas(desired: Set[Node], durable: Set[Node]): Set[Node] =
    desired.filter(node => not(durable.contains(node)))

  pure def validCommitThreshold(desired: Set[Node], minCommit: int): bool = all {
    desired.size() > 0,
    minCommit > 0,
    minCommit <= desired.size(),
  }

  def activeStageTargets: Set[Node] =
    Nodes.filter(node => isSome(stageInflight.get(node)))

  def stageFanoutWithinCap: bool =
    activeStageTargets.size() <= StageFanoutCap

  def allRepairInflightEmpty: bool =
    Nodes.filter(node => isSome(repairInflight.get(node))).size() == 0

  def metadataReceiptAccepts(replica: Node, hash: ContentHash): bool =
    match metadataReceiptHash.get(replica) {
      | Some(existingHash) => existingHash == hash
      | None => true
    }

  def targetVersionHashStable: bool =
    not(pending) or not(targetVersionIsCurrent) or currentObjectHash == pendingHash

  def targetVersionIdentityStable: bool =
    not(pending) or not(targetVersionIsCurrent) or all {
      currentObjectHash == pendingHash,
      currentObjectVersion == pendingVersion,
    }

  def committedTargetIdentityStable: bool =
    if (visibleCommitted) {
      all {
        committedTargetHash == Some(pendingHash),
        committedTargetVersion == Some(pendingVersion),
      }
    } else {
      all {
        committedTargetHash == None,
        committedTargetVersion == None,
      }
    }

  def canPutReceipt(replica: Node, hash: ContentHash): bool =
    all {
      metadataReceiptAccepts(replica, hash),
      not(targetVersionIsCurrent) or currentObjectHash == hash,
    }

  def repairResponseRequestAuthorized(replica: Node, requestId: RequestId): bool =
    any {
      repairInflight.get(replica) == Some(requestId),
      all {
        allRepairInflightEmpty,
        repairJobs.contains(replica),
      },
    }

  action init = all {
    pending' = false,
    pendingHash' = ExpectedHash,
    pendingVersion' = Version1,
    desiredReplicas' = Set(),
    minCommitReplicas' = 0,
    visibleCommitted' = false,
    commitReceipts' = Set(),
    committedTargetHash' = None,
    committedTargetVersion' = None,
    targetVersionIsCurrent' = false,
    currentObjectHash' = ExpectedHash,
    currentObjectVersion' = Version1,
    metadataReceiptHash' = emptyHashMap,
    durableReceipts' = Set(),
    currentLocations' = Set(),
    repairJobs' = Set(),
    stageInflight' = emptyRequestMap,
    stageRetryDue' = Set(),
    stageRetriedTargets' = Set(),
    stageReceiptTargets' = Set(),
    parallelStageSuccessTargets' = Set(),
    postCommitStageReceiptTargets' = Set(),
    repairInflight' = emptyRequestMap,
    localManifestHash' = emptyHashMap,
    lastAcceptedHash' = None,
    lastDecision' = NoDecision,
  }

  action beginIntent(desired: Set[Node], minCommit: int): bool = all {
    not(pending),
    validCommitThreshold(desired, minCommit),
    pending' = true,
    pendingHash' = ExpectedHash,
    pendingVersion' = Version1,
    desiredReplicas' = desired,
    minCommitReplicas' = minCommit,
    visibleCommitted' = false,
    commitReceipts' = Set(),
    committedTargetHash' = None,
    committedTargetVersion' = None,
    targetVersionIsCurrent' = true,
    currentObjectHash' = ExpectedHash,
    currentObjectVersion' = Version1,
    metadataReceiptHash' = emptyHashMap,
    durableReceipts' = Set(),
    currentLocations' = Set(),
    repairJobs' = desired,
    stageInflight' = emptyRequestMap,
    stageRetryDue' = Set(),
    stageRetriedTargets' = Set(),
    stageReceiptTargets' = Set(),
    parallelStageSuccessTargets' = Set(),
    postCommitStageReceiptTargets' = Set(),
    repairInflight' = emptyRequestMap,
    localManifestHash' = emptyHashMap,
    lastAcceptedHash' = None,
    lastDecision' = NoDecision,
  }

  action advanceCurrentObject(hash: ContentHash): bool = all {
    pending,
    targetVersionIsCurrent,
    pending' = pending,
    pendingHash' = pendingHash,
    pendingVersion' = pendingVersion,
    desiredReplicas' = desiredReplicas,
    minCommitReplicas' = minCommitReplicas,
    visibleCommitted' = visibleCommitted,
    commitReceipts' = commitReceipts,
    committedTargetHash' = committedTargetHash,
    committedTargetVersion' = committedTargetVersion,
    targetVersionIsCurrent' = false,
    currentObjectHash' = hash,
    currentObjectVersion' = Version2,
    metadataReceiptHash' = metadataReceiptHash,
    durableReceipts' = durableReceipts,
    currentLocations' = Set(),
    repairJobs' = repairJobs,
    stageInflight' = stageInflight,
    stageRetryDue' = stageRetryDue,
    stageRetriedTargets' = stageRetriedTargets,
    stageReceiptTargets' = stageReceiptTargets,
    parallelStageSuccessTargets' = parallelStageSuccessTargets,
    postCommitStageReceiptTargets' = postCommitStageReceiptTargets,
    repairInflight' = repairInflight,
    localManifestHash' = localManifestHash,
    lastAcceptedHash' = lastAcceptedHash,
    lastDecision' = NoDecision,
  }

  action cacheLocalManifest(replica: Node, hash: ContentHash): bool = all {
    pending,
    pending' = pending,
    pendingHash' = pendingHash,
    pendingVersion' = pendingVersion,
    desiredReplicas' = desiredReplicas,
    minCommitReplicas' = minCommitReplicas,
    visibleCommitted' = visibleCommitted,
    commitReceipts' = commitReceipts,
    committedTargetHash' = committedTargetHash,
    committedTargetVersion' = committedTargetVersion,
    targetVersionIsCurrent' = targetVersionIsCurrent,
    currentObjectHash' = currentObjectHash,
    currentObjectVersion' = currentObjectVersion,
    metadataReceiptHash' = metadataReceiptHash,
    durableReceipts' = durableReceipts,
    currentLocations' = currentLocations,
    repairJobs' = repairJobs,
    stageInflight' = stageInflight,
    stageRetryDue' = stageRetryDue,
    stageRetriedTargets' = stageRetriedTargets,
    stageReceiptTargets' = stageReceiptTargets,
    parallelStageSuccessTargets' = parallelStageSuccessTargets,
    postCommitStageReceiptTargets' = postCommitStageReceiptTargets,
    repairInflight' = repairInflight,
    localManifestHash' = localManifestHash.set(replica, Some(hash)),
    lastAcceptedHash' = lastAcceptedHash,
    lastDecision' = NoDecision,
  }

  action injectExistingMetadataReceipt(replica: Node, hash: ContentHash): bool = all {
    pending,
    metadataReceiptHash.get(replica) == None,
    pending' = pending,
    pendingHash' = pendingHash,
    pendingVersion' = pendingVersion,
    desiredReplicas' = desiredReplicas,
    minCommitReplicas' = minCommitReplicas,
    visibleCommitted' = visibleCommitted,
    commitReceipts' = commitReceipts,
    committedTargetHash' = committedTargetHash,
    committedTargetVersion' = committedTargetVersion,
    targetVersionIsCurrent' = targetVersionIsCurrent,
    currentObjectHash' = currentObjectHash,
    currentObjectVersion' = currentObjectVersion,
    metadataReceiptHash' = metadataReceiptHash.set(replica, Some(hash)),
    durableReceipts' = durableReceipts,
    currentLocations' = currentLocations,
    repairJobs' = repairJobs,
    stageInflight' = stageInflight,
    stageRetryDue' = stageRetryDue,
    stageRetriedTargets' = stageRetriedTargets,
    stageReceiptTargets' = stageReceiptTargets,
    parallelStageSuccessTargets' = parallelStageSuccessTargets,
    postCommitStageReceiptTargets' = postCommitStageReceiptTargets,
    repairInflight' = repairInflight,
    localManifestHash' = localManifestHash,
    lastAcceptedHash' = lastAcceptedHash,
    lastDecision' = NoDecision,
  }

  action sendStage(replica: Node, requestId: RequestId): bool = all {
    pending,
    desiredReplicas.contains(replica),
    not(durableReceipts.contains(replica)),
    stageInflight.get(replica) == None,
    activeStageTargets.size() < StageFanoutCap,
    pending' = pending,
    pendingHash' = pendingHash,
    pendingVersion' = pendingVersion,
    desiredReplicas' = desiredReplicas,
    minCommitReplicas' = minCommitReplicas,
    visibleCommitted' = visibleCommitted,
    commitReceipts' = commitReceipts,
    committedTargetHash' = committedTargetHash,
    committedTargetVersion' = committedTargetVersion,
    targetVersionIsCurrent' = targetVersionIsCurrent,
    currentObjectHash' = currentObjectHash,
    currentObjectVersion' = currentObjectVersion,
    metadataReceiptHash' = metadataReceiptHash,
    durableReceipts' = durableReceipts,
    currentLocations' = currentLocations,
    repairJobs' = repairJobs,
    stageInflight' = stageInflight.set(replica, Some(requestId)),
    stageRetryDue' = stageRetryDue.filter(node => node != replica),
    stageRetriedTargets' =
      if (stageRetryDue.contains(replica)) {
        stageRetriedTargets.union(Set(replica))
      } else {
        stageRetriedTargets
      },
    stageReceiptTargets' = stageReceiptTargets,
    parallelStageSuccessTargets' = parallelStageSuccessTargets,
    postCommitStageReceiptTargets' = postCommitStageReceiptTargets,
    repairInflight' = repairInflight,
    localManifestHash' = localManifestHash,
    lastAcceptedHash' = lastAcceptedHash,
    lastDecision' = NoDecision,
  }

  action sendRepair(replica: Node, requestId: RequestId): bool = all {
    pending,
    repairJobs.contains(replica),
    repairInflight.get(replica) == None,
    pending' = pending,
    pendingHash' = pendingHash,
    pendingVersion' = pendingVersion,
    desiredReplicas' = desiredReplicas,
    minCommitReplicas' = minCommitReplicas,
    visibleCommitted' = visibleCommitted,
    commitReceipts' = commitReceipts,
    committedTargetHash' = committedTargetHash,
    committedTargetVersion' = committedTargetVersion,
    targetVersionIsCurrent' = targetVersionIsCurrent,
    currentObjectHash' = currentObjectHash,
    currentObjectVersion' = currentObjectVersion,
    metadataReceiptHash' = metadataReceiptHash,
    durableReceipts' = durableReceipts,
    currentLocations' = currentLocations,
    repairJobs' = repairJobs,
    stageInflight' = stageInflight,
    stageRetryDue' = stageRetryDue,
    stageRetriedTargets' = stageRetriedTargets,
    stageReceiptTargets' = stageReceiptTargets,
    parallelStageSuccessTargets' = parallelStageSuccessTargets,
    postCommitStageReceiptTargets' = postCommitStageReceiptTargets,
    repairInflight' = repairInflight.set(replica, Some(requestId)),
    localManifestHash' = localManifestHash,
    lastAcceptedHash' = lastAcceptedHash,
    lastDecision' = NoDecision,
  }

  action hydrateMatchingMetadataReceipt(replica: Node): bool =
    if (pending
      and desiredReplicas.contains(replica)
      and metadataReceiptHash.get(replica) == Some(pendingHash)) {
      all {
        pending' = pending,
        pendingHash' = pendingHash,
        pendingVersion' = pendingVersion,
        desiredReplicas' = desiredReplicas,
        minCommitReplicas' = minCommitReplicas,
        visibleCommitted' = visibleCommitted,
        commitReceipts' = commitReceipts,
        committedTargetHash' = committedTargetHash,
        committedTargetVersion' = committedTargetVersion,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
        currentObjectVersion' = currentObjectVersion,
        metadataReceiptHash' = metadataReceiptHash,
        durableReceipts' = durableReceipts.union(Set(replica)),
        currentLocations' =
          if (targetVersionIsCurrent and currentObjectHash == pendingHash) {
            currentLocations.union(Set(replica))
          } else {
            currentLocations
          },
        repairJobs' = missingDesiredReplicas(desiredReplicas, durableReceipts.union(Set(replica))),
        stageInflight' = stageInflight,
        stageRetryDue' = stageRetryDue.filter(node => node != replica),
        stageRetriedTargets' = stageRetriedTargets,
        stageReceiptTargets' = stageReceiptTargets,
        parallelStageSuccessTargets' = parallelStageSuccessTargets,
        postCommitStageReceiptTargets' = postCommitStageReceiptTargets,
        repairInflight' = repairInflight,
        localManifestHash' = localManifestHash,
        lastAcceptedHash' = Some(pendingHash),
        lastDecision' = ExistingReceiptHydrated,
      }
    } else {
      all {
        pending' = pending,
        pendingHash' = pendingHash,
        pendingVersion' = pendingVersion,
        desiredReplicas' = desiredReplicas,
        minCommitReplicas' = minCommitReplicas,
        visibleCommitted' = visibleCommitted,
        commitReceipts' = commitReceipts,
        committedTargetHash' = committedTargetHash,
        committedTargetVersion' = committedTargetVersion,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
        currentObjectVersion' = currentObjectVersion,
        metadataReceiptHash' = metadataReceiptHash,
        durableReceipts' = durableReceipts,
        currentLocations' = currentLocations,
        repairJobs' = repairJobs,
        stageInflight' = stageInflight,
        stageRetryDue' = stageRetryDue,
        stageRetriedTargets' = stageRetriedTargets,
        stageReceiptTargets' = stageReceiptTargets,
        parallelStageSuccessTargets' = parallelStageSuccessTargets,
        postCommitStageReceiptTargets' = postCommitStageReceiptTargets,
        repairInflight' = repairInflight,
        localManifestHash' = localManifestHash,
        lastAcceptedHash' = lastAcceptedHash,
        lastDecision' = LocalManifestIgnored,
      }
    }

  action recordLocalReceiptIfDesired(replica: Node): bool =
    if (pending
      and desiredReplicas.contains(replica)
      and not(durableReceipts.contains(replica))
      and localManifestHash.get(replica) == Some(pendingHash)
      and canPutReceipt(replica, pendingHash)) {
      all {
        pending' = pending,
        pendingHash' = pendingHash,
        pendingVersion' = pendingVersion,
        desiredReplicas' = desiredReplicas,
        minCommitReplicas' = minCommitReplicas,
        visibleCommitted' = visibleCommitted,
        commitReceipts' = commitReceipts,
        committedTargetHash' = committedTargetHash,
        committedTargetVersion' = committedTargetVersion,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
        currentObjectVersion' = currentObjectVersion,
        metadataReceiptHash' = metadataReceiptHash.set(replica, Some(pendingHash)),
        durableReceipts' = durableReceipts.union(Set(replica)),
        currentLocations' =
          if (targetVersionIsCurrent and currentObjectHash == pendingHash) {
            currentLocations.union(Set(replica))
          } else {
            currentLocations
          },
        repairJobs' = missingDesiredReplicas(desiredReplicas, durableReceipts.union(Set(replica))),
        stageInflight' = stageInflight,
        stageRetryDue' = stageRetryDue.filter(node => node != replica),
        stageRetriedTargets' = stageRetriedTargets,
        stageReceiptTargets' = stageReceiptTargets,
        parallelStageSuccessTargets' = parallelStageSuccessTargets,
        postCommitStageReceiptTargets' = postCommitStageReceiptTargets,
        repairInflight' = repairInflight,
        localManifestHash' = localManifestHash,
        lastAcceptedHash' = Some(pendingHash),
        lastDecision' = ReceiptRecorded,
      }
    } else {
      all {
        pending' = pending,
        pendingHash' = pendingHash,
        pendingVersion' = pendingVersion,
        desiredReplicas' = desiredReplicas,
        minCommitReplicas' = minCommitReplicas,
        visibleCommitted' = visibleCommitted,
        commitReceipts' = commitReceipts,
        committedTargetHash' = committedTargetHash,
        committedTargetVersion' = committedTargetVersion,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
        currentObjectVersion' = currentObjectVersion,
        metadataReceiptHash' = metadataReceiptHash,
        durableReceipts' = durableReceipts,
        currentLocations' = currentLocations,
        repairJobs' = repairJobs,
        stageInflight' = stageInflight,
        stageRetryDue' = stageRetryDue,
        stageRetriedTargets' = stageRetriedTargets,
        stageReceiptTargets' = stageReceiptTargets,
        parallelStageSuccessTargets' = parallelStageSuccessTargets,
        postCommitStageReceiptTargets' = postCommitStageReceiptTargets,
        repairInflight' = repairInflight,
        localManifestHash' = localManifestHash,
        lastAcceptedHash' = lastAcceptedHash,
        lastDecision' = LocalManifestIgnored,
      }
    }

  /// Publication snapshots the W receipts and target identity. Later matching
  /// stage responses extend durable placement but cannot rewrite that commit.
  action commitVisibleTarget: bool = all {
    pending,
    not(visibleCommitted),
    targetVersionIsCurrent,
    currentObjectHash == pendingHash,
    currentObjectVersion == pendingVersion,
    durableReceipts.size() >= minCommitReplicas,
    pending' = pending,
    pendingHash' = pendingHash,
    pendingVersion' = pendingVersion,
    desiredReplicas' = desiredReplicas,
    minCommitReplicas' = minCommitReplicas,
    visibleCommitted' = true,
    commitReceipts' = durableReceipts,
    committedTargetHash' = Some(pendingHash),
    committedTargetVersion' = Some(pendingVersion),
    targetVersionIsCurrent' = targetVersionIsCurrent,
    currentObjectHash' = currentObjectHash,
    currentObjectVersion' = currentObjectVersion,
    metadataReceiptHash' = metadataReceiptHash,
    durableReceipts' = durableReceipts,
    currentLocations' = currentLocations,
    repairJobs' = repairJobs,
    stageInflight' = stageInflight,
    stageRetryDue' = stageRetryDue,
    stageRetriedTargets' = stageRetriedTargets,
    stageReceiptTargets' = stageReceiptTargets,
    parallelStageSuccessTargets' = parallelStageSuccessTargets,
    postCommitStageReceiptTargets' = postCommitStageReceiptTargets,
    repairInflight' = repairInflight,
    localManifestHash' = localManifestHash,
    lastAcceptedHash' = lastAcceptedHash,
    lastDecision' = NoDecision,
  }

  action receiveStageStored(
    replica: Node,
    requestId: RequestId,
    responseHash: ContentHash
  ): bool =
    if (pending
      and desiredReplicas.contains(replica)
      and stageInflight.get(replica) == Some(requestId)
      and responseHash == pendingHash
      and canPutReceipt(replica, responseHash)) {
      all {
        pending' = pending,
        pendingHash' = pendingHash,
        pendingVersion' = pendingVersion,
        desiredReplicas' = desiredReplicas,
        minCommitReplicas' = minCommitReplicas,
        visibleCommitted' = visibleCommitted,
        commitReceipts' = commitReceipts,
        committedTargetHash' = committedTargetHash,
        committedTargetVersion' = committedTargetVersion,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
        currentObjectVersion' = currentObjectVersion,
        metadataReceiptHash' = metadataReceiptHash.set(replica, Some(responseHash)),
        durableReceipts' = durableReceipts.union(Set(replica)),
        currentLocations' =
          if (targetVersionIsCurrent and currentObjectHash == responseHash) {
            currentLocations.union(Set(replica))
          } else {
            currentLocations
          },
        repairJobs' = missingDesiredReplicas(desiredReplicas, durableReceipts.union(Set(replica))),
        stageInflight' = stageInflight.set(replica, None),
        stageRetryDue' = stageRetryDue.filter(node => node != replica),
        stageRetriedTargets' = stageRetriedTargets,
        stageReceiptTargets' = stageReceiptTargets.union(Set(replica)),
        parallelStageSuccessTargets' =
          if (activeStageTargets.size() > 1) {
            parallelStageSuccessTargets.union(Set(replica))
          } else {
            parallelStageSuccessTargets
          },
        postCommitStageReceiptTargets' =
          if (visibleCommitted and not(commitReceipts.contains(replica))) {
            postCommitStageReceiptTargets.union(Set(replica))
          } else {
            postCommitStageReceiptTargets
          },
        repairInflight' = repairInflight,
        localManifestHash' = localManifestHash,
        lastAcceptedHash' = Some(responseHash),
        lastDecision' = ReceiptRecorded,
      }
    } else {
      all {
        pending' = pending,
        pendingHash' = pendingHash,
        pendingVersion' = pendingVersion,
        desiredReplicas' = desiredReplicas,
        minCommitReplicas' = minCommitReplicas,
        visibleCommitted' = visibleCommitted,
        commitReceipts' = commitReceipts,
        committedTargetHash' = committedTargetHash,
        committedTargetVersion' = committedTargetVersion,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
        currentObjectVersion' = currentObjectVersion,
        metadataReceiptHash' = metadataReceiptHash,
        durableReceipts' = durableReceipts,
        currentLocations' = currentLocations,
        repairJobs' = repairJobs,
        stageInflight' = stageInflight,
        stageRetryDue' = stageRetryDue,
        stageRetriedTargets' = stageRetriedTargets,
        stageReceiptTargets' = stageReceiptTargets,
        parallelStageSuccessTargets' = parallelStageSuccessTargets,
        postCommitStageReceiptTargets' = postCommitStageReceiptTargets,
        repairInflight' = repairInflight,
        localManifestHash' = localManifestHash,
        lastAcceptedHash' = lastAcceptedHash,
        lastDecision' =
          if (stageInflight.get(replica) == Some(requestId)) {
            ResponseRejected
          } else {
            ResponseDiscarded
          },
      }
    }

  action receiveStageRejected(replica: Node, requestId: RequestId): bool =
    if (pending and stageInflight.get(replica) == Some(requestId)) {
      all {
        pending' = pending,
        pendingHash' = pendingHash,
        pendingVersion' = pendingVersion,
        desiredReplicas' = desiredReplicas,
        minCommitReplicas' = minCommitReplicas,
        visibleCommitted' = visibleCommitted,
        commitReceipts' = commitReceipts,
        committedTargetHash' = committedTargetHash,
        committedTargetVersion' = committedTargetVersion,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
        currentObjectVersion' = currentObjectVersion,
        metadataReceiptHash' = metadataReceiptHash,
        durableReceipts' = durableReceipts,
        currentLocations' = currentLocations,
        repairJobs' = repairJobs,
        stageInflight' = stageInflight.set(replica, None),
        stageRetryDue' =
          if (durableReceipts.contains(replica)) {
            stageRetryDue.filter(node => node != replica)
          } else {
            stageRetryDue.union(Set(replica))
          },
        stageRetriedTargets' = stageRetriedTargets,
        stageReceiptTargets' = stageReceiptTargets,
        parallelStageSuccessTargets' = parallelStageSuccessTargets,
        postCommitStageReceiptTargets' = postCommitStageReceiptTargets,
        repairInflight' = repairInflight,
        localManifestHash' = localManifestHash,
        lastAcceptedHash' = lastAcceptedHash,
        lastDecision' = RetryScheduled,
      }
    } else {
      all {
        pending' = pending,
        pendingHash' = pendingHash,
        pendingVersion' = pendingVersion,
        desiredReplicas' = desiredReplicas,
        minCommitReplicas' = minCommitReplicas,
        visibleCommitted' = visibleCommitted,
        commitReceipts' = commitReceipts,
        committedTargetHash' = committedTargetHash,
        committedTargetVersion' = committedTargetVersion,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
        currentObjectVersion' = currentObjectVersion,
        metadataReceiptHash' = metadataReceiptHash,
        durableReceipts' = durableReceipts,
        currentLocations' = currentLocations,
        repairJobs' = repairJobs,
        stageInflight' = stageInflight,
        stageRetryDue' = stageRetryDue,
        stageRetriedTargets' = stageRetriedTargets,
        stageReceiptTargets' = stageReceiptTargets,
        parallelStageSuccessTargets' = parallelStageSuccessTargets,
        postCommitStageReceiptTargets' = postCommitStageReceiptTargets,
        repairInflight' = repairInflight,
        localManifestHash' = localManifestHash,
        lastAcceptedHash' = lastAcceptedHash,
        lastDecision' = ResponseDiscarded,
      }
    }

  action receiveRepairCommitted(
    replica: Node,
    requestId: RequestId,
    responseHash: ContentHash
  ): bool =
    if (pending
      and desiredReplicas.contains(replica)
      and repairResponseRequestAuthorized(replica, requestId)
      and responseHash == pendingHash
      and canPutReceipt(replica, responseHash)) {
      all {
        pending' = pending,
        pendingHash' = pendingHash,
        pendingVersion' = pendingVersion,
        desiredReplicas' = desiredReplicas,
        minCommitReplicas' = minCommitReplicas,
        visibleCommitted' = visibleCommitted,
        commitReceipts' = commitReceipts,
        committedTargetHash' = committedTargetHash,
        committedTargetVersion' = committedTargetVersion,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
        currentObjectVersion' = currentObjectVersion,
        metadataReceiptHash' = metadataReceiptHash.set(replica, Some(responseHash)),
        durableReceipts' = durableReceipts.union(Set(replica)),
        currentLocations' =
          if (targetVersionIsCurrent and currentObjectHash == responseHash) {
            currentLocations.union(Set(replica))
          } else {
            currentLocations
          },
        repairJobs' = missingDesiredReplicas(desiredReplicas, durableReceipts.union(Set(replica))),
        stageInflight' = stageInflight,
        stageRetryDue' = stageRetryDue.filter(node => node != replica),
        stageRetriedTargets' = stageRetriedTargets,
        stageReceiptTargets' = stageReceiptTargets,
        parallelStageSuccessTargets' = parallelStageSuccessTargets,
        postCommitStageReceiptTargets' = postCommitStageReceiptTargets,
        repairInflight' = repairInflight.set(replica, None),
        localManifestHash' = localManifestHash,
        lastAcceptedHash' = Some(responseHash),
        lastDecision' = ReceiptRecorded,
      }
    } else {
      all {
        pending' = pending,
        pendingHash' = pendingHash,
        pendingVersion' = pendingVersion,
        desiredReplicas' = desiredReplicas,
        minCommitReplicas' = minCommitReplicas,
        visibleCommitted' = visibleCommitted,
        commitReceipts' = commitReceipts,
        committedTargetHash' = committedTargetHash,
        committedTargetVersion' = committedTargetVersion,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
        currentObjectVersion' = currentObjectVersion,
        metadataReceiptHash' = metadataReceiptHash,
        durableReceipts' = durableReceipts,
        currentLocations' = currentLocations,
        repairJobs' = repairJobs,
        stageInflight' = stageInflight,
        stageRetryDue' = stageRetryDue,
        stageRetriedTargets' = stageRetriedTargets,
        stageReceiptTargets' = stageReceiptTargets,
        parallelStageSuccessTargets' = parallelStageSuccessTargets,
        postCommitStageReceiptTargets' = postCommitStageReceiptTargets,
        repairInflight' = repairInflight,
        localManifestHash' = localManifestHash,
        lastAcceptedHash' = lastAcceptedHash,
        lastDecision' =
          if (repairResponseRequestAuthorized(replica, requestId)) {
            ResponseRejected
          } else {
            ResponseDiscarded
          },
      }
    }

  action receiveRepairRejected(replica: Node, requestId: RequestId): bool =
    if (pending and repairResponseRequestAuthorized(replica, requestId)) {
      all {
        pending' = pending,
        pendingHash' = pendingHash,
        pendingVersion' = pendingVersion,
        desiredReplicas' = desiredReplicas,
        minCommitReplicas' = minCommitReplicas,
        visibleCommitted' = visibleCommitted,
        commitReceipts' = commitReceipts,
        committedTargetHash' = committedTargetHash,
        committedTargetVersion' = committedTargetVersion,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
        currentObjectVersion' = currentObjectVersion,
        metadataReceiptHash' = metadataReceiptHash,
        durableReceipts' = durableReceipts,
        currentLocations' = currentLocations,
        repairJobs' = repairJobs,
        stageInflight' = stageInflight,
        stageRetryDue' = stageRetryDue,
        stageRetriedTargets' = stageRetriedTargets,
        stageReceiptTargets' = stageReceiptTargets,
        parallelStageSuccessTargets' = parallelStageSuccessTargets,
        postCommitStageReceiptTargets' = postCommitStageReceiptTargets,
        repairInflight' = repairInflight.set(replica, None),
        localManifestHash' = localManifestHash,
        lastAcceptedHash' = lastAcceptedHash,
        lastDecision' = RetryScheduled,
      }
    } else {
      all {
        pending' = pending,
        pendingHash' = pendingHash,
        pendingVersion' = pendingVersion,
        desiredReplicas' = desiredReplicas,
        minCommitReplicas' = minCommitReplicas,
        visibleCommitted' = visibleCommitted,
        commitReceipts' = commitReceipts,
        committedTargetHash' = committedTargetHash,
        committedTargetVersion' = committedTargetVersion,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
        currentObjectVersion' = currentObjectVersion,
        metadataReceiptHash' = metadataReceiptHash,
        durableReceipts' = durableReceipts,
        currentLocations' = currentLocations,
        repairJobs' = repairJobs,
        stageInflight' = stageInflight,
        stageRetryDue' = stageRetryDue,
        stageRetriedTargets' = stageRetriedTargets,
        stageReceiptTargets' = stageReceiptTargets,
        parallelStageSuccessTargets' = parallelStageSuccessTargets,
        postCommitStageReceiptTargets' = postCommitStageReceiptTargets,
        repairInflight' = repairInflight,
        localManifestHash' = localManifestHash,
        lastAcceptedHash' = lastAcceptedHash,
        lastDecision' = ResponseDiscarded,
      }
    }

  action step = any {
    {
      nondet desired = oneOf(ReplicaSets)
      nondet minCommit = oneOf(CommitThresholds)
      beginIntent(desired, minCommit)
    },
    {
      nondet hash = oneOf(ContentHashes)
      advanceCurrentObject(hash)
    },
    {
      nondet replica = oneOf(Nodes)
      nondet hash = oneOf(ContentHashes)
      any {
        cacheLocalManifest(replica, hash),
        injectExistingMetadataReceipt(replica, hash),
      }
    },
    {
      nondet replica = oneOf(Nodes)
      nondet requestId = oneOf(RequestIds)
      any {
        sendStage(replica, requestId),
        sendRepair(replica, requestId),
      }
    },
    {
      nondet replica = oneOf(Nodes)
      any {
        hydrateMatchingMetadataReceipt(replica),
        recordLocalReceiptIfDesired(replica),
      }
    },
    commitVisibleTarget,
    {
      nondet replica = oneOf(Nodes)
      nondet requestId = oneOf(RequestIds)
      nondet hash = oneOf(ContentHashes)
      any {
        receiveStageStored(replica, requestId, hash),
        receiveRepairCommitted(replica, requestId, hash),
      }
    },
    {
      nondet replica = oneOf(Nodes)
      nondet requestId = oneOf(RequestIds)
      any {
        receiveStageRejected(replica, requestId),
        receiveRepairRejected(replica, requestId),
      }
    },
  }

  /// Coverage schedules call only real protocol actions. The three distinct
  /// policies select independent traces for parallel completion, isolated
  /// failure/retry, and a receipt that arrives after the W commit. Symbolic
  /// verification continues to explore the unrestricted `step` relation.
  action parallelStageCoverageStep =
    if (not(pending)) {
      beginIntent(Set(N1, N2, N3), 3)
    } else {
      all {
        desiredReplicas == Set(N1, N2, N3),
        minCommitReplicas == 3,
        if (stageInflight.get(N2) == None
          and stageInflight.get(N3) == None
          and not(stageReceiptTargets.contains(N3))) {
          sendStage(N2, Req1)
        } else if (isSome(stageInflight.get(N2))
          and stageInflight.get(N3) == None
          and not(stageReceiptTargets.contains(N3))) {
          sendStage(N3, Req1)
        } else if (isSome(stageInflight.get(N2))
          and isSome(stageInflight.get(N3))
          and not(stageReceiptTargets.contains(N3))) {
          receiveStageStored(N3, Req1, ExpectedHash)
        } else {
          recordLocalReceiptIfDesired(N1)
        },
      }
    }

  action isolatedRetryCoverageStep =
    if (not(pending)) {
      beginIntent(Set(N1, N2, N3), 2)
    } else {
      all {
        desiredReplicas == Set(N1, N2, N3),
        minCommitReplicas == 2,
        if (stageInflight.get(N2) == None
          and stageInflight.get(N3) == None
          and not(stageRetryDue.contains(N2))
          and not(stageRetriedTargets.contains(N2))) {
          sendStage(N2, Req1)
        } else if (isSome(stageInflight.get(N2))
          and stageInflight.get(N3) == None
          and not(stageRetryDue.contains(N2))) {
          sendStage(N3, Req1)
        } else if (isSome(stageInflight.get(N2))
          and isSome(stageInflight.get(N3))
          and not(stageRetriedTargets.contains(N2))) {
          receiveStageRejected(N2, Req1)
        } else if (stageRetryDue.contains(N2)
          and isSome(stageInflight.get(N3))) {
          sendStage(N2, Req2)
        } else {
          recordLocalReceiptIfDesired(N1)
        },
      }
    }

  action postCommitReceiptCoverageStep =
    if (not(pending)) {
      beginIntent(Set(N2, N3), 1)
    } else {
      all {
        desiredReplicas == Set(N2, N3),
        minCommitReplicas == 1,
        if (stageInflight.get(N2) == None
          and stageInflight.get(N3) == None
          and durableReceipts.size() == 0) {
          sendStage(N2, Req1)
        } else if (isSome(stageInflight.get(N2))
          and stageInflight.get(N3) == None
          and durableReceipts.size() == 0) {
          sendStage(N3, Req1)
        } else if (isSome(stageInflight.get(N2))
          and isSome(stageInflight.get(N3))
          and durableReceipts.size() == 0) {
          receiveStageStored(N2, Req1, ExpectedHash)
        } else if (not(visibleCommitted)
          and durableReceipts == Set(N2)
          and isSome(stageInflight.get(N3))) {
          commitVisibleTarget
        } else if (visibleCommitted
          and isSome(stageInflight.get(N3))
          and not(postCommitStageReceiptTargets.contains(N3))) {
          receiveStageStored(N3, Req1, ExpectedHash)
        } else {
          recordLocalReceiptIfDesired(N2)
        },
      }
    }

  action coverageStep = any {
    parallelStageCoverageStep,
    isolatedRetryCoverageStep,
    postCommitReceiptCoverageStep,
  }
}
