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
/// - durable receipts and current-location updates only use the pending hash.
module dfs_repair_content_validation {
  import common.* from "../../lib/common"

  type RequestId = Req1 | Req2
  type ContentHash = ExpectedHash | OtherHash
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
  var desiredReplicas: Set[Node]
  var targetVersionIsCurrent: bool
  var currentObjectHash: ContentHash
  var metadataReceiptHash: Node -> Option[ContentHash]
  var durableReceipts: Set[Node]
  var currentLocations: Set[Node]
  var repairJobs: Set[Node]
  var stageInflight: Node -> Option[RequestId]
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

  def allRepairInflightEmpty: bool =
    Nodes.filter(node => isSome(repairInflight.get(node))).size() == 0

  def metadataReceiptAccepts(replica: Node, hash: ContentHash): bool =
    match metadataReceiptHash.get(replica) {
      | Some(existingHash) => existingHash == hash
      | None => true
    }

  def targetVersionHashStable: bool =
    not(pending) or not(targetVersionIsCurrent) or currentObjectHash == pendingHash

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
    desiredReplicas' = Set(),
    targetVersionIsCurrent' = false,
    currentObjectHash' = ExpectedHash,
    metadataReceiptHash' = emptyHashMap,
    durableReceipts' = Set(),
    currentLocations' = Set(),
    repairJobs' = Set(),
    stageInflight' = emptyRequestMap,
    repairInflight' = emptyRequestMap,
    localManifestHash' = emptyHashMap,
    lastAcceptedHash' = None,
    lastDecision' = NoDecision,
  }

  action beginIntent(desired: Set[Node]): bool = all {
    not(pending),
    desired.size() > 0,
    pending' = true,
    pendingHash' = ExpectedHash,
    desiredReplicas' = desired,
    targetVersionIsCurrent' = true,
    currentObjectHash' = ExpectedHash,
    metadataReceiptHash' = emptyHashMap,
    durableReceipts' = Set(),
    currentLocations' = Set(),
    repairJobs' = desired,
    stageInflight' = emptyRequestMap,
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
    desiredReplicas' = desiredReplicas,
    targetVersionIsCurrent' = false,
    currentObjectHash' = hash,
    metadataReceiptHash' = metadataReceiptHash,
    durableReceipts' = durableReceipts,
    currentLocations' = Set(),
    repairJobs' = repairJobs,
    stageInflight' = stageInflight,
    repairInflight' = repairInflight,
    localManifestHash' = localManifestHash,
    lastAcceptedHash' = lastAcceptedHash,
    lastDecision' = NoDecision,
  }

  action cacheLocalManifest(replica: Node, hash: ContentHash): bool = all {
    pending,
    pending' = pending,
    pendingHash' = pendingHash,
    desiredReplicas' = desiredReplicas,
    targetVersionIsCurrent' = targetVersionIsCurrent,
    currentObjectHash' = currentObjectHash,
    metadataReceiptHash' = metadataReceiptHash,
    durableReceipts' = durableReceipts,
    currentLocations' = currentLocations,
    repairJobs' = repairJobs,
    stageInflight' = stageInflight,
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
    desiredReplicas' = desiredReplicas,
    targetVersionIsCurrent' = targetVersionIsCurrent,
    currentObjectHash' = currentObjectHash,
    metadataReceiptHash' = metadataReceiptHash.set(replica, Some(hash)),
    durableReceipts' = durableReceipts,
    currentLocations' = currentLocations,
    repairJobs' = repairJobs,
    stageInflight' = stageInflight,
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
    pending' = pending,
    pendingHash' = pendingHash,
    desiredReplicas' = desiredReplicas,
    targetVersionIsCurrent' = targetVersionIsCurrent,
    currentObjectHash' = currentObjectHash,
    metadataReceiptHash' = metadataReceiptHash,
    durableReceipts' = durableReceipts,
    currentLocations' = currentLocations,
    repairJobs' = repairJobs,
    stageInflight' = stageInflight.set(replica, Some(requestId)),
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
    desiredReplicas' = desiredReplicas,
    targetVersionIsCurrent' = targetVersionIsCurrent,
    currentObjectHash' = currentObjectHash,
    metadataReceiptHash' = metadataReceiptHash,
    durableReceipts' = durableReceipts,
    currentLocations' = currentLocations,
    repairJobs' = repairJobs,
    stageInflight' = stageInflight,
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
        desiredReplicas' = desiredReplicas,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
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
        repairInflight' = repairInflight,
        localManifestHash' = localManifestHash,
        lastAcceptedHash' = Some(pendingHash),
        lastDecision' = ExistingReceiptHydrated,
      }
    } else {
      all {
        pending' = pending,
        pendingHash' = pendingHash,
        desiredReplicas' = desiredReplicas,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
        metadataReceiptHash' = metadataReceiptHash,
        durableReceipts' = durableReceipts,
        currentLocations' = currentLocations,
        repairJobs' = repairJobs,
        stageInflight' = stageInflight,
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
        desiredReplicas' = desiredReplicas,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
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
        repairInflight' = repairInflight,
        localManifestHash' = localManifestHash,
        lastAcceptedHash' = Some(pendingHash),
        lastDecision' = ReceiptRecorded,
      }
    } else {
      all {
        pending' = pending,
        pendingHash' = pendingHash,
        desiredReplicas' = desiredReplicas,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
        metadataReceiptHash' = metadataReceiptHash,
        durableReceipts' = durableReceipts,
        currentLocations' = currentLocations,
        repairJobs' = repairJobs,
        stageInflight' = stageInflight,
        repairInflight' = repairInflight,
        localManifestHash' = localManifestHash,
        lastAcceptedHash' = lastAcceptedHash,
        lastDecision' = LocalManifestIgnored,
      }
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
        desiredReplicas' = desiredReplicas,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
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
        repairInflight' = repairInflight,
        localManifestHash' = localManifestHash,
        lastAcceptedHash' = Some(responseHash),
        lastDecision' = ReceiptRecorded,
      }
    } else {
      all {
        pending' = pending,
        pendingHash' = pendingHash,
        desiredReplicas' = desiredReplicas,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
        metadataReceiptHash' = metadataReceiptHash,
        durableReceipts' = durableReceipts,
        currentLocations' = currentLocations,
        repairJobs' = repairJobs,
        stageInflight' = stageInflight,
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
        desiredReplicas' = desiredReplicas,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
        metadataReceiptHash' = metadataReceiptHash,
        durableReceipts' = durableReceipts,
        currentLocations' = currentLocations,
        repairJobs' = repairJobs,
        stageInflight' = stageInflight.set(replica, None),
        repairInflight' = repairInflight,
        localManifestHash' = localManifestHash,
        lastAcceptedHash' = lastAcceptedHash,
        lastDecision' = RetryScheduled,
      }
    } else {
      all {
        pending' = pending,
        pendingHash' = pendingHash,
        desiredReplicas' = desiredReplicas,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
        metadataReceiptHash' = metadataReceiptHash,
        durableReceipts' = durableReceipts,
        currentLocations' = currentLocations,
        repairJobs' = repairJobs,
        stageInflight' = stageInflight,
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
        desiredReplicas' = desiredReplicas,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
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
        repairInflight' = repairInflight.set(replica, None),
        localManifestHash' = localManifestHash,
        lastAcceptedHash' = Some(responseHash),
        lastDecision' = ReceiptRecorded,
      }
    } else {
      all {
        pending' = pending,
        pendingHash' = pendingHash,
        desiredReplicas' = desiredReplicas,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
        metadataReceiptHash' = metadataReceiptHash,
        durableReceipts' = durableReceipts,
        currentLocations' = currentLocations,
        repairJobs' = repairJobs,
        stageInflight' = stageInflight,
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
        desiredReplicas' = desiredReplicas,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
        metadataReceiptHash' = metadataReceiptHash,
        durableReceipts' = durableReceipts,
        currentLocations' = currentLocations,
        repairJobs' = repairJobs,
        stageInflight' = stageInflight,
        repairInflight' = repairInflight.set(replica, None),
        localManifestHash' = localManifestHash,
        lastAcceptedHash' = lastAcceptedHash,
        lastDecision' = RetryScheduled,
      }
    } else {
      all {
        pending' = pending,
        pendingHash' = pendingHash,
        desiredReplicas' = desiredReplicas,
        targetVersionIsCurrent' = targetVersionIsCurrent,
        currentObjectHash' = currentObjectHash,
        metadataReceiptHash' = metadataReceiptHash,
        durableReceipts' = durableReceipts,
        currentLocations' = currentLocations,
        repairJobs' = repairJobs,
        stageInflight' = stageInflight,
        repairInflight' = repairInflight,
        localManifestHash' = localManifestHash,
        lastAcceptedHash' = lastAcceptedHash,
        lastDecision' = ResponseDiscarded,
      }
    }

  action step = any {
    {
      nondet desired = oneOf(ReplicaSets)
      beginIntent(desired)
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
}
