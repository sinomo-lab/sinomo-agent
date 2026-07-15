/// DFS coordinator receipt-bucket equivalence.
///
/// Evidence class: target hardening for replacing full historical receipt-map
/// scans in `DistributedEventualCoordinator` with per-intent receipt buckets.
/// The reference state is a flat bounded `(intent_id, replica_node_id)` map;
/// the optimized state is an `intent_id -> replica_node_id -> receipt` map.
///
/// Every accepted receipt is deterministic for its intent and replica. Thus a
/// duplicate record or restore overwrites the same key with the same value and
/// is idempotent. Runtime receipt recording rejects a closed intent or an
/// unexpected replica, while durable restore accepts a retained closed intent
/// but still rejects an unexpected replica. Rejections mutate neither view.
///
/// `receipts_for_intent` is modeled as a bounded ordered projection in the
/// same `NodeA < NodeB < NodeC` order produced by both concrete BTree layouts.
/// Replica policy selection, intent commit, repair-job maintenance, durable
/// persistence, and local-first operation are outside this refinement slice.
module dfs_receipt_bucket_equivalence {
  import common.* from "../../lib/common"

  type IntentId = IntentA | IntentB | IntentClosed
  type ReplicaNode = NodeA | NodeB | NodeC
  type ObjectId = ObjectA | ObjectB | ObjectClosed
  type TargetVersion = VersionA | VersionB | VersionClosed
  /// `NonDurable` is an uninhabited storage-state alternative in this slice;
  /// every accepted coordinator receipt is the `Durable` variant.
  type ReceiptState = Durable | NonDurable
  type ReceiptOperation = RecordReceipt | RestoreReceipt
  type ReceiptTransition = Initialized | Recorded | Restored | Rejected

  /// Finite encoding of the flat `(intent_id, replica_node_id)` BTree key.
  type FlatReceiptKey = ReceiptAAtNodeA | ReceiptAAtNodeB | ReceiptAAtNodeC
    | ReceiptBAtNodeA | ReceiptBAtNodeB | ReceiptBAtNodeC
    | ReceiptClosedAtNodeA | ReceiptClosedAtNodeB | ReceiptClosedAtNodeC

  type ReceiptRecord = {
    intent: IntentId,
    object: ObjectId,
    version: TargetVersion,
    replica: ReplicaNode,
    state: ReceiptState,
  }

  /// A bounded sequence shape for the ordered result of
  /// `receipts_for_intent`. `first`, `second`, and `third` are compacted in
  /// replica-node order; absent trailing positions are `None`.
  type OrderedReceiptView = {
    length: int,
    first: Option[ReceiptRecord],
    second: Option[ReceiptRecord],
    third: Option[ReceiptRecord],
  }

  pure val IntentIds: Set[IntentId] = Set(IntentA, IntentB, IntentClosed)
  pure val ReplicaNodes: Set[ReplicaNode] = Set(NodeA, NodeB, NodeC)
  pure val ReceiptOperations: Set[ReceiptOperation] =
    Set(RecordReceipt, RestoreReceipt)
  pure val FlatReceiptKeys: Set[FlatReceiptKey] = Set(
    ReceiptAAtNodeA,
    ReceiptAAtNodeB,
    ReceiptAAtNodeC,
    ReceiptBAtNodeA,
    ReceiptBAtNodeB,
    ReceiptBAtNodeC,
    ReceiptClosedAtNodeA,
    ReceiptClosedAtNodeB,
    ReceiptClosedAtNodeC
  )

  pure val emptyFlatReceipts: FlatReceiptKey -> Option[ReceiptRecord] =
    FlatReceiptKeys.mapBy(_ => None)

  pure val emptyIntentBucket: ReplicaNode -> Option[ReceiptRecord] =
    ReplicaNodes.mapBy(_ => None)

  pure val emptyReceiptBuckets:
    IntentId -> (ReplicaNode -> Option[ReceiptRecord]) =
      IntentIds.mapBy(_ => emptyIntentBucket)

  var flatReceipts: FlatReceiptKey -> Option[ReceiptRecord]
  var receiptBuckets: IntentId -> (ReplicaNode -> Option[ReceiptRecord])

  /// Snapshots make exact-key update, overwrite idempotency, rejection
  /// stuttering, and cross-intent isolation explicit transition obligations.
  var beforeFlatReceipts: FlatReceiptKey -> Option[ReceiptRecord]
  var beforeReceiptBuckets:
    IntentId -> (ReplicaNode -> Option[ReceiptRecord])
  var lastTransition: ReceiptTransition
  var lastOperation: ReceiptOperation
  var lastIntent: IntentId
  var lastReplica: ReplicaNode
  var lastOverwrote: bool

  pure def objectFor(intent: IntentId): ObjectId =
    match intent {
      | IntentA => ObjectA
      | IntentB => ObjectB
      | IntentClosed => ObjectClosed
    }

  pure def versionFor(intent: IntentId): TargetVersion =
    match intent {
      | IntentA => VersionA
      | IntentB => VersionB
      | IntentClosed => VersionClosed
    }

  pure def receiptFor(
    intent: IntentId,
    replica: ReplicaNode,
  ): ReceiptRecord = {
    intent: intent,
    object: objectFor(intent),
    version: versionFor(intent),
    replica: replica,
    state: Durable,
  }

  pure def flatKey(
    intent: IntentId,
    replica: ReplicaNode,
  ): FlatReceiptKey =
    match intent {
      | IntentA =>
          match replica {
            | NodeA => ReceiptAAtNodeA
            | NodeB => ReceiptAAtNodeB
            | NodeC => ReceiptAAtNodeC
          }
      | IntentB =>
          match replica {
            | NodeA => ReceiptBAtNodeA
            | NodeB => ReceiptBAtNodeB
            | NodeC => ReceiptBAtNodeC
          }
      | IntentClosed =>
          match replica {
            | NodeA => ReceiptClosedAtNodeA
            | NodeB => ReceiptClosedAtNodeB
            | NodeC => ReceiptClosedAtNodeC
          }
    }

  /// Bounded desired-replica policies provide both accepted and unexpected
  /// replica paths without modeling policy selection itself.
  pure def desiredReplica(
    intent: IntentId,
    replica: ReplicaNode,
  ): bool =
    match intent {
      | IntentA => replica == NodeA or replica == NodeB
      | IntentB => replica == NodeB or replica == NodeC
      | IntentClosed => replica == NodeA or replica == NodeC
    }

  /// Runtime recording is allowed for the two open/committed representatives.
  /// `IntentClosed` abstracts Rejected, Conflict, or Aborted states.
  pure def recordAccepted(
    intent: IntentId,
    replica: ReplicaNode,
  ): bool =
    intent != IntentClosed and desiredReplica(intent, replica)

  /// Hydration accepts retained closed intents, matching
  /// `restore_durable_receipt_deferring_repair_rebuild`.
  pure def restoreAccepted(
    intent: IntentId,
    replica: ReplicaNode,
  ): bool = desiredReplica(intent, replica)

  pure def operationAccepted(
    operation: ReceiptOperation,
    intent: IntentId,
    replica: ReplicaNode,
  ): bool =
    if (operation == RecordReceipt) {
      recordAccepted(intent, replica)
    } else {
      restoreAccepted(intent, replica)
    }

  pure def flatReceiptAt(
    source: FlatReceiptKey -> Option[ReceiptRecord],
    intent: IntentId,
    replica: ReplicaNode,
  ): Option[ReceiptRecord] = source.get(flatKey(intent, replica))

  pure def bucketReceiptAt(
    source: IntentId -> (ReplicaNode -> Option[ReceiptRecord]),
    intent: IntentId,
    replica: ReplicaNode,
  ): Option[ReceiptRecord] = source.get(intent).get(replica)

  pure def putFlatReceipt(
    source: FlatReceiptKey -> Option[ReceiptRecord],
    intent: IntentId,
    replica: ReplicaNode,
  ): FlatReceiptKey -> Option[ReceiptRecord] =
    source.set(flatKey(intent, replica), Some(receiptFor(intent, replica)))

  pure def putBucketReceipt(
    source: IntentId -> (ReplicaNode -> Option[ReceiptRecord]),
    intent: IntentId,
    replica: ReplicaNode,
  ): IntentId -> (ReplicaNode -> Option[ReceiptRecord]) =
    source.set(
      intent,
      source.get(intent).set(replica, Some(receiptFor(intent, replica)))
    )

  pure def presentCount(value: Option[ReceiptRecord]): int =
    if (isSome(value)) 1 else 0

  pure def orderedReceiptView(
    nodeAReceipt: Option[ReceiptRecord],
    nodeBReceipt: Option[ReceiptRecord],
    nodeCReceipt: Option[ReceiptRecord],
  ): OrderedReceiptView = {
    length:
      presentCount(nodeAReceipt)
        + presentCount(nodeBReceipt)
        + presentCount(nodeCReceipt),
    first:
      if (isSome(nodeAReceipt)) {
        nodeAReceipt
      } else if (isSome(nodeBReceipt)) {
        nodeBReceipt
      } else {
        nodeCReceipt
      },
    second:
      if (isSome(nodeAReceipt)) {
        if (isSome(nodeBReceipt)) nodeBReceipt else nodeCReceipt
      } else if (isSome(nodeBReceipt)) {
        nodeCReceipt
      } else {
        None
      },
    third:
      if (all {
        isSome(nodeAReceipt),
        isSome(nodeBReceipt),
        isSome(nodeCReceipt),
      }) nodeCReceipt else None,
  }

  pure def referenceReceiptsForIntentAt(
    source: FlatReceiptKey -> Option[ReceiptRecord],
    intent: IntentId,
  ): OrderedReceiptView =
    orderedReceiptView(
      flatReceiptAt(source, intent, NodeA),
      flatReceiptAt(source, intent, NodeB),
      flatReceiptAt(source, intent, NodeC)
    )

  pure def bucketReceiptsForIntentAt(
    source: IntentId -> (ReplicaNode -> Option[ReceiptRecord]),
    intent: IntentId,
  ): OrderedReceiptView =
    orderedReceiptView(
      bucketReceiptAt(source, intent, NodeA),
      bucketReceiptAt(source, intent, NodeB),
      bucketReceiptAt(source, intent, NodeC)
    )

  def referenceReceiptsForIntent(intent: IntentId): OrderedReceiptView =
    referenceReceiptsForIntentAt(flatReceipts, intent)

  def bucketReceiptsForIntent(intent: IntentId): OrderedReceiptView =
    bucketReceiptsForIntentAt(receiptBuckets, intent)

  pure def referenceDurableNodesAt(
    source: FlatReceiptKey -> Option[ReceiptRecord],
    intent: IntentId,
  ): Set[ReplicaNode] =
    ReplicaNodes.filter(replica =>
      isSome(flatReceiptAt(source, intent, replica))
    )

  pure def bucketDurableNodesAt(
    source: IntentId -> (ReplicaNode -> Option[ReceiptRecord]),
    intent: IntentId,
  ): Set[ReplicaNode] =
    ReplicaNodes.filter(replica =>
      isSome(bucketReceiptAt(source, intent, replica))
    )

  def referenceDurableNodes(intent: IntentId): Set[ReplicaNode] =
    referenceDurableNodesAt(flatReceipts, intent)

  def bucketDurableNodes(intent: IntentId): Set[ReplicaNode] =
    bucketDurableNodesAt(receiptBuckets, intent)

  def referenceDurableCount(intent: IntentId): int =
    referenceDurableNodes(intent).size()

  def bucketDurableCount(intent: IntentId): int =
    bucketDurableNodes(intent).size()

  pure def nodeRank(replica: ReplicaNode): int =
    match replica {
      | NodeA => 0
      | NodeB => 1
      | NodeC => 2
    }

  pure def unwrapReceipt(value: Option[ReceiptRecord]): ReceiptRecord =
    match value {
      | Some(receipt) => receipt
      | None => receiptFor(IntentClosed, NodeC)
    }

  /// A flat query considers every historical receipt key. A bucket query only
  /// considers the bounded replica nodes for one intent.
  pure val referenceQueryWidth: int = 9
  pure val bucketQueryWidth: int = 3

  action init = all {
    flatReceipts' = emptyFlatReceipts,
    receiptBuckets' = emptyReceiptBuckets,
    beforeFlatReceipts' = emptyFlatReceipts,
    beforeReceiptBuckets' = emptyReceiptBuckets,
    lastTransition' = Initialized,
    lastOperation' = RecordReceipt,
    lastIntent' = IntentA,
    lastReplica' = NodeA,
    lastOverwrote' = false,
  }

  action recordDurableReceipt(
    intent: IntentId,
    replica: ReplicaNode,
  ): bool = all {
    recordAccepted(intent, replica),
    beforeFlatReceipts' = flatReceipts,
    beforeReceiptBuckets' = receiptBuckets,
    flatReceipts' = putFlatReceipt(flatReceipts, intent, replica),
    receiptBuckets' = putBucketReceipt(receiptBuckets, intent, replica),
    lastTransition' = Recorded,
    lastOperation' = RecordReceipt,
    lastIntent' = intent,
    lastReplica' = replica,
    lastOverwrote' = isSome(flatReceiptAt(flatReceipts, intent, replica)),
  }

  action restoreDurableReceipt(
    intent: IntentId,
    replica: ReplicaNode,
  ): bool = all {
    restoreAccepted(intent, replica),
    beforeFlatReceipts' = flatReceipts,
    beforeReceiptBuckets' = receiptBuckets,
    flatReceipts' = putFlatReceipt(flatReceipts, intent, replica),
    receiptBuckets' = putBucketReceipt(receiptBuckets, intent, replica),
    lastTransition' = Restored,
    lastOperation' = RestoreReceipt,
    lastIntent' = intent,
    lastReplica' = replica,
    lastOverwrote' = isSome(flatReceiptAt(flatReceipts, intent, replica)),
  }

  /// Rejected record/restore attempts are observations only. This is the
  /// optimization's failure boundary: neither representation is mutated.
  action rejectReceipt(
    operation: ReceiptOperation,
    intent: IntentId,
    replica: ReplicaNode,
  ): bool = all {
    not(operationAccepted(operation, intent, replica)),
    beforeFlatReceipts' = flatReceipts,
    beforeReceiptBuckets' = receiptBuckets,
    flatReceipts' = flatReceipts,
    receiptBuckets' = receiptBuckets,
    lastTransition' = Rejected,
    lastOperation' = operation,
    lastIntent' = intent,
    lastReplica' = replica,
    lastOverwrote' = false,
  }

  action step = any {
    {
      nondet intent = oneOf(IntentIds)
      nondet replica = oneOf(ReplicaNodes)
      recordDurableReceipt(intent, replica)
    },
    {
      nondet intent = oneOf(IntentIds)
      nondet replica = oneOf(ReplicaNodes)
      restoreDurableReceipt(intent, replica)
    },
    {
      nondet operation = oneOf(ReceiptOperations)
      nondet intent = oneOf(IntentIds)
      nondet replica = oneOf(ReplicaNodes)
      rejectReceipt(operation, intent, replica)
    },
  }
}
