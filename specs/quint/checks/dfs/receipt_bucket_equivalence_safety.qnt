/// Safety and refinement checks for DFS coordinator receipt buckets.
module dfs_receipt_bucket_equivalence_safety {
  import common.* from "../../lib/common"
  import dfs_receipt_bucket_equivalence.* from "../../models/dfs/receipt_bucket_equivalence"

  def everyReceiptLocationIsEquivalent: bool =
    IntentIds.forall(intent =>
      ReplicaNodes.forall(replica =>
        flatReceiptAt(flatReceipts, intent, replica)
          == bucketReceiptAt(receiptBuckets, intent, replica)
      )
    )

  def everyReceiptHasCanonicalContent: bool =
    IntentIds.forall(intent =>
      ReplicaNodes.forall(replica =>
        match flatReceiptAt(flatReceipts, intent, replica) {
          | Some(receipt) => receipt == receiptFor(intent, replica)
          | None => true
        }
      )
    )

  def durableNodesAndCountsAreEquivalent: bool =
    IntentIds.forall(intent => all {
      referenceDurableNodes(intent) == bucketDurableNodes(intent),
      referenceDurableCount(intent) == bucketDurableCount(intent),
    })

  def orderedReceiptViewsAreEquivalent: bool =
    IntentIds.forall(intent =>
      referenceReceiptsForIntent(intent) == bucketReceiptsForIntent(intent)
    )

  def orderedReceiptViewsAreWellFormed: bool =
    IntentIds.forall(intent => {
      val view = bucketReceiptsForIntent(intent)
      all {
        view.length == bucketDurableCount(intent),
        view.length >= 0,
        view.length <= 3,
        (view.length >= 1) iff isSome(view.first),
        (view.length >= 2) iff isSome(view.second),
        (view.length >= 3) iff isSome(view.third),
        isSome(view.first) implies
          unwrapReceipt(view.first).intent == intent,
        isSome(view.second) implies all {
          unwrapReceipt(view.second).intent == intent,
          nodeRank(unwrapReceipt(view.first).replica)
            < nodeRank(unwrapReceipt(view.second).replica),
        },
        isSome(view.third) implies all {
          unwrapReceipt(view.third).intent == intent,
          nodeRank(unwrapReceipt(view.second).replica)
            < nodeRank(unwrapReceipt(view.third).replica),
        },
      }
    })

  def acceptedReceiptUpdatedExactLocations: bool =
    any {
      lastTransition == Recorded,
      lastTransition == Restored,
    } implies all {
      flatReceipts == putFlatReceipt(
        beforeFlatReceipts,
        lastIntent,
        lastReplica
      ),
      receiptBuckets == putBucketReceipt(
        beforeReceiptBuckets,
        lastIntent,
        lastReplica
      ),
    }

  def acceptedReceiptIsolatedFromOtherKeys: bool =
    any {
      lastTransition == Recorded,
      lastTransition == Restored,
    } implies all {
      IntentIds.forall(intent =>
        intent != lastIntent implies all {
          referenceReceiptsForIntentAt(flatReceipts, intent)
            == referenceReceiptsForIntentAt(beforeFlatReceipts, intent),
          bucketReceiptsForIntentAt(receiptBuckets, intent)
            == bucketReceiptsForIntentAt(beforeReceiptBuckets, intent),
        }
      ),
      ReplicaNodes.forall(replica =>
        replica != lastReplica implies all {
          flatReceiptAt(flatReceipts, lastIntent, replica)
            == flatReceiptAt(beforeFlatReceipts, lastIntent, replica),
          bucketReceiptAt(receiptBuckets, lastIntent, replica)
            == bucketReceiptAt(beforeReceiptBuckets, lastIntent, replica),
        }
      ),
    }

  def duplicateOverwriteIsIdempotent: bool =
    all {
      any {
        lastTransition == Recorded,
        lastTransition == Restored,
      },
      lastOverwrote,
    } implies all {
      flatReceipts == beforeFlatReceipts,
      receiptBuckets == beforeReceiptBuckets,
      referenceDurableCount(lastIntent)
        == referenceDurableNodesAt(beforeFlatReceipts, lastIntent).size(),
      bucketDurableCount(lastIntent)
        == bucketDurableNodesAt(beforeReceiptBuckets, lastIntent).size(),
    }

  def newReceiptAddsExactlyOneNode: bool =
    all {
      any {
        lastTransition == Recorded,
        lastTransition == Restored,
      },
      not(lastOverwrote),
    } implies all {
      referenceDurableCount(lastIntent)
        == referenceDurableNodesAt(beforeFlatReceipts, lastIntent).size() + 1,
      bucketDurableCount(lastIntent)
        == bucketDurableNodesAt(beforeReceiptBuckets, lastIntent).size() + 1,
    }

  def rejectionMutatedNeitherRepresentation: bool =
    lastTransition == Rejected implies all {
      flatReceipts == beforeFlatReceipts,
      receiptBuckets == beforeReceiptBuckets,
      not(operationAccepted(lastOperation, lastIntent, lastReplica)),
    }

  def restoreMayHydrateClosedIntent: bool =
    all {
      lastTransition == Restored,
      lastIntent == IntentClosed,
    } implies all {
      restoreAccepted(lastIntent, lastReplica),
      not(recordAccepted(lastIntent, lastReplica)),
      isSome(bucketReceiptAt(receiptBuckets, lastIntent, lastReplica)),
    }

  def optimizedQueryHasIntentLocalScope: bool = all {
    referenceQueryWidth == 9,
    bucketQueryWidth == 3,
    bucketQueryWidth < referenceQueryWidth,
  }

  def witnessFirstRecordEquivalent: bool = all {
    lastTransition == Recorded,
    not(lastOverwrote),
    everyReceiptLocationIsEquivalent,
  }

  def witnessDuplicateRecordIdempotent: bool = all {
    lastTransition == Recorded,
    lastOverwrote,
    duplicateOverwriteIsIdempotent,
  }

  def witnessRestoreDurableEquivalent: bool = all {
    lastTransition == Restored,
    everyReceiptLocationIsEquivalent,
  }

  def witnessRestoreClosedIntentAccepted: bool = all {
    lastTransition == Restored,
    lastIntent == IntentClosed,
    restoreMayHydrateClosedIntent,
  }

  def witnessDurableCountAndNodesEquivalent: bool = all {
    IntentIds.exists(intent => bucketDurableCount(intent) >= 2),
    durableNodesAndCountsAreEquivalent,
  }

  def witnessReceiptsForIntentOrdered: bool =
    IntentIds.exists(intent => all {
      bucketReceiptsForIntent(intent).length >= 2,
      orderedReceiptViewsAreEquivalent,
      orderedReceiptViewsAreWellFormed,
    })

  def witnessCrossIntentIsolation: bool = all {
    any {
      lastTransition == Recorded,
      lastTransition == Restored,
    },
    IntentIds.exists(intent => all {
      intent != lastIntent,
      bucketDurableNodesAt(beforeReceiptBuckets, intent).size() > 0,
      bucketReceiptsForIntentAt(receiptBuckets, intent)
        == bucketReceiptsForIntentAt(beforeReceiptBuckets, intent),
    }),
    acceptedReceiptIsolatedFromOtherKeys,
  }

  def witnessRejectedReceiptNoMutation: bool = all {
    lastTransition == Rejected,
    rejectionMutatedNeitherRepresentation,
  }

  def safety: bool = all {
    everyReceiptLocationIsEquivalent,
    everyReceiptHasCanonicalContent,
    durableNodesAndCountsAreEquivalent,
    orderedReceiptViewsAreEquivalent,
    orderedReceiptViewsAreWellFormed,
    acceptedReceiptUpdatedExactLocations,
    acceptedReceiptIsolatedFromOtherKeys,
    duplicateOverwriteIsIdempotent,
    newReceiptAddsExactlyOneNode,
    rejectionMutatedNeitherRepresentation,
    restoreMayHydrateClosedIntent,
    optimizedQueryHasIntentLocalScope,
  }
}
