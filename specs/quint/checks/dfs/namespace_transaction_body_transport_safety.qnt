/// Safety and bounded reachability for streamed namespace-transaction bodies.
module dfs_namespace_transaction_body_transport_safety {
  import common.* from "../../lib/common"
  import dfs_namespace_transaction_body_transport.* from "../../models/dfs/namespace_transaction_body_transport"

  def countersStayBounded: bool = all {
    namespaceBodyTransport.bodyBytes >= 0,
    namespaceBodyTransport.bodyBytes <= CompleteBodyBytes,
    namespaceBodyTransport.rpcCount >= 0,
    namespaceBodyTransport.rpcCount <= 1,
    namespaceBodyTransport.headRecordCount >= 0,
    namespaceBodyTransport.headRecordCount <= TransactionRecordCount,
    namespaceBodyTransport.reconstructedRecordCount >= 0,
    namespaceBodyTransport.reconstructedRecordCount <= TransactionRecordCount,
    namespaceBodyTransport.transactionApplyCount >= 0,
    namespaceBodyTransport.transactionApplyCount <= 1,
    namespaceBodyTransport.visibleRecordCount >= 0,
    namespaceBodyTransport.visibleRecordCount <= TransactionRecordCount,
  }

  /// The failing inline encoding is over the control limit, while detachment
  /// gives the control plane a fixed bound independent of transaction rows.
  def oversizedTransactionUsesBoundedHead: bool = all {
    LegacyInlineHeadBytes > ControlFrameLimitBytes,
    StreamedHeadBudgetBytes < ControlFrameLimitBytes,
    namespaceBodyTransport.phase != TransactionReady implies all {
      namespaceBodyTransport.rpcCount == 1,
      namespaceBodyTransport.headRecordCount == 0,
    },
  }

  /// The legacy route rejects the detached body before decoding. Publishing
  /// Optional is the minimum refinement that admits the body while retaining
  /// compatibility with ordinary body-free namespace operations.
  def routeBodyContractIsExplicitAndBackwardCompatible: bool = all {
    any {
      namespaceBodyTransport.phase == BodyReceiving,
      namespaceBodyTransport.phase == BodyValidated,
      namespaceBodyTransport.phase == TransactionDispatched,
      namespaceBodyTransport.phase == TransactionApplied,
    } implies namespaceBodyTransport.routeBodyMode == RouteBodyOptional,
    namespaceBodyTransport.bodyIntegrity == BodyRejectedByRoute implies all {
      namespaceBodyTransport.phase == BodyRejected,
      namespaceBodyTransport.routeBodyMode == RouteBodyNone,
      namespaceBodyTransport.reconstructedRecordCount == 0,
      namespaceBodyTransport.transactionApplyCount == 0,
      namespaceBodyTransport.visibleRecordCount == 0,
    },
    namespaceBodyTransport.noBodyCompatibilityAccepted implies
      namespaceBodyTransport.routeBodyMode == RouteBodyOptional,
  }

  /// No partial or invalid body can materialize a dispatchable transaction.
  def reconstructionRequiresCompleteValidBody: bool =
    namespaceBodyTransport.reconstructedRecordCount > 0 implies all {
      namespaceBodyTransport.bodyIntegrity == BodyExact,
      namespaceBodyTransport.bodyBytes == CompleteBodyBytes,
      namespaceBodyTransport.reconstructedRecordCount == TransactionRecordCount,
      any {
        namespaceBodyTransport.phase == BodyValidated,
        namespaceBodyTransport.phase == TransactionDispatched,
        namespaceBodyTransport.phase == TransactionApplied,
      },
    }

  /// The body codec is a transport-only change: transaction identity and the
  /// exact file/file/parent order survive reconstruction.
  def reconstructionPreservesIdentityAndOrder: bool =
    namespaceBodyTransport.reconstructedRecordCount == TransactionRecordCount implies all {
      namespaceBodyTransport.reconstructedTransactionId == OriginalTransaction,
      namespaceBodyTransport.reconstructedFirst == FirstFileRecord,
      namespaceBodyTransport.reconstructedSecond == SecondFileRecord,
      namespaceBodyTransport.reconstructedThird == ParentRecord,
    }

  def dispatchRequiresValidatedExactTransaction: bool =
    any {
      namespaceBodyTransport.phase == TransactionDispatched,
      namespaceBodyTransport.phase == TransactionApplied,
    } implies all {
      namespaceBodyTransport.bodyIntegrity == BodyExact,
      namespaceBodyTransport.bodyBytes == CompleteBodyBytes,
      namespaceBodyTransport.reconstructedRecordCount == TransactionRecordCount,
      namespaceBodyTransport.reconstructedTransactionId == OriginalTransaction,
      reconstructionPreservesIdentityAndOrder,
    }

  /// One session body still produces one metadata transaction. Visibility is
  /// all-or-nothing and application cannot repeat within this request.
  def applicationIsAtomicAndAtMostOnce: bool = all {
    namespaceBodyTransport.transactionApplyCount <= 1,
    any {
      namespaceBodyTransport.visibleRecordCount == 0,
      namespaceBodyTransport.visibleRecordCount == TransactionRecordCount,
    },
    namespaceBodyTransport.visibleRecordCount == TransactionRecordCount
      iff namespaceBodyTransport.phase == TransactionApplied,
    namespaceBodyTransport.phase == TransactionApplied implies all {
      namespaceBodyTransport.rpcCount == 1,
      namespaceBodyTransport.transactionApplyCount == 1,
      namespaceBodyTransport.visibleRecordCount == TransactionRecordCount,
    },
  }

  def rejectionNeverMutatesNamespace: bool =
    namespaceBodyTransport.phase == BodyRejected implies all {
      namespaceBodyTransport.bodyIntegrity != BodyExact,
      namespaceBodyTransport.reconstructedRecordCount == 0,
      namespaceBodyTransport.reconstructedTransactionId == TransactionNotReconstructed,
      namespaceBodyTransport.transactionApplyCount == 0,
      namespaceBodyTransport.visibleRecordCount == 0,
    }

  def witnessOversizedInlineHeadIsDetached: bool = all {
    namespaceBodyTransport.lastTransition == OversizedTransactionDetached,
    namespaceBodyTransport.phase == StreamHeadPrepared,
    namespaceBodyTransport.rpcCount == 1,
    namespaceBodyTransport.headRecordCount == 0,
    LegacyInlineHeadBytes > ControlFrameLimitBytes,
    StreamedHeadBudgetBytes < ControlFrameLimitBytes,
  }

  def witnessLegacyRouteRejectsDetachedBody: bool = all {
    namespaceBodyTransport.lastTransition == LegacyRouteRejectedBody,
    namespaceBodyTransport.phase == BodyRejected,
    namespaceBodyTransport.bodyIntegrity == BodyRejectedByRoute,
    namespaceBodyTransport.routeBodyMode == RouteBodyNone,
    rejectionNeverMutatesNamespace,
  }

  def witnessOptionalRouteAcceptsDetachedBody: bool = all {
    namespaceBodyTransport.lastTransition == StreamHeadAccepted,
    namespaceBodyTransport.phase == BodyReceiving,
    namespaceBodyTransport.routeBodyMode == RouteBodyOptional,
    namespaceBodyTransport.visibleRecordCount == 0,
  }

  def witnessOptionalRouteStillAcceptsNoBody: bool = all {
    namespaceBodyTransport.lastTransition == OptionalRouteAcceptedNoBodyProbe,
    namespaceBodyTransport.phase == StreamHeadPrepared,
    namespaceBodyTransport.routeBodyMode == RouteBodyOptional,
    namespaceBodyTransport.noBodyCompatibilityAccepted,
    namespaceBodyTransport.visibleRecordCount == 0,
  }

  def witnessHealthyBodyValidatesExactly: bool = all {
    namespaceBodyTransport.lastTransition == ExactBodyValidated,
    namespaceBodyTransport.phase == BodyValidated,
    reconstructionRequiresCompleteValidBody,
    reconstructionPreservesIdentityAndOrder,
    namespaceBodyTransport.visibleRecordCount == 0,
  }

  def witnessHealthyTransactionAppliesAtomically: bool = all {
    namespaceBodyTransport.lastTransition == AtomicTransactionApplied,
    namespaceBodyTransport.phase == TransactionApplied,
    namespaceBodyTransport.transactionApplyCount == 1,
    namespaceBodyTransport.visibleRecordCount == TransactionRecordCount,
    dispatchRequiresValidatedExactTransaction,
  }

  def witnessCorruptBodyIsRejected: bool = all {
    namespaceBodyTransport.lastTransition == CorruptBodyRejected,
    namespaceBodyTransport.phase == BodyRejected,
    namespaceBodyTransport.bodyIntegrity == BodyCorrupt,
    rejectionNeverMutatesNamespace,
  }

  def witnessTruncatedBodyIsRejected: bool = all {
    namespaceBodyTransport.lastTransition == TruncatedBodyRejected,
    namespaceBodyTransport.phase == BodyRejected,
    namespaceBodyTransport.bodyIntegrity == BodyTruncated,
    namespaceBodyTransport.bodyBytes == CompleteBodyBytes - 1,
    rejectionNeverMutatesNamespace,
  }

  def witnessCancelledBodyIsRejected: bool = all {
    namespaceBodyTransport.lastTransition == CancelledBodyRejected,
    namespaceBodyTransport.phase == BodyRejected,
    namespaceBodyTransport.bodyIntegrity == BodyCancelled,
    rejectionNeverMutatesNamespace,
  }

  def safety: bool = all {
    countersStayBounded,
    oversizedTransactionUsesBoundedHead,
    routeBodyContractIsExplicitAndBackwardCompatible,
    reconstructionRequiresCompleteValidBody,
    reconstructionPreservesIdentityAndOrder,
    dispatchRequiresValidatedExactTransaction,
    applicationIsAtomicAndAtMostOnce,
    rejectionNeverMutatesNamespace,
  }
}
