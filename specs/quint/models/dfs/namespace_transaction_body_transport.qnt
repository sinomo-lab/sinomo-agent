/// Streamed body transport for one oversized same-shard namespace transaction.
///
/// Evidence class: core-algorithm transport refinement for moving an encoded
/// `DfsNamespaceTransaction` out of the bounded object-RPC control head and
/// into the canonical authenticated request body.
///
/// The bounded transaction has two file records followed by one parent record.
/// The legacy inline representation exceeds the 64 KiB control-frame limit.
/// The optimized representation sends one small descriptor head and one body;
/// the receiver collects and validates the complete body before reconstructing
/// or dispatching the transaction. A complete valid body preserves transaction
/// identity and record order, and the metadata authority still performs one
/// atomic application. Corruption, truncation, or cancellation before body
/// completion cannot expose any transaction record.
///
/// Concrete SOBJ field sizes, transfer-frame chunking, relay authentication,
/// retry policy, flow-window scheduling, and metadata-backend recovery are
/// outside this slice. Existing session and namespace-transaction models own
/// those contracts; this model covers their composition boundary.
module dfs_namespace_transaction_body_transport {
  import common.* from "../../lib/common"

  type NamespaceBodyPhase =
    | TransactionReady
    | StreamHeadPrepared
    | BodyReceiving
    | BodyValidated
    | TransactionDispatched
    | TransactionApplied
    | BodyRejected

  type NamespaceBodyIntegrity =
    | BodyAbsent
    | BodyExact
    | BodyCorrupt
    | BodyTruncated
    | BodyCancelled
    | BodyRejectedByRoute

  type NamespaceRouteBodyMode =
    | RouteBodyNone
    | RouteBodyOptional

  type NamespaceTransactionId =
    | TransactionNotReconstructed
    | OriginalTransaction

  type NamespaceTransactionRecord =
    | RecordNotReconstructed
    | FirstFileRecord
    | SecondFileRecord
    | ParentRecord

  type NamespaceBodyTransition =
    | BodyTransportInitialized
    | OversizedTransactionDetached
    | LegacyRouteRejectedBody
    | OptionalRouteBodyContractPublished
    | OptionalRouteAcceptedNoBodyProbe
    | StreamHeadAccepted
    | ExactBodyValidated
    | CorruptBodyRejected
    | TruncatedBodyRejected
    | CancelledBodyRejected
    | ValidatedTransactionDispatched
    | AtomicTransactionApplied
    | BodyTransportStuttered

  type NamespaceBodyTransportState = {
    phase: NamespaceBodyPhase,
    bodyIntegrity: NamespaceBodyIntegrity,
    bodyBytes: int,
    rpcCount: int,
    headRecordCount: int,
    reconstructedRecordCount: int,
    reconstructedTransactionId: NamespaceTransactionId,
    reconstructedFirst: NamespaceTransactionRecord,
    reconstructedSecond: NamespaceTransactionRecord,
    reconstructedThird: NamespaceTransactionRecord,
    transactionApplyCount: int,
    visibleRecordCount: int,
    routeBodyMode: NamespaceRouteBodyMode,
    noBodyCompatibilityAccepted: bool,
    lastTransition: NamespaceBodyTransition,
  }

  /// The concrete failing r6 signed request head was 89,596 bytes. The
  /// streamed descriptor head budget is deliberately conservative rather than
  /// claiming an exact encoder size in this abstract model.
  pure val ControlFrameLimitBytes: int = 65536
  pure val LegacyInlineHeadBytes: int = 89596
  pure val StreamedHeadBudgetBytes: int = 16384
  pure val CompleteBodyBytes: int = 80000
  pure val TransactionRecordCount: int = 3

  pure val initialState: NamespaceBodyTransportState = {
    phase: TransactionReady,
    bodyIntegrity: BodyAbsent,
    bodyBytes: 0,
    rpcCount: 0,
    headRecordCount: TransactionRecordCount,
    reconstructedRecordCount: 0,
    reconstructedTransactionId: TransactionNotReconstructed,
    reconstructedFirst: RecordNotReconstructed,
    reconstructedSecond: RecordNotReconstructed,
    reconstructedThird: RecordNotReconstructed,
    transactionApplyCount: 0,
    visibleRecordCount: 0,
    routeBodyMode: RouteBodyNone,
    noBodyCompatibilityAccepted: false,
    lastTransition: BodyTransportInitialized,
  }

  var namespaceBodyTransport: NamespaceBodyTransportState

  action init = namespaceBodyTransport' = initialState

  /// The original atomic transaction becomes one body. The bounded head keeps
  /// only its descriptor/sentinel and therefore carries zero transaction rows.
  action detachOversizedTransaction = all {
    namespaceBodyTransport.phase == TransactionReady,
    namespaceBodyTransport' = namespaceBodyTransport
      .with("phase", StreamHeadPrepared)
      .with("rpcCount", 1)
      .with("headRecordCount", 0)
      .with("lastTransition", OversizedTransactionDetached),
  }

  /// The r7 compatibility gap: the transaction was correctly detached, but
  /// the namespace route still advertised the legacy no-body-only contract.
  /// Route admission rejects it before collection or DFS dispatch.
  action rejectDetachedBodyAtLegacyRoute = all {
    namespaceBodyTransport.phase == StreamHeadPrepared,
    namespaceBodyTransport.routeBodyMode == RouteBodyNone,
    namespaceBodyTransport' = namespaceBodyTransport
      .with("phase", BodyRejected)
      .with("bodyIntegrity", BodyRejectedByRoute)
      .with("lastTransition", LegacyRouteRejectedBody),
  }

  /// Refinement implemented by the DFS service catalog. Optional, rather than
  /// Required, is load-bearing: ordinary small namespace requests remain
  /// inline while oversized ApplyTransaction requests carry one body.
  action publishOptionalRouteBodyContract = all {
    namespaceBodyTransport.phase == StreamHeadPrepared,
    namespaceBodyTransport.routeBodyMode == RouteBodyNone,
    namespaceBodyTransport' = namespaceBodyTransport
      .with("routeBodyMode", RouteBodyOptional)
      .with("lastTransition", OptionalRouteBodyContractPublished),
  }

  /// A separate body-free compatibility probe remains admissible after the
  /// route changes to Optional. It does not consume or mutate the modeled
  /// oversized transaction.
  action acceptNoBodyCompatibilityProbe = all {
    namespaceBodyTransport.phase == StreamHeadPrepared,
    namespaceBodyTransport.routeBodyMode == RouteBodyOptional,
    not(namespaceBodyTransport.noBodyCompatibilityAccepted),
    namespaceBodyTransport' = namespaceBodyTransport
      .with("noBodyCompatibilityAccepted", true)
      .with("lastTransition", OptionalRouteAcceptedNoBodyProbe),
  }

  action acceptStreamHead = all {
    namespaceBodyTransport.phase == StreamHeadPrepared,
    namespaceBodyTransport.routeBodyMode == RouteBodyOptional,
    namespaceBodyTransport' = namespaceBodyTransport
      .with("phase", BodyReceiving)
      .with("lastTransition", StreamHeadAccepted),
  }

  /// Collection, length validation, digest validation, and SOBJ decode form
  /// one abstract gate. Dispatch cannot observe a partial reconstruction.
  action validateExactBody = all {
    namespaceBodyTransport.phase == BodyReceiving,
    namespaceBodyTransport' = namespaceBodyTransport
      .with("phase", BodyValidated)
      .with("bodyIntegrity", BodyExact)
      .with("bodyBytes", CompleteBodyBytes)
      .with("reconstructedRecordCount", TransactionRecordCount)
      .with("reconstructedTransactionId", OriginalTransaction)
      .with("reconstructedFirst", FirstFileRecord)
      .with("reconstructedSecond", SecondFileRecord)
      .with("reconstructedThird", ParentRecord)
      .with("lastTransition", ExactBodyValidated),
  }

  action rejectCorruptBody = all {
    namespaceBodyTransport.phase == BodyReceiving,
    namespaceBodyTransport' = namespaceBodyTransport
      .with("phase", BodyRejected)
      .with("bodyIntegrity", BodyCorrupt)
      .with("bodyBytes", CompleteBodyBytes)
      .with("lastTransition", CorruptBodyRejected),
  }

  action rejectTruncatedBody = all {
    namespaceBodyTransport.phase == BodyReceiving,
    namespaceBodyTransport' = namespaceBodyTransport
      .with("phase", BodyRejected)
      .with("bodyIntegrity", BodyTruncated)
      .with("bodyBytes", CompleteBodyBytes - 1)
      .with("lastTransition", TruncatedBodyRejected),
  }

  action rejectCancelledBody = all {
    namespaceBodyTransport.phase == BodyReceiving,
    namespaceBodyTransport' = namespaceBodyTransport
      .with("phase", BodyRejected)
      .with("bodyIntegrity", BodyCancelled)
      .with("bodyBytes", 0)
      .with("lastTransition", CancelledBodyRejected),
  }

  action dispatchValidatedTransaction = all {
    namespaceBodyTransport.phase == BodyValidated,
    namespaceBodyTransport' = namespaceBodyTransport
      .with("phase", TransactionDispatched)
      .with("lastTransition", ValidatedTransactionDispatched),
  }

  /// The existing namespace backend applies this one reconstructed transaction
  /// atomically; no body-transport chunk becomes an independently visible row.
  action applyAtomicTransaction = all {
    namespaceBodyTransport.phase == TransactionDispatched,
    namespaceBodyTransport.transactionApplyCount == 0,
    namespaceBodyTransport' = namespaceBodyTransport
      .with("phase", TransactionApplied)
      .with("transactionApplyCount", 1)
      .with("visibleRecordCount", TransactionRecordCount)
      .with("lastTransition", AtomicTransactionApplied),
  }

  action stutter =
    namespaceBodyTransport' = namespaceBodyTransport
      .with("lastTransition", BodyTransportStuttered)

  action step = any {
    detachOversizedTransaction,
    rejectDetachedBodyAtLegacyRoute,
    publishOptionalRouteBodyContract,
    acceptNoBodyCompatibilityProbe,
    acceptStreamHead,
    validateExactBody,
    rejectCorruptBody,
    rejectTruncatedBody,
    rejectCancelledBody,
    dispatchValidatedTransaction,
    applyAtomicTransaction,
    stutter,
  }
}
