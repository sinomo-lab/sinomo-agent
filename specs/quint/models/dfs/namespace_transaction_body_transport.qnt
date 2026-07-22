/// Object-RPC call body transport for one same-shard namespace transaction.
///
/// Evidence class: core-algorithm composition refinement for carrying an
/// encoded `DfsNamespaceTransaction` through the authenticated object-RPC
/// session used by every request. The attachment is selected because the
/// transaction is a semantic file-like value, never because its encoding
/// crosses a byte threshold. There is no alternate RPC or inline fallback in
/// this model.
///
/// The bounded transaction has two file records followed by one parent record.
/// The receiver validates the complete attachment before reconstructing or
/// dispatching the transaction. A valid attachment preserves transaction
/// identity and record order, and the metadata authority performs one atomic
/// application. Corruption, truncation, or cancellation before attachment
/// validation cannot expose any transaction record.
///
/// Concrete SOBJ field sizes, transfer-frame chunking, relay authentication,
/// retry policy, flow-window scheduling, resource exhaustion, and
/// metadata-backend recovery are outside this slice. Existing routed-stream and
/// namespace-transaction models own those contracts; this model covers their
/// composition boundary.
module dfs_namespace_transaction_body_transport {
  import common.* from "../../lib/common"

  type NamespaceCallPhase =
    | TransactionReady
    | ObjectRpcCallPrepared
    | AttachmentReceiving
    | AttachmentValidated
    | TransactionReconstructed
    | TransactionDispatched
    | TransactionApplied
    | AttachmentRejected

  type NamespaceAttachmentIntegrity =
    | AttachmentNotChecked
    | AttachmentExact
    | AttachmentCorrupt
    | AttachmentTruncated
    | AttachmentCancelled

  type NamespaceAttachmentRole =
    | AttachmentRoleUnset
    | NamespaceTransactionAttachment

  type NamespaceTransactionId =
    | TransactionNotReconstructed
    | OriginalTransaction

  type NamespaceTransactionRecord =
    | RecordNotReconstructed
    | FirstFileRecord
    | SecondFileRecord
    | ParentRecord

  type NamespaceCallTransition =
    | CallTransportInitialized
    | ObjectRpcCallPreparedTransition
    | CanonicalAttachmentAccepted
    | ExactAttachmentValidated
    | ExactTransactionReconstructed
    | CorruptAttachmentRejected
    | TruncatedAttachmentRejected
    | CancelledAttachmentRejected
    | ValidatedTransactionDispatched
    | AtomicTransactionApplied
    | DuplicateApplicationSuppressed
    | CallTransportStuttered

  type NamespaceCallTransportState = {
    phase: NamespaceCallPhase,
    attachmentIntegrity: NamespaceAttachmentIntegrity,
    callCount: int,
    attachmentRole: NamespaceAttachmentRole,
    reconstructedRecordCount: int,
    reconstructedTransactionId: NamespaceTransactionId,
    reconstructedFirst: NamespaceTransactionRecord,
    reconstructedSecond: NamespaceTransactionRecord,
    reconstructedThird: NamespaceTransactionRecord,
    transactionApplyCount: int,
    visibleRecordCount: int,
    lastTransition: NamespaceCallTransition,
  }

  pure val TransactionRecordCount: int = 3

  pure val initialState: NamespaceCallTransportState = {
    phase: TransactionReady,
    attachmentIntegrity: AttachmentNotChecked,
    callCount: 0,
    attachmentRole: AttachmentRoleUnset,
    reconstructedRecordCount: 0,
    reconstructedTransactionId: TransactionNotReconstructed,
    reconstructedFirst: RecordNotReconstructed,
    reconstructedSecond: RecordNotReconstructed,
    reconstructedThird: RecordNotReconstructed,
    transactionApplyCount: 0,
    visibleRecordCount: 0,
    lastTransition: CallTransportInitialized,
  }

  var namespaceCallTransport: NamespaceCallTransportState

  action init = namespaceCallTransport' = initialState

  /// Every encoded namespace transaction enters the same object-RPC call
  /// contract with one semantic attachment. No size predicate selects this
  /// action or another transport API.
  action prepareObjectRpcCall = all {
    namespaceCallTransport.phase == TransactionReady,
    namespaceCallTransport' = namespaceCallTransport
      .with("phase", ObjectRpcCallPrepared)
      .with("callCount", 1)
      .with("attachmentRole", NamespaceTransactionAttachment)
      .with("lastTransition", ObjectRpcCallPreparedTransition),
  }

  action acceptCanonicalAttachment = all {
    namespaceCallTransport.phase == ObjectRpcCallPrepared,
    namespaceCallTransport.attachmentRole == NamespaceTransactionAttachment,
    namespaceCallTransport' = namespaceCallTransport
      .with("phase", AttachmentReceiving)
      .with("lastTransition", CanonicalAttachmentAccepted),
  }

  /// Length, digest, and complete SOBJ validation form one abstract gate. No
  /// transaction row is reconstructed while this gate is in progress.
  action validateExactAttachment = all {
    namespaceCallTransport.phase == AttachmentReceiving,
    namespaceCallTransport' = namespaceCallTransport
      .with("phase", AttachmentValidated)
      .with("attachmentIntegrity", AttachmentExact)
      .with("lastTransition", ExactAttachmentValidated),
  }

  /// Reconstruction is enabled only by the exact-attachment gate and restores
  /// the original transaction identity and file/file/parent ordering at once.
  action reconstructValidatedTransaction = all {
    namespaceCallTransport.phase == AttachmentValidated,
    namespaceCallTransport.attachmentIntegrity == AttachmentExact,
    namespaceCallTransport' = namespaceCallTransport
      .with("phase", TransactionReconstructed)
      .with("reconstructedRecordCount", TransactionRecordCount)
      .with("reconstructedTransactionId", OriginalTransaction)
      .with("reconstructedFirst", FirstFileRecord)
      .with("reconstructedSecond", SecondFileRecord)
      .with("reconstructedThird", ParentRecord)
      .with("lastTransition", ExactTransactionReconstructed),
  }

  action rejectCorruptAttachment = all {
    namespaceCallTransport.phase == AttachmentReceiving,
    namespaceCallTransport' = namespaceCallTransport
      .with("phase", AttachmentRejected)
      .with("attachmentIntegrity", AttachmentCorrupt)
      .with("lastTransition", CorruptAttachmentRejected),
  }

  action rejectTruncatedAttachment = all {
    namespaceCallTransport.phase == AttachmentReceiving,
    namespaceCallTransport' = namespaceCallTransport
      .with("phase", AttachmentRejected)
      .with("attachmentIntegrity", AttachmentTruncated)
      .with("lastTransition", TruncatedAttachmentRejected),
  }

  action rejectCancelledAttachment = all {
    namespaceCallTransport.phase == AttachmentReceiving,
    namespaceCallTransport' = namespaceCallTransport
      .with("phase", AttachmentRejected)
      .with("attachmentIntegrity", AttachmentCancelled)
      .with("lastTransition", CancelledAttachmentRejected),
  }

  action dispatchValidatedTransaction = all {
    namespaceCallTransport.phase == TransactionReconstructed,
    namespaceCallTransport' = namespaceCallTransport
      .with("phase", TransactionDispatched)
      .with("lastTransition", ValidatedTransactionDispatched),
  }

  /// The metadata backend applies the reconstructed transaction atomically;
  /// no transfer frame becomes an independently visible namespace row.
  action applyAtomicTransaction = all {
    namespaceCallTransport.phase == TransactionDispatched,
    namespaceCallTransport.transactionApplyCount == 0,
    namespaceCallTransport' = namespaceCallTransport
      .with("phase", TransactionApplied)
      .with("transactionApplyCount", 1)
      .with("visibleRecordCount", TransactionRecordCount)
      .with("lastTransition", AtomicTransactionApplied),
  }

  /// A repeated delivery or local completion attempt cannot apply the same
  /// object-RPC call twice.
  action suppressDuplicateApplication = all {
    namespaceCallTransport.phase == TransactionApplied,
    namespaceCallTransport.transactionApplyCount == 1,
    namespaceCallTransport' = namespaceCallTransport
      .with("lastTransition", DuplicateApplicationSuppressed),
  }

  action stutter =
    namespaceCallTransport' = namespaceCallTransport
      .with("lastTransition", CallTransportStuttered)

  action step = any {
    prepareObjectRpcCall,
    acceptCanonicalAttachment,
    validateExactAttachment,
    reconstructValidatedTransaction,
    rejectCorruptAttachment,
    rejectTruncatedAttachment,
    rejectCancelledAttachment,
    dispatchValidatedTransaction,
    applyAtomicTransaction,
    suppressDuplicateApplication,
    stutter,
  }
}
