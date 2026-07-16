/// Canonical object-RPC session transport for one same-shard namespace transaction.
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
/// metadata-backend recovery are outside this slice. Existing session and
/// namespace-transaction models own those contracts; this model covers their
/// composition boundary.
module dfs_namespace_transaction_body_transport {
  import common.* from "../../lib/common"

  type NamespaceSessionPhase =
    | TransactionReady
    | CanonicalSessionPrepared
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

  type NamespaceSessionTransition =
    | SessionTransportInitialized
    | CanonicalSessionCallPrepared
    | CanonicalAttachmentAccepted
    | ExactAttachmentValidated
    | ExactTransactionReconstructed
    | CorruptAttachmentRejected
    | TruncatedAttachmentRejected
    | CancelledAttachmentRejected
    | ValidatedTransactionDispatched
    | AtomicTransactionApplied
    | DuplicateApplicationSuppressed
    | SessionTransportStuttered

  type NamespaceSessionTransportState = {
    phase: NamespaceSessionPhase,
    attachmentIntegrity: NamespaceAttachmentIntegrity,
    sessionCount: int,
    attachmentRole: NamespaceAttachmentRole,
    reconstructedRecordCount: int,
    reconstructedTransactionId: NamespaceTransactionId,
    reconstructedFirst: NamespaceTransactionRecord,
    reconstructedSecond: NamespaceTransactionRecord,
    reconstructedThird: NamespaceTransactionRecord,
    transactionApplyCount: int,
    visibleRecordCount: int,
    lastTransition: NamespaceSessionTransition,
  }

  pure val TransactionRecordCount: int = 3

  pure val initialState: NamespaceSessionTransportState = {
    phase: TransactionReady,
    attachmentIntegrity: AttachmentNotChecked,
    sessionCount: 0,
    attachmentRole: AttachmentRoleUnset,
    reconstructedRecordCount: 0,
    reconstructedTransactionId: TransactionNotReconstructed,
    reconstructedFirst: RecordNotReconstructed,
    reconstructedSecond: RecordNotReconstructed,
    reconstructedThird: RecordNotReconstructed,
    transactionApplyCount: 0,
    visibleRecordCount: 0,
    lastTransition: SessionTransportInitialized,
  }

  var namespaceSessionTransport: NamespaceSessionTransportState

  action init = namespaceSessionTransport' = initialState

  /// Every encoded namespace transaction enters the same object-RPC session
  /// contract with one semantic attachment. No size predicate selects this
  /// action or another transport API.
  action prepareCanonicalSessionCall = all {
    namespaceSessionTransport.phase == TransactionReady,
    namespaceSessionTransport' = namespaceSessionTransport
      .with("phase", CanonicalSessionPrepared)
      .with("sessionCount", 1)
      .with("attachmentRole", NamespaceTransactionAttachment)
      .with("lastTransition", CanonicalSessionCallPrepared),
  }

  action acceptCanonicalAttachment = all {
    namespaceSessionTransport.phase == CanonicalSessionPrepared,
    namespaceSessionTransport.attachmentRole == NamespaceTransactionAttachment,
    namespaceSessionTransport' = namespaceSessionTransport
      .with("phase", AttachmentReceiving)
      .with("lastTransition", CanonicalAttachmentAccepted),
  }

  /// Length, digest, and complete SOBJ validation form one abstract gate. No
  /// transaction row is reconstructed while this gate is in progress.
  action validateExactAttachment = all {
    namespaceSessionTransport.phase == AttachmentReceiving,
    namespaceSessionTransport' = namespaceSessionTransport
      .with("phase", AttachmentValidated)
      .with("attachmentIntegrity", AttachmentExact)
      .with("lastTransition", ExactAttachmentValidated),
  }

  /// Reconstruction is enabled only by the exact-attachment gate and restores
  /// the original transaction identity and file/file/parent ordering at once.
  action reconstructValidatedTransaction = all {
    namespaceSessionTransport.phase == AttachmentValidated,
    namespaceSessionTransport.attachmentIntegrity == AttachmentExact,
    namespaceSessionTransport' = namespaceSessionTransport
      .with("phase", TransactionReconstructed)
      .with("reconstructedRecordCount", TransactionRecordCount)
      .with("reconstructedTransactionId", OriginalTransaction)
      .with("reconstructedFirst", FirstFileRecord)
      .with("reconstructedSecond", SecondFileRecord)
      .with("reconstructedThird", ParentRecord)
      .with("lastTransition", ExactTransactionReconstructed),
  }

  action rejectCorruptAttachment = all {
    namespaceSessionTransport.phase == AttachmentReceiving,
    namespaceSessionTransport' = namespaceSessionTransport
      .with("phase", AttachmentRejected)
      .with("attachmentIntegrity", AttachmentCorrupt)
      .with("lastTransition", CorruptAttachmentRejected),
  }

  action rejectTruncatedAttachment = all {
    namespaceSessionTransport.phase == AttachmentReceiving,
    namespaceSessionTransport' = namespaceSessionTransport
      .with("phase", AttachmentRejected)
      .with("attachmentIntegrity", AttachmentTruncated)
      .with("lastTransition", TruncatedAttachmentRejected),
  }

  action rejectCancelledAttachment = all {
    namespaceSessionTransport.phase == AttachmentReceiving,
    namespaceSessionTransport' = namespaceSessionTransport
      .with("phase", AttachmentRejected)
      .with("attachmentIntegrity", AttachmentCancelled)
      .with("lastTransition", CancelledAttachmentRejected),
  }

  action dispatchValidatedTransaction = all {
    namespaceSessionTransport.phase == TransactionReconstructed,
    namespaceSessionTransport' = namespaceSessionTransport
      .with("phase", TransactionDispatched)
      .with("lastTransition", ValidatedTransactionDispatched),
  }

  /// The metadata backend applies the reconstructed transaction atomically;
  /// no transfer frame becomes an independently visible namespace row.
  action applyAtomicTransaction = all {
    namespaceSessionTransport.phase == TransactionDispatched,
    namespaceSessionTransport.transactionApplyCount == 0,
    namespaceSessionTransport' = namespaceSessionTransport
      .with("phase", TransactionApplied)
      .with("transactionApplyCount", 1)
      .with("visibleRecordCount", TransactionRecordCount)
      .with("lastTransition", AtomicTransactionApplied),
  }

  /// A repeated delivery or local completion attempt cannot apply the same
  /// canonical session call twice.
  action suppressDuplicateApplication = all {
    namespaceSessionTransport.phase == TransactionApplied,
    namespaceSessionTransport.transactionApplyCount == 1,
    namespaceSessionTransport' = namespaceSessionTransport
      .with("lastTransition", DuplicateApplicationSuppressed),
  }

  action stutter =
    namespaceSessionTransport' = namespaceSessionTransport
      .with("lastTransition", SessionTransportStuttered)

  action step = any {
    prepareCanonicalSessionCall,
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
