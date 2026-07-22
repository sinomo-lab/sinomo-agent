/// Safety and bounded reachability for canonical namespace-transaction RPC bodies.
module dfs_namespace_transaction_body_transport_safety {
  import common.* from "../../lib/common"
  import dfs_namespace_transaction_body_transport.* from "../../models/dfs/namespace_transaction_body_transport"

  def countersStayBounded: bool = all {
    namespaceCallTransport.callCount >= 0,
    namespaceCallTransport.callCount <= 1,
    namespaceCallTransport.reconstructedRecordCount >= 0,
    namespaceCallTransport.reconstructedRecordCount <= TransactionRecordCount,
    namespaceCallTransport.transactionApplyCount >= 0,
    namespaceCallTransport.transactionApplyCount <= 1,
    namespaceCallTransport.visibleRecordCount >= 0,
    namespaceCallTransport.visibleRecordCount <= TransactionRecordCount,
  }

  /// The modeled transaction has exactly one transport path: one canonical
  /// object-RPC call with a semantic attachment, independent of byte count.
  def objectRpcCallIsTheOnlyTransportPath: bool = all {
    namespaceCallTransport.phase == TransactionReady implies all {
      namespaceCallTransport.callCount == 0,
      namespaceCallTransport.attachmentRole == AttachmentRoleUnset,
    },
    namespaceCallTransport.phase != TransactionReady implies all {
      namespaceCallTransport.callCount == 1,
      namespaceCallTransport.attachmentRole == NamespaceTransactionAttachment,
    },
  }

  /// Validation does not itself make transaction rows observable or even
  /// reconstruct them. Reconstruction is an all-at-once successor step.
  def reconstructionRequiresExactAttachment: bool = all {
    namespaceCallTransport.reconstructedRecordCount > 0 implies all {
      namespaceCallTransport.attachmentIntegrity == AttachmentExact,
      namespaceCallTransport.reconstructedRecordCount == TransactionRecordCount,
      any {
        namespaceCallTransport.phase == TransactionReconstructed,
        namespaceCallTransport.phase == TransactionDispatched,
        namespaceCallTransport.phase == TransactionApplied,
      },
    },
    namespaceCallTransport.phase == AttachmentValidated implies all {
      namespaceCallTransport.attachmentIntegrity == AttachmentExact,
      namespaceCallTransport.reconstructedRecordCount == 0,
      namespaceCallTransport.visibleRecordCount == 0,
    },
  }

  /// The attachment codec is transport-only: transaction identity and the
  /// exact file/file/parent order survive reconstruction.
  def reconstructionPreservesIdentityAndOrder: bool =
    namespaceCallTransport.reconstructedRecordCount == TransactionRecordCount implies all {
      namespaceCallTransport.reconstructedTransactionId == OriginalTransaction,
      namespaceCallTransport.reconstructedFirst == FirstFileRecord,
      namespaceCallTransport.reconstructedSecond == SecondFileRecord,
      namespaceCallTransport.reconstructedThird == ParentRecord,
    }

  def dispatchRequiresValidatedExactTransaction: bool =
    any {
      namespaceCallTransport.phase == TransactionDispatched,
      namespaceCallTransport.phase == TransactionApplied,
    } implies all {
      namespaceCallTransport.attachmentIntegrity == AttachmentExact,
      namespaceCallTransport.reconstructedRecordCount == TransactionRecordCount,
      namespaceCallTransport.reconstructedTransactionId == OriginalTransaction,
      reconstructionPreservesIdentityAndOrder,
    }

  /// One object-RPC call produces at most one metadata transaction.
  /// Visibility is all-or-nothing, including after a duplicate apply attempt.
  def applicationIsAtomicAndAtMostOnce: bool = all {
    namespaceCallTransport.transactionApplyCount <= 1,
    any {
      namespaceCallTransport.visibleRecordCount == 0,
      namespaceCallTransport.visibleRecordCount == TransactionRecordCount,
    },
    namespaceCallTransport.visibleRecordCount == TransactionRecordCount
      iff namespaceCallTransport.phase == TransactionApplied,
    namespaceCallTransport.phase == TransactionApplied implies all {
      namespaceCallTransport.callCount == 1,
      namespaceCallTransport.transactionApplyCount == 1,
      namespaceCallTransport.visibleRecordCount == TransactionRecordCount,
    },
  }

  /// Every attachment integrity failure terminates before reconstruction,
  /// dispatch, or namespace mutation.
  def rejectionNeverReconstructsOrMutatesNamespace: bool =
    namespaceCallTransport.phase == AttachmentRejected implies all {
      any {
        namespaceCallTransport.attachmentIntegrity == AttachmentCorrupt,
        namespaceCallTransport.attachmentIntegrity == AttachmentTruncated,
        namespaceCallTransport.attachmentIntegrity == AttachmentCancelled,
      },
      namespaceCallTransport.reconstructedRecordCount == 0,
      namespaceCallTransport.reconstructedTransactionId == TransactionNotReconstructed,
      namespaceCallTransport.reconstructedFirst == RecordNotReconstructed,
      namespaceCallTransport.reconstructedSecond == RecordNotReconstructed,
      namespaceCallTransport.reconstructedThird == RecordNotReconstructed,
      namespaceCallTransport.transactionApplyCount == 0,
      namespaceCallTransport.visibleRecordCount == 0,
    }

  def witnessObjectRpcCallIsPrepared: bool = all {
    namespaceCallTransport.lastTransition == ObjectRpcCallPreparedTransition,
    namespaceCallTransport.phase == ObjectRpcCallPrepared,
    namespaceCallTransport.callCount == 1,
    namespaceCallTransport.attachmentRole == NamespaceTransactionAttachment,
  }

  def witnessCanonicalAttachmentIsAccepted: bool = all {
    namespaceCallTransport.lastTransition == CanonicalAttachmentAccepted,
    namespaceCallTransport.phase == AttachmentReceiving,
    namespaceCallTransport.attachmentRole == NamespaceTransactionAttachment,
    namespaceCallTransport.visibleRecordCount == 0,
  }

  def witnessExactAttachmentValidatesWithoutVisibility: bool = all {
    namespaceCallTransport.lastTransition == ExactAttachmentValidated,
    namespaceCallTransport.phase == AttachmentValidated,
    namespaceCallTransport.attachmentIntegrity == AttachmentExact,
    namespaceCallTransport.reconstructedRecordCount == 0,
    namespaceCallTransport.visibleRecordCount == 0,
  }

  def witnessExactTransactionReconstructsInOrder: bool = all {
    namespaceCallTransport.lastTransition == ExactTransactionReconstructed,
    namespaceCallTransport.phase == TransactionReconstructed,
    reconstructionRequiresExactAttachment,
    reconstructionPreservesIdentityAndOrder,
    namespaceCallTransport.visibleRecordCount == 0,
  }

  def witnessHealthyTransactionAppliesAtomically: bool = all {
    namespaceCallTransport.lastTransition == AtomicTransactionApplied,
    namespaceCallTransport.phase == TransactionApplied,
    namespaceCallTransport.transactionApplyCount == 1,
    namespaceCallTransport.visibleRecordCount == TransactionRecordCount,
    dispatchRequiresValidatedExactTransaction,
  }

  def witnessDuplicateApplicationIsSuppressed: bool = all {
    namespaceCallTransport.lastTransition == DuplicateApplicationSuppressed,
    namespaceCallTransport.phase == TransactionApplied,
    namespaceCallTransport.transactionApplyCount == 1,
    namespaceCallTransport.visibleRecordCount == TransactionRecordCount,
  }

  def witnessCorruptAttachmentIsRejected: bool = all {
    namespaceCallTransport.lastTransition == CorruptAttachmentRejected,
    namespaceCallTransport.phase == AttachmentRejected,
    namespaceCallTransport.attachmentIntegrity == AttachmentCorrupt,
    rejectionNeverReconstructsOrMutatesNamespace,
  }

  def witnessTruncatedAttachmentIsRejected: bool = all {
    namespaceCallTransport.lastTransition == TruncatedAttachmentRejected,
    namespaceCallTransport.phase == AttachmentRejected,
    namespaceCallTransport.attachmentIntegrity == AttachmentTruncated,
    rejectionNeverReconstructsOrMutatesNamespace,
  }

  def witnessCancelledAttachmentIsRejected: bool = all {
    namespaceCallTransport.lastTransition == CancelledAttachmentRejected,
    namespaceCallTransport.phase == AttachmentRejected,
    namespaceCallTransport.attachmentIntegrity == AttachmentCancelled,
    rejectionNeverReconstructsOrMutatesNamespace,
  }

  def safety: bool = all {
    countersStayBounded,
    objectRpcCallIsTheOnlyTransportPath,
    reconstructionRequiresExactAttachment,
    reconstructionPreservesIdentityAndOrder,
    dispatchRequiresValidatedExactTransaction,
    applicationIsAtomicAndAtMostOnce,
    rejectionNeverReconstructsOrMutatesNamespace,
  }
}
