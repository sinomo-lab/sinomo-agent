/// Safety and bounded reachability for canonical namespace-transaction sessions.
module dfs_namespace_transaction_body_transport_safety {
  import common.* from "../../lib/common"
  import dfs_namespace_transaction_body_transport.* from "../../models/dfs/namespace_transaction_body_transport"

  def countersStayBounded: bool = all {
    namespaceSessionTransport.sessionCount >= 0,
    namespaceSessionTransport.sessionCount <= 1,
    namespaceSessionTransport.reconstructedRecordCount >= 0,
    namespaceSessionTransport.reconstructedRecordCount <= TransactionRecordCount,
    namespaceSessionTransport.transactionApplyCount >= 0,
    namespaceSessionTransport.transactionApplyCount <= 1,
    namespaceSessionTransport.visibleRecordCount >= 0,
    namespaceSessionTransport.visibleRecordCount <= TransactionRecordCount,
  }

  /// The modeled transaction has exactly one transport path: one canonical
  /// object-RPC session with a semantic attachment, independent of byte count.
  def canonicalSessionIsTheOnlyTransportPath: bool = all {
    namespaceSessionTransport.phase == TransactionReady implies all {
      namespaceSessionTransport.sessionCount == 0,
      namespaceSessionTransport.attachmentRole == AttachmentRoleUnset,
    },
    namespaceSessionTransport.phase != TransactionReady implies all {
      namespaceSessionTransport.sessionCount == 1,
      namespaceSessionTransport.attachmentRole == NamespaceTransactionAttachment,
    },
  }

  /// Validation does not itself make transaction rows observable or even
  /// reconstruct them. Reconstruction is an all-at-once successor step.
  def reconstructionRequiresExactAttachment: bool = all {
    namespaceSessionTransport.reconstructedRecordCount > 0 implies all {
      namespaceSessionTransport.attachmentIntegrity == AttachmentExact,
      namespaceSessionTransport.reconstructedRecordCount == TransactionRecordCount,
      any {
        namespaceSessionTransport.phase == TransactionReconstructed,
        namespaceSessionTransport.phase == TransactionDispatched,
        namespaceSessionTransport.phase == TransactionApplied,
      },
    },
    namespaceSessionTransport.phase == AttachmentValidated implies all {
      namespaceSessionTransport.attachmentIntegrity == AttachmentExact,
      namespaceSessionTransport.reconstructedRecordCount == 0,
      namespaceSessionTransport.visibleRecordCount == 0,
    },
  }

  /// The attachment codec is transport-only: transaction identity and the
  /// exact file/file/parent order survive reconstruction.
  def reconstructionPreservesIdentityAndOrder: bool =
    namespaceSessionTransport.reconstructedRecordCount == TransactionRecordCount implies all {
      namespaceSessionTransport.reconstructedTransactionId == OriginalTransaction,
      namespaceSessionTransport.reconstructedFirst == FirstFileRecord,
      namespaceSessionTransport.reconstructedSecond == SecondFileRecord,
      namespaceSessionTransport.reconstructedThird == ParentRecord,
    }

  def dispatchRequiresValidatedExactTransaction: bool =
    any {
      namespaceSessionTransport.phase == TransactionDispatched,
      namespaceSessionTransport.phase == TransactionApplied,
    } implies all {
      namespaceSessionTransport.attachmentIntegrity == AttachmentExact,
      namespaceSessionTransport.reconstructedRecordCount == TransactionRecordCount,
      namespaceSessionTransport.reconstructedTransactionId == OriginalTransaction,
      reconstructionPreservesIdentityAndOrder,
    }

  /// One canonical session produces at most one metadata transaction.
  /// Visibility is all-or-nothing, including after a duplicate apply attempt.
  def applicationIsAtomicAndAtMostOnce: bool = all {
    namespaceSessionTransport.transactionApplyCount <= 1,
    any {
      namespaceSessionTransport.visibleRecordCount == 0,
      namespaceSessionTransport.visibleRecordCount == TransactionRecordCount,
    },
    namespaceSessionTransport.visibleRecordCount == TransactionRecordCount
      iff namespaceSessionTransport.phase == TransactionApplied,
    namespaceSessionTransport.phase == TransactionApplied implies all {
      namespaceSessionTransport.sessionCount == 1,
      namespaceSessionTransport.transactionApplyCount == 1,
      namespaceSessionTransport.visibleRecordCount == TransactionRecordCount,
    },
  }

  /// Every attachment integrity failure terminates before reconstruction,
  /// dispatch, or namespace mutation.
  def rejectionNeverReconstructsOrMutatesNamespace: bool =
    namespaceSessionTransport.phase == AttachmentRejected implies all {
      any {
        namespaceSessionTransport.attachmentIntegrity == AttachmentCorrupt,
        namespaceSessionTransport.attachmentIntegrity == AttachmentTruncated,
        namespaceSessionTransport.attachmentIntegrity == AttachmentCancelled,
      },
      namespaceSessionTransport.reconstructedRecordCount == 0,
      namespaceSessionTransport.reconstructedTransactionId == TransactionNotReconstructed,
      namespaceSessionTransport.reconstructedFirst == RecordNotReconstructed,
      namespaceSessionTransport.reconstructedSecond == RecordNotReconstructed,
      namespaceSessionTransport.reconstructedThird == RecordNotReconstructed,
      namespaceSessionTransport.transactionApplyCount == 0,
      namespaceSessionTransport.visibleRecordCount == 0,
    }

  def witnessCanonicalSessionIsPrepared: bool = all {
    namespaceSessionTransport.lastTransition == CanonicalSessionCallPrepared,
    namespaceSessionTransport.phase == CanonicalSessionPrepared,
    namespaceSessionTransport.sessionCount == 1,
    namespaceSessionTransport.attachmentRole == NamespaceTransactionAttachment,
  }

  def witnessCanonicalAttachmentIsAccepted: bool = all {
    namespaceSessionTransport.lastTransition == CanonicalAttachmentAccepted,
    namespaceSessionTransport.phase == AttachmentReceiving,
    namespaceSessionTransport.attachmentRole == NamespaceTransactionAttachment,
    namespaceSessionTransport.visibleRecordCount == 0,
  }

  def witnessExactAttachmentValidatesWithoutVisibility: bool = all {
    namespaceSessionTransport.lastTransition == ExactAttachmentValidated,
    namespaceSessionTransport.phase == AttachmentValidated,
    namespaceSessionTransport.attachmentIntegrity == AttachmentExact,
    namespaceSessionTransport.reconstructedRecordCount == 0,
    namespaceSessionTransport.visibleRecordCount == 0,
  }

  def witnessExactTransactionReconstructsInOrder: bool = all {
    namespaceSessionTransport.lastTransition == ExactTransactionReconstructed,
    namespaceSessionTransport.phase == TransactionReconstructed,
    reconstructionRequiresExactAttachment,
    reconstructionPreservesIdentityAndOrder,
    namespaceSessionTransport.visibleRecordCount == 0,
  }

  def witnessHealthyTransactionAppliesAtomically: bool = all {
    namespaceSessionTransport.lastTransition == AtomicTransactionApplied,
    namespaceSessionTransport.phase == TransactionApplied,
    namespaceSessionTransport.transactionApplyCount == 1,
    namespaceSessionTransport.visibleRecordCount == TransactionRecordCount,
    dispatchRequiresValidatedExactTransaction,
  }

  def witnessDuplicateApplicationIsSuppressed: bool = all {
    namespaceSessionTransport.lastTransition == DuplicateApplicationSuppressed,
    namespaceSessionTransport.phase == TransactionApplied,
    namespaceSessionTransport.transactionApplyCount == 1,
    namespaceSessionTransport.visibleRecordCount == TransactionRecordCount,
  }

  def witnessCorruptAttachmentIsRejected: bool = all {
    namespaceSessionTransport.lastTransition == CorruptAttachmentRejected,
    namespaceSessionTransport.phase == AttachmentRejected,
    namespaceSessionTransport.attachmentIntegrity == AttachmentCorrupt,
    rejectionNeverReconstructsOrMutatesNamespace,
  }

  def witnessTruncatedAttachmentIsRejected: bool = all {
    namespaceSessionTransport.lastTransition == TruncatedAttachmentRejected,
    namespaceSessionTransport.phase == AttachmentRejected,
    namespaceSessionTransport.attachmentIntegrity == AttachmentTruncated,
    rejectionNeverReconstructsOrMutatesNamespace,
  }

  def witnessCancelledAttachmentIsRejected: bool = all {
    namespaceSessionTransport.lastTransition == CancelledAttachmentRejected,
    namespaceSessionTransport.phase == AttachmentRejected,
    namespaceSessionTransport.attachmentIntegrity == AttachmentCancelled,
    rejectionNeverReconstructsOrMutatesNamespace,
  }

  def safety: bool = all {
    countersStayBounded,
    canonicalSessionIsTheOnlyTransportPath,
    reconstructionRequiresExactAttachment,
    reconstructionPreservesIdentityAndOrder,
    dispatchRequiresValidatedExactTransaction,
    applicationIsAtomicAndAtMostOnce,
    rejectionNeverReconstructsOrMutatesNamespace,
  }
}
