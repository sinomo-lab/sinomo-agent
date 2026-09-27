/// Safety and witnesses for initial authored I/R/P group-sync batching.
/// Unrestricted depth eight passed; depth ten remains incomplete. This bounded
/// result is not a proof of longer recovery, filesystem behavior, or Rust conformance.
module dfs_initial_prepare_batch_safety {
  import dfs_initial_prepare_batch.* from "../../models/dfs/initial_prepare_batch"

  def sequencesRemainContiguousAndUnique = all {
    // A set of h distinct integers within 1..h equals the complete range.
    // This avoids Apalache's unsupported state-dependent range expression.
    s.frames.map(f => f.sequence).size() == s.physicalHead,
    s.frames.forall(f => f.sequence >= 1 and f.sequence <= s.physicalHead),
    s.frames.size() == s.physicalHead,
    s.frames.forall(f => f.identity == 1),
  }
  def unchangedLogicalRecordOrder = s.frames.forall(f =>
    s.frames.filter(other => other.attempt == f.attempt).forall(other =>
      f.sequence - ordinal(f.kind) == other.sequence - ordinal(other.kind)))
  def durabilityAndTableBounds = all {
    s.durableHead >= 0, s.durableHead <= s.physicalHead,
    s.tableHead >= 0, s.tableHead <= s.durableHead,
    s.tablePrepareReference >= 0, s.tablePrepareReference <= s.tableHead,
  }
  def fastPathExcludesSerialContracts =
    s.fastGroups > 0 implies ordinary(s.inputClass)
  def terminalIdentityIsNeverDowngraded =
    s.terminalIdentityAfter == s.terminalIdentityBefore
  def initialBatchNeverPublishesNamespace = not(s.childVisible)
  def everyPrepareHasItsExactPriorIntentAndW =
    s.frames.filter(f => f.kind == NamespacePrepare).forall(p => completePrepareProof(s, p.sequence))
  def uncertaintyBlocksNewAppendAndAuthority =
    s.uncertain implies not(admitted(s))
  def decoderOrReplacementCannotCertify =
    Set(DecodeOnly, HandleReplaced, RecoverySyncFailed).contains(s.last) implies all {
      s.uncertain, s.phase == NeedsRecovery, not(admitted(s)),
    }
  def authorityNamesDurableResolvedPrepare = s.last == AuthorityPublished implies all {
    admitted(s), s.authoritySequence == s.tablePrepareReference,
    s.authoritySequence > 0, s.authoritySequence <= s.durableHead,
    completePrepareProof(s, s.authoritySequence),
    s.tableReferenceCost > 0, not(s.deferredPrepare),
  }
  def incompleteReceiptSetCannotAuthorize =
    s.tablePrepareReference == 0 implies s.last != AuthorityPublished
  def prevalidationReadSetUnaffectedByIntentAndReceiptTableWrites =
    s.phase == Appending implies s.namespaceReadSet == s.validatedReadSet
  def deferredPrepareWaitsForTableAppend =
    s.phase == TablePending and s.deferredPrepare implies all {
      s.livePrepareReference == s.tablePrepareReference,
      s.last != AuthorityPublished, not(admitted(s)),
    }
  def referenceAccountingMatchesMaterializedPrefix = all {
    s.tableReferenceCost == prepareSequences(s, s.tableHead).size(),
    s.tablePrepareReference > 0 implies completePrepareProof(s, s.tablePrepareReference),
  }
  def failedTableAppendPreservesJournalAndRollsBackProjection =
    s.last == TableAppendFailed implies all {
      s.livePrepareReference == s.tablePrepareReference,
      s.durableHead == s.plannedHead, certificateValid(s), not(admitted(s)),
    }
  def safety = all {
    sequencesRemainContiguousAndUnique, unchangedLogicalRecordOrder, durabilityAndTableBounds,
    fastPathExcludesSerialContracts, terminalIdentityIsNeverDowngraded,
    initialBatchNeverPublishesNamespace, everyPrepareHasItsExactPriorIntentAndW,
    uncertaintyBlocksNewAppendAndAuthority, decoderOrReplacementCannotCertify,
    authorityNamesDurableResolvedPrepare, incompleteReceiptSetCannotAuthorize,
    prevalidationReadSetUnaffectedByIntentAndReceiptTableWrites,
    deferredPrepareWaitsForTableAppend, referenceAccountingMatchesMaterializedPrefix,
    failedTableAppendPreservesJournalAndRollsBackProjection,
  }

  def witnessHealthyGroupedPrepareAuthorizes =
    s.last == AuthorityPublished and s.fastGroups == 1 and not(s.sawResync)
  def witnessIntentOnlyCutStaysHidden =
    s.last == TailCertified and s.groupOffset == 1 and s.tablePrepareReference == 0
  def witnessOneOfTwoReceiptsCannotAuthorize =
    s.last == TailCertified and s.groupOffset == 2 and s.tablePrepareReference == 0
  def witnessAllReceiptsWithoutPrepareCannotAuthorize =
    s.last == TailCertified and s.groupOffset == 3 and s.tablePrepareReference == 0
  def witnessCompletePrepareBeforeSyncIsNotAuthority =
    s.last == UncertainAppendFailed and s.groupOffset == 4 and s.uncertain
  def witnessRetryAppendsNewSequencesAndAuthorizes =
    s.last == AuthorityPublished and s.attempts == 2 and s.sawPartialRecovery
  def witnessTornPrepareRepaired =
    s.last == IncompleteTailTruncated and s.groupOffset == 3
  def witnessFailedResyncRemainsFenced = s.last == RecoverySyncFailed
  def witnessNewHandleCannotForgetUncertainty = s.last == HandleReplaced
  def witnessReopenNeedsExplicitRecertification = s.last == DecodeOnly and s.reopens > 0
  def witnessPreparedReferenceDeferredUntilTableAppend =
    s.last == TablePlanned and s.deferredPrepare
  def witnessTableFailureThenExactAuthority =
    s.last == AuthorityPublished and s.sawTableRollback
  def witnessUnknownHashActiveIntentCanBeFilled =
    s.last == AuthorityPublished and s.inputClass == CompatibleActiveIntent
  def witnessExactPreparedRetryKeepsIdentity =
    s.last == AuthorityPublished and s.inputClass == ExactPreparedRetry and s.fastGroups > 0
  def witnessTerminalIntentRetainsSerialPath =
    s.last == FallbackSelected and s.inputClass == TerminalIntent
  def witnessCompactedIntentRetainsSerialPath =
    s.last == FallbackSelected and s.inputClass == CompactedIntent
  def witnessTerminalPrepareRetainsSerialPath =
    s.last == FallbackSelected and s.inputClass == TerminalPrepare
  def witnessCompactedPrepareRetainsSerialPath =
    s.last == FallbackSelected and s.inputClass == CompactedPrepare
  def witnessBackfillRetainsSerialOrdering =
    s.last == FallbackSelected and s.inputClass == HistoryBackfill
  def witnessBulkRetainsSerialPath =
    s.last == FallbackSelected and s.inputClass == BulkNamespace
  def witnessAttachedCompletionRetainsSerialPath =
    s.last == FallbackSelected and s.inputClass == AttachedCompletion
  def witnessRetainedAndAdvisoryCasesRemainSerial =
    s.last == FallbackSelected and s.inputClass == RetainedOrAdvisory
  def witnessConflictingActiveIntentIsNotBatched =
    s.last == FallbackSelected and s.inputClass == ConflictingActiveIntent
  def witnessParentOrExclusiveChangeRejected =
    s.last == CandidateRejected and s.namespaceReadSet != 0
  def witnessBadReceiptOrIdentityRejected =
    s.last == CandidateRejected and not(s.candidateValid)
  def witnessCompleteMalformedFrameRejected = s.last == CompleteCorruptionRejected
}
