module dfs_requester_prestage_binding_safety {
  import dfs_requester_prestage_binding.* from "../../models/dfs/requester_prestage_binding"
  def safety: bool = all {
    s.receipts.subseteq(s.durableBindings),
    s.receipts.forall(receipt => receipt == canonicalReceipt),
    s.published >= 0, s.published <= 1,
    s.stageCount >= 0, s.stageCount <= 1,
    s.phase == Committed implies all {
      exactDurableReceipt, s.metadataAuthority, s.published == 1,
    },
    s.phase != Committed implies s.published == 0,
    legacyWouldCreditWrongTuple implies s.published == 0,
    s.lostReply implies all { s.phase == Committed, exactDurableReceipt },
    s.restarted implies exactDurableReceipt,
  }
  def witnessUnchangedCreateUsesRequesterWithoutStage: bool = all {
    s.phase == Committed, s.canonicalBinding == 0, s.selectedReplica == 0, s.stageCount == 0,
  }
  def witnessVersionRebaseSuppressesLegacyCredit: bool = all {
    legacyWouldCreditWrongTuple, s.canonicalBinding == 1, s.receipts == Set(),
  }
  def witnessObjectRebaseSuppressesLegacyCredit: bool = all {
    legacyWouldCreditWrongTuple, s.canonicalBinding == 2, s.receipts == Set(),
  }
  def witnessExcludedRequesterNeedsSelectedReplica: bool = all {
    s.phase == Admitted, s.selectedReplica == 1, not(exactDurableReceipt), s.receipts == Set(),
  }
  def witnessRebasedTupleBoundBeforeCommit: bool = all {
    s.phase == Committed, s.canonicalBinding != 0, s.stageCount == 1,
  }
  def witnessLostReplyCrashRetainsCanonicalBinding: bool = all {
    s.lostReply, not(s.requesterAlive), s.canonicalBinding != 0, exactDurableReceipt,
  }
  def witnessRestartNeedsNoPostReplyBinding: bool = all {
    s.restarted, not(s.replyDelivered), s.canonicalBinding != 0, exactDurableReceipt,
  }
  def witnessCancelledCallerCannotUndoDurableBinding: bool = all {
    s.cancelled, s.phase == Committed, exactDurableReceipt,
  }
  def witnessWAloneCannotPublish: bool = all {
    s.phase == Admitted, exactDurableReceipt, not(s.metadataAuthority), s.published == 0,
  }
  def witnessRemovedParentCannotPublish: bool = all { s.phase == Rejected, s.rejectReason == 1 }
  def witnessExclusiveRaceCannotPublish: bool = all { s.phase == Rejected, s.rejectReason == 2 }
  def witnessChangedRetryDoesNotChangeBinding: bool = all {
    s.phase == Committed, s.changedRetryRejected, exactDurableReceipt,
  }
}
