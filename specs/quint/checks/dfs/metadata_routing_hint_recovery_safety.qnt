/// Safety and bounded witnesses for disposable routing-hint recovery.
module dfs_metadata_routing_hint_recovery_safety {
  import common.* from "../../lib/common"
  import dfs_metadata_routing_hint_recovery.* from "../../models/dfs/metadata_routing_hint_recovery"

  def relaxedHintFollowsCanonicalDurability =
    s.relaxedInstallObserved implies s.durableIntent and s.durableReceipt
  def recoveredViewUsesCanonicalOwnership = s.recovered implies all {
    s.hint != HintIoError,
    s.activeVisible == activeOwned(s),
    s.historicalOwner == historicalOwned(s),
    s.receiptOwner == exactReceiptOwned(s),
  }
  def hintContentsCannotGrantOrHideAuthority =
    Set(Missing, ExactHint, Malformed, ForeignObject, WrongShard).forall(h => {
      val restored = recover(s.with("hint", h))
      all {
        restored.activeVisible == activeOwned(s),
        restored.historicalOwner == historicalOwned(s),
        restored.receiptOwner == exactReceiptOwned(s),
      }
    })
  def ioErrorsStayVisible = s.last == RecoveryRejected implies all {
    s.hint == HintIoError, not(s.recovered), not(s.activeVisible),
    s.historicalOwner == None, s.receiptOwner == None,
  }
  def visibilityRequiresDurableAdmission = s.committed implies all {
    s.durableIntent, s.durableReceipt, s.namespacePrepared,
    s.admissionValid, not(s.aborted),
  }
  def hiddenAmbiguityCannotCreditReceipt =
    s.recovered and s.physical == Hidden and s.ambiguousReceipt
      implies s.receiptOwner == None
  def movedActiveOwnershipDoesNotDependOnHint = s.recovered implies all {
    s.durableIntent and not(s.committed) and not(s.aborted)
      and s.physical == MovedCompatible implies s.activeVisible,
    s.physical == MovedSuperseded implies not(s.activeVisible),
  }
  def inv = all {
    relaxedHintFollowsCanonicalDurability, recoveredViewUsesCanonicalOwnership,
    hintContentsCannotGrantOrHideAuthority, ioErrorsStayVisible,
    visibilityRequiresDurableAdmission, hiddenAmbiguityCannotCreditReceipt,
    movedActiveOwnershipDoesNotDependOnHint,
  }
  def safety = inv
  def witnessRelaxedHintAfterDurableIntent = s.last == HintInstalled
  def witnessMissingHintReopenRecoversPending = all {
    s.reopened, s.recovered, s.hint == Missing, s.activeVisible,
    s.physical == Hidden, s.receiptOwner == Some(Original),
  }
  def witnessMalformedHintRecoversPending = all {
    s.recovered, s.hint == Malformed, s.activeVisible,
    s.physical == Hidden, s.receiptOwner == Some(Original),
  }
  def witnessForeignObjectHintCannotHidePending =
    s.recovered and s.hint == ForeignObject and s.activeVisible
  def witnessWrongShardHintCannotHidePending =
    s.recovered and s.hint == WrongShard and s.activeVisible
  def witnessRealIoErrorRejectsRecovery = s.last == RecoveryRejected
  def witnessReopenThenCanonicalCompletion =
    s.reopened and s.last == NamespacePublished and s.hint != ExactHint
  def witnessCompatibleMovedIntentRetained =
    s.recovered and s.physical == MovedCompatible and s.activeVisible
  def witnessSupersededMovedIntentHidden = all {
    s.recovered, s.physical == MovedSuperseded,
    not(s.committed), not(s.aborted), not(s.activeVisible),
  }
  def witnessHiddenAbortedHistoryRetained =
    s.recovered and s.aborted and s.physical == Hidden and s.historicalOwner == Some(Original)
  def witnessMovedHistoricalOwnerIsCanonical = all {
    s.recovered, s.committed, s.physical == MovedCompatible,
    s.historicalOwner == Some(Destination), s.hint == Missing,
  }
  def witnessAmbiguousReceiptFailsClosed = all {
    s.recovered, s.physical == Hidden, s.ambiguousReceipt,
    s.receiptOwner == None,
  }
  def witnessParentOrExclusiveRejectionKeepsHidden = all {
    not(s.admissionValid), s.durableIntent, s.physical == Hidden, not(s.committed),
  }
  def witnessHintAloneCannotCreditReceipt = all {
    not(s.durableIntent), s.recovered, s.hint == ExactHint,
    not(s.activeVisible), s.receiptOwner == None,
  }
}
