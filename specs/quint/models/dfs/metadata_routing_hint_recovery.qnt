/// Target refinement for derived object-to-shard routing hints.
/// Canonical intent/receipt and namespace records retain their existing durable
/// semantics. Only a missing routing hint may be installed without a barrier,
/// after pending intent+receipt durability. A crash may lose or tear that hint.
/// Two physical shards and one object/version/hash identity model ownership;
/// IO failure differs from absent/undecodable/wrong-object hint data.
/// This is not a model of the full metadata publication or handoff protocol:
/// their exact authority, W, parent and exclusive-admission proofs are inputs.
module dfs_metadata_routing_hint_recovery {
  import common.* from "../../lib/common"

  type Hint = Missing | ExactHint | Malformed | ForeignObject | WrongShard | HintIoError
  type Physical = Hidden | OriginalVisible | MovedCompatible | MovedSuperseded
  type Owner = Original | Destination
  type Last = Initialized | JournalPersisted | HintInstalled | HintChanged | Reopened
    | RecoveryAccepted | RecoveryRejected | NamespacePrepared | NamespacePublished
    | AdmissionRejected | IntentAborted | PhysicalChanged | AmbiguityInstalled

  type State = {
    durableIntent: bool, durableReceipt: bool, namespacePrepared: bool,
    committed: bool, aborted: bool, physical: Physical, hint: Hint,
    ambiguousReceipt: bool, admissionValid: bool, reopened: bool,
    relaxedInstallObserved: bool, recovered: bool,
    activeVisible: bool, historicalOwner: Option[Owner], receiptOwner: Option[Owner],
    last: Last,
  }
  pure val initial: State = {
    durableIntent: false, durableReceipt: false, namespacePrepared: false,
    committed: false, aborted: false, physical: Hidden, hint: Missing,
    ambiguousReceipt: false, admissionValid: true, reopened: false,
    relaxedInstallObserved: false, recovered: false,
    activeVisible: false, historicalOwner: None, receiptOwner: None,
    last: Initialized,
  }
  var s: State

  pure def activeOwned(x: State): bool = all {
    x.durableIntent, not(x.committed), not(x.aborted),
    x.physical != MovedSuperseded,
  }
  pure def historicalOwned(x: State): Option[Owner] =
    if (not(x.durableIntent) or not(x.committed or x.aborted)) None
    else if (x.physical == Hidden or x.physical == OriginalVisible) Some(Original)
    else Some(Destination)
  pure def exactReceiptOwned(x: State): Option[Owner] =
    if (not(x.durableReceipt)) None
    else if (x.physical == OriginalVisible) Some(Original)
    else if (x.physical == MovedCompatible or x.physical == MovedSuperseded) Some(Destination)
    else if (activeOwned(x) and not(x.ambiguousReceipt)) Some(Original)
    else None

  /// Hints never supply an ownership proof. Visible state is selected from
  /// physical tables/routing; hidden creates require one exact active intent.
  pure def recover(x: State): State =
    if (x.hint == HintIoError) x.with("recovered", false)
      .with("activeVisible", false).with("historicalOwner", None)
      .with("receiptOwner", None).with("last", RecoveryRejected)
    else x.with("recovered", true).with("activeVisible", activeOwned(x))
      .with("historicalOwner", historicalOwned(x))
      .with("receiptOwner", exactReceiptOwned(x)).with("last", RecoveryAccepted)

  action init = s' = initial
  action persistPending = all {
    not(s.durableIntent), s.physical == Hidden,
    s' = s.with("durableIntent", true).with("durableReceipt", true)
      .with("recovered", false).with("last", JournalPersisted),
  }
  action installRelaxedHint = all {
    s.durableIntent, s.durableReceipt, s.hint == Missing,
    s' = s.with("hint", ExactHint).with("relaxedInstallObserved", true)
      .with("recovered", false).with("last", HintInstalled),
  }
  action changeHint = {
    nondet nextHint = Set(Missing, ExactHint, Malformed, ForeignObject, WrongShard, HintIoError).oneOf()
    all {
      nextHint != s.hint,
      s' = s.with("hint", nextHint).with("recovered", false).with("last", HintChanged),
    }
  }
  action reopenWithUncertainHint = {
    nondet nextHint = Set(Missing, ExactHint, Malformed, ForeignObject, WrongShard).oneOf()
    all {
      s.durableIntent, not(s.reopened),
      s' = s.with("hint", nextHint).with("reopened", true)
        .with("recovered", false).with("last", Reopened),
    }
  }
  action recoverFromCanonicalState = s' = recover(s)
  action prepareNamespace = all {
    s.recovered, s.activeVisible, s.receiptOwner == Some(Original),
    s.physical == Hidden, s.admissionValid, not(s.namespacePrepared),
    s' = s.with("namespacePrepared", true).with("last", NamespacePrepared),
  }
  action commitNamespace = all {
    s.recovered, s.activeVisible, s.namespacePrepared,
    s.receiptOwner == Some(Original), s.physical == Hidden, s.admissionValid,
    s' = s.with("committed", true).with("physical", OriginalVisible)
      .with("recovered", false).with("last", NamespacePublished),
  }
  action invalidateParentOrExclusiveAdmission = all {
    not(s.committed), s.admissionValid,
    s' = s.with("admissionValid", false).with("recovered", false)
      .with("last", AdmissionRejected),
  }
  action abortHiddenIntent = all {
    s.durableIntent, not(s.committed), not(s.aborted), s.physical == Hidden,
    s' = s.with("aborted", true).with("recovered", false).with("last", IntentAborted),
  }
  action moveCompatiblePhysicalObject = all {
    s.durableIntent, s.physical == Hidden or s.physical == OriginalVisible,
    s' = s.with("physical", MovedCompatible).with("recovered", false)
      .with("last", PhysicalChanged),
  }
  action advanceMovedPhysicalObject = all {
    s.physical == MovedCompatible,
    s' = s.with("physical", MovedSuperseded).with("recovered", false)
      .with("last", PhysicalChanged),
  }
  action installAmbiguousPendingReceiptOwner = all {
    s.durableIntent, s.physical == Hidden, not(s.ambiguousReceipt),
    s' = s.with("ambiguousReceipt", true).with("recovered", false)
      .with("last", AmbiguityInstalled),
  }
  action step = any {
    persistPending, installRelaxedHint, changeHint, reopenWithUncertainHint,
    recoverFromCanonicalState, prepareNamespace, commitNamespace,
    invalidateParentOrExclusiveAdmission, abortHiddenIntent,
    moveCompatiblePhysicalObject, advanceMovedPhysicalObject,
    installAmbiguousPendingReceiptOwner,
  }
  action coverageStep = step
}
