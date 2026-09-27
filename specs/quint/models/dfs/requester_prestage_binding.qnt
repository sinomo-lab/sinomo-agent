/// Target refinement: requester-prestaged bytes cannot acknowledge a rebound
/// object/version/hash tuple. Binding 0 is the original tuple, 1 changes its
/// version, and 2 changes its object. Node 0 is the requester; node 1 is another
/// selected content replica. A durable binding includes manifest + reservation.
/// Blob bytes alone remain only a fetch source. Existing canonical StageBlob
/// must bind a replacement tuple before W counts, independently of reply loss.
/// Namespace/authority gates are abstracted, not changed by this model.
module dfs_requester_prestage_binding {
  type Phase = Idle | Captured | Admitted | Committed | Rejected
  type StageState = {
    phase: Phase, canonicalBinding: int, selectedReplica: int,
    durableBindings: Set[(int, int)], receipts: Set[(int, int)],
    fetchedBytes: bool, stageCount: int, metadataAuthority: bool, published: int,
    replyDelivered: bool, lostReply: bool, requesterAlive: bool, restarted: bool,
    cancelled: bool, changedRetryRejected: bool, rejectReason: int,
  }
  pure val initial: StageState = {
    phase: Idle, canonicalBinding: 0, selectedReplica: 0,
    durableBindings: Set(), receipts: Set(), fetchedBytes: false,
    stageCount: 0, metadataAuthority: true, published: 0,
    replyDelivered: false, lostReply: false, requesterAlive: true,
    restarted: false, cancelled: false, changedRetryRejected: false, rejectReason: 0,
  }
  var s: StageState
  def canonicalReceipt: (int, int) = (s.selectedReplica, s.canonicalBinding)
  def exactDurableReceipt: bool =
    s.receipts.contains(canonicalReceipt) and s.durableBindings.contains(canonicalReceipt)
  /// Observe the old unsafe eligibility rule without using it to publish.
  def legacyWouldCreditWrongTuple: bool = all {
    s.phase == Admitted, s.selectedReplica == 0, s.canonicalBinding != 0,
    s.durableBindings.contains((0, 0)), not(exactDurableReceipt),
  }
  action init = s' = initial
  action captureRequester = all {
    s.phase == Idle,
    s' = s.with("phase", Captured).with("durableBindings", Set((0, 0))),
  }
  action admit(binding: int, selected: int): bool = all {
    s.phase == Captured,
    s' = s.with("phase", Admitted)
      .with("canonicalBinding", binding).with("selectedReplica", selected)
      .with("receipts", if (binding == 0 and selected == 0) { Set((0, 0)) } else { Set() }),
  }
  action rejectAdmission(reason: int): bool = all {
    s.phase == Captured,
    s' = s.with("phase", Rejected).with("rejectReason", reason),
  }
  action fetchBytes = all {
    s.phase == Admitted, not(exactDurableReceipt), not(s.fetchedBytes), s.requesterAlive,
    s' = s.with("fetchedBytes", true),
  }
  action bindCanonicalReplica = all {
    s.phase == Admitted, s.fetchedBytes, not(exactDurableReceipt), s.stageCount == 0,
    s' = s.with("durableBindings", s.durableBindings.union(Set(canonicalReceipt)))
      .with("receipts", Set(canonicalReceipt)).with("stageCount", 1),
  }
  action loseAuthority = all {
    s.phase == Admitted, s.metadataAuthority,
    s' = s.with("metadataAuthority", false),
  }
  action publish = all {
    s.phase == Admitted, exactDurableReceipt, s.metadataAuthority,
    s' = s.with("phase", Committed).with("published", 1),
  }
  action cancelCaller = all {
    s.phase == Admitted, not(s.cancelled),
    s' = s.with("cancelled", true),
  }
  action loseReplyAndCrash = all {
    s.phase == Committed, not(s.replyDelivered), s.requesterAlive, not(s.lostReply),
    s' = s.with("lostReply", true).with("requesterAlive", false),
  }
  action restartRequester = all {
    not(s.requesterAlive), not(s.restarted),
    s' = s.with("requesterAlive", true).with("restarted", true),
  }
  action deliverReply = all {
    s.phase == Committed, s.requesterAlive, not(s.replyDelivered),
    s' = s.with("replyDelivered", true),
  }
  action rejectChangedRetry = all {
    s.phase != Idle, not(s.changedRetryRejected),
    s' = s.with("changedRetryRejected", true),
  }
  action step = {
    nondet binding = Set(0, 1, 2).oneOf()
    nondet selected = Set(0, 1).oneOf()
    nondet reason = Set(1, 2, 3).oneOf()
    any {
      captureRequester, admit(binding, selected), rejectAdmission(reason),
      fetchBytes, bindCanonicalReplica, loseAuthority, publish, cancelCaller,
      loseReplyAndCrash, restartRequester, deliverReply, rejectChangedRetry,
      s' = s,
    }
  }
}
