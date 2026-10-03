/// One-sync content-binding publication and replay, complementary to
/// content_binding_lifecycle. One live unit-sized tuple fits a one-unit quota;
/// a second tuple must be rejected until domain-authorized deletion. A complete
/// frame contains payload + exact manifest + causal reservation together.
///
/// `visible` may exist after a failed sync without being certified. Reopening
/// validates and synchronizes the surviving complete prefix; index observation
/// alone never certifies it. Torn EOF and complete malformed records differ.
/// Header activation, physical reclamation and held readers are proved by the
/// lifecycle slice. Domain GC permission/freshness/grace are explicit inputs;
/// this slice proves local incarnation/epoch ordering and reservation renewal.
module dfs_content_binding_publication {
  type Binding = {
    present: bool, tuple: int, payload: int, manifestPayload: int,
    incarnation: int, epoch: int, manifest: bool, reservation: bool, bytes: bool,
  }
  pure val empty: Binding = {
    present: false, tuple: 0, payload: 0, manifestPayload: 0,
    incarnation: 1, epoch: 0, manifest: false, reservation: false, bytes: false,
  }
  type State = {
    visible: Binding, durable: Binding, cache: Binding,
    stamp: int, durableStamp: int, cacheStamp: int, certifiedStamp: int,
    pending: bool, ready: bool, torn: bool, tornObserved: bool, malformed: bool,
    opened: bool, blocked: bool, syncFailed: bool, uncertainObserved: bool, retriedSync: bool,
    acked: Set[(int, int)], deleted: Set[(int, int)], lostReply: bool,
    crashedAfterLostReply: bool, crashes: int, indexLoss: bool,
    epoch: int, cutPresent: bool, cutEpoch: int, cutIncarnation: int,
    negativeBlocked: bool, negativeReleased: bool, positiveReleased: bool,
    deletePermit: bool, conflictRejected: bool, quotaRejected: bool,
    renewalCount: int,
  }
  pure val initial: State = {
    visible: empty, durable: empty, cache: empty,
    stamp: 0, durableStamp: 0, cacheStamp: 0, certifiedStamp: 0,
    pending: false, ready: false, torn: false, tornObserved: false, malformed: false,
    opened: true, blocked: false, syncFailed: false, uncertainObserved: false, retriedSync: false,
    acked: Set(), deleted: Set(), lostReply: false, crashedAfterLostReply: false,
    crashes: 0, indexLoss: false, epoch: 0, cutPresent: false, cutEpoch: 0, cutIncarnation: 1,
    negativeBlocked: false, negativeReleased: false, positiveReleased: false,
    deletePermit: false, conflictRejected: false, quotaRejected: false, renewalCount: 0,
  }
  var s: State
  pure def valid(binding: Binding): bool = not(binding.present) or all {
    binding.payload == binding.manifestPayload, binding.incarnation == 1,
    (binding.manifest or binding.reservation) implies binding.bytes,
  }
  def writable: bool = all {
    s.opened, not(s.blocked), not(s.pending), not(s.torn), not(s.malformed), s.stamp < 4,
  }
  def receiptAllowed: bool = all {
    s.opened, not(s.blocked), not(s.torn), not(s.malformed), not(s.pending), s.ready,
    s.certifiedStamp == s.stamp, s.durableStamp == s.stamp,
    s.visible == s.durable, valid(s.durable),
    s.durable.manifest, s.durable.reservation, s.durable.bytes,
  }
  action init = s' = initial
  action appendPublication(tuple: int, payload: int): bool = all {
    writable,
    val prior = s.durable
    if (prior.present and prior.tuple == tuple and prior.payload != payload) {
      s' = s.with("conflictRejected", true)
    } else if (prior.manifest and prior.tuple != tuple) {
      s' = s.with("quotaRejected", true)
    } else {
      val binding: Binding = {
        present: true, tuple: tuple, payload: payload, manifestPayload: payload,
        incarnation: 1, epoch: s.epoch, manifest: true, reservation: true, bytes: true,
      }
      s' = s.with("visible", binding).with("stamp", s.stamp + 1)
        .with("pending", true).with("ready", false).with("cacheStamp", -1)
        .with("deletePermit", false)
        .with("renewalCount", if (prior.present and prior.tuple == tuple) { s.renewalCount + 1 } else { s.renewalCount })
    },
  }
  action failSync(persisted: bool): bool = all {
    s.pending, not(s.syncFailed),
    s' = s.with("syncFailed", true).with("certifiedStamp", -1)
      .with("durable", if (persisted) { s.visible } else { s.durable })
      .with("durableStamp", if (persisted) { s.stamp } else { s.durableStamp }),
  }
  action observeUncertain = all {
    s.pending, s.syncFailed,
    s' = s.with("cache", s.visible).with("cacheStamp", s.stamp).with("uncertainObserved", true),
  }
  action syncFrame = all {
    s.pending, s.opened, not(s.blocked), not(s.torn), not(s.malformed), valid(s.visible),
    s' = s.with("durable", s.visible).with("durableStamp", s.stamp)
      .with("certifiedStamp", s.stamp).with("cache", s.visible).with("cacheStamp", s.stamp)
      .with("deleted", s.deleted.exclude(Set((s.visible.tuple, s.visible.payload))))
      .with("pending", false).with("ready", true).with("retriedSync", s.retriedSync or s.syncFailed),
  }
  action acknowledge = all {
    receiptAllowed,
    s' = s.with("acked", s.acked.union(Set((s.durable.tuple, s.durable.payload))))
      .with("ready", false),
  }
  action loseReply = all { s.acked.size() > 0, s' = s.with("lostReply", true) }
  action appendTorn = all { writable, s' = s.with("torn", true).with("tornObserved", true).with("cacheStamp", -1).with("ready", false) }
  action appendMalformed = all { writable, s' = s.with("malformed", true).with("cacheStamp", -1).with("ready", false) }
  action loseIndex = all { s' = s.with("cache", empty).with("cacheStamp", -1).with("indexLoss", true) }
  action crash = all {
    s.opened, s.crashes == 0,
    s' = s.with("visible", s.durable).with("stamp", s.durableStamp)
      .with("cache", empty).with("cacheStamp", -1).with("certifiedStamp", -1)
      .with("opened", false).with("pending", false).with("ready", false)
      .with("crashes", 1).with("crashedAfterLostReply", s.lostReply),
  }
  action reopen = all {
    not(s.opened),
    if (s.malformed) { s' = s.with("blocked", true) }
    else {
      // Mandatory cold validation + sync of a complete surviving frame.
      s' = s.with("cache", s.durable).with("cacheStamp", s.durableStamp)
        .with("certifiedStamp", s.durableStamp).with("torn", false).with("opened", true)
    },
  }
  action rebuildIndex = all {
    s.opened, not(s.pending), s.cacheStamp == -1, not(s.blocked),
    if (s.malformed) { s' = s.with("blocked", true) }
    else { s' = s.with("cache", s.durable).with("cacheStamp", s.durableStamp) },
  }
  action beginCut = all {
    writable, s.epoch < 2,
    s' = s.with("epoch", s.epoch + 1).with("cutEpoch", s.epoch + 1)
      .with("cutIncarnation", 1).with("cutPresent", true),
  }
  action foreignCut = all { s.cutPresent, s' = s.with("cutIncarnation", 2) }
  action releaseReservation(positive: bool): bool = all {
    writable, s.durable.reservation,
    val negative = s.cutPresent and s.cutIncarnation == s.durable.incarnation and s.durable.epoch < s.cutEpoch
    if (positive or negative) {
      val released = s.durable.with("reservation", false)
      s' = s.with("visible", released).with("durable", released).with("cache", released)
        .with("stamp", s.stamp + 1).with("durableStamp", s.stamp + 1)
        .with("cacheStamp", s.stamp + 1).with("certifiedStamp", s.stamp + 1)
        .with("ready", false).with("positiveReleased", s.positiveReleased or positive)
        .with("negativeReleased", s.negativeReleased or not(positive))
    } else { s' = s.with("negativeBlocked", true) },
  }
  action authorizeVersionDeletion = all { s.durable.manifest, s' = s.with("deletePermit", true) }
  action deleteVersion = all {
    writable, s.deletePermit, not(s.durable.reservation), s.durable.manifest,
    val retired = s.durable.with("manifest", false).with("bytes", false)
    s' = s.with("visible", retired).with("durable", retired).with("cache", retired)
      .with("stamp", s.stamp + 1).with("durableStamp", s.stamp + 1)
      .with("cacheStamp", s.stamp + 1).with("certifiedStamp", s.stamp + 1)
      .with("ready", false).with("deleted", s.deleted.union(Set((s.durable.tuple, s.durable.payload)))),
  }
  action step = {
    nondet tuple = Set(0, 1).oneOf()
    nondet payload = Set(0, 1).oneOf()
    nondet choice = Set(false, true).oneOf()
    any {
      appendPublication(tuple, payload), failSync(choice), observeUncertain, syncFrame,
      acknowledge, loseReply, appendTorn, appendMalformed, loseIndex, crash,
      reopen, rebuildIndex, beginCut, foreignCut, releaseReservation(choice),
      authorizeVersionDeletion, deleteVersion, s' = s,
    }
  }
  action renewalCoverageStep =
    if (s.stamp == 0) { appendPublication(0, 0) }
    else if (s.pending) { syncFrame }
    else if (s.epoch == 0) { beginCut }
    else if (s.renewalCount == 0) { appendPublication(s.durable.tuple, s.durable.payload) }
    else if (s.durable.reservation and not(s.negativeBlocked)) { releaseReservation(false) }
    else { s' = s }
  action coverageStep = any { step, renewalCoverageStep }
}
