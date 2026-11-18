/// Converting takeover: when a closed shard's primary is lost, a write-set
/// member converts from its own copy in its place, and its activation makes
/// it primary at the next epoch.
///
/// One switched shard of old storage: write set M1, M2 and M3 with M1
/// primary, and M4, a configured recruit outside the write set. The model
/// keeps only what the converting takeover decides: the serving record
/// (epoch, primary, write set), the storage record with one Converting
/// attempt or the activated one, the image each member has staged, the loss
/// verdicts, and whether each member's own copy holds the shard's committed
/// history. Every write-set member's copy does, which the write-set takeover
/// model establishes; the recruit's does not. A closed shard commits nothing,
/// so copies do not change. Journals, open stores, local activation, count
/// contributions and the write-set protocol of an open shard are left to
/// their own models.
///
/// Storage transactions compare the shard's keys exactly, so each is one
/// atomic action on the current state. The primary may record its own
/// attempt. Another write-set member may record a takeover once the converter
/// it replaces is judged lost: the first round replaces at most the lost
/// primary's own attempt, and only the next round, by another member,
/// replaces a recorded round, counting every earlier converter but its own
/// as lost. Loss verdicts are inputs that may be wrong. The takeover's
/// promises only rank candidates and are not modeled, so any write-set member
/// may race for a round.
///
/// `Mode` selects the design or one broken variant used by a counterexample
/// scenario; `init` uses the design.
module dfs_converting_takeover {
  import common.* from "../../lib/common"

  type Member = M1 | M2 | M3 | M4
  pure val Members: Set[Member] = Set(M1, M2, M3, M4)
  pure val MaxRound: int = 3

  type Mode =
    | Design
    | PrimaryReplacesTakeover
    | RoundNotDerived
    | NoLossVerdict
    | ActivationKeepsServingRecord
    | ActivateWithoutInstall
    | ConverterOutsideWriteSet

  type Serving = { epoch: int, primary: Member, writeSet: Set[Member] }

  /// One attempt, under the serving record of its epoch. The primary's own
  /// attempt has round 0 and names itself as previous primary; a takeover
  /// names the lost primary, its round from 1 and the earlier converters it
  /// does not count on.
  type AttemptId = {
    epoch: int, converter: Member, previous: Member, round: int, lost: Set[Member],
  }
  type Attempt = { id: AttemptId, sealed: bool }
  type Storage =
    | NoRecord
    | Converting(Attempt)
    | Converted(AttemptId)

  /// A Converting record written in the place of `was`, with the loss verdict
  /// of the converter it replaced when it was written.
  type Replacement = { by: AttemptId, was: Storage, replacedLost: bool }
  type Activation = {
    id: AttemptId, before: Serving, after: Serving, staged: Set[Member], imageComplete: bool,
  }

  type State = {
    serving: Serving,
    storage: Storage,
    staged: Member -> Option[AttemptId],
    complete: Member -> bool,
    judgedLost: Set[Member],
    // History, for the invariants: every sealed attempt, every Converting
    // record written and every activation.
    sealedEver: Set[AttemptId],
    replacements: Set[Replacement],
    activations: Set[Activation],
    mode: Mode,
  }

  var s: State

  pure val initialServing: Serving = { epoch: 1, primary: M1, writeSet: Set(M1, M2, M3) }

  pure def initialState(mode: Mode): State = {
    serving: initialServing,
    storage: NoRecord,
    staged: Members.mapBy(_ => None),
    complete: Members.mapBy(m => m != M4),
    judgedLost: Set(),
    sealedEver: Set(),
    replacements: Set(),
    activations: Set(),
    mode: mode,
  }

  /// Whether a recorded attempt is a takeover at the serving epoch in the
  /// place of the serving primary: the epoch is then the takeover's.
  pure def isEpochTakeover(id: AttemptId, r: Serving): bool =
    id.epoch == r.epoch and id.round > 0 and id.previous == r.primary

  /// Whether an attempt runs under the serving record: its epoch, with its
  /// converter as primary, or for a takeover with its lost primary still
  /// primary and its converter another write-set member.
  def runsUnder(id: AttemptId): bool = {
    val r = s.serving
    val eligible = if (s.mode == ConverterOutsideWriteSet) Members.contains(id.converter) else r.writeSet.contains(id.converter)
    all {
      id.epoch == r.epoch,
      if (id.round == 0) id.converter == r.primary
      else all { id.previous == r.primary, id.converter != r.primary, eligible },
    }
  }

  /// The members an attempt's image must be staged on before it activates:
  /// the write set, without the lost primary and the earlier converters for a
  /// takeover.
  pure def installSet(id: AttemptId, r: Serving): Set[Member] =
    if (id.round == 0) r.writeSet
    else r.writeSet.exclude(Set(r.primary)).exclude(id.lost)

  def stagedFor(id: AttemptId): Set[Member] = Members.filter(m => s.staged.get(m) == Some(id))

  def installed(id: AttemptId): bool =
    s.mode == ActivateWithoutInstall or installSet(id, s.serving).subseteq(stagedFor(id))

  action initWith(mode: Mode): bool = s' = initialState(mode)

  action init = initWith(Design)

  /// The network or membership judges a member lost, or no longer.
  action judge(m: Member, lost: bool): bool = all {
    s.judgedLost.contains(m) != lost,
    s' = s.with("judgedLost", if (lost) s.judgedLost.union(Set(m)) else s.judgedLost.exclude(Set(m))),
  }

  /// The primary records its own attempt Converting from its own copy. It
  /// replaces no record, or an attempt the serving record no longer runs; it
  /// never replaces a takeover recorded at its epoch.
  action beginConversion: bool = {
    val r = s.serving
    val id: AttemptId = { epoch: r.epoch, converter: r.primary, previous: r.primary, round: 0, lost: Set() }
    val admitted = match s.storage {
      | NoRecord => true
      | Converting(a) =>
        if (isEpochTakeover(a.id, r)) s.mode == PrimaryReplacesTakeover else not(runsUnder(a.id))
      | Converted(_) => false
    }
    all {
      admitted,
      s' = s.with("storage", Converting({ id: id, sealed: false }))
        .with("replacements", s.replacements.union(Set({ by: id, was: s.storage, replacedLost: false }))),
    }
  }

  /// Another write-set member records a converting takeover from its own
  /// copy. With no takeover recorded at the epoch it runs the first round in
  /// the lost primary's place; otherwise the round after the recorded one,
  /// whose converter it judges lost.
  action beginTakeover(c: Member): bool = {
    val r = s.serving
    val recorded: Option[AttemptId] = match s.storage {
      | Converting(a) => if (isEpochTakeover(a.id, r)) Some(a.id) else None
      | _ => None
    }
    val derived = s.mode != RoundNotDerived
    val replaced = match recorded { | Some(a) => a.converter | None => r.primary }
    val round = match recorded { | Some(a) => if (derived) a.round + 1 else 1 | None => 1 }
    val lost = match recorded {
      | Some(a) => if (derived) a.lost.union(Set(a.converter)).exclude(Set(c)) else Set()
      | None => Set()
    }
    val id: AttemptId = { epoch: r.epoch, converter: c, previous: r.primary, round: round, lost: lost }
    val open = match s.storage { | Converted(_) => false | _ => true }
    all {
      open,
      round <= MaxRound,
      c != replaced,
      s.mode == NoLossVerdict or s.judgedLost.contains(replaced),
      runsUnder(id),
      s' = s.with("storage", Converting({ id: id, sealed: false }))
        .with("replacements", s.replacements.union(Set({
          by: id, was: s.storage, replacedLost: s.judgedLost.contains(replaced),
        }))),
    }
  }

  /// The converter seals its built image into the attempt, which must still
  /// run under the serving record.
  action seal: bool = match s.storage {
    | Converting(a) => all {
        not(a.sealed),
        runsUnder(a.id),
        s' = s.with("storage", Converting({ id: a.id, sealed: true }))
          .with("sealedEver", s.sealedEver.union(Set(a.id))),
      }
    | _ => all { false, s' = s }
  }

  /// A member stages a sealed image beside its own copy. A stale image may
  /// still arrive; staging alone changes nothing the record admits.
  action stage(m: Member, id: AttemptId): bool = all {
    s.sealedEver.contains(id),
    s' = s.with("staged", s.staged.set(m, Some(id))),
  }

  /// The primary's own attempt activates on the storage record alone.
  action activateConversion: bool = match s.storage {
    | Converting(a) => all {
        a.id.round == 0,
        a.sealed,
        runsUnder(a.id),
        installed(a.id),
        s' = s.with("storage", Converted(a.id))
          .with("activations", s.activations.union(Set({
            id: a.id, before: s.serving, after: s.serving, staged: stagedFor(a.id),
            imageComplete: s.complete.get(a.id.converter),
          }))),
      }
    | _ => all { false, s' = s }
  }

  /// A takeover activates and makes its converter primary at the next epoch
  /// in one transaction: the lost primary leaves the write set.
  action activateTakeover: bool = match s.storage {
    | Converting(a) => {
        val r = s.serving
        val moved: Serving = {
          epoch: r.epoch + 1, primary: a.id.converter,
          writeSet: r.writeSet.exclude(Set(r.primary)).union(Set(a.id.converter)),
        }
        val after = if (s.mode == ActivationKeepsServingRecord) r else moved
        all {
          a.id.round > 0,
          a.sealed,
          runsUnder(a.id),
          installed(a.id),
          s' = s.with("serving", after)
            .with("storage", Converted(a.id))
            .with("activations", s.activations.union(Set({
              id: a.id, before: r, after: after, staged: stagedFor(a.id),
              imageComplete: s.complete.get(a.id.converter),
            }))),
        }
      }
    | _ => all { false, s' = s }
  }

  action stageAny(m: Member): bool =
    if (s.sealedEver == Set()) all { false, s' = s }
    else {
      nondet id = s.sealedEver.oneOf()
      stage(m, id)
    }

  action step = {
    nondet m = Members.oneOf()
    nondet b = Set(true, false).oneOf()
    any {
      judge(m, b), beginConversion, beginTakeover(m), seal, stageAny(m),
      activateConversion, activateTakeover,
    }
  }
}
