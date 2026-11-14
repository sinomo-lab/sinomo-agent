/// Target hardening: shard metadata write sets with automatic takeover.
///
/// One metadata shard with four possible members, by default W floor 2 and
/// N 3; a run may instead start with the floor equal to N. Cluster
/// KV is a linearizable compare-and-swap register holding the shard record:
/// primary, epoch, generation, members, write set and epoch start. A commit
/// needs a current-epoch acknowledgement from every write-set member the
/// primary knows of, plus every member it has submitted to add. Write-set and
/// primary changes are guarded transactions on the whole record; a submitted
/// transaction may apply or abort later, and its submitter learns the outcome
/// separately. A takeover candidate is a write-set member that reads the record,
/// durably promises to accept nothing more at that epoch, then submits the swap.
///
/// Journals are durable and survive crashes; reads in progress, activation and
/// the record view are refreshed on restart. Acknowledgements remain available
/// as delayed messages. Takeover, shrink and recruitment are enabled whenever
/// their guards hold: the network verdicts that trigger them in the design are
/// liveness inputs, so safety must hold even when they are wrong.
///
/// A primary republishing an inherited prepared change under its new epoch is
/// modeled as committing the inherited entry once every write-set member has
/// acknowledged it at that epoch. A follower accepting a primary's journal
/// replaces its own, which covers incremental catch-up, dropping an
/// unacknowledged tail and snapshot install.
///
/// `Mode` selects the design or one broken variant used by the counterexample
/// scenarios; `init` uses the design. Excludes liveness, reads, content, real
/// storage and transport.
module dfs_metadata_write_set_takeover {
  import common.* from "../../lib/common"

  type Member = M1 | M2 | M3 | M4
  pure val Members: Set[Member] = Set(M1, M2, M3, M4)
  pure val Values: Set[int] = Set(1, 2, 3)
  pure val DefaultWFloor: int = 2
  pure val DefaultNCopies: int = 3
  pure val MaxEpoch: int = 3

  type Mode =
    | Design
    | EpochOnlyCompare
    | OutOfSyncCandidate
    | AddWithoutCatchUp
    | IgnorePendingAdd
    | VolatilePendingAdd
    | NoPromise

  type Event =
    | Initialized | Prepared | Accepted | Committed | Observed
    | QuorumConfirmed | Promised | SwapSubmitted | SwapApplied | SwapRejected
    | SwapLearned | PromiseReleased | Activated | Shrunk | AddSubmitted
    | AddApplied | AddRejected | Recruited | Retired | Crashed

  type Entry = { epoch: int, value: int }
  type Record = {
    primary: Member, epoch: int, gen: int,
    members: Set[Member], writeSet: Set[Member], epochStart: int,
  }
  type SwapTxn = { candidate: Member, basedOn: Record, start: int }
  type SwapOutcome = { txn: SwapTxn, applied: bool }
  type AddTxn = { primary: Member, member: Member, basedOn: Record }
  type PendingAdd = { member: Member, basedOn: Record }
  type Ack = { member: Member, epoch: int, gen: int, length: int }
  type CommitRecord = {
    primary: Member, epoch: int, entries: List[Entry],
    writeSetSize: int, deposed: bool,
  }

  type State = {
    kv: Record,
    view: Member -> Record,
    log: Member -> List[Entry],
    fence: Member -> int,
    promise: Member -> Option[Record],
    reading: Member -> Option[Record],
    active: Member -> int,
    pendingAdd: Member -> Set[PendingAdd],
    swapsInFlight: Set[SwapTxn],
    swapOutcomes: Set[SwapOutcome],
    addsInFlight: Set[AddTxn],
    acks: Set[Ack],
    commits: Set[CommitRecord],
    prepared: Set[int],
    swapLogs: int -> List[Entry],
    droppedTail: bool,
    // The shard's W floor and N copies, fixed for a run.
    wFloor: int,
    nCopies: int,
    mode: Mode,
    last: Event,
  }

  var s: State

  pure val initialRecord: Record = {
    primary: M1, epoch: 1, gen: 1,
    members: Set(M1, M2, M3), writeSet: Set(M1, M2, M3), epochStart: 0,
  }

  pure def initialState(mode: Mode): State = initialStateWithBounds(mode, DefaultWFloor, DefaultNCopies)

  pure def initialStateWithBounds(mode: Mode, wFloor: int, nCopies: int): State = {
    kv: initialRecord,
    view: Members.mapBy(_ => initialRecord),
    log: Members.mapBy(_ => List()),
    fence: Members.mapBy(_ => 1),
    promise: Members.mapBy(_ => None),
    reading: Members.mapBy(_ => None),
    active: Members.mapBy(m => if (m == M1) 1 else 0),
    pendingAdd: Members.mapBy(_ => Set()),
    swapsInFlight: Set(),
    swapOutcomes: Set(),
    addsInFlight: Set(),
    acks: Set(),
    commits: Set(),
    prepared: Set(),
    swapLogs: Map(),
    droppedTail: false,
    wFloor: wFloor,
    nCopies: nCopies,
    mode: mode,
    last: Initialized,
  }

  pure def maxInt(a: int, b: int): int = if (a > b) a else b

  pure def isPrefix(a: List[Entry], b: List[Entry]): bool =
    a.length() <= b.length() and a.indices().forall(i => a[i] == b[i])

  /// A node reading the record: its view, fence and pending adds follow the
  /// record, and it stops serving as primary once the record names another
  /// primary or epoch. A pending add stays required while the record still
  /// equals the one its transaction compared, because it may still apply.
  pure def refresh(st: State, n: Member): State = {
    val r = st.kv
    val keepActive = r.primary == n and st.active.get(n) == r.epoch
    st.with("view", st.view.set(n, r))
      .with("fence", st.fence.set(n, maxInt(st.fence.get(n), r.epoch)))
      .with("active", if (keepActive) st.active else st.active.set(n, 0))
      .with("pendingAdd", st.pendingAdd.set(n, st.pendingAdd.get(n).filter(pa => pa.basedOn == r)))
  }

  def isActivePrimary(p: Member): bool = all {
    s.view.get(p).primary == p,
    s.active.get(p) > 0,
    s.active.get(p) == s.view.get(p).epoch,
  }

  /// The primary's current record, checked by its guarded transaction.
  def ownsCurrentRecord(p: Member): bool = all {
    isActivePrimary(p),
    s.view.get(p) == s.kv,
    s.kv.primary == p,
  }

  def requiredAcks(p: Member): Set[Member] = {
    val pending = if (s.mode == IgnorePendingAdd) Set() else s.pendingAdd.get(p).map(pa => pa.member)
    s.view.get(p).writeSet.union(pending).exclude(Set(p))
  }

  def ackedAtLeast(m: Member, e: int, k: int): bool =
    s.acks.exists(a => a.member == m and a.epoch == e and a.length >= k)

  action initWith(mode: Mode): bool = s' = initialState(mode)

  action initWithBounds(mode: Mode, wFloor: int, nCopies: int): bool =
    s' = initialStateWithBounds(mode, wFloor, nCopies)

  action init = initWith(Design)

  action prepare(p: Member, v: int): bool = all {
    isActivePrimary(p),
    not(s.prepared.contains(v)),
    s' = s.with("log", s.log.set(p, s.log.get(p).append({ epoch: s.view.get(p).epoch, value: v })))
      .with("prepared", s.prepared.union(Set(v)))
      .with("last", Prepared),
  }

  /// A follower stores the primary's journal and acknowledges it. It checks
  /// the primary and epoch against its own view and its fence.
  action accept(f: Member, p: Member): bool = {
    val e = s.view.get(p).epoch
    val vf = s.view.get(f)
    val sent = s.log.get(p)
    all {
      f != p,
      isActivePrimary(p),
      vf.primary == p,
      vf.epoch == e,
      vf.members.contains(f),
      e >= s.fence.get(f),
      s' = s.with("log", s.log.set(f, sent))
        .with("acks", s.acks.union(Set({ member: f, epoch: e, gen: vf.gen, length: sent.length() })))
        .with("droppedTail", s.droppedTail or not(isPrefix(s.log.get(f), sent)))
        .with("last", Accepted),
    }
  }

  /// Report success for the first k journal entries.
  action commit(p: Member, k: int): bool = {
    val e = s.view.get(p).epoch
    val entries = s.log.get(p)
    all {
      isActivePrimary(p),
      s.view.get(p).writeSet.size() >= s.wFloor,
      k >= 1,
      k <= entries.length(),
      requiredAcks(p).forall(m => ackedAtLeast(m, e, k)),
      s' = s.with("commits", s.commits.union(Set({
          primary: p, epoch: e, entries: entries.slice(0, k),
          writeSetSize: s.view.get(p).writeSet.size(), deposed: s.kv.epoch > e,
        })))
        .with("last", Committed),
    }
  }

  action observe(n: Member): bool = all {
    s.view.get(n) != s.kv,
    s' = refresh(s, n).with("last", Observed),
  }

  /// A write-set member that can read the record linearizably starts a takeover.
  action confirmQuorum(c: Member): bool = {
    val r = s.kv
    val eligible = if (s.mode == OutOfSyncCandidate) r.members.contains(c) else r.writeSet.contains(c)
    all {
      eligible,
      r.primary != c,
      r.epoch < MaxEpoch,
      s.reading.get(c) == None,
      s.promise.get(c) == None,
      s' = refresh(s, c)
        .with("reading", s.reading.set(c, Some(r)))
        .with("last", QuorumConfirmed),
    }
  }

  /// The durable promise: nothing more is accepted at the read epoch.
  action promise(c: Member): bool = {
    val snap = s.reading.get(c).unwrapOr(initialRecord)
    all {
      s.reading.get(c) != None,
      s.promise.get(c) == None,
      s' = s.with("promise", s.promise.set(c, Some(snap)))
        .with("fence", if (s.mode == NoPromise) s.fence else s.fence.set(c, maxInt(s.fence.get(c), snap.epoch + 1)))
        .with("last", Promised),
    }
  }

  action submitSwap(c: Member): bool = {
    val snap = s.reading.get(c).unwrapOr(initialRecord)
    all {
      s.reading.get(c) != None,
      s.promise.get(c) == Some(snap),
      s' = s.with("swapsInFlight", s.swapsInFlight.union(Set({ candidate: c, basedOn: snap, start: s.log.get(c).length() })))
        .with("reading", s.reading.set(c, None))
        .with("last", SwapSubmitted),
    }
  }

  /// Cluster KV applies the swap only if the record still equals the one read;
  /// otherwise, or if the transaction aborts, it is rejected.
  action resolveSwap(t: SwapTxn, commitIt: bool): bool = {
    val r = s.kv
    val matches = if (s.mode == EpochOnlyCompare) r.epoch == t.basedOn.epoch else r == t.basedOn
    val applied = commitIt and matches
    val updated: Record = {
      primary: t.candidate, epoch: r.epoch + 1, gen: r.gen + 1, members: r.members,
      writeSet: r.writeSet.exclude(Set(r.primary)).union(Set(t.candidate)),
      epochStart: t.start,
    }
    all {
      s.swapsInFlight.contains(t),
      s' = s.with("kv", if (applied) updated else r)
        .with("swapLogs", if (applied) s.swapLogs.put(updated.epoch, s.log.get(t.candidate)) else s.swapLogs)
        .with("swapsInFlight", s.swapsInFlight.exclude(Set(t)))
        .with("swapOutcomes", s.swapOutcomes.union(Set({ txn: t, applied: applied })))
        .with("last", if (applied) SwapApplied else SwapRejected),
    }
  }

  /// The candidate learns its swap's outcome. It releases its promise only
  /// when its own takeover did not happen and the read epoch is still current.
  action learnSwap(o: SwapOutcome): bool = {
    val c = o.txn.candidate
    val snap = o.txn.basedOn
    val release = not(o.applied) and s.kv.epoch == snap.epoch
    val refreshed = refresh(s, c)
    all {
      s.swapOutcomes.contains(o),
      s.promise.get(c) == Some(snap),
      s' = refreshed
        .with("promise", refreshed.promise.set(c, None))
        .with("fence", if (release) refreshed.fence.set(c, s.kv.epoch) else refreshed.fence)
        .with("swapOutcomes", s.swapOutcomes.exclude(Set(o)))
        .with("last", if (release) PromiseReleased else SwapLearned),
    }
  }

  action activate(c: Member): bool = all {
    s.view.get(c).primary == c,
    s.active.get(c) != s.view.get(c).epoch,
    s' = s.with("active", s.active.set(c, s.view.get(c).epoch)).with("last", Activated),
  }

  action shrink(p: Member, m: Member): bool = {
    val r = s.kv
    val updated = r.with("writeSet", r.writeSet.exclude(Set(m))).with("gen", r.gen + 1)
    all {
      ownsCurrentRecord(p),
      r.writeSet.contains(m),
      m != p,
      r.writeSet.size() - 1 >= s.wFloor,
      s' = s.with("kv", updated)
        .with("view", s.view.set(p, updated))
        .with("pendingAdd", s.pendingAdd.set(p, Set()))
        .with("last", Shrunk),
    }
  }

  /// A caught-up member is submitted for the write set. Catch-up evidence is
  /// an acknowledgement of the primary's whole journal made under the current
  /// record generation, so it describes the member's current copy rather than
  /// one it has since deleted. The primary requires the member's
  /// acknowledgements from now on, durably, while the add may still apply.
  action submitAdd(p: Member, m: Member): bool = {
    val r = s.kv
    val caughtUp = s.mode == AddWithoutCatchUp or s.acks.exists(a => all {
      a.member == m, a.epoch == r.epoch, a.gen == r.gen, a.length == s.log.get(p).length(),
    })
    all {
      ownsCurrentRecord(p),
      r.members.contains(m),
      not(r.writeSet.contains(m)),
      caughtUp,
      not(s.addsInFlight.exists(t => t.member == m)),
      s' = s.with("addsInFlight", s.addsInFlight.union(Set({ primary: p, member: m, basedOn: r })))
        .with("pendingAdd", s.pendingAdd.set(p, s.pendingAdd.get(p).union(Set({ member: m, basedOn: r }))))
        .with("last", AddSubmitted),
    }
  }

  action resolveAdd(t: AddTxn, commitIt: bool): bool = {
    val r = s.kv
    val applied = commitIt and r == t.basedOn
    val updated = r.with("writeSet", r.writeSet.union(Set(t.member))).with("gen", r.gen + 1)
    all {
      s.addsInFlight.contains(t),
      s' = s.with("kv", if (applied) updated else r)
        .with("addsInFlight", s.addsInFlight.exclude(Set(t)))
        .with("last", if (applied) AddApplied else AddRejected),
    }
  }

  /// A recruit joins the configured members while the write set has fewer
  /// than N members, or as a replacement: when every configured member is in
  /// the write set and there are at most N of them, one member beyond N may
  /// be recruited, for a dead member held at a floor equal to N or a mobile
  /// member. The surplus member is retired once the replacement has joined.
  action recruit(p: Member, m: Member): bool = {
    val r = s.kv
    val updated = r.with("members", r.members.union(Set(m))).with("gen", r.gen + 1)
    val replacement = r.members == r.writeSet and r.members.size() <= s.nCopies
    all {
      ownsCurrentRecord(p),
      not(r.members.contains(m)),
      r.writeSet.size() < s.nCopies or replacement,
      s' = s.with("kv", updated)
        .with("view", s.view.set(p, updated))
        .with("pendingAdd", s.pendingAdd.set(p, Set()))
        .with("last", Recruited),
    }
  }

  /// A member outside the write set leaves the shard and deletes its copy.
  action retire(p: Member, m: Member): bool = {
    val r = s.kv
    val updated = r.with("members", r.members.exclude(Set(m))).with("gen", r.gen + 1)
    all {
      ownsCurrentRecord(p),
      r.members.contains(m),
      not(r.writeSet.contains(m)),
      r.members.size() > s.nCopies,
      s' = s.with("kv", updated)
        .with("view", s.view.set(p, updated))
        .with("log", s.log.set(m, List()))
        .with("pendingAdd", s.pendingAdd.set(p, Set()))
        .with("last", Retired),
    }
  }

  /// Process loss and restart. Journals, fences, promises and, in the design,
  /// pending adds are durable; the restarted node re-reads the record.
  action crash(n: Member): bool = {
    val refreshed = refresh(s, n)
    s' = refreshed
      .with("reading", refreshed.reading.set(n, None))
      .with("active", refreshed.active.set(n, 0))
      .with("pendingAdd", if (s.mode == VolatilePendingAdd) refreshed.pendingAdd.set(n, Set()) else refreshed.pendingAdd)
      .with("last", Crashed)
  }

  action resolveAnySwap(commitIt: bool): bool =
    if (s.swapsInFlight == Set()) all { false, s' = s }
    else {
      nondet t = s.swapsInFlight.oneOf()
      resolveSwap(t, commitIt)
    }

  action learnAnySwap: bool =
    if (s.swapOutcomes == Set()) all { false, s' = s }
    else {
      nondet o = s.swapOutcomes.oneOf()
      learnSwap(o)
    }

  action resolveAnyAdd(commitIt: bool): bool =
    if (s.addsInFlight == Set()) all { false, s' = s }
    else {
      nondet t = s.addsInFlight.oneOf()
      resolveAdd(t, commitIt)
    }

  action step = {
    nondet p = Members.oneOf()
    nondet q = Members.oneOf()
    nondet v = Values.oneOf()
    nondet k = Set(1, 2, 3).oneOf()
    nondet b = Set(true, false).oneOf()
    any {
      prepare(p, v), accept(q, p), commit(p, k), observe(p),
      confirmQuorum(p), promise(p), submitSwap(p), resolveAnySwap(b),
      learnAnySwap, activate(p), shrink(p, q), submitAdd(p, q),
      resolveAnyAdd(b), recruit(p, q), retire(p, q), crash(p),
    }
  }

  /// The same actions, weighted toward replication, completed takeovers and
  /// rejoins so that bounded runs reach second takeovers and inherited
  /// prepared changes. Safety runs use `step`.
  action coverageStep = {
    nondet p = Members.oneOf()
    nondet q = Members.oneOf()
    nondet v = Values.oneOf()
    nondet k = Set(1, 2, 3).oneOf()
    nondet b = Set(true, false).oneOf()
    any {
      prepare(p, v), accept(q, p), accept(q, p), accept(q, p), commit(p, k), commit(p, k),
      observe(p), observe(p), confirmQuorum(p), promise(p), promise(p), submitSwap(p),
      submitSwap(p), resolveAnySwap(true), resolveAnySwap(b), learnAnySwap, learnAnySwap,
      activate(p), activate(p), shrink(p, q), submitAdd(p, q), submitAdd(p, q),
      resolveAnyAdd(true), recruit(p, q), retire(p, q), crash(p),
    }
  }
}
