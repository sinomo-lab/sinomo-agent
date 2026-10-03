/// Bounded target/refinement model: unrestricted safety checked through6 transitions.
/// Seed0x51a0b00d20260928:5000 traces x40 steps reached all six witnesses;
/// later wrap/recovery witnesses are simulation evidence, not a liveness proof.
/// Two physical shards; one shard opened per invocation; raw visit budget B=2.
/// Keys are dense within each shard only to keep the finite pagination abstraction
/// small. The production cursor must use actual physical ordered keys, not counts.
/// through is frozen when entering a shard: later appends cannot extend this pass.
/// Shared owner cursor is advisory and survives shell cancellation. Recovery effects
/// still require current canonical source authority and unchanged intent identity.
/// Fair progress requires eventual stable topology and sufficiently many uncancelled
/// invocations; unrestricted cancellation is intentionally allowed to prevent claims
/// of unconditional temporal liveness. No generic filtered-scan fallback is modeled.
module dfs_bounded_intent_scan {
  val B = 2
  val Keys = Set(0, 1, 2, 3, 4, 5)
  type Cursor = { shard: int, after: int, through: int, entered: bool }
  type Work = {
    phase: int, start: Cursor, next: Cursor, generation: int, identity: int,
    raw: Set[int], pending: Set[int], output: Set[int], eof: bool,
  }
  type Effect = {
    object: int, observedGeneration: int, admittedGeneration: int,
    observedIdentity: int, currentIdentity: int, sourceValid: bool,
  }
  type State = {
    cursor: Cursor, w0: Work, w1: Work, generation: int, identity: int,
    count0: int, count1: int, active: Set[int], effects: Set[Effect],
    cancelledAfterCursor: bool, sawCasMiss: bool, wrapped: bool,
    sawTerminalVisit: bool, sawRejectedVisit: bool, sawDuplicateVisit: bool,
    sawAppendBeyondCut: bool, sawReplacedBehindCursor: bool,
    lastVisited: int, lastOutput: int, lastShardOpens: int, last: str,
  }
  pure def beginning(shard: int): Cursor = { shard: shard, after: -1, through: -1, entered: false }
  pure val emptyWork: Work = {
    phase: 0, start: beginning(0), next: beginning(0), generation: 0, identity: 0,
    raw: Set(), pending: Set(), output: Set(), eof: false,
  }
  pure def worker(x: State, id: int): Work = if (id == 0) x.w0 else x.w1
  pure def setWorker(x: State, id: int, w: Work): State =
    if (id == 0) x.with("w0", w) else x.with("w1", w)
  pure def count(x: State, shard: int): int = if (shard == 0) x.count0 else x.count1
  // key2 duplicates key0; key1 is rejected by canonical ownership validation.
  pure def object(shard: int, key: int): int = shard * 8 + (if (key == 2) 0 else key)
  pure def sourceCurrent(key: int): bool = key != 1
  pure def currentEffect(x: State, w: Work, key: int): bool = all {
    w.generation == x.generation, w.identity == x.identity,
    sourceCurrent(key), x.active.contains(object(w.start.shard, key)),
  }
  var s: State
  action init = s' = {
    cursor: beginning(0), w0: emptyWork, w1: emptyWork, generation: 0, identity: 0,
    count0: 4, count1: 4, active: Set(0, 8, 3, 11), effects: Set(),
    cancelledAfterCursor: false, sawCasMiss: false, wrapped: false,
    sawTerminalVisit: false, sawRejectedVisit: false, sawDuplicateVisit: false,
    sawAppendBeyondCut: false, sawReplacedBehindCursor: false,
    lastVisited: 0, lastOutput: 0, lastShardOpens: 0, last: "init",
  }
  action readRawPage = {
    nondet id = Set(0, 1).oneOf()
    val w = worker(s, id)
    val c = s.cursor
    val through = if (c.entered) c.through else count(s, c.shard) - 1
    val raw = Keys.filter(k => k > c.after and k <= through and k <= c.after + B)
    val end = if (c.after + B < through) c.after + B else through
    val eof = end >= through
    val followingCursor = if (eof) beginning(if (c.shard == 0) 1 else 0)
      else { shard: c.shard, after: end, through: through, entered: true }
    all {
      w.phase == 0,
      s' = setWorker(s, id, {
        phase: 1, start: c, next: followingCursor, generation: s.generation, identity: s.identity,
        raw: raw, pending: raw, output: Set(), eof: eof,
      }).with("lastVisited", raw.size()).with("lastShardOpens", 1)
        .with("lastOutput", 0).with("last", "read-raw-page")
        .with("sawTerminalVisit", s.sawTerminalVisit or raw.exists(k => not(s.active.contains(object(c.shard, k)))))
        .with("sawRejectedVisit", s.sawRejectedVisit or raw.contains(1))
        .with("sawDuplicateVisit", s.sawDuplicateVisit or raw.contains(2)),
    }
  }
  action publishCursor = {
    nondet id = Set(0, 1).oneOf()
    val w = worker(s, id)
    val matches = w.start == s.cursor and w.generation == s.generation
    all {
      w.phase == 1,
      // Completed raw-page observation is published before active recovery.
      // A distinct current cursor or generation rejects this completion.
      // Equal-cursor ABA may conservatively repeat an old cut; no authority
      // comes from this advisory CAS and later complete wraps revisit it.
      s' = setWorker(s, id, w.with("phase", 2))
        .with("cursor", if (matches) w.next else s.cursor)
        .with("sawCasMiss", s.sawCasMiss or not(matches))
        .with("wrapped", s.wrapped or (matches and w.eof and w.start.shard == 1))
        .with("last", if (matches) "cursor-advanced" else "cursor-cas-miss"),
    }
  }
  action inspectOne = {
    nondet id = Set(0, 1).oneOf()
    val w = worker(s, id)
    nondet key = w.pending.oneOf()
    val logical = object(w.start.shard, key)
    val allowed = currentEffect(s, w, key) and not(w.output.contains(logical))
    val output = if (allowed) w.output.union(Set(logical)) else w.output
    all {
      w.phase == 2,
      s' = setWorker(s, id, w.with("pending", w.pending.exclude(Set(key))).with("output", output))
        .with("effects", if (allowed) s.effects.union(Set({
          object: logical, observedGeneration: w.generation, admittedGeneration: s.generation,
          observedIdentity: w.identity, currentIdentity: s.identity, sourceValid: sourceCurrent(key),
        })) else s.effects)
        .with("lastOutput", output.size()).with("last", if (allowed) "recover-current" else "skip"),
    }
  }
  action cancelShell = {
    nondet id = Set(0, 1).oneOf()
    val w = worker(s, id)
    all {
      w.phase != 0,
      s' = setWorker(s, id, emptyWork)
        .with("cancelledAfterCursor", s.cancelledAfterCursor or w.phase == 2)
        .with("last", "cancel-shell"),
    }
  }
  action finishPage = {
    nondet id = Set(0, 1).oneOf()
    val w = worker(s, id)
    all { w.phase == 2, w.pending.size() == 0,
      s' = setWorker(s, id, emptyWork).with("last", "finish-page") }
  }
  action appendAfterCut = {
    nondet shard = Set(0, 1).oneOf()
    all {
      count(s, shard) < 6,
      s' = (if (shard == 0) s.with("count0", s.count0 + 1) else s.with("count1", s.count1 + 1))
        .with("active", s.active.union(Set(object(shard, count(s, shard)))))
        .with("sawAppendBeyondCut", s.sawAppendBeyondCut or (s.cursor.shard == shard and s.cursor.entered))
        .with("last", "append-after-cut"),
    }
  }
  action replaceBeforeCursor = all {
    s.identity == 0, s.cursor.entered,
    // Abstract a new/backdated active identity located before the saved after-key.
    s' = s.with("identity", 1).with("active", s.active.union(Set(object(s.cursor.shard, 0))))
      .with("sawReplacedBehindCursor", true).with("last", "replace-before-cursor"),
  }
  action topologyReset = all {
    s.generation == 0,
    s' = s.with("generation", 1).with("cursor", beginning(0)).with("last", "topology-reset"),
  }
  action step = any { readRawPage, publishCursor, inspectOne, cancelShell,
    finishPage, appendAfterCut, replaceBeforeCursor, topologyReset }
  val rawBudget = s.lastVisited <= B and s.lastShardOpens <= 1
  val outputBudget = all { s.lastOutput <= B, s.w0.output.size() <= B, s.w1.output.size() <= B }
  val rawBeforeFilter = Set(s.w0, s.w1).forall(w => w.pending.subseteq(w.raw))
  val exactEffect = s.effects.forall(e => all {
    e.observedGeneration == e.admittedGeneration,
    e.observedIdentity == e.currentIdentity, e.sourceValid,
    e.object != 1, e.object != 9,
  })
  val cursorShape = all { Set(0, 1).contains(s.cursor.shard), s.cursor.after >= -1,
    s.cursor.through >= -1, s.cursor.through < 6,
    s.cursor.entered implies s.cursor.after <= s.cursor.through }
  val safety = rawBudget and outputBudget and rawBeforeFilter and exactEffect and cursorShape
}
