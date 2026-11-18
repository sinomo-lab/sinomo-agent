/// Count contributions while a target shard converts.
///
/// One canonical object owned by target shard T, and the names that refer to
/// it: hardlink aliases whose rows live in T or in one of the source shards
/// S1 and S2. While T is Active its maintained count of the object is the
/// number of names. T starts either Active or as old storage, which has no
/// maintained count. Its conversion records Converting first, then reads,
/// once per round and one source at a time, what each source contributes
/// (its live aliases of the object) and the source's jobs that touch T. It
/// accepts the contributions only after two consecutive complete rounds give
/// identical answers and no answer names a job touching T that is not parked
/// on T, and activates with its own rows plus the accepted contributions.
///
/// Linking or unlinking a name changes the object's count, so it needs T's
/// canonical object and waits while T is not Active. A cross-shard rename of
/// an alias between the sources is a job coordinated by the alias's shard. It
/// can start at any time; the step that moves the alias row checks T's
/// storage record in the same transaction, and while T is not Active the
/// coordinator records the job parked on T instead. So the contributions to
/// T are frozen while T is closed.
///
/// Sources are open current-format shards. `Mode` selects the design or one
/// broken variant used by a counterexample scenario; `init` uses the
/// design. Excludes directory children, legacy and crash-left jobs, per-pair
/// settlement of intermediate job phases, serving changes, transport and
/// liveness.
module dfs_count_contributions {
  import common.* from "../../lib/common"

  type Shard = T | S1 | S2
  pure val Sources: Set[Shard] = Set(S1, S2)
  type Alias = A1 | A2
  pure val Aliases: Set[Alias] = Set(A1, A2)

  type Mode =
    | Design
    | MovesIgnoreClosedTarget
    | CountChangesIgnoreClosedTarget

  type Event =
    | Initialized | Linked | Unlinked | MoveStarted | MoveParked | Moved
    | ConvertingRecorded | SourceRead | RoundHeld | RoundAgain | Accepted | Activated

  type TargetState = OldStorage | Converting | Active

  type MoveJob = { dest: Shard, parked: bool }
  type Job = NoJob | Moving(MoveJob)

  /// A job the source reports as touching T, with whether its coordinator
  /// recorded it parked on T.
  type ReportedJob = { alias: Alias, parked: bool }
  /// One source's answer in one round.
  type Answer = { contribution: int, jobs: Set[ReportedJob] }

  type State = {
    target: TargetState,
    // T's maintained count of the object; exact only while T is Active.
    count: int,
    // T's own alias rows, captured when T recorded Converting.
    own: int,
    linked: Set[Alias],
    loc: Alias -> Shard,
    jobs: Alias -> Job,
    round: Shard -> Option[Answer],
    previous: Option[Shard -> Answer],
    accepted: Option[int],
    // History, for the invariants and witnesses: the contributions when T
    // recorded Converting, rounds held by a running job, and whether T
    // activated from old storage with a parked job outstanding.
    frozen: Shard -> int,
    heldRounds: int,
    activatedWithParkedJob: bool,
    mode: Mode,
    last: Event,
  }

  var s: State

  pure val emptyRound: Shard -> Option[Answer] = Sources.mapBy(_ => None)

  pure def initialState(mode: Mode, old: bool): State = {
    target: if (old) OldStorage else Active,
    count: if (old) 0 else 2,
    own: 0,
    linked: Aliases,
    loc: Aliases.mapBy(a => if (a == A1) S1 else T),
    jobs: Aliases.mapBy(_ => NoJob),
    round: emptyRound,
    previous: None,
    accepted: None,
    frozen: Sources.mapBy(_ => 0),
    heldRounds: 0,
    activatedWithParkedJob: false,
    mode: mode,
    last: Initialized,
  }

  /// What a shard's rows contribute to the object's count.
  def contribution(sh: Shard): int = s.linked.filter(a => s.loc.get(a) == sh).size()

  def isMoving(a: Alias): bool = match s.jobs.get(a) {
    | Moving(_) => true
    | NoJob => false
  }

  def isParked(a: Alias): bool = match s.jobs.get(a) {
    | Moving(j) => j.parked
    | NoJob => false
  }

  /// A source's answer: its contribution and the jobs it coordinates that
  /// touch T.
  def answer(sh: Shard): Answer = {
    contribution: contribution(sh),
    jobs: Aliases.filter(a => isMoving(a) and s.loc.get(a) == sh).map(a => { alias: a, parked: isParked(a) }),
  }

  action initWith(mode: Mode, old: bool): bool = s' = initialState(mode, old)

  action init = {
    nondet old = Set(true, false).oneOf()
    initWith(Design, old)
  }

  /// A new name for the object, in any shard; it needs T's canonical object.
  action link(a: Alias, sh: Shard): bool = all {
    s.mode == CountChangesIgnoreClosedTarget or s.target == Active,
    not(s.linked.contains(a)),
    s' = s.with("linked", s.linked.union(Set(a)))
      .with("loc", s.loc.set(a, sh))
      .with("count", if (s.target == Active) s.count + 1 else s.count)
      .with("last", Linked),
  }

  action unlink(a: Alias): bool = all {
    s.mode == CountChangesIgnoreClosedTarget or s.target == Active,
    s.linked.contains(a),
    not(isMoving(a)),
    s' = s.with("linked", s.linked.exclude(Set(a)))
      .with("count", if (s.target == Active) s.count - 1 else s.count)
      .with("last", Unlinked),
  }

  /// The alias's shard starts a cross-shard rename to the other source.
  action startMove(a: Alias, dest: Shard): bool = all {
    s.linked.contains(a),
    Sources.contains(s.loc.get(a)),
    Sources.contains(dest),
    dest != s.loc.get(a),
    not(isMoving(a)),
    s' = s.with("jobs", s.jobs.set(a, Moving({ dest: dest, parked: false })))
      .with("last", MoveStarted),
  }

  /// The job's next step finds T not Active, and its coordinator records it
  /// parked on T.
  action park(a: Alias): bool = match s.jobs.get(a) {
    | Moving(j) => all {
        not(j.parked),
        s.target != Active,
        s' = s.with("jobs", s.jobs.set(a, Moving({ dest: j.dest, parked: true })))
          .with("last", MoveParked),
      }
    | NoJob => all { false, s' = s }
  }

  /// The step that moves the alias row, in one transaction with its check
  /// that T is Active.
  action moveStep(a: Alias): bool = match s.jobs.get(a) {
    | Moving(j) => all {
        s.mode == MovesIgnoreClosedTarget or s.target == Active,
        s' = s.with("loc", s.loc.set(a, j.dest))
          .with("jobs", s.jobs.set(a, NoJob))
          .with("last", Moved),
      }
    | NoJob => all { false, s' = s }
  }

  /// T records Converting before it reads any contribution.
  action beginConversion: bool = all {
    s.target == OldStorage,
    s' = s.with("target", Converting)
      .with("own", contribution(T))
      .with("frozen", Sources.mapBy(sh => contribution(sh)))
      .with("round", emptyRound)
      .with("previous", None)
      .with("accepted", None)
      .with("last", ConvertingRecorded),
  }

  /// T reads one source's answer in the current round.
  action readSource(sh: Shard): bool = all {
    s.target == Converting,
    s.accepted == None,
    Sources.contains(sh),
    s.round.get(sh) == None,
    s' = s.with("round", s.round.set(sh, Some(answer(sh)))).with("last", SourceRead),
  }

  /// A complete round replaces the previous one. It is accepted only when no
  /// answer names a job touching T that is not parked on T, and it equals the
  /// previous round.
  action completeRound: bool = {
    val complete = Sources.forall(sh => s.round.get(sh) != None)
    val answers = Sources.mapBy(sh => unwrapOr(s.round.get(sh), { contribution: 0, jobs: Set() }))
    val held = Sources.exists(sh => answers.get(sh).jobs.exists(j => not(j.parked)))
    val agreed = not(held) and s.previous == Some(answers)
    val total = answers.get(S1).contribution + answers.get(S2).contribution
    all {
      s.target == Converting,
      s.accepted == None,
      complete,
      s' = s.with("previous", Some(answers))
        .with("round", emptyRound)
        .with("accepted", if (agreed) Some(total) else None)
        .with("heldRounds", if (held) s.heldRounds + 1 else s.heldRounds)
        .with("last", if (agreed) Accepted else if (held) RoundHeld else RoundAgain),
    }
  }

  /// T activates with its own rows plus the accepted contributions.
  action activate: bool = all {
    s.target == Converting,
    s.accepted != None,
    s' = s.with("target", Active)
      .with("count", s.own + unwrapOr(s.accepted, 0))
      .with("activatedWithParkedJob", Aliases.exists(a => isParked(a)))
      .with("last", Activated),
  }

  action step = {
    nondet a = Aliases.oneOf()
    nondet sh = Set(T, S1, S2).oneOf()
    nondet dest = Sources.oneOf()
    any {
      link(a, sh), unlink(a), startMove(a, dest), park(a), moveStep(a),
      beginConversion, readSource(sh), completeRound, activate,
    }
  }
}
