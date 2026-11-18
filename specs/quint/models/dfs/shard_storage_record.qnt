/// Per-shard storage record: the storage generation one metadata shard
/// serves, the evidence that activated it, and the conversion that changes
/// it.
///
/// One shard with three configured members; a switched shard's write set
/// starts as two of them. Cluster KV holds the shard's serving record (epoch,
/// primary and write set), its storage record and the filesystem's catalog
/// generation as separate keys. Each storage transaction compares the shard's
/// own keys exactly and writes only the storage record, so it is modeled as
/// one atomic action on the current state: a transaction built from an older
/// read is rejected and changes nothing. Data, journals and count
/// contributions are abstracted away; an attempt's source is an opaque
/// capture identity.
///
/// A shard born current has an Active native-genesis record. A shard with no
/// record is old storage that the migration owner converts: its primary
/// records an attempt Converting from its own copy, seals the image into the
/// attempt, has it staged on the members the install rule names and
/// activates it; each member then activates its staged copy, and a member
/// left out repairs by snapshot once the record is Active. A member's slot
/// opens only while its local generation header matches an Active record.
/// Serving changes and catalog changes are environment actions enabled at
/// any time; a serving change never touches the storage record and leaves a
/// Converting attempt superseded.
///
/// `Mode` selects the design or one broken variant used by a counterexample
/// scenario; `init` uses the design. The converting takeover is modeled in
/// `converting_takeover` and count contributions in `count_contributions`.
/// Excludes transport, timers and liveness.
module dfs_shard_storage_record {
  type Member = N1 | N2 | N3
  pure val Members: Set[Member] = Set(N1, N2, N3)
  /// Distinct captures a converter can take of its own copy.
  pure val Sources: Set[int] = Set(1, 2)
  pure val MaxEpoch: int = 3
  pure val MaxCatalogGen: int = 3

  type Mode =
    | Design
    | ConvertActive
    | NoSupersession
    | ActivateWithoutInstall
    | ActivationAdvancesCatalog

  type Event =
    | Initialized | ConvertingRecorded | Sealed | Staged | Activated
    | LocalActivated | Repaired | Opened | ServingChanged | CatalogChanged

  /// One attempt: the serving record it runs under and the source its
  /// converter captured of its own copy.
  type AttemptId = { epoch: int, converter: Member, source: int }
  type Attempt = { id: AttemptId, sealed: bool }

  type Record =
    | NoRecord
    | Converting(Attempt)
    | NativeGenesis
    | Converted(AttemptId)

  /// A member's local generation header.
  type Header =
    | OldStorage
    | GenesisHeader
    | StagedHeader(AttemptId)
    | ActiveHeader(AttemptId)

  type Recorded = { id: AttemptId, catalog: int }
  type Activation = {
    id: AttemptId, epoch: int, primary: Member, writeSet: Set[Member],
    installed: Set[Member], catalog: int,
  }

  type State = {
    epoch: int,
    primary: Member,
    // Whether the shard has a write set or uses the primary-and-majority
    // rule; fixed for a run.
    switched: bool,
    writeSet: Set[Member],
    catalogGen: int,
    storage: Record,
    header: Member -> Header,
    open: Set[Member],
    // History, for the invariants: every attempt recorded Converting with the
    // catalog generation at that time, every sealed attempt, every attempt a
    // serving change superseded, every activation, every repaired member, the
    // number of catalog changes, and whether a Converting record ever
    // replaced an Active one.
    recorded: Set[Recorded],
    sealedEver: Set[AttemptId],
    superseded: Set[AttemptId],
    activations: Set[Activation],
    repaired: Set[Member],
    catalogChanges: int,
    replacedActive: bool,
    mode: Mode,
    last: Event,
  }

  var s: State

  pure def initialState(mode: Mode, switched: bool, genesis: bool): State = {
    epoch: 1,
    primary: N1,
    switched: switched,
    writeSet: Set(N1, N2),
    catalogGen: 1,
    storage: if (genesis) NativeGenesis else NoRecord,
    header: Members.mapBy(_ => if (genesis) GenesisHeader else OldStorage),
    open: Set(),
    recorded: Set(),
    sealedEver: Set(),
    superseded: Set(),
    activations: Set(),
    repaired: Set(),
    catalogChanges: 0,
    replacedActive: false,
    mode: mode,
    last: Initialized,
  }

  pure def runsUnder(id: AttemptId, epoch: int, primary: Member): bool =
    id.epoch == epoch and id.converter == primary

  /// The open gate: a header may serve only under an Active record naming
  /// its generation.
  pure def admits(record: Record, header: Header): bool = match record {
    | NativeGenesis => header == GenesisHeader
    | Converted(id) => header == ActiveHeader(id)
    | _ => false
  }

  pure def isActive(record: Record): bool = match record {
    | NativeGenesis => true
    | Converted(_) => true
    | _ => false
  }

  /// The install rule: every write-set member on a switched shard, otherwise
  /// the primary and a majority of the configured members.
  pure def installRuleMet(switched: bool, writeSet: Set[Member], primary: Member, staged: Set[Member]): bool =
    if (switched) writeSet.subseteq(staged)
    else staged.contains(primary) and staged.size() * 2 > Members.size()

  def stagedFor(id: AttemptId): Set[Member] =
    Members.filter(m => s.header.get(m) == StagedHeader(id))

  action initWith(mode: Mode, switched: bool, genesis: bool): bool =
    s' = initialState(mode, switched, genesis)

  action init = {
    nondet switched = Set(true, false).oneOf()
    nondet genesis = Set(true, false).oneOf()
    initWith(Design, switched, genesis)
  }

  /// The primary records its attempt Converting before it reads any input.
  /// It replaces no record, or a Converting attempt a serving change
  /// superseded. An Active record never converts again, and another capture
  /// by the same primary at the same epoch is refused.
  action beginConversion(source: int): bool = {
    val id = { epoch: s.epoch, converter: s.primary, source: source }
    val admitted = match s.storage {
      | NoRecord => true
      | Converting(a) => not(runsUnder(a.id, s.epoch, s.primary))
      | _ => s.mode == ConvertActive
    }
    all {
      Sources.contains(source),
      admitted,
      s' = s.with("storage", Converting({ id: id, sealed: false }))
        .with("recorded", s.recorded.union(Set({ id: id, catalog: s.catalogGen })))
        .with("replacedActive", s.replacedActive or isActive(s.storage))
        .with("last", ConvertingRecorded),
    }
  }

  /// The built image is sealed into the attempt, which must still run under
  /// the current serving record.
  action seal: bool = match s.storage {
    | Converting(a) => all {
        not(a.sealed),
        runsUnder(a.id, s.epoch, s.primary),
        s' = s.with("storage", Converting({ id: a.id, sealed: true }))
          .with("sealedEver", s.sealedEver.union(Set(a.id)))
          .with("last", Sealed),
      }
    | _ => all { false, s' = s }
  }

  /// A member stages a sealed image beside its own copy. The image of an
  /// attempt the record has moved past may still arrive; staging opens
  /// nothing, and a copy the record admits is not replaced.
  action stage(m: Member, id: AttemptId): bool = all {
    s.sealedEver.contains(id),
    not(admits(s.storage, s.header.get(m))),
    s' = s.with("header", s.header.set(m, StagedHeader(id)))
      .with("last", Staged),
  }

  /// One transaction on the shard's own keys: the sealed attempt still runs
  /// under the serving record and its install rule holds. The serving record
  /// and the filesystem's catalog generation are unchanged.
  action activate: bool = match s.storage {
    | Converting(a) => all {
        a.sealed,
        s.mode == NoSupersession or runsUnder(a.id, s.epoch, s.primary),
        s.mode == ActivateWithoutInstall or installRuleMet(s.switched, s.writeSet, s.primary, stagedFor(a.id)),
        s' = s.with("storage", Converted(a.id))
          .with("activations", s.activations.union(Set({
            id: a.id, epoch: s.epoch, primary: s.primary, writeSet: s.writeSet,
            installed: stagedFor(a.id), catalog: s.catalogGen,
          })))
          .with("catalogGen", if (s.mode == ActivationAdvancesCatalog) s.catalogGen + 1 else s.catalogGen)
          .with("last", Activated),
      }
    | _ => all { false, s' = s }
  }

  /// A member activates its staged copy of the attempt the record activated.
  action activateLocal(m: Member): bool = match s.storage {
    | Converted(id) => all {
        s.header.get(m) == StagedHeader(id),
        s' = s.with("header", s.header.set(m, ActiveHeader(id)))
          .with("last", LocalActivated),
      }
    | _ => all { false, s' = s }
  }

  /// A member without the active generation installs the serving primary's
  /// snapshot.
  action repair(m: Member): bool = all {
    m != s.primary,
    s.open.contains(s.primary),
    not(admits(s.storage, s.header.get(m))),
    s' = s.with("header", s.header.set(m, s.header.get(s.primary)))
      .with("repaired", s.repaired.union(Set(m)))
      .with("last", Repaired),
  }

  action openSlot(m: Member): bool = all {
    not(s.open.contains(m)),
    admits(s.storage, s.header.get(m)),
    s' = s.with("open", s.open.union(Set(m)))
      .with("last", Opened),
  }

  /// A takeover, rotation or handoff: the next epoch, possibly under another
  /// primary, who joins a switched shard's write set. It neither compares nor
  /// writes the storage record.
  action servingChange(q: Member): bool = {
    val superseded = match s.storage {
      | Converting(a) => s.superseded.union(Set(a.id))
      | _ => s.superseded
    }
    all {
      s.epoch < MaxEpoch,
      s' = s.with("epoch", s.epoch + 1)
        .with("primary", q)
        .with("writeSet", if (s.switched) s.writeSet.union(Set(q)) else s.writeSet)
        .with("superseded", superseded)
        .with("last", ServingChanged),
    }
  }

  /// Another shard's birth or a path assignment advances the catalog
  /// generation.
  action catalogChange: bool = all {
    s.catalogGen < MaxCatalogGen,
    s' = s.with("catalogGen", s.catalogGen + 1)
      .with("catalogChanges", s.catalogChanges + 1)
      .with("last", CatalogChanged),
  }

  action stageAny(m: Member): bool =
    if (s.sealedEver == Set()) all { false, s' = s }
    else {
      nondet id = s.sealedEver.oneOf()
      stage(m, id)
    }

  action step = {
    nondet m = Members.oneOf()
    nondet q = Members.oneOf()
    nondet source = Sources.oneOf()
    any {
      beginConversion(source), seal, stageAny(m), activate, activateLocal(m),
      repair(m), openSlot(m), servingChange(q), catalogChange,
    }
  }
}
