/// Whole logical shard target contract, bounded abstraction.
/// Native trace replay is a separate, capture-scoped refinement check.
/// Recovery requires the configured metadata majority and operation-bound
/// native Raft permit observed from the implementation.
/// Native Raft quorum commitment and native durable-prefix certification are
/// explicit abstract inputs. No election, OS scheduling, timers or storage-byte
/// model is substituted for the existing mechanisms.
module dfs_whole_shard {
  pure val Nodes = Set(1, 2, 3, 4)
  pure val Operations = Set(1, 2, 3)
  pure val Sequences = Set(1, 2, 3, 4, 5, 6)
  pure val Voters = Set(1, 2, 3)
  pure val InitialMembers = Set(1, 2, 3)
  pure val ReplacementMembers = Set(1, 2, 4)
  pure val W = 1
  type Entry = { kind: int, op: int, digest: int, body: int, epoch: int }
  type Prefix = int -> Entry
  type Prepare = {
    present: bool, op: int, digest: int, body: int, epoch: int, revision: int,
    primary: int, seq: int, base: int, target: int, receipts: Set[int], prefix: Prefix,
  }
  type Permit = {
    present: bool, op: int, digest: int, epoch: int, revision: int,
    primary: int, seq: int, voters: Set[int], recovery: Set[int],
  }
  type Commit = {
    present: bool, op: int, digest: int, body: int, epoch: int, revision: int,
    primary: int, seq: int, base: int, target: int, members: Set[int],
    admittedEpoch: int, admittedRevision: int, admittedPrimary: int, admittedFrozen: bool, admittedPending: bool,
    receipts: Set[int], acks: Set[int], voters: Set[int], recovery: Set[int], path: int,
  }
  pure val EmptyEntry: Entry = { kind: 0, op: 0, digest: 0, body: 0, epoch: 0 }
  pure val EmptyPrefix: Prefix = Sequences.mapBy(_ => EmptyEntry)
  pure val EmptyPrepare: Prepare = {
    present: false, op: 0, digest: 0, body: 0, epoch: 0, revision: 0,
    primary: 0, seq: 0, base: 0, target: 0, receipts: Set(), prefix: EmptyPrefix,
  }
  pure val EmptyPermit: Permit = {
    present: false, op: 0, digest: 0, epoch: 0, revision: 0,
    primary: 0, seq: 0, voters: Set(), recovery: Set(),
  }
  pure val EmptyCommit: Commit = {
    present: false, op: 0, digest: 0, body: 0, epoch: 0, revision: 0,
    primary: 0, seq: 0, base: 0, target: 0, members: Set(), admittedEpoch: 0, admittedRevision: 0, admittedPrimary: 0, admittedFrozen: false, admittedPending: false,
    receipts: Set(), acks: Set(), voters: Set(), recovery: Set(), path: 0,
  }
  type Profile = {
    nodes: Set[int], voters: Set[int], initialMembers: Set[int],
    replacementMembers: Set[int], contentReplicas: Set[int], requiredW: int, metadataOnlyOperations: Set[int],
  }
  pure val defaultProfile: Profile = {
    nodes: Nodes, voters: Voters, initialMembers: InitialMembers,
    replacementMembers: ReplacementMembers, contentReplicas: Nodes, requiredW: W, metadataOnlyOperations: Set(),
  }
  pure def profileValid(profile: Profile): bool = all {
    profile.nodes.subseteq(Nodes), profile.nodes.size() > 0,
    profile.voters.subseteq(profile.nodes), profile.voters.size() > 0,
    profile.initialMembers.subseteq(profile.nodes), profile.initialMembers.contains(1),
    profile.replacementMembers.subseteq(profile.nodes), profile.replacementMembers.contains(1),
    profile.contentReplicas.subseteq(profile.nodes), profile.contentReplicas.size() > 0,
    profile.requiredW >= 1, profile.requiredW <= profile.contentReplicas.size(),
    profile.metadataOnlyOperations.subseteq(Operations),
  }
  type State = {
    primary: int, epoch: int, revision: int, profile: Profile, members: Set[int], online: Set[int],
    bound: int -> int, head: int, journal: Prefix, logs: int -> Prefix,
    heads: int -> int, replicaRead: int -> int, replicaFreeze: int -> int, replicaIngestRead: int -> int,
    prepares: int -> Prepare, receipts: int -> Set[int],
    acks: int -> Set[int], permits: int -> Permit, commits: int -> Commit,
    visible: int, visibleBodies: int -> int, pending: bool, observedPending: bool, frozen: bool,
    desiredPrimary: int, desiredMembers: Set[int], capturedFloor: int, capturedHead: int, pendingJournalHead: int,
    raftWatermarkHead: int, raftWatermarkPrefix: Prefix, capturedWatermarkPrefix: Prefix,
    permitFloor: int, frozenHead: int, frozenPrefix: Prefix,
    handoffAcks: Set[int], promotionMarker: bool, activated: bool,
    lostReply: Set[int], replayed: Set[int], changedRetryRejected: bool,
    minorityRejected: bool, staleEpochRejected: bool, partialActivationRejected: bool,
    guardOp: int, guardPath: int, guardAcks: Set[int], guardPermit: Permit, guardPending: bool, guardPriorBound: int,
    cachedPermitHandoffRejected: bool, wrongDigestRejected: bool, divergentPrefixRejected: bool,
    lateWriteCaptured: bool, activationReplyLost: bool, last: str,
  }
  pure val initial: State = {
    primary: 1, epoch: 1, revision: 1, profile: defaultProfile, members: InitialMembers, online: Nodes,
    bound: Nodes.mapBy(n => if (n == 1) 1 else 0), head: 0, journal: EmptyPrefix,
    logs: Nodes.mapBy(_ => EmptyPrefix), heads: Nodes.mapBy(_ => 0),
    replicaRead: Nodes.mapBy(_ => 0), replicaFreeze: Nodes.mapBy(_ => 0), replicaIngestRead: Nodes.mapBy(_ => 0),
    prepares: Operations.mapBy(_ => EmptyPrepare), receipts: Operations.mapBy(_ => Set()),
    acks: Operations.mapBy(_ => Set()), permits: Operations.mapBy(_ => EmptyPermit),
    commits: Operations.mapBy(_ => EmptyCommit), visible: 0,
    visibleBodies: Operations.mapBy(_ => 0), pending: false, observedPending: false, frozen: false,
    desiredPrimary: 1, desiredMembers: InitialMembers, capturedFloor: 0, capturedHead: 0, pendingJournalHead: 0,
    raftWatermarkHead: 0, raftWatermarkPrefix: EmptyPrefix, capturedWatermarkPrefix: EmptyPrefix,
    permitFloor: 0, frozenHead: 0, frozenPrefix: EmptyPrefix,
    handoffAcks: Set(), promotionMarker: false, activated: false,
    lostReply: Set(), replayed: Set(), changedRetryRejected: false,
    minorityRejected: false, staleEpochRejected: false, partialActivationRejected: false,
    guardOp: 0, guardPath: 0, guardAcks: Set(), guardPermit: EmptyPermit, guardPending: false, guardPriorBound: 0,
    cachedPermitHandoffRejected: false, wrongDigestRejected: false, divergentPrefixRejected: false,
    lateWriteCaptured: false, activationReplyLost: false, last: "init",
  }
  var s: State
  pure def quorum(members: Set[int]): int = members.size() / 2 + 1
  /// Immutable body classification changes only content W, never metadata authority.
  def contentW(op: int): int = if (s.profile.metadataOnlyOperations.contains(op)) 0 else s.profile.requiredW
  def contentReceiptsValid(op: int, receipts: Set[int]): bool =
    if (contentW(op) == 0) receipts == Set()
    else all { receipts.size() >= contentW(op), receipts.subseteq(s.profile.contentReplicas) }
  pure def samePrefix(actual: Prefix, expected: Prefix, through: int): bool =
    Sequences.filter(i => i <= through).forall(i => actual.get(i) == expected.get(i))
  pure def digest(op: int, body: int): int = op * 10 + body
  def writable: bool = all {
    s.online.contains(s.primary), s.bound.get(s.primary) == s.epoch,
    not(s.observedPending), not(s.frozen) or s.activated,
  }
  /// Hidden preparation owns storage but does not mint effect authority. An
  /// unbound backend may prepare; a retained stale binding remains fenced.
  def preparationWritable: bool = all {
    s.online.contains(s.primary), s.bound.get(s.primary) == 0 or s.bound.get(s.primary) == s.epoch,
    not(s.observedPending), not(s.frozen) or s.activated,
  }
  def current(p: Prepare): bool = all {
    p.present, p.primary == s.primary, p.epoch == s.epoch, p.revision == s.revision,
    p.base == s.visible, p.target == s.visible + 1,
    p.digest == digest(p.op, p.body), samePrefix(s.journal, p.prefix, p.seq),
  }
  def matchingPermit(op: int): bool = {
    val p = s.prepares.get(op)
    val permit = s.permits.get(op)
    all {
      permit.present, permit.op == p.op, permit.digest == p.digest,
      permit.seq == p.seq, permit.epoch == p.epoch, permit.revision == p.revision,
      permit.primary == p.primary, permit.voters.subseteq(s.profile.voters),
      permit.voters.size() >= quorum(s.profile.voters), permit.recovery.subseteq(s.members),
      permit.recovery.size() >= quorum(s.members), permit.recovery.contains(s.primary),
    }
  }
  def canCommit(op: int): bool = {
    val p = s.prepares.get(op)
    all {
      preparationWritable, not(s.pending), s.guardOp == 0, current(p), contentReceiptsValid(op, p.receipts),
      s.acks.get(op) == s.members or matchingPermit(op),
    }
  }
  def jointProof: bool = all {
    s.handoffAcks.subseteq(s.profile.initialMembers.union(s.desiredMembers)),
    s.handoffAcks.contains(1), s.handoffAcks.contains(s.desiredPrimary),
    s.handoffAcks.intersect(s.profile.initialMembers).size() >= quorum(s.profile.initialMembers),
    s.handoffAcks.intersect(s.desiredMembers).size() >= quorum(s.desiredMembers),
  }
  action init = s' = initial
  /// Native replay supplies a certified immutable profile, including exact voters.
  /// Node 1 names the retiring primary by an injective dictionary, not a new node.
  action initFromProfile(profile: Profile): bool = all {
    profileValid(profile),
    s' = initial.with("profile", profile).with("members", profile.initialMembers)
      .with("desiredMembers", profile.initialMembers).with("online", profile.nodes)
      .with("bound", profile.nodes.mapBy(n => if (n == 1) 1 else 0))
      .with("logs", profile.nodes.mapBy(_ => EmptyPrefix))
      .with("heads", profile.nodes.mapBy(_ => 0))
      .with("replicaRead", profile.nodes.mapBy(_ => 0))
      .with("replicaFreeze", profile.nodes.mapBy(_ => 0))
      .with("replicaIngestRead", profile.nodes.mapBy(_ => 0)),
  }
  /// Refinement may begin after real content staging. These sets and writer
  /// bindings come from the certified held cut; no prepared operation or
  /// unresolved handoff is omitted by this initialization boundary.
  action initFromCertifiedCut(profile: Profile, receipts: int -> Set[int], bound: int -> int, online: Set[int]): bool = all {
    profileValid(profile), online.subseteq(profile.nodes), online.contains(1),
    Operations.forall(op => receipts.get(op).subseteq(profile.contentReplicas)
      and (not(profile.metadataOnlyOperations.contains(op)) or receipts.get(op) == Set())),
    profile.nodes.forall(n => bound.get(n) == 0 or bound.get(n) == 1),
    s' = initial.with("profile", profile).with("members", profile.initialMembers)
      .with("desiredMembers", profile.initialMembers).with("online", online)
      .with("bound", bound).with("receipts", receipts)
      .with("logs", profile.nodes.mapBy(_ => EmptyPrefix))
      .with("heads", profile.nodes.mapBy(_ => 0))
      .with("replicaRead", profile.nodes.mapBy(_ => 0))
      .with("replicaFreeze", profile.nodes.mapBy(_ => 0))
      .with("replicaIngestRead", profile.nodes.mapBy(_ => 0)),
  }
  action changeConnectivity(nodes: Set[int]): bool = all {
    nodes.subseteq(s.profile.nodes),
    s' = s.with("online", nodes).with("last", "connectivity"),
  }
  /// Exact tuple durability receipt; content W does not supply metadata authority.
  action stage(op: int, node: int): bool = all {
    contentW(op) > 0, s.online.contains(node), s.profile.contentReplicas.contains(node), not(s.prepares.get(op).present),
    s' = s.with("receipts", s.receipts.set(op, s.receipts.get(op).union(Set(node))))
      .with("last", "content-receipt"),
  }
  /// Complete hidden payload is validated and durable before its prefix may ACK.
  action prepare(op: int): bool = all {
    preparationWritable, not(s.prepares.get(op).present), not(s.commits.get(op).present),
    s.head < 6, contentReceiptsValid(op, s.receipts.get(op)),
    val entry: Entry = { kind: 1, op: op, digest: digest(op, 1), body: 1, epoch: s.epoch }
    val prefix = s.journal.set(s.head + 1, entry)
    val p: Prepare = {
      present: true, op: op, digest: digest(op, 1), body: 1, epoch: s.epoch,
      revision: s.revision, primary: s.primary, seq: s.head + 1,
      base: s.visible, target: s.visible + 1, receipts: s.receipts.get(op), prefix: prefix,
    }
    s' = s.with("head", s.head + 1).with("journal", prefix)
      .with("logs", s.logs.set(s.primary, prefix)).with("heads", s.heads.set(s.primary, s.head + 1))
      .with("prepares", s.prepares.set(op, p)).with("acks", s.acks.set(op, Set(s.primary)))
      .with("last", "prepare-payload"),
  }
  /// Receiver durability and source acknowledgement are separate observations.
  /// Only an authenticated contiguous prefix of the existing native journal is
  /// copied; receipt W and source author permission do not change here.
  /// This is a normal read observation, never publication authority. Zero is
  /// Unknown; 1 is actual absence, 2 is a present marker admitted by the exact
  /// observed ingest permit, and 3 is a present marker rejected by that permit.
  /// Native refinement derives this classification from complete marker/permit
  /// bytes and links read to ingest under the SAME held writer and admission.
  action observeReplicaFreezeRead(node: int, read: int, freeze: int): bool = all {
    s.profile.nodes.contains(node), read > s.replicaRead.get(node),
    freeze >= 1, freeze <= 3,
    s' = s.with("replicaRead", s.replicaRead.set(node, read))
      .with("replicaFreeze", s.replicaFreeze.set(node, freeze))
      .with("last", "replica-freeze-read"),
  }
  action ingestPrefix(node: int, through: int): bool = all {
    s.replicaRead.get(node) > s.replicaIngestRead.get(node),
    s.replicaFreeze.get(node) == 1 or s.replicaFreeze.get(node) == 2,
    s.online.contains(node), s.online.contains(s.primary),
    s.members.union(if (s.pending) s.desiredMembers else Set()).contains(node),
    through >= s.heads.get(node), through <= s.head,
    samePrefix(s.logs.get(node), s.journal, s.heads.get(node)),
    val prefix = Sequences.mapBy(i => if (i <= through) s.journal.get(i) else EmptyEntry)
    s' = s.with("logs", s.logs.set(node, prefix)).with("heads", s.heads.set(node, through))
      .with("replicaIngestRead", s.replicaIngestRead.set(node, s.replicaRead.get(node)))
      .with("last", "replica-prefix-durable"),
  }
  /// M completes the tables for already accepted R durability. It consumes no
  /// new read, creates no ACK, and cannot advance the durable receiver prefix.
  action materializeReplicaPrefix(node: int, through: int): bool = all {
    s.profile.nodes.contains(node), through == s.heads.get(node),
    s.replicaIngestRead.get(node) > 0,
    s' = s.with("last", "replica-prefix-materialized"),
  }
  /// Authenticated native ingest, contiguous exact source prefix, then durable ACK.
  action acknowledgePrepare(op: int, node: int): bool = {
    val p = s.prepares.get(op)
    all {
      current(p), s.members.contains(node), s.online.contains(node),
      s.online.contains(s.primary), s.heads.get(node) <= s.head,
      samePrefix(s.journal, p.prefix, p.seq),
      s' = s.with("logs", s.logs.set(node, s.journal)).with("heads", s.heads.set(node, s.head))
        .with("acks", s.acks.set(op, s.acks.get(op).union(Set(node))))
        .with("last", "prepare-prefix-ack"),
    }
  }
  /// Abstract native Raft result: exact operation permit, never DFS election.
  action issuePermit(op: int): bool = {
    val p = s.prepares.get(op)
    val recovery = s.acks.get(op).intersect(s.online)
    val voters = s.profile.voters.intersect(s.online)
    val permit: Permit = {
      present: true, op: op, digest: p.digest, epoch: p.epoch, revision: p.revision,
      primary: p.primary, seq: p.seq, voters: voters, recovery: recovery,
    }
    all {
      current(p), not(s.pending), s.acks.get(op) != s.members,
      recovery.size() >= quorum(s.members), recovery.contains(s.primary),
      voters.size() >= quorum(s.profile.voters), p.seq >= s.permitFloor,
      s' = s.with("permits", s.permits.set(op, permit)).with("permitFloor", p.seq)
        .with("raftWatermarkHead", p.seq).with("raftWatermarkPrefix", p.prefix)
        .with("last", "native-raft-permit"),
    }
  }
  /// Real PublicationGuard spans authoritative exact-key admission and publication.
  /// A pending record excludes new admission on either authority path. A pending
  /// Raft decision may race an already admitted writer; freeze waits for release.
  action admitCommit(op: int): bool = all {
    canCommit(op),
    val full = s.acks.get(op) == s.members
    s' = s.with("guardOp", op).with("guardPath", if (full) 1 else 2).with("guardPending", s.pending)
      .with("guardPriorBound", s.bound.get(s.primary)).with("bound", s.bound.set(s.primary, s.epoch))
      .with("guardAcks", if (full) s.acks.get(op) else Set())
      .with("guardPermit", if (full) EmptyPermit else s.permits.get(op))
      .with("last", "publication-guard-admitted"),
  }
  /// Digest names the exact durable hidden payload. Namespace visibility is atomic.
  action commit(op: int, suppliedDigest: int, loseReply: bool): bool = {
    val p = s.prepares.get(op)
    val full = s.guardPath == 1
    val permit = s.guardPermit
    val record: Commit = {
      present: true, op: op, digest: p.digest, body: p.body, epoch: p.epoch,
      revision: p.revision, primary: p.primary, seq: p.seq, base: p.base, target: p.target,
      members: s.members, receipts: p.receipts,
      admittedEpoch: s.epoch, admittedRevision: s.revision, admittedPrimary: s.primary,
      admittedFrozen: s.frozen and not(s.activated), admittedPending: s.guardPending,
      acks: if (full) s.guardAcks else Set(),
      voters: if (full) Set() else permit.voters,
      recovery: if (full) Set() else permit.recovery, path: if (full) 1 else 2,
    }
    val entry: Entry = { kind: 2, op: op, digest: suppliedDigest, body: 0, epoch: s.epoch }
    val prefix = s.journal.set(s.head + 1, entry)
    all {
      suppliedDigest == p.digest, s.guardOp == op, writable, current(p),
      not(s.commits.get(op).present), s.head < 6,
      s' = s.with("head", s.head + 1).with("journal", prefix)
        .with("logs", s.logs.set(s.primary, prefix)).with("heads", s.heads.set(s.primary, s.head + 1))
        .with("commits", s.commits.set(op, record)).with("visible", p.target)
        .with("visibleBodies", s.visibleBodies.set(op, p.body))
        .with("lostReply", if (loseReply) s.lostReply.union(Set(op)) else s.lostReply)
        .with("last", if (loseReply) "commit-unknown-reply" else "commit-known"),
    }
  }
  /// Delivery observations grant no effect or authority; an unknown reply can
  /// precede a later durable effect retained by its actual native owner.
  action observeReply(op: int, unknown: bool): bool = all {
    Operations.contains(op),
    s' = s.with("lostReply", if (unknown) s.lostReply.union(Set(op)) else s.lostReply)
      .with("last", "reply-observed"),
  }
  action observeActivationReply(unknown: bool): bool = all {
    s.activated,
    s' = s.with("activationReplyLost", s.activationReplyLost or unknown)
      .with("last", "activation-reply-observed"),
  }
  action releasePublicationGuard(op: int): bool = all {
    s.guardOp == op,
    s' = s.with("guardOp", 0).with("bound", s.bound.set(s.primary, s.guardPriorBound)).with("last", "publication-guard-released"),
  }
  action replay(op: int, suppliedDigest: int): bool = all {
    s.commits.get(op).present, suppliedDigest == s.commits.get(op).digest,
    s' = s.with("replayed", s.replayed.union(Set(op))).with("last", "exact-replay"),
  }
  action rejectChangedRetry(op: int): bool = all {
    s.prepares.get(op).present,
    digest(op, 2) != s.prepares.get(op).digest,
    s' = s.with("changedRetryRejected", true).with("last", "changed-payload-rejected"),
  }
  action rejectMinority(op: int): bool = all {
    current(s.prepares.get(op)), contentReceiptsValid(op, s.prepares.get(op).receipts),
    not(canCommit(op)), s.acks.get(op).size() < quorum(s.members),
    s' = s.with("minorityRejected", true).with("last", "W-only-rejected"),
  }
  /// Every cause reuses the same native decision/freeze/joint-prefix protocol.
  action beginHandoffTarget(targetPrimary: int, targetMembers: Set[int], event: str): bool = all {
    s.epoch == 1, not(s.pending), not(s.activated),
    targetMembers.subseteq(s.profile.nodes), targetMembers.contains(targetPrimary),
    s.profile.voters.intersect(s.online).size() >= quorum(s.profile.voters),
    s' = s.with("pending", true).with("capturedFloor", s.permitFloor)
      .with("capturedHead", s.raftWatermarkHead).with("capturedWatermarkPrefix", s.raftWatermarkPrefix)
      .with("pendingJournalHead", s.head)
      .with("desiredPrimary", targetPrimary).with("desiredMembers", targetMembers)
      .with("last", event),
  }
  action beginHandoff(promote: bool): bool = all {
    if (promote) s.profile.initialMembers.contains(2)
      else s.profile.replacementMembers != s.members,
    beginHandoffTarget(if (promote) 2 else 1,
      if (promote) s.profile.initialMembers else s.profile.replacementMembers,
      "handoff-pending"),
  }
  /// Same-topology StorageImport is current epoch transition data. Source-cut,
  /// image and pending mapping identity are certified opaque native inputs;
  /// this model contains no format decoder or conversion implementation.
  action beginStorageImport = beginHandoffTarget(s.primary, s.members, "storage-import-pending")
  /// Publication guard serializes this durable freeze with every canonical commit.
  action freezeRetiring = all {
    s.pending, not(s.frozen), s.online.contains(1), s.guardOp == 0,
    s' = s.with("observedPending", true).with("frozen", true)
      .with("frozenHead", s.head).with("frozenPrefix", s.journal)
      .with("lateWriteCaptured", s.head > s.pendingJournalHead)
      .with("last", "retiring-prefix-frozen"),
  }
  /// Desired primary persists promotion identity before its exact-prefix ACK.
  action persistPromotionMarker = all {
    s.frozen, s.desiredPrimary != 1, s.online.contains(s.desiredPrimary),
    s' = s.with("promotionMarker", true).with("last", "promotion-marker"),
  }
  action acknowledgeHandoff(node: int): bool = all {
    s.frozen, s.pending, s.online.contains(node), s.online.contains(1),
    s.profile.initialMembers.union(s.desiredMembers).contains(node),
    node != s.desiredPrimary or s.desiredPrimary == 1 or s.promotionMarker,
    s' = s.with("logs", s.logs.set(node, s.frozenPrefix))
      .with("heads", s.heads.set(node, s.frozenHead))
      .with("handoffAcks", s.handoffAcks.union(Set(node)))
      .with("last", "handoff-prefix-ack"),
  }
  action rejectPartialActivation = all {
    s.frozen, s.pending, not(jointProof),
    s' = s.with("partialActivationRejected", true).with("last", "joint-proof-rejected"),
  }
  action activate(loseReply: bool): bool = all {
    s.pending, s.frozen, jointProof, s.frozenHead >= s.capturedFloor,
    s.frozenHead >= s.capturedHead, samePrefix(s.frozenPrefix, s.capturedWatermarkPrefix, s.capturedHead),
    s.profile.voters.intersect(s.online).size() >= quorum(s.profile.voters),
    s.handoffAcks.forall(n => all {
      s.heads.get(n) == s.frozenHead, samePrefix(s.logs.get(n), s.frozenPrefix, s.frozenHead),
    }),
    s' = s.with("primary", s.desiredPrimary).with("epoch", 2).with("revision", 2)
      .with("members", s.desiredMembers).with("pending", false).with("activated", true)
      .with("activationReplyLost", loseReply).with("observedPending", false)
      .with("last", "handoff-activated"),
  }
  /// Completion alone never changes a process-local writer binding.
  action bindSuccessor = all {
    s.activated, not(s.pending), s.online.contains(s.primary),
    s.heads.get(s.primary) >= s.frozenHead,
    samePrefix(s.logs.get(s.primary), s.frozenPrefix, s.frozenHead),
    s.primary == 1 or s.promotionMarker,
    s' = s.with("bound", s.bound.set(s.primary, s.epoch)).with("last", "successor-bound"),
  }
  action restart(node: int): bool =
    s' = s.with("bound", s.bound.set(node, 0))
      .with("guardOp", if (node == s.primary) 0 else s.guardOp)
      .with("replicaRead", s.replicaRead.set(node, 0))
      .with("replicaFreeze", s.replicaFreeze.set(node, 0))
      .with("replicaIngestRead", s.replicaIngestRead.set(node, 0)).with("last", "restart-unbound")
  action rejectStaleEpoch(op: int): bool = all {
    s.prepares.get(op).present, s.prepares.get(op).epoch != s.epoch,
    not(s.commits.get(op).present), not(canCommit(op)),
    s' = s.with("staleEpochRejected", true).with("last", "stale-epoch-rejected"),
  }
  action rejectCachedPermitAfterHandoff(op: int): bool = all {
    s.pending, s.guardOp == 0, matchingPermit(op),
    s.acks.get(op) != s.members, not(canCommit(op)),
    s' = s.with("cachedPermitHandoffRejected", true).with("last", "cached-permit-handoff-rejected"),
  }
  action rejectWrongCommitDigest(op: int): bool = all {
    s.prepares.get(op).present, digest(op, 2) != s.prepares.get(op).digest,
    s' = s.with("wrongDigestRejected", true).with("last", "commit-digest-rejected"),
  }
  /// A complete divergent native prefix cannot become an authority ACK.
  /// Rejected input is represented explicitly; durable receiver state is unchanged.
  action rejectDivergentPrefix(op: int, node: int): bool = {
    val p = s.prepares.get(op)
    val wrong = p.prefix.set(p.seq, EmptyEntry)
    all {
      p.present, s.members.contains(node), node != s.primary,
      not(samePrefix(wrong, p.prefix, p.seq)),
      s' = s.with("divergentPrefixRejected", true).with("last", "divergent-prefix-rejected"),
    }
  }
  action stutter = s' = s.with("last", "stutter")
  action step = {
    nondet op = Operations.oneOf()
    nondet node = Nodes.oneOf()
    nondet through = Set(0, 1, 2, 3, 4, 5, 6).oneOf()
    nondet connected = Nodes.powerset().oneOf()
    nondet flag = Set(false, true).oneOf()
    nondet readFreeze = Set(1, 2, 3).oneOf()
    any {
      changeConnectivity(connected), stage(op, node), prepare(op),
      observeReplicaFreezeRead(node, s.replicaRead.get(node) + 1, readFreeze),
      ingestPrefix(node, through), materializeReplicaPrefix(node, through),
      acknowledgePrepare(op, node), issuePermit(op), admitCommit(op), commit(op, digest(op, 1), flag),
      replay(op, digest(op, 1)), observeReply(op, flag), observeActivationReply(flag),
      releasePublicationGuard(op), rejectChangedRetry(op), rejectMinority(op),
      beginHandoff(flag), beginStorageImport, freezeRetiring, persistPromotionMarker,
      acknowledgeHandoff(node), rejectPartialActivation, activate(flag), bindSuccessor,
      restart(node), rejectStaleEpoch(op), rejectCachedPermitAfterHandoff(op),
      rejectWrongCommitDigest(op), rejectDivergentPrefix(op, node), stutter,
    }
  }
}
