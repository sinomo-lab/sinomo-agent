/// DRAFT custody/compaction TARGET; no production authority or second queue.
/// Two original effects, three custody stores, two attempts per effect, one
/// shared input, exact per-effect results and two indexed pages. Effect 2 is
/// admitted once as a dependent Repair; dependency generation ranges 0..2.
/// Certificate actions assume a reviewed DFS recovery certifier, not an election
/// or a replacement of FullShard. Two complete durable copies model survival of
/// one unavailable store. Later recovery needs one reachable exact holder.
/// Persistence actions are atomic fsync abstractions. Receiver settlement and
/// duplicate suppression are assumed effect-specific facts, not current Rust.
/// parentW1Done assumes an external completed parent write; no W1 receipt,
/// canonical publication or object-holder state is modeled by that marker.
/// Companion copy slice permits one authoritative withdrawal/effect. Exact
/// application/withdrawal authority are assumed; no fresh holder revalidation.
/// No process restart, codec, multi-slot order, partition fencing or liveness.
module dfs_availability_effect_custody {
  import dfs_availability_effect_custody_contract.* from "../../contracts/dfs/availability_effect_custody"
  type State = {
    live: Set[int], registered: Set[int], kinds: int -> int, modes: int -> int,
    heads: (int, int) -> int, payload: Set[int], roots: Set[(int, int)],
    custody: Set[int], claims: Set[(int, int)], sent: Set[(int, int)],
    notSent: Set[(int, int)], uncertain: Set[(int, int)], rejectedAttempts: Set[(int, int)],
    certifiedBeforeSend: Set[int], applied: int -> int, receiverTerminal: Set[int], receiverOutcomes: int -> int,
    lostReplies: Set[int], localResults: Set[int], observedOutcomes: int -> int,
    resultBytes: (int, int) -> int, resultOutcomes: (int, int) -> int,
    terminalWitnesses: Set[(int, int)], terminal: Set[int], localApplied: Set[int], compacted: Set[int],
    retirement: Set[int], retired: Set[int], replayed: Set[int], rejectedOriginals: Set[int],
    ownerLost: bool, recoveredAfterLoss: Set[int], parentW1Done: bool,
    payloadUsers: Set[int], bodyUsers: Set[int], generation: int,
    checkedGeneration: int, payloadReleased: bool, releaseWasFresh: bool,
    revision: int, cutRevision: int, cutGeneration: int, cutRows: Set[int],
    pageCertificates: Set[int], loadedPages: Set[int], activationGate: bool,
    currentReceipt: Set[int], knownWithdrawal: Set[int], deficit: Set[int], localClosedBirth: Set[int],
  }
  pure val initial: State = {
    live: Stores, registered: Set(1), kinds: Map(1 -> Stage, 2 -> Repair),
    modes: Map(1 -> Attached, 2 -> Attached),
    heads: Keys.mapBy(k => if (k == (1, 1)) FullExact else Absent),
    payload: Set(1), roots: Set((1, 1)), custody: Set(),
    claims: Set(), sent: Set(), notSent: Set(), uncertain: Set(), rejectedAttempts: Set(),
    certifiedBeforeSend: Set(), applied: Effects.mapBy(_ => 0), receiverTerminal: Set(), receiverOutcomes: Effects.mapBy(_ => 0),
    lostReplies: Set(), localResults: Set(), observedOutcomes: Effects.mapBy(_ => 0),
    resultBytes: Keys.mapBy(_ => Absent), resultOutcomes: Keys.mapBy(_ => 0),
    terminalWitnesses: Set(), terminal: Set(), localApplied: Set(), compacted: Set(),
    retirement: Set(), retired: Set(), replayed: Set(), rejectedOriginals: Set(),
    ownerLost: false, recoveredAfterLoss: Set(), parentW1Done: false,
    payloadUsers: Set(), bodyUsers: Set(), generation: 0,
    checkedGeneration: -1, payloadReleased: false, releaseWasFresh: true,
    revision: 0, cutRevision: -1, cutGeneration: -1, cutRows: Set(),
    pageCertificates: Set(), loadedPages: Set(), activationGate: false,
    currentReceipt: Set(), knownWithdrawal: Set(), deficit: Set(), localClosedBirth: Set(),
  }
  var s: State
  /// A changed durable obligation invalidates this bounded gate; no lease or
  /// continuity protocol is silently assumed for work admitted after a cut.
  pure def revised(candidate: State): State = candidate.with("revision", candidate.revision + 1)
    .with("activationGate", false)
  def fullHead(e: int, n: int): bool = s.heads.get((e, n)) == FullExact
  def completeAt(e: int, n: int): bool = all {
    fullHead(e, n), not(needsAttachment(s.modes.get(e))) or s.payload.contains(n),
  }
  def completeHolders(e: int): Set[int] = Stores.filter(n => s.roots.contains((e, n)) and completeAt(e, n))
  def bodyReachable(e: int): bool = completeHolders(e).intersect(s.live).size() > 0
  def headReachable(e: int): bool = s.live.exists(n => fullHead(e, n))
  def exactTerminalReachable(e: int): bool = s.live.exists(n => all {
    s.terminalWitnesses.contains((e, n)), s.resultBytes.get((e, n)) == FullExact,
    settles(s.kinds.get(e), s.resultOutcomes.get((e, n))),
    s.resultOutcomes.get((e, n)) == s.receiverOutcomes.get(e),
  })
  def unresolved(e: int): bool = s.claims.exists(pair => pair._1 == e and not(s.notSent.contains(pair)))
    and not(s.terminal.contains(e))
  def originalCovered(e: int): bool = if (s.retired.contains(e)) s.retirement.contains(e)
    else if (s.terminal.contains(e)) all { exactTerminalReachable(e), s.localApplied.contains(e) or headReachable(e) }
    else all { headReachable(e), not(unresolved(e)) or all { s.custody.contains(e), bodyReachable(e) } }
  def custodyReady(e: int): bool = all { s.custody.contains(e), bodyReachable(e) }
  def canActivate: bool = all {
    s.cutRevision == s.revision, s.cutGeneration == s.generation,
    s.cutRows == s.registered, Pages.subseteq(s.pageCertificates),
    s.cutRows.forall(e => originalCovered(e)),
  }
  action init = s' = initial
  action initRepair = s' = initial.with("kinds", initial.kinds.set(1, Repair))
  action initInline = s' = initial.with("modes", initial.modes.set(1, Inline))
  action initCommit = s' = initial.with("kinds", initial.kinds.set(1, Commit))
    .with("modes", initial.modes.set(1, NoPayload))
  action persistHead(e: int, n: int, quality: int): bool = all {
    s.registered.contains(e), s.live.contains(n), Set(DigestOnly, FullExact).contains(quality),
    quality >= s.heads.get((e, n)), not(s.compacted.contains(e)),
    s' = s.with("heads", s.heads.set((e, n), quality)),
  }
  action persistPayload(n: int): bool = all {
    s.live.contains(n), not(s.payloadReleased), s' = s.with("payload", s.payload.union(Set(n))),
  }
  action bindOriginal(e: int, n: int): bool = all {
    s.registered.contains(e), s.live.contains(n), completeAt(e, n),
    s' = revised(s.with("roots", s.roots.union(Set((e, n))))),
  }
  /// Assumed valid recovery certificate over exact roots. No serving authority.
  action certifyCustody(e: int): bool = all {
    s.registered.contains(e), completeHolders(e).size() >= 2,
    s' = revised(s.with("custody", s.custody.union(Set(e)))),
  }
  /// Conservative durable uncertainty is installed BEFORE carrier dispatch.
  action arm(e: int, attempt: int): bool = all {
    s.registered.contains(e), Attempts.contains(attempt), custodyReady(e),
    not(s.terminal.contains(e)), not(s.claims.contains((e, attempt))),
    s' = revised(s.with("claims", s.claims.union(Set((e, attempt))))
      .with("uncertain", s.uncertain.union(Set((e, attempt))))),
  }
  action dispatch(e: int, attempt: int): bool = all {
    custodyReady(e), s.uncertain.contains((e, attempt)),
    not(s.notSent.contains((e, attempt))), not(s.sent.contains((e, attempt))), not(s.terminal.contains(e)),
    s' = s.with("sent", s.sent.union(Set((e, attempt))))
      .with("certifiedBeforeSend", s.certifiedBeforeSend.union(Set(e))),
  }
  action markDefinitelyUnsent(e: int, attempt: int): bool = all {
    s.claims.contains((e, attempt)), not(s.sent.contains((e, attempt))),
    s' = revised(s.with("notSent", s.notSent.union(Set((e, attempt))))
      .with("uncertain", s.uncertain.exclude(Set((e, attempt))))),
  }
  action rejectLaterEnvelope(e: int, attempt: int): bool = all {
    s.claims.contains((e, attempt)),
    s' = revised(s.with("rejectedAttempts", s.rejectedAttempts.union(Set((e, attempt))))),
  }
  /// The receiver's exact settlement/idempotency contract is ASSUMED here.
  action receiverExecute(e: int, attempt: int): bool = all {
    s.sent.contains((e, attempt)), s.applied.get(e) == 0,
    s' = s.with("applied", s.applied.set(e, 1)).with("receiverTerminal", s.receiverTerminal.union(Set(e)))
      .with("receiverOutcomes", s.receiverOutcomes.set(e, if (s.kinds.get(e) == Stage) Durable else Committed)),
  }
  action dropReply(e: int): bool = all {
    s.receiverTerminal.contains(e), not(s.localResults.contains(e)),
    s' = s.with("lostReplies", s.lostReplies.union(Set(e))),
  }
  action observeExactResult(e: int): bool = all {
    s.receiverTerminal.contains(e), not(s.retired.contains(e)),
    // Exact correlated receiver evidence must be provided by the future protocol.
    s' = s.with("localResults", s.localResults.union(Set(e)))
      .with("observedOutcomes", s.observedOutcomes.set(e, s.receiverOutcomes.get(e)))
      .with("recoveredAfterLoss", if (s.ownerLost) s.recoveredAfterLoss.union(Set(e)) else s.recoveredAfterLoss),
  }
  action observeIntermediateDurable(e: int): bool = all {
    s.sent.exists(pair => pair._1 == e), not(s.terminal.contains(e)),
    s' = s.with("localResults", s.localResults.union(Set(e)))
      .with("observedOutcomes", s.observedOutcomes.set(e, Durable)),
  }
  action persistResult(e: int, n: int, quality: int): bool = all {
    s.localResults.contains(e), s.live.contains(n), Set(DigestOnly, FullExact).contains(quality),
    quality >= s.resultBytes.get((e, n)),
    not(s.terminalWitnesses.contains((e, n))) or s.resultOutcomes.get((e, n)) == s.observedOutcomes.get(e),
    s' = s.with("resultBytes", s.resultBytes.set((e, n), quality))
      .with("resultOutcomes", s.resultOutcomes.set((e, n), s.observedOutcomes.get(e))),
  }
  action persistTerminal(e: int, n: int, kind: int): bool = all {
    s.localResults.contains(e), s.live.contains(n), kind == s.kinds.get(e),
    s.receiverTerminal.contains(e), s.resultBytes.get((e, n)) == FullExact,
    s.resultOutcomes.get((e, n)) == s.observedOutcomes.get(e),
    s.resultOutcomes.get((e, n)) == s.receiverOutcomes.get(e),
    settles(kind, s.resultOutcomes.get((e, n))),
    s' = revised(s.with("terminalWitnesses", s.terminalWitnesses.union(Set((e, n))))),
  }
  action certifyTerminal(e: int): bool = all {
    Stores.filter(n => all { s.terminalWitnesses.contains((e, n)), s.resultBytes.get((e, n)) == FullExact,
      settles(s.kinds.get(e), s.resultOutcomes.get((e, n))),
      s.resultOutcomes.get((e, n)) == s.receiverOutcomes.get(e) }).size() >= 2,
    s' = revised(s.with("terminal", s.terminal.union(Set(e)))
      .with("uncertain", s.uncertain.filter(pair => pair._1 != e))),
  }
  /// Assumes a survivor-recoverable durable completion fact for the admitted
  /// handler, NOT a volatile cache update or an A-only marker. It survives loss
  /// by assumption; its actual certificate/storage protocol remains unmodeled.
  /// Current completion reconstruction needs the original head until this cut.
  action applyLocalOutcome(e: int): bool = all {
    s.terminal.contains(e), exactTerminalReachable(e), headReachable(e),
    s' = revised(s.with("localApplied", s.localApplied.union(Set(e)))),
  }
  /// Bounded companion slice: exact current application and authoritative
  /// withdrawal are assumed native inputs, not a derived consensus protocol.
  /// One withdrawal/effect; no fresh post-withdrawal holder revalidation here.
  /// Historical localApplied stays monotonic when current copies disappear.
  action importCurrentApplication(e: int): bool = all {
    s.terminal.contains(e), s.localApplied.contains(e), exactTerminalReachable(e),
    not(s.knownWithdrawal.contains(e)),
    s' = revised(s.with("currentReceipt", s.currentReceipt.union(Set(e)))
      .with("deficit", s.deficit.exclude(Set(e)))),
  }
  action withdrawCurrentCopy(e: int): bool = all {
    s.currentReceipt.contains(e), not(s.knownWithdrawal.contains(e)),
    s' = revised(s.with("currentReceipt", s.currentReceipt.exclude(Set(e)))
      .with("knownWithdrawal", s.knownWithdrawal.union(Set(e)))
      .with("deficit", s.deficit.union(Set(e)))),
  }
  action recoverStoredPositive(e: int): bool = all {
    s.terminal.contains(e), s.localApplied.contains(e), exactTerminalReachable(e),
    s' = if (s.currentReceipt.contains(e) and not(s.knownWithdrawal.contains(e)))
      revised(s.with("localClosedBirth", s.localClosedBirth.union(Set(e)))) else s,
  }
  action loseOwner = all {
    s.live.contains(1),
    s' = s.with("live", s.live.exclude(Set(1))).with("ownerLost", true)
      .with("localResults", Set()).with("observedOutcomes", Effects.mapBy(_ => 0))
      .with("checkedGeneration", -1).with("activationGate", false),
  }
  action assumeCompletedParentW1 = all {
    s.terminal.contains(1), exactTerminalReachable(1), s' = s.with("parentW1Done", true),
  }
  action admitChild(needsParentHead: bool): bool = all {
    s.live.contains(1), not(s.registered.contains(2)), not(s.payloadReleased), s.generation == 0,
    s.payload.intersect(s.live).size() > 0, not(needsParentHead) or headReachable(1),
    s' = revised(s.with("registered", s.registered.union(Set(2)))
      .with("heads", s.heads.set((2, 1), FullExact))
      .with("payloadUsers", Set(2)).with("bodyUsers", if (needsParentHead) Set(2) else Set())
      .with("generation", 1)),
  }
  action settleChild = all {
    s.terminal.contains(2), s.localApplied.contains(2), s.payloadUsers.contains(2), s.generation == 1,
    s' = revised(s.with("payloadUsers", Set()).with("bodyUsers", Set()).with("generation", 2)),
  }
  action checkCompaction = all {
    s.terminal.contains(1), s.localApplied.contains(1), exactTerminalReachable(1),
    s' = s.with("checkedGeneration", s.generation),
  }
  action releaseParentBody = all {
    s.terminal.contains(1), s.localApplied.contains(1), exactTerminalReachable(1), s.bodyUsers == Set(),
    s.checkedGeneration >= 0, s.checkedGeneration == s.generation,
    s' = revised(s.with("heads", Keys.mapBy(key => if (key._1 == 1) Absent else s.heads.get(key)))
      .with("roots", s.roots.filter(pair => pair._1 != 1)).with("compacted", s.compacted.union(Set(1)))
      .with("releaseWasFresh", s.releaseWasFresh and s.checkedGeneration == s.generation)),
  }
  action releasePayload = all {
    s.terminal.contains(1), s.localApplied.contains(1), exactTerminalReachable(1), s.payloadUsers == Set(),
    s.registered.forall(e => not(needsAttachment(s.modes.get(e))) or s.localApplied.contains(e)),
    s.checkedGeneration >= 0, s.checkedGeneration == s.generation,
    s' = revised(s.with("payload", Set()).with("payloadReleased", true)
      .with("releaseWasFresh", s.releaseWasFresh and s.checkedGeneration == s.generation)),
  }
  /// Retirement is a separate reviewed exact rejection certificate, NEVER age.
  action certifyRetirement(e: int): bool = all {
    s.terminal.contains(e), s.compacted.contains(e), exactTerminalReachable(e),
    s' = revised(s.with("retirement", s.retirement.union(Set(e)))),
  }
  action retireWitness(e: int): bool = all {
    s.retirement.contains(e), s.compacted.contains(e),
    s' = revised(s.with("retired", s.retired.union(Set(e)))
      .with("terminalWitnesses", s.terminalWitnesses.filter(pair => pair._1 != e))
      .with("resultOutcomes", Keys.mapBy(key => if (key._1 == e) 0 else s.resultOutcomes.get(key)))
      .with("resultBytes", Keys.mapBy(key => if (key._1 == e) Absent else s.resultBytes.get(key)))),
  }
  action delayedOriginal(e: int): bool = any {
    all { s.terminal.contains(e), not(s.retired.contains(e)), exactTerminalReachable(e),
      s' = s.with("replayed", s.replayed.union(Set(e))) },
    all { s.retired.contains(e), s.retirement.contains(e),
      s' = s.with("rejectedOriginals", s.rejectedOriginals.union(Set(e))) },
  }
  action captureCut = s' = s.with("cutRevision", s.revision).with("cutGeneration", s.generation)
    .with("cutRows", s.registered).with("pageCertificates", Set()).with("loadedPages", Set())
    .with("activationGate", false)
  action certifyPage(page: int, revision: int): bool = all {
    Pages.contains(page), revision == s.cutRevision, s.cutRevision == s.revision,
    s.cutGeneration == s.generation, s.cutRows == s.registered,
    s.cutRows.filter(e => pageOf(e) == page).forall(e => originalCovered(e)),
    s' = s.with("pageCertificates", s.pageCertificates.union(Set(page))),
  }
  action loadPage(page: int): bool = all {
    s.pageCertificates.contains(page), s' = s.with("loadedPages", s.loadedPages.union(Set(page))),
  }
  /// Only the CUSTODY gate; not actual metadata/cluster authority activation.
  action activateCustodyGate = all { canActivate, s' = s.with("activationGate", true) }
}

/// Narrow ACK/late-fact target, separate from the older custody state above.
/// Two pages/origins, three nonadjacent facts, selected transfer plus altered
/// binding probes. No model/native equivalence, future-closure or liveness.
module dfs_custody_ack {
  import dfs_custody_ack_contract.* from "../../contracts/dfs/availability_effect_custody"
  type AckState = {
    readable: Set[int], leafSynced: Set[int], pageCertified: Set[int], stagedAcks: Set[int],
    stagedEmissions: int, stageEmissionCertified: bool,
    authority: bool, assigned: bool, queueReadable: bool, prefixSynced: bool, queueCertified: bool,
    installedOrigins: Set[int], cursorReadable: bool, cursorCertified: bool,
    installedAckHistory: bool, installedEmissions: int, installEmissionCertified: bool,
    imported: Set[Fact], factCertified: Set[int], frontierSeq: int, frontierDigest: int,
    lateAcks: Set[FactAck], lateEmissionCertified: bool, sourceAcked: Set[int],
    retained: Set[Fact], sourceFrontier: int, sourceFrontierDigest: int, recoveryResponsibility: bool,
    responsibilityRetired: bool, payloadReady: bool, executor: bool, rootReleased: bool,
    futureClosed: bool,
  }
  pure val initialAck: AckState = {
    readable: Set(), leafSynced: Set(), pageCertified: Set(), stagedAcks: Set(), stagedEmissions: 0, stageEmissionCertified: true,
    authority: false, assigned: false, queueReadable: false, prefixSynced: false, queueCertified: false,
    installedOrigins: Set(), cursorReadable: false, cursorCertified: false,
    installedAckHistory: false, installedEmissions: 0, installEmissionCertified: true,
    imported: Set(), factCertified: Set(), frontierSeq: cutSeq, frontierDigest: transfer.inventory,
    lateAcks: Set(), lateEmissionCertified: true, sourceAcked: Set(), retained: Set(), sourceFrontier: cutSeq,
    sourceFrontierDigest: transfer.inventory,
    recoveryResponsibility: false, responsibilityRetired: false, payloadReady: false,
    executor: false, rootReleased: false, futureClosed: false,
  }
  var a: AckState
  action initAck = a' = initialAck
  /// stagePage assumes the exact named page bytes and certified cursor/ancestry.
  action stagePage(p: int): bool = all { pages.contains(p),
    a' = a.with("readable", a.readable.union(Set(p))).with("leafSynced", a.leafSynced.union(Set(p)))
      .with("pageCertified", a.pageCertified.union(Set(p))),
  }
  action replayReadablePage(p: int): bool = all { pages.contains(p),
    a' = a.with("readable", a.readable.union(Set(p))),
  }
  action syncPageLeafOnly(p: int): bool = all { a.readable.contains(p),
    a' = a.with("leafSynced", a.leafSynced.union(Set(p))),
  }
  def stageCertified(p: int): bool = all { a.readable.contains(p), a.leafSynced.contains(p), a.pageCertified.contains(p) }
  action emitStageAck(p: int): bool = all { stageCertified(p),
    a' = a.with("stagedAcks", a.stagedAcks.union(Set(p))).with("stagedEmissions", a.stagedEmissions + 1)
      .with("stageEmissionCertified", a.stageEmissionCertified and stageCertified(p)),
  }
  action assumeAuthority = a' = a.with("authority", true)
  def mayAssign: bool = a.authority and pages.subseteq(a.stagedAcks)
  action assignmentCas: bool = all { mayAssign,
    a' = a.with("assigned", true),
  }
  /// Abstract existing checked rebind + native queue fsync, not an author grant.
  def mayImportQueue: bool = all { a.assigned, a.authority, pages.subseteq(a.stagedAcks) }
  action importQueue: bool = all { mayImportQueue,
    a' = a.with("queueReadable", true).with("prefixSynced", true).with("queueCertified", true).with("installedOrigins", origins),
  }
  action importOneOrigin(e: int): bool = all { a.assigned, a.authority, origins.contains(e),
    a' = a.with("queueReadable", true).with("prefixSynced", true).with("queueCertified", true)
      .with("installedOrigins", a.installedOrigins.union(Set(e))),
  }
  def mayCertifyCursor: bool = a.queueCertified and a.installedOrigins == origins
  action certifyInstalledCursor: bool = all { mayCertifyCursor,
    a' = a.with("cursorReadable", true).with("cursorCertified", true),
  }
  def installationCertified: bool = all { a.assigned, a.authority, a.queueCertified,
    a.prefixSynced, a.cursorCertified, a.installedOrigins == origins }
  action emitInstalledAck: bool = all { installationCertified,
    a' = a.with("installedAckHistory", true).with("installedEmissions", a.installedEmissions + 1)
      .with("installEmissionCertified", a.installEmissionCertified and installationCertified),
  }
  /// Historical evidence stays. Current-session capabilities are discarded.
  action crashReplay = a' = a.with("pageCertified", Set()).with("leafSynced", Set())
    .with("prefixSynced", false).with("queueCertified", false)
    .with("cursorCertified", false).with("factCertified", Set())
  action syncQueuePrefixOnly: bool = all { a.queueReadable, a' = a.with("prefixSynced", true) }
  action recertifyInstallation: bool = all { a.assigned, a.queueReadable, a.cursorReadable,
    a.installedOrigins == origins,
    a' = a.with("prefixSynced", true).with("queueCertified", true).with("cursorCertified", true)
      .with("factCertified", a.imported.map(f => f.seq)),
  }
  /// A's synchronized fact append does not depend on B staging/installation or
  /// retirement. This action assumes actual source sequence + provenance.
  def mayRetain(f: Fact): bool = all { f.transfer == transfer, f.source == transfer.source,
    f.sourceQ == transfer.sourceQ, f.seq > cutSeq,
    a.retained.forall(old => old.seq < f.seq), not(a.futureClosed),
    if (a.retained == Set()) all { f.prevCut, f.prevSeq == cutSeq, f.prevDigest == transfer.inventory }
    else a.retained.exists(prev => all { not(f.prevCut), f.prevSeq == prev.seq, f.prevDigest == prev.digest,
      a.retained.forall(old => old.seq <= prev.seq) }),
  }
  action retainFact(f: Fact): bool = all { mayRetain(f),
    a' = a.with("retained", a.retained.union(Set(f))),
  }
  def bindingValid(f: Fact): bool = all { f.transfer == transfer,
    f.source == transfer.source, f.sourceQ == transfer.sourceQ,
    a.installedOrigins.contains(f.origin), f.originDigest == originDigests.get(f.origin),
    f.digest == f.material + 1000, f.seq > cutSeq,
  }
  def knownFact(f: Fact): bool = a.imported.exists(old => old.seq == f.seq)
  def followsFrontier(f: Fact): bool = all { f.prevSeq == a.frontierSeq, f.prevDigest == a.frontierDigest,
    f.prevCut == (a.frontierSeq == cutSeq), f.seq > a.frontierSeq,
  }
  def mayImport(f: Fact, authenticated: bool): bool = all {
    authenticated, a.retained.contains(f), a.queueCertified, a.cursorCertified,
    a.installedOrigins == origins, bindingValid(f),
    if (knownFact(f)) a.imported.contains(f) else followsFrontier(f),
  }
  action importFact(f: Fact, authenticated: bool): bool = all { mayImport(f, authenticated),
    a' = if (knownFact(f)) a else a.with("imported", a.imported.union(Set(f)))
      .with("factCertified", a.factCertified.union(Set(f.seq)))
      .with("frontierSeq", f.seq).with("frontierDigest", f.digest),
  }
  /// B ACK follows its durable import; a lost reply does not record A's ACK.
  def lateCertified(f: Fact): bool = all { installationCertified, a.imported.contains(f), a.factCertified.contains(f.seq) }
  action emitLateAck(f: Fact): bool = all { lateCertified(f),
    a' = a.with("lateAcks", a.lateAcks.union(Set({ transfer: transfer, seq: f.seq,
      digest: f.digest, installSeq: f.seq + 100 })))
      .with("lateEmissionCertified", a.lateEmissionCertified and lateCertified(f)),
  }
  def mayPersistSourceAck(ack: FactAck): bool = all { ack.transfer == transfer,
    a.lateAcks.contains(ack), a.retained.exists(f => f.seq == ack.seq and f.digest == ack.digest),
    // A advances only a contiguous accepted transfer chain, not max(global seq).
    a.sourceAcked.contains(ack.seq) or a.retained.exists(f => all { f.seq == ack.seq,
      f.prevSeq == a.sourceFrontier, f.prevDigest == a.sourceFrontierDigest,
      f.prevCut == (a.sourceFrontier == cutSeq) }),
  }
  action persistSourceAck(ack: FactAck): bool = all { mayPersistSourceAck(ack),
    a' = if (a.sourceAcked.contains(ack.seq)) a else
      a.with("sourceAcked", a.sourceAcked.union(Set(ack.seq))).with("sourceFrontier", ack.seq)
        .with("sourceFrontierDigest", ack.digest),
  }
  /// Separate assumed all-unresolved-attempt responsibility, never an ACK.
  action assumeRecoveryResponsibility: bool = all { a.installedAckHistory,
    a' = a.with("recoveryResponsibility", true),
  }
  action retireResponsibility: bool = all { a.recoveryResponsibility,
    a' = a.with("responsibilityRetired", true),
  }
}
