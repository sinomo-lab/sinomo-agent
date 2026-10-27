/// Small separate C5/C6 companion; completed ACK target inputs stay unchanged.
/// Durable sets represent actual native certificates, not readable tuples.
/// Two origins/roles/generations; prescribed traces, not exhaustive/liveness.
module dfs_repair_terminal {
  import dfs_repair_terminal_contract.* from "../../contracts/dfs/availability_repair_terminal"
  type State = {
    originals: Set[int], coveredOriginals: Set[int], seen: Set[int], required: Set[(int, int)],
    pRead: Set[int], pSync: Set[int], pCert: Set[int], pBody: int -> int, pStore: int -> int,
    cRead: Set[int], cSync: Set[int], cCert: Set[int],
    mPrepared: Set[(int, int)], mRead: Set[(int, int)], mSync: Set[(int, int)], mCert: Set[(int, int)],
    tRead: Set[int], tSync: Set[int], tCert: Set[int], result: int -> int, tStore: int -> int,
    emitted: Set[int], emittedBody: int -> int, emittedResult: int -> int, emittedStore: int -> int,
    observed: Set[int], saved: Set[int], savedCert: Set[int], savedBody: int -> int, savedResult: int -> int, savedStore: int -> int, replies: int,
    stores: int -> int, epoch: int, payload: Set[(int, int)], holds: Set[(int, int, int)],
    contentWrites: int, metadataWrites: int, cacheWrites: int, receiptWrites: int, headWrites: int,
    currentCopy: bool, currentHead: bool, applied: Set[int], withdrawn: bool, deficit: bool,
    cut: int, causalProof: bool, proofCut: int, liveProofStore: int, nativeAuthority: bool,
    unresolved: Set[int], settled: Set[int], child: bool, borrower: bool, forwardBody: bool, forwardStub: bool,
    dep: int, installedHead: bool, recoveryPromise: bool, responsibilityRetired: bool,
    reservations: Set[int], coverageRev: int, rootProofs: Set[int], rootProofCert: Set[int], proofStore: int -> int,
    proofDep: int -> int, proofCoverage: int -> int, proofRoute: int -> int,
    released: Set[(int, int)], transferReleased: bool, lateFacts: int,
    effectAudit: bool, terminalAudit: bool, replyAudit: bool, replayAudit: bool,
    applicationAudit: bool, releaseAudit: bool,
  }
  pure val initial: State = {
    originals: Set(1), coveredOriginals: Set(1), seen: Set(), required: Set(),
    pRead: Set(), pSync: Set(), pCert: Set(), pBody: Origins.mapBy(_ => 0), pStore: Origins.mapBy(_ => 0),
    cRead: Set(), cSync: Set(), cCert: Set(), mPrepared: Set(), mRead: Set(), mSync: Set(), mCert: Set(),
    tRead: Set(), tSync: Set(), tCert: Set(), result: Origins.mapBy(_ => 0), tStore: Origins.mapBy(_ => 0),
    emitted: Set(), emittedBody: Origins.mapBy(_ => 0), emittedResult: Origins.mapBy(_ => 0), emittedStore: Origins.mapBy(_ => 0),
    observed: Set(), saved: Set(), savedCert: Set(), savedBody: Origins.mapBy(_ => 0), savedResult: Origins.mapBy(_ => 0),
    savedStore: Origins.mapBy(_ => 0), replies: 0,
    stores: Nodes.mapBy(_ => 1), epoch: 1, payload: Set((1, 1), (3, 1)), holds: Set((1, 1, 1), (1, 3, 1)),
    contentWrites: 0, metadataWrites: 0, cacheWrites: 0, receiptWrites: 0, headWrites: 0,
    currentCopy: false, currentHead: false, applied: Set(), withdrawn: false, deficit: false,
    cut: 0, causalProof: false, proofCut: -1, liveProofStore: 0, nativeAuthority: true,
    unresolved: Set(1), settled: Set(), child: false, borrower: false, forwardBody: true, forwardStub: true,
    dep: 0, installedHead: false, recoveryPromise: false, responsibilityRetired: false,
    reservations: Set(), coverageRev: 0, rootProofs: Set(), rootProofCert: Set(), proofStore: Nodes.mapBy(_ => 0),
    proofDep: Nodes.mapBy(_ => -1), proofCoverage: Nodes.mapBy(_ => -1), proofRoute: Nodes.mapBy(_ => 0),
    released: Set(), transferReleased: false, lateFacts: 0,
    effectAudit: true, terminalAudit: true, replyAudit: true, replayAudit: true,
    applicationAudit: true, releaseAudit: true,
  }
  var s: State
  action init = s' = initial
  action initAuthoritative = s' = initial.with("required", Set((1, 1), (1, 2)))
  action initSourceOnly = s' = initial.with("holds", Set((1, 1, 1))).with("coveredOriginals", Set())
  def holders(e: int): Set[int] = Nodes.filter(n => all {
    s.holds.contains((e, n, s.stores.get(n))), s.payload.contains((n, s.stores.get(n))),
  })
  def exactPrepared(e: int): bool = all { s.pCert.contains(e), s.pSync.contains(e),
    s.pBody.get(e) == Bodies.get(e), s.pStore.get(e) == s.stores.get(3) }
  def exactContent(e: int): bool = exactPrepared(e) and s.cSync.contains(e) and s.cCert.contains(e)
  def metadataComplete(e: int): bool = s.required.filter(k => k._1 == e).subseteq(s.mSync.intersect(s.mCert))
  def mayPrepare(e: int, body: int): bool = all { s.originals.contains(e), s.coveredOriginals.contains(e),
    body == Bodies.get(e), not(s.seen.contains(e)) }
  action appendPrepared(e: int, body: int): bool = all { mayPrepare(e, body),
    s' = s.with("pRead", s.pRead.union(Set(e))).with("seen", s.seen.union(Set(e)))
      .with("pBody", s.pBody.set(e, body)).with("pStore", s.pStore.set(e, s.stores.get(3))),
  }
  def maySyncPrepared(e: int): bool = all { s.pRead.contains(e), s.pBody.get(e) == Bodies.get(e), s.pStore.get(e) == s.stores.get(3) }
  action syncPrepared(e: int): bool = all { maySyncPrepared(e),
    s' = s.with("pSync", s.pSync.union(Set(e))).with("pCert", s.pCert.union(Set(e))),
  }
  def mayContent(e: int): bool = exactPrepared(e) and not(s.cRead.contains(e))
  def contentCandidate(e: int): State = s.with("cRead", s.cRead.union(Set(e))).with("contentWrites", s.contentWrites + 1)
  action appendContent(e: int): bool = all { mayContent(e),
    s' = contentCandidate(e).with("effectAudit", s.effectAudit and exactPrepared(e)),
  }
  action syncContent(e: int): bool = all { exactPrepared(e), s.cRead.contains(e),
    s' = s.with("cSync", s.cSync.union(Set(e))).with("cCert", s.cCert.union(Set(e)))
      .with("payload", s.payload.union(Set((3, s.stores.get(3))))),
  }
  def mayPrepareRole(e: int, role: int): bool = all { exactContent(e), s.required.contains((e, role)),
    role == 1 or s.mSync.contains((e, 1)), not(s.mPrepared.contains((e, role))) }
  action prepareRole(e: int, role: int): bool = all { mayPrepareRole(e, role),
    s' = s.with("mPrepared", s.mPrepared.union(Set((e, role)))),
  }
  def mayCommitRole(e: int, role: int): bool = all { exactContent(e), s.nativeAuthority,
    s.required.contains((e, role)), s.mPrepared.contains((e, role)), not(s.mRead.contains((e, role))) }
  action appendRoleCommit(e: int, role: int): bool = all { mayCommitRole(e, role),
    s' = s.with("mRead", s.mRead.union(Set((e, role)))).with("metadataWrites", s.metadataWrites + 1)
      .with("effectAudit", s.effectAudit and mayCommitRole(e, role)),
  }
  /// Assumes actual durable exact V7 frame/prefix, not re-encoding a digest.
  action certifyRoleCommit(e: int, role: int): bool = all { s.mRead.contains((e, role)), s.mPrepared.contains((e, role)),
    s' = s.with("mSync", s.mSync.union(Set((e, role)))).with("mCert", s.mCert.union(Set((e, role)))),
  }
  def mayTerminal(e: int, result: int): bool = all { exactContent(e), metadataComplete(e),
    result == Results.get(e), not(s.tRead.contains(e)) }
  def terminalCandidate(e: int, result: int): State = s.with("tRead", s.tRead.union(Set(e)))
    .with("result", s.result.set(e, result)).with("tStore", s.tStore.set(e, s.stores.get(3)))
  action appendTerminal(e: int, result: int): bool = all { mayTerminal(e, result),
    s' = terminalCandidate(e, result).with("terminalAudit", s.terminalAudit and mayTerminal(e, result)),
  }
  def maySyncTerminal(e: int): bool = all { s.tRead.contains(e), exactContent(e), metadataComplete(e),
    s.result.get(e) == Results.get(e), s.tStore.get(e) == s.stores.get(3) }
  action syncTerminal(e: int): bool = all { maySyncTerminal(e),
    s' = s.with("tSync", s.tSync.union(Set(e))).with("tCert", s.tCert.union(Set(e))),
  }
  action crashReceiver(drop: bool): bool = s' = s.with("pRead", if (drop) s.pSync else s.pRead)
    .with("cRead", if (drop) s.cSync else s.cRead).with("mRead", if (drop) s.mSync else s.mRead)
    .with("tRead", if (drop) s.tSync else s.tRead).with("pCert", Set())
    .with("cCert", Set()).with("mCert", Set()).with("tCert", Set())
  /// Assumed native exact-history/namespace recertifier; readable-only suffix is
  /// NOT promoted. Scope is one receiver crash, not arbitrary media rollback.
  action recertifyReceiver = s' = s.with("pCert", s.pSync).with("cCert", s.cSync)
    .with("mCert", s.mSync).with("tCert", s.tSync)
  def historicalAvailable(e: int): bool = s.savedCert.contains(e) or (s.tSync.contains(e) and s.tCert.contains(e))
  def historicalBody(e: int): int = if (s.savedCert.contains(e)) s.savedBody.get(e) else s.pBody.get(e)
  def historicalResult(e: int): int = if (s.savedCert.contains(e)) s.savedResult.get(e) else s.result.get(e)
  def historicalStore(e: int): int = if (s.savedCert.contains(e)) s.savedStore.get(e) else s.tStore.get(e)
  def mayReply(e: int, body: int, result: int): bool = all { historicalAvailable(e),
    body == Bodies.get(e), historicalBody(e) == body, result == Results.get(e), historicalResult(e) == result }
  pure def noEffects(before: State, after: State): bool = all {
    before.contentWrites == after.contentWrites, before.metadataWrites == after.metadataWrites,
    before.cacheWrites == after.cacheWrites, before.receiptWrites == after.receiptWrites,
    before.headWrites == after.headWrites, before.currentCopy == after.currentCopy,
    before.currentHead == after.currentHead, before.deficit == after.deficit,
    before.applied == after.applied, before.unresolved == after.unresolved, before.settled == after.settled,
    before.cut == after.cut, before.withdrawn == after.withdrawn,
    before.holds == after.holds, before.released == after.released, before.dep == after.dep,
    before.reservations == after.reservations, before.responsibilityRetired == after.responsibilityRetired,
  }
  def replyCandidate(e: int): State = s.with("emitted", s.emitted.union(Set(e))).with("replies", s.replies + 1)
    .with("emittedBody", s.emittedBody.set(e, historicalBody(e)))
    .with("emittedResult", s.emittedResult.set(e, historicalResult(e)))
    .with("emittedStore", s.emittedStore.set(e, historicalStore(e)))
  action historicalReply(e: int, body: int, result: int): bool = all { mayReply(e, body, result),
    s' = replyCandidate(e).with("replyAudit", s.replyAudit and mayReply(e, body, result))
      .with("replayAudit", s.replayAudit and noEffects(s, replyCandidate(e))),
  }
  action observeReply(e: int): bool = all { s.emitted.contains(e),
    s' = s.with("observed", s.observed.union(Set(e))),
  }
  action saveReply(e: int): bool = all { s.observed.contains(e),
    s' = s.with("saved", s.saved.union(Set(e))).with("savedCert", s.savedCert.union(Set(e)))
      .with("savedBody", s.savedBody.set(e, s.emittedBody.get(e)))
      .with("savedResult", s.savedResult.set(e, s.emittedResult.get(e))).with("savedStore", s.savedStore.set(e, s.emittedStore.get(e))),
  }
  action crashSource = s' = s.with("observed", Set()).with("causalProof", false)
    .with("savedCert", Set()).with("rootProofCert", Set())
  /// Assumed exact source queue/prefix recertification; retains historical data.
  action recertifySource = s' = s.with("savedCert", s.saved).with("rootProofCert", s.rootProofs)
  /// Independent assumed metadata-owner current-holder/conditional-cut input.
  /// Fresh post-withdrawal revalidation is excluded from this target.
  def mayCausalProof: bool = all { s.nativeAuthority, s.savedCert.contains(1), exactContent(1), not(s.withdrawn) }
  action assumeCausalProof: bool = all { mayCausalProof,
    s' = s.with("causalProof", true).with("proofCut", s.cut).with("liveProofStore", s.stores.get(3)),
  }
  def mayApply: bool = all { s.causalProof, s.nativeAuthority, s.proofCut == s.cut,
    s.liveProofStore == s.stores.get(3), not(s.withdrawn) }
  def applicationCandidate: State = s.with("currentCopy", true).with("currentHead", true)
    .with("applied", s.applied.union(Set(1))).with("cut", s.cut + 1)
    .with("cacheWrites", s.cacheWrites + 1).with("receiptWrites", s.receiptWrites + 1).with("headWrites", s.headWrites + 1)
  action applyCurrent: bool = all { mayApply,
    s' = applicationCandidate.with("applicationAudit", s.applicationAudit and mayApply),
  }
  /// Assumes actual ordered V7 withdrawal, never an absent receipt heuristic.
  action withdraw = s' = s.with("withdrawn", true).with("deficit", true).with("currentCopy", false)
    .with("currentHead", false).with("cut", s.cut + 1).with("metadataWrites", s.metadataWrites + 1)
  /// Independent full-origin custody input, not inferred from these byte sets.
  action sealSecondOriginal: bool = all { s.payload.contains((1, s.stores.get(1))), s.payload.contains((3, s.stores.get(3))),
    s' = s.with("originals", s.originals.union(Set(2))).with("coveredOriginals", s.coveredOriginals.union(Set(2)))
      .with("unresolved", s.unresolved.union(Set(2)))
      .with("holds", s.holds.union(Set((2, 1, s.stores.get(1)), (2, 3, s.stores.get(3))))),
  }
  action installHead = s' = s.with("installedHead", true)
  def successorPayloadReady: bool = s.installedHead and holders(1).exclude(Set(1)).size() > 0
  /// Assumes complete installed original/attempt responsibility, not page ACK.
  action assumeRecoveryPromise: bool = all { successorPayloadReady,
    s' = s.with("recoveryPromise", true),
  }
  action retireResponsibility: bool = all { s.recoveryPromise,
    s' = s.with("responsibilityRetired", true),
  }
  action lateFact: bool = all { s.forwardStub, s' = s.with("lateFacts", s.lateFacts + 1) }
  /// Assumed checked forwarding/body-dependency rebind to survivor custody or
  /// exact saved outcome. Never a proof that no future late fact can arrive.
  def mayDischargeForwardBody: bool = s.savedCert.contains(1) or s.recoveryPromise
  action dischargeForwardBody: bool = all { mayDischargeForwardBody,
    s' = s.with("forwardBody", false).with("dep", s.dep + 1).with("reservations", Set()),
  }
  def mayEnrollChild: bool = holders(1).contains(1) and not(s.originals.contains(2))
  action enrollChild: bool = all { mayEnrollChild,
    s' = s.with("child", true).with("originals", s.originals.union(Set(2)))
      .with("unresolved", s.unresolved.union(Set(2))).with("holds", s.holds.union(Set((2, 1, s.stores.get(1)))))
      .with("dep", s.dep + 1).with("reservations", Set()),
  }
  def mayBorrowBody: bool = holders(1).contains(1) and not(s.borrower)
  action borrowBody: bool = all { mayBorrowBody,
    s' = s.with("borrower", true).with("dep", s.dep + 1).with("reservations", Set()),
  }
  action returnBody: bool = all { s.borrower,
    s' = s.with("borrower", false).with("dep", s.dep + 1).with("reservations", Set()),
  }
  /// Actual independent B bytes/hold certificate is assumed, never a head.
  def mayQualifyB: bool = not(s.released.contains((2, s.stores.get(2))))
  action qualifyBHold: bool = all { mayQualifyB,
    s' = if (holders(1).contains(2)) s else s.with("payload", s.payload.union(Set((2, s.stores.get(2)))))
      .with("holds", s.holds.union(Set((1, 2, s.stores.get(2))))).with("coverageRev", s.coverageRev + 1).with("reservations", Set()),
  }
  def dependenciesReady: bool = all { not(s.borrower), not(s.forwardBody), s.forwardStub,
    not(s.child) or holders(2).contains(1) }
  def settlementReady: bool = s.savedCert.contains(1) and s.applied.contains(1) and dependenciesReady
  action settleOriginal: bool = all { settlementReady,
    s' = s.with("unresolved", s.unresolved.exclude(Set(1))).with("settled", s.settled.union(Set(1))),
  }
  def postHolders(n: int): Set[int] = holders(1).exclude(Set(n))
  def mayReserve(n: int, route: int): bool = all { holders(1).contains(n), s.reservations == Set(), dependenciesReady,
    if (route == TransferRoute) s.responsibilityRetired and postHolders(n).size() >= 2
    else route == SettlementRoute and settlementReady and s.settled.contains(1) and not(s.unresolved.contains(1)),
  }
  /// Native artifact-scoped cross-owner reservation is an explicit oracle.
  action reserveRoot(n: int, route: int): bool = all { mayReserve(n, route),
    s' = s.with("reservations", Set(n)).with("proofStore", s.proofStore.set(n, s.stores.get(n)))
      .with("proofDep", s.proofDep.set(n, s.dep)).with("proofCoverage", s.proofCoverage.set(n, s.coverageRev))
      .with("proofRoute", s.proofRoute.set(n, route)).with("rootProofs", s.rootProofs.exclude(Set(n)))
      .with("rootProofCert", s.rootProofCert.exclude(Set(n))),
  }
  action persistRootProof(n: int): bool = all { s.reservations == Set(n),
    s' = s.with("rootProofs", s.rootProofs.union(Set(n))).with("rootProofCert", s.rootProofCert.union(Set(n))),
  }
  def mayRelease(n: int): bool = all { s.rootProofCert.contains(n), s.rootProofs.contains(n), s.reservations == Set(n),
    s.proofStore.get(n) == s.stores.get(n), holders(1).contains(n),
    s.proofDep.get(n) == s.dep, s.proofCoverage.get(n) == s.coverageRev, dependenciesReady,
    if (s.proofRoute.get(n) == TransferRoute) postHolders(n).size() >= 2
    else s.proofRoute.get(n) == SettlementRoute and settlementReady and s.settled.contains(1) and not(s.unresolved.contains(1)),
  }
  def releaseCandidate(n: int): State = s.with("holds", s.holds.exclude(Set((1, n, s.stores.get(n)))))
    .with("released", s.released.union(Set((n, s.stores.get(n))))).with("reservations", Set())
    .with("coverageRev", s.coverageRev + 1)
    .with("transferReleased", s.transferReleased or s.proofRoute.get(n) == TransferRoute)
  def mayReplayRelease(n: int): bool = all { s.released.contains((n, s.stores.get(n))),
    s.rootProofCert.contains(n), s.rootProofs.contains(n), s.proofStore.get(n) == s.stores.get(n) }
  action releaseRoot(n: int): bool = all { mayRelease(n) or mayReplayRelease(n),
    s' = if (mayReplayRelease(n)) s else releaseCandidate(n).with("releaseAudit", s.releaseAudit and mayRelease(n)),
  }
  action servingRebind = s' = s.with("epoch", 2).with("causalProof", false)
  /// Explicit controlled replacement revokes pending proofs, never membership.
  /// Retained source history is separate; a new empty receiver has no Terminal.
  def mayReplace(n: int): bool = all {
    s.stores.get(n) == 1,
    not(s.transferReleased) or holders(1).exclude(Set(n)).size() >= 2,
    n != 1 or all { not(s.borrower), not(s.forwardBody), not(s.child),
      s.unresolved.forall(e => holders(e).exclude(Set(1)).size() > 0) },
  }
  action replaceStore(n: int): bool = all { mayReplace(n),
    s' = s.with("stores", s.stores.set(n, 2)).with("coverageRev", s.coverageRev + 1)
      .with("reservations", Set()).with("causalProof", false)
      .with("pRead", if (n == 3) Set() else s.pRead).with("pSync", if (n == 3) Set() else s.pSync)
      .with("pCert", if (n == 3) Set() else s.pCert).with("cRead", if (n == 3) Set() else s.cRead)
      .with("pBody", if (n == 3) Origins.mapBy(_ => 0) else s.pBody)
      .with("pStore", if (n == 3) Origins.mapBy(_ => 0) else s.pStore)
      .with("cSync", if (n == 3) Set() else s.cSync).with("cCert", if (n == 3) Set() else s.cCert)
      .with("tRead", if (n == 3) Set() else s.tRead).with("tSync", if (n == 3) Set() else s.tSync)
      .with("tCert", if (n == 3) Set() else s.tCert)
      .with("result", if (n == 3) Origins.mapBy(_ => 0) else s.result)
      .with("tStore", if (n == 3) Origins.mapBy(_ => 0) else s.tStore),
  }
}
