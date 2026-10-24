/// Unregistered DRAFT target, outside *_safety.qnt conformance inventory.
module dfs_availability_effect_custody_target {
  import dfs_availability_effect_custody_contract.* from "../../contracts/dfs/availability_effect_custody"
  import dfs_availability_effect_custody.* from "../../models/dfs/availability_effect_custody"
  def dispatchCovered: bool = s.sent.forall(pair => s.certifiedBeforeSend.contains(pair._1))
  def unresolvedRetained: bool = s.registered.forall(e => unresolved(e) implies all {
    s.custody.contains(e), bodyReachable(e),
    s.claims.filter(pair => pair._1 == e and not(s.notSent.contains(pair))).subseteq(s.uncertain),
  })
  def terminalMaterial: bool = s.terminal.forall(e => s.retired.contains(e) or exactTerminalReachable(e))
  def localApplicationRetained: bool = s.terminal.forall(e =>
    s.localApplied.contains(e) or headReachable(e))
  def releaseSafe: bool = all {
    s.compacted.forall(e => s.terminal.contains(e) and s.localApplied.contains(e)),
    s.bodyUsers == Set() or Stores.exists(n => fullHead(1, n)),
    s.payloadUsers == Set() or all { not(s.payloadReleased), s.payload.size() > 0 },
    s.releaseWasFresh,
  }
  def retiredSafe: bool = all {
    s.retired.subseteq(s.retirement), s.rejectedOriginals.subseteq(s.retired),
    s.replayed.subseteq(s.terminal), Effects.forall(e => s.applied.get(e) <= 1),
  }
  def coverageSafe: bool = s.activationGate implies canActivate
  def custodySafety: bool = all { dispatchCovered, unresolvedRetained, terminalMaterial, localApplicationRetained,
    releaseSafe, retiredSafe, coverageSafe }
  def withdrawalSafety: bool = Effects.forall(e => s.knownWithdrawal.contains(e) implies all {
    not(s.currentReceipt.contains(e)), s.deficit.contains(e),
  })
  def safety: bool = custodySafety and withdrawalSafety

}

module dfs_custody_ack_target {
  import dfs_custody_ack_contract.* from "../../contracts/dfs/availability_effect_custody"
  import dfs_custody_ack.* from "../../models/dfs/availability_effect_custody"
  def stageSafety: bool = a.stageEmissionCertified
  def installSafety: bool = all { a.installEmissionCertified,
    a.installedAckHistory implies all { a.assigned, a.queueReadable, a.cursorReadable, a.installedOrigins == origins },
    a.assigned implies all { a.authority, pages.subseteq(a.stagedAcks) },
    a.queueReadable implies a.assigned,
  }
  def evidenceOnly: bool = all { not(a.executor), not(a.payloadReady), not(a.rootReleased),
    a.responsibilityRetired implies a.recoveryResponsibility,
  }
  /// Imported history uses immutable facts; certifier capability may be lost.
  def immutableFactIds: bool = a.imported.forall(f => a.imported.forall(other => f.seq != other.seq or f == other))
  def frontierSafety: bool = if (a.imported == Set())
    a.frontierSeq == cutSeq and a.frontierDigest == transfer.inventory
    else a.imported.exists(tip => all { tip.seq == a.frontierSeq, tip.digest == a.frontierDigest,
      a.imported.forall(f => f.seq <= tip.seq) })
  def chainSafety: bool = all { immutableFactIds, frontierSafety, a.imported.subseteq(a.retained),
    a.imported.forall(f => all { a.installedOrigins == origins, bindingValid(f),
    if (f.prevCut) all { f.prevSeq == cutSeq, f.prevDigest == transfer.inventory }
    else a.imported.exists(prev => prev.seq == f.prevSeq and prev.digest == f.prevDigest and prev.seq < f.seq),
    }),
  }
  def sourceAckChainSafety: bool = all {
    if (a.sourceAcked == Set()) a.sourceFrontier == cutSeq and a.sourceFrontierDigest == transfer.inventory
    else a.retained.exists(tip => all { a.sourceAcked.contains(tip.seq), tip.seq == a.sourceFrontier,
      tip.digest == a.sourceFrontierDigest, a.sourceAcked.forall(seq => seq <= tip.seq) }),
    a.sourceAcked.forall(seq => a.retained.exists(f => f.seq == seq and
      (if (f.prevCut) f.prevSeq == cutSeq and f.prevDigest == transfer.inventory
       else a.sourceAcked.contains(f.prevSeq)))),
  }
  def lateAckSafety: bool = all { a.lateEmissionCertified,
    a.lateAcks.forall(ack => ack.transfer == transfer and
      a.imported.exists(f => f.seq == ack.seq and f.digest == ack.digest)),
    a.sourceAcked.forall(seq => a.lateAcks.exists(ack => ack.seq == seq)), sourceAckChainSafety,
  }
  def futureSafety: bool = not(a.futureClosed)
  def ackSafety: bool = all { stageSafety, installSafety, evidenceOnly, chainSafety, lateAckSafety, futureSafety }
}
