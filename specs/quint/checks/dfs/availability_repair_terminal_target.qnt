/// Unregistered C5/C6 target; no *_safety conformance/production claim.
module dfs_repair_terminal_target {
  import dfs_repair_terminal_contract.* from "../../contracts/dfs/availability_repair_terminal"
  import dfs_repair_terminal.* from "../../models/dfs/availability_repair_terminal"
  def preparedSafety: bool = s.effectAudit
  def terminalSafety: bool = all { s.terminalAudit, s.tSync.forall(e => all {
    s.pSync.contains(e), s.cSync.contains(e), s.required.filter(k => k._1 == e).subseteq(s.mSync),
    s.pBody.get(e) == Bodies.get(e), s.result.get(e) == Results.get(e),
  }) }
  def savedSafety: bool = s.savedCert.subseteq(s.saved) and s.saved.forall(e => all { s.savedBody.get(e) == Bodies.get(e),
    s.savedResult.get(e) == Results.get(e), Set(1, 2).contains(s.savedStore.get(e)) })
  def replySafety: bool = s.replyAudit and s.replayAudit and savedSafety
  def applicationSafety: bool = s.applicationAudit and (s.withdrawn implies all {
    not(s.currentCopy), not(s.currentHead), s.deficit,
  })
  def physicalSafety: bool = s.holds.forall(h => Nodes.contains(h._2)) and
    s.pSync.forall(e => s.pStore.get(e) == s.stores.get(3))
  def dependencySafety: bool = all { s.forwardStub,
    s.child implies all { s.originals.contains(2), holders(2).contains(1) },
    (s.borrower or s.forwardBody) implies holders(1).contains(1),
    s.unresolved.forall(e => s.originals.contains(e) and holders(e).size() > 0),
  }
  def noResurrection: bool = s.released.forall(r => not(s.holds.contains((1, r._1, r._2))))
  def retirementSafety: bool = all { s.releaseAudit, s.reservations.size() <= 1, noResurrection,
    s.responsibilityRetired implies s.recoveryPromise,
    s.transferReleased implies (holders(1).size() >= 2 or s.settled.contains(1)),
    s.settled.forall(e => s.saved.contains(e) and s.applied.contains(e) and not(s.unresolved.contains(e))),
    dependencySafety,
  }
  def safety: bool = all { preparedSafety, terminalSafety, replySafety, applicationSafety, physicalSafety, retirementSafety }
}
