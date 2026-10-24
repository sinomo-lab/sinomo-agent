/// DRAFT TARGET only; deliberately outside the registered *_safety.qnt inventory.
/// Bounded execution is not exhaustive verification or Rust conformance.
module dfs_availability_accepted_bundle_safety {
  import dfs_availability_accepted_bundle.* from "../../models/dfs/availability_accepted_bundle"
  import dfs_availability_accepted_bundle_contract.* from "../../contracts/dfs/availability_accepted_bundle"
  def preservesPossibleOldDecision: bool = Ballots.forall(old => Ballots.forall(higher =>
    higher > old and issuedQuorum(old) and s.proposals.get(higher) != 0
      implies s.proposals.get(higher) == s.proposals.get(old)))
  def safety: bool = all {
    s.observedBundles.size() <= 1,
    preservesPossibleOldDecision,
    Members.forall(n => s.promised.get(n) >= s.accepted.get(n).ballot),
    s.delivered.subseteq(s.issued),
    s.observedBallots.forall(e => all {
      deliveredQuorum(e), s.proposals.get(e) != 0,
      s.observedBundles.contains(s.proposals.get(e)),
    }),
    s.observedBundles.forall(bundle => s.observedBallots.exists(e => s.proposals.get(e) == bundle)),
    Ballots.forall(e => Members.forall(n =>
      s.cuts.get(e).get(n).present implies s.cuts.get(e).get(n) == s.durableCuts.get(e).get(n))),
    Ballots.forall(e => s.proposals.get(e) == 0 or all {
      proposalPreservesCut(s.cuts.get(e), s.selectedQuorums.get(e), s.proposals.get(e)),
      Members.filter(n => s.issued.contains((e, n))).forall(n =>
        s.acceptanceHistory.contains((e, n, s.proposals.get(e)))),
    }),
  }
  def witnessDelayedOldProof: bool = s.delayedOldProof and s.observedBundles.size() == 1
  def witnessDurablePromiseWithoutReply: bool = all {
    s.durableCuts.get(2).get(2).present, not(s.cuts.get(2).get(2).present),
    not(s.available.contains(2)),
  }
  def witnessSingleIntersectionHolder: bool = all {
    issuedQuorum(1), s.selectedQuorums.get(2) == Set(2, 3),
    Set(2, 3).filter(n => s.cuts.get(2).get(n).bundle == s.proposals.get(1)).size() == 1,
    s.proposals.get(2) == s.proposals.get(1), s.observedBallots.contains(2),
  }
  /// Ghost history establishes event ordering, not durable original-body custody.
  def witnessPrivateOldValueExcludedAfterReconnect: bool = all {
    s.privateBeforeOutage, not(issuedQuorum(1)),
    s.proposals.get(2) == 2, s.survivorProofObserved,
    s.reconnectedAfterSurvivorProof, s.privateRejectedAfterReconnect,
  }
  def witnessSurvivorChosenProof: bool = currentSurvivorProof
  /// Reservation monotonicity is assumed; this is not a serving-role/failback model.
  def witnessReconnectKeepsReservation: bool = s.available == Members and s.reserved == 2 and s.reconnectedAfterSurvivorProof
}
