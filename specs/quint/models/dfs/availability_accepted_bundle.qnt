/// DRAFT TARGET, NOT IMPLEMENTED RUST CONFORMANCE.
/// One accepted slot, three fixed metadata members, three authority reservations,
/// and two injective aliases for exact immutable proposal/terminal-frame bytes.
/// Assumes the certified V7 admission cut has already completed. Existing cluster
/// authority is an independent reservation oracle, not per-operation metadata KV.
/// Durable promises/accepted records are atomic abstract actions. Promise cuts
/// become proposer-visible only on separate delivery, including after loss or
/// restart. Availability changes never erase durable state. ACK issue/delivery
/// are separate so an old proof can arrive after a higher promise quorum.
/// A globally unique immutable proposal per ballot is assumed, not a receiver
/// same-ballot conflict check. acceptanceHistory is proof-only ghost history,
/// not retained original-body custody or a durable decision certificate.
/// Excludes canonical multi-slot ordering/prefixes, real storage/fsync, native
/// wire validation, durable decision certificates, canonical publication, reads,
/// effect/attachment custody and liveness. Those remain
/// separate protocol/refinement and process/transport/disk obligations.
module dfs_availability_accepted_bundle {
  import dfs_availability_accepted_bundle_contract.* from "../../contracts/dfs/availability_accepted_bundle"
  type State = {
    reserved: int, controlAuthority: bool, available: Set[int],
    promised: int -> int, accepted: int -> Accepted,
    durableCuts: int -> (int -> Reply), cuts: int -> (int -> Reply),
    proposals: int -> int,
    selectedQuorums: int -> Set[int],
    acceptanceHistory: Set[(int, int, int)],
    issued: Set[(int, int)], delivered: Set[(int, int)],
    observedBundles: Set[int], observedBallots: Set[int],
    deliveredAfterHigherPromise: Set[(int, int)],
    delayedOldProof: bool, staleRejected: bool, minorityRejected: bool,
    survivorPromises: Set[int], survivorAccepts: Set[int], survivorDeliveries: Set[int],
    survivorProofObserved: bool, reconnectedAfterSurvivorProof: bool,
    privateBeforeOutage: bool, privateRejectedAfterReconnect: bool,
  }
  pure val initial: State = {
    reserved: 1, controlAuthority: true, available: Members,
    promised: Members.mapBy(_ => 1), accepted: Members.mapBy(_ => EmptyAccepted),
    // Ballot 1 starts at the certified empty post-admission cut. Later ballots
    // must collect their own durable promise cuts.
    durableCuts: Ballots.mapBy(e => Members.mapBy(_ => EmptyReply.with("present", e == 1))),
    cuts: Ballots.mapBy(e => Members.mapBy(_ => EmptyReply.with("present", e == 1))),
    proposals: Ballots.mapBy(_ => 0), selectedQuorums: Ballots.mapBy(_ => Set()),
    acceptanceHistory: Set(), issued: Set(), delivered: Set(),
    observedBundles: Set(), observedBallots: Set(),
    deliveredAfterHigherPromise: Set(),
    delayedOldProof: false, staleRejected: false, minorityRejected: false,
    survivorPromises: Set(), survivorAccepts: Set(), survivorDeliveries: Set(),
    survivorProofObserved: false, reconnectedAfterSurvivorProof: false,
    privateBeforeOutage: false, privateRejectedAfterReconnect: false,
  }
  var s: State
  def issuedQuorum(ballot: int): bool = Members.filter(n => s.issued.contains((ballot, n))).size() >= 2
  def deliveredQuorum(ballot: int): bool = Members.filter(n => s.delivered.contains((ballot, n))).size() >= 2
  def durablePromiseQuorum(ballot: int): bool = Members.filter(n => s.durableCuts.get(ballot).get(n).present).size() >= 2
  /// A historical proof alone cannot certify a later outage interval.
  def currentSurvivorProof: bool = all {
    s.available == Set(2, 3), s.survivorProofObserved,
    s.observedBallots.contains(2), s.selectedQuorums.get(2) == Set(2, 3),
    Set(2, 3).subseteq(s.survivorPromises),
    Set(2, 3).subseteq(s.survivorAccepts),
    Set(2, 3).subseteq(s.survivorDeliveries),
  }
  action init = s' = initial
  action reserveNext = all {
    s.controlAuthority, s.reserved < 3,
    // A topology reservation alone does not promise/freeze any participant.
    s' = s.with("reserved", s.reserved + 1),
  }
  action promise(ballot: int, node: int): bool = all {
    Ballots.contains(ballot), Members.contains(node), ballot <= s.reserved,
    s.available.contains(node), ballot >= s.promised.get(node),
    not(s.durableCuts.get(ballot).get(node).present),
    s' = s.with("promised", s.promised.set(node, ballot))
      .with("durableCuts", s.durableCuts.set(ballot, s.durableCuts.get(ballot).set(node, {
        present: true, ballot: s.accepted.get(node).ballot,
        bundle: s.accepted.get(node).bundle,
      })))
      .with("survivorPromises", if (ballot == 2 and s.available == Set(2, 3)) s.survivorPromises.union(Set(node)) else s.survivorPromises),
  }
  action deliverPromise(ballot: int, node: int): bool = all {
    Ballots.contains(ballot), Members.contains(node),
    s.durableCuts.get(ballot).get(node).present, not(s.cuts.get(ballot).get(node).present),
    // A captured reply may arrive after a higher promise; preserve its exact cut.
    s' = s.with("cuts", s.cuts.set(ballot, s.cuts.get(ballot).set(node, s.durableCuts.get(ballot).get(node)))),
  }
  action choose(ballot: int, quorum: Set[int], bundle: int): bool = all {
    Ballots.contains(ballot), ballot <= s.reserved,
    s.available.contains(ballot), s.proposals.get(ballot) == 0,
    proposalPreservesCut(s.cuts.get(ballot), quorum, bundle),
    s' = s.with("proposals", s.proposals.set(ballot, bundle))
      .with("selectedQuorums", s.selectedQuorums.set(ballot, quorum)),
  }
  action accept(ballot: int, node: int): bool = all {
    Ballots.contains(ballot), Members.contains(node), ballot <= s.reserved,
    s.available.contains(node), s.proposals.get(ballot) != 0,
    ballot >= s.promised.get(node),
    s' = s.with("promised", s.promised.set(node, ballot))
      .with("accepted", s.accepted.set(node, { ballot: ballot, bundle: s.proposals.get(ballot) }))
      .with("acceptanceHistory", s.acceptanceHistory.union(Set((ballot, node, s.proposals.get(ballot)))))
      .with("issued", s.issued.union(Set((ballot, node))))
      .with("survivorAccepts", if (ballot == 2 and s.available == Set(2, 3)) s.survivorAccepts.union(Set(node)) else s.survivorAccepts),
  }
  action deliverAck(ballot: int, node: int): bool = all {
    s.issued.contains((ballot, node)), not(s.delivered.contains((ballot, node))),
    // Do not recheck the current promise: this reply was issued before it.
    s' = s.with("delivered", s.delivered.union(Set((ballot, node))))
      .with("deliveredAfterHigherPromise", if (all {
        Members.filter(n => s.delivered.contains((ballot, n))).size() == 1,
        Ballots.exists(higher => higher > ballot and durablePromiseQuorum(higher)),
      })
        s.deliveredAfterHigherPromise.union(Set((ballot, node))) else s.deliveredAfterHigherPromise)
      .with("survivorDeliveries", if (ballot == 2 and s.available == Set(2, 3)) s.survivorDeliveries.union(Set(node)) else s.survivorDeliveries),
  }
  /// Observing a chosen-value proof is NOT native commit or activation.
  action observeChosen(ballot: int): bool = all {
    Ballots.contains(ballot), deliveredQuorum(ballot), s.proposals.get(ballot) != 0,
    not(s.observedBallots.contains(ballot)),
    s' = s.with("observedBundles", s.observedBundles.union(Set(s.proposals.get(ballot))))
      .with("observedBallots", s.observedBallots.union(Set(ballot)))
      .with("delayedOldProof", s.delayedOldProof or all {
        s.observedBallots.exists(e => e > ballot),
        Members.exists(n => s.deliveredAfterHigherPromise.contains((ballot, n))),
      })
      .with("survivorProofObserved", s.survivorProofObserved or all {
        ballot == 2, s.available == Set(2, 3), s.selectedQuorums.get(2) == Set(2, 3),
        Set(2, 3).subseteq(s.survivorPromises), Set(2, 3).subseteq(s.survivorAccepts),
        Set(2, 3).subseteq(s.survivorDeliveries),
      }),
  }
  action rejectStale(ballot: int, node: int): bool = all {
    Ballots.contains(ballot), Members.contains(node), ballot < s.promised.get(node),
    s.available.contains(node), s.proposals.get(ballot) != 0,
    s' = s.with("staleRejected", true)
      .with("privateRejectedAfterReconnect", s.privateRejectedAfterReconnect or all {
        ballot == 1, node == 1, s.privateBeforeOutage, s.reconnectedAfterSurvivorProof,
      }),
  }
  action rejectMinority(ballot: int): bool = all {
    Ballots.contains(ballot), s.proposals.get(ballot) != 0, not(deliveredQuorum(ballot)),
    s' = s.with("minorityRejected", true),
  }
  action changeAvailability(nodes: Set[int]): bool = all {
    nodes.subseteq(Members),
    s' = s.with("available", nodes)
      // Witness-only interval tracking. Erase no durable promise/acceptance.
      // A reconnect or new A outage cannot combine events from different cuts.
      .with("survivorPromises", if (s.available.contains(1) != nodes.contains(1)) Set() else s.survivorPromises)
      .with("survivorAccepts", if (s.available.contains(1) != nodes.contains(1)) Set() else s.survivorAccepts)
      .with("survivorDeliveries", if (s.available.contains(1) != nodes.contains(1)) Set() else s.survivorDeliveries)
      .with("privateBeforeOutage", s.privateBeforeOutage or all {
        s.reserved == 1, s.available.contains(1), not(nodes.contains(1)),
        s.accepted.get(1) == { ballot: 1, bundle: 1 },
        Members.filter(n => s.issued.contains((1, n))) == Set(1),
      })
      .with("reconnectedAfterSurvivorProof", s.reconnectedAfterSurvivorProof or all {
        currentSurvivorProof, nodes == Members,
      }),
  }
  action changeControl(available: bool): bool = s' = s.with("controlAuthority", available)
  action stutter = s' = s
  action step = {
    nondet ballot = Ballots.oneOf()
    nondet node = Members.oneOf()
    nondet quorum = Quorums.oneOf()
    nondet bundle = Bundles.oneOf()
    nondet live = Set(Set(), Set(1), Set(2), Set(3), Set(1, 2), Set(1, 3), Set(2, 3), Members).oneOf()
    nondet control = Set(false, true).oneOf()
    any {
      reserveNext, promise(ballot, node), deliverPromise(ballot, node), choose(ballot, quorum, bundle),
      accept(ballot, node), deliverAck(ballot, node), observeChosen(ballot),
      rejectStale(ballot, node), rejectMinority(ballot),
      changeAvailability(live), changeControl(control), stutter,
    }
  }
}
