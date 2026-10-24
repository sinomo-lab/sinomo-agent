/// TARGET HARDENING. Deterministic tests over one accepted slot only.
/// Three fixed nodes/ballots, two immutable bundle aliases; no native authority,
/// publication, bytes, custody, read freshness, fsync, or liveness conformance.
/// Prefix helpers are not test cases. Select exactly names ending in Test.
module dfs_availability_accepted_bundle_witnesses {
  import dfs_availability_accepted_bundle.* from "../../models/dfs/availability_accepted_bundle"
  import dfs_availability_accepted_bundle_safety.* from "../../checks/dfs/availability_accepted_bundle_target"
  import dfs_availability_accepted_bundle_contract.* from "../../contracts/dfs/availability_accepted_bundle"
  pure val AB = Set(1, 2)
  pure val BC = Set(2, 3)
  pure val AC = Set(1, 3)

  run oldAcceptedPrefix = init.expect(safety)
    .then(choose(1, AB, 1)).expect(safety)
    .then(accept(1, 1)).expect(safety)
    .then(accept(1, 2)).expect(safety)

  run intersectingPromisePrefix = oldAcceptedPrefix
    .then(deliverAck(1, 1)).expect(safety)
    .then(reserveNext).expect(safety)
    .then(changeAvailability(BC)).expect(safety)
    .then(promise(2, 2)).expect(safety)
    .then(promise(2, 3)).expect(safety)
    .then(deliverPromise(2, 2)).expect(safety)
    .then(deliverPromise(2, 3)).expect(safety)

  run survivorPrefix = init.expect(safety)
    .then(changeAvailability(BC)).expect(safety)
    .then(reserveNext).expect(safety)
    .then(promise(2, 2)).expect(safety)
    .then(promise(2, 3)).expect(safety)
    .then(deliverPromise(2, 2)).expect(safety)
    .then(deliverPromise(2, 3)).expect(safety)
    .then(choose(2, BC, 2)).expect(safety)
    .then(accept(2, 2)).expect(safety)
    .then(accept(2, 3)).expect(safety)
    .then(deliverAck(2, 2)).expect(safety)
    .then(deliverAck(2, 3)).expect(safety)
    .then(observeChosen(2)).expect(all { safety, witnessSurvivorChosenProof })

  run privateSurvivorPrefix = init.expect(safety)
    .then(choose(1, AB, 1)).expect(safety)
    .then(accept(1, 1)).expect(safety)
    .then(deliverAck(1, 1)).expect(safety)
    .then(changeAvailability(BC)).expect(all { safety, s.privateBeforeOutage })
    .then(reserveNext).expect(safety)
    .then(promise(2, 2)).expect(safety)
    .then(promise(2, 3)).expect(safety)
    .then(deliverPromise(2, 2)).expect(safety)
    .then(deliverPromise(2, 3)).expect(safety)
    .then(choose(2, BC, 2)).expect(safety)
    .then(accept(2, 2)).expect(safety)
    .then(accept(2, 3)).expect(safety)
    .then(deliverAck(2, 2)).expect(safety)
    .then(deliverAck(2, 3)).expect(safety)
    .then(observeChosen(2)).expect(all { safety, witnessSurvivorChosenProof })

  run delayedOldQuorumProofTest = intersectingPromisePrefix
    .then(choose(2, BC, 1)).expect(safety)
    .then(accept(2, 2)).expect(safety)
    .then(accept(2, 3)).expect(safety)
    .then(deliverAck(2, 2)).expect(safety)
    .then(deliverAck(2, 3)).expect(safety)
    .then(observeChosen(2)).expect(all { safety, not(witnessDelayedOldProof), witnessSurvivorChosenProof })
    .then(deliverAck(1, 2)).expect(all { safety, s.deliveredAfterHigherPromise.contains((1, 2)) })
    .then(observeChosen(1)).expect(all { safety, witnessDelayedOldProof, witnessSingleIntersectionHolder,
      s.observedBundles == Set(1), s.observedBallots == Set(1, 2) })

  run undeliveredPromisePrefix = init.expect(safety)
    .then(choose(1, AB, 1)).expect(safety)
    .then(accept(1, 2)).expect(safety)
    .then(reserveNext).expect(safety)
    .then(promise(2, 2)).expect(safety)
    .then(changeAvailability(AC)).expect(all { safety, witnessDurablePromiseWithoutReply,
      s.durableCuts.get(2).get(2) == {present: true, ballot: 1, bundle: 1} })
    .then(promise(2, 3)).expect(safety)
    .then(deliverPromise(2, 3)).expect(safety)
    .then(changeAvailability(Members)).expect(safety)

  run durablePromiseWithoutReplyTest = undeliveredPromisePrefix
    .then(deliverPromise(2, 2)).expect(all { safety,
      s.cuts.get(2).get(2) == {present: true, ballot: 1, bundle: 1} })
    .then(choose(2, BC, 1)).expect(safety)
    .then(accept(2, 2)).expect(safety)
    .then(accept(2, 3)).expect(safety)
    .then(deliverAck(2, 2)).expect(safety)
    .then(deliverAck(2, 3)).expect(safety)
    .then(observeChosen(2)).expect(all { safety, s.observedBundles == Set(1) })

  run survivorAndReconnectTest = survivorPrefix
    .expect(all { s.survivorPromises == BC, s.survivorAccepts == BC, s.survivorDeliveries == BC,
      s.selectedQuorums.get(2) == BC, s.observedBundles == Set(2), s.observedBallots == Set(2),
      not(s.issued.contains((2, 1))), not(s.delivered.contains((2, 1))),
      not(s.reconnectedAfterSurvivorProof) })
    .then(changeAvailability(Members)).expect(all { safety, witnessReconnectKeepsReservation,
      not(witnessSurvivorChosenProof) })

  pure val privateRejectionEffect = {
    promised: Members.mapBy(_ => 2),
    accepted: Members.mapBy(n => if (n == 1) {ballot: 1, bundle: 1} else {ballot: 2, bundle: 2}),
    history: Set((1, 1, 1), (2, 2, 2), (2, 3, 2)),
    proposals: Ballots.mapBy(e => if (e == 1) 1 else if (e == 2) 2 else 0),
    selected: Ballots.mapBy(e => if (e == 1) AB else if (e == 2) BC else Set()),
    issued: Set((1, 1), (2, 2), (2, 3)), delivered: Set((1, 1), (2, 2), (2, 3)),
    bundles: Set(2), ballots: Set(2),
  }
  pure val privateRejectionState = initial.with("reserved", 2)
    .with("promised", privateRejectionEffect.promised)
    .with("accepted", privateRejectionEffect.accepted)
    .with("acceptanceHistory", privateRejectionEffect.history)
    .with("proposals", privateRejectionEffect.proposals)
    .with("selectedQuorums", privateRejectionEffect.selected)
    .with("issued", privateRejectionEffect.issued).with("delivered", privateRejectionEffect.delivered)
    .with("observedBundles", privateRejectionEffect.bundles).with("observedBallots", privateRejectionEffect.ballots)
    .with("durableCuts", initial.durableCuts.set(2, Members.mapBy(n =>
      if (n == 1) {present: true, ballot: 1, bundle: 1} else EmptyReply.with("present", true))))
    .with("cuts", initial.cuts.set(2, Members.mapBy(n => EmptyReply.with("present", n != 1))))
    .with("survivorProofObserved", true).with("reconnectedAfterSurvivorProof", true)
    .with("privateBeforeOutage", true)
  run privateOldValueFencedAfterReconnectTest = privateSurvivorPrefix
    .then(changeAvailability(Members)).expect(all { safety, witnessReconnectKeepsReservation })
    .then(promise(2, 1)).expect(all { safety, s == privateRejectionState })
    .then(rejectStale(1, 1)).expect(all { safety,
      s == privateRejectionState.with("staleRejected", true).with("privateRejectedAfterReconnect", true),
      witnessPrivateOldValueExcludedAfterReconnect })

  run thirdPromisePrefix = privateSurvivorPrefix
    .then(reserveNext).expect(safety)
    .then(changeAvailability(AC)).expect(safety)
    .then(promise(3, 1)).expect(safety)
    .then(promise(3, 3)).expect(safety)
    .then(deliverPromise(3, 1)).expect(safety)
    .then(deliverPromise(3, 3)).expect(all { safety,
      s.cuts.get(3).get(1) == {present: true, ballot: 1, bundle: 1},
      s.cuts.get(3).get(3) == {present: true, ballot: 2, bundle: 2},
      highestAccepted(s.cuts.get(3), AC) == Set(3), BC.intersect(AC) == Set(3) })

  run thirdBallotHighestIntersectionTest = thirdPromisePrefix
    .then(choose(3, AC, 2)).expect(safety)
    .then(accept(3, 1)).expect(safety)
    .then(accept(3, 3)).expect(safety)
    .then(deliverAck(3, 1)).expect(safety)
    .then(deliverAck(3, 3)).expect(safety)
    .then(observeChosen(3)).expect(all { safety, s.observedBundles == Set(2), s.observedBallots == Set(2, 3) })

  run earlyOldAckIsNotDelayedProofTest = oldAcceptedPrefix
    .then(deliverAck(1, 1)).expect(safety)
    .then(deliverAck(1, 2)).expect(safety)
    .then(reserveNext).expect(safety)
    .then(promise(2, 2)).expect(safety)
    .then(promise(2, 3)).expect(safety)
    .then(deliverPromise(2, 2)).expect(safety)
    .then(deliverPromise(2, 3)).expect(safety)
    .then(choose(2, BC, 1)).expect(safety)
    .then(accept(2, 2)).expect(safety)
    .then(accept(2, 3)).expect(safety)
    .then(deliverAck(2, 2)).expect(safety)
    .then(deliverAck(2, 3)).expect(safety)
    .then(observeChosen(2)).expect(all { safety, not(witnessDelayedOldProof) })
    .then(observeChosen(1)).expect(all { safety, not(witnessDelayedOldProof) })

  run splitOutagePromiseEventsRejectedTest = init.expect(safety)
    .then(changeAvailability(BC)).expect(safety)
    .then(reserveNext).expect(safety)
    .then(promise(2, 2)).expect(safety)
    .then(changeAvailability(Members)).expect(safety)
    .then(changeAvailability(BC)).expect(safety)
    .then(promise(2, 3)).expect(safety)
    .then(deliverPromise(2, 2)).expect(safety)
    .then(deliverPromise(2, 3)).expect(safety)
    .then(choose(2, BC, 2)).expect(safety)
    .then(accept(2, 2)).expect(safety)
    .then(accept(2, 3)).expect(safety)
    .then(deliverAck(2, 2)).expect(safety)
    .then(deliverAck(2, 3)).expect(safety)
    .then(observeChosen(2)).expect(all { safety, not(witnessSurvivorChosenProof), s.survivorPromises == Set(3) })

  run onlinePromisesDoNotProveOutageTest = init.expect(safety)
    .then(reserveNext).expect(safety)
    .then(promise(2, 2)).expect(safety)
    .then(promise(2, 3)).expect(safety)
    .then(deliverPromise(2, 2)).expect(safety)
    .then(deliverPromise(2, 3)).expect(safety)
    .then(changeAvailability(BC)).expect(safety)
    .then(choose(2, BC, 2)).expect(safety)
    .then(accept(2, 2)).expect(safety)
    .then(accept(2, 3)).expect(safety)
    .then(deliverAck(2, 2)).expect(safety)
    .then(deliverAck(2, 3)).expect(safety)
    .then(observeChosen(2)).expect(all { safety, not(witnessSurvivorChosenProof) })

  run ackDeliveryAfterReconnectDoesNotProveOutageTest = init.expect(safety)
    .then(changeAvailability(BC)).expect(safety)
    .then(reserveNext).expect(safety)
    .then(promise(2, 2)).expect(safety)
    .then(promise(2, 3)).expect(safety)
    .then(deliverPromise(2, 2)).expect(safety)
    .then(deliverPromise(2, 3)).expect(safety)
    .then(choose(2, BC, 2)).expect(safety)
    .then(accept(2, 2)).expect(safety)
    .then(accept(2, 3)).expect(safety)
    .then(deliverAck(2, 2)).expect(safety)
    .then(changeAvailability(Members)).expect(safety)
    .then(deliverAck(2, 3)).expect(safety)
    .then(changeAvailability(BC)).expect(safety)
    .then(observeChosen(2)).expect(all { safety, not(witnessSurvivorChosenProof) })

  run historicalProofDoesNotProveNewOutageTest = survivorPrefix
    .then(changeAvailability(Members)).expect(safety)
    .then(changeAvailability(BC)).expect(all { safety, not(witnessSurvivorChosenProof),
      s.survivorPromises == Set(), s.survivorAccepts == Set(), s.survivorDeliveries == Set() })

  run historicalProofDoesNotCertifyLaterReconnectTest = survivorPrefix
    .then(changeAvailability(AB)).expect(all { safety, not(s.reconnectedAfterSurvivorProof) })
    .then(changeAvailability(BC)).expect(safety)
    .then(changeAvailability(Members)).expect(all { safety, not(witnessReconnectKeepsReservation) })

  pure val minorityState = initial
    .with("available", Set(1)).with("proposals", initial.proposals.set(1, 1))
    .with("selectedQuorums", initial.selectedQuorums.set(1, AB))
    .with("accepted", initial.accepted.set(1, {ballot: 1, bundle: 1}))
    .with("acceptanceHistory", Set((1, 1, 1))).with("issued", Set((1, 1))).with("delivered", Set((1, 1)))
  run minorityPrefix = init.expect(safety)
    .then(choose(1, AB, 1)).expect(safety)
    .then(accept(1, 1)).expect(safety)
    .then(deliverAck(1, 1)).expect(safety)
    .then(changeAvailability(Set(1))).expect(all { safety, s == minorityState })

  run minorityRejectionPreservesEvidenceTest = minorityPrefix
    .then(rejectMinority(1)).expect(all { safety, s == minorityState.with("minorityRejected", true) })

  pure val fencedState = initial.with("reserved", 2)
    .with("proposals", initial.proposals.set(1, 1)).with("selectedQuorums", initial.selectedQuorums.set(1, AB))
    .with("accepted", initial.accepted.set(1, {ballot: 1, bundle: 1}))
    .with("acceptanceHistory", Set((1, 1, 1))).with("issued", Set((1, 1)))
    .with("promised", initial.promised.set(2, 2))
    .with("durableCuts", initial.durableCuts.set(2, initial.durableCuts.get(2).set(2, EmptyReply.with("present", true))))
  run fencedPrefix = init.expect(safety)
    .then(choose(1, AB, 1)).expect(safety)
    .then(accept(1, 1)).expect(safety)
    .then(reserveNext).expect(safety)
    .then(promise(2, 2)).expect(all { safety, s == fencedState })

  run staleRejectionPreservesEvidenceTest = fencedPrefix
    .then(rejectStale(1, 2)).expect(all { safety, s == fencedState.with("staleRejected", true) })

  // Deliberately broken test-only transitions are excluded from model.step.
  // They show these checks can detect mistakes; they are no candidate protocol.
  action brokenIgnoreHighest = s' = s.with("proposals", s.proposals.set(2, 2))
    .with("selectedQuorums", s.selectedQuorums.set(2, BC))
  action brokenObservePrivateMinority = s' = s.with("observedBundles", s.observedBundles.union(Set(1)))
    .with("observedBallots", s.observedBallots.union(Set(1)))
  run highestAcceptedSensitivityTest = intersectingPromisePrefix
    .then(brokenIgnoreHighest).expect(not(safety))
  run minorityProofSensitivityTest = privateSurvivorPrefix
    .then(brokenObservePrivateMinority).expect(all { not(safety), s.observedBundles == Set(1, 2) })

  // Delayed reply must return its captured cut, not current accepted state.
  run capturedPromiseReplyDoesNotRereadAcceptedTest = init.expect(safety)
    .then(choose(1, AB, 1)).expect(safety)
    .then(accept(1, 2)).expect(safety)
    .then(reserveNext).expect(safety)
    .then(promise(2, 2)).expect(safety)
    .then(changeAvailability(AC)).expect(safety)
    .then(promise(2, 1)).expect(safety)
    .then(promise(2, 3)).expect(safety)
    .then(deliverPromise(2, 1)).expect(safety)
    .then(deliverPromise(2, 3)).expect(safety)
    .then(changeAvailability(Members)).expect(safety)
    .then(choose(2, AC, 2)).expect(safety)
    .then(accept(2, 2)).expect(safety)
    .then(deliverPromise(2, 2)).expect(all { safety,
      s.accepted.get(2) == {ballot: 2, bundle: 2},
      s.cuts.get(2).get(2) == {present: true, ballot: 1, bundle: 1} })

  // A reservation alone promises no acceptor and must not suppress old proof.
  run reservationAloneDoesNotFenceOldProofTest = init.expect(safety)
    .then(choose(1, AB, 1)).expect(safety)
    .then(reserveNext).expect(safety)
    .then(accept(1, 1)).expect(safety)
    .then(accept(1, 2)).expect(safety)
    .then(deliverAck(1, 1)).expect(safety)
    .then(deliverAck(1, 2)).expect(safety)
    .then(observeChosen(1)).expect(all { safety, s.reserved == 2, s.observedBundles == Set(1) })
    .then(promise(2, 2)).expect(safety)
    .then(promise(2, 3)).expect(safety)
    .then(deliverPromise(2, 2)).expect(safety)
    .then(deliverPromise(2, 3)).expect(safety)

  // Provenance must reject even a single unopposed observation without quorum.
  run minorityOnlyProofProvenanceSensitivityTest = init.expect(safety)
    .then(choose(1, AB, 1)).expect(safety)
    .then(accept(1, 1)).expect(safety)
    .then(deliverAck(1, 1)).expect(safety)
    .then(brokenObservePrivateMinority).expect(all { not(safety), s.observedBundles == Set(1) })

  // A disabled action has no next state. fail() is terminal in each negative
  // test; it is not an executable rejection transition or a state-preserving
  // retry. Marker rejection transitions are tested separately by full equality.
  run conflictingBundleAfterOldQuorumDisabledTest = intersectingPromisePrefix
    .then(choose(2, BC, 2).fail())
  run duplicateAckDeliveryDisabledTest = delayedOldQuorumProofTest
    .then(deliverAck(1, 2).fail())
  run duplicateOldProofObservationDisabledTest = delayedOldQuorumProofTest
    .then(observeChosen(1).fail())
  run choiceWithoutDeliveredPromiseDisabledTest = undeliveredPromisePrefix
    .expect(all { s.available == Members, not(s.cuts.get(2).get(2).present) })
    .then(choose(2, BC, 1).fail())
  run promiseRecaptureDisabledTest = undeliveredPromisePrefix
    .then(promise(2, 2).fail())
  run reportedPrivateValueCannotBeReplacedTest = undeliveredPromisePrefix
    .then(deliverPromise(2, 2)).expect(safety)
    .then(choose(2, BC, 2).fail())
  run reconnectAloneDoesNotFenceOldBallotTest = privateSurvivorPrefix
    .then(changeAvailability(Members)).expect(all { safety, s.promised.get(1) == 1 })
    .then(rejectStale(1, 1).fail())
  run oldAcceptanceAfterReconnectPromiseDisabledTest = privateSurvivorPrefix
    .then(changeAvailability(Members)).expect(safety)
    .then(promise(2, 1)).expect(all { safety, s == privateRejectionState })
    .then(accept(1, 1).fail())
  run thirdBallotLowerPrivateValueDisabledTest = thirdPromisePrefix
    .then(choose(3, AC, 1).fail())
  run unissuedAckDeliveryDisabledTest = init.expect(all { safety, s == initial })
    .then(deliverAck(1, 3).fail())
  run unavailableAcceptanceDisabledTest = minorityPrefix
    .then(accept(1, 2).fail())
  run minorityChosenObservationDisabledTest = minorityPrefix
    .then(rejectMinority(1)).expect(all { safety, s == minorityState.with("minorityRejected", true) })
    .then(observeChosen(1).fail())
  run staleAcceptanceDisabledTest = fencedPrefix
    .then(accept(1, 2).fail())
}
