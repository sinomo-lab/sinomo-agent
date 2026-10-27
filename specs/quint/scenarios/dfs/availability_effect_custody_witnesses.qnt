/// DRAFT TARGET: deterministic custody/compaction guards, not a complete
/// replication/authority protocol or actual Rust/transport/disk acceptance.
/// Select Test$ for controls. Counterexample$ deliberately fails a named
/// invariant using a test-only mutant; none is a permitted model action.
module dfs_availability_effect_custody_witnesses {
  import dfs_availability_effect_custody_contract.* from "../../contracts/dfs/availability_effect_custody"
  import dfs_availability_effect_custody.* from "../../models/dfs/availability_effect_custody"
  import dfs_availability_effect_custody_target.* from "../../checks/dfs/availability_effect_custody_target"

  run attachedCustodyPrefix = init.expect(safety)
    .then(persistHead(1, 2, FullExact)).expect(safety)
    .then(persistPayload(2)).expect(safety)
    .then(bindOriginal(1, 2)).expect(safety)
    .then(certifyCustody(1)).expect(all { safety, custodyReady(1) })
  run sentPrefix = attachedCustodyPrefix
    .then(arm(1, 1)).expect(safety)
    .then(dispatch(1, 1)).expect(all { safety, s.sent == Set((1, 1)) })
  run receiverPrefix = sentPrefix
    .then(receiverExecute(1, 1)).expect(all { safety, s.applied.get(1) == 1 })
  run observedPrefix = receiverPrefix.then(observeExactResult(1)).expect(safety)
  run survivorTerminalPrefix = observedPrefix
    .then(persistResult(1, 2, FullExact)).expect(safety)
    .then(persistTerminal(1, 2, Stage)).expect(safety)
    .then(persistResult(1, 3, FullExact)).expect(safety)
    .then(persistTerminal(1, 3, Stage)).expect(safety)
    .then(certifyTerminal(1)).expect(all { safety, s.terminal == Set(1) })
  run terminalPrefix = survivorTerminalPrefix.then(applyLocalOutcome(1)).expect(safety)
  run commitTerminalPrefix = initCommit.expect(safety)
    .then(persistHead(1, 2, FullExact)).expect(safety)
    .then(bindOriginal(1, 2)).expect(safety)
    .then(certifyCustody(1)).expect(safety)
    .then(arm(1, 1)).expect(safety)
    .then(dispatch(1, 1)).expect(safety)
    .then(receiverExecute(1, 1)).expect(safety)
    .then(observeExactResult(1)).expect(safety)
    .then(persistResult(1, 2, FullExact)).expect(safety)
    .then(persistTerminal(1, 2, Commit)).expect(safety)
    .then(persistResult(1, 3, FullExact)).expect(safety)
    .then(persistTerminal(1, 3, Commit)).expect(safety)
    .then(certifyTerminal(1)).expect(all { safety, s.payload == Set(1) })
    .then(applyLocalOutcome(1)).expect(safety)
  run childSentPrefix = terminalPrefix
    .then(checkCompaction).expect(safety)
    .then(assumeCompletedParentW1).expect(safety)
    .then(admitChild(false)).expect(safety)
    .then(bindOriginal(2, 1)).expect(safety)
    .then(persistHead(2, 2, FullExact)).expect(safety)
    .then(bindOriginal(2, 2)).expect(safety)
    .then(certifyCustody(2)).expect(safety)
    .then(arm(2, 1)).expect(safety)
    .then(dispatch(2, 1)).expect(all { safety, unresolved(2) })
  run childTerminalPrefix = childSentPrefix
    .then(receiverExecute(2, 1)).expect(safety)
    .then(observeExactResult(2)).expect(safety)
    .then(persistResult(2, 2, FullExact)).expect(safety)
    .then(persistTerminal(2, 2, Repair)).expect(safety)
    .then(persistResult(2, 3, FullExact)).expect(safety)
    .then(persistTerminal(2, 3, Repair)).expect(safety)
    .then(certifyTerminal(2)).expect(safety)
    .then(applyLocalOutcome(2)).expect(safety)

  run repairTerminalPrefix = initRepair.expect(safety)
    .then(persistHead(1, 2, FullExact)).expect(safety)
    .then(persistPayload(2)).expect(safety)
    .then(bindOriginal(1, 2)).expect(safety)
    .then(certifyCustody(1)).expect(safety)
    .then(arm(1, 1)).expect(safety)
    .then(dispatch(1, 1)).expect(safety)
    .then(receiverExecute(1, 1)).expect(safety)
    .then(observeExactResult(1)).expect(safety)
    .then(persistResult(1, 2, FullExact)).expect(safety)
    .then(persistTerminal(1, 2, Repair)).expect(safety)
    .then(persistResult(1, 3, FullExact)).expect(safety)
    .then(persistTerminal(1, 3, Repair)).expect(safety)
    .then(certifyTerminal(1)).expect(safety)
    .then(applyLocalOutcome(1)).expect(safety)
  run appliedCopyPrefix = repairTerminalPrefix
    .then(importCurrentApplication(1)).expect(all { safety, s.currentReceipt == Set(1), s.localClosedBirth == Set() })
  run withdrawnCopyPrefix = appliedCopyPrefix
    .then(withdrawCurrentCopy(1)).expect(all { safety, s.currentReceipt == Set(),
      s.localApplied == Set(1), s.knownWithdrawal == Set(1), s.deficit == Set(1), s.localClosedBirth == Set() })

  run alreadyAppliedWithoutWithdrawalClosesTest = appliedCopyPrefix
    .then(recoverStoredPositive(1)).expect(all { safety, s.localClosedBirth == Set(1),
      s.currentReceipt == Set(1), s.knownWithdrawal == Set(), s.deficit == Set() })
  run knownWithdrawalBlocksStoredPositiveTest = withdrawnCopyPrefix
    .then(recoverStoredPositive(1)).expect(all { safety, s.localClosedBirth == Set(),
      s.currentReceipt == Set(), s.deficit == Set(1), s.localApplied == Set(1), bodyReachable(1) })
  run withdrawalAfterClosureDoesNotReopenBirthTest = alreadyAppliedWithoutWithdrawalClosesTest
    .then(withdrawCurrentCopy(1)).expect(safety)
    .then(recoverStoredPositive(1)).expect(all { safety, s.localClosedBirth == Set(1),
      s.currentReceipt == Set(), s.deficit == Set(1), s.localApplied == Set(1) })
  run withdrawnPendingAllowsUnrelatedEffectTest = withdrawnCopyPrefix
    .then(admitChild(false)).expect(safety)
    .then(bindOriginal(2, 1)).expect(safety)
    .then(persistHead(2, 2, FullExact)).expect(safety)
    .then(bindOriginal(2, 2)).expect(safety)
    .then(certifyCustody(2)).expect(safety)
    .then(arm(2, 1)).expect(safety)
    .then(dispatch(2, 1)).expect(safety)
    .then(receiverExecute(2, 1)).expect(safety)
    .then(observeExactResult(2)).expect(safety)
    .then(persistResult(2, 2, FullExact)).expect(safety)
    .then(persistTerminal(2, 2, Repair)).expect(safety)
    .then(persistResult(2, 3, FullExact)).expect(safety)
    .then(persistTerminal(2, 3, Repair)).expect(safety)
    .then(certifyTerminal(2)).expect(safety)
    .then(applyLocalOutcome(2)).expect(safety)
    .then(importCurrentApplication(2)).expect(safety)
    .then(recoverStoredPositive(2)).expect(all { safety, s.localClosedBirth == Set(2),
      s.currentReceipt == Set(2), s.knownWithdrawal == Set(1), s.deficit == Set(1), bodyReachable(1) })
  action brokenRestoreStalePositive = s' = revised(s.with("currentReceipt", s.currentReceipt.union(Set(1)))
    .with("deficit", s.deficit.exclude(Set(1))).with("localClosedBirth", s.localClosedBirth.union(Set(1))))
  run stalePositiveRestorationSensitivityTest = withdrawnCopyPrefix
    .then(brokenRestoreStalePositive).expect(all { custodySafety, not(withdrawalSafety), not(safety) })
  run unsafeStalePositiveCounterexample = withdrawnCopyPrefix
    .then(brokenRestoreStalePositive).expect(safety)

  // Complete heads without separately persisted attachment bytes are not ready.
  run attachmentMissingPayloadArmDisabledTest = init.expect(safety)
    .then(persistHead(1, 2, FullExact)).expect(all { safety, not(custodyReady(1)), s.applied.get(1) == 0, s.sent == Set() })
    .then(arm(1, 1).fail())
  run attachmentMissingPayloadRootDisabledTest = init.expect(safety)
    .then(persistHead(1, 2, FullExact)).expect(safety)
    .then(bindOriginal(1, 2).fail())
  run digestOnlyHeadCannotCertifyTest = init.expect(safety)
    .then(persistHead(1, 2, DigestOnly)).expect(safety)
    .then(persistPayload(2)).expect(all { safety, s.heads.get((1, 2)) == DigestOnly })
    .then(certifyCustody(1).fail())
  run completeAttachmentDispatchTest = sentPrefix.expect(all { safety, s.certifiedBeforeSend == Set(1) })
  run inlineHeadContainsBodyTest = initInline.expect(safety)
    .then(persistHead(1, 2, FullExact)).expect(safety)
    .then(bindOriginal(1, 2)).expect(safety)
    .then(certifyCustody(1)).expect(all { safety, s.payload == Set(1) })
    .then(arm(1, 1)).expect(safety)
    .then(dispatch(1, 1)).expect(safety)
  run inlineDigestCannotBindTest = initInline.expect(safety)
    .then(persistHead(1, 2, DigestOnly)).expect(safety)
    .then(bindOriginal(1, 2).fail())
  run lostReplyThenOwnerLossTest = receiverPrefix
    .then(dropReply(1)).expect(safety)
    .then(loseOwner).expect(all { safety, unresolved(1), s.localResults == Set(),
      completeHolders(1).intersect(s.live) == Set(2), s.applied.get(1) == 1 })
    .then(observeExactResult(1)).expect(all { safety, s.recoveredAfterLoss == Set(1) })
    .then(persistResult(1, 2, FullExact)).expect(safety)
    .then(persistTerminal(1, 2, Stage)).expect(safety)
    .then(persistResult(1, 3, FullExact)).expect(safety)
    .then(persistTerminal(1, 3, Stage)).expect(safety)
    .then(certifyTerminal(1)).expect(safety)
    .then(delayedOriginal(1)).expect(all { safety, s.replayed == Set(1),
      s.claims == Set((1, 1)), s.applied.get(1) == 1 })
  run localTerminalCannotCheckCompactionTest = observedPrefix
    .then(persistResult(1, 1, FullExact)).expect(safety)
    .then(persistTerminal(1, 1, Stage)).expect(all { safety, s.terminal == Set() })
    .then(checkCompaction.fail())
  run localTerminalThenLossRetainsUnresolvedTest = observedPrefix
    .then(persistResult(1, 1, FullExact)).expect(safety)
    .then(persistTerminal(1, 1, Stage)).expect(safety)
    .then(loseOwner).expect(all { safety, unresolved(1), bodyReachable(1),
      s.terminal == Set(), s.localResults == Set(), s.compacted == Set() })
  run resultDigestIsNotResultMaterialTest = observedPrefix
    .then(persistResult(1, 2, DigestOnly)).expect(safety)
    .then(persistTerminal(1, 2, Stage).fail())
  run oneTerminalCopyCannotCertifyTest = observedPrefix
    .then(persistResult(1, 1, FullExact)).expect(safety)
    .then(persistTerminal(1, 1, Stage)).expect(safety)
    .then(certifyTerminal(1).fail())
  run survivorTerminalStillNeedsLocalApplicationTest = survivorTerminalPrefix
    .expect(all { safety, s.terminal == Set(1), s.localApplied == Set(), fullHead(1, 2) })
    .then(checkCompaction.fail())
  run stageResultCannotSettleCommitTest = initCommit.expect(safety)
    .then(persistHead(1, 2, FullExact)).expect(safety)
    .then(bindOriginal(1, 2)).expect(safety)
    .then(certifyCustody(1)).expect(safety)
    .then(arm(1, 1)).expect(safety)
    .then(dispatch(1, 1)).expect(safety)
    .then(receiverExecute(1, 1)).expect(safety)
    .then(observeExactResult(1)).expect(safety)
    .then(persistResult(1, 2, FullExact)).expect(safety)
    .then(persistTerminal(1, 2, Stage).fail())
  run commitSpecificSettlementTest = commitTerminalPrefix.expect(all { safety, s.kinds.get(1) == Commit })
  run sameKindDurableCannotSettleCommitTest = initCommit.expect(safety)
    .then(persistHead(1, 2, FullExact)).expect(safety)
    .then(bindOriginal(1, 2)).expect(safety)
    .then(certifyCustody(1)).expect(safety)
    .then(arm(1, 1)).expect(safety)
    .then(dispatch(1, 1)).expect(safety)
    .then(receiverExecute(1, 1)).expect(safety)
    .then(observeIntermediateDurable(1)).expect(safety)
    .then(persistResult(1, 2, FullExact)).expect(all { safety, s.resultOutcomes.get((1, 2)) == Durable })
    .then(persistTerminal(1, 2, Commit).fail())
  run changingObservationCannotRelabelStoredResultTest = initCommit.expect(safety)
    .then(persistHead(1, 2, FullExact)).expect(safety)
    .then(bindOriginal(1, 2)).expect(safety)
    .then(certifyCustody(1)).expect(safety)
    .then(arm(1, 1)).expect(safety)
    .then(dispatch(1, 1)).expect(safety)
    .then(receiverExecute(1, 1)).expect(safety)
    .then(observeIntermediateDurable(1)).expect(safety)
    .then(persistResult(1, 2, FullExact)).expect(safety)
    .then(observeExactResult(1)).expect(all { safety, s.observedOutcomes.get(1) == Committed,
      s.resultOutcomes.get((1, 2)) == Durable })
    .then(persistTerminal(1, 2, Commit).fail())
  run stageResultCannotSettleRepairTest = childSentPrefix
    .then(receiverExecute(2, 1)).expect(safety)
    .then(observeExactResult(2)).expect(safety)
    .then(persistResult(2, 2, FullExact)).expect(safety)
    .then(persistTerminal(2, 2, Stage).fail())
  run sameKindDurableCannotSettleRepairTest = childSentPrefix
    .then(receiverExecute(2, 1)).expect(safety)
    .then(observeIntermediateDurable(2)).expect(safety)
    .then(persistResult(2, 2, FullExact)).expect(safety)
    .then(persistTerminal(2, 2, Repair).fail())
  run laterUnsentCannotClearEarlierUncertaintyTest = sentPrefix
    .then(arm(1, 2)).expect(safety)
    .then(markDefinitelyUnsent(1, 2)).expect(all { safety, unresolved(1), s.uncertain == Set((1, 1)) })
    .then(rejectLaterEnvelope(1, 2)).expect(all { safety, unresolved(1), s.terminal == Set(), s.uncertain == Set((1, 1)) })
  run sentAttemptCannotBecomeDefinitelyUnsentTest = sentPrefix.then(markDefinitelyUnsent(1, 1).fail())
  run parentW1CannotReleaseChildPayloadTest = childSentPrefix
    .then(checkCompaction).expect(safety)
    .then(releaseParentBody).expect(all { safety, s.parentW1Done, s.compacted == Set(1),
      unresolved(2), bodyReachable(2), s.payloadUsers == Set(2) })
    .then(releasePayload.fail())
  run aOnlyUnsentPayloadAllowsUnrelatedCustodyGateTest = commitTerminalPrefix
    .then(assumeCompletedParentW1).expect(safety)
    .then(admitChild(false)).expect(safety)
    .then(persistHead(2, 2, FullExact)).expect(safety)
    .then(loseOwner).expect(all { safety, s.payload == Set(1),
      s.payload.intersect(s.live) == Set(), not(unresolved(2)), not(bodyReachable(2)), originalCovered(2) })
    .then(captureCut).expect(safety)
    .then(certifyPage(1, s.cutRevision)).expect(safety)
    .then(certifyPage(2, s.cutRevision)).expect(safety)
    .then(loadPage(1)).expect(safety)
    .then(activateCustodyGate).expect(all { safety, s.activationGate, s.loadedPages == Set(1), not(custodyReady(2)) })
    .then(arm(2, 1).fail())
  run terminalWitnessBeforeBodyReleaseCrashTest = terminalPrefix
    .then(loseOwner).expect(all { safety, not(s.compacted.contains(1)), fullHead(1, 2) })
    .then(delayedOriginal(1)).expect(all { safety, s.applied.get(1) == 1, s.replayed == Set(1) })
  run dependencyChangeRejectsStaleCheckTest = terminalPrefix
    .then(checkCompaction).expect(all { safety, s.checkedGeneration == 0 })
    .then(admitChild(false)).expect(all { safety, s.generation == 1, s.bodyUsers == Set(), s.checkedGeneration == 0 })
    .then(releaseParentBody.fail())
  run dependentHeadBlocksFreshReleaseTest = terminalPrefix
    .then(admitChild(true)).expect(safety)
    .then(checkCompaction).expect(all { safety, s.checkedGeneration == s.generation, s.bodyUsers == Set(2) })
    .then(releaseParentBody.fail())
  run dependencyAbaDoesNotReuseGenerationTest = childTerminalPrefix
    .then(settleChild).expect(all { safety, s.generation == 2, s.payloadUsers == Set(), s.checkedGeneration == 0 })
    .then(releaseParentBody.fail())
  run settledChildAllowsFreshReleaseTest = childTerminalPrefix
    .then(settleChild).expect(all { safety, s.generation == 2, s.payloadUsers == Set() })
    .then(checkCompaction).expect(safety)
    .then(releaseParentBody).expect(safety)
    .then(releasePayload).expect(all { safety, s.payloadReleased, s.applied.get(2) == 1 })
  run delayedOriginalAfterCompactionAndRetirementTest = terminalPrefix
    .then(checkCompaction).expect(safety)
    .then(releaseParentBody).expect(safety)
    .then(delayedOriginal(1)).expect(all { safety, s.replayed == Set(1), s.applied.get(1) == 1 })
    .then(certifyRetirement(1)).expect(safety)
    .then(retireWitness(1)).expect(safety)
    .then(delayedOriginal(1)).expect(all { safety, s.rejectedOriginals == Set(1), s.applied.get(1) == 1 })
  run witnessRetirementWithoutFloorDisabledTest = terminalPrefix
    .then(checkCompaction).expect(safety)
    .then(releaseParentBody).expect(safety)
    .then(retireWitness(1).fail())
  run incompletePageCoverageBlocksGateTest = attachedCustodyPrefix
    .then(admitChild(false)).expect(safety)
    .then(persistHead(2, 2, FullExact)).expect(safety)
    .then(captureCut).expect(safety)
    .then(certifyPage(1, s.cutRevision)).expect(safety)
    .then(loadPage(1)).expect(all { safety, s.cutRows == Set(1, 2), s.pageCertificates == Set(1) })
    .then(activateCustodyGate.fail())
  run coverageDoesNotRequireBacklogHydrationTest = attachedCustodyPrefix
    .then(admitChild(false)).expect(safety)
    .then(persistHead(2, 2, FullExact)).expect(safety)
    .then(captureCut).expect(safety)
    .then(certifyPage(1, s.cutRevision)).expect(safety)
    .then(loadPage(1)).expect(safety)
    .then(certifyPage(2, s.cutRevision)).expect(safety)
    .then(activateCustodyGate).expect(all { safety, s.activationGate, s.loadedPages == Set(1), not(s.terminal.contains(2)) })
  run newObligationInvalidatesCompleteCutTest = attachedCustodyPrefix
    .then(captureCut).expect(safety)
    .then(certifyPage(1, s.cutRevision)).expect(safety)
    .then(certifyPage(2, s.cutRevision)).expect(safety)
    .then(activateCustodyGate).expect(safety)
    .then(admitChild(false)).expect(all { safety, not(s.activationGate), not(canActivate), s.cutRows == Set(1) })
  run mixedGenerationPageRejectedTest = attachedCustodyPrefix
    .then(captureCut).expect(safety)
    .then(certifyPage(1, s.cutRevision)).expect(safety)
    .then(admitChild(false)).expect(safety)
    .then(certifyPage(2, s.cutRevision).fail())

  // Deliberately broken test-only actions. These are not candidate transitions.
  action brokenDispatchBeforeCustody = s' = s.with("claims", Set((1, 1)))
    .with("uncertain", Set((1, 1))).with("sent", Set((1, 1)))
  action brokenReleaseBeforeWitness = s' = s
    .with("heads", Keys.mapBy(key => if (key._1 == 1) Absent else s.heads.get(key)))
    .with("roots", s.roots.filter(pair => pair._1 != 1)).with("compacted", Set(1))
  action brokenDropSurvivorBeforeWitness = s' = s
    .with("heads", Keys.mapBy(key => if (key._1 == 1 and key._2 != 1) Absent else s.heads.get(key)))
    .with("roots", s.roots.filter(pair => pair._1 != 1 or pair._2 == 1))
  action brokenIgnoreDependency = s' = s
    .with("heads", Keys.mapBy(key => if (key._1 == 1) Absent else s.heads.get(key)))
    .with("roots", s.roots.filter(pair => pair._1 != 1)).with("compacted", Set(1))
    .with("releaseWasFresh", s.releaseWasFresh and s.checkedGeneration == s.generation)
  action brokenFreshRetiredEffect = s' = s.with("applied", s.applied.set(1, 2))
  action brokenActivateLoadedPage = s' = s.with("activationGate", true)

  run dispatchSensitivityTest = init.expect(safety)
    .then(brokenDispatchBeforeCustody).expect(all { not(dispatchCovered), not(unresolvedRetained) })
  run terminalReleaseSensitivityTest = observedPrefix
    .then(brokenReleaseBeforeWitness).expect(all { not(releaseSafe), not(unresolvedRetained) })
  run localApplicationReleaseSensitivityTest = survivorTerminalPrefix
    .then(brokenReleaseBeforeWitness).expect(all { not(localApplicationRetained), not(releaseSafe) })
  run releaseBeforeWitnessThenCrashSensitivityTest = observedPrefix
    .then(brokenDropSurvivorBeforeWitness).expect(all { safety, bodyReachable(1), completeHolders(1) == Set(1) })
    .then(loseOwner).expect(all { not(safety), s.localResults == Set(),
      not(bodyReachable(1)), not(exactTerminalReachable(1)) })
  run dependencySensitivityTest = terminalPrefix
    .then(checkCompaction).expect(safety)
    .then(admitChild(false)).expect(safety)
    .then(brokenIgnoreDependency).expect(all { not(releaseSafe), s.bodyUsers == Set(), not(s.releaseWasFresh) })
  run liveHeadDependencySensitivityTest = terminalPrefix
    .then(admitChild(true)).expect(safety)
    .then(checkCompaction).expect(safety)
    .then(brokenIgnoreDependency).expect(all { not(releaseSafe), s.releaseWasFresh, s.bodyUsers == Set(2) })
  run retiredIdentitySensitivityTest = delayedOriginalAfterCompactionAndRetirementTest
    .then(brokenFreshRetiredEffect).expect(not(retiredSafe))
  run coverageSensitivityTest = attachedCustodyPrefix
    .then(admitChild(false)).expect(safety)
    .then(persistHead(2, 2, FullExact)).expect(safety)
    .then(captureCut).expect(safety)
    .then(certifyPage(1, s.cutRevision)).expect(safety)
    .then(loadPage(1)).expect(safety)
    .then(brokenActivateLoadedPage).expect(not(coverageSafe))

  // Separate expected-failure executions retain actual mutant counterexamples.
  run unsafeDispatchCounterexample = init.expect(safety).then(brokenDispatchBeforeCustody).expect(safety)
  run unsafeReleaseCounterexample = observedPrefix.then(brokenDropSurvivorBeforeWitness)
    .then(loseOwner).expect(safety)
  run unsafeDependencyCounterexample = terminalPrefix.then(checkCompaction).expect(safety)
    .then(admitChild(false)).expect(safety).then(brokenIgnoreDependency).expect(safety)
  run unsafeRetiredIdentityCounterexample = delayedOriginalAfterCompactionAndRetirementTest
    .then(brokenFreshRetiredEffect).expect(safety)
  run unsafeCoverageCounterexample = attachedCustodyPrefix
    .then(admitChild(false)).expect(safety).then(persistHead(2, 2, FullExact)).expect(safety)
    .then(captureCut).expect(safety)
    .then(certifyPage(1, s.cutRevision)).expect(safety).then(loadPage(1)).expect(safety)
    .then(brokenActivateLoadedPage).expect(safety)
}

/// Select this companion explicitly with --main; do not rerun/transfer the old
/// target evidence. Test-only mutants are never permitted protocol actions.
module dfs_custody_ack_witnesses {
  import dfs_custody_ack_contract.* from "../../contracts/dfs/availability_effect_custody"
  import dfs_custody_ack.* from "../../models/dfs/availability_effect_custody"
  import dfs_custody_ack_target.* from "../../checks/dfs/availability_effect_custody_target"
  pure val firstAck: FactAck = { transfer: transfer, seq: 11, digest: 1501, installSeq: 111 }
  pure val secondAck: FactAck = { transfer: transfer, seq: 15, digest: 1502, installSeq: 115 }

  run stagedPrefix = initAck.expect(ackSafety)
    .then(stagePage(1)).expect(ackSafety).then(emitStageAck(1)).expect(ackSafety)
    .then(stagePage(2)).expect(ackSafety).then(emitStageAck(2)).expect(ackSafety)
  run assignedPrefix = stagedPrefix.then(assumeAuthority).expect(ackSafety)
    .then(assignmentCas).expect(ackSafety)
  run installedPrefix = assignedPrefix.then(importQueue).expect(ackSafety)
    .then(certifyInstalledCursor).expect(ackSafety).then(emitInstalledAck).expect(ackSafety)
  run firstPrefix = installedPrefix.then(retainFact(first)).expect(ackSafety)
    .then(importFact(first, true)).expect(ackSafety)
  run secondPrefix = firstPrefix.then(retainFact(second)).expect(ackSafety)
    .then(importFact(second, true)).expect(ackSafety)
  run responsibilityPrefix = secondPrefix.then(assumeRecoveryResponsibility).expect(ackSafety)
    .then(retireResponsibility).expect(all { ackSafety, a.responsibilityRetired, not(a.rootReleased) })

  run stagingDoesNotInstallTest = stagedPrefix
    .then(all { not(installationCertified), a' = a }).expect(all { ackSafety, not(a.installedAckHistory),
      not(a.queueReadable), not(a.executor), not(a.payloadReady), not(a.rootReleased), not(a.responsibilityRetired) })
  run pagesCannotGrantAuthorityTest = stagedPrefix.then(all { not(mayAssign), a' = a }).expect(all { ackSafety, not(a.assigned) })
  run incompletePagesBlockAssignmentTest = initAck.then(assumeAuthority)
    .then(stagePage(1)).then(emitStageAck(1)).then(all { not(mayAssign), a' = a }).expect(ackSafety)
  run importBeforeAssignmentDisabledTest = stagedPrefix.then(all { not(mayImportQueue), a' = a }).expect(ackSafety)
  run orderedInstallationTest = assignedPrefix.then(all { not(installationCertified), a' = a })
    .then(importQueue).expect(ackSafety).then(all { not(installationCertified), a' = a })
    .then(certifyInstalledCursor).then(emitInstalledAck).expect(all { ackSafety, a.installedEmissions == 1 })
  run inventoryIsNotExecutablePayloadTest = installedPrefix.expect(all { ackSafety,
    a.installedOrigins == origins, not(a.payloadReady), not(a.executor), not(a.rootReleased) })
  run readablePageCannotRenewAckTest = initAck.then(replayReadablePage(1))
    .then(all { not(stageCertified(1)), a' = a }).expect(all { ackSafety, a.stagedEmissions == 0 })
    .then(syncPageLeafOnly(1)).then(all { not(stageCertified(1)), a' = a }).expect(all { ackSafety, a.leafSynced == Set(1) })
    .then(stagePage(1)).then(emitStageAck(1)).expect(all { ackSafety, a.stagedEmissions == 1 })
  run historicalAckSurvivesUncertifiedReplayTest = installedPrefix.then(crashReplay)
    .expect(all { ackSafety, a.installedAckHistory, a.installedEmissions == 1 })
    .then(all { not(stageCertified(1)), a' = a }).then(all { not(installationCertified), a' = a })
    .then(syncQueuePrefixOnly).then(all { not(installationCertified), a' = a }).expect(all { ackSafety, a.prefixSynced })
    .then(recertifyInstallation).then(emitInstalledAck)
    .expect(all { ackSafety, a.installedEmissions == 2 })
  run sourceMayRetainBeforeInstallationTest = stagedPrefix.then(retainFact(first)).expect(ackSafety)
    .then(all { not(mayImport(first, true)), a' = a }).then(assumeAuthority).then(assignmentCas)
    .then(importQueue).then(certifyInstalledCursor).then(emitInstalledAck)
    .then(importFact(first, true)).expect(all { ackSafety, a.frontierSeq == 11 })
  run partialOriginInstallCannotAcceptLateFactTest = assignedPrefix.then(retainFact(first)).then(importOneOrigin(1))
    .then(all { not(mayCertifyCursor), a' = a }).then(all { not(mayImport(first, true)), a' = a })
    .expect(all { ackSafety, a.installedOrigins == Set(1), a.imported == Set() })
    .then(importOneOrigin(2)).then(certifyInstalledCursor).then(emitInstalledAck)
    .then(importFact(first, true)).expect(ackSafety)
  run sourceFactIdentityImmutableAcrossOriginsTest = firstPrefix.then(all { not(mayRetain(first.with("origin", 2))), a' = a })
    .then(all { not(mayRetain(first.with("seq", 10))), a' = a }).expect(all { ackSafety, a.retained == Set(first) })
  run duplicateAfterFrontierAdvancedTest = secondPrefix.then(importFact(first, true))
    .expect(all { ackSafety, a.frontierSeq == 15, a.imported == Set(first, second) })
  run changedDuplicateRejectedTest = secondPrefix
    .then(all { not(mayImport(first.with("material", 599).with("digest", 1599), true)), a' = a })
    .then(all { not(mayImport(first.with("origin", 2).with("originDigest", 102), true)), a' = a })
    .then(all { not(mayImport(first.with("prevDigest", 999), true)), a' = a })
    .expect(all { ackSafety, a.frontierSeq == 15, a.imported == Set(first, second) })
  run wrongFactBindingsRejectedTest = installedPrefix.then(retainFact(first))
    .then(all { not(mayImport(first.with("transfer", transfer.with("handoff", 8)), true)), a' = a })
    .then(all { not(mayImport(first.with("transfer", transfer.with("inventory", 81)), true)), a' = a })
    .then(all { not(mayImport(first.with("transfer", transfer.with("installerQ", 21)), true)), a' = a })
    .then(all { not(mayImport(first.with("transfer", transfer.with("sourceQ", 12)), true)), a' = a })
    .then(all { not(mayImport(first.with("sourceQ", 12), true)), a' = a })
    .then(all { not(mayImport(first.with("source", 3), true)), a' = a })
    .then(all { not(mayImport(first.with("origin", 2), true)), a' = a })
    .then(all { not(mayImport(first.with("originDigest", 999), true)), a' = a })
    .then(all { not(mayImport(first, false)), a' = a })
    .expect(all { ackSafety, a.frontierSeq == cutSeq, a.imported == Set() })
  run wrongAnchorAndOutOfOrderRejectedTest = installedPrefix.then(retainFact(first)).then(retainFact(second))
    .then(all { not(mayImport(first.with("prevDigest", 81), true)), a' = a })
    .then(all { not(mayImport(first.with("prevCut", false), true)), a' = a })
    .then(all { not(mayImport(second, true)), a' = a }).expect(all { ackSafety, a.frontierSeq == cutSeq, a.imported == Set() })
    .then(importFact(first, true)).then(importFact(second, true))
    .expect(all { ackSafety, a.frontierSeq == 15, a.imported == Set(first, second) })
  run lateAckRequiresImportAndCertificationTest = installedPrefix.then(retainFact(first)).then(all { not(lateCertified(first)), a' = a })
    .then(importFact(first, true)).then(crashReplay).then(all { not(lateCertified(first)), a' = a })
    .expect(all { ackSafety, a.lateAcks == Set() })
    .then(recertifyInstallation).then(emitLateAck(first)).expect(ackSafety)
  run lostAckThenExactReplayTest = firstPrefix.then(emitLateAck(first))
    .expect(all { ackSafety, a.sourceAcked == Set(), a.sourceFrontier == cutSeq })
    .then(importFact(first, true)).then(emitLateAck(first)).then(persistSourceAck(firstAck))
    .then(persistSourceAck(firstAck)).expect(all { ackSafety, a.sourceFrontier == 11, a.sourceAcked == Set(11) })
  run crossTransferAckCannotAdvanceSourceTest = firstPrefix.then(emitLateAck(first))
    .then(all { not(mayPersistSourceAck(firstAck.with("transfer", transfer.with("handoff", 8)))), a' = a })
    .then(all { not(mayPersistSourceAck(firstAck.with("transfer", transfer.with("installerQ", 21)))), a' = a })
    .then(all { not(mayPersistSourceAck(firstAck.with("digest", 1599))), a' = a })
    .then(all { not(mayPersistSourceAck(firstAck.with("installSeq", 999))), a' = a })
    .expect(all { ackSafety, a.sourceFrontier == cutSeq, a.sourceAcked == Set() })
  run sourceAckChainNotGlobalMaximumTest = secondPrefix.then(emitLateAck(first)).then(emitLateAck(second))
    .then(all { not(mayPersistSourceAck(secondAck)), a' = a }).expect(all { ackSafety, a.sourceFrontier == cutSeq })
    .then(persistSourceAck(firstAck)).then(persistSourceAck(secondAck)).then(persistSourceAck(firstAck))
    .expect(all { ackSafety, a.sourceFrontier == 15, a.sourceAcked == Set(11, 15) })
  run ackAndRetirementDoNotCloseFutureTest = responsibilityPrefix
    .then(emitLateAck(first)).then(emitLateAck(second)).then(persistSourceAck(firstAck)).then(persistSourceAck(secondAck))
    .then(retainFact(future)).then(importFact(future, true)).then(emitLateAck(future))
    .expect(all { ackSafety, a.frontierSeq == 23, a.sourceFrontier == 15,
      a.responsibilityRetired, not(a.futureClosed), not(a.rootReleased) })

  action brokenInstalledFromStaging = a' = a.with("installedAckHistory", true)
    .with("installEmissionCertified", a.installEmissionCertified and installationCertified)
  action brokenReadableRenewal = a' = a.with("stagedEmissions", a.stagedEmissions + 1)
    .with("stageEmissionCertified", a.stageEmissionCertified and stageCertified(1))
  action brokenWrongPredecessor = a' = a.with("imported", a.imported.union(Set(second)))
    .with("frontierSeq", second.seq).with("frontierDigest", second.digest)
  action brokenRootFromResponsibility = a' = a.with("rootReleased", true)
  action brokenFutureClosure = a' = a.with("futureClosed", true)
  action brokenCrossTransferAck = a' = a.with("lateAcks", a.lateAcks.union(Set(firstAck.with("transfer", transfer.with("installerQ", 21)))))
  action brokenConflictingIdentity = a' = a.with("imported", a.imported.union(Set(second.with("origin", 1).with("originDigest", 101))))
  action brokenFrontier = a' = a.with("frontierSeq", 23).with("frontierDigest", 999)
  action brokenSourceAckFrontier = a' = a.with("sourceFrontier", 23).with("sourceFrontierDigest", 1503)

  run installedAckSensitivityTest = stagedPrefix.then(brokenInstalledFromStaging).expect(not(installSafety))
  run readableRenewalSensitivityTest = initAck.then(replayReadablePage(1)).then(brokenReadableRenewal).expect(not(stageSafety))
    .then(stagePage(1)).then(emitStageAck(1)).expect(not(stageSafety))
  run predecessorSensitivityTest = installedPrefix.then(retainFact(first)).then(retainFact(second))
    .then(brokenWrongPredecessor).expect(not(chainSafety))
    .then(brokenConflictingIdentity).expect(not(immutableFactIds))
    .then(brokenFrontier).expect(not(frontierSafety))
  run rootRetirementSensitivityTest = responsibilityPrefix.then(brokenRootFromResponsibility).expect(not(evidenceOnly))
  run futureClosureSensitivityTest = firstPrefix.then(emitLateAck(first)).then(brokenFutureClosure).expect(not(futureSafety))
    .then(brokenSourceAckFrontier).expect(not(sourceAckChainSafety))
  run crossTransferAckSensitivityTest = firstPrefix.then(brokenCrossTransferAck).expect(not(lateAckSafety))

  run unsafeInstalledAckCounterexample = stagedPrefix.then(brokenInstalledFromStaging).expect(ackSafety)
  run unsafeReadableRenewalCounterexample = initAck.then(replayReadablePage(1)).then(brokenReadableRenewal).expect(ackSafety)
  run unsafePredecessorCounterexample = installedPrefix.then(retainFact(first)).then(retainFact(second))
    .then(brokenWrongPredecessor).expect(ackSafety)
  run unsafeRootRetirementCounterexample = responsibilityPrefix.then(brokenRootFromResponsibility).expect(ackSafety)
  run unsafeFutureClosureCounterexample = firstPrefix.then(emitLateAck(first)).then(brokenFutureClosure).expect(ackSafety)
  run unsafeCrossTransferAckCounterexample = firstPrefix.then(brokenCrossTransferAck).expect(ackSafety)
}
