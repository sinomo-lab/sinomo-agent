/// Safety and reachability checks for exact-union membership-handoff sync.
module dfs_metadata_handoff_union_sync_safety {
  import common.* from "../../lib/common"
  import dfs_metadata_handoff_union_sync.* from "../../models/dfs/metadata_handoff_union_sync"

  def acknowledgementSetsStayInsideRequiredUnion: bool = all {
    oldAcknowledged.subseteq(RequiredUnion),
    fixedAcknowledged.subseteq(RequiredUnion),
    oldAcknowledged.contains(N1),
    fixedAcknowledged.contains(N1),
    removalAcknowledged.subseteq(RemovalRequiredUnion),
    removalAcknowledged.contains(N1),
  }

  def activationRequiresExactUnion: bool = all {
    oldActivated implies oldAcknowledged == RequiredUnion,
    fixedActivated implies fixedAcknowledged == RequiredUnion,
    removalActivated implies removalAcknowledged == RemovalRequiredUnion,
  }

  def pendingAndActivatedStatesAreDisjoint: bool = all {
    oldHandoffPending iff not(oldActivated),
    fixedHandoffPending iff not(fixedActivated),
    removalHandoffPending iff not(removalActivated),
  }

  /// A partial handoff proof is availability work, not a terminal result.
  def fixedPartialUnionRemainsPendingAndRetryEnabled: bool = all {
    fixedHandoffPending,
    not(isExactUnion(fixedAcknowledged)),
  } implies all {
    fixedRetryEnabled,
    not(fixedActivated),
  }

  /// The hardened handoff primitive is full-union-only. Ordinary POSIX sync
  /// retains its fallback in its separate proof boundary.
  def fixedHandoffNeverUsesOrdinaryFallback: bool =
    not(fixedOrdinaryFallbackUsed)

  /// Both algorithms observe the same partial-union fallback opportunity. The
  /// old path omits sync purpose; the fixed path rejects fallback for membership
  /// handoff purpose (and this replacement case also has unequal member sets).
  /// This prevents the no-fallback claim from being an unexercised flag.
  def sharedFallbackEvaluationSeparatesPurposes: bool =
    any {
      lastTransition == OldFallbackRejectedUnconfiguredNode,
      lastTransition == OldFallbackReturnedPartialCertificate,
    } implies all {
      fallbackEvaluationObserved,
      oldOrdinaryFallbackUsed,
      not(fixedOrdinaryFallbackUsed),
      HandoffTargetMembership == RequiredUnion,
      ActiveAssignment == RetiringAssignment,
      HandoffTargetMembership != ActiveAssignment,
      oldOrdinaryFallbackEligible(previousOldAcknowledged),
      not(ordinaryFallbackEligibleForPurpose(
        MembershipHandoffSync,
        HandoffTargetMembership,
        ActiveAssignment,
        RequiredUnion,
        previousFixedAcknowledged
      )),
    }

  /// Every fixed-path transition preserves already observed fresh evidence.
  def fixedAcknowledgementsNeverReset: bool =
    previousFixedAcknowledged.subseteq(fixedAcknowledged)

  /// Removal-only changes do not make the sync ordinary. Even though the
  /// target union equals the active retiring assignment, purpose keeps the
  /// handoff on the full-shard path while preserving ordinary fallback for an
  /// actual ordinary metadata sync over the same sets.
  def removalOnlyFallbackIsSeparatedByPurpose: bool =
    removalFallbackEvaluationObserved implies all {
      RemovalDesiredAssignment.subseteq(RemovalRetiringAssignment),
      RemovalDesiredAssignment != RemovalRetiringAssignment,
      RemovalRequiredUnion == RemovalRetiringAssignment,
      RemovalTargetMembership == RemovalActiveAssignment,
      isPartialRemovalOrdinaryQuorum(removalFallbackAcknowledged),
      ordinaryFallbackEligibleForPurpose(
        OrdinaryMetadataSync,
        RemovalTargetMembership,
        RemovalActiveAssignment,
        RemovalRequiredUnion,
        removalFallbackAcknowledged
      ),
      not(ordinaryFallbackEligibleForPurpose(
        MembershipHandoffSync,
        RemovalTargetMembership,
        RemovalActiveAssignment,
        RemovalRequiredUnion,
        removalFallbackAcknowledged
      )),
      removalOrdinaryFallbackWouldBeUsed,
      not(removalHandoffFallbackUsed),
    }

  def removalOnlyPartialProofRemainsPending: bool = all {
    removalHandoffPending,
    not(isExactRemovalUnion(removalAcknowledged)),
  } implies all {
    removalRetryEnabled,
    not(removalActivated),
    not(removalFullShardCertificateObserved),
  }

  def removalOnlyAcknowledgementsNeverReset: bool =
    previousRemovalAcknowledged.subseteq(removalAcknowledged)

  def removalOnlyActivationRequiresFullShardCertificate: bool = all {
    removalActivated implies all {
      removalAcknowledged == RemovalRequiredUnion,
      removalAcknowledged == RemovalRetiringAssignment,
      removalFullShardCertificateObserved,
      not(removalHandoffPending),
      not(removalRetryEnabled),
      not(removalHandoffFallbackUsed),
    },
    removalFullShardCertificateObserved implies removalActivated,
  }

  def oldFallbackIsOnlyTriedOnPartialQuorum: bool =
    any {
      lastTransition == OldFallbackRejectedUnconfiguredNode,
      lastTransition == OldFallbackReturnedPartialCertificate,
    } implies all {
      previousOldAcknowledged.size() >= OrdinaryQuorumSize,
      previousOldAcknowledged != RequiredUnion,
      oldAcknowledged == LocalPrimaryAcknowledgement,
      oldHandoffPending,
      oldRetryEnabled,
      not(oldActivated),
    }

  def rejectedOldFallbackContainsDesiredOnlyNode: bool =
    lastTransition == OldFallbackRejectedUnconfiguredNode implies all {
      previousOldAcknowledged.contains(N3),
      not(RetiringAssignment.contains(N3)),
      not(retiringWatermarkAccepts(previousOldAcknowledged)),
    }

  def returnedOldFallbackIsStillNotAnActivationProof: bool =
    lastTransition == OldFallbackReturnedPartialCertificate implies all {
      retiringWatermarkAccepts(previousOldAcknowledged),
      previousOldAcknowledged != RequiredUnion,
      oldHandoffPending,
      not(oldActivated),
    }

  def fixedActivationConsumesRetryOnlyAfterExactUnion: bool =
    fixedActivated implies all {
      fixedAcknowledged == RequiredUnion,
      not(fixedHandoffPending),
      not(fixedRetryEnabled),
    }

  /// Old-fallback counterexample: N3 makes `{N1,N3}` an ordinary quorum, but
  /// N3 is outside the retiring assignment. The fallback rejects and resets
  /// old evidence while the fixed path retains the same partial union.
  def witnessOldFallbackRejectsDesiredNodeAndResets: bool = all {
    lastTransition == OldFallbackRejectedUnconfiguredNode,
    previousOldAcknowledged == Set(N1, N3),
    oldAcknowledged == Set(N1),
    fixedAcknowledged == Set(N1, N3),
    fixedHandoffPending,
    fixedRetryEnabled,
    fallbackEvaluationObserved,
    oldOrdinaryFallbackUsed,
    not(fixedOrdinaryFallbackUsed),
  }

  def witnessOldConfiguredQuorumStillReturnsPartial: bool = all {
    lastTransition == OldFallbackReturnedPartialCertificate,
    previousOldAcknowledged == Set(N1, N2),
    oldAcknowledged == Set(N1),
    fixedAcknowledged == Set(N1, N2),
    fixedHandoffPending,
    fixedRetryEnabled,
    fallbackEvaluationObserved,
    oldOrdinaryFallbackUsed,
    not(fixedOrdinaryFallbackUsed),
  }

  def witnessFixedPartialUnionWaitsWithoutFallback: bool = all {
    lastTransition == FreshAcknowledgementRecorded,
    fixedAcknowledged.size() == OrdinaryQuorumSize,
    fixedAcknowledged != RequiredUnion,
    fixedHandoffPending,
    fixedRetryEnabled,
    not(fixedOrdinaryFallbackUsed),
  }

  /// After the old `{N1,N3}` fallback reset, a later N2 ACK completes the
  /// retained fixed union while the old attempt has only `{N1,N2}`.
  def witnessLateFinalAckCompletesRetainedUnion: bool = all {
    lastTransition == FreshAcknowledgementRecorded,
    lastAcknowledgingNode == N2,
    fixedAcknowledged == RequiredUnion,
    oldAcknowledged == Set(N1, N2),
    fixedHandoffPending,
    fixedRetryEnabled,
  }

  def witnessFixedExactUnionActivates: bool = all {
    lastTransition == FixedExactUnionActivated,
    fixedActivated,
    fixedAcknowledged == RequiredUnion,
    not(fixedHandoffPending),
    not(fixedRetryEnabled),
  }

  def witnessOldExactUnionCanActivateWithoutPrematureFallback: bool = all {
    lastTransition == OldExactUnionActivated,
    oldActivated,
    oldAcknowledged == RequiredUnion,
  }

  /// N2 first creates an ordinary quorum in a removal-only handoff. The same
  /// member sets make ordinary fallback eligible, proving the purpose guard is
  /// exercised rather than a set-inequality accident. The handoff retains that
  /// partial evidence, then N3 completes the exact retiring union and only the
  /// full-shard certificate activates it.
  def witnessRemovalOnlyHandoffRequiresFullRetiringUnion: bool = all {
    lastTransition == RemovalExactUnionActivated,
    removalFallbackEvaluationObserved,
    removalFallbackAcknowledged == Set(N1, N2),
    RemovalDesiredAssignment == Set(N1, N2),
    RemovalRetiringAssignment == Set(N1, N2, N3),
    RemovalTargetMembership == RemovalActiveAssignment,
    removalOrdinaryFallbackWouldBeUsed,
    not(removalHandoffFallbackUsed),
    removalAcknowledged == RemovalRequiredUnion,
    removalAcknowledged == RemovalRetiringAssignment,
    removalFullShardCertificateObserved,
    removalActivated,
    not(removalHandoffPending),
    not(removalRetryEnabled),
  }

  def safety: bool = all {
    acknowledgementSetsStayInsideRequiredUnion,
    activationRequiresExactUnion,
    pendingAndActivatedStatesAreDisjoint,
    fixedPartialUnionRemainsPendingAndRetryEnabled,
    fixedHandoffNeverUsesOrdinaryFallback,
    sharedFallbackEvaluationSeparatesPurposes,
    fixedAcknowledgementsNeverReset,
    removalOnlyFallbackIsSeparatedByPurpose,
    removalOnlyPartialProofRemainsPending,
    removalOnlyAcknowledgementsNeverReset,
    removalOnlyActivationRequiresFullShardCertificate,
    oldFallbackIsOnlyTriedOnPartialQuorum,
    rejectedOldFallbackContainsDesiredOnlyNode,
    returnedOldFallbackIsStillNotAnActivationProof,
    fixedActivationConsumesRetryOnlyAfterExactUnion,
  }
}
