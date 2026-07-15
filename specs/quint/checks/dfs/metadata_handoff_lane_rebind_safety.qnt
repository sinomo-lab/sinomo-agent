/// Safety and reachability checks for completed-handoff lane rebinding.
module dfs_metadata_handoff_lane_rebind_safety {
  import dfs_metadata_handoff_lane_rebind.* from "../../models/dfs/metadata_handoff_lane_rebind"

  def prefixRelationIsWellFormed: bool =
    if (prefixRelation == PrefixUnavailable) {
      all { markerSequence == 0, currentSequence == 0 }
    } else if (prefixRelation == PrefixMarkerEqual) {
      all { markerSequence > 0, currentSequence == markerSequence }
    } else if (prefixRelation == PrefixContiguousExtension) {
      all {
        markerSequence > 0,
        currentSequence > markerSequence,
        postActivationAppendCommitted,
      }
    } else {
      all {
        prefixRelation == PrefixDivergent,
        markerSequence > 0,
        currentSequence >= markerSequence,
        not(postActivationAppendCommitted),
      }
    }

  def activeFreshLaneHasExactEvidence: bool =
    freshLaneBinding == LaneActive implies exactRebindEvidence

  def successfulFreshBootstrapHasExactBoundLane: bool =
    freshBootstrapOutcome == BootstrapSucceeded implies all {
      freshLaneBinding == LaneActive,
      exactRebindEvidence,
    }

  def rejectedRebindDoesNotBindLane: bool =
    lastTransition == FreshLaneRebindRejected implies all {
      freshLaneBinding == LaneUnbound,
      not(exactRebindEvidence),
    }

  def divergentCurrentPrefixNeverBindsFreshLane: bool =
    prefixRelation == PrefixDivergent implies freshLaneBinding != LaneActive

  def postActivationAppendIsAContiguousExtension: bool =
    lastTransition == PostActivationDurableAppendCommitted implies all {
      prefixRelation == PrefixContiguousExtension,
      currentSequence == markerSequence + 1,
      currentPrefixMatchesMarkerOrExtension,
      exactRebindEvidence,
      freshLaneBinding == LaneActive,
    }

  def postAppendReopenDropsOnlyProcessBinding: bool =
    lastTransition == FreshLaneReopenedAfterAppend implies all {
      postActivationAppendCommitted,
      prefixRelation == PrefixContiguousExtension,
      currentSequence == markerSequence + 1,
      exactRebindEvidence,
      freshLaneBinding == LaneUnbound,
      freshBootstrapOutcome == BootstrapNotAttempted,
    }

  /// Completing the transition or rebinding another handle cannot directly
  /// bind the cached pre-completion lane. That lane may become active only at
  /// its own exact changed-generation proof boundary.
  def cachedLaneBindsOnlyFromExactChangedGenerationProof: bool =
    staleLaneBinding == LaneActive implies all {
      cachedLaneProofBound,
      exactChangedGenerationEvidenceForCachedLane,
    }

  def cachedGenerationObservationNeverBindsLane: bool =
    lastTransition == CachedGenerationObserved implies all {
      staleLaneBinding == LaneUnbound,
      not(cachedLaneProofBound),
    }

  def invalidGenerationEvidenceCannotBindCachedLane: bool =
    cachedGenerationEvidence != GenerationChangedExact implies all {
      staleLaneBinding != LaneActive,
      not(cachedLaneProofBound),
    }

  def cachedRebindOrReopenRequiresExactChangedGenerationProof: bool = all {
    lastTransition == CachedLaneRebindAccepted implies all {
      staleLaneBinding == LaneActive,
      cachedLaneProofBound,
      not(cachedLaneReopened),
      exactChangedGenerationEvidenceForCachedLane,
    },
    lastTransition == CachedLaneReopenAccepted implies all {
      staleLaneBinding == LaneActive,
      cachedLaneProofBound,
      cachedLaneReopened,
      exactChangedGenerationEvidenceForCachedLane,
    },
    any {
      lastTransition == CachedLaneRebindRejected,
      lastTransition == CachedLaneReopenRejected,
    } implies all {
      staleLaneBinding == LaneUnbound,
      not(cachedLaneProofBound),
      not(exactChangedGenerationEvidenceForCachedLane),
    },
  }

  def completionNeverDirectlyBindsCachedLane: bool =
    lastTransition == ExactHandoffCompleted implies all {
      staleLaneBinding != LaneActive,
      not(cachedLaneProofBound),
    }

  def successfulCachedMutationHasExactProofBoundLane: bool =
    staleBootstrapOutcome == BootstrapSucceeded implies all {
      staleLaneBinding == LaneActive,
      cachedLaneProofBound,
      exactChangedGenerationEvidenceForCachedLane,
    }

  def cachedAuthorityGenerationStateIsWellFormed: bool = all {
    currentAuthorityGeneration >= 1,
    staleLaneBinding != LaneClosed implies all {
      cachedOpenAuthorityGeneration >= 1,
      cachedOpenAuthorityGeneration <= currentAuthorityGeneration,
    },
    cachedLaneReopened implies all {
      cachedLaneProofBound,
      staleLaneBinding == LaneActive,
    },
  }

  def witnessExactHandoffCompletesWithoutBindingFreshLane: bool = all {
    lastTransition == ExactHandoffCompleted,
    exactRebindEvidence,
    prefixRelation == PrefixMarkerEqual,
    freshLaneBinding == LaneClosed,
  }

  def witnessFreshLaneOpensUnboundBehindActivatedMarker: bool = all {
    lastTransition == FreshLaneOpened,
    exactRebindEvidence,
    prefixRelation == PrefixMarkerEqual,
    freshLaneBinding == LaneUnbound,
  }

  /// This is the Docker failure cut: the old CLI path went directly from a
  /// fresh backend to namespace bootstrap.
  def witnessDirectBootstrapRejectsUnboundFreshLane: bool = all {
    lastTransition == FreshBootstrapAttempted,
    exactRebindEvidence,
    prefixRelation == PrefixMarkerEqual,
    freshLaneBinding == LaneUnbound,
    freshBootstrapOutcome == BootstrapRejected,
  }

  def witnessExactEvidenceRebindsFreshLane: bool = all {
    lastTransition == FreshLaneRebindAccepted,
    exactRebindEvidence,
    prefixRelation == PrefixMarkerEqual,
    freshLaneBinding == LaneActive,
  }

  def witnessBootstrapSucceedsAfterExactRebind: bool = all {
    lastTransition == FreshBootstrapAttempted,
    exactRebindEvidence,
    prefixRelation == PrefixMarkerEqual,
    freshLaneBinding == LaneActive,
    freshBootstrapOutcome == BootstrapSucceeded,
  }

  def witnessWrongProofCannotRebind: bool = all {
    lastTransition == FreshLaneRebindRejected,
    controlCompleted,
    markerActivated,
    activeEpochMatchesMarker,
    not(proofMatchesMarkerAndActive),
  }

  def witnessWrongActiveEpochCannotRebind: bool = all {
    lastTransition == FreshLaneRebindRejected,
    controlCompleted,
    markerActivated,
    not(activeEpochMatchesMarker),
    proofMatchesMarkerAndActive,
  }

  def witnessPendingMarkerCannotRebind: bool = all {
    lastTransition == FreshLaneRebindRejected,
    controlCompleted,
    not(markerActivated),
    activeEpochMatchesMarker,
    proofMatchesMarkerAndActive,
  }

  def witnessDivergentCurrentPrefixCannotRebind: bool = all {
    lastTransition == FreshLaneRebindRejected,
    controlCompleted,
    markerActivated,
    activeEpochMatchesMarker,
    proofMatchesMarkerAndActive,
    prefixRelation == PrefixDivergent,
    currentSequence == markerSequence + 1,
    freshLaneBinding == LaneUnbound,
  }

  def witnessPostAppendFreshLaneRebindsFromContiguousExtension: bool = all {
    lastTransition == FreshLaneRebindAccepted,
    postActivationAppendCommitted,
    prefixRelation == PrefixContiguousExtension,
    currentSequence == markerSequence + 1,
    exactRebindEvidence,
    freshLaneBinding == LaneActive,
  }

  def witnessPostAppendBootstrapSucceeds: bool = all {
    lastTransition == FreshBootstrapAttempted,
    postActivationAppendCommitted,
    prefixRelation == PrefixContiguousExtension,
    currentSequence == markerSequence + 1,
    exactRebindEvidence,
    freshLaneBinding == LaneActive,
    freshBootstrapOutcome == BootstrapSucceeded,
  }

  def witnessPreActivationLaneRemainsUnboundAfterCompletion: bool = all {
    lastTransition == ExactHandoffCompleted,
    staleLaneBinding == LaneUnbound,
    exactRebindEvidence,
    cachedOpenAuthorityGeneration == 1,
    currentAuthorityGeneration == 2,
    completionNeverDirectlyBindsCachedLane,
  }

  def witnessStaleBootstrapRejectedAfterCompletion: bool = all {
    lastTransition == StaleBootstrapAttempted,
    exactRebindEvidence,
    staleLaneBinding == LaneUnbound,
    staleBootstrapOutcome == BootstrapRejected,
  }

  def witnessCachedLaneOpenedUnboundBeforeCompletion: bool = all {
    lastTransition == StaleLaneOpened,
    not(controlCompleted),
    staleLaneBinding == LaneUnbound,
    cachedOpenAuthorityGeneration == currentAuthorityGeneration,
    cachedGenerationEvidence == GenerationNotObserved,
    not(cachedLaneProofBound),
  }

  def witnessChangedGenerationObservationDoesNotBindCachedLane: bool = all {
    lastTransition == CachedGenerationObserved,
    cachedGenerationEvidence == GenerationChangedExact,
    currentAuthorityGeneration > cachedOpenAuthorityGeneration,
    staleLaneBinding == LaneUnbound,
    cachedGenerationObservationNeverBindsLane,
  }

  def witnessDirectCachedMutationRejectedAfterGenerationChange: bool = all {
    lastTransition == StaleBootstrapAttempted,
    cachedGenerationEvidence == GenerationChangedExact,
    currentAuthorityGeneration > cachedOpenAuthorityGeneration,
    staleLaneBinding == LaneUnbound,
    staleBootstrapOutcome == BootstrapRejected,
  }

  def witnessMissingGenerationEvidenceCannotBind: bool = all {
    lastTransition == CachedLaneRebindRejected,
    cachedGenerationEvidence == GenerationMissing,
    staleLaneBinding == LaneUnbound,
    not(cachedLaneProofBound),
  }

  def witnessUnchangedGenerationEvidenceCannotBind: bool = all {
    lastTransition == CachedLaneRebindRejected,
    cachedGenerationEvidence == GenerationUnchanged,
    staleLaneBinding == LaneUnbound,
    not(cachedLaneProofBound),
  }

  def witnessMismatchedGenerationEvidenceCannotBind: bool = all {
    lastTransition == CachedLaneRebindRejected,
    cachedGenerationEvidence == GenerationChangedMismatched,
    staleLaneBinding == LaneUnbound,
    not(cachedLaneProofBound),
  }

  def witnessExactChangedGenerationRebindsCachedLane: bool = all {
    lastTransition == CachedLaneRebindAccepted,
    cachedGenerationEvidence == GenerationChangedExact,
    currentAuthorityGeneration > cachedOpenAuthorityGeneration,
    staleLaneBinding == LaneActive,
    cachedLaneProofBound,
    not(cachedLaneReopened),
    exactChangedGenerationEvidenceForCachedLane,
  }

  def witnessCachedMutationSucceedsAfterExactRebind: bool = all {
    lastTransition == StaleBootstrapAttempted,
    staleLaneBinding == LaneActive,
    cachedLaneProofBound,
    not(cachedLaneReopened),
    staleBootstrapOutcome == BootstrapSucceeded,
    successfulCachedMutationHasExactProofBoundLane,
  }

  def witnessExactChangedGenerationReopensCachedLane: bool = all {
    lastTransition == CachedLaneReopenAccepted,
    cachedGenerationEvidence == GenerationChangedExact,
    currentAuthorityGeneration > cachedOpenAuthorityGeneration,
    staleLaneBinding == LaneActive,
    cachedLaneProofBound,
    cachedLaneReopened,
    exactChangedGenerationEvidenceForCachedLane,
  }

  def witnessCachedMutationSucceedsAfterExactReopen: bool = all {
    lastTransition == StaleBootstrapAttempted,
    staleLaneBinding == LaneActive,
    cachedLaneProofBound,
    cachedLaneReopened,
    staleBootstrapOutcome == BootstrapSucceeded,
    successfulCachedMutationHasExactProofBoundLane,
  }

  def safety: bool = all {
    prefixRelationIsWellFormed,
    activeFreshLaneHasExactEvidence,
    successfulFreshBootstrapHasExactBoundLane,
    rejectedRebindDoesNotBindLane,
    divergentCurrentPrefixNeverBindsFreshLane,
    postActivationAppendIsAContiguousExtension,
    postAppendReopenDropsOnlyProcessBinding,
    cachedLaneBindsOnlyFromExactChangedGenerationProof,
    cachedGenerationObservationNeverBindsLane,
    invalidGenerationEvidenceCannotBindCachedLane,
    cachedRebindOrReopenRequiresExactChangedGenerationProof,
    completionNeverDirectlyBindsCachedLane,
    successfulCachedMutationHasExactProofBoundLane,
    cachedAuthorityGenerationStateIsWellFormed,
  }
}
