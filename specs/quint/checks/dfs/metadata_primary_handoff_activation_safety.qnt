/// Safety and reachability checks for proof-bound metadata-primary rotation.
module dfs_metadata_primary_handoff_activation_safety {
  import common.* from "../../lib/common"
  import dfs_metadata_primary_handoff_activation.* from "../../models/dfs/metadata_primary_handoff_activation"

  def atMostOneWritableAuthority: bool =
    not(all { retiringWritable, promotedWritable })

  def pendingBeforeFenceRetainsOldAuthority: bool = all {
    handoffPending,
    not(retiringFenced),
  } implies retiringWritable

  def fenceRevokesRetiringAuthority: bool =
    retiringFenced implies not(retiringWritable)

  def activationRequiresExactProofAndPromotionMarker: bool =
    activePrimary == N2 implies all {
      not(handoffPending),
      retiringFenced,
      exactUnionAcknowledged,
      promotionEchoVerified,
      completionProofExact,
      promotedMarker != MarkerAbsent,
      retiringMarkerActivated,
    }

  def promotedBindingRequiresCompletedExactEvidence: bool =
    promotedLaneBound implies all {
      activePrimary == N2,
      not(handoffPending),
      completionProofExact,
      promotedMarker == MarkerActivatedExact,
      promotedLaneOpen,
    }

  def promotedAcknowledgementRequiresDurableMarker: bool =
    acknowledged.contains(N2) implies all {
      promotedMarker != MarkerAbsent,
      promotionEchoVerified,
    }

  def partialHandoffNeverActivatesOrFallsBack: bool = all {
    handoffPending,
    not(exactUnionAcknowledged),
  } implies all {
    activePrimary == N1,
    not(completionProofExact),
    not(ordinaryFallbackUsed),
  }

  def retiringLaneCannotConsumePromotedProof: bool =
    activePrimary == N2 implies not(retiringLaneBound)

  def directAndMixedChangesNeverActivate: bool = any {
    lastTransition == DirectKvTransferRejected,
    lastTransition == MixedReconfigurationRejected,
  } implies activePrimary == N1

  def successfulNewWriteHasUniquePromotedAuthority: bool =
    newPrimaryWriteSucceeded implies all {
      promotedWritable,
      not(retiringWritable),
    }

  def witnessDirectKvTransferRemainsRejected: bool = all {
    lastTransition == DirectKvTransferRejected,
    directKvTransferWasRejected,
    activePrimary == N1,
  }

  def witnessMixedPrimaryMembershipChangeRejected: bool = all {
    lastTransition == MixedReconfigurationRejected,
    mixedReconfigurationWasRejected,
    activePrimary == N1,
  }

  def witnessLateOldWriteCapturedBeforeFence: bool = all {
    lastTransition == LateRetiringWriteCommitted,
    handoffPending,
    not(retiringFenced),
    lateRetiringWriteCaptured,
    retiringWritable,
  }

  def witnessFenceCarriesLateWrite: bool = all {
    lastTransition == RetiringPrimaryFenced,
    lateRetiringWriteCaptured,
    acknowledged == Set(N1),
    not(retiringWritable),
  }

  def witnessPartialExactUnionRemainsPending: bool = all {
    lastTransition == PartialActivationRejected,
    handoffPending,
    not(exactUnionAcknowledged),
    activePrimary == N1,
    not(ordinaryFallbackUsed),
  }

  def witnessPromotionAckWithoutMarkerRejected: bool = all {
    lastTransition == PromotionAckRejectedWithoutMarker,
    promotedMarker == MarkerAbsent,
    not(acknowledged.contains(N2)),
    not(promotionEchoVerified),
  }

  def witnessPromotionAckFollowsDurableMarker: bool = all {
    lastTransition == PromotedReplicaAcknowledged,
    promotedMarker == MarkerPendingExact,
    acknowledged.contains(N2),
    promotionEchoVerified,
  }

  def witnessTargetCrashAfterAckBeforeCas: bool = all {
    lastTransition == PromotedLaneCrashed,
    handoffPending,
    exactUnionAcknowledged,
    promotedMarker == MarkerPendingExact,
    not(promotedLaneOpen),
    not(promotedLaneBound),
  }

  def witnessTargetReopenRetainsMarkerBeforeCas: bool = all {
    lastTransition == PromotedLaneReopened,
    handoffPending,
    promotedMarker == MarkerPendingExact,
    promotedLaneOpen,
    not(promotedLaneBound),
  }

  def witnessActivationCarriesExactUnionAndLateWrite: bool = all {
    lastTransition == HandoffActivated,
    activePrimary == N2,
    exactUnionAcknowledged,
    completionProofExact,
    lateRetiringWriteCaptured,
  }

  def witnessLostCasResponseStillRecoverable: bool = all {
    lastTransition == ActivationResponseLost,
    activationResponseWasLost,
    activePrimary == N2,
    completionProofExact,
    not(promotedLaneBound),
  }

  def witnessPromotedLaneBindsAfterExactCompletion: bool = all {
    lastTransition == PromotedLaneBound,
    promotedWritable,
    not(retiringWritable),
  }

  def witnessOldPrimaryRestartRemainsFenced: bool = all {
    lastTransition == RetiringLaneRestarted,
    activePrimary == N2,
    retiringMarkerActivated,
    not(retiringLaneBound),
  }

  def witnessStaleOldLaneCannotConsumePromotionProof: bool = all {
    lastTransition == RetiringLaneRebindRejected,
    completionProofExact,
    not(retiringLaneBound),
  }

  def witnessSupersededInactiveMarkerCanBeReplaced: bool = all {
    lastTransition == SupersededMarkerReplaced,
    supersededMarkerWasReplaced,
    retiringMarkerActivated,
    not(retiringLaneBound),
  }

  def witnessNewPrimaryWriteSucceedsWithoutConflict: bool = all {
    lastTransition == NewPrimaryWriteCommitted,
    newPrimaryWriteSucceeded,
    promotedWritable,
    not(retiringWritable),
  }

  def safety: bool = all {
    atMostOneWritableAuthority,
    pendingBeforeFenceRetainsOldAuthority,
    fenceRevokesRetiringAuthority,
    activationRequiresExactProofAndPromotionMarker,
    promotedBindingRequiresCompletedExactEvidence,
    promotedAcknowledgementRequiresDurableMarker,
    partialHandoffNeverActivatesOrFallsBack,
    retiringLaneCannotConsumePromotedProof,
    directAndMixedChangesNeverActivate,
    successfulNewWriteHasUniquePromotedAuthority,
  }
}
