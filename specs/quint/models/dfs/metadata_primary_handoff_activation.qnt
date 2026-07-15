/// Proof-bound metadata-primary rotation over an unchanged shard membership.
///
/// N1 is the retiring primary, N2 is an existing replica promoted to primary,
/// and N3 remains a replica. The configured node set never changes. Pending
/// control-plane intent alone does not fence N1: a write already admitted by
/// the retiring generation may finish before N1 installs its durable fence.
/// Activation then requires the exact fenced prefix at every member, including
/// a handoff-bound acknowledgement from N2 written only after N2 persists its
/// own local marker. Completion does not mutate process-local bindings; N2
/// must freshly verify the active config, completion proof, and marker before
/// it can publish, while N1 remains fenced.
module dfs_metadata_primary_handoff_activation {
  import common.* from "../../lib/common"

  type PromotionMarker = MarkerAbsent | MarkerPendingExact | MarkerActivatedExact
  type PrimaryHandoffTransition =
    | PrimaryHandoffInitialized
    | DirectKvTransferRejected
    | MixedReconfigurationRejected
    | HandoffBegan
    | LateRetiringWriteCommitted
    | RetiringPrimaryFenced
    | ReplicaAcknowledged
    | PromotionAckRejectedWithoutMarker
    | PromotionMarkerPersisted
    | PromotedReplicaAcknowledged
    | PartialActivationRejected
    | PromotedLaneCrashed
    | PromotedLaneReopened
    | HandoffActivated
    | ActivationResponseLost
    | PromotedLaneBound
    | RetiringLaneRestarted
    | RetiringLaneRebindRejected
    | SupersededMarkerReplaced
    | NewPrimaryWriteCommitted
    | PrimaryHandoffStuttered

  pure val ConfiguredMembers: Set[Node] = Set(N1, N2, N3)

  var activePrimary: Node
  var handoffPending: bool
  var retiringFenced: bool
  var retiringLaneBound: bool
  var retiringMarkerActivated: bool
  var promotedMarker: PromotionMarker
  var promotedLaneOpen: bool
  var promotedLaneBound: bool
  var acknowledged: Set[Node]
  var promotionEchoVerified: bool
  var completionProofExact: bool
  var lateRetiringWriteCaptured: bool
  var directKvTransferWasRejected: bool
  var mixedReconfigurationWasRejected: bool
  var ordinaryFallbackUsed: bool
  var activationResponseWasLost: bool
  var supersededMarkerWasReplaced: bool
  var newPrimaryWriteSucceeded: bool
  var lastTransition: PrimaryHandoffTransition

  pure def exactUnionAcknowledged: bool = acknowledged == ConfiguredMembers

  pure def retiringWritable: bool = all {
    activePrimary == N1,
    retiringLaneBound,
    not(retiringFenced),
  }

  pure def promotedWritable: bool = all {
    activePrimary == N2,
    promotedLaneOpen,
    promotedLaneBound,
    promotedMarker == MarkerActivatedExact,
    completionProofExact,
    not(handoffPending),
  }

  action init = all {
    activePrimary' = N1,
    handoffPending' = false,
    retiringFenced' = false,
    retiringLaneBound' = true,
    retiringMarkerActivated' = false,
    promotedMarker' = MarkerAbsent,
    promotedLaneOpen' = true,
    promotedLaneBound' = false,
    acknowledged' = Set(),
    promotionEchoVerified' = false,
    completionProofExact' = false,
    lateRetiringWriteCaptured' = false,
    directKvTransferWasRejected' = false,
    mixedReconfigurationWasRejected' = false,
    ordinaryFallbackUsed' = false,
    activationResponseWasLost' = false,
    supersededMarkerWasReplaced' = false,
    newPrimaryWriteSucceeded' = false,
    lastTransition' = PrimaryHandoffInitialized,
  }

  action rejectDirectKvTransfer = all {
    activePrimary == N1,
    activePrimary' = activePrimary,
    handoffPending' = handoffPending,
    retiringFenced' = retiringFenced,
    retiringLaneBound' = retiringLaneBound,
    retiringMarkerActivated' = retiringMarkerActivated,
    promotedMarker' = promotedMarker,
    promotedLaneOpen' = promotedLaneOpen,
    promotedLaneBound' = promotedLaneBound,
    acknowledged' = acknowledged,
    promotionEchoVerified' = promotionEchoVerified,
    completionProofExact' = completionProofExact,
    lateRetiringWriteCaptured' = lateRetiringWriteCaptured,
    directKvTransferWasRejected' = true,
    mixedReconfigurationWasRejected' = mixedReconfigurationWasRejected,
    ordinaryFallbackUsed' = ordinaryFallbackUsed,
    activationResponseWasLost' = activationResponseWasLost,
    supersededMarkerWasReplaced' = supersededMarkerWasReplaced,
    newPrimaryWriteSucceeded' = newPrimaryWriteSucceeded,
    lastTransition' = DirectKvTransferRejected,
  }

  action rejectMixedReconfiguration = all {
    activePrimary == N1,
    activePrimary' = activePrimary,
    handoffPending' = handoffPending,
    retiringFenced' = retiringFenced,
    retiringLaneBound' = retiringLaneBound,
    retiringMarkerActivated' = retiringMarkerActivated,
    promotedMarker' = promotedMarker,
    promotedLaneOpen' = promotedLaneOpen,
    promotedLaneBound' = promotedLaneBound,
    acknowledged' = acknowledged,
    promotionEchoVerified' = promotionEchoVerified,
    completionProofExact' = completionProofExact,
    lateRetiringWriteCaptured' = lateRetiringWriteCaptured,
    directKvTransferWasRejected' = directKvTransferWasRejected,
    mixedReconfigurationWasRejected' = true,
    ordinaryFallbackUsed' = ordinaryFallbackUsed,
    activationResponseWasLost' = activationResponseWasLost,
    supersededMarkerWasReplaced' = supersededMarkerWasReplaced,
    newPrimaryWriteSucceeded' = newPrimaryWriteSucceeded,
    lastTransition' = MixedReconfigurationRejected,
  }

  action beginHandoff = all {
    activePrimary == N1,
    not(handoffPending),
    not(retiringFenced),
    activePrimary' = N1,
    handoffPending' = true,
    retiringFenced' = false,
    retiringLaneBound' = retiringLaneBound,
    retiringMarkerActivated' = false,
    promotedMarker' = MarkerAbsent,
    promotedLaneOpen' = promotedLaneOpen,
    promotedLaneBound' = false,
    acknowledged' = Set(),
    promotionEchoVerified' = false,
    completionProofExact' = false,
    lateRetiringWriteCaptured' = false,
    directKvTransferWasRejected' = directKvTransferWasRejected,
    mixedReconfigurationWasRejected' = mixedReconfigurationWasRejected,
    ordinaryFallbackUsed' = false,
    activationResponseWasLost' = false,
    supersededMarkerWasReplaced' = supersededMarkerWasReplaced,
    newPrimaryWriteSucceeded' = false,
    lastTransition' = HandoffBegan,
  }

  action commitLateRetiringWrite = all {
    handoffPending,
    retiringWritable,
    not(lateRetiringWriteCaptured),
    activePrimary' = activePrimary,
    handoffPending' = handoffPending,
    retiringFenced' = retiringFenced,
    retiringLaneBound' = retiringLaneBound,
    retiringMarkerActivated' = retiringMarkerActivated,
    promotedMarker' = promotedMarker,
    promotedLaneOpen' = promotedLaneOpen,
    promotedLaneBound' = promotedLaneBound,
    acknowledged' = acknowledged,
    promotionEchoVerified' = promotionEchoVerified,
    completionProofExact' = completionProofExact,
    lateRetiringWriteCaptured' = true,
    directKvTransferWasRejected' = directKvTransferWasRejected,
    mixedReconfigurationWasRejected' = mixedReconfigurationWasRejected,
    ordinaryFallbackUsed' = ordinaryFallbackUsed,
    activationResponseWasLost' = activationResponseWasLost,
    supersededMarkerWasReplaced' = supersededMarkerWasReplaced,
    newPrimaryWriteSucceeded' = newPrimaryWriteSucceeded,
    lastTransition' = LateRetiringWriteCommitted,
  }

  action fenceRetiringPrimary = all {
    handoffPending,
    activePrimary == N1,
    not(retiringFenced),
    activePrimary' = activePrimary,
    handoffPending' = true,
    retiringFenced' = true,
    retiringLaneBound' = false,
    retiringMarkerActivated' = false,
    promotedMarker' = promotedMarker,
    promotedLaneOpen' = promotedLaneOpen,
    promotedLaneBound' = false,
    acknowledged' = Set(N1),
    promotionEchoVerified' = false,
    completionProofExact' = false,
    lateRetiringWriteCaptured' = lateRetiringWriteCaptured,
    directKvTransferWasRejected' = directKvTransferWasRejected,
    mixedReconfigurationWasRejected' = mixedReconfigurationWasRejected,
    ordinaryFallbackUsed' = false,
    activationResponseWasLost' = activationResponseWasLost,
    supersededMarkerWasReplaced' = supersededMarkerWasReplaced,
    newPrimaryWriteSucceeded' = false,
    lastTransition' = RetiringPrimaryFenced,
  }

  action acknowledgeReplica = all {
    handoffPending,
    retiringFenced,
    not(acknowledged.contains(N3)),
    activePrimary' = activePrimary,
    handoffPending' = handoffPending,
    retiringFenced' = retiringFenced,
    retiringLaneBound' = retiringLaneBound,
    retiringMarkerActivated' = retiringMarkerActivated,
    promotedMarker' = promotedMarker,
    promotedLaneOpen' = promotedLaneOpen,
    promotedLaneBound' = promotedLaneBound,
    acknowledged' = acknowledged.union(Set(N3)),
    promotionEchoVerified' = promotionEchoVerified,
    completionProofExact' = completionProofExact,
    lateRetiringWriteCaptured' = lateRetiringWriteCaptured,
    directKvTransferWasRejected' = directKvTransferWasRejected,
    mixedReconfigurationWasRejected' = mixedReconfigurationWasRejected,
    ordinaryFallbackUsed' = ordinaryFallbackUsed,
    activationResponseWasLost' = activationResponseWasLost,
    supersededMarkerWasReplaced' = supersededMarkerWasReplaced,
    newPrimaryWriteSucceeded' = newPrimaryWriteSucceeded,
    lastTransition' = ReplicaAcknowledged,
  }

  action rejectPromotionAckWithoutMarker = all {
    handoffPending,
    retiringFenced,
    promotedMarker == MarkerAbsent,
    activePrimary' = activePrimary,
    handoffPending' = handoffPending,
    retiringFenced' = retiringFenced,
    retiringLaneBound' = retiringLaneBound,
    retiringMarkerActivated' = retiringMarkerActivated,
    promotedMarker' = promotedMarker,
    promotedLaneOpen' = promotedLaneOpen,
    promotedLaneBound' = promotedLaneBound,
    acknowledged' = acknowledged,
    promotionEchoVerified' = false,
    completionProofExact' = completionProofExact,
    lateRetiringWriteCaptured' = lateRetiringWriteCaptured,
    directKvTransferWasRejected' = directKvTransferWasRejected,
    mixedReconfigurationWasRejected' = mixedReconfigurationWasRejected,
    ordinaryFallbackUsed' = ordinaryFallbackUsed,
    activationResponseWasLost' = activationResponseWasLost,
    supersededMarkerWasReplaced' = supersededMarkerWasReplaced,
    newPrimaryWriteSucceeded' = newPrimaryWriteSucceeded,
    lastTransition' = PromotionAckRejectedWithoutMarker,
  }

  action persistPromotionMarker = all {
    handoffPending,
    retiringFenced,
    promotedMarker == MarkerAbsent,
    activePrimary' = activePrimary,
    handoffPending' = handoffPending,
    retiringFenced' = retiringFenced,
    retiringLaneBound' = retiringLaneBound,
    retiringMarkerActivated' = retiringMarkerActivated,
    promotedMarker' = MarkerPendingExact,
    promotedLaneOpen' = promotedLaneOpen,
    promotedLaneBound' = false,
    acknowledged' = acknowledged,
    promotionEchoVerified' = false,
    completionProofExact' = completionProofExact,
    lateRetiringWriteCaptured' = lateRetiringWriteCaptured,
    directKvTransferWasRejected' = directKvTransferWasRejected,
    mixedReconfigurationWasRejected' = mixedReconfigurationWasRejected,
    ordinaryFallbackUsed' = ordinaryFallbackUsed,
    activationResponseWasLost' = activationResponseWasLost,
    supersededMarkerWasReplaced' = supersededMarkerWasReplaced,
    newPrimaryWriteSucceeded' = newPrimaryWriteSucceeded,
    lastTransition' = PromotionMarkerPersisted,
  }

  action acknowledgePromotedReplica = all {
    handoffPending,
    retiringFenced,
    promotedMarker == MarkerPendingExact,
    not(acknowledged.contains(N2)),
    activePrimary' = activePrimary,
    handoffPending' = handoffPending,
    retiringFenced' = retiringFenced,
    retiringLaneBound' = retiringLaneBound,
    retiringMarkerActivated' = retiringMarkerActivated,
    promotedMarker' = promotedMarker,
    promotedLaneOpen' = promotedLaneOpen,
    promotedLaneBound' = false,
    acknowledged' = acknowledged.union(Set(N2)),
    promotionEchoVerified' = true,
    completionProofExact' = completionProofExact,
    lateRetiringWriteCaptured' = lateRetiringWriteCaptured,
    directKvTransferWasRejected' = directKvTransferWasRejected,
    mixedReconfigurationWasRejected' = mixedReconfigurationWasRejected,
    ordinaryFallbackUsed' = ordinaryFallbackUsed,
    activationResponseWasLost' = activationResponseWasLost,
    supersededMarkerWasReplaced' = supersededMarkerWasReplaced,
    newPrimaryWriteSucceeded' = newPrimaryWriteSucceeded,
    lastTransition' = PromotedReplicaAcknowledged,
  }

  action rejectPartialActivation = all {
    handoffPending,
    retiringFenced,
    not(exactUnionAcknowledged),
    activePrimary' = activePrimary,
    handoffPending' = true,
    retiringFenced' = retiringFenced,
    retiringLaneBound' = retiringLaneBound,
    retiringMarkerActivated' = retiringMarkerActivated,
    promotedMarker' = promotedMarker,
    promotedLaneOpen' = promotedLaneOpen,
    promotedLaneBound' = promotedLaneBound,
    acknowledged' = acknowledged,
    promotionEchoVerified' = promotionEchoVerified,
    completionProofExact' = false,
    lateRetiringWriteCaptured' = lateRetiringWriteCaptured,
    directKvTransferWasRejected' = directKvTransferWasRejected,
    mixedReconfigurationWasRejected' = mixedReconfigurationWasRejected,
    ordinaryFallbackUsed' = false,
    activationResponseWasLost' = activationResponseWasLost,
    supersededMarkerWasReplaced' = supersededMarkerWasReplaced,
    newPrimaryWriteSucceeded' = newPrimaryWriteSucceeded,
    lastTransition' = PartialActivationRejected,
  }

  action crashPromotedLaneBeforeActivation = all {
    handoffPending,
    promotionEchoVerified,
    exactUnionAcknowledged,
    activePrimary' = activePrimary,
    handoffPending' = handoffPending,
    retiringFenced' = retiringFenced,
    retiringLaneBound' = retiringLaneBound,
    retiringMarkerActivated' = retiringMarkerActivated,
    promotedMarker' = promotedMarker,
    promotedLaneOpen' = false,
    promotedLaneBound' = false,
    acknowledged' = acknowledged,
    promotionEchoVerified' = promotionEchoVerified,
    completionProofExact' = completionProofExact,
    lateRetiringWriteCaptured' = lateRetiringWriteCaptured,
    directKvTransferWasRejected' = directKvTransferWasRejected,
    mixedReconfigurationWasRejected' = mixedReconfigurationWasRejected,
    ordinaryFallbackUsed' = ordinaryFallbackUsed,
    activationResponseWasLost' = activationResponseWasLost,
    supersededMarkerWasReplaced' = supersededMarkerWasReplaced,
    newPrimaryWriteSucceeded' = newPrimaryWriteSucceeded,
    lastTransition' = PromotedLaneCrashed,
  }

  action reopenPromotedLane = all {
    not(promotedLaneOpen),
    activePrimary' = activePrimary,
    handoffPending' = handoffPending,
    retiringFenced' = retiringFenced,
    retiringLaneBound' = retiringLaneBound,
    retiringMarkerActivated' = retiringMarkerActivated,
    promotedMarker' = promotedMarker,
    promotedLaneOpen' = true,
    promotedLaneBound' = false,
    acknowledged' = acknowledged,
    promotionEchoVerified' = promotionEchoVerified,
    completionProofExact' = completionProofExact,
    lateRetiringWriteCaptured' = lateRetiringWriteCaptured,
    directKvTransferWasRejected' = directKvTransferWasRejected,
    mixedReconfigurationWasRejected' = mixedReconfigurationWasRejected,
    ordinaryFallbackUsed' = ordinaryFallbackUsed,
    activationResponseWasLost' = activationResponseWasLost,
    supersededMarkerWasReplaced' = supersededMarkerWasReplaced,
    newPrimaryWriteSucceeded' = newPrimaryWriteSucceeded,
    lastTransition' = PromotedLaneReopened,
  }

  action activateHandoff = all {
    handoffPending,
    retiringFenced,
    exactUnionAcknowledged,
    promotionEchoVerified,
    promotedMarker == MarkerPendingExact,
    activePrimary' = N2,
    handoffPending' = false,
    retiringFenced' = true,
    retiringLaneBound' = false,
    retiringMarkerActivated' = true,
    promotedMarker' = promotedMarker,
    promotedLaneOpen' = promotedLaneOpen,
    promotedLaneBound' = false,
    acknowledged' = acknowledged,
    promotionEchoVerified' = promotionEchoVerified,
    completionProofExact' = true,
    lateRetiringWriteCaptured' = lateRetiringWriteCaptured,
    directKvTransferWasRejected' = directKvTransferWasRejected,
    mixedReconfigurationWasRejected' = mixedReconfigurationWasRejected,
    ordinaryFallbackUsed' = false,
    activationResponseWasLost' = false,
    supersededMarkerWasReplaced' = supersededMarkerWasReplaced,
    newPrimaryWriteSucceeded' = false,
    lastTransition' = HandoffActivated,
  }

  action loseActivationResponse = all {
    activePrimary == N2,
    completionProofExact,
    not(promotedLaneBound),
    activePrimary' = activePrimary,
    handoffPending' = handoffPending,
    retiringFenced' = retiringFenced,
    retiringLaneBound' = retiringLaneBound,
    retiringMarkerActivated' = retiringMarkerActivated,
    promotedMarker' = promotedMarker,
    promotedLaneOpen' = promotedLaneOpen,
    promotedLaneBound' = promotedLaneBound,
    acknowledged' = acknowledged,
    promotionEchoVerified' = promotionEchoVerified,
    completionProofExact' = completionProofExact,
    lateRetiringWriteCaptured' = lateRetiringWriteCaptured,
    directKvTransferWasRejected' = directKvTransferWasRejected,
    mixedReconfigurationWasRejected' = mixedReconfigurationWasRejected,
    ordinaryFallbackUsed' = ordinaryFallbackUsed,
    activationResponseWasLost' = true,
    supersededMarkerWasReplaced' = supersededMarkerWasReplaced,
    newPrimaryWriteSucceeded' = newPrimaryWriteSucceeded,
    lastTransition' = ActivationResponseLost,
  }

  action bindPromotedLane = all {
    activePrimary == N2,
    not(handoffPending),
    completionProofExact,
    promotedMarker == MarkerPendingExact,
    promotedLaneOpen,
    activePrimary' = activePrimary,
    handoffPending' = false,
    retiringFenced' = retiringFenced,
    retiringLaneBound' = false,
    retiringMarkerActivated' = retiringMarkerActivated,
    promotedMarker' = MarkerActivatedExact,
    promotedLaneOpen' = true,
    promotedLaneBound' = true,
    acknowledged' = acknowledged,
    promotionEchoVerified' = promotionEchoVerified,
    completionProofExact' = completionProofExact,
    lateRetiringWriteCaptured' = lateRetiringWriteCaptured,
    directKvTransferWasRejected' = directKvTransferWasRejected,
    mixedReconfigurationWasRejected' = mixedReconfigurationWasRejected,
    ordinaryFallbackUsed' = ordinaryFallbackUsed,
    activationResponseWasLost' = activationResponseWasLost,
    supersededMarkerWasReplaced' = supersededMarkerWasReplaced,
    newPrimaryWriteSucceeded' = newPrimaryWriteSucceeded,
    lastTransition' = PromotedLaneBound,
  }

  action restartRetiringLane = all {
    retiringFenced,
    activePrimary == N2,
    activePrimary' = activePrimary,
    handoffPending' = handoffPending,
    retiringFenced' = retiringFenced,
    retiringLaneBound' = false,
    retiringMarkerActivated' = retiringMarkerActivated,
    promotedMarker' = promotedMarker,
    promotedLaneOpen' = promotedLaneOpen,
    promotedLaneBound' = promotedLaneBound,
    acknowledged' = acknowledged,
    promotionEchoVerified' = promotionEchoVerified,
    completionProofExact' = completionProofExact,
    lateRetiringWriteCaptured' = lateRetiringWriteCaptured,
    directKvTransferWasRejected' = directKvTransferWasRejected,
    mixedReconfigurationWasRejected' = mixedReconfigurationWasRejected,
    ordinaryFallbackUsed' = ordinaryFallbackUsed,
    activationResponseWasLost' = activationResponseWasLost,
    supersededMarkerWasReplaced' = supersededMarkerWasReplaced,
    newPrimaryWriteSucceeded' = newPrimaryWriteSucceeded,
    lastTransition' = RetiringLaneRestarted,
  }

  action rejectRetiringLaneRebind = all {
    activePrimary == N2,
    completionProofExact,
    retiringMarkerActivated,
    not(retiringLaneBound),
    activePrimary' = activePrimary,
    handoffPending' = handoffPending,
    retiringFenced' = retiringFenced,
    retiringLaneBound' = false,
    retiringMarkerActivated' = retiringMarkerActivated,
    promotedMarker' = promotedMarker,
    promotedLaneOpen' = promotedLaneOpen,
    promotedLaneBound' = promotedLaneBound,
    acknowledged' = acknowledged,
    promotionEchoVerified' = promotionEchoVerified,
    completionProofExact' = completionProofExact,
    lateRetiringWriteCaptured' = lateRetiringWriteCaptured,
    directKvTransferWasRejected' = directKvTransferWasRejected,
    mixedReconfigurationWasRejected' = mixedReconfigurationWasRejected,
    ordinaryFallbackUsed' = ordinaryFallbackUsed,
    activationResponseWasLost' = activationResponseWasLost,
    supersededMarkerWasReplaced' = supersededMarkerWasReplaced,
    newPrimaryWriteSucceeded' = newPrimaryWriteSucceeded,
    lastTransition' = RetiringLaneRebindRejected,
  }

  action replaceSupersededInactiveMarker = all {
    activePrimary == N2,
    retiringMarkerActivated,
    not(retiringLaneBound),
    activePrimary' = activePrimary,
    handoffPending' = handoffPending,
    retiringFenced' = retiringFenced,
    retiringLaneBound' = false,
    retiringMarkerActivated' = retiringMarkerActivated,
    promotedMarker' = promotedMarker,
    promotedLaneOpen' = promotedLaneOpen,
    promotedLaneBound' = promotedLaneBound,
    acknowledged' = acknowledged,
    promotionEchoVerified' = promotionEchoVerified,
    completionProofExact' = completionProofExact,
    lateRetiringWriteCaptured' = lateRetiringWriteCaptured,
    directKvTransferWasRejected' = directKvTransferWasRejected,
    mixedReconfigurationWasRejected' = mixedReconfigurationWasRejected,
    ordinaryFallbackUsed' = ordinaryFallbackUsed,
    activationResponseWasLost' = activationResponseWasLost,
    supersededMarkerWasReplaced' = true,
    newPrimaryWriteSucceeded' = newPrimaryWriteSucceeded,
    lastTransition' = SupersededMarkerReplaced,
  }

  action commitNewPrimaryWrite = all {
    promotedWritable,
    activePrimary' = activePrimary,
    handoffPending' = handoffPending,
    retiringFenced' = retiringFenced,
    retiringLaneBound' = retiringLaneBound,
    retiringMarkerActivated' = retiringMarkerActivated,
    promotedMarker' = promotedMarker,
    promotedLaneOpen' = promotedLaneOpen,
    promotedLaneBound' = promotedLaneBound,
    acknowledged' = acknowledged,
    promotionEchoVerified' = promotionEchoVerified,
    completionProofExact' = completionProofExact,
    lateRetiringWriteCaptured' = lateRetiringWriteCaptured,
    directKvTransferWasRejected' = directKvTransferWasRejected,
    mixedReconfigurationWasRejected' = mixedReconfigurationWasRejected,
    ordinaryFallbackUsed' = ordinaryFallbackUsed,
    activationResponseWasLost' = activationResponseWasLost,
    supersededMarkerWasReplaced' = supersededMarkerWasReplaced,
    newPrimaryWriteSucceeded' = true,
    lastTransition' = NewPrimaryWriteCommitted,
  }

  action stutter = all {
    activePrimary' = activePrimary,
    handoffPending' = handoffPending,
    retiringFenced' = retiringFenced,
    retiringLaneBound' = retiringLaneBound,
    retiringMarkerActivated' = retiringMarkerActivated,
    promotedMarker' = promotedMarker,
    promotedLaneOpen' = promotedLaneOpen,
    promotedLaneBound' = promotedLaneBound,
    acknowledged' = acknowledged,
    promotionEchoVerified' = promotionEchoVerified,
    completionProofExact' = completionProofExact,
    lateRetiringWriteCaptured' = lateRetiringWriteCaptured,
    directKvTransferWasRejected' = directKvTransferWasRejected,
    mixedReconfigurationWasRejected' = mixedReconfigurationWasRejected,
    ordinaryFallbackUsed' = ordinaryFallbackUsed,
    activationResponseWasLost' = activationResponseWasLost,
    supersededMarkerWasReplaced' = supersededMarkerWasReplaced,
    newPrimaryWriteSucceeded' = newPrimaryWriteSucceeded,
    lastTransition' = PrimaryHandoffStuttered,
  }

  action step = any {
    rejectDirectKvTransfer,
    rejectMixedReconfiguration,
    beginHandoff,
    commitLateRetiringWrite,
    fenceRetiringPrimary,
    acknowledgeReplica,
    rejectPromotionAckWithoutMarker,
    persistPromotionMarker,
    acknowledgePromotedReplica,
    rejectPartialActivation,
    crashPromotedLaneBeforeActivation,
    reopenPromotedLane,
    activateHandoff,
    loseActivationResponse,
    bindPromotedLane,
    restartRetiringLane,
    rejectRetiringLaneRebind,
    replaceSupersededInactiveMarker,
    commitNewPrimaryWrite,
    stutter,
  }
}
