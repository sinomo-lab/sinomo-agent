/// Rebind a freshly opened metadata-service lane after a completed handoff.
///
/// Evidence class: target hardening for the completed-handoff reopen path.
/// The control-plane activation transaction and durable local marker survive
/// process/lane creation, but `instance_authority_epoch` is deliberately
/// process-local. Consequently a freshly opened lane is unbound even when the
/// marker already names an activated successor generation. It must verify the
/// exact active assignment, completion proof, and marker before bootstrap may
/// perform an authoritative mutation. After activation, legitimate durable
/// appends may advance the local prefix beyond the immutable marker. A later
/// fresh lane therefore accepts either the marker-equal prefix or a proven
/// contiguous extension, but never a divergent prefix at the same height.
///
/// The cached lane models a mutation handle opened unbound before activation.
/// Completion or rebinding a fresh lane must not grant it authority directly.
/// After it observes a changed authority generation, it may mutate only after
/// its own exact proof-bound rebind or reopen; missing, unchanged, or
/// mismatched generation evidence leaves it unbound.
module dfs_metadata_handoff_lane_rebind {
  type LaneBinding = LaneClosed | LaneUnbound | LaneActive
  type BootstrapOutcome = BootstrapNotAttempted | BootstrapRejected | BootstrapSucceeded
  type PrefixRelation =
    | PrefixUnavailable
    | PrefixMarkerEqual
    | PrefixContiguousExtension
    | PrefixDivergent
  type EvidenceFault = WrongProof | WrongActiveEpoch | PendingMarker | DivergentPrefix
  type AuthorityGenerationEvidence =
    | GenerationNotObserved
    | GenerationMissing
    | GenerationUnchanged
    | GenerationChangedMismatched
    | GenerationChangedExact
  type RebindTransition =
    | RebindInitialized
    | StaleLaneOpened
    | CachedGenerationObserved
    | CachedLaneRebindAccepted
    | CachedLaneRebindRejected
    | CachedLaneReopenAccepted
    | CachedLaneReopenRejected
    | ExactHandoffCompleted
    | MismatchedEvidenceInstalled
    | FreshLaneOpened
    | FreshLaneRebindAccepted
    | FreshLaneRebindRejected
    | PostActivationDurableAppendCommitted
    | FreshLaneReopenedAfterAppend
    | FreshBootstrapAttempted
    | StaleBootstrapAttempted
    | RebindStuttered

  var controlCompleted: bool
  var markerActivated: bool
  var activeEpochMatchesMarker: bool
  var proofMatchesMarkerAndActive: bool
  var markerSequence: int
  var currentSequence: int
  var prefixRelation: PrefixRelation
  var postActivationAppendCommitted: bool
  var currentAuthorityGeneration: int
  var cachedOpenAuthorityGeneration: int
  var cachedGenerationEvidence: AuthorityGenerationEvidence
  var cachedLaneProofBound: bool
  var cachedLaneReopened: bool
  var freshLaneBinding: LaneBinding
  var staleLaneBinding: LaneBinding
  var freshBootstrapOutcome: BootstrapOutcome
  var staleBootstrapOutcome: BootstrapOutcome
  var lastTransition: RebindTransition

  pure def currentPrefixMatchesMarkerOrExtension: bool = any {
    all {
      prefixRelation == PrefixMarkerEqual,
      markerSequence > 0,
      currentSequence == markerSequence,
    },
    all {
      prefixRelation == PrefixContiguousExtension,
      markerSequence > 0,
      currentSequence > markerSequence,
    },
  }

  /// This conjunction abstracts the Rust verifier's exact successor epoch,
  /// handoff id, retiring assignment, desired active assignment, immutable
  /// marker proof, and current journal-prefix ancestry comparisons.
  pure def exactRebindEvidence: bool = all {
    controlCompleted,
    markerActivated,
    activeEpochMatchesMarker,
    proofMatchesMarkerAndActive,
    currentPrefixMatchesMarkerOrExtension,
  }

  /// A cached mutation lane may bind only after observing a committed
  /// authority generation different from the one it cached when opened. The
  /// generation observation is not sufficient by itself: all durable handoff
  /// evidence must still be exact at the bind or reopen boundary.
  pure def exactChangedGenerationEvidenceForCachedLane: bool = all {
    cachedGenerationEvidence == GenerationChangedExact,
    cachedOpenAuthorityGeneration > 0,
    currentAuthorityGeneration > cachedOpenAuthorityGeneration,
    exactRebindEvidence,
  }

  action init = all {
    controlCompleted' = false,
    markerActivated' = false,
    activeEpochMatchesMarker' = false,
    proofMatchesMarkerAndActive' = false,
    markerSequence' = 0,
    currentSequence' = 0,
    prefixRelation' = PrefixUnavailable,
    postActivationAppendCommitted' = false,
    currentAuthorityGeneration' = 1,
    cachedOpenAuthorityGeneration' = 0,
    cachedGenerationEvidence' = GenerationNotObserved,
    cachedLaneProofBound' = false,
    cachedLaneReopened' = false,
    freshLaneBinding' = LaneClosed,
    staleLaneBinding' = LaneClosed,
    freshBootstrapOutcome' = BootstrapNotAttempted,
    staleBootstrapOutcome' = BootstrapNotAttempted,
    lastTransition' = RebindInitialized,
  }

  action openStaleLaneBeforeCompletion = all {
    not(controlCompleted),
    staleLaneBinding == LaneClosed,
    controlCompleted' = controlCompleted,
    markerActivated' = markerActivated,
    activeEpochMatchesMarker' = activeEpochMatchesMarker,
    proofMatchesMarkerAndActive' = proofMatchesMarkerAndActive,
    markerSequence' = markerSequence,
    currentSequence' = currentSequence,
    prefixRelation' = prefixRelation,
    postActivationAppendCommitted' = postActivationAppendCommitted,
    currentAuthorityGeneration' = currentAuthorityGeneration,
    cachedOpenAuthorityGeneration' = currentAuthorityGeneration,
    cachedGenerationEvidence' = GenerationNotObserved,
    cachedLaneProofBound' = false,
    cachedLaneReopened' = false,
    freshLaneBinding' = freshLaneBinding,
    staleLaneBinding' = LaneUnbound,
    freshBootstrapOutcome' = freshBootstrapOutcome,
    staleBootstrapOutcome' = BootstrapNotAttempted,
    lastTransition' = StaleLaneOpened,
  }

  action completeExactHandoff = all {
    not(controlCompleted),
    controlCompleted' = true,
    markerActivated' = true,
    activeEpochMatchesMarker' = true,
    proofMatchesMarkerAndActive' = true,
    markerSequence' = 1,
    currentSequence' = 1,
    prefixRelation' = PrefixMarkerEqual,
    postActivationAppendCommitted' = false,
    currentAuthorityGeneration' = currentAuthorityGeneration + 1,
    cachedOpenAuthorityGeneration' = cachedOpenAuthorityGeneration,
    cachedGenerationEvidence' = cachedGenerationEvidence,
    cachedLaneProofBound' = cachedLaneProofBound,
    cachedLaneReopened' = cachedLaneReopened,
    freshLaneBinding' = freshLaneBinding,
    // Durable activation never mutates another live handle's process-local
    // authority binding.
    staleLaneBinding' = staleLaneBinding,
    freshBootstrapOutcome' = freshBootstrapOutcome,
    staleBootstrapOutcome' = staleBootstrapOutcome,
    lastTransition' = ExactHandoffCompleted,
  }

  /// Model persisted observations from another generation. None may authorize
  /// a fresh lane: the completion proof, active epoch, and durable marker are
  /// all independently load-bearing.
  action installMismatchedEvidence(fault: EvidenceFault): bool = all {
    not(controlCompleted),
    controlCompleted' = true,
    markerActivated' = fault != PendingMarker,
    activeEpochMatchesMarker' = fault != WrongActiveEpoch,
    proofMatchesMarkerAndActive' = fault != WrongProof,
    markerSequence' = 1,
    currentSequence' = if (fault == DivergentPrefix) 2 else 1,
    prefixRelation' =
      if (fault == DivergentPrefix) PrefixDivergent else PrefixMarkerEqual,
    postActivationAppendCommitted' = false,
    currentAuthorityGeneration' = currentAuthorityGeneration + 1,
    cachedOpenAuthorityGeneration' = cachedOpenAuthorityGeneration,
    cachedGenerationEvidence' = cachedGenerationEvidence,
    cachedLaneProofBound' = cachedLaneProofBound,
    cachedLaneReopened' = cachedLaneReopened,
    freshLaneBinding' = freshLaneBinding,
    staleLaneBinding' = staleLaneBinding,
    freshBootstrapOutcome' = freshBootstrapOutcome,
    staleBootstrapOutcome' = staleBootstrapOutcome,
    lastTransition' = MismatchedEvidenceInstalled,
  }

  action openFreshLane = all {
    freshLaneBinding == LaneClosed,
    controlCompleted' = controlCompleted,
    markerActivated' = markerActivated,
    activeEpochMatchesMarker' = activeEpochMatchesMarker,
    proofMatchesMarkerAndActive' = proofMatchesMarkerAndActive,
    markerSequence' = markerSequence,
    currentSequence' = currentSequence,
    prefixRelation' = prefixRelation,
    postActivationAppendCommitted' = postActivationAppendCommitted,
    currentAuthorityGeneration' = currentAuthorityGeneration,
    cachedOpenAuthorityGeneration' = cachedOpenAuthorityGeneration,
    cachedGenerationEvidence' = cachedGenerationEvidence,
    cachedLaneProofBound' = cachedLaneProofBound,
    cachedLaneReopened' = cachedLaneReopened,
    // Opening from an activated durable marker intentionally starts unbound.
    freshLaneBinding' = LaneUnbound,
    staleLaneBinding' = staleLaneBinding,
    freshBootstrapOutcome' = BootstrapNotAttempted,
    staleBootstrapOutcome' = staleBootstrapOutcome,
    lastTransition' = FreshLaneOpened,
  }

  action probeFreshLaneRebind = all {
    freshLaneBinding == LaneUnbound,
    controlCompleted' = controlCompleted,
    markerActivated' = markerActivated,
    activeEpochMatchesMarker' = activeEpochMatchesMarker,
    proofMatchesMarkerAndActive' = proofMatchesMarkerAndActive,
    markerSequence' = markerSequence,
    currentSequence' = currentSequence,
    prefixRelation' = prefixRelation,
    postActivationAppendCommitted' = postActivationAppendCommitted,
    currentAuthorityGeneration' = currentAuthorityGeneration,
    cachedOpenAuthorityGeneration' = cachedOpenAuthorityGeneration,
    cachedGenerationEvidence' = cachedGenerationEvidence,
    cachedLaneProofBound' = cachedLaneProofBound,
    cachedLaneReopened' = cachedLaneReopened,
    freshLaneBinding' =
      if (exactRebindEvidence) LaneActive else LaneUnbound,
    staleLaneBinding' = staleLaneBinding,
    freshBootstrapOutcome' = freshBootstrapOutcome,
    staleBootstrapOutcome' = staleBootstrapOutcome,
    lastTransition' =
      if (exactRebindEvidence) FreshLaneRebindAccepted else FreshLaneRebindRejected,
  }

  /// The cached lane was opened before completion and therefore holds the old
  /// process-local generation. Merely observing any generation class never
  /// mutates its authority binding.
  action observeCachedAuthorityGeneration(
    evidence: AuthorityGenerationEvidence,
  ): bool = all {
    controlCompleted,
    staleLaneBinding == LaneUnbound,
    controlCompleted' = controlCompleted,
    markerActivated' = markerActivated,
    activeEpochMatchesMarker' = activeEpochMatchesMarker,
    proofMatchesMarkerAndActive' = proofMatchesMarkerAndActive,
    markerSequence' = markerSequence,
    currentSequence' = currentSequence,
    prefixRelation' = prefixRelation,
    postActivationAppendCommitted' = postActivationAppendCommitted,
    currentAuthorityGeneration' = currentAuthorityGeneration,
    cachedOpenAuthorityGeneration' = cachedOpenAuthorityGeneration,
    cachedGenerationEvidence' = evidence,
    cachedLaneProofBound' = false,
    cachedLaneReopened' = cachedLaneReopened,
    freshLaneBinding' = freshLaneBinding,
    staleLaneBinding' = LaneUnbound,
    freshBootstrapOutcome' = freshBootstrapOutcome,
    staleBootstrapOutcome' = BootstrapNotAttempted,
    lastTransition' = CachedGenerationObserved,
  }

  /// Same-handle refresh is allowed only after exact changed-generation
  /// evidence is joined with the full durable handoff proof.
  action probeCachedLaneRebind = all {
    staleLaneBinding == LaneUnbound,
    controlCompleted' = controlCompleted,
    markerActivated' = markerActivated,
    activeEpochMatchesMarker' = activeEpochMatchesMarker,
    proofMatchesMarkerAndActive' = proofMatchesMarkerAndActive,
    markerSequence' = markerSequence,
    currentSequence' = currentSequence,
    prefixRelation' = prefixRelation,
    postActivationAppendCommitted' = postActivationAppendCommitted,
    currentAuthorityGeneration' = currentAuthorityGeneration,
    cachedOpenAuthorityGeneration' = cachedOpenAuthorityGeneration,
    cachedGenerationEvidence' = cachedGenerationEvidence,
    cachedLaneProofBound' = exactChangedGenerationEvidenceForCachedLane,
    cachedLaneReopened' = cachedLaneReopened,
    freshLaneBinding' = freshLaneBinding,
    staleLaneBinding' =
      if (exactChangedGenerationEvidenceForCachedLane) LaneActive else LaneUnbound,
    freshBootstrapOutcome' = freshBootstrapOutcome,
    staleBootstrapOutcome' = BootstrapNotAttempted,
    lastTransition' =
      if (exactChangedGenerationEvidenceForCachedLane) CachedLaneRebindAccepted else CachedLaneRebindRejected,
  }

  /// Rebuilding the cached lane is an alternative to same-handle refresh, but
  /// reopening is not a shortcut around validation: it consumes the identical
  /// exact changed-generation and durable-proof conjunction.
  action reopenCachedLaneWithProof = all {
    staleLaneBinding == LaneUnbound,
    controlCompleted' = controlCompleted,
    markerActivated' = markerActivated,
    activeEpochMatchesMarker' = activeEpochMatchesMarker,
    proofMatchesMarkerAndActive' = proofMatchesMarkerAndActive,
    markerSequence' = markerSequence,
    currentSequence' = currentSequence,
    prefixRelation' = prefixRelation,
    postActivationAppendCommitted' = postActivationAppendCommitted,
    currentAuthorityGeneration' = currentAuthorityGeneration,
    cachedOpenAuthorityGeneration' = cachedOpenAuthorityGeneration,
    cachedGenerationEvidence' = cachedGenerationEvidence,
    cachedLaneProofBound' = exactChangedGenerationEvidenceForCachedLane,
    cachedLaneReopened' = exactChangedGenerationEvidenceForCachedLane,
    freshLaneBinding' = freshLaneBinding,
    staleLaneBinding' =
      if (exactChangedGenerationEvidenceForCachedLane) LaneActive else LaneUnbound,
    freshBootstrapOutcome' = freshBootstrapOutcome,
    staleBootstrapOutcome' = BootstrapNotAttempted,
    lastTransition' =
      if (exactChangedGenerationEvidenceForCachedLane) CachedLaneReopenAccepted else CachedLaneReopenRejected,
  }

  /// An active successor may durably extend the completed marker prefix. The
  /// marker remains immutable; ancestry plus a strictly greater sequence makes
  /// the new current prefix valid evidence for future lane rebinding.
  action commitPostActivationDurableAppend = all {
    freshLaneBinding == LaneActive,
    exactRebindEvidence,
    prefixRelation == PrefixMarkerEqual,
    currentSequence == markerSequence,
    not(postActivationAppendCommitted),
    controlCompleted' = controlCompleted,
    markerActivated' = markerActivated,
    activeEpochMatchesMarker' = activeEpochMatchesMarker,
    proofMatchesMarkerAndActive' = proofMatchesMarkerAndActive,
    markerSequence' = markerSequence,
    currentSequence' = currentSequence + 1,
    prefixRelation' = PrefixContiguousExtension,
    postActivationAppendCommitted' = true,
    currentAuthorityGeneration' = currentAuthorityGeneration,
    cachedOpenAuthorityGeneration' = cachedOpenAuthorityGeneration,
    cachedGenerationEvidence' = cachedGenerationEvidence,
    cachedLaneProofBound' = cachedLaneProofBound,
    cachedLaneReopened' = cachedLaneReopened,
    freshLaneBinding' = freshLaneBinding,
    staleLaneBinding' = staleLaneBinding,
    freshBootstrapOutcome' = freshBootstrapOutcome,
    staleBootstrapOutcome' = staleBootstrapOutcome,
    lastTransition' = PostActivationDurableAppendCommitted,
  }

  /// Reopening discards only the process-local authority binding. Durable
  /// marker, completion proof, and extended current prefix all survive.
  action reopenFreshLaneAfterPostActivationAppend = all {
    postActivationAppendCommitted,
    prefixRelation == PrefixContiguousExtension,
    freshLaneBinding == LaneActive,
    controlCompleted' = controlCompleted,
    markerActivated' = markerActivated,
    activeEpochMatchesMarker' = activeEpochMatchesMarker,
    proofMatchesMarkerAndActive' = proofMatchesMarkerAndActive,
    markerSequence' = markerSequence,
    currentSequence' = currentSequence,
    prefixRelation' = prefixRelation,
    postActivationAppendCommitted' = postActivationAppendCommitted,
    currentAuthorityGeneration' = currentAuthorityGeneration,
    cachedOpenAuthorityGeneration' = cachedOpenAuthorityGeneration,
    cachedGenerationEvidence' = cachedGenerationEvidence,
    cachedLaneProofBound' = cachedLaneProofBound,
    cachedLaneReopened' = cachedLaneReopened,
    freshLaneBinding' = LaneUnbound,
    staleLaneBinding' = staleLaneBinding,
    freshBootstrapOutcome' = BootstrapNotAttempted,
    staleBootstrapOutcome' = staleBootstrapOutcome,
    lastTransition' = FreshLaneReopenedAfterAppend,
  }

  action attemptFreshBootstrap = all {
    freshLaneBinding != LaneClosed,
    controlCompleted' = controlCompleted,
    markerActivated' = markerActivated,
    activeEpochMatchesMarker' = activeEpochMatchesMarker,
    proofMatchesMarkerAndActive' = proofMatchesMarkerAndActive,
    markerSequence' = markerSequence,
    currentSequence' = currentSequence,
    prefixRelation' = prefixRelation,
    postActivationAppendCommitted' = postActivationAppendCommitted,
    currentAuthorityGeneration' = currentAuthorityGeneration,
    cachedOpenAuthorityGeneration' = cachedOpenAuthorityGeneration,
    cachedGenerationEvidence' = cachedGenerationEvidence,
    cachedLaneProofBound' = cachedLaneProofBound,
    cachedLaneReopened' = cachedLaneReopened,
    freshLaneBinding' = freshLaneBinding,
    staleLaneBinding' = staleLaneBinding,
    freshBootstrapOutcome' =
      if (freshLaneBinding == LaneActive) BootstrapSucceeded else BootstrapRejected,
    staleBootstrapOutcome' = staleBootstrapOutcome,
    lastTransition' = FreshBootstrapAttempted,
  }

  action attemptStaleBootstrap = all {
    staleLaneBinding != LaneClosed,
    controlCompleted' = controlCompleted,
    markerActivated' = markerActivated,
    activeEpochMatchesMarker' = activeEpochMatchesMarker,
    proofMatchesMarkerAndActive' = proofMatchesMarkerAndActive,
    markerSequence' = markerSequence,
    currentSequence' = currentSequence,
    prefixRelation' = prefixRelation,
    postActivationAppendCommitted' = postActivationAppendCommitted,
    currentAuthorityGeneration' = currentAuthorityGeneration,
    cachedOpenAuthorityGeneration' = cachedOpenAuthorityGeneration,
    cachedGenerationEvidence' = cachedGenerationEvidence,
    cachedLaneProofBound' = cachedLaneProofBound,
    cachedLaneReopened' = cachedLaneReopened,
    freshLaneBinding' = freshLaneBinding,
    staleLaneBinding' = staleLaneBinding,
    freshBootstrapOutcome' = freshBootstrapOutcome,
    staleBootstrapOutcome' =
      if (staleLaneBinding == LaneActive) BootstrapSucceeded else BootstrapRejected,
    lastTransition' = StaleBootstrapAttempted,
  }

  action stutter = all {
    controlCompleted' = controlCompleted,
    markerActivated' = markerActivated,
    activeEpochMatchesMarker' = activeEpochMatchesMarker,
    proofMatchesMarkerAndActive' = proofMatchesMarkerAndActive,
    markerSequence' = markerSequence,
    currentSequence' = currentSequence,
    prefixRelation' = prefixRelation,
    postActivationAppendCommitted' = postActivationAppendCommitted,
    currentAuthorityGeneration' = currentAuthorityGeneration,
    cachedOpenAuthorityGeneration' = cachedOpenAuthorityGeneration,
    cachedGenerationEvidence' = cachedGenerationEvidence,
    cachedLaneProofBound' = cachedLaneProofBound,
    cachedLaneReopened' = cachedLaneReopened,
    freshLaneBinding' = freshLaneBinding,
    staleLaneBinding' = staleLaneBinding,
    freshBootstrapOutcome' = freshBootstrapOutcome,
    staleBootstrapOutcome' = staleBootstrapOutcome,
    lastTransition' = RebindStuttered,
  }

  action step = any {
    openStaleLaneBeforeCompletion,
    completeExactHandoff,
    installMismatchedEvidence(WrongProof),
    installMismatchedEvidence(WrongActiveEpoch),
    installMismatchedEvidence(PendingMarker),
    installMismatchedEvidence(DivergentPrefix),
    openFreshLane,
    probeFreshLaneRebind,
    observeCachedAuthorityGeneration(GenerationMissing),
    observeCachedAuthorityGeneration(GenerationUnchanged),
    observeCachedAuthorityGeneration(GenerationChangedMismatched),
    observeCachedAuthorityGeneration(GenerationChangedExact),
    probeCachedLaneRebind,
    reopenCachedLaneWithProof,
    commitPostActivationDurableAppend,
    reopenFreshLaneAfterPostActivationAppend,
    attemptFreshBootstrap,
    attemptStaleBootstrap,
    stutter,
  }

  /// Coverage-only schedules call the real actions. Symbolic verification
  /// continues to use the unrestricted `step` relation.
  action postAppendReopenCoverageStep =
    if (not(controlCompleted)) {
      completeExactHandoff
    } else if (freshLaneBinding == LaneClosed) {
      openFreshLane
    } else if (freshLaneBinding == LaneUnbound) {
      probeFreshLaneRebind
    } else if (not(postActivationAppendCommitted)) {
      commitPostActivationDurableAppend
    } else if (lastTransition == PostActivationDurableAppendCommitted) {
      reopenFreshLaneAfterPostActivationAppend
    } else {
      attemptFreshBootstrap
    }

  action divergentPrefixCoverageStep =
    if (not(controlCompleted)) {
      installMismatchedEvidence(DivergentPrefix)
    } else if (freshLaneBinding == LaneClosed) {
      openFreshLane
    } else if (freshLaneBinding == LaneUnbound) {
      probeFreshLaneRebind
    } else {
      stutter
    }

  action cachedEvidenceCoverageStep(
    evidence: AuthorityGenerationEvidence,
    useReopen: bool,
  ): bool =
    if (staleLaneBinding == LaneClosed) {
      if (not(controlCompleted)) openStaleLaneBeforeCompletion else stutter
    } else if (not(controlCompleted)) {
      completeExactHandoff
    } else if (all {
      staleLaneBinding == LaneUnbound,
      cachedGenerationEvidence != evidence,
    }) {
      observeCachedAuthorityGeneration(evidence)
    } else if (staleLaneBinding == LaneUnbound) {
      if (useReopen) reopenCachedLaneWithProof else probeCachedLaneRebind
    } else {
      attemptStaleBootstrap
    }

  action cachedChangedGenerationDirectMutationCoverageStep =
    if (staleLaneBinding == LaneClosed) {
      if (not(controlCompleted)) openStaleLaneBeforeCompletion else stutter
    } else if (not(controlCompleted)) {
      completeExactHandoff
    } else if (cachedGenerationEvidence != GenerationChangedExact) {
      observeCachedAuthorityGeneration(GenerationChangedExact)
    } else {
      attemptStaleBootstrap
    }

  action coverageStep = any {
    step,
    postAppendReopenCoverageStep,
    divergentPrefixCoverageStep,
    cachedEvidenceCoverageStep(GenerationMissing, false),
    cachedEvidenceCoverageStep(GenerationUnchanged, false),
    cachedEvidenceCoverageStep(GenerationChangedMismatched, false),
    cachedEvidenceCoverageStep(GenerationChangedExact, false),
    cachedEvidenceCoverageStep(GenerationChangedExact, true),
    cachedChangedGenerationDirectMutationCoverageStep,
  }
}
