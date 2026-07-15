/// Purpose-separated backend selection for metadata handoff prefix probes.
///
/// Evidence class: target hardening for the live failure where ordinary
/// replication updates the canonical direct backend, while a routed service
/// lane opened earlier retains a stale in-memory view. The old ordinary probe
/// read that routed lane, rejected an exact durable prefix, and left the shard
/// frozen for another identical retry. Ordinary handoff verification must read
/// the canonical direct backend instead.
///
/// Promoted-primary handoff remains a separate purpose. It must use the routed
/// current lane because that path verifies and persists the promotion marker;
/// canonical prefix equality alone cannot replace that marker proof. This
/// bounded model includes two old-path retries, one fixed ordinary probe, one
/// canonical mismatch, and current/stale promoted-marker cases. Unbounded
/// fairness, transport loss, and backend I/O failure remain outside the slice.
module dfs_metadata_handoff_probe_backend_selection {
  type ProbeScenario =
    | ScenarioUnselected
    | OrdinaryExactCanonicalStaleRouted
    | OrdinaryCanonicalMismatch
    | PromotedCurrentMarker
    | PromotedStaleMarker

  type PrefixView = PrefixUnknown | PrefixExact | PrefixStale | PrefixMismatch
  type PromotionMarkerView = MarkerAbsent | MarkerCurrent | MarkerStale

  type ProbeBackendTransition =
    | ProbeBackendInitialized
    | OrdinaryExactStaleScenarioSelected
    | OrdinaryMismatchScenarioSelected
    | PromotedCurrentMarkerScenarioSelected
    | PromotedStaleMarkerScenarioSelected
    | OldRoutedOrdinaryProbeRejected
    | CanonicalDirectOrdinaryProbeAccepted
    | CanonicalDirectOrdinaryProbeRejected
    | PromotedSpecialRoutedProbeAccepted
    | PromotedSpecialRoutedProbeRejected
    | ProbeBackendStuttered

  type ProbeBackendState = {
    scenario: ProbeScenario,
    canonicalDirectBackend: PrefixView,
    routedLaneBackend: PrefixView,
    promotionMarker: PromotionMarkerView,
    ordinaryHandoffFrozen: bool,
    ordinaryRetryEnabled: bool,
    oldRoutedProbeAttempts: int,
    oldRoutedOrdinaryAck: bool,
    directOrdinaryProbeObserved: bool,
    directProbeUsedCanonicalBackend: bool,
    directOrdinaryAck: bool,
    directOrdinaryRejected: bool,
    ordinaryPrefixAckFloorPersisted: bool,
    promotedProbeObserved: bool,
    promotedProbeUsedSpecialRoutedPath: bool,
    promotedAck: bool,
    promotedRejected: bool,
    lastTransition: ProbeBackendTransition,
  }

  pure def canonicalDirectPrefixIsExact(state: ProbeBackendState): bool =
    state.canonicalDirectBackend == PrefixExact

  pure def promotedSpecialRoutedEvidenceIsExact(
    state: ProbeBackendState,
  ): bool = all {
    state.routedLaneBackend == PrefixExact,
    state.promotionMarker == MarkerCurrent,
  }

  pure val initialState: ProbeBackendState = {
    scenario: ScenarioUnselected,
    canonicalDirectBackend: PrefixUnknown,
    routedLaneBackend: PrefixUnknown,
    promotionMarker: MarkerAbsent,
    ordinaryHandoffFrozen: false,
    ordinaryRetryEnabled: false,
    oldRoutedProbeAttempts: 0,
    oldRoutedOrdinaryAck: false,
    directOrdinaryProbeObserved: false,
    directProbeUsedCanonicalBackend: false,
    directOrdinaryAck: false,
    directOrdinaryRejected: false,
    ordinaryPrefixAckFloorPersisted: false,
    promotedProbeObserved: false,
    promotedProbeUsedSpecialRoutedPath: false,
    promotedAck: false,
    promotedRejected: false,
    lastTransition: ProbeBackendInitialized,
  }

  var probeBackend: ProbeBackendState

  action init = probeBackend' = initialState

  /// Ordinary replication has made durable canonical state exact, but the
  /// independently opened routed lane still exposes its pre-replication view.
  action selectOrdinaryExactCanonicalStaleRouted = all {
    probeBackend.scenario == ScenarioUnselected,
    probeBackend' = probeBackend
      .with("scenario", OrdinaryExactCanonicalStaleRouted)
      .with("canonicalDirectBackend", PrefixExact)
      .with("routedLaneBackend", PrefixStale)
      .with("ordinaryHandoffFrozen", true)
      .with("ordinaryRetryEnabled", true)
      .with("lastTransition", OrdinaryExactStaleScenarioSelected),
  }

  action selectOrdinaryCanonicalMismatch = all {
    probeBackend.scenario == ScenarioUnselected,
    probeBackend' = probeBackend
      .with("scenario", OrdinaryCanonicalMismatch)
      .with("canonicalDirectBackend", PrefixMismatch)
      .with("routedLaneBackend", PrefixStale)
      .with("ordinaryHandoffFrozen", true)
      .with("ordinaryRetryEnabled", true)
      .with("lastTransition", OrdinaryMismatchScenarioSelected),
  }

  action selectPromotedCurrentMarker = all {
    probeBackend.scenario == ScenarioUnselected,
    probeBackend' = probeBackend
      .with("scenario", PromotedCurrentMarker)
      .with("canonicalDirectBackend", PrefixExact)
      .with("routedLaneBackend", PrefixExact)
      .with("promotionMarker", MarkerCurrent)
      .with("lastTransition", PromotedCurrentMarkerScenarioSelected),
  }

  action selectPromotedStaleMarker = all {
    probeBackend.scenario == ScenarioUnselected,
    probeBackend' = probeBackend
      .with("scenario", PromotedStaleMarker)
      .with("canonicalDirectBackend", PrefixExact)
      .with("routedLaneBackend", PrefixExact)
      .with("promotionMarker", MarkerStale)
      .with("lastTransition", PromotedStaleMarkerScenarioSelected),
  }

  /// Counterexample path: retrying the ordinary probe against the stale routed
  /// lane cannot observe the exact canonical durable prefix, so every bounded
  /// attempt returns to the same frozen retry state.
  action rejectOldOrdinaryProbeThroughStaleRoutedLane = all {
    probeBackend.scenario == OrdinaryExactCanonicalStaleRouted,
    probeBackend.routedLaneBackend == PrefixStale,
    not(probeBackend.directOrdinaryProbeObserved),
    probeBackend' = probeBackend
      .with("ordinaryHandoffFrozen", true)
      .with("ordinaryRetryEnabled", true)
      .with("oldRoutedProbeAttempts", probeBackend.oldRoutedProbeAttempts + 1)
      .with("oldRoutedOrdinaryAck", false)
      .with("lastTransition", OldRoutedOrdinaryProbeRejected),
  }

  /// Hardened ordinary probes are purpose-bound to the direct backend. Their
  /// ACK and durable prefix floor are exact functions of canonical state and do
  /// not consult an independently stale routed lane.
  action probeOrdinaryThroughCanonicalDirectBackend = all {
    any {
      probeBackend.scenario == OrdinaryExactCanonicalStaleRouted,
      probeBackend.scenario == OrdinaryCanonicalMismatch,
    },
    not(probeBackend.directOrdinaryProbeObserved),
    probeBackend' = probeBackend
      .with("ordinaryHandoffFrozen", not(canonicalDirectPrefixIsExact(probeBackend)))
      .with("ordinaryRetryEnabled", not(canonicalDirectPrefixIsExact(probeBackend)))
      .with("directOrdinaryProbeObserved", true)
      .with("directProbeUsedCanonicalBackend", true)
      .with("directOrdinaryAck", canonicalDirectPrefixIsExact(probeBackend))
      .with("directOrdinaryRejected", not(canonicalDirectPrefixIsExact(probeBackend)))
      .with(
        "ordinaryPrefixAckFloorPersisted",
        canonicalDirectPrefixIsExact(probeBackend)
      )
      .with(
        "lastTransition",
        if (canonicalDirectPrefixIsExact(probeBackend)) {
          CanonicalDirectOrdinaryProbeAccepted
        } else {
          CanonicalDirectOrdinaryProbeRejected
        }
      ),
  }

  /// Promotion probes deliberately retain the routed path: current routed
  /// prefix plus current promotion marker are both load-bearing evidence.
  action probePromotedPrimaryThroughSpecialRoutedMarkerPath = all {
    any {
      probeBackend.scenario == PromotedCurrentMarker,
      probeBackend.scenario == PromotedStaleMarker,
    },
    not(probeBackend.promotedProbeObserved),
    probeBackend' = probeBackend
      .with("promotedProbeObserved", true)
      .with("promotedProbeUsedSpecialRoutedPath", true)
      .with("promotedAck", promotedSpecialRoutedEvidenceIsExact(probeBackend))
      .with("promotedRejected", not(promotedSpecialRoutedEvidenceIsExact(probeBackend)))
      .with(
        "lastTransition",
        if (promotedSpecialRoutedEvidenceIsExact(probeBackend)) {
          PromotedSpecialRoutedProbeAccepted
        } else {
          PromotedSpecialRoutedProbeRejected
        }
      ),
  }

  action stutter =
    probeBackend' = probeBackend.with("lastTransition", ProbeBackendStuttered)

  action step = any {
    selectOrdinaryExactCanonicalStaleRouted,
    selectOrdinaryCanonicalMismatch,
    selectPromotedCurrentMarker,
    selectPromotedStaleMarker,
    rejectOldOrdinaryProbeThroughStaleRoutedLane,
    probeOrdinaryThroughCanonicalDirectBackend,
    probePromotedPrimaryThroughSpecialRoutedMarkerPath,
    stutter,
  }

  /// Coverage-only schedulers invoke the ordinary transition actions. The
  /// symbolic check retains unrestricted backend/purpose interleaving via
  /// `step`.
  action ordinaryStaleLaneCoverageStep =
    if (probeBackend.scenario == ScenarioUnselected) {
      selectOrdinaryExactCanonicalStaleRouted
    } else if (probeBackend.oldRoutedProbeAttempts < 2) {
      rejectOldOrdinaryProbeThroughStaleRoutedLane
    } else if (not(probeBackend.directOrdinaryProbeObserved)) {
      probeOrdinaryThroughCanonicalDirectBackend
    } else {
      stutter
    }

  action ordinaryMismatchCoverageStep =
    if (probeBackend.scenario == ScenarioUnselected) {
      selectOrdinaryCanonicalMismatch
    } else if (not(probeBackend.directOrdinaryProbeObserved)) {
      probeOrdinaryThroughCanonicalDirectBackend
    } else {
      stutter
    }

  action promotedCurrentMarkerCoverageStep =
    if (probeBackend.scenario == ScenarioUnselected) {
      selectPromotedCurrentMarker
    } else if (not(probeBackend.promotedProbeObserved)) {
      probePromotedPrimaryThroughSpecialRoutedMarkerPath
    } else {
      stutter
    }

  action promotedStaleMarkerCoverageStep =
    if (probeBackend.scenario == ScenarioUnselected) {
      selectPromotedStaleMarker
    } else if (not(probeBackend.promotedProbeObserved)) {
      probePromotedPrimaryThroughSpecialRoutedMarkerPath
    } else {
      stutter
    }

  action coverageStep = any {
    step,
    ordinaryStaleLaneCoverageStep,
    ordinaryMismatchCoverageStep,
    promotedCurrentMarkerCoverageStep,
    promotedStaleMarkerCoverageStep,
  }
}
