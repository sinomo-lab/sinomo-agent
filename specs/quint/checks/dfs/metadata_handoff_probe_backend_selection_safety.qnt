/// Safety and bounded liveness witnesses for purpose-separated probe backends.
module dfs_metadata_handoff_probe_backend_selection_safety {
  import dfs_metadata_handoff_probe_backend_selection.* from "../../models/dfs/metadata_handoff_probe_backend_selection"

  def selectedScenarioViewsStayDistinctAndStable: bool =
    if (probeBackend.scenario == OrdinaryExactCanonicalStaleRouted) {
      all {
        probeBackend.canonicalDirectBackend == PrefixExact,
        probeBackend.routedLaneBackend == PrefixStale,
      }
    } else if (probeBackend.scenario == OrdinaryCanonicalMismatch) {
      all {
        probeBackend.canonicalDirectBackend == PrefixMismatch,
        probeBackend.routedLaneBackend == PrefixStale,
      }
    } else if (probeBackend.scenario == PromotedCurrentMarker) {
      all {
        probeBackend.canonicalDirectBackend == PrefixExact,
        probeBackend.routedLaneBackend == PrefixExact,
        probeBackend.promotionMarker == MarkerCurrent,
      }
    } else if (probeBackend.scenario == PromotedStaleMarker) {
      all {
        probeBackend.canonicalDirectBackend == PrefixExact,
        probeBackend.routedLaneBackend == PrefixExact,
        probeBackend.promotionMarker == MarkerStale,
      }
    } else {
      probeBackend.scenario == ScenarioUnselected
    }

  /// The fixed ordinary decision is exactly the durable canonical proof. The
  /// routed view is intentionally absent from this equivalence.
  def directOrdinaryAckIffCanonicalDurablePrefixExact: bool =
    probeBackend.directOrdinaryProbeObserved implies all {
      probeBackend.directProbeUsedCanonicalBackend,
      probeBackend.directOrdinaryAck
        iff canonicalDirectPrefixIsExact(probeBackend),
      probeBackend.directOrdinaryRejected
        iff not(canonicalDirectPrefixIsExact(probeBackend)),
      probeBackend.ordinaryPrefixAckFloorPersisted
        iff canonicalDirectPrefixIsExact(probeBackend),
    }

  def ordinaryPrefixFloorRequiresCanonicalDirectAck: bool =
    probeBackend.ordinaryPrefixAckFloorPersisted implies all {
      probeBackend.directOrdinaryProbeObserved,
      probeBackend.directProbeUsedCanonicalBackend,
      probeBackend.directOrdinaryAck,
      canonicalDirectPrefixIsExact(probeBackend),
    }

  def staleRoutedViewCannotKeepSuccessfulOrdinaryProbeFrozen: bool = all {
    probeBackend.scenario == OrdinaryExactCanonicalStaleRouted,
    probeBackend.directOrdinaryProbeObserved,
  } implies all {
    probeBackend.directOrdinaryAck,
    probeBackend.ordinaryPrefixAckFloorPersisted,
    not(probeBackend.ordinaryHandoffFrozen),
    not(probeBackend.ordinaryRetryEnabled),
  }

  def canonicalMismatchStillRejectsOrdinaryProbe: bool = all {
    probeBackend.scenario == OrdinaryCanonicalMismatch,
    probeBackend.directOrdinaryProbeObserved,
  } implies all {
    probeBackend.directOrdinaryRejected,
    not(probeBackend.directOrdinaryAck),
    not(probeBackend.ordinaryPrefixAckFloorPersisted),
    probeBackend.ordinaryHandoffFrozen,
    probeBackend.ordinaryRetryEnabled,
  }

  /// Promotion is purpose-separated: exact canonical state is insufficient
  /// without current routed state and the current promotion marker.
  def promotedAckIffSpecialRoutedMarkerEvidenceExact: bool =
    probeBackend.promotedProbeObserved implies all {
      probeBackend.promotedProbeUsedSpecialRoutedPath,
      probeBackend.promotedAck
        iff promotedSpecialRoutedEvidenceIsExact(probeBackend),
      probeBackend.promotedRejected
        iff not(promotedSpecialRoutedEvidenceIsExact(probeBackend)),
      not(probeBackend.directOrdinaryProbeObserved),
      not(probeBackend.ordinaryPrefixAckFloorPersisted),
    }

  def oldRoutedRetryNeverMutatesCanonicalProof: bool =
    probeBackend.lastTransition == OldRoutedOrdinaryProbeRejected implies all {
      probeBackend.canonicalDirectBackend == PrefixExact,
      probeBackend.routedLaneBackend == PrefixStale,
      not(probeBackend.oldRoutedOrdinaryAck),
      probeBackend.ordinaryHandoffFrozen,
      probeBackend.ordinaryRetryEnabled,
      not(probeBackend.ordinaryPrefixAckFloorPersisted),
    }

  def witnessOldStaleRoutedProbeRepeatsWhileCanonicalIsExact: bool = all {
    probeBackend.lastTransition == OldRoutedOrdinaryProbeRejected,
    probeBackend.oldRoutedProbeAttempts >= 2,
    probeBackend.canonicalDirectBackend == PrefixExact,
    probeBackend.routedLaneBackend == PrefixStale,
    probeBackend.ordinaryHandoffFrozen,
    probeBackend.ordinaryRetryEnabled,
  }

  def witnessCanonicalDirectProbeBypassesStaleRoutedLane: bool = all {
    probeBackend.lastTransition == CanonicalDirectOrdinaryProbeAccepted,
    probeBackend.oldRoutedProbeAttempts >= 2,
    probeBackend.canonicalDirectBackend == PrefixExact,
    probeBackend.routedLaneBackend == PrefixStale,
    probeBackend.directOrdinaryAck,
    probeBackend.ordinaryPrefixAckFloorPersisted,
    not(probeBackend.ordinaryHandoffFrozen),
  }

  def witnessCanonicalDirectMismatchStillRejects: bool = all {
    probeBackend.lastTransition == CanonicalDirectOrdinaryProbeRejected,
    probeBackend.canonicalDirectBackend == PrefixMismatch,
    probeBackend.directOrdinaryRejected,
    not(probeBackend.ordinaryPrefixAckFloorPersisted),
    probeBackend.ordinaryHandoffFrozen,
  }

  def witnessPromotedCurrentMarkerUsesSpecialRoutedPath: bool = all {
    probeBackend.lastTransition == PromotedSpecialRoutedProbeAccepted,
    probeBackend.canonicalDirectBackend == PrefixExact,
    probeBackend.routedLaneBackend == PrefixExact,
    probeBackend.promotionMarker == MarkerCurrent,
    probeBackend.promotedProbeUsedSpecialRoutedPath,
    probeBackend.promotedAck,
    not(probeBackend.directOrdinaryProbeObserved),
  }

  def witnessPromotedStaleMarkerRejectsSpecialRoutedPath: bool = all {
    probeBackend.lastTransition == PromotedSpecialRoutedProbeRejected,
    probeBackend.canonicalDirectBackend == PrefixExact,
    probeBackend.routedLaneBackend == PrefixExact,
    probeBackend.promotionMarker == MarkerStale,
    probeBackend.promotedProbeUsedSpecialRoutedPath,
    probeBackend.promotedRejected,
    not(probeBackend.promotedAck),
  }

  def safety: bool = all {
    selectedScenarioViewsStayDistinctAndStable,
    directOrdinaryAckIffCanonicalDurablePrefixExact,
    ordinaryPrefixFloorRequiresCanonicalDirectAck,
    staleRoutedViewCannotKeepSuccessfulOrdinaryProbeFrozen,
    canonicalMismatchStillRejectsOrdinaryProbe,
    promotedAckIffSpecialRoutedMarkerEvidenceExact,
    oldRoutedRetryNeverMutatesCanonicalProof,
  }
}
