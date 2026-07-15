/// Exact-prefix validation before persisting a DFS metadata authority ACK.
///
/// Evidence class: target hardening for the liveness failure where a replica
/// with divergent prior history applies and re-reads the named prepared
/// operation at the expected sequence, persists an authority-acknowledgement
/// floor, and then cannot accept the authoritative snapshot needed to repair
/// its divergent prefix. Operation presence is necessary but not sufficient:
/// the receiver's complete resulting prefix must equal the primary's prepared
/// prefix before the ACK floor may advance.
///
/// This bounded model applies one named operation to either the exact
/// predecessor or a divergent predecessor. The divergent result has the same
/// sequence and named operation as the primary but a different prefix digest.
/// It must reject without a floor, install one authoritative snapshot, and may
/// then acknowledge the repaired exact prefix. Torn persistence, transport
/// retry, multiple operations, and concurrent snapshot installation remain
/// outside this slice.
module dfs_metadata_authority_ack_prefix_validation {
  type PriorPrefix = PriorUnknown | ExactPredecessor | DivergentPredecessor
  type ReceiverPrefix = PrefixBeforePrepared | PreparedPrefixExact | PreparedPrefixDivergent
  type AckPrefixTransition =
    | AckPrefixInitialized
    | NamedOperationAppliedToExactPredecessor
    | NamedOperationAppliedToDivergentPredecessor
    | HealthyAuthorityAckAdmitted
    | DivergentAuthorityAckRejected
    | AuthoritativeSnapshotInstalled
    | RepairedAuthorityAckAdmitted
    | AckPrefixStuttered

  type AckPrefixState = {
    priorPrefix: PriorPrefix,
    receiverPrefix: ReceiverPrefix,
    receiverSequence: int,
    namedOperationAppliedAndVerified: bool,
    authorityFloorPersisted: bool,
    authorityFloorSequence: int,
    authorityAckAccepted: bool,
    divergentAckRejected: bool,
    authoritativeSnapshotInstalled: bool,
    lastTransition: AckPrefixTransition,
  }

  pure val PrimaryPreparedSequence: int = 1

  pure def resultingPrefixEqualsPrimaryPrepared(
    state: AckPrefixState,
  ): bool = all {
    state.receiverSequence == PrimaryPreparedSequence,
    state.receiverPrefix == PreparedPrefixExact,
  }

  /// A divergent authority floor at the prepared sequence would fence the
  /// exact snapshot at that same sequence. Omitting the rejected floor keeps
  /// authoritative repair admissible.
  pure def snapshotRepairAllowedWith(
    floorPersisted: bool,
    prefixIsExact: bool,
  ): bool = not(floorPersisted) or prefixIsExact

  pure def authoritativeSnapshotRepairAllowed(state: AckPrefixState): bool =
    snapshotRepairAllowedWith(
      state.authorityFloorPersisted,
      resultingPrefixEqualsPrimaryPrepared(state)
    )

  pure val initialState: AckPrefixState = {
    priorPrefix: PriorUnknown,
    receiverPrefix: PrefixBeforePrepared,
    receiverSequence: 0,
    namedOperationAppliedAndVerified: false,
    authorityFloorPersisted: false,
    authorityFloorSequence: 0,
    authorityAckAccepted: false,
    divergentAckRejected: false,
    authoritativeSnapshotInstalled: false,
    lastTransition: AckPrefixInitialized,
  }

  var ackPrefix: AckPrefixState

  action init = ackPrefix' = initialState

  action applyNamedOperationToExactPredecessor = all {
    not(ackPrefix.namedOperationAppliedAndVerified),
    ackPrefix' = ackPrefix
      .with("priorPrefix", ExactPredecessor)
      .with("receiverPrefix", PreparedPrefixExact)
      .with("receiverSequence", PrimaryPreparedSequence)
      .with("namedOperationAppliedAndVerified", true)
      .with("lastTransition", NamedOperationAppliedToExactPredecessor),
  }

  /// The operation payload is valid and re-readable, but hashing it over a
  /// divergent predecessor produces a different complete prefix at the same
  /// sequence. This is the observed failure cut.
  action applyNamedOperationToDivergentPredecessor = all {
    not(ackPrefix.namedOperationAppliedAndVerified),
    ackPrefix' = ackPrefix
      .with("priorPrefix", DivergentPredecessor)
      .with("receiverPrefix", PreparedPrefixDivergent)
      .with("receiverSequence", PrimaryPreparedSequence)
      .with("namedOperationAppliedAndVerified", true)
      .with("lastTransition", NamedOperationAppliedToDivergentPredecessor),
  }

  action admitAuthorityAckForExactResult = all {
    ackPrefix.namedOperationAppliedAndVerified,
    resultingPrefixEqualsPrimaryPrepared(ackPrefix),
    not(ackPrefix.authorityAckAccepted),
    ackPrefix' = ackPrefix
      .with("authorityFloorPersisted", true)
      .with("authorityFloorSequence", PrimaryPreparedSequence)
      .with("authorityAckAccepted", true)
      .with(
        "lastTransition",
        if (ackPrefix.authoritativeSnapshotInstalled) {
          RepairedAuthorityAckAdmitted
        } else {
          HealthyAuthorityAckAdmitted
        }
      ),
  }

  /// Rejection is side-effect free with respect to the durable authority floor.
  /// The named operation may remain locally materialized, but it is not an ACK.
  action rejectAuthorityAckForDivergentResult = all {
    ackPrefix.namedOperationAppliedAndVerified,
    not(resultingPrefixEqualsPrimaryPrepared(ackPrefix)),
    not(ackPrefix.divergentAckRejected),
    not(ackPrefix.authorityFloorPersisted),
    ackPrefix' = ackPrefix
      .with("divergentAckRejected", true)
      .with("lastTransition", DivergentAuthorityAckRejected),
  }

  action installAuthoritativeSnapshotAfterRejection = all {
    ackPrefix.divergentAckRejected,
    ackPrefix.receiverPrefix == PreparedPrefixDivergent,
    not(ackPrefix.authorityFloorPersisted),
    authoritativeSnapshotRepairAllowed(ackPrefix),
    not(ackPrefix.authoritativeSnapshotInstalled),
    ackPrefix' = ackPrefix
      .with("receiverPrefix", PreparedPrefixExact)
      .with("receiverSequence", PrimaryPreparedSequence)
      .with("authoritativeSnapshotInstalled", true)
      .with("lastTransition", AuthoritativeSnapshotInstalled),
  }

  action stutter =
    ackPrefix' = ackPrefix.with("lastTransition", AckPrefixStuttered)

  action step = any {
    applyNamedOperationToExactPredecessor,
    applyNamedOperationToDivergentPredecessor,
    admitAuthorityAckForExactResult,
    rejectAuthorityAckForDivergentResult,
    installAuthoritativeSnapshotAfterRejection,
    stutter,
  }

  /// Coverage-only schedules call the ordinary actions. Apalache verifies the
  /// unrestricted `step` relation.
  action healthyCoverageStep =
    if (not(ackPrefix.namedOperationAppliedAndVerified)) {
      applyNamedOperationToExactPredecessor
    } else if (not(ackPrefix.authorityAckAccepted)) {
      admitAuthorityAckForExactResult
    } else {
      stutter
    }

  action divergentRepairCoverageStep =
    if (not(ackPrefix.namedOperationAppliedAndVerified)) {
      applyNamedOperationToDivergentPredecessor
    } else if (not(ackPrefix.divergentAckRejected)) {
      rejectAuthorityAckForDivergentResult
    } else if (not(ackPrefix.authoritativeSnapshotInstalled)) {
      installAuthoritativeSnapshotAfterRejection
    } else if (not(ackPrefix.authorityAckAccepted)) {
      admitAuthorityAckForExactResult
    } else {
      stutter
    }

  action coverageStep = any {
    step,
    healthyCoverageStep,
    divergentRepairCoverageStep,
  }
}
