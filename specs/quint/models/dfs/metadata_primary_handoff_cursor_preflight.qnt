/// Cursor-assisted preflight for a proof-bound metadata-primary handoff.
///
/// Evidence class: core-algorithm target hardening for the placement stall in
/// which replica expansion certifies head 1, an ordinary write advances the
/// primary to head 2, and ordinary replication durably catches the promotion
/// target up without publishing another KV watermark. The old exact-current-
/// watermark preflight then remains false even though the real handoff can run.
///
/// The optimized rule uses the retiring primary's durable sender cursor only
/// to begin a pending handoff. It never treats that cursor as fingerprint or
/// activation proof. The retiring primary still fences its latest head, every
/// member of the unchanged ring must freshly acknowledge that exact prefix,
/// and only then may the target become primary. A cursor backed by divergent
/// target state can therefore begin at most a repairable pending handoff.
///
/// N1 is the retiring primary, N2 the target replica, and N3 a stable replica.
/// Heads 1 through 3, one late pre-fence write, divergent target state, and
/// authoritative repair are bounded here. Crash/rebind and transport retry are
/// covered by adjacent handoff models.
module dfs_metadata_primary_handoff_cursor_preflight {
  import common.* from "../../lib/common"

  type HandoffPhase = Expanded | Pending | Fenced | Activated
  type CursorPreflightTransition =
    | CursorPreflightInitialized
    | PrimaryHeadAdvanced
    | TargetCursorCaughtUpExact
    | TargetCursorCaughtUpDivergent
    | HandoffBeganFromCertifiedHead
    | HandoffBeganFromDurableCursor
    | RetiringPrimaryFenced
    | StableReplicaAcknowledged
    | TargetExactProbeAccepted
    | TargetDivergentProbeRejected
    | TargetSnapshotRepaired
    | TargetActivated
    | CursorPreflightStuttered

  type CursorPreflightState = {
    phase: HandoffPhase,
    activePrimary: Node,
    primaryHead: int,
    certifiedHead: int,
    targetCursor: int,
    targetPrefixHead: int,
    targetExactFingerprint: bool,
    fencedHead: int,
    retiringFenceInstalled: bool,
    freshExactAcks: Set[Node],
    divergentProbeRejected: bool,
    snapshotRepaired: bool,
    beganFromCursor: bool,
    lastTransition: CursorPreflightTransition,
  }

  pure val ConfiguredMembers: Set[Node] = Set(N1, N2, N3)

  pure def certifiedHeadPreflightReady(state: CursorPreflightState): bool =
    state.certifiedHead == state.primaryHead

  pure def caughtUpCursorPreflightReady(state: CursorPreflightState): bool =
    state.targetCursor >= state.primaryHead

  pure def optimizedPreflightReady(state: CursorPreflightState): bool = any {
    certifiedHeadPreflightReady(state),
    caughtUpCursorPreflightReady(state),
  }

  pure def retiringWritable(state: CursorPreflightState): bool = all {
    state.activePrimary == N1,
    not(state.retiringFenceInstalled),
  }

  pure def promotedWritable(state: CursorPreflightState): bool = all {
    state.phase == Activated,
    state.activePrimary == N2,
    state.retiringFenceInstalled,
    state.freshExactAcks == ConfiguredMembers,
    state.targetExactFingerprint,
    state.targetPrefixHead == state.fencedHead,
  }

  pure val initialState: CursorPreflightState = {
    phase: Expanded,
    activePrimary: N1,
    primaryHead: 1,
    certifiedHead: 1,
    targetCursor: 1,
    targetPrefixHead: 1,
    targetExactFingerprint: true,
    fencedHead: 0,
    retiringFenceInstalled: false,
    freshExactAcks: Set(),
    divergentProbeRejected: false,
    snapshotRepaired: false,
    beganFromCursor: false,
    lastTransition: CursorPreflightInitialized,
  }

  var preflight: CursorPreflightState

  action init = preflight' = initialState

  /// A pending intent does not fence N1. One already admitted write may still
  /// advance the head; the later publication fence captures that successor.
  action advancePrimaryHead = all {
    any { preflight.phase == Expanded, preflight.phase == Pending },
    preflight.primaryHead < 3,
    preflight' = preflight
      .with("primaryHead", preflight.primaryHead + 1)
      .with("targetExactFingerprint", false)
      .with("lastTransition", PrimaryHeadAdvanced),
  }

  action catchUpTargetExactly = all {
    any { preflight.phase == Expanded, preflight.phase == Fenced },
    not(preflight.freshExactAcks.contains(N2)),
    preflight' = preflight
      .with("targetCursor", preflight.primaryHead)
      .with("targetPrefixHead", preflight.primaryHead)
      .with("targetExactFingerprint", true)
      .with("lastTransition", TargetCursorCaughtUpExact),
  }

  /// Cursor persistence is deliberately independent from exact-prefix proof.
  action recordCaughtUpDivergentCursor = all {
    any { preflight.phase == Expanded, preflight.phase == Fenced },
    not(preflight.freshExactAcks.contains(N2)),
    preflight' = preflight
      .with("targetCursor", preflight.primaryHead)
      .with("targetPrefixHead", preflight.primaryHead)
      .with("targetExactFingerprint", false)
      .with("lastTransition", TargetCursorCaughtUpDivergent),
  }

  action beginHandoff = all {
    preflight.phase == Expanded,
    preflight.activePrimary == N1,
    optimizedPreflightReady(preflight),
    preflight' = preflight
      .with("phase", Pending)
      .with(
        "beganFromCursor",
        not(certifiedHeadPreflightReady(preflight))
          and caughtUpCursorPreflightReady(preflight)
      )
      .with(
        "lastTransition",
        if (certifiedHeadPreflightReady(preflight)) {
          HandoffBeganFromCertifiedHead
        } else {
          HandoffBeganFromDurableCursor
        }
      ),
  }

  action installRetiringFence = all {
    preflight.phase == Pending,
    preflight.activePrimary == N1,
    preflight' = preflight
      .with("phase", Fenced)
      .with("fencedHead", preflight.primaryHead)
      .with("retiringFenceInstalled", true)
      .with("freshExactAcks", Set(N1))
      .with("lastTransition", RetiringPrimaryFenced),
  }

  action acknowledgeStableReplica = all {
    preflight.phase == Fenced,
    not(preflight.freshExactAcks.contains(N3)),
    preflight' = preflight
      .with("freshExactAcks", preflight.freshExactAcks.union(Set(N3)))
      .with("lastTransition", StableReplicaAcknowledged),
  }

  action probeTargetPrefix = all {
    preflight.phase == Fenced,
    not(preflight.freshExactAcks.contains(N2)),
    if (all {
      preflight.targetCursor >= preflight.fencedHead,
      preflight.targetPrefixHead == preflight.fencedHead,
      preflight.targetExactFingerprint,
    }) {
      preflight' = preflight
        .with("freshExactAcks", preflight.freshExactAcks.union(Set(N2)))
        .with("lastTransition", TargetExactProbeAccepted)
    } else {
      preflight' = preflight
        .with("divergentProbeRejected", true)
        .with("lastTransition", TargetDivergentProbeRejected)
    },
  }

  action repairTargetFromAuthoritativeSnapshot = all {
    preflight.phase == Fenced,
    preflight.divergentProbeRejected,
    not(preflight.freshExactAcks.contains(N2)),
    preflight' = preflight
      .with("targetCursor", preflight.fencedHead)
      .with("targetPrefixHead", preflight.fencedHead)
      .with("targetExactFingerprint", true)
      .with("snapshotRepaired", true)
      .with("lastTransition", TargetSnapshotRepaired),
  }

  action activateTarget = all {
    preflight.phase == Fenced,
    preflight.retiringFenceInstalled,
    preflight.freshExactAcks == ConfiguredMembers,
    preflight' = preflight
      .with("phase", Activated)
      .with("activePrimary", N2)
      .with("lastTransition", TargetActivated),
  }

  action stutter =
    preflight' = preflight.with("lastTransition", CursorPreflightStuttered)

  action step = any {
    advancePrimaryHead,
    catchUpTargetExactly,
    recordCaughtUpDivergentCursor,
    beginHandoff,
    installRetiringFence,
    acknowledgeStableReplica,
    probeTargetPrefix,
    repairTargetFromAuthoritativeSnapshot,
    activateTarget,
    stutter,
  }
}
