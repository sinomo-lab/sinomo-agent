/// Canonical projection contract for the opt-in local-first DFS extension.
///
/// Local branches are durable overlays, not metadata authorities. Focused
/// local-first models construct these compact snapshots around each transition
/// and use `projectionStepAllowed` to show that local work stutters on the
/// canonical DFS state. The only non-stuttering extension transition is an
/// authority-backed reconciliation commit with the same evidence required by
/// an ordinary strong commit.
module dfs_local_first_projection {
  type ProjectionStepKind =
    | LocalOverlayStep
    | LocalDurabilityStep
    | ReconcilePlanningStep
    | LocalConflictSidecarStep
    | StrongCanonicalCommitStep
    | ReconcileCanonicalCommitStep
    | ReconcileConflictCommitStep

  type CanonicalProjection = {
    authorityEpoch: int,
    journalHead: int,
    objectRevision: int,
    namespaceRevision: int,
    visibleAuthorityCount: int,
    visibleHeadCount: int,
    canonicalConflictCount: int,
  }

  type CanonicalCommitEvidence = {
    quorumAvailable: bool,
    currentAuthority: bool,
    epochMatches: bool,
    planHeadMatches: bool,
    contentPolicySatisfied: bool,
    metadataPrepareSatisfied: bool,
    atomicPublication: bool,
  }

  pure def canonicalProjectionWellFormed(
    projection: CanonicalProjection
  ): bool = all {
    projection.authorityEpoch > 0,
    projection.journalHead >= 0,
    projection.objectRevision >= 0,
    projection.namespaceRevision >= 0,
    projection.visibleAuthorityCount >= 0,
    projection.visibleAuthorityCount <= 1,
    projection.visibleHeadCount >= 0,
    projection.visibleHeadCount <= 1,
    projection.canonicalConflictCount == 0,
  }

  pure def canonicalProjectionStutters(
    before: CanonicalProjection,
    after: CanonicalProjection
  ): bool = before == after

  pure def canonicalCommitEvidenceReady(
    evidence: CanonicalCommitEvidence
  ): bool = all {
    evidence.quorumAvailable,
    evidence.currentAuthority,
    evidence.epochMatches,
    evidence.planHeadMatches,
    evidence.contentPolicySatisfied,
    evidence.metadataPrepareSatisfied,
    evidence.atomicPublication,
  }

  /// The focused extension models abstract an accepted file or namespace
  /// transaction as one journal publication. File and namespace revisions are
  /// monotone. A reconciliation outcome or an unrelated strong transaction may
  /// advance the journal without changing the bounded file/namespace slice.
  pure def authorizedCanonicalAdvance(
    before: CanonicalProjection,
    after: CanonicalProjection,
    evidence: CanonicalCommitEvidence
  ): bool = all {
    canonicalProjectionWellFormed(before),
    canonicalProjectionWellFormed(after),
    canonicalCommitEvidenceReady(evidence),
    after.authorityEpoch == before.authorityEpoch,
    after.journalHead == before.journalHead + 1,
    after.objectRevision >= before.objectRevision,
    after.namespaceRevision >= before.namespaceRevision,
    after.visibleAuthorityCount == 1,
    after.visibleHeadCount == 1,
    after.canonicalConflictCount == before.canonicalConflictCount,
  }

  pure def localProjectionStep(kind: ProjectionStepKind): bool = any {
    kind == LocalOverlayStep,
    kind == LocalDurabilityStep,
    kind == ReconcilePlanningStep,
    kind == LocalConflictSidecarStep,
  }

  pure def canonicalProjectionStep(kind: ProjectionStepKind): bool =
    kind == StrongCanonicalCommitStep
      or kind == ReconcileCanonicalCommitStep
      or kind == ReconcileConflictCommitStep

  pure def projectionStepAllowed(
    kind: ProjectionStepKind,
    before: CanonicalProjection,
    after: CanonicalProjection,
    evidence: CanonicalCommitEvidence
  ): bool =
    if (localProjectionStep(kind)) {
      all {
        canonicalProjectionWellFormed(before),
        canonicalProjectionStutters(before, after),
      }
    } else if (canonicalProjectionStep(kind)) {
      authorizedCanonicalAdvance(before, after, evidence)
    } else {
      false
    }

  /// Strong operations cannot mint extension-layer conflicts. Only a
  /// reconciliation sidecar transition may grow that disjoint set.
  pure def localConflictCountAllowed(
    kind: ProjectionStepKind,
    beforeCount: int,
    afterCount: int
  ): bool = all {
    beforeCount >= 0,
    afterCount >= 0,
    if (
      kind == LocalConflictSidecarStep
        or kind == ReconcileConflictCommitStep
    ) {
      afterCount >= beforeCount
    } else {
      afterCount == beforeCount
    },
  }
}
