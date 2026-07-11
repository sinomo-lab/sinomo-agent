/// Typed publication evidence for the local-first reconciliation gateway.
///
/// The earlier projection contract deliberately represented canonical commit
/// authority as seven booleans. This module refines those booleans into
/// correlated, finite certificates. A permit is valid only for one request
/// payload, authority epoch, configuration revision, journal predecessor, and
/// atomic publication envelope. Reusing any certificate after one of those
/// facts changes makes the derived evidence false.
module dfs_local_first_reconcile_permit {
  import common.* from "../../lib/common"
  import dfs_local_first_projection.* from "local_first_projection"

  type ReconcileRequestId = GatewayRequest | GatewayRequestReserved
  type ReconcilePayloadDigest = PayloadA | PayloadB
  type RetainedArtifactDigest = ArtifactA | ArtifactB
  type ReconcileOutcome = PublishApply | PublishConverged | PublishConflict
  type ReconcileResultDigest = ResultApplied | ResultConverged | ResultConflict

  type ReconcilePublicationPlan = {
    requestId: ReconcileRequestId,
    payloadDigest: ReconcilePayloadDigest,
    outcome: ReconcileOutcome,
    primary: Node,
    authorityEpoch: int,
    configRevision: int,
    journalHead: int,
    objectRevision: int,
    targetObjectRevision: int,
    retainedArtifact: RetainedArtifactDigest,
  }

  /// The journal stores this whole envelope at its linearization point. The
  /// terminal result is therefore part of the same durable record as the
  /// canonical delta and optional conflict sidecar descriptor.
  type AtomicReconcilePublication = {
    requestId: ReconcileRequestId,
    payloadDigest: ReconcilePayloadDigest,
    outcome: ReconcileOutcome,
    previousJournalHead: int,
    nextJournalHead: int,
    previousObjectRevision: int,
    nextObjectRevision: int,
    resultDigest: ReconcileResultDigest,
    conflictArtifact: Option[RetainedArtifactDigest],
  }

  /// Evidence that one quorum acknowledged the exact entry intended for the
  /// plan's successor journal slot. Carrying the publication envelope prevents
  /// votes for one outcome or canonical delta from authorizing another.
  type ReconcileQuorumCertificate = {
    configRevision: int,
    authorityEpoch: int,
    journalIndex: int,
    payloadDigest: ReconcilePayloadDigest,
    publication: AtomicReconcilePublication,
    voters: Set[Node],
  }

  /// The route and authority epoch are frozen independently from the quorum
  /// certificate so a stale primary cannot borrow another epoch's votes.
  type ReconcileAuthorityCertificate = {
    primary: Node,
    configRevision: int,
    authorityEpoch: int,
  }

  /// Minimal retention-receipt certificate for this bounded model. It proves
  /// only that the symbolic artifact was acknowledged by the configured
  /// number of distinct holders; concrete byte hashes and storage recovery
  /// remain a lower-layer refinement obligation.
  type ReconcileContentCertificate = {
    payloadDigest: ReconcilePayloadDigest,
    retainedArtifact: RetainedArtifactDigest,
    holders: Set[Node],
  }

  /// Metadata prepare votes are bound to the same epoch, predecessor, payload,
  /// and successor entry as the quorum certificate.
  type ReconcilePrepareCertificate = {
    configRevision: int,
    authorityEpoch: int,
    journalHead: int,
    journalIndex: int,
    payloadDigest: ReconcilePayloadDigest,
    publication: AtomicReconcilePublication,
    voters: Set[Node],
  }

  type ReconcilePermit = {
    plan: ReconcilePublicationPlan,
    quorum: ReconcileQuorumCertificate,
    authority: ReconcileAuthorityCertificate,
    content: ReconcileContentCertificate,
    prepare: ReconcilePrepareCertificate,
    publication: AtomicReconcilePublication,
  }

  pure val ReconcilePayloads: Set[ReconcilePayloadDigest] =
    Set(PayloadA, PayloadB)
  pure val ReconcileOutcomes: Set[ReconcileOutcome] =
    Set(PublishApply, PublishConverged, PublishConflict)
  pure val contentReceiptThreshold: int = 2

  pure def retainedArtifactFor(
    payload: ReconcilePayloadDigest
  ): RetainedArtifactDigest =
    match payload {
      | PayloadA => ArtifactA
      | PayloadB => ArtifactB
    }

  pure def resultDigestFor(outcome: ReconcileOutcome): ReconcileResultDigest =
    match outcome {
      | PublishApply => ResultApplied
      | PublishConverged => ResultConverged
      | PublishConflict => ResultConflict
    }

  pure def targetObjectRevisionFor(
    outcome: ReconcileOutcome,
    currentRevision: int
  ): int =
    if (outcome == PublishApply) {
      currentRevision + 1
    } else {
      currentRevision
    }

  pure def conflictArtifactFor(
    outcome: ReconcileOutcome,
    artifact: RetainedArtifactDigest
  ): Option[RetainedArtifactDigest] =
    if (outcome == PublishConflict) { Some(artifact) } else { None }

  pure def publicationForPlan(
    plan: ReconcilePublicationPlan
  ): AtomicReconcilePublication = {
    requestId: plan.requestId,
    payloadDigest: plan.payloadDigest,
    outcome: plan.outcome,
    previousJournalHead: plan.journalHead,
    nextJournalHead: plan.journalHead + 1,
    previousObjectRevision: plan.objectRevision,
    nextObjectRevision: plan.targetObjectRevision,
    resultDigest: resultDigestFor(plan.outcome),
    conflictArtifact:
      conflictArtifactFor(plan.outcome, plan.retainedArtifact),
  }

  pure def reconcilePlanWellFormed(
    plan: ReconcilePublicationPlan
  ): bool = all {
    plan.authorityEpoch > 0,
    plan.configRevision > 0,
    plan.journalHead >= 0,
    plan.objectRevision >= 0,
    plan.targetObjectRevision
      == targetObjectRevisionFor(plan.outcome, plan.objectRevision),
    plan.retainedArtifact == retainedArtifactFor(plan.payloadDigest),
  }

  pure def quorumCertificateReady(
    permit: ReconcilePermit,
    currentConfigRevision: int
  ): bool = all {
    permit.quorum.configRevision == currentConfigRevision,
    permit.quorum.configRevision == permit.plan.configRevision,
    permit.quorum.authorityEpoch == permit.plan.authorityEpoch,
    permit.quorum.journalIndex == permit.plan.journalHead + 1,
    permit.quorum.payloadDigest == permit.plan.payloadDigest,
    permit.quorum.publication == permit.publication,
    permit.quorum.voters.subseteq(Voters),
    hasQuorum(permit.quorum.voters),
  }

  pure def authorityCertificateReady(
    permit: ReconcilePermit,
    currentPrimary: Node,
    currentAuthorityEpoch: int,
    currentConfigRevision: int
  ): bool = all {
    permit.authority.primary == currentPrimary,
    permit.authority.primary == permit.plan.primary,
    permit.authority.authorityEpoch == currentAuthorityEpoch,
    permit.authority.authorityEpoch == permit.plan.authorityEpoch,
    permit.authority.configRevision == currentConfigRevision,
    permit.authority.configRevision == permit.plan.configRevision,
  }

  pure def epochCertificatesMatch(
    permit: ReconcilePermit,
    currentAuthorityEpoch: int
  ): bool = all {
    permit.plan.authorityEpoch == currentAuthorityEpoch,
    permit.quorum.authorityEpoch == currentAuthorityEpoch,
    permit.authority.authorityEpoch == currentAuthorityEpoch,
    permit.prepare.authorityEpoch == currentAuthorityEpoch,
  }

  pure def planHeadCertificateReady(
    permit: ReconcilePermit,
    currentJournalHead: int,
    currentObjectRevision: int
  ): bool = all {
    permit.plan.journalHead == currentJournalHead,
    permit.plan.objectRevision == currentObjectRevision,
    permit.quorum.journalIndex == currentJournalHead + 1,
    permit.prepare.journalHead == currentJournalHead,
    permit.prepare.journalIndex == currentJournalHead + 1,
  }

  pure def contentCertificateReady(permit: ReconcilePermit): bool = all {
    permit.content.payloadDigest == permit.plan.payloadDigest,
    permit.content.retainedArtifact == permit.plan.retainedArtifact,
    permit.content.holders.subseteq(Nodes),
    permit.content.holders.size() >= contentReceiptThreshold,
  }

  pure def prepareCertificateReady(
    permit: ReconcilePermit,
    currentConfigRevision: int
  ): bool = all {
    permit.prepare.configRevision == currentConfigRevision,
    permit.prepare.configRevision == permit.plan.configRevision,
    permit.prepare.authorityEpoch == permit.plan.authorityEpoch,
    permit.prepare.journalHead == permit.plan.journalHead,
    permit.prepare.journalIndex == permit.plan.journalHead + 1,
    permit.prepare.payloadDigest == permit.plan.payloadDigest,
    permit.prepare.publication == permit.publication,
    permit.prepare.voters.subseteq(Voters),
    hasQuorum(permit.prepare.voters),
  }

  pure def atomicPublicationReady(permit: ReconcilePermit): bool = all {
    reconcilePlanWellFormed(permit.plan),
    permit.publication == publicationForPlan(permit.plan),
  }

  /// This is the only adapter back to the original projection contract. None
  /// of its fields is asserted directly: each is derived from a typed witness
  /// and its correlation with the current canonical state.
  pure def canonicalEvidenceFromPermit(
    permit: ReconcilePermit,
    currentPrimary: Node,
    currentAuthorityEpoch: int,
    currentConfigRevision: int,
    currentJournalHead: int,
    currentObjectRevision: int
  ): CanonicalCommitEvidence = {
    quorumAvailable:
      quorumCertificateReady(permit, currentConfigRevision),
    currentAuthority:
      authorityCertificateReady(
        permit,
        currentPrimary,
        currentAuthorityEpoch,
        currentConfigRevision
      ),
    epochMatches:
      epochCertificatesMatch(permit, currentAuthorityEpoch),
    planHeadMatches:
      planHeadCertificateReady(
        permit,
        currentJournalHead,
        currentObjectRevision
      ),
    contentPolicySatisfied: contentCertificateReady(permit),
    metadataPrepareSatisfied:
      prepareCertificateReady(permit, currentConfigRevision),
    atomicPublication: atomicPublicationReady(permit),
  }

  pure def reconcilePermitReady(
    permit: ReconcilePermit,
    currentPrimary: Node,
    currentAuthorityEpoch: int,
    currentConfigRevision: int,
    currentJournalHead: int,
    currentObjectRevision: int
  ): bool = all {
    reconcilePlanWellFormed(permit.plan),
    canonicalCommitEvidenceReady(canonicalEvidenceFromPermit(
      permit,
      currentPrimary,
      currentAuthorityEpoch,
      currentConfigRevision,
      currentJournalHead,
      currentObjectRevision
    )),
  }
}
