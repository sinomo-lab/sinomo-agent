/// One-shard canonical publication gateway for local-first reconciliation.
///
/// This model turns the local-first projection's publication assumption into
/// an executable journal lifecycle. Planning and certificate collection are
/// volatile. Append creates an uncommitted slot. Only a fresh, quorum-backed
/// commit advances canonical state, and its durable entry contains both the
/// canonical delta and terminal result. Table/result/sidecar materialization
/// may lag and is rebuilt from that exact committed entry after a crash.
module dfs_local_first_reconcile_gateway {
  import common.* from "../../lib/common"
  import dfs_local_first_reconcile_permit.* from "../../contracts/dfs/local_first_reconcile_permit"

  type GatewayPhase =
    | GatewayIdle
    | GatewayPlanned
    | GatewayPermitReady
    | GatewayEntryAppended
    | GatewayCommitted
    | GatewayMaterialized
    | GatewayStopped

  type ConnectivityProfile =
    | PrimaryOnly
    | PrimaryQuorum
    | AllGatewayNodes

  type GatewayResponse =
    | NoGatewayResponse
    | GatewayPlanReady
    | GatewayPermitConstructed
    | GatewayPublicationRejected
    | GatewayOutcomeUnknown
    | GatewayResultReturned(ReconcileResultDigest)
    | GatewayPayloadMismatch

  type GatewayDecision =
    | NoGatewayDecision
    | GatewayConnectivityChanged
    | GatewayPlanCreated
    | GatewayQuorumAcksCollected
    | GatewayContentReceiptsCollected
    | GatewayPrepareAcksCollected
    | GatewayAllEvidenceCollected
    | GatewayPermitBuilt
    | GatewayEntryWasAppended
    | GatewayInsufficientEvidenceRejected
    | GatewayStalePublicationRejected
    | GatewayReconcileCommittedWithReply
    | GatewayReconcileCommittedReplyLost
    | GatewayProcessCrashedBeforeCommit
    | GatewayProcessCrashedAfterCommit
    | GatewayProcessRestarted
    | GatewayCommittedEntryMaterialized
    | GatewayExactRetryReplayed
    | GatewayChangedRetryRejected
    | GatewayStrongHeadAdvanced
    | GatewayAuthorityEpochAdvanced
    | GatewayTerminalStutter

  type GatewayCanonicalState = {
    primary: Node,
    authorityEpoch: int,
    configRevision: int,
    journalHead: int,
    objectRevision: int,
    canonicalConflictCount: int,
  }

  type GatewayEvidenceCollection = {
    quorumAcks: Set[Node],
    contentHolders: Set[Node],
    prepareAcks: Set[Node],
  }

  type GatewayJournalEntry = {
    index: int,
    permit: ReconcilePermit,
  }

  type GatewayJournalState = {
    entry: Option[GatewayJournalEntry],
    committed: bool,
  }

  type GatewayMaterializedState = {
    objectRevision: int,
    resultRecord: Option[AtomicReconcilePublication],
    conflictArtifact: Option[RetainedArtifactDigest],
  }

  /// Pre-state snapshot used to express transition refinement and crash
  /// properties as ordinary state invariants in the companion check module.
  type GatewayAudit = {
    canonicalBefore: GatewayCanonicalState,
    onlineBefore: Set[Node],
    phaseBefore: GatewayPhase,
    planBefore: Option[ReconcilePublicationPlan],
    evidenceBefore: GatewayEvidenceCollection,
    permitBefore: Option[ReconcilePermit],
    journalBefore: GatewayJournalState,
    materializedBefore: GatewayMaterializedState,
  }

  pure val MaxGatewayEpoch: int = 2
  pure val MaxGatewayJournalHead: int = 3
  pure val MaxGatewayObjectRevision: int = 2

  pure val initialCanonicalState: GatewayCanonicalState = {
    primary: N1,
    authorityEpoch: 1,
    configRevision: 1,
    journalHead: 0,
    objectRevision: 0,
    canonicalConflictCount: 0,
  }

  pure val emptyEvidenceCollection: GatewayEvidenceCollection = {
    quorumAcks: Set(),
    contentHolders: Set(),
    prepareAcks: Set(),
  }

  pure val emptyJournalState: GatewayJournalState = {
    entry: None,
    committed: false,
  }

  pure val initialMaterializedState: GatewayMaterializedState = {
    objectRevision: 0,
    resultRecord: None,
    conflictArtifact: None,
  }

  pure val defaultGatewayPlan: ReconcilePublicationPlan = {
    requestId: GatewayRequest,
    payloadDigest: PayloadA,
    outcome: PublishConverged,
    primary: N1,
    authorityEpoch: 1,
    configRevision: 1,
    journalHead: 0,
    objectRevision: 0,
    targetObjectRevision: 0,
    retainedArtifact: ArtifactA,
  }

  pure val defaultGatewayPermit: ReconcilePermit = {
    plan: defaultGatewayPlan,
    quorum: {
      configRevision: 1,
      authorityEpoch: 1,
      journalIndex: 1,
      payloadDigest: PayloadA,
      publication: publicationForPlan(defaultGatewayPlan),
      voters: Set(N1, N2),
    },
    authority: {
      primary: N1,
      configRevision: 1,
      authorityEpoch: 1,
    },
    content: {
      payloadDigest: PayloadA,
      retainedArtifact: ArtifactA,
      holders: Set(N1, N2),
    },
    prepare: {
      configRevision: 1,
      authorityEpoch: 1,
      journalHead: 0,
      journalIndex: 1,
      payloadDigest: PayloadA,
      publication: publicationForPlan(defaultGatewayPlan),
      voters: Set(N1, N2),
    },
    publication: publicationForPlan(defaultGatewayPlan),
  }

  pure val defaultGatewayEntry: GatewayJournalEntry = {
    index: 1,
    permit: defaultGatewayPermit,
  }

  pure val initialGatewayAudit: GatewayAudit = {
    canonicalBefore: initialCanonicalState,
    onlineBefore: Nodes,
    phaseBefore: GatewayIdle,
    planBefore: None,
    evidenceBefore: emptyEvidenceCollection,
    permitBefore: None,
    journalBefore: emptyJournalState,
    materializedBefore: initialMaterializedState,
  }

  var gatewayCanonical: GatewayCanonicalState
  var gatewayOnline: Set[Node]
  var gatewayPhase: GatewayPhase
  var gatewayPlan: Option[ReconcilePublicationPlan]
  var gatewayEvidence: GatewayEvidenceCollection
  var gatewayPermit: Option[ReconcilePermit]
  var gatewayJournal: GatewayJournalState
  var gatewayMaterialized: GatewayMaterializedState
  var gatewayLastResponse: GatewayResponse
  var gatewayLastDecision: GatewayDecision
  var gatewayLastSuppliedPayload: Option[ReconcilePayloadDigest]
  var gatewayAudit: GatewayAudit

  pure def onlineForProfile(
    profile: ConnectivityProfile,
    primary: Node
  ): Set[Node] =
    match profile {
      | PrimaryOnly => Set(primary)
      | PrimaryQuorum =>
          if (primary == N3) { Set(N2, N3) } else { Set(N1, N2) }
      | AllGatewayNodes => Nodes
    }

  pure def unwrapGatewayPlan(
    value: Option[ReconcilePublicationPlan]
  ): ReconcilePublicationPlan = unwrapOr(value, defaultGatewayPlan)

  pure def unwrapGatewayPermit(
    value: Option[ReconcilePermit]
  ): ReconcilePermit = unwrapOr(value, defaultGatewayPermit)

  pure def unwrapGatewayEntry(
    value: Option[GatewayJournalEntry]
  ): GatewayJournalEntry = unwrapOr(value, defaultGatewayEntry)

  pure def planForCurrentState(
    payload: ReconcilePayloadDigest,
    outcome: ReconcileOutcome,
    canonical: GatewayCanonicalState
  ): ReconcilePublicationPlan = {
    requestId: GatewayRequest,
    payloadDigest: payload,
    outcome: outcome,
    primary: canonical.primary,
    authorityEpoch: canonical.authorityEpoch,
    configRevision: canonical.configRevision,
    journalHead: canonical.journalHead,
    objectRevision: canonical.objectRevision,
    targetObjectRevision:
      targetObjectRevisionFor(outcome, canonical.objectRevision),
    retainedArtifact: retainedArtifactFor(payload),
  }

  pure def permitForEvidence(
    plan: ReconcilePublicationPlan,
    evidence: GatewayEvidenceCollection
  ): ReconcilePermit = {
    plan: plan,
    quorum: {
      configRevision: plan.configRevision,
      authorityEpoch: plan.authorityEpoch,
      journalIndex: plan.journalHead + 1,
      payloadDigest: plan.payloadDigest,
      publication: publicationForPlan(plan),
      voters: evidence.quorumAcks,
    },
    authority: {
      primary: plan.primary,
      configRevision: plan.configRevision,
      authorityEpoch: plan.authorityEpoch,
    },
    content: {
      payloadDigest: plan.payloadDigest,
      retainedArtifact: plan.retainedArtifact,
      holders: evidence.contentHolders,
    },
    prepare: {
      configRevision: plan.configRevision,
      authorityEpoch: plan.authorityEpoch,
      journalHead: plan.journalHead,
      journalIndex: plan.journalHead + 1,
      payloadDigest: plan.payloadDigest,
      publication: publicationForPlan(plan),
      voters: evidence.prepareAcks,
    },
    publication: publicationForPlan(plan),
  }

  def currentGatewayAudit: GatewayAudit = {
    canonicalBefore: gatewayCanonical,
    onlineBefore: gatewayOnline,
    phaseBefore: gatewayPhase,
    planBefore: gatewayPlan,
    evidenceBefore: gatewayEvidence,
    permitBefore: gatewayPermit,
    journalBefore: gatewayJournal,
    materializedBefore: gatewayMaterialized,
  }

  def currentPermitReady(permit: ReconcilePermit): bool =
    reconcilePermitReady(
      permit,
      gatewayCanonical.primary,
      gatewayCanonical.authorityEpoch,
      gatewayCanonical.configRevision,
      gatewayCanonical.journalHead,
      gatewayCanonical.objectRevision
    )

  action init = all {
    gatewayCanonical' = initialCanonicalState,
    gatewayOnline' = Nodes,
    gatewayPhase' = GatewayIdle,
    gatewayPlan' = None,
    gatewayEvidence' = emptyEvidenceCollection,
    gatewayPermit' = None,
    gatewayJournal' = emptyJournalState,
    gatewayMaterialized' = initialMaterializedState,
    gatewayLastResponse' = NoGatewayResponse,
    gatewayLastDecision' = NoGatewayDecision,
    gatewayLastSuppliedPayload' = None,
    gatewayAudit' = initialGatewayAudit,
  }

  action setGatewayConnectivity(profile: ConnectivityProfile): bool = all {
    gatewayPhase != GatewayStopped,
    gatewayOnline' = onlineForProfile(profile, gatewayCanonical.primary),
    gatewayCanonical' = gatewayCanonical,
    gatewayPhase' = gatewayPhase,
    gatewayPlan' = gatewayPlan,
    gatewayEvidence' = gatewayEvidence,
    gatewayPermit' = gatewayPermit,
    gatewayJournal' = gatewayJournal,
    gatewayMaterialized' = gatewayMaterialized,
    gatewayLastResponse' = NoGatewayResponse,
    gatewayLastDecision' = GatewayConnectivityChanged,
    gatewayLastSuppliedPayload' = None,
    gatewayAudit' = currentGatewayAudit,
  }

  action planReconcilePublication(
    payload: ReconcilePayloadDigest,
    outcome: ReconcileOutcome
  ): bool = {
    val nextPlan = planForCurrentState(payload, outcome, gatewayCanonical)
    all {
      gatewayPhase == GatewayIdle,
      gatewayJournal.entry == None,
      not(gatewayJournal.committed),
      nextPlan.targetObjectRevision <= MaxGatewayObjectRevision,
      gatewayCanonical' = gatewayCanonical,
      gatewayOnline' = gatewayOnline,
      gatewayPhase' = GatewayPlanned,
      gatewayPlan' = Some(nextPlan),
      gatewayEvidence' = emptyEvidenceCollection,
      gatewayPermit' = None,
      gatewayJournal' = gatewayJournal,
      gatewayMaterialized' = gatewayMaterialized,
      gatewayLastResponse' = GatewayPlanReady,
      gatewayLastDecision' = GatewayPlanCreated,
      gatewayLastSuppliedPayload' = Some(payload),
      gatewayAudit' = currentGatewayAudit,
    }
  }

  action collectGatewayQuorumAcks: bool = all {
    gatewayPhase == GatewayPlanned,
    hasQuorum(gatewayOnline),
    gatewayCanonical' = gatewayCanonical,
    gatewayOnline' = gatewayOnline,
    gatewayPhase' = gatewayPhase,
    gatewayPlan' = gatewayPlan,
    gatewayEvidence' = {
      quorumAcks: gatewayOnline.intersect(Voters),
      contentHolders: gatewayEvidence.contentHolders,
      prepareAcks: gatewayEvidence.prepareAcks,
    },
    gatewayPermit' = gatewayPermit,
    gatewayJournal' = gatewayJournal,
    gatewayMaterialized' = gatewayMaterialized,
    gatewayLastResponse' = NoGatewayResponse,
    gatewayLastDecision' = GatewayQuorumAcksCollected,
    gatewayLastSuppliedPayload' = gatewayLastSuppliedPayload,
    gatewayAudit' = currentGatewayAudit,
  }

  action collectGatewayContentReceipts: bool = all {
    gatewayPhase == GatewayPlanned,
    gatewayOnline.size() >= contentReceiptThreshold,
    gatewayCanonical' = gatewayCanonical,
    gatewayOnline' = gatewayOnline,
    gatewayPhase' = gatewayPhase,
    gatewayPlan' = gatewayPlan,
    gatewayEvidence' = {
      quorumAcks: gatewayEvidence.quorumAcks,
      contentHolders: gatewayOnline,
      prepareAcks: gatewayEvidence.prepareAcks,
    },
    gatewayPermit' = gatewayPermit,
    gatewayJournal' = gatewayJournal,
    gatewayMaterialized' = gatewayMaterialized,
    gatewayLastResponse' = NoGatewayResponse,
    gatewayLastDecision' = GatewayContentReceiptsCollected,
    gatewayLastSuppliedPayload' = gatewayLastSuppliedPayload,
    gatewayAudit' = currentGatewayAudit,
  }

  action collectGatewayPrepareAcks: bool = all {
    gatewayPhase == GatewayPlanned,
    hasQuorum(gatewayOnline),
    gatewayCanonical' = gatewayCanonical,
    gatewayOnline' = gatewayOnline,
    gatewayPhase' = gatewayPhase,
    gatewayPlan' = gatewayPlan,
    gatewayEvidence' = {
      quorumAcks: gatewayEvidence.quorumAcks,
      contentHolders: gatewayEvidence.contentHolders,
      prepareAcks: gatewayOnline.intersect(Voters),
    },
    gatewayPermit' = gatewayPermit,
    gatewayJournal' = gatewayJournal,
    gatewayMaterialized' = gatewayMaterialized,
    gatewayLastResponse' = NoGatewayResponse,
    gatewayLastDecision' = GatewayPrepareAcksCollected,
    gatewayLastSuppliedPayload' = gatewayLastSuppliedPayload,
    gatewayAudit' = currentGatewayAudit,
  }

  /// Bundled collection keeps randomized exploration from spending nearly all
  /// traces in the three independent collection orders. The certificates are
  /// identical to taking the three granular actions against one online view.
  action collectAllGatewayEvidence: bool = all {
    gatewayPhase == GatewayPlanned,
    hasQuorum(gatewayOnline),
    gatewayOnline.size() >= contentReceiptThreshold,
    gatewayCanonical' = gatewayCanonical,
    gatewayOnline' = gatewayOnline,
    gatewayPhase' = gatewayPhase,
    gatewayPlan' = gatewayPlan,
    gatewayEvidence' = {
      quorumAcks: gatewayOnline.intersect(Voters),
      contentHolders: gatewayOnline,
      prepareAcks: gatewayOnline.intersect(Voters),
    },
    gatewayPermit' = gatewayPermit,
    gatewayJournal' = gatewayJournal,
    gatewayMaterialized' = gatewayMaterialized,
    gatewayLastResponse' = NoGatewayResponse,
    gatewayLastDecision' = GatewayAllEvidenceCollected,
    gatewayLastSuppliedPayload' = gatewayLastSuppliedPayload,
    gatewayAudit' = currentGatewayAudit,
  }

  action buildReconcilePermit: bool = {
    val plan = unwrapGatewayPlan(gatewayPlan)
    val permit = permitForEvidence(plan, gatewayEvidence)
    all {
      gatewayPhase == GatewayPlanned,
      gatewayPlan != None,
      currentPermitReady(permit),
      gatewayCanonical' = gatewayCanonical,
      gatewayOnline' = gatewayOnline,
      gatewayPhase' = GatewayPermitReady,
      gatewayPlan' = gatewayPlan,
      gatewayEvidence' = gatewayEvidence,
      gatewayPermit' = Some(permit),
      gatewayJournal' = gatewayJournal,
      gatewayMaterialized' = gatewayMaterialized,
      gatewayLastResponse' = GatewayPermitConstructed,
      gatewayLastDecision' = GatewayPermitBuilt,
      gatewayLastSuppliedPayload' = Some(plan.payloadDigest),
      gatewayAudit' = currentGatewayAudit,
    }
  }

  action appendReconcileEntry: bool = {
    val permit = unwrapGatewayPermit(gatewayPermit)
    all {
      gatewayPhase == GatewayPermitReady,
      gatewayPermit != None,
      gatewayJournal.entry == None,
      currentPermitReady(permit),
      permit.publication.nextJournalHead <= MaxGatewayJournalHead,
      gatewayCanonical' = gatewayCanonical,
      gatewayOnline' = gatewayOnline,
      gatewayPhase' = GatewayEntryAppended,
      gatewayPlan' = gatewayPlan,
      gatewayEvidence' = gatewayEvidence,
      gatewayPermit' = gatewayPermit,
      gatewayJournal' = {
        entry: Some({
          index: permit.publication.nextJournalHead,
          permit: permit,
        }),
        committed: false,
      },
      gatewayMaterialized' = gatewayMaterialized,
      gatewayLastResponse' = NoGatewayResponse,
      gatewayLastDecision' = GatewayEntryWasAppended,
      gatewayLastSuppliedPayload' = Some(permit.plan.payloadDigest),
      gatewayAudit' = currentGatewayAudit,
    }
  }

  action rejectInsufficientGatewayEvidence: bool = all {
    gatewayPhase == GatewayPlanned,
    any {
      not(hasQuorum(gatewayOnline)),
      gatewayOnline.size() < contentReceiptThreshold,
    },
    gatewayCanonical' = gatewayCanonical,
    gatewayOnline' = gatewayOnline,
    gatewayPhase' = GatewayIdle,
    gatewayPlan' = None,
    gatewayEvidence' = emptyEvidenceCollection,
    gatewayPermit' = None,
    gatewayJournal' = emptyJournalState,
    gatewayMaterialized' = gatewayMaterialized,
    gatewayLastResponse' = GatewayPublicationRejected,
    gatewayLastDecision' = GatewayInsufficientEvidenceRejected,
    gatewayLastSuppliedPayload' = gatewayLastSuppliedPayload,
    gatewayAudit' = currentGatewayAudit,
  }

  action rejectStaleGatewayPublication: bool = {
    val plan = unwrapGatewayPlan(gatewayPlan)
    val permit = unwrapGatewayPermit(gatewayPermit)
    all {
      any {
        gatewayPhase == GatewayPlanned,
        gatewayPhase == GatewayPermitReady,
        gatewayPhase == GatewayEntryAppended,
      },
      gatewayPlan != None,
      any {
        plan.primary != gatewayCanonical.primary,
        plan.authorityEpoch != gatewayCanonical.authorityEpoch,
        plan.configRevision != gatewayCanonical.configRevision,
        plan.journalHead != gatewayCanonical.journalHead,
        plan.objectRevision != gatewayCanonical.objectRevision,
        gatewayPermit != None and not(currentPermitReady(permit)),
      },
      not(gatewayJournal.committed),
      gatewayCanonical' = gatewayCanonical,
      gatewayOnline' = gatewayOnline,
      gatewayPhase' = GatewayIdle,
      gatewayPlan' = None,
      gatewayEvidence' = emptyEvidenceCollection,
      gatewayPermit' = None,
      gatewayJournal' = emptyJournalState,
      gatewayMaterialized' = gatewayMaterialized,
      gatewayLastResponse' = GatewayPublicationRejected,
      gatewayLastDecision' = GatewayStalePublicationRejected,
      gatewayLastSuppliedPayload' = Some(plan.payloadDigest),
      gatewayAudit' = currentGatewayAudit,
    }
  }

  /// Sole local-first linearization point. The exact terminal result and the
  /// canonical delta already reside in the appended envelope; commit merely
  /// marks that entry durable and advances the logical canonical projection.
  action commitReconcileEntry(deliverReply: bool): bool = {
    val entry = unwrapGatewayEntry(gatewayJournal.entry)
    val permit = entry.permit
    val publication = permit.publication
    all {
      gatewayPhase == GatewayEntryAppended,
      gatewayJournal.entry != None,
      not(gatewayJournal.committed),
      entry.index == gatewayCanonical.journalHead + 1,
      currentPermitReady(permit),
      gatewayCanonical' = {
        primary: gatewayCanonical.primary,
        authorityEpoch: gatewayCanonical.authorityEpoch,
        configRevision: gatewayCanonical.configRevision,
        journalHead: publication.nextJournalHead,
        objectRevision: publication.nextObjectRevision,
        canonicalConflictCount: gatewayCanonical.canonicalConflictCount,
      },
      gatewayOnline' = gatewayOnline,
      gatewayPhase' = GatewayCommitted,
      gatewayPlan' = None,
      gatewayEvidence' = emptyEvidenceCollection,
      gatewayPermit' = None,
      gatewayJournal' = {
        entry: gatewayJournal.entry,
        committed: true,
      },
      gatewayMaterialized' = gatewayMaterialized,
      gatewayLastResponse' =
        if (deliverReply) {
          GatewayResultReturned(publication.resultDigest)
        } else {
          GatewayOutcomeUnknown
        },
      gatewayLastDecision' =
        if (deliverReply) {
          GatewayReconcileCommittedWithReply
        } else {
          GatewayReconcileCommittedReplyLost
        },
      gatewayLastSuppliedPayload' = Some(publication.payloadDigest),
      gatewayAudit' = currentGatewayAudit,
    }
  }

  /// An unrelated ordinary strong transaction invalidates a cached plan by
  /// consuming its predecessor journal head. It is environment behavior, not
  /// a local-first publication.
  action advanceStrongCanonicalHead: bool = all {
    gatewayPhase != GatewayStopped,
    not(gatewayJournal.committed),
    hasQuorum(gatewayOnline),
    gatewayCanonical.journalHead < MaxGatewayJournalHead,
    gatewayCanonical.objectRevision < MaxGatewayObjectRevision,
    gatewayCanonical' = {
      primary: gatewayCanonical.primary,
      authorityEpoch: gatewayCanonical.authorityEpoch,
      configRevision: gatewayCanonical.configRevision,
      journalHead: gatewayCanonical.journalHead + 1,
      objectRevision: gatewayCanonical.objectRevision + 1,
      canonicalConflictCount: gatewayCanonical.canonicalConflictCount,
    },
    gatewayOnline' = gatewayOnline,
    gatewayPhase' = gatewayPhase,
    gatewayPlan' = gatewayPlan,
    gatewayEvidence' = gatewayEvidence,
    gatewayPermit' = gatewayPermit,
    gatewayJournal' = gatewayJournal,
    gatewayMaterialized' = {
      objectRevision: gatewayMaterialized.objectRevision + 1,
      resultRecord: gatewayMaterialized.resultRecord,
      conflictArtifact: gatewayMaterialized.conflictArtifact,
    },
    gatewayLastResponse' = NoGatewayResponse,
    gatewayLastDecision' = GatewayStrongHeadAdvanced,
    gatewayLastSuppliedPayload' = None,
    gatewayAudit' = currentGatewayAudit,
  }

  /// A committed authority movement changes epoch and consumes a journal slot.
  /// Any old appended reconciliation stays uncommitted and must be rejected.
  action advanceGatewayAuthority(newPrimary: Node): bool = all {
    gatewayPhase != GatewayStopped,
    not(gatewayJournal.committed),
    hasQuorum(gatewayOnline),
    newPrimary != gatewayCanonical.primary,
    gatewayCanonical.authorityEpoch < MaxGatewayEpoch,
    gatewayCanonical.journalHead < MaxGatewayJournalHead,
    gatewayCanonical' = {
      primary: newPrimary,
      authorityEpoch: gatewayCanonical.authorityEpoch + 1,
      configRevision: gatewayCanonical.configRevision,
      journalHead: gatewayCanonical.journalHead + 1,
      objectRevision: gatewayCanonical.objectRevision,
      canonicalConflictCount: gatewayCanonical.canonicalConflictCount,
    },
    gatewayOnline' = gatewayOnline,
    gatewayPhase' = gatewayPhase,
    gatewayPlan' = gatewayPlan,
    gatewayEvidence' = gatewayEvidence,
    gatewayPermit' = gatewayPermit,
    gatewayJournal' = gatewayJournal,
    gatewayMaterialized' = gatewayMaterialized,
    gatewayLastResponse' = NoGatewayResponse,
    gatewayLastDecision' = GatewayAuthorityEpochAdvanced,
    gatewayLastSuppliedPayload' = None,
    gatewayAudit' = currentGatewayAudit,
  }

  action crashGatewayBeforeCommit: bool = all {
    gatewayPhase != GatewayStopped,
    not(gatewayJournal.committed),
    gatewayCanonical' = gatewayCanonical,
    gatewayOnline' = gatewayOnline,
    gatewayPhase' = GatewayStopped,
    gatewayPlan' = None,
    gatewayEvidence' = emptyEvidenceCollection,
    gatewayPermit' = None,
    gatewayJournal' = emptyJournalState,
    gatewayMaterialized' = gatewayMaterialized,
    gatewayLastResponse' = NoGatewayResponse,
    gatewayLastDecision' = GatewayProcessCrashedBeforeCommit,
    gatewayLastSuppliedPayload' = None,
    gatewayAudit' = currentGatewayAudit,
  }

  action crashGatewayAfterCommit: bool = all {
    gatewayPhase != GatewayStopped,
    gatewayJournal.committed,
    gatewayCanonical' = gatewayCanonical,
    gatewayOnline' = gatewayOnline,
    gatewayPhase' = GatewayStopped,
    gatewayPlan' = None,
    gatewayEvidence' = emptyEvidenceCollection,
    gatewayPermit' = None,
    gatewayJournal' = gatewayJournal,
    gatewayMaterialized' = gatewayMaterialized,
    gatewayLastResponse' = NoGatewayResponse,
    gatewayLastDecision' = GatewayProcessCrashedAfterCommit,
    gatewayLastSuppliedPayload' = None,
    gatewayAudit' = currentGatewayAudit,
  }

  action restartGatewayProcess: bool = all {
    gatewayPhase == GatewayStopped,
    gatewayCanonical' = gatewayCanonical,
    gatewayOnline' = gatewayOnline,
    gatewayPhase' =
      if (gatewayJournal.committed) {
        if (gatewayMaterialized.resultRecord != None) {
          GatewayMaterialized
        } else {
          GatewayCommitted
        }
      } else {
        GatewayIdle
      },
    gatewayPlan' = None,
    gatewayEvidence' = emptyEvidenceCollection,
    gatewayPermit' = None,
    gatewayJournal' = gatewayJournal,
    gatewayMaterialized' = gatewayMaterialized,
    gatewayLastResponse' = NoGatewayResponse,
    gatewayLastDecision' = GatewayProcessRestarted,
    gatewayLastSuppliedPayload' = None,
    gatewayAudit' = currentGatewayAudit,
  }

  /// Idempotent table/index recovery from the durable journal envelope.
  action materializeCommittedReconcileEntry: bool = {
    val entry = unwrapGatewayEntry(gatewayJournal.entry)
    val publication = entry.permit.publication
    all {
      gatewayPhase == GatewayCommitted,
      gatewayJournal.committed,
      gatewayJournal.entry != None,
      any {
        gatewayMaterialized.objectRevision != publication.nextObjectRevision,
        gatewayMaterialized.resultRecord != Some(publication),
        gatewayMaterialized.conflictArtifact != publication.conflictArtifact,
      },
      gatewayCanonical' = gatewayCanonical,
      gatewayOnline' = gatewayOnline,
      gatewayPhase' = GatewayMaterialized,
      gatewayPlan' = None,
      gatewayEvidence' = emptyEvidenceCollection,
      gatewayPermit' = None,
      gatewayJournal' = gatewayJournal,
      gatewayMaterialized' = {
        objectRevision: publication.nextObjectRevision,
        resultRecord: Some(publication),
        conflictArtifact: publication.conflictArtifact,
      },
      gatewayLastResponse' = NoGatewayResponse,
      gatewayLastDecision' = GatewayCommittedEntryMaterialized,
      gatewayLastSuppliedPayload' = None,
      gatewayAudit' = currentGatewayAudit,
    }
  }

  action retryCommittedResult(
    suppliedPayload: ReconcilePayloadDigest
  ): bool = {
    val entry = unwrapGatewayEntry(gatewayJournal.entry)
    val publication = entry.permit.publication
    all {
      gatewayPhase != GatewayStopped,
      gatewayJournal.committed,
      gatewayJournal.entry != None,
      gatewayCanonical' = gatewayCanonical,
      gatewayOnline' = gatewayOnline,
      gatewayPhase' = gatewayPhase,
      gatewayPlan' = gatewayPlan,
      gatewayEvidence' = gatewayEvidence,
      gatewayPermit' = gatewayPermit,
      gatewayJournal' = gatewayJournal,
      gatewayMaterialized' = gatewayMaterialized,
      gatewayLastResponse' =
        if (suppliedPayload == publication.payloadDigest) {
          GatewayResultReturned(publication.resultDigest)
        } else {
          GatewayPayloadMismatch
        },
      gatewayLastDecision' =
        if (suppliedPayload == publication.payloadDigest) {
          GatewayExactRetryReplayed
        } else {
          GatewayChangedRetryRejected
        },
      gatewayLastSuppliedPayload' = Some(suppliedPayload),
      gatewayAudit' = currentGatewayAudit,
    }
  }

  action stutterGateway: bool = all {
    gatewayCanonical' = gatewayCanonical,
    gatewayOnline' = gatewayOnline,
    gatewayPhase' = gatewayPhase,
    gatewayPlan' = gatewayPlan,
    gatewayEvidence' = gatewayEvidence,
    gatewayPermit' = gatewayPermit,
    gatewayJournal' = gatewayJournal,
    gatewayMaterialized' = gatewayMaterialized,
    gatewayLastResponse' = gatewayLastResponse,
    gatewayLastDecision' = GatewayTerminalStutter,
    gatewayLastSuppliedPayload' = gatewayLastSuppliedPayload,
    gatewayAudit' = currentGatewayAudit,
  }

  action step = any {
    {
      nondet profile = oneOf(Set(
        PrimaryOnly,
        PrimaryQuorum,
        AllGatewayNodes
      ))
      setGatewayConnectivity(profile)
    },
    {
      nondet payload = oneOf(ReconcilePayloads)
      nondet outcome = oneOf(ReconcileOutcomes)
      planReconcilePublication(payload, outcome)
    },
    collectGatewayQuorumAcks,
    collectGatewayContentReceipts,
    collectGatewayPrepareAcks,
    collectAllGatewayEvidence,
    buildReconcilePermit,
    appendReconcileEntry,
    rejectInsufficientGatewayEvidence,
    rejectStaleGatewayPublication,
    {
      nondet deliverReply = oneOf(Set(true, false))
      commitReconcileEntry(deliverReply)
    },
    advanceStrongCanonicalHead,
    {
      nondet newPrimary = oneOf(Nodes)
      advanceGatewayAuthority(newPrimary)
    },
    crashGatewayBeforeCommit,
    crashGatewayAfterCommit,
    restartGatewayProcess,
    materializeCommittedReconcileEntry,
    {
      nondet suppliedPayload = oneOf(ReconcilePayloads)
      retryCommittedResult(suppliedPayload)
    },
    stutterGateway,
  }
}
