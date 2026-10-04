/// Target refinement: ordinary-create pending intent -> exact receipts -> hidden prepare.
/// Unrestricted safety passed through eight transitions. The depth-ten supported
/// encoding timed out; the preceding dynamic-range encoding failed translation.
/// All 26 witnesses have 5,000 x 32 simulation coverage (fixed seed recorded in
/// docs/formal/cluster-verification.md); longer completion is simulation evidence.
/// coverageStep changes selection frequency only; verification always uses step.
/// An eligible create may carry no base token or its exact hidden transient
/// object version. Durable child/path absence is a separate precondition; the
/// model abstracts that predicate and does not represent the base token.
///
/// One ordinary create uses the unchanged I,R1,R2,P mutation alphabet: pending
/// intent, two exact selected-replica receipts (W=2), hidden namespace prepare.
/// Retried groups append new sequences while retaining canonical operation
/// identity. Existing compatible active I may fill its unknown hash; an exact
/// Prepared P may repeat. All other classified inputs delegate to the existing
/// serial API, whose internal behavior is outside this fast-path refinement.
///
/// The root certificate/recovery actions abstract the same strict generation,
/// resync, writer-flock and sequence contract as authored_journal_batch.qnt.
/// Initial group rollover uses that primitive's independent proof; this slice
/// focuses logical cuts, namespace read-set equivalence, and authority timing.
module dfs_initial_prepare_batch {
  type InputClass = FreshCreate | CompatibleActiveIntent | ExactPreparedRetry
    | TerminalIntent | CompactedIntent | TerminalPrepare | CompactedPrepare
    | HistoryBackfill | BulkNamespace | AttachedCompletion | RetainedOrAdvisory
    | ConflictingActiveIntent
  type Kind = PendingIntent | ReceiptOne | ReceiptTwo | NamespacePrepare
  type Frame = { sequence: int, attempt: int, kind: Kind, identity: int, hash: int }
  type Phase = Idle | Appending | NeedsRecovery | NeedsTables | TablePending
    | SerialFallback | Rejected
  type Event = Initialized | GroupAdmitted | FrameAppended | TornAppend
    | UncertainAppendFailed | GroupSynced | ProcessReopened | HandleReplaced
    | DecodeOnly | IncompleteTailTruncated | RecoverySyncFailed | TailCertified
    | CompleteCorruptionRejected | TablePlanned | TableAppendFailed | TableApplied
    | AuthorityPublished | CandidateRejected | FallbackSelected | NamespaceChanged

  type State = {
    inputClass: InputClass, candidateValid: bool,
    frames: Set[Frame], physicalHead: int, durableHead: int,
    generation: int, certifiedGeneration: int, certifiedHead: int,
    uncertain: bool, torn: bool, corrupt: bool, nextSequence: int,
    phase: Phase, attempts: int, groupBase: int, groupOffset: int,
    namespaceReadSet: int, validatedReadSet: int,
    tableHead: int, tablePrepareReference: int, tableReferenceCost: int,
    livePrepareReference: int, plannedHead: int, plannedPrepare: int,
    plannedReferenceCost: int, deferredPrepare: bool,
    authoritySequence: int, childVisible: bool,
    terminalIdentityBefore: int, terminalIdentityAfter: int,
    fastGroups: int, fallbackSelected: bool,
    reopens: int, handles: int, sawResync: bool, sawPartialRecovery: bool,
    sawDeferredPrepare: bool, sawTableRollback: bool, last: Event,
  }
  pure def ordinary(input: InputClass): bool =
    Set(FreshCreate, CompatibleActiveIntent, ExactPreparedRetry).contains(input)
  pure def frame(seq: int, attempt: int, kind: Kind, hash: int): Frame =
    { sequence: seq, attempt: attempt, kind: kind, identity: 1, hash: hash }
  pure def seed(input: InputClass): Set[Frame] =
    if (input == ExactPreparedRetry) Set(
      frame(1, 0, PendingIntent, 1), frame(2, 0, ReceiptOne, 1),
      frame(3, 0, ReceiptTwo, 1), frame(4, 0, NamespacePrepare, 1))
    else if (input == CompatibleActiveIntent) Set(frame(1, 0, PendingIntent, 0))
    else Set()
  pure def initial(input: InputClass, valid: bool): State = {
    inputClass: input, candidateValid: valid,
    frames: seed(input), physicalHead: seed(input).size(), durableHead: seed(input).size(),
    generation: 0, certifiedGeneration: 0, certifiedHead: seed(input).size(),
    uncertain: false, torn: false, corrupt: false, nextSequence: seed(input).size() + 1,
    phase: Idle, attempts: 0, groupBase: seed(input).size() + 1, groupOffset: 0,
    namespaceReadSet: 0, validatedReadSet: 0,
    tableHead: seed(input).size(),
    tablePrepareReference: if (input == ExactPreparedRetry) 4 else 0,
    tableReferenceCost: if (input == ExactPreparedRetry) 1 else 0,
    livePrepareReference: if (input == ExactPreparedRetry) 4 else 0,
    plannedHead: seed(input).size(), plannedPrepare: 0, plannedReferenceCost: 0,
    deferredPrepare: false, authoritySequence: 0, childVisible: false,
    terminalIdentityBefore: if (input == TerminalIntent or input == TerminalPrepare) 7 else 0,
    terminalIdentityAfter: if (input == TerminalIntent or input == TerminalPrepare) 7 else 0,
    fastGroups: 0, fallbackSelected: false,
    reopens: 0, handles: 0, sawResync: false, sawPartialRecovery: false,
    sawDeferredPrepare: false, sawTableRollback: false, last: Initialized,
  }
  var s: State

  pure def certificateValid(x: State): bool = all {
    not(x.uncertain), not(x.torn), not(x.corrupt),
    x.generation == x.certifiedGeneration,
    x.physicalHead == x.durableHead, x.certifiedHead == x.durableHead,
    x.nextSequence == x.physicalHead + 1,
  }
  pure def admitted(x: State): bool =
    x.phase == Idle and certificateValid(x) and x.tableHead == x.durableHead
  pure def kindAt(offset: int): Kind =
    if (offset == 0) PendingIntent else if (offset == 1) ReceiptOne
    else if (offset == 2) ReceiptTwo else NamespacePrepare
  pure def ordinal(kind: Kind): int =
    if (kind == PendingIntent) 0 else if (kind == ReceiptOne) 1
    else if (kind == ReceiptTwo) 2 else 3
  pure def prepareSequences(x: State, end: int): Set[int] =
    x.frames.filter(f => f.sequence <= end and f.kind == NamespacePrepare).map(f => f.sequence)
  pure def completePrepareProof(x: State, sequence: int): bool =
    x.frames.exists(p => all {
      p.sequence == sequence, p.kind == NamespacePrepare, p.identity == 1, p.hash == 1,
      Set(PendingIntent, ReceiptOne, ReceiptTwo).forall(kind =>
        x.frames.exists(f => all {
          f.sequence == p.sequence - 3 + ordinal(kind), f.attempt == p.attempt, f.kind == kind,
          f.identity == p.identity, f.hash == p.hash,
        })),
    })

  action init = {
    nondet input = Set(FreshCreate, CompatibleActiveIntent, ExactPreparedRetry,
      TerminalIntent, CompactedIntent, TerminalPrepare, CompactedPrepare,
      HistoryBackfill, BulkNamespace, AttachedCompletion, RetainedOrAdvisory,
      ConflictingActiveIntent).oneOf()
    nondet valid = Set(false, true).oneOf()
    s' = initial(input, valid)
  }
  action chooseSerialFallback = all {
    admitted(s), not(ordinary(s.inputClass)),
    // Opaque delegation, not a fresh prepare or an authority ACK. In
    // particular, an existing terminal identity is never rewritten to Pending.
    s' = s.with("phase", SerialFallback).with("fallbackSelected", true)
      .with("last", FallbackSelected),
  }
  action rejectInvalidCandidate = all {
    admitted(s), ordinary(s.inputClass),
    not(s.candidateValid) or s.namespaceReadSet != 0,
    s' = s.with("phase", Rejected).with("last", CandidateRejected),
  }
  action beginGroupedPrepare = all {
    admitted(s), ordinary(s.inputClass), s.candidateValid, s.namespaceReadSet == 0,
    s.attempts < 2, s.authoritySequence == 0,
    s' = s.with("phase", Appending).with("uncertain", true)
      .with("attempts", s.attempts + 1).with("groupBase", s.nextSequence)
      .with("groupOffset", 0).with("validatedReadSet", s.namespaceReadSet)
      .with("fastGroups", s.fastGroups + 1).with("last", GroupAdmitted),
  }
  action appendOneExactFrame = all {
    s.phase == Appending, s.groupOffset < 4, not(s.torn),
    s.namespaceReadSet == s.validatedReadSet,
    val entry = frame(s.groupBase + s.groupOffset, s.attempts, kindAt(s.groupOffset), 1)
    s' = s.with("frames", s.frames.union(Set(entry)))
      .with("physicalHead", s.physicalHead + 1).with("generation", s.generation + 1)
      .with("groupOffset", s.groupOffset + 1).with("last", FrameAppended),
  }
  action failWithCompletePrefix = all {
    s.phase == Appending,
    s' = s.with("phase", NeedsRecovery).with("uncertain", true)
      .with("last", UncertainAppendFailed),
  }
  action failWithTornFrame = all {
    s.phase == Appending, s.groupOffset < 4,
    s' = s.with("phase", NeedsRecovery).with("torn", true)
      .with("generation", s.generation + 1).with("last", TornAppend),
  }
  action syncWholeGroup = all {
    s.phase == Appending, s.groupOffset == 4, not(s.torn), not(s.corrupt),
    s' = s.with("durableHead", s.physicalHead).with("certifiedHead", s.physicalHead)
      .with("certifiedGeneration", s.generation).with("nextSequence", s.physicalHead + 1)
      .with("uncertain", false).with("phase", NeedsTables).with("last", GroupSynced),
  }
  action decodeOnly = all {
    s.phase == NeedsRecovery, not(s.corrupt),
    s' = s.with("last", DecodeOnly),
  }
  action replaceHandle = all {
    s.phase == NeedsRecovery, s.handles < 2,
    s' = s.with("handles", s.handles + 1).with("last", HandleReplaced),
  }
  action reopen = {
    nondet retained = 0.to(12).oneOf()
    all {
      s.reopens < 2, retained >= s.durableHead, retained <= s.physicalHead,
      s' = s.with("frames", s.frames.filter(f => f.sequence <= retained))
        .with("physicalHead", retained).with("generation", s.generation + 1)
        .with("uncertain", true).with("phase", NeedsRecovery)
        .with("torn", s.torn and retained == s.physicalHead)
        .with("tableHead", 0).with("tablePrepareReference", 0)
        .with("livePrepareReference", 0).with("tableReferenceCost", 0)
        .with("deferredPrepare", false).with("reopens", s.reopens + 1)
        .with("last", ProcessReopened),
    }
  }
  action truncateIncompleteSuffix = all {
    s.phase == NeedsRecovery, s.torn, not(s.corrupt),
    s' = s.with("torn", false).with("generation", s.generation + 1)
      .with("last", IncompleteTailTruncated),
  }
  action failResync = all {
    s.phase == NeedsRecovery,
    s' = s.with("uncertain", true).with("last", RecoverySyncFailed),
  }
  action certifyRecoveredPrefix = all {
    s.phase == NeedsRecovery, not(s.torn), not(s.corrupt),
    s' = s.with("durableHead", s.physicalHead).with("certifiedHead", s.physicalHead)
      .with("certifiedGeneration", s.generation).with("nextSequence", s.physicalHead + 1)
      .with("uncertain", false).with("phase", NeedsTables).with("sawResync", true)
      .with("sawPartialRecovery", s.sawPartialRecovery or (s.groupOffset > 0 and s.groupOffset < 4))
      .with("last", TailCertified),
  }
  action rejectCompleteMalformedFrame = all {
    s.phase == NeedsRecovery, not(s.corrupt), s.physicalHead > s.durableHead,
    s' = s.with("corrupt", true).with("uncertain", true)
      .with("last", CompleteCorruptionRejected),
  }

  action planTables = {
    nondet reference = Set(0).union(prepareSequences(s, s.durableHead)).oneOf()
    all {
      s.phase == NeedsTables, certificateValid(s),
      prepareSequences(s, s.durableHead).forall(seq => seq <= reference),
      val deferred = reference > 0 and reference == s.durableHead
      s' = s.with("phase", TablePending).with("plannedHead", s.durableHead)
        .with("plannedPrepare", reference)
        .with("plannedReferenceCost", prepareSequences(s, s.durableHead).size())
        .with("deferredPrepare", deferred)
        .with("livePrepareReference", if (deferred) s.tablePrepareReference else reference)
        .with("sawDeferredPrepare", s.sawDeferredPrepare or deferred).with("last", TablePlanned),
    }
  }
  action failTableAppend = all {
    s.phase == TablePending,
    s' = s.with("phase", NeedsTables).with("livePrepareReference", s.tablePrepareReference)
      .with("deferredPrepare", false).with("sawTableRollback", true).with("last", TableAppendFailed),
  }
  action applyTables = all {
    s.phase == TablePending,
    s' = s.with("tableHead", s.plannedHead).with("tablePrepareReference", s.plannedPrepare)
      .with("livePrepareReference", s.plannedPrepare)
      .with("tableReferenceCost", s.plannedReferenceCost)
      .with("deferredPrepare", false).with("phase", Idle).with("last", TableApplied),
  }
  action publishOperationAuthority = all {
    admitted(s), ordinary(s.inputClass), s.candidateValid, s.namespaceReadSet == 0,
    s.tablePrepareReference > 0,
    completePrepareProof(s, s.tablePrepareReference),
    s' = s.with("authoritySequence", s.tablePrepareReference).with("last", AuthorityPublished),
  }
  action changeParentOrExclusiveReadSet = all {
    admitted(s), s.tablePrepareReference == 0, s.namespaceReadSet == 0,
    s' = s.with("namespaceReadSet", 1).with("last", NamespaceChanged),
  }
  action step = any {
    chooseSerialFallback, rejectInvalidCandidate, beginGroupedPrepare,
    appendOneExactFrame, failWithCompletePrefix, failWithTornFrame, syncWholeGroup,
    decodeOnly, replaceHandle, reopen, truncateIncompleteSuffix, failResync,
    certifyRecoveredPrefix, rejectCompleteMalformedFrame,
    planTables, failTableAppend, applyTables,
    publishOperationAuthority, changeParentOrExclusiveReadSet,
  }
  /// Coverage-only completion bias; all alternatives are unchanged step actions.
  /// Bounded verification must continue to use unrestricted step.
  action progress = any {
    chooseSerialFallback, rejectInvalidCandidate, beginGroupedPrepare,
    appendOneExactFrame, syncWholeGroup, truncateIncompleteSuffix,
    certifyRecoveredPrefix, planTables, applyTables, publishOperationAuthority,
  }
  action coverageStep = any { step, progress, progress, progress }

}
