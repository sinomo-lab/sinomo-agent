/// Single-round replicated write commit (docs/plans/dfs-single-round-write-commit.md).
///
/// One W=2 write on a shard whose write set is the primary and one member. The
/// primary stores the content, appends a prepared change that names both
/// holders before the second has answered, and sends content and frames in one
/// exchange. The second holder is either the write-set member or a node
/// outside the write set. The model covers the commit point, success reported
/// before the commit record is durable, a crash of the primary between any two
/// steps, its restart, and takeover by the member.
///
/// Storage damage, holder substitution through a superseding change, content
/// larger than one message and write sets above two members are outside this
/// model. Message loss is represented by a message that is never delivered.
module dfs_single_round_write_commit {
  type Node = Primary | Member | Outside
  /// Negative controls. Every invariant is stated for Sound.
  type Mode = Sound | CommitWithoutHolder | AcknowledgeWithoutContent | AbortAfterSend
  type Event = Initialized | ContentStored | ChangePrepared | ExchangeSent
    | MemberStored | MemberAcknowledged | OutsideStored | OutsideConfirmed
    | CommittedByPrimary | CommitRecordPersisted | AbortedLocally
    | PrimaryCrashed | PrimaryRestarted | MemberTookOver | CommittedBySuccessor

  type State = {
    mode: Mode,
    // The W=2 holder besides the primary.
    secondHolder: Node,
    // Durable state.
    primaryContent: bool, primaryPrepare: bool, primaryCommitRecord: bool,
    memberFrames: bool, memberContent: bool, outsideContent: bool,
    aborted: bool,
    // Messages that exist; one that is never delivered is lost.
    memberMessage: bool, outsideMessage: bool,
    // What the serving primary has collected since it last started.
    collecting: bool, memberAcknowledged: bool, outsideConfirmed: bool, applied: bool,
    serving: Node, primaryUp: bool,
    success: bool,
    committedByPrimary: bool, committedBySuccessor: bool, recommitted: bool,
    crashes: int, last: Event,
  }

  pure def initial(mode: Mode, second: Node): State = {
    mode: mode, secondHolder: second,
    primaryContent: false, primaryPrepare: false, primaryCommitRecord: false,
    memberFrames: false, memberContent: false, outsideContent: false,
    aborted: false, memberMessage: false, outsideMessage: false,
    collecting: false, memberAcknowledged: false, outsideConfirmed: false, applied: false,
    serving: Primary, primaryUp: true, success: false,
    committedByPrimary: false, committedBySuccessor: false, recommitted: false,
    crashes: 0, last: Initialized,
  }
  var s: State

  pure def secondHolderHoldsContent(x: State): bool =
    if (x.secondHolder == Member) x.memberContent else x.outsideContent
  pure def primaryServes(x: State): bool = x.serving == Primary and x.primaryUp
  /// The primary's own holding needs no confirmation: it stored the content
  /// before it prepared. The member's acknowledgement confirms its content.
  pure def primaryHasHolderEvidence(x: State): bool =
    if (x.secondHolder == Member) x.memberAcknowledged else x.outsideConfirmed
  /// A successor relies on the prepared change for the old primary's holding,
  /// checks its own content locally and asks only a holder outside the write set.
  pure def successorHasHolderEvidence(x: State): bool =
    if (x.secondHolder == Member) x.memberContent else x.outsideConfirmed

  action init = {
    nondet mode = Set(Sound, CommitWithoutHolder, AcknowledgeWithoutContent, AbortAfterSend).oneOf()
    nondet second = Set(Member, Outside).oneOf()
    s' = initial(mode, second)
  }
  action storeContent = all {
    primaryServes(s), not(s.aborted), not(s.primaryContent),
    s' = s.with("primaryContent", true).with("last", ContentStored),
  }
  action prepare = all {
    primaryServes(s), not(s.aborted), s.primaryContent, not(s.primaryPrepare),
    s' = s.with("primaryPrepare", true).with("last", ChangePrepared),
  }
  /// Also the re-send a restarted primary makes for a change it still holds.
  action send = all {
    primaryServes(s), not(s.aborted), s.primaryPrepare, not(s.collecting),
    s' = s.with("collecting", true).with("memberMessage", true)
      .with("outsideMessage", s.secondHolder == Outside).with("last", ExchangeSent),
  }
  action abortLocally = all {
    primaryServes(s), not(s.aborted), not(s.applied), not(s.success),
    not(s.memberMessage) or s.mode == AbortAfterSend,
    s' = s.with("aborted", true).with("collecting", false).with("last", AbortedLocally),
  }
  action memberStores = all {
    s.memberMessage, not(s.memberFrames),
    s' = s.with("memberFrames", true)
      .with("memberContent", s.secondHolder == Member and s.mode != AcknowledgeWithoutContent)
      .with("last", MemberStored),
  }
  action memberAcknowledges = all {
    primaryServes(s), s.collecting, s.memberFrames, not(s.memberAcknowledged),
    s.secondHolder == Member implies (s.memberContent or s.mode == AcknowledgeWithoutContent),
    s' = s.with("memberAcknowledged", true).with("last", MemberAcknowledged),
  }
  action outsideStores = all {
    s.outsideMessage, not(s.outsideContent),
    s' = s.with("outsideContent", true).with("last", OutsideStored),
  }
  /// Answers whichever primary serves: the original one that is collecting, or
  /// a successor that asks before it commits.
  action outsideConfirms = all {
    s.secondHolder == Outside, s.outsideContent, not(s.outsideConfirmed),
    (primaryServes(s) and s.collecting) or (s.serving == Member and s.memberFrames),
    s' = s.with("outsideConfirmed", true).with("last", OutsideConfirmed),
  }
  action commitAtPrimary = all {
    primaryServes(s), not(s.aborted), s.collecting, s.primaryPrepare, not(s.applied),
    s.memberAcknowledged,
    primaryHasHolderEvidence(s) or s.mode == CommitWithoutHolder,
    s' = s.with("applied", true).with("success", true).with("committedByPrimary", true)
      .with("recommitted", s.recommitted or s.crashes > 0).with("last", CommittedByPrimary),
  }
  action persistCommitRecord = all {
    primaryServes(s), s.applied, not(s.primaryCommitRecord),
    s' = s.with("primaryCommitRecord", true).with("last", CommitRecordPersisted),
  }
  action crashPrimary = all {
    primaryServes(s), s.crashes < 2,
    s' = s.with("primaryUp", false).with("collecting", false)
      .with("memberAcknowledged", false).with("outsideConfirmed", false)
      .with("applied", false).with("crashes", s.crashes + 1).with("last", PrimaryCrashed),
  }
  action restartPrimary = all {
    s.serving == Primary, not(s.primaryUp),
    s' = s.with("primaryUp", true).with("applied", s.primaryCommitRecord)
      .with("last", PrimaryRestarted),
  }
  action memberTakesOver = all {
    s.serving == Primary, not(s.primaryUp),
    s' = s.with("serving", Member).with("outsideConfirmed", false).with("last", MemberTookOver),
  }
  action commitAtSuccessor = all {
    s.serving == Member, s.memberFrames, not(s.committedBySuccessor),
    not(s.aborted) or s.mode == AbortAfterSend,
    successorHasHolderEvidence(s) or s.mode == CommitWithoutHolder,
    s' = s.with("committedBySuccessor", true).with("success", true)
      .with("last", CommittedBySuccessor),
  }
  action step = any {
    storeContent, prepare, send, abortLocally, memberStores, memberAcknowledges,
    outsideStores, outsideConfirms, commitAtPrimary, persistCommitRecord,
    crashPrimary, restartPrimary, memberTakesOver, commitAtSuccessor,
  }
  /// Coverage-only completion bias; every alternative is an unchanged step action.
  action progress = any {
    storeContent, prepare, send, memberStores, memberAcknowledges, outsideStores,
    outsideConfirms, commitAtPrimary, commitAtSuccessor,
  }
  action coverageStep = any { step, progress, progress }
}
