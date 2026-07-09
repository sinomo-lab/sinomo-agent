/// Membership changes over the abstract KV commit contract.
///
/// This module is intentionally layered above `raft_kv`. It does not re-model
/// leader election, replication, network delivery, or per-node logs. Instead,
/// it consumes the lower-layer guarantee as a minimal interface: the KV exposes
/// one canonical append-only committed command prefix, and membership state is
/// the deterministic replay of that prefix.
module membership_kv {
  import common.* from "./common"
  import membership_base.* from "./membership_base"

  type MemberCommand =
    | CmdOpenInvite(Node)
    | CmdExpireInvite(Node)
    | CmdCommitPending(Node)
    | CmdAcceptLearner(Node)
    | CmdRecordCatchUp(Node)
    | CmdPromoteVoter(Node)
    | CmdDemoteLearner(Node)
    | CmdDemoteNonVoting(Node)
    | CmdMarkSuspect(Node)
    | CmdMarkOffline(Node)
    | CmdRestoreHealthy(Node)
    | CmdRemove(Node)

  type KvEntry = { actor: Node, command: MemberCommand }
  type KvSlot = Empty | Filled(KvEntry)

  type MembershipView = {
    roles: Node -> MemberRole,
    states: Node -> MemberState,
    invites: Node -> InviteState,
    records: Node -> bool,
    caughtUp: Node -> bool,
    promotionBase: Node -> bool,
    promotionActor: Node -> Option[Node],
  }

  type RaftConfig = {
    voters: Set[Node],
    learners: Set[Node],
    nonVoting: Set[Node],
  }

  pure val MaxKvIndex: int = 4
  pure val KvIndexes: Set[int] = Set(1, 2, 3, 4)

  var committed: int -> KvSlot
  var nextKvIndex: int
  var kvRevision: int
  var memberRole: Node -> MemberRole
  var memberState: Node -> MemberState
  var inviteState: Node -> InviteState
  var kvRecordCommitted: Node -> bool
  var caughtUp: Node -> bool
  var promotionBase: Node -> bool
  var promotionActor: Node -> Option[Node]
  var raftVoters: Set[Node]
  var raftLearners: Set[Node]
  var raftNonVoting: Set[Node]

  pure val emptyKvLog: int -> KvSlot =
    KvIndexes.mapBy(_ => Empty)

  pure val initialMembershipView: MembershipView = {
    roles: initialRole,
    states: initialState,
    invites: Nodes.mapBy(_ => NoInvite),
    records: initialKvRecordCommitted,
    caughtUp: initialCaughtUp,
    promotionBase: initialPromotionBase,
    promotionActor: emptyPromotionActor,
  }

  pure def projectRaftConfig(view: MembershipView): RaftConfig = {
    voters: voterSetOf(view.states),
    learners: Nodes.filter(n => view.states.get(n) == Learner),
    nonVoting: Nodes.filter(n => view.states.get(n) == NonVoting),
  }

  pure val initialRaftConfig: RaftConfig =
    projectRaftConfig(initialMembershipView)

  pure def commandEnabled(view: MembershipView, actor: Node, command: MemberCommand): bool =
    voterSetOf(view.states).contains(actor)
      and match command {
        | CmdOpenInvite(target) =>
            actor != target
              and view.states.get(target) == NonMember
              and view.invites.get(target) != InviteOpen
        | CmdExpireInvite(target) =>
            view.invites.get(target) == InviteOpen
        | CmdCommitPending(target) =>
            actor != target
              and view.invites.get(target) == InviteOpen
              and view.states.get(target) == NonMember
        | CmdAcceptLearner(target) =>
            actor != target
              and (
                all {
                  view.states.get(target) == NonMember,
                  view.invites.get(target) == InviteOpen,
                }
                  or all {
                    view.states.get(target) == Pending,
                    view.records.get(target),
                  }
              )
        | CmdRecordCatchUp(target) =>
            canCatchUp(view.states.get(target)) and view.records.get(target)
        | CmdPromoteVoter(target) =>
            actor != target
              and (
                view.states.get(target) == Learner
                  or view.states.get(target) == NonVoting
              )
              and view.records.get(target)
              and view.caughtUp.get(target)
              and view.promotionBase.get(target)
        | CmdDemoteLearner(target) =>
            view.states.get(target) == Voter and voterSetOf(view.states).size() > 1
        | CmdDemoteNonVoting(target) =>
            view.states.get(target) == Voter and voterSetOf(view.states).size() > 1
        | CmdMarkSuspect(target) =>
            isAuthorizedState(view.states.get(target))
              and view.states.get(target) != Suspect
              and view.states.get(target) != Offline
              and view.states.get(target) != Removed
              and (
                view.states.get(target) != Voter
                  or voterSetOf(view.states).size() > 1
              )
        | CmdMarkOffline(target) =>
            isAuthorizedState(view.states.get(target))
              and view.states.get(target) != Offline
              and view.states.get(target) != Removed
              and (
                view.states.get(target) != Voter
                  or voterSetOf(view.states).size() > 1
              )
        | CmdRestoreHealthy(target) =>
            canBecomeHealthy(view.states.get(target))
              and view.records.get(target)
              and (
                stateForRole(view.roles.get(target)) != Voter
                  or view.caughtUp.get(target)
              )
        | CmdRemove(target) =>
            actor != target
              and view.states.get(target) != NonMember
              and view.states.get(target) != Removed
              and (
                view.states.get(target) != Voter
                  or voterSetOf(view.states).size() > 1
              )
      }

  pure def applyMemberCommand(
    view: MembershipView,
    actor: Node,
    command: MemberCommand,
  ): MembershipView =
    match command {
      | CmdOpenInvite(target) => {
          roles: view.roles,
          states: view.states,
          invites: view.invites.set(target, InviteOpen),
          records: view.records,
          caughtUp: view.caughtUp,
          promotionBase: view.promotionBase,
          promotionActor: view.promotionActor,
        }
      | CmdExpireInvite(target) => {
          roles: view.roles,
          states: view.states,
          invites: view.invites.set(target, InviteExpired),
          records: view.records,
          caughtUp: view.caughtUp,
          promotionBase: view.promotionBase,
          promotionActor: view.promotionActor,
        }
      | CmdCommitPending(target) => {
          roles: view.roles.set(target, LearnerRole),
          states: view.states.set(target, Pending),
          invites: view.invites.set(target, InviteUsed),
          records: view.records.set(target, true),
          caughtUp: view.caughtUp.set(target, false),
          promotionBase: view.promotionBase,
          promotionActor: view.promotionActor,
        }
      | CmdAcceptLearner(target) => {
          roles: view.roles.set(target, LearnerRole),
          states: view.states.set(target, Learner),
          invites: view.invites.set(target, InviteUsed),
          records: view.records.set(target, true),
          caughtUp: view.caughtUp.set(target, false),
          promotionBase: view.promotionBase.set(target, true),
          promotionActor: view.promotionActor,
        }
      | CmdRecordCatchUp(target) => {
          roles: view.roles,
          states: view.states,
          invites: view.invites,
          records: view.records,
          caughtUp: view.caughtUp.set(target, true),
          promotionBase: view.promotionBase,
          promotionActor: view.promotionActor,
        }
      | CmdPromoteVoter(target) => {
          roles: view.roles.set(target, VoterRole),
          states: view.states.set(target, Voter),
          invites: view.invites,
          records: view.records,
          caughtUp: view.caughtUp,
          promotionBase: view.promotionBase,
          promotionActor: view.promotionActor.set(target, Some(actor)),
        }
      | CmdDemoteLearner(target) => {
          roles: view.roles.set(target, LearnerRole),
          states: view.states.set(target, Learner),
          invites: view.invites,
          records: view.records,
          caughtUp: view.caughtUp,
          promotionBase: view.promotionBase.set(target, true),
          promotionActor: view.promotionActor,
        }
      | CmdDemoteNonVoting(target) => {
          roles: view.roles.set(target, NonVotingRole),
          states: view.states.set(target, NonVoting),
          invites: view.invites,
          records: view.records,
          caughtUp: view.caughtUp,
          promotionBase: view.promotionBase.set(target, true),
          promotionActor: view.promotionActor,
        }
      | CmdMarkSuspect(target) => {
          roles: view.roles,
          states: view.states.set(target, Suspect),
          invites: view.invites,
          records: view.records,
          caughtUp: view.caughtUp,
          promotionBase: view.promotionBase,
          promotionActor: view.promotionActor,
        }
      | CmdMarkOffline(target) => {
          roles: view.roles,
          states: view.states.set(target, Offline),
          invites: view.invites,
          records: view.records,
          caughtUp: view.caughtUp,
          promotionBase: view.promotionBase,
          promotionActor: view.promotionActor,
        }
      | CmdRestoreHealthy(target) => {
          roles: view.roles,
          states: view.states.set(target, stateForRole(view.roles.get(target))),
          invites: view.invites,
          records: view.records,
          caughtUp: view.caughtUp,
          promotionBase: view.promotionBase,
          promotionActor: view.promotionActor,
        }
      | CmdRemove(target) => {
          roles: view.roles,
          states: view.states.set(target, Removed),
          invites: view.invites,
          records: view.records.set(target, true),
          caughtUp: view.caughtUp.set(target, false),
          promotionBase: view.promotionBase.set(target, false),
          promotionActor: view.promotionActor.set(target, None),
        }
    }

  pure def applySlot(view: MembershipView, slot: KvSlot): MembershipView =
    match slot {
      | Filled(entry) => applyMemberCommand(view, entry.actor, entry.command)
      | Empty => view
    }

  def currentMembershipView: MembershipView = {
    roles: memberRole,
    states: memberState,
    invites: inviteState,
    records: kvRecordCommitted,
    caughtUp: caughtUp,
    promotionBase: promotionBase,
    promotionActor: promotionActor,
  }

  def currentRaftConfig: RaftConfig = {
    voters: raftVoters,
    learners: raftLearners,
    nonVoting: raftNonVoting,
  }

  def replayMembershipAt(index: int): MembershipView = {
    val after1 = if (index >= 1) applySlot(initialMembershipView, committed.get(1)) else initialMembershipView
    val after2 = if (index >= 2) applySlot(after1, committed.get(2)) else after1
    val after3 = if (index >= 3) applySlot(after2, committed.get(3)) else after2
    if (index >= 4) applySlot(after3, committed.get(4)) else after3
  }

  action init = all {
    committed' = emptyKvLog,
    nextKvIndex' = 1,
    kvRevision' = 1,
    memberRole' = initialMembershipView.roles,
    memberState' = initialMembershipView.states,
    inviteState' = initialMembershipView.invites,
    kvRecordCommitted' = initialMembershipView.records,
    caughtUp' = initialMembershipView.caughtUp,
    promotionBase' = initialMembershipView.promotionBase,
    promotionActor' = initialMembershipView.promotionActor,
    raftVoters' = initialRaftConfig.voters,
    raftLearners' = initialRaftConfig.learners,
    raftNonVoting' = initialRaftConfig.nonVoting,
  }

  action commitMemberCommand(actor: Node, command: MemberCommand): bool = {
    val view = currentMembershipView
    val entry = { actor: actor, command: command }
    val nextView = applyMemberCommand(view, actor, command)
    val nextConfig = projectRaftConfig(nextView)
    all {
      nextKvIndex <= MaxKvIndex,
      commandEnabled(view, actor, command),
      committed' = committed.set(nextKvIndex, Filled(entry)),
      nextKvIndex' = nextKvIndex + 1,
      kvRevision' = kvRevision + 1,
      memberRole' = nextView.roles,
      memberState' = nextView.states,
      inviteState' = nextView.invites,
      kvRecordCommitted' = nextView.records,
      caughtUp' = nextView.caughtUp,
      promotionBase' = nextView.promotionBase,
      promotionActor' = nextView.promotionActor,
      raftVoters' = nextConfig.voters,
      raftLearners' = nextConfig.learners,
      raftNonVoting' = nextConfig.nonVoting,
    }
  }

  /// Focused transition surface for symbolic verification. It covers the
  /// membership lifecycle that first has to refine the KV commit contract:
  /// invite, pending/learner acceptance, catch-up evidence, and voter promotion.
  action stepCore = {
    nondet actor = oneOf(Nodes)
    nondet target = oneOf(Nodes)
    any {
      commitMemberCommand(actor, CmdOpenInvite(target)),
      commitMemberCommand(actor, CmdCommitPending(target)),
      commitMemberCommand(actor, CmdAcceptLearner(target)),
      commitMemberCommand(actor, CmdRecordCatchUp(target)),
      commitMemberCommand(actor, CmdPromoteVoter(target)),
    }
  }

  /// Broader exploratory surface. This is useful for randomized runs, but is
  /// intentionally not the default symbolic step because it explodes the
  /// command branching factor.
  action step = {
    nondet actor = oneOf(Nodes)
    nondet target = oneOf(Nodes)
    any {
      stepCore,
      commitMemberCommand(actor, CmdExpireInvite(target)),
      commitMemberCommand(actor, CmdDemoteLearner(target)),
      commitMemberCommand(actor, CmdDemoteNonVoting(target)),
      commitMemberCommand(actor, CmdMarkSuspect(target)),
      commitMemberCommand(actor, CmdMarkOffline(target)),
      commitMemberCommand(actor, CmdRestoreHealthy(target)),
      commitMemberCommand(actor, CmdRemove(target)),
    }
  }

  /// Safety/refinement: the materialized member view is exactly the replay of
  /// the abstract committed KV prefix.
  def membershipMatchesKvReplay: bool = {
    val replayed = replayMembershipAt(nextKvIndex - 1)
    all {
      memberRole == replayed.roles,
      memberState == replayed.states,
      inviteState == replayed.invites,
      kvRecordCommitted == replayed.records,
      caughtUp == replayed.caughtUp,
      promotionBase == replayed.promotionBase,
      promotionActor == replayed.promotionActor,
    }
  }

  /// Safety/refinement: the materialized Raft config is exactly the projection
  /// of committed membership records.
  def raftConfigMatchesMembership: bool = {
    val projected = projectRaftConfig(currentMembershipView)
    all {
      raftVoters == projected.voters,
      raftLearners == projected.learners,
      raftNonVoting == projected.nonVoting,
    }
  }

  /// Safety: every committed membership command was valid in the member view
  /// produced by the committed prefix before it.
  def committedCommandsWereEnabled: bool =
    KvIndexes
      .filter(i => i < nextKvIndex)
      .forall(i =>
        match committed.get(i) {
          | Filled(entry) => commandEnabled(replayMembershipAt(i - 1), entry.actor, entry.command)
          | Empty => false
        }
      )

  def inactiveStatesAreOutsideRaftConfig: bool =
    Nodes.forall(n =>
      isRaftConfigState(memberState.get(n)) == (
        raftVoters.contains(n) or raftLearners.contains(n) or raftNonVoting.contains(n)
      )
    )

  def authorizedRequiresKvRecord: bool =
    authorizedMemberSetOf(memberState).forall(n => kvRecordCommitted.get(n))

  def pendingAndRemovedAreNotAuthorized: bool =
    Nodes.forall(n =>
      (
        memberState.get(n) != Pending
          and memberState.get(n) != Removed
      )
        or not(authorizedMemberSetOf(memberState).contains(n))
    )

  /// Safety: every voter is backed by a committed member record and catch-up
  /// evidence. N1 is the bootstrap voter; all later voters record a distinct
  /// actor that committed the promotion.
  def votersWerePromotedSafely: bool =
    raftVoters.forall(n =>
      kvRecordCommitted.get(n)
        and caughtUp.get(n)
        and promotionBase.get(n)
        and (
          n == N1
            or (
              isSome(promotionActor.get(n))
                and unwrapOr(promotionActor.get(n), n) != n
            )
        )
    )

  def voterSetIsNonEmpty: bool =
    raftVoters.size() >= 1

  def kvIndexAndRevisionInBounds: bool =
    nextKvIndex >= 1
      and nextKvIndex <= MaxKvIndex + 1
      and kvRevision == nextKvIndex

  def safety: bool = all {
    membershipMatchesKvReplay,
    raftConfigMatchesMembership,
    committedCommandsWereEnabled,
    inactiveStatesAreOutsideRaftConfig,
    authorizedRequiresKvRecord,
    pendingAndRemovedAreNotAuthorized,
    votersWerePromotedSafely,
    voterSetIsNonEmpty,
    kvIndexAndRevisionInBounds,
  }
}
