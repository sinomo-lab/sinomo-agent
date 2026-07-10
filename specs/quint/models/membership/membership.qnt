/// Cluster membership model for Sinomo's control plane.
///
/// This spec models the authoritative membership state that is stored through
/// the cluster KV. Network observations and accepted-looking responses are
/// modeled separately so we can state the important safety rule directly:
/// reachability, discovery, and unverified acceptance do not authorize a node.
///
/// Evidence status: the catch-up certificate required before voter promotion is
/// target hardening. Current Rust validates learner/non-voting provenance and
/// configuration overlap, but does not yet enforce this `caughtUp` predicate.
module membership {
  import common.* from "../../lib/common"
  import membership_base.* from "../../lib/membership_base"

  var memberRole: Node -> MemberRole
  var memberState: Node -> MemberState
  var inviteState: Node -> InviteState
  var joinPhase: Node -> JoinPhase
  var kvRecordCommitted: Node -> bool
  var authoritativeEvidence: Node -> bool
  var caughtUp: Node -> bool
  var promotionBase: Node -> bool
  var promotionActor: Node -> Option[Node]
  var discovered: Set[Node]
  var shadowObserved: Set[Node]
  var shadowMembers: Set[Node]
  var kvRevision: int

  pure val MaxRevision: int = 6

  def voterSet: Set[Node] =
    voterSetOf(memberState)

  def raftMemberSet: Set[Node] =
    raftMemberSetOf(memberState)

  def authorizedMemberSet: Set[Node] =
    authorizedMemberSetOf(memberState)

  def canCommit(actor: Node): bool =
    voterSet.contains(actor) and voterSet.size() >= quorumSize(voterSet)

  action init = all {
    memberRole' = initialRole,
    memberState' = initialState,
    inviteState' = Nodes.mapBy(_ => NoInvite),
    joinPhase' = Nodes.mapBy(_ => NoJoin),
    kvRecordCommitted' = initialKvRecordCommitted,
    authoritativeEvidence' = Nodes.mapBy(_ => false),
    caughtUp' = initialCaughtUp,
    promotionBase' = initialPromotionBase,
    promotionActor' = emptyPromotionActor,
    discovered' = Set(),
    shadowObserved' = Set(),
    shadowMembers' = Set(),
    kvRevision' = 1,
  }

  action observeDiscovery(node: Node): bool = all {
    discovered' = discovered.union(Set(node)),
    shadowObserved' = shadowObserved,
    shadowMembers' = shadowMembers,
    memberRole' = memberRole,
    memberState' = memberState,
    inviteState' = inviteState,
    joinPhase' = joinPhase,
    kvRecordCommitted' = kvRecordCommitted,
    authoritativeEvidence' = authoritativeEvidence,
    caughtUp' = caughtUp,
    promotionBase' = promotionBase,
    promotionActor' = promotionActor,
    kvRevision' = kvRevision,
  }

  action observeShadowEndpoint(node: Node): bool = all {
    shadowObserved' = shadowObserved.union(Set(node)),
    discovered' = discovered,
    shadowMembers' = shadowMembers,
    memberRole' = memberRole,
    memberState' = memberState,
    inviteState' = inviteState,
    joinPhase' = joinPhase,
    kvRecordCommitted' = kvRecordCommitted,
    authoritativeEvidence' = authoritativeEvidence,
    caughtUp' = caughtUp,
    promotionBase' = promotionBase,
    promotionActor' = promotionActor,
    kvRevision' = kvRevision,
  }

  action openInvite(actor: Node, target: Node): bool = all {
    canCommit(actor),
    actor != target,
    memberState.get(target) == NonMember,
    inviteState.get(target) != InviteOpen,
    kvRevision < MaxRevision,
    inviteState' = inviteState.set(target, InviteOpen),
    kvRevision' = kvRevision + 1,
    memberRole' = memberRole,
    memberState' = memberState,
    joinPhase' = joinPhase,
    kvRecordCommitted' = kvRecordCommitted,
    authoritativeEvidence' = authoritativeEvidence,
    caughtUp' = caughtUp,
    promotionBase' = promotionBase,
    promotionActor' = promotionActor,
    discovered' = discovered,
    shadowObserved' = shadowObserved,
    shadowMembers' = shadowMembers,
  }

  action expireInvite(target: Node): bool = all {
    inviteState.get(target) == InviteOpen,
    inviteState' = inviteState.set(target, InviteExpired),
    memberRole' = memberRole,
    memberState' = memberState,
    joinPhase' = joinPhase,
    kvRecordCommitted' = kvRecordCommitted,
    authoritativeEvidence' = authoritativeEvidence,
    caughtUp' = caughtUp,
    promotionBase' = promotionBase,
    promotionActor' = promotionActor,
    discovered' = discovered,
    shadowObserved' = shadowObserved,
    shadowMembers' = shadowMembers,
    kvRevision' = kvRevision,
  }

  action startJoin(target: Node): bool = all {
    inviteState.get(target) == InviteOpen,
    memberState.get(target) == NonMember,
    joinPhase.get(target) == NoJoin,
    joinPhase' = joinPhase.set(target, LocalPending),
    memberRole' = memberRole,
    memberState' = memberState,
    inviteState' = inviteState,
    kvRecordCommitted' = kvRecordCommitted,
    authoritativeEvidence' = authoritativeEvidence,
    caughtUp' = caughtUp,
    promotionBase' = promotionBase,
    promotionActor' = promotionActor,
    discovered' = discovered,
    shadowObserved' = shadowObserved,
    shadowMembers' = shadowMembers,
    kvRevision' = kvRevision,
  }

  action receiveUnverifiedAccept(target: Node): bool = all {
    joinPhase.get(target) == LocalPending,
    joinPhase' = joinPhase.set(target, AcceptedUnverified),
    memberRole' = memberRole,
    memberState' = memberState,
    inviteState' = inviteState,
    kvRecordCommitted' = kvRecordCommitted,
    authoritativeEvidence' = authoritativeEvidence,
    caughtUp' = caughtUp,
    promotionBase' = promotionBase,
    promotionActor' = promotionActor,
    discovered' = discovered,
    shadowObserved' = shadowObserved,
    shadowMembers' = shadowMembers,
    kvRevision' = kvRevision,
  }

  action receiveAuthoritativeEvidence(target: Node): bool = all {
    joinPhase.get(target) == LocalPending or joinPhase.get(target) == AcceptedUnverified,
    authoritativeEvidence' = authoritativeEvidence.set(target, true),
    joinPhase' = joinPhase.set(target, AcceptedWithAuthority),
    memberRole' = memberRole,
    memberState' = memberState,
    inviteState' = inviteState,
    kvRecordCommitted' = kvRecordCommitted,
    caughtUp' = caughtUp,
    promotionBase' = promotionBase,
    promotionActor' = promotionActor,
    discovered' = discovered,
    shadowObserved' = shadowObserved,
    shadowMembers' = shadowMembers,
    kvRevision' = kvRevision,
  }

  action commitPendingMember(actor: Node, target: Node): bool = all {
    canCommit(actor),
    actor != target,
    inviteState.get(target) == InviteOpen,
    joinPhase.get(target) == LocalPending
      or joinPhase.get(target) == AcceptedUnverified
      or joinPhase.get(target) == AcceptedWithAuthority,
    memberState.get(target) == NonMember,
    kvRevision < MaxRevision,
    memberRole' = memberRole.set(target, LearnerRole),
    memberState' = memberState.set(target, Pending),
    inviteState' = inviteState.set(target, InviteUsed),
    kvRecordCommitted' = kvRecordCommitted.set(target, true),
    promotionBase' = promotionBase,
    caughtUp' = caughtUp.set(target, false),
    authoritativeEvidence' = authoritativeEvidence,
    joinPhase' = joinPhase,
    promotionActor' = promotionActor,
    discovered' = discovered,
    shadowObserved' = shadowObserved,
    shadowMembers' = shadowMembers,
    kvRevision' = kvRevision + 1,
  }

  action acceptJoinAsLearner(actor: Node, target: Node): bool = all {
    canCommit(actor),
    actor != target,
    kvRevision < MaxRevision,
    any {
      all {
        memberState.get(target) == NonMember,
        inviteState.get(target) == InviteOpen,
        joinPhase.get(target) == LocalPending
          or joinPhase.get(target) == AcceptedUnverified
          or joinPhase.get(target) == AcceptedWithAuthority,
      },
      all {
        memberState.get(target) == Pending,
        kvRecordCommitted.get(target),
      },
    },
    memberRole' = memberRole.set(target, LearnerRole),
    memberState' = memberState.set(target, Learner),
    inviteState' = inviteState.set(target, InviteUsed),
    kvRecordCommitted' = kvRecordCommitted.set(target, true),
    promotionBase' = promotionBase.set(target, true),
    caughtUp' = caughtUp.set(target, false),
    authoritativeEvidence' = authoritativeEvidence,
    joinPhase' =
      if (authoritativeEvidence.get(target)) {
        joinPhase.set(target, AcceptedWithAuthority)
      } else {
        joinPhase
      },
    promotionActor' = promotionActor,
    discovered' = discovered,
    shadowObserved' = shadowObserved,
    shadowMembers' = shadowMembers,
    kvRevision' = kvRevision + 1,
  }

  action catchUp(target: Node): bool = all {
    canCatchUp(memberState.get(target)),
    kvRecordCommitted.get(target),
    caughtUp' = caughtUp.set(target, true),
    memberRole' = memberRole,
    memberState' = memberState,
    inviteState' = inviteState,
    joinPhase' = joinPhase,
    kvRecordCommitted' = kvRecordCommitted,
    authoritativeEvidence' = authoritativeEvidence,
    promotionBase' = promotionBase,
    promotionActor' = promotionActor,
    discovered' = discovered,
    shadowObserved' = shadowObserved,
    shadowMembers' = shadowMembers,
    kvRevision' = kvRevision,
  }

  action promoteToVoter(actor: Node, target: Node): bool = all {
    canCommit(actor),
    actor != target,
    kvRevision < MaxRevision,
    memberState.get(target) == Learner or memberState.get(target) == NonVoting,
    kvRecordCommitted.get(target),
    caughtUp.get(target),
    promotionBase.get(target),
    memberRole' = memberRole.set(target, VoterRole),
    memberState' = memberState.set(target, Voter),
    promotionActor' = promotionActor.set(target, Some(actor)),
    inviteState' = inviteState,
    joinPhase' = joinPhase,
    kvRecordCommitted' = kvRecordCommitted,
    authoritativeEvidence' = authoritativeEvidence,
    caughtUp' = caughtUp,
    promotionBase' = promotionBase,
    discovered' = discovered,
    shadowObserved' = shadowObserved,
    shadowMembers' = shadowMembers,
    kvRevision' = kvRevision + 1,
  }

  action demoteVoter(actor: Node, target: Node, role: MemberRole): bool = all {
    canCommit(actor),
    memberState.get(target) == Voter,
    role != VoterRole,
    voterSet.size() > 1,
    kvRevision < MaxRevision,
    memberRole' = memberRole.set(target, role),
    memberState' = memberState.set(target, stateForRole(role)),
    inviteState' = inviteState,
    joinPhase' = joinPhase,
    kvRecordCommitted' = kvRecordCommitted,
    authoritativeEvidence' = authoritativeEvidence,
    caughtUp' = caughtUp,
    promotionBase' = promotionBase,
    promotionActor' = promotionActor,
    discovered' = discovered,
    shadowObserved' = shadowObserved,
    shadowMembers' = shadowMembers,
    kvRevision' = kvRevision + 1,
  }

  action markSuspect(actor: Node, target: Node): bool = all {
    canCommit(actor),
    isAuthorizedState(memberState.get(target)),
    memberState.get(target) != Suspect,
    memberState.get(target) != Offline,
    memberState.get(target) != Removed,
    not(memberState.get(target) == Voter) or voterSet.size() > 1,
    kvRevision < MaxRevision,
    memberState' = memberState.set(target, Suspect),
    memberRole' = memberRole,
    inviteState' = inviteState,
    joinPhase' = joinPhase,
    kvRecordCommitted' = kvRecordCommitted,
    authoritativeEvidence' = authoritativeEvidence,
    caughtUp' = caughtUp,
    promotionBase' = promotionBase,
    promotionActor' = promotionActor,
    discovered' = discovered,
    shadowObserved' = shadowObserved,
    shadowMembers' = shadowMembers,
    kvRevision' = kvRevision + 1,
  }

  action markOffline(actor: Node, target: Node): bool = all {
    canCommit(actor),
    isAuthorizedState(memberState.get(target)),
    memberState.get(target) != Offline,
    memberState.get(target) != Removed,
    not(memberState.get(target) == Voter) or voterSet.size() > 1,
    kvRevision < MaxRevision,
    memberState' = memberState.set(target, Offline),
    memberRole' = memberRole,
    inviteState' = inviteState,
    joinPhase' = joinPhase,
    kvRecordCommitted' = kvRecordCommitted,
    authoritativeEvidence' = authoritativeEvidence,
    caughtUp' = caughtUp,
    promotionBase' = promotionBase,
    promotionActor' = promotionActor,
    discovered' = discovered,
    shadowObserved' = shadowObserved,
    shadowMembers' = shadowMembers,
    kvRevision' = kvRevision + 1,
  }

  action restoreHealthy(actor: Node, target: Node): bool = {
    val restoredState = stateForRole(memberRole.get(target))
    all {
      canCommit(actor),
      canBecomeHealthy(memberState.get(target)),
      kvRecordCommitted.get(target),
      not(restoredState == Voter) or caughtUp.get(target),
      kvRevision < MaxRevision,
      memberState' = memberState.set(target, restoredState),
      memberRole' = memberRole,
      inviteState' = inviteState,
      joinPhase' = joinPhase,
      kvRecordCommitted' = kvRecordCommitted,
      authoritativeEvidence' = authoritativeEvidence,
      caughtUp' = caughtUp,
      promotionBase' = promotionBase,
      promotionActor' = promotionActor,
      discovered' = discovered,
      shadowObserved' = shadowObserved,
      shadowMembers' = shadowMembers,
      kvRevision' = kvRevision + 1,
    }
  }

  action removeMember(actor: Node, target: Node): bool = all {
    canCommit(actor),
    actor != target,
    memberState.get(target) != NonMember,
    memberState.get(target) != Removed,
    not(memberState.get(target) == Voter) or voterSet.size() > 1,
    kvRevision < MaxRevision,
    memberState' = memberState.set(target, Removed),
    inviteState' = inviteState,
    joinPhase' = joinPhase.set(target, NoJoin),
    kvRecordCommitted' = kvRecordCommitted.set(target, true),
    authoritativeEvidence' = authoritativeEvidence.set(target, false),
    caughtUp' = caughtUp.set(target, false),
    memberRole' = memberRole,
    promotionBase' = promotionBase.set(target, false),
    promotionActor' = promotionActor.set(target, None),
    discovered' = discovered,
    shadowObserved' = shadowObserved,
    shadowMembers' = shadowMembers,
    kvRevision' = kvRevision + 1,
  }

  action step = any {
    {
      nondet target = oneOf(Nodes)
      any {
        observeDiscovery(target),
        observeShadowEndpoint(target),
        expireInvite(target),
        startJoin(target),
        receiveUnverifiedAccept(target),
        receiveAuthoritativeEvidence(target),
        catchUp(target),
      }
    },
    {
      nondet actor = oneOf(Nodes)
      nondet target = oneOf(Nodes)
      any {
        openInvite(actor, target),
        commitPendingMember(actor, target),
        acceptJoinAsLearner(actor, target),
        promoteToVoter(actor, target),
        markSuspect(actor, target),
        markOffline(actor, target),
        restoreHealthy(actor, target),
        removeMember(actor, target),
      }
    },
    {
      nondet actor = oneOf(Nodes)
      nondet target = oneOf(Nodes)
      any {
        demoteVoter(actor, target, LearnerRole),
        demoteVoter(actor, target, NonVotingRole),
      }
    }
  }
}
