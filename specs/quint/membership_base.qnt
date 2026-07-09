/// Shared membership vocabulary for cluster control-plane specifications.
module membership_base {
  import common.* from "./common"

  type MemberRole =
    | VoterRole
    | LearnerRole
    | NonVotingRole

  /// `NonMember` is a model-only absence state. Real persisted member records
  /// start at `Pending`; before that there is no `/cluster/members/<node_id>`
  /// record.
  type MemberState =
    | NonMember
    | Pending
    | Learner
    | Voter
    | NonVoting
    | Suspect
    | Offline
    | Removed

  type InviteState =
    | NoInvite
    | InviteOpen
    | InviteUsed
    | InviteExpired

  type JoinPhase =
    | NoJoin
    | LocalPending
    | AcceptedUnverified
    | AcceptedWithAuthority

  pure val initialRole: Node -> MemberRole =
    Nodes.mapBy(n => if (n == N1) VoterRole else LearnerRole)

  pure val initialState: Node -> MemberState =
    Nodes.mapBy(n => if (n == N1) Voter else NonMember)

  pure val initialKvRecordCommitted: Node -> bool =
    Nodes.mapBy(n => n == N1)

  pure val initialCaughtUp: Node -> bool =
    Nodes.mapBy(n => n == N1)

  pure val initialPromotionBase: Node -> bool =
    Nodes.mapBy(n => n == N1)

  pure val emptyPromotionActor: Node -> Option[Node] =
    Nodes.mapBy(_ => None)

  pure def stateForRole(role: MemberRole): MemberState =
    match role {
      | VoterRole => Voter
      | LearnerRole => Learner
      | NonVotingRole => NonVoting
    }

  pure def isAuthorizedState(state: MemberState): bool =
    match state {
      | NonMember => false
      | Pending => false
      | Removed => false
      | _ => true
    }

  pure def isRaftConfigState(state: MemberState): bool =
    match state {
      | Voter => true
      | Learner => true
      | NonVoting => true
      | _ => false
    }

  pure def canBecomeHealthy(state: MemberState): bool =
    match state {
      | Suspect => true
      | Offline => true
      | _ => false
    }

  pure def canCatchUp(state: MemberState): bool =
    match state {
      | Learner => true
      | NonVoting => true
      | Suspect => true
      | Offline => true
      | Voter => true
      | _ => false
    }

  pure def voterSetOf(states: Node -> MemberState): Set[Node] =
    Nodes.filter(n => states.get(n) == Voter)

  pure def raftMemberSetOf(states: Node -> MemberState): Set[Node] =
    Nodes.filter(n => isRaftConfigState(states.get(n)))

  pure def authorizedMemberSetOf(states: Node -> MemberState): Set[Node] =
    Nodes.filter(n => isAuthorizedState(states.get(n)))
}
