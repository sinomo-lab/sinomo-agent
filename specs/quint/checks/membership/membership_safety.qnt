/// Invariant checks for `membership`.
module membership_safety {
  import common.* from "../../lib/common"
  import membership_base.* from "../../lib/membership_base"
  import membership.* from "../../models/membership/membership"


  /// Safety: anything trusted by membership must have a committed KV record.
  def authorizedRequiresKvRecord: bool =
    authorizedMemberSet.forall(n => kvRecordCommitted.get(n))

  /// Safety: pending records are real records, but they are not authorized for
  /// normal cluster work.

  def pendingIsNotAuthorized: bool =
    Nodes.forall(n => not(memberState.get(n) == Pending) or not(authorizedMemberSet.contains(n)))

  /// Safety: removed members keep removal evidence but lose authorization.

  def removedIsNotAuthorized: bool =
    Nodes.forall(n => not(memberState.get(n) == Removed) or not(authorizedMemberSet.contains(n)))

  /// Safety: discovery and unverified accepted responses alone cannot create a
  /// membership record or authorize a node.

  def observationsAreNotAuthority: bool =
    Nodes.forall(n =>
      not(
        (
          discovered.contains(n)
            or joinPhase.get(n) == AcceptedUnverified
        )
          and not(kvRecordCommitted.get(n))
      )
        or memberState.get(n) == NonMember
    )

  /// Safety: Raft config only includes states that the Rust membership-to-Raft
  /// projection includes: voter, learner, and non-voting.

  def raftConfigExcludesInactiveStates: bool =
    Nodes.forall(n =>
      isRaftConfigState(memberState.get(n)) == raftMemberSet.contains(n)
    )

  /// Safety: promotion to voter requires a committed learner/non-voting base
  /// and catch-up proof. N1 is the bootstrap voter.

  def votersWerePromotedSafely: bool =
    voterSet.forall(n =>
      caughtUp.get(n)
        and kvRecordCommitted.get(n)
        and promotionBase.get(n)
        and (
          n == N1
            or (
              isSome(promotionActor.get(n))
                and unwrapOr(promotionActor.get(n), n) != n
            )
        )
    )

  /// Safety: membership mutations never leave the cluster without a voter.

  def voterSetIsNonEmpty: bool =
    voterSet.size() >= 1

  /// Safety: shadow daemon reachability can be observed, but a shadow process is
  /// never a `/cluster/members/<node_id>` participant.

  def shadowDaemonIsNotMember: bool =
    shadowObserved.subseteq(Nodes) and shadowMembers == Set()


  def revisionInBounds: bool =
    kvRevision >= 1 and kvRevision <= MaxRevision


  def safety: bool = all {
    authorizedRequiresKvRecord,
    pendingIsNotAuthorized,
    removedIsNotAuthorized,
    observationsAreNotAuthority,
    raftConfigExcludesInactiveStates,
    votersWerePromotedSafely,
    voterSetIsNonEmpty,
    shadowDaemonIsNotMember,
    revisionInBounds,
  }
}
