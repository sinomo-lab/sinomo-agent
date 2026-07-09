/// Invariant checks for `membership_kv`.
module membership_kv_safety {
  import common.* from "../../lib/common"
  import membership_base.* from "../../lib/membership_base"
  import membership_kv.* from "../../models/membership/membership_kv"


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
