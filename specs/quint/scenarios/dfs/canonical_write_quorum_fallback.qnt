/// Deterministic canonical-write trace for the exceptional cluster-quorum
/// fallback and for fencing a permit after a proof-carrying promotion.
module dfs_canonical_write_quorum_fallback_scenario {
  import dfs_canonical_write_eligibility from "../../models/dfs/canonical_write_eligibility"
  import dfs_canonical_write_eligibility_safety from "../../checks/dfs/canonical_write_eligibility_safety"

  type Phase =
    | Start
    | InitialPrepareDurable
    | RetiringPrimaryIsolated
    | HandoffPending
    | RetiringPrimaryReconnected
    | RetiringPrimaryFrozen
    | HandoffActivated
    | ThirdReplicaPartitioned
    | QuorumPrepareDurable
    | QuorumWritePublished
    | StalePermitPrepared
    | AuthorityPromoted
    | StalePermitRejected

  var phase: Phase

  action init = all {
    dfs_canonical_write_eligibility::init,
    phase' = Start,
  }

  action step =
    if (phase == Start) {
      all {
        dfs_canonical_write_eligibility::beginIntent(
          dfs_canonical_write_eligibility::FullShardPreparedIntent
        ),
        phase' = InitialPrepareDurable,
      }
    } else if (phase == InitialPrepareDurable) {
      all {
        dfs_canonical_write_eligibility::changeNetworkPartition(
          dfs_canonical_write_eligibility::OldPrimaryIsolated
        ),
        phase' = RetiringPrimaryIsolated,
      }
    } else if (phase == RetiringPrimaryIsolated) {
      all {
        dfs_canonical_write_eligibility::commitSamePrimaryMembershipReconfiguration,
        phase' = HandoffPending,
      }
    } else if (phase == HandoffPending) {
      all {
        dfs_canonical_write_eligibility::changeNetworkPartition(
          dfs_canonical_write_eligibility::Connected
        ),
        phase' = RetiringPrimaryReconnected,
      }
    } else if (phase == RetiringPrimaryReconnected) {
      all {
        dfs_canonical_write_eligibility::acknowledgeRetiringPrimaryFence,
        phase' = RetiringPrimaryFrozen,
      }
    } else if (phase == RetiringPrimaryFrozen) {
      all {
        dfs_canonical_write_eligibility::completeAuthorityHandoff(false),
        phase' = HandoffActivated,
      }
    } else if (phase == HandoffActivated) {
      all {
        dfs_canonical_write_eligibility::changeNetworkPartition(
          dfs_canonical_write_eligibility::ThirdReplicaIsolated
        ),
        phase' = ThirdReplicaPartitioned,
      }
    } else if (phase == ThirdReplicaPartitioned) {
      all {
        dfs_canonical_write_eligibility::beginIntent(
          dfs_canonical_write_eligibility::QuorumPreparedIntent
        ),
        phase' = QuorumPrepareDurable,
      }
    } else if (phase == QuorumPrepareDurable) {
      all {
        dfs_canonical_write_eligibility::publishVisibleVersion(
          2,
          dfs_canonical_write_eligibility::ClusterQuorumPermit
        ),
        phase' = QuorumWritePublished,
      }
    } else if (phase == QuorumWritePublished) {
      all {
        dfs_canonical_write_eligibility::beginIntent(
          dfs_canonical_write_eligibility::QuorumPreparedIntent
        ),
        phase' = StalePermitPrepared,
      }
    } else if (phase == StalePermitPrepared) {
      all {
        dfs_canonical_write_eligibility::promoteReplicaWithTransferProof,
        phase' = AuthorityPromoted,
      }
    } else if (phase == AuthorityPromoted) {
      all {
        dfs_canonical_write_eligibility::rejectStaleAuthority(3),
        phase' = StalePermitRejected,
      }
    } else {
      all {
        phase == StalePermitRejected,
        dfs_canonical_write_eligibility::stutter,
        phase' = phase,
      }
    }

  def safety: bool = all {
    dfs_canonical_write_eligibility_safety::safety,
    phase == QuorumWritePublished implies
      dfs_canonical_write_eligibility_safety::witnessQuorumPermitPublishesWithoutFullShardAcks,
    phase == StalePermitRejected implies
      dfs_canonical_write_eligibility_safety::witnessStaleEpochOrPermitCannotPublish,
  }

  def witnessQuorumPermitPublishesWithoutFullShardAcks: bool =
    phase.in(Set(QuorumWritePublished, StalePermitPrepared, AuthorityPromoted, StalePermitRejected)) and
      dfs_canonical_write_eligibility_safety::witnessQuorumPermitPublishesWithoutFullShardAcks

  def witnessStaleEpochOrPermitCannotPublish: bool =
    phase == StalePermitRejected and
      dfs_canonical_write_eligibility_safety::witnessStaleEpochOrPermitCannotPublish
}
