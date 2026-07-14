/// Deterministic same-primary membership handoff with a retiring-primary write
/// after the quorum-side pending decision and before the local backend freeze.
module dfs_canonical_write_membership_handoff_scenario {
  import dfs_canonical_write_eligibility from "../../models/dfs/canonical_write_eligibility"
  import dfs_canonical_write_eligibility_safety from "../../checks/dfs/canonical_write_eligibility_safety"

  type Phase =
    | Start
    | InitialPrepareDurable
    | RetiringPrimaryIsolated
    | HandoffPending
    | LateRetiringPrepareDurable
    | RetiringPrimaryReconnected
    | RetiringPrimaryFrozen
    | HandoffActivated

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
        dfs_canonical_write_eligibility::beginIntent(
          dfs_canonical_write_eligibility::FullShardPreparedIntent
        ),
        phase' = LateRetiringPrepareDurable,
      }
    } else if (phase == LateRetiringPrepareDurable) {
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
    } else {
      all {
        phase == HandoffActivated,
        dfs_canonical_write_eligibility::stutter,
        phase' = phase,
      }
    }

  def safety: bool = all {
    dfs_canonical_write_eligibility_safety::safety,
    phase == RetiringPrimaryFrozen implies all {
      dfs_canonical_write_eligibility::retiringPrimaryFenceAcknowledged,
      dfs_canonical_write_eligibility::retiringFenceOperationFloor
        > dfs_canonical_write_eligibility::requiredHandoffOperationFloor,
    },
    phase == HandoffActivated implies
      dfs_canonical_write_eligibility_safety::witnessPostCommitRetiringWriteIsCapturedAndCarried,
  }

  def witnessPostCommitRetiringWriteIsCapturedAndCarried: bool =
    phase == HandoffActivated and
      dfs_canonical_write_eligibility_safety::witnessPostCommitRetiringWriteIsCapturedAndCarried
}
