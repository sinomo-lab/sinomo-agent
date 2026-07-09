/// Invariant checks for `dfs_replication_policy`.
module dfs_replication_policy_safety {
  import common.* from "../../lib/common"
  import dfs_replication_policy.* from "../../models/dfs/replication_policy"

  def activePoliciesAreValid: bool =
    inactivePhase(phase)
      or validReplicationPolicy(desiredReplicas, minCommitReplicas, objectPolicy)


  def durableReceiptsAreDesiredReplicas: bool =
    durableReceipts.subseteq(desiredReplicas)


  def commitReceiptsAreCapturedDesiredReplicas: bool =
    if (phase == Committed) {
      all {
        commitReceipts.subseteq(desiredReplicas),
        commitReceipts.subseteq(durableReceipts),
        commitReceipts.size() >= minCommitReplicas,
      }
    } else {
      commitReceipts == Set()
    }


  def visibleCommitRequiresReceiptThreshold: bool =
    phase != Committed or commitReceipts.size() >= minCommitReplicas


  def repairJobsExactlyMissingAfterCommit: bool =
    if (phase == Committed) {
      repairJobs == missingDesiredReplicas(desiredReplicas, durableReceipts)
    } else {
      repairJobs == Set()
    }


  def repairJobsAreDesiredAndNotDurable: bool =
    all {
      repairJobs.subseteq(desiredReplicas),
      repairJobs.intersect(durableReceipts) == Set(),
    }


  def strictConfirmedCommitsHaveNoRepairBacklog: bool =
    all {
      phase == Committed,
      objectPolicy == ReplicatedConfirmed,
    } implies repairJobs == Set()


  def rejectedIntentNeverBecomesVisible: bool =
    phase != Rejected or all {
      commitReceipts == Set(),
      repairJobs == Set(),
    }


  def eventualCommitMayHaveRepairBacklogOnlyBelowAllReplicas: bool =
    all {
      phase == Committed,
      objectPolicy == ReplicatedEventual,
      repairJobs.size() > 0,
    } implies minCommitReplicas < desiredReplicas.size()


  def safety: bool = all {
    activePoliciesAreValid,
    durableReceiptsAreDesiredReplicas,
    commitReceiptsAreCapturedDesiredReplicas,
    visibleCommitRequiresReceiptThreshold,
    repairJobsExactlyMissingAfterCommit,
    repairJobsAreDesiredAndNotDurable,
    strictConfirmedCommitsHaveNoRepairBacklog,
    rejectedIntentNeverBecomesVisible,
    eventualCommitMayHaveRepairBacklogOnlyBelowAllReplicas,
  }
}
