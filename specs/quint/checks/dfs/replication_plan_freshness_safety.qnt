module dfs_replication_plan_freshness_safety {
  import dfs_replication_plan_freshness.* from "../../models/dfs/replication_plan_freshness"
  def safety = all {
    plan.deferred implies all { plan.cursor == 0, not(plan.acknowledged), not(plan.snapshot) },
    plan.snapshot implies plan.planHead == plan.head,
    plan.acknowledged implies all { plan.exact, plan.cursor == plan.peer, not(plan.rejected) },
  }
  def witnessObsoleteProbeRetriesWithoutSnapshot =
    plan.probe and plan.rejected and plan.deferred and plan.head > plan.planHead
  def witnessObsoleteBatchRetriesWithoutSnapshot =
    not(plan.probe) and plan.rejected and plan.deferred and plan.head > plan.planHead
  def witnessDeliveryHintDoesNotAcknowledge =
    plan.deferred and not(plan.rejected) and plan.hint >= plan.target and plan.cursor == 0
  def witnessUnchangedDivergenceStillRepairs =
    plan.snapshot and not(plan.exact)
  def witnessCurrentExactProbeAcknowledges =
    plan.probe and plan.acknowledged
}
