/// Target hardening for background replication racing foreground authority
/// ACKs. Delivery hints may retire obsolete work, never acknowledge a probe.
/// A rejected plan is retried if the primary advanced; unchanged divergence
/// retains recovery. Transport, disk persistence and authority admission are
/// covered by their existing models and runtime tests.
module dfs_replication_plan_freshness {
  type State = {
    head: int, peer: int, hint: int, cursor: int,
    planHead: int, target: int, probe: bool, exact: bool,
    prepared: bool, rejected: bool, deferred: bool,
    snapshot: bool, acknowledged: bool,
  }
  var plan: State
  def done = plan.deferred or plan.snapshot or plan.acknowledged
  action init = plan' = {
    head: 1, peer: 1, hint: 0, cursor: 0, planHead: 0, target: 0,
    probe: false, exact: true, prepared: false, rejected: false,
    deferred: false, snapshot: false, acknowledged: false,
  }
  action prepare = all {
    not(plan.prepared),
    nondet probe = Set(true, false).oneOf()
    nondet target = Set(1, plan.head).oneOf()
    plan' = plan.with("prepared", true).with("planHead", plan.head)
      .with("target", target).with("probe", probe),
  }
  action foregroundWrite = all {
    not(done), plan.head < 3,
    plan' = plan.with("head", plan.head + 1).with("peer", plan.head + 1),
  }
  action observeExactAck = all {
    not(done), plan.exact,
    plan' = plan.with("hint", plan.peer),
  }
  action loseReplicaHistory = all {
    not(done), plan.exact,
    plan' = plan.with("exact", false),
  }
  action discardDeliveredBatch = all {
    plan.prepared, not(done), not(plan.probe), plan.hint >= plan.target,
    plan' = plan.with("deferred", true),
  }
  action acknowledgeExact = all {
    plan.prepared, not(done), not(plan.rejected), plan.exact,
    plan.peer == plan.target,
    plan' = plan.with("acknowledged", true).with("cursor", plan.target),
  }
  action reject = all {
    plan.prepared, not(done), not(plan.rejected),
    plan.peer != plan.target or not(plan.exact),
    plan' = plan.with("rejected", true),
  }
  action deferObsoletePlan = all {
    plan.rejected, not(done), plan.head > plan.planHead,
    plan' = plan.with("deferred", true),
  }
  action repairCurrentPlan = all {
    plan.rejected, not(done), plan.head == plan.planHead,
    plan' = plan.with("snapshot", true),
  }
  action step = any {
    prepare, foregroundWrite, observeExactAck, loseReplicaHistory,
    discardDeliveredBatch, acknowledgeExact, reject, deferObsoletePlan,
    repairCurrentPlan, plan' = plan,
  }
}
