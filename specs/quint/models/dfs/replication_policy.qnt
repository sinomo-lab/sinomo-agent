/// DFS replicated-eventual N/W receipt policy and repair backlog.
///
/// This module models the `DfsReplicationPolicy` and
/// `DistributedEventualCoordinator` surface used by DFS writes:
/// - `W = min_commit_replicas` and `N = desired_replicas` are validated.
/// - replicated-confirmed writes require `W == N`.
/// - only desired replicas may create durable receipts.
/// - a visible commit requires at least `W` durable receipts.
/// - after commit, repair jobs are exactly the desired replicas still missing
///   durable receipts; later durable repair receipts retire those jobs.
module dfs_replication_policy {
  import common.* from "../../lib/common"

  type DfsObjectPolicy = ReplicatedEventual | ReplicatedConfirmed
  type WritePhase = Idle | AwaitingReceipts | Committed | Rejected

  pure val ReplicaSets: Set[Set[Node]] = Set(
    Set(N1),
    Set(N2),
    Set(N3),
    Set(N1, N2),
    Set(N1, N3),
    Set(N2, N3),
    Set(N1, N2, N3)
  )
  pure val Thresholds: Set[int] = Set(1, 2, 3)
  pure val Policies: Set[DfsObjectPolicy] = Set(ReplicatedEventual, ReplicatedConfirmed)

  var phase: WritePhase
  var objectPolicy: DfsObjectPolicy
  var desiredReplicas: Set[Node]
  var minCommitReplicas: int
  var durableReceipts: Set[Node]
  var commitReceipts: Set[Node]
  var repairJobs: Set[Node]

  pure def validReplicationPolicy(
    desired: Set[Node],
    minCommit: int,
    policy: DfsObjectPolicy
  ): bool =
    all {
      desired.size() > 0,
      minCommit > 0,
      minCommit <= desired.size(),
      policy == ReplicatedEventual or minCommit == desired.size(),
    }

  pure def missingDesiredReplicas(desired: Set[Node], durable: Set[Node]): Set[Node] =
    desired.filter(node => not(durable.contains(node)))

  pure def inactivePhase(phaseValue: WritePhase): bool =
    phaseValue == Idle or phaseValue == Rejected

  action init = all {
    phase' = Idle,
    objectPolicy' = ReplicatedEventual,
    desiredReplicas' = Set(),
    minCommitReplicas' = 0,
    durableReceipts' = Set(),
    commitReceipts' = Set(),
    repairJobs' = Set(),
  }

  action beginIntent(
    desired: Set[Node],
    minCommit: int,
    policy: DfsObjectPolicy
  ): bool = all {
    phase == Idle,
    validReplicationPolicy(desired, minCommit, policy),
    phase' = AwaitingReceipts,
    objectPolicy' = policy,
    desiredReplicas' = desired,
    minCommitReplicas' = minCommit,
    durableReceipts' = Set(),
    commitReceipts' = Set(),
    repairJobs' = Set(),
  }

  action recordDurableReceipt(replica: Node): bool = all {
    phase == AwaitingReceipts or phase == Committed,
    desiredReplicas.contains(replica),
    phase' = phase,
    objectPolicy' = objectPolicy,
    desiredReplicas' = desiredReplicas,
    minCommitReplicas' = minCommitReplicas,
    durableReceipts' = durableReceipts.union(Set(replica)),
    commitReceipts' = commitReceipts,
    repairJobs' =
      if (phase == Committed) {
        repairJobs.filter(node => node != replica)
      } else {
        repairJobs
      },
  }

  action commitVisibleVersion: bool = all {
    phase == AwaitingReceipts,
    durableReceipts.size() >= minCommitReplicas,
    phase' = Committed,
    objectPolicy' = objectPolicy,
    desiredReplicas' = desiredReplicas,
    minCommitReplicas' = minCommitReplicas,
    durableReceipts' = durableReceipts,
    commitReceipts' = durableReceipts,
    repairJobs' = missingDesiredReplicas(desiredReplicas, durableReceipts),
  }

  action rejectBeforeCommit: bool = all {
    phase == AwaitingReceipts,
    durableReceipts.size() < minCommitReplicas,
    phase' = Rejected,
    objectPolicy' = objectPolicy,
    desiredReplicas' = desiredReplicas,
    minCommitReplicas' = minCommitReplicas,
    durableReceipts' = durableReceipts,
    commitReceipts' = Set(),
    repairJobs' = Set(),
  }

  action repairMissingReplica(replica: Node): bool = all {
    phase == Committed,
    repairJobs.contains(replica),
    phase' = phase,
    objectPolicy' = objectPolicy,
    desiredReplicas' = desiredReplicas,
    minCommitReplicas' = minCommitReplicas,
    durableReceipts' = durableReceipts.union(Set(replica)),
    commitReceipts' = commitReceipts,
    repairJobs' = repairJobs.filter(node => node != replica),
  }

  action step = any {
    {
      nondet desired = oneOf(ReplicaSets)
      nondet minCommit = oneOf(Thresholds)
      nondet policy = oneOf(Policies)
      beginIntent(desired, minCommit, policy)
    },
    {
      nondet replica = oneOf(Nodes)
      recordDurableReceipt(replica)
    },
    commitVisibleVersion,
    rejectBeforeCommit,
    {
      nondet replica = oneOf(Nodes)
      repairMissingReplica(replica)
    },
  }
}
