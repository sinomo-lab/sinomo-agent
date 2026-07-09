/// DFS metadata authority over abstract committed cluster control-plane facts.
///
/// This module sits above the KV and membership layers. It assumes the control
/// plane can provide a committed metadata-shard primary and fenced epoch. DFS
/// metadata then verifies local safety: visible object versions advance only by
/// the current shard primary, at the current epoch, after the receipt threshold.
module dfs_metadata {
  import common.* from "./common"
  import membership_base.* from "./membership_base"

  type DfsObject = ObjA | ObjB

  type WriteIntent = {
    coordinator: Node,
    epoch: int,
    baseVersion: int,
    targetVersion: int,
    receipts: Set[Node],
  }

  type CommitRecord = {
    coordinator: Node,
    epoch: int,
    baseVersion: int,
    receipts: Set[Node],
  }

  pure val Objects: Set[DfsObject] = Set(ObjA, ObjB)
  pure val MaxVersion: int = 3
  pure val Versions: Set[int] = Set(1, 2, 3)
  pure val MaxEpoch: int = 3
  pure val Epochs: Set[int] = Set(1, 2, 3)
  pure val MinCommitReceipts: int = 2

  /// This is a small committed membership snapshot consumed from the
  /// membership/KV layer: one voter, one learner, and one non-voting data node.
  pure val committedMemberState: Node -> MemberState =
    Nodes.mapBy(n =>
      if (n == N1) {
        Voter
      } else if (n == N2) {
        Learner
      } else {
        NonVoting
      }
    )

  var shardPrimary: Node
  var shardEpoch: int
  var primaryByEpoch: int -> Option[Node]
  var visibleVersion: DfsObject -> int
  var intent: DfsObject -> Option[WriteIntent]
  var commitLog: DfsObject -> (int -> Option[CommitRecord])
  var conflicts: Set[DfsObject]

  pure val emptyPrimaryByEpoch: int -> Option[Node] =
    Epochs.mapBy(_ => None)

  pure val emptyObjectCommits: int -> Option[CommitRecord] =
    Versions.mapBy(_ => None)

  pure def isActiveDfsMember(node: Node): bool =
    match committedMemberState.get(node) {
      | Voter => true
      | Learner => true
      | NonVoting => true
      | _ => false
    }

  pure def isSomeIntent(value: Option[WriteIntent]): bool =
    match value {
      | Some(_) => true
      | None => false
    }

  pure def isSomeCommit(value: Option[CommitRecord]): bool =
    match value {
      | Some(_) => true
      | None => false
    }

  pure def unwrapIntent(value: Option[WriteIntent]): WriteIntent =
    match value {
      | Some(inner) => inner
      | None => {
          coordinator: N1,
          epoch: 0,
          baseVersion: 0,
          targetVersion: 0,
          receipts: Set(),
        }
    }

  pure def receiptThresholdMet(receipts: Set[Node]): bool =
    receipts.size() >= MinCommitReceipts

  pure def receiptsAreEligible(receipts: Set[Node]): bool =
    receipts.subseteq(Nodes.filter(n => isActiveDfsMember(n)))

  def intentCanCommit(object: DfsObject): bool = {
    val pending = unwrapIntent(intent.get(object))
    all {
      isSomeIntent(intent.get(object)),
      pending.coordinator == shardPrimary,
      pending.epoch == shardEpoch,
      pending.baseVersion == visibleVersion.get(object),
      pending.targetVersion == visibleVersion.get(object) + 1,
      pending.targetVersion <= MaxVersion,
      receiptThresholdMet(pending.receipts),
      receiptsAreEligible(pending.receipts),
    }
  }

  action init = all {
    shardPrimary' = N1,
    shardEpoch' = 1,
    primaryByEpoch' = emptyPrimaryByEpoch.set(1, Some(N1)),
    visibleVersion' = Objects.mapBy(_ => 0),
    intent' = Objects.mapBy(_ => None),
    commitLog' = Objects.mapBy(_ => emptyObjectCommits),
    conflicts' = Set(),
  }

  /// Abstract control-plane KV commit for `/dfs/.../metadata-shards/<shard>`.
  /// The lower layers guarantee this update is ordered and authoritative.
  action commitPrimaryChange(newPrimary: Node): bool = all {
    isActiveDfsMember(newPrimary),
    newPrimary != shardPrimary,
    shardEpoch < MaxEpoch,
    shardPrimary' = newPrimary,
    shardEpoch' = shardEpoch + 1,
    primaryByEpoch' = primaryByEpoch.set(shardEpoch + 1, Some(newPrimary)),
    visibleVersion' = visibleVersion,
    intent' = intent,
    commitLog' = commitLog,
    conflicts' = conflicts,
  }

  action beginIntent(object: DfsObject): bool = {
    val nextVersion = visibleVersion.get(object) + 1
    all {
      not(isSomeIntent(intent.get(object))),
      nextVersion <= MaxVersion,
      isActiveDfsMember(shardPrimary),
      intent' = intent.set(object, Some({
        coordinator: shardPrimary,
        epoch: shardEpoch,
        baseVersion: visibleVersion.get(object),
        targetVersion: nextVersion,
        receipts: Set(shardPrimary),
      })),
      shardPrimary' = shardPrimary,
      shardEpoch' = shardEpoch,
      primaryByEpoch' = primaryByEpoch,
      visibleVersion' = visibleVersion,
      commitLog' = commitLog,
      conflicts' = conflicts,
    }
  }

  action recordReceipt(object: DfsObject, replica: Node): bool = {
    val pending = unwrapIntent(intent.get(object))
    all {
      isSomeIntent(intent.get(object)),
      isActiveDfsMember(replica),
      intent' = intent.set(object, Some({
        coordinator: pending.coordinator,
        epoch: pending.epoch,
        baseVersion: pending.baseVersion,
        targetVersion: pending.targetVersion,
        receipts: pending.receipts.union(Set(replica)),
      })),
      shardPrimary' = shardPrimary,
      shardEpoch' = shardEpoch,
      primaryByEpoch' = primaryByEpoch,
      visibleVersion' = visibleVersion,
      commitLog' = commitLog,
      conflicts' = conflicts,
    }
  }

  action commitVisibleVersion(object: DfsObject): bool = {
    val pending = unwrapIntent(intent.get(object))
    all {
      intentCanCommit(object),
      commitLog.get(object).get(pending.targetVersion) == None,
      visibleVersion' = visibleVersion.set(object, pending.targetVersion),
      commitLog' = commitLog.set(
        object,
        commitLog.get(object).set(pending.targetVersion, Some({
          coordinator: pending.coordinator,
          epoch: pending.epoch,
          baseVersion: pending.baseVersion,
          receipts: pending.receipts,
        }))
      ),
      intent' = intent.set(object, None),
      shardPrimary' = shardPrimary,
      shardEpoch' = shardEpoch,
      primaryByEpoch' = primaryByEpoch,
      conflicts' = conflicts,
    }
  }

  action abortIntent(object: DfsObject): bool = all {
    isSomeIntent(intent.get(object)),
    intent' = intent.set(object, None),
    shardPrimary' = shardPrimary,
    shardEpoch' = shardEpoch,
    primaryByEpoch' = primaryByEpoch,
    visibleVersion' = visibleVersion,
    commitLog' = commitLog,
    conflicts' = conflicts,
  }

  action step = any {
    {
      nondet newPrimary = oneOf(Nodes)
      commitPrimaryChange(newPrimary)
    },
    {
      nondet object = oneOf(Objects)
      any {
        beginIntent(object),
        commitVisibleVersion(object),
        abortIntent(object),
      }
    },
    {
      nondet object = oneOf(Objects)
      nondet replica = oneOf(Nodes)
      recordReceipt(object, replica)
    },
  }

  def currentPrimaryIsActive: bool =
    isActiveDfsMember(shardPrimary)

  def currentEpochHasPrimary: bool =
    primaryByEpoch.get(shardEpoch) == Some(shardPrimary)

  def visibleVersionsAreDense: bool =
    Objects.forall(object =>
      Versions.forall(version =>
        if (version <= visibleVersion.get(object)) {
          isSomeCommit(commitLog.get(object).get(version))
        } else {
          not(isSomeCommit(commitLog.get(object).get(version)))
        }
      )
    )

  def commitsUseConfiguredPrimaryForTheirEpoch: bool =
    Objects.forall(object =>
      Versions.forall(version =>
        match commitLog.get(object).get(version) {
          | Some(record) => primaryByEpoch.get(record.epoch) == Some(record.coordinator)
          | None => true
        }
      )
    )

  def commitsHaveValidReceiptThreshold: bool =
    Objects.forall(object =>
      Versions.forall(version =>
        match commitLog.get(object).get(version) {
          | Some(record) => receiptThresholdMet(record.receipts) and receiptsAreEligible(record.receipts)
          | None => true
        }
      )
    )

  def commitsAdvanceByOneFromBase: bool =
    Objects.forall(object =>
      Versions.forall(version =>
        match commitLog.get(object).get(version) {
          | Some(record) => record.baseVersion + 1 == version
          | None => true
        }
      )
    )

  def commitEligibleIntentsUseCurrentAuthority: bool =
    Objects.forall(object =>
      not(intentCanCommit(object))
        or match intent.get(object) {
          | Some(pending) => pending.coordinator == shardPrimary and pending.epoch == shardEpoch
          | None => false
        }
    )

  def noNormalConflictRecords: bool =
    conflicts == Set()

  def versionsAndEpochInBounds: bool =
    all {
      shardEpoch >= 1,
      shardEpoch <= MaxEpoch,
      Objects.forall(object =>
        visibleVersion.get(object) >= 0 and visibleVersion.get(object) <= MaxVersion
      ),
    }

  def safety: bool = all {
    currentPrimaryIsActive,
    currentEpochHasPrimary,
    visibleVersionsAreDense,
    commitsUseConfiguredPrimaryForTheirEpoch,
    commitsHaveValidReceiptThreshold,
    commitsAdvanceByOneFromBase,
    commitEligibleIntentsUseCurrentAuthority,
    noNormalConflictRecords,
    versionsAndEpochInBounds,
  }
}
