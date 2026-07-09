/// Invariant checks for `dfs_no_conflict`.
module dfs_no_conflict_safety {
  import common.* from "../../lib/common"
  import dfs_no_conflict.* from "../../models/dfs/no_conflict"

  def normalOperationHasNoConflictRecords: bool =
    conflictRecords == Set()


  def normalWriteOutcomeNeverRecordsConflict: bool =
    lastDecision != ConflictRecorded


  def currentEpochHasPrimary: bool =
    primaryByEpoch.get(shardEpoch) == Some(shardPrimary)


  def visibleVersionInBounds: bool =
    visibleVersion >= 0 and visibleVersion <= MaxVersion


  def committedVersionsAreDense: bool =
    Versions.forall(version =>
      if (version <= visibleVersion) {
        isSome(commitLog.get(version))
      } else {
        not(isSome(commitLog.get(version)))
      }
    )


  def commitsAdvanceByOneFromBase: bool =
    Versions.forall(version =>
      match commitLog.get(version) {
        | Some(record) => record.baseVersion + 1 == version
        | None => true
      }
    )


  def commitsUseConfiguredPrimaryForTheirEpoch: bool =
    Versions.forall(version =>
      match commitLog.get(version) {
        | Some(record) => primaryByEpoch.get(record.epoch) == Some(record.coordinator)
        | None => true
      }
    )


  def pendingIntentDoesNotDuplicateCommittedVersion: bool =
    match pendingIntent {
      | Some(intent) => commitLog.get(intent.targetVersion) == None
      | None => true
    }


  def stalePendingCannotBeCommitReady: bool =
    match pendingIntent {
      | Some(intent) =>
          all {
            any {
              intent.coordinator != shardPrimary,
              intent.epoch != shardEpoch,
              intent.baseVersion != visibleVersion,
            },
            commitLog.get(intent.targetVersion) == None,
          } implies not(pendingCanCommit)
      | None => true
    }


  def safety: bool = all {
    normalOperationHasNoConflictRecords,
    normalWriteOutcomeNeverRecordsConflict,
    currentEpochHasPrimary,
    visibleVersionInBounds,
    committedVersionsAreDense,
    commitsAdvanceByOneFromBase,
    commitsUseConfiguredPrimaryForTheirEpoch,
    pendingIntentDoesNotDuplicateCommittedVersion,
    stalePendingCannotBeCommitReady,
  }
}
