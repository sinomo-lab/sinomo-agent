/// Invariant checks for `dfs_metadata`.
module dfs_metadata_safety {
  import common.* from "../../lib/common"
  import dfs_metadata.* from "../../models/dfs/metadata"

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
