/// Invariant checks for `dfs_repair_content_validation`.
module dfs_repair_content_validation_safety {
  import common.* from "../../lib/common"
  import dfs_repair_content_validation.* from "../../models/dfs/repair_content_validation"

  def durableReceiptsAreDesiredReplicas: bool =
    durableReceipts.subseteq(desiredReplicas)


  def durableReceiptsHaveMatchingMetadataHash: bool =
    durableReceipts
      .filter(node => metadataReceiptHash.get(node) != Some(pendingHash))
      .size() == 0


  def lastAcceptedHashMatchesPending: bool =
    match lastAcceptedHash {
      | Some(hash) => hash == pendingHash
      | None => true
    }


  def currentLocationsAreDurable: bool =
    currentLocations.subseteq(durableReceipts)


  def currentLocationsOnlyForCurrentTargetHash: bool =
    currentLocations.size() == 0
      or all {
        targetVersionIsCurrent,
        currentObjectHash == pendingHash,
      }


  def repairJobsExactlyMissingDesiredReceipts: bool =
    if (pending) {
      repairJobs == missingDesiredReplicas(desiredReplicas, durableReceipts)
    } else {
      repairJobs == Set()
    }


  def wrongHashMetadataDoesNotSuppressRepair: bool =
    if (pending) {
      desiredReplicas
        .filter(node =>
          not(durableReceipts.contains(node))
            and metadataReceiptHash.get(node) == Some(OtherHash)
        )
        .subseteq(repairJobs)
    } else {
      true
    }


  def inflightTargetsAreDesiredReplicas: bool =
    all {
      Nodes
        .filter(node => isSome(stageInflight.get(node)))
        .subseteq(desiredReplicas),
      Nodes
        .filter(node => isSome(repairInflight.get(node)))
        .subseteq(desiredReplicas),
    }


  def stableTargetVersionHash: bool =
    targetVersionHashStable


  def safety: bool = all {
    durableReceiptsAreDesiredReplicas,
    durableReceiptsHaveMatchingMetadataHash,
    lastAcceptedHashMatchesPending,
    currentLocationsAreDurable,
    currentLocationsOnlyForCurrentTargetHash,
    repairJobsExactlyMissingDesiredReceipts,
    wrongHashMetadataDoesNotSuppressRepair,
    inflightTargetsAreDesiredReplicas,
    stableTargetVersionHash,
  }
}
