/// Native-shape bounded model scenario; not a native Rust capture.
module dfs_whole_shard_one_voter_scenario {
  import dfs_whole_shard from "../../models/dfs/whole_shard"
  import dfs_whole_shard_safety from "../../checks/dfs/whole_shard_safety"
  var phase: int
  action init = all {
    dfs_whole_shard::initFromProfile({ nodes: Set(1, 2, 3, 4), voters: Set(1),
      initialMembers: Set(1, 2, 3), replacementMembers: Set(1, 2, 4),
      contentReplicas: Set(1, 2, 3, 4), requiredW: 1, metadataOnlyOperations: Set() }),
    phase' = 0,
  }
  action step =
    if (phase == 0) {
      all { dfs_whole_shard::stage(1, 1), phase' = phase + 1 }
    } else if (phase == 1) {
      all { dfs_whole_shard::prepare(1), phase' = phase + 1 }
    } else if (phase == 2) {
      all { dfs_whole_shard::acknowledgePrepare(1, 2), phase' = phase + 1 }
    } else if (phase == 3) {
      all { dfs_whole_shard::acknowledgePrepare(1, 3), phase' = phase + 1 }
    } else if (phase == 4) {
      all { dfs_whole_shard::admitCommit(1), phase' = phase + 1 }
    } else if (phase == 5) {
      all { dfs_whole_shard::commit(1, dfs_whole_shard::digest(1, 1), false), phase' = phase + 1 }
    } else if (phase == 6) {
      all { dfs_whole_shard::releasePublicationGuard(1), phase' = phase + 1 }
    } else if (phase == 7) {
      all { dfs_whole_shard::stage(2, 1), phase' = phase + 1 }
    } else if (phase == 8) {
      all { dfs_whole_shard::prepare(2), phase' = phase + 1 }
    } else if (phase == 9) {
      all { dfs_whole_shard::changeConnectivity(Set(1, 2)), phase' = phase + 1 }
    } else if (phase == 10) {
      all { dfs_whole_shard::acknowledgePrepare(2, 2), phase' = phase + 1 }
    } else if (phase == 11) {
      all { dfs_whole_shard::issuePermit(2), phase' = phase + 1 }
    } else if (phase == 12) {
      all { dfs_whole_shard::admitCommit(2), phase' = phase + 1 }
    } else if (phase == 13) {
      all { dfs_whole_shard::commit(2, dfs_whole_shard::digest(2, 1), false), phase' = phase + 1 }
    } else if (phase == 14) {
      all { dfs_whole_shard::releasePublicationGuard(2), phase' = phase + 1 }
    } else { all { dfs_whole_shard::stutter, phase' = phase } }
  def safety: bool = dfs_whole_shard_safety::safety
  def terminalWitness: bool = phase == 15 and all {
    dfs_whole_shard::s.profile.voters == Set(1),
    dfs_whole_shard_safety::witnessHealthy, dfs_whole_shard_safety::witnessFallback,
  }
}
