module dfs_whole_shard_cached_permit_handoff_scenario {
  import dfs_whole_shard from "../../models/dfs/whole_shard"
  import dfs_whole_shard_safety from "../../checks/dfs/whole_shard_safety"
  var phase: int
  action init = all { dfs_whole_shard::init, phase' = 0 }
  action step =
    if (phase == 0) {
      all { dfs_whole_shard::stage(1, 1), phase' = phase + 1 }
    } else if (phase == 1) {
      all { dfs_whole_shard::prepare(1), phase' = phase + 1 }
    } else if (phase == 2) {
      all { dfs_whole_shard::changeConnectivity(Set(1, 2)), phase' = phase + 1 }
    } else if (phase == 3) {
      all { dfs_whole_shard::acknowledgePrepare(1, 2), phase' = phase + 1 }
    } else if (phase == 4) {
      all { dfs_whole_shard::issuePermit(1), phase' = phase + 1 }
    } else if (phase == 5) {
      all { dfs_whole_shard::beginHandoff(false), phase' = phase + 1 }
    } else if (phase == 6) {
      all { dfs_whole_shard::rejectCachedPermitAfterHandoff(1), phase' = phase + 1 }
    } else { all { dfs_whole_shard::stutter, phase' = phase } }
  def safety: bool = dfs_whole_shard_safety::safety
  def terminalWitness: bool = phase == 7 and all { dfs_whole_shard_safety::witnessCachedPermitHandoffRejected }
}
