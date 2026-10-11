module dfs_whole_shard_healthy_scenario {
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
      all { dfs_whole_shard::acknowledgePrepare(1, 2), phase' = phase + 1 }
    } else if (phase == 3) {
      all { dfs_whole_shard::acknowledgePrepare(1, 3), phase' = phase + 1 }
    } else if (phase == 4) {
      all { dfs_whole_shard::admitCommit(1), phase' = phase + 1 }
    } else if (phase == 5) {
      all { dfs_whole_shard::commit(1, dfs_whole_shard::digest(1, 1), true), phase' = phase + 1 }
    } else if (phase == 6) {
      all { dfs_whole_shard::releasePublicationGuard(1), phase' = phase + 1 }
    } else if (phase == 7) {
      all { dfs_whole_shard::restart(1), phase' = phase + 1 }
    } else if (phase == 8) {
      all { dfs_whole_shard::replay(1, dfs_whole_shard::digest(1, 1)), phase' = phase + 1 }
    } else if (phase == 9) {
      all { dfs_whole_shard::rejectChangedRetry(1), phase' = phase + 1 }
    } else { all { dfs_whole_shard::stutter, phase' = phase } }
  def safety: bool = dfs_whole_shard_safety::safety
  def terminalWitness: bool = phase == 10 and all { dfs_whole_shard_safety::witnessHealthy, dfs_whole_shard_safety::witnessUnknownReplay, dfs_whole_shard_safety::witnessChangedRetryRejected }
}
