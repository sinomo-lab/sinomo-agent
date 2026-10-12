/// Model sensitivity witness, never native authorization or conformance evidence.
module dfs_whole_shard_metadata_content_scenario {
  import dfs_whole_shard from "../../models/dfs/whole_shard"
  import dfs_whole_shard_safety from "../../checks/dfs/whole_shard_safety"
  pure val profile = dfs_whole_shard::defaultProfile
    .with("nodes", Set(1, 2, 3)).with("voters", Set(1, 2, 3))
    .with("replacementMembers", Set(1, 2, 3))
    .with("contentReplicas", Set(1, 3)).with("requiredW", 2)
    .with("metadataOnlyOperations", Set(1, 2))
  var phase: int
  action init = all {
    dfs_whole_shard::initFromCertifiedCut(profile,
      dfs_whole_shard::Operations.mapBy(_ => Set()),
      profile.nodes.mapBy(n => if (n == 1) 1 else 0), profile.nodes),
    phase' = 0,
  }
  action step =
    if (phase == 0) {
      all { dfs_whole_shard::prepare(1), phase' = phase + 1 }
    } else if (phase == 1) {
      all { dfs_whole_shard::acknowledgePrepare(1, 2), phase' = phase + 1 }
    } else if (phase == 2) {
      all { dfs_whole_shard::acknowledgePrepare(1, 3), phase' = phase + 1 }
    } else if (phase == 3) {
      all { dfs_whole_shard::admitCommit(1), phase' = phase + 1 }
    } else if (phase == 4) {
      all { dfs_whole_shard::commit(1, 11, false), phase' = phase + 1 }
    } else if (phase == 5) {
      all { dfs_whole_shard::releasePublicationGuard(1), phase' = phase + 1 }
    } else if (phase == 6) {
      all { dfs_whole_shard::prepare(2), phase' = phase + 1 }
    } else if (phase == 7) {
      all { dfs_whole_shard::acknowledgePrepare(2, 2), phase' = phase + 1 }
    } else if (phase == 8) {
      all { dfs_whole_shard::acknowledgePrepare(2, 3), phase' = phase + 1 }
    } else if (phase == 9) {
      all { dfs_whole_shard::admitCommit(2), phase' = phase + 1 }
    } else if (phase == 10) {
      all { dfs_whole_shard::commit(2, 21, false), phase' = phase + 1 }
    } else if (phase == 11) {
      all { dfs_whole_shard::releasePublicationGuard(2), phase' = phase + 1 }
    } else if (phase == 12) {
      all { dfs_whole_shard::stage(3, 1), phase' = phase + 1 }
    } else if (phase == 13) {
      all { dfs_whole_shard::stage(3, 3), phase' = phase + 1 }
    } else if (phase == 14) {
      all { dfs_whole_shard::prepare(3), phase' = phase + 1 }
    } else if (phase == 15) {
      all { dfs_whole_shard::acknowledgePrepare(3, 2), phase' = phase + 1 }
    } else if (phase == 16) {
      all { dfs_whole_shard::acknowledgePrepare(3, 3), phase' = phase + 1 }
    } else if (phase == 17) {
      all { dfs_whole_shard::admitCommit(3), phase' = phase + 1 }
    } else if (phase == 18) {
      all { dfs_whole_shard::commit(3, 31, false), phase' = phase + 1 }
    } else if (phase == 19) {
      all { dfs_whole_shard::releasePublicationGuard(3), phase' = phase + 1 }
    } else { all { dfs_whole_shard::stutter, phase' = phase } }
  def safety: bool = dfs_whole_shard_safety::safety
  def terminalWitness: bool = phase == 20 and all {
    dfs_whole_shard::s.head == 6, dfs_whole_shard::s.visible == 3,
    dfs_whole_shard::s.commits.get(1).receipts == Set(),
    dfs_whole_shard::s.commits.get(2).receipts == Set(),
    dfs_whole_shard::s.commits.get(3).receipts == Set(1, 3),
    dfs_whole_shard::s.commits.get(1).acks == Set(1, 2, 3),
    dfs_whole_shard::s.commits.get(2).acks == Set(1, 2, 3),
    dfs_whole_shard::s.commits.get(3).acks == Set(1, 2, 3),
  }
}
