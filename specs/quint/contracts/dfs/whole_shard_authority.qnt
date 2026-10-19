/// Current ordinary metadata authority. Content receipts and KV votes are
/// separate inputs; neither can replace the complete assigned participant set.
module dfs_whole_shard_authority {
  pure def publicationProofAllowed(standalone: bool, primary: int, members: Set[int], acks: Set[int]): bool =
    if (standalone) all { members == Set(primary), acks == Set() }
    else all { members.size() > 0, members.contains(primary), acks == members }
}
