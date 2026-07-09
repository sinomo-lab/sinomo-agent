/// Shared finite model values for Sinomo cluster protocol specifications.
module common {
  /// A deliberately tiny cluster. Three voters are enough to exercise quorum,
  /// minority partitions, leader loss, and stale follower catch-up.
  type Node = N1 | N2 | N3

  /// Quint has no built-in optional type. Keeping it here avoids each spec
  /// inventing a local variant with slightly different names.
  type Option[a] = Some(a) | None

  pure val Nodes: Set[Node] = Set(N1, N2, N3)
  pure val Voters: Set[Node] = Nodes

  pure def quorumSize(voters: Set[Node]): int = (voters.size() / 2) + 1

  pure def hasQuorum(nodes: Set[Node]): bool =
    nodes.intersect(Voters).size() >= quorumSize(Voters)

  pure def onlineVoters(online: Node -> bool): Set[Node] =
    Voters.filter(n => online.get(n))

  pure def isSome(value: Option[a]): bool =
    match value {
      | Some(_) => true
      | None => false
    }

  pure def unwrapOr(value: Option[a], fallback: a): a =
    match value {
      | Some(inner) => inner
      | None => fallback
    }
}
