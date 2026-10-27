/// TARGET HARDENING ONLY. This is not current V7 publication authority.
/// Bundle aliases injectively name full immutable request/prepare/terminal
/// bytes, including the original terminal timestamp. Encoding is not modeled.
module dfs_availability_accepted_bundle_contract {
  pure val Members = Set(1, 2, 3)
  pure val Ballots = Set(1, 2, 3)
  pure val Bundles = Set(1, 2)
  pure val Quorums = Set(Set(1, 2), Set(1, 3), Set(2, 3))
  type Accepted = { ballot: int, bundle: int }
  type Reply = { present: bool, ballot: int, bundle: int }
  pure val EmptyAccepted: Accepted = { ballot: 0, bundle: 0 }
  pure val EmptyReply: Reply = { present: false, ballot: 0, bundle: 0 }
  pure def highestAccepted(cut: int -> Reply, quorum: Set[int]): Set[int] =
    quorum.filter(n => cut.get(n).bundle != 0).filter(n =>
      quorum.forall(other => cut.get(other).ballot <= cut.get(n).ballot))
  pure def proposalPreservesCut(cut: int -> Reply, quorum: Set[int], bundle: int): bool = all {
    Quorums.contains(quorum), Bundles.contains(bundle),
    quorum.forall(n => cut.get(n).present),
    highestAccepted(cut, quorum).forall(n => cut.get(n).bundle == bundle),
  }
}
