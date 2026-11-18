/// Safety and witnesses of count contributions while a target shard
/// converts. Bounded abstract contract; not a proof of the Rust
/// implementation.
module dfs_count_contributions_safety {
  import common.* from "../../lib/common"
  import dfs_count_contributions.* from "../../models/dfs/count_contributions"

  /// Shape of the state; it holds in every mode.
  def wellFormed: bool = all {
    s.count >= 0,
    Aliases.forall(a => isMoving(a) implies (s.linked.contains(a) and Sources.contains(s.loc.get(a)))),
  }

  /// While T is converting, every shard's contribution to it, T's own rows
  /// included, stays what it was when T recorded Converting.
  def contributionsFrozenWhileConverting: bool =
    s.target == Converting implies all {
      contribution(T) == s.own,
      Sources.forall(sh => contribution(sh) == s.frozen.get(sh)),
    }

  /// Contributions T accepted are the sources' actual contributions until T
  /// activates with them.
  def acceptedContributionsAreExact: bool = match s.accepted {
    | Some(n) => s.target == Converting implies n == contribution(S1) + contribution(S2)
    | None => true
  }

  /// T's maintained count is the number of names while T is Active.
  def countsExact: bool = s.target == Active implies s.count == s.linked.size()

  def safety: bool = all {
    contributionsFrozenWhileConverting,
    acceptedContributionsAreExact,
    countsExact,
  }

  /// A round was held by a job touching T that had not parked.
  def witnessRoundHeldByRunningJob: bool = s.heldRounds > 0
  /// T activated while a job carrying one of its names was parked on it.
  def witnessActivatedWithParkedJob: bool = s.activatedWithParkedJob
  /// T converted from old storage and its count is exact with a name in
  /// another shard.
  def witnessConvertedWithContribution: bool =
    s.target == Active and s.previous != None and s.own < s.count
}
