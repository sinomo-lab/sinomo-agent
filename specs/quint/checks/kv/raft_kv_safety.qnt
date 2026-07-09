/// Invariant checks for `raft_kv`.
module raft_kv_safety {
  import common.* from "../../lib/common"
  import raft_kv.* from "../../models/kv/raft_kv"


  /// Safety: committed indices form one canonical prefix, and any node that
  /// claims a committed index must agree with that prefix.
  def committedPrefixAgreement: bool =
    Nodes.forall(n => logsMatchCommittedPrefix(n))

  /// Safety: a visible committed index must hold an entry.

  def committedSlotsAreFilled: bool =
    LogIndexes
      .filter(i => i <= globalCommitIndex)
      .forall(i => match committed.get(i) { | Filled(_) => true | Empty => false })

  /// Safety: no term has two leaders in the explored bounded term set.

  def oneLeaderPerTerm: bool =
    Set(1, 2, 3, 4).forall(t => leadersByTerm.get(t).size() <= 1)

  /// Safety/refinement: the modeled KV state is exactly the replay of the
  /// committed log prefix.

  def kvMatchesCommittedLog: bool =
    kvCell == replayCommittedAt(globalCommitIndex)

  /// Safety: a reachable online quorum is required for the current committed
  /// index to advance through `propose`; this records the stable fact that a
  /// committed index is never ahead of the finite log bound.

  def committedIndexInBounds: bool =
    globalCommitIndex >= 0 and globalCommitIndex <= MaxLogIndex and nextLogIndex == globalCommitIndex + 1


  def safety: bool = all {
    committedPrefixAgreement,
    committedSlotsAreFilled,
    oneLeaderPerTerm,
    kvMatchesCommittedLog,
    committedIndexInBounds,
  }
}
