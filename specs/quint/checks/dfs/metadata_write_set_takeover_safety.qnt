/// Target hardening: invariants of shard metadata write sets and automatic
/// takeover. Bounded simulation; not a proof of the Rust implementation.
module dfs_metadata_write_set_takeover_safety {
  import dfs_metadata_write_set_takeover.* from "../../models/dfs/metadata_write_set_takeover"

  /// Any two reported successes describe the same history.
  def committedHistoriesNeverDiverge: bool =
    s.commits.forall(a => s.commits.forall(b => isPrefix(a.entries, b.entries) or isPrefix(b.entries, a.entries)))

  /// Every write-set member holds every committed entry, which is what makes
  /// any of them a valid successor.
  def writeSetHoldsEveryCommittedEntry: bool =
    s.commits.forall(c => s.kv.writeSet.forall(m => isPrefix(c.entries, s.log.get(m))))

  /// After a takeover swap, an earlier primary's success, even one reported
  /// late from delayed acknowledgements, covers only entries the successor
  /// held when the swap applied, so epoch start is exact.
  def oldPrimaryCommitsOnlyWhatSuccessorHeld: bool =
    s.commits.forall(c => s.swapLogs.keys().forall(e => c.epoch < e implies isPrefix(c.entries, s.swapLogs.get(e))))

  /// The record's primary holds every acknowledged change across takeovers.
  def noAcknowledgedChangeIsLost: bool =
    s.commits.forall(c => isPrefix(c.entries, s.log.get(s.kv.primary)))

  def shrinkNeverGoesBelowFloor: bool =
    s.last == Shrunk implies s.kv.writeSet.size() >= s.wFloor

  def commitsMeetTheFloor: bool =
    s.commits.forall(c => c.writeSetSize >= s.wFloor)

  def recordIsWellFormed: bool = all {
    s.kv.writeSet.contains(s.kv.primary),
    s.kv.writeSet.subseteq(s.kv.members),
  }

  def onePrimaryServesEachEpoch: bool =
    Members.forall(a => Members.forall(b => all {
      a != b, s.active.get(a) > 0, s.active.get(a) == s.active.get(b),
    } implies false))

  def safety: bool = all {
    committedHistoriesNeverDiverge,
    writeSetHoldsEveryCommittedEntry,
    oldPrimaryCommitsOnlyWhatSuccessorHeld,
    noAcknowledgedChangeIsLost,
    shrinkNeverGoesBelowFloor,
    commitsMeetTheFloor,
    recordIsWellFormed,
    onePrimaryServesEachEpoch,
  }

  def witnessSuccessorCommits: bool = s.commits.exists(c => c.epoch >= 2)

  /// A change prepared under one epoch and acknowledged to its caller only
  /// under a later one: the successor completed it.
  def witnessSuccessorCompletesPreparedChange: bool =
    s.commits.exists(c => c.entries.indices().exists(i => all {
      c.entries[i].epoch < c.epoch,
      not(s.commits.exists(o => o.epoch == c.entries[i].epoch and o.entries.length() > i)),
    }))

  def witnessSecondTakeover: bool = s.kv.epoch == MaxEpoch and s.commits.exists(c => c.epoch == MaxEpoch)
  def witnessCommitAtFloor: bool = s.commits.exists(c => c.writeSetSize == s.wFloor)
  def witnessPromiseReleased: bool = s.last == PromiseReleased
  def witnessRecruitJoinsWriteSet: bool = s.kv.writeSet.contains(M4)
  /// With the floor equal to N, a recruit beyond N replaced a member held at
  /// the floor, joined the write set, and the shard committed at the floor
  /// with it.
  def witnessReplacementRecruitJoins: bool = all {
    s.wFloor == s.nCopies,
    s.kv.writeSet.contains(M4),
    s.commits.exists(c => c.writeSetSize == s.wFloor and c.entries.length() >= 1),
  }
  def witnessOldPrimaryRejoins: bool = s.kv.primary != M1 and s.kv.writeSet.contains(M1)
  def witnessDeposedPrimaryLateCommit: bool = s.commits.exists(c => c.deposed)
  def witnessFollowerDropsTail: bool = s.droppedTail
  /// A follower dropped its unacknowledged tail on its own view of a takeover
  /// record, before it accepted the successor's journal.
  def witnessFollowerAligns: bool = s.last == Aligned
}
