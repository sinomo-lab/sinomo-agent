/// Deterministic traces for shard metadata write sets and automatic takeover.
/// Design traces keep the full invariant suite; each counterexample enables
/// one broken variant and reaches the violation its invariant detects.

/// M1 commits value 1 on the whole write set, prepares value 2 that only M2
/// acknowledges and value 3 that nobody acknowledges, then stops. M2 takes
/// over without M1 and completes value 2. M1 returns, drops its
/// unacknowledged tail and rejoins the write set as a follower.
module dfs_write_set_abrupt_takeover_scenario {
  import common.* from "../../lib/common"
  import dfs_metadata_write_set_takeover as W from "../../models/dfs/metadata_write_set_takeover"
  import dfs_metadata_write_set_takeover_safety as C from "../../checks/dfs/metadata_write_set_takeover_safety"
  var phase: int
  action init = all { W::initWith(W::Design), phase' = 0 }
  action step = any {
    all { phase == 0, W::prepare(W::M1, 1), phase' = 1 },
    all { phase == 1, W::accept(W::M2, W::M1), phase' = 2 },
    all { phase == 2, W::accept(W::M3, W::M1), phase' = 3 },
    all { phase == 3, W::commit(W::M1, 1), phase' = 4 },
    all { phase == 4, W::prepare(W::M1, 2), phase' = 5 },
    all { phase == 5, W::accept(W::M2, W::M1), phase' = 6 },
    all { phase == 6, W::prepare(W::M1, 3), phase' = 7 },
    all { phase == 7, W::confirmQuorum(W::M2), phase' = 8 },
    all { phase == 8, W::promise(W::M2), phase' = 9 },
    all { phase == 9, W::submitSwap(W::M2), phase' = 10 },
    all { phase == 10, W::resolveAnySwap(true), phase' = 11 },
    all { phase == 11, W::learnAnySwap, phase' = 12 },
    all { phase == 12, W::activate(W::M2), phase' = 13 },
    all { phase == 13, W::observe(W::M3), phase' = 14 },
    all { phase == 14, W::accept(W::M3, W::M2), phase' = 15 },
    all { phase == 15, W::commit(W::M2, 2), phase' = 16 },
    all { phase == 16, W::observe(W::M1), phase' = 17 },
    all { phase == 17, W::accept(W::M1, W::M2), phase' = 18 },
    all { phase == 18, W::submitAdd(W::M2, W::M1), phase' = 19 },
    all { phase == 19, W::resolveAnyAdd(true), phase' = 20 },
    all { phase == 20, W::observe(W::M2), phase' = 21 },
    all { phase == 21, W::s' = W::s, phase' = phase },
  }
  def safety: bool = C::safety
  def terminalWitness: bool = all { phase == 21, W::s.kv.primary == W::M2, W::s.kv.writeSet == Set(W::M1, W::M2, W::M3), W::s.log.get(W::M1) == W::s.log.get(W::M2), C::witnessSuccessorCompletesPreparedChange, C::witnessFollowerDropsTail, C::witnessOldPrimaryRejoins }
}

/// M1 commits value 1, then prepares value 2, which only M3 acknowledges. M2
/// takes over at epoch start 1 without M1. M3 and then M1 align on their own
/// view of the takeover record: each drops value 2, which nobody committed,
/// before accepting M2's journal. M2 commits value 3 with M3, and M1 rejoins.
module dfs_write_set_align_tail_scenario {
  import common.* from "../../lib/common"
  import dfs_metadata_write_set_takeover as W from "../../models/dfs/metadata_write_set_takeover"
  import dfs_metadata_write_set_takeover_safety as C from "../../checks/dfs/metadata_write_set_takeover_safety"
  var phase: int
  action init = all { W::initWith(W::Design), phase' = 0 }
  action step = any {
    all { phase == 0, W::prepare(W::M1, 1), phase' = 1 },
    all { phase == 1, W::accept(W::M2, W::M1), phase' = 2 },
    all { phase == 2, W::accept(W::M3, W::M1), phase' = 3 },
    all { phase == 3, W::commit(W::M1, 1), phase' = 4 },
    all { phase == 4, W::prepare(W::M1, 2), phase' = 5 },
    all { phase == 5, W::accept(W::M3, W::M1), phase' = 6 },
    all { phase == 6, W::confirmQuorum(W::M2), phase' = 7 },
    all { phase == 7, W::promise(W::M2), phase' = 8 },
    all { phase == 8, W::submitSwap(W::M2), phase' = 9 },
    all { phase == 9, W::resolveAnySwap(true), phase' = 10 },
    all { phase == 10, W::learnAnySwap, phase' = 11 },
    all { phase == 11, W::activate(W::M2), phase' = 12 },
    all { phase == 12, W::observe(W::M3), phase' = 13 },
    all { phase == 13, W::align(W::M3), phase' = 14 },
    all { phase == 14, W::accept(W::M3, W::M2), phase' = 15 },
    all { phase == 15, W::prepare(W::M2, 3), phase' = 16 },
    all { phase == 16, W::accept(W::M3, W::M2), phase' = 17 },
    all { phase == 17, W::commit(W::M2, 2), phase' = 18 },
    all { phase == 18, W::observe(W::M1), phase' = 19 },
    all { phase == 19, W::align(W::M1), phase' = 20 },
    all { phase == 20, W::accept(W::M1, W::M2), phase' = 21 },
    all { phase == 21, W::submitAdd(W::M2, W::M1), phase' = 22 },
    all { phase == 22, W::resolveAnyAdd(true), phase' = 23 },
    all { phase == 23, W::observe(W::M2), phase' = 24 },
    all { phase == 24, W::s' = W::s, phase' = phase },
  }
  def safety: bool = C::safety
  def terminalWitness: bool = all { phase == 24, W::s.kv.primary == W::M2, W::s.kv.writeSet == Set(W::M1, W::M2, W::M3), W::s.log.get(W::M1) == W::s.log.get(W::M2), W::s.log.get(W::M3) == W::s.log.get(W::M2), W::s.commits.exists(c => c.epoch == 2 and c.entries.length() == 2), C::witnessFollowerDropsTail }
}

/// Two candidates promise against the same record. M3 wins the first swap.
/// M2 must learn that its original swap was rejected and confirm the new
/// record before it can take over the next epoch.
module dfs_write_set_successive_candidates_scenario {
  import common.* from "../../lib/common"
  import dfs_metadata_write_set_takeover as W from "../../models/dfs/metadata_write_set_takeover"
  import dfs_metadata_write_set_takeover_safety as C from "../../checks/dfs/metadata_write_set_takeover_safety"
  var phase: int
  action init = all { W::initWithBounds(W::Design, 1, 3), phase' = 0 }
  action step = any {
    all { phase == 0, W::confirmQuorum(W::M2), phase' = 1 },
    all { phase == 1, W::promise(W::M2), phase' = 2 },
    all { phase == 2, W::confirmQuorum(W::M3), phase' = 3 },
    all { phase == 3, W::promise(W::M3), phase' = 4 },
    all { phase == 4, W::submitSwap(W::M3), phase' = 5 },
    all { phase == 5, W::resolveAnySwap(true), phase' = 6 },
    all { phase == 6, W::learnAnySwap, phase' = 7 },
    all { phase == 7, W::activate(W::M3), phase' = 8 },
    all { phase == 8, W::submitSwap(W::M2), phase' = 9 },
    all { phase == 9, W::resolveAnySwap(true), phase' = 10 },
    all { phase == 10, W::learnAnySwap, phase' = 11 },
    all { phase == 11, W::confirmQuorum(W::M2), phase' = 12 },
    all { phase == 12, W::promise(W::M2), phase' = 13 },
    all { phase == 13, W::submitSwap(W::M2), phase' = 14 },
    all { phase == 14, W::resolveAnySwap(true), phase' = 15 },
    all { phase == 15, W::learnAnySwap, phase' = 16 },
    all { phase == 16, W::activate(W::M2), phase' = 17 },
    all { phase == 17, W::s' = W::s, phase' = phase },
  }
  def safety: bool = C::safety
  def terminalWitness: bool = all { phase == 17, W::s.kv.epoch == 3, W::s.kv.primary == W::M2, W::s.kv.writeSet == Set(W::M2), W::s.promise.get(W::M2) == None }
}

/// M3 reads the record and promises, but M1 removes M3 from the write set and
/// commits at the W floor without it. M3's swap is rejected; M3 releases its
/// promise, catches up and is added back.
module dfs_write_set_shrink_race_scenario {
  import common.* from "../../lib/common"
  import dfs_metadata_write_set_takeover as W from "../../models/dfs/metadata_write_set_takeover"
  import dfs_metadata_write_set_takeover_safety as C from "../../checks/dfs/metadata_write_set_takeover_safety"
  var phase: int
  action init = all { W::initWith(W::Design), phase' = 0 }
  action step = any {
    all { phase == 0, W::confirmQuorum(W::M3), phase' = 1 },
    all { phase == 1, W::promise(W::M3), phase' = 2 },
    all { phase == 2, W::shrink(W::M1, W::M3), phase' = 3 },
    all { phase == 3, W::prepare(W::M1, 1), phase' = 4 },
    all { phase == 4, W::accept(W::M2, W::M1), phase' = 5 },
    all { phase == 5, W::commit(W::M1, 1), phase' = 6 },
    all { phase == 6, W::submitSwap(W::M3), phase' = 7 },
    all { phase == 7, W::resolveAnySwap(true), phase' = 8 },
    all { phase == 8, W::learnAnySwap, phase' = 9 },
    all { phase == 9, W::accept(W::M3, W::M1), phase' = 10 },
    all { phase == 10, W::submitAdd(W::M1, W::M3), phase' = 11 },
    all { phase == 11, W::resolveAnyAdd(true), phase' = 12 },
    all { phase == 12, W::observe(W::M1), phase' = 13 },
    all { phase == 13, W::s' = W::s, phase' = phase },
  }
  def safety: bool = C::safety
  def terminalWitness: bool = all { phase == 13, W::s.kv.primary == W::M1, W::s.kv.epoch == 1, W::s.kv.writeSet == Set(W::M1, W::M2, W::M3), W::s.promise.get(W::M3) == None, W::s.fence.get(W::M3) == 1, W::s.log.get(W::M3) == W::s.log.get(W::M1), C::witnessCommitAtFloor }
}

/// M1 removes M3, recruits M4 and submits M4 for the write set, then
/// restarts before the add applies. The durable pending add keeps M4's
/// acknowledgement required, so when the add applies M4 holds every
/// committed change and can take over after M1 is lost.
module dfs_write_set_pending_add_crash_scenario {
  import common.* from "../../lib/common"
  import dfs_metadata_write_set_takeover as W from "../../models/dfs/metadata_write_set_takeover"
  import dfs_metadata_write_set_takeover_safety as C from "../../checks/dfs/metadata_write_set_takeover_safety"
  var phase: int
  action init = all { W::initWith(W::Design), phase' = 0 }
  action step = any {
    all { phase == 0, W::shrink(W::M1, W::M3), phase' = 1 },
    all { phase == 1, W::recruit(W::M1, W::M4), phase' = 2 },
    all { phase == 2, W::observe(W::M4), phase' = 3 },
    all { phase == 3, W::accept(W::M4, W::M1), phase' = 4 },
    all { phase == 4, W::submitAdd(W::M1, W::M4), phase' = 5 },
    all { phase == 5, W::crash(W::M1), phase' = 6 },
    all { phase == 6, W::activate(W::M1), phase' = 7 },
    all { phase == 7, W::prepare(W::M1, 1), phase' = 8 },
    all { phase == 8, W::accept(W::M2, W::M1), phase' = 9 },
    all { phase == 9, W::accept(W::M4, W::M1), phase' = 10 },
    all { phase == 10, W::commit(W::M1, 1), phase' = 11 },
    all { phase == 11, W::resolveAnyAdd(true), phase' = 12 },
    all { phase == 12, W::confirmQuorum(W::M4), phase' = 13 },
    all { phase == 13, W::promise(W::M4), phase' = 14 },
    all { phase == 14, W::submitSwap(W::M4), phase' = 15 },
    all { phase == 15, W::resolveAnySwap(true), phase' = 16 },
    all { phase == 16, W::learnAnySwap, phase' = 17 },
    all { phase == 17, W::activate(W::M4), phase' = 18 },
    all { phase == 18, W::observe(W::M2), phase' = 19 },
    all { phase == 19, W::accept(W::M2, W::M4), phase' = 20 },
    all { phase == 20, W::prepare(W::M4, 2), phase' = 21 },
    all { phase == 21, W::accept(W::M2, W::M4), phase' = 22 },
    all { phase == 22, W::commit(W::M4, 2), phase' = 23 },
    all { phase == 23, W::s' = W::s, phase' = phase },
  }
  def safety: bool = C::safety
  def terminalWitness: bool = all { phase == 23, W::s.kv.primary == W::M4, W::s.kv.writeSet == Set(W::M2, W::M4), C::witnessSuccessorCommits, W::s.commits.exists(c => c.epoch == 2 and c.entries.length() == 2) }
}

/// The W floor equals N (3). M3 stops answering: removing it would take the
/// write set below the floor, so it stays and writes wait. M1 recruits M4 as
/// a replacement beyond N, which every configured member being in the write
/// set admits; M4 catches up and joins, only then is M3 removed, and M1
/// commits at the floor with M4.
module dfs_write_set_replacement_recruit_scenario {
  import common.* from "../../lib/common"
  import dfs_metadata_write_set_takeover as W from "../../models/dfs/metadata_write_set_takeover"
  import dfs_metadata_write_set_takeover_safety as C from "../../checks/dfs/metadata_write_set_takeover_safety"
  var phase: int
  action init = all { W::initWithBounds(W::Design, 3, 3), phase' = 0 }
  action step = any {
    all { phase == 0, W::s.kv.writeSet.size() - 1 < W::s.wFloor, W::s' = W::s, phase' = 1 },
    all { phase == 1, W::recruit(W::M1, W::M4), phase' = 2 },
    all { phase == 2, W::observe(W::M4), phase' = 3 },
    all { phase == 3, W::accept(W::M4, W::M1), phase' = 4 },
    all { phase == 4, W::submitAdd(W::M1, W::M4), phase' = 5 },
    all { phase == 5, W::resolveAnyAdd(true), phase' = 6 },
    all { phase == 6, W::observe(W::M1), phase' = 7 },
    all { phase == 7, W::shrink(W::M1, W::M3), phase' = 8 },
    all { phase == 8, W::prepare(W::M1, 1), phase' = 9 },
    all { phase == 9, W::observe(W::M2), phase' = 10 },
    all { phase == 10, W::accept(W::M2, W::M1), phase' = 11 },
    all { phase == 11, W::observe(W::M4), phase' = 12 },
    all { phase == 12, W::accept(W::M4, W::M1), phase' = 13 },
    all { phase == 13, W::commit(W::M1, 1), phase' = 14 },
    all { phase == 14, W::s' = W::s, phase' = phase },
  }
  def safety: bool = C::safety
  def terminalWitness: bool = all { phase == 14, W::s.kv.writeSet == Set(W::M1, W::M2, W::M4), W::s.kv.members == Set(W::M1, W::M2, W::M3, W::M4), C::witnessReplacementRecruitJoins }
}

/// Counterexample: if the pending add were lost on restart, M1 would commit
/// without M4 and the add would then put M4 in the write set without the
/// committed change.
module dfs_write_set_volatile_pending_add_counterexample {
  import common.* from "../../lib/common"
  import dfs_metadata_write_set_takeover as W from "../../models/dfs/metadata_write_set_takeover"
  import dfs_metadata_write_set_takeover_safety as C from "../../checks/dfs/metadata_write_set_takeover_safety"
  var phase: int
  action init = all { W::initWith(W::VolatilePendingAdd), phase' = 0 }
  action step = any {
    all { phase == 0, W::shrink(W::M1, W::M3), phase' = 1 },
    all { phase == 1, W::recruit(W::M1, W::M4), phase' = 2 },
    all { phase == 2, W::observe(W::M4), phase' = 3 },
    all { phase == 3, W::accept(W::M4, W::M1), phase' = 4 },
    all { phase == 4, W::submitAdd(W::M1, W::M4), phase' = 5 },
    all { phase == 5, W::crash(W::M1), phase' = 6 },
    all { phase == 6, W::activate(W::M1), phase' = 7 },
    all { phase == 7, W::prepare(W::M1, 1), phase' = 8 },
    all { phase == 8, W::accept(W::M2, W::M1), phase' = 9 },
    all { phase == 9, W::commit(W::M1, 1), phase' = 10 },
    all { phase == 10, W::resolveAnyAdd(true), phase' = 11 },
    all { phase == 11, W::s' = W::s, phase' = phase },
  }
  def safety: bool = C::recordIsWellFormed
  def terminalWitness: bool = all { phase == 11, not(C::writeSetHoldsEveryCommittedEntry) }
}

/// Counterexample: a swap that compared only the epoch would let M3, removed
/// from the write set after it read the record, take over without the
/// change M1 committed in its absence.
module dfs_write_set_epoch_only_compare_counterexample {
  import common.* from "../../lib/common"
  import dfs_metadata_write_set_takeover as W from "../../models/dfs/metadata_write_set_takeover"
  import dfs_metadata_write_set_takeover_safety as C from "../../checks/dfs/metadata_write_set_takeover_safety"
  var phase: int
  action init = all { W::initWith(W::EpochOnlyCompare), phase' = 0 }
  action step = any {
    all { phase == 0, W::confirmQuorum(W::M3), phase' = 1 },
    all { phase == 1, W::promise(W::M3), phase' = 2 },
    all { phase == 2, W::shrink(W::M1, W::M3), phase' = 3 },
    all { phase == 3, W::prepare(W::M1, 1), phase' = 4 },
    all { phase == 4, W::accept(W::M2, W::M1), phase' = 5 },
    all { phase == 5, W::commit(W::M1, 1), phase' = 6 },
    all { phase == 6, W::submitSwap(W::M3), phase' = 7 },
    all { phase == 7, W::resolveAnySwap(true), phase' = 8 },
    all { phase == 8, W::s' = W::s, phase' = phase },
  }
  def safety: bool = C::recordIsWellFormed
  def terminalWitness: bool = all { phase == 8, not(C::noAcknowledgedChangeIsLost) }
}

/// Counterexample: a shard member outside the write set lacks changes
/// committed without it, so letting it take over loses them.
module dfs_write_set_out_of_sync_candidate_counterexample {
  import common.* from "../../lib/common"
  import dfs_metadata_write_set_takeover as W from "../../models/dfs/metadata_write_set_takeover"
  import dfs_metadata_write_set_takeover_safety as C from "../../checks/dfs/metadata_write_set_takeover_safety"
  var phase: int
  action init = all { W::initWith(W::OutOfSyncCandidate), phase' = 0 }
  action step = any {
    all { phase == 0, W::shrink(W::M1, W::M3), phase' = 1 },
    all { phase == 1, W::prepare(W::M1, 1), phase' = 2 },
    all { phase == 2, W::accept(W::M2, W::M1), phase' = 3 },
    all { phase == 3, W::commit(W::M1, 1), phase' = 4 },
    all { phase == 4, W::confirmQuorum(W::M3), phase' = 5 },
    all { phase == 5, W::promise(W::M3), phase' = 6 },
    all { phase == 6, W::submitSwap(W::M3), phase' = 7 },
    all { phase == 7, W::resolveAnySwap(true), phase' = 8 },
    all { phase == 8, W::s' = W::s, phase' = phase },
  }
  def safety: bool = C::recordIsWellFormed
  def terminalWitness: bool = all { phase == 8, not(C::noAcknowledgedChangeIsLost) }
}

/// Counterexample: adding a member before it acknowledges the primary's
/// whole journal puts a write-set member behind a committed change.
module dfs_write_set_add_without_catch_up_counterexample {
  import common.* from "../../lib/common"
  import dfs_metadata_write_set_takeover as W from "../../models/dfs/metadata_write_set_takeover"
  import dfs_metadata_write_set_takeover_safety as C from "../../checks/dfs/metadata_write_set_takeover_safety"
  var phase: int
  action init = all { W::initWith(W::AddWithoutCatchUp), phase' = 0 }
  action step = any {
    all { phase == 0, W::shrink(W::M1, W::M3), phase' = 1 },
    all { phase == 1, W::prepare(W::M1, 1), phase' = 2 },
    all { phase == 2, W::accept(W::M2, W::M1), phase' = 3 },
    all { phase == 3, W::commit(W::M1, 1), phase' = 4 },
    all { phase == 4, W::submitAdd(W::M1, W::M3), phase' = 5 },
    all { phase == 5, W::resolveAnyAdd(true), phase' = 6 },
    all { phase == 6, W::s' = W::s, phase' = phase },
  }
  def safety: bool = C::recordIsWellFormed
  def terminalWitness: bool = all { phase == 6, not(C::writeSetHoldsEveryCommittedEntry) }
}

/// Counterexample: once an add has applied but before the primary observes
/// it, a commit that ignored the submitted member would leave that
/// write-set member without the change.
module dfs_write_set_ignored_pending_add_counterexample {
  import common.* from "../../lib/common"
  import dfs_metadata_write_set_takeover as W from "../../models/dfs/metadata_write_set_takeover"
  import dfs_metadata_write_set_takeover_safety as C from "../../checks/dfs/metadata_write_set_takeover_safety"
  var phase: int
  action init = all { W::initWith(W::IgnorePendingAdd), phase' = 0 }
  action step = any {
    all { phase == 0, W::shrink(W::M1, W::M3), phase' = 1 },
    all { phase == 1, W::observe(W::M3), phase' = 2 },
    all { phase == 2, W::accept(W::M3, W::M1), phase' = 3 },
    all { phase == 3, W::submitAdd(W::M1, W::M3), phase' = 4 },
    all { phase == 4, W::resolveAnyAdd(true), phase' = 5 },
    all { phase == 5, W::prepare(W::M1, 1), phase' = 6 },
    all { phase == 6, W::accept(W::M2, W::M1), phase' = 7 },
    all { phase == 7, W::commit(W::M1, 1), phase' = 8 },
    all { phase == 8, W::s' = W::s, phase' = phase },
  }
  def safety: bool = C::recordIsWellFormed
  def terminalWitness: bool = all { phase == 8, not(C::writeSetHoldsEveryCommittedEntry) }
}

/// Counterexample: without the promise, the successor keeps accepting the
/// old primary's records after its swap applies, so the old primary can
/// still commit past epoch start. Nothing is lost, because the successor
/// holds those records, but epoch start no longer bounds the old history.
module dfs_write_set_no_promise_counterexample {
  import common.* from "../../lib/common"
  import dfs_metadata_write_set_takeover as W from "../../models/dfs/metadata_write_set_takeover"
  import dfs_metadata_write_set_takeover_safety as C from "../../checks/dfs/metadata_write_set_takeover_safety"
  var phase: int
  action init = all { W::initWith(W::NoPromise), phase' = 0 }
  action step = any {
    all { phase == 0, W::prepare(W::M1, 1), phase' = 1 },
    all { phase == 1, W::accept(W::M2, W::M1), phase' = 2 },
    all { phase == 2, W::accept(W::M3, W::M1), phase' = 3 },
    all { phase == 3, W::commit(W::M1, 1), phase' = 4 },
    all { phase == 4, W::confirmQuorum(W::M2), phase' = 5 },
    all { phase == 5, W::promise(W::M2), phase' = 6 },
    all { phase == 6, W::submitSwap(W::M2), phase' = 7 },
    all { phase == 7, W::resolveAnySwap(true), phase' = 8 },
    all { phase == 8, W::prepare(W::M1, 2), phase' = 9 },
    all { phase == 9, W::accept(W::M2, W::M1), phase' = 10 },
    all { phase == 10, W::accept(W::M3, W::M1), phase' = 11 },
    all { phase == 11, W::commit(W::M1, 2), phase' = 12 },
    all { phase == 12, W::s' = W::s, phase' = phase },
  }
  def safety: bool = C::recordIsWellFormed
  def terminalWitness: bool = all { phase == 12, not(C::oldPrimaryCommitsOnlyWhatSuccessorHeld), C::writeSetHoldsEveryCommittedEntry, C::noAcknowledgedChangeIsLost, C::committedHistoriesNeverDiverge }
}

/// Counterexample: a follower that aligned to epoch start again after it had
/// acknowledged the new primary's change past epoch start would drop a change
/// the new primary already committed with that acknowledgement.
module dfs_write_set_align_below_current_ack_counterexample {
  import common.* from "../../lib/common"
  import dfs_metadata_write_set_takeover as W from "../../models/dfs/metadata_write_set_takeover"
  import dfs_metadata_write_set_takeover_safety as C from "../../checks/dfs/metadata_write_set_takeover_safety"
  var phase: int
  action init = all { W::initWith(W::AlignBelowCurrentAck), phase' = 0 }
  action step = any {
    all { phase == 0, W::prepare(W::M1, 1), phase' = 1 },
    all { phase == 1, W::accept(W::M2, W::M1), phase' = 2 },
    all { phase == 2, W::accept(W::M3, W::M1), phase' = 3 },
    all { phase == 3, W::commit(W::M1, 1), phase' = 4 },
    all { phase == 4, W::confirmQuorum(W::M2), phase' = 5 },
    all { phase == 5, W::promise(W::M2), phase' = 6 },
    all { phase == 6, W::submitSwap(W::M2), phase' = 7 },
    all { phase == 7, W::resolveAnySwap(true), phase' = 8 },
    all { phase == 8, W::learnAnySwap, phase' = 9 },
    all { phase == 9, W::activate(W::M2), phase' = 10 },
    all { phase == 10, W::observe(W::M3), phase' = 11 },
    all { phase == 11, W::accept(W::M3, W::M2), phase' = 12 },
    all { phase == 12, W::prepare(W::M2, 2), phase' = 13 },
    all { phase == 13, W::accept(W::M3, W::M2), phase' = 14 },
    all { phase == 14, W::commit(W::M2, 2), phase' = 15 },
    all { phase == 15, W::align(W::M3), phase' = 16 },
    all { phase == 16, W::s' = W::s, phase' = phase },
  }
  def safety: bool = C::recordIsWellFormed
  def terminalWitness: bool = all { phase == 16, not(C::writeSetHoldsEveryCommittedEntry) }
}
