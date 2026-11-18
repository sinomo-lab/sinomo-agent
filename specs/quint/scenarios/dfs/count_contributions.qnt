/// Deterministic traces of count contributions while a target shard
/// converts. The design trace keeps the full invariant suite; each
/// counterexample enables one broken variant and reaches the violation its
/// invariant detects.

/// T converts while S1 renames alias A1 to S2. The rename started before T
/// recorded Converting, so the first round is held by it; its next step finds
/// T closed and parks. The next two rounds agree, T activates with its own
/// alias A2 plus A1, and the parked rename then completes. Later links and
/// unlinks keep the count exact.
module dfs_count_contributions_conversion_scenario {
  import common.* from "../../lib/common"
  import dfs_count_contributions as K from "../../models/dfs/count_contributions"
  import dfs_count_contributions_safety as C from "../../checks/dfs/count_contributions_safety"
  var phase: int
  action init = all { K::initWith(K::Design, true), phase' = 0 }
  action step = any {
    all { phase == 0, K::startMove(K::A1, K::S2), phase' = 1 },
    all { phase == 1, K::beginConversion, phase' = 2 },
    all { phase == 2, K::readSource(K::S1), phase' = 3 },
    all { phase == 3, K::readSource(K::S2), phase' = 4 },
    all { phase == 4, K::completeRound, phase' = 5 },
    all { phase == 5, K::park(K::A1), phase' = 6 },
    all { phase == 6, K::readSource(K::S1), phase' = 7 },
    all { phase == 7, K::readSource(K::S2), phase' = 8 },
    all { phase == 8, K::completeRound, phase' = 9 },
    all { phase == 9, K::readSource(K::S2), phase' = 10 },
    all { phase == 10, K::readSource(K::S1), phase' = 11 },
    all { phase == 11, K::completeRound, phase' = 12 },
    all { phase == 12, K::activate, phase' = 13 },
    all { phase == 13, K::moveStep(K::A1), phase' = 14 },
    all { phase == 14, K::unlink(K::A2), phase' = 15 },
    all { phase == 15, K::link(K::A2, K::S1), phase' = 16 },
    all { phase == 16, K::s' = K::s, phase' = phase },
  }
  def safety: bool = C::safety
  def terminalWitness: bool = all {
    phase == 16,
    K::s.count == 2,
    K::s.loc.get(K::A1) == K::S2,
    C::witnessRoundHeldByRunningJob,
    C::witnessActivatedWithParkedJob,
  }
}

/// Counterexample: if a rename moved an alias row while T is closed, T could
/// read A1 in S1, the rename could move it, and T could read it again in S2,
/// twice in a row. The two rounds agree, no rename is running when either
/// source is read, and T activates counting A1 twice.
module dfs_count_contributions_moves_ignore_closed_target_counterexample {
  import common.* from "../../lib/common"
  import dfs_count_contributions as K from "../../models/dfs/count_contributions"
  import dfs_count_contributions_safety as C from "../../checks/dfs/count_contributions_safety"
  var phase: int
  action init = all { K::initWith(K::MovesIgnoreClosedTarget, true), phase' = 0 }
  action step = any {
    all { phase == 0, K::beginConversion, phase' = 1 },
    all { phase == 1, K::readSource(K::S1), phase' = 2 },
    all { phase == 2, K::startMove(K::A1, K::S2), phase' = 3 },
    all { phase == 3, K::moveStep(K::A1), phase' = 4 },
    all { phase == 4, K::readSource(K::S2), phase' = 5 },
    all { phase == 5, K::completeRound, phase' = 6 },
    all { phase == 6, K::startMove(K::A1, K::S1), phase' = 7 },
    all { phase == 7, K::moveStep(K::A1), phase' = 8 },
    all { phase == 8, K::readSource(K::S1), phase' = 9 },
    all { phase == 9, K::startMove(K::A1, K::S2), phase' = 10 },
    all { phase == 10, K::moveStep(K::A1), phase' = 11 },
    all { phase == 11, K::readSource(K::S2), phase' = 12 },
    all { phase == 12, K::completeRound, phase' = 13 },
    all { phase == 13, K::activate, phase' = 14 },
    all { phase == 14, K::s' = K::s, phase' = phase },
  }
  def safety: bool = C::wellFormed
  def terminalWitness: bool = all {
    phase == 14,
    K::s.count == 3,
    not(C::countsExact),
  }
}

/// Counterexample: a rename that moved an alias row while T is closed
/// changes what the sources contribute after T recorded Converting.
module dfs_count_contributions_move_while_closed_counterexample {
  import common.* from "../../lib/common"
  import dfs_count_contributions as K from "../../models/dfs/count_contributions"
  import dfs_count_contributions_safety as C from "../../checks/dfs/count_contributions_safety"
  var phase: int
  action init = all { K::initWith(K::MovesIgnoreClosedTarget, true), phase' = 0 }
  action step = any {
    all { phase == 0, K::beginConversion, phase' = 1 },
    all { phase == 1, K::startMove(K::A1, K::S2), phase' = 2 },
    all { phase == 2, K::moveStep(K::A1), phase' = 3 },
    all { phase == 3, K::s' = K::s, phase' = phase },
  }
  def safety: bool = C::countsExact
  def terminalWitness: bool = all { phase == 3, not(C::contributionsFrozenWhileConverting) }
}

/// Counterexample: an unlink that did not wait for T's canonical object
/// changes a source's contribution after T accepted two agreeing rounds.
module dfs_count_contributions_unlink_while_closed_accepted_counterexample {
  import common.* from "../../lib/common"
  import dfs_count_contributions as K from "../../models/dfs/count_contributions"
  import dfs_count_contributions_safety as C from "../../checks/dfs/count_contributions_safety"
  var phase: int
  action init = all { K::initWith(K::CountChangesIgnoreClosedTarget, true), phase' = 0 }
  action step = any {
    all { phase == 0, K::beginConversion, phase' = 1 },
    all { phase == 1, K::readSource(K::S1), phase' = 2 },
    all { phase == 2, K::readSource(K::S2), phase' = 3 },
    all { phase == 3, K::completeRound, phase' = 4 },
    all { phase == 4, K::readSource(K::S1), phase' = 5 },
    all { phase == 5, K::readSource(K::S2), phase' = 6 },
    all { phase == 6, K::completeRound, phase' = 7 },
    all { phase == 7, K::unlink(K::A1), phase' = 8 },
    all { phase == 8, K::s' = K::s, phase' = phase },
  }
  def safety: bool = C::countsExact
  def terminalWitness: bool = all { phase == 8, not(C::acceptedContributionsAreExact) }
}

/// Counterexample: the same unlink before T activates leaves T's count one
/// above the number of names.
module dfs_count_contributions_unlink_while_closed_count_counterexample {
  import common.* from "../../lib/common"
  import dfs_count_contributions as K from "../../models/dfs/count_contributions"
  import dfs_count_contributions_safety as C from "../../checks/dfs/count_contributions_safety"
  var phase: int
  action init = all { K::initWith(K::CountChangesIgnoreClosedTarget, true), phase' = 0 }
  action step = any {
    all { phase == 0, K::beginConversion, phase' = 1 },
    all { phase == 1, K::readSource(K::S1), phase' = 2 },
    all { phase == 2, K::readSource(K::S2), phase' = 3 },
    all { phase == 3, K::completeRound, phase' = 4 },
    all { phase == 4, K::readSource(K::S1), phase' = 5 },
    all { phase == 5, K::readSource(K::S2), phase' = 6 },
    all { phase == 6, K::completeRound, phase' = 7 },
    all { phase == 7, K::unlink(K::A1), phase' = 8 },
    all { phase == 8, K::activate, phase' = 9 },
    all { phase == 9, K::s' = K::s, phase' = phase },
  }
  def safety: bool = C::wellFormed
  def terminalWitness: bool = all { phase == 9, not(C::countsExact) }
}
