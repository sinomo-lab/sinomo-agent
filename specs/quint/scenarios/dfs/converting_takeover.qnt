/// Deterministic traces of the converting takeover. Design traces keep the
/// full invariant suite; each counterexample enables one broken variant and
/// reaches the violation its invariant detects.

/// The primary converts old storage from its own copy: its attempt is
/// sealed, staged on the whole write set and activated without a serving
/// change.
module dfs_converting_takeover_primary_conversion_scenario {
  import common.* from "../../lib/common"
  import dfs_converting_takeover as T from "../../models/dfs/converting_takeover"
  import dfs_converting_takeover_safety as C from "../../checks/dfs/converting_takeover_safety"
  var phase: int
  action init = all { T::initWith(T::Design), phase' = 0 }
  action step = any {
    all { phase == 0, T::beginConversion, phase' = 1 },
    all { phase == 1, T::seal, phase' = 2 },
    all { phase == 2, T::stageAny(T::M1), phase' = 3 },
    all { phase == 3, T::stageAny(T::M2), phase' = 4 },
    all { phase == 4, T::stageAny(T::M3), phase' = 5 },
    all { phase == 5, T::activateConversion, phase' = 6 },
    all { phase == 6, T::s' = T::s, phase' = phase },
  }
  def safety: bool = C::safety
  def terminalWitness: bool = all { phase == 6, T::s.serving == T::initialServing, C::witnessPrimaryConversionActivates }
}

/// M1 is judged lost. M2 converts from its own copy in its place; the image
/// is staged on the write set without M1, and its activation makes M2
/// primary at epoch 2 with M1 out of the write set.
module dfs_converting_takeover_takeover_scenario {
  import common.* from "../../lib/common"
  import dfs_converting_takeover as T from "../../models/dfs/converting_takeover"
  import dfs_converting_takeover_safety as C from "../../checks/dfs/converting_takeover_safety"
  var phase: int
  action init = all { T::initWith(T::Design), phase' = 0 }
  action step = any {
    all { phase == 0, T::judge(T::M1, true), phase' = 1 },
    all { phase == 1, T::beginTakeover(T::M2), phase' = 2 },
    all { phase == 2, T::seal, phase' = 3 },
    all { phase == 3, T::stageAny(T::M2), phase' = 4 },
    all { phase == 4, T::stageAny(T::M3), phase' = 5 },
    all { phase == 5, T::activateTakeover, phase' = 6 },
    all { phase == 6, T::s' = T::s, phase' = phase },
  }
  def safety: bool = C::safety
  def terminalWitness: bool = all { phase == 6, T::s.serving == { epoch: 2, primary: T::M2, writeSet: Set(T::M2, T::M3) }, C::witnessTakeoverActivates }
}

/// M1 records its own attempt and is judged lost; M2's first round replaces
/// it. M2 is judged lost and M3's second round replaces M2's. M3 is judged
/// lost in turn while M2 is reachable again, so M2 runs the third round,
/// counting only M3 as lost: its image is staged on M2 alone, and its
/// activation makes M2 primary at epoch 2.
module dfs_converting_takeover_later_rounds_scenario {
  import common.* from "../../lib/common"
  import dfs_converting_takeover as T from "../../models/dfs/converting_takeover"
  import dfs_converting_takeover_safety as C from "../../checks/dfs/converting_takeover_safety"
  var phase: int
  action init = all { T::initWith(T::Design), phase' = 0 }
  action step = any {
    all { phase == 0, T::beginConversion, phase' = 1 },
    all { phase == 1, T::judge(T::M1, true), phase' = 2 },
    all { phase == 2, T::beginTakeover(T::M2), phase' = 3 },
    all { phase == 3, T::judge(T::M2, true), phase' = 4 },
    all { phase == 4, T::beginTakeover(T::M3), phase' = 5 },
    all { phase == 5, T::judge(T::M3, true), phase' = 6 },
    all { phase == 6, T::judge(T::M2, false), phase' = 7 },
    all { phase == 7, T::beginTakeover(T::M2), phase' = 8 },
    all { phase == 8, T::seal, phase' = 9 },
    all { phase == 9, T::stageAny(T::M2), phase' = 10 },
    all { phase == 10, T::activateTakeover, phase' = 11 },
    all { phase == 11, T::s' = T::s, phase' = phase },
  }
  def safety: bool = C::safety
  def terminalWitness: bool = all { phase == 11, T::s.serving.primary == T::M2, C::witnessLaterRoundRecorded, C::witnessTakeoverReplacesPrimaryAttempt, T::s.activations.exists(act => act.id.round == 3 and act.id.lost == Set(T::M3)) }
}

/// Counterexample: a primary that replaced a takeover recorded at its epoch
/// would let a lost primary and its successor replace each other.
module dfs_converting_takeover_primary_replaces_takeover_counterexample {
  import common.* from "../../lib/common"
  import dfs_converting_takeover as T from "../../models/dfs/converting_takeover"
  import dfs_converting_takeover_safety as C from "../../checks/dfs/converting_takeover_safety"
  var phase: int
  action init = all { T::initWith(T::PrimaryReplacesTakeover), phase' = 0 }
  action step = any {
    all { phase == 0, T::judge(T::M1, true), phase' = 1 },
    all { phase == 1, T::beginTakeover(T::M2), phase' = 2 },
    all { phase == 2, T::beginConversion, phase' = 3 },
    all { phase == 3, T::s' = T::s, phase' = phase },
  }
  def safety: bool = C::activatedImageHoldsCommittedHistory
  def terminalWitness: bool = all { phase == 3, not(C::primaryNeverReplacesTakeover) }
}

/// Counterexample: a takeover that always ran the first round would record
/// round 1 twice at one epoch, with two converters.
module dfs_converting_takeover_round_not_derived_counterexample {
  import common.* from "../../lib/common"
  import dfs_converting_takeover as T from "../../models/dfs/converting_takeover"
  import dfs_converting_takeover_safety as C from "../../checks/dfs/converting_takeover_safety"
  var phase: int
  action init = all { T::initWith(T::RoundNotDerived), phase' = 0 }
  action step = any {
    all { phase == 0, T::judge(T::M1, true), phase' = 1 },
    all { phase == 1, T::beginTakeover(T::M2), phase' = 2 },
    all { phase == 2, T::judge(T::M2, true), phase' = 3 },
    all { phase == 3, T::beginTakeover(T::M3), phase' = 4 },
    all { phase == 4, T::s' = T::s, phase' = phase },
  }
  def safety: bool = C::activatedImageHoldsCommittedHistory
  def terminalWitness: bool = all { phase == 4, not(C::takeoverRoundsFollowTheRecordedRound) }
}

/// Counterexample: a takeover without the loss verdict replaces a primary
/// that is still reachable.
module dfs_converting_takeover_no_loss_verdict_counterexample {
  import common.* from "../../lib/common"
  import dfs_converting_takeover as T from "../../models/dfs/converting_takeover"
  import dfs_converting_takeover_safety as C from "../../checks/dfs/converting_takeover_safety"
  var phase: int
  action init = all { T::initWith(T::NoLossVerdict), phase' = 0 }
  action step = any {
    all { phase == 0, T::beginTakeover(T::M2), phase' = 1 },
    all { phase == 1, T::s' = T::s, phase' = phase },
  }
  def safety: bool = C::activatedImageHoldsCommittedHistory
  def terminalWitness: bool = all { phase == 1, not(C::replacedConverterWasJudgedLost) }
}

/// Counterexample: a takeover activation that did not move the serving
/// record would leave the lost primary serving the converted image.
module dfs_converting_takeover_activation_keeps_serving_record_counterexample {
  import common.* from "../../lib/common"
  import dfs_converting_takeover as T from "../../models/dfs/converting_takeover"
  import dfs_converting_takeover_safety as C from "../../checks/dfs/converting_takeover_safety"
  var phase: int
  action init = all { T::initWith(T::ActivationKeepsServingRecord), phase' = 0 }
  action step = any {
    all { phase == 0, T::judge(T::M1, true), phase' = 1 },
    all { phase == 1, T::beginTakeover(T::M2), phase' = 2 },
    all { phase == 2, T::seal, phase' = 3 },
    all { phase == 3, T::stageAny(T::M2), phase' = 4 },
    all { phase == 4, T::stageAny(T::M3), phase' = 5 },
    all { phase == 5, T::activateTakeover, phase' = 6 },
    all { phase == 6, T::s' = T::s, phase' = phase },
  }
  def safety: bool = C::activatedImageHoldsCommittedHistory
  def terminalWitness: bool = all { phase == 6, not(C::activationMovesTheServingRecordExactly) }
}

/// Counterexample: an activation before the image is staged on the install
/// rule's members leaves them without the shard's generation.
module dfs_converting_takeover_activate_without_install_counterexample {
  import common.* from "../../lib/common"
  import dfs_converting_takeover as T from "../../models/dfs/converting_takeover"
  import dfs_converting_takeover_safety as C from "../../checks/dfs/converting_takeover_safety"
  var phase: int
  action init = all { T::initWith(T::ActivateWithoutInstall), phase' = 0 }
  action step = any {
    all { phase == 0, T::judge(T::M1, true), phase' = 1 },
    all { phase == 1, T::beginTakeover(T::M2), phase' = 2 },
    all { phase == 2, T::seal, phase' = 3 },
    all { phase == 3, T::activateTakeover, phase' = 4 },
    all { phase == 4, T::s' = T::s, phase' = phase },
  }
  def safety: bool = C::activatedImageHoldsCommittedHistory
  def terminalWitness: bool = all { phase == 4, not(C::activationMeetsInstallRule) }
}

/// Counterexample: M4, a recruit outside the write set, converts from a copy
/// without the committed history, and its activation makes it primary.
module dfs_converting_takeover_converter_outside_write_set_counterexample {
  import common.* from "../../lib/common"
  import dfs_converting_takeover as T from "../../models/dfs/converting_takeover"
  import dfs_converting_takeover_safety as C from "../../checks/dfs/converting_takeover_safety"
  var phase: int
  action init = all { T::initWith(T::ConverterOutsideWriteSet), phase' = 0 }
  action step = any {
    all { phase == 0, T::judge(T::M1, true), phase' = 1 },
    all { phase == 1, T::beginTakeover(T::M4), phase' = 2 },
    all { phase == 2, T::seal, phase' = 3 },
    all { phase == 3, T::stageAny(T::M2), phase' = 4 },
    all { phase == 4, T::stageAny(T::M3), phase' = 5 },
    all { phase == 5, T::activateTakeover, phase' = 6 },
    all { phase == 6, T::s' = T::s, phase' = phase },
  }
  def safety: bool = C::activationMeetsInstallRule
  def terminalWitness: bool = all { phase == 6, not(C::activatedImageHoldsCommittedHistory) }
}
