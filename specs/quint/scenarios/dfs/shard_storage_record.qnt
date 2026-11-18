/// Deterministic traces of the per-shard storage record. Design traces keep
/// the full invariant suite; each counterexample enables one broken variant
/// and reaches the violation its invariant detects.

/// A switched shard of old storage converts on its primary while another
/// shard's birth advances the catalog generation: Converting, seal, stage on
/// the write set N1 and N2, activate. Both activate their staged copies and
/// open, and N3, outside the write set, repairs by snapshot.
module dfs_shard_storage_record_conversion_scenario {
  import dfs_shard_storage_record as R from "../../models/dfs/shard_storage_record"
  import dfs_shard_storage_record_safety as C from "../../checks/dfs/shard_storage_record_safety"
  var phase: int
  pure val attempt: R::AttemptId = { epoch: 1, converter: R::N1, source: 1 }
  action init = all { R::initWith(R::Design, true, false), phase' = 0 }
  action step = any {
    all { phase == 0, R::beginConversion(1), phase' = 1 },
    all { phase == 1, R::catalogChange, phase' = 2 },
    all { phase == 2, R::seal, phase' = 3 },
    all { phase == 3, R::stage(R::N1, attempt), phase' = 4 },
    all { phase == 4, R::stage(R::N2, attempt), phase' = 5 },
    all { phase == 5, R::activate, phase' = 6 },
    all { phase == 6, R::activateLocal(R::N1), phase' = 7 },
    all { phase == 7, R::activateLocal(R::N2), phase' = 8 },
    all { phase == 8, R::openSlot(R::N1), phase' = 9 },
    all { phase == 9, R::openSlot(R::N2), phase' = 10 },
    all { phase == 10, R::repair(R::N3), phase' = 11 },
    all { phase == 11, R::openSlot(R::N3), phase' = 12 },
    all { phase == 12, R::s' = R::s, phase' = phase },
  }
  def safety: bool = C::safety
  def terminalWitness: bool = all { phase == 12, R::s.open == R::Members, R::s.epoch == 1, C::witnessActivatedAcrossCatalogChange, C::witnessRepairedBySnapshot }
}

/// An unswitched shard: N1's attempt is staged on N1 only when a serving
/// change makes N2 primary. N2's attempt replaces the superseded one and
/// activates with N2 and N3, a majority including the primary. N1's stale
/// staged image never opens; N1 repairs by snapshot.
module dfs_shard_storage_record_superseded_scenario {
  import dfs_shard_storage_record as R from "../../models/dfs/shard_storage_record"
  import dfs_shard_storage_record_safety as C from "../../checks/dfs/shard_storage_record_safety"
  var phase: int
  pure val first: R::AttemptId = { epoch: 1, converter: R::N1, source: 1 }
  pure val second: R::AttemptId = { epoch: 2, converter: R::N2, source: 2 }
  action init = all { R::initWith(R::Design, false, false), phase' = 0 }
  action step = any {
    all { phase == 0, R::beginConversion(1), phase' = 1 },
    all { phase == 1, R::seal, phase' = 2 },
    all { phase == 2, R::stage(R::N1, first), phase' = 3 },
    all { phase == 3, R::servingChange(R::N2), phase' = 4 },
    all { phase == 4, R::beginConversion(2), phase' = 5 },
    all { phase == 5, R::seal, phase' = 6 },
    all { phase == 6, R::stage(R::N2, second), phase' = 7 },
    all { phase == 7, R::stage(R::N3, second), phase' = 8 },
    all { phase == 8, R::activate, phase' = 9 },
    all { phase == 9, R::activateLocal(R::N2), phase' = 10 },
    all { phase == 10, R::activateLocal(R::N3), phase' = 11 },
    all { phase == 11, R::openSlot(R::N2), phase' = 12 },
    all { phase == 12, R::openSlot(R::N3), phase' = 13 },
    all { phase == 13, R::repair(R::N1), phase' = 14 },
    all { phase == 14, R::openSlot(R::N1), phase' = 15 },
    all { phase == 15, R::s' = R::s, phase' = phase },
  }
  def safety: bool = C::safety
  def terminalWitness: bool = all { phase == 15, C::witnessSupersededAttemptReplaced, C::witnessMajorityActivation, C::witnessRepairedBySnapshot }
}

/// Counterexample: if an Active record could be replaced by a Converting
/// one, a member already serving the native genesis would keep serving a
/// generation the record no longer admits.
module dfs_shard_storage_record_convert_active_counterexample {
  import dfs_shard_storage_record as R from "../../models/dfs/shard_storage_record"
  import dfs_shard_storage_record_safety as C from "../../checks/dfs/shard_storage_record_safety"
  var phase: int
  action init = all { R::initWith(R::ConvertActive, true, true), phase' = 0 }
  action step = any {
    all { phase == 0, R::openSlot(R::N1), phase' = 1 },
    all { phase == 1, R::beginConversion(1), phase' = 2 },
    all { phase == 2, R::s' = R::s, phase' = phase },
  }
  def safety: bool = C::activationMeetsInstallRule
  def terminalWitness: bool = all { phase == 2, not(C::openSlotsServeTheActiveGeneration), not(C::activeRecordIsFinal) }
}

/// Counterexample: without the serving-record check, N1's attempt activates
/// after a serving change made N2 primary at epoch 2.
module dfs_shard_storage_record_no_supersession_counterexample {
  import dfs_shard_storage_record as R from "../../models/dfs/shard_storage_record"
  import dfs_shard_storage_record_safety as C from "../../checks/dfs/shard_storage_record_safety"
  var phase: int
  pure val attempt: R::AttemptId = { epoch: 1, converter: R::N1, source: 1 }
  action init = all { R::initWith(R::NoSupersession, false, false), phase' = 0 }
  action step = any {
    all { phase == 0, R::beginConversion(1), phase' = 1 },
    all { phase == 1, R::seal, phase' = 2 },
    all { phase == 2, R::stage(R::N2, attempt), phase' = 3 },
    all { phase == 3, R::stage(R::N3, attempt), phase' = 4 },
    all { phase == 4, R::servingChange(R::N2), phase' = 5 },
    all { phase == 5, R::activate, phase' = 6 },
    all { phase == 6, R::s' = R::s, phase' = phase },
  }
  def safety: bool = C::openSlotsServeTheActiveGeneration
  def terminalWitness: bool = all { phase == 6, not(C::activationRunsUnderItsServingRecord) }
}

/// Counterexample: an activation before the image is staged on the write set
/// leaves the shard Active with no member holding its generation.
module dfs_shard_storage_record_activate_without_install_counterexample {
  import dfs_shard_storage_record as R from "../../models/dfs/shard_storage_record"
  import dfs_shard_storage_record_safety as C from "../../checks/dfs/shard_storage_record_safety"
  var phase: int
  action init = all { R::initWith(R::ActivateWithoutInstall, true, false), phase' = 0 }
  action step = any {
    all { phase == 0, R::beginConversion(1), phase' = 1 },
    all { phase == 1, R::seal, phase' = 2 },
    all { phase == 2, R::activate, phase' = 3 },
    all { phase == 3, R::s' = R::s, phase' = phase },
  }
  def safety: bool = C::openSlotsServeTheActiveGeneration
  def terminalWitness: bool = all { phase == 3, not(C::activationMeetsInstallRule) }
}

/// Counterexample: an activation that also advanced the filesystem's catalog
/// generation would reject other shards' path assignments built from the
/// earlier generation.
module dfs_shard_storage_record_activation_advances_catalog_counterexample {
  import dfs_shard_storage_record as R from "../../models/dfs/shard_storage_record"
  import dfs_shard_storage_record_safety as C from "../../checks/dfs/shard_storage_record_safety"
  var phase: int
  pure val attempt: R::AttemptId = { epoch: 1, converter: R::N1, source: 1 }
  action init = all { R::initWith(R::ActivationAdvancesCatalog, true, false), phase' = 0 }
  action step = any {
    all { phase == 0, R::beginConversion(1), phase' = 1 },
    all { phase == 1, R::seal, phase' = 2 },
    all { phase == 2, R::stage(R::N1, attempt), phase' = 3 },
    all { phase == 3, R::stage(R::N2, attempt), phase' = 4 },
    all { phase == 4, R::activate, phase' = 5 },
    all { phase == 5, R::s' = R::s, phase' = phase },
  }
  def safety: bool = C::activationMeetsInstallRule
  def terminalWitness: bool = all { phase == 5, not(C::catalogGenerationMovesOnlyWithTheCatalog) }
}
