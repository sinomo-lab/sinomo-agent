/// Safety and witnesses of the per-shard storage record and its conversion.
/// Bounded abstract contract; not a proof of the Rust implementation.
module dfs_shard_storage_record_safety {
  import dfs_shard_storage_record.* from "../../models/dfs/shard_storage_record"

  /// Every open slot's header matches an Active record, so all open slots
  /// serve one generation. Once a member serves, neither the record nor that
  /// member's header changes again.
  def openSlotsServeTheActiveGeneration: bool = all {
    s.open.forall(m => admits(s.storage, s.header.get(m))),
    s.open.forall(a => s.open.forall(b => s.header.get(a) == s.header.get(b))),
  }

  /// An Active record is final: no Converting record ever replaces it.
  def activeRecordIsFinal: bool = not(s.replacedActive)

  /// An attempt activates only under the serving record it started under:
  /// its own epoch, with its converter as primary. So an attempt a serving
  /// change superseded never activates.
  def activationRunsUnderItsServingRecord: bool =
    s.activations.forall(a => all {
      a.id.epoch == a.epoch,
      a.id.converter == a.primary,
      not(s.superseded.contains(a.id)),
    })

  /// An activation was sealed and met the shard's install rule.
  def activationMeetsInstallRule: bool =
    s.activations.forall(a => all {
      s.sealedEver.contains(a.id),
      installRuleMet(s.switched, a.writeSet, a.primary, a.installed),
    })

  /// Conversion never advances the catalog generation: only catalog changes
  /// do.
  def catalogGenerationMovesOnlyWithTheCatalog: bool = s.catalogGen == 1 + s.catalogChanges

  def safety: bool = all {
    openSlotsServeTheActiveGeneration,
    activeRecordIsFinal,
    activationRunsUnderItsServingRecord,
    activationMeetsInstallRule,
    catalogGenerationMovesOnlyWithTheCatalog,
  }

  def witnessGenesisServes: bool = s.storage == NativeGenesis and s.open != Set()
  def witnessConvertedServes: bool = isActive(s.storage) and s.activations != Set() and s.open != Set()
  /// The unswitched rule activated with the primary and one other member.
  def witnessMajorityActivation: bool = s.activations.exists(a => not(s.switched) and a.installed.size() == 2)
  /// A serving change superseded one attempt and the next primary's attempt
  /// activated.
  def witnessSupersededAttemptReplaced: bool =
    s.superseded != Set() and s.activations.exists(a => a.epoch > 1)
  /// The catalog generation moved while the attempt converted, and it still
  /// activated.
  def witnessActivatedAcrossCatalogChange: bool =
    s.activations.exists(a => s.recorded.exists(r => r.id == a.id and r.catalog < a.catalog))
  def witnessRepairedBySnapshot: bool = s.repaired != Set() and s.open.size() == 3
}
