/// Safety and witnesses of the converting takeover. Bounded abstract
/// contract; not a proof of the Rust implementation.
module dfs_converting_takeover_safety {
  import common.* from "../../lib/common"
  import dfs_converting_takeover.* from "../../models/dfs/converting_takeover"

  /// The primary's own attempt never replaces a takeover recorded at its
  /// epoch.
  def primaryNeverReplacesTakeover: bool =
    s.replacements.forall(rep => rep.by.round == 0 implies match rep.was {
      | Converting(a) => not(a.id.epoch == rep.by.epoch and a.id.round > 0)
      | _ => true
    })

  /// A first takeover round replaces at most the lost primary's own attempt;
  /// a later round replaces exactly the recorded round before it, with
  /// another converter, and counts every earlier converter but its own as
  /// lost. So each round at an epoch is recorded with one converter.
  def takeoverRoundsFollowTheRecordedRound: bool = all {
    s.replacements.forall(rep => rep.by.round > 0 implies match rep.was {
      | Converting(a) =>
        if (a.id.epoch == rep.by.epoch and a.id.round > 0) all {
          rep.by.round == a.id.round + 1,
          rep.by.converter != a.id.converter,
          rep.by.lost == a.id.lost.union(Set(a.id.converter)).exclude(Set(rep.by.converter)),
        } else rep.by.round == 1 and rep.by.lost == Set()
      | _ => rep.by.round == 1 and rep.by.lost == Set()
    }),
    s.replacements.forall(a => s.replacements.forall(b =>
      (a.by.round > 0 and a.by.epoch == b.by.epoch and a.by.round == b.by.round) implies a.by == b.by)),
  }

  /// A takeover replaces a converter only once it is judged lost.
  def replacedConverterWasJudgedLost: bool =
    s.replacements.forall(rep => rep.by.round > 0 implies rep.replacedLost)

  /// An attempt activates under the serving record of its own epoch. The
  /// primary's own attempt leaves the serving record unchanged; a takeover's
  /// activation makes its converter primary at the next epoch, with the lost
  /// primary out of the write set.
  def activationMovesTheServingRecordExactly: bool =
    s.activations.forall(act => all {
      act.before.epoch == act.id.epoch,
      if (act.id.round == 0) act.after == act.before and act.before.primary == act.id.converter
      else act.after == {
        epoch: act.before.epoch + 1, primary: act.id.converter,
        writeSet: act.before.writeSet.exclude(Set(act.before.primary)).union(Set(act.id.converter)),
      },
    })

  /// An activation's image was staged on every member of its install rule.
  def activationMeetsInstallRule: bool =
    s.activations.forall(act => installSet(act.id, act.before).subseteq(act.staged))

  /// The activated image, the converter's own copy, holds the shard's
  /// committed history.
  def activatedImageHoldsCommittedHistory: bool =
    s.activations.forall(act => act.imageComplete)

  def safety: bool = all {
    primaryNeverReplacesTakeover,
    takeoverRoundsFollowTheRecordedRound,
    replacedConverterWasJudgedLost,
    activationMovesTheServingRecordExactly,
    activationMeetsInstallRule,
    activatedImageHoldsCommittedHistory,
  }

  def witnessPrimaryConversionActivates: bool = s.activations.exists(act => act.id.round == 0)
  def witnessTakeoverActivates: bool = s.activations.exists(act => act.id.round >= 1)
  /// A later round replaced a lost converter.
  def witnessLaterRoundRecorded: bool = s.replacements.exists(rep => rep.by.round >= 2)
  /// A takeover replaced the lost primary's own attempt.
  def witnessTakeoverReplacesPrimaryAttempt: bool = s.replacements.exists(rep => rep.by.round == 1 and match rep.was {
    | Converting(a) => a.id.round == 0
    | _ => false
  })
}
