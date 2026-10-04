// Unrestricted safety checked through6 transitions; longer progress witnesses simulated.
module dfs_bounded_intent_scan_safety {
  import dfs_bounded_intent_scan.* from "../../models/dfs/bounded_intent_scan"
  val invariant = safety
  val witnessRawRejectedBudget = s.sawTerminalVisit and s.sawRejectedVisit and s.sawDuplicateVisit
  val witnessCancellationWrap = s.cancelledAfterCursor and s.wrapped and s.effects.size() > 0
  val witnessSharedCursorCas = s.sawCasMiss
  val witnessPinnedCut = s.sawAppendBeyondCut and s.wrapped
  val witnessBackdatedWrap = s.sawReplacedBehindCursor and s.wrapped and s.effects.exists(e => e.currentIdentity == 1)
  val witnessGenerationReset = s.generation == 1 and s.effects.exists(e => e.admittedGeneration == 1)
}
