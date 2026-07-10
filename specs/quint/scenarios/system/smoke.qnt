/// Whole-system randomized smoke exploration.
///
/// This is not the strong proof unit. It runs the shared `system_base` state
/// machine with broad cross-layer invariants from `system_guarantees` to catch
/// integration contradictions between membership, endpoint selection, routing,
/// delivery retry, and DFS metadata authority.
module system_smoke {
  import system_base.* from "../../models/system/base"
  import system_guarantees.* from "../../contracts/system/guarantees"

  action init = systemInit
  action step = systemStep

  def safety: bool = systemSafety
}
