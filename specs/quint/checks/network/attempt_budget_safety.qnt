/// Invariant checks for `network_attempt_budget`.
module network_attempt_budget_safety {
  import common.* from "../../lib/common"
  import network_attempt_budget.* from "../../models/network/attempt_budget"

  def finalBudgetNeverExceedsRouteBudget: bool =
    finalAttemptBudget <= remainingRouteBudget


  def splitBudgetMatchesAlgorithm: bool =
    splitEndpointBudget
      == endpointAttemptBudget(
        endpointKind,
        remainingRouteBudget,
        endpointsLeft,
        endpointCount
      )


  def finalBudgetMatchesAlgorithm: bool =
    finalAttemptBudget
      == finalHopAttemptBudget(
        endpointKind,
        remainingRouteBudget,
        endpointsLeft,
        splitEndpointBudget,
        useFullResponseBudget
      )


  def unverifiedAndTunnelSplitBudgetAreCapped: bool =
    all {
      endpointKind != VerifiedDirect,
      endpointsLeft > 1,
    } implies splitEndpointBudget <= UnverifiedEndpointAttemptMax


  def fullResponseWithFallbackKeepsReserve: bool =
    all {
      useFullResponseBudget,
      endpointsLeft > 1,
      remainingRouteBudget > FallbackReserve,
    } implies finalAttemptBudget <= remainingRouteBudget - FallbackReserve


  def fullResponseLastEndpointUsesRemainingBudget: bool =
    all {
      useFullResponseBudget,
      endpointsLeft == 1,
    } implies finalAttemptBudget == remainingRouteBudget


  def verifiedDirectGetsPreferredWindowWhenPossible: bool =
    all {
      endpointKind == VerifiedDirect,
      endpointsLeft > 1,
      remainingRouteBudget > FallbackReserve,
    } implies
      finalAttemptBudget
        == minInt(
          remainingRouteBudget - FallbackReserve,
          VerifiedDirectPreferred
        )


  def safety: bool = all {
    finalBudgetNeverExceedsRouteBudget,
    splitBudgetMatchesAlgorithm,
    finalBudgetMatchesAlgorithm,
    unverifiedAndTunnelSplitBudgetAreCapped,
    fullResponseWithFallbackKeepsReserve,
    fullResponseLastEndpointUsesRemainingBudget,
    verifiedDirectGetsPreferredWindowWhenPossible,
  }
}
