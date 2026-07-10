/// Invariant checks and branch witnesses for `network_attempt_budget`.
module network_attempt_budget_safety {
  import common.* from "../../lib/common"
  import network_attempt_budget.* from "../../models/network/attempt_budget"

  def routeBudgetMatchesRequestClass: bool =
    remainingRouteBudget
      <= routeDeadlineMaximum(requestUsesFullResponseBudget)


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


  def fullResponseActivationMatchesAlgorithm: bool =
    useFullResponseBudget
      == hopUsesFullResponseBudget(
        endpointKind,
        endpointsLeft,
        requestUsesFullResponseBudget
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


  def everySplitBudgetIsCapped: bool =
    splitEndpointBudget <= MaxEndpointAttemptBudget


  def singleEndpointBaseBudgetIsStillCapped: bool =
    endpointCount == 1 implies
      baseEndpointAttemptBudget(
        remainingRouteBudget,
        endpointsLeft,
        endpointCount
      )
        == minInt(remainingRouteBudget, MaxEndpointAttemptBudget)


  def unverifiedAndTunnelFallbackSplitsAreCapped: bool =
    any {
      endpointKind == UnverifiedDirect,
      endpointShouldReserveFallback(endpointKind, endpointsLeft),
    } implies splitEndpointBudget <= UnverifiedEndpointAttemptMax


  def fullResponseLastEndpointUsesRemainingBudget: bool =
    all {
      requestUsesFullResponseBudget,
      endpointsLeft == 1,
    } implies all {
      useFullResponseBudget,
      finalAttemptBudget == remainingRouteBudget,
    }


  def unverifiedFullResponseWithFallbackStaysOnFairSplit: bool =
    all {
      requestUsesFullResponseBudget,
      endpointsLeft > 1,
      not(endpointIsVerified(endpointKind, endpointsLeft)),
    } implies all {
      not(useFullResponseBudget),
      finalAttemptBudget == splitEndpointBudget,
    }


  def verifiedFullResponseReservesEveryFallback: bool = {
    val reserve = totalFallbackReserve(endpointsLeft)
    all {
      requestUsesFullResponseBudget,
      endpointIsVerified(endpointKind, endpointsLeft),
      endpointsLeft > 1,
      remainingRouteBudget > reserve,
    } implies all {
      useFullResponseBudget,
      finalAttemptBudget
        == minInt(
          remainingRouteBudget - reserve,
          VerifiedFullResponseAttemptMax
        ),
    }
  }


  def ordinaryVerifiedAttemptReservesEveryFallback: bool = {
    val reserve = totalFallbackReserve(endpointsLeft)
    all {
      not(requestUsesFullResponseBudget),
      endpointIsVerified(endpointKind, endpointsLeft),
      remainingRouteBudget > reserve,
    } implies
      finalAttemptBudget
        == minInt(
          remainingRouteBudget - reserve,
          VerifiedDirectPreferred
        )
  }


  def syntheticTunnelVerificationDependsOnFallbacks: bool =
    endpointKind == SyntheticTunnel implies
      (endpointIsVerified(endpointKind, endpointsLeft) iff endpointsLeft == 1)


  /// Witness: the production single-endpoint 5-second split cap is exercised.
  def witnessSingleEndpointCap: bool = all {
    phase == Planned,
    endpointKind == VerifiedDirect,
    endpointCount == 1,
    remainingRouteBudget > MaxEndpointAttemptBudget,
    splitEndpointBudget == MaxEndpointAttemptBudget,
  }


  /// Witness: a verified full-response attempt keeps one reserve per fallback.
  def witnessVerifiedFullResponseWithFallbacks: bool = all {
    phase == Planned,
    endpointKind == VerifiedDirect,
    requestUsesFullResponseBudget,
    endpointsLeft > 1,
    remainingRouteBudget > totalFallbackReserve(endpointsLeft),
    finalAttemptBudget
      == minInt(
        remainingRouteBudget - totalFallbackReserve(endpointsLeft),
        VerifiedFullResponseAttemptMax
      ),
  }


  /// Witness: an unverified endpoint is limited to the production 2-second cap.
  def witnessUnverifiedEndpointCap: bool = all {
    phase == Planned,
    endpointKind == UnverifiedDirect,
    splitEndpointBudget == UnverifiedEndpointAttemptMax,
  }


  /// Witness: a healthy synthetic tunnel is still capped while a fallback exists.
  def witnessTunnelFallbackCap: bool = all {
    phase == Planned,
    endpointKind == SyntheticTunnel,
    endpointsLeft > 1,
    splitEndpointBudget == UnverifiedEndpointAttemptMax,
  }


  def safety: bool = all {
    routeBudgetMatchesRequestClass,
    finalBudgetNeverExceedsRouteBudget,
    splitBudgetMatchesAlgorithm,
    fullResponseActivationMatchesAlgorithm,
    finalBudgetMatchesAlgorithm,
    everySplitBudgetIsCapped,
    singleEndpointBaseBudgetIsStillCapped,
    unverifiedAndTunnelFallbackSplitsAreCapped,
    fullResponseLastEndpointUsesRemainingBudget,
    unverifiedFullResponseWithFallbackStaysOnFairSplit,
    verifiedFullResponseReservesEveryFallback,
    ordinaryVerifiedAttemptReservesEveryFallback,
    syntheticTunnelVerificationDependsOnFallbacks,
  }
}
