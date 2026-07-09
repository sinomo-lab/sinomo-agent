/// Bounded endpoint-attempt budget model for cluster object-RPC routing.
///
/// This module abstracts the duration arithmetic in
/// `crates/sinomo-network/src/route.rs` into small integer budget units. It
/// covers the fragile cases from the routed data RPC fixes: unverified/tunnel
/// endpoints are capped, full-response reads reserve fallback budget, and the
/// last endpoint may use the remaining route budget.
module network_attempt_budget {
  type EndpointKind = VerifiedDirect | UnverifiedDirect | SyntheticTunnel
  type BudgetPhase = Idle | Planned
  type Decision = NoDecision | AttemptBudgetPlanned | TerminalStutter

  pure val BudgetUnits: Set[int] = Set(0, 1, 2, 3, 4, 5)
  pure val EndpointCounts: Set[int] = Set(1, 2, 3)
  pure val EndpointKinds: Set[EndpointKind] =
    Set(VerifiedDirect, UnverifiedDirect, SyntheticTunnel)

  pure val MaxEndpointAttemptBudget: int = 5
  pure val UnverifiedEndpointAttemptMax: int = 2
  pure val VerifiedDirectPreferred: int = 4
  pure val FullResponsePreferred: int = 5
  pure val FallbackReserve: int = 1

  var phase: BudgetPhase
  var endpointKind: EndpointKind
  var remainingRouteBudget: int
  var endpointsLeft: int
  var endpointCount: int
  var useFullResponseBudget: bool
  var splitEndpointBudget: int
  var finalAttemptBudget: int
  var lastDecision: Decision

  pure def minInt(left: int, right: int): int =
    if (left <= right) { left } else { right }

  pure def saturatingSub(left: int, right: int): int =
    if (left <= right) { 0 } else { left - right }

  pure def ceilDiv(left: int, right: int): int =
    if (right <= 0) { 0 } else { (left + right - 1) / right }

  pure def baseEndpointAttemptBudget(
    remaining: int,
    left: int,
    count: int
  ): int =
    if (count <= 1) {
      remaining
    } else {
      minInt(ceilDiv(remaining, left), MaxEndpointAttemptBudget)
    }

  pure def endpointShouldReserveFallback(kind: EndpointKind, left: int): bool =
    kind == SyntheticTunnel and left > 1

  pure def endpointAttemptBudget(
    kind: EndpointKind,
    remaining: int,
    left: int,
    count: int
  ): int = {
    val base = baseEndpointAttemptBudget(remaining, left, count)
    if (kind == UnverifiedDirect or endpointShouldReserveFallback(kind, left)) {
      minInt(base, UnverifiedEndpointAttemptMax)
    } else {
      base
    }
  }

  pure def verifiedDirectPreferredBudget(remaining: int, left: int): int = {
    val reserve = if (left > 1) { FallbackReserve } else { 0 }
    minInt(saturatingSub(remaining, reserve), VerifiedDirectPreferred)
  }

  pure def fullResponsePreferredBudget(remaining: int, left: int): int = {
    val reserve = if (left > 1) { FallbackReserve } else { 0 }
    minInt(saturatingSub(remaining, reserve), FullResponsePreferred)
  }

  pure def finalHopAttemptBudget(
    kind: EndpointKind,
    remaining: int,
    left: int,
    endpointBudget: int,
    fullResponse: bool
  ): int =
    if (fullResponse and left <= 1) {
      remaining
    } else if (fullResponse and kind != VerifiedDirect) {
      val preferred = fullResponsePreferredBudget(remaining, left)
      if (preferred > 0 and preferred >= endpointBudget) {
        minInt(preferred, remaining)
      } else {
        endpointBudget
      }
    } else if (kind == VerifiedDirect) {
      val preferred = verifiedDirectPreferredBudget(remaining, left)
      if (preferred > 0) {
        minInt(preferred, remaining)
      } else {
        minInt(endpointBudget, remaining)
      }
    } else {
      endpointBudget
    }

  action init = all {
    phase' = Idle,
    endpointKind' = VerifiedDirect,
    remainingRouteBudget' = 0,
    endpointsLeft' = 1,
    endpointCount' = 1,
    useFullResponseBudget' = false,
    splitEndpointBudget' = 0,
    finalAttemptBudget' = 0,
    lastDecision' = NoDecision,
  }

  action planAttempt(
    kind: EndpointKind,
    remaining: int,
    left: int,
    count: int,
    fullResponse: bool
  ): bool = {
    val split = endpointAttemptBudget(kind, remaining, left, count)
    all {
      phase == Idle,
      BudgetUnits.contains(remaining),
      EndpointCounts.contains(left),
      EndpointCounts.contains(count),
      left <= count,
      endpointKind' = kind,
      remainingRouteBudget' = remaining,
      endpointsLeft' = left,
      endpointCount' = count,
      useFullResponseBudget' = fullResponse,
      splitEndpointBudget' = split,
      finalAttemptBudget' =
        finalHopAttemptBudget(kind, remaining, left, split, fullResponse),
      phase' = Planned,
      lastDecision' = AttemptBudgetPlanned,
    }
  }

  action stutterPlanned: bool = all {
    phase == Planned,
    endpointKind' = endpointKind,
    remainingRouteBudget' = remainingRouteBudget,
    endpointsLeft' = endpointsLeft,
    endpointCount' = endpointCount,
    useFullResponseBudget' = useFullResponseBudget,
    splitEndpointBudget' = splitEndpointBudget,
    finalAttemptBudget' = finalAttemptBudget,
    phase' = phase,
    lastDecision' = TerminalStutter,
  }

  action step = any {
    {
      nondet kind = oneOf(EndpointKinds)
      nondet remaining = oneOf(BudgetUnits)
      nondet left = oneOf(EndpointCounts)
      nondet count = oneOf(EndpointCounts)
      nondet fullResponse = oneOf(Set(true, false))
      planAttempt(kind, remaining, left, count, fullResponse)
    },
    stutterPlanned,
  }
}
