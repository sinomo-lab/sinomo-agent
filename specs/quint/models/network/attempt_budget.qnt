/// Bounded endpoint-attempt budget model for cluster object-RPC routing.
///
/// This module transliterates the duration arithmetic in
/// `crates/sinomo-network/src/route.rs` into finite integer units. One unit is
/// 500 ms, so the production bounds remain exact:
///
/// - ordinary route deadline: 5 s = 10 units
/// - full-response route deadline: 60 s = 120 units
/// - endpoint-attempt maximum: 5 s = 10 units
/// - unverified/tunnel maximum: 2 s = 4 units
/// - verified direct preferred window: 3.5 s = 7 units
/// - verified full-response maximum: 10 s = 20 units
/// - reserve for each fallback endpoint: 500 ms = 1 unit
///
/// Evidence class: current-behavior refinement of the production duration
/// arithmetic. Wall-clock scheduling and transport progress are omitted.
module network_attempt_budget {
  /// `SyntheticTunnel` represents a tunnel with healthy route evidence. It is
  /// intentionally treated as unverified while alternatives remain, and as a
  /// verified endpoint only when it is the last endpoint.
  type EndpointKind = VerifiedDirect | UnverifiedDirect | SyntheticTunnel
  type BudgetPhase = Idle | Planned
  type Decision = NoDecision | AttemptBudgetPlanned | TerminalStutter

  /// Representative finite route-budget values. Arithmetic results need not
  /// themselves be members of this input set.
  pure val BudgetUnits: Set[int] = Set(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 20, 60, 120
  )
  pure val EndpointCounts: Set[int] = Set(1, 2, 3, 4, 5, 6)
  pure val EndpointKinds: Set[EndpointKind] =
    Set(VerifiedDirect, UnverifiedDirect, SyntheticTunnel)

  pure val BudgetQuantumMs: int = 500
  pure val OrdinaryRouteDeadlineMax: int = 10
  pure val FullResponseRouteDeadlineMax: int = 120
  pure val MaxEndpointAttemptBudget: int = 10
  pure val UnverifiedEndpointAttemptMax: int = 4
  pure val VerifiedDirectPreferred: int = 7
  pure val VerifiedFullResponseAttemptMax: int = 20
  pure val FallbackReserve: int = 1

  var phase: BudgetPhase
  var endpointKind: EndpointKind
  var remainingRouteBudget: int
  var endpointsLeft: int
  var endpointCount: int
  var requestUsesFullResponseBudget: bool
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

  pure def routeDeadlineMaximum(fullResponse: bool): int =
    if (fullResponse) {
      FullResponseRouteDeadlineMax
    } else {
      OrdinaryRouteDeadlineMax
    }

  pure def fallbackReserveCount(left: int): int =
    if (left <= 1) { 0 } else { left - 1 }

  pure def totalFallbackReserve(left: int): int =
    FallbackReserve * fallbackReserveCount(left)

  pure def baseEndpointAttemptBudget(
    remaining: int,
    left: int,
    count: int
  ): int = {
    val fairShare =
      if (count <= 1) {
        remaining
      } else {
        minInt(ceilDiv(remaining, left), remaining)
      }
    minInt(fairShare, MaxEndpointAttemptBudget)
  }

  pure def endpointShouldReserveFallback(kind: EndpointKind, left: int): bool =
    kind == SyntheticTunnel and left > 1

  pure def endpointIsVerified(kind: EndpointKind, left: int): bool =
    any {
      kind == VerifiedDirect,
      kind == SyntheticTunnel and left <= 1,
    }

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

  pure def hopUsesFullResponseBudget(
    kind: EndpointKind,
    left: int,
    requestFullResponse: bool
  ): bool =
    all {
      requestFullResponse,
      not(endpointShouldReserveFallback(kind, left)),
      any {
        left <= 1,
        endpointIsVerified(kind, left),
      },
    }

  pure def verifiedPreferredBudget(remaining: int, left: int): int =
    minInt(
      saturatingSub(remaining, totalFallbackReserve(left)),
      VerifiedDirectPreferred
    )

  pure def verifiedFullResponseBudget(remaining: int, left: int): int =
    minInt(
      saturatingSub(remaining, totalFallbackReserve(left)),
      VerifiedFullResponseAttemptMax
    )

  /// This is a direct translation of
  /// `cluster_target_final_hop_attempt_budget`. The boolean is the active hop
  /// policy after the caller has applied `hopUsesFullResponseBudget`.
  pure def finalHopAttemptBudget(
    kind: EndpointKind,
    remaining: int,
    left: int,
    endpointBudget: int,
    fullResponse: bool
  ): int = {
    val verified = endpointIsVerified(kind, left)
    val fullResponsePreferred = verifiedFullResponseBudget(remaining, left)
    if (fullResponse and verified and left > 1 and fullResponsePreferred > 0) {
      fullResponsePreferred
    } else if (fullResponse and left <= 1) {
      remaining
    } else if (fullResponse and not(verified)) {
      endpointBudget
    } else if (verified) {
      val preferred = verifiedPreferredBudget(remaining, left)
      if (preferred > 0) {
        minInt(preferred, remaining)
      } else {
        minInt(endpointBudget, remaining)
      }
    } else {
      endpointBudget
    }
  }

  action init = all {
    phase' = Idle,
    endpointKind' = VerifiedDirect,
    remainingRouteBudget' = 0,
    endpointsLeft' = 1,
    endpointCount' = 1,
    requestUsesFullResponseBudget' = false,
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
    requestFullResponse: bool
  ): bool = {
    val split = endpointAttemptBudget(kind, remaining, left, count)
    val useFullResponse =
      hopUsesFullResponseBudget(kind, left, requestFullResponse)
    all {
      phase == Idle,
      BudgetUnits.contains(remaining),
      remaining <= routeDeadlineMaximum(requestFullResponse),
      EndpointCounts.contains(left),
      EndpointCounts.contains(count),
      left <= count,
      endpointKind' = kind,
      remainingRouteBudget' = remaining,
      endpointsLeft' = left,
      endpointCount' = count,
      requestUsesFullResponseBudget' = requestFullResponse,
      useFullResponseBudget' = useFullResponse,
      splitEndpointBudget' = split,
      finalAttemptBudget' =
        finalHopAttemptBudget(kind, remaining, left, split, useFullResponse),
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
    requestUsesFullResponseBudget' = requestUsesFullResponseBudget,
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
      nondet requestFullResponse = oneOf(Set(true, false))
      planAttempt(kind, remaining, left, count, requestFullResponse)
    },
    stutterPlanned,
  }
}
