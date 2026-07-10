/// Timed delivery retry model.
///
/// This layer models the caller-visible retry loop around a global request
/// deadline and per-attempt timeout. It intentionally consumes endpoint choice
/// and route validity as lower-layer facts; the focus here is timing, fallback,
/// late response rejection, stable delivery identity, and distinct transport
/// attempt identity.
module network_timed_delivery_retry {
  import common.* from "../../lib/common"
  import network_time.* from "../../lib/network_time"
  import delivery_contract.* from "../../contracts/network/delivery_contract"

  type TimedEndpoint = TimedPrimary | TimedFallback
  type EndpointOutcome =
    | EndpointResponds
    | EndpointDrops
    | EndpointSlow
    | EndpointIdentityMismatch
  type TimedDeliveryKey = StableDeliveryKey | OtherDeliveryKey
  type TimedAttemptNonce = PrimaryAttemptNonce | FallbackAttemptNonce
  type RetryPhase =
    | RetryIdle
    | RetryPrepared
    | RetryAttempting
    | RetryPrimaryTimedOut
    | RetryDelivered
    | RetryFailed
  type RetryDecision =
    | RetryNoDecision
    | RetryTicked
    | RequestPrepared
    | PrimaryStarted
    | FallbackStarted
    | ResponseAccepted
    | AttemptTimedOut
    | IdentityMismatchObserved
    | LateResponseIgnored
    | DeadlineExpiredBeforeAttempt
    | DuplicateReplayObserved
    | RetryStuttered

  pure val TimedEndpoints: Set[TimedEndpoint] = Set(TimedPrimary, TimedFallback)
  pure val EndpointOutcomes: Set[EndpointOutcome] =
    Set(EndpointResponds, EndpointDrops, EndpointSlow, EndpointIdentityMismatch)

  var retryNow: int
  var deadlineAt: int
  var phase: RetryPhase
  var hasFallback: bool
  var endpointOutcome: TimedEndpoint -> EndpointOutcome
  var attemptedEndpoints: Set[TimedEndpoint]
  var timedOutEndpoints: Set[TimedEndpoint]
  var failedEndpoints: Set[TimedEndpoint]
  var suppressedEndpoints: Set[TimedEndpoint]
  var lateResponses: Set[TimedEndpoint]
  var currentEndpoint: Option[TimedEndpoint]
  var attemptStartedAt: Option[int]
  var currentAttemptTimeoutAt: Option[int]
  var responseDueAt: TimedEndpoint -> int
  var deliveryKey: Option[TimedDeliveryKey]
  var primaryAttemptKey: Option[TimedDeliveryKey]
  var fallbackAttemptKey: Option[TimedDeliveryKey]
  var attemptNonce: TimedEndpoint -> Option[TimedAttemptNonce]
  var acceptedEndpoint: Option[TimedEndpoint]
  var deliveredAt: Option[int]
  var acceptedAttemptTimeoutAt: Option[int]
  var replayCache: Set[TimedDeliveryKey]
  var dispatchCount: int
  var terminalError: Option[TimedDeliveryError]
  var retryLastDecision: RetryDecision

  pure val defaultOutcomes: TimedEndpoint -> EndpointOutcome =
    TimedEndpoints.mapBy(_ => EndpointDrops)

  pure val zeroResponseDueAt: TimedEndpoint -> int =
    TimedEndpoints.mapBy(_ => 0)

  pure val emptyNonce: TimedEndpoint -> Option[TimedAttemptNonce] =
    TimedEndpoints.mapBy(_ => None)

  pure def nonceFor(endpoint: TimedEndpoint): TimedAttemptNonce =
    match endpoint {
      | TimedPrimary => PrimaryAttemptNonce
      | TimedFallback => FallbackAttemptNonce
    }

  def canAcceptCurrentResponse(endpoint: TimedEndpoint): bool =
    all {
      phase == RetryAttempting,
      currentEndpoint == Some(endpoint),
      endpointOutcome.get(endpoint) == EndpointResponds,
      match currentAttemptTimeoutAt {
        | Some(timeoutAt) =>
            responseAcceptable(retryNow, responseDueAt.get(endpoint), timeoutAt, deadlineAt)
        | None => false
      },
    }

  def canMoveToFallbackAfter(endpoint: TimedEndpoint): bool =
    all {
      endpoint == TimedPrimary,
      hasFallback,
      not(attemptedEndpoints.contains(TimedFallback)),
      not(suppressedEndpoints.contains(TimedFallback)),
      canStartAttempt(retryNow, deadlineAt),
    }

  action init = all {
    retryNow' = 0,
    deadlineAt' = RequestDeadlineTicks,
    phase' = RetryIdle,
    hasFallback' = false,
    endpointOutcome' = defaultOutcomes,
    attemptedEndpoints' = Set(),
    timedOutEndpoints' = Set(),
    failedEndpoints' = Set(),
    suppressedEndpoints' = Set(),
    lateResponses' = Set(),
    currentEndpoint' = None,
    attemptStartedAt' = None,
    currentAttemptTimeoutAt' = None,
    responseDueAt' = zeroResponseDueAt,
    deliveryKey' = None,
    primaryAttemptKey' = None,
    fallbackAttemptKey' = None,
    attemptNonce' = emptyNonce,
    acceptedEndpoint' = None,
    deliveredAt' = None,
    acceptedAttemptTimeoutAt' = None,
    replayCache' = Set(),
    dispatchCount' = 0,
    terminalError' = None,
    retryLastDecision' = RetryNoDecision,
  }

  action tick: bool = all {
    retryNow < MaxNetworkTick,
    retryNow' = tickNext(retryNow),
    deadlineAt' = deadlineAt,
    phase' = phase,
    hasFallback' = hasFallback,
    endpointOutcome' = endpointOutcome,
    attemptedEndpoints' = attemptedEndpoints,
    timedOutEndpoints' = timedOutEndpoints,
    failedEndpoints' = failedEndpoints,
    suppressedEndpoints' = suppressedEndpoints,
    lateResponses' = lateResponses,
    currentEndpoint' = currentEndpoint,
    attemptStartedAt' = attemptStartedAt,
    currentAttemptTimeoutAt' = currentAttemptTimeoutAt,
    responseDueAt' = responseDueAt,
    deliveryKey' = deliveryKey,
    primaryAttemptKey' = primaryAttemptKey,
    fallbackAttemptKey' = fallbackAttemptKey,
    attemptNonce' = attemptNonce,
    acceptedEndpoint' = acceptedEndpoint,
    deliveredAt' = deliveredAt,
    acceptedAttemptTimeoutAt' = acceptedAttemptTimeoutAt,
    replayCache' = replayCache,
    dispatchCount' = dispatchCount,
    terminalError' = terminalError,
    retryLastDecision' = RetryTicked,
  }

  action beginRequest(
    fallbackConfigured: bool,
    primaryOutcome: EndpointOutcome,
    fallbackOutcome: EndpointOutcome,
    primaryDelay: int,
    fallbackDelay: int
  ): bool = all {
    phase == RetryIdle,
    PacketDelayChoices.contains(primaryDelay),
    PacketDelayChoices.contains(fallbackDelay),
    retryNow' = retryNow,
    deadlineAt' = RequestDeadlineTicks,
    phase' = RetryPrepared,
    hasFallback' = fallbackConfigured,
    endpointOutcome' =
      TimedEndpoints.mapBy(endpoint =>
        if (endpoint == TimedPrimary) { primaryOutcome } else { fallbackOutcome }
      ),
    attemptedEndpoints' = Set(),
    timedOutEndpoints' = Set(),
    failedEndpoints' = Set(),
    suppressedEndpoints' = Set(),
    lateResponses' = Set(),
    currentEndpoint' = None,
    attemptStartedAt' = None,
    currentAttemptTimeoutAt' = None,
    responseDueAt' =
      TimedEndpoints.mapBy(endpoint =>
        if (endpoint == TimedPrimary) {
          boundedAdd(retryNow, primaryDelay)
        } else {
          boundedAdd(retryNow, fallbackDelay)
        }
      ),
    deliveryKey' = Some(StableDeliveryKey),
    primaryAttemptKey' = None,
    fallbackAttemptKey' = None,
    attemptNonce' = emptyNonce,
    acceptedEndpoint' = None,
    deliveredAt' = None,
    acceptedAttemptTimeoutAt' = None,
    replayCache' = Set(),
    dispatchCount' = 0,
    terminalError' = None,
    retryLastDecision' = RequestPrepared,
  }

  action startAttempt(endpoint: TimedEndpoint): bool = {
    val isPrimaryStart = endpoint == TimedPrimary and phase == RetryPrepared
    val isFallbackStart = endpoint == TimedFallback and phase == RetryPrimaryTimedOut
    all {
      any { isPrimaryStart, isFallbackStart },
      canStartAttempt(retryNow, deadlineAt),
      not(attemptedEndpoints.contains(endpoint)),
      not(suppressedEndpoints.contains(endpoint)),
      retryNow' = retryNow,
      deadlineAt' = deadlineAt,
      phase' = RetryAttempting,
      hasFallback' = hasFallback,
      endpointOutcome' = endpointOutcome,
      attemptedEndpoints' = attemptedEndpoints.union(Set(endpoint)),
      timedOutEndpoints' = timedOutEndpoints,
      failedEndpoints' = failedEndpoints,
      suppressedEndpoints' = suppressedEndpoints,
      lateResponses' = lateResponses,
      currentEndpoint' = Some(endpoint),
      attemptStartedAt' = Some(retryNow),
      currentAttemptTimeoutAt' = Some(attemptTimeoutAt(retryNow, deadlineAt)),
      responseDueAt' = responseDueAt,
      deliveryKey' = deliveryKey,
      primaryAttemptKey' =
        if (endpoint == TimedPrimary) { deliveryKey } else { primaryAttemptKey },
      fallbackAttemptKey' =
        if (endpoint == TimedFallback) { deliveryKey } else { fallbackAttemptKey },
      attemptNonce' = attemptNonce.set(endpoint, Some(nonceFor(endpoint))),
      acceptedEndpoint' = acceptedEndpoint,
      deliveredAt' = deliveredAt,
      acceptedAttemptTimeoutAt' = acceptedAttemptTimeoutAt,
      replayCache' = replayCache,
      dispatchCount' = dispatchCount,
      terminalError' = terminalError,
      retryLastDecision' =
        if (endpoint == TimedPrimary) { PrimaryStarted } else { FallbackStarted },
    }
  }

  action acceptResponse(endpoint: TimedEndpoint): bool = all {
    canAcceptCurrentResponse(endpoint),
    retryNow' = retryNow,
    deadlineAt' = deadlineAt,
    phase' = RetryDelivered,
    hasFallback' = hasFallback,
    endpointOutcome' = endpointOutcome,
    attemptedEndpoints' = attemptedEndpoints,
    timedOutEndpoints' = timedOutEndpoints,
    failedEndpoints' = failedEndpoints,
    suppressedEndpoints' = suppressedEndpoints,
    lateResponses' = lateResponses,
    currentEndpoint' = None,
    attemptStartedAt' = attemptStartedAt,
    currentAttemptTimeoutAt' = currentAttemptTimeoutAt,
    responseDueAt' = responseDueAt,
    deliveryKey' = deliveryKey,
    primaryAttemptKey' = primaryAttemptKey,
    fallbackAttemptKey' = fallbackAttemptKey,
    attemptNonce' = attemptNonce,
    acceptedEndpoint' = Some(endpoint),
    deliveredAt' = Some(retryNow),
    acceptedAttemptTimeoutAt' = currentAttemptTimeoutAt,
    replayCache' = replayCache.union(Set(StableDeliveryKey)),
    dispatchCount' = dispatchCount + 1,
    terminalError' = None,
    retryLastDecision' = ResponseAccepted,
  }

  action timeoutAttempt(endpoint: TimedEndpoint): bool = {
    val canFallback = canMoveToFallbackAfter(endpoint)
    val expiredByDeadline = terminalDeadlineAllowed(retryNow, deadlineAt)
    all {
      phase == RetryAttempting,
      currentEndpoint == Some(endpoint),
      match currentAttemptTimeoutAt {
        | Some(timeoutAt) => retryNow >= timeoutAt or expiredByDeadline
        | None => false
      },
      retryNow' = retryNow,
      deadlineAt' = deadlineAt,
      phase' = if (canFallback) { RetryPrimaryTimedOut } else { RetryFailed },
      hasFallback' = hasFallback,
      endpointOutcome' = endpointOutcome,
      attemptedEndpoints' = attemptedEndpoints,
      timedOutEndpoints' = timedOutEndpoints.union(Set(endpoint)),
      failedEndpoints' = failedEndpoints.union(Set(endpoint)),
      suppressedEndpoints' = suppressedEndpoints,
      lateResponses' = lateResponses,
      currentEndpoint' = None,
      attemptStartedAt' = attemptStartedAt,
      currentAttemptTimeoutAt' = None,
      responseDueAt' = responseDueAt,
      deliveryKey' = deliveryKey,
      primaryAttemptKey' = primaryAttemptKey,
      fallbackAttemptKey' = fallbackAttemptKey,
      attemptNonce' = attemptNonce,
      acceptedEndpoint' = acceptedEndpoint,
      deliveredAt' = deliveredAt,
      acceptedAttemptTimeoutAt' = acceptedAttemptTimeoutAt,
      replayCache' = replayCache,
      dispatchCount' = dispatchCount,
      terminalError' =
        if (canFallback) {
          None
        } else if (expiredByDeadline) {
          Some(TimedDeadlineExpired)
        } else {
          Some(TimedAllEndpointsFailed)
        },
      retryLastDecision' = AttemptTimedOut,
    }
  }

  action observeIdentityMismatch(endpoint: TimedEndpoint): bool = {
    val canFallback = canMoveToFallbackAfter(endpoint)
    all {
      phase == RetryAttempting,
      currentEndpoint == Some(endpoint),
      endpointOutcome.get(endpoint) == EndpointIdentityMismatch,
      retryNow' = retryNow,
      deadlineAt' = deadlineAt,
      phase' = if (canFallback) { RetryPrimaryTimedOut } else { RetryFailed },
      hasFallback' = hasFallback,
      endpointOutcome' = endpointOutcome,
      attemptedEndpoints' = attemptedEndpoints,
      timedOutEndpoints' = timedOutEndpoints,
      failedEndpoints' = failedEndpoints.union(Set(endpoint)),
      suppressedEndpoints' = suppressedEndpoints.union(Set(endpoint)),
      lateResponses' = lateResponses,
      currentEndpoint' = None,
      attemptStartedAt' = attemptStartedAt,
      currentAttemptTimeoutAt' = None,
      responseDueAt' = responseDueAt,
      deliveryKey' = deliveryKey,
      primaryAttemptKey' = primaryAttemptKey,
      fallbackAttemptKey' = fallbackAttemptKey,
      attemptNonce' = attemptNonce,
      acceptedEndpoint' = acceptedEndpoint,
      deliveredAt' = deliveredAt,
      acceptedAttemptTimeoutAt' = acceptedAttemptTimeoutAt,
      replayCache' = replayCache,
      dispatchCount' = dispatchCount,
      terminalError' = if (canFallback) { None } else { Some(TimedAllEndpointsFailed) },
      retryLastDecision' = IdentityMismatchObserved,
    }
  }

  action ignoreLateResponse(endpoint: TimedEndpoint): bool = {
    val currentAttemptExpired =
      match currentAttemptTimeoutAt {
        | Some(timeoutAt) => retryNow >= timeoutAt
        | None => true
      }
    all {
      attemptedEndpoints.contains(endpoint),
      any {
        endpointOutcome.get(endpoint) == EndpointResponds,
        endpointOutcome.get(endpoint) == EndpointSlow,
      },
      responseDueAt.get(endpoint) <= retryNow,
      phase != RetryDelivered,
      any {
        terminalDeadlineAllowed(retryNow, deadlineAt),
        timedOutEndpoints.contains(endpoint),
        currentEndpoint != Some(endpoint),
        currentAttemptExpired,
      },
      retryNow' = retryNow,
      deadlineAt' = deadlineAt,
      phase' = phase,
      hasFallback' = hasFallback,
      endpointOutcome' = endpointOutcome,
      attemptedEndpoints' = attemptedEndpoints,
      timedOutEndpoints' = timedOutEndpoints,
      failedEndpoints' = failedEndpoints,
      suppressedEndpoints' = suppressedEndpoints,
      lateResponses' = lateResponses.union(Set(endpoint)),
      currentEndpoint' = currentEndpoint,
      attemptStartedAt' = attemptStartedAt,
      currentAttemptTimeoutAt' = currentAttemptTimeoutAt,
      responseDueAt' = responseDueAt,
      deliveryKey' = deliveryKey,
      primaryAttemptKey' = primaryAttemptKey,
      fallbackAttemptKey' = fallbackAttemptKey,
      attemptNonce' = attemptNonce,
      acceptedEndpoint' = acceptedEndpoint,
      deliveredAt' = deliveredAt,
      acceptedAttemptTimeoutAt' = acceptedAttemptTimeoutAt,
      replayCache' = replayCache,
      dispatchCount' = dispatchCount,
      terminalError' = terminalError,
      retryLastDecision' = LateResponseIgnored,
    }
  }

  action expireBeforeAttempt: bool = all {
    any {
      phase == RetryPrepared,
      phase == RetryPrimaryTimedOut,
    },
    terminalDeadlineAllowed(retryNow, deadlineAt),
    retryNow' = retryNow,
    deadlineAt' = deadlineAt,
    phase' = RetryFailed,
    hasFallback' = hasFallback,
    endpointOutcome' = endpointOutcome,
    attemptedEndpoints' = attemptedEndpoints,
    timedOutEndpoints' = timedOutEndpoints,
    failedEndpoints' = failedEndpoints,
    suppressedEndpoints' = suppressedEndpoints,
    lateResponses' = lateResponses,
    currentEndpoint' = None,
    attemptStartedAt' = attemptStartedAt,
    currentAttemptTimeoutAt' = currentAttemptTimeoutAt,
    responseDueAt' = responseDueAt,
    deliveryKey' = deliveryKey,
    primaryAttemptKey' = primaryAttemptKey,
    fallbackAttemptKey' = fallbackAttemptKey,
    attemptNonce' = attemptNonce,
    acceptedEndpoint' = acceptedEndpoint,
    deliveredAt' = deliveredAt,
    acceptedAttemptTimeoutAt' = acceptedAttemptTimeoutAt,
    replayCache' = replayCache,
    dispatchCount' = dispatchCount,
    terminalError' = Some(TimedDeadlineExpired),
    retryLastDecision' = DeadlineExpiredBeforeAttempt,
  }

  action duplicateReplay: bool = all {
    phase == RetryDelivered,
    replayCache.contains(StableDeliveryKey),
    retryNow' = retryNow,
    deadlineAt' = deadlineAt,
    phase' = phase,
    hasFallback' = hasFallback,
    endpointOutcome' = endpointOutcome,
    attemptedEndpoints' = attemptedEndpoints,
    timedOutEndpoints' = timedOutEndpoints,
    failedEndpoints' = failedEndpoints,
    suppressedEndpoints' = suppressedEndpoints,
    lateResponses' = lateResponses,
    currentEndpoint' = currentEndpoint,
    attemptStartedAt' = attemptStartedAt,
    currentAttemptTimeoutAt' = currentAttemptTimeoutAt,
    responseDueAt' = responseDueAt,
    deliveryKey' = deliveryKey,
    primaryAttemptKey' = primaryAttemptKey,
    fallbackAttemptKey' = fallbackAttemptKey,
    attemptNonce' = attemptNonce,
    acceptedEndpoint' = acceptedEndpoint,
    deliveredAt' = deliveredAt,
    acceptedAttemptTimeoutAt' = acceptedAttemptTimeoutAt,
    replayCache' = replayCache,
    dispatchCount' = dispatchCount,
    terminalError' = terminalError,
    retryLastDecision' = DuplicateReplayObserved,
  }

  action stutter: bool = all {
    retryNow' = retryNow,
    deadlineAt' = deadlineAt,
    phase' = phase,
    hasFallback' = hasFallback,
    endpointOutcome' = endpointOutcome,
    attemptedEndpoints' = attemptedEndpoints,
    timedOutEndpoints' = timedOutEndpoints,
    failedEndpoints' = failedEndpoints,
    suppressedEndpoints' = suppressedEndpoints,
    lateResponses' = lateResponses,
    currentEndpoint' = currentEndpoint,
    attemptStartedAt' = attemptStartedAt,
    currentAttemptTimeoutAt' = currentAttemptTimeoutAt,
    responseDueAt' = responseDueAt,
    deliveryKey' = deliveryKey,
    primaryAttemptKey' = primaryAttemptKey,
    fallbackAttemptKey' = fallbackAttemptKey,
    attemptNonce' = attemptNonce,
    acceptedEndpoint' = acceptedEndpoint,
    deliveredAt' = deliveredAt,
    acceptedAttemptTimeoutAt' = acceptedAttemptTimeoutAt,
    replayCache' = replayCache,
    dispatchCount' = dispatchCount,
    terminalError' = terminalError,
    retryLastDecision' = RetryStuttered,
  }

  action step = any {
    tick,
    {
      nondet fallbackConfigured = oneOf(Set(true, false))
      nondet primaryOutcome = oneOf(EndpointOutcomes)
      nondet fallbackOutcome = oneOf(EndpointOutcomes)
      nondet primaryDelay = oneOf(PacketDelayChoices)
      nondet fallbackDelay = oneOf(PacketDelayChoices)
      beginRequest(
        fallbackConfigured,
        primaryOutcome,
        fallbackOutcome,
        primaryDelay,
        fallbackDelay
      )
    },
    startAttempt(TimedPrimary),
    startAttempt(TimedFallback),
    {
      nondet endpoint = oneOf(TimedEndpoints)
      any {
        acceptResponse(endpoint),
        timeoutAttempt(endpoint),
        observeIdentityMismatch(endpoint),
        ignoreLateResponse(endpoint),
      }
    },
    expireBeforeAttempt,
    duplicateReplay,
    stutter,
  }
}
