/// Cluster delivery retry and connectivity-feedback safety.
///
/// This module models the retry loop around route endpoint attempts in
/// `crates/sinomo-network/src/route.rs`: one preferred endpoint, one fallback
/// endpoint, stable inner delivery identity, connectivity observations, and
/// replay-cache suppression of duplicate delivery.
module network_delivery_retry {
  import common.* from "../../lib/common"

  type DeliveryEndpoint = PrimaryEndpoint | FallbackEndpoint
  type DeliveryKey = KeyA
    | KeyB
  type AttemptResult =
    AttemptSuccess
    | AttemptTimeout
    | TransientFailure
    | AuthDenied
    | IdentityMismatch
  type Feedback = FeedbackUnknown | FeedbackHealthy | FeedbackDegraded | FeedbackIdentityMismatch
  type DeliveryPhase = Idle | Prepared | PrimaryFailed | Delivered | Failed
  type DeliveryError = DeadlineExpired | TtlExpired | AllEndpointsFailed
  type Decision =
    NoDecision
    | RequestPrepared
    | PrimaryAttempted
    | FallbackAttempted
    | FailedBeforeAttempt
    | FailedAfterPrimaryNoFallback
    | DeadlineExpiredAfterPrimary
    | TtlExpiredAfterPrimary
    | DuplicateReplayReturned
    | TerminalStutter

  pure val Endpoints: Set[DeliveryEndpoint] = Set(PrimaryEndpoint, FallbackEndpoint)
  pure val AttemptResults: Set[AttemptResult] = Set(
    AttemptSuccess,
    AttemptTimeout,
    TransientFailure,
    AuthDenied,
    IdentityMismatch
  )

  var phase: DeliveryPhase
  var hasFallback: bool
  var budgetRemaining: bool
  var ttlAvailable: bool
  var primaryResult: AttemptResult
  var fallbackResult: AttemptResult
  var attemptedEndpoints: Set[DeliveryEndpoint]
  var failedEndpoints: Set[DeliveryEndpoint]
  var suppressedEndpoints: Set[DeliveryEndpoint]
  var endpointFeedback: DeliveryEndpoint -> Feedback
  var deliveryKey: Option[DeliveryKey]
  var primaryAttemptKey: Option[DeliveryKey]
  var fallbackAttemptKey: Option[DeliveryKey]
  var replayCache: Set[DeliveryKey]
  var dispatchCount: int
  var responseDelivered: bool
  var terminalError: Option[DeliveryError]
  var duplicateReplayObserved: bool
  var lastDecision: Decision

  pure val emptyFeedback: DeliveryEndpoint -> Feedback =
    Endpoints.mapBy(_ => FeedbackUnknown)

  pure def resultFailed(result: AttemptResult): bool =
    result != AttemptSuccess

  pure def feedbackFor(result: AttemptResult): Feedback =
    match result {
      | AttemptSuccess => FeedbackHealthy
      | IdentityMismatch => FeedbackIdentityMismatch
      | _ => FeedbackDegraded
    }

  pure def resultForEndpoint(
    endpoint: DeliveryEndpoint,
    primary: AttemptResult,
    fallback: AttemptResult
  ): AttemptResult =
    if (endpoint == PrimaryEndpoint) { primary } else { fallback }

  pure def terminalErrorForBudget(ttlOk: bool): DeliveryError =
    if (ttlOk) { DeadlineExpired } else { TtlExpired }

  action init = all {
    phase' = Idle,
    hasFallback' = false,
    budgetRemaining' = false,
    ttlAvailable' = false,
    primaryResult' = AttemptTimeout,
    fallbackResult' = AttemptTimeout,
    attemptedEndpoints' = Set(),
    failedEndpoints' = Set(),
    suppressedEndpoints' = Set(),
    endpointFeedback' = emptyFeedback,
    deliveryKey' = None,
    primaryAttemptKey' = None,
    fallbackAttemptKey' = None,
    replayCache' = Set(),
    dispatchCount' = 0,
    responseDelivered' = false,
    terminalError' = None,
    duplicateReplayObserved' = false,
    lastDecision' = NoDecision,
  }

  action beginRequest(
    fallbackConfigured: bool,
    initialBudgetRemaining: bool,
    initialTtlAvailable: bool,
    preferredResult: AttemptResult,
    backupResult: AttemptResult
  ): bool = all {
    phase == Idle,
    phase' = Prepared,
    hasFallback' = fallbackConfigured,
    budgetRemaining' = initialBudgetRemaining,
    ttlAvailable' = initialTtlAvailable,
    primaryResult' = preferredResult,
    fallbackResult' = backupResult,
    attemptedEndpoints' = Set(),
    failedEndpoints' = Set(),
    suppressedEndpoints' = Set(),
    endpointFeedback' = emptyFeedback,
    deliveryKey' = Some(KeyA),
    primaryAttemptKey' = None,
    fallbackAttemptKey' = None,
    replayCache' = Set(),
    dispatchCount' = 0,
    responseDelivered' = false,
    terminalError' = None,
    duplicateReplayObserved' = false,
    lastDecision' = RequestPrepared,
  }

  action failBeforeAttempt: bool = all {
    phase == Prepared,
    not(budgetRemaining and ttlAvailable),
    phase' = Failed,
    hasFallback' = hasFallback,
    budgetRemaining' = budgetRemaining,
    ttlAvailable' = ttlAvailable,
    primaryResult' = primaryResult,
    fallbackResult' = fallbackResult,
    attemptedEndpoints' = attemptedEndpoints,
    failedEndpoints' = failedEndpoints,
    suppressedEndpoints' = suppressedEndpoints,
    endpointFeedback' = endpointFeedback,
    deliveryKey' = deliveryKey,
    primaryAttemptKey' = primaryAttemptKey,
    fallbackAttemptKey' = fallbackAttemptKey,
    replayCache' = replayCache,
    dispatchCount' = dispatchCount,
    responseDelivered' = false,
    terminalError' = Some(terminalErrorForBudget(ttlAvailable)),
    duplicateReplayObserved' = duplicateReplayObserved,
    lastDecision' = FailedBeforeAttempt,
  }

  action attemptPrimary: bool = {
    val failed = resultFailed(primaryResult)
    all {
      phase == Prepared,
      budgetRemaining,
      ttlAvailable,
      phase' = if (failed) { PrimaryFailed } else { Delivered },
      hasFallback' = hasFallback,
      budgetRemaining' = budgetRemaining,
      ttlAvailable' = ttlAvailable,
      primaryResult' = primaryResult,
      fallbackResult' = fallbackResult,
      attemptedEndpoints' = attemptedEndpoints.union(Set(PrimaryEndpoint)),
      failedEndpoints' =
        if (failed) { failedEndpoints.union(Set(PrimaryEndpoint)) } else { failedEndpoints },
      suppressedEndpoints' =
        if (primaryResult == IdentityMismatch) {
          suppressedEndpoints.union(Set(PrimaryEndpoint))
        } else {
          suppressedEndpoints
        },
      endpointFeedback' =
        endpointFeedback.set(PrimaryEndpoint, feedbackFor(primaryResult)),
      deliveryKey' = deliveryKey,
      primaryAttemptKey' = deliveryKey,
      fallbackAttemptKey' = fallbackAttemptKey,
      replayCache' =
        if (failed) { replayCache } else { replayCache.union(Set(KeyA)) },
      dispatchCount' = if (failed) { dispatchCount } else { dispatchCount + 1 },
      responseDelivered' = not(failed),
      terminalError' = None,
      duplicateReplayObserved' = duplicateReplayObserved,
      lastDecision' = PrimaryAttempted,
    }
  }

  action failAfterPrimaryWithoutFallback: bool = all {
    phase == PrimaryFailed,
    not(hasFallback),
    phase' = Failed,
    hasFallback' = hasFallback,
    budgetRemaining' = budgetRemaining,
    ttlAvailable' = ttlAvailable,
    primaryResult' = primaryResult,
    fallbackResult' = fallbackResult,
    attemptedEndpoints' = attemptedEndpoints,
    failedEndpoints' = failedEndpoints,
    suppressedEndpoints' = suppressedEndpoints,
    endpointFeedback' = endpointFeedback,
    deliveryKey' = deliveryKey,
    primaryAttemptKey' = primaryAttemptKey,
    fallbackAttemptKey' = fallbackAttemptKey,
    replayCache' = replayCache,
    dispatchCount' = dispatchCount,
    responseDelivered' = false,
    terminalError' = Some(AllEndpointsFailed),
    duplicateReplayObserved' = duplicateReplayObserved,
    lastDecision' = FailedAfterPrimaryNoFallback,
  }

  action expireDeadlineAfterPrimary: bool = all {
    phase == PrimaryFailed,
    hasFallback,
    budgetRemaining,
    ttlAvailable,
    phase' = Failed,
    hasFallback' = hasFallback,
    budgetRemaining' = false,
    ttlAvailable' = ttlAvailable,
    primaryResult' = primaryResult,
    fallbackResult' = fallbackResult,
    attemptedEndpoints' = attemptedEndpoints,
    failedEndpoints' = failedEndpoints,
    suppressedEndpoints' = suppressedEndpoints,
    endpointFeedback' = endpointFeedback,
    deliveryKey' = deliveryKey,
    primaryAttemptKey' = primaryAttemptKey,
    fallbackAttemptKey' = fallbackAttemptKey,
    replayCache' = replayCache,
    dispatchCount' = dispatchCount,
    responseDelivered' = false,
    terminalError' = Some(DeadlineExpired),
    duplicateReplayObserved' = duplicateReplayObserved,
    lastDecision' = DeadlineExpiredAfterPrimary,
  }

  action expireTtlAfterPrimary: bool = all {
    phase == PrimaryFailed,
    hasFallback,
    budgetRemaining,
    ttlAvailable,
    phase' = Failed,
    hasFallback' = hasFallback,
    budgetRemaining' = budgetRemaining,
    ttlAvailable' = false,
    primaryResult' = primaryResult,
    fallbackResult' = fallbackResult,
    attemptedEndpoints' = attemptedEndpoints,
    failedEndpoints' = failedEndpoints,
    suppressedEndpoints' = suppressedEndpoints,
    endpointFeedback' = endpointFeedback,
    deliveryKey' = deliveryKey,
    primaryAttemptKey' = primaryAttemptKey,
    fallbackAttemptKey' = fallbackAttemptKey,
    replayCache' = replayCache,
    dispatchCount' = dispatchCount,
    responseDelivered' = false,
    terminalError' = Some(TtlExpired),
    duplicateReplayObserved' = duplicateReplayObserved,
    lastDecision' = TtlExpiredAfterPrimary,
  }

  action attemptFallback: bool = {
    val failed = resultFailed(fallbackResult)
    all {
      phase == PrimaryFailed,
      hasFallback,
      budgetRemaining,
      ttlAvailable,
      not(suppressedEndpoints.contains(FallbackEndpoint)),
      phase' = if (failed) { Failed } else { Delivered },
      hasFallback' = hasFallback,
      budgetRemaining' = budgetRemaining,
      ttlAvailable' = ttlAvailable,
      primaryResult' = primaryResult,
      fallbackResult' = fallbackResult,
      attemptedEndpoints' = attemptedEndpoints.union(Set(FallbackEndpoint)),
      failedEndpoints' =
        if (failed) { failedEndpoints.union(Set(FallbackEndpoint)) } else { failedEndpoints },
      suppressedEndpoints' =
        if (fallbackResult == IdentityMismatch) {
          suppressedEndpoints.union(Set(FallbackEndpoint))
        } else {
          suppressedEndpoints
        },
      endpointFeedback' =
        endpointFeedback.set(FallbackEndpoint, feedbackFor(fallbackResult)),
      deliveryKey' = deliveryKey,
      primaryAttemptKey' = primaryAttemptKey,
      fallbackAttemptKey' = deliveryKey,
      replayCache' =
        if (failed) { replayCache } else { replayCache.union(Set(KeyA)) },
      dispatchCount' = if (failed) { dispatchCount } else { dispatchCount + 1 },
      responseDelivered' = not(failed),
      terminalError' = if (failed) { Some(AllEndpointsFailed) } else { None },
      duplicateReplayObserved' = duplicateReplayObserved,
      lastDecision' = FallbackAttempted,
    }
  }

  action duplicateReplay: bool = all {
    phase == Delivered,
    replayCache.contains(KeyA),
    phase' = phase,
    hasFallback' = hasFallback,
    budgetRemaining' = budgetRemaining,
    ttlAvailable' = ttlAvailable,
    primaryResult' = primaryResult,
    fallbackResult' = fallbackResult,
    attemptedEndpoints' = attemptedEndpoints,
    failedEndpoints' = failedEndpoints,
    suppressedEndpoints' = suppressedEndpoints,
    endpointFeedback' = endpointFeedback,
    deliveryKey' = deliveryKey,
    primaryAttemptKey' = primaryAttemptKey,
    fallbackAttemptKey' = fallbackAttemptKey,
    replayCache' = replayCache,
    dispatchCount' = dispatchCount,
    responseDelivered' = responseDelivered,
    terminalError' = terminalError,
    duplicateReplayObserved' = true,
    lastDecision' = DuplicateReplayReturned,
  }

  action stutterTerminal: bool = all {
    any {
      phase == Delivered,
      phase == Failed,
    },
    phase' = phase,
    hasFallback' = hasFallback,
    budgetRemaining' = budgetRemaining,
    ttlAvailable' = ttlAvailable,
    primaryResult' = primaryResult,
    fallbackResult' = fallbackResult,
    attemptedEndpoints' = attemptedEndpoints,
    failedEndpoints' = failedEndpoints,
    suppressedEndpoints' = suppressedEndpoints,
    endpointFeedback' = endpointFeedback,
    deliveryKey' = deliveryKey,
    primaryAttemptKey' = primaryAttemptKey,
    fallbackAttemptKey' = fallbackAttemptKey,
    replayCache' = replayCache,
    dispatchCount' = dispatchCount,
    responseDelivered' = responseDelivered,
    terminalError' = terminalError,
    duplicateReplayObserved' = duplicateReplayObserved,
    lastDecision' = TerminalStutter,
  }

  action step = any {
    {
      nondet fallbackConfigured = oneOf(Set(true, false))
      nondet initialBudgetRemaining = oneOf(Set(true, false))
      nondet initialTtlAvailable = oneOf(Set(true, false))
      nondet preferredResult = oneOf(AttemptResults)
      nondet backupResult = oneOf(AttemptResults)
      beginRequest(
        fallbackConfigured,
        initialBudgetRemaining,
        initialTtlAvailable,
        preferredResult,
        backupResult
      )
    },
    failBeforeAttempt,
    attemptPrimary,
    failAfterPrimaryWithoutFallback,
    expireDeadlineAfterPrimary,
    expireTtlAfterPrimary,
    attemptFallback,
    duplicateReplay,
    stutterTerminal,
  }
}
