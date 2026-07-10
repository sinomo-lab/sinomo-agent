/// Cluster delivery retry and connectivity-feedback safety.
///
/// This module models the retry loop around route endpoint attempts in
/// `crates/sinomo-network/src/route.rs`: one preferred endpoint, one fallback
/// endpoint, a stable domain operation identity, fresh authentication for each
/// endpoint attempt, and connectivity observations.
///
/// A dispatched attempt may have reached the domain even when its response is
/// lost. Therefore retrying may dispatch the same operation more than once.
/// Generic object RPC deliberately does not model a replay cache or an
/// at-most-once domain effect; side-effecting routes must enforce idempotency at
/// their own domain boundary.
///
/// Evidence class: current-behavior refinement of the bounded endpoint retry
/// loop. Domain idempotency is an explicit lower-layer obligation.
module network_delivery_retry {
  import common.* from "../../lib/common"

  type DeliveryEndpoint = PrimaryEndpoint | FallbackEndpoint
  type OperationId = OperationA | OperationB
  type AttemptNonce = PrimaryAttemptNonce | FallbackAttemptNonce
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
    | TerminalStutter

  pure val Endpoints: Set[DeliveryEndpoint] = Set(PrimaryEndpoint, FallbackEndpoint)
  pure val OperationIds: Set[OperationId] = Set(OperationA, OperationB)
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
  var operationId: Option[OperationId]
  var primaryAttemptOperationId: Option[OperationId]
  var fallbackAttemptOperationId: Option[OperationId]
  var primaryAttemptNonce: Option[AttemptNonce]
  var fallbackAttemptNonce: Option[AttemptNonce]
  var dispatchCount: int
  var responseDelivered: bool
  var terminalError: Option[DeliveryError]
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
    operationId' = None,
    primaryAttemptOperationId' = None,
    fallbackAttemptOperationId' = None,
    primaryAttemptNonce' = None,
    fallbackAttemptNonce' = None,
    dispatchCount' = 0,
    responseDelivered' = false,
    terminalError' = None,
    lastDecision' = NoDecision,
  }

  action beginRequest(
    fallbackConfigured: bool,
    initialBudgetRemaining: bool,
    initialTtlAvailable: bool,
    preferredResult: AttemptResult,
    backupResult: AttemptResult,
    requestedOperationId: OperationId
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
    operationId' = Some(requestedOperationId),
    primaryAttemptOperationId' = None,
    fallbackAttemptOperationId' = None,
    primaryAttemptNonce' = None,
    fallbackAttemptNonce' = None,
    dispatchCount' = 0,
    responseDelivered' = false,
    terminalError' = None,
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
    operationId' = operationId,
    primaryAttemptOperationId' = primaryAttemptOperationId,
    fallbackAttemptOperationId' = fallbackAttemptOperationId,
    primaryAttemptNonce' = primaryAttemptNonce,
    fallbackAttemptNonce' = fallbackAttemptNonce,
    dispatchCount' = dispatchCount,
    responseDelivered' = false,
    terminalError' = Some(terminalErrorForBudget(ttlAvailable)),
    lastDecision' = FailedBeforeAttempt,
  }

  action attemptPrimary: bool = {
    val failed = resultFailed(primaryResult)
    all {
      phase == Prepared,
      budgetRemaining,
      ttlAvailable,
      operationId != None,
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
      operationId' = operationId,
      primaryAttemptOperationId' = operationId,
      fallbackAttemptOperationId' = fallbackAttemptOperationId,
      primaryAttemptNonce' = Some(PrimaryAttemptNonce),
      fallbackAttemptNonce' = fallbackAttemptNonce,
      dispatchCount' = dispatchCount + 1,
      responseDelivered' = not(failed),
      terminalError' = None,
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
    operationId' = operationId,
    primaryAttemptOperationId' = primaryAttemptOperationId,
    fallbackAttemptOperationId' = fallbackAttemptOperationId,
    primaryAttemptNonce' = primaryAttemptNonce,
    fallbackAttemptNonce' = fallbackAttemptNonce,
    dispatchCount' = dispatchCount,
    responseDelivered' = false,
    terminalError' = Some(AllEndpointsFailed),
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
    operationId' = operationId,
    primaryAttemptOperationId' = primaryAttemptOperationId,
    fallbackAttemptOperationId' = fallbackAttemptOperationId,
    primaryAttemptNonce' = primaryAttemptNonce,
    fallbackAttemptNonce' = fallbackAttemptNonce,
    dispatchCount' = dispatchCount,
    responseDelivered' = false,
    terminalError' = Some(DeadlineExpired),
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
    operationId' = operationId,
    primaryAttemptOperationId' = primaryAttemptOperationId,
    fallbackAttemptOperationId' = fallbackAttemptOperationId,
    primaryAttemptNonce' = primaryAttemptNonce,
    fallbackAttemptNonce' = fallbackAttemptNonce,
    dispatchCount' = dispatchCount,
    responseDelivered' = false,
    terminalError' = Some(TtlExpired),
    lastDecision' = TtlExpiredAfterPrimary,
  }

  action attemptFallback: bool = {
    val failed = resultFailed(fallbackResult)
    all {
      phase == PrimaryFailed,
      hasFallback,
      budgetRemaining,
      ttlAvailable,
      operationId != None,
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
      operationId' = operationId,
      primaryAttemptOperationId' = primaryAttemptOperationId,
      fallbackAttemptOperationId' = operationId,
      primaryAttemptNonce' = primaryAttemptNonce,
      fallbackAttemptNonce' = Some(FallbackAttemptNonce),
      dispatchCount' = dispatchCount + 1,
      responseDelivered' = not(failed),
      terminalError' = if (failed) { Some(AllEndpointsFailed) } else { None },
      lastDecision' = FallbackAttempted,
    }
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
    operationId' = operationId,
    primaryAttemptOperationId' = primaryAttemptOperationId,
    fallbackAttemptOperationId' = fallbackAttemptOperationId,
    primaryAttemptNonce' = primaryAttemptNonce,
    fallbackAttemptNonce' = fallbackAttemptNonce,
    dispatchCount' = dispatchCount,
    responseDelivered' = responseDelivered,
    terminalError' = terminalError,
    lastDecision' = TerminalStutter,
  }

  action step = any {
    {
      nondet fallbackConfigured = oneOf(Set(true, false))
      nondet initialBudgetRemaining = oneOf(Set(true, false))
      nondet initialTtlAvailable = oneOf(Set(true, false))
      nondet preferredResult = oneOf(AttemptResults)
      nondet backupResult = oneOf(AttemptResults)
      nondet requestedOperationId = oneOf(OperationIds)
      beginRequest(
        fallbackConfigured,
        initialBudgetRemaining,
        initialTtlAvailable,
        preferredResult,
        backupResult,
        requestedOperationId
      )
    },
    failBeforeAttempt,
    attemptPrimary,
    failAfterPrimaryWithoutFallback,
    expireDeadlineAfterPrimary,
    expireTtlAfterPrimary,
    attemptFallback,
    stutterTerminal,
  }
}
