/// Invariant checks for `network_delivery_retry`.
module network_delivery_retry_safety {
  import common.* from "../../lib/common"
  import network_delivery_retry.* from "../../models/network/delivery_retry"

  def responseRequiresSuccessfulAttempt: bool =
    responseDelivered implies any {
      attemptedEndpoints.contains(PrimaryEndpoint)
        and primaryResult == AttemptSuccess,
      attemptedEndpoints.contains(FallbackEndpoint)
        and fallbackResult == AttemptSuccess,
    }


  def attemptedOperationsAreStable: bool = all {
    primaryAttemptOperationId == None or primaryAttemptOperationId == operationId,
    fallbackAttemptOperationId == None or fallbackAttemptOperationId == operationId,
  }


  def attemptsCarryFreshEndpointAuthentication: bool = all {
    attemptedEndpoints.contains(PrimaryEndpoint)
      iff primaryAttemptNonce == Some(PrimaryAttemptNonce),
    attemptedEndpoints.contains(FallbackEndpoint)
      iff fallbackAttemptNonce == Some(FallbackAttemptNonce),
    all {
      primaryAttemptNonce != None,
      fallbackAttemptNonce != None,
    } implies primaryAttemptNonce != fallbackAttemptNonce,
  }


  def multipleDispatchesKeepOperationButRefreshAuthentication: bool =
    dispatchCount > 1 implies all {
      primaryAttemptOperationId == operationId,
      fallbackAttemptOperationId == operationId,
      primaryAttemptNonce != fallbackAttemptNonce,
    }


  /// This is an endpoint-attempt bound, not an at-most-once domain-effect claim.
  /// A primary timeout followed by a fallback attempt reaches dispatchCount = 2.
  def dispatchesMatchEndpointAttempts: bool = all {
    dispatchCount == attemptedEndpoints.size(),
    dispatchCount <= Endpoints.size(),
  }


  def failedEndpointsWereAttempted: bool =
    failedEndpoints.subseteq(attemptedEndpoints)


  def identityMismatchEndpointsAreSuppressed: bool =
    Endpoints.forall(endpoint =>
      all {
        attemptedEndpoints.contains(endpoint),
        resultForEndpoint(endpoint, primaryResult, fallbackResult) == IdentityMismatch,
      } implies all {
        suppressedEndpoints.contains(endpoint),
        endpointFeedback.get(endpoint) == FeedbackIdentityMismatch,
      }
    )


  def suppressedEndpointsWereAttempted: bool =
    suppressedEndpoints.subseteq(attemptedEndpoints)


  def connectivityFeedbackMatchesAttempts: bool =
    Endpoints.forall(endpoint =>
      attemptedEndpoints.contains(endpoint) implies
        endpointFeedback.get(endpoint)
          == feedbackFor(resultForEndpoint(endpoint, primaryResult, fallbackResult))
    )


  def terminalBudgetFailuresDoNotDeliver: bool =
    match terminalError {
      | Some(DeadlineExpired) => not(responseDelivered)
      | Some(TtlExpired) => not(responseDelivered)
      | _ => true
    }


  def allEndpointsFailedWithFallbackAttemptsFallback: bool =
    all {
      phase == Failed,
      terminalError == Some(AllEndpointsFailed),
      hasFallback,
      attemptedEndpoints.contains(PrimaryEndpoint),
    } implies attemptedEndpoints.contains(FallbackEndpoint)


  def fallbackAvailableWhileBudgetRemainsDoesNotTerminallyFailEarly: bool =
    all {
      phase == Failed,
      hasFallback,
      budgetRemaining,
      ttlAvailable,
      attemptedEndpoints.contains(PrimaryEndpoint),
      not(attemptedEndpoints.contains(FallbackEndpoint)),
    } implies terminalError != Some(AllEndpointsFailed)


  def ttlOrDeadlineFailureCannotSucceed: bool =
    all {
      terminalError != None,
      any {
        terminalError == Some(DeadlineExpired),
        terminalError == Some(TtlExpired),
      },
    } implies all {
      phase == Failed,
      not(responseDelivered),
    }


  def deliveredIsTerminalWithoutError: bool =
    responseDelivered implies all {
      phase == Delivered,
      terminalError == None,
    }


  /// Witness: the preferred endpoint carries the request's operation identity
  /// and a per-attempt authentication nonce.
  def witnessPrimaryAttemptIdentityAndNonce: bool = all {
    lastDecision == PrimaryAttempted,
    attemptedEndpoints.contains(PrimaryEndpoint),
    primaryAttemptOperationId == operationId,
    primaryAttemptNonce == Some(PrimaryAttemptNonce),
  }


  /// Witness: retry can dispatch the same domain operation twice, while the
  /// authentication nonce is fresh for the fallback endpoint.
  def witnessFallbackRetryStableOperationFreshNonce: bool = all {
    lastDecision == FallbackAttempted,
    dispatchCount == 2,
    primaryAttemptOperationId == operationId,
    fallbackAttemptOperationId == operationId,
    primaryAttemptNonce == Some(PrimaryAttemptNonce),
    fallbackAttemptNonce == Some(FallbackAttemptNonce),
    primaryAttemptNonce != fallbackAttemptNonce,
  }


  def safety: bool = all {
    responseRequiresSuccessfulAttempt,
    attemptedOperationsAreStable,
    attemptsCarryFreshEndpointAuthentication,
    multipleDispatchesKeepOperationButRefreshAuthentication,
    dispatchesMatchEndpointAttempts,
    failedEndpointsWereAttempted,
    identityMismatchEndpointsAreSuppressed,
    suppressedEndpointsWereAttempted,
    connectivityFeedbackMatchesAttempts,
    terminalBudgetFailuresDoNotDeliver,
    allEndpointsFailedWithFallbackAttemptsFallback,
    fallbackAvailableWhileBudgetRemainsDoesNotTerminallyFailEarly,
    ttlOrDeadlineFailureCannotSucceed,
    deliveredIsTerminalWithoutError,
  }
}
