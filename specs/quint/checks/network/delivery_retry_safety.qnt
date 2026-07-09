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


  def deliveredResponsesAreCached: bool =
    responseDelivered implies replayCache.contains(KeyA)


  def duplicateReplayDoesNotRedispatch: bool =
    all {
      dispatchCount <= 1,
      duplicateReplayObserved implies all {
        replayCache.contains(KeyA),
        dispatchCount == 1,
      },
    }


  def attemptKeysAreStable: bool =
    match primaryAttemptKey {
      | Some(primaryKey) =>
          match fallbackAttemptKey {
            | Some(fallbackKey) => primaryKey == fallbackKey
            | None => true
          }
      | None => fallbackAttemptKey == None
    }


  def attemptsUseOriginalDeliveryKey: bool = all {
    primaryAttemptKey == None or primaryAttemptKey == deliveryKey,
    fallbackAttemptKey == None or fallbackAttemptKey == deliveryKey,
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
      replayCache.size() == 0,
    }


  def deliveredIsTerminalWithoutError: bool =
    responseDelivered implies all {
      phase == Delivered,
      terminalError == None,
    }


  def safety: bool = all {
    responseRequiresSuccessfulAttempt,
    deliveredResponsesAreCached,
    duplicateReplayDoesNotRedispatch,
    attemptKeysAreStable,
    attemptsUseOriginalDeliveryKey,
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
