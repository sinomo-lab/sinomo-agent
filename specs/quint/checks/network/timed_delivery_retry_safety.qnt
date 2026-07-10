/// Invariant checks for `network_timed_delivery_retry`.
module network_timed_delivery_retry_safety {
  import common.* from "../../lib/common"
  import network_time.* from "../../lib/network_time"
  import delivery_contract.* from "../../contracts/network/delivery_contract"
  import network_timed_delivery_retry.* from "../../models/network/timed_delivery_retry"

  def timeAndDeadlineAreBounded: bool = all {
    validTick(retryNow),
    validTick(deadlineAt),
    deadlineAt == RequestDeadlineTicks,
  }

  def attemptsReferToPreparedRequest: bool =
    attemptedEndpoints.size() > 0 implies deliveryKey == Some(StableDeliveryKey)

  def attemptTimeoutNeverExtendsDeadline: bool =
    match currentAttemptTimeoutAt {
      | Some(timeoutAt) => timeoutAt <= deadlineAt
      | None => true
    }

  def acceptedAttemptTimeoutNeverExtendsDeadline: bool =
    match acceptedAttemptTimeoutAt {
      | Some(timeoutAt) => timeoutAt <= deadlineAt
      | None => true
    }

  def attemptsUseStableDeliveryKey: bool = all {
    primaryAttemptKey == None or primaryAttemptKey == deliveryKey,
    fallbackAttemptKey == None or fallbackAttemptKey == deliveryKey,
  }

  def attemptNoncesAreDistinct: bool =
    match attemptNonce.get(TimedPrimary) {
      | Some(primaryNonce) =>
          match attemptNonce.get(TimedFallback) {
            | Some(fallbackNonce) => primaryNonce != fallbackNonce
            | None => true
          }
      | None => true
    }

  def deliveredBeforeDeadlineAndAttemptTimeout: bool =
    phase == RetryDelivered implies all {
      terminalError == None,
      match deliveredAt {
        | Some(doneAt) => all {
            doneAt < deadlineAt,
            match acceptedAttemptTimeoutAt {
              | Some(timeoutAt) => doneAt < timeoutAt
              | None => false
            },
          }
        | None => false
      },
    }

  def acceptedResponseWasDueAndSuccessful: bool =
    phase == RetryDelivered implies
      match acceptedEndpoint {
        | Some(endpoint) =>
            match deliveredAt {
              | Some(doneAt) => all {
                  endpointOutcome.get(endpoint) == EndpointResponds,
                  responseDueAt.get(endpoint) <= doneAt,
                  attemptedEndpoints.contains(endpoint),
                }
              | None => false
            }
        | None => false
      }

  def lateResponsesDoNotDispatch: bool =
    lateResponses.forall(endpoint => acceptedEndpoint != Some(endpoint))

  def terminalDeadlineRequiresExpiredClock: bool =
    terminalError == Some(TimedDeadlineExpired) implies terminalDeadlineAllowed(retryNow, deadlineAt)

  def allEndpointsFailedDoesNotSkipUsableFallback: bool =
    all {
      phase == RetryFailed,
      terminalError == Some(TimedAllEndpointsFailed),
      hasFallback,
      not(attemptedEndpoints.contains(TimedFallback)),
    } implies any {
      not(canStartAttempt(retryNow, deadlineAt)),
      suppressedEndpoints.contains(TimedFallback),
    }

  def primaryFailureWithFallbackDoesNotTerminallyFailEarly: bool =
    all {
      hasFallback,
      failedEndpoints.contains(TimedPrimary),
      not(attemptedEndpoints.contains(TimedFallback)),
      not(suppressedEndpoints.contains(TimedFallback)),
      canStartAttempt(retryNow, deadlineAt),
    } implies terminalError != Some(TimedAllEndpointsFailed)

  def failedAndSuppressedEndpointsWereAttempted: bool = all {
    failedEndpoints.subseteq(attemptedEndpoints),
    timedOutEndpoints.subseteq(attemptedEndpoints),
    suppressedEndpoints.subseteq(attemptedEndpoints),
  }

  def suppressedEndpointsAreIdentityMismatches: bool =
    suppressedEndpoints.forall(endpoint =>
      endpointOutcome.get(endpoint) == EndpointIdentityMismatch
    )

  def deliveredResponsesAreCached: bool =
    phase == RetryDelivered implies replayCache.contains(StableDeliveryKey)

  def duplicateReplayDoesNotRedispatch: bool = all {
    dispatchCount <= 1,
    retryLastDecision == DuplicateReplayObserved implies all {
      replayCache.contains(StableDeliveryKey),
      dispatchCount == 1,
    },
  }

  def safety: bool = all {
    timeAndDeadlineAreBounded,
    attemptsReferToPreparedRequest,
    attemptTimeoutNeverExtendsDeadline,
    acceptedAttemptTimeoutNeverExtendsDeadline,
    attemptsUseStableDeliveryKey,
    attemptNoncesAreDistinct,
    deliveredBeforeDeadlineAndAttemptTimeout,
    acceptedResponseWasDueAndSuccessful,
    lateResponsesDoNotDispatch,
    terminalDeadlineRequiresExpiredClock,
    allEndpointsFailedDoesNotSkipUsableFallback,
    primaryFailureWithFallbackDoesNotTerminallyFailEarly,
    failedAndSuppressedEndpointsWereAttempted,
    suppressedEndpointsAreIdentityMismatches,
    deliveredResponsesAreCached,
    duplicateReplayDoesNotRedispatch,
  }
}
