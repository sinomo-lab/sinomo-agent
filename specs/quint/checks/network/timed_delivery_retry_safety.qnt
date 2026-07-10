/// Invariant checks for `network_timed_delivery_retry`.
module network_timed_delivery_retry_safety {
  import common.* from "../../lib/common"
  import network_time.* from "../../lib/network_time"
  import delivery_contract.* from "../../contracts/network/delivery_contract"
  import network_timed_delivery_retry.* from "../../models/network/timed_delivery_retry"

  def timeAndDeadlineAreBounded: bool = all {
    validTick(retryNow),
    validScheduledTick(deadlineAt),
    match requestStartedAt {
      | Some(startedAt) => all {
          validTick(startedAt),
          startedAt <= deadlineAt,
          deadlineAt == requestDeadlineAt(startedAt),
          deadlineAt - startedAt == RequestDeadlineTicks,
        }
      | None => all {
          phase == RetryIdle,
          deadlineAt == 0,
        }
    },
  }

  /// Regression guard: neither idle ticks nor preparation latency may move the
  /// deadline origin back to model initialization.
  def requestDeadlineUsesRequestStart: bool =
    match requestStartedAt {
      | Some(startedAt) => deadlineAt == durationEndAt(startedAt, RequestDeadlineTicks)
      | None => true
    }

  def attemptsReferToPreparedRequest: bool =
    attemptedEndpoints.size() > 0 implies deliveryKey == Some(StableOperationId)

  /// Regression guard: a fallback response cannot already be due before that
  /// fallback attempt has started.
  def responsesUseTheirAttemptStart: bool =
    attemptedEndpoints.forall(endpoint =>
      match attemptStartedAt.get(endpoint) {
        | Some(startedAt) => all {
            validTick(startedAt),
            startedAt < deadlineAt,
            PacketDelayChoices.contains(responseDelay.get(endpoint)),
            validScheduledTick(responseDueAt.get(endpoint)),
            responseDueAt.get(endpoint) == durationEndAt(
              startedAt,
              responseDelay.get(endpoint)
            ),
            startedAt <= responseDueAt.get(endpoint),
          }
        | None => false
      }
    )

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

  def attemptsUseStableOperationId: bool = all {
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
                  match attemptStartedAt.get(endpoint) {
                    | Some(startedAt) => startedAt <= doneAt
                    | None => false
                  },
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

  def observedDuplicateResponsesWereAttempted: bool =
    duplicateResponses.subseteq(attemptedEndpoints)

  /// Named witnesses are exercised by the formal harness to keep important
  /// success and failure branches from becoming vacuous.
  def witnessPrimaryDelivery: bool = all {
    phase == RetryDelivered,
    acceptedEndpoint == Some(TimedPrimary),
  }

  def witnessFallbackDelivery: bool = all {
    phase == RetryDelivered,
    acceptedEndpoint == Some(TimedFallback),
  }

  def witnessTimeoutFailure: bool = all {
    phase == RetryFailed,
    timedOutEndpoints.size() > 0,
    terminalError == Some(TimedAllEndpointsFailed),
  }

  def witnessDeadlineFailure: bool = all {
    phase == RetryFailed,
    terminalError == Some(TimedDeadlineExpired),
  }

  def witnessLateResponse: bool =
    lateResponses.size() > 0

  def safety: bool = all {
    timeAndDeadlineAreBounded,
    requestDeadlineUsesRequestStart,
    attemptsReferToPreparedRequest,
    responsesUseTheirAttemptStart,
    attemptTimeoutNeverExtendsDeadline,
    acceptedAttemptTimeoutNeverExtendsDeadline,
    attemptsUseStableOperationId,
    attemptNoncesAreDistinct,
    deliveredBeforeDeadlineAndAttemptTimeout,
    acceptedResponseWasDueAndSuccessful,
    lateResponsesDoNotDispatch,
    terminalDeadlineRequiresExpiredClock,
    allEndpointsFailedDoesNotSkipUsableFallback,
    primaryFailureWithFallbackDoesNotTerminallyFailEarly,
    failedAndSuppressedEndpointsWereAttempted,
    suppressedEndpointsAreIdentityMismatches,
    observedDuplicateResponsesWereAttempted,
  }
}
