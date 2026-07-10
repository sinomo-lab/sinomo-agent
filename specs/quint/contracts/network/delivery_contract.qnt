/// Shared timed-delivery contract predicates.
module delivery_contract {
  import network_time.* from "../../lib/network_time"

  /// Operation identity is stable across endpoint retries, while the
  /// authentication nonce identifies one concrete transport attempt.
  type TimedEndpoint = TimedPrimary | TimedFallback
  type TimedOperationId = StableOperationId | OtherOperationId
  type TimedAttemptNonce = PrimaryAttemptNonce | FallbackAttemptNonce

  type TimedDeliveryError =
    | TimedDeadlineExpired
    | TimedAttemptTimedOut
    | TimedAllEndpointsFailed
    | TimedTtlExpired

  pure val TimedEndpoints: Set[TimedEndpoint] = Set(TimedPrimary, TimedFallback)

  pure def nonceFor(endpoint: TimedEndpoint): TimedAttemptNonce =
    match endpoint {
      | TimedPrimary => PrimaryAttemptNonce
      | TimedFallback => FallbackAttemptNonce
    }

  pure def requestDeadlineAt(startedAt: int): int =
    durationEndAt(startedAt, RequestDeadlineTicks)

  pure def requestDeadlineFits(startedAt: int): bool =
    requestDeadlineAt(startedAt) <= MaxNetworkTick

  pure def attemptTimeoutAt(startedAt: int, deadlineAt: int): int =
    minTime(durationEndAt(startedAt, AttemptTimeoutTicks), deadlineAt)

  pure def packetExpiresAt(sentAt: int): int =
    durationEndAt(sentAt, PacketLifetimeTicks)

  pure def packetAlive(now: int, expiresAt: int): bool =
    now <= expiresAt

  pure def responseAcceptable(
    now: int,
    responseDueAt: int,
    attemptExpiresAt: int,
    deadlineAt: int
  ): bool =
    all {
      now >= responseDueAt,
      now < attemptExpiresAt,
      beforeDeadline(now, deadlineAt),
    }

  pure def canStartAttempt(now: int, deadlineAt: int): bool =
    beforeDeadline(now, deadlineAt)

  pure def terminalDeadlineAllowed(now: int, deadlineAt: int): bool =
    deadlineExpired(now, deadlineAt)
}
