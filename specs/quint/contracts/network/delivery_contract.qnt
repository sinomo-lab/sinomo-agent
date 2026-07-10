/// Shared timed-delivery contract predicates.
module delivery_contract {
  import network_time.* from "../../lib/network_time"

  type TimedDeliveryError =
    | TimedDeadlineExpired
    | TimedAttemptTimedOut
    | TimedAllEndpointsFailed
    | TimedTtlExpired

  pure def attemptTimeoutAt(startedAt: int, deadlineAt: int): int =
    minTime(boundedAdd(startedAt, AttemptTimeoutTicks), deadlineAt)

  pure def packetExpiresAt(sentAt: int): int =
    boundedAdd(sentAt, PacketLifetimeTicks)

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
