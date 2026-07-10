/// Shared bounded logical time for network models.
module network_time {
  pure val MaxNetworkTick: int = 6
  pure val RequestDeadlineTicks: int = 5
  pure val AttemptTimeoutTicks: int = 2
  pure val PacketLifetimeTicks: int = 4

  pure val NetworkTicks: Set[int] = Set(0, 1, 2, 3, 4, 5, 6)
  pure val PacketDelayChoices: Set[int] = Set(1, 2, 3)

  pure def validTick(tick: int): bool =
    tick >= 0 and tick <= MaxNetworkTick

  pure def boundedAdd(tick: int, delta: int): int =
    if (tick + delta > MaxNetworkTick) { MaxNetworkTick } else { tick + delta }

  pure def minTime(left: int, right: int): int =
    if (left <= right) { left } else { right }

  pure def tickNext(tick: int): int =
    boundedAdd(tick, 1)

  pure def beforeDeadline(now: int, deadlineAt: int): bool =
    now < deadlineAt

  pure def deadlineExpired(now: int, deadlineAt: int): bool =
    now >= deadlineAt
}
