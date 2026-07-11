/// DFS whole-file POSIX advisory-lock contract.
///
/// This target-hardening model defines a session-scoped, cooperative lock
/// manager for one file. `SharedLock` corresponds to a read lock and
/// `ExclusiveLock` to a write lock. The locks are advisory: the model specifies
/// lock-manager outcomes, not mandatory rejection of file I/O performed by a
/// client that bypasses the lock API.
///
/// Every grant carries both the current metadata-authority epoch and a unique
/// grant id. An authority-epoch change fences all prior grants and pending
/// requests. A session that can still reach the current authority renews its
/// lease on every logical tick; only a session whose renewal path is unavailable
/// counts down toward expiry. Explicit unlock, session close, session crash,
/// and expiry all unblock a compatible waiter. The top-level transition gives
/// an unblocked waiter strict priority, which turns cleanup into a bounded
/// safety transition instead of relying on scheduler fairness after the
/// blocker is gone.
///
/// Evidence class: target-hardening contract. The current DFS does not expose
/// this distributed advisory-lock protocol yet. Eventual expiry still assumes
/// weak fairness for `tick` after a holder loses renewal access; continued
/// service after epoch exhaustion is outside this deliberately finite
/// verification bound.
module dfs_posix_advisory_locks {
  import common.* from "../../lib/common"

  type LockSession = SessionA | SessionB
  type LockMode = SharedLock | ExclusiveLock
  type SessionLifecycle = SessionOpen | SessionClosed | SessionCrashed

  type LockToken = {
    session: LockSession,
    authorityEpoch: int,
    grantId: int,
  }

  type HeldLock = {
    mode: LockMode,
    token: LockToken,
    leaseTicksRemaining: int,
  }

  type WaitRequest = {
    mode: LockMode,
    authorityEpoch: int,
  }

  type UnblockCause =
    | NoUnblockCause
    | ExplicitUnlockCause
    | SessionCloseCause
    | SessionCrashCause
    | LeaseExpiryCause

  type LockEvent =
    | LockInitialized
    | NonBlockingGranted
    | NonBlockingConflict
    | BlockingGranted
    | BlockingQueued
    | WaitingGrantPromoted
    | TokenUnlocked
    | StaleTokenRejected
    | LeaseTicked
    | SessionClosedEvent
    | SessionCrashedEvent
    | SessionOpenedEvent
    | RenewalUnavailable
    | RenewalRestored
    | AuthorityEpochAdvanced

  /// `observedTicks` is a saturating observation only. Lease expiry is driven
  /// by the finite per-grant countdown, so ticks remain enabled after its cap.
  type LockState = {
    observedTicks: int,
    authorityEpoch: int,
    nextGrantId: int,
    lifecycle: LockSession -> SessionLifecycle,
    renewalAvailable: LockSession -> bool,
    held: LockSession -> Option[HeldLock],
    heldBeforeTick: LockSession -> Option[HeldLock],
    heldBeforeTokenAttempt: LockSession -> Option[HeldLock],
    waiting: LockSession -> Option[WaitRequest],
    waitingBeforeEpochAdvance: LockSession -> Option[WaitRequest],
    fencedWaiterResponses: Set[LockSession],
    lostLockResponses: Set[LockSession],
    issuedTokens: Set[LockToken],
    expiredLastTick: Set[LockSession],
    fencedLastEpoch: Set[LockSession],
    lastActor: LockSession,
    lastMode: LockMode,
    lastToken: Option[LockToken],
    lastUnblockCause: UnblockCause,
    lastEvent: LockEvent,
  }

  pure val LockSessions: Set[LockSession] = Set(SessionA, SessionB)
  pure val LockModes: Set[LockMode] = Set(SharedLock, ExclusiveLock)
  pure val LeaseLengthTicks: int = 2
  pure val MaxObservedTicks: int = 4
  pure val MaxAuthorityEpoch: int = 2
  pure val MaxGrantId: int = 8

  pure val emptyHeld: LockSession -> Option[HeldLock] =
    LockSessions.mapBy(_ => None)

  pure val emptyWaiting: LockSession -> Option[WaitRequest] =
    LockSessions.mapBy(_ => None)

  var lockState: LockState

  pure def modesConflict(requested: LockMode, heldMode: LockMode): bool =
    requested == ExclusiveLock or heldMode == ExclusiveLock

  pure def removeSession(
    sessions: Set[LockSession],
    session: LockSession
  ): Set[LockSession] =
    sessions.filter(candidate => candidate != session)

  pure def lockIsActive(lock: HeldLock, epoch: int): bool = all {
    lock.token.authorityEpoch == epoch,
    lock.leaseTicksRemaining > 0,
  }

  pure def sessionHasActiveLock(
    held: LockSession -> Option[HeldLock],
    epoch: int,
    session: LockSession,
  ): bool =
    match held.get(session) {
      | Some(lock) => lockIsActive(lock, epoch)
      | None => false
    }

  pure def requestConflictsIn(
    held: LockSession -> Option[HeldLock],
    epoch: int,
    requester: LockSession,
    requestedMode: LockMode,
  ): bool =
    LockSessions.exists(holder => all {
      holder != requester,
      match held.get(holder) {
        | Some(lock) => all {
            lockIsActive(lock, epoch),
            modesConflict(requestedMode, lock.mode),
          }
        | None => false
      },
    })

  pure def exactCurrentTokenIn(
    held: LockSession -> Option[HeldLock],
    epoch: int,
    token: LockToken,
  ): bool =
    match held.get(token.session) {
      | Some(lock) => all {
          lockIsActive(lock, epoch),
          lock.token == token,
        }
      | None => false
    }

  def canStartRequest(session: LockSession): bool = all {
    lockState.lifecycle.get(session) == SessionOpen,
    lockState.renewalAvailable.get(session),
    lockState.held.get(session) == None,
    lockState.waiting.get(session) == None,
  }

  def waitingRequestIsCurrent(session: LockSession): bool =
    match lockState.waiting.get(session) {
      | Some(request) => all {
          request.authorityEpoch == lockState.authorityEpoch,
          lockState.lifecycle.get(session) == SessionOpen,
        }
      | None => false
    }

  def waitingRequestIsGrantable(session: LockSession): bool =
    match lockState.waiting.get(session) {
      | Some(request) => all {
          waitingRequestIsCurrent(session),
          lockState.renewalAvailable.get(session),
          not(requestConflictsIn(
            lockState.held,
            lockState.authorityEpoch,
            session,
            request.mode
          )),
          lockState.nextGrantId <= MaxGrantId,
        }
      | None => false
    }

  def hasGrantableWaiter: bool =
    LockSessions.exists(waitingRequestIsGrantable)

  pure def renewedOrTickedHeldLocks(
    held: LockSession -> Option[HeldLock],
    epoch: int,
    renewalAvailable: LockSession -> bool,
  ): LockSession -> Option[HeldLock] =
    LockSessions.mapBy(session =>
      match held.get(session) {
        | Some(lock) =>
            if (not(lockIsActive(lock, epoch))) {
              None
            } else if (renewalAvailable.get(session)) {
              Some(lock.with("leaseTicksRemaining", LeaseLengthTicks))
            } else if (lock.leaseTicksRemaining <= 1) {
              None
            } else {
              Some(lock.with("leaseTicksRemaining", lock.leaseTicksRemaining - 1))
            }
        | None => None
      }
    )

  pure def sessionsExpiringOnTick(
    held: LockSession -> Option[HeldLock],
    epoch: int,
    renewalAvailable: LockSession -> bool,
  ): Set[LockSession] =
    LockSessions.filter(session =>
      match held.get(session) {
        | Some(lock) => all {
          lockIsActive(lock, epoch),
          not(renewalAvailable.get(session)),
          lock.leaseTicksRemaining <= 1,
          }
        | None => false
      }
    )

  def participantsInCurrentEpoch: Set[LockSession] =
    LockSessions.filter(session => any {
      sessionHasActiveLock(lockState.held, lockState.authorityEpoch, session),
      waitingRequestIsCurrent(session),
    })

  action init = all {
    lockState' = {
      observedTicks: 0,
      authorityEpoch: 0,
      nextGrantId: 1,
      lifecycle: LockSessions.mapBy(_ => SessionOpen),
      renewalAvailable: LockSessions.mapBy(_ => true),
      held: emptyHeld,
      heldBeforeTick: emptyHeld,
      heldBeforeTokenAttempt: emptyHeld,
      waiting: emptyWaiting,
      waitingBeforeEpochAdvance: emptyWaiting,
      fencedWaiterResponses: Set(),
      lostLockResponses: Set(),
      issuedTokens: Set(),
      expiredLastTick: Set(),
      fencedLastEpoch: Set(),
      lastActor: SessionA,
      lastMode: SharedLock,
      lastToken: None,
      lastUnblockCause: NoUnblockCause,
      lastEvent: LockInitialized,
    },
  }

  action requestNonBlocking(session: LockSession, mode: LockMode): bool = {
    val conflicts = requestConflictsIn(
      lockState.held,
      lockState.authorityEpoch,
      session,
      mode
    )
    val token = {
      session: session,
      authorityEpoch: lockState.authorityEpoch,
      grantId: lockState.nextGrantId,
    }
    val grantedLock = {
      mode: mode,
      token: token,
      leaseTicksRemaining: LeaseLengthTicks,
    }
    all {
      canStartRequest(session),
      conflicts or lockState.nextGrantId <= MaxGrantId,
      lockState' = if (conflicts) {
        lockState
          .with(
            "fencedWaiterResponses",
            removeSession(lockState.fencedWaiterResponses, session)
          )
          .with(
            "lostLockResponses",
            removeSession(lockState.lostLockResponses, session)
          )
          .with("expiredLastTick", Set())
          .with("fencedLastEpoch", Set())
          .with("lastActor", session)
          .with("lastMode", mode)
          .with("lastToken", None)
          .with("lastUnblockCause", NoUnblockCause)
          .with("lastEvent", NonBlockingConflict)
      } else {
        lockState
          .with(
            "fencedWaiterResponses",
            removeSession(lockState.fencedWaiterResponses, session)
          )
          .with(
            "lostLockResponses",
            removeSession(lockState.lostLockResponses, session)
          )
          .with("nextGrantId", lockState.nextGrantId + 1)
          .with("held", lockState.held.set(session, Some(grantedLock)))
          .with("issuedTokens", lockState.issuedTokens.union(Set(token)))
          .with("expiredLastTick", Set())
          .with("fencedLastEpoch", Set())
          .with("lastActor", session)
          .with("lastMode", mode)
          .with("lastToken", Some(token))
          .with("lastUnblockCause", NoUnblockCause)
          .with("lastEvent", NonBlockingGranted)
      },
    }
  }

  action requestBlocking(session: LockSession, mode: LockMode): bool = {
    val conflicts = requestConflictsIn(
      lockState.held,
      lockState.authorityEpoch,
      session,
      mode
    )
    val token = {
      session: session,
      authorityEpoch: lockState.authorityEpoch,
      grantId: lockState.nextGrantId,
    }
    val grantedLock = {
      mode: mode,
      token: token,
      leaseTicksRemaining: LeaseLengthTicks,
    }
    val request = {
      mode: mode,
      authorityEpoch: lockState.authorityEpoch,
    }
    all {
      canStartRequest(session),
      lockState.nextGrantId <= MaxGrantId,
      lockState' = if (conflicts) {
        lockState
          .with(
            "fencedWaiterResponses",
            removeSession(lockState.fencedWaiterResponses, session)
          )
          .with(
            "lostLockResponses",
            removeSession(lockState.lostLockResponses, session)
          )
          .with("waiting", lockState.waiting.set(session, Some(request)))
          .with("expiredLastTick", Set())
          .with("fencedLastEpoch", Set())
          .with("lastActor", session)
          .with("lastMode", mode)
          .with("lastToken", None)
          .with("lastUnblockCause", NoUnblockCause)
          .with("lastEvent", BlockingQueued)
      } else {
        lockState
          .with(
            "fencedWaiterResponses",
            removeSession(lockState.fencedWaiterResponses, session)
          )
          .with(
            "lostLockResponses",
            removeSession(lockState.lostLockResponses, session)
          )
          .with("nextGrantId", lockState.nextGrantId + 1)
          .with("held", lockState.held.set(session, Some(grantedLock)))
          .with("issuedTokens", lockState.issuedTokens.union(Set(token)))
          .with("expiredLastTick", Set())
          .with("fencedLastEpoch", Set())
          .with("lastActor", session)
          .with("lastMode", mode)
          .with("lastToken", Some(token))
          .with("lastUnblockCause", NoUnblockCause)
          .with("lastEvent", BlockingGranted)
      },
    }
  }

  action promoteWaiting(session: LockSession): bool = {
    val request = match lockState.waiting.get(session) {
      | Some(value) => value
      | None => { mode: SharedLock, authorityEpoch: lockState.authorityEpoch }
    }
    val token = {
      session: session,
      authorityEpoch: lockState.authorityEpoch,
      grantId: lockState.nextGrantId,
    }
    val grantedLock = {
      mode: request.mode,
      token: token,
      leaseTicksRemaining: LeaseLengthTicks,
    }
    all {
      waitingRequestIsGrantable(session),
      lockState' = lockState
        .with("nextGrantId", lockState.nextGrantId + 1)
        .with("held", lockState.held.set(session, Some(grantedLock)))
        .with("waiting", lockState.waiting.set(session, None))
        .with("issuedTokens", lockState.issuedTokens.union(Set(token)))
        .with("lastActor", session)
        .with("lastMode", request.mode)
        .with("lastToken", Some(token))
        .with("lastEvent", WaitingGrantPromoted),
    }
  }

  action unlockWithToken(token: LockToken): bool = {
    val exact = exactCurrentTokenIn(
      lockState.held,
      lockState.authorityEpoch,
      token
    )
    all {
      lockState.issuedTokens.contains(token),
      lockState' = if (exact) {
        lockState
          .with("heldBeforeTokenAttempt", lockState.held)
          .with("held", lockState.held.set(token.session, None))
          .with("expiredLastTick", Set())
          .with("fencedLastEpoch", Set())
          .with("lastActor", token.session)
          .with("lastToken", Some(token))
          .with("lastUnblockCause", ExplicitUnlockCause)
          .with("lastEvent", TokenUnlocked)
      } else {
        lockState
          .with("heldBeforeTokenAttempt", lockState.held)
          .with("expiredLastTick", Set())
          .with("fencedLastEpoch", Set())
          .with("lastActor", token.session)
          .with("lastToken", Some(token))
          .with("lastEvent", StaleTokenRejected)
      },
    }
  }

  action tick: bool = {
    val expired = sessionsExpiringOnTick(
      lockState.held,
      lockState.authorityEpoch,
      lockState.renewalAvailable
    )
    all {
      lockState' = lockState
        .with(
          "observedTicks",
          if (lockState.observedTicks < MaxObservedTicks) {
            lockState.observedTicks + 1
          } else {
            MaxObservedTicks
          }
        )
        .with("heldBeforeTick", lockState.held)
        .with(
          "held",
          renewedOrTickedHeldLocks(
            lockState.held,
            lockState.authorityEpoch,
            lockState.renewalAvailable
          )
        )
        .with(
          "lostLockResponses",
          lockState.lostLockResponses.union(expired)
        )
        .with("expiredLastTick", expired)
        .with("fencedLastEpoch", Set())
        .with("lastToken", None)
        .with(
          "lastUnblockCause",
          if (expired.size() > 0) { LeaseExpiryCause } else { NoUnblockCause }
        )
        .with("lastEvent", LeaseTicked),
    }
  }

  action closeSession(session: LockSession): bool = all {
    lockState.lifecycle.get(session) == SessionOpen,
    lockState' = lockState
      .with(
        "lifecycle",
        lockState.lifecycle.set(session, SessionClosed)
      )
      .with(
        "renewalAvailable",
        lockState.renewalAvailable.set(session, false)
      )
      .with("held", lockState.held.set(session, None))
      .with("waiting", lockState.waiting.set(session, None))
      .with("expiredLastTick", Set())
      .with("fencedLastEpoch", Set())
      .with("lastActor", session)
      .with("lastToken", None)
      .with("lastUnblockCause", SessionCloseCause)
      .with("lastEvent", SessionClosedEvent),
  }

  action crashSession(session: LockSession): bool = all {
    lockState.lifecycle.get(session) == SessionOpen,
    lockState' = lockState
      .with(
        "lifecycle",
        lockState.lifecycle.set(session, SessionCrashed)
      )
      .with(
        "renewalAvailable",
        lockState.renewalAvailable.set(session, false)
      )
      .with("held", lockState.held.set(session, None))
      .with("waiting", lockState.waiting.set(session, None))
      .with("expiredLastTick", Set())
      .with("fencedLastEpoch", Set())
      .with("lastActor", session)
      .with("lastToken", None)
      .with("lastUnblockCause", SessionCrashCause)
      .with("lastEvent", SessionCrashedEvent),
  }

  action openSession(session: LockSession): bool = all {
    lockState.lifecycle.get(session) != SessionOpen,
    lockState.held.get(session) == None,
    lockState.waiting.get(session) == None,
    lockState' = lockState
      .with(
        "lifecycle",
        lockState.lifecycle.set(session, SessionOpen)
      )
      .with(
        "renewalAvailable",
        lockState.renewalAvailable.set(session, true)
      )
      .with(
        "fencedWaiterResponses",
        removeSession(lockState.fencedWaiterResponses, session)
      )
      .with(
        "lostLockResponses",
        removeSession(lockState.lostLockResponses, session)
      )
      .with("expiredLastTick", Set())
      .with("fencedLastEpoch", Set())
      .with("lastActor", session)
      .with("lastToken", None)
      .with("lastUnblockCause", NoUnblockCause)
      .with("lastEvent", SessionOpenedEvent),
  }

  /// Renewal reachability is session-local: a partition can isolate the old
  /// holder while a waiter on the quorum side remains able to renew. Losing
  /// this path does not revoke the lock immediately; the finite lease must run
  /// out, which gives an in-flight holder its bounded permit window.
  action loseRenewalAvailability(session: LockSession): bool = all {
    lockState.lifecycle.get(session) == SessionOpen,
    lockState.renewalAvailable.get(session),
    lockState' = lockState
      .with(
        "renewalAvailable",
        lockState.renewalAvailable.set(session, false)
      )
      .with("expiredLastTick", Set())
      .with("fencedLastEpoch", Set())
      .with("lastActor", session)
      .with("lastToken", None)
      .with("lastUnblockCause", NoUnblockCause)
      .with("lastEvent", RenewalUnavailable),
  }

  action restoreRenewalAvailability(session: LockSession): bool = all {
    lockState.lifecycle.get(session) == SessionOpen,
    not(lockState.renewalAvailable.get(session)),
    lockState' = lockState
      .with(
        "renewalAvailable",
        lockState.renewalAvailable.set(session, true)
      )
      .with("expiredLastTick", Set())
      .with("fencedLastEpoch", Set())
      .with("lastActor", session)
      .with("lastToken", None)
      .with("lastEvent", RenewalRestored),
  }

  /// A new quorum-authorized metadata epoch wins over every old grant,
  /// regardless of how many DFS content shards remain reachable on the old
  /// side. Old holders are fenced. Every blocking request is explicitly moved
  /// to a terminal fenced-response set before the wait table is cleared, so a
  /// blocked syscall cannot disappear silently during authority transfer.
  action advanceAuthorityEpoch: bool = {
    val fencedWaiters = LockSessions.filter(waitingRequestIsCurrent)
    val fencedHolders = LockSessions.filter(session =>
      sessionHasActiveLock(lockState.held, lockState.authorityEpoch, session)
    )
    all {
      lockState.authorityEpoch < MaxAuthorityEpoch,
      lockState' = lockState
        .with("authorityEpoch", lockState.authorityEpoch + 1)
        .with("held", emptyHeld)
        .with("waitingBeforeEpochAdvance", lockState.waiting)
        .with("waiting", emptyWaiting)
        .with(
          "fencedWaiterResponses",
          lockState.fencedWaiterResponses.union(fencedWaiters)
        )
        .with(
          "lostLockResponses",
          lockState.lostLockResponses.union(fencedHolders)
        )
        .with("expiredLastTick", Set())
        .with("fencedLastEpoch", participantsInCurrentEpoch)
        .with("lastToken", None)
        .with("lastUnblockCause", NoUnblockCause)
        .with("lastEvent", AuthorityEpochAdvanced),
    }
  }

  /// When cleanup makes a waiter grantable, the next transition must drain it.
  /// This is the safety/total-transition encoding of "an obsolete lock cannot
  /// permanently block the current epoch". Weak fairness is needed only to
  /// ensure an unavailable holder's finite lease eventually receives `tick`
  /// actions; healthy reachable holders renew instead of expiring.
  action step =
    if (hasGrantableWaiter) {
      nondet session = oneOf(LockSessions.filter(waitingRequestIsGrantable))
      promoteWaiting(session)
    } else {
      any {
        {
          nondet session = oneOf(LockSessions)
          nondet mode = oneOf(LockModes)
          requestNonBlocking(session, mode)
        },
        {
          nondet session = oneOf(LockSessions)
          nondet mode = oneOf(LockModes)
          requestBlocking(session, mode)
        },
        {
          nondet token = oneOf(lockState.issuedTokens)
          all {
            lockState.issuedTokens.size() > 0,
            unlockWithToken(token),
          }
        },
        tick,
        {
          nondet session = oneOf(LockSessions)
          closeSession(session)
        },
        {
          nondet session = oneOf(LockSessions)
          crashSession(session)
        },
        {
          nondet session = oneOf(LockSessions)
          openSession(session)
        },
        {
          nondet session = oneOf(LockSessions)
          loseRenewalAvailability(session)
        },
        {
          nondet session = oneOf(LockSessions)
          restoreRenewalAvailability(session)
        },
        advanceAuthorityEpoch,
      }
    }
}
