/// Safety properties and focused witnesses for `dfs_posix_advisory_locks`.
module dfs_posix_advisory_locks_safety {
  import common.* from "../../lib/common"
  import dfs_posix_advisory_locks.* from "../../models/dfs/posix_advisory_locks"

  def stateBoundsHold: bool = all {
    lockState.observedTicks >= 0,
    lockState.observedTicks <= MaxObservedTicks,
    lockState.authorityEpoch >= 0,
    lockState.authorityEpoch <= MaxAuthorityEpoch,
    lockState.nextGrantId >= 1,
    lockState.nextGrantId <= MaxGrantId + 1,
  }

  def everyHeldLockIsLiveAndEpochFenced: bool =
    LockSessions.forall(session =>
      match lockState.held.get(session) {
        | Some(lock) => all {
            lock.token.session == session,
            lock.token.authorityEpoch == lockState.authorityEpoch,
            lock.leaseTicksRemaining > 0,
            lock.leaseTicksRemaining <= LeaseLengthTicks,
            lockState.lifecycle.get(session) == SessionOpen,
            lockState.issuedTokens.contains(lock.token),
          }
        | None => true
      }
    )

  def everyWaiterIsLiveAndEpochFenced: bool =
    LockSessions.forall(session =>
      match lockState.waiting.get(session) {
        | Some(request) => all {
            request.authorityEpoch == lockState.authorityEpoch,
            lockState.lifecycle.get(session) == SessionOpen,
            lockState.held.get(session) == None,
          }
        | None => true
      }
    )

  /// A session has at most one active request. Starting its next request
  /// consumes any prior terminal fenced response, so a later epoch cannot be
  /// justified by a stale response for an earlier call.
  def activeWaiterHasNoConsumedFencedResponse: bool =
    LockSessions.forall(session =>
      lockState.waiting.get(session) != None implies
        not(lockState.fencedWaiterResponses.contains(session))
    )

  /// Whole-file shared locks are compatible, but an exclusive lock excludes
  /// every lock owned by the other session.
  def exclusiveLocksAreMutuallyExclusive: bool =
    LockSessions.forall(left => LockSessions.forall(right =>
      all {
        left != right,
        sessionHasActiveLock(
          lockState.held,
          lockState.authorityEpoch,
          left
        ),
        sessionHasActiveLock(
          lockState.held,
          lockState.authorityEpoch,
          right
        ),
      } implies
        match lockState.held.get(left) {
          | Some(leftLock) => match lockState.held.get(right) {
              | Some(rightLock) => all {
                  leftLock.mode == SharedLock,
                  rightLock.mode == SharedLock,
                }
              | None => false
            }
          | None => false
        }
    ))

  def nonBlockingConflictDoesNotQueue: bool =
    lockState.lastEvent == NonBlockingConflict implies all {
      lockState.lastToken == None,
      lockState.waiting.get(lockState.lastActor) == None,
      lockState.held.get(lockState.lastActor) == None,
    }

  def blockingConflictQueuesWithoutGrant: bool =
    lockState.lastEvent == BlockingQueued implies all {
      lockState.lastToken == None,
      lockState.held.get(lockState.lastActor) == None,
      match lockState.waiting.get(lockState.lastActor) {
        | Some(request) => all {
            request.mode == lockState.lastMode,
            request.authorityEpoch == lockState.authorityEpoch,
            requestConflictsIn(
              lockState.held,
              lockState.authorityEpoch,
              lockState.lastActor,
              request.mode
            ),
          }
        | None => false
      },
    }

  def grantsReturnTheExactHeldToken: bool =
    any {
      lockState.lastEvent == NonBlockingGranted,
      lockState.lastEvent == BlockingGranted,
      lockState.lastEvent == WaitingGrantPromoted,
    } implies
      match lockState.lastToken {
        | Some(token) => all {
            token.session == lockState.lastActor,
            token.authorityEpoch == lockState.authorityEpoch,
            lockState.renewalAvailable.get(token.session),
            exactCurrentTokenIn(
              lockState.held,
              lockState.authorityEpoch,
              token
            ),
          }
        | None => false
      }

  /// A release is an exact-token operation, not merely a session lookup. This
  /// makes a token from an old epoch or an old same-session grant harmless.
  def rejectedTokenDoesNotMatchCurrentGrant: bool =
    lockState.lastEvent == StaleTokenRejected implies
      match lockState.lastToken {
        | Some(token) => all {
            not(exactCurrentTokenIn(
              lockState.held,
              lockState.authorityEpoch,
              token
            )),
            lockState.held == lockState.heldBeforeTokenAttempt,
          }
        | None => false
      }

  def closedOrCrashedSessionsOwnNothing: bool =
    LockSessions.forall(session =>
      lockState.lifecycle.get(session) != SessionOpen implies all {
        lockState.held.get(session) == None,
        lockState.waiting.get(session) == None,
        not(lockState.renewalAvailable.get(session)),
      }
    )

  /// Logical time does not make a healthy lock disappear. A session that can
  /// reach the current authority renews the exact same grant token back to the
  /// full bounded lease; only unavailable sessions count down.
  def reachableHoldersRenewOnTick: bool =
    lockState.lastEvent == LeaseTicked implies
      LockSessions.forall(session =>
        match lockState.heldBeforeTick.get(session) {
          | Some(before) =>
              if (all {
                lockIsActive(before, lockState.authorityEpoch),
                lockState.renewalAvailable.get(session),
              }) {
                match lockState.held.get(session) {
                  | Some(after) => all {
                      after.mode == before.mode,
                      after.token == before.token,
                      after.leaseTicksRemaining == LeaseLengthTicks,
                    }
                  | None => false
                }
              } else {
                true
              }
          | None => true
        }
      )

  def expiredSessionsNoLongerHoldLocks: bool =
    lockState.expiredLastTick.forall(session =>
      all {
        lockState.held.get(session) == None,
        not(lockState.renewalAvailable.get(session)),
        lockState.lostLockResponses.contains(session),
      }
    )

  /// Old-epoch state is removed atomically. Since conflict checks consider only
  /// current active grants, no obsolete lock can block a current request.
  def epochAdvanceLeavesCleanLockManager: bool =
    lockState.lastEvent == AuthorityEpochAdvanced implies all {
      LockSessions.forall(session => lockState.held.get(session) == None),
      LockSessions.forall(session => lockState.waiting.get(session) == None),
      LockSessions.forall(session =>
        match lockState.waitingBeforeEpochAdvance.get(session) {
          | Some(_) => lockState.fencedWaiterResponses.contains(session)
          | None => true
        }
      ),
      lockState.fencedLastEpoch.forall(session => any {
        lockState.fencedWaiterResponses.contains(session),
        lockState.lostLockResponses.contains(session),
      }),
    }

  def issuedTokenIdsAreUnique: bool =
    lockState.issuedTokens.forall(left =>
      lockState.issuedTokens.forall(right =>
        left.grantId == right.grantId implies left == right
      )
    )

  /// With only two sessions, at most one request can wait behind an active
  /// holder. This also makes strict waiter-drain priority deterministic.
  def atMostOneWaiter: bool =
    LockSessions.filter(session => lockState.waiting.get(session) != None).size() <= 1

  /// Coverage witnesses are kept out of `safety`; a runner can request them
  /// independently to ensure every important outcome remains reachable.
  def witnessCompatibleSharedLocks: bool = all {
    LockSessions.filter(session =>
      match lockState.held.get(session) {
        | Some(lock) => lock.mode == SharedLock
        | None => false
      }
    ).size() == 2,
  }

  def witnessExclusiveConflictRejectedNonBlocking: bool =
    lockState.lastEvent == NonBlockingConflict

  def witnessBlockingWaiterQueued: bool =
    lockState.lastEvent == BlockingQueued

  def witnessWaiterGrantedAfterUnlock: bool = all {
    lockState.lastEvent == WaitingGrantPromoted,
    lockState.lastUnblockCause == ExplicitUnlockCause,
  }

  def witnessWaiterGrantedAfterClose: bool = all {
    lockState.lastEvent == WaitingGrantPromoted,
    lockState.lastUnblockCause == SessionCloseCause,
  }

  def witnessWaiterGrantedAfterCrash: bool = all {
    lockState.lastEvent == WaitingGrantPromoted,
    lockState.lastUnblockCause == SessionCrashCause,
  }

  def witnessWaiterGrantedAfterLeaseExpiry: bool = all {
    lockState.lastEvent == WaitingGrantPromoted,
    lockState.lastUnblockCause == LeaseExpiryCause,
    lockState.lostLockResponses.size() > 0,
  }

  def witnessHealthyTickRenewsHeldLock: bool = all {
    lockState.lastEvent == LeaseTicked,
    LockSessions.exists(session =>
      match lockState.heldBeforeTick.get(session) {
        | Some(before) => all {
            lockState.renewalAvailable.get(session),
            match lockState.held.get(session) {
              | Some(after) => all {
                  after.token == before.token,
                  after.leaseTicksRemaining == LeaseLengthTicks,
                }
              | None => false
            },
          }
        | None => false
      }
    ),
  }

  def witnessEpochFencesHeldLockOrWaiter: bool = all {
    lockState.lastEvent == AuthorityEpochAdvanced,
    lockState.fencedLastEpoch.size() > 0,
    epochAdvanceLeavesCleanLockManager,
  }

  def witnessEpochSettlesBlockedWaiter: bool = all {
    lockState.lastEvent == AuthorityEpochAdvanced,
    LockSessions.exists(session => all {
      lockState.waitingBeforeEpochAdvance.get(session) != None,
      lockState.fencedWaiterResponses.contains(session),
    }),
  }

  def witnessStaleTokenCannotUnlockNewEpoch: bool = all {
    lockState.lastEvent == StaleTokenRejected,
    match lockState.lastToken {
      | Some(token) => token.authorityEpoch < lockState.authorityEpoch
      | None => false
    },
    LockSessions.exists(session =>
      sessionHasActiveLock(
        lockState.held,
        lockState.authorityEpoch,
        session
      )
    ),
  }

  def safety: bool = all {
    stateBoundsHold,
    everyHeldLockIsLiveAndEpochFenced,
    everyWaiterIsLiveAndEpochFenced,
    activeWaiterHasNoConsumedFencedResponse,
    exclusiveLocksAreMutuallyExclusive,
    nonBlockingConflictDoesNotQueue,
    blockingConflictQueuesWithoutGrant,
    grantsReturnTheExactHeldToken,
    rejectedTokenDoesNotMatchCurrentGrant,
    closedOrCrashedSessionsOwnNothing,
    reachableHoldersRenewOnTick,
    expiredSessionsNoLongerHoldLocks,
    epochAdvanceLeavesCleanLockManager,
    issuedTokenIdsAreUnique,
    atMostOneWaiter,
  }
}
