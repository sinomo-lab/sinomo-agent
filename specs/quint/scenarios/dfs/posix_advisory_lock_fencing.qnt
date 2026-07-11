/// Deterministic regression for a blocking advisory lock across epoch change.
///
/// Session A holds an exclusive lock, session B blocks, and a quorum-authorized
/// epoch advance explicitly settles B's old request as fenced and reports A's
/// lock as lost. B then acquires a new-epoch lock. A's stale token cannot
/// release it, while B's exact token can.
module dfs_posix_advisory_lock_fencing_scenario {
  import common.* from "../../lib/common"
  import dfs_posix_advisory_locks from "../../models/dfs/posix_advisory_locks"

  type Phase =
    | Start
    | OldLockGranted
    | WaiterQueued
    | OldEpochFenced
    | NewLockGranted
    | StaleUnlockRejected
    | NewLockReleased

  pure val OldToken: dfs_posix_advisory_locks::LockToken = {
    session: dfs_posix_advisory_locks::SessionA,
    authorityEpoch: 0,
    grantId: 1,
  }

  pure val NewToken: dfs_posix_advisory_locks::LockToken = {
    session: dfs_posix_advisory_locks::SessionB,
    authorityEpoch: 1,
    grantId: 2,
  }

  var phase: Phase

  action init = all {
    dfs_posix_advisory_locks::init,
    phase' = Start,
  }

  action step =
    if (phase == Start) {
      all {
        dfs_posix_advisory_locks::requestNonBlocking(
          dfs_posix_advisory_locks::SessionA,
          dfs_posix_advisory_locks::ExclusiveLock
        ),
        phase' = OldLockGranted,
      }
    } else if (phase == OldLockGranted) {
      all {
        dfs_posix_advisory_locks::requestBlocking(
          dfs_posix_advisory_locks::SessionB,
          dfs_posix_advisory_locks::ExclusiveLock
        ),
        phase' = WaiterQueued,
      }
    } else if (phase == WaiterQueued) {
      all {
        dfs_posix_advisory_locks::advanceAuthorityEpoch,
        phase' = OldEpochFenced,
      }
    } else if (phase == OldEpochFenced) {
      all {
        dfs_posix_advisory_locks::requestNonBlocking(
          dfs_posix_advisory_locks::SessionB,
          dfs_posix_advisory_locks::ExclusiveLock
        ),
        phase' = NewLockGranted,
      }
    } else if (phase == NewLockGranted) {
      all {
        dfs_posix_advisory_locks::unlockWithToken(OldToken),
        phase' = StaleUnlockRejected,
      }
    } else if (phase == StaleUnlockRejected) {
      all {
        dfs_posix_advisory_locks::unlockWithToken(NewToken),
        phase' = NewLockReleased,
      }
    } else {
      all {
        phase == NewLockReleased,
        dfs_posix_advisory_locks::tick,
        phase' = phase,
      }
    }

  def safety: bool = all {
    dfs_posix_advisory_locks::LockSessions.forall(session =>
      match dfs_posix_advisory_locks::lockState.held.get(session) {
        | Some(lock) => all {
            lock.token.authorityEpoch
              == dfs_posix_advisory_locks::lockState.authorityEpoch,
            lock.leaseTicksRemaining > 0,
            dfs_posix_advisory_locks::lockState.lifecycle.get(session)
              == dfs_posix_advisory_locks::SessionOpen,
          }
        | None => true
      }
    ),
    dfs_posix_advisory_locks::LockSessions.filter(session =>
      dfs_posix_advisory_locks::lockState.waiting.get(session) != None
    ).size() <= 1,
    match phase {
      | Start =>
          dfs_posix_advisory_locks::lockState.held
            == dfs_posix_advisory_locks::emptyHeld
      | OldLockGranted => all {
          dfs_posix_advisory_locks::exactCurrentTokenIn(
            dfs_posix_advisory_locks::lockState.held,
            dfs_posix_advisory_locks::lockState.authorityEpoch,
            OldToken
          ),
          dfs_posix_advisory_locks::lockState.authorityEpoch == 0,
        }
      | WaiterQueued =>
          dfs_posix_advisory_locks::lockState.waiting.get(
            dfs_posix_advisory_locks::SessionB
          ) != None
      | OldEpochFenced => all {
          dfs_posix_advisory_locks::lockState.authorityEpoch == 1,
          dfs_posix_advisory_locks::lockState.held
            == dfs_posix_advisory_locks::emptyHeld,
          dfs_posix_advisory_locks::lockState.waiting
            == dfs_posix_advisory_locks::emptyWaiting,
          dfs_posix_advisory_locks::lockState.lostLockResponses.contains(
            dfs_posix_advisory_locks::SessionA
          ),
          dfs_posix_advisory_locks::lockState.fencedWaiterResponses.contains(
            dfs_posix_advisory_locks::SessionB
          ),
        }
      | NewLockGranted =>
          dfs_posix_advisory_locks::exactCurrentTokenIn(
            dfs_posix_advisory_locks::lockState.held,
            dfs_posix_advisory_locks::lockState.authorityEpoch,
            NewToken
          )
      | StaleUnlockRejected => all {
          dfs_posix_advisory_locks::lockState.lastEvent
            == dfs_posix_advisory_locks::StaleTokenRejected,
          dfs_posix_advisory_locks::exactCurrentTokenIn(
            dfs_posix_advisory_locks::lockState.held,
            dfs_posix_advisory_locks::lockState.authorityEpoch,
            NewToken
          ),
        }
      | NewLockReleased => all {
          dfs_posix_advisory_locks::lockState.held
            == dfs_posix_advisory_locks::emptyHeld,
          dfs_posix_advisory_locks::lockState.lastEvent
            == dfs_posix_advisory_locks::TokenUnlocked
            or dfs_posix_advisory_locks::lockState.lastEvent
              == dfs_posix_advisory_locks::LeaseTicked,
        }
    },
  }
}
