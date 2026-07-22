//! Renewable exclusive advisory-lock leases for synchronous DFS callers.
//!
//! The guard is intentionally anchored to a caller-supplied stable object id.
//! Its path is only a routing hint, so a same-shard rename cannot silently move
//! the lease to another object. Each lock-manager command is retried with the
//! exact same request id and payload within one bounded command deadline.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, TryLockError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::posix::locks::DEFAULT_DFS_ADVISORY_LOCK_RPC_TIMEOUT;
use crate::{
    DfsAdvisoryLockCommand, DfsAdvisoryLockConflict, DfsAdvisoryLockGrant, DfsAdvisoryLockMode,
    DfsAdvisoryLockOutcome, DfsAdvisoryLockRequestId, DfsAdvisoryLockSessionId,
    DfsAdvisoryLockToken, DfsError, DfsMessageTransport, DfsObjectId, DfsPosixError,
    NetworkedDfsService,
};

/// Normal cadence for refreshing a live lease well before its authority-side
/// 120-second validity window ends.
pub const DEFAULT_DFS_RENEWABLE_LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(5);

/// Overall budget shared by service-lane admission, the first command attempt,
/// and one exact retry. Advisory-lock transitions synchronously replicate their
/// exact metadata prefix, so the lease wrapper must preserve the full lock RPC
/// budget instead of imposing a shorter WAN-incompatible ceiling.
pub const DEFAULT_DFS_RENEWABLE_LEASE_COMMAND_TIMEOUT: Duration =
    DEFAULT_DFS_ADVISORY_LOCK_RPC_TIMEOUT;

/// Maximum wall-clock budget for all best-effort lease cleanup. Unlock and
/// session-close share this one deadline so cleanup cannot add two independent
/// network timeouts after a prompt cancellation.
pub const DEFAULT_DFS_RENEWABLE_LEASE_RELEASE_TIMEOUT: Duration = Duration::from_secs(5);

const RENEWABLE_LEASE_SERVICE_POLL_INTERVAL: Duration = Duration::from_millis(2);
static NEXT_RENEWABLE_LEASE_REQUEST: AtomicU64 = AtomicU64::new(1);

/// Stable DFS object identity used by a renewable lease.
///
/// Callers create and retain the anchor object outside any destructible
/// resource protected by the lease, then obtain `object_id` from a DFS stat.
/// `path` remains a routing hint while `object_id` is the lock identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsRenewableLeaseAnchor {
    pub path: String,
    pub object_id: DfsObjectId,
}

impl DfsRenewableLeaseAnchor {
    pub fn new(path: impl Into<String>, object_id: DfsObjectId) -> Self {
        Self {
            path: path.into(),
            object_id,
        }
    }
}

/// Timing policy for a renewable lease guard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DfsRenewableLeaseConfig {
    pub renew_interval: Duration,
    pub command_timeout: Duration,
}

impl Default for DfsRenewableLeaseConfig {
    fn default() -> Self {
        Self {
            renew_interval: DEFAULT_DFS_RENEWABLE_LEASE_RENEW_INTERVAL,
            command_timeout: DEFAULT_DFS_RENEWABLE_LEASE_COMMAND_TIMEOUT,
        }
    }
}

/// Acquisition, health, or release failure for a renewable lease.
#[derive(Debug, Error)]
pub enum DfsRenewableLeaseError {
    #[error("renewable DFS lease configuration is invalid: {detail}")]
    InvalidConfig { detail: String },
    #[error("renewable DFS lease service is busy for {path}")]
    Busy { path: String },
    #[error("renewable DFS lease would block on {conflict:?}")]
    WouldBlock { conflict: DfsAdvisoryLockConflict },
    #[error("renewable DFS lease authority is warming until Unix nanos {retry_after_unix_nanos}")]
    AuthorityWarming { retry_after_unix_nanos: u64 },
    #[error("renewable DFS lease command timed out")]
    TimedOut,
    #[error("renewable DFS lease state mutex is poisoned")]
    StatePoisoned,
    #[error("renewable DFS lease service mutex is poisoned")]
    ServicePoisoned,
    #[error("renewable DFS lease renewer thread could not start: {detail}")]
    ThreadSpawn { detail: String },
    #[error("renewable DFS lease renewer thread panicked")]
    ThreadPanicked,
    #[error("renewable DFS lease was lost or fenced")]
    Lost,
    #[error("renewable DFS lease was already released")]
    Released,
    #[error("DFS advisory-lock operation failed: {source}")]
    Dfs {
        #[source]
        source: DfsPosixError,
    },
    #[error("unexpected DFS advisory-lock outcome: {outcome:?}")]
    UnexpectedOutcome { outcome: DfsAdvisoryLockOutcome },
    #[error("DFS authority returned a grant outside the requested lease fence")]
    GrantFenceMismatch,
}

impl DfsRenewableLeaseError {
    fn from_posix(source: DfsPosixError) -> Self {
        match source {
            DfsPosixError::Dfs(DfsError::Busy { path }) => Self::Busy { path },
            source => Self::Dfs { source },
        }
    }
}

#[derive(Debug)]
struct RenewableLeaseCommandResult {
    outcome: DfsAdvisoryLockOutcome,
    local_lease_deadline: Option<Instant>,
}

#[derive(Debug)]
struct RenewableLeaseState {
    grant: DfsAdvisoryLockGrant,
    local_lease_deadline: Instant,
    lost: bool,
    stop: bool,
    released: bool,
}

#[derive(Debug)]
struct RenewableLeaseStatusShared {
    state: Mutex<RenewableLeaseState>,
    changed: Condvar,
}

/// Cloneable loss notification handle for a renewable lease.
///
/// This handle deliberately does not own the transport-typed guard. A daemon
/// can move it to a cancellation thread while retaining the guard in the
/// lifecycle operation that must explicitly release the lease.
#[derive(Clone, Debug)]
pub struct DfsRenewableLeaseStatus {
    shared: Arc<RenewableLeaseStatusShared>,
}

impl DfsRenewableLeaseStatus {
    /// Returns true after authority fencing, a terminal renewal response, or
    /// the conservative local monotonic deadline. Loss is sticky.
    pub fn is_lost(&self) -> bool {
        let Ok(mut state) = self.shared.state.lock() else {
            return true;
        };
        observe_local_deadline(&self.shared, &mut state);
        state.lost
    }

    /// Wait until this lease is lost or normally released.
    ///
    /// Returns true only for loss. Normal explicit release wakes every waiter
    /// and returns false, allowing a cancellation monitor to exit without
    /// cancelling work that completed while authority was still held.
    pub fn wait_until_lost(&self) -> bool {
        loop {
            let mut state = match self.shared.state.lock() {
                Ok(state) => state,
                Err(_) => return true,
            };
            observe_local_deadline(&self.shared, &mut state);
            if state.lost {
                return true;
            }
            if state.released {
                return false;
            }
            let wait = state
                .local_lease_deadline
                .saturating_duration_since(Instant::now());
            match self.shared.changed.wait_timeout(state, wait) {
                Ok(_) => {}
                Err(_) => return true,
            }
        }
    }
}

#[derive(Debug)]
struct RenewableLeaseShared<T> {
    service: Arc<Mutex<NetworkedDfsService<T>>>,
    anchor: DfsRenewableLeaseAnchor,
    session_id: DfsAdvisoryLockSessionId,
    config: DfsRenewableLeaseConfig,
    status: Arc<RenewableLeaseStatusShared>,
}

/// RAII guard for one renewable, whole-file, exclusive DFS advisory lock.
///
/// The guard renews on a background thread. [`Self::ensure_held`] is the
/// operation boundary: callers must invoke it immediately before and after a
/// protected synchronous action. A failed renewal does not immediately invent
/// loss, but the original monotonic deadline remains authoritative and marks
/// the guard lost even if the renewer thread is delayed.
#[derive(Debug)]
pub struct DfsRenewableExclusiveLease<T: DfsMessageTransport + Send + 'static> {
    shared: Arc<RenewableLeaseShared<T>>,
    renewer: Option<JoinHandle<()>>,
}

impl<T: DfsMessageTransport + Send + 'static> DfsRenewableExclusiveLease<T> {
    /// Attempt an exclusive acquisition without entering the lock-manager wait
    /// queue. `WouldBlock` and authority warm-up are returned as typed errors.
    pub fn try_acquire(
        service: Arc<Mutex<NetworkedDfsService<T>>>,
        anchor: DfsRenewableLeaseAnchor,
        session_id: DfsAdvisoryLockSessionId,
        process_id: Option<u32>,
        config: DfsRenewableLeaseConfig,
    ) -> Result<Self, DfsRenewableLeaseError> {
        validate_config(config)?;
        let acquire_request_id = next_request_id(&session_id, "acquire");
        let command = DfsAdvisoryLockCommand::Acquire {
            request_id: acquire_request_id.clone(),
            session_id: session_id.clone(),
            mode: DfsAdvisoryLockMode::Exclusive,
            blocking: false,
            process_id,
        };
        let response = run_command(&service, &anchor, command, config.command_timeout)?;
        let grant = match response.outcome {
            DfsAdvisoryLockOutcome::Acquired { grant } => grant,
            DfsAdvisoryLockOutcome::WouldBlock { conflict } => {
                return Err(DfsRenewableLeaseError::WouldBlock { conflict });
            }
            DfsAdvisoryLockOutcome::AuthorityWarming {
                retry_after_unix_nanos,
            } => {
                return Err(DfsRenewableLeaseError::AuthorityWarming {
                    retry_after_unix_nanos,
                });
            }
            outcome => return Err(DfsRenewableLeaseError::UnexpectedOutcome { outcome }),
        };
        if !acquired_grant_matches(&grant, &anchor, &session_id, &acquire_request_id) {
            best_effort_release_grant(
                &service,
                &anchor,
                &session_id,
                grant,
                config.command_timeout,
                "fence-mismatch",
            );
            return Err(DfsRenewableLeaseError::GrantFenceMismatch);
        }
        let Some(local_lease_deadline) = response
            .local_lease_deadline
            .filter(|deadline| *deadline > Instant::now())
        else {
            best_effort_release_grant(
                &service,
                &anchor,
                &session_id,
                grant,
                config.command_timeout,
                "elapsed-acquire",
            );
            return Err(DfsRenewableLeaseError::Lost);
        };
        let shared = Arc::new(RenewableLeaseShared {
            service,
            anchor,
            session_id,
            config,
            status: Arc::new(RenewableLeaseStatusShared {
                state: Mutex::new(RenewableLeaseState {
                    grant,
                    local_lease_deadline,
                    lost: false,
                    stop: false,
                    released: false,
                }),
                changed: Condvar::new(),
            }),
        });
        let thread_shared = Arc::clone(&shared);
        let renewer = match thread::Builder::new()
            .name("sinomo-dfs-lease-renewer".to_string())
            .spawn(move || renew_loop(thread_shared))
        {
            Ok(renewer) => renewer,
            Err(source) => {
                best_effort_release_without_thread(&shared);
                return Err(DfsRenewableLeaseError::ThreadSpawn {
                    detail: source.to_string(),
                });
            }
        };
        Ok(Self {
            shared,
            renewer: Some(renewer),
        })
    }

    pub fn anchor(&self) -> &DfsRenewableLeaseAnchor {
        &self.shared.anchor
    }

    pub fn session_id(&self) -> &DfsAdvisoryLockSessionId {
        &self.shared.session_id
    }

    /// Clone a transport-independent observer suitable for a guest-cancellation
    /// monitor or another thread that must not own the lease guard.
    pub fn status(&self) -> DfsRenewableLeaseStatus {
        DfsRenewableLeaseStatus {
            shared: Arc::clone(&self.shared.status),
        }
    }

    /// Returns the current exact grant only while its local monotonic deadline
    /// remains live.
    pub fn grant(&self) -> Result<DfsAdvisoryLockGrant, DfsRenewableLeaseError> {
        let mut state = lock_state(&self.shared)?;
        observe_local_deadline(&self.shared.status, &mut state);
        if state.released {
            return Err(DfsRenewableLeaseError::Released);
        }
        if state.lost {
            return Err(DfsRenewableLeaseError::Lost);
        }
        Ok(state.grant.clone())
    }

    /// Fence a protected action against an elapsed, remotely rejected, or
    /// authority-epoch-fenced lease.
    pub fn ensure_held(&self) -> Result<(), DfsRenewableLeaseError> {
        self.grant().map(|_| ())
    }

    pub fn is_lost(&self) -> bool {
        self.status().is_lost()
    }

    /// Stop renewal, then idempotently unlock the exact grant and close its
    /// session. Both commands use exact-payload retry and bounded deadlines.
    pub fn release(mut self) -> Result<(), DfsRenewableLeaseError> {
        self.release_once()
    }

    fn release_once(&mut self) -> Result<(), DfsRenewableLeaseError> {
        let release_deadline = Instant::now()
            .checked_add(
                self.shared
                    .config
                    .command_timeout
                    .min(DEFAULT_DFS_RENEWABLE_LEASE_RELEASE_TIMEOUT),
            )
            .ok_or(DfsRenewableLeaseError::TimedOut)?;
        let grant = {
            let mut state = lock_state(&self.shared)?;
            if state.released {
                return Ok(());
            }
            state.stop = true;
            state.released = true;
            state.grant.clone()
        };
        self.shared.status.changed.notify_all();

        let mut first_error = None;
        if let Some(renewer) = self.renewer.take() {
            // A renewer may itself be stalled in transport. Never let joining it
            // escape the cleanup deadline; the released state prevents a detached
            // late response from reviving the guard, and authority expiry remains
            // the safe fallback when best-effort unlock cannot acquire the service.
            if renewer.is_finished() && renewer.join().is_err() {
                first_error = Some(DfsRenewableLeaseError::ThreadPanicked);
            }
        }

        let unlock = DfsAdvisoryLockCommand::Unlock {
            request_id: next_request_id(&self.shared.session_id, "unlock"),
            token: grant.token,
        };
        match run_command_until(
            &self.shared.service,
            &self.shared.anchor,
            unlock,
            release_deadline,
        ) {
            Ok(RenewableLeaseCommandResult {
                outcome:
                    DfsAdvisoryLockOutcome::Unlocked
                    | DfsAdvisoryLockOutcome::LockLost
                    | DfsAdvisoryLockOutcome::StaleToken
                    | DfsAdvisoryLockOutcome::Fenced { .. },
                ..
            }) => {}
            Ok(response) => {
                first_error.get_or_insert(DfsRenewableLeaseError::UnexpectedOutcome {
                    outcome: response.outcome,
                });
            }
            Err(error) => {
                first_error.get_or_insert(error);
            }
        };

        let close = DfsAdvisoryLockCommand::Close {
            request_id: next_request_id(&self.shared.session_id, "close"),
            session_id: self.shared.session_id.clone(),
        };
        match run_command_until(
            &self.shared.service,
            &self.shared.anchor,
            close,
            release_deadline,
        ) {
            Ok(RenewableLeaseCommandResult {
                outcome: DfsAdvisoryLockOutcome::Closed { .. },
                ..
            }) => {}
            Ok(response) => {
                first_error.get_or_insert(DfsRenewableLeaseError::UnexpectedOutcome {
                    outcome: response.outcome,
                });
            }
            Err(error) => {
                first_error.get_or_insert(error);
            }
        };
        first_error.map_or(Ok(()), Err)
    }
}

impl<T: DfsMessageTransport + Send + 'static> Drop for DfsRenewableExclusiveLease<T> {
    fn drop(&mut self) {
        if let Err(error) = self.release_once() {
            tracing::debug!(error = %error, "best-effort renewable DFS lease release failed");
        }
    }
}

fn validate_config(config: DfsRenewableLeaseConfig) -> Result<(), DfsRenewableLeaseError> {
    if config.renew_interval.is_zero() {
        return Err(DfsRenewableLeaseError::InvalidConfig {
            detail: "renew_interval must be greater than zero".to_string(),
        });
    }
    if config.command_timeout.is_zero() {
        return Err(DfsRenewableLeaseError::InvalidConfig {
            detail: "command_timeout must be greater than zero".to_string(),
        });
    }
    Ok(())
}

fn lock_state<'a, T>(
    shared: &'a RenewableLeaseShared<T>,
) -> Result<MutexGuard<'a, RenewableLeaseState>, DfsRenewableLeaseError> {
    shared
        .status
        .state
        .lock()
        .map_err(|_| DfsRenewableLeaseError::StatePoisoned)
}

fn observe_local_deadline(status: &RenewableLeaseStatusShared, state: &mut RenewableLeaseState) {
    if !state.released && !state.lost && state.local_lease_deadline <= Instant::now() {
        state.lost = true;
        status.changed.notify_all();
    }
}

fn mark_lost(status: &RenewableLeaseStatusShared, state: &mut RenewableLeaseState) {
    if !state.lost {
        state.lost = true;
        status.changed.notify_all();
    }
}

fn next_request_id(
    session_id: &DfsAdvisoryLockSessionId,
    operation: &str,
) -> DfsAdvisoryLockRequestId {
    let unix_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    DfsAdvisoryLockRequestId::new(format!(
        "renewable-{}-{operation}-{unix_nanos}-{}",
        session_id.0,
        NEXT_RENEWABLE_LEASE_REQUEST.fetch_add(1, Ordering::Relaxed)
    ))
}

fn acquired_grant_matches(
    grant: &DfsAdvisoryLockGrant,
    anchor: &DfsRenewableLeaseAnchor,
    session_id: &DfsAdvisoryLockSessionId,
    acquire_request_id: &DfsAdvisoryLockRequestId,
) -> bool {
    grant.mode == DfsAdvisoryLockMode::Exclusive
        && grant.token.object_id == anchor.object_id
        // The service scopes caller ids with its node identity before the
        // command crosses the lock-manager boundary. The caller-generated
        // identity remains the exact suffix of that scoped capability.
        && grant.token.session_id.0.ends_with(&session_id.0)
        && grant.token.grant_id.0.ends_with(&acquire_request_id.0)
}

fn renewed_grant_matches(grant: &DfsAdvisoryLockGrant, token: &DfsAdvisoryLockToken) -> bool {
    grant.mode == DfsAdvisoryLockMode::Exclusive && grant.token == *token
}

fn renew_loop<T: DfsMessageTransport + Send + 'static>(shared: Arc<RenewableLeaseShared<T>>) {
    loop {
        let (grant, wait) = {
            let mut state = match shared.status.state.lock() {
                Ok(state) => state,
                Err(poisoned) => {
                    mark_lost(&shared.status, &mut poisoned.into_inner());
                    return;
                }
            };
            observe_local_deadline(&shared.status, &mut state);
            if state.stop || state.released || state.lost {
                return;
            }
            let until_deadline = state
                .local_lease_deadline
                .saturating_duration_since(Instant::now());
            (
                state.grant.clone(),
                shared.config.renew_interval.min(until_deadline),
            )
        };

        let state = match shared.status.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                mark_lost(&shared.status, &mut poisoned.into_inner());
                return;
            }
        };
        let (mut state, _) = match shared.status.changed.wait_timeout(state, wait) {
            Ok(result) => result,
            Err(poisoned) => {
                let (mut state, _) = poisoned.into_inner();
                mark_lost(&shared.status, &mut state);
                return;
            }
        };
        observe_local_deadline(&shared.status, &mut state);
        if state.stop || state.released || state.lost {
            return;
        }
        // Another renewal cannot race this single renewer, but release may
        // have changed lifecycle state while the thread slept.
        if state.grant.token != grant.token {
            mark_lost(&shared.status, &mut state);
            return;
        }
        drop(state);

        let command = DfsAdvisoryLockCommand::Renew {
            request_id: next_request_id(&shared.session_id, "renew"),
            token: grant.token.clone(),
        };
        let response = run_command(
            &shared.service,
            &shared.anchor,
            command,
            shared.config.command_timeout,
        );
        let mut state = match shared.status.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                mark_lost(&shared.status, &mut poisoned.into_inner());
                return;
            }
        };
        if state.stop || state.released {
            return;
        }
        // Once the previously certified local deadline elapses, uncertainty is
        // terminal for this guard. A late renewal response must never revive a
        // capability that callers have already fenced as lost.
        observe_local_deadline(&shared.status, &mut state);
        if state.lost {
            return;
        }
        if state.grant.token != grant.token {
            mark_lost(&shared.status, &mut state);
            return;
        }
        match response {
            Ok(RenewableLeaseCommandResult {
                outcome: DfsAdvisoryLockOutcome::Renewed { grant: renewed },
                local_lease_deadline: Some(deadline),
            }) if renewed_grant_matches(&renewed, &grant.token) && deadline > Instant::now() => {
                state.grant = renewed;
                state.local_lease_deadline = deadline;
            }
            Ok(_) => mark_lost(&shared.status, &mut state),
            Err(_) => observe_local_deadline(&shared.status, &mut state),
        }
    }
}

fn run_command<T: DfsMessageTransport>(
    service: &Arc<Mutex<NetworkedDfsService<T>>>,
    anchor: &DfsRenewableLeaseAnchor,
    command: DfsAdvisoryLockCommand,
    timeout: Duration,
) -> Result<RenewableLeaseCommandResult, DfsRenewableLeaseError> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(DfsRenewableLeaseError::TimedOut)?;
    run_command_until(service, anchor, command, deadline)
}

fn run_command_until<T: DfsMessageTransport>(
    service: &Arc<Mutex<NetworkedDfsService<T>>>,
    anchor: &DfsRenewableLeaseAnchor,
    command: DfsAdvisoryLockCommand,
    deadline: Instant,
) -> Result<RenewableLeaseCommandResult, DfsRenewableLeaseError> {
    // The authority reports relative remaining validity after settling the
    // command. Projecting it from request start is conservative across service
    // queueing, RPC work, and the exact retry.
    let started_at = Instant::now();
    let mut service = lock_service_until(service, deadline)?;
    let retry_command = command.clone();
    let first_budget = deadline.saturating_duration_since(Instant::now());
    if first_budget.is_zero() {
        return Err(DfsRenewableLeaseError::TimedOut);
    }
    let first = service.posix_advisory_lock_for_object_with_timeout(
        &anchor.path,
        &anchor.object_id,
        command,
        first_budget,
    );
    let outcome = match first {
        Ok(outcome) => outcome,
        Err(first_error) => {
            let retry_budget = deadline.saturating_duration_since(Instant::now());
            if retry_budget.is_zero() {
                return Err(DfsRenewableLeaseError::from_posix(first_error));
            }
            service
                .posix_advisory_lock_for_object_with_timeout(
                    &anchor.path,
                    &anchor.object_id,
                    retry_command,
                    retry_budget,
                )
                .map_err(DfsRenewableLeaseError::from_posix)?
        }
    };
    let local_lease_deadline = lease_remaining(&outcome)
        .filter(|remaining| !remaining.is_zero())
        .and_then(|remaining| started_at.checked_add(remaining));
    Ok(RenewableLeaseCommandResult {
        outcome,
        local_lease_deadline,
    })
}

fn lock_service_until<'a, T>(
    service: &'a Arc<Mutex<NetworkedDfsService<T>>>,
    deadline: Instant,
) -> Result<MutexGuard<'a, NetworkedDfsService<T>>, DfsRenewableLeaseError> {
    loop {
        match service.try_lock() {
            Ok(service) => return Ok(service),
            Err(TryLockError::Poisoned(_)) => {
                return Err(DfsRenewableLeaseError::ServicePoisoned);
            }
            Err(TryLockError::WouldBlock) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(DfsRenewableLeaseError::TimedOut);
                }
                thread::sleep(RENEWABLE_LEASE_SERVICE_POLL_INTERVAL.min(remaining));
            }
        }
    }
}

fn lease_remaining(outcome: &DfsAdvisoryLockOutcome) -> Option<Duration> {
    match outcome {
        DfsAdvisoryLockOutcome::Acquired { grant }
        | DfsAdvisoryLockOutcome::Renewed { grant }
        | DfsAdvisoryLockOutcome::Converted { grant } => {
            Some(Duration::from_nanos(grant.lease_remaining_nanos))
        }
        _ => None,
    }
}

fn best_effort_release_without_thread<T: DfsMessageTransport>(shared: &RenewableLeaseShared<T>) {
    let grant = match shared.status.state.lock() {
        Ok(mut state) => {
            state.stop = true;
            state.released = true;
            state.grant.clone()
        }
        Err(_) => return,
    };
    best_effort_release_grant(
        &shared.service,
        &shared.anchor,
        &shared.session_id,
        grant,
        shared.config.command_timeout,
        "spawn-failed",
    );
}

fn best_effort_release_grant<T: DfsMessageTransport>(
    service: &Arc<Mutex<NetworkedDfsService<T>>>,
    anchor: &DfsRenewableLeaseAnchor,
    session_id: &DfsAdvisoryLockSessionId,
    grant: DfsAdvisoryLockGrant,
    command_timeout: Duration,
    operation: &str,
) {
    let Some(deadline) = Instant::now()
        .checked_add(command_timeout.min(DEFAULT_DFS_RENEWABLE_LEASE_RELEASE_TIMEOUT))
    else {
        return;
    };
    let _ = run_command_until(
        service,
        anchor,
        DfsAdvisoryLockCommand::Unlock {
            request_id: next_request_id(session_id, &format!("{operation}-unlock")),
            token: grant.token,
        },
        deadline,
    );
    let _ = run_command_until(
        service,
        anchor,
        DfsAdvisoryLockCommand::Close {
            request_id: next_request_id(session_id, &format!("{operation}-close")),
            session_id: session_id.clone(),
        },
        deadline,
    );
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::path::Path;

    use sinomo_core::network::NetworkError;
    use sinomo_core::node::NodeId;

    use super::*;
    use crate::{
        DfsMessageTransport, DfsReplicationPolicy, DfsRoutedCall, DfsRoutedMessage,
        DfsTransportIdentity, DfsWriteOptions, LocalContentStore, NetworkedDfsService,
        open_root_local_metadata_backend,
    };

    #[derive(Debug)]
    struct LocalOnlyTransport {
        node_id: NodeId,
        incoming: VecDeque<DfsRoutedMessage>,
    }

    impl LocalOnlyTransport {
        fn new(node_id: NodeId) -> Self {
            Self {
                node_id,
                incoming: VecDeque::new(),
            }
        }
    }

    impl DfsTransportIdentity for LocalOnlyTransport {
        fn local_node_id(&self) -> &NodeId {
            &self.node_id
        }
    }

    impl DfsMessageTransport for LocalOnlyTransport {
        fn send_routed_call(
            &mut self,
            target_node_id: &NodeId,
            _call: DfsRoutedCall,
        ) -> Result<(), NetworkError> {
            Err(NetworkError::UnknownNode {
                node_id: target_node_id.clone(),
            })
        }

        fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
            Ok(self.incoming.drain(..).collect())
        }
    }

    fn open_service(
        root: &Path,
        node_id: NodeId,
        content_name: &str,
    ) -> NetworkedDfsService<LocalOnlyTransport> {
        NetworkedDfsService::new(
            LocalOnlyTransport::new(node_id),
            LocalContentStore::new(root.join(content_name)).expect("lease test content store"),
        )
        .with_metadata_backend(
            open_root_local_metadata_backend(root.join("metadata"))
                .expect("lease test metadata backend"),
        )
        .expect("attach lease test metadata backend")
    }

    fn two_services(
        name: &str,
    ) -> (
        sinomo_testdir::TestDir,
        Arc<Mutex<NetworkedDfsService<LocalOnlyTransport>>>,
        Arc<Mutex<NetworkedDfsService<LocalOnlyTransport>>>,
        DfsRenewableLeaseAnchor,
    ) {
        let root = sinomo_testdir::TestDir::new(&format!("sinomo-dfs-{name}"));
        let node_id = NodeId::from_legacy_named(&format!("node_{name}").replace('-', "_"))
            .expect("lease test node id");
        let mut first = open_service(&root, node_id.clone(), "content-a");
        first
            .bootstrap_distributed_sifs()
            .expect("bootstrap lease test SIFS");
        let path = "/users/renewable-lease-anchor.lock";
        first
            .posix_write_file(
                path,
                b"sinomo-renewable-lease-v1\n",
                DfsWriteOptions {
                    policy: DfsReplicationPolicy::new(1, 1).expect("lease test policy"),
                    replica_candidates: vec![node_id.clone()],
                    wait_for_commit: true,
                    foreground_replica_repair: true,
                },
            )
            .expect("create stable lease anchor");
        let object_id = first
            .posix_stat(path)
            .expect("stat stable lease anchor")
            .object_id;
        let second = open_service(&root, node_id, "content-b");
        (
            root,
            Arc::new(Mutex::new(first)),
            Arc::new(Mutex::new(second)),
            DfsRenewableLeaseAnchor::new(path, object_id),
        )
    }

    fn fast_config() -> DfsRenewableLeaseConfig {
        DfsRenewableLeaseConfig {
            renew_interval: Duration::from_millis(20),
            command_timeout: Duration::from_secs(1),
        }
    }

    #[test]
    fn default_command_timeout_preserves_full_advisory_lock_rpc_budget() {
        assert_eq!(
            DfsRenewableLeaseConfig::default().command_timeout,
            DEFAULT_DFS_ADVISORY_LOCK_RPC_TIMEOUT
        );
    }

    fn wait_until(mut predicate: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !predicate() {
            assert!(
                Instant::now() < deadline,
                "renewable lease condition did not become true"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn two_services_exclude_renew_and_release_an_exclusive_lease() {
        let (_root, first, second, anchor) = two_services("renewable-lease-exclusion");
        let first_session = DfsAdvisoryLockSessionId::new("lease-holder-a");
        let lease = DfsRenewableExclusiveLease::try_acquire(
            Arc::clone(&first),
            anchor.clone(),
            first_session,
            Some(1001),
            fast_config(),
        )
        .expect("first exclusive lease");
        let original_deadline = lease
            .grant()
            .expect("initial live grant")
            .lease_deadline_unix_nanos;

        let contender = DfsRenewableExclusiveLease::try_acquire(
            Arc::clone(&second),
            anchor.clone(),
            DfsAdvisoryLockSessionId::new("lease-holder-b-before-release"),
            Some(1002),
            fast_config(),
        );
        assert!(matches!(
            contender,
            Err(DfsRenewableLeaseError::WouldBlock { .. })
        ));

        wait_until(|| {
            lease
                .grant()
                .is_ok_and(|grant| grant.lease_deadline_unix_nanos > original_deadline)
        });
        lease.ensure_held().expect("renewed lease remains held");
        let released_status = lease.status();
        lease.release().expect("explicit lease release");
        assert!(
            !released_status.wait_until_lost(),
            "normal release wakes observers without reporting loss"
        );

        let successor = DfsRenewableExclusiveLease::try_acquire(
            second,
            anchor,
            DfsAdvisoryLockSessionId::new("lease-holder-b-after-release"),
            Some(1002),
            fast_config(),
        )
        .expect("successor acquires after release");
        successor.release().expect("successor release");
    }

    #[test]
    fn remote_session_close_marks_the_guard_lost_and_fences_use() {
        let (_root, first, second, anchor) = two_services("renewable-lease-loss");
        let session_id = DfsAdvisoryLockSessionId::new("lease-holder-lost");
        let lease = DfsRenewableExclusiveLease::try_acquire(
            first,
            anchor.clone(),
            session_id.clone(),
            Some(2001),
            fast_config(),
        )
        .expect("lease before remote close");
        let status = lease.status();
        let sticky_status = status.clone();
        let (observed_tx, observed_rx) = std::sync::mpsc::channel();
        let observer = thread::spawn(move || {
            observed_tx
                .send(status.wait_until_lost())
                .expect("send observed lease status");
        });

        let close_outcome = second
            .lock()
            .expect("second service")
            .posix_advisory_lock_for_object(
                &anchor.path,
                &anchor.object_id,
                DfsAdvisoryLockCommand::Close {
                    request_id: DfsAdvisoryLockRequestId::new("external-session-close"),
                    session_id,
                },
            )
            .expect("external close");
        assert!(matches!(
            close_outcome,
            DfsAdvisoryLockOutcome::Closed {
                released_grant: true,
                ..
            }
        ));

        assert!(
            observed_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("lease-loss observer notification"),
            "authority loss must notify the observer as loss"
        );
        observer.join().expect("lease-loss observer");
        assert!(sticky_status.is_lost(), "observed loss must remain sticky");
        assert!(lease.is_lost());
        assert!(matches!(
            lease.ensure_held(),
            Err(DfsRenewableLeaseError::Lost)
        ));

        let successor = DfsRenewableExclusiveLease::try_acquire(
            Arc::clone(&second),
            anchor,
            DfsAdvisoryLockSessionId::new("lease-successor-after-loss"),
            Some(2002),
            fast_config(),
        )
        .expect("successor acquires after lost holder is fenced");
        successor.release().expect("successor release");
        lease.release().expect("lost guard release is idempotent");
    }

    #[test]
    fn elapsed_monotonic_deadline_is_sticky_until_release() {
        let (_root, first, second, anchor) = two_services("renewable-lease-deadline");
        let lease = DfsRenewableExclusiveLease::try_acquire(
            first,
            anchor.clone(),
            DfsAdvisoryLockSessionId::new("lease-holder-deadline"),
            Some(3001),
            fast_config(),
        )
        .expect("lease before local deadline");
        lease
            .shared
            .status
            .state
            .lock()
            .expect("lease state")
            .local_lease_deadline = Instant::now();

        assert!(lease.is_lost());
        assert!(matches!(
            lease.ensure_held(),
            Err(DfsRenewableLeaseError::Lost)
        ));
        thread::sleep(fast_config().renew_interval * 2);
        assert!(
            lease.is_lost(),
            "a renewal response must not revive an elapsed local lease"
        );
        lease.release().expect("release elapsed lease");

        let successor = DfsRenewableExclusiveLease::try_acquire(
            second,
            anchor,
            DfsAdvisoryLockSessionId::new("lease-successor-after-deadline"),
            Some(3002),
            fast_config(),
        )
        .expect("successor acquires after elapsed guard release");
        successor.release().expect("successor release");
    }

    #[test]
    fn stalled_unlock_and_close_share_one_release_deadline() {
        let (_root, first, _second, anchor) = two_services("renewable-lease-release-budget");
        let config = DfsRenewableLeaseConfig {
            renew_interval: Duration::from_secs(60),
            command_timeout: Duration::from_millis(300),
        };
        let lease = DfsRenewableExclusiveLease::try_acquire(
            Arc::clone(&first),
            anchor,
            DfsAdvisoryLockSessionId::new("lease-holder-stalled-release"),
            Some(4001),
            config,
        )
        .expect("lease before stalled release");
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let (unlock_tx, unlock_rx) = std::sync::mpsc::channel();
        let holder = thread::spawn(move || {
            let _service = first.lock().expect("hold lease service");
            locked_tx.send(()).expect("report held service");
            unlock_rx.recv().expect("release held service");
        });
        locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("service lock held");

        let started_at = Instant::now();
        let error = lease
            .release()
            .expect_err("stalled cleanup should exhaust its shared deadline");
        let elapsed = started_at.elapsed();

        unlock_tx.send(()).expect("release service holder");
        holder.join().expect("service holder");
        assert!(matches!(error, DfsRenewableLeaseError::TimedOut));
        assert!(
            elapsed < Duration::from_millis(450),
            "unlock and close must share the 300ms release budget, elapsed {elapsed:?}"
        );
    }
}
