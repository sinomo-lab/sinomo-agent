//! The adapter side of the mounted-mutation handoff.
//!
//! A mounted adapter brackets each mutation with steps the main daemon must
//! acknowledge: Arm before the mutation enters the service, and one closing
//! step after it (committed work, an observed error, or a no-write proof).
//! This module owns what every adapter does with a step the daemon has not
//! acknowledged:
//!
//! - **Silence is sent again.** A step that got no answer is sent again after
//!   an interval that doubles to the daemon client's per-attempt bound. The
//!   interval is a fallback: an acknowledgement or a marker release on the
//!   same mount, and the mount's close, end the pause at once. Nothing counts
//!   attempts, and a slow daemon is never taken for a lost one.
//! - **A refusal ends the step.** A step the handoff owner refused, or that
//!   the adapter's own outbox cannot record, is not sent again.
//! - **A call's deadline ends the call, not the step.** A step still
//!   unacknowledged when its call's deadline passes moves to the mount's
//!   re-sender, which keeps sending it until the daemon answers or the mount
//!   closes.

use crate::{
    DaemonDfsMountedMutationHandoffClient, DaemonDfsMountedMutationHandoffError,
    DfsCancellationFlag, DfsError, MountedSifsHostMountSessionLease,
    MountedSifsMutationArmRequestId, MountedSifsMutationCapturedWork,
    MountedSifsMutationErrorObservation, MountedSifsMutationLease, MountedSifsMutationMarkerId,
    MountedSifsMutationNonce, MountedSifsMutationOutboxPhase, MountedSifsMutationOutboxStore,
    MountedSifsMutationPrepareError,
};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// First interval before an unacknowledged handoff step is sent again. It
/// doubles to the daemon client's per-attempt bound. The timer only re-sends.
pub const MOUNTED_SIFS_HANDOFF_RESEND_FIRST: Duration = Duration::from_millis(250);

/// Why one attempt at a handoff step produced no acknowledgement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MountedSifsHandoffStepError {
    /// A definite refusal: the handoff owner answered and refused, or the
    /// adapter's own outbox cannot produce the step. Sending it again gets
    /// the same answer.
    Rejected(String),
    /// No answer yet: the daemon was not reached, did not answer within the
    /// attempt, or the outbox has no room for the step yet.
    Unacknowledged(String),
}

impl MountedSifsHandoffStepError {
    fn rejected(error: impl std::fmt::Display) -> Self {
        Self::Rejected(error.to_string())
    }
}

impl std::fmt::Display for MountedSifsHandoffStepError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(detail) => write!(formatter, "handoff step rejected: {detail}"),
            Self::Unacknowledged(detail) => {
                write!(formatter, "handoff step not acknowledged: {detail}")
            }
        }
    }
}

impl From<DaemonDfsMountedMutationHandoffError> for MountedSifsHandoffStepError {
    fn from(error: DaemonDfsMountedMutationHandoffError) -> Self {
        match error {
            DaemonDfsMountedMutationHandoffError::Rejected(error) => {
                Self::Rejected(error.to_string())
            }
            DaemonDfsMountedMutationHandoffError::Unacknowledged(error) => {
                Self::Unacknowledged(error.to_string())
            }
        }
    }
}

impl From<MountedSifsMutationPrepareError> for MountedSifsHandoffStepError {
    fn from(error: MountedSifsMutationPrepareError) -> Self {
        match error {
            // A full outbox is backpressure: the step waits for room.
            MountedSifsMutationPrepareError::NoRoom(error) => {
                Self::Unacknowledged(error.to_string())
            }
            MountedSifsMutationPrepareError::Refused(error) => Self::Rejected(error.to_string()),
        }
    }
}

/// How sending a step until its acknowledgement ended without one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MountedSifsHandoffEnded {
    /// The step was definitely refused.
    Rejected(String),
    /// The call's deadline passed or the mount is closing. The step may
    /// still be acknowledged later.
    Left,
}

/// Send one handoff step until it is acknowledged.
///
/// `events` reads the mount's handoff event count and `pause` waits out one
/// re-send interval given the count read before the attempt, returning
/// `false` once the waiting must end (the deadline passed or the mount is
/// closing). An event that arrives while an attempt is in flight therefore
/// ends the following pause at once.
pub fn mounted_sifs_handoff_until_acknowledged(
    events: impl Fn() -> u64,
    mut pause: impl FnMut(u64, u32) -> bool,
    deadline: Option<Instant>,
    mut step: impl FnMut() -> Result<(), MountedSifsHandoffStepError>,
) -> Result<(), MountedSifsHandoffEnded> {
    let mut attempt = 0;
    loop {
        if deadline.is_some_and(|at| Instant::now() >= at) {
            return Err(MountedSifsHandoffEnded::Left);
        }
        let seen = events();
        match step() {
            Ok(()) => return Ok(()),
            Err(MountedSifsHandoffStepError::Rejected(detail)) => {
                return Err(MountedSifsHandoffEnded::Rejected(detail));
            }
            Err(MountedSifsHandoffStepError::Unacknowledged(_)) => {}
        }
        if !pause(seen, attempt) {
            return Err(MountedSifsHandoffEnded::Left);
        }
        attempt = attempt.saturating_add(1);
    }
}

/// The daemon's end of the handoff, as an adapter drives it. One call is one
/// attempt at one step.
pub trait MountedSifsHandoffDaemon: std::fmt::Debug + Send + Sync {
    fn arm(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        marker_nonce: &MountedSifsMutationNonce,
        arm_request_id: &MountedSifsMutationArmRequestId,
    ) -> Result<(), MountedSifsHandoffStepError>;

    fn commit_applied(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        marker_nonce: &MountedSifsMutationNonce,
        arm_request_id: &MountedSifsMutationArmRequestId,
        captured_work: &MountedSifsMutationCapturedWork,
    ) -> Result<(), MountedSifsHandoffStepError>;

    fn commit_unknown(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        marker_nonce: &MountedSifsMutationNonce,
        arm_request_id: &MountedSifsMutationArmRequestId,
        observation: &MountedSifsMutationErrorObservation,
    ) -> Result<(), MountedSifsHandoffStepError>;

    fn prove_no_write(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        marker_nonce: &MountedSifsMutationNonce,
        arm_request_id: &MountedSifsMutationArmRequestId,
        proof_code: &str,
    ) -> Result<(), MountedSifsHandoffStepError>;

    fn abort_unstarted(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        marker_nonce: &MountedSifsMutationNonce,
    ) -> Result<(), MountedSifsHandoffStepError>;

    /// The longest one attempt waits for its answer. The re-send interval
    /// doubles up to this bound.
    fn attempt_bound(&self) -> Duration;
}

impl MountedSifsHandoffDaemon for DaemonDfsMountedMutationHandoffClient {
    fn arm(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        marker_nonce: &MountedSifsMutationNonce,
        arm_request_id: &MountedSifsMutationArmRequestId,
    ) -> Result<(), MountedSifsHandoffStepError> {
        DaemonDfsMountedMutationHandoffClient::arm(
            self,
            marker_id.clone(),
            marker_nonce.clone(),
            arm_request_id.clone(),
        )
        .map(drop)
        .map_err(Into::into)
    }

    fn commit_applied(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        marker_nonce: &MountedSifsMutationNonce,
        arm_request_id: &MountedSifsMutationArmRequestId,
        captured_work: &MountedSifsMutationCapturedWork,
    ) -> Result<(), MountedSifsHandoffStepError> {
        DaemonDfsMountedMutationHandoffClient::commit_applied(
            self,
            marker_id.clone(),
            marker_nonce.clone(),
            arm_request_id.clone(),
            captured_work.clone(),
        )
        .map(drop)
        .map_err(Into::into)
    }

    fn commit_unknown(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        marker_nonce: &MountedSifsMutationNonce,
        arm_request_id: &MountedSifsMutationArmRequestId,
        observation: &MountedSifsMutationErrorObservation,
    ) -> Result<(), MountedSifsHandoffStepError> {
        DaemonDfsMountedMutationHandoffClient::commit_unknown(
            self,
            marker_id.clone(),
            marker_nonce.clone(),
            arm_request_id.clone(),
            observation.clone(),
        )
        .map(drop)
        .map_err(Into::into)
    }

    fn prove_no_write(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        marker_nonce: &MountedSifsMutationNonce,
        arm_request_id: &MountedSifsMutationArmRequestId,
        proof_code: &str,
    ) -> Result<(), MountedSifsHandoffStepError> {
        DaemonDfsMountedMutationHandoffClient::prove_no_write(
            self,
            marker_id.clone(),
            marker_nonce.clone(),
            arm_request_id.clone(),
            proof_code,
        )
        .map(drop)
        .map_err(Into::into)
    }

    fn abort_unstarted(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        marker_nonce: &MountedSifsMutationNonce,
    ) -> Result<(), MountedSifsHandoffStepError> {
        DaemonDfsMountedMutationHandoffClient::abort_unstarted(
            self,
            marker_id.clone(),
            marker_nonce.clone(),
        )
        .map(drop)
        .map_err(Into::into)
    }

    fn attempt_bound(&self) -> Duration {
        self.timeout()
    }
}

/// What is left of a handoff whose call returned before the daemon
/// acknowledged it.
#[derive(Clone, Debug)]
pub enum MountedSifsUnfinishedHandoff {
    /// Arm was definitely refused, so the operation never ran; the marker is
    /// aborted as unstarted.
    Unstarted,
    /// Arm was sent and not acknowledged; the operation never entered the
    /// service. Once Arm is acknowledged the marker is closed as no-write.
    ArmedWithoutEntry {
        arm_request_id: MountedSifsMutationArmRequestId,
    },
    Captured {
        arm_request_id: MountedSifsMutationArmRequestId,
        captured_work: MountedSifsMutationCapturedWork,
    },
    Unknown {
        arm_request_id: MountedSifsMutationArmRequestId,
        observation: MountedSifsMutationErrorObservation,
    },
    NoWrite {
        arm_request_id: MountedSifsMutationArmRequestId,
        proof_code: String,
    },
}

/// The handoff events of one mount: an acknowledgement, a marker release, a
/// step handed to the re-sender, and the mount's close. Every pause between
/// re-sends waits here, so each of them ends it.
#[derive(Debug, Default)]
pub struct MountedSifsHandoffWake {
    state: Mutex<MountedSifsHandoffWakeState>,
    changed: Condvar,
}

#[derive(Debug, Default)]
struct MountedSifsHandoffWakeState {
    events: u64,
    closed: bool,
}

impl MountedSifsHandoffWake {
    fn state(&self) -> std::sync::MutexGuard<'_, MountedSifsHandoffWakeState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The count of events so far. Read it before an attempt and pass it to
    /// [`Self::wait`], so an event during the attempt is not missed.
    pub fn events(&self) -> u64 {
        self.state().events
    }

    /// Record one event and end every pause that started before it.
    pub fn notify(&self) {
        let mut state = self.state();
        state.events = state.events.wrapping_add(1);
        drop(state);
        self.changed.notify_all();
    }

    /// The mount is closing: end every pause for good.
    pub fn close(&self) {
        self.state().closed = true;
        self.changed.notify_all();
    }

    pub fn is_closed(&self) -> bool {
        self.state().closed
    }

    /// Wait until an event after `seen`, the close, or `until`. `false` once
    /// the mount is closing.
    pub fn wait(&self, seen: u64, until: Instant) -> bool {
        let mut state = self.state();
        loop {
            if state.closed {
                return false;
            }
            if state.events != seen {
                return true;
            }
            let remaining = until.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return true;
            }
            state = self
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
    }
}

struct PendingHandoff {
    due: Instant,
    last_attempt: Option<Instant>,
    attempt: u32,
    lease: MountedSifsMutationLease<'static>,
    step: MountedSifsUnfinishedHandoff,
    /// Counts this step as unfinished for as long as it exists.
    _held: UnfinishedHandoffCount,
}

/// One step counted among a mount's unfinished handoffs until it is dropped:
/// acknowledged, refused, or released by the mount's close.
struct UnfinishedHandoffCount(Arc<std::sync::atomic::AtomicUsize>);

impl UnfinishedHandoffCount {
    fn hold(count: &Arc<std::sync::atomic::AtomicUsize>) -> Self {
        count.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Self(Arc::clone(count))
    }
}

impl Drop for UnfinishedHandoffCount {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

/// The mounted-mutation handoff of one mount: its outbox session, its channel
/// to the daemon, and the re-sender that finishes steps whose calls returned.
pub struct MountedSifsMutationHandoff {
    store: MountedSifsMutationOutboxStore,
    session: Option<MountedSifsHostMountSessionLease>,
    daemon: Arc<dyn MountedSifsHandoffDaemon>,
    wake: Arc<MountedSifsHandoffWake>,
    pending: Arc<Mutex<Vec<PendingHandoff>>>,
    /// Steps the re-sender holds, the one it is sending included.
    unfinished: Arc<std::sync::atomic::AtomicUsize>,
    /// Closes the wake when the mount's own cancellation fires.
    _closing: Option<crate::service_state::DfsCancellationWakerRegistration>,
}

impl std::fmt::Debug for MountedSifsMutationHandoff {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MountedSifsMutationHandoff")
            .field("session", &self.session)
            .field("daemon", &self.daemon)
            .field("unfinished", &self.unfinished_count())
            .finish_non_exhaustive()
    }
}

impl MountedSifsMutationHandoff {
    /// Start the handoff of a mount whose `session` is already open in
    /// `store`. `closing`, when the mount has a cancellation of its own,
    /// closes the handoff as soon as it fires.
    ///
    /// The re-sender starts here, before the mount is exposed. Every step it
    /// holds owns an outbox lease, so the outbox's marker limit bounds it.
    pub fn start(
        store: MountedSifsMutationOutboxStore,
        session: MountedSifsHostMountSessionLease,
        daemon: Arc<dyn MountedSifsHandoffDaemon>,
        closing: Option<&DfsCancellationFlag>,
    ) -> std::io::Result<Self> {
        let wake = Arc::new(MountedSifsHandoffWake::default());
        let closing = closing.map(|flag| {
            let close = Arc::clone(&wake);
            let waker: crate::service_state::DfsCancellationWaker = Arc::new(move || close.close());
            let registration = flag.register_waker(&waker);
            if flag.is_cancelled() {
                wake.close();
            }
            registration
        });
        let pending = Arc::new(Mutex::new(Vec::new()));
        let unfinished = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let resender = MountedSifsHandoffResender {
            daemon: Arc::clone(&daemon),
            wake: Arc::clone(&wake),
            pending: Arc::clone(&pending),
        };
        std::thread::Builder::new()
            .name("sinomo-mount-handoff-resend".into())
            .spawn(move || resender.run())?;
        Ok(Self {
            store,
            session: Some(session),
            daemon,
            wake,
            pending,
            unfinished,
            _closing: closing,
        })
    }

    pub fn session(&self) -> Option<&MountedSifsHostMountSessionLease> {
        self.session.as_ref()
    }

    /// The events every pause of this mount's handoff waits on.
    pub fn wake(&self) -> &Arc<MountedSifsHandoffWake> {
        &self.wake
    }

    /// Steps the re-sender still holds: those waiting out an interval and
    /// the one it is sending now.
    pub fn unfinished_count(&self) -> usize {
        self.unfinished.load(std::sync::atomic::Ordering::Acquire)
    }

    /// The interval before attempt `attempt + 1` of a step.
    pub fn resend_delay(&self, attempt: u32) -> Duration {
        resend_delay(self.daemon.as_ref(), attempt)
    }

    /// Wait out one re-send interval, within `deadline`. `false` once the
    /// mount is closing or `deadline` has passed. An event after `seen`
    /// ends the wait at once.
    pub fn pause_before_resend(&self, seen: u64, attempt: u32, deadline: Option<Instant>) -> bool {
        let now = Instant::now();
        let mut until = now + self.resend_delay(attempt);
        if let Some(deadline) = deadline {
            if deadline <= now {
                return false;
            }
            until = until.min(deadline);
        }
        self.wake.wait(seen, until) && deadline.is_none_or(|at| Instant::now() < at)
    }

    /// Send `step` until it is acknowledged, within `deadline`.
    pub fn until_acknowledged(
        &self,
        deadline: Option<Instant>,
        step: impl FnMut() -> Result<(), MountedSifsHandoffStepError>,
    ) -> Result<(), MountedSifsHandoffEnded> {
        mounted_sifs_handoff_until_acknowledged(
            || self.wake.events(),
            |seen, attempt| self.pause_before_resend(seen, attempt, deadline),
            deadline,
            step,
        )
    }

    /// One attempt at a Prepared marker for `operation`. A full outbox is
    /// [`MountedSifsHandoffStepError::Unacknowledged`]: room is made when a
    /// marker is released.
    pub fn prepare(
        &self,
        operation: &str,
        stable_operation_id: Option<String>,
    ) -> Result<MountedSifsMutationLease<'_>, MountedSifsHandoffStepError> {
        let session = self.session.as_ref().ok_or_else(|| {
            MountedSifsHandoffStepError::Rejected(
                "mounted-mutation host-mount session is closed".into(),
            )
        })?;
        self.store
            .prepare(session, stable_operation_id, operation)
            .map_err(Into::into)
    }

    pub fn arm(
        &self,
        lease: &MountedSifsMutationLease<'_>,
        arm_request_id: &MountedSifsMutationArmRequestId,
    ) -> Result<(), MountedSifsHandoffStepError> {
        let marker = lease.marker();
        self.acknowledged(
            self.daemon
                .arm(&marker.marker_id, &marker.marker_nonce, arm_request_id),
        )
    }

    pub fn abort_unstarted(
        &self,
        lease: &MountedSifsMutationLease<'_>,
    ) -> Result<(), MountedSifsHandoffStepError> {
        let marker = lease.marker();
        self.acknowledged(
            self.daemon
                .abort_unstarted(&marker.marker_id, &marker.marker_nonce),
        )
    }

    pub fn persist_captured(
        &self,
        lease: &mut MountedSifsMutationLease<'_>,
        arm_request_id: &MountedSifsMutationArmRequestId,
        captured_work: &MountedSifsMutationCapturedWork,
    ) -> Result<(), MountedSifsHandoffStepError> {
        lease
            .record_captured_success(arm_request_id, captured_work.clone())
            .map_err(MountedSifsHandoffStepError::rejected)
    }

    pub fn persist_unknown(
        &self,
        lease: &mut MountedSifsMutationLease<'_>,
        arm_request_id: &MountedSifsMutationArmRequestId,
        observation: &MountedSifsMutationErrorObservation,
    ) -> Result<(), MountedSifsHandoffStepError> {
        lease
            .record_unknown_after_error(arm_request_id, observation.clone())
            .map_err(MountedSifsHandoffStepError::rejected)
    }

    pub fn persist_proven_no_write(
        &self,
        lease: &mut MountedSifsMutationLease<'_>,
        arm_request_id: &MountedSifsMutationArmRequestId,
        proof_code: &str,
    ) -> Result<(), MountedSifsHandoffStepError> {
        lease
            .record_proven_no_write(arm_request_id, proof_code)
            .map_err(MountedSifsHandoffStepError::rejected)
    }

    pub fn commit_captured(
        &self,
        lease: &MountedSifsMutationLease<'_>,
        arm_request_id: &MountedSifsMutationArmRequestId,
        captured_work: &MountedSifsMutationCapturedWork,
    ) -> Result<(), MountedSifsHandoffStepError> {
        let marker = lease.marker();
        self.acknowledged(self.daemon.commit_applied(
            &marker.marker_id,
            &marker.marker_nonce,
            arm_request_id,
            captured_work,
        ))
    }

    pub fn commit_unknown(
        &self,
        lease: &MountedSifsMutationLease<'_>,
        arm_request_id: &MountedSifsMutationArmRequestId,
        observation: &MountedSifsMutationErrorObservation,
    ) -> Result<(), MountedSifsHandoffStepError> {
        let marker = lease.marker();
        self.acknowledged(self.daemon.commit_unknown(
            &marker.marker_id,
            &marker.marker_nonce,
            arm_request_id,
            observation,
        ))
    }

    pub fn commit_proven_no_write(
        &self,
        lease: &MountedSifsMutationLease<'_>,
        arm_request_id: &MountedSifsMutationArmRequestId,
        proof_code: &str,
    ) -> Result<(), MountedSifsHandoffStepError> {
        let marker = lease.marker();
        self.acknowledged(self.daemon.prove_no_write(
            &marker.marker_id,
            &marker.marker_nonce,
            arm_request_id,
            proof_code,
        ))
    }

    /// Release a marker the daemon acknowledged. The release makes room in
    /// the outbox, so it ends the pause of a call waiting for room.
    pub fn release_after_daemon_ack(
        &self,
        lease: MountedSifsMutationLease<'_>,
    ) -> Result<(), DfsError> {
        let released = lease.release_after_daemon_ack();
        self.wake.notify();
        released
    }

    /// Give up a marker whose step was definitely refused. Its lease ends
    /// here; the daemon's recovery owns the marker from its durable phase.
    pub fn release_refused(&self, lease: MountedSifsMutationLease<'_>, detail: &str) {
        release_refused(&self.wake, lease, detail);
    }

    /// Keep sending a step the daemon has not acknowledged after its call
    /// returned, until the daemon answers or the mount closes.
    ///
    /// The marker stays this mount's: only its live operation lease lets the
    /// daemon accept the step, so the re-sender keeps the lease until then.
    pub fn finish_later(
        &self,
        lease: MountedSifsMutationLease<'_>,
        step: MountedSifsUnfinishedHandoff,
    ) {
        if self.wake.is_closed() {
            return;
        }
        if let Ok(mut pending) = self.pending.lock() {
            pending.push(PendingHandoff {
                due: Instant::now(),
                last_attempt: None,
                attempt: 0,
                lease: lease.into_owned(),
                step,
                _held: UnfinishedHandoffCount::hold(&self.unfinished),
            });
        }
        self.wake.notify();
    }

    /// End every wait of this mount's handoff and stop its re-sender. Steps
    /// it still holds keep their durable markers for the daemon's recovery.
    pub fn close(&self) {
        self.wake.close();
        // The leases end with the mount's session: release them before the
        // session is closed, as the session's close requires.
        if let Ok(mut pending) = self.pending.lock() {
            pending.clear();
        }
    }

    /// Close the handoff and its host-mount session.
    pub fn close_clean(&mut self) -> Result<(), DfsError> {
        self.close();
        match self.session.take() {
            Some(session) => session.close_clean(),
            None => Ok(()),
        }
    }

    fn acknowledged(
        &self,
        answer: Result<(), MountedSifsHandoffStepError>,
    ) -> Result<(), MountedSifsHandoffStepError> {
        if answer.is_ok() {
            // The daemon answers: a step waiting out its interval goes now.
            self.wake.notify();
        }
        answer
    }
}

impl Drop for MountedSifsMutationHandoff {
    fn drop(&mut self) {
        if let Err(error) = self.close_clean() {
            tracing::error!(
                target: "sinomo.dfs.mounted_mutation",
                %error,
                "failed to close a mounted-mutation host-mount session"
            );
        }
    }
}

fn resend_delay(daemon: &dyn MountedSifsHandoffDaemon, attempt: u32) -> Duration {
    MOUNTED_SIFS_HANDOFF_RESEND_FIRST
        .saturating_mul(1 << attempt.min(16))
        .min(daemon.attempt_bound())
}

fn release_refused(
    wake: &MountedSifsHandoffWake,
    lease: MountedSifsMutationLease<'_>,
    detail: &str,
) {
    let marker = lease.marker();
    tracing::error!(
        target: "sinomo.dfs.mounted_mutation",
        marker_id = %marker.marker_id,
        mount_session_id = %marker.origin.mount_session_id,
        operation = %marker.origin.operation,
        phase = ?marker.phase,
        detail,
        "a mounted-mutation handoff step was refused; its marker is left to recovery"
    );
    drop(lease);
    wake.notify();
}

struct MountedSifsHandoffResender {
    daemon: Arc<dyn MountedSifsHandoffDaemon>,
    wake: Arc<MountedSifsHandoffWake>,
    pending: Arc<Mutex<Vec<PendingHandoff>>>,
}

impl MountedSifsHandoffResender {
    fn run(self) {
        let mut seen = self.wake.events();
        loop {
            if self.wake.is_closed() {
                return;
            }
            let now = Instant::now();
            let due = {
                let Ok(mut pending) = self.pending.lock() else {
                    return;
                };
                let index = pending.iter().position(|item| item.due <= now);
                index.map(|index| pending.swap_remove(index))
            };
            if let Some(mut item) = due {
                item.last_attempt = Some(Instant::now());
                match send_unfinished_step(self.daemon.as_ref(), &mut item.lease, &mut item.step) {
                    Ok(()) => {
                        if let Err(error) = item.lease.release_after_daemon_ack() {
                            tracing::error!(
                                target: "sinomo.dfs.mounted_mutation",
                                %error,
                                "an acknowledged mounted-mutation marker did not release"
                            );
                        }
                        self.wake.notify();
                    }
                    Err(MountedSifsHandoffStepError::Rejected(detail)) => {
                        release_refused(&self.wake, item.lease, &detail);
                    }
                    Err(MountedSifsHandoffStepError::Unacknowledged(_)) => {
                        item.due =
                            Instant::now() + resend_delay(self.daemon.as_ref(), item.attempt);
                        item.attempt = item.attempt.saturating_add(1);
                        // The mount may have closed during the attempt; its
                        // close already released every lease still queued.
                        if self.wake.is_closed() {
                            return;
                        }
                        if let Ok(mut pending) = self.pending.lock() {
                            pending.push(item);
                        }
                    }
                }
                continue;
            }
            let next_due = self
                .pending
                .lock()
                .ok()
                .and_then(|pending| pending.iter().map(|item| item.due).min());
            // With nothing queued only an event can bring work.
            let until = next_due.unwrap_or_else(|| now + Duration::from_secs(3600));
            if !self.wake.wait(seen, until) {
                return;
            }
            let events = self.wake.events();
            if events != seen {
                seen = events;
                // The daemon answered, or a step arrived: a queued step need
                // not sit out the rest of its interval. One that was just
                // tried still waits the first interval.
                let now = Instant::now();
                if let Ok(mut pending) = self.pending.lock() {
                    for item in pending.iter_mut() {
                        let earliest = item
                            .last_attempt
                            .map_or(now, |at| at + MOUNTED_SIFS_HANDOFF_RESEND_FIRST);
                        item.due = item.due.min(earliest.max(now));
                    }
                }
            }
        }
    }
}

/// One attempt at what is left of a handoff.
fn send_unfinished_step(
    daemon: &dyn MountedSifsHandoffDaemon,
    lease: &mut MountedSifsMutationLease<'_>,
    step: &mut MountedSifsUnfinishedHandoff,
) -> Result<(), MountedSifsHandoffStepError> {
    let marker = lease.marker().clone();
    // A call may leave its worker either before or after the local evidence
    // reached disk. Persist before asking the daemon to accept it.
    if matches!(
        marker.phase,
        MountedSifsMutationOutboxPhase::Prepared | MountedSifsMutationOutboxPhase::Armed { .. }
    ) {
        match &*step {
            MountedSifsUnfinishedHandoff::Captured {
                arm_request_id,
                captured_work,
            } => lease
                .record_captured_success(arm_request_id, captured_work.clone())
                .map_err(MountedSifsHandoffStepError::rejected)?,
            MountedSifsUnfinishedHandoff::Unknown {
                arm_request_id,
                observation,
            } => lease
                .record_unknown_after_error(arm_request_id, observation.clone())
                .map_err(MountedSifsHandoffStepError::rejected)?,
            MountedSifsUnfinishedHandoff::NoWrite {
                arm_request_id,
                proof_code,
            } => lease
                .record_proven_no_write(arm_request_id, proof_code.as_str())
                .map_err(MountedSifsHandoffStepError::rejected)?,
            MountedSifsUnfinishedHandoff::ArmedWithoutEntry { .. }
            | MountedSifsUnfinishedHandoff::Unstarted => {}
        }
    }
    match step {
        MountedSifsUnfinishedHandoff::Unstarted => {
            daemon.abort_unstarted(&marker.marker_id, &marker.marker_nonce)
        }
        MountedSifsUnfinishedHandoff::ArmedWithoutEntry { arm_request_id } => {
            daemon.arm(&marker.marker_id, &marker.marker_nonce, arm_request_id)?;
            *step = MountedSifsUnfinishedHandoff::NoWrite {
                arm_request_id: arm_request_id.clone(),
                proof_code: "mount-arm-acknowledged-after-its-call".into(),
            };
            send_unfinished_step(daemon, lease, step)
        }
        MountedSifsUnfinishedHandoff::Captured {
            arm_request_id,
            captured_work,
        } => daemon.commit_applied(
            &marker.marker_id,
            &marker.marker_nonce,
            arm_request_id,
            captured_work,
        ),
        MountedSifsUnfinishedHandoff::Unknown {
            arm_request_id,
            observation,
        } => daemon.commit_unknown(
            &marker.marker_id,
            &marker.marker_nonce,
            arm_request_id,
            observation,
        ),
        MountedSifsUnfinishedHandoff::NoWrite {
            arm_request_id,
            proof_code,
        } => daemon.prove_no_write(
            &marker.marker_id,
            &marker.marker_nonce,
            arm_request_id,
            proof_code,
        ),
    }
}

/// The daemon's end of the handoff for adapter tests, over the real outbox.
/// It applies the daemon's own durable transitions, and a test makes it
/// silent or makes it refuse a step.
#[cfg(any(test, feature = "test-support"))]
#[derive(Debug)]
pub struct MountedSifsHandoffTestDaemon {
    store: MountedSifsMutationOutboxStore,
    silent: std::sync::atomic::AtomicBool,
    refused_steps: Mutex<std::collections::BTreeSet<&'static str>>,
    attempts: Mutex<Vec<&'static str>>,
    attempt_bound: Duration,
}

#[cfg(any(test, feature = "test-support"))]
impl MountedSifsHandoffTestDaemon {
    pub const ARM: &'static str = "arm";
    pub const COMMIT_APPLIED: &'static str = "commit-applied";
    pub const COMMIT_UNKNOWN: &'static str = "commit-unknown";
    pub const PROVE_NO_WRITE: &'static str = "prove-no-write";
    pub const ABORT_UNSTARTED: &'static str = "abort-unstarted";

    /// A daemon that answers every step. Its short per-attempt bound keeps
    /// every re-send interval short.
    pub fn new(store: MountedSifsMutationOutboxStore) -> Arc<Self> {
        Self::with_attempt_bound(store, Duration::from_millis(20))
    }

    pub fn with_attempt_bound(
        store: MountedSifsMutationOutboxStore,
        attempt_bound: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            silent: std::sync::atomic::AtomicBool::new(false),
            refused_steps: Mutex::default(),
            attempts: Mutex::default(),
            attempt_bound,
        })
    }

    /// A silent daemon answers nothing: every step is unacknowledged.
    pub fn set_silent(&self, silent: bool) {
        self.silent
            .store(silent, std::sync::atomic::Ordering::Release);
    }

    /// Definitely refuse `step` from now on.
    pub fn refuse(&self, step: &'static str) {
        self.refused_steps.lock().unwrap().insert(step);
    }

    pub fn refuse_nothing(&self) {
        self.refused_steps.lock().unwrap().clear();
    }

    /// How often `step` was attempted, answered or not.
    pub fn attempts_of(&self, step: &str) -> usize {
        self.attempts
            .lock()
            .unwrap()
            .iter()
            .filter(|attempted| **attempted == step)
            .count()
    }

    fn step(
        &self,
        step: &'static str,
        apply: impl FnOnce() -> Result<(), DfsError>,
    ) -> Result<(), MountedSifsHandoffStepError> {
        self.attempts.lock().unwrap().push(step);
        if self.silent.load(std::sync::atomic::Ordering::Acquire) {
            return Err(MountedSifsHandoffStepError::Unacknowledged(
                "the test daemon is silent".into(),
            ));
        }
        if self.refused_steps.lock().unwrap().contains(step) {
            return Err(MountedSifsHandoffStepError::Rejected(
                "the test daemon refused the step".into(),
            ));
        }
        apply().map_err(MountedSifsHandoffStepError::rejected)
    }
}

#[cfg(any(test, feature = "test-support"))]
impl MountedSifsHandoffDaemon for MountedSifsHandoffTestDaemon {
    fn arm(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        marker_nonce: &MountedSifsMutationNonce,
        arm_request_id: &MountedSifsMutationArmRequestId,
    ) -> Result<(), MountedSifsHandoffStepError> {
        self.step(Self::ARM, || {
            self.store
                .arm_live(marker_id, marker_nonce, arm_request_id)
                .map(drop)
        })
    }

    fn commit_applied(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        marker_nonce: &MountedSifsMutationNonce,
        arm_request_id: &MountedSifsMutationArmRequestId,
        captured_work: &MountedSifsMutationCapturedWork,
    ) -> Result<(), MountedSifsHandoffStepError> {
        self.step(Self::COMMIT_APPLIED, || {
            self.store
                .accept_captured_live(marker_id, marker_nonce, arm_request_id, captured_work)
                .map(drop)
        })
    }

    fn commit_unknown(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        marker_nonce: &MountedSifsMutationNonce,
        arm_request_id: &MountedSifsMutationArmRequestId,
        observation: &MountedSifsMutationErrorObservation,
    ) -> Result<(), MountedSifsHandoffStepError> {
        self.step(Self::COMMIT_UNKNOWN, || {
            self.store
                .accept_unknown_live(marker_id, marker_nonce, arm_request_id, observation)
                .map(drop)
        })
    }

    fn prove_no_write(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        marker_nonce: &MountedSifsMutationNonce,
        arm_request_id: &MountedSifsMutationArmRequestId,
        proof_code: &str,
    ) -> Result<(), MountedSifsHandoffStepError> {
        self.step(Self::PROVE_NO_WRITE, || {
            self.store
                .prove_no_write_live(marker_id, marker_nonce, arm_request_id, proof_code)
                .map(drop)
        })
    }

    fn abort_unstarted(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        marker_nonce: &MountedSifsMutationNonce,
    ) -> Result<(), MountedSifsHandoffStepError> {
        self.step(Self::ABORT_UNSTARTED, || {
            self.store
                .abort_unstarted_live(marker_id, marker_nonce)
                .map(drop)
        })
    }

    fn attempt_bound(&self) -> Duration {
        self.attempt_bound
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS, MountedSifsMutationAdapter};
    use sinomo_core::local::SinomoDataDir;

    type TestDaemon = MountedSifsHandoffTestDaemon;

    struct TestMount {
        handoff: MountedSifsMutationHandoff,
        daemon: Arc<TestDaemon>,
        store: MountedSifsMutationOutboxStore,
        _root: sinomo_testdir::TestDir,
    }

    fn test_mount(name: &str, attempt_bound: Duration) -> TestMount {
        let root = sinomo_testdir::TestDir::new(name);
        let store = MountedSifsMutationOutboxStore::new(&SinomoDataDir::new(root.path())).unwrap();
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::Nfs, "nfs:handoff-test")
            .unwrap();
        let daemon = TestDaemon::with_attempt_bound(store.clone(), attempt_bound);
        let handoff =
            MountedSifsMutationHandoff::start(store.clone(), session, daemon.clone(), None)
                .unwrap();
        TestMount {
            handoff,
            daemon,
            store,
            _root: root,
        }
    }

    /// Wait for `reached`, which an event the test already caused makes true.
    fn eventually(what: &str, reached: impl Fn() -> bool) {
        let until = Instant::now() + Duration::from_secs(20);
        while !reached() {
            assert!(Instant::now() < until, "{what} did not happen");
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    #[test]
    fn silence_is_sent_again_without_a_cap_and_a_refusal_ends_the_step() {
        let attempts = std::cell::Cell::new(0u32);
        let answered = mounted_sifs_handoff_until_acknowledged(
            || 0,
            |_, _| true,
            None,
            || {
                attempts.set(attempts.get() + 1);
                if attempts.get() <= 500 {
                    Err(MountedSifsHandoffStepError::Unacknowledged("silent".into()))
                } else {
                    Ok(())
                }
            },
        );
        assert_eq!(answered, Ok(()));
        assert_eq!(attempts.get(), 501, "nothing counts attempts");

        attempts.set(0);
        let refused = mounted_sifs_handoff_until_acknowledged(
            || 0,
            |_, _| panic!("a refusal is never paused on"),
            None,
            || {
                attempts.set(attempts.get() + 1);
                Err(MountedSifsHandoffStepError::Rejected("refused".into()))
            },
        );
        assert_eq!(
            refused,
            Err(MountedSifsHandoffEnded::Rejected("refused".into()))
        );
        assert_eq!(attempts.get(), 1);

        let past = mounted_sifs_handoff_until_acknowledged(
            || 0,
            |_, _| true,
            Some(Instant::now()),
            || panic!("a call past its deadline sends nothing"),
        );
        assert_eq!(past, Err(MountedSifsHandoffEnded::Left));
    }

    #[test]
    fn a_handoff_event_ends_a_resend_pause_long_before_its_timer() {
        // The per-attempt bound is the largest interval: a minute.
        let mount = test_mount("handoff-pause-event", Duration::from_secs(60));
        assert_eq!(mount.handoff.resend_delay(20), Duration::from_secs(60));
        for close in [false, true] {
            let seen = mount.handoff.wake().events();
            let started = Instant::now();
            let resumed = std::thread::scope(|scope| {
                let pause = scope.spawn(|| mount.handoff.pause_before_resend(seen, 20, None));
                std::thread::sleep(Duration::from_millis(50));
                if close {
                    mount.handoff.close();
                } else {
                    mount.handoff.wake().notify();
                }
                pause.join().unwrap()
            });
            assert_eq!(resumed, !close, "close ends the waiting for good");
            assert!(
                started.elapsed() < Duration::from_secs(20),
                "the pause ended on the event, not on its minute-long timer"
            );
        }
    }

    #[test]
    fn an_event_during_an_attempt_ends_the_pause_that_follows_it() {
        let mount = test_mount("handoff-pause-missed-event", Duration::from_secs(60));
        let seen = mount.handoff.wake().events();
        mount.handoff.wake().notify();
        let started = Instant::now();
        assert!(mount.handoff.pause_before_resend(seen, 20, None));
        assert!(started.elapsed() < Duration::from_secs(20));
        // A deadline still bounds a pause no event ends.
        let seen = mount.handoff.wake().events();
        assert!(!mount.handoff.pause_before_resend(
            seen,
            20,
            Some(Instant::now() + Duration::from_millis(30))
        ));
    }

    #[test]
    fn a_full_outbox_is_no_room_and_a_closed_session_is_a_refusal() {
        let mut mount = test_mount("handoff-prepare-typing", Duration::from_millis(20));
        let mut fillers = Vec::new();
        for _ in 0..MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS {
            fillers.push(
                mount
                    .handoff
                    .prepare("test.filler", None)
                    .unwrap()
                    .into_owned(),
            );
        }
        assert!(matches!(
            mount.handoff.prepare("test.overflow", None),
            Err(MountedSifsHandoffStepError::Unacknowledged(_))
        ));
        // Releasing a marker makes room and is a handoff event.
        let events = mount.handoff.wake().events();
        let mut released = fillers.pop().unwrap();
        let arm_request_id = MountedSifsMutationArmRequestId::generate();
        mount.handoff.arm(&released, &arm_request_id).unwrap();
        mount
            .handoff
            .persist_proven_no_write(&mut released, &arm_request_id, "test-filler-released")
            .unwrap();
        mount
            .handoff
            .commit_proven_no_write(&released, &arm_request_id, "test-filler-released")
            .unwrap();
        mount.handoff.release_after_daemon_ack(released).unwrap();
        assert_ne!(mount.handoff.wake().events(), events);
        let lease = mount.handoff.prepare("test.after-release", None).unwrap();
        drop(lease);
        drop(fillers);

        mount.handoff.close_clean().unwrap();
        assert!(matches!(
            mount.handoff.prepare("test.closed", None),
            Err(MountedSifsHandoffStepError::Rejected(_))
        ));
    }

    #[test]
    fn the_resender_sends_a_step_past_its_call_until_the_daemon_acknowledges() {
        let mount = test_mount("handoff-resender", Duration::from_millis(20));
        mount.daemon.set_silent(true);
        let lease = mount.handoff.prepare("test.mkdir", None).unwrap();
        let arm_request_id = MountedSifsMutationArmRequestId::generate();
        // The call's deadline has passed: it sends nothing itself.
        assert_eq!(
            mount
                .handoff
                .until_acknowledged(Some(Instant::now()), || mount
                    .handoff
                    .arm(&lease, &arm_request_id)),
            Err(MountedSifsHandoffEnded::Left)
        );
        assert_eq!(mount.daemon.attempts_of(TestDaemon::ARM), 0);
        mount.handoff.finish_later(
            lease,
            MountedSifsUnfinishedHandoff::ArmedWithoutEntry { arm_request_id },
        );
        // Far more attempts than any cap would allow, for as long as the
        // daemon stays silent.
        eventually("Arm re-sent by the re-sender", || {
            mount.daemon.attempts_of(TestDaemon::ARM) >= 12
        });
        assert_eq!(mount.handoff.unfinished_count(), 1);
        assert_eq!(mount.store.status().unwrap().live_marker_count, 1);

        mount.daemon.set_silent(false);
        eventually("the step was acknowledged and its marker released", || {
            mount.handoff.unfinished_count() == 0
                && mount.store.status().unwrap().pending_marker_count == 0
        });
        assert_eq!(mount.daemon.attempts_of(TestDaemon::PROVE_NO_WRITE), 1);
    }

    #[test]
    fn the_resender_gives_up_a_refused_step_and_stops_at_close() {
        let mount = test_mount("handoff-resender-refused", Duration::from_millis(20));
        mount.daemon.refuse(TestDaemon::ARM);
        let lease = mount.handoff.prepare("test.refused", None).unwrap();
        mount.handoff.finish_later(
            lease,
            MountedSifsUnfinishedHandoff::ArmedWithoutEntry {
                arm_request_id: MountedSifsMutationArmRequestId::generate(),
            },
        );
        eventually("the refused step left the re-sender", || {
            mount.handoff.unfinished_count() == 0
        });
        assert_eq!(mount.daemon.attempts_of(TestDaemon::ARM), 1);

        mount.daemon.refuse_nothing();
        mount.daemon.set_silent(true);
        let lease = mount.handoff.prepare("test.closing", None).unwrap();
        mount.handoff.finish_later(
            lease,
            MountedSifsUnfinishedHandoff::ArmedWithoutEntry {
                arm_request_id: MountedSifsMutationArmRequestId::generate(),
            },
        );
        eventually("the re-sender is sending", || {
            mount.daemon.attempts_of(TestDaemon::ARM) >= 3
        });
        mount.handoff.close();
        assert_eq!(mount.handoff.unfinished_count(), 0);
        let after_close = mount.daemon.attempts_of(TestDaemon::ARM);
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            mount.daemon.attempts_of(TestDaemon::ARM) <= after_close + 1,
            "the re-sender stopped with the mount"
        );
    }
}
