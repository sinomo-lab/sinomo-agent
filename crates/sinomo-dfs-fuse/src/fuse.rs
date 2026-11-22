use crate::{
    DfsFuseError, DfsFuseLinuxPreflight, DfsFuseMountHandle, DfsFuseMountOptions, Result,
    join_sifs_child, normalize_sifs_root, parent_sifs_path, prepare_safe_mount_point,
    validate_safe_mount_point_path,
};
use fuser::{
    AccessFlags, BsdFileFlags, Config, Errno, FileAttr, FileHandle, FileType, Filesystem,
    FopenFlags, Generation, INodeNo, InitFlags, KernelConfig, LockOwner, MountOption, OpenFlags,
    RenameFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyLock, ReplyOpen, ReplyStatfs, ReplyWrite, Request, SessionACL, TimeOrNow, WriteFlags,
};
use sinomo_runtime::dfs::{
    self, DfsError, DfsObjectKind, DfsPosixCreateAttrs, DfsPosixError, DfsWriteOptions,
};
use sinomo_runtime::local::SinomoDataDir;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::io;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const ROOT_INO: u64 = 1;
const DEFAULT_BLOCK_SIZE: u32 = 4096;
const LOCK_RENEW_INTERVAL: Duration = Duration::from_secs(5);
/// A blocking lock waiter re-sends its queued acquire on this interval. The
/// shard primary announces no grant to a waiting requester, so the timer is
/// what asks again; it never ends the wait.
const BLOCKING_LOCK_RESEND_INTERVAL: Duration = Duration::from_millis(500);
const LOCKED_IO_MIN_LEASE: Duration = Duration::from_secs(60);
const MAX_ACTIVE_LOCK_KEYS_PER_MOUNT: usize = 2;
const FUSE_CREATE_RACE_RETRY_ROUNDS: usize = 4;
const MIN_FUSE_READ_WORKERS: usize = 4;
const MAX_FUSE_READ_WORKERS: usize = 16;
const FUSE_READ_QUEUE_DEPTH_PER_WORKER: usize = 4;
// Bound acknowledged-but-not-yet-published data per inode. Ordinary
// Linux FUSE writes arrive as much smaller kernel chunks, so this keeps a
// sequential transfer to one canonical DFS publication without allowing an
// idle inode to retain unbounded mount memory.
const MAX_DIRTY_WRITE_BYTES_PER_INODE: usize = 64 * 1024 * 1024;
static NEXT_MOUNT_SESSION_ID: AtomicU64 = AtomicU64::new(1);
type SifsService = dfs::MountedSifsService;
/// A mount's lock: its waiters wait for the holder's release, within the
/// syscall's deadline when the mount has an operation budget.
type MountLock<T> = dfs::MountedSifsLock<T>;
type MountLockGuard<'a, T> = dfs::MountedSifsLockGuard<'a, T>;
type FuseReadJob = Box<dyn FnOnce() + Send + 'static>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FuseMountedMutationOperation {
    MountOpen,
    BufferedWritePublication,
    ImmediateWritePublication,
    FileSync,
    TruncateObject,
    TruncatePath,
    SetMode,
    SetOwner,
    SetSymlinkOwner,
    SetTimes,
    Mknod,
    Mkdir,
    Unlink,
    Rmdir,
    Symlink,
    Rename,
    Hardlink,
    Create,
    DirectorySync,
    DirectoryRefresh,
    AdvisoryLockAcquire,
    AdvisoryLockQuery,
    AdvisoryLockRenew,
    AdvisoryLockConvert,
    AdvisoryLockUnlock,
    AdvisoryLockCancel,
    AdvisoryLockClose,
}

impl FuseMountedMutationOperation {
    #[cfg(test)]
    const ALL: [Self; 27] = [
        Self::MountOpen,
        Self::BufferedWritePublication,
        Self::ImmediateWritePublication,
        Self::FileSync,
        Self::TruncateObject,
        Self::TruncatePath,
        Self::SetMode,
        Self::SetOwner,
        Self::SetSymlinkOwner,
        Self::SetTimes,
        Self::Mknod,
        Self::Mkdir,
        Self::Unlink,
        Self::Rmdir,
        Self::Symlink,
        Self::Rename,
        Self::Hardlink,
        Self::Create,
        Self::DirectorySync,
        Self::DirectoryRefresh,
        Self::AdvisoryLockAcquire,
        Self::AdvisoryLockQuery,
        Self::AdvisoryLockRenew,
        Self::AdvisoryLockConvert,
        Self::AdvisoryLockUnlock,
        Self::AdvisoryLockCancel,
        Self::AdvisoryLockClose,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::MountOpen => "fuse.mount-open",
            Self::BufferedWritePublication => "fuse.buffered-write-publication",
            Self::ImmediateWritePublication => "fuse.immediate-write-publication",
            Self::FileSync => "fuse.file-sync",
            Self::TruncateObject => "fuse.truncate-object",
            Self::TruncatePath => "fuse.truncate-path",
            Self::SetMode => "fuse.set-mode",
            Self::SetOwner => "fuse.set-owner",
            Self::SetSymlinkOwner => "fuse.set-symlink-owner",
            Self::SetTimes => "fuse.set-times",
            Self::Mknod => "fuse.mknod",
            Self::Mkdir => "fuse.mkdir",
            Self::Unlink => "fuse.unlink",
            Self::Rmdir => "fuse.rmdir",
            Self::Symlink => "fuse.symlink",
            Self::Rename => "fuse.rename",
            Self::Hardlink => "fuse.hardlink",
            Self::Create => "fuse.create",
            Self::DirectorySync => "fuse.directory-sync",
            Self::DirectoryRefresh => "fuse.directory-refresh",
            Self::AdvisoryLockAcquire => "fuse.advisory-lock-acquire",
            Self::AdvisoryLockQuery => "fuse.advisory-lock-query",
            Self::AdvisoryLockRenew => "fuse.advisory-lock-renew",
            Self::AdvisoryLockConvert => "fuse.advisory-lock-convert",
            Self::AdvisoryLockUnlock => "fuse.advisory-lock-unlock",
            Self::AdvisoryLockCancel => "fuse.advisory-lock-cancel",
            Self::AdvisoryLockClose => "fuse.advisory-lock-close",
        }
    }
}

#[derive(Debug)]
struct FuseMountedMutationError {
    errno: libc::c_int,
    error_code: &'static str,
}

#[derive(Clone, Copy, Debug)]
struct FuseMountedMutationPreflightError {
    errno: libc::c_int,
    proof_code: &'static str,
}

impl FuseMountedMutationError {
    fn ambiguous(errno: libc::c_int, error_code: &'static str) -> Self {
        Self { errno, error_code }
    }
}

impl From<DfsPosixError> for FuseMountedMutationError {
    fn from(error: DfsPosixError) -> Self {
        let error_code = match &error {
            DfsPosixError::NotFound { .. } => "dfs-posix-not-found",
            DfsPosixError::AlreadyExists { .. } => "dfs-posix-already-exists",
            DfsPosixError::NotDirectory { .. } => "dfs-posix-not-directory",
            DfsPosixError::IsDirectory { .. } => "dfs-posix-is-directory",
            DfsPosixError::NotSymlink { .. } => "dfs-posix-not-symlink",
            DfsPosixError::MissingParent { .. } => "dfs-posix-missing-parent",
            DfsPosixError::InvalidPath { .. } => "dfs-posix-invalid-path",
            DfsPosixError::SymlinkContentPending { .. } => "dfs-posix-symlink-content-pending",
            DfsPosixError::Conflict { .. } => "dfs-posix-conflict",
            DfsPosixError::DirectoryNotEmpty { .. } => "dfs-posix-directory-not-empty",
            DfsPosixError::PendingWrite { .. } => "dfs-posix-pending-write",
            DfsPosixError::PartialBulkCommit { .. } => "dfs-posix-partial-bulk-commit",
            DfsPosixError::Dfs(DfsError::MetadataCommitOutcomeUnknown { .. }) => {
                "dfs-posix-commit-outcome-unknown"
            }
            DfsPosixError::Dfs(_) => "dfs-posix-dfs-error",
            DfsPosixError::Metadata(_) => "dfs-posix-metadata-error",
        };
        let errno = errno_for_posix_error(error);
        Self { errno, error_code }
    }
}

/// Why one attempt at a handoff step produced no acknowledgement: a definite
/// refusal, or no answer yet.
type FuseHandoffStepError = dfs::MountedSifsHandoffStepError;
/// How sending a handoff step until its acknowledgement ended without one.
type FuseHandoffEnded = dfs::MountedSifsHandoffEnded;
/// What is left of a mounted-mutation handoff whose syscall returned before
/// the daemon acknowledged it.
type FuseUnfinishedHandoff = dfs::MountedSifsUnfinishedHandoff;

trait FuseMountedMutationCoordinator {
    type Admission<'a>
    where
        Self: 'a;
    type Lease<'a>
    where
        Self: 'a;

    fn admit(&self) -> std::result::Result<Self::Admission<'_>, ()>;
    fn is_gated(&self) -> bool;
    fn gate_before_mount(&self);
    fn retire_gated_session(&self) -> std::result::Result<(), ()>;
    /// One attempt at a Prepared marker. A full outbox is not a refusal: the
    /// attempt is made again once a marker is released.
    fn prepare(
        &self,
        operation: FuseMountedMutationOperation,
        stable_operation_id: Option<String>,
    ) -> std::result::Result<Self::Lease<'_>, FuseHandoffStepError>;
    fn arm(
        &self,
        lease: &mut Self::Lease<'_>,
    ) -> std::result::Result<dfs::MountedSifsMutationArmRequestId, FuseHandoffStepError>;
    /// Send Arm under `arm_request_id`. Sending it again under the same id
    /// is exact; the daemon acknowledges the one transition.
    fn arm_as(
        &self,
        lease: &mut Self::Lease<'_>,
        arm_request_id: &dfs::MountedSifsMutationArmRequestId,
    ) -> std::result::Result<(), FuseHandoffStepError> {
        let _ = arm_request_id;
        self.arm(lease).map(drop)
    }
    /// Abort a marker whose Arm was definitely refused: its operation never
    /// ran.
    fn abort_unstarted(
        &self,
        lease: &Self::Lease<'_>,
    ) -> std::result::Result<(), FuseHandoffStepError> {
        let _ = lease;
        Ok(())
    }
    /// The count of this mount's handoff events, read before an attempt so
    /// an event during it ends the following pause.
    fn handoff_events(&self) -> u64 {
        0
    }
    /// Wait until a handoff step the daemon did not acknowledge is due to be
    /// sent again: one re-send interval, ended sooner by a handoff event after
    /// `seen_events`. `false` ends the waiting: the syscall's deadline passed
    /// or the mount is closing. The interval only re-sends; nothing counts
    /// attempts.
    fn pause_before_resend(
        &self,
        seen_events: u64,
        attempt: u32,
        deadline: Option<Instant>,
    ) -> bool {
        let _ = (seen_events, attempt, deadline);
        false
    }
    /// Keep sending a step the daemon has not acknowledged after its syscall
    /// returned, until the daemon acknowledges it or the mount closes.
    fn finish_later(&self, lease: Self::Lease<'_>, step: FuseUnfinishedHandoff) {
        drop((lease, step));
    }
    /// Give up a marker whose step the daemon definitely refused. The lease
    /// ends here; the daemon's recovery owns the marker.
    fn release_refused(&self, lease: Self::Lease<'_>, detail: &str) {
        let _ = detail;
        drop(lease);
    }
    fn persist_captured(
        &self,
        lease: &mut Self::Lease<'_>,
        arm_request_id: &dfs::MountedSifsMutationArmRequestId,
        captured_work: &dfs::MountedSifsMutationCapturedWork,
    ) -> std::result::Result<(), FuseHandoffStepError>;
    fn persist_unknown(
        &self,
        lease: &mut Self::Lease<'_>,
        arm_request_id: &dfs::MountedSifsMutationArmRequestId,
        observation: &dfs::MountedSifsMutationErrorObservation,
    ) -> std::result::Result<(), FuseHandoffStepError>;
    fn persist_proven_no_write(
        &self,
        lease: &mut Self::Lease<'_>,
        arm_request_id: &dfs::MountedSifsMutationArmRequestId,
        proof_code: &'static str,
    ) -> std::result::Result<(), FuseHandoffStepError>;
    fn commit_captured(
        &self,
        lease: &Self::Lease<'_>,
        arm_request_id: &dfs::MountedSifsMutationArmRequestId,
        captured_work: &dfs::MountedSifsMutationCapturedWork,
    ) -> std::result::Result<(), FuseHandoffStepError>;
    fn commit_unknown(
        &self,
        lease: &Self::Lease<'_>,
        arm_request_id: &dfs::MountedSifsMutationArmRequestId,
        observation: &dfs::MountedSifsMutationErrorObservation,
    ) -> std::result::Result<(), FuseHandoffStepError>;
    fn commit_proven_no_write(
        &self,
        lease: &Self::Lease<'_>,
        arm_request_id: &dfs::MountedSifsMutationArmRequestId,
        proof_code: &'static str,
    ) -> std::result::Result<(), FuseHandoffStepError>;
    fn release_after_daemon_ack(&self, lease: Self::Lease<'_>) -> std::result::Result<(), ()>;
}

/// The FUSE mount's end of the mounted-mutation handoff. The re-send of
/// unacknowledged steps, the outbox session and the re-sender that outlives a
/// syscall are the shared [`dfs::MountedSifsMutationHandoff`]; this adds the
/// mount's mutation lane and its mount-open gate.
#[derive(Debug)]
struct FuseMountedMutationHandoff {
    handoff: dfs::MountedSifsMutationHandoff,
    gated: AtomicBool,
    mutation_lane: MountLock<()>,
}

impl FuseMountedMutationHandoff {
    fn new(handoff: dfs::MountedSifsMutationHandoff) -> Self {
        Self {
            handoff,
            gated: AtomicBool::new(false),
            mutation_lane: MountLock::new(()),
        }
    }

    fn close_clean(&mut self) {
        if self.handoff.close_clean().is_err() {
            self.gated.store(true, Ordering::Release);
        }
    }

    fn retire_gated_session(&self) -> std::result::Result<(), ()> {
        let Some(session) = self.handoff.session() else {
            return Ok(());
        };
        session.close_clean_shared().map_err(|_| ())
    }
}

impl Drop for FuseMountedMutationHandoff {
    fn drop(&mut self) {
        self.close_clean();
    }
}

impl FuseMountedMutationCoordinator for FuseMountedMutationHandoff {
    type Admission<'a> = MountLockGuard<'a, ()>;
    type Lease<'a> = dfs::MountedSifsMutationLease<'a>;

    fn admit(&self) -> std::result::Result<Self::Admission<'_>, ()> {
        dfs::MountedSifsCall::lock(&self.mutation_lane).map_err(|_| ())
    }

    fn is_gated(&self) -> bool {
        self.gated.load(Ordering::Acquire)
    }

    fn gate_before_mount(&self) {
        self.gated.store(true, Ordering::Release);
    }

    fn retire_gated_session(&self) -> std::result::Result<(), ()> {
        FuseMountedMutationHandoff::retire_gated_session(self)
    }

    fn prepare(
        &self,
        operation: FuseMountedMutationOperation,
        stable_operation_id: Option<String>,
    ) -> std::result::Result<Self::Lease<'_>, FuseHandoffStepError> {
        self.handoff.prepare(operation.label(), stable_operation_id)
    }

    fn arm(
        &self,
        lease: &mut Self::Lease<'_>,
    ) -> std::result::Result<dfs::MountedSifsMutationArmRequestId, FuseHandoffStepError> {
        let arm_request_id = dfs::MountedSifsMutationArmRequestId::generate();
        self.arm_as(lease, &arm_request_id)?;
        Ok(arm_request_id)
    }

    fn arm_as(
        &self,
        lease: &mut Self::Lease<'_>,
        arm_request_id: &dfs::MountedSifsMutationArmRequestId,
    ) -> std::result::Result<(), FuseHandoffStepError> {
        self.handoff.arm(lease, arm_request_id)
    }

    fn abort_unstarted(
        &self,
        lease: &Self::Lease<'_>,
    ) -> std::result::Result<(), FuseHandoffStepError> {
        self.handoff.abort_unstarted(lease)
    }

    fn handoff_events(&self) -> u64 {
        self.handoff.wake().events()
    }

    fn pause_before_resend(
        &self,
        seen_events: u64,
        attempt: u32,
        deadline: Option<Instant>,
    ) -> bool {
        self.handoff
            .pause_before_resend(seen_events, attempt, deadline)
    }

    fn finish_later(&self, lease: Self::Lease<'_>, step: FuseUnfinishedHandoff) {
        self.handoff.finish_later(lease, step);
    }

    fn release_refused(&self, lease: Self::Lease<'_>, detail: &str) {
        self.handoff.release_refused(lease, detail);
    }

    fn persist_captured(
        &self,
        lease: &mut Self::Lease<'_>,
        arm_request_id: &dfs::MountedSifsMutationArmRequestId,
        captured_work: &dfs::MountedSifsMutationCapturedWork,
    ) -> std::result::Result<(), FuseHandoffStepError> {
        self.handoff
            .persist_captured(lease, arm_request_id, captured_work)
    }

    fn persist_unknown(
        &self,
        lease: &mut Self::Lease<'_>,
        arm_request_id: &dfs::MountedSifsMutationArmRequestId,
        observation: &dfs::MountedSifsMutationErrorObservation,
    ) -> std::result::Result<(), FuseHandoffStepError> {
        let persisted = self
            .handoff
            .persist_unknown(lease, arm_request_id, observation);
        // The existing durability attempt precedes diagnostics. Keep its result
        // even when stderr is unavailable, and expose no command or nonce data.
        use std::io::Write as _;
        let marker = lease.marker();
        let _ = writeln!(
            io::stderr().lock(),
            "sinomo-dfs-fuse: mounted-mutation-unknown phase=unknown-persist-attempted marker_id={} mount_session_id={} operation={:?} arm_request_id={} error_code={:?} persistence_succeeded={}",
            marker.marker_id,
            marker.origin.mount_session_id,
            marker.origin.operation,
            arm_request_id,
            observation.error_code,
            persisted.is_ok(),
        );
        persisted
    }

    fn persist_proven_no_write(
        &self,
        lease: &mut Self::Lease<'_>,
        arm_request_id: &dfs::MountedSifsMutationArmRequestId,
        proof_code: &'static str,
    ) -> std::result::Result<(), FuseHandoffStepError> {
        self.handoff
            .persist_proven_no_write(lease, arm_request_id, proof_code)
    }

    fn commit_captured(
        &self,
        lease: &Self::Lease<'_>,
        arm_request_id: &dfs::MountedSifsMutationArmRequestId,
        captured_work: &dfs::MountedSifsMutationCapturedWork,
    ) -> std::result::Result<(), FuseHandoffStepError> {
        self.handoff
            .commit_captured(lease, arm_request_id, captured_work)
    }

    fn commit_unknown(
        &self,
        lease: &Self::Lease<'_>,
        arm_request_id: &dfs::MountedSifsMutationArmRequestId,
        observation: &dfs::MountedSifsMutationErrorObservation,
    ) -> std::result::Result<(), FuseHandoffStepError> {
        self.handoff
            .commit_unknown(lease, arm_request_id, observation)
    }

    fn commit_proven_no_write(
        &self,
        lease: &Self::Lease<'_>,
        arm_request_id: &dfs::MountedSifsMutationArmRequestId,
        proof_code: &'static str,
    ) -> std::result::Result<(), FuseHandoffStepError> {
        self.handoff
            .commit_proven_no_write(lease, arm_request_id, proof_code)
    }

    fn release_after_daemon_ack(&self, lease: Self::Lease<'_>) -> std::result::Result<(), ()> {
        self.handoff.release_after_daemon_ack(lease).map_err(|_| ())
    }
}

fn fuse_mounted_mutation_handoff_required(clustered: bool, _read_only: bool) -> bool {
    // A distributed directory read can synchronously ingest a primary's
    // journal into this node's durable replica. Read-only mounts therefore
    // need the same crash-safe handoff even though user-visible mutations are
    // rejected before service entry.
    clustered
}

fn prepare_fuse_clustered_mount_handoff_with<C, H, E>(
    client: C,
    probe: impl FnOnce(&C) -> std::result::Result<(), E>,
    open_host_session: impl FnOnce(C) -> std::result::Result<H, E>,
) -> std::result::Result<H, E> {
    // Probe is the daemon readiness boundary. Publishing the host session
    // before that ACK could leave a mount-owned lease behind for a daemon that
    // never proved it can accept recovery work.
    probe(&client)?;
    open_host_session(client)
}

/// Send one handoff step until the daemon acknowledges it. A step that got no
/// answer is sent again after an interval that doubles to the handoff
/// client's per-attempt bound; a handoff event on the mount ends the interval
/// sooner, and nothing counts attempts. It ends without an acknowledgement
/// only when the daemon definitely refused the step, or when the syscall's
/// deadline passed or the mount is closing first.
fn until_acknowledged<C: FuseMountedMutationCoordinator>(
    coordinator: &C,
    deadline: Option<Instant>,
    step: impl FnMut() -> std::result::Result<(), FuseHandoffStepError>,
) -> std::result::Result<(), FuseHandoffEnded> {
    dfs::mounted_sifs_handoff_until_acknowledged(
        || coordinator.handoff_events(),
        |seen_events, attempt| coordinator.pause_before_resend(seen_events, attempt, deadline),
        deadline,
        step,
    )
}

/// Close an armed operation that never wrote. A daemon that has not
/// acknowledged by the syscall's deadline is sent the proof again after the
/// syscall returns; the operation's own answer does not depend on it. A
/// refused proof stays in the outbox as the adapter recorded it.
fn finish_proven_no_write<C: FuseMountedMutationCoordinator>(
    coordinator: &C,
    mut lease: C::Lease<'_>,
    arm_request_id: &dfs::MountedSifsMutationArmRequestId,
    proof_code: &'static str,
    deadline: Option<Instant>,
) -> std::result::Result<(), libc::c_int> {
    let mut persisted = false;
    match until_acknowledged(coordinator, deadline, || {
        if !persisted {
            coordinator.persist_proven_no_write(&mut lease, arm_request_id, proof_code)?;
            persisted = true;
        }
        coordinator.commit_proven_no_write(&lease, arm_request_id, proof_code)
    }) {
        Ok(()) => coordinator
            .release_after_daemon_ack(lease)
            .map_err(|()| libc::EIO),
        Err(FuseHandoffEnded::Left) => {
            coordinator.finish_later(
                lease,
                FuseUnfinishedHandoff::NoWrite {
                    arm_request_id: arm_request_id.clone(),
                    proof_code: proof_code.to_string(),
                },
            );
            Err(libc::EIO)
        }
        Err(FuseHandoffEnded::Rejected(detail)) => {
            coordinator.release_refused(lease, &detail);
            Err(libc::EIO)
        }
    }
}

/// Close a marker whose Arm the daemon definitely refused. Its operation
/// never ran, so the marker is aborted as unstarted; the abort is sent until
/// the daemon answers, and after the syscall's deadline by the re-sender.
fn release_unstarted<C: FuseMountedMutationCoordinator>(
    coordinator: &C,
    lease: C::Lease<'_>,
    deadline: Option<Instant>,
) {
    match until_acknowledged(coordinator, deadline, || {
        coordinator.abort_unstarted(&lease)
    }) {
        Ok(()) => {
            let _ = coordinator.release_after_daemon_ack(lease);
        }
        Err(FuseHandoffEnded::Left) => {
            coordinator.finish_later(lease, FuseUnfinishedHandoff::Unstarted);
        }
        Err(FuseHandoffEnded::Rejected(detail)) => coordinator.release_refused(lease, &detail),
    }
}

#[derive(Debug)]
enum FuseMountOpenHandoffError<E> {
    Service(E),
    Handoff {
        detail: &'static str,
        service_error: Option<E>,
    },
}

fn fuse_mount_open_committed_work() -> dfs::DfsCommittedWork {
    // Metadata-backend construction can finish durable recovery before a
    // service-local tracker exists. Mount activation therefore transfers every
    // coalesced ownership edge, even when the opened service did not observe a
    // new edge itself. These bits are monotonic wakeups, so the conservative
    // handoff is safe and keeps constructor recovery daemon-owned.
    dfs::DfsCommittedWork {
        publish_metadata: true,
        repair_maintenance: true,
        namespace_maintenance: true,
        metadata_replica_progress: true,
        metadata_materialization: true,
        semantic_changes: true,
    }
}

fn finish_fuse_mount_open_unknown<C: FuseMountedMutationCoordinator>(
    coordinator: &C,
    mut lease: C::Lease<'_>,
    arm_request_id: &dfs::MountedSifsMutationArmRequestId,
    error_code: &'static str,
) -> std::result::Result<(), ()> {
    // Full daemon recovery is the evidence-preserving fallback.
    let captured_work = dfs::MountedSifsMutationCapturedWork::new(fuse_mount_open_committed_work());
    let observation =
        dfs::MountedSifsMutationErrorObservation::new(captured_work, error_code).map_err(|_| ())?;
    coordinator
        .persist_unknown(&mut lease, arm_request_id, &observation)
        .and_then(|_| coordinator.commit_unknown(&lease, arm_request_id, &observation))
        .map_err(|_| ())
        .and_then(|_| coordinator.release_after_daemon_ack(lease))
}

fn run_fuse_clustered_mount_open_with<C, R, E>(
    coordinator: &C,
    open_service: impl FnOnce() -> std::result::Result<R, E>,
) -> std::result::Result<R, FuseMountOpenHandoffError<E>>
where
    C: FuseMountedMutationCoordinator,
{
    // Keep the same lane for mount activation and every later callback. No
    // service/backend open may cross Prepare+Arm, and no callback can become
    // visible until the daemon has durably accepted this first operation.
    let _admission = match coordinator.admit() {
        Ok(admission) => admission,
        Err(()) => {
            coordinator.gate_before_mount();
            let _ = coordinator.retire_gated_session();
            return Err(FuseMountOpenHandoffError::Handoff {
                detail: "mounted DFS mount-open admission failed",
                service_error: None,
            });
        }
    };
    if coordinator.is_gated() {
        coordinator.gate_before_mount();
        let _ = coordinator.retire_gated_session();
        return Err(FuseMountOpenHandoffError::Handoff {
            detail: "mounted DFS mount-open handoff was already gated",
            service_error: None,
        });
    }
    let mut lease = match coordinator.prepare(FuseMountedMutationOperation::MountOpen, None) {
        Ok(lease) => lease,
        Err(_) => {
            coordinator.gate_before_mount();
            let _ = coordinator.retire_gated_session();
            return Err(FuseMountOpenHandoffError::Handoff {
                detail: "mounted DFS mount-open Prepare failed",
                service_error: None,
            });
        }
    };
    let arm_request_id = match coordinator.arm(&mut lease) {
        Ok(arm_request_id) => arm_request_id,
        Err(_) => {
            coordinator.gate_before_mount();
            let _ = coordinator.retire_gated_session();
            return Err(FuseMountOpenHandoffError::Handoff {
                detail: "mounted DFS mount-open Arm failed",
                service_error: None,
            });
        }
    };

    let opened = match open_service() {
        Ok(opened) => opened,
        Err(service_error) => {
            coordinator.gate_before_mount();
            let accepted = finish_fuse_mount_open_unknown(
                coordinator,
                lease,
                &arm_request_id,
                "fuse-mount-open-service-error",
            );
            let retired = coordinator.retire_gated_session();
            if accepted.is_err() || retired.is_err() {
                return Err(FuseMountOpenHandoffError::Handoff {
                    detail: "mounted DFS mount-open error recovery handoff failed",
                    service_error: Some(service_error),
                });
            }
            return Err(FuseMountOpenHandoffError::Service(service_error));
        }
    };

    let captured_work = dfs::MountedSifsMutationCapturedWork::new(fuse_mount_open_committed_work());

    if coordinator
        .persist_captured(&mut lease, &arm_request_id, &captured_work)
        .and_then(|_| coordinator.commit_captured(&lease, &arm_request_id, &captured_work))
        .map_err(|_| ())
        .and_then(|_| coordinator.release_after_daemon_ack(lease))
        .is_err()
    {
        coordinator.gate_before_mount();
        let _ = coordinator.retire_gated_session();
        return Err(FuseMountOpenHandoffError::Handoff {
            detail: "mounted DFS mount-open committed-work handoff failed",
            service_error: None,
        });
    }
    Ok(opened)
}

fn run_fuse_mounted_mutation_with<C, S, R>(
    coordinator: &C,
    service: &MountLock<S>,
    operation: FuseMountedMutationOperation,
    mutation: impl FnOnce(
        &mut S,
    ) -> dfs::MountedSifsMutationReport<
        std::result::Result<R, FuseMountedMutationError>,
    >,
) -> std::result::Result<R, libc::c_int>
where
    C: FuseMountedMutationCoordinator,
{
    run_fuse_mounted_mutation_until(coordinator, service, operation, None, mutation)
}

/// [`run_fuse_mounted_mutation_with`] under the syscall's deadline, which
/// bounds its waits for the daemon as well as its DFS work.
fn run_fuse_mounted_mutation_until<C, S, R>(
    coordinator: &C,
    service: &MountLock<S>,
    operation: FuseMountedMutationOperation,
    deadline: Option<Instant>,
    mutation: impl FnOnce(
        &mut S,
    ) -> dfs::MountedSifsMutationReport<
        std::result::Result<R, FuseMountedMutationError>,
    >,
) -> std::result::Result<R, libc::c_int>
where
    C: FuseMountedMutationCoordinator,
{
    let _call = dfs::MountedSifsCall::enter(deadline);
    run_fuse_mounted_mutation_with_lock_and_preflight(
        coordinator,
        service,
        operation,
        None,
        deadline,
        |service| dfs::MountedSifsCall::lock(service).map_err(|_| libc::EIO),
        |_| Ok(()),
        mutation,
    )
}

fn run_fuse_mounted_mutation_with_lock<'service, C, S, R>(
    coordinator: &C,
    service: &'service MountLock<S>,
    operation: FuseMountedMutationOperation,
    stable_operation_id: Option<String>,
    lock_service: impl FnOnce(
        &'service MountLock<S>,
    ) -> std::result::Result<MountLockGuard<'service, S>, libc::c_int>,
    mutation: impl FnOnce(
        &mut S,
    ) -> dfs::MountedSifsMutationReport<
        std::result::Result<R, FuseMountedMutationError>,
    >,
) -> std::result::Result<R, libc::c_int>
where
    C: FuseMountedMutationCoordinator,
{
    run_fuse_mounted_mutation_with_lock_and_preflight(
        coordinator,
        service,
        operation,
        stable_operation_id,
        None,
        lock_service,
        |_| Ok(()),
        mutation,
    )
}

#[allow(clippy::too_many_arguments)]
fn run_fuse_mounted_mutation_with_lock_and_preflight<'service, C, S, R>(
    coordinator: &C,
    service: &'service MountLock<S>,
    operation: FuseMountedMutationOperation,
    stable_operation_id: Option<String>,
    deadline: Option<Instant>,
    lock_service: impl FnOnce(
        &'service MountLock<S>,
    ) -> std::result::Result<MountLockGuard<'service, S>, libc::c_int>,
    preflight: impl FnOnce(&mut S) -> std::result::Result<(), FuseMountedMutationPreflightError>,
    mutation: impl FnOnce(
        &mut S,
    ) -> dfs::MountedSifsMutationReport<
        std::result::Result<R, FuseMountedMutationError>,
    >,
) -> std::result::Result<R, libc::c_int>
where
    C: FuseMountedMutationCoordinator,
{
    // Admission remains held while the service lane is deliberately dropped
    // for the daemon RPC, so mutations hand their work to the daemon in the
    // order they ran. Nothing here retires the mount: a step the daemon has
    // not answered is sent again, a step it refused ends this call and
    // releases the marker, and the outbox's marker limit holds the next
    // mutation back until there is room.
    let _admission = coordinator.admit().map_err(|()| libc::EIO)?;
    if coordinator.is_gated() {
        return Err(libc::EIO);
    }
    // A full outbox, or one still being classified, is not a refusal: wait
    // for a marker's release instead.
    let mut prepared = None;
    let mut stable_operation_id = Some(stable_operation_id);
    if until_acknowledged(coordinator, deadline, || {
        let id = stable_operation_id.take().unwrap_or_default();
        match coordinator.prepare(operation, id.clone()) {
            Ok(lease) => {
                prepared = Some(lease);
                Ok(())
            }
            Err(error) => {
                stable_operation_id = Some(id);
                Err(error)
            }
        }
    })
    .is_err()
    {
        // Nothing is held yet: no marker was prepared.
        return Err(libc::EIO);
    }
    let mut lease = prepared.expect("an acknowledged Prepare returned its lease");
    let arm_request_id = dfs::MountedSifsMutationArmRequestId::generate();
    match until_acknowledged(coordinator, deadline, || {
        coordinator.arm_as(&mut lease, &arm_request_id)
    }) {
        Ok(()) => {}
        Err(FuseHandoffEnded::Left) => {
            // The daemon may hold the Arm. The operation never enters the
            // service; the marker is closed once the daemon answers.
            coordinator.finish_later(
                lease,
                FuseUnfinishedHandoff::ArmedWithoutEntry { arm_request_id },
            );
            return Err(libc::EIO);
        }
        Err(FuseHandoffEnded::Rejected(_)) => {
            // The daemon refused the Arm, so the operation never runs.
            release_unstarted(coordinator, lease, deadline);
            return Err(libc::EIO);
        }
    }
    let mut service = match lock_service(service) {
        Ok(service) => service,
        Err(errno) => {
            finish_proven_no_write(
                coordinator,
                lease,
                &arm_request_id,
                "fuse-service-lock-not-entered",
                deadline,
            )?;
            return Err(errno);
        }
    };
    if let Err(preflight) = preflight(&mut service) {
        drop(service);
        finish_proven_no_write(
            coordinator,
            lease,
            &arm_request_id,
            preflight.proof_code,
            deadline,
        )?;
        return Err(preflight.errno);
    }
    let report = mutation(&mut service);
    // The main daemon can wait for disk, queue ownership, or network work.
    // Never make that RPC while holding the mount-wide service lane.
    drop(service);

    let captured_work = dfs::MountedSifsMutationCapturedWork::new(report.committed_work);
    match report.outcome {
        Ok(value) => {
            let mut persisted = false;
            match until_acknowledged(coordinator, deadline, || {
                if !persisted {
                    coordinator.persist_captured(&mut lease, &arm_request_id, &captured_work)?;
                    persisted = true;
                }
                coordinator.commit_captured(&lease, &arm_request_id, &captured_work)
            }) {
                Ok(()) => {
                    coordinator
                        .release_after_daemon_ack(lease)
                        .map_err(|()| libc::EIO)?;
                    Ok(value)
                }
                Err(FuseHandoffEnded::Left) => {
                    // The change is made and its work is durable in the
                    // outbox; the daemon has not accepted it within the
                    // syscall's deadline. The syscall does not report success
                    // before the daemon owns the work.
                    coordinator.finish_later(
                        lease,
                        FuseUnfinishedHandoff::Captured {
                            arm_request_id,
                            captured_work,
                        },
                    );
                    Err(libc::EIO)
                }
                Err(FuseHandoffEnded::Rejected(detail)) => {
                    // The change is made and its exact work is in the
                    // outbox, which the daemon's recovery reads once this
                    // lease ends.
                    coordinator.release_refused(lease, &detail);
                    Err(libc::EIO)
                }
            }
        }
        Err(error) => {
            // An error after service entry may have left work behind even
            // when its operation-local capture happens to be empty.
            // Preserve exactly what was observed and hand it to the
            // daemon. The mount keeps serving: the error is this call's
            // answer, never a reason to retire the session.
            let observation = match dfs::MountedSifsMutationErrorObservation::new(
                captured_work,
                error.error_code,
            ) {
                Ok(observation) => observation,
                Err(_) => return Err(libc::EIO),
            };
            let mut persisted = false;
            match until_acknowledged(coordinator, deadline, || {
                if !persisted {
                    coordinator.persist_unknown(&mut lease, &arm_request_id, &observation)?;
                    persisted = true;
                }
                coordinator.commit_unknown(&lease, &arm_request_id, &observation)
            }) {
                Ok(()) => {
                    coordinator
                        .release_after_daemon_ack(lease)
                        .map_err(|()| libc::EIO)?;
                    Err(error.errno)
                }
                Err(FuseHandoffEnded::Left) => {
                    coordinator.finish_later(
                        lease,
                        FuseUnfinishedHandoff::Unknown {
                            arm_request_id,
                            observation,
                        },
                    );
                    Err(libc::EIO)
                }
                Err(FuseHandoffEnded::Rejected(detail)) => {
                    // The daemon refused the record of an operation whose
                    // outcome this mount cannot vouch for. The record stays
                    // in the outbox for the daemon's recovery, which reads it
                    // once the mount's session ends.
                    coordinator.release_refused(lease, &detail);
                    Err(libc::EIO)
                }
            }
        }
    }
}

fn run_fuse_service_mounted_mutation_with_lock_and_preflight<'service, R>(
    handoff: Option<&FuseMountedMutationHandoff>,
    service: &'service MountLock<SifsService>,
    operation: FuseMountedMutationOperation,
    stable_operation_id: Option<String>,
    lock_service: impl FnOnce(
        &'service MountLock<SifsService>,
    )
        -> std::result::Result<MountLockGuard<'service, SifsService>, libc::c_int>,
    preflight: impl FnOnce(
        &mut SifsService,
    ) -> std::result::Result<(), FuseMountedMutationPreflightError>,
    mutation: impl FnOnce(&mut SifsService) -> std::result::Result<R, FuseMountedMutationError>,
) -> std::result::Result<R, libc::c_int> {
    let Some(handoff) = handoff else {
        let mut service = lock_service(service)?;
        preflight(&mut service).map_err(|error| error.errno)?;
        return mutation(&mut service).map_err(|error| error.errno);
    };
    run_fuse_mounted_mutation_with_lock_and_preflight(
        handoff,
        service,
        operation,
        stable_operation_id,
        dfs::MountedSifsCall::deadline(),
        lock_service,
        preflight,
        |service| service.capture_mounted_mutation(mutation),
    )
}

/// Run one syscall's DFS work under the mount's operation budget. The
/// deadline is the caller's own: it bounds every wait the work makes and
/// travels with every request it sends.
fn within_operation_deadline<R>(
    service: &mut SifsService,
    deadline: Option<Instant>,
    work: impl FnOnce(&mut SifsService) -> R,
) -> R {
    let Some(deadline) = deadline else {
        return work(service);
    };
    let previous = service.set_transport_operation_deadline(Some(deadline));
    let result = work(service);
    service.set_transport_operation_deadline(previous);
    result
}

trait ReplyErrorExt {
    fn error_from_i32(self, error: libc::c_int);
}

macro_rules! impl_reply_error_ext {
    ($($reply:ty),+ $(,)?) => {
        $(
            impl ReplyErrorExt for $reply {
                fn error_from_i32(self, error: libc::c_int) {
                    self.error(Errno::from_i32(error));
                }
            }
        )+
    };
}

impl_reply_error_ext!(
    ReplyAttr,
    ReplyCreate,
    ReplyData,
    ReplyDirectory,
    ReplyEmpty,
    ReplyEntry,
    ReplyLock,
    ReplyOpen,
    ReplyStatfs,
    ReplyWrite,
);

struct FuseReadExecutor {
    sender: Option<crossbeam_channel::Sender<FuseReadJob>>,
    workers: Vec<thread::JoinHandle<()>>,
    cancellation: dfs::DfsCancellationFlag,
}

impl std::fmt::Debug for FuseReadExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FuseReadExecutor")
            .field("worker_count", &self.workers.len())
            .finish_non_exhaustive()
    }
}

impl FuseReadExecutor {
    fn new(cancellation: dfs::DfsCancellationFlag) -> io::Result<Self> {
        let available = thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(MIN_FUSE_READ_WORKERS);
        let worker_count = available
            .saturating_mul(2)
            .clamp(MIN_FUSE_READ_WORKERS, MAX_FUSE_READ_WORKERS);
        Self::with_worker_count(worker_count, cancellation)
    }

    fn with_worker_count(
        worker_count: usize,
        cancellation: dfs::DfsCancellationFlag,
    ) -> io::Result<Self> {
        let worker_count = worker_count.max(1);
        let queue_depth = worker_count.saturating_mul(FUSE_READ_QUEUE_DEPTH_PER_WORKER);
        let (sender, receiver) = crossbeam_channel::bounded::<FuseReadJob>(queue_depth);
        let mut workers: Vec<thread::JoinHandle<()>> = Vec::with_capacity(worker_count);
        for index in 0..worker_count {
            let receiver = receiver.clone();
            let worker = match thread::Builder::new()
                .name(format!("sinomo-fuse-read-{index}"))
                .spawn(move || {
                    loop {
                        let Ok(job) = receiver.recv() else {
                            break;
                        };
                        // A single failed request must not silently reduce the
                        // bounded pool's capacity for the rest of the mount.
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
                    }
                }) {
                Ok(worker) => worker,
                Err(error) => {
                    drop(sender);
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(error);
                }
            };
            workers.push(worker);
        }
        Ok(Self {
            sender: Some(sender),
            workers,
            cancellation,
        })
    }

    fn submit(&self, job: FuseReadJob) -> std::result::Result<(), FuseReadJob> {
        let Some(sender) = self.sender.as_ref() else {
            return Err(job);
        };
        match dfs::MountedSifsCall::deadline() {
            Some(deadline) => sender
                .send_deadline(job, deadline)
                .map_err(|error| error.into_inner()),
            None => sender.send(job).map_err(|error| error.into_inner()),
        }
    }
}

impl Drop for FuseReadExecutor {
    fn drop(&mut self) {
        // Cancel before closing and joining. Jobs which were accepted but have
        // not started must not each spend a remote timeout during teardown.
        self.cancellation.cancel();
        self.sender.take();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

pub fn mount_sifs(mut options: DfsFuseMountOptions) -> Result<()> {
    options.root_path = normalize_sifs_root(&options.root_path)?;
    let mount_helper = prepare_safe_fuse_mount(&mut options)?;
    let mount_point = options.mount_point.clone();
    let cancellation = dfs::DfsCancellationFlag::new();
    let fs = SifsFuseFileSystem::open(options.clone(), cancellation)?;
    let mount_config = fuse_mount_config(&options, true);
    fuser::mount2(fs, &mount_point, &mount_config)
        .map_err(|source| map_mount_error(&mount_point, mount_helper.exists(), source))
}

pub fn mount_sifs_background(
    mut options: DfsFuseMountOptions,
    cancellation: dfs::DfsCancellationFlag,
) -> Result<DfsFuseMountHandle> {
    options.root_path = normalize_sifs_root(&options.root_path)?;
    prepare_safe_fuse_mount(&mut options)?;
    let mount_point = options.mount_point.clone();
    let fs = SifsFuseFileSystem::open(options.clone(), cancellation.clone())?;
    let mount_config = background_fuse_mount_config(&options, true);
    let session = fuser::spawn_mount2(fs, &mount_point, &mount_config)
        .map_err(|source| map_mount_error(&mount_point, true, source))?;
    Ok(DfsFuseMountHandle::new(mount_point, session, cancellation))
}

fn prepare_safe_fuse_mount(options: &mut DfsFuseMountOptions) -> Result<PathBuf> {
    validate_safe_mount_point_path(&options.mount_point)?;
    let mount_helper = required_fuse_mount_helper(&options.mount_point)?;
    #[cfg(target_os = "linux")]
    preflight_linux_sifs_background_mount_with(
        options,
        Path::new("/dev/fuse"),
        &mount_helper,
        Path::new("/etc/fuse.conf"),
    )?;
    validate_mount_point_preserves_data_dir(options, &options.mount_point)?;
    recover_disconnected_mount_point(&options.mount_point, Some(&mount_helper))?;
    let mount_point = prepare_safe_mount_point(&options.mount_point)?;
    validate_mount_point_preserves_data_dir(options, &mount_point)?;
    options.mount_point = mount_point;
    Ok(mount_helper)
}

#[cfg(target_os = "linux")]
pub fn preflight_linux_sifs_background_mount(
    options: &DfsFuseMountOptions,
) -> Result<DfsFuseLinuxPreflight> {
    validate_safe_mount_point_path(&options.mount_point)?;
    let mount_helper = required_fuse_mount_helper(&options.mount_point)?;
    preflight_linux_sifs_background_mount_with(
        options,
        Path::new("/dev/fuse"),
        &mount_helper,
        Path::new("/etc/fuse.conf"),
    )
}

#[cfg(target_os = "linux")]
fn preflight_linux_sifs_background_mount_with(
    options: &DfsFuseMountOptions,
    fuse_device: &Path,
    mount_helper: &Path,
    fuse_config: &Path,
) -> Result<DfsFuseLinuxPreflight> {
    validate_safe_mount_point_path(&options.mount_point)?;
    let metadata = std::fs::metadata(fuse_device).map_err(|source| DfsFuseError::Mount {
        mount_point: options.mount_point.display().to_string(),
        detail: format!(
            "{} is unavailable; load or enable the FUSE kernel device: {source}",
            fuse_device.display()
        ),
    })?;
    if !metadata.file_type().is_char_device() {
        return Err(DfsFuseError::Mount {
            mount_point: options.mount_point.display().to_string(),
            detail: format!("{} is not a character device", fuse_device.display()),
        });
    }
    if !mount_helper.is_file() {
        return Err(DfsFuseError::Mount {
            mount_point: options.mount_point.display().to_string(),
            detail: format!(
                "background mount lifecycle requires an executable `fusermount3` or `fusermount` helper; {} is missing",
                mount_helper.display()
            ),
        });
    }
    let allow_other_enabled = if options.allow_other {
        fuse_config_allows_other(fuse_config).map_err(|source| DfsFuseError::Mount {
            mount_point: options.mount_point.display().to_string(),
            detail: format!(
                "`--allow-other` requires an active `user_allow_other` entry in {}: {source}",
                fuse_config.display()
            ),
        })?
    } else {
        false
    };
    if options.allow_other && !allow_other_enabled {
        return Err(DfsFuseError::Mount {
            mount_point: options.mount_point.display().to_string(),
            detail: format!(
                "`--allow-other` requires an active `user_allow_other` entry in {}",
                fuse_config.display()
            ),
        });
    }
    let direct_fuse_device_access = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(fuse_device)
        .is_ok();
    Ok(DfsFuseLinuxPreflight {
        mount_point: options.mount_point.clone(),
        fuse_device: fuse_device.to_path_buf(),
        mount_helper: mount_helper.to_path_buf(),
        direct_fuse_device_access,
        allow_other_enabled,
    })
}

#[cfg(target_os = "linux")]
fn fuse_config_allows_other(path: &Path) -> io::Result<bool> {
    let contents = std::fs::read_to_string(path)?;
    Ok(contents.lines().any(|line| {
        line.split_once('#').map_or(line, |(value, _)| value).trim() == "user_allow_other"
    }))
}

fn required_fuse_mount_helper(mount_point: &Path) -> Result<PathBuf> {
    fuse_mount_helper().ok_or_else(|| DfsFuseError::Mount {
        mount_point: mount_point.display().to_string(),
        detail: "background mount lifecycle requires an executable `fusermount3` or `fusermount` helper on PATH or in a standard system location".to_string(),
    })
}

fn validate_mount_point_preserves_data_dir(
    options: &DfsFuseMountOptions,
    mount_point: &Path,
) -> Result<()> {
    let data_dir = match options.data_dir.as_ref() {
        Some(path) => SinomoDataDir::new(path),
        None => SinomoDataDir::from_env_or_home()
            .map_err(|source| DfsFuseError::LocalData(source.to_string()))?,
    };
    let data_root = absolute_or_canonical_host_path(&data_dir.root()).map_err(|source| {
        DfsFuseError::Mount {
            mount_point: mount_point.display().to_string(),
            detail: format!("failed to resolve Sinomo data directory for mount safety: {source}"),
        }
    })?;
    if data_root.starts_with(mount_point) || mount_point.starts_with(&data_root) {
        return Err(DfsFuseError::InvalidMountPoint {
            mount_point: mount_point.display().to_string(),
            detail: format!(
                "refusing a mount that overlaps the Sinomo data directory {}",
                data_root.display()
            ),
        });
    }
    Ok(())
}

fn absolute_or_canonical_host_path(path: &Path) -> io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut candidate = absolute.as_path();
    let mut missing = Vec::new();
    loop {
        match std::fs::canonicalize(candidate) {
            Ok(mut resolved) => {
                for component in missing.iter().rev() {
                    resolved.push(component);
                }
                return Ok(resolved);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let component = candidate.file_name().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("cannot resolve {}", absolute.display()),
                    )
                })?;
                missing.push(component.to_os_string());
                candidate = candidate.parent().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("cannot resolve {}", absolute.display()),
                    )
                })?;
            }
            Err(error) => return Err(error),
        }
    }
}

fn recover_disconnected_mount_point(path: &Path, mount_helper: Option<&Path>) -> Result<()> {
    let Err(source) = std::fs::metadata(path) else {
        return Ok(());
    };
    if !is_disconnected_mount_error(&source) {
        return Ok(());
    }
    let Some(mount_helper) = mount_helper else {
        return Err(DfsFuseError::Mount {
            mount_point: path.display().to_string(),
            detail: format!(
                "stale disconnected FUSE mount requires `fusermount3` or `fusermount` cleanup: {source}"
            ),
        });
    };
    let status = Command::new(mount_helper)
        .arg("-u")
        .arg("-z")
        .arg(path)
        .status()
        .map_err(|cleanup_error| DfsFuseError::Mount {
            mount_point: path.display().to_string(),
            detail: format!(
                "failed to start stale FUSE mount cleanup with {}: {cleanup_error}",
                mount_helper.display()
            ),
        })?;
    if status.success() {
        Ok(())
    } else {
        Err(DfsFuseError::Mount {
            mount_point: path.display().to_string(),
            detail: format!(
                "stale FUSE mount cleanup with {} exited with {status}",
                mount_helper.display()
            ),
        })
    }
}

fn is_disconnected_mount_error(source: &io::Error) -> bool {
    source.raw_os_error() == Some(libc::ENOTCONN)
}

fn fuse_mount_config(options: &DfsFuseMountOptions, mount_helper_available: bool) -> Config {
    let mut mount_options = vec![
        MountOption::FSName(options.mount_name.clone()),
        MountOption::DefaultPermissions,
    ];
    // fuser implements AutoUnmount through fusermount by implicitly enabling
    // `allow_other`. Only request it when a helper is available and the caller
    // explicitly opted in, keeping default foreground mounts owner-only.
    if mount_helper_available && options.allow_other {
        mount_options.push(MountOption::AutoUnmount);
    }
    if options.read_only {
        mount_options.push(MountOption::RO);
    }
    let mut config = Config::default();
    config.mount_options = mount_options;
    config.acl = if options.allow_other {
        SessionACL::All
    } else {
        SessionACL::Owner
    };
    config
}

fn background_fuse_mount_config(
    options: &DfsFuseMountOptions,
    mount_helper_available: bool,
) -> Config {
    // BackgroundSession owns the mount and unmounts it when dropped. Do not
    // force AutoUnmount for an owner-only mount: fuser implements that option
    // by implicitly adding allow_other, which fails on standard installations
    // without `user_allow_other` in /etc/fuse.conf and silently widens the
    // kernel mount request beyond the caller's intent.
    fuse_mount_config(options, mount_helper_available)
}

fn fuse_mount_helper() -> Option<PathBuf> {
    fuse_mount_helper_on_path(std::env::var_os("PATH").as_deref()).or_else(|| {
        [
            "/usr/bin/fusermount3",
            "/usr/bin/fusermount",
            "/bin/fusermount3",
            "/bin/fusermount",
            "/usr/local/bin/fusermount3",
            "/usr/local/bin/fusermount",
        ]
        .into_iter()
        .map(PathBuf::from)
        .find(|candidate| candidate.is_file())
    })
}

fn fuse_mount_helper_on_path(path: Option<&OsStr>) -> Option<PathBuf> {
    let path = path?;
    std::env::split_paths(path).find_map(|directory| {
        ["fusermount3", "fusermount"]
            .into_iter()
            .map(|name| directory.join(name))
            .find(|candidate| candidate.is_file())
    })
}


fn map_mount_error(
    mount_point: &Path,
    mount_helper_available: bool,
    source: io::Error,
) -> DfsFuseError {
    let detail = if !mount_point.exists() {
        format!("mount point does not exist: {source}")
    } else if !Path::new("/dev/fuse").exists() {
        format!("/dev/fuse is unavailable; load or enable the FUSE kernel device: {source}")
    } else if !mount_helper_available
        && matches!(
            source.kind(),
            io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
        )
    {
        format!(
            "direct /dev/fuse mounting was not permitted and no executable `fusermount3` or \
             `fusermount` helper was found; grant the service mount capability or install a FUSE \
             helper: {source}"
        )
    } else {
        source.to_string()
    };
    DfsFuseError::Mount {
        mount_point: mount_point.display().to_string(),
        detail,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FusePresentedOwner {
    uid: u32,
    gid: u32,
}

impl FusePresentedOwner {
    fn for_mount(allow_other: bool) -> Option<Self> {
        if allow_other {
            return None;
        }
        // SAFETY: `geteuid` and `getegid` have no preconditions and only read
        // the effective credentials of this process, which owns the FUSE
        // session.
        let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
        Some(Self { uid, gid })
    }
}

#[derive(Debug)]
struct SifsFuseFileSystem {
    dfs: Arc<MountLock<SifsService>>,
    mounted_mutation_handoff: Option<Arc<FuseMountedMutationHandoff>>,
    content_reads: dfs::MountedSifsContentReadContext,
    read_cancellation: dfs::DfsCancellationFlag,
    read_executor: FuseReadExecutor,
    state: Arc<MountLock<FuseState>>,
    read_only: bool,
    presented_owner: Option<FusePresentedOwner>,
    attr_ttl: Duration,
    entry_ttl: Duration,
    write_replica_candidates: Vec<dfs::DfsNodeId>,
    /// The operator's bound on one syscall's DFS work; `None` waits for
    /// events only.
    operation_budget: Option<Duration>,
    lock_mount_session_id: String,
    lock_request_sequence: Arc<AtomicU64>,
    lock_renewer_stop: Arc<AtomicBool>,
}

impl SifsFuseFileSystem {
    fn open(
        options: DfsFuseMountOptions,
        read_cancellation: dfs::DfsCancellationFlag,
    ) -> Result<Self> {
        if read_cancellation.is_cancelled() {
            return Err(DfsFuseError::Dfs(
                "SIFS FUSE mount was cancelled before open".to_string(),
            ));
        }
        let data_dir = match options.data_dir.as_ref() {
            Some(path) => SinomoDataDir::new(path),
            None => SinomoDataDir::from_env_or_home()
                .map_err(|source| DfsFuseError::LocalData(source.to_string()))?,
        };
        let presented_owner = FusePresentedOwner::for_mount(options.allow_other);
        let clustered = sinomo_runtime::network::ClusterPaths::new(&data_dir)
            .state
            .exists();
        let lock_mount_session_id = format!(
            "fuse-{}-{}-{}",
            std::process::id(),
            system_time_to_nanos(SystemTime::now()),
            NEXT_MOUNT_SESSION_ID.fetch_add(1, Ordering::Relaxed)
        );
        let mounted_mutation_client =
            if fuse_mounted_mutation_handoff_required(clustered, options.read_only) {
                let daemon = sinomo_runtime::api::DaemonApiClient::connect_for_paths(
                    &sinomo_runtime::api::DaemonPaths::new(&data_dir),
                )
                .map_err(|source| DfsFuseError::Dfs(source.to_string()))?;
                let client = sinomo_runtime::dfs::DaemonDfsMountedMutationHandoffClient::new(
                    data_dir.clone(),
                    daemon.address().to_string(),
                )
                .map_err(|source| DfsFuseError::Dfs(source.to_string()))?;
                Some(client)
            } else {
                None
            };
        // A clustered backend open can finish interrupted durable commits or
        // bootstrap/rebind shard state. Establish the host-session lease
        // immediately after Probe, before constructing any writable DFS
        // backend, so MountOpen can Arm around that entire crash window.
        let mounted_mutation_handoff = match mounted_mutation_client {
            Some(client) => Some(prepare_fuse_clustered_mount_handoff_with(
                client,
                |client| {
                    client
                        .probe_fuse()
                        .map(drop)
                        .map_err(|source| DfsFuseError::Dfs(source.to_string()))
                },
                |client| {
                    let store = dfs::MountedSifsMutationOutboxStore::new(&data_dir)
                        .map_err(|source| DfsFuseError::Dfs(source.to_string()))?;
                    let session = store
                        .open_fuse_mount_session(&options.mount_point)
                        .map_err(|source| DfsFuseError::Dfs(source.to_string()))?;
                    // The mount's own cancellation closes the handoff, so
                    // no wait for the daemon outlives the mount.
                    dfs::MountedSifsMutationHandoff::start(
                        store,
                        session,
                        Arc::new(client),
                        Some(&read_cancellation),
                    )
                    .map(FuseMountedMutationHandoff::new)
                    .map_err(|error| {
                        DfsFuseError::Dfs(format!("start mounted handoff worker: {error}"))
                    })
                },
            )?),
            None => None,
        };
        let dfs::OpenedMountedSifsService {
            service,
            replica_candidates,
        } = if clustered {
            let handoff = mounted_mutation_handoff
                .as_ref()
                .expect("clustered FUSE mount established its mandatory handoff");
            run_fuse_clustered_mount_open_with(
                handoff,
                || {
                    let peer_transport = Arc::new(
                        sinomo_runtime::dfs::DaemonDfsPeerTransport::connect(data_dir.clone())
                            .map_err(|source| DfsFuseError::Dfs(source.to_string()))?,
                    );
                    let sync_peer_transport: Arc<dyn dfs::DfsPeerTransport> =
                        peer_transport.clone();
                    let async_peer_transport: Arc<dyn dfs::AsyncDfsPeerTransport> = peer_transport;
                    dfs::open_mounted_sifs_service_with_peer_transports_and_default_control_plane_and_cancellation(
                        data_dir.clone(),
                        sync_peer_transport,
                        async_peer_transport,
                        Some(read_cancellation.clone()),
                    )
                    .map_err(|source| DfsFuseError::Dfs(source.to_string()))
                },
            )
            .map_err(|error| match error {
                FuseMountOpenHandoffError::Service(source) => source,
                FuseMountOpenHandoffError::Handoff {
                    detail,
                    service_error,
                } => DfsFuseError::Dfs(service_error.map_or_else(
                    || detail.to_string(),
                    |source| format!("{source}; {detail}"),
                )),
            })?
        } else {
            // A standalone one-node data directory has no daemon-owned cluster
            // transport to share, so retain the local-only mounted service.
            dfs::open_mounted_sifs_service_with_cancellation(
                Some(data_dir.root()),
                Some(read_cancellation.clone()),
            )
            .map_err(|source| DfsFuseError::Dfs(source.to_string()))?
        };
        // Resolve the local-only fallback before the service is placed behind
        // the mount-wide mutex. FUSE mutation handlers already hold that
        // mutex while calling `write_options`; trying to recover the local
        // node id there would recursively lock a non-reentrant mutex whenever
        // the cluster currently advertises no content-service candidates.
        let replica_candidates =
            fuse_write_replica_candidates(replica_candidates, service.node_id().clone());
        let content_reads = service.mounted_content_read_context();
        let read_executor = FuseReadExecutor::new(read_cancellation.clone())
            .map_err(|source| DfsFuseError::Dfs(format!("start FUSE read workers: {source}")))?;
        let dfs = Arc::new(MountLock::new(service));
        let state = Arc::new(MountLock::new(FuseState::new(options.root_path.clone())));
        let mounted_mutation_handoff = mounted_mutation_handoff.map(Arc::new);
        let lock_request_sequence = Arc::new(AtomicU64::new(1));
        let lock_renewer_stop = Arc::new(AtomicBool::new(false));
        spawn_lock_renewer(
            Arc::downgrade(&dfs),
            Arc::downgrade(&state),
            mounted_mutation_handoff.as_ref().map(Arc::downgrade),
            lock_request_sequence.clone(),
            lock_renewer_stop.clone(),
        );
        Ok(Self {
            dfs,
            mounted_mutation_handoff,
            content_reads,
            read_cancellation,
            read_executor,
            state,
            read_only: options.read_only,
            presented_owner,
            attr_ttl: options.attr_ttl,
            entry_ttl: options.entry_ttl,
            write_replica_candidates: replica_candidates,
            operation_budget: options.operation_budget,
            lock_mount_session_id,
            lock_request_sequence,
            lock_renewer_stop,
        })
    }

    fn lock_state(&self) -> std::result::Result<MountLockGuard<'_, FuseState>, libc::c_int> {
        dfs::MountedSifsCall::lock(&self.state).map_err(|_| libc::EIO)
    }

    fn ensure_handle_lock_not_lost(&self, fh: u64) -> std::result::Result<(), libc::c_int> {
        let mut state = self.lock_state()?;
        state.expire_elapsed_locks(Instant::now());
        if state.has_lost_lock_for_handle(fh) {
            Err(libc::EIO)
        } else {
            Ok(())
        }
    }

    fn ensure_io_lock_not_lost(
        &self,
        ino: u64,
        fh: u64,
        lock_owner: Option<u64>,
    ) -> std::result::Result<(), libc::c_int> {
        // Drop the state guard before the path fallback, which also consults
        // FuseState. Holding the guard across that fallback would deadlock.
        let object_id = {
            let state = self.lock_state()?;
            state
                .handles
                .get(&fh)
                .map(|handle| handle.object_id.clone())
        };
        let object_id = match object_id {
            Some(object_id) => Some(object_id),
            None => {
                let path = self.path_for_ino(ino)?;
                Some(self.stat_path(&path, true)?.object_id)
            }
        };
        let mut state = self.lock_state()?;
        state.expire_elapsed_locks(Instant::now());
        if state.has_lost_lock_for_handle(fh) {
            return Err(libc::EIO);
        }
        let (Some(lock_owner), Some(object_id)) = (lock_owner, object_id) else {
            return Ok(());
        };
        let key = (object_id, lock_owner);
        if let Some(lost) = state.lost_locks.get_mut(&key) {
            lost.associated_fhs.insert(fh);
            return Err(libc::EIO);
        }
        if let Some(held) = state.held_locks.get_mut(&key) {
            held.associated_fhs.insert(fh);
        }
        if let Some(pending) = state.pending_locks.get_mut(&key) {
            pending.associated_fhs.insert(fh);
        }
        Ok(())
    }

    fn ensure_handle_lock_budget(
        &self,
        fh: u64,
        lock_owner: Option<u64>,
        minimum_remaining: Duration,
    ) -> std::result::Result<(), libc::c_int> {
        self.ensure_handle_lock_not_lost(fh)?;
        let now = Instant::now();
        let renewals = {
            let mut state = self.lock_state()?;
            state.expire_elapsed_locks(now);
            let object_id = state
                .handles
                .get(&fh)
                .map(|handle| handle.object_id.clone())
                .ok_or(libc::EBADF)?;
            state
                .held_locks
                .iter_mut()
                .filter_map(|(key, held)| {
                    let owner_matches =
                        lock_owner.is_some_and(|owner| key.0 == object_id && key.1 == owner);
                    if held.associated_fhs.contains(&fh) || owner_matches {
                        held.associated_fhs.insert(fh);
                        Some((key.clone(), held.clone()))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };
        for (key, held) in renewals {
            let command = dfs::DfsAdvisoryLockCommand::Renew {
                request_id: self.next_lock_request_id("io-budget-renew"),
                token: held.grant.token.clone(),
            };
            let response = match run_advisory_lock_command(
                self.mounted_mutation_handoff.as_deref(),
                &self.dfs,
                &held.path,
                &held.object_id,
                command,
            ) {
                Ok(response) => response,
                Err(_) => {
                    mark_fuse_lock_lost(&self.state, &key, &held.grant);
                    return Err(libc::EIO);
                }
            };
            let FuseLockCommandResult {
                outcome,
                local_lease_deadline,
            } = response;
            let dfs::DfsAdvisoryLockOutcome::Renewed { grant: renewed } = outcome else {
                mark_fuse_lock_lost(&self.state, &key, &held.grant);
                return Err(libc::EIO);
            };
            let Some(local_deadline) = local_lease_deadline else {
                mark_fuse_lock_lost(&self.state, &key, &held.grant);
                return Err(libc::EIO);
            };
            if local_deadline.saturating_duration_since(Instant::now()) < minimum_remaining {
                mark_fuse_lock_lost(&self.state, &key, &held.grant);
                return Err(libc::EIO);
            }
            let mut state = self.lock_state()?;
            if let Some(current) = state.held_locks.get_mut(&key)
                && current.grant == held.grant
            {
                current.grant = renewed;
                current.local_lease_deadline = local_deadline;
            }
        }
        self.ensure_handle_lock_not_lost(fh)
    }

    fn next_lock_request_id(&self, operation: &str) -> dfs::DfsAdvisoryLockRequestId {
        next_lock_request_id(&self.lock_request_sequence, operation)
    }

    fn lock_session_id(&self, lock_owner: u64) -> dfs::DfsAdvisoryLockSessionId {
        dfs::DfsAdvisoryLockSessionId::new(format!("{}:{lock_owner}", self.lock_mount_session_id))
    }

    fn lock_dfs(&self) -> std::result::Result<dfs::MountedSifsServiceGuard<'_>, libc::c_int> {
        if self
            .mounted_mutation_handoff
            .as_ref()
            .is_some_and(|handoff| handoff.is_gated())
        {
            return Err(libc::EIO);
        }
        dfs::MountedSifsCall::lock_service(&self.dfs).map_err(|_| libc::EIO)
    }

    fn run_mounted_mutation<R>(
        &self,
        operation: FuseMountedMutationOperation,
        mutation: impl FnOnce(&mut SifsService) -> std::result::Result<R, FuseMountedMutationError>,
    ) -> std::result::Result<R, libc::c_int> {
        // The budget covers the whole syscall, its wait for the mount's
        // service lane included.
        let deadline = dfs::MountedSifsCall::deadline();
        let mutation =
            move |service: &mut SifsService| within_operation_deadline(service, deadline, mutation);
        let Some(handoff) = self.mounted_mutation_handoff.as_ref() else {
            return self
                .lock_dfs()
                .and_then(|mut service| mutation(&mut service).map_err(|error| error.errno));
        };
        run_fuse_mounted_mutation_until(
            handoff.as_ref(),
            &self.dfs,
            operation,
            deadline,
            |service| service.capture_mounted_mutation(mutation),
        )
    }

    /// The deadline of a syscall that starts now: the mount's operation
    /// budget, when its operator set one. Without one the syscall waits for
    /// events only.
    fn operation_deadline(&self) -> Option<Instant> {
        self.operation_budget
            .and_then(|budget| Instant::now().checked_add(budget))
    }

    fn write_options(&self) -> DfsWriteOptions {
        fuse_write_options(self.write_replica_candidates.clone())
    }

    fn ensure_writable(&self) -> std::result::Result<(), libc::c_int> {
        if self.read_only {
            Err(libc::EROFS)
        } else if self
            .mounted_mutation_handoff
            .as_ref()
            .is_some_and(|handoff| handoff.is_gated())
        {
            Err(libc::EIO)
        } else {
            Ok(())
        }
    }

    fn path_for_ino(&self, ino: u64) -> std::result::Result<String, libc::c_int> {
        self.lock_state()?
            .path_for(ino)
            .ok_or(libc::ENOENT)
            .map(ToOwned::to_owned)
    }

    fn child_path(&self, parent: u64, name: &OsStr) -> std::result::Result<String, libc::c_int> {
        let parent_path = self.path_for_ino(parent)?;
        join_sifs_child(&parent_path, name).map_err(|_| libc::EINVAL)
    }

    fn intern_entry(
        &self,
        path: String,
        object_id: &dfs::DfsObjectId,
    ) -> std::result::Result<u64, libc::c_int> {
        Ok(self.lock_state()?.intern_entry(path, object_id.clone()))
    }

    fn stat_path(
        &self,
        path: &str,
        follow: bool,
    ) -> std::result::Result<dfs::DfsStat, libc::c_int> {
        let mut dfs = self.lock_dfs()?;
        let result = if follow {
            dfs.posix_stat_distributed(path)
        } else {
            dfs.posix_lstat_distributed(path)
        };
        result.map_err(errno_for_posix_error)
    }

    fn reply_entry_for_path(&self, reply: ReplyEntry, path: String, stat: dfs::DfsStat) {
        match self.intern_entry(path, &stat.object_id) {
            Ok(ino) => reply.entry(
                &self.entry_ttl,
                &file_attr(ino, &stat, self.presented_owner),
                Generation(0),
            ),
            Err(errno) => reply.error_from_i32(errno),
        }
    }

    fn commit_handle(&self, fh: u64) -> std::result::Result<(), libc::c_int> {
        let gate = self.inode_publication_gate(fh)?;
        let _guard = dfs::MountedSifsCall::lock(&gate).map_err(|_| libc::EIO)?;
        self.commit_inode_ranges(fh).map(drop)
    }

    fn inode_publication_gate(
        &self,
        fh: u64,
    ) -> std::result::Result<Arc<MountLock<()>>, libc::c_int> {
        Ok(self
            .lock_state()?
            .handles
            .get(&fh)
            .ok_or(libc::EBADF)?
            .publication_gate
            .clone())
    }

    // Caller holds the inode gate. Reads can still see the frozen bytes while
    // the immutable batch is in flight. An ambiguous reply retains its ID.
    fn commit_inode_ranges(
        &self,
        fh: u64,
    ) -> std::result::Result<Option<dfs::DfsStat>, libc::c_int> {
        self.ensure_handle_lock_not_lost(fh)?;
        let mut last = None;
        loop {
            let Some(publication) = self.lock_state()?.take_dirty_publication(fh)? else {
                return Ok(last);
            };
            for writer in &publication.writers {
                self.ensure_handle_lock_not_lost(*writer)?;
                self.ensure_handle_lock_budget(*writer, None, LOCKED_IO_MIN_LEASE)?;
            }
            let options = self.write_options();
            let stat = self.run_mounted_mutation(
                FuseMountedMutationOperation::BufferedWritePublication,
                |dfs| {
                    dfs.posix_publish_dirty_ranges(
                        &publication.base,
                        &publication.dirty.ranges,
                        &publication.operation_id,
                        options,
                    )
                    .map_err(FuseMountedMutationError::from)
                },
            )?;
            let mut state = self.lock_state()?;
            state.refresh_object_handle_stats(&publication.base.object_id, &stat)?;
            let dirty = state
                .dirty_inodes
                .get_mut(&publication.base.object_id)
                .ok_or(libc::ESTALE)?;
            dirty.frozen = None;
            if dirty
                .base
                .as_ref()
                .is_some_and(|base| base.version == publication.base.version)
            {
                dirty.base = Some(stat.clone());
            }
            if dirty.active.is_empty() {
                state.dirty_inodes.remove(&publication.base.object_id);
            }
            last = Some(stat);
        }
    }

    fn commit_dirty_handles_for_path(
        &self,
        path: &str,
        include_descendants: bool,
    ) -> std::result::Result<(), libc::c_int> {
        let handles = self
            .lock_state()?
            .dirty_handles_for_path(path, include_descendants);
        for fh in handles {
            self.commit_handle(fh)?;
        }
        Ok(())
    }

    fn commit_dirty_handles_for_object(
        &self,
        object_id: &dfs::DfsObjectId,
        except_fh: Option<u64>,
    ) -> std::result::Result<(), libc::c_int> {
        let handles = self
            .lock_state()?
            .dirty_handles_for_object(object_id, except_fh);
        for fh in handles {
            self.commit_handle(fh)?;
        }
        Ok(())
    }

    fn commit_dirty_handles_for_handle_object(
        &self,
        fh: u64,
    ) -> std::result::Result<dfs::DfsObjectId, libc::c_int> {
        let object_id = self
            .lock_state()?
            .handles
            .get(&fh)
            .ok_or(libc::EBADF)?
            .object_id
            .clone();
        self.commit_dirty_handles_for_object(&object_id, None)?;
        Ok(object_id)
    }

    fn sync_handle(&self, fh: u64, datasync: bool) -> std::result::Result<(), libc::c_int> {
        let kind = if datasync {
            dfs::DfsPosixSyncKind::FileData
        } else {
            dfs::DfsPosixSyncKind::File
        };
        let target = {
            let gate = self.inode_publication_gate(fh)?;
            let _guard = dfs::MountedSifsCall::lock(&gate).map_err(|_| libc::EIO)?;
            match self.commit_inode_ranges(fh)? {
                Some(stat) => dfs::DfsPosixFileSyncTarget {
                    kind,
                    object_id: stat.object_id,
                    file_system_id: stat.file_system_id,
                    path: stat.path,
                    version: stat.version,
                    content_hash: stat.content_hash.ok_or(libc::EIO)?,
                },
                None => self.current_file_sync_target_for_handle(fh, kind)?,
            }
        };
        self.ensure_handle_lock_budget(fh, None, LOCKED_IO_MIN_LEASE)?;
        self.run_mounted_mutation(FuseMountedMutationOperation::FileSync, |dfs| {
            dfs.posix_sync_file_target(&target)
                .map(drop)
                .map_err(FuseMountedMutationError::from)
        })
    }

    fn current_file_sync_target_for_handle(
        &self,
        fh: u64,
        kind: dfs::DfsPosixSyncKind,
    ) -> std::result::Result<dfs::DfsPosixFileSyncTarget, libc::c_int> {
        let object_id = self
            .lock_state()?
            .handles
            .get(&fh)
            .ok_or(libc::EBADF)?
            .object_id
            .clone();
        self.lock_dfs()?
            .posix_capture_file_sync_target_for_object(&object_id, kind)
            .map_err(errno_for_posix_error)
    }

    fn refresh_opened_object_stats(
        &self,
        object_id: &dfs::DfsObjectId,
        stat: &dfs::DfsStat,
    ) -> std::result::Result<(), libc::c_int> {
        self.lock_state()?
            .refresh_object_handle_stats(object_id, stat)
    }

    fn handle_lock_identity(
        &self,
        ino: u64,
        fh: u64,
    ) -> std::result::Result<(String, dfs::DfsObjectId), libc::c_int> {
        if let Some(handle) = self.lock_state()?.handles.get(&fh) {
            return Ok((handle.path.clone(), handle.object_id.clone()));
        }
        let path = self.path_for_ino(ino)?;
        let stat = self.stat_path(&path, true)?;
        Ok((path, stat.object_id))
    }

    fn query_advisory_lock(
        &self,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        mode: dfs::DfsAdvisoryLockMode,
    ) -> std::result::Result<Option<dfs::DfsAdvisoryLockConflict>, libc::c_int> {
        let (path, object_id) = self.handle_lock_identity(ino, fh)?;
        let key = (object_id.clone(), lock_owner);
        let mut state = self.lock_state()?;
        state.expire_elapsed_locks(Instant::now());
        if state.lost_locks.contains_key(&key) {
            return Err(libc::EIO);
        }
        drop(state);
        let command = dfs::DfsAdvisoryLockCommand::Query {
            request_id: self.next_lock_request_id("query"),
            session_id: self.lock_session_id(lock_owner),
            mode,
        };
        match run_advisory_lock_command(
            self.mounted_mutation_handoff.as_deref(),
            &self.dfs,
            &path,
            &object_id,
            command,
        )?
        .outcome
        {
            dfs::DfsAdvisoryLockOutcome::QueryResult { conflict } => Ok(conflict),
            outcome => Err(errno_for_unexpected_lock_outcome(&outcome)),
        }
    }

    fn set_advisory_lock(
        &self,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        mode: dfs::DfsAdvisoryLockMode,
        process_id: u32,
        blocking: bool,
        reply: ReplyEmpty,
    ) {
        let (path, object_id) = match self.handle_lock_identity(ino, fh) {
            Ok(identity) => identity,
            Err(errno) => return reply.error_from_i32(errno),
        };
        let key = (object_id.clone(), lock_owner);
        let (existing, prior_lost_fhs, can_start_lock_key) = match self.lock_state() {
            Ok(mut state) => {
                state.expire_elapsed_locks(Instant::now());
                (
                    state.held_locks.get(&key).cloned(),
                    state
                        .lost_locks
                        .get(&key)
                        .map(|lost| lost.associated_fhs.clone())
                        .unwrap_or_default(),
                    state.can_start_lock_key(&key),
                )
            }
            Err(errno) => return reply.error_from_i32(errno),
        };
        if let Some(existing) = existing {
            let command = if existing.grant.mode == mode {
                dfs::DfsAdvisoryLockCommand::Renew {
                    request_id: self.next_lock_request_id("setlk-renew"),
                    token: existing.grant.token.clone(),
                }
            } else {
                dfs::DfsAdvisoryLockCommand::Convert {
                    request_id: self.next_lock_request_id("setlk-convert"),
                    token: existing.grant.token.clone(),
                    mode,
                }
            };
            match run_advisory_lock_command(
                self.mounted_mutation_handoff.as_deref(),
                &self.dfs,
                &path,
                &object_id,
                command,
            ) {
                Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::Renewed { grant },
                    local_lease_deadline,
                })
                | Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::Converted { grant },
                    local_lease_deadline,
                }) => {
                    let mut associated_fhs = existing.associated_fhs.clone();
                    associated_fhs.extend(prior_lost_fhs.iter().copied());
                    associated_fhs.insert(fh);
                    let held = fuse_held_lock(
                        path,
                        object_id,
                        existing.session_id,
                        grant,
                        local_lease_deadline,
                        associated_fhs.clone(),
                    );
                    let mut state = match self.lock_state() {
                        Ok(state) => state,
                        Err(errno) => return reply.error_from_i32(errno),
                    };
                    if let Some(held) = held {
                        state.held_locks.insert(key.clone(), held);
                        state.lost_locks.remove(&key);
                        reply.ok();
                    } else {
                        state.held_locks.remove(&key);
                        state.record_lost_lock(key, associated_fhs);
                        reply.error_from_i32(libc::EIO);
                    }
                }
                // Two shared holders attempting blocking upgrades would both
                // retain their old grant and wait forever. Surface POSIX's
                // deadlock result while keeping the existing lock intact.
                Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::WouldBlock { .. },
                    ..
                }) if blocking => reply.error_from_i32(libc::EDEADLK),
                Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::WouldBlock { .. },
                    ..
                })
                | Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::AuthorityWarming { .. },
                    ..
                }) => reply.error_from_i32(libc::EAGAIN),
                Ok(response) => {
                    let outcome = response.outcome;
                    if lock_outcome_loses_grant(&outcome)
                        && let Ok(mut state) = self.lock_state()
                    {
                        let mut associated_fhs = state
                            .held_locks
                            .remove(&key)
                            .map(|held| held.associated_fhs)
                            .unwrap_or_default();
                        associated_fhs.extend(prior_lost_fhs.iter().copied());
                        associated_fhs.insert(fh);
                        state.record_lost_lock(key, associated_fhs);
                    }
                    reply.error_from_i32(errno_for_unexpected_lock_outcome(&outcome));
                }
                Err(errno) => {
                    // After both exact retries fail, a renew or conversion may
                    // have committed remotely. Stop using the grant locally;
                    // its finite server lease bounds any uncertain residue.
                    if let Ok(mut state) = self.lock_state() {
                        let mut associated_fhs = state
                            .held_locks
                            .remove(&key)
                            .map(|held| held.associated_fhs)
                            .unwrap_or_else(|| existing.associated_fhs.clone());
                        associated_fhs.extend(prior_lost_fhs.iter().copied());
                        associated_fhs.insert(fh);
                        state.record_lost_lock(key, associated_fhs);
                    }
                    reply.error_from_i32(errno);
                }
            }
            return;
        }

        if !can_start_lock_key {
            return reply.error_from_i32(libc::ENOLCK);
        }

        let acquire_request_id = self.next_lock_request_id("acquire");
        let session_id = self.lock_session_id(lock_owner);
        let command = dfs::DfsAdvisoryLockCommand::Acquire {
            request_id: acquire_request_id.clone(),
            session_id: session_id.clone(),
            mode,
            blocking,
            process_id: Some(process_id),
        };
        if blocking {
            let mut associated_fhs = prior_lost_fhs;
            associated_fhs.insert(fh);
            let pending = FusePendingLock {
                path: path.clone(),
                object_id: object_id.clone(),
                session_id: session_id.clone(),
                acquire_request_id,
                associated_fhs,
            };
            match self.lock_state() {
                Ok(mut state) => {
                    if state.pending_locks.contains_key(&key) {
                        return reply.error_from_i32(libc::EDEADLK);
                    }
                    state.pending_locks.insert(key.clone(), pending);
                }
                Err(errno) => return reply.error_from_i32(errno),
            }
            spawn_blocking_lock_waiter(
                Arc::downgrade(&self.dfs),
                Arc::downgrade(&self.state),
                self.mounted_mutation_handoff.as_ref().map(Arc::downgrade),
                self.lock_renewer_stop.clone(),
                key,
                command,
                reply,
            );
            return;
        }

        match run_advisory_lock_command(
            self.mounted_mutation_handoff.as_deref(),
            &self.dfs,
            &path,
            &object_id,
            command,
        ) {
            Ok(FuseLockCommandResult {
                outcome: dfs::DfsAdvisoryLockOutcome::Acquired { grant },
                local_lease_deadline,
            }) => {
                let mut associated_fhs = prior_lost_fhs;
                associated_fhs.insert(fh);
                let held = fuse_held_lock(
                    path.clone(),
                    object_id.clone(),
                    session_id.clone(),
                    grant,
                    local_lease_deadline,
                    associated_fhs.clone(),
                );
                let mut state = match self.lock_state() {
                    Ok(state) => state,
                    Err(errno) => return reply.error_from_i32(errno),
                };
                if let Some(held) = held {
                    state.held_locks.insert(key.clone(), held);
                    state.lost_locks.remove(&key);
                    reply.ok();
                } else {
                    state.record_lost_lock(key, associated_fhs);
                    reply.error_from_i32(libc::EIO);
                }
            }
            Ok(FuseLockCommandResult {
                outcome: dfs::DfsAdvisoryLockOutcome::WouldBlock { .. },
                ..
            })
            | Ok(FuseLockCommandResult {
                outcome: dfs::DfsAdvisoryLockOutcome::AuthorityWarming { .. },
                ..
            }) => reply.error_from_i32(libc::EAGAIN),
            Ok(response) => {
                reply.error_from_i32(errno_for_unexpected_lock_outcome(&response.outcome))
            }
            Err(errno) => {
                let mut associated_fhs = prior_lost_fhs;
                associated_fhs.insert(fh);
                if let Ok(mut state) = self.lock_state() {
                    state.record_lost_lock(key, associated_fhs);
                }
                reply.error_from_i32(errno);
            }
        }
    }

    fn unlock_advisory_lock(
        &self,
        ino: u64,
        fh: u64,
        lock_owner: u64,
    ) -> std::result::Result<(), libc::c_int> {
        let (path, object_id) = self.handle_lock_identity(ino, fh)?;
        let key = (object_id.clone(), lock_owner);
        let (held, pending, lost) = {
            let mut state = self.lock_state()?;
            state.expire_elapsed_locks(Instant::now());
            (
                state.held_locks.get(&key).cloned(),
                state.pending_locks.get(&key).cloned(),
                state.lost_locks.get(&key).cloned(),
            )
        };
        let mut observed_loss = lost.is_some();
        let mut lost_associated_fhs = lost.map(|lost| lost.associated_fhs).unwrap_or_default();
        if let Some(held) = held {
            lost_associated_fhs.extend(held.associated_fhs.iter().copied());
            let command = dfs::DfsAdvisoryLockCommand::Unlock {
                request_id: self.next_lock_request_id("unlock"),
                token: held.grant.token,
            };
            match run_advisory_lock_command(
                self.mounted_mutation_handoff.as_deref(),
                &self.dfs,
                &path,
                &object_id,
                command,
            ) {
                Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::Unlocked,
                    ..
                }) => {}
                Ok(response) if lock_outcome_loses_grant(&response.outcome) => observed_loss = true,
                Ok(response) => {
                    return Err(errno_for_unexpected_lock_outcome(&response.outcome));
                }
                Err(_) => observed_loss = true,
            }
        } else if let Some(pending) = pending {
            lost_associated_fhs.extend(pending.associated_fhs.iter().copied());
            // Publish local cancellation before waiting for the serialized
            // service lane. The blocking waiter rechecks this marker while it
            // owns that lane and therefore cannot send a later first Acquire.
            self.lock_state()?.pending_locks.remove(&key);
            let command = dfs::DfsAdvisoryLockCommand::Cancel {
                request_id: self.next_lock_request_id("cancel"),
                session_id: pending.session_id,
                acquire_request_id: pending.acquire_request_id,
            };
            match run_advisory_lock_command(
                self.mounted_mutation_handoff.as_deref(),
                &self.dfs,
                &path,
                &object_id,
                command,
            ) {
                Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::Cancelled { .. },
                    ..
                }) => {}
                Ok(response) => {
                    return Err(errno_for_unexpected_lock_outcome(&response.outcome));
                }
                Err(_) => observed_loss = true,
            }
        } else if observed_loss {
            return Err(libc::EIO);
        }
        let mut state = self.lock_state()?;
        state.held_locks.remove(&key);
        state.pending_locks.remove(&key);
        if observed_loss {
            state.record_lost_lock(key, lost_associated_fhs);
            Err(libc::EIO)
        } else {
            state.lost_locks.remove(&key);
            Ok(())
        }
    }

    fn close_advisory_locks_for_handle(
        &self,
        fh: u64,
        lock_owner: u64,
    ) -> std::result::Result<(), libc::c_int> {
        let Some((path, object_id)) = self
            .lock_state()?
            .handles
            .get(&fh)
            .map(|handle| (handle.path.clone(), handle.object_id.clone()))
        else {
            return Ok(());
        };
        let key = (object_id.clone(), lock_owner);
        let (tracked, was_lost, pending) = {
            let mut state = self.lock_state()?;
            state.expire_elapsed_locks(Instant::now());
            (
                state.held_locks.contains_key(&key)
                    || state.pending_locks.contains_key(&key)
                    || state.lost_locks.contains_key(&key),
                state.lost_locks.contains_key(&key),
                state.pending_locks.get(&key).cloned(),
            )
        };
        if !tracked {
            return Ok(());
        }
        // Stop waiter dispatch and background renewal before the close RPC.
        // Any waiter already holding the service lane linearizes first; Close
        // then releases its result. A waiter behind us observes no pending
        // marker and cannot send a new Acquire after Close.
        {
            let mut state = self.lock_state()?;
            state.held_locks.remove(&key);
            state.pending_locks.remove(&key);
            state.lost_locks.remove(&key);
        }
        let pending_cleanup_result = if let Some(pending) = pending {
            let cancel = dfs::DfsAdvisoryLockCommand::Cancel {
                request_id: self.next_lock_request_id("close-cancel"),
                session_id: pending.session_id,
                acquire_request_id: pending.acquire_request_id,
            };
            match run_advisory_lock_command(
                self.mounted_mutation_handoff.as_deref(),
                &self.dfs,
                &path,
                &object_id,
                cancel,
            ) {
                Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::Cancelled { .. },
                    ..
                }) => Ok(()),
                Ok(response) => Err(errno_for_unexpected_lock_outcome(&response.outcome)),
                Err(errno) => Err(errno),
            }
        } else {
            Ok(())
        };
        let command = dfs::DfsAdvisoryLockCommand::Close {
            request_id: self.next_lock_request_id("close"),
            session_id: self.lock_session_id(lock_owner),
        };
        let result = run_advisory_lock_command(
            self.mounted_mutation_handoff.as_deref(),
            &self.dfs,
            &path,
            &object_id,
            command,
        );
        // Closing an fd must stop renewal even when the cluster is unavailable;
        // the server-side lease then guarantees bounded cleanup.
        let close_result = match result {
            Ok(FuseLockCommandResult {
                outcome: dfs::DfsAdvisoryLockOutcome::Closed { .. },
                ..
            }) => Ok(()),
            Ok(response) if lock_outcome_loses_grant(&response.outcome) => Ok(()),
            Ok(response) => Err(errno_for_unexpected_lock_outcome(&response.outcome)),
            Err(errno) => Err(errno),
        };
        let cleanup_result = pending_cleanup_result.and(close_result);
        if was_lost {
            Err(libc::EIO)
        } else {
            cleanup_result
        }
    }

    fn close_advisory_locks_for_release(
        &self,
        fh: u64,
        lock_owner: Option<u64>,
    ) -> std::result::Result<(), libc::c_int> {
        let mut state = self.lock_state()?;
        state.expire_elapsed_locks(Instant::now());
        let mut lock_owners = state.lock_owners_for_handle(fh);
        drop(state);
        lock_owners.extend(lock_owner);
        let mut first_error = None;
        for lock_owner in lock_owners {
            if let Err(errno) = self.close_advisory_locks_for_handle(fh, lock_owner)
                && first_error.is_none()
            {
                first_error = Some(errno);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn plan_read_handle_or_path(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        lock_owner: Option<u64>,
    ) -> std::result::Result<FuseReadPlan, libc::c_int> {
        self.ensure_io_lock_not_lost(ino, fh, lock_owner)?;
        if offset < 0 {
            return Err(libc::EINVAL);
        }
        let start = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let opened_read = {
            let state = self.lock_state()?;
            state.opened_read(fh, start, size as usize)
        };
        self.ensure_handle_lock_budget(fh, lock_owner, LOCKED_IO_MIN_LEASE)?;
        // SIFS content versions currently also order metadata-only atime
        // updates. Advancing that version after a read leaves the new version
        // without matching immutable content/receipts, which breaks later
        // range reads and fsync validation. Keep FUSE reads noatime until DFS
        // has a metadata revision independent from the content version.
        Ok(match opened_read {
            Some(read) => FuseReadPlan::Opened(read),
            None => FuseReadPlan::Path {
                path: self.path_for_ino(ino)?,
                offset: start,
                max_bytes: size as usize,
            },
        })
    }

    fn write_handle(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        lock_owner: Option<u64>,
    ) -> std::result::Result<u32, libc::c_int> {
        self.ensure_writable()?;
        if offset < 0 {
            return Err(libc::EINVAL);
        }
        let start = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let written = u32::try_from(data.len()).map_err(|_| libc::EFBIG)?;
        let (object_id, sync_mode) = {
            let state = self.lock_state()?;
            let handle = state.handles.get(&fh).ok_or(libc::EBADF)?;
            if !handle.writable {
                return Err(libc::EBADF);
            }
            (handle.object_id.clone(), handle.sync_mode)
        };
        self.ensure_io_lock_not_lost(ino, fh, lock_owner)?;
        self.ensure_handle_lock_budget(fh, lock_owner, LOCKED_IO_MIN_LEASE)?;
        if data.is_empty() {
            return Ok(0);
        }

        let gate = self.inode_publication_gate(fh)?;
        let _guard = dfs::MountedSifsCall::lock(&gate).map_err(|_| libc::EIO)?;
        if sync_mode != HandleSyncMode::None {
            self.commit_inode_ranges(fh)?;
            self.publish_immediate_handle_write(
                fh, &object_id, start, data, sync_mode, lock_owner,
            )?;
            return Ok(written);
        }
        loop {
            let outcome = self.lock_state()?.buffer_handle_write(fh, start, data)?;
            match outcome {
                DirtyWriteInsert::Buffered => return Ok(written),
                DirtyWriteInsert::NeedsPublication => {
                    self.commit_inode_ranges(fh)?;
                }
                DirtyWriteInsert::PublishDirectly => {
                    self.commit_inode_ranges(fh)?;
                    self.publish_immediate_handle_write(
                        fh,
                        &object_id,
                        start,
                        data,
                        HandleSyncMode::None,
                        lock_owner,
                    )?;
                    return Ok(written);
                }
            }
        }
    }

    fn publish_immediate_handle_write(
        &self,
        fh: u64,
        object_id: &dfs::DfsObjectId,
        offset: u64,
        data: &[u8],
        sync_mode: HandleSyncMode,
        lock_owner: Option<u64>,
    ) -> std::result::Result<(), libc::c_int> {
        // The state snapshot above carries only stable identity. Do not hold
        // FuseState while content upload, metadata publication, or a sync
        // barrier waits on disk/network.
        let write_options = self.write_options();
        let stat = self.run_mounted_mutation(
            FuseMountedMutationOperation::ImmediateWritePublication,
            |dfs| {
                dfs.posix_write_file_at_for_object(object_id, offset, data, write_options)
                    .map_err(FuseMountedMutationError::from)
            },
        )?;
        self.refresh_opened_object_stats(object_id, &stat)?;
        if let Some(kind) = sync_mode.kind() {
            self.ensure_handle_lock_budget(fh, lock_owner, LOCKED_IO_MIN_LEASE)?;
            let target = self.current_file_sync_target_for_handle(fh, kind)?;
            self.run_mounted_mutation(FuseMountedMutationOperation::FileSync, |dfs| {
                dfs.posix_sync_file_target(&target)
                    .map(drop)
                    .map_err(FuseMountedMutationError::from)
            })?;
        }
        Ok(())
    }

    fn truncate_handle_or_path(
        &self,
        ino: u64,
        fh: Option<u64>,
        size: u64,
    ) -> std::result::Result<(), libc::c_int> {
        self.ensure_writable()?;
        let handle = if let Some(fh) = fh {
            let state = self.lock_state()?;
            Some(
                state
                    .handles
                    .get(&fh)
                    .map(|handle| {
                        (
                            handle.path.clone(),
                            handle.object_id.clone(),
                            handle.sync_mode,
                        )
                    })
                    .ok_or(libc::EBADF)?,
            )
        } else {
            None
        };
        let (path, object_id, sync_mode) = match handle {
            Some((path, object_id, sync_mode)) => (path, Some(object_id), sync_mode),
            None => (self.path_for_ino(ino)?, None, HandleSyncMode::None),
        };
        if let Some(object_id) = object_id.as_ref() {
            // Preserve object-wide write/truncate program order: buffered
            // writes from every descriptor name pre-truncate offsets.
            self.commit_dirty_handles_for_object(object_id, None)?;
        } else {
            self.commit_dirty_handles_for_path(&path, false)?;
        }
        if let Some(fh) = fh {
            self.ensure_handle_lock_budget(fh, None, LOCKED_IO_MIN_LEASE)?;
        }
        let write_options = self.write_options();
        let operation = if object_id.is_some() {
            FuseMountedMutationOperation::TruncateObject
        } else {
            FuseMountedMutationOperation::TruncatePath
        };
        let stat = self.run_mounted_mutation(operation, |dfs| {
            if let Some(object_id) = object_id.as_ref() {
                dfs.posix_truncate_file_for_object(object_id, size, write_options)
            } else {
                dfs.posix_truncate_file(&path, size, write_options)
            }
            .map_err(FuseMountedMutationError::from)
        })?;
        self.refresh_opened_object_stats(&stat.object_id, &stat)?;
        if let Some(kind) = sync_mode.kind() {
            if let Some(fh) = fh {
                self.ensure_handle_lock_budget(fh, None, LOCKED_IO_MIN_LEASE)?;
            }
            let target = fh
                .map(|fh| self.current_file_sync_target_for_handle(fh, kind))
                .transpose()?;
            let target = target.as_ref().ok_or(libc::EIO)?;
            self.run_mounted_mutation(FuseMountedMutationOperation::FileSync, |dfs| {
                dfs.posix_sync_file_target(target)
                    .map(drop)
                    .map_err(FuseMountedMutationError::from)
            })?;
        }
        Ok(())
    }
}

impl Filesystem for SifsFuseFileSystem {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> io::Result<()> {
        config
            .add_capabilities(InitFlags::FUSE_POSIX_LOCKS)
            .map_err(|_| io::Error::from_raw_os_error(libc::EIO))
    }

    fn destroy(&mut self) {
        self.lock_renewer_stop.store(true, Ordering::Release);
        self.read_cancellation.cancel();
        drop(self.mounted_mutation_handoff.take());
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        let parent = parent.0;
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(errno) => return reply.error_from_i32(errno),
        };
        match self.stat_path(&path, false) {
            Ok(stat) => self.reply_entry_for_path(reply, path, stat),
            Err(errno) => reply.error_from_i32(errno),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, fh: Option<FileHandle>, reply: ReplyAttr) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        let ino = ino.0;
        let fh = fh.map(|fh| fh.0);
        if let Some(fh) = fh {
            match self.lock_state() {
                Ok(state) => {
                    if let Some(stat) = state.visible_handle_stat(fh) {
                        return reply
                            .attr(&self.attr_ttl, &file_attr(ino, &stat, self.presented_owner));
                    }
                }
                Err(errno) => return reply.error_from_i32(errno),
            }
        }
        let path = match self.path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => return reply.error_from_i32(errno),
        };
        match self.stat_path(&path, false) {
            Ok(stat) => reply.attr(&self.attr_ttl, &file_attr(ino, &stat, self.presented_owner)),
            Err(errno) => reply.error_from_i32(errno),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        let ino = ino.0;
        let fh = fh.map(|fh| fh.0);
        let changes_metadata =
            mode.is_some() || uid.is_some() || gid.is_some() || atime.is_some() || mtime.is_some();
        if size.is_none() && changes_metadata {
            let commit_result = match fh {
                Some(fh) => self.commit_dirty_handles_for_handle_object(fh).map(drop),
                None => self
                    .path_for_ino(ino)
                    .and_then(|path| self.commit_dirty_handles_for_path(&path, false)),
            };
            if let Err(errno) = commit_result {
                return reply.error_from_i32(errno);
            }
        }
        if let Some(size) = size
            && let Err(errno) = self.truncate_handle_or_path(ino, fh, size)
        {
            return reply.error_from_i32(errno);
        }
        let path = match self.path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => return reply.error_from_i32(errno),
        };
        let mut result = Ok(());
        if let Some(mode) = mode {
            result = result.and_then(|_| {
                self.ensure_writable()?;
                self.run_mounted_mutation(FuseMountedMutationOperation::SetMode, |dfs| {
                    dfs.posix_set_mode(&path, mode)
                        .map(|_| ())
                        .map_err(FuseMountedMutationError::from)
                })
            });
        }
        if uid.is_some() || gid.is_some() {
            result = result.and_then(|_| {
                self.ensure_writable()?;
                let current = self.stat_path(&path, false)?;
                let uid = uid.unwrap_or(current.uid);
                let gid = gid.unwrap_or(current.gid);
                let operation = if current.kind == DfsObjectKind::Symlink {
                    FuseMountedMutationOperation::SetSymlinkOwner
                } else {
                    FuseMountedMutationOperation::SetOwner
                };
                self.run_mounted_mutation(operation, |dfs| {
                    let updated = if current.kind == DfsObjectKind::Symlink {
                        dfs.posix_set_symlink_owner(&path, uid, gid)
                    } else {
                        dfs.posix_set_owner(&path, uid, gid)
                    };
                    updated.map(|_| ()).map_err(FuseMountedMutationError::from)
                })
            });
        }
        if atime.is_some() || mtime.is_some() {
            result = result.and_then(|_| {
                self.ensure_writable()?;
                self.run_mounted_mutation(FuseMountedMutationOperation::SetTimes, |dfs| {
                    dfs.posix_set_times(
                        &path,
                        atime.map(time_or_now_to_nanos),
                        mtime.map(time_or_now_to_nanos),
                    )
                    .map(|_| ())
                    .map_err(FuseMountedMutationError::from)
                })
            });
        }
        if let Err(errno) = result {
            return reply.error_from_i32(errno);
        }
        match self.stat_path(&path, false) {
            Ok(stat) => reply.attr(&self.attr_ttl, &file_attr(ino, &stat, self.presented_owner)),
            Err(errno) => reply.error_from_i32(errno),
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        let ino = ino.0;
        let path = match self.path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => return reply.error_from_i32(errno),
        };
        let result = self.lock_dfs().and_then(|mut dfs| {
            dfs.posix_readlink_distributed(&path)
                .map_err(errno_for_posix_error)
        });
        match result {
            Ok(target) => reply.data(target.as_bytes()),
            Err(errno) => reply.error_from_i32(errno),
        }
    }

    fn mknod(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        let parent = parent.0;
        if let Err(errno) = self.ensure_writable() {
            return reply.error_from_i32(errno);
        }
        let file_type = mode & libc::S_IFMT;
        if file_type != 0 && file_type != libc::S_IFREG {
            return reply.error_from_i32(libc::ENOSYS);
        }
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(errno) => return reply.error_from_i32(errno),
        };
        let attrs = DfsPosixCreateAttrs::new(created_mode(mode, umask), req.uid(), req.gid());
        let write_options = self.write_options();
        let result = self.run_mounted_mutation(FuseMountedMutationOperation::Mknod, |dfs| {
            dfs.posix_create_file_with_attrs(&path, write_options, attrs)
                .map_err(FuseMountedMutationError::from)
        });
        match result {
            Ok(stat) => self.reply_entry_for_path(reply, path, stat),
            Err(errno) => reply.error_from_i32(errno),
        }
    }

    fn mkdir(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        let parent = parent.0;
        if let Err(errno) = self.ensure_writable() {
            return reply.error_from_i32(errno);
        }
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(errno) => return reply.error_from_i32(errno),
        };
        let attrs = DfsPosixCreateAttrs::new(created_mode(mode, umask), req.uid(), req.gid());
        let result = self.run_mounted_mutation(FuseMountedMutationOperation::Mkdir, |dfs| {
            dfs.posix_mkdir_with_create_attrs(&path, attrs)
                .map_err(FuseMountedMutationError::from)
        });
        match result {
            Ok(stat) => self.reply_entry_for_path(reply, path, stat),
            Err(errno) => reply.error_from_i32(errno),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        let parent = parent.0;
        if let Err(errno) = self.ensure_writable() {
            return reply.error_from_i32(errno);
        }
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(errno) => return reply.error_from_i32(errno),
        };
        if let Err(errno) = self.commit_dirty_handles_for_path(&path, false) {
            return reply.error_from_i32(errno);
        }
        let result = self.run_mounted_mutation(FuseMountedMutationOperation::Unlink, |dfs| {
            dfs.posix_unlink(&path)
                .map_err(FuseMountedMutationError::from)
        });
        match result {
            Ok(_) => {
                if let Ok(mut state) = self.lock_state() {
                    state.remove_path(&path);
                }
                reply.ok();
            }
            Err(errno) => reply.error_from_i32(errno),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        let parent = parent.0;
        if let Err(errno) = self.ensure_writable() {
            return reply.error_from_i32(errno);
        }
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(errno) => return reply.error_from_i32(errno),
        };
        let result = self.run_mounted_mutation(FuseMountedMutationOperation::Rmdir, |dfs| {
            dfs.posix_rmdir(&path)
                .map_err(FuseMountedMutationError::from)
        });
        match result {
            Ok(_) => {
                if let Ok(mut state) = self.lock_state() {
                    state.remove_path(&path);
                }
                reply.ok();
            }
            Err(errno) => reply.error_from_i32(errno),
        }
    }

    fn symlink(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        link: &std::path::Path,
        reply: ReplyEntry,
    ) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        let parent = parent.0;
        if let Err(errno) = self.ensure_writable() {
            return reply.error_from_i32(errno);
        }
        let Some(target) = link.to_str() else {
            return reply.error_from_i32(libc::EINVAL);
        };
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(errno) => return reply.error_from_i32(errno),
        };
        let attrs = DfsPosixCreateAttrs::new(0o777, req.uid(), req.gid());
        let write_options = self.write_options();
        let result = self.run_mounted_mutation(FuseMountedMutationOperation::Symlink, |dfs| {
            dfs.posix_symlink_with_create_attrs(target, &path, write_options, attrs)
                .map_err(FuseMountedMutationError::from)
        });
        match result {
            Ok(stat) => self.reply_entry_for_path(reply, path, stat),
            Err(errno) => reply.error_from_i32(errno),
        }
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        if !flags.is_empty() {
            return reply.error_from_i32(libc::ENOSYS);
        }
        let parent = parent.0;
        let newparent = newparent.0;
        if let Err(errno) = self.ensure_writable() {
            return reply.error_from_i32(errno);
        }
        let from = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(errno) => return reply.error_from_i32(errno),
        };
        let to = match self.child_path(newparent, newname) {
            Ok(path) => path,
            Err(errno) => return reply.error_from_i32(errno),
        };
        // Publish both sides before namespace replacement. Source descriptors
        // then carry the latest stable object version through the rename, and
        // an open destination cannot lose acknowledged data when replaced.
        if let Err(errno) = self
            .commit_dirty_handles_for_path(&from, true)
            .and_then(|_| self.commit_dirty_handles_for_path(&to, true))
        {
            return reply.error_from_i32(errno);
        }
        let result = self.run_mounted_mutation(FuseMountedMutationOperation::Rename, |dfs| {
            dfs.posix_rename(&from, &to)
                .map_err(FuseMountedMutationError::from)
        });
        match result {
            Ok(_) => {
                if let Ok(mut state) = self.lock_state() {
                    state.rename_path(&from, to);
                }
                reply.ok();
            }
            Err(errno) => reply.error_from_i32(errno),
        }
    }

    fn link(
        &self,
        _req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        let ino = ino.0;
        let newparent = newparent.0;
        if let Err(errno) = self.ensure_writable() {
            return reply.error_from_i32(errno);
        }
        let existing = match self.path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => return reply.error_from_i32(errno),
        };
        let link_path = match self.child_path(newparent, newname) {
            Ok(path) => path,
            Err(errno) => return reply.error_from_i32(errno),
        };
        let result = self.run_mounted_mutation(FuseMountedMutationOperation::Hardlink, |dfs| {
            dfs.posix_hardlink(&existing, &link_path)
                .map_err(FuseMountedMutationError::from)
        });
        match result {
            Ok(stat) => match self.lock_state() {
                Ok(mut state) => match state.intern_alias_path(ino, link_path, &stat.object_id) {
                    Ok(link_ino) => reply.entry(
                        &self.entry_ttl,
                        &file_attr(link_ino, &stat, self.presented_owner),
                        Generation(0),
                    ),
                    Err(errno) => reply.error_from_i32(errno),
                },
                Err(errno) => reply.error_from_i32(errno),
            },
            Err(errno) => reply.error_from_i32(errno),
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        let ino = ino.0;
        let flags = flags.0;
        let writable = flags_has_write_access(flags);
        if writable && let Err(errno) = self.ensure_writable() {
            return reply.error_from_i32(errno);
        }
        let path = match self.path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => return reply.error_from_i32(errno),
        };
        match self.stat_path(&path, true) {
            Ok(stat) if stat.kind == DfsObjectKind::Directory => reply.error_from_i32(libc::EISDIR),
            Ok(stat) => match self.lock_state() {
                Ok(mut state) => {
                    let fh = state.allocate_handle(path, stat, flags);
                    reply.opened(FileHandle(fh), FopenFlags::empty());
                }
                Err(errno) => reply.error_from_i32(errno),
            },
            Err(errno) => reply.error_from_i32(errno),
        }
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        let parent = parent.0;
        if let Err(errno) = self.ensure_writable() {
            return reply.error_from_i32(errno);
        }
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(errno) => return reply.error_from_i32(errno),
        };
        let attrs = DfsPosixCreateAttrs::new(created_mode(mode, umask), req.uid(), req.gid());
        let write_options = self.write_options();
        let result = self.run_mounted_mutation(FuseMountedMutationOperation::Create, |dfs| {
            create_file_for_fuse(dfs, &path, write_options, attrs, flags)
        });
        match result {
            Ok(stat) => match self.lock_state() {
                Ok(mut state) => {
                    let ino = state.intern_entry(path.clone(), stat.object_id.clone());
                    let fh = state.allocate_handle(path, stat.clone(), flags);
                    reply.created(
                        &self.entry_ttl,
                        &file_attr(ino, &stat, self.presented_owner),
                        Generation(0),
                        FileHandle(fh),
                        FopenFlags::empty(),
                    );
                }
                Err(errno) => reply.error_from_i32(errno),
            },
            Err(errno) => reply.error_from_i32(errno),
        }
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        let ino = ino.0;
        let fh = fh.0;
        let offset = match i64::try_from(offset) {
            Ok(offset) => offset,
            Err(_) => return reply.error_from_i32(libc::EOVERFLOW),
        };
        let lock_owner = lock_owner.map(|owner| owner.0);
        let plan = match self.plan_read_handle_or_path(ino, fh, offset, size, lock_owner) {
            Ok(plan) => plan,
            Err(errno) => return reply.error_from_i32(errno),
        };
        match plan {
            FuseReadPlan::Opened(read) => {
                let content_reads = self.content_reads.clone();
                let deadline = dfs::MountedSifsCall::deadline();
                let job: FuseReadJob = Box::new(move || {
                    let _call = dfs::MountedSifsCall::enter(deadline);
                    if deadline.is_some_and(|at| Instant::now() >= at) {
                        return reply.error_from_i32(libc::EIO);
                    }
                    let base_request_len = read.base_request_len();
                    let base = if base_request_len == 0 {
                        Ok(Vec::new())
                    } else {
                        content_reads.read_opened_file_range(
                            &read.identity.path,
                            &read.identity.opened_stat,
                            read.offset,
                            base_request_len,
                        )
                    };
                    match base {
                        Ok(base) => match read.finish(base) {
                            Ok(bytes) => reply.data(&bytes),
                            Err(errno) => reply.error_from_i32(errno),
                        },
                        Err(error) => reply.error_from_i32(errno_for_posix_error(error)),
                    }
                });
                if let Err(job) = self.read_executor.submit(job) {
                    // An expired queued read reports EIO before touching DFS.
                    // A disconnected queue also leaves this callback owning the reply.
                    job();
                }
            }
            FuseReadPlan::Path {
                path,
                offset,
                max_bytes,
            } => {
                let result = self.lock_dfs().and_then(|mut dfs| {
                    dfs.posix_read_file_range_distributed(&path, offset, max_bytes)
                        .map_err(errno_for_posix_error)
                });
                match result {
                    Ok(bytes) => reply.data(&bytes),
                    Err(errno) => reply.error_from_i32(errno),
                }
            }
        }
    }

    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        let ino = ino.0;
        let fh = fh.0;
        let offset = match i64::try_from(offset) {
            Ok(offset) => offset,
            Err(_) => return reply.error_from_i32(libc::EOVERFLOW),
        };
        let lock_owner = lock_owner.map(|owner| owner.0);
        match self.write_handle(ino, fh, offset, data, lock_owner) {
            Ok(written) => reply.written(written),
            Err(errno) => reply.error_from_i32(errno),
        }
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        let fh = fh.0;
        let lock_owner = lock_owner.0;
        let commit_result = self.commit_handle(fh);
        let close_result = self.close_advisory_locks_for_handle(fh, lock_owner);
        let result = commit_result.and(close_result);
        match result {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error_from_i32(errno),
        }
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        let fh = fh.0;
        let lock_owner = lock_owner.map(|owner| owner.0);
        let commit_result = self.commit_handle(fh);
        let commit_succeeded = commit_result.is_ok();
        let close_result = self.close_advisory_locks_for_release(fh, lock_owner);
        let result = commit_result.and(close_result);
        if commit_succeeded && let Ok(mut state) = self.lock_state() {
            state.handles.remove(&fh);
        }
        match result {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error_from_i32(errno),
        }
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        let fh = fh.0;
        match self.sync_handle(fh, datasync) {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error_from_i32(errno),
        }
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        let ino = ino.0;
        match self.path_for_ino(ino).and_then(|path| {
            self.stat_path(&path, true).and_then(|stat| {
                if stat.kind == DfsObjectKind::Directory {
                    Ok(())
                } else {
                    Err(libc::ENOTDIR)
                }
            })
        }) {
            Ok(()) => reply.opened(FileHandle(0), FopenFlags::empty()),
            Err(errno) => reply.error_from_i32(errno),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        let ino = ino.0;
        let path = match self.path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => return reply.error_from_i32(errno),
        };
        let children = match self
            .run_mounted_mutation(FuseMountedMutationOperation::DirectoryRefresh, |dfs| {
                Ok(dfs.posix_list_dir_distributed(&path))
            }) {
            Ok(Ok(children)) => children,
            Ok(Err(error)) => return reply.error_from_i32(errno_for_posix_error(error)),
            Err(errno) => return reply.error_from_i32(errno),
        };
        let start = match usize::try_from(offset) {
            Ok(start) => start,
            Err(_) => return reply.error_from_i32(libc::EOVERFLOW),
        };
        let parent_ino = match self.lock_state() {
            Ok(mut state) => state.intern_path(parent_sifs_path(&path)),
            Err(errno) => return reply.error_from_i32(errno),
        };
        let mut entries = vec![
            (ino, FileType::Directory, ".".to_string()),
            (parent_ino, FileType::Directory, "..".to_string()),
        ];
        for child in children {
            let child_ino = match self.intern_entry(child.path.clone(), &child.object_id) {
                Ok(child_ino) => child_ino,
                Err(errno) => return reply.error_from_i32(errno),
            };
            entries.push((child_ino, file_type_for_kind(&child.kind), child.name));
        }
        for (index, (entry_ino, kind, name)) in entries.into_iter().enumerate().skip(start) {
            if reply.add(INodeNo(entry_ino), (index + 1) as u64, kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        reply.ok();
    }

    fn fsyncdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        let ino = ino.0;
        let result = self.path_for_ino(ino).and_then(|path| {
            self.run_mounted_mutation(FuseMountedMutationOperation::DirectorySync, |dfs| {
                dfs.posix_sync_directory(&path)
                    .map(drop)
                    .map_err(FuseMountedMutationError::from)
            })
        });
        match result {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error_from_i32(errno),
        }
    }

    fn getlk(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        lock_owner: LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        _pid: u32,
        reply: ReplyLock,
    ) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        let ino = ino.0;
        let fh = fh.0;
        let lock_owner = lock_owner.0;
        if !is_whole_file_lock_range(start, end) {
            return reply.error_from_i32(libc::EOPNOTSUPP);
        }
        let mode = match advisory_lock_mode(typ) {
            Ok(mode) => mode,
            Err(errno) => return reply.error_from_i32(errno),
        };
        match self.query_advisory_lock(ino, fh, lock_owner, mode) {
            Ok(Some(conflict)) => reply.locked(
                0,
                end,
                advisory_lock_type(conflict.mode),
                conflict.process_id.unwrap_or(0),
            ),
            Ok(None) => reply.locked(0, end, libc::F_UNLCK, 0),
            Err(errno) => reply.error_from_i32(errno),
        }
    }

    fn setlk(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        lock_owner: LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
        reply: ReplyEmpty,
    ) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        let ino = ino.0;
        let fh = fh.0;
        let lock_owner = lock_owner.0;
        if !is_whole_file_lock_range(start, end) {
            return reply.error_from_i32(libc::EOPNOTSUPP);
        }
        if typ == libc::F_UNLCK {
            return match self.unlock_advisory_lock(ino, fh, lock_owner) {
                Ok(()) => reply.ok(),
                Err(errno) => reply.error_from_i32(errno),
            };
        }
        let mode = match advisory_lock_mode(typ) {
            Ok(mode) => mode,
            Err(errno) => return reply.error_from_i32(errno),
        };
        self.set_advisory_lock(ino, fh, lock_owner, mode, pid, sleep, reply);
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        reply.statfs(
            1024 * 1024 * 1024,
            1024 * 1024 * 1024,
            1024 * 1024 * 1024,
            1024 * 1024 * 1024,
            1024 * 1024 * 1024,
            DEFAULT_BLOCK_SIZE,
            255,
            DEFAULT_BLOCK_SIZE,
        );
    }

    fn access(&self, _req: &Request, _ino: INodeNo, mask: AccessFlags, reply: ReplyEmpty) {
        let _call = dfs::MountedSifsCall::enter(self.operation_deadline());
        if mask.contains(AccessFlags::W_OK)
            && let Err(errno) = self.ensure_writable()
        {
            return reply.error_from_i32(errno);
        }
        reply.ok();
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FuseOpenedReadIdentity {
    path: String,
    opened_stat: dfs::DfsStat,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FuseOpenedRead {
    identity: FuseOpenedReadIdentity,
    offset: u64,
    max_bytes: usize,
    logical_size: u64,
    dirty_overlays: Vec<DirtyWriteRange>,
}

impl FuseOpenedRead {
    fn requested_len(&self) -> usize {
        if self.offset >= self.logical_size {
            return 0;
        }
        usize::try_from(self.logical_size - self.offset)
            .unwrap_or(usize::MAX)
            .min(self.max_bytes)
    }

    fn base_request_len(&self) -> usize {
        let committed_size = self.identity.opened_stat.size_bytes.unwrap_or(0);
        if self.offset >= committed_size {
            return 0;
        }
        usize::try_from(committed_size - self.offset)
            .unwrap_or(usize::MAX)
            .min(self.requested_len())
    }

    fn finish(self, base: Vec<u8>) -> std::result::Result<Vec<u8>, libc::c_int> {
        let requested_len = self.requested_len();
        let base_request_len = self.base_request_len();
        if base.len() < base_request_len {
            // The opened stat names an exact immutable version and size. A
            // short read inside that committed extent is corruption or a
            // stale peer response, not a sparse hole.
            return Err(libc::EIO);
        }
        let mut bytes = vec![0; requested_len];
        bytes[..base_request_len].copy_from_slice(&base[..base_request_len]);
        let request_end = self.offset.saturating_add(requested_len as u64);
        for overlay in self.dirty_overlays {
            let overlay_end = overlay.offset.saturating_add(overlay.bytes.len() as u64);
            let copy_start = overlay.offset.max(self.offset);
            let copy_end = overlay_end.min(request_end);
            if copy_start >= copy_end {
                continue;
            }
            let source_start = usize::try_from(copy_start - overlay.offset)
                .expect("dirty read source offset fits in the resident buffer");
            let target_start = usize::try_from(copy_start - self.offset)
                .expect("dirty read target offset is bounded by max_bytes");
            let len = usize::try_from(copy_end - copy_start)
                .expect("dirty read length is bounded by max_bytes");
            bytes[target_start..target_start + len]
                .copy_from_slice(&overlay.bytes[source_start..source_start + len]);
        }
        Ok(bytes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum FuseReadPlan {
    Opened(FuseOpenedRead),
    Path {
        path: String,
        offset: u64,
        max_bytes: usize,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DirtyWriteRange {
    offset: u64,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DirtyWriteBuffer {
    ranges: BTreeMap<u64, Vec<u8>>,
    buffered_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DirtyWriteInsert {
    Buffered,
    NeedsPublication,
    PublishDirectly,
}

impl DirtyWriteBuffer {
    fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.ranges.len()
    }

    #[cfg(test)]
    fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    fn try_write(
        &mut self,
        offset: u64,
        bytes: &[u8],
        limit: usize,
    ) -> std::result::Result<DirtyWriteInsert, libc::c_int> {
        if bytes.is_empty() {
            return Ok(DirtyWriteInsert::Buffered);
        }
        let end = offset
            .checked_add(u64::try_from(bytes.len()).map_err(|_| libc::EFBIG)?)
            .ok_or(libc::EFBIG)?;
        if bytes.len() > limit {
            return Ok(if self.is_empty() {
                DirtyWriteInsert::PublishDirectly
            } else {
                DirtyWriteInsert::NeedsPublication
            });
        }

        let prior = self
            .ranges
            .range(..=offset)
            .next_back()
            .map(|(&range_start, range_bytes)| (range_start, range_bytes.len()));
        if let Some((prior_start, prior_len)) = prior {
            let prior_end = prior_start.saturating_add(prior_len as u64);
            if end <= prior_end {
                // A rewrite fully contained in one resident range can update
                // in place. This is also important for page-cache retry
                // patterns that repeatedly dirty the same kernel chunk.
                let target = usize::try_from(offset - prior_start)
                    .expect("contained dirty write offset fits in resident range");
                self.ranges
                    .get_mut(&prior_start)
                    .expect("contained dirty range remains present")[target..target + bytes.len()]
                    .copy_from_slice(bytes);
                return Ok(DirtyWriteInsert::Buffered);
            }
            let next_start = self.ranges.range(offset..).next().map(|(&start, _)| start);
            if prior_end == offset && next_start.is_none_or(|next| next > end) {
                let projected_bytes = self
                    .buffered_bytes
                    .checked_add(bytes.len())
                    .ok_or(libc::EFBIG)?;
                if projected_bytes > limit {
                    return Ok(DirtyWriteInsert::NeedsPublication);
                }
                // This is the dominant kernel pattern. Vec's spare capacity
                // makes sequential coalescing amortized O(total bytes) rather
                // than reallocating and copying the whole file per chunk.
                self.ranges
                    .get_mut(&prior_start)
                    .expect("append dirty range remains present")
                    .extend_from_slice(bytes);
                self.buffered_bytes = projected_bytes;
                return Ok(DirtyWriteInsert::Buffered);
            }
        }

        let mut merged_start = offset;
        let mut merged_end = end;
        if let Some((&prior_start, prior_bytes)) = self.ranges.range(..=offset).next_back() {
            let prior_end = prior_start.saturating_add(prior_bytes.len() as u64);
            if prior_end >= offset {
                merged_start = prior_start;
                merged_end = merged_end.max(prior_end);
            }
        }

        let mut merged_keys = Vec::new();
        let mut replaced_bytes = 0usize;
        for (&range_start, range_bytes) in self.ranges.range(merged_start..) {
            if range_start > merged_end {
                break;
            }
            let range_end = range_start.saturating_add(range_bytes.len() as u64);
            if range_end < merged_start {
                continue;
            }
            merged_end = merged_end.max(range_end);
            merged_keys.push(range_start);
            replaced_bytes = replaced_bytes.saturating_add(range_bytes.len());
        }
        let merged_len = usize::try_from(merged_end - merged_start).map_err(|_| libc::EFBIG)?;
        let projected_bytes = self
            .buffered_bytes
            .checked_sub(replaced_bytes)
            .and_then(|retained| retained.checked_add(merged_len))
            .ok_or(libc::EFBIG)?;
        if projected_bytes > limit {
            return Ok(DirtyWriteInsert::NeedsPublication);
        }

        let mut merged = vec![0; merged_len];
        for range_start in &merged_keys {
            let range_bytes = self
                .ranges
                .get(range_start)
                .expect("planned dirty range remains present");
            let target = usize::try_from(*range_start - merged_start)
                .expect("dirty range offset fits in merged buffer");
            merged[target..target + range_bytes.len()].copy_from_slice(range_bytes);
        }
        let incoming_target = usize::try_from(offset - merged_start)
            .expect("incoming dirty range offset fits in merged buffer");
        merged[incoming_target..incoming_target + bytes.len()].copy_from_slice(bytes);
        for range_start in merged_keys {
            self.ranges.remove(&range_start);
        }
        self.ranges.insert(merged_start, merged);
        self.buffered_bytes = projected_bytes;
        Ok(DirtyWriteInsert::Buffered)
    }

    fn overlays_for_read(&self, offset: u64, max_bytes: usize) -> Vec<DirtyWriteRange> {
        let request_end = offset.saturating_add(max_bytes as u64);
        self.ranges
            .iter()
            .filter_map(|(&range_start, range_bytes)| {
                let range_end = range_start.saturating_add(range_bytes.len() as u64);
                let copy_start = range_start.max(offset);
                let copy_end = range_end.min(request_end);
                if copy_start >= copy_end {
                    return None;
                }
                let source_start = usize::try_from(copy_start - range_start).ok()?;
                let len = usize::try_from(copy_end - copy_start).ok()?;
                Some(DirtyWriteRange {
                    offset: copy_start,
                    bytes: range_bytes[source_start..source_start + len].to_vec(),
                })
            })
            .collect()
    }


}

#[derive(Debug)]
struct FuseDirtyPublication {
    base: dfs::DfsStat,
    operation_id: String,
    writers: BTreeSet<u64>,
    dirty: DirtyWriteBuffer,
}

#[derive(Debug, Default)]
struct DirtyInode {
    base: Option<dfs::DfsStat>,
    active: DirtyWriteBuffer,
    writers: BTreeSet<u64>,
    frozen: Option<Arc<FuseDirtyPublication>>,
}

#[derive(Debug)]
struct FuseState {
    next_ino: u64,
    next_fh: u64,
    paths_by_ino: BTreeMap<u64, BTreeSet<String>>,
    preferred_path_by_ino: BTreeMap<u64, String>,
    ino_by_path: BTreeMap<String, u64>,
    ino_by_object_id: BTreeMap<dfs::DfsObjectId, u64>,
    object_id_by_ino: BTreeMap<u64, dfs::DfsObjectId>,
    handles: BTreeMap<u64, OpenFileHandle>,
    dirty_inodes: BTreeMap<dfs::DfsObjectId, DirtyInode>,
    held_locks: BTreeMap<(dfs::DfsObjectId, u64), FuseHeldLock>,
    pending_locks: BTreeMap<(dfs::DfsObjectId, u64), FusePendingLock>,
    lost_locks: BTreeMap<(dfs::DfsObjectId, u64), FuseLostLock>,
}

impl FuseState {
    fn new(root_path: String) -> Self {
        let mut paths_by_ino = BTreeMap::new();
        let mut preferred_path_by_ino = BTreeMap::new();
        let mut ino_by_path = BTreeMap::new();
        paths_by_ino.insert(ROOT_INO, BTreeSet::from([root_path.clone()]));
        preferred_path_by_ino.insert(ROOT_INO, root_path.clone());
        ino_by_path.insert(root_path, ROOT_INO);
        Self {
            next_ino: ROOT_INO + 1,
            next_fh: 1,
            paths_by_ino,
            preferred_path_by_ino,
            ino_by_path,
            ino_by_object_id: BTreeMap::new(),
            object_id_by_ino: BTreeMap::new(),
            handles: BTreeMap::new(),
            dirty_inodes: BTreeMap::new(),
            held_locks: BTreeMap::new(),
            pending_locks: BTreeMap::new(),
            lost_locks: BTreeMap::new(),
        }
    }

    fn path_for(&self, ino: u64) -> Option<&str> {
        let paths = self.paths_by_ino.get(&ino)?;
        self.preferred_path_by_ino
            .get(&ino)
            .filter(|path| paths.contains(path.as_str()))
            .or_else(|| paths.first())
            .map(String::as_str)
    }

    #[cfg(test)]
    fn opened_read_identity(&self, fh: u64) -> Option<FuseOpenedReadIdentity> {
        self.handles.get(&fh).map(|handle| FuseOpenedReadIdentity {
            path: handle.path.clone(),
            opened_stat: handle.opened_stat.clone(),
        })
    }

    fn opened_read(&self, fh: u64, offset: u64, max_bytes: usize) -> Option<FuseOpenedRead> {
        let handle = self.handles.get(&fh)?;
        let object_id = &handle.object_id;
        let logical_size = self.dirty_size(object_id).max(handle.logical_size);
        let mut dirty_overlays = Vec::new();
        if let Some(inode) = self.dirty_inodes.get(object_id) {
            if let Some(frozen) = &inode.frozen {
                dirty_overlays.extend(frozen.dirty.overlays_for_read(offset, max_bytes));
            }
            dirty_overlays.extend(inode.active.overlays_for_read(offset, max_bytes));
        }
        Some(FuseOpenedRead {
            identity: FuseOpenedReadIdentity {
                path: handle.path.clone(),
                opened_stat: handle.opened_stat.clone(),
            },
            offset,
            max_bytes,
            logical_size,
            dirty_overlays,
        })
    }

    fn dirty_size(&self, object: &dfs::DfsObjectId) -> u64 {
        self.dirty_inodes.get(object).map_or(0, |inode| {
            let end = |ranges: &DirtyWriteBuffer| {
                ranges
                    .ranges
                    .last_key_value()
                    .map_or(0, |(at, bytes)| at + bytes.len() as u64)
            };
            end(&inode.active).max(inode.frozen.as_ref().map_or(0, |p| end(&p.dirty)))
        })
    }

    fn visible_handle_stat(&self, fh: u64) -> Option<dfs::DfsStat> {
        let handle = self.handles.get(&fh)?;
        let mut stat = handle.visible_stat();
        stat.size_bytes = Some(handle.logical_size.max(self.dirty_size(&handle.object_id)));
        Some(stat)
    }

    fn intern_path(&mut self, path: String) -> u64 {
        if let Some(&ino) = self.ino_by_path.get(&path) {
            self.preferred_path_by_ino.insert(ino, path);
            return ino;
        }
        let ino = self.allocate_inode();
        self.attach_path(ino, path);
        ino
    }

    fn intern_entry(&mut self, path: String, object_id: dfs::DfsObjectId) -> u64 {
        if let Some(&ino) = self.ino_by_object_id.get(&object_id) {
            if self.ino_by_path.get(&path).copied() != Some(ino) {
                self.detach_path(&path, false);
                self.attach_path(ino, path);
            } else {
                self.preferred_path_by_ino.insert(ino, path);
            }
            return ino;
        }

        if let Some(&ino) = self.ino_by_path.get(&path) {
            match self.object_id_by_ino.get(&ino) {
                Some(existing) if existing == &object_id => {
                    self.ino_by_object_id.insert(object_id, ino);
                    self.preferred_path_by_ino.insert(ino, path);
                    return ino;
                }
                None => {
                    self.object_id_by_ino.insert(ino, object_id.clone());
                    self.ino_by_object_id.insert(object_id, ino);
                    self.preferred_path_by_ino.insert(ino, path);
                    return ino;
                }
                Some(_) => {
                    self.detach_path(&path, false);
                }
            }
        }

        let ino = self.allocate_inode();
        self.attach_path(ino, path);
        self.object_id_by_ino.insert(ino, object_id.clone());
        self.ino_by_object_id.insert(object_id, ino);
        ino
    }

    fn intern_alias_path(
        &mut self,
        target_ino: u64,
        path: String,
        object_id: &dfs::DfsObjectId,
    ) -> std::result::Result<u64, libc::c_int> {
        if !self.paths_by_ino.contains_key(&target_ino) {
            return Err(libc::ENOENT);
        }
        if self
            .object_id_by_ino
            .get(&target_ino)
            .is_some_and(|existing| existing != object_id)
        {
            return Err(libc::ESTALE);
        }
        if let Some(&existing_ino) = self.ino_by_object_id.get(object_id)
            && existing_ino != target_ino
        {
            if existing_ino == ROOT_INO {
                return Err(libc::ESTALE);
            }
            self.merge_inode_paths(existing_ino, target_ino);
        }
        self.object_id_by_ino.insert(target_ino, object_id.clone());
        self.ino_by_object_id.insert(object_id.clone(), target_ino);
        if self.ino_by_path.get(&path).copied() != Some(target_ino) {
            self.detach_path(&path, false);
            self.attach_path(target_ino, path);
        } else {
            self.preferred_path_by_ino.insert(target_ino, path);
        }
        Ok(target_ino)
    }

    fn remove_path(&mut self, path: &str) {
        if self.ino_by_path.get(path).copied() == Some(ROOT_INO) {
            return;
        }
        self.detach_path(path, false);
    }

    fn allocate_inode(&mut self) -> u64 {
        let ino = self.next_ino;
        self.next_ino = self.next_ino.saturating_add(1);
        ino
    }

    fn attach_path(&mut self, ino: u64, path: String) {
        self.paths_by_ino
            .entry(ino)
            .or_default()
            .insert(path.clone());
        self.preferred_path_by_ino.insert(ino, path.clone());
        self.ino_by_path.insert(path, ino);
    }

    fn detach_path(&mut self, path: &str, preserve_inode: bool) -> Option<u64> {
        let ino = self.ino_by_path.remove(path)?;
        let remove_inode = self.paths_by_ino.get_mut(&ino).is_some_and(|paths| {
            paths.remove(path);
            paths.is_empty()
        });
        if remove_inode {
            self.paths_by_ino.remove(&ino);
            self.preferred_path_by_ino.remove(&ino);
            if !preserve_inode && ino != ROOT_INO {
                self.remove_inode_identity(ino);
            }
        } else if self
            .preferred_path_by_ino
            .get(&ino)
            .is_some_and(|preferred| preferred == path)
            && let Some(replacement) = self
                .paths_by_ino
                .get(&ino)
                .and_then(|paths| paths.first())
                .cloned()
        {
            self.preferred_path_by_ino.insert(ino, replacement);
        }
        Some(ino)
    }

    fn remove_inode_identity(&mut self, ino: u64) {
        self.preferred_path_by_ino.remove(&ino);
        if let Some(object_id) = self.object_id_by_ino.remove(&ino)
            && self.ino_by_object_id.get(&object_id).copied() == Some(ino)
        {
            self.ino_by_object_id.remove(&object_id);
        }
    }

    fn merge_inode_paths(&mut self, from_ino: u64, to_ino: u64) {
        if from_ino == to_ino {
            return;
        }
        let paths = self.paths_by_ino.remove(&from_ino).unwrap_or_default();
        for path in paths {
            self.ino_by_path.insert(path.clone(), to_ino);
            self.paths_by_ino.entry(to_ino).or_default().insert(path);
        }
        self.remove_inode_identity(from_ino);
    }

    fn has_lost_lock_for_handle(&self, fh: u64) -> bool {
        self.lost_locks
            .values()
            .any(|lost| lost.associated_fhs.contains(&fh))
    }

    fn expire_elapsed_locks(&mut self, now: Instant) {
        let expired = self
            .held_locks
            .iter()
            .filter_map(|(key, held)| (held.local_lease_deadline <= now).then_some(key.clone()))
            .collect::<Vec<_>>();
        for key in expired {
            if let Some(held) = self.held_locks.remove(&key) {
                self.record_lost_lock(key, held.associated_fhs);
            }
        }
    }

    fn lock_owners_for_handle(&self, fh: u64) -> BTreeSet<u64> {
        self.held_locks
            .iter()
            .filter_map(|((_, lock_owner), held)| {
                held.associated_fhs.contains(&fh).then_some(*lock_owner)
            })
            .chain(
                self.pending_locks
                    .iter()
                    .filter_map(|((_, lock_owner), pending)| {
                        pending.associated_fhs.contains(&fh).then_some(*lock_owner)
                    }),
            )
            .chain(
                self.lost_locks
                    .iter()
                    .filter_map(|((_, lock_owner), lost)| {
                        lost.associated_fhs.contains(&fh).then_some(*lock_owner)
                    }),
            )
            .collect()
    }

    fn can_start_lock_key(&self, key: &(dfs::DfsObjectId, u64)) -> bool {
        if self.held_locks.contains_key(key) || self.pending_locks.contains_key(key) {
            return true;
        }
        self.held_locks
            .keys()
            .chain(self.pending_locks.keys())
            .collect::<BTreeSet<_>>()
            .len()
            < MAX_ACTIVE_LOCK_KEYS_PER_MOUNT
    }

    fn record_lost_lock(
        &mut self,
        key: (dfs::DfsObjectId, u64),
        associated_fhs: impl IntoIterator<Item = u64>,
    ) {
        self.lost_locks
            .entry(key)
            .or_default()
            .associated_fhs
            .extend(associated_fhs);
    }

    fn rename_path(&mut self, from: &str, to: String) {
        if self
            .paths_by_ino
            .get(&ROOT_INO)
            .is_some_and(|paths| paths.contains(from))
        {
            return;
        }
        if from == to
            || self
                .ino_by_path
                .get(from)
                .zip(self.ino_by_path.get(&to))
                .is_some_and(|(from_ino, to_ino)| from_ino == to_ino)
        {
            return;
        }
        let rewritten_inodes = self
            .ino_by_path
            .iter()
            .filter_map(|(path, ino)| {
                renamed_path(path, from, &to).map(|renamed| (*ino, path.clone(), renamed))
            })
            .collect::<Vec<_>>();
        let rewritten_old_paths = rewritten_inodes
            .iter()
            .map(|(_, old_path, _)| old_path.as_str())
            .collect::<BTreeSet<_>>();
        for (_, _, renamed) in &rewritten_inodes {
            if !rewritten_old_paths.contains(renamed.as_str()) {
                self.remove_path(renamed);
            }
        }
        for (_, old_path, _) in &rewritten_inodes {
            self.detach_path(old_path, true);
        }
        for (ino, _, renamed) in rewritten_inodes {
            self.attach_path(ino, renamed);
        }
        for handle in self.handles.values_mut() {
            if let Some(renamed) = renamed_path(&handle.path, from, &to) {
                handle.path = renamed;
            }
        }
        for held in self.held_locks.values_mut() {
            if let Some(renamed) = renamed_path(&held.path, from, &to) {
                held.path = renamed;
            }
        }
        for pending in self.pending_locks.values_mut() {
            if let Some(renamed) = renamed_path(&pending.path, from, &to) {
                pending.path = renamed;
            }
        }
    }

    fn allocate_handle(&mut self, path: String, stat: dfs::DfsStat, flags: i32) -> u64 {
        let fh = self.next_fh;
        self.next_fh = self.next_fh.saturating_add(1);
        let logical_size = stat.size_bytes.unwrap_or(0);
        let publication_gate = self
            .handles
            .values()
            .find(|h| h.object_id == stat.object_id)
            .map(|h| h.publication_gate.clone())
            .unwrap_or_default();
        self.handles.insert(
            fh,
            OpenFileHandle {
                path,
                object_id: stat.object_id.clone(),
                opened_stat: stat,
                logical_size,
                publication_gate,
                writable: flags_has_write_access(flags),
                sync_mode: HandleSyncMode::from_open_flags(flags),
            },
        );
        fh
    }

    #[cfg(test)]
    fn refresh_handle_stat(
        &mut self,
        fh: u64,
        stat: &dfs::DfsStat,
    ) -> std::result::Result<(), libc::c_int> {
        let handle = self.handles.get_mut(&fh).ok_or(libc::EBADF)?;
        if handle.object_id != stat.object_id {
            return Err(libc::ESTALE);
        }
        handle.refresh_stat(stat);
        Ok(())
    }

    fn refresh_object_handle_stats(
        &mut self,
        object_id: &dfs::DfsObjectId,
        stat: &dfs::DfsStat,
    ) -> std::result::Result<(), libc::c_int> {
        if &stat.object_id != object_id {
            return Err(libc::ESTALE);
        }
        for handle in self
            .handles
            .values_mut()
            .filter(|handle| &handle.object_id == object_id)
        {
            handle.refresh_stat(stat);
        }
        Ok(())
    }

    fn buffer_handle_write(
        &mut self,
        fh: u64,
        offset: u64,
        bytes: &[u8],
    ) -> std::result::Result<DirtyWriteInsert, libc::c_int> {
        let handle = self.handles.get_mut(&fh).ok_or(libc::EBADF)?;
        if !handle.writable {
            return Err(libc::EBADF);
        }
        if handle.sync_mode != HandleSyncMode::None {
            return Ok(DirtyWriteInsert::PublishDirectly);
        }
        let end = offset
            .checked_add(u64::try_from(bytes.len()).map_err(|_| libc::EFBIG)?)
            .ok_or(libc::EFBIG)?;
        let inode = self
            .dirty_inodes
            .entry(handle.object_id.clone())
            .or_default();
        if inode
            .base
            .as_ref()
            .is_some_and(|base| base.version != handle.opened_stat.version)
        {
            return Ok(DirtyWriteInsert::NeedsPublication);
        }
        // Bound metadata as well as bytes for sparse single-byte writes.
        if inode.active.ranges.len() >= 1024 && !inode.active.ranges.contains_key(&offset) {
            return Ok(DirtyWriteInsert::NeedsPublication);
        }
        let outcome = inode
            .active
            .try_write(offset, bytes, MAX_DIRTY_WRITE_BYTES_PER_INODE)?;
        if outcome == DirtyWriteInsert::Buffered && !bytes.is_empty() {
            inode.base.get_or_insert_with(|| handle.opened_stat.clone());
            inode.writers.insert(fh);
            handle.logical_size = handle.logical_size.max(end);
        }
        if inode.active.is_empty() && inode.frozen.is_none() {
            self.dirty_inodes.remove(&handle.object_id);
        }
        Ok(outcome)
    }

    fn take_dirty_publication(
        &mut self,
        fh: u64,
    ) -> std::result::Result<Option<Arc<FuseDirtyPublication>>, libc::c_int> {
        let handle = self.handles.get(&fh).ok_or(libc::EBADF)?;
        let Some(inode) = self.dirty_inodes.get_mut(&handle.object_id) else {
            return Ok(None);
        };
        if inode.frozen.is_none() && !inode.active.is_empty() {
            inode.frozen = Some(Arc::new(FuseDirtyPublication {
                base: inode.base.take().ok_or(libc::ESTALE)?,
                operation_id: format!("fuse_ranges_{}", dfs::DfsVersion::generate()),
                writers: std::mem::take(&mut inode.writers),
                dirty: std::mem::take(&mut inode.active),
            }));
        }
        Ok(inode.frozen.clone())
    }

    fn dirty_writer(&self, object: &dfs::DfsObjectId) -> Option<u64> {
        let inode = self.dirty_inodes.get(object)?;
        inode
            .frozen
            .as_ref()
            .and_then(|p| p.writers.first())
            .or_else(|| inode.writers.first())
            .copied()
    }

    fn dirty_handles_for_path(&self, path: &str, include_descendants: bool) -> Vec<u64> {
        let matching_objects = self
            .handles
            .values()
            .filter(|handle| {
                handle.path == path
                    || (include_descendants && renamed_path(&handle.path, path, path).is_some())
            })
            .map(|handle| handle.object_id.clone())
            .collect::<BTreeSet<_>>();
        matching_objects
            .iter()
            .filter_map(|object| self.dirty_writer(object))
            .collect()
    }

    fn dirty_handles_for_object(
        &self,
        object_id: &dfs::DfsObjectId,
        except_fh: Option<u64>,
    ) -> Vec<u64> {
        self.dirty_writer(object_id)
            .filter(|fh| Some(*fh) != except_fh)
            .into_iter()
            .collect()
    }
}

#[derive(Clone, Debug)]
struct OpenFileHandle {
    path: String,
    object_id: dfs::DfsObjectId,
    // Exact immutable content identity last published by this descriptor.
    // Dirty data is deliberately separate so opened-version reads never ask a
    // peer for bytes under a size/version combination that was not committed.
    opened_stat: dfs::DfsStat,
    logical_size: u64,
    publication_gate: Arc<MountLock<()>>,
    writable: bool,
    sync_mode: HandleSyncMode,
}

impl OpenFileHandle {
    fn visible_stat(&self) -> dfs::DfsStat {
        let mut stat = self.opened_stat.clone();
        stat.size_bytes = Some(self.logical_size);
        stat
    }

    fn refresh_stat(&mut self, stat: &dfs::DfsStat) {
        self.opened_stat = stat.clone();
        let committed_size = stat.size_bytes.unwrap_or(0);
        self.logical_size = committed_size;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HandleSyncMode {
    None,
    Data,
    Full,
}

impl HandleSyncMode {
    fn from_open_flags(flags: i32) -> Self {
        // Linux defines O_SYNC as O_DSYNC plus an additional bit, so test the
        // stronger mode first.
        if flags & libc::O_SYNC == libc::O_SYNC {
            Self::Full
        } else if flags & libc::O_DSYNC == libc::O_DSYNC {
            Self::Data
        } else {
            Self::None
        }
    }

    fn kind(self) -> Option<dfs::DfsPosixSyncKind> {
        match self {
            Self::None => None,
            Self::Data => Some(dfs::DfsPosixSyncKind::FileData),
            Self::Full => Some(dfs::DfsPosixSyncKind::File),
        }
    }
}

#[derive(Clone, Debug)]
struct FuseHeldLock {
    path: String,
    object_id: dfs::DfsObjectId,
    session_id: dfs::DfsAdvisoryLockSessionId,
    grant: dfs::DfsAdvisoryLockGrant,
    local_lease_deadline: Instant,
    associated_fhs: BTreeSet<u64>,
}

#[derive(Clone, Debug)]
struct FusePendingLock {
    path: String,
    object_id: dfs::DfsObjectId,
    session_id: dfs::DfsAdvisoryLockSessionId,
    acquire_request_id: dfs::DfsAdvisoryLockRequestId,
    associated_fhs: BTreeSet<u64>,
}

#[derive(Clone, Debug, Default)]
struct FuseLostLock {
    associated_fhs: BTreeSet<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FuseLockInstallOutcome {
    Installed,
    Expired,
    Cancelled,
}

impl Drop for SifsFuseFileSystem {
    fn drop(&mut self) {
        self.lock_renewer_stop.store(true, Ordering::Release);
        // Field teardown drops the executor after this hook; cancellation must
        // be visible before it starts joining in-flight workers.
        self.read_cancellation.cancel();
        // `destroy` is the ordinary mount-stop boundary. A failed mount setup
        // can instead drop the filesystem directly, which is also a complete
        // stop and must leave an orderly closed host-session marker.
        drop(self.mounted_mutation_handoff.take());
    }
}

fn next_lock_request_id(sequence: &AtomicU64, operation: &str) -> dfs::DfsAdvisoryLockRequestId {
    dfs::DfsAdvisoryLockRequestId::new(format!(
        "fuse-{operation}-{}-{}",
        system_time_to_nanos(SystemTime::now()),
        sequence.fetch_add(1, Ordering::Relaxed)
    ))
}

fn fuse_held_lock(
    path: String,
    object_id: dfs::DfsObjectId,
    session_id: dfs::DfsAdvisoryLockSessionId,
    grant: dfs::DfsAdvisoryLockGrant,
    local_lease_deadline: Option<Instant>,
    associated_fhs: BTreeSet<u64>,
) -> Option<FuseHeldLock> {
    let local_lease_deadline =
        local_lease_deadline.filter(|deadline| *deadline > Instant::now())?;
    Some(FuseHeldLock {
        path,
        object_id,
        session_id,
        grant,
        local_lease_deadline,
        associated_fhs,
    })
}

#[derive(Debug)]
struct FuseLockCommandResult {
    outcome: dfs::DfsAdvisoryLockOutcome,
    local_lease_deadline: Option<Instant>,
}

fn lock_outcome_lease_remaining(outcome: &dfs::DfsAdvisoryLockOutcome) -> Option<Duration> {
    let grant = match outcome {
        dfs::DfsAdvisoryLockOutcome::Acquired { grant }
        | dfs::DfsAdvisoryLockOutcome::Renewed { grant }
        | dfs::DfsAdvisoryLockOutcome::Converted { grant } => grant,
        _ => return None,
    };
    Some(Duration::from_nanos(grant.lease_remaining_nanos))
}

fn conservative_local_lease_deadline(
    outcome: &dfs::DfsAdvisoryLockOutcome,
    request_started_at: Instant,
) -> Option<Instant> {
    lock_outcome_lease_remaining(outcome)
        .filter(|remaining| !remaining.is_zero())
        .and_then(|remaining| request_started_at.checked_add(remaining))
}

fn fuse_advisory_lock_mutation_operation(
    command: &dfs::DfsAdvisoryLockCommand,
) -> FuseMountedMutationOperation {
    match command {
        dfs::DfsAdvisoryLockCommand::Acquire { .. } => {
            FuseMountedMutationOperation::AdvisoryLockAcquire
        }
        dfs::DfsAdvisoryLockCommand::Query { .. } => {
            FuseMountedMutationOperation::AdvisoryLockQuery
        }
        dfs::DfsAdvisoryLockCommand::Renew { .. } => {
            FuseMountedMutationOperation::AdvisoryLockRenew
        }
        dfs::DfsAdvisoryLockCommand::Convert { .. } => {
            FuseMountedMutationOperation::AdvisoryLockConvert
        }
        dfs::DfsAdvisoryLockCommand::Unlock { .. } => {
            FuseMountedMutationOperation::AdvisoryLockUnlock
        }
        dfs::DfsAdvisoryLockCommand::Cancel { .. } => {
            FuseMountedMutationOperation::AdvisoryLockCancel
        }
        dfs::DfsAdvisoryLockCommand::Close { .. } => {
            FuseMountedMutationOperation::AdvisoryLockClose
        }
    }
}

fn renamed_path(path: &str, from: &str, to: &str) -> Option<String> {
    if path == from {
        return Some(to.to_string());
    }
    let suffix = path.strip_prefix(from)?;
    suffix.starts_with('/').then(|| format!("{to}{suffix}"))
}

fn upgrade_background_mounted_mutation_handoff(
    handoff: &Option<Weak<FuseMountedMutationHandoff>>,
) -> std::result::Result<Option<Arc<FuseMountedMutationHandoff>>, libc::c_int> {
    match handoff {
        Some(handoff) => handoff.upgrade().map(Some).ok_or(libc::EIO),
        None => Ok(None),
    }
}

fn preflight_blocking_lock_acquire(
    state: &Arc<MountLock<FuseState>>,
    key: &(dfs::DfsObjectId, u64),
    acquire_request_id: &dfs::DfsAdvisoryLockRequestId,
) -> std::result::Result<(), FuseMountedMutationPreflightError> {
    let state = state
        .lock()
        .map_err(|_| FuseMountedMutationPreflightError {
            errno: libc::EIO,
            proof_code: "fuse-blocking-lock-state-unavailable-before-service-entry",
        })?;
    if state
        .pending_locks
        .get(key)
        .is_some_and(|pending| &pending.acquire_request_id == acquire_request_id)
    {
        Ok(())
    } else {
        Err(FuseMountedMutationPreflightError {
            errno: libc::EINTR,
            proof_code: "fuse-blocking-lock-cancelled-before-service-entry",
        })
    }
}

fn run_advisory_lock_command(
    handoff: Option<&FuseMountedMutationHandoff>,
    service: &Arc<MountLock<SifsService>>,
    path: &str,
    object_id: &dfs::DfsObjectId,
    command: dfs::DfsAdvisoryLockCommand,
) -> std::result::Result<FuseLockCommandResult, libc::c_int> {
    run_advisory_lock_command_with_preflight(handoff, service, path, object_id, command, |_| Ok(()))
}

fn run_advisory_lock_command_with_preflight(
    handoff: Option<&FuseMountedMutationHandoff>,
    service: &Arc<MountLock<SifsService>>,
    path: &str,
    object_id: &dfs::DfsObjectId,
    command: dfs::DfsAdvisoryLockCommand,
    preflight: impl FnOnce(
        &mut SifsService,
    ) -> std::result::Result<(), FuseMountedMutationPreflightError>,
) -> std::result::Result<FuseLockCommandResult, libc::c_int> {
    let service_entry_timed_out = AtomicBool::new(false);
    run_advisory_lock_command_with_preflight_and_entry_timeout(
        handoff,
        service,
        path,
        object_id,
        command,
        preflight,
        &service_entry_timed_out,
    )
}

#[allow(clippy::too_many_arguments)]
fn run_advisory_lock_command_with_preflight_and_entry_timeout(
    handoff: Option<&FuseMountedMutationHandoff>,
    service: &Arc<MountLock<SifsService>>,
    path: &str,
    object_id: &dfs::DfsObjectId,
    command: dfs::DfsAdvisoryLockCommand,
    preflight: impl FnOnce(
        &mut SifsService,
    ) -> std::result::Result<(), FuseMountedMutationPreflightError>,
    service_entry_timed_out: &AtomicBool,
) -> std::result::Result<FuseLockCommandResult, libc::c_int> {
    // Anchor before the first attempt. The authority reports a relative
    // duration after it has durably settled the transition; projecting that
    // duration from this earlier instant is conservative across request,
    // response, and exact-retry delay and can never extend the server lease.
    let started_at = Instant::now();
    // The command adds no deadline of its own: the caller's, if it has one,
    // is the only bound in time.
    let deadline = dfs::MountedSifsCall::deadline();
    let operation = fuse_advisory_lock_mutation_operation(&command);
    let stable_operation_id = Some(command.request_id().0.clone());
    run_fuse_service_mounted_mutation_with_lock_and_preflight(
        handoff,
        service.as_ref(),
        operation,
        stable_operation_id,
        |service| match lock_service_within_deadline(service) {
            Err(errno) if errno == libc::ETIMEDOUT => {
                service_entry_timed_out.store(true, Ordering::Release);
                Err(errno)
            }
            result => result,
        },
        preflight,
        |service| {
            run_advisory_lock_command_with_service(
                service, path, object_id, command, started_at, deadline,
            )
            .map_err(|errno| {
                FuseMountedMutationError::ambiguous(errno, "fuse-advisory-lock-command-error")
            })
        },
    )
}

/// Wait for the mount's service lane until the caller's deadline, if it has
/// one. `ETIMEDOUT` is a positive fact that the command never entered the
/// service.
fn lock_service_within_deadline(
    service: &MountLock<SifsService>,
) -> std::result::Result<MountLockGuard<'_, SifsService>, libc::c_int> {
    dfs::MountedSifsCall::lock(service).map_err(|error| {
        if error.kind() == io::ErrorKind::TimedOut {
            libc::ETIMEDOUT
        } else {
            libc::EIO
        }
    })
}

fn run_advisory_lock_command_with_service(
    service: &mut SifsService,
    path: &str,
    object_id: &dfs::DfsObjectId,
    command: dfs::DfsAdvisoryLockCommand,
    started_at: Instant,
    deadline: Option<Instant>,
) -> std::result::Result<FuseLockCommandResult, libc::c_int> {
    let retry_command = command.clone();
    // What is left of the caller's budget; a caller without one waits for
    // the command's outcome.
    let remaining = || deadline.map(|at| at.saturating_duration_since(Instant::now()));
    let first_budget = remaining();
    if first_budget.is_some_and(|budget| budget.is_zero()) {
        return Err(libc::ETIMEDOUT);
    }
    let first =
        service.posix_advisory_lock_for_object_with_timeout(path, object_id, command, first_budget);
    let outcome = match first {
        Ok(outcome) => Ok(outcome),
        Err(first_error) => {
            if std::env::var_os("SINOMO_FUSE_LOCK_DEBUG").is_some() {
                eprintln!("sinomo-dfs-fuse: advisory-lock first attempt failed: {first_error:?}");
            }
            let retry_budget = remaining();
            if retry_budget.is_some_and(|budget| budget.is_zero()) {
                return Err(errno_for_posix_error(first_error));
            }
            service
                .posix_advisory_lock_for_object_with_timeout(
                    path,
                    object_id,
                    retry_command,
                    retry_budget,
                )
                .map_err(|error| {
                    if std::env::var_os("SINOMO_FUSE_LOCK_DEBUG").is_some() {
                        eprintln!("sinomo-dfs-fuse: advisory-lock exact retry failed: {error:?}");
                    }
                    errno_for_posix_error(error)
                })
        }
    }?;
    let local_lease_deadline = conservative_local_lease_deadline(&outcome, started_at);
    Ok(FuseLockCommandResult {
        outcome,
        local_lease_deadline,
    })
}

#[allow(clippy::too_many_arguments)]
fn spawn_blocking_lock_waiter(
    service: Weak<MountLock<SifsService>>,
    state: Weak<MountLock<FuseState>>,
    handoff: Option<Weak<FuseMountedMutationHandoff>>,
    stop: Arc<AtomicBool>,
    key: (dfs::DfsObjectId, u64),
    command: dfs::DfsAdvisoryLockCommand,
    reply: ReplyEmpty,
) {
    let deadline = dfs::MountedSifsCall::deadline();
    thread::spawn(move || {
        let _call = dfs::MountedSifsCall::enter(deadline);
        // The wait between two sends of the queued acquire. The mount's
        // close and a cancel of this waiter end it at the next send.
        let pause = |interval: Duration| {
            thread::sleep(deadline.map_or(interval, |at| {
                interval.min(at.saturating_duration_since(Instant::now()))
            }));
        };
        let mut reply = Some(reply);
        let acquire_request_id = command.request_id().clone();
        loop {
            if deadline.is_some_and(|at| Instant::now() >= at) {
                remove_pending_lock(&state, &key, &acquire_request_id);
                reply
                    .take()
                    .expect("blocking lock reply")
                    .error_from_i32(libc::EIO);
                return;
            }
            if stop.load(Ordering::Acquire) {
                remove_pending_lock(&state, &key, &acquire_request_id);
                reply
                    .take()
                    .expect("blocking lock reply")
                    .error_from_i32(libc::EINTR);
                return;
            }
            let Some(state_arc) = state.upgrade() else {
                reply
                    .take()
                    .expect("blocking lock reply")
                    .error_from_i32(libc::EINTR);
                return;
            };
            let current_pending = state_arc
                .lock()
                .ok()
                .and_then(|state| state.pending_locks.get(&key).cloned())
                .filter(|pending| pending.acquire_request_id == acquire_request_id);
            let Some(current_pending) = current_pending else {
                reply
                    .take()
                    .expect("blocking lock reply")
                    .error_from_i32(libc::EINTR);
                return;
            };
            let Some(service_arc) = service.upgrade() else {
                remove_pending_lock(&state, &key, &acquire_request_id);
                reply
                    .take()
                    .expect("blocking lock reply")
                    .error_from_i32(libc::EIO);
                return;
            };
            let handoff_arc = match upgrade_background_mounted_mutation_handoff(&handoff) {
                Ok(handoff) => handoff,
                Err(errno) => {
                    remove_pending_lock(&state, &key, &acquire_request_id);
                    reply
                        .take()
                        .expect("blocking lock reply")
                        .error_from_i32(errno);
                    return;
                }
            };
            let service_entry_timed_out = AtomicBool::new(false);
            let state_for_preflight = Arc::clone(&state_arc);
            let key_for_preflight = key.clone();
            let request_for_preflight = acquire_request_id.clone();
            let outcome = run_advisory_lock_command_with_preflight_and_entry_timeout(
                handoff_arc.as_deref(),
                &service_arc,
                &current_pending.path,
                &current_pending.object_id,
                command.clone(),
                move |_| {
                    // Cancel/close removes the pending marker before waiting
                    // for mutation admission. Recheck only after admission and
                    // the service lane are both held so no first Acquire can
                    // cross a locally linearized cancellation.
                    preflight_blocking_lock_acquire(
                        &state_for_preflight,
                        &key_for_preflight,
                        &request_for_preflight,
                    )
                },
                &service_entry_timed_out,
            );
            if matches!(outcome, Err(errno) if errno == libc::ETIMEDOUT)
                && service_entry_timed_out.load(Ordering::Acquire)
            {
                // The caller's deadline passed while this command queued for
                // the service lane: a positive local no-entry fact, not
                // ambiguity in the advisory-lock RPC. The loop's deadline
                // check answers the caller.
                continue;
            }
            match outcome {
                Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::Acquired { grant },
                    local_lease_deadline,
                }) => {
                    let install_outcome = match state_arc.lock() {
                        Ok(mut state) => {
                            let current_pending = state
                                .pending_locks
                                .get(&key)
                                .filter(|pending| pending.acquire_request_id == acquire_request_id)
                                .cloned();
                            let Some(current_pending) = current_pending else {
                                reply
                                    .take()
                                    .expect("blocking lock reply")
                                    .error_from_i32(libc::EINTR);
                                return;
                            };
                            state.pending_locks.remove(&key);
                            let associated_fhs = current_pending.associated_fhs.clone();
                            if let Some(held) = fuse_held_lock(
                                current_pending.path,
                                current_pending.object_id,
                                current_pending.session_id,
                                grant,
                                local_lease_deadline,
                                associated_fhs.clone(),
                            ) {
                                state.held_locks.insert(key.clone(), held);
                                state.lost_locks.remove(&key);
                                FuseLockInstallOutcome::Installed
                            } else {
                                state.record_lost_lock(key.clone(), associated_fhs);
                                FuseLockInstallOutcome::Expired
                            }
                        }
                        Err(_) => FuseLockInstallOutcome::Cancelled,
                    };
                    match install_outcome {
                        FuseLockInstallOutcome::Installed => {
                            reply.take().expect("blocking lock reply").ok()
                        }
                        FuseLockInstallOutcome::Expired => reply
                            .take()
                            .expect("blocking lock reply")
                            .error_from_i32(libc::EIO),
                        FuseLockInstallOutcome::Cancelled => reply
                            .take()
                            .expect("blocking lock reply")
                            .error_from_i32(libc::EINTR),
                    }
                    return;
                }
                Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::Waiting { .. },
                    ..
                }) => {
                    pause(BLOCKING_LOCK_RESEND_INTERVAL);
                }
                Ok(FuseLockCommandResult {
                    outcome:
                        dfs::DfsAdvisoryLockOutcome::AuthorityWarming {
                            retry_after_unix_nanos,
                        },
                    ..
                }) => {
                    let remaining = retry_after_unix_nanos
                        .saturating_sub(system_time_to_nanos(SystemTime::now()));
                    pause(Duration::from_nanos(remaining).min(BLOCKING_LOCK_RESEND_INTERVAL));
                }
                Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::Cancelled { .. },
                    ..
                }) => {
                    remove_pending_lock(&state, &key, &acquire_request_id);
                    reply
                        .take()
                        .expect("blocking lock reply")
                        .error_from_i32(libc::EINTR);
                    return;
                }
                Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::WaitExpired { .. },
                    ..
                }) => {
                    remove_pending_lock(&state, &key, &acquire_request_id);
                    reply
                        .take()
                        .expect("blocking lock reply")
                        .error_from_i32(libc::ETIMEDOUT);
                    return;
                }
                Ok(response) => {
                    remove_pending_lock(&state, &key, &acquire_request_id);
                    reply
                        .take()
                        .expect("blocking lock reply")
                        .error_from_i32(errno_for_unexpected_lock_outcome(&response.outcome));
                    return;
                }
                Err(errno) => {
                    if let Ok(mut state) = state_arc.lock()
                        && let Some(pending) = state.pending_locks.remove(&key)
                    {
                        state.record_lost_lock(key.clone(), pending.associated_fhs);
                    }
                    reply
                        .take()
                        .expect("blocking lock reply")
                        .error_from_i32(errno);
                    return;
                }
            }
        }
    });
}

fn remove_pending_lock(
    state: &Weak<MountLock<FuseState>>,
    key: &(dfs::DfsObjectId, u64),
    acquire_request_id: &dfs::DfsAdvisoryLockRequestId,
) {
    if let Some(state) = state.upgrade()
        && let Ok(mut state) = state.lock()
        && state
            .pending_locks
            .get(key)
            .is_some_and(|pending| &pending.acquire_request_id == acquire_request_id)
    {
        state.pending_locks.remove(key);
    }
}

fn spawn_lock_renewer(
    service: Weak<MountLock<SifsService>>,
    state: Weak<MountLock<FuseState>>,
    handoff: Option<Weak<FuseMountedMutationHandoff>>,
    request_sequence: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) {
    thread::spawn(move || {
        while !stop.load(Ordering::Acquire) {
            thread::sleep(LOCK_RENEW_INTERVAL);
            if stop.load(Ordering::Acquire) {
                return;
            }
            let (Some(service), Some(state)) = (service.upgrade(), state.upgrade()) else {
                return;
            };
            let held = match state.lock() {
                Ok(state) => state
                    .held_locks
                    .iter()
                    .map(|(key, held)| (key.clone(), held.clone()))
                    .collect::<Vec<_>>(),
                Err(_) => return,
            };
            for (key, held) in held {
                if stop.load(Ordering::Acquire) {
                    return;
                }
                let command = dfs::DfsAdvisoryLockCommand::Renew {
                    request_id: next_lock_request_id(&request_sequence, "renew"),
                    token: held.grant.token.clone(),
                };
                let handoff = match upgrade_background_mounted_mutation_handoff(&handoff) {
                    Ok(handoff) => handoff,
                    Err(_) => {
                        mark_fuse_lock_lost(&state, &key, &held.grant);
                        return;
                    }
                };
                // A renewal has no syscall behind it. Its own bound is the
                // lease it renews: past that there is nothing left to renew,
                // and the next key must not wait behind it.
                let _call = dfs::MountedSifsCall::enter(Some(held.local_lease_deadline));
                match run_advisory_lock_command(
                    handoff.as_deref(),
                    &service,
                    &held.path,
                    &held.object_id,
                    command,
                ) {
                    Ok(FuseLockCommandResult {
                        outcome: dfs::DfsAdvisoryLockOutcome::Renewed { grant },
                        local_lease_deadline,
                    }) => {
                        if let Some(local_lease_deadline) = local_lease_deadline
                            && local_lease_deadline > Instant::now()
                        {
                            if let Ok(mut state) = state.lock()
                                && let Some(current) = state.held_locks.get_mut(&key)
                                && current.grant == held.grant
                            {
                                current.grant = grant;
                                current.local_lease_deadline = local_lease_deadline;
                            }
                        } else {
                            mark_fuse_lock_lost(&state, &key, &held.grant);
                        }
                    }
                    Ok(response) if lock_outcome_loses_grant(&response.outcome) => {
                        mark_fuse_lock_lost(&state, &key, &held.grant);
                    }
                    Ok(_) | Err(_) => {
                        if held.local_lease_deadline <= Instant::now() {
                            mark_fuse_lock_lost(&state, &key, &held.grant);
                        }
                    }
                }
            }
        }
    });
}

fn mark_fuse_lock_lost(
    state: &Arc<MountLock<FuseState>>,
    key: &(dfs::DfsObjectId, u64),
    expected_grant: &dfs::DfsAdvisoryLockGrant,
) {
    if let Ok(mut state) = state.lock() {
        let is_expected = state
            .held_locks
            .get(key)
            .is_some_and(|current| current.grant == *expected_grant);
        if is_expected {
            let held = state
                .held_locks
                .remove(key)
                .expect("expected advisory lock disappeared while state was held");
            state.record_lost_lock(key.clone(), held.associated_fhs);
        }
    }
}

fn is_whole_file_lock_range(start: u64, end: u64) -> bool {
    start == 0 && (end == u64::MAX || end == i64::MAX as u64)
}

fn advisory_lock_mode(
    lock_type: i32,
) -> std::result::Result<dfs::DfsAdvisoryLockMode, libc::c_int> {
    match lock_type {
        libc::F_RDLCK => Ok(dfs::DfsAdvisoryLockMode::Shared),
        libc::F_WRLCK => Ok(dfs::DfsAdvisoryLockMode::Exclusive),
        _ => Err(libc::EINVAL),
    }
}

fn advisory_lock_type(mode: dfs::DfsAdvisoryLockMode) -> i32 {
    match mode {
        dfs::DfsAdvisoryLockMode::Shared => libc::F_RDLCK,
        dfs::DfsAdvisoryLockMode::Exclusive => libc::F_WRLCK,
    }
}

fn lock_outcome_loses_grant(outcome: &dfs::DfsAdvisoryLockOutcome) -> bool {
    matches!(
        outcome,
        dfs::DfsAdvisoryLockOutcome::Fenced { .. }
            | dfs::DfsAdvisoryLockOutcome::LockLost
            | dfs::DfsAdvisoryLockOutcome::StaleToken
            | dfs::DfsAdvisoryLockOutcome::StaleAuthority { .. }
    )
}

fn errno_for_unexpected_lock_outcome(outcome: &dfs::DfsAdvisoryLockOutcome) -> libc::c_int {
    match outcome {
        dfs::DfsAdvisoryLockOutcome::WouldBlock { .. }
        | dfs::DfsAdvisoryLockOutcome::Waiting { .. }
        | dfs::DfsAdvisoryLockOutcome::AuthorityWarming { .. } => libc::EAGAIN,
        dfs::DfsAdvisoryLockOutcome::SessionBusy => libc::EDEADLK,
        dfs::DfsAdvisoryLockOutcome::RequestMismatch => libc::EINVAL,
        dfs::DfsAdvisoryLockOutcome::WaitExpired { .. } => libc::ETIMEDOUT,
        dfs::DfsAdvisoryLockOutcome::Cancelled { .. } => libc::EINTR,
        dfs::DfsAdvisoryLockOutcome::Fenced { .. }
        | dfs::DfsAdvisoryLockOutcome::LockLost
        | dfs::DfsAdvisoryLockOutcome::StaleToken
        | dfs::DfsAdvisoryLockOutcome::StaleAuthority { .. } => libc::EIO,
        _ => libc::EIO,
    }
}

fn file_attr(
    ino: u64,
    stat: &dfs::DfsStat,
    presented_owner: Option<FusePresentedOwner>,
) -> FileAttr {
    let size = stat.size_bytes.unwrap_or(0);
    // Owner-only FUSE sessions admit requests solely from the process that
    // mounted the filesystem. DFS-native writers do not have a portable Unix
    // identity and therefore use neutral durable ownership (0:0). Presenting
    // that metadata verbatim would make the kernel's `default_permissions`
    // reject the mount owner before FUSE can handle the request. Project an
    // owner-only view to the mount process while preserving durable uid/gid
    // for explicit multi-user (`allow_other`) mounts.
    let (uid, gid) = presented_owner.map_or((stat.uid, stat.gid), |owner| (owner.uid, owner.gid));
    FileAttr {
        ino: INodeNo(ino),
        size,
        blocks: size.div_ceil(512),
        atime: nanos_to_system_time(stat.accessed_at_unix_nanos),
        mtime: nanos_to_system_time(stat.modified_at_unix_nanos),
        ctime: nanos_to_system_time(stat.changed_at_unix_nanos),
        crtime: nanos_to_system_time(stat.created_at_unix_nanos),
        kind: file_type_for_kind(&stat.kind),
        perm: (stat.mode & 0o7777) as u16,
        nlink: stat.link_count.min(u32::MAX as u64) as u32,
        uid,
        gid,
        rdev: 0,
        blksize: DEFAULT_BLOCK_SIZE,
        flags: 0,
    }
}

fn file_type_for_kind(kind: &DfsObjectKind) -> FileType {
    match kind {
        DfsObjectKind::Directory | DfsObjectKind::Mount => FileType::Directory,
        DfsObjectKind::Symlink => FileType::Symlink,
        DfsObjectKind::File
        | DfsObjectKind::Artifact
        | DfsObjectKind::Hardlink
        | DfsObjectKind::Tombstone => FileType::RegularFile,
    }
}

fn errno_for_posix_error(error: DfsPosixError) -> libc::c_int {
    match error {
        DfsPosixError::NotFound { .. } => libc::ENOENT,
        DfsPosixError::AlreadyExists { .. } => libc::EEXIST,
        DfsPosixError::NotDirectory { .. } => libc::ENOTDIR,
        DfsPosixError::IsDirectory { .. } => libc::EISDIR,
        DfsPosixError::NotSymlink { .. } => libc::EINVAL,
        DfsPosixError::MissingParent { .. } => libc::ENOENT,
        DfsPosixError::InvalidPath { .. } => libc::EINVAL,
        DfsPosixError::PendingWrite { .. } => libc::EAGAIN,
        DfsPosixError::SymlinkContentPending { .. } => libc::EAGAIN,
        DfsPosixError::Conflict { .. } => libc::ESTALE,
        DfsPosixError::DirectoryNotEmpty { .. } => libc::ENOTEMPTY,
        // In-flight cross-shard moves reject mutations transiently; tell the
        // application to retry rather than reporting an I/O failure.
        DfsPosixError::Dfs(DfsError::Busy { .. }) => libc::EAGAIN,
        // An unknown outcome is what is left once the call has followed its
        // shard's record for as long as its caller allowed: the change may
        // still commit. It is reported as an I/O error; the mount keeps
        // serving, and a batch that keeps its operation id is published
        // again by the next flush.
        DfsPosixError::PartialBulkCommit { .. }
        | DfsPosixError::Dfs(_)
        | DfsPosixError::Metadata(_) => libc::EIO,
    }
}

fn created_mode(mode: u32, umask: u32) -> u32 {
    (mode & 0o7777) & !(umask & 0o777)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExistingFileCreateAction {
    Reject,
    ReturnExisting,
    Truncate,
}

fn existing_file_create_action(flags: i32) -> ExistingFileCreateAction {
    if flags & libc::O_EXCL != 0 {
        ExistingFileCreateAction::Reject
    } else if flags & libc::O_TRUNC != 0 {
        ExistingFileCreateAction::Truncate
    } else {
        ExistingFileCreateAction::ReturnExisting
    }
}

fn create_file_for_fuse(
    dfs: &mut SifsService,
    path: &str,
    options: DfsWriteOptions,
    attrs: DfsPosixCreateAttrs,
    flags: i32,
) -> std::result::Result<dfs::DfsStat, FuseMountedMutationError> {
    if existing_file_create_action(flags) == ExistingFileCreateAction::Reject {
        return dfs
            .posix_create_file_with_attrs(path, options, attrs)
            .map_err(FuseMountedMutationError::from);
    }

    let mut create_needed = true;
    for _ in 0..=FUSE_CREATE_RACE_RETRY_ROUNDS {
        if create_needed {
            match dfs.posix_create_file_following_symlink_with_attrs(path, options.clone(), attrs) {
                Ok(created) => return Ok(created),
                Err(DfsPosixError::AlreadyExists { .. }) => create_needed = false,
                Err(error) => return Err(FuseMountedMutationError::from(error)),
            }
        }
        match dfs.posix_stat_distributed(path) {
            Ok(existing) => {
                if existing.kind == DfsObjectKind::Directory {
                    return Err(FuseMountedMutationError::ambiguous(
                        libc::EISDIR,
                        "fuse-create-existing-directory",
                    ));
                }
                if existing.kind != DfsObjectKind::File {
                    return Err(FuseMountedMutationError::ambiguous(
                        libc::EINVAL,
                        "fuse-create-existing-non-file",
                    ));
                }
                if existing_file_create_action(flags) == ExistingFileCreateAction::Truncate {
                    // The create attrs are ignored while this inode still
                    // exists, but become the initial attrs if an unlink wins
                    // just before the truncating write recreates the path.
                    return dfs
                        .posix_write_file_with_create_attrs(path, &[], options.clone(), attrs)
                        .map_err(FuseMountedMutationError::from);
                }
                match dfs.posix_stat_distributed(path) {
                    Ok(confirmed) if confirmed.object_id == existing.object_id => {
                        return Ok(confirmed);
                    }
                    Ok(_) => continue,
                    Err(DfsPosixError::NotFound { .. }) => {
                        create_needed = true;
                        continue;
                    }
                    Err(error) => return Err(FuseMountedMutationError::from(error)),
                }
            }
            Err(DfsPosixError::NotFound { .. }) => {
                create_needed = true;
                continue;
            }
            Err(error) => return Err(FuseMountedMutationError::from(error)),
        }
    }
    Err(FuseMountedMutationError::ambiguous(
        libc::EAGAIN,
        "fuse-create-race-retry-exhausted",
    ))
}

fn fuse_write_replica_candidates(
    replica_candidates: Vec<dfs::DfsNodeId>,
    local_node_id: dfs::DfsNodeId,
) -> Vec<dfs::DfsNodeId> {
    if replica_candidates.is_empty() {
        vec![local_node_id]
    } else {
        replica_candidates
    }
}

fn fuse_write_options(replica_candidates: Vec<dfs::DfsNodeId>) -> DfsWriteOptions {
    let mut options = DfsWriteOptions::replicated_eventual(replica_candidates);
    // Replicated-eventual becomes visible once its W durable receipt threshold
    // is met. Finishing the desired N-W replicas belongs to repair; waiting for
    // it on every kernel write serializes each FUSE chunk behind another WAN
    // round trip. fsync still captures the returned version and waits for all
    // desired receipts through posix_sync_file_target.
    options.foreground_replica_repair = false;
    options
}

fn flags_has_write_access(flags: i32) -> bool {
    matches!(flags & libc::O_ACCMODE, libc::O_WRONLY | libc::O_RDWR)
}

fn nanos_to_system_time(nanos: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(nanos)
}

fn system_time_to_nanos(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

fn time_or_now_to_nanos(time: TimeOrNow) -> u64 {
    match time {
        TimeOrNow::SpecificTime(time) => system_time_to_nanos(time),
        TimeOrNow::Now => system_time_to_nanos(SystemTime::now()),
    }
}

#[cfg(test)]
mod errno_tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard};

    #[derive(Debug, Default)]
    struct TestMountedMutationLease;

    #[derive(Debug, Default)]
    struct TestMountedMutationCoordinator {
        events: Arc<Mutex<Vec<&'static str>>>,
        gated: AtomicBool,
        mutation_lane: Mutex<()>,
        fail_arm: AtomicBool,
        fail_commit: AtomicBool,
        /// The daemon answers again from the first re-send on.
        answers_on_resend: AtomicBool,
        /// The daemon definitely refuses instead of staying silent.
        refuses: AtomicBool,
        /// The outbox has no room for the next Prepare.
        outbox_full: AtomicBool,
        service_lock: Option<Arc<MountLock<usize>>>,
        commit_saw_service_lock_free: AtomicBool,
        commit_block: Option<Arc<(Mutex<(bool, bool)>, std::sync::Condvar)>>,
        captured_work: Mutex<Vec<dfs::MountedSifsMutationCapturedWork>>,
        unknown_observations: Mutex<Vec<dfs::MountedSifsMutationErrorObservation>>,
        prepared_operations: Mutex<Vec<(FuseMountedMutationOperation, Option<String>)>>,
    }

    impl TestMountedMutationCoordinator {
        fn record(&self, event: &'static str) {
            self.events.lock().expect("event lock").push(event);
        }

        /// What a failing step returns: the daemon's refusal, or its silence.
        fn failure(&self) -> FuseHandoffStepError {
            if self.refuses.load(Ordering::Acquire) {
                FuseHandoffStepError::Rejected("the test daemon refused the step".into())
            } else {
                unacknowledged()
            }
        }
    }

    fn unacknowledged() -> FuseHandoffStepError {
        FuseHandoffStepError::Unacknowledged("the test daemon did not answer".into())
    }

    fn rejected<E: std::fmt::Display>(error: E) -> FuseHandoffStepError {
        FuseHandoffStepError::Rejected(error.to_string())
    }

    impl FuseMountedMutationCoordinator for TestMountedMutationCoordinator {
        type Admission<'a> = MutexGuard<'a, ()>;
        type Lease<'a> = TestMountedMutationLease;

        fn admit(&self) -> std::result::Result<Self::Admission<'_>, ()> {
            self.mutation_lane.lock().map_err(|_| ())
        }

        fn is_gated(&self) -> bool {
            self.gated.load(Ordering::Acquire)
        }

        fn gate_before_mount(&self) {
            self.record("gate-before-mount");
            self.gated.store(true, Ordering::Release);
        }

        fn retire_gated_session(&self) -> std::result::Result<(), ()> {
            self.record("retire-gated-session");
            Ok(())
        }

        fn prepare(
            &self,
            operation: FuseMountedMutationOperation,
            stable_operation_id: Option<String>,
        ) -> std::result::Result<Self::Lease<'_>, FuseHandoffStepError> {
            self.record("prepare");
            if self.outbox_full.swap(false, Ordering::AcqRel) {
                return Err(unacknowledged());
            }
            self.prepared_operations
                .lock()
                .expect("prepared operation lock")
                .push((operation, stable_operation_id));
            Ok(TestMountedMutationLease)
        }

        fn pause_before_resend(
            &self,
            _seen_events: u64,
            _attempt: u32,
            _deadline: Option<Instant>,
        ) -> bool {
            if !self.answers_on_resend.load(Ordering::Acquire) {
                return false;
            }
            self.record("resend");
            self.fail_arm.store(false, Ordering::Release);
            self.fail_commit.store(false, Ordering::Release);
            true
        }

        fn abort_unstarted(
            &self,
            _lease: &Self::Lease<'_>,
        ) -> std::result::Result<(), FuseHandoffStepError> {
            self.record("abort-unstarted");
            Ok(())
        }

        fn release_refused(&self, _lease: Self::Lease<'_>, _detail: &str) {
            self.record("release-refused");
        }

        fn finish_later(&self, _lease: Self::Lease<'_>, _step: FuseUnfinishedHandoff) {
            self.record("finish-later");
        }

        fn arm(
            &self,
            _lease: &mut Self::Lease<'_>,
        ) -> std::result::Result<dfs::MountedSifsMutationArmRequestId, FuseHandoffStepError>
        {
            self.record("arm");
            if self.fail_arm.load(Ordering::Acquire) {
                Err(self.failure())
            } else {
                self.record("arm-ack");
                Ok(dfs::MountedSifsMutationArmRequestId::generate())
            }
        }

        fn persist_captured(
            &self,
            _lease: &mut Self::Lease<'_>,
            _arm_request_id: &dfs::MountedSifsMutationArmRequestId,
            captured_work: &dfs::MountedSifsMutationCapturedWork,
        ) -> std::result::Result<(), FuseHandoffStepError> {
            self.record("persist-captured");
            self.captured_work
                .lock()
                .expect("captured work lock")
                .push(captured_work.clone());
            Ok(())
        }

        fn persist_unknown(
            &self,
            _lease: &mut Self::Lease<'_>,
            _arm_request_id: &dfs::MountedSifsMutationArmRequestId,
            observation: &dfs::MountedSifsMutationErrorObservation,
        ) -> std::result::Result<(), FuseHandoffStepError> {
            self.record("persist-unknown");
            self.unknown_observations
                .lock()
                .expect("observation lock")
                .push(observation.clone());
            Ok(())
        }

        fn persist_proven_no_write(
            &self,
            _lease: &mut Self::Lease<'_>,
            _arm_request_id: &dfs::MountedSifsMutationArmRequestId,
            _proof_code: &'static str,
        ) -> std::result::Result<(), FuseHandoffStepError> {
            self.record("persist-proven-no-write");
            Ok(())
        }

        fn commit_captured(
            &self,
            _lease: &Self::Lease<'_>,
            _arm_request_id: &dfs::MountedSifsMutationArmRequestId,
            _captured_work: &dfs::MountedSifsMutationCapturedWork,
        ) -> std::result::Result<(), FuseHandoffStepError> {
            self.record("commit-captured");
            if let Some(service_lock) = self.service_lock.as_ref() {
                self.commit_saw_service_lock_free
                    .store(service_lock.try_lock().is_ok(), Ordering::Release);
            }
            if let Some(block) = self.commit_block.as_ref() {
                let (state, wake) = &**block;
                let mut state = state.lock().expect("commit block lock");
                state.0 = true;
                wake.notify_all();
                while !state.1 {
                    state = wake.wait(state).expect("commit block wait");
                }
            }
            if self.fail_commit.load(Ordering::Acquire) {
                Err(self.failure())
            } else {
                self.record("commit-ack");
                Ok(())
            }
        }

        fn commit_unknown(
            &self,
            _lease: &Self::Lease<'_>,
            _arm_request_id: &dfs::MountedSifsMutationArmRequestId,
            _observation: &dfs::MountedSifsMutationErrorObservation,
        ) -> std::result::Result<(), FuseHandoffStepError> {
            self.record("commit-unknown");
            self.record("commit-ack");
            Ok(())
        }

        fn commit_proven_no_write(
            &self,
            _lease: &Self::Lease<'_>,
            _arm_request_id: &dfs::MountedSifsMutationArmRequestId,
            _proof_code: &'static str,
        ) -> std::result::Result<(), FuseHandoffStepError> {
            self.record("commit-proven-no-write");
            self.record("commit-ack");
            Ok(())
        }

        fn release_after_daemon_ack(&self, _lease: Self::Lease<'_>) -> std::result::Result<(), ()> {
            self.record("release");
            Ok(())
        }
    }

    #[derive(Debug)]
    struct TestStoreMountOpenCoordinator {
        store: dfs::MountedSifsMutationOutboxStore,
        session: dfs::MountedSifsHostMountSessionLease,
        gated: AtomicBool,
        mutation_lane: Mutex<()>,
    }

    impl FuseMountedMutationCoordinator for TestStoreMountOpenCoordinator {
        type Admission<'a> = MutexGuard<'a, ()>;
        type Lease<'a> = dfs::MountedSifsMutationLease<'a>;

        fn admit(&self) -> std::result::Result<Self::Admission<'_>, ()> {
            self.mutation_lane.lock().map_err(|_| ())
        }

        fn is_gated(&self) -> bool {
            self.gated.load(Ordering::Acquire)
        }

        fn gate_before_mount(&self) {
            self.gated.store(true, Ordering::Release);
        }

        fn retire_gated_session(&self) -> std::result::Result<(), ()> {
            self.session.close_clean_shared().map_err(|_| ())
        }

        fn prepare(
            &self,
            operation: FuseMountedMutationOperation,
            stable_operation_id: Option<String>,
        ) -> std::result::Result<Self::Lease<'_>, FuseHandoffStepError> {
            self.store
                .prepare(&self.session, stable_operation_id, operation.label())
                .map_err(rejected)
        }

        fn arm(
            &self,
            lease: &mut Self::Lease<'_>,
        ) -> std::result::Result<dfs::MountedSifsMutationArmRequestId, FuseHandoffStepError>
        {
            let arm_request_id = dfs::MountedSifsMutationArmRequestId::generate();
            let marker = lease.marker();
            self.store
                .arm_live(&marker.marker_id, &marker.marker_nonce, &arm_request_id)
                .map_err(rejected)?;
            Ok(arm_request_id)
        }

        fn persist_captured(
            &self,
            lease: &mut Self::Lease<'_>,
            arm_request_id: &dfs::MountedSifsMutationArmRequestId,
            captured_work: &dfs::MountedSifsMutationCapturedWork,
        ) -> std::result::Result<(), FuseHandoffStepError> {
            lease
                .record_captured_success(arm_request_id, captured_work.clone())
                .map_err(rejected)
        }

        fn persist_unknown(
            &self,
            lease: &mut Self::Lease<'_>,
            arm_request_id: &dfs::MountedSifsMutationArmRequestId,
            observation: &dfs::MountedSifsMutationErrorObservation,
        ) -> std::result::Result<(), FuseHandoffStepError> {
            lease
                .record_unknown_after_error(arm_request_id, observation.clone())
                .map_err(rejected)
        }

        fn persist_proven_no_write(
            &self,
            lease: &mut Self::Lease<'_>,
            arm_request_id: &dfs::MountedSifsMutationArmRequestId,
            proof_code: &'static str,
        ) -> std::result::Result<(), FuseHandoffStepError> {
            lease
                .record_proven_no_write(arm_request_id, proof_code)
                .map_err(rejected)
        }

        fn commit_captured(
            &self,
            lease: &Self::Lease<'_>,
            arm_request_id: &dfs::MountedSifsMutationArmRequestId,
            captured_work: &dfs::MountedSifsMutationCapturedWork,
        ) -> std::result::Result<(), FuseHandoffStepError> {
            let marker = lease.marker();
            self.store
                .accept_captured_live(
                    &marker.marker_id,
                    &marker.marker_nonce,
                    arm_request_id,
                    captured_work,
                )
                .map(drop)
                .map_err(rejected)
        }

        fn commit_unknown(
            &self,
            lease: &Self::Lease<'_>,
            arm_request_id: &dfs::MountedSifsMutationArmRequestId,
            observation: &dfs::MountedSifsMutationErrorObservation,
        ) -> std::result::Result<(), FuseHandoffStepError> {
            let marker = lease.marker();
            self.store
                .accept_unknown_live(
                    &marker.marker_id,
                    &marker.marker_nonce,
                    arm_request_id,
                    observation,
                )
                .map(drop)
                .map_err(rejected)
        }

        fn commit_proven_no_write(
            &self,
            lease: &Self::Lease<'_>,
            arm_request_id: &dfs::MountedSifsMutationArmRequestId,
            proof_code: &'static str,
        ) -> std::result::Result<(), FuseHandoffStepError> {
            let marker = lease.marker();
            self.store
                .prove_no_write_live(
                    &marker.marker_id,
                    &marker.marker_nonce,
                    arm_request_id,
                    proof_code,
                )
                .map(drop)
                .map_err(rejected)
        }

        fn release_after_daemon_ack(&self, lease: Self::Lease<'_>) -> std::result::Result<(), ()> {
            lease.release_after_daemon_ack().map_err(|_| ())
        }
    }

    fn test_mutation_report<R>(
        outcome: std::result::Result<R, FuseMountedMutationError>,
        committed_work: dfs::DfsCommittedWork,
    ) -> dfs::MountedSifsMutationReport<std::result::Result<R, FuseMountedMutationError>> {
        dfs::MountedSifsMutationReport {
            outcome,
            committed_work,
        }
    }

    #[test]
    fn an_unknown_commit_outcome_is_an_io_error_and_a_pending_write_is_retriable() {
        let unknown = || {
            DfsPosixError::Dfs(DfsError::MetadataCommitOutcomeUnknown {
                file_system_id: dfs::sifs_file_system_id(),
                shard_id: "root".into(),
                epoch: 1,
                operation_id: "write:dfs_write_op_1".into(),
                cause: dfs::DfsCommitOutcomeUnknownCause::Deadline,
                current_primary_node_id: None,
            })
        };
        let error = FuseMountedMutationError::from(unknown());
        assert_eq!(error.errno, libc::EIO);
        assert_eq!(error.error_code, "dfs-posix-commit-outcome-unknown");
        assert_eq!(errno_for_posix_error(unknown()), libc::EIO);
        // A sync the write set holds but cannot yet certify, as a directory
        // sync below the shard's W floor reports it, is a typed outcome with
        // its own errno.
        assert_eq!(
            errno_for_posix_error(DfsPosixError::PendingWrite {
                intent_id: "directory-sync:dir@v1".into(),
                durable_receipts: 1,
                required_receipts: 2,
            }),
            libc::EAGAIN
        );
    }

    #[test]
    fn every_durable_fuse_mutation_has_one_bounded_handoff_label() {
        let labels = FuseMountedMutationOperation::ALL
            .into_iter()
            .map(FuseMountedMutationOperation::label)
            .collect::<BTreeSet<_>>();
        assert_eq!(labels.len(), FuseMountedMutationOperation::ALL.len());
        assert!(labels.iter().all(|label| label.starts_with("fuse.")));
        assert!(
            labels
                .iter()
                .all(|label| { label.len() <= dfs::MOUNTED_SIFS_MUTATION_OUTBOX_MAX_LABEL_BYTES })
        );
    }

    #[test]
    fn clustered_mount_open_orders_probe_session_arm_service_and_daemon_ack() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let events_for_probe = Arc::clone(&events);
        let events_for_session = Arc::clone(&events);
        let coordinator = prepare_fuse_clustered_mount_handoff_with(
            (),
            move |_| {
                events_for_probe.lock().expect("event lock").push("probe");
                Ok::<_, ()>(())
            },
            move |_| {
                events_for_session
                    .lock()
                    .expect("event lock")
                    .push("session");
                Ok::<_, ()>(TestMountedMutationCoordinator {
                    events: Arc::clone(&events_for_session),
                    ..TestMountedMutationCoordinator::default()
                })
            },
        )
        .expect("prepare test mount handoff");
        let service_open_events = Arc::clone(&events);
        let opened = run_fuse_clustered_mount_open_with(
            &coordinator,
            move || {
                service_open_events
                    .lock()
                    .expect("event lock")
                    .push("service-open-bootstrap");
                Ok::<_, ()>(7usize)
            },
        )
        .expect("mount-open handoff");

        assert_eq!(opened, 7);
        assert_eq!(
            *events.lock().expect("event lock"),
            [
                "probe",
                "session",
                "prepare",
                "arm",
                "arm-ack",
                "service-open-bootstrap",
                "persist-captured",
                "commit-captured",
                "commit-ack",
                "release",
            ]
        );
        assert_eq!(
            *coordinator
                .prepared_operations
                .lock()
                .expect("prepared operation lock"),
            [(FuseMountedMutationOperation::MountOpen, None)]
        );
        let captured = coordinator
            .captured_work
            .lock()
            .expect("captured work lock");
        assert_eq!(captured.len(), 1);
        let work = &captured[0];
        assert!(work.committed_work.publish_metadata);
        assert!(work.committed_work.repair_maintenance);
        assert!(work.committed_work.namespace_maintenance);
        assert!(work.committed_work.metadata_replica_progress);
        assert!(work.committed_work.metadata_materialization);
    }

    #[test]
    fn clustered_mount_open_error_persists_unknown_without_fatal_mount_gate() {
        let coordinator = TestMountedMutationCoordinator::default();
        let result = run_fuse_clustered_mount_open_with(
            &coordinator,
            || {
                coordinator.record("service-open-bootstrap");
                Err::<usize, _>("open-failed")
            },
        );

        assert!(matches!(
            result,
            Err(FuseMountOpenHandoffError::Service("open-failed"))
        ));
        assert!(coordinator.is_gated());
        assert_eq!(
            *coordinator.events.lock().expect("event lock"),
            [
                "prepare",
                "arm",
                "arm-ack",
                "service-open-bootstrap",
                "gate-before-mount",
                "persist-unknown",
                "commit-unknown",
                "commit-ack",
                "release",
                "retire-gated-session",
            ]
        );
        let observations = coordinator
            .unknown_observations
            .lock()
            .expect("observation lock");
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].error_code, "fuse-mount-open-service-error");
        assert!(
            observations[0]
                .captured_work
                .committed_work
                .publish_metadata
        );
        assert!(
            observations[0]
                .captured_work
                .committed_work
                .metadata_materialization
        );
        assert!(
            !coordinator
                .events
                .lock()
                .expect("event lock")
                .contains(&"gate"),
            "pre-FUSE failures must not invoke the fatal mounted gate"
        );
    }

    #[test]
    fn clustered_mount_open_service_error_retains_full_recovery_marker() {
        let root = sinomo_testdir::TestDir::new("sinomo-fuse-mount-open-error-marker");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let store = dfs::MountedSifsMutationOutboxStore::new(&data_dir).expect("outbox");
        let session = store
            .open_host_mount_session(
                dfs::MountedSifsMutationAdapter::Fuse,
                "fuse:test-mount-open-error",
            )
            .expect("host session");
        let coordinator = TestStoreMountOpenCoordinator {
            store: store.clone(),
            session,
            gated: AtomicBool::new(false),
            mutation_lane: Mutex::new(()),
        };

        let result = run_fuse_clustered_mount_open_with(
            &coordinator,
            || Err::<usize, _>("service-open-error"),
        );

        assert!(matches!(
            result,
            Err(FuseMountOpenHandoffError::Service("service-open-error"))
        ));
        let status = store.status().expect("post-error status");
        assert_eq!(status.pending_marker_count, 1);
        assert_eq!(status.full_recovery_marker_count, 1);
        let marker_ids = store
            .recovery_candidate_marker_ids()
            .expect("recovery candidates");
        assert_eq!(marker_ids.len(), 1);
        let marker = store
            .marker(&marker_ids[0])
            .expect("read marker")
            .expect("retained marker");
        assert_eq!(
            marker.origin.operation,
            FuseMountedMutationOperation::MountOpen.label()
        );
        assert!(matches!(
            marker.phase,
            dfs::MountedSifsMutationOutboxPhase::DaemonAccepted {
                requires_full_recovery: true,
                error_observation: Some(observation),
                ..
            } if observation.error_code == "fuse-mount-open-service-error"
        ));
    }

    #[test]
    fn clustered_mount_open_crash_after_arm_retains_full_recovery_marker() {
        let root = sinomo_testdir::TestDir::new("sinomo-fuse-mount-open-arm-crash");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let store = dfs::MountedSifsMutationOutboxStore::new(&data_dir).expect("outbox");
        let session = store
            .open_host_mount_session(
                dfs::MountedSifsMutationAdapter::Fuse,
                "fuse:test-mount-open-crash",
            )
            .expect("host session");
        let lease = store
            .prepare(
                &session,
                None,
                FuseMountedMutationOperation::MountOpen.label(),
            )
            .expect("prepare mount-open");
        let marker_id = lease.marker().marker_id.clone();
        let marker_nonce = lease.marker().marker_nonce.clone();
        let arm_request_id = dfs::MountedSifsMutationArmRequestId::generate();
        store
            .arm_live(&marker_id, &marker_nonce, &arm_request_id)
            .expect("arm mount-open");

        // Simulate a hard process stop after backend entry but before capture:
        // both liveness leases disappear without authoring a no-write proof.
        drop(lease);
        drop(session);

        let status = store.status().expect("post-crash status");
        assert_eq!(status.pending_marker_count, 1);
        assert_eq!(status.full_recovery_marker_count, 1);
        let marker = store
            .marker(&marker_id)
            .expect("read marker")
            .expect("retained marker");
        assert_eq!(
            marker.origin.operation,
            FuseMountedMutationOperation::MountOpen.label()
        );
        assert!(matches!(
            marker.phase,
            dfs::MountedSifsMutationOutboxPhase::Armed {
                arm_request_id: current,
            } if current == arm_request_id
        ));
    }

    #[test]
    fn every_advisory_lock_command_maps_to_its_durable_handoff_operation() {
        let object_id = test_object_id("fusehandofflockobject");
        let token = test_grant(
            object_id,
            "fuse-handoff-lock-grant",
            dfs::DfsAdvisoryLockMode::Exclusive,
        )
        .token;
        let session_id = dfs::DfsAdvisoryLockSessionId::new("fuse-handoff-lock-session");
        let acquire_request_id = dfs::DfsAdvisoryLockRequestId::new("fuse-handoff-lock-acquire");
        let commands = [
            dfs::DfsAdvisoryLockCommand::Acquire {
                request_id: acquire_request_id.clone(),
                session_id: session_id.clone(),
                mode: dfs::DfsAdvisoryLockMode::Exclusive,
                blocking: true,
                process_id: Some(42),
            },
            dfs::DfsAdvisoryLockCommand::Query {
                request_id: dfs::DfsAdvisoryLockRequestId::new("fuse-handoff-lock-query"),
                session_id: session_id.clone(),
                mode: dfs::DfsAdvisoryLockMode::Exclusive,
            },
            dfs::DfsAdvisoryLockCommand::Renew {
                request_id: dfs::DfsAdvisoryLockRequestId::new("fuse-handoff-lock-renew"),
                token: token.clone(),
            },
            dfs::DfsAdvisoryLockCommand::Convert {
                request_id: dfs::DfsAdvisoryLockRequestId::new("fuse-handoff-lock-convert"),
                token: token.clone(),
                mode: dfs::DfsAdvisoryLockMode::Shared,
            },
            dfs::DfsAdvisoryLockCommand::Unlock {
                request_id: dfs::DfsAdvisoryLockRequestId::new("fuse-handoff-lock-unlock"),
                token,
            },
            dfs::DfsAdvisoryLockCommand::Cancel {
                request_id: dfs::DfsAdvisoryLockRequestId::new("fuse-handoff-lock-cancel"),
                session_id: session_id.clone(),
                acquire_request_id,
            },
            dfs::DfsAdvisoryLockCommand::Close {
                request_id: dfs::DfsAdvisoryLockRequestId::new("fuse-handoff-lock-close"),
                session_id,
            },
        ];
        let expected = [
            FuseMountedMutationOperation::AdvisoryLockAcquire,
            FuseMountedMutationOperation::AdvisoryLockQuery,
            FuseMountedMutationOperation::AdvisoryLockRenew,
            FuseMountedMutationOperation::AdvisoryLockConvert,
            FuseMountedMutationOperation::AdvisoryLockUnlock,
            FuseMountedMutationOperation::AdvisoryLockCancel,
            FuseMountedMutationOperation::AdvisoryLockClose,
        ];

        assert_eq!(
            commands
                .iter()
                .map(fuse_advisory_lock_mutation_operation)
                .collect::<Vec<_>>(),
            expected
        );
    }

    #[test]
    fn foreground_advisory_lock_mutation_uses_the_acknowledged_handoff() {
        let coordinator = TestMountedMutationCoordinator::default();
        let service = MountLock::new(0usize);
        let result = run_fuse_mounted_mutation_with_lock(
            &coordinator,
            &service,
            FuseMountedMutationOperation::AdvisoryLockAcquire,
            Some("fuse-foreground-lock-acquire".to_string()),
            |service| service.lock().map_err(|_| libc::EIO),
            |value| {
                *value += 1;
                test_mutation_report(Ok(*value), dfs::DfsCommittedWork::default())
            },
        );

        assert_eq!(result, Ok(1));
        assert_eq!(
            *coordinator
                .prepared_operations
                .lock()
                .expect("prepared operation lock"),
            [(
                FuseMountedMutationOperation::AdvisoryLockAcquire,
                Some("fuse-foreground-lock-acquire".to_string()),
            )]
        );
        assert_eq!(
            *coordinator.events.lock().expect("event lock"),
            [
                "prepare",
                "arm",
                "arm-ack",
                "persist-captured",
                "commit-captured",
                "commit-ack",
                "release",
            ]
        );
    }

    #[test]
    fn background_renewal_and_waiter_cancel_share_the_serialized_handoff() {
        let coordinator = TestMountedMutationCoordinator::default();
        let service = MountLock::new(0usize);
        let renewed = run_fuse_mounted_mutation_with_lock(
            &coordinator,
            &service,
            FuseMountedMutationOperation::AdvisoryLockRenew,
            Some("fuse-background-lock-renew".to_string()),
            |service| service.lock().map_err(|_| libc::EIO),
            |value| {
                *value += 1;
                test_mutation_report(Ok(*value), dfs::DfsCommittedWork::default())
            },
        );
        assert_eq!(renewed, Ok(1));

        let waiter_state = Arc::new(MountLock::new(FuseState::new("/".to_string())));
        let waiter_key = (test_object_id("fusecancelledwaiterobject"), 77);
        let acquire_request_id =
            dfs::DfsAdvisoryLockRequestId::new("fuse-cancelled-waiter-acquire");
        let waiter_state_for_preflight = Arc::clone(&waiter_state);
        let waiter_key_for_preflight = waiter_key.clone();
        let request_for_preflight = acquire_request_id.clone();
        let cancel_mutations = AtomicU64::new(0);
        let cancelled: std::result::Result<(), libc::c_int> =
            run_fuse_mounted_mutation_with_lock_and_preflight(
                &coordinator,
                &service,
                FuseMountedMutationOperation::AdvisoryLockCancel,
                Some("fuse-background-lock-cancel".to_string()),
                None,
                |service| service.lock().map_err(|_| libc::EIO),
                move |_| {
                    preflight_blocking_lock_acquire(
                        &waiter_state_for_preflight,
                        &waiter_key_for_preflight,
                        &request_for_preflight,
                    )
                },
                |_| {
                    cancel_mutations.fetch_add(1, Ordering::Relaxed);
                    test_mutation_report(Ok(()), dfs::DfsCommittedWork::default())
                },
            );

        assert_eq!(cancelled, Err(libc::EINTR));
        assert_eq!(cancel_mutations.load(Ordering::Relaxed), 0);
        assert!(!coordinator.is_gated());
        assert_eq!(
            *coordinator
                .prepared_operations
                .lock()
                .expect("prepared operation lock"),
            [
                (
                    FuseMountedMutationOperation::AdvisoryLockRenew,
                    Some("fuse-background-lock-renew".to_string()),
                ),
                (
                    FuseMountedMutationOperation::AdvisoryLockCancel,
                    Some("fuse-background-lock-cancel".to_string()),
                ),
            ]
        );
        let events = coordinator.events.lock().expect("event lock");
        assert!(events.contains(&"persist-captured"));
        assert!(events.contains(&"commit-captured"));
        assert!(events.contains(&"persist-proven-no-write"));
        assert!(events.contains(&"commit-proven-no-write"));
        assert!(!events.contains(&"persist-unknown"));
    }

    #[test]
    fn daemon_arm_ack_precedes_service_mutation() {
        let coordinator = TestMountedMutationCoordinator::default();
        coordinator.fail_arm.store(true, Ordering::Release);
        let service = MountLock::new(0usize);
        let mutation_calls = AtomicU64::new(0);

        let result: std::result::Result<(), libc::c_int> = run_fuse_mounted_mutation_with(
            &coordinator,
            &service,
            FuseMountedMutationOperation::Mkdir,
            |_| {
                mutation_calls.fetch_add(1, Ordering::Relaxed);
                test_mutation_report(Ok(()), dfs::DfsCommittedWork::default())
            },
        );

        assert_eq!(result, Err(libc::EIO));
        assert_eq!(mutation_calls.load(Ordering::Relaxed), 0);
        assert!(
            !coordinator.is_gated(),
            "an unacknowledged Arm retires nothing"
        );
        assert_eq!(
            *coordinator.events.lock().expect("event lock"),
            ["prepare", "arm", "finish-later"]
        );
        // The mount keeps serving once the daemon answers.
        coordinator.fail_arm.store(false, Ordering::Release);
        let served: std::result::Result<(), libc::c_int> = run_fuse_mounted_mutation_with(
            &coordinator,
            &service,
            FuseMountedMutationOperation::Mkdir,
            |_| {
                mutation_calls.fetch_add(1, Ordering::Relaxed);
                test_mutation_report(Ok(()), dfs::DfsCommittedWork::default())
            },
        );
        assert_eq!(served, Ok(()));
        assert_eq!(mutation_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn an_unacknowledged_handoff_step_is_sent_again_until_the_daemon_answers() {
        for fail in ["arm", "commit"] {
            let coordinator = TestMountedMutationCoordinator::default();
            coordinator.answers_on_resend.store(true, Ordering::Release);
            match fail {
                "arm" => coordinator.fail_arm.store(true, Ordering::Release),
                _ => coordinator.fail_commit.store(true, Ordering::Release),
            }
            let service = MountLock::new(0usize);
            let result: std::result::Result<(), libc::c_int> = run_fuse_mounted_mutation_with(
                &coordinator,
                &service,
                FuseMountedMutationOperation::Mkdir,
                |value| {
                    *value += 1;
                    test_mutation_report(Ok(()), dfs::DfsCommittedWork::default())
                },
            );
            assert_eq!(result, Ok(()), "{fail} was sent again and acknowledged");
            assert_eq!(
                *service.lock().expect("service lock"),
                1,
                "the mutation ran once"
            );
            let events = coordinator.events.lock().expect("event lock");
            assert!(events.contains(&"resend"));
            assert!(events.contains(&"release"));
            assert!(!events.contains(&"finish-later"));
            assert!(!coordinator.is_gated());
        }
    }

    #[test]
    fn directory_refresh_read_error_commits_captured_work_without_gating() {
        let coordinator = TestMountedMutationCoordinator::default();
        let service = MountLock::new(0usize);
        let committed_work = dfs::DfsCommittedWork {
            metadata_replica_progress: true,
            ..dfs::DfsCommittedWork::default()
        };
        let result: std::result::Result<std::result::Result<(), DfsPosixError>, libc::c_int> =
            run_fuse_mounted_mutation_with(
                &coordinator,
                &service,
                FuseMountedMutationOperation::DirectoryRefresh,
                |_| {
                    test_mutation_report(
                        Ok(Err(DfsPosixError::NotFound {
                            path: "/missing".to_string(),
                        })),
                        committed_work,
                    )
                },
            );

        assert!(matches!(result, Ok(Err(DfsPosixError::NotFound { .. }))));
        assert!(!coordinator.is_gated());
        assert_eq!(
            *coordinator.events.lock().expect("event lock"),
            [
                "prepare",
                "arm",
                "arm-ack",
                "persist-captured",
                "commit-captured",
                "commit-ack",
                "release",
            ]
        );
        assert!(
            coordinator
                .unknown_observations
                .lock()
                .expect("observation lock")
                .is_empty()
        );
    }

    #[test]
    fn daemon_commit_runs_without_the_service_lock_and_before_success_returns() {
        let service = Arc::new(MountLock::new(0usize));
        let commit_block = Arc::new((Mutex::new((false, false)), std::sync::Condvar::new()));
        let coordinator = Arc::new(TestMountedMutationCoordinator {
            service_lock: Some(Arc::clone(&service)),
            commit_block: Some(Arc::clone(&commit_block)),
            ..TestMountedMutationCoordinator::default()
        });
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let service_for_thread = Arc::clone(&service);
        let coordinator_for_thread = Arc::clone(&coordinator);
        let worker = thread::spawn(move || {
            let result = run_fuse_mounted_mutation_with(
                coordinator_for_thread.as_ref(),
                service_for_thread.as_ref(),
                FuseMountedMutationOperation::Rename,
                |value| {
                    *value += 1;
                    test_mutation_report(Ok(7usize), dfs::DfsCommittedWork::default())
                },
            );
            result_tx.send(result).expect("send result");
        });
        {
            let (state, wake) = &*commit_block;
            let mut state = state.lock().expect("commit block lock");
            while !state.0 {
                state = wake.wait(state).expect("commit entry wait");
            }
        }
        assert!(
            coordinator
                .commit_saw_service_lock_free
                .load(Ordering::Acquire)
        );
        assert!(matches!(
            result_rx.recv_timeout(Duration::from_millis(50)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        {
            let (state, wake) = &*commit_block;
            let mut state = state.lock().expect("commit block lock");
            state.1 = true;
            wake.notify_all();
        }
        assert_eq!(result_rx.recv_timeout(Duration::from_secs(2)), Ok(Ok(7)));
        worker.join().expect("mutation worker");
        let events = coordinator.events.lock().expect("event lock");
        assert_eq!(
            events.as_slice(),
            [
                "prepare",
                "arm",
                "arm-ack",
                "persist-captured",
                "commit-captured",
                "commit-ack",
                "release",
            ]
        );
    }

    #[test]
    fn service_error_persists_unknown_with_exact_work_and_the_mount_keeps_serving() {
        let coordinator = TestMountedMutationCoordinator::default();
        let service = MountLock::new(0usize);
        let committed_work = dfs::DfsCommittedWork {
            publish_metadata: true,
            ..dfs::DfsCommittedWork::default()
        };
        let result: std::result::Result<(), libc::c_int> = run_fuse_mounted_mutation_with(
            &coordinator,
            &service,
            FuseMountedMutationOperation::ImmediateWritePublication,
            |_| {
                test_mutation_report(
                    Err(FuseMountedMutationError {
                        errno: libc::EIO,
                        error_code: "test-unknown-write",
                    }),
                    committed_work,
                )
            },
        );
        assert_eq!(result, Err(libc::EIO));
        assert!(
            !coordinator.is_gated(),
            "an unknown outcome retires nothing"
        );
        let observations = coordinator
            .unknown_observations
            .lock()
            .expect("observation lock");
        assert_eq!(observations.len(), 1);
        assert!(
            observations[0]
                .captured_work
                .committed_work
                .publish_metadata
        );
        drop(observations);
        let later_mutations = AtomicU64::new(0);
        let later = run_fuse_mounted_mutation_with(
            &coordinator,
            &service,
            FuseMountedMutationOperation::Mkdir,
            |_| {
                later_mutations.fetch_add(1, Ordering::Relaxed);
                test_mutation_report(Ok(()), dfs::DfsCommittedWork::default())
            },
        );
        assert_eq!(later, Ok(()), "the mount keeps serving");
        assert_eq!(later_mutations.load(Ordering::Relaxed), 1);
        let events = coordinator.events.lock().expect("event lock");
        assert!(events.contains(&"persist-unknown"));
        assert!(events.contains(&"persist-captured"));
        assert!(!events.contains(&"gate"));
        assert!(!events.contains(&"retire-gated-session"));
    }

    #[test]
    fn typed_error_returns_its_errno_and_the_mount_keeps_serving() {
        let coordinator = TestMountedMutationCoordinator::default();
        let service = MountLock::new(0usize);
        let result: std::result::Result<(), libc::c_int> = run_fuse_mounted_mutation_with(
            &coordinator,
            &service,
            FuseMountedMutationOperation::Unlink,
            |_| {
                test_mutation_report(
                    Err(FuseMountedMutationError::from(DfsPosixError::NotFound {
                        path: "/missing".to_string(),
                    })),
                    dfs::DfsCommittedWork::default(),
                )
            },
        );

        assert_eq!(result, Err(libc::ENOENT));
        assert!(!coordinator.is_gated(), "a definite error retires nothing");
        {
            // An empty capture is still handed over as observed, not as a
            // proof that nothing was written.
            let events = coordinator.events.lock().expect("event lock");
            assert!(events.contains(&"persist-unknown"));
            assert!(!events.contains(&"persist-proven-no-write"));
            assert!(!events.contains(&"gate"));
        }
        for (error, errno) in [
            (
                DfsPosixError::Dfs(DfsError::Busy {
                    path: "/moving".to_string(),
                }),
                libc::EAGAIN,
            ),
            (
                DfsPosixError::Conflict {
                    path: "/changed".to_string(),
                    reason: "generation changed".to_string(),
                },
                libc::ESTALE,
            ),
            (
                DfsPosixError::Dfs(DfsError::NetworkDelivery {
                    message: "not sent".to_string(),
                    delivery_state:
                        sinomo_runtime::network::NetworkDeliveryState::DefinitelyNotSent,
                }),
                libc::EIO,
            ),
        ] {
            let result: std::result::Result<(), libc::c_int> = run_fuse_mounted_mutation_with(
                &coordinator,
                &service,
                FuseMountedMutationOperation::Rename,
                |_| {
                    test_mutation_report(
                        Err(FuseMountedMutationError::from(error)),
                        dfs::DfsCommittedWork::default(),
                    )
                },
            );
            assert_eq!(result, Err(errno));
            assert!(!coordinator.is_gated());
        }
        let served: std::result::Result<(), libc::c_int> = run_fuse_mounted_mutation_with(
            &coordinator,
            &service,
            FuseMountedMutationOperation::Mkdir,
            |_| test_mutation_report(Ok(()), dfs::DfsCommittedWork::default()),
        );
        assert_eq!(served, Ok(()), "the mount keeps serving");
    }

    #[test]
    fn failure_before_service_entry_attests_proven_no_write() {
        let coordinator = TestMountedMutationCoordinator::default();
        let service = Arc::new(MountLock::new(0usize));
        let service_to_poison = Arc::clone(&service);
        assert!(
            thread::spawn(move || {
                let _service = service_to_poison.lock().expect("service lock");
                panic!("poison service lane before mutation entry");
            })
            .join()
            .is_err()
        );
        let mutation_calls = AtomicU64::new(0);
        let result: std::result::Result<(), libc::c_int> = run_fuse_mounted_mutation_with(
            &coordinator,
            service.as_ref(),
            FuseMountedMutationOperation::Mkdir,
            |_| {
                mutation_calls.fetch_add(1, Ordering::Relaxed);
                test_mutation_report(Ok(()), dfs::DfsCommittedWork::default())
            },
        );

        assert_eq!(result, Err(libc::EIO));
        assert_eq!(mutation_calls.load(Ordering::Relaxed), 0);
        let events = coordinator.events.lock().expect("event lock");
        assert!(events.contains(&"persist-proven-no-write"));
        assert!(events.contains(&"commit-proven-no-write"));
        assert!(!events.contains(&"persist-unknown"));
    }

    #[test]
    fn mutation_admission_stays_closed_until_the_prior_daemon_ack() {
        let service = Arc::new(MountLock::new(0usize));
        let commit_block = Arc::new((Mutex::new((false, false)), std::sync::Condvar::new()));
        let coordinator = Arc::new(TestMountedMutationCoordinator {
            fail_commit: AtomicBool::new(true),
            commit_block: Some(Arc::clone(&commit_block)),
            ..TestMountedMutationCoordinator::default()
        });
        let (first_tx, first_rx) = std::sync::mpsc::channel();
        let first_service = Arc::clone(&service);
        let first_coordinator = Arc::clone(&coordinator);
        let first_worker = thread::spawn(move || {
            let result = run_fuse_mounted_mutation_with(
                first_coordinator.as_ref(),
                first_service.as_ref(),
                FuseMountedMutationOperation::SetMode,
                |value| {
                    *value += 1;
                    test_mutation_report(Ok(()), dfs::DfsCommittedWork::default())
                },
            );
            first_tx.send(result).expect("send first result");
        });
        {
            let (state, wake) = &*commit_block;
            let mut state = state.lock().expect("commit block lock");
            while !state.0 {
                state = wake.wait(state).expect("commit entry wait");
            }
        }

        let second_mutations = Arc::new(AtomicU64::new(0));
        let (second_tx, second_rx) = std::sync::mpsc::channel();
        let second_service = Arc::clone(&service);
        let second_coordinator = Arc::clone(&coordinator);
        let second_mutations_for_worker = Arc::clone(&second_mutations);
        let second_worker = thread::spawn(move || {
            let result = run_fuse_mounted_mutation_with(
                second_coordinator.as_ref(),
                second_service.as_ref(),
                FuseMountedMutationOperation::SetTimes,
                |value| {
                    second_mutations_for_worker.fetch_add(1, Ordering::Relaxed);
                    *value += 1;
                    test_mutation_report(Ok(()), dfs::DfsCommittedWork::default())
                },
            );
            second_tx.send(result).expect("send second result");
        });
        assert!(matches!(
            second_rx.recv_timeout(Duration::from_millis(50)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        assert_eq!(second_mutations.load(Ordering::Relaxed), 0);
        assert_eq!(*service.lock().expect("service lock"), 1);

        {
            let (state, wake) = &*commit_block;
            let mut state = state.lock().expect("commit block lock");
            state.1 = true;
            wake.notify_all();
        }
        assert_eq!(
            first_rx.recv_timeout(Duration::from_secs(2)),
            Ok(Err(libc::EIO))
        );
        // The mount keeps serving: the second mutation runs once the first
        // handoff has left the lane, and meets the same silent daemon.
        assert_eq!(
            second_rx.recv_timeout(Duration::from_secs(2)),
            Ok(Err(libc::EIO))
        );
        assert_eq!(second_mutations.load(Ordering::Relaxed), 1);
        assert_eq!(*service.lock().expect("service lock"), 2);
        first_worker.join().expect("first mutation worker");
        second_worker.join().expect("second mutation worker");
    }

    #[test]
    fn deadline_while_waiting_for_the_service_never_enters_the_mutation() {
        let service = MountLock::new(0usize);
        let held = service.lock().unwrap();
        let coordinator = TestMountedMutationCoordinator::default();
        let result = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    run_fuse_mounted_mutation_until(
                        &coordinator,
                        &service,
                        FuseMountedMutationOperation::Mkdir,
                        Some(Instant::now() + Duration::from_millis(40)),
                        |_value| -> dfs::MountedSifsMutationReport<
                            std::result::Result<(), FuseMountedMutationError>,
                        > {
                            panic!("the service lane is held past the syscall deadline")
                        },
                    )
                })
                .join()
                .unwrap()
        });
        assert_eq!(result, Err(libc::EIO));
        assert_eq!(*held, 0);
    }

    #[test]
    fn unacknowledged_commit_never_reports_success_and_the_mount_keeps_serving() {
        let coordinator = TestMountedMutationCoordinator::default();
        coordinator.fail_commit.store(true, Ordering::Release);
        let service = MountLock::new(0usize);
        let first = run_fuse_mounted_mutation_with(
            &coordinator,
            &service,
            FuseMountedMutationOperation::SetMode,
            |_| test_mutation_report(Ok(()), dfs::DfsCommittedWork::default()),
        );
        assert_eq!(first, Err(libc::EIO));
        assert!(!coordinator.is_gated());
        {
            let events = coordinator.events.lock().expect("event lock");
            assert!(!events.contains(&"release"));
            assert!(
                events.contains(&"finish-later"),
                "the marker is handed to the re-sender"
            );
        }
        coordinator.fail_commit.store(false, Ordering::Release);
        let later_mutations = AtomicU64::new(0);
        let second = run_fuse_mounted_mutation_with(
            &coordinator,
            &service,
            FuseMountedMutationOperation::SetTimes,
            |_| {
                later_mutations.fetch_add(1, Ordering::Relaxed);
                test_mutation_report(Ok(()), dfs::DfsCommittedWork::default())
            },
        );
        assert_eq!(second, Ok(()));
        assert_eq!(later_mutations.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn a_refused_arm_ends_the_syscall_aborts_its_marker_and_frees_the_lane() {
        let coordinator = TestMountedMutationCoordinator::default();
        coordinator.refuses.store(true, Ordering::Release);
        coordinator.fail_arm.store(true, Ordering::Release);
        // A re-send would clear the failure: a refusal must end without one.
        coordinator.answers_on_resend.store(true, Ordering::Release);
        let service = MountLock::new(0usize);
        let refused: std::result::Result<(), libc::c_int> = run_fuse_mounted_mutation_with(
            &coordinator,
            &service,
            FuseMountedMutationOperation::Mkdir,
            |_| panic!("a refused Arm never enters the service"),
        );
        assert_eq!(refused, Err(libc::EIO));
        {
            let events = coordinator.events.lock().expect("event lock");
            assert_eq!(
                events.as_slice(),
                ["prepare", "arm", "abort-unstarted", "release"],
                "the refusal was not sent again and its marker was aborted"
            );
        }
        assert!(!coordinator.is_gated());
        coordinator.fail_arm.store(false, Ordering::Release);
        let next = run_fuse_mounted_mutation_with(
            &coordinator,
            &service,
            FuseMountedMutationOperation::Mkdir,
            |value| {
                *value += 1;
                test_mutation_report(Ok(()), dfs::DfsCommittedWork::default())
            },
        );
        assert_eq!(next, Ok(()), "the mutation lane was released");
    }

    #[test]
    fn a_refused_commit_ends_the_syscall_and_gives_up_its_marker() {
        let coordinator = TestMountedMutationCoordinator::default();
        coordinator.refuses.store(true, Ordering::Release);
        coordinator.fail_commit.store(true, Ordering::Release);
        coordinator.answers_on_resend.store(true, Ordering::Release);
        let service = MountLock::new(0usize);
        let refused = run_fuse_mounted_mutation_with(
            &coordinator,
            &service,
            FuseMountedMutationOperation::SetMode,
            |value| {
                *value += 1;
                test_mutation_report(Ok(()), dfs::DfsCommittedWork::default())
            },
        );
        assert_eq!(
            refused,
            Err(libc::EIO),
            "the syscall does not report success before the daemon owns the work"
        );
        let events = coordinator.events.lock().expect("event lock").clone();
        assert!(events.contains(&"release-refused"));
        assert!(!events.contains(&"resend"));
        assert!(!events.contains(&"finish-later"));
        assert!(!coordinator.is_gated());
    }

    #[test]
    fn a_full_outbox_is_waited_on_and_the_mutation_runs_once_there_is_room() {
        let coordinator = TestMountedMutationCoordinator::default();
        coordinator.outbox_full.store(true, Ordering::Release);
        coordinator.answers_on_resend.store(true, Ordering::Release);
        let service = MountLock::new(0usize);
        let result = run_fuse_mounted_mutation_with(
            &coordinator,
            &service,
            FuseMountedMutationOperation::Mkdir,
            |value| {
                *value += 1;
                test_mutation_report(Ok(()), dfs::DfsCommittedWork::default())
            },
        );
        assert_eq!(result, Ok(()));
        let events = coordinator.events.lock().expect("event lock");
        assert_eq!(
            &events[..3],
            ["prepare", "resend", "prepare"],
            "Prepare was made again instead of the mutation being refused"
        );
    }

    type TestDaemon = dfs::MountedSifsHandoffTestDaemon;

    /// The production FUSE handoff over a real outbox, with a daemon the test
    /// makes silent or makes refuse.
    struct TestFuseHandoff {
        handoff: FuseMountedMutationHandoff,
        daemon: Arc<TestDaemon>,
        store: dfs::MountedSifsMutationOutboxStore,
        _root: sinomo_testdir::TestDir,
    }

    impl TestFuseHandoff {
        fn start(name: &str) -> Self {
            let root = sinomo_testdir::TestDir::new(name);
            let store = dfs::MountedSifsMutationOutboxStore::new(&SinomoDataDir::new(root.path()))
                .expect("open the mount's outbox");
            let session = store
                .open_host_mount_session(
                    dfs::MountedSifsMutationAdapter::Fuse,
                    format!("fuse-root:{name}"),
                )
                .expect("open the mount's session");
            let daemon = TestDaemon::new(store.clone());
            let handoff = FuseMountedMutationHandoff::new(
                dfs::MountedSifsMutationHandoff::start(
                    store.clone(),
                    session,
                    daemon.clone(),
                    None,
                )
                .expect("start the mount's handoff"),
            );
            Self {
                handoff,
                daemon,
                store,
                _root: root,
            }
        }

        fn unfinished(&self) -> usize {
            self.handoff.handoff.unfinished_count()
        }

        fn status(&self) -> dfs::MountedSifsMutationOutboxStatus {
            self.store.status().expect("outbox status")
        }
    }

    /// Wait for `reached`, which an event the test already caused makes true.
    fn eventually(what: &str, reached: impl Fn() -> bool) {
        let until = Instant::now() + Duration::from_secs(20);
        while !reached() {
            assert!(Instant::now() < until, "{what} did not happen");
            thread::sleep(Duration::from_millis(2));
        }
    }

    #[test]
    fn the_resender_finishes_a_handoff_whose_syscall_ran_out_of_budget() {
        let mount = TestFuseHandoff::start("fuse-resender-past-budget");
        let service = MountLock::new(0usize);
        // The daemon falls silent once the mutation is in the service: its
        // Arm was acknowledged, its committed work is not.
        let result = run_fuse_mounted_mutation_until(
            &mount.handoff,
            &service,
            FuseMountedMutationOperation::Mkdir,
            Some(Instant::now() + Duration::from_millis(150)),
            |value| {
                *value += 1;
                mount.daemon.set_silent(true);
                test_mutation_report(
                    Ok(()),
                    dfs::DfsCommittedWork {
                        publish_metadata: true,
                        ..dfs::DfsCommittedWork::default()
                    },
                )
            },
        );
        assert_eq!(
            result,
            Err(libc::EIO),
            "the syscall ends at its budget without reporting success"
        );
        assert_eq!(*service.lock().unwrap(), 1, "the mutation ran once");
        assert!(!mount.handoff.is_gated(), "the mount keeps serving");
        // The re-sender has the step and keeps sending it, with no cap.
        eventually("the step reached the re-sender", || mount.unfinished() == 1);
        let sent = mount.daemon.attempts_of(TestDaemon::COMMIT_APPLIED);
        eventually("the re-sender sent the commit again", || {
            mount.daemon.attempts_of(TestDaemon::COMMIT_APPLIED) >= sent + 10
        });
        assert_eq!(mount.status().live_marker_count, 1);

        mount.daemon.set_silent(false);
        eventually("the daemon acknowledged the commit", || {
            mount.unfinished() == 0 && mount.status().pending_marker_count == 0
        });
        // The next mutation is unaffected.
        let next = run_fuse_mounted_mutation_with(
            &mount.handoff,
            &service,
            FuseMountedMutationOperation::Mkdir,
            |value| {
                *value += 1;
                test_mutation_report(Ok(()), dfs::DfsCommittedWork::default())
            },
        );
        assert_eq!(next, Ok(()));
        assert_eq!(*service.lock().unwrap(), 2);
    }

    #[test]
    fn a_mutation_waiting_for_the_daemon_holds_neither_the_service_lane_nor_a_poll() {
        let mount = TestFuseHandoff::start("fuse-arm-wait-frees-service");
        mount.daemon.set_silent(true);
        let service = MountLock::new(0usize);
        thread::scope(|scope| {
            let mutation = scope.spawn(|| {
                run_fuse_mounted_mutation_with(
                    &mount.handoff,
                    &service,
                    FuseMountedMutationOperation::Mkdir,
                    |value| {
                        *value += 1;
                        test_mutation_report(Ok(()), dfs::DfsCommittedWork::default())
                    },
                )
            });
            // No budget: Arm is sent for as long as the daemon stays silent.
            eventually("Arm sent again", || {
                mount.daemon.attempts_of(TestDaemon::ARM) >= 5
            });
            assert!(!mutation.is_finished());
            // A read on the same mount enters the service meanwhile.
            assert_eq!(
                *dfs::MountedSifsCall::lock(&service).expect("the service lane is free"),
                0
            );
            mount.daemon.set_silent(false);
            assert_eq!(mutation.join().unwrap(), Ok(()));
        });
        assert_eq!(*service.lock().unwrap(), 1);
        assert_eq!(mount.unfinished(), 0);
        assert_eq!(mount.status().pending_marker_count, 0);
    }

    #[test]
    fn a_full_outbox_holds_a_fuse_mutation_until_a_marker_is_released() {
        let mount = TestFuseHandoff::start("fuse-outbox-full");
        let shared = &mount.handoff.handoff;
        let mut fillers = Vec::new();
        for _ in 0..dfs::MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS {
            fillers.push(
                shared
                    .prepare("fuse.test-filler", None)
                    .expect("the outbox has room")
                    .into_owned(),
            );
        }
        let service = MountLock::new(0usize);
        thread::scope(|scope| {
            let mutation = scope.spawn(|| {
                run_fuse_mounted_mutation_with(
                    &mount.handoff,
                    &service,
                    FuseMountedMutationOperation::Mkdir,
                    |value| {
                        *value += 1;
                        test_mutation_report(Ok(()), dfs::DfsCommittedWork::default())
                    },
                )
            });
            thread::sleep(Duration::from_millis(300));
            assert!(!mutation.is_finished(), "a full outbox is waited on");
            assert_eq!(mount.daemon.attempts_of(TestDaemon::ARM), 0);

            // Close one marker as no-write: its release makes room.
            let mut released = fillers.pop().unwrap();
            let arm_request_id = dfs::MountedSifsMutationArmRequestId::generate();
            shared.arm(&released, &arm_request_id).unwrap();
            shared
                .persist_proven_no_write(&mut released, &arm_request_id, "fuse-test-filler")
                .unwrap();
            shared
                .commit_proven_no_write(&released, &arm_request_id, "fuse-test-filler")
                .unwrap();
            shared.release_after_daemon_ack(released).unwrap();
            assert_eq!(mutation.join().unwrap(), Ok(()));
        });
        assert_eq!(*service.lock().unwrap(), 1);
        drop(fillers);
    }

    #[test]
    fn a_refused_error_record_stays_in_the_outbox_until_the_session_ends() {
        let mount = TestFuseHandoff::start("fuse-refused-unknown");
        mount.daemon.refuse(TestDaemon::COMMIT_UNKNOWN);
        let service = MountLock::new(0usize);
        let result: std::result::Result<(), libc::c_int> = run_fuse_mounted_mutation_with(
            &mount.handoff,
            &service,
            FuseMountedMutationOperation::Rename,
            |_| {
                test_mutation_report(
                    Err(FuseMountedMutationError::ambiguous(
                        libc::EBUSY,
                        "fuse-test-ambiguous-error",
                    )),
                    dfs::DfsCommittedWork::default(),
                )
            },
        );
        assert_eq!(result, Err(libc::EIO), "the refusal ends the syscall");
        assert_eq!(mount.daemon.attempts_of(TestDaemon::COMMIT_UNKNOWN), 1);
        // The record is durable but no daemon owns it: recovery takes a
        // marker in this phase only once its mount session has ended.
        let status = mount.status();
        assert_eq!(status.deferred_live_session_marker_count, 1);
        assert_eq!(status.exact_recovery_marker_count, 0);
        assert!(!mount.handoff.is_gated());

        mount.daemon.refuse_nothing();
        let next = run_fuse_mounted_mutation_with(
            &mount.handoff,
            &service,
            FuseMountedMutationOperation::Mkdir,
            |_| test_mutation_report(Ok(()), dfs::DfsCommittedWork::default()),
        );
        assert_eq!(next, Ok(()), "the mount keeps serving mutations");
    }

    #[test]
    fn every_clustered_mount_requires_the_mutation_handoff() {
        assert!(fuse_mounted_mutation_handoff_required(true, false));
        assert!(fuse_mounted_mutation_handoff_required(true, true));
        assert!(!fuse_mounted_mutation_handoff_required(false, false));
        assert!(!fuse_mounted_mutation_handoff_required(false, true));
    }

    #[test]
    fn foreground_mount_rejects_unsafe_host_paths_before_opening_sifs() {
        for mount_point in ["/", "/etc/sinomo-unsafe-mount"] {
            let error = mount_sifs(DfsFuseMountOptions::new(mount_point))
                .expect_err("unsafe foreground mount must fail before FUSE or DFS side effects");
            assert!(
                matches!(error, DfsFuseError::InvalidMountPoint { .. }),
                "unexpected error for {mount_point}: {error}"
            );
        }
    }

    #[test]
    fn fuse_mount_helper_lookup_finds_the_installed_program() {
        let root = sinomo_testdir::TestDir::new("sinomo-fuse-helper-path");
        let helper = root.join("fusermount3");
        std::fs::write(&helper, b"helper").expect("write helper fixture");

        assert_eq!(
            fuse_mount_helper_on_path(Some(root.as_os_str())),
            Some(helper)
        );
        assert!(fuse_mount_helper_on_path(None).is_none());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_background_preflight_requires_explicit_allow_other_consent() {
        let root = sinomo_testdir::TestDir::new("sinomo-fuse-linux-preflight");
        let helper = root.join("fusermount3");
        let fuse_config = root.join("fuse.conf");
        std::fs::write(&helper, b"helper").expect("write helper fixture");

        std::fs::write(&fuse_config, b"# user_allow_other\n").expect("fuse config");
        let mut options = DfsFuseMountOptions::new(root.join("mount"));
        options.allow_other = true;

        let error = preflight_linux_sifs_background_mount_with(
            &options,
            Path::new("/dev/null"),
            &helper,
            &fuse_config,
        )
        .expect_err("commented consent must fail closed");
        assert!(error.to_string().contains("user_allow_other"));

        std::fs::write(
            &fuse_config,
            b"# owner-only remains the default\n user_allow_other # explicit\n",
        )
        .expect("enabled fuse config");
        let report = preflight_linux_sifs_background_mount_with(
            &options,
            Path::new("/dev/null"),
            &helper,
            &fuse_config,
        )
        .expect("explicit allow_other consent");
        assert!(report.allow_other_enabled);
        assert_eq!(report.mount_helper, helper);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_background_preflight_requires_an_executable_helper_and_fuse_device() {
        let root = sinomo_testdir::TestDir::new("sinomo-fuse-linux-preflight-capability");
        let helper = root.join("fusermount3");
        let missing_device = root.join("missing-fuse");
        let options = DfsFuseMountOptions::new(root.join("mount"));
        std::fs::write(&helper, b"helper").expect("write helper fixture");

        let helper_error = preflight_linux_sifs_background_mount_with(
            &options,
            Path::new("/dev/null"),
            &root.join("missing-helper"),
            root.join("unused-fuse.conf").as_path(),
        )
        .expect_err("missing helper must fail");
        assert!(helper_error.to_string().contains("is missing"));


        let device_error = preflight_linux_sifs_background_mount_with(
            &options,
            &missing_device,
            &helper,
            root.join("unused-fuse.conf").as_path(),
        )
        .expect_err("missing fuse device must fail");
        assert!(device_error.to_string().contains("missing-fuse"));
    }

    #[test]
    fn background_mount_cannot_hide_its_sinomo_data_directory() {
        let root = sinomo_testdir::TestDir::new("sinomo-fuse-data-dir-mount-safety");
        let data_dir = root.join("data");
        let nested_mount = data_dir.join("mounts").join("sifs");
        std::fs::create_dir_all(&nested_mount).expect("nested mount point");

        let options = DfsFuseMountOptions::new(&root).with_data_dir(&data_dir);
        let error = validate_mount_point_preserves_data_dir(&options, &root)
            .expect_err("mount ancestor must not hide the data directory");
        assert!(error.to_string().contains("data directory"));

        let nested_options = DfsFuseMountOptions::new(&nested_mount).with_data_dir(&data_dir);
        let error = validate_mount_point_preserves_data_dir(&nested_options, &nested_mount)
            .expect_err("a mount inside the data directory must also be rejected");
        assert!(error.to_string().contains("overlaps"));

        let sibling = SinomoDataDir::new(&data_dir)
            .external_mounts_dir()
            .join("sifs/by-process/111");
        let sibling_options = DfsFuseMountOptions::new(&sibling).with_data_dir(&data_dir);
        validate_mount_point_preserves_data_dir(&sibling_options, &sibling)
            .expect("generated sibling projection must preserve DATA");
        #[cfg(unix)]
        {
            let alias = root.join("mount-alias");
            std::os::unix::fs::symlink(&nested_mount, &alias).unwrap();
            let resolved = std::fs::canonicalize(alias).unwrap();
            assert!(
                validate_mount_point_preserves_data_dir(&sibling_options, &resolved).is_err(),
                "canonical aliases back into DATA remain unsafe"
            );
        }
    }

    #[test]
    fn pre_cancelled_mount_open_stops_before_touching_local_state() {
        let cancellation = dfs::DfsCancellationFlag::new();
        cancellation.cancel();
        let error = SifsFuseFileSystem::open(
            DfsFuseMountOptions::new("/tmp/sinomo-pre-cancelled-mount"),
            cancellation,
        )
        .expect_err("pre-cancelled mount must stop before open");
        assert!(error.to_string().contains("cancelled before open"));
    }

    #[test]
    fn stale_mount_recovery_only_classifies_enotconn() {
        assert!(is_disconnected_mount_error(&io::Error::from_raw_os_error(
            libc::ENOTCONN
        )));
        assert!(!is_disconnected_mount_error(&io::Error::from_raw_os_error(
            libc::EACCES
        )));
        assert!(!is_disconnected_mount_error(&io::Error::new(
            io::ErrorKind::NotFound,
            "missing"
        )));
    }

    #[test]
    fn owner_only_foreground_mount_omits_auto_unmount_and_allow_other() {
        let options = DfsFuseMountOptions::new("/mnt/sinomo");

        let direct_config = fuse_mount_config(&options, false);
        assert!(
            direct_config
                .mount_options
                .contains(&MountOption::DefaultPermissions)
        );
        assert!(
            !direct_config
                .mount_options
                .contains(&MountOption::AutoUnmount)
        );
        assert_eq!(direct_config.acl, SessionACL::Owner);

        let helper_config = fuse_mount_config(&options, true);
        assert!(
            !helper_config
                .mount_options
                .contains(&MountOption::AutoUnmount)
        );
        assert_eq!(helper_config.acl, SessionACL::Owner);
    }

    #[test]
    fn allow_other_mount_retains_helper_auto_unmount() {
        let mut options = DfsFuseMountOptions::new("/mnt/sinomo");
        options.allow_other = true;

        let direct_config = fuse_mount_config(&options, false);
        assert!(
            !direct_config
                .mount_options
                .contains(&MountOption::AutoUnmount)
        );
        assert_eq!(direct_config.acl, SessionACL::All);

        let helper_config = fuse_mount_config(&options, true);
        assert!(
            helper_config
                .mount_options
                .contains(&MountOption::AutoUnmount)
        );
        assert_eq!(helper_config.acl, SessionACL::All);
    }

    #[test]
    fn owner_only_background_mount_does_not_implicitly_request_allow_other() {
        let options = DfsFuseMountOptions::new("/mnt/sinomo");

        let background_config = background_fuse_mount_config(&options, true);
        assert!(
            !background_config
                .mount_options
                .contains(&MountOption::AutoUnmount)
        );
        assert_eq!(background_config.acl, SessionACL::Owner);
    }

    #[test]
    fn created_entry_mode_applies_the_request_umask() {
        assert_eq!(created_mode(libc::S_IFREG | 0o666, 0o022), 0o644);
        assert_eq!(created_mode(libc::S_IFDIR | 0o777, 0o027), 0o750);
        assert_eq!(created_mode(0o700, 0), 0o700);
        assert_eq!(created_mode(0o6777, 0o7000), 0o6777);
    }

    #[test]
    fn owner_only_file_attrs_project_the_mount_owner() {
        let stat = test_stat("owner-only-object", "owner-only-version", "/services/chat");
        let owner = FusePresentedOwner {
            uid: 1_234,
            gid: 5_678,
        };

        let attr = file_attr(7, &stat, Some(owner));

        assert_eq!(attr.uid, owner.uid);
        assert_eq!(attr.gid, owner.gid);
        assert_eq!(attr.perm, 0o644);
    }

    #[test]
    fn allow_other_file_attrs_preserve_durable_ownership() {
        let stat = test_stat("shared-object", "shared-version", "/services/shared");

        let attr = file_attr(8, &stat, None);

        assert_eq!(attr.uid, stat.uid);
        assert_eq!(attr.gid, stat.gid);
        assert_eq!(attr.perm, 0o644);
    }

    #[test]
    fn pending_symlink_content_is_retryable_at_the_mount_boundary() {
        assert_eq!(
            errno_for_posix_error(DfsPosixError::SymlinkContentPending {
                path: "/services/link".to_string(),
                stage: "fetch-link-body".to_string(),
                detail: "replication is pending".to_string(),
            }),
            libc::EAGAIN
        );
    }

    #[test]
    fn create_flags_preserve_existing_files_unless_exclusive_or_truncating() {
        assert_eq!(
            existing_file_create_action(libc::O_CREAT),
            ExistingFileCreateAction::ReturnExisting
        );
        assert_eq!(
            existing_file_create_action(libc::O_CREAT | libc::O_TRUNC),
            ExistingFileCreateAction::Truncate
        );
        assert_eq!(
            existing_file_create_action(libc::O_CREAT | libc::O_EXCL),
            ExistingFileCreateAction::Reject
        );
        assert_eq!(
            existing_file_create_action(libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC),
            ExistingFileCreateAction::Reject
        );
    }

    fn test_object_id(name: &str) -> dfs::DfsObjectId {
        dfs::DfsObjectId::from_legacy_named(name).expect("test object id")
    }

    fn test_stat(object_name: &str, version_name: &str, path: &str) -> dfs::DfsStat {
        dfs::DfsStat {
            file_system_id: dfs::DfsFileSystemId::from_legacy_named("sifs")
                .expect("test filesystem id"),
            object_id: test_object_id(object_name),
            path: path.to_string(),
            kind: DfsObjectKind::File,
            version: dfs::DfsVersion::from_legacy_named(version_name).expect("test version"),
            content_hash: Some(format!("hash-{version_name}")),
            size_bytes: Some(12),
            link_count: 1,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 2,
            created_at_unix_nanos: 3,
            changed_at_unix_nanos: 4,
            policy: sinomo_runtime::core::DfsObjectPolicy::ReplicatedEventual,
            current_locations: vec![
                dfs::DfsNodeId::from_legacy_named("fuse-test-node").expect("test node id"),
            ],
        }
    }

    fn test_grant(
        object_id: dfs::DfsObjectId,
        grant_id: &str,
        mode: dfs::DfsAdvisoryLockMode,
    ) -> dfs::DfsAdvisoryLockGrant {
        dfs::DfsAdvisoryLockGrant {
            token: dfs::DfsAdvisoryLockToken {
                file_system_id: dfs::DfsFileSystemId::from_legacy_named("sifs")
                    .expect("test filesystem id"),
                object_id,
                session_id: dfs::DfsAdvisoryLockSessionId::new("test-session"),
                authority_epoch: 7,
                grant_id: dfs::DfsAdvisoryLockRequestId::new(grant_id),
            },
            mode,
            process_id: Some(42),
            lease_deadline_unix_nanos: 999,
            lease_remaining_nanos: Duration::from_secs(60).as_nanos() as u64,
        }
    }

    fn live_local_lease_deadline() -> Option<Instant> {
        Instant::now().checked_add(Duration::from_secs(60))
    }

    #[test]
    fn busy_cross_shard_moves_map_to_eagain() {
        assert_eq!(
            errno_for_posix_error(DfsPosixError::Dfs(DfsError::Busy {
                path: "/users/moving.txt".to_string(),
            })),
            libc::EAGAIN
        );
        // Other internal errors still surface as I/O failures.
        assert_eq!(
            errno_for_posix_error(DfsPosixError::Dfs(DfsError::Distributed {
                message: "boom".to_string(),
            })),
            libc::EIO
        );
    }

    #[test]
    fn distributed_lock_surface_only_accepts_whole_file_ranges() {
        assert!(is_whole_file_lock_range(0, u64::MAX));
        assert!(is_whole_file_lock_range(0, i64::MAX as u64));
        assert!(!is_whole_file_lock_range(1, u64::MAX));
        assert!(!is_whole_file_lock_range(0, 4095));
    }

    #[test]
    fn fuse_lock_types_map_to_shared_and_exclusive_modes() {
        assert_eq!(
            advisory_lock_mode(libc::F_RDLCK),
            Ok(dfs::DfsAdvisoryLockMode::Shared)
        );
        assert_eq!(
            advisory_lock_mode(libc::F_WRLCK),
            Ok(dfs::DfsAdvisoryLockMode::Exclusive)
        );
        assert_eq!(advisory_lock_mode(libc::F_UNLCK), Err(libc::EINVAL));
        assert_eq!(
            advisory_lock_type(dfs::DfsAdvisoryLockMode::Shared),
            libc::F_RDLCK
        );
        assert_eq!(
            advisory_lock_type(dfs::DfsAdvisoryLockMode::Exclusive),
            libc::F_WRLCK
        );
    }

    #[test]
    fn open_sync_flags_select_the_matching_distributed_barrier() {
        assert_eq!(
            HandleSyncMode::from_open_flags(libc::O_RDWR),
            HandleSyncMode::None
        );
        assert_eq!(
            HandleSyncMode::from_open_flags(libc::O_RDWR | libc::O_DSYNC).kind(),
            Some(dfs::DfsPosixSyncKind::FileData)
        );
        assert_eq!(
            HandleSyncMode::from_open_flags(libc::O_RDWR | libc::O_SYNC).kind(),
            Some(dfs::DfsPosixSyncKind::File)
        );
    }

    #[test]
    fn ordinary_fuse_writes_commit_at_w_and_leave_n_minus_w_to_repair() {
        let local = dfs::DfsNodeId::from_legacy_named("fuse-write-local").expect("local node");
        let replica =
            dfs::DfsNodeId::from_legacy_named("fuse-write-replica").expect("replica node");

        let options = fuse_write_options(vec![local, replica]);

        assert!(options.wait_for_commit, "a returned write must be visible");
        assert_eq!(options.policy.min_commit_replicas, 1);
        assert_eq!(options.policy.desired_replicas, 2);
        assert!(
            !options.foreground_replica_repair,
            "replicated-eventual repair must not extend every FUSE write"
        );
    }

    #[test]
    fn sequential_eight_mib_fuse_chunks_coalesce_to_one_publication() {
        let path = "/root/sequential.bin";
        let mut opened = test_stat("fusesequentialobject", "fusesequentialversion", path);
        opened.size_bytes = Some(0);
        let mut state = FuseState::new("/root".to_string());
        let fh = state.allocate_handle(path.to_string(), opened, libc::O_RDWR);
        let chunk = vec![0x5a; 64 * 1024];

        for index in 0..128u64 {
            assert_eq!(
                state
                    .buffer_handle_write(fh, index * chunk.len() as u64, &chunk)
                    .expect("buffer sequential kernel chunk"),
                DirtyWriteInsert::Buffered
            );
        }

        let handle = state.handles.get(&fh).expect("open handle");
        assert_eq!(state.dirty_inodes[&handle.object_id].active.len(), 1);
        assert_eq!(
            state.dirty_inodes[&handle.object_id]
                .active
                .buffered_bytes(),
            8 * 1024 * 1024
        );
        assert_eq!(handle.logical_size, 8 * 1024 * 1024);
        let publication = state
            .take_dirty_publication(fh)
            .expect("take publication")
            .expect("dirty publication");
        assert_eq!(publication.dirty.len(), 1);
        assert_eq!(
            publication
                .dirty
                .ranges
                .get(&0)
                .expect("coalesced sequential range")
                .len(),
            8 * 1024 * 1024
        );
    }

    #[test]
    fn overlapping_dirty_writes_are_merged_with_latest_bytes_winning() {
        let mut dirty = DirtyWriteBuffer::default();
        assert_eq!(
            dirty.try_write(4, b"abcd", 64).expect("first write"),
            DirtyWriteInsert::Buffered
        );
        assert_eq!(
            dirty.try_write(6, b"WXYZ", 64).expect("overlap write"),
            DirtyWriteInsert::Buffered
        );
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty.buffered_bytes(), 6);
        assert_eq!(dirty.ranges.get(&4).expect("merged range"), b"abWXYZ");
    }

    #[test]
    fn dirty_buffer_requests_publication_before_exceeding_its_bound() {
        let mut dirty = DirtyWriteBuffer::default();
        assert_eq!(
            dirty.try_write(0, b"123456", 8).expect("first write"),
            DirtyWriteInsert::Buffered
        );
        assert_eq!(
            dirty
                .try_write(16, b"789", 8)
                .expect("bounded second write"),
            DirtyWriteInsert::NeedsPublication
        );
        assert_eq!(dirty.buffered_bytes(), 6);
        assert_eq!(dirty.ranges.len(), 1);

        let mut empty = DirtyWriteBuffer::default();
        assert_eq!(
            empty
                .try_write(0, b"larger-than-limit", 4)
                .expect("large direct write"),
            DirtyWriteInsert::PublishDirectly
        );
        assert!(empty.is_empty());
    }

    #[test]
    fn opened_reads_overlay_dirty_bytes_and_zero_fill_new_sparse_extents() {
        let path = "/root/read-your-writes.bin";
        let mut opened = test_stat("fuserywobject", "fuserywversion", path);
        opened.size_bytes = Some(6);
        let mut state = FuseState::new("/root".to_string());
        let fh = state.allocate_handle(path.to_string(), opened, libc::O_RDWR);
        assert_eq!(
            state
                .buffer_handle_write(fh, 2, b"XY")
                .expect("overwrite committed extent"),
            DirtyWriteInsert::Buffered
        );
        assert_eq!(
            state
                .buffer_handle_write(fh, 8, b"Z")
                .expect("extend with sparse gap"),
            DirtyWriteInsert::Buffered
        );

        let read = state.opened_read(fh, 0, 16).expect("opened read plan");
        assert_eq!(read.base_request_len(), 6);
        assert_eq!(
            read.finish(b"abcdef".to_vec()).expect("overlay base read"),
            b"abXYef\0\0Z"
        );
        let sparse = state.opened_read(fh, 6, 3).expect("sparse read plan");
        assert_eq!(sparse.base_request_len(), 0);
        assert_eq!(
            sparse.finish(Vec::new()).expect("local sparse read"),
            b"\0\0Z"
        );
        assert_eq!(
            state
                .visible_handle_stat(fh)
                .expect("visible handle stat")
                .size_bytes,
            Some(9)
        );
    }

    #[test]
    fn dirty_visibility_and_publication_order_are_object_wide_across_handles() {
        let path = "/root/shared-open.bin";
        let mut opened = test_stat("fusesharedobject", "fusesharedversion", path);
        opened.size_bytes = Some(4);
        let mut state = FuseState::new("/root".to_string());
        let writer = state.allocate_handle(path.to_string(), opened.clone(), libc::O_RDWR);
        let reader = state.allocate_handle(path.to_string(), opened, libc::O_RDONLY);
        state
            .buffer_handle_write(writer, 4, b"++")
            .expect("buffer through writer");

        assert_eq!(
            state.dirty_handles_for_object(
                &state.handles.get(&writer).expect("writer").object_id,
                Some(reader),
            ),
            vec![writer]
        );
        let read = state
            .opened_read(reader, 0, 6)
            .expect("reader sees mount-local dirty object state");
        assert_eq!(read.base_request_len(), 4);
        assert_eq!(
            read.finish(b"base".to_vec()).expect("object-wide overlay"),
            b"base++"
        );
        assert_eq!(
            state
                .visible_handle_stat(reader)
                .expect("reader-visible stat")
                .size_bytes,
            Some(6)
        );
    }

    #[test]
    fn alternating_handles_freeze_one_ordered_inode_publication() {
        let path = "/root/alternate";
        let stat = test_stat("alternatingobject", "alternatingversion", path);
        let mut state = FuseState::new("/root".into());
        let a = state.allocate_handle(path.into(), stat.clone(), libc::O_RDWR);
        let b = state.allocate_handle(path.into(), stat, libc::O_RDWR);
        for (fh, at, bytes) in [(a, 1, &b"ab"[..]), (b, 9, &b"z"[..]), (b, 2, &b"c"[..])] {
            assert_eq!(
                state.buffer_handle_write(fh, at, bytes).unwrap(),
                DirtyWriteInsert::Buffered
            );
        }
        let batch = state.take_dirty_publication(a).unwrap().unwrap();
        assert!(Arc::ptr_eq(
            &batch,
            &state.take_dirty_publication(b).unwrap().unwrap()
        ));
        assert_eq!(batch.writers, BTreeSet::from([a, b]));
        assert_eq!(
            batch.dirty.ranges,
            BTreeMap::from([(1, b"ac".to_vec()), (9, b"z".to_vec())])
        );
    }

    #[test]
    fn sync_handles_never_accept_buffered_dirty_state() {
        let path = "/root/sync.bin";
        let opened = test_stat("fusesyncobject", "fusesyncversion", path);
        let mut state = FuseState::new("/root".to_string());
        for flags in [libc::O_RDWR | libc::O_DSYNC, libc::O_RDWR | libc::O_SYNC] {
            let fh = state.allocate_handle(path.to_string(), opened.clone(), flags);
            assert_eq!(
                state
                    .buffer_handle_write(fh, 0, b"immediate")
                    .expect("classify sync write"),
                DirtyWriteInsert::PublishDirectly
            );
            assert!(state.dirty_inodes.is_empty());
        }
    }

    #[test]
    fn failed_publication_can_restore_all_acknowledged_dirty_bytes() {
        let path = "/root/retry.bin";
        let mut opened = test_stat("fuseretryobject", "fuseretryversion", path);
        opened.size_bytes = Some(0);
        let mut state = FuseState::new("/root".to_string());
        let fh = state.allocate_handle(path.to_string(), opened, libc::O_RDWR);
        state
            .buffer_handle_write(fh, 0, b"retry-me")
            .expect("buffer write");
        let publication = state
            .take_dirty_publication(fh)
            .expect("take publication")
            .expect("dirty publication");
        // The uncertain operation stays frozen, including its original ID,
        // while newer accepted bytes remain separately visible.
        state.buffer_handle_write(fh, 0, b"new").unwrap();
        let retry = state.take_dirty_publication(fh).unwrap().unwrap();
        assert!(Arc::ptr_eq(&publication, &retry));
        assert_eq!(
            state.opened_read(fh, 0, 8).unwrap().finish(vec![]).unwrap(),
            b"newry-me"
        );
        let restored = &retry.dirty;
        assert_eq!(restored.buffered_bytes(), 8);
        assert_eq!(
            restored.ranges.get(&0).expect("restored range"),
            b"retry-me"
        );
    }

    #[test]
    fn empty_content_service_candidates_fall_back_before_the_dfs_mutex_is_taken() {
        let local =
            dfs::DfsNodeId::from_legacy_named("fuse-write-fallback-local").expect("local node");

        assert_eq!(
            fuse_write_replica_candidates(Vec::new(), local.clone()),
            vec![local]
        );
    }

    #[test]
    fn lock_warmup_is_retriable_but_lost_grants_are_io_failures() {
        let warming = dfs::DfsAdvisoryLockOutcome::AuthorityWarming {
            retry_after_unix_nanos: 99,
        };
        assert_eq!(errno_for_unexpected_lock_outcome(&warming), libc::EAGAIN);
        assert!(!lock_outcome_loses_grant(&warming));

        let lost = dfs::DfsAdvisoryLockOutcome::LockLost;
        assert_eq!(errno_for_unexpected_lock_outcome(&lost), libc::EIO);
        assert!(lock_outcome_loses_grant(&lost));
    }

    #[test]
    fn stale_renewal_cannot_remove_a_newer_full_grant() {
        let object_id = test_object_id("fuselockobject");
        let key = (object_id.clone(), 9);
        let original_grant = test_grant(
            object_id.clone(),
            "grant-original",
            dfs::DfsAdvisoryLockMode::Shared,
        );
        let held = fuse_held_lock(
            "/locked.txt".to_string(),
            object_id,
            dfs::DfsAdvisoryLockSessionId::new("test-session"),
            original_grant.clone(),
            live_local_lease_deadline(),
            BTreeSet::from([3]),
        )
        .expect("live grant");
        let state = Arc::new(MountLock::new(FuseState::new("/".to_string())));
        state
            .lock()
            .expect("state")
            .held_locks
            .insert(key.clone(), held);

        {
            let mut state = state.lock().expect("state");
            let current = state.held_locks.get_mut(&key).expect("held lock");
            current.grant.mode = dfs::DfsAdvisoryLockMode::Exclusive;
        }
        mark_fuse_lock_lost(&state, &key, &original_grant);
        {
            let state = state.lock().expect("state");
            assert_eq!(
                state.held_locks.get(&key).expect("newer grant").grant.mode,
                dfs::DfsAdvisoryLockMode::Exclusive
            );
            assert!(!state.lost_locks.contains_key(&key));
        }

        let current_grant = state
            .lock()
            .expect("state")
            .held_locks
            .get(&key)
            .expect("newer grant")
            .grant
            .clone();
        mark_fuse_lock_lost(&state, &key, &current_grant);
        let state = state.lock().expect("state");
        assert!(!state.held_locks.contains_key(&key));
        assert_eq!(
            state
                .lost_locks
                .get(&key)
                .expect("lost marker")
                .associated_fhs,
            BTreeSet::from([3])
        );
    }

    #[test]
    fn handle_associations_find_every_release_fallback_owner() {
        let object_id = test_object_id("fusereleaseobject");
        let mut state = FuseState::new("/".to_string());
        let held = fuse_held_lock(
            "/locked.txt".to_string(),
            object_id.clone(),
            dfs::DfsAdvisoryLockSessionId::new("held-session"),
            test_grant(
                object_id.clone(),
                "grant-held",
                dfs::DfsAdvisoryLockMode::Exclusive,
            ),
            live_local_lease_deadline(),
            BTreeSet::from([11]),
        )
        .expect("live grant");
        state.held_locks.insert((object_id.clone(), 101), held);
        state.pending_locks.insert(
            (object_id.clone(), 202),
            FusePendingLock {
                path: "/locked.txt".to_string(),
                object_id: object_id.clone(),
                session_id: dfs::DfsAdvisoryLockSessionId::new("pending-session"),
                acquire_request_id: dfs::DfsAdvisoryLockRequestId::new("pending-request"),
                associated_fhs: BTreeSet::from([11]),
            },
        );
        state.record_lost_lock((object_id, 303), [12]);

        assert_eq!(state.lock_owners_for_handle(11), BTreeSet::from([101, 202]));
        assert_eq!(state.lock_owners_for_handle(12), BTreeSet::from([303]));
    }

    #[test]
    fn missing_or_elapsed_local_lease_becomes_a_handle_scoped_loss() {
        let object_id = test_object_id("fuseexpiredobject");
        let key = (object_id.clone(), 404);
        let mut zero = test_grant(
            object_id.clone(),
            "grant-zero",
            dfs::DfsAdvisoryLockMode::Exclusive,
        );
        zero.lease_remaining_nanos = 0;
        assert!(
            fuse_held_lock(
                "/expired.txt".to_string(),
                object_id.clone(),
                dfs::DfsAdvisoryLockSessionId::new("expired-session"),
                zero,
                None,
                BTreeSet::from([14]),
            )
            .is_none()
        );

        let mut state = FuseState::new("/".to_string());
        let mut held = fuse_held_lock(
            "/expired.txt".to_string(),
            object_id.clone(),
            dfs::DfsAdvisoryLockSessionId::new("expired-session"),
            test_grant(
                object_id,
                "grant-elapsed",
                dfs::DfsAdvisoryLockMode::Exclusive,
            ),
            Some(Instant::now() + Duration::from_secs(1)),
            BTreeSet::from([14]),
        )
        .expect("initially live grant");
        held.local_lease_deadline = Instant::now();
        state.held_locks.insert(key.clone(), held);
        state.expire_elapsed_locks(Instant::now());

        assert!(!state.held_locks.contains_key(&key));
        assert!(state.has_lost_lock_for_handle(14));
    }

    #[test]
    fn local_lease_projection_is_anchored_before_the_rpc() {
        let object_id = test_object_id("fuseleaseprojection");
        let grant = test_grant(
            object_id,
            "grant-projection",
            dfs::DfsAdvisoryLockMode::Exclusive,
        );
        let remaining = Duration::from_nanos(grant.lease_remaining_nanos);
        let started_at = Instant::now();
        let outcome = dfs::DfsAdvisoryLockOutcome::Acquired { grant };

        assert_eq!(
            conservative_local_lease_deadline(&outcome, started_at),
            started_at.checked_add(remaining)
        );
    }

    #[test]
    fn mounted_lock_key_cap_rejects_the_next_distinct_key() {
        let mut state = FuseState::new("/".to_string());
        let mut first_key = None;
        for index in 0..MAX_ACTIVE_LOCK_KEYS_PER_MOUNT {
            let object_id = test_object_id(&format!("fusecapobject{index}"));
            let key = (object_id.clone(), index as u64 + 1);
            first_key.get_or_insert_with(|| key.clone());
            state.pending_locks.insert(
                key,
                FusePendingLock {
                    path: format!("/pending-{index}.txt"),
                    object_id,
                    session_id: dfs::DfsAdvisoryLockSessionId::new(format!(
                        "pending-session-{index}"
                    )),
                    acquire_request_id: dfs::DfsAdvisoryLockRequestId::new(format!(
                        "pending-request-{index}"
                    )),
                    associated_fhs: BTreeSet::from([index as u64 + 10]),
                },
            );
        }

        assert!(state.can_start_lock_key(&first_key.expect("first key")));
        assert!(!state.can_start_lock_key(&(test_object_id("fusecapfifth"), 99)));
    }

    #[test]
    fn directory_rename_rewrites_descendant_handles_and_lock_paths() {
        let object_id = test_object_id("fuserenameobject");
        let opened_stat = test_stat("fuserenameobject", "fuserenameversion", "/root/a/child.txt");
        let mut state = FuseState::new("/root".to_string());
        let directory_ino = state.intern_path("/root/a".to_string());
        let file_ino = state.intern_path("/root/a/child.txt".to_string());
        let fh = state.allocate_handle(
            "/root/a/child.txt".to_string(),
            opened_stat.clone(),
            libc::O_RDWR,
        );
        state.held_locks.insert(
            (object_id.clone(), 1),
            fuse_held_lock(
                "/root/a/child.txt".to_string(),
                object_id.clone(),
                dfs::DfsAdvisoryLockSessionId::new("held-session"),
                test_grant(
                    object_id.clone(),
                    "grant-rename",
                    dfs::DfsAdvisoryLockMode::Shared,
                ),
                live_local_lease_deadline(),
                BTreeSet::from([fh]),
            )
            .expect("live grant"),
        );
        state.pending_locks.insert(
            (object_id.clone(), 2),
            FusePendingLock {
                path: "/root/a/deeper/child.txt".to_string(),
                object_id,
                session_id: dfs::DfsAdvisoryLockSessionId::new("pending-session"),
                acquire_request_id: dfs::DfsAdvisoryLockRequestId::new("pending-rename"),
                associated_fhs: BTreeSet::from([fh]),
            },
        );

        state.rename_path("/root/a", "/root/b".to_string());

        assert_eq!(state.path_for(directory_ino), Some("/root/b"));
        assert_eq!(state.path_for(file_ino), Some("/root/b/child.txt"));
        assert_eq!(
            state.handles.get(&fh).expect("handle").path,
            "/root/b/child.txt"
        );
        assert_eq!(
            state.handles.get(&fh).expect("handle").opened_stat,
            opened_stat,
            "rename changes the live pathname but not the exact opened content identity"
        );
        assert_eq!(
            state.held_locks.values().next().expect("held").path,
            "/root/b/child.txt"
        );
        assert_eq!(
            state.pending_locks.values().next().expect("pending").path,
            "/root/b/deeper/child.txt"
        );
        assert_eq!(renamed_path("/root/ab", "/root/a", "/root/b"), None);
    }

    #[test]
    fn hardlink_aliases_share_target_inode_and_survive_individual_unlinks() {
        let object_id = test_object_id("fusehardlinkidentity");
        let original = "/root/original.txt";
        let alias_a = "/root/alias-a.txt";
        let alias_b = "/root/alias-b.txt";
        let mut state = FuseState::new("/root".to_string());

        let target_ino = state.intern_entry(original.to_string(), object_id.clone());
        assert_eq!(
            state.intern_entry(alias_a.to_string(), object_id.clone()),
            target_ino,
            "lookup/readdir of a hardlink must reuse the target inode"
        );
        assert_eq!(
            state
                .intern_alias_path(target_ino, alias_b.to_string(), &object_id)
                .expect("link alias"),
            target_ino,
            "link(2) must reply with the supplied target inode"
        );

        state.remove_path(original);
        assert_eq!(state.ino_by_path.get(alias_a), Some(&target_ino));
        assert_eq!(state.ino_by_path.get(alias_b), Some(&target_ino));
        assert!(state.path_for(target_ino).is_some());
        assert_eq!(state.ino_by_object_id.get(&object_id), Some(&target_ino));

        state.remove_path(alias_a);
        assert_eq!(state.path_for(target_ino), Some(alias_b));
        state.remove_path(alias_b);
        assert_eq!(state.path_for(target_ino), None);
        assert!(!state.ino_by_object_id.contains_key(&object_id));
    }

    #[test]
    fn freshly_observed_object_path_becomes_the_inode_routing_path() {
        let object_id = test_object_id("fuseremoterenameidentity");
        let mut state = FuseState::new("/root".to_string());
        let ino = state.intern_entry("/root/a-stale.txt".to_string(), object_id.clone());

        assert_eq!(
            state.intern_entry("/root/z-current.txt".to_string(), object_id),
            ino
        );
        assert_eq!(
            state.path_for(ino),
            Some("/root/z-current.txt"),
            "a fresh lookup after a remote rename must not route through a lexically earlier stale path"
        );

        state.remove_path("/root/z-current.txt");
        assert_eq!(
            state.path_for(ino),
            Some("/root/a-stale.txt"),
            "removing the preferred alias must retain a remaining hardlink path"
        );
    }

    #[test]
    fn hardlink_rename_moves_only_the_named_alias_and_preserves_other_identities() {
        let source_object = test_object_id("fusehardlinkrenamesource");
        let destination_object = test_object_id("fusehardlinkrenamedestination");
        let mut state = FuseState::new("/root".to_string());
        let source_ino = state.intern_entry("/root/source".to_string(), source_object);
        assert_eq!(
            state.intern_entry(
                "/root/source-alias".to_string(),
                test_object_id("fusehardlinkrenamesource"),
            ),
            source_ino
        );
        let destination_ino =
            state.intern_entry("/root/destination".to_string(), destination_object.clone());
        assert_eq!(
            state.intern_entry("/root/destination-alias".to_string(), destination_object),
            destination_ino
        );

        state.rename_path("/root/source", "/root/destination".to_string());
        assert!(!state.ino_by_path.contains_key("/root/source"));
        assert_eq!(
            state.ino_by_path.get("/root/destination"),
            Some(&source_ino)
        );
        assert_eq!(
            state.ino_by_path.get("/root/source-alias"),
            Some(&source_ino),
            "renaming one hardlink must not move its siblings"
        );
        assert_eq!(
            state.ino_by_path.get("/root/destination-alias"),
            Some(&destination_ino),
            "replacing a path must not detach other links to the replaced object"
        );

        state.rename_path("/root/source-alias", "/root/destination".to_string());
        assert_eq!(
            state.ino_by_path.get("/root/source-alias"),
            Some(&source_ino)
        );
        assert_eq!(
            state.ino_by_path.get("/root/destination"),
            Some(&source_ino)
        );
    }

    #[test]
    fn handle_keeps_opened_identity_after_unlink_and_refreshes_only_same_object() {
        let path = "/root/opened.txt";
        let opened = test_stat("fuseopenedobject", "fuseopenedversion1", path);
        let mut state = FuseState::new("/root".to_string());
        state.intern_path(path.to_string());
        let fh = state.allocate_handle(path.to_string(), opened.clone(), libc::O_RDWR);

        state.remove_path(path);
        assert_eq!(
            state.handles.get(&fh).expect("open handle").opened_stat,
            opened,
            "unlink removes the pathname cache, not an open descriptor's immutable content identity"
        );

        let mut committed = test_stat("fuseopenedobject", "fuseopenedversion2", path);
        committed.size_bytes = Some(24);
        state
            .refresh_handle_stat(fh, &committed)
            .expect("refresh same object after descriptor write");
        assert_eq!(
            state
                .handles
                .get(&fh)
                .expect("refreshed handle")
                .opened_stat,
            committed
        );

        let replacement = test_stat("fusereplacementobject", "fuseopenedversion3", path);
        assert_eq!(
            state.refresh_handle_stat(fh, &replacement),
            Err(libc::ESTALE)
        );
        assert_eq!(
            state
                .handles
                .get(&fh)
                .expect("unchanged handle")
                .opened_stat,
            committed,
            "a pathname replacement must not retarget an existing descriptor"
        );
    }

    #[test]
    fn bounded_read_executor_runs_blocking_requests_concurrently() {
        let executor = FuseReadExecutor::with_worker_count(2, dfs::DfsCancellationFlag::new())
            .expect("read executor");
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (completed_tx, completed_rx) = std::sync::mpsc::channel();
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        for _ in 0..2 {
            let entered_tx = entered_tx.clone();
            let completed_tx = completed_tx.clone();
            let release = Arc::clone(&release);
            assert!(
                executor
                    .submit(Box::new(move || {
                        entered_tx.send(()).expect("entered signal");
                        let (released, wake) = &*release;
                        let mut released = released.lock().expect("release lock");
                        while !*released {
                            released = wake.wait(released).expect("release wait");
                        }
                        completed_tx.send(()).expect("completion signal");
                    }))
                    .is_ok(),
                "submit read job"
            );
        }

        let first_entered = entered_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        let second_entered = entered_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        {
            let (released, wake) = &*release;
            *released.lock().expect("release lock") = true;
            wake.notify_all();
        }
        assert!(first_entered && second_entered, "read jobs did not overlap");
        completed_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("first completion");
        completed_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("second completion");
        drop(executor);
    }

    #[test]
    fn read_executor_shutdown_cancels_queued_timeout_waves_before_join() {
        let cancellation = dfs::DfsCancellationFlag::new();
        let executor =
            FuseReadExecutor::with_worker_count(2, cancellation.clone()).expect("read executor");
        let started = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let skipped = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        for _ in 0..10 {
            let cancellation = cancellation.clone();
            let started = Arc::clone(&started);
            let skipped = Arc::clone(&skipped);
            let entered_tx = entered_tx.clone();
            let release = Arc::clone(&release);
            assert!(
                executor
                    .submit(Box::new(move || {
                        // Mirrors MountedSifsContentReadContext's pre-send
                        // check: queued requests reply Cancelled without
                        // starting another network-timeout wave.
                        if cancellation.is_cancelled() {
                            skipped.fetch_add(1, Ordering::SeqCst);
                            return;
                        }
                        started.fetch_add(1, Ordering::SeqCst);
                        entered_tx.send(()).expect("entered remote send");
                        let (released, wake) = &*release;
                        let mut released = released.lock().expect("release lock");
                        while !*released {
                            released = wake.wait(released).expect("release wait");
                        }
                    }))
                    .is_ok(),
                "submit queued reply"
            );
        }

        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("first in-flight read");
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("second in-flight read");
        let shutdown = thread::spawn(move || drop(executor));
        let deadline = Instant::now() + Duration::from_secs(2);
        while !cancellation.is_cancelled() && Instant::now() < deadline {
            thread::yield_now();
        }
        assert!(
            cancellation.is_cancelled(),
            "executor drop did not cancel before joining"
        );
        {
            let (released, wake) = &*release;
            *released.lock().expect("release lock") = true;
            wake.notify_all();
        }
        shutdown.join().expect("executor shutdown");

        assert_eq!(started.load(Ordering::SeqCst), 2);
        assert_eq!(skipped.load(Ordering::SeqCst), 8);
    }

    #[test]
    fn queued_opened_read_identity_survives_handle_close() {
        let path = "/root/close-race.txt";
        let opened = test_stat("fusecloseraceobject", "fusecloseraceversion", path);
        let mut state = FuseState::new("/root".to_string());
        let fh = state.allocate_handle(path.to_string(), opened.clone(), libc::O_RDONLY);
        let identity = state
            .opened_read_identity(fh)
            .expect("snapshot opened identity before queueing");

        let executor = FuseReadExecutor::with_worker_count(1, dfs::DfsCancellationFlag::new())
            .expect("read executor");
        let (blocker_entered_tx, blocker_entered_rx) = std::sync::mpsc::channel();
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let release_for_blocker = Arc::clone(&release);
        assert!(
            executor
                .submit(Box::new(move || {
                    blocker_entered_tx.send(()).expect("blocker entered");
                    let (released, wake) = &*release_for_blocker;
                    let mut released = released.lock().expect("release lock");
                    while !*released {
                        released = wake.wait(released).expect("release wait");
                    }
                }))
                .is_ok(),
            "submit blocker"
        );
        blocker_entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("blocker running");
        let (observed_tx, observed_rx) = std::sync::mpsc::channel();
        assert!(
            executor
                .submit(Box::new(move || {
                    observed_tx.send(identity).expect("observed identity");
                }))
                .is_ok(),
            "submit opened read"
        );

        // release(2) removes the handle synchronously, while a previously
        // accepted read owns its exact identity and remains valid.
        state.handles.remove(&fh);
        assert!(state.opened_read_identity(fh).is_none());
        {
            let (released, wake) = &*release;
            *released.lock().expect("release lock") = true;
            wake.notify_all();
        }
        let observed = observed_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("queued read completed after close");
        assert_eq!(observed.path, path);
        assert_eq!(observed.opened_stat, opened);
        drop(executor);
    }
}
