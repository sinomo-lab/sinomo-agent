//! Crash-safe ownership transfer for mounted NFS operations.
//!
//! NFS is a long-lived mounted client, not a second daemon. Clustered reads may
//! ingest authoritative namespace journals and writes may commit DFS work, so
//! the same outbox/daemon handoff used by FUSE and Cloud Files is mandatory.
//!
//! The export is never fenced. A handoff step the daemon has not answered is
//! sent again until it is acknowledged, by the request while its operation
//! budget lasts and by the export's re-sender afterwards. A step the daemon
//! definitely refused ends its request with `NFS3ERR_IO` and releases the
//! marker. Sending, re-sending and the re-sender are the shared
//! [`dfs::MountedSifsMutationHandoff`].

use crate::{DfsNfsError, Result};
use sinomo_api::DaemonApiClient;
use sinomo_api::DaemonPaths;
use sinomo_core::local::SinomoDataDir;
use sinomo_dfs as dfs;
use sinomo_dfs::DaemonDfsMountedMutationHandoffClient;
use std::sync::Arc;
use std::time::Instant;

type ServiceLock = dfs::MountedSifsLock<dfs::MountedSifsService>;

#[derive(Debug)]
pub(crate) struct NfsMountedMutationHandoff {
    handoff: dfs::MountedSifsMutationHandoff,
    /// Mutations hand their work to the daemon in the order they ran.
    lane: dfs::MountedSifsLock<()>,
}

impl NfsMountedMutationHandoff {
    /// Open the export's handoff. `closing` is the export's own departure:
    /// once it fires, no wait for the daemon goes on.
    pub(crate) fn open(
        data_dir: &SinomoDataDir,
        mount_identity: String,
        closing: &dfs::DfsCancellationFlag,
    ) -> Result<Self> {
        let daemon = DaemonApiClient::connect_for_paths(&DaemonPaths::new(data_dir))
            .map_err(|source| DfsNfsError::Dfs(source.to_string()))?;
        let client = DaemonDfsMountedMutationHandoffClient::new(
            data_dir.clone(),
            daemon.address().to_string(),
        )
        .map_err(|source| DfsNfsError::Dfs(source.to_string()))?;
        client
            .probe()
            .map_err(|source| DfsNfsError::Dfs(source.to_string()))?;
        let store = dfs::MountedSifsMutationOutboxStore::new(data_dir)
            .map_err(|source| DfsNfsError::Dfs(source.to_string()))?;
        let session = store
            .open_host_mount_session(dfs::MountedSifsMutationAdapter::Nfs, mount_identity)
            .map_err(|source| DfsNfsError::Dfs(source.to_string()))?;
        Self::start(store, session, Arc::new(client), closing)
    }

    fn start(
        store: dfs::MountedSifsMutationOutboxStore,
        session: dfs::MountedSifsHostMountSessionLease,
        daemon: Arc<dyn dfs::MountedSifsHandoffDaemon>,
        closing: &dfs::DfsCancellationFlag,
    ) -> Result<Self> {
        let handoff = dfs::MountedSifsMutationHandoff::start(store, session, daemon, Some(closing))
            .map_err(|source| {
                DfsNfsError::Dfs(format!("start NFS mounted handoff worker: {source}"))
            })?;
        Ok(Self {
            handoff,
            lane: dfs::MountedSifsLock::new(()),
        })
    }

    fn admit(&self) -> Result<dfs::MountedSifsLockGuard<'_, ()>> {
        dfs::MountedSifsCall::lock(&self.lane).map_err(|source| {
            DfsNfsError::Dfs(format!(
                "NFS mounted-mutation lane was not entered: {source}"
            ))
        })
    }

    /// Open the export's service inside its first handoff. The export is not
    /// serving yet: a step the daemon does not acknowledge fails the open
    /// instead of being sent again, and the marker is left to recovery.
    pub(crate) fn run_mount_open<T>(&self, open: impl FnOnce() -> Result<T>) -> Result<T> {
        let _admission = self.admit()?;
        let mut lease = self
            .handoff
            .prepare("nfs.mount-open", None)
            .map_err(step_error)?;
        let arm_request_id = dfs::MountedSifsMutationArmRequestId::generate();
        self.handoff
            .arm(&lease, &arm_request_id)
            .map_err(step_error)?;
        let opened = match open() {
            Ok(opened) => opened,
            Err(error) => {
                // Full daemon recovery is the evidence-preserving fallback.
                let captured =
                    dfs::MountedSifsMutationCapturedWork::new(mount_open_committed_work());
                if let Ok(observation) = dfs::MountedSifsMutationErrorObservation::new(
                    captured,
                    "nfs-mount-open-service-error",
                ) && self
                    .handoff
                    .persist_unknown(&mut lease, &arm_request_id, &observation)
                    .and_then(|()| {
                        self.handoff
                            .commit_unknown(&lease, &arm_request_id, &observation)
                    })
                    .is_ok()
                {
                    let _ = self.handoff.release_after_daemon_ack(lease);
                }
                return Err(error);
            }
        };
        let captured = dfs::MountedSifsMutationCapturedWork::new(mount_open_committed_work());
        self.handoff
            .persist_captured(&mut lease, &arm_request_id, &captured)
            .and_then(|()| {
                self.handoff
                    .commit_captured(&lease, &arm_request_id, &captured)
            })
            .map_err(step_error)?;
        self.handoff
            .release_after_daemon_ack(lease)
            .map_err(|source| DfsNfsError::Dfs(source.to_string()))?;
        Ok(opened)
    }

    /// Run one mutation of a serving export inside its handoff.
    ///
    /// The request's operation budget, when the export has one, bounds every
    /// wait here. A step still unacknowledged at that deadline moves to the
    /// export's re-sender and the request ends with an I/O error; the export
    /// keeps serving. The service lock is held only while the mutation runs,
    /// never across a wait for the daemon.
    ///
    /// A run that hands back a wait for a newer shard record runs again after
    /// that wait, which the request makes holding neither the service lock
    /// nor its mutation admission. The marker stays armed across the runs, so
    /// a waiting request writes nothing to the outbox: the daemon is handed
    /// the work of every run once, when the request ends.
    pub(crate) fn run_operation<R>(
        &self,
        operation: &'static str,
        stable_operation_id: Option<String>,
        service: &ServiceLock,
        mut mutation: impl FnMut(
            &mut dfs::MountedSifsService,
        ) -> std::result::Result<R, dfs::DfsPosixError>,
    ) -> std::result::Result<R, dfs::DfsPosixError> {
        let deadline = dfs::MountedSifsCall::deadline();
        let mut admission = Some(self.admit().map_err(handoff_posix_error)?);
        // A full outbox is not a refusal: wait for a marker's release.
        let mut prepared = None;
        let mut stable_operation_id = Some(stable_operation_id);
        self.handoff
            .until_acknowledged(deadline, || {
                let id = stable_operation_id.take().unwrap_or_default();
                match self.handoff.prepare(operation, id.clone()) {
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
            .map_err(|ended| ended_error("Prepare", ended))?;
        let lease = prepared.expect("an acknowledged Prepare returned its lease");
        let arm_request_id = dfs::MountedSifsMutationArmRequestId::generate();
        match self
            .handoff
            .until_acknowledged(deadline, || self.handoff.arm(&lease, &arm_request_id))
        {
            Ok(()) => {}
            Err(dfs::MountedSifsHandoffEnded::Left) => {
                // The daemon may hold the Arm. The mutation never enters the
                // service; the marker is closed once the daemon answers.
                self.handoff.finish_later(
                    lease,
                    dfs::MountedSifsUnfinishedHandoff::ArmedWithoutEntry { arm_request_id },
                );
                return Err(ended_error("Arm", dfs::MountedSifsHandoffEnded::Left));
            }
            Err(ended @ dfs::MountedSifsHandoffEnded::Rejected(_)) => {
                // The daemon refused the Arm, so the mutation never runs.
                self.release_unstarted(lease, deadline);
                return Err(ended_error("Arm", ended));
            }
        }
        let mut entered = match dfs::MountedSifsCall::lock_service(service) {
            Ok(service) => service,
            Err(error) => {
                self.finish_no_write(lease, &arm_request_id, "nfs-service-not-entered", deadline)?;
                return Err(handoff_posix_error(DfsNfsError::Dfs(error.to_string())));
            }
        };
        let mut replay = dfs::MountedSifsCallReplay::new();
        let mut committed_work = dfs::DfsCommittedWork::default();
        let outcome = loop {
            let report =
                entered.capture_mounted_mutation(|service| replay.run(service, &mut mutation));
            drop(entered);
            committed_work.merge(report.committed_work);
            let Some(wait) = replay.take_record_wait() else {
                break report.outcome;
            };
            drop(admission.take());
            wait.wait();
            let reentered = self.admit().and_then(|admitted| {
                dfs::MountedSifsCall::lock_service(service)
                    .map(|service| (admitted, service))
                    .map_err(|error| DfsNfsError::Dfs(error.to_string()))
            });
            match reentered {
                Ok((admitted, service)) => {
                    admission = Some(admitted);
                    entered = service;
                }
                // The runs before may have written: what they left is handed
                // to the daemon as observed.
                Err(error) => break Err(handoff_posix_error(error)),
            }
        };
        let captured = dfs::MountedSifsMutationCapturedWork::new(committed_work);
        match outcome {
            Ok(value) => {
                self.finish_captured(lease, &arm_request_id, captured, deadline)?;
                Ok(value)
            }
            Err(error) if positive_no_write_error(&error) && capture_is_empty(&captured) => {
                self.finish_no_write(
                    lease,
                    &arm_request_id,
                    "nfs-typed-error-before-write",
                    deadline,
                )?;
                Err(error)
            }
            Err(error) => {
                // The error is this request's answer. What it may have left
                // behind is handed to the daemon as observed, and the export
                // keeps serving.
                let observation =
                    dfs::MountedSifsMutationErrorObservation::new(captured, nfs_error_code(&error))
                        .map_err(|source| {
                            handoff_posix_error(DfsNfsError::Dfs(source.to_string()))
                        })?;
                self.finish_unknown(lease, &arm_request_id, observation, deadline)?;
                Err(error)
            }
        }
    }

    /// Close a marker whose Arm the daemon definitely refused: its mutation
    /// never ran, so the marker is aborted as unstarted.
    fn release_unstarted(
        &self,
        lease: dfs::MountedSifsMutationLease<'_>,
        deadline: Option<Instant>,
    ) {
        match self
            .handoff
            .until_acknowledged(deadline, || self.handoff.abort_unstarted(&lease))
        {
            Ok(()) => {
                let _ = self.handoff.release_after_daemon_ack(lease);
            }
            Err(dfs::MountedSifsHandoffEnded::Left) => self
                .handoff
                .finish_later(lease, dfs::MountedSifsUnfinishedHandoff::Unstarted),
            Err(dfs::MountedSifsHandoffEnded::Rejected(detail)) => {
                self.handoff.release_refused(lease, &detail);
            }
        }
    }

    fn finish_captured(
        &self,
        mut lease: dfs::MountedSifsMutationLease<'_>,
        arm_request_id: &dfs::MountedSifsMutationArmRequestId,
        captured: dfs::MountedSifsMutationCapturedWork,
        deadline: Option<Instant>,
    ) -> std::result::Result<(), dfs::DfsPosixError> {
        let mut persisted = false;
        let ended = self.handoff.until_acknowledged(deadline, || {
            if !persisted {
                self.handoff
                    .persist_captured(&mut lease, arm_request_id, &captured)?;
                persisted = true;
            }
            self.handoff
                .commit_captured(&lease, arm_request_id, &captured)
        });
        self.finish(
            lease,
            ended,
            "captured work",
            dfs::MountedSifsUnfinishedHandoff::Captured {
                arm_request_id: arm_request_id.clone(),
                captured_work: captured,
            },
        )
    }

    fn finish_unknown(
        &self,
        mut lease: dfs::MountedSifsMutationLease<'_>,
        arm_request_id: &dfs::MountedSifsMutationArmRequestId,
        observation: dfs::MountedSifsMutationErrorObservation,
        deadline: Option<Instant>,
    ) -> std::result::Result<(), dfs::DfsPosixError> {
        let mut persisted = false;
        let ended = self.handoff.until_acknowledged(deadline, || {
            if !persisted {
                self.handoff
                    .persist_unknown(&mut lease, arm_request_id, &observation)?;
                persisted = true;
            }
            self.handoff
                .commit_unknown(&lease, arm_request_id, &observation)
        });
        self.finish(
            lease,
            ended,
            "error observation",
            dfs::MountedSifsUnfinishedHandoff::Unknown {
                arm_request_id: arm_request_id.clone(),
                observation,
            },
        )
    }

    fn finish_no_write(
        &self,
        mut lease: dfs::MountedSifsMutationLease<'_>,
        arm_request_id: &dfs::MountedSifsMutationArmRequestId,
        proof_code: &'static str,
        deadline: Option<Instant>,
    ) -> std::result::Result<(), dfs::DfsPosixError> {
        let mut persisted = false;
        let ended = self.handoff.until_acknowledged(deadline, || {
            if !persisted {
                self.handoff
                    .persist_proven_no_write(&mut lease, arm_request_id, proof_code)?;
                persisted = true;
            }
            self.handoff
                .commit_proven_no_write(&lease, arm_request_id, proof_code)
        });
        self.finish(
            lease,
            ended,
            "no-write proof",
            dfs::MountedSifsUnfinishedHandoff::NoWrite {
                arm_request_id: arm_request_id.clone(),
                proof_code: proof_code.to_string(),
            },
        )
    }

    /// End one closing step: release an acknowledged marker, hand a step
    /// that outlived its request to the re-sender, or give up a refused one.
    fn finish(
        &self,
        lease: dfs::MountedSifsMutationLease<'_>,
        ended: std::result::Result<(), dfs::MountedSifsHandoffEnded>,
        step: &'static str,
        unfinished: dfs::MountedSifsUnfinishedHandoff,
    ) -> std::result::Result<(), dfs::DfsPosixError> {
        match ended {
            Ok(()) => self
                .handoff
                .release_after_daemon_ack(lease)
                .map_err(|source| handoff_posix_error(DfsNfsError::Dfs(source.to_string()))),
            Err(dfs::MountedSifsHandoffEnded::Left) => {
                self.handoff.finish_later(lease, unfinished);
                Err(ended_error(step, dfs::MountedSifsHandoffEnded::Left))
            }
            Err(dfs::MountedSifsHandoffEnded::Rejected(detail)) => {
                self.handoff.release_refused(lease, &detail);
                Err(ended_error(
                    step,
                    dfs::MountedSifsHandoffEnded::Rejected(detail),
                ))
            }
        }
    }
}

fn mount_open_committed_work() -> dfs::DfsCommittedWork {
    dfs::DfsCommittedWork {
        publish_metadata: true,
        repair_maintenance: true,
        namespace_maintenance: true,
        metadata_replica_progress: true,
        metadata_materialization: true,
        semantic_changes: true,
    }
}

fn capture_is_empty(captured: &dfs::MountedSifsMutationCapturedWork) -> bool {
    captured.committed_work == dfs::DfsCommittedWork::default()
}

fn positive_no_write_error(error: &dfs::DfsPosixError) -> bool {
    matches!(
        error,
        dfs::DfsPosixError::NotFound { .. }
            | dfs::DfsPosixError::AlreadyExists { .. }
            | dfs::DfsPosixError::NotDirectory { .. }
            | dfs::DfsPosixError::IsDirectory { .. }
            | dfs::DfsPosixError::NotSymlink { .. }
            | dfs::DfsPosixError::MissingParent { .. }
            | dfs::DfsPosixError::InvalidPath { .. }
            | dfs::DfsPosixError::DirectoryNotEmpty { .. }
    )
}

fn nfs_error_code(error: &dfs::DfsPosixError) -> &'static str {
    match error {
        dfs::DfsPosixError::NotFound { .. } => "nfs-dfs-not-found",
        dfs::DfsPosixError::AlreadyExists { .. } => "nfs-dfs-already-exists",
        dfs::DfsPosixError::NotDirectory { .. } => "nfs-dfs-not-directory",
        dfs::DfsPosixError::IsDirectory { .. } => "nfs-dfs-is-directory",
        dfs::DfsPosixError::NotSymlink { .. } => "nfs-dfs-not-symlink",
        dfs::DfsPosixError::MissingParent { .. } => "nfs-dfs-missing-parent",
        dfs::DfsPosixError::InvalidPath { .. } => "nfs-dfs-invalid-path",
        dfs::DfsPosixError::PendingWrite { .. } => "nfs-dfs-pending-write",
        dfs::DfsPosixError::SymlinkContentPending { .. } => "nfs-dfs-symlink-pending",
        dfs::DfsPosixError::Conflict { .. } => "nfs-dfs-conflict",
        dfs::DfsPosixError::DirectoryNotEmpty { .. } => "nfs-dfs-directory-not-empty",
        dfs::DfsPosixError::PartialBulkCommit { .. } => "nfs-dfs-partial-bulk-commit",
        dfs::DfsPosixError::Dfs(error) if error.is_commit_outcome_unknown() => {
            "nfs-dfs-commit-outcome-unknown"
        }
        dfs::DfsPosixError::Dfs(_) => "nfs-dfs-error",
        dfs::DfsPosixError::Metadata(_) => "nfs-metadata-error",
    }
}

fn handoff_posix_error(error: DfsNfsError) -> dfs::DfsPosixError {
    dfs::DfsPosixError::Dfs(dfs::DfsError::Distributed {
        message: error.to_string(),
    })
}

fn step_error(error: dfs::MountedSifsHandoffStepError) -> DfsNfsError {
    DfsNfsError::Dfs(format!("NFS mounted-mutation {error}"))
}

/// The request's answer when a handoff step ended without the daemon's
/// acknowledgement. Both endings are an I/O error to the NFS client.
fn ended_error(step: &'static str, ended: dfs::MountedSifsHandoffEnded) -> dfs::DfsPosixError {
    handoff_posix_error(DfsNfsError::Dfs(match ended {
        dfs::MountedSifsHandoffEnded::Left => {
            format!("the daemon has not acknowledged the NFS {step} handoff step")
        }
        dfs::MountedSifsHandoffEnded::Rejected(detail) => {
            format!("the daemon refused the NFS {step} handoff step: {detail}")
        }
    }))
}

#[cfg(test)]
mod tests;
