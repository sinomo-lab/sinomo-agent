//! Crash-safe ownership transfer for mounted NFS operations.
//!
//! NFS is a long-lived mounted client, not a second daemon. Clustered reads may
//! ingest authoritative namespace journals and writes may commit DFS work, so
//! the same outbox/daemon handoff used by FUSE and Cloud Files is mandatory.

use crate::{DfsNfsError, Result};
use sinomo_api::DaemonApiClient;
use sinomo_api::DaemonPaths;
use sinomo_core::local::SinomoDataDir;
use sinomo_dfs as dfs;
use sinomo_dfs::DaemonDfsMountedMutationHandoffClient;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};

#[derive(Debug)]
pub(crate) struct NfsMountedMutationHandoff {
    store: dfs::MountedSifsMutationOutboxStore,
    session: Option<dfs::MountedSifsHostMountSessionLease>,
    client: DaemonDfsMountedMutationHandoffClient,
    gated: AtomicBool,
    lane: Mutex<()>,
}

impl NfsMountedMutationHandoff {
    pub(crate) fn open(data_dir: &SinomoDataDir, mount_identity: String) -> Result<Self> {
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
        Ok(Self {
            store,
            session: Some(session),
            client,
            gated: AtomicBool::new(false),
            lane: Mutex::new(()),
        })
    }

    pub(crate) fn is_gated(&self) -> bool {
        self.gated.load(Ordering::Acquire)
    }

    fn gate(&self, detail: &str) {
        self.gated.store(true, Ordering::Release);
        tracing::error!(target: "sinomo.dfs.nfs", detail, "NFS mounted mutation was fenced");
    }

    fn admit(&self) -> Result<MutexGuard<'_, ()>> {
        self.lane
            .lock()
            .map_err(|_| DfsNfsError::Dfs("NFS mounted-mutation lane is poisoned".to_string()))
    }

    fn session(&self) -> Result<&dfs::MountedSifsHostMountSessionLease> {
        self.session
            .as_ref()
            .ok_or_else(|| DfsNfsError::Dfs("NFS mounted-mutation session is closed".to_string()))
    }

    fn prepare(
        &self,
        operation: &'static str,
        stable_operation_id: Option<String>,
    ) -> Result<dfs::MountedSifsMutationLease<'_>> {
        self.store
            .prepare(self.session()?, stable_operation_id, operation)
            .map_err(|source| DfsNfsError::Dfs(source.to_string()))
    }

    fn arm(
        &self,
        lease: &dfs::MountedSifsMutationLease<'_>,
    ) -> Result<dfs::MountedSifsMutationArmRequestId> {
        let arm_request_id = dfs::MountedSifsMutationArmRequestId::generate();
        let marker = lease.marker();
        self.client
            .arm(
                marker.marker_id.clone(),
                marker.marker_nonce.clone(),
                arm_request_id.clone(),
            )
            .map_err(|source| DfsNfsError::Dfs(source.to_string()))?;
        Ok(arm_request_id)
    }

    pub(crate) fn run_mount_open<T>(&self, open: impl FnOnce() -> Result<T>) -> Result<T> {
        let _admission = self.admit()?;
        if self.is_gated() {
            return Err(DfsNfsError::Dfs(
                "NFS mounted-mutation handoff is fenced".to_string(),
            ));
        }
        let lease = self.prepare("nfs.mount-open", None)?;
        let arm_request_id = self.arm(&lease)?;
        let opened = match open() {
            Ok(opened) => opened,
            Err(error) => {
                self.gate("mounted SIFS service failed to open after NFS Arm");
                let captured =
                    dfs::MountedSifsMutationCapturedWork::new(mount_open_committed_work());
                let observation = dfs::MountedSifsMutationErrorObservation::new(
                    captured,
                    "nfs-mount-open-service-error",
                )
                .map_err(|source| DfsNfsError::Dfs(source.to_string()))?;
                self.finish_unknown(lease, &arm_request_id, &observation)?;
                return Err(error);
            }
        };
        let captured = dfs::MountedSifsMutationCapturedWork::new(mount_open_committed_work());
        if let Err(error) = self.finish_captured(lease, &arm_request_id, &captured) {
            self.gate("daemon rejected NFS mount-open committed work");
            return Err(error);
        }
        Ok(opened)
    }

    pub(crate) fn run_operation<R>(
        &self,
        operation: &'static str,
        stable_operation_id: Option<String>,
        service: &Mutex<dfs::MountedSifsService>,
        mutation: impl FnOnce(
            &mut dfs::MountedSifsService,
        ) -> std::result::Result<R, dfs::DfsPosixError>,
    ) -> std::result::Result<R, dfs::DfsPosixError> {
        let _admission = self.admit().map_err(handoff_posix_error)?;
        if self.is_gated() {
            return Err(handoff_posix_error(DfsNfsError::Dfs(
                "NFS mounted-mutation handoff is fenced".to_string(),
            )));
        }
        let lease = self
            .prepare(operation, stable_operation_id)
            .map_err(handoff_posix_error)?;
        let arm_request_id = self.arm(&lease).map_err(handoff_posix_error)?;
        let report = {
            let mut service = service.lock().map_err(|_| {
                handoff_posix_error(DfsNfsError::Dfs(
                    "NFS mounted SIFS service is poisoned".to_string(),
                ))
            })?;
            service.capture_mounted_mutation(mutation)
        };
        let captured = dfs::MountedSifsMutationCapturedWork::new(report.committed_work);
        match report.outcome {
            Ok(value) => {
                self.finish_captured(lease, &arm_request_id, &captured)
                    .map_err(|error| {
                        self.gate("daemon rejected captured NFS mutation work");
                        handoff_posix_error(error)
                    })?;
                Ok(value)
            }
            Err(error) if positive_no_write_error(&error) && capture_is_empty(&captured) => {
                self.finish_no_write(lease, &arm_request_id, "nfs-typed-error-before-write")
                    .map_err(|handoff_error| {
                        self.gate("daemon rejected NFS no-write proof");
                        handoff_posix_error(handoff_error)
                    })?;
                Err(error)
            }
            Err(error) => {
                self.gate("NFS mutation returned an ambiguous error after service entry");
                let observation =
                    dfs::MountedSifsMutationErrorObservation::new(captured, nfs_error_code(&error))
                        .map_err(|source| {
                            handoff_posix_error(DfsNfsError::Dfs(source.to_string()))
                        })?;
                self.finish_unknown(lease, &arm_request_id, &observation)
                    .map_err(handoff_posix_error)?;
                Err(error)
            }
        }
    }

    fn finish_captured(
        &self,
        mut lease: dfs::MountedSifsMutationLease<'_>,
        arm_request_id: &dfs::MountedSifsMutationArmRequestId,
        captured: &dfs::MountedSifsMutationCapturedWork,
    ) -> Result<()> {
        lease
            .record_captured_success(arm_request_id, captured.clone())
            .map_err(|source| DfsNfsError::Dfs(source.to_string()))?;
        let marker = lease.marker();
        self.client
            .commit_applied(
                marker.marker_id.clone(),
                marker.marker_nonce.clone(),
                arm_request_id.clone(),
                captured.clone(),
            )
            .map_err(|source| DfsNfsError::Dfs(source.to_string()))?;
        lease
            .release_after_daemon_ack()
            .map_err(|source| DfsNfsError::Dfs(source.to_string()))
    }

    fn finish_unknown(
        &self,
        mut lease: dfs::MountedSifsMutationLease<'_>,
        arm_request_id: &dfs::MountedSifsMutationArmRequestId,
        observation: &dfs::MountedSifsMutationErrorObservation,
    ) -> Result<()> {
        lease
            .record_unknown_after_error(arm_request_id, observation.clone())
            .map_err(|source| DfsNfsError::Dfs(source.to_string()))?;
        let marker = lease.marker();
        self.client
            .commit_unknown(
                marker.marker_id.clone(),
                marker.marker_nonce.clone(),
                arm_request_id.clone(),
                observation.clone(),
            )
            .map_err(|source| DfsNfsError::Dfs(source.to_string()))?;
        lease
            .release_after_daemon_ack()
            .map_err(|source| DfsNfsError::Dfs(source.to_string()))
    }

    fn finish_no_write(
        &self,
        mut lease: dfs::MountedSifsMutationLease<'_>,
        arm_request_id: &dfs::MountedSifsMutationArmRequestId,
        proof_code: &'static str,
    ) -> Result<()> {
        lease
            .record_proven_no_write(arm_request_id, proof_code)
            .map_err(|source| DfsNfsError::Dfs(source.to_string()))?;
        let marker = lease.marker();
        self.client
            .prove_no_write(
                marker.marker_id.clone(),
                marker.marker_nonce.clone(),
                arm_request_id.clone(),
                proof_code,
            )
            .map_err(|source| DfsNfsError::Dfs(source.to_string()))?;
        lease
            .release_after_daemon_ack()
            .map_err(|source| DfsNfsError::Dfs(source.to_string()))
    }
}

impl Drop for NfsMountedMutationHandoff {
    fn drop(&mut self) {
        if let Some(session) = self.session.take()
            && let Err(error) = session.close_clean()
        {
            self.gated.store(true, Ordering::Release);
            tracing::error!(target: "sinomo.dfs.nfs", %error, "failed to close NFS mount session");
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
        dfs::DfsPosixError::Dfs(_) => "nfs-dfs-error",
        dfs::DfsPosixError::Metadata(_) => "nfs-metadata-error",
    }
}

fn handoff_posix_error(error: DfsNfsError) -> dfs::DfsPosixError {
    dfs::DfsPosixError::Dfs(dfs::DfsError::Distributed {
        message: error.to_string(),
    })
}
