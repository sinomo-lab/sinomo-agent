//! Platform-neutral state and worker shell for writable Windows Cloud Files.
//!
//! The live CFAPI adapter stays gated until DFS exposes every stable mutation
//! and read-only receipt-reconciliation contract. This module still owns the
//! ordering contract so the Windows layer only supplies notifications,
//! concrete DFS calls, and idempotent placeholder effects.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};
use std::thread;
use std::time::{Duration, Instant};

use sinomo_dfs_cloud_files_api::{
    CLOUD_FILES_SYNC_ATTENTION_SUMMARY_LIMIT, CLOUD_FILES_WRITABLE_SYNC_CONTRACT_NOT_READY_CODE,
    CloudFilesLocalChangeSyncStatus, CloudFilesModuleError, CloudFilesPlaceholderIdentity,
    CloudFilesRetainedLocalEvidenceSummary, CloudFilesSyncAttentionKind,
    CloudFilesSyncAttentionNextAction, CloudFilesSyncAttentionSummary,
};
use sinomo_runtime::dfs;
use sinomo_runtime::local::SinomoDataDir;

use crate::local_change_queue::{
    CloudFilesConflictEvidence, CloudFilesDestinationPrecondition, CloudFilesExpectedObjectState,
    CloudFilesLocalChangeQueue, CloudFilesLocalChangeQueueError, CloudFilesLocalOperation,
    CloudFilesLocalOperationKind, CloudFilesLocalOperationPreconditions,
    CloudFilesLocalOperationRequest, CloudFilesLocalOperationStatus,
    CloudFilesRemoteMutationEvidence, CloudFilesStagedContent, fresh_local_change_scope_is_absent,
};
use crate::namespace_reconciler::{
    CloudFilesLocalPathState, CloudFilesMaterializationAuditIntentStore, CloudFilesNamespaceAction,
    CloudFilesNamespaceConflict, CloudFilesNamespaceCursor, CloudFilesNamespaceEffectAdapter,
    CloudFilesNamespaceFeedBatch, CloudFilesNamespaceFeedEvent, CloudFilesNamespaceProvenance,
    CloudFilesNamespaceReconcileError, CloudFilesNamespaceReconcileProgress,
    CloudFilesNamespaceReconciler, CloudFilesNamespaceSuppression,
    CloudFilesReconciledManifestEntry, fresh_reconciliation_scope_is_absent,
};
#[cfg(test)]
use crate::sync_executor::CloudFilesLocalCompletionOutcome;
use crate::sync_executor::{
    CloudFilesDfsMutationAdapter, CloudFilesDfsMutationCommand, CloudFilesDfsMutationOutcome,
    CloudFilesDfsMutationReconciliation, CloudFilesLocalCompletionEffect,
    CloudFilesSyncExecutionProgress, CloudFilesSyncExecutorError,
    execute_ready_cloud_files_operations_bounded,
};

/// One remote mutation per cooperative pass keeps cancellation latency tied to
/// one bounded DFS operation rather than to the current queue depth.
const CLOUD_FILES_MUTATIONS_PER_PASS: usize = 1;
/// Poll/process one shard-local feed batch per pass. Later batches retain their
/// durable input cursors and are fetched again on the next pass.
const CLOUD_FILES_FEED_BATCHES_PER_PASS: usize = 1;
const CLOUD_FILES_FEED_EVENTS_PER_PASS: usize = 32;
pub(crate) const CLOUD_FILES_DIRECTORY_PAGE_ENTRIES: usize = 32;
const CLOUD_FILES_SERVICE_LOCK_TIMEOUT: Duration = Duration::from_millis(250);
const CLOUD_FILES_SERVICE_LOCK_RETRY: Duration = Duration::from_millis(5);
const CLOUD_FILES_MOUNT_OPEN_OPERATION: &str = "cloud-files.mount-open";
const CLOUD_FILES_MOUNT_OPEN_ERROR: &str = "cloud-files-mount-open-error";
const CLOUD_FILES_MOUNT_OPEN_CAPTURE_INVALID: &str = "cloud-files-mount-open-capture-invalid";

/// Strictly read-only admission for a Fresh v3 registration. The mutable store
/// constructors are deliberately not called until a nonce journal owns their
/// effects. Any exact pre-existing scope is retained and fails closed.
pub(crate) fn fresh_writable_scopes_are_absent(
    data_dir: &SinomoDataDir,
    mount: &str,
    root: &str,
) -> Result<bool, CloudFilesWindowsSyncError> {
    if !fresh_local_change_scope_is_absent(data_dir, mount, root).map_err(queue_error)? {
        return Ok(false);
    }
    fresh_reconciliation_scope_is_absent(data_dir, mount, root).map_err(reconcile_error)
}

/// Apply the mounted-reader policy used by the production Cloud Files service.
/// Explicit directory refresh remains handoff-wrapped, but stat/lstat must not
/// implicitly ingest replica journals through an unowned SyncPath fallback.
pub(crate) fn configure_cloud_files_mounted_read_policy<T: dfs::DfsMessageTransport>(
    service: &mut dfs::NetworkedDfsService<T>,
) {
    service.disable_implicit_read_replica_ingest();
}

fn cloud_files_mount_open_committed_work() -> dfs::DfsCommittedWork {
    // Backend constructors and bootstrap recovery predate operation-local work
    // tracking. Conservatively transfer every bounded daemon ownership bit.
    dfs::DfsCommittedWork {
        publish_metadata: true,
        repair_maintenance: true,
        namespace_maintenance: true,
        metadata_replica_progress: true,
        metadata_materialization: true,
    }
}

/// Local daemon seam for the mounted-mutation durability protocol. Keeping the
/// seam small lets the ordering contract be tested without starting a daemon;
/// the production implementation below is still the only constructor exposed
/// to the Windows mount path.
pub(crate) trait CloudFilesMountedMutationDaemon {
    fn probe(&self) -> Result<(), String>;

    fn arm(
        &self,
        marker_id: dfs::MountedSifsMutationMarkerId,
        marker_nonce: dfs::MountedSifsMutationNonce,
        arm_request_id: dfs::MountedSifsMutationArmRequestId,
    ) -> Result<(), String>;

    fn commit_applied(
        &self,
        marker_id: dfs::MountedSifsMutationMarkerId,
        marker_nonce: dfs::MountedSifsMutationNonce,
        arm_request_id: dfs::MountedSifsMutationArmRequestId,
        captured_work: dfs::MountedSifsMutationCapturedWork,
    ) -> Result<(), String>;

    fn commit_unknown(
        &self,
        marker_id: dfs::MountedSifsMutationMarkerId,
        marker_nonce: dfs::MountedSifsMutationNonce,
        arm_request_id: dfs::MountedSifsMutationArmRequestId,
        observation: dfs::MountedSifsMutationErrorObservation,
    ) -> Result<(), String>;

    fn prove_no_write(
        &self,
        marker_id: dfs::MountedSifsMutationMarkerId,
        marker_nonce: dfs::MountedSifsMutationNonce,
        arm_request_id: dfs::MountedSifsMutationArmRequestId,
        proof_code: &'static str,
    ) -> Result<(), String>;
}

impl CloudFilesMountedMutationDaemon
    for sinomo_runtime::api::DaemonDfsMountedMutationHandoffClient
{
    fn probe(&self) -> Result<(), String> {
        sinomo_runtime::api::DaemonDfsMountedMutationHandoffClient::probe(self)
            .map(drop)
            .map_err(|error| error.to_string())
    }

    fn arm(
        &self,
        marker_id: dfs::MountedSifsMutationMarkerId,
        marker_nonce: dfs::MountedSifsMutationNonce,
        arm_request_id: dfs::MountedSifsMutationArmRequestId,
    ) -> Result<(), String> {
        sinomo_runtime::api::DaemonDfsMountedMutationHandoffClient::arm(
            self,
            marker_id,
            marker_nonce,
            arm_request_id,
        )
        .map(drop)
        .map_err(|error| error.to_string())
    }

    fn commit_applied(
        &self,
        marker_id: dfs::MountedSifsMutationMarkerId,
        marker_nonce: dfs::MountedSifsMutationNonce,
        arm_request_id: dfs::MountedSifsMutationArmRequestId,
        captured_work: dfs::MountedSifsMutationCapturedWork,
    ) -> Result<(), String> {
        sinomo_runtime::api::DaemonDfsMountedMutationHandoffClient::commit_applied(
            self,
            marker_id,
            marker_nonce,
            arm_request_id,
            captured_work,
        )
        .map(drop)
        .map_err(|error| error.to_string())
    }

    fn commit_unknown(
        &self,
        marker_id: dfs::MountedSifsMutationMarkerId,
        marker_nonce: dfs::MountedSifsMutationNonce,
        arm_request_id: dfs::MountedSifsMutationArmRequestId,
        observation: dfs::MountedSifsMutationErrorObservation,
    ) -> Result<(), String> {
        sinomo_runtime::api::DaemonDfsMountedMutationHandoffClient::commit_unknown(
            self,
            marker_id,
            marker_nonce,
            arm_request_id,
            observation,
        )
        .map(drop)
        .map_err(|error| error.to_string())
    }

    fn prove_no_write(
        &self,
        marker_id: dfs::MountedSifsMutationMarkerId,
        marker_nonce: dfs::MountedSifsMutationNonce,
        arm_request_id: dfs::MountedSifsMutationArmRequestId,
        proof_code: &'static str,
    ) -> Result<(), String> {
        sinomo_runtime::api::DaemonDfsMountedMutationHandoffClient::prove_no_write(
            self,
            marker_id,
            marker_nonce,
            arm_request_id,
            proof_code,
        )
        .map(drop)
        .map_err(|error| error.to_string())
    }
}

struct CloudFilesMountedMutationHandoffInner<C> {
    store: dfs::MountedSifsMutationOutboxStore,
    session: Option<dfs::MountedSifsHostMountSessionLease>,
    client: C,
}

/// One mount-scoped handoff owner. Its mutex is deliberately distinct from
/// the mounted DFS service mutex: it remains held while the service lock is
/// dropped for the daemon Commit RPC, preventing a later local mutation from
/// crossing an unacknowledged ambiguity fence.
pub(crate) struct CloudFilesMountedMutationHandoff<
    C = sinomo_runtime::api::DaemonDfsMountedMutationHandoffClient,
> {
    inner: Mutex<CloudFilesMountedMutationHandoffInner<C>>,
    gated: AtomicBool,
    gate_detail: Mutex<Option<String>>,
}

#[cfg(windows)]
impl CloudFilesMountedMutationHandoff {
    pub(crate) fn open(
        data_dir: &SinomoDataDir,
        mount_identity: impl Into<String>,
    ) -> Result<Self, String> {
        let daemon = sinomo_runtime::api::DaemonApiClient::connect_for_paths(
            &sinomo_runtime::api::DaemonPaths::new(data_dir),
        )
        .map_err(|error| error.to_string())?;
        let client = sinomo_runtime::api::DaemonDfsMountedMutationHandoffClient::new(
            data_dir.clone(),
            daemon.address().to_string(),
        )
        .map_err(|error| error.to_string())?;
        Self::open_with_client(data_dir, mount_identity, client)
    }
}

impl<C: CloudFilesMountedMutationDaemon> CloudFilesMountedMutationHandoff<C> {
    fn open_with_client(
        data_dir: &SinomoDataDir,
        mount_identity: impl Into<String>,
        client: C,
    ) -> Result<Self, String> {
        // Probe precedes even the host-session marker. A provider must never
        // become visible if its main daemon cannot accept durable handoffs.
        client.probe()?;
        let store = dfs::MountedSifsMutationOutboxStore::new(data_dir)
            .map_err(|error| error.to_string())?;
        let session = store
            .open_host_mount_session(dfs::MountedSifsMutationAdapter::CloudFiles, mount_identity)
            .map_err(|error| error.to_string())?;
        Ok(Self {
            inner: Mutex::new(CloudFilesMountedMutationHandoffInner {
                store,
                session: Some(session),
                client,
            }),
            gated: AtomicBool::new(false),
            gate_detail: Mutex::new(None),
        })
    }

    pub(crate) fn ensure_ready(&self) -> Result<(), String> {
        if self.gated.load(Ordering::Acquire) {
            return Err(self.gated_error());
        }
        let inner = self
            .inner
            .lock()
            .map_err(|_| "Cloud Files mounted-mutation lane is poisoned".to_string())?;
        if inner.session.is_none() {
            return Err("Cloud Files mounted-mutation host session is closed".to_string());
        }
        Ok(())
    }

    fn gated_error(&self) -> String {
        let detail = self
            .gate_detail
            .lock()
            .ok()
            .and_then(|detail| detail.clone())
            .unwrap_or_else(|| "the original ambiguity detail is unavailable".to_string());
        format!("mounted-mutation handoff is gated after an ambiguous Cloud Files write: {detail}")
    }

    pub(crate) fn close_clean(&self) -> Result<(), String> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| "Cloud Files mounted-mutation lane is poisoned".to_string())?;
        let Some(session) = inner.session.take() else {
            return Ok(());
        };
        session.close_clean().map_err(|error| {
            let detail = format!("close Cloud Files mounted-mutation host session: {error}");
            self.record_gate_detail(&detail);
            detail
        })
    }

    fn record_gate_detail(&self, detail: &str) {
        if let Ok(mut first_detail) = self.gate_detail.lock()
            && first_detail.is_none()
        {
            *first_detail = Some(detail.to_string());
        }
        self.gated.store(true, Ordering::Release);
    }

    fn gate<T>(&self, detail: impl Into<String>) -> Result<T, String> {
        let detail = detail.into();
        self.record_gate_detail(&detail);
        #[cfg(windows)]
        super::host_log(&format!(
            "Cloud Files mounted-mutation handoff entered its ambiguity gate: {detail}"
        ));
        Err(detail)
    }

    /// Open and bootstrap the writable DFS service behind one durable mount
    /// marker before the Cloud Files registration can become host-visible.
    ///
    /// Constructor recovery is not operation-local today, so a successful
    /// open transfers every committed-work bit plus the service's exact
    /// durable repair queue snapshot. Any open error or unrepresentable repair
    /// snapshot remains a full-recovery ambiguity.
    pub(crate) fn run_captured_mount_open<R, E>(
        &self,
        open: impl FnOnce() -> Result<(R, Vec<dfs::DfsRepairAttemptRecord>), E>,
    ) -> Result<R, String>
    where
        E: std::fmt::Display,
    {
        let inner = self
            .inner
            .lock()
            .map_err(|_| "Cloud Files mounted-mutation lane is poisoned".to_string())?;
        if self.gated.load(Ordering::Acquire) {
            return Err(self.gated_error());
        }
        let session = inner
            .session
            .as_ref()
            .ok_or_else(|| "Cloud Files mounted-mutation host session is closed".to_string())?;
        let mut lease = match inner
            .store
            .prepare(session, None, CLOUD_FILES_MOUNT_OPEN_OPERATION)
        {
            Ok(lease) => lease,
            Err(error) => {
                return self.gate(format!(
                    "prepare durable Cloud Files mount open before DFS service entry: {error}"
                ));
            }
        };
        let arm_request_id = dfs::MountedSifsMutationArmRequestId::generate();
        let marker = lease.marker().clone();
        if let Err(error) = inner.client.arm(
            marker.marker_id.clone(),
            marker.marker_nonce.clone(),
            arm_request_id.clone(),
        ) {
            return self.gate(format!(
                "arm Cloud Files mount open before DFS service entry: {error}"
            ));
        }

        let (opened, repair_records) = match open() {
            Ok(opened) => opened,
            Err(error) => {
                let service_error = error.to_string();
                self.record_gate_detail(&format!(
                    "Cloud Files DFS mount open returned after service entry: {service_error}"
                ));
                let completion = self.finish_mount_open_unknown(
                    &inner,
                    lease,
                    marker,
                    arm_request_id,
                    CLOUD_FILES_MOUNT_OPEN_ERROR,
                );
                return match completion {
                    Ok(()) => Err(format!(
                        "Cloud Files DFS mount open returned after service entry: {service_error}"
                    )),
                    Err(handoff_error) => Err(format!(
                        "Cloud Files DFS mount open returned after service entry: {service_error}; commit Unknown mount-open evidence: {handoff_error}"
                    )),
                };
            }
        };
        let captured_work = match dfs::MountedSifsMutationCapturedWork::new(
            cloud_files_mount_open_committed_work(),
            repair_records,
        ) {
            Ok(captured_work) => captured_work,
            Err(error) => {
                let capture_error = error.to_string();
                self.record_gate_detail(&format!(
                    "capture Cloud Files DFS mount-open recovery work: {capture_error}"
                ));
                let completion = self.finish_mount_open_unknown(
                    &inner,
                    lease,
                    marker,
                    arm_request_id,
                    CLOUD_FILES_MOUNT_OPEN_CAPTURE_INVALID,
                );
                return match completion {
                    Ok(()) => Err(format!(
                        "capture Cloud Files DFS mount-open recovery work: {capture_error}"
                    )),
                    Err(handoff_error) => Err(format!(
                        "capture Cloud Files DFS mount-open recovery work: {capture_error}; commit Unknown mount-open evidence: {handoff_error}"
                    )),
                };
            }
        };
        if let Err(error) = lease
            .record_captured_success(&arm_request_id, captured_work.clone())
            .map_err(|source| source.to_string())
            .and_then(|_| {
                inner.client.commit_applied(
                    marker.marker_id,
                    marker.marker_nonce,
                    arm_request_id,
                    captured_work,
                )
            })
            .and_then(|_| lease.release_after_daemon_ack().map_err(|e| e.to_string()))
        {
            return self.gate(format!(
                "commit captured Cloud Files mount open to the daemon: {error}"
            ));
        }
        Ok(opened)
    }

    fn finish_mount_open_unknown(
        &self,
        inner: &CloudFilesMountedMutationHandoffInner<C>,
        mut lease: dfs::MountedSifsMutationLease<'_>,
        marker: dfs::MountedSifsMutationOutboxMarker,
        arm_request_id: dfs::MountedSifsMutationArmRequestId,
        error_code: &'static str,
    ) -> Result<(), String> {
        let captured_work = dfs::MountedSifsMutationCapturedWork::new(
            cloud_files_mount_open_committed_work(),
            Vec::new(),
        )
        .map_err(|error| error.to_string())?;
        let observation = dfs::MountedSifsMutationErrorObservation::new(captured_work, error_code)
            .map_err(|error| error.to_string())?;
        lease
            .record_unknown_after_error(&arm_request_id, observation.clone())
            .map_err(|error| error.to_string())?;
        inner.client.commit_unknown(
            marker.marker_id,
            marker.marker_nonce,
            arm_request_id,
            observation,
        )?;
        lease
            .release_after_daemon_ack()
            .map_err(|error| error.to_string())
    }

    fn run_captured_mutation<S, R, E>(
        &self,
        service: &Arc<Mutex<S>>,
        stable_operation_id: &str,
        operation: &'static str,
        mutation: impl FnOnce(&mut S) -> dfs::MountedSifsMutationReport<Result<R, E>>,
    ) -> Result<R, String>
    where
        E: std::fmt::Display,
    {
        // This guard is the mutation lane. It covers Prepare through lease
        // release even while the independent service mutex is dropped.
        let inner = self
            .inner
            .lock()
            .map_err(|_| "Cloud Files mounted-mutation lane is poisoned".to_string())?;
        if self.gated.load(Ordering::Acquire) {
            return Err(self.gated_error());
        }
        let session = inner
            .session
            .as_ref()
            .ok_or_else(|| "Cloud Files mounted-mutation host session is closed".to_string())?;
        let mut lease =
            match inner
                .store
                .prepare(session, Some(stable_operation_id.to_string()), operation)
            {
                Ok(lease) => lease,
                Err(error) => {
                    return self.gate(format!(
                    "prepare durable Cloud Files mounted mutation before DFS service entry: {error}"
                ));
                }
            };
        let arm_request_id = dfs::MountedSifsMutationArmRequestId::generate();
        let marker = lease.marker().clone();
        if let Err(error) = inner.client.arm(
            marker.marker_id.clone(),
            marker.marker_nonce.clone(),
            arm_request_id.clone(),
        ) {
            return self.gate(format!(
                "arm Cloud Files mounted mutation before DFS service entry: {error}"
            ));
        }

        let mut service = match lock_service_with_timeout(service, CLOUD_FILES_SERVICE_LOCK_TIMEOUT)
        {
            Ok(service) => service,
            Err(error) => {
                const PROOF: &str = "cloud-files-service-lock-not-entered";
                let completion = lease
                    .record_proven_no_write(&arm_request_id, PROOF)
                    .map_err(|source| source.to_string())
                    .and_then(|_| {
                        inner.client.prove_no_write(
                            marker.marker_id,
                            marker.marker_nonce,
                            arm_request_id,
                            PROOF,
                        )
                    })
                    .and_then(|_| lease.release_after_daemon_ack().map_err(|e| e.to_string()));
                return match completion {
                    Ok(()) => Err(error),
                    Err(handoff_error) => self.gate(format!(
                        "prove Cloud Files mutation did not enter DFS after lock failure: {handoff_error}"
                    )),
                };
            }
        };
        let report = mutation(&mut service);
        // The daemon may block on fsync/queue ownership. Never hold the DFS
        // service mutex during Commit, while still retaining the lane above.
        drop(service);

        let captured_work = dfs::MountedSifsMutationCapturedWork::new(
            report.committed_work,
            report.committed_repair_records,
        )
        .map_err(|error| {
            let detail = format!("validate captured Cloud Files mounted mutation: {error}");
            self.record_gate_detail(&detail);
            detail
        })?;
        match report.outcome {
            Ok(value) => {
                if let Err(error) = lease
                    .record_captured_success(&arm_request_id, captured_work.clone())
                    .map_err(|source| source.to_string())
                    .and_then(|_| {
                        inner.client.commit_applied(
                            marker.marker_id,
                            marker.marker_nonce,
                            arm_request_id,
                            captured_work,
                        )
                    })
                    .and_then(|_| lease.release_after_daemon_ack().map_err(|e| e.to_string()))
                {
                    return self.gate(format!(
                        "commit captured Cloud Files mutation to the daemon: {error}"
                    ));
                }
                Ok(value)
            }
            Err(error) => {
                // Once the service was entered, every Err is ambiguous even if
                // this operation's work capture is empty. Gate later receipt
                // reconciliation as well as later mutations.
                let service_error = error.to_string();
                self.record_gate_detail(&format!(
                    "Cloud Files DFS mutation returned after service entry: {service_error}"
                ));
                let observation = dfs::MountedSifsMutationErrorObservation::new(
                    captured_work,
                    "cloud-files-dfs-service-error",
                )
                .map_err(|source| source.to_string())?;
                lease
                    .record_unknown_after_error(&arm_request_id, observation.clone())
                    .map_err(|source| source.to_string())?;
                inner.client.commit_unknown(
                    marker.marker_id,
                    marker.marker_nonce,
                    arm_request_id,
                    observation,
                )?;
                lease
                    .release_after_daemon_ack()
                    .map_err(|source| source.to_string())?;
                Err(format!(
                    "Cloud Files DFS mutation returned after service entry: {service_error}"
                ))
            }
        }
    }

    /// Capture a mounted read which may synchronously ingest a metadata
    /// journal into the local durable replica.
    ///
    /// Unlike a user mutation, an ordinary DFS read error does not make the
    /// local effect ambiguous. The operation-local receipt remains exact even
    /// when the read returns `Err`, so acknowledge that captured work before
    /// returning the nested read outcome to the adapter. Only a failure of the
    /// handoff protocol itself gates the mount.
    pub(crate) fn run_captured_read_ingest<S, R, E>(
        &self,
        service: &Arc<Mutex<S>>,
        operation: &'static str,
        read: impl FnOnce(&mut S) -> dfs::MountedSifsMutationReport<Result<R, E>>,
    ) -> Result<Result<R, E>, String> {
        // Use the same mount-wide lane as writes. A directory refresh must not
        // enter DFS while an earlier write is waiting for its daemon ACK, nor
        // may a later write cross this refresh before its captured replica
        // progress has a durable owner.
        let inner = self
            .inner
            .lock()
            .map_err(|_| "Cloud Files mounted-mutation lane is poisoned".to_string())?;
        if self.gated.load(Ordering::Acquire) {
            return Err(self.gated_error());
        }
        let session = inner
            .session
            .as_ref()
            .ok_or_else(|| "Cloud Files mounted-mutation host session is closed".to_string())?;
        let mut lease = match inner.store.prepare(session, None, operation) {
            Ok(lease) => lease,
            Err(error) => {
                return self.gate(format!(
                    "prepare durable Cloud Files mounted read ingest before DFS service entry: {error}"
                ));
            }
        };
        let arm_request_id = dfs::MountedSifsMutationArmRequestId::generate();
        let marker = lease.marker().clone();
        if let Err(error) = inner.client.arm(
            marker.marker_id.clone(),
            marker.marker_nonce.clone(),
            arm_request_id.clone(),
        ) {
            return self.gate(format!(
                "arm Cloud Files mounted read ingest before DFS service entry: {error}"
            ));
        }

        let mut service = match lock_service_with_timeout(service, CLOUD_FILES_SERVICE_LOCK_TIMEOUT)
        {
            Ok(service) => service,
            Err(error) => {
                const PROOF: &str = "cloud-files-read-ingest-service-lock-not-entered";
                let completion = lease
                    .record_proven_no_write(&arm_request_id, PROOF)
                    .map_err(|source| source.to_string())
                    .and_then(|_| {
                        inner.client.prove_no_write(
                            marker.marker_id,
                            marker.marker_nonce,
                            arm_request_id,
                            PROOF,
                        )
                    })
                    .and_then(|_| lease.release_after_daemon_ack().map_err(|e| e.to_string()));
                return match completion {
                    Ok(()) => Err(error),
                    Err(handoff_error) => self.gate(format!(
                        "prove Cloud Files read ingest did not enter DFS after lock failure: {handoff_error}"
                    )),
                };
            }
        };
        let report = read(&mut service);
        // Preserve the lane while releasing the DFS mutex for the daemon RPC.
        drop(service);

        let captured_work = match dfs::MountedSifsMutationCapturedWork::new(
            report.committed_work,
            report.committed_repair_records,
        ) {
            Ok(captured_work) => captured_work,
            Err(error) => {
                return self.gate(format!(
                    "validate captured Cloud Files mounted read ingest: {error}"
                ));
            }
        };
        if let Err(error) = lease
            .record_captured_success(&arm_request_id, captured_work.clone())
            .map_err(|source| source.to_string())
            .and_then(|_| {
                inner.client.commit_applied(
                    marker.marker_id,
                    marker.marker_nonce,
                    arm_request_id,
                    captured_work,
                )
            })
            .and_then(|_| lease.release_after_daemon_ack().map_err(|e| e.to_string()))
        {
            return self.gate(format!(
                "commit captured Cloud Files mounted read ingest to the daemon: {error}"
            ));
        }
        Ok(report.outcome)
    }
}

impl<C> Drop for CloudFilesMountedMutationHandoff<C> {
    fn drop(&mut self) {
        let inner = match self.inner.get_mut() {
            Ok(inner) => inner,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(session) = inner.session.take()
            && session.close_clean().is_err()
        {
            self.gated.store(true, Ordering::Release);
            if let Ok(detail) = self.gate_detail.get_mut()
                && detail.is_none()
            {
                *detail = Some(
                    "drop Cloud Files mounted-mutation host session after close failure"
                        .to_string(),
                );
            }
        }
    }
}

pub(crate) fn sparse_root_is_enumerated(
    prepopulate_depth: u32,
    already_materialized: bool,
) -> bool {
    prepopulate_depth > 0 || already_materialized
}

pub(crate) fn sparse_child_directory_is_enumerated(
    child_depth: u32,
    prepopulate_depth: u32,
    already_materialized: bool,
) -> bool {
    child_depth < prepopulate_depth || already_materialized
}

pub(crate) fn cloud_files_root_kind_is_supported(kind: &dfs::DfsObjectKind) -> bool {
    matches!(
        kind,
        dfs::DfsObjectKind::Directory | dfs::DfsObjectKind::Mount
    )
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct CloudFilesSparseKnownPathPage {
    pub path: Option<String>,
    pub before: Option<String>,
    pub complete: bool,
    pub examined: usize,
}

/// Select at most one materialized manifest path while examining a fixed
/// number of reverse-lexicographic keys. Prefix descendants sort after their
/// parent, so reverse traversal guarantees leaf-before-parent deletion within
/// each missing subtree without cloning/sorting the whole manifest.
pub(crate) fn next_sparse_known_path(
    manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
    materialized_directories: &BTreeSet<String>,
    before: Option<&str>,
    examine_limit: usize,
) -> CloudFilesSparseKnownPathPage {
    use std::ops::Bound::{Excluded, Unbounded};

    let mut page = CloudFilesSparseKnownPathPage {
        before: before.map(str::to_string),
        ..CloudFilesSparseKnownPathPage::default()
    };
    if examine_limit == 0 {
        return page;
    }
    for _ in 0..examine_limit {
        let next = match page.before.as_deref() {
            Some(before) => manifest
                .range::<str, _>((Unbounded, Excluded(before)))
                .next_back(),
            None => manifest.iter().next_back(),
        };
        let Some((path, _)) = next else {
            page.complete = true;
            break;
        };
        page.examined = page.examined.saturating_add(1);
        page.before = Some(path.clone());
        let owned = sifs_parent_path(path)
            .is_some_and(|parent| materialized_directories.contains(&parent))
            || materialized_directories.contains(path);
        if owned {
            page.path = Some(path.clone());
            break;
        }
    }
    page
}

/// Explicit Windows projection for every SIFS object kind. Unsupported kinds
/// stay authoritative in the sparse reconciliation manifest, but no file-like
/// placeholder is ever synthesized for them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CloudFilesObjectProjection {
    File,
    Directory,
    Unsupported(&'static str),
}

pub(crate) fn cloud_files_object_projection(
    kind: &dfs::DfsObjectKind,
) -> CloudFilesObjectProjection {
    match kind {
        dfs::DfsObjectKind::File => CloudFilesObjectProjection::File,
        dfs::DfsObjectKind::Directory | dfs::DfsObjectKind::Mount => {
            CloudFilesObjectProjection::Directory
        }
        dfs::DfsObjectKind::Symlink => CloudFilesObjectProjection::Unsupported("symlink"),
        dfs::DfsObjectKind::Hardlink => CloudFilesObjectProjection::Unsupported("hardlink"),
        dfs::DfsObjectKind::Artifact => CloudFilesObjectProjection::Unsupported("artifact"),
        dfs::DfsObjectKind::Tombstone => CloudFilesObjectProjection::Unsupported("tombstone"),
    }
}

#[derive(Debug)]
pub struct CloudFilesWindowsSyncError {
    pub code: &'static str,
    pub detail: String,
}

impl std::fmt::Display for CloudFilesWindowsSyncError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.detail)
    }
}

impl std::error::Error for CloudFilesWindowsSyncError {}

fn sync_error(code: &'static str, detail: impl Into<String>) -> CloudFilesWindowsSyncError {
    CloudFilesWindowsSyncError {
        code,
        detail: detail.into(),
    }
}

fn sifs_parent_path(path: &str) -> Option<String> {
    if path == "/" {
        return None;
    }
    let (parent, _) = path.rsplit_once('/')?;
    Some(if parent.is_empty() {
        "/".to_string()
    } else {
        parent.to_string()
    })
}

fn queue_error(error: CloudFilesLocalChangeQueueError) -> CloudFilesWindowsSyncError {
    sync_error("local-change-queue", error.to_string())
}

fn reconcile_error(error: CloudFilesNamespaceReconcileError) -> CloudFilesWindowsSyncError {
    if error.detail == "sync-cancelled" {
        sync_error(
            "sync-cancelled",
            "writable Cloud Files worker was cancelled",
        )
    } else {
        sync_error("namespace-reconciliation", error.to_string())
    }
}

fn executor_error(error: CloudFilesSyncExecutorError) -> CloudFilesWindowsSyncError {
    sync_error("mutation-executor", error.to_string())
}

/// One stable local observation produced by the Windows change accelerator.
///
/// `capture_id` must be stable for the same Windows change, normally from a
/// volume/file identity plus USN (or equivalent durable journal position).
/// File bytes are captured once and immediately moved into the queue's
/// immutable content-addressed staging area.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CloudFilesCapturedContent {
    /// Test/synthetic seam. Production Windows capture uses `Staged` and never
    /// materializes a whole file in memory.
    Inline(Vec<u8>),
    Staged(CloudFilesStagedContent),
}

/// Return whether immutable bytes captured from a projected file are already
/// the exact authoritative content recorded in the namespace manifest.
///
/// Cloud Files can clear `InSyncState` for metadata-only local observations.
/// That state is an accelerator, not proof that file bytes changed. Production
/// capture therefore compares the stable content-addressed blob before it
/// creates a conditional replace operation.
pub(crate) fn staged_content_matches_manifest_entry(
    staged: &CloudFilesStagedContent,
    entry: &CloudFilesReconciledManifestEntry,
) -> bool {
    entry.kind == "file"
        && entry.content_hash.as_deref() == Some(staged.content_hash.as_str())
        && entry.content_size_bytes == Some(staged.length)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloudFilesCapturedLocalChange {
    pub capture_id: String,
    pub kind: CloudFilesLocalOperationKind,
    pub source_path: String,
    pub destination_path: Option<String>,
    pub preconditions: CloudFilesLocalOperationPreconditions,
    pub captured_content: Option<CloudFilesCapturedContent>,
}

/// A local Cloud Files placeholder whose exact remote identity was omitted
/// from the intentionally sparse durable manifest. It must be authoritatively
/// recovered before the same local path can be classified as a create,
/// replace, delete, or rename.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloudFilesSparsePlaceholderObservation {
    pub local_path: String,
    pub identity: CloudFilesPlaceholderIdentity,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CloudFilesCapturedChangeBatch {
    pub changes: Vec<CloudFilesCapturedLocalChange>,
    pub sparse_placeholders: Vec<CloudFilesSparsePlaceholderObservation>,
    /// `ReadDirectoryChangesW` reports overflow by returning no usable byte
    /// range. The worker discards partial observations and performs a full
    /// reconciliation before accepting more notifications.
    pub overflowed: bool,
}

/// Semantic records emitted by the Windows directory-change parser. Keeping
/// the pairing state here makes split-buffer rename behavior testable without
/// requiring a Windows host.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CloudFilesDirectoryChange {
    Added(PathBuf),
    Modified(PathBuf),
    Removed(PathBuf),
    Renamed { from: PathBuf, to: PathBuf },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) enum CloudFilesDirectoryChangeAction {
    Added,
    Modified,
    Removed,
    RenamedOldName,
    RenamedNewName,
}

/// `ReadDirectoryChangesW` may split the old/new halves of one rename across
/// two buffers. The unmatched old name therefore belongs to the watcher, not
/// to one parser invocation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct CloudFilesRenamePairingState {
    pending_old_name: Option<PathBuf>,
}

impl CloudFilesRenamePairingState {
    pub(crate) fn push(
        &mut self,
        action: CloudFilesDirectoryChangeAction,
        path: PathBuf,
        output: &mut Vec<CloudFilesDirectoryChange>,
    ) {
        match action {
            CloudFilesDirectoryChangeAction::RenamedOldName => {
                if let Some(old) = self.pending_old_name.replace(path) {
                    output.push(CloudFilesDirectoryChange::Removed(old));
                }
            }
            CloudFilesDirectoryChangeAction::RenamedNewName => {
                if let Some(from) = self.pending_old_name.take() {
                    output.push(CloudFilesDirectoryChange::Renamed { from, to: path });
                } else {
                    output.push(CloudFilesDirectoryChange::Added(path));
                }
            }
            action => {
                if let Some(old) = self.pending_old_name.take() {
                    output.push(CloudFilesDirectoryChange::Removed(old));
                }
                output.push(match action {
                    CloudFilesDirectoryChangeAction::Added => {
                        CloudFilesDirectoryChange::Added(path)
                    }
                    CloudFilesDirectoryChangeAction::Modified => {
                        CloudFilesDirectoryChange::Modified(path)
                    }
                    CloudFilesDirectoryChangeAction::Removed => {
                        CloudFilesDirectoryChange::Removed(path)
                    }
                    CloudFilesDirectoryChangeAction::RenamedOldName
                    | CloudFilesDirectoryChangeAction::RenamedNewName => unreachable!(),
                });
            }
        }
    }

    #[cfg_attr(not(windows), allow(dead_code))]
    pub(crate) fn reset_for_overflow(&mut self) {
        self.pending_old_name = None;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CloudFilesDirectoryChangePlan {
    Ordered(Vec<CloudFilesDirectoryChange>),
    FullScanRequired,
}

/// Preserve notification order and accept coalescing only when every repeated
/// path is an upsert. Remove+recreate, modify+rename, and rename chains need a
/// snapshot because a set-based reduction cannot retain their semantics.
pub(crate) fn plan_directory_changes(
    changes: Vec<CloudFilesDirectoryChange>,
) -> CloudFilesDirectoryChangePlan {
    let mut seen = BTreeMap::<PathBuf, bool>::new();
    for change in &changes {
        let roles: Vec<(&PathBuf, bool)> = match change {
            CloudFilesDirectoryChange::Added(path) | CloudFilesDirectoryChange::Modified(path) => {
                vec![(path, true)]
            }
            CloudFilesDirectoryChange::Removed(path) => vec![(path, false)],
            CloudFilesDirectoryChange::Renamed { from, to } => {
                vec![(from, false), (to, false)]
            }
        };
        for (path, upsert) in roles {
            if let Some(previous_upsert) = seen.get(path) {
                if !(*previous_upsert && upsert) {
                    return CloudFilesDirectoryChangePlan::FullScanRequired;
                }
            } else {
                seen.insert(path.clone(), upsert);
            }
        }
    }

    let mut keep = vec![true; changes.len()];
    let mut last_upsert = BTreeMap::<PathBuf, usize>::new();
    for (index, change) in changes.iter().enumerate() {
        if let CloudFilesDirectoryChange::Added(path) | CloudFilesDirectoryChange::Modified(path) =
            change
            && let Some(previous) = last_upsert.insert(path.clone(), index)
        {
            keep[previous] = false;
        }
    }
    let ordered = changes
        .into_iter()
        .enumerate()
        .filter_map(|(index, change)| keep[index].then_some(change))
        .collect();
    CloudFilesDirectoryChangePlan::Ordered(ordered)
}

pub(crate) fn local_capture_retry_delay(attempt: u32) -> Duration {
    const INITIAL_MILLIS: u64 = 50;
    const MAX_MILLIS: u64 = 2_000;
    let shift = attempt.saturating_sub(1).min(6);
    Duration::from_millis(
        INITIAL_MILLIS
            .saturating_mul(1_u64 << shift)
            .min(MAX_MILLIS),
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CloudFilesDeferredCaptureRetry {
    attempts: u32,
    retry_at: Duration,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct CloudFilesLocalCaptureRetries {
    deferred: BTreeMap<PathBuf, CloudFilesDeferredCaptureRetry>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CloudFilesOfflineCreatePlan {
    QueueNow,
    DeferUntilParentIsRemote,
    ConflictWithRemote,
}

/// Classify a no-identity local path discovered in a previously owned root.
/// Upload is allowed only with an authoritative absent destination and a
/// remotely established directory parent. Nested offline subtrees advance one
/// parent generation at a time rather than racing unordered child creates.
pub(crate) fn plan_owned_offline_create(
    root: &str,
    path: &str,
    remote_manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
) -> CloudFilesOfflineCreatePlan {
    if remote_manifest.contains_key(path) {
        return CloudFilesOfflineCreatePlan::ConflictWithRemote;
    }
    let parent = path
        .rsplit_once('/')
        .map(|(parent, _)| if parent.is_empty() { "/" } else { parent })
        .unwrap_or("/");
    if parent == root {
        return CloudFilesOfflineCreatePlan::QueueNow;
    }
    match remote_manifest.get(parent) {
        Some(entry) if matches!(entry.kind.as_str(), "directory" | "mount") => {
            CloudFilesOfflineCreatePlan::QueueNow
        }
        Some(_) => CloudFilesOfflineCreatePlan::ConflictWithRemote,
        None => CloudFilesOfflineCreatePlan::DeferUntilParentIsRemote,
    }
}

impl CloudFilesLocalCaptureRetries {
    pub(crate) fn schedule(&mut self, path: PathBuf, now: Duration) -> u32 {
        let attempts = self
            .deferred
            .get(&path)
            .map_or(1, |retry| retry.attempts.saturating_add(1));
        self.deferred.insert(
            path,
            CloudFilesDeferredCaptureRetry {
                attempts,
                retry_at: now.saturating_add(local_capture_retry_delay(attempts)),
            },
        );
        attempts
    }

    #[cfg(test)]
    pub(crate) fn due(&self, now: Duration) -> Vec<PathBuf> {
        self.due_bounded(now, usize::MAX)
    }

    pub(crate) fn due_bounded(&self, now: Duration, limit: usize) -> Vec<PathBuf> {
        self.deferred
            .iter()
            .filter(|(_, retry)| retry.retry_at <= now)
            .take(limit)
            .map(|(path, _)| path.clone())
            .collect()
    }

    pub(crate) fn len(&self) -> usize {
        self.deferred.len()
    }

    pub(crate) fn next_due_in(&self, now: Duration) -> Option<Duration> {
        self.deferred
            .values()
            .map(|retry| retry.retry_at.saturating_sub(now))
            .min()
    }

    pub(crate) fn clear(&mut self, path: &PathBuf) {
        self.deferred.remove(path);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloudFilesStartupConflictEvidence {
    pub path: String,
    pub detail: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CloudFilesStartupScanBatch {
    pub changes: Vec<CloudFilesCapturedLocalChange>,
    pub sparse_placeholders: Vec<CloudFilesSparsePlaceholderObservation>,
    pub complete: bool,
}

pub trait CloudFilesLocalChangeAccelerator {
    /// Arm a lossless observer without draining, classifying, staging, or
    /// enqueueing any local change. The returned epoch is the beginning of the
    /// recovery audit barrier.
    fn arm_cursor_recovery_observer(&mut self) -> Result<u64, String>;

    /// Return the current raw observer epoch. Any change, notification
    /// overflow, or observer failure since the arm epoch must fail closed.
    fn cursor_recovery_observation_epoch(&self) -> Result<u64, String>;

    /// Complete one marker-bound create/remove round trip on the continuously
    /// armed observer. The implementation must acknowledge only after parsing
    /// the complete batches containing both exact sentinel actions and must
    /// fail on any non-sentinel record, overflow, or observer failure. This is
    /// a namespace-event fence; the synchronous CFAPI in-sync audit below is
    /// the corresponding content-state fence.
    fn cursor_recovery_namespace_fence(
        &mut self,
        recovery_id: &str,
        recovery_token: &str,
        nonce: &str,
    ) -> Result<u64, String> {
        let _ = (recovery_id, recovery_token, nonce);
        self.cursor_recovery_observation_epoch()
    }

    /// Read-only final proof for authorized cursor recovery. Implementations
    /// must recursively verify that every physical local descendant is the
    /// exact in-sync entry described by the recovered authoritative manifest.
    /// Missing authoritative descendants remain valid sparse projection and
    /// are populated lazily after release. This hook must never stage content
    /// or classify a local mutation.
    fn audit_cursor_recovery_root(
        &mut self,
        mount: &str,
        root: &str,
        remote_manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
        cancelled: &AtomicBool,
    ) -> Result<(), String>;

    /// Release the exact child handles retained by the recovery audit. The
    /// worker calls this only after synchronous host Ready and durable removal
    /// of the plan-bound namespace Terminal marker.
    fn release_cursor_recovery_root_audit(&mut self) {}

    /// Revalidate the parked observer snapshot immediately before the
    /// namespace Terminal marker is removed. Any post-terminal change,
    /// overflow, or watcher failure keeps the recovery fail-closed.
    fn verify_cursor_recovery_root_audit(&self) -> Result<(), String> {
        Ok(())
    }

    /// Discover local divergence that may have happened while the provider was
    /// stopped. The previous durable manifest distinguishes a true local
    /// delete from a remote path that has never been materialized locally.
    fn startup_scan(
        &mut self,
        _previous_manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
        _remote_manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
        _cancelled: &AtomicBool,
    ) -> Result<CloudFilesStartupScanBatch, String> {
        Ok(CloudFilesStartupScanBatch {
            changes: Vec::new(),
            sparse_placeholders: Vec::new(),
            complete: true,
        })
    }

    /// Return local differences that cannot safely become remote mutations.
    /// The worker persists these independently from the mutation queue before
    /// it can report readiness.
    fn drain_startup_conflicts(&mut self) -> Vec<CloudFilesStartupConflictEvidence> {
        Vec::new()
    }

    /// Requeue one classification after the worker has durably recovered its
    /// exact remote identity. `accepted=false` permanently blocks this scan
    /// observation; it must never fall through to offline-create handling.
    fn finish_sparse_placeholder_resolution(&mut self, _path: &str, _accepted: bool) {}

    /// Start the lossy notification accelerator before startup scanning. This
    /// closes the scan-to-connection observation gap; placeholder effects are
    /// filtered through the exact local suppression ledger.
    fn activate(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn drain_changes(
        &mut self,
        manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
        cancelled: &AtomicBool,
    ) -> Result<CloudFilesCapturedChangeBatch, String>;

    /// Deadline for local paths whose stable-read capture was deferred by a
    /// live writer. `None` means the observer has no timer-owned work and the
    /// worker may block indefinitely until an explicit notification arrives.
    fn next_capture_retry_in(&self) -> Option<Duration> {
        None
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CloudFilesFullScanSnapshot {
    /// Authoritative upserts observed in this bounded scan quantum.
    pub manifest: BTreeMap<String, CloudFilesReconciledManifestEntry>,
    /// Previously materialized paths authoritatively absent in this quantum.
    pub removed_paths: Vec<String>,
    /// Directory audit intents authoritatively resolved in this quantum. An
    /// existing directory becomes durable ownership evidence even when its
    /// page was empty or every child already matched. A directory also present
    /// in `removed_paths` is atomically removed instead, then its intent can be
    /// acknowledged without forcing an endless re-audit.
    pub materialized_directories: Vec<String>,
    /// Cursor boundary is meaningful only when this terminal delta is true.
    pub scan_complete: bool,
    pub cursors: BTreeMap<String, CloudFilesNamespaceCursor>,
}

/// Concrete Windows implementations obtain batches from the routed,
/// per-shard DFS feed and obtain a full placeholder snapshot from exact DFS
/// stat/list calls. A full snapshot must be taken after its cursor boundaries
/// are observed; replay after those older boundaries is intentionally safe.
pub trait CloudFilesNamespaceSource {
    fn poll_batches(
        &mut self,
        root: &str,
        cursors: &[CloudFilesNamespaceCursor],
    ) -> Result<Vec<CloudFilesNamespaceFeedBatch>, String>;

    fn full_scan(
        &mut self,
        root: &str,
        previous_cursors: &[CloudFilesNamespaceCursor],
        materialized_manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
        materialized_directories: &std::collections::BTreeSet<String>,
    ) -> Result<CloudFilesFullScanSnapshot, String>;

    fn resolve_placeholder_identity(
        &mut self,
        identity: &CloudFilesPlaceholderIdentity,
    ) -> Result<Option<CloudFilesReconciledManifestEntry>, String>;

    fn has_pending_materialized_directories(
        &self,
        _durable_materialized_directories: &BTreeSet<String>,
    ) -> Result<bool, String> {
        Ok(false)
    }

    fn acknowledge_materialized_directories(
        &mut self,
        _directories: &[String],
    ) -> Result<(), String> {
        Ok(())
    }

    /// Retires every durable population intent only after the stronger
    /// cursor-recovery root audit has recursively proved that every physical
    /// entry still matches the exact terminal manifest. Cursor recovery parks
    /// further population while this proof is established, so the set cannot
    /// grow across this acknowledgement.
    fn acknowledge_materialized_directories_after_cursor_recovery_root_audit(
        &mut self,
    ) -> Result<(), String> {
        Ok(())
    }
}

/// One idempotent Windows placeholder adapter shared by feed reconciliation,
/// local mutation completion, and startup/overflow full scans.
pub trait CloudFilesWindowsPlaceholderEffect:
    CloudFilesNamespaceEffectAdapter + CloudFilesLocalCompletionEffect
{
    /// Must be idempotent. A crash after this effect but before durable cursor
    /// installation repeats the same full reconciliation on restart.
    fn apply_full_scan(&mut self, snapshot: &CloudFilesFullScanSnapshot) -> Result<(), String>;

    /// Local completion computes suppression before its idempotent Windows
    /// effect, then hands it back for durable recording. If a process dies in
    /// the narrow gap, replay is still safe because every placeholder effect
    /// must recognize its already-applied exact identity.
    fn drain_namespace_suppressions(&mut self) -> Vec<CloudFilesNamespaceSuppression> {
        Vec::new()
    }
}

/// Additional readiness proof required by the writable worker. Merely
/// implementing the phase-3 mutation trait is insufficient: every mutation
/// kind, including mkdir, must have stable caller IDs and read-only receipt
/// reconciliation before Windows registration is allowed.
pub trait CloudFilesWritableMutationAdapter: CloudFilesDfsMutationAdapter {
    fn ensure_contracts_ready(&self) -> Result<(), String>;
}

/// Small concrete mapping layer. A DFS backend supplies one method per stable
/// mutation plus one read-only reconciliation method; the adapter validates
/// queue shapes before dispatching them.
pub trait CloudFilesStableMutationBackend {
    fn ensure_contracts_ready(&self) -> Result<(), String>;

    /// Reactive, non-authoritative notification emitted after the queue owns
    /// durable InFlight state and immediately before the remote mutation.
    /// Implementations must never make mutation success depend on decoration.
    fn operation_started(&mut self, _command: &CloudFilesDfsMutationCommand) {}

    fn create_file(
        &mut self,
        command: &CloudFilesDfsMutationCommand,
    ) -> Result<CloudFilesDfsMutationOutcome, String>;

    fn create_directory(
        &mut self,
        command: &CloudFilesDfsMutationCommand,
    ) -> Result<CloudFilesDfsMutationOutcome, String>;

    fn replace_file(
        &mut self,
        command: &CloudFilesDfsMutationCommand,
    ) -> Result<CloudFilesDfsMutationOutcome, String>;

    fn unlink(
        &mut self,
        command: &CloudFilesDfsMutationCommand,
    ) -> Result<CloudFilesDfsMutationOutcome, String>;

    fn rmdir(
        &mut self,
        command: &CloudFilesDfsMutationCommand,
    ) -> Result<CloudFilesDfsMutationOutcome, String>;

    fn rename(
        &mut self,
        command: &CloudFilesDfsMutationCommand,
    ) -> Result<CloudFilesDfsMutationOutcome, String>;

    /// This method may inspect receipts and authoritative generations, but it
    /// must not submit or replay the mutation itself.
    fn reconcile(
        &mut self,
        command: &CloudFilesDfsMutationCommand,
    ) -> Result<CloudFilesDfsMutationReconciliation, String>;
}

fn cloud_files_mounted_mutation_label(kind: &CloudFilesLocalOperationKind) -> &'static str {
    match kind {
        CloudFilesLocalOperationKind::CreateFile => "cloud-files.create-file",
        CloudFilesLocalOperationKind::CreateDirectory => "cloud-files.create-directory",
        CloudFilesLocalOperationKind::ReplaceFile => "cloud-files.replace-file",
        CloudFilesLocalOperationKind::DeleteFile => "cloud-files.unlink",
        CloudFilesLocalOperationKind::DeleteDirectory => "cloud-files.rmdir",
        CloudFilesLocalOperationKind::Rename => "cloud-files.rename",
    }
}

pub struct CloudFilesConcreteDfsAdapter<B> {
    backend: B,
}

/// Production mapping over the mounted SIFS service. The service is shared
/// with hydration/feed workers behind one short-lived mutex; durable queue
/// state is never held while this mutex is acquired.
pub struct CloudFilesDfsServiceMutationBackend<T> {
    service: Arc<Mutex<dfs::NetworkedDfsService<T>>>,
    write_options: dfs::DfsWriteOptions,
    cancelled: Arc<AtomicBool>,
    handoff: Arc<CloudFilesMountedMutationHandoff>,
    operation_started: Arc<dyn Fn(&CloudFilesDfsMutationCommand) + Send + Sync>,
}

impl<T> CloudFilesDfsServiceMutationBackend<T> {
    #[cfg(windows)]
    pub(crate) fn new_with_cancellation(
        service: Arc<Mutex<dfs::NetworkedDfsService<T>>>,
        write_options: dfs::DfsWriteOptions,
        cancelled: Arc<AtomicBool>,
        handoff: Arc<CloudFilesMountedMutationHandoff>,
        operation_started: Arc<dyn Fn(&CloudFilesDfsMutationCommand) + Send + Sync>,
    ) -> Self {
        Self {
            service,
            write_options,
            cancelled,
            handoff,
            operation_started,
        }
    }

    pub fn shared_service(&self) -> &Arc<Mutex<dfs::NetworkedDfsService<T>>> {
        &self.service
    }
}

struct CancellationReader<R> {
    inner: R,
    cancelled: Arc<AtomicBool>,
}

impl<R> CancellationReader<R> {
    fn new(inner: R, cancelled: Arc<AtomicBool>) -> Self {
        Self { inner, cancelled }
    }
}

impl<R: Read> Read for CancellationReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.cancelled.load(Ordering::SeqCst) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "Cloud Files DFS content upload was cancelled",
            ));
        }
        self.inner.read(buffer)
    }
}

fn definitive_namespace_conflict(error: &dfs::DfsPosixError) -> Option<CloudFilesConflictEvidence> {
    let code = match error {
        dfs::DfsPosixError::NotFound { .. } => "namespace-not-found",
        dfs::DfsPosixError::AlreadyExists { .. } => "namespace-already-exists",
        dfs::DfsPosixError::NotDirectory { .. } => "namespace-not-directory",
        dfs::DfsPosixError::IsDirectory { .. } => "namespace-is-directory",
        dfs::DfsPosixError::NotSymlink { .. } => "namespace-not-symlink",
        dfs::DfsPosixError::MissingParent { .. } => "namespace-missing-parent",
        dfs::DfsPosixError::InvalidPath { .. } => "namespace-invalid-path",
        dfs::DfsPosixError::Conflict { .. } => "namespace-generation-conflict",
        dfs::DfsPosixError::DirectoryNotEmpty { .. } => "namespace-directory-not-empty",
        dfs::DfsPosixError::Dfs(_)
        | dfs::DfsPosixError::Metadata(_)
        | dfs::DfsPosixError::PendingWrite { .. }
        | dfs::DfsPosixError::SymlinkContentPending { .. }
        | dfs::DfsPosixError::PartialBulkCommit { .. } => return None,
    };
    Some(conflict_evidence(code, error.to_string()))
}

fn classify_definitive_namespace_mutation_report<R>(
    report: dfs::MountedSifsMutationReport<Result<R, dfs::DfsPosixError>>,
) -> dfs::MountedSifsMutationReport<Result<Result<R, CloudFilesConflictEvidence>, dfs::DfsPosixError>>
{
    let outcome = match report.outcome {
        Ok(value) => Ok(Ok(value)),
        Err(error) => match definitive_namespace_conflict(&error) {
            Some(conflict) => Ok(Err(conflict)),
            None => Err(error),
        },
    };
    dfs::MountedSifsMutationReport {
        outcome,
        committed_work: report.committed_work,
        committed_repair_records: report.committed_repair_records,
    }
}

impl<T: dfs::DfsMessageTransport> CloudFilesStableMutationBackend
    for CloudFilesDfsServiceMutationBackend<T>
{
    fn ensure_contracts_ready(&self) -> Result<(), String> {
        self.handoff.ensure_ready()?;
        if self.write_options.replica_candidates.is_empty() {
            return Err(
                "no advertised DFS content replica is available for durable Cloud Files writes"
                    .to_string(),
            );
        }
        Ok(())
    }

    fn operation_started(&mut self, command: &CloudFilesDfsMutationCommand) {
        (self.operation_started)(command);
    }

    fn create_file(
        &mut self,
        command: &CloudFilesDfsMutationCommand,
    ) -> Result<CloudFilesDfsMutationOutcome, String> {
        let content = required_staged_content(command)?;
        let reader = File::open(&content.blob_path)
            .map_err(|error| format!("open immutable Cloud Files queue blob: {error}"))?;
        let reader = CancellationReader::new(reader, Arc::clone(&self.cancelled));
        let stat = self.handoff.run_captured_mutation(
            &self.service,
            &command.operation_id,
            cloud_files_mounted_mutation_label(&command.kind),
            |service| {
                service.capture_mounted_mutation(|service| {
                    service.posix_create_file_from_reader_with_operation_id(
                        &command.source_path,
                        reader,
                        self.write_options.clone(),
                        &command.operation_id,
                    )
                })
            },
        )?;
        Ok(CloudFilesDfsMutationOutcome::Applied(
            mutation_evidence_from_stat(command, stat),
        ))
    }

    fn create_directory(
        &mut self,
        command: &CloudFilesDfsMutationCommand,
    ) -> Result<CloudFilesDfsMutationOutcome, String> {
        let result = self.handoff.run_captured_mutation(
            &self.service,
            &command.operation_id,
            cloud_files_mounted_mutation_label(&command.kind),
            |service| {
                classify_definitive_namespace_mutation_report(service.capture_mounted_mutation(
                    |service| {
                        service.posix_mkdir_with_operation_id(
                            &command.source_path,
                            &command.operation_id,
                        )
                    },
                ))
            },
        )?;
        Ok(match result {
            Ok(stat) => {
                CloudFilesDfsMutationOutcome::Applied(mutation_evidence_from_stat(command, stat))
            }
            Err(conflict) => CloudFilesDfsMutationOutcome::Conflict(conflict),
        })
    }

    fn replace_file(
        &mut self,
        command: &CloudFilesDfsMutationCommand,
    ) -> Result<CloudFilesDfsMutationOutcome, String> {
        if let Some(stat) = self.authoritative_unchanged_replace(command)? {
            return Ok(CloudFilesDfsMutationOutcome::Applied(
                mutation_evidence_from_stat(command, stat),
            ));
        }
        let expected = required_source_precondition(command)?;
        let content = required_staged_content(command)?;
        let object_id = parse_object_id(&expected.object_id)?;
        let version = parse_version(&expected.version)?;
        let reader = File::open(&content.blob_path)
            .map_err(|error| format!("open immutable Cloud Files queue blob: {error}"))?;
        let reader = CancellationReader::new(reader, Arc::clone(&self.cancelled));
        let stat = self.handoff.run_captured_mutation(
            &self.service,
            &command.operation_id,
            cloud_files_mounted_mutation_label(&command.kind),
            |service| {
                service.capture_mounted_mutation(|service| {
                    service.posix_replace_file_if_version_from_reader_with_operation_id(
                        &command.source_path,
                        &object_id,
                        &version,
                        reader,
                        self.write_options.clone(),
                        &command.operation_id,
                    )
                })
            },
        )?;
        Ok(CloudFilesDfsMutationOutcome::Applied(
            mutation_evidence_from_stat(command, stat),
        ))
    }

    fn unlink(
        &mut self,
        command: &CloudFilesDfsMutationCommand,
    ) -> Result<CloudFilesDfsMutationOutcome, String> {
        let expected = namespace_object_precondition(required_source_precondition(command)?)?;
        let result = self.handoff.run_captured_mutation(
            &self.service,
            &command.operation_id,
            cloud_files_mounted_mutation_label(&command.kind),
            |service| {
                classify_definitive_namespace_mutation_report(service.capture_mounted_mutation(
                    |service| {
                        service.posix_unlink_if_generation(
                            &command.source_path,
                            command.operation_id.clone(),
                            expected,
                        )
                    },
                ))
            },
        )?;
        Ok(match result {
            Ok(stat) => {
                CloudFilesDfsMutationOutcome::Applied(mutation_evidence_from_stat(command, stat))
            }
            Err(conflict) => CloudFilesDfsMutationOutcome::Conflict(conflict),
        })
    }

    fn rmdir(
        &mut self,
        command: &CloudFilesDfsMutationCommand,
    ) -> Result<CloudFilesDfsMutationOutcome, String> {
        let expected = namespace_object_precondition(required_source_precondition(command)?)?;
        let result = self.handoff.run_captured_mutation(
            &self.service,
            &command.operation_id,
            cloud_files_mounted_mutation_label(&command.kind),
            |service| {
                classify_definitive_namespace_mutation_report(service.capture_mounted_mutation(
                    |service| {
                        service.posix_rmdir_if_generation(
                            &command.source_path,
                            command.operation_id.clone(),
                            expected,
                        )
                    },
                ))
            },
        )?;
        Ok(match result {
            Ok(stat) => {
                CloudFilesDfsMutationOutcome::Applied(mutation_evidence_from_stat(command, stat))
            }
            Err(conflict) => CloudFilesDfsMutationOutcome::Conflict(conflict),
        })
    }

    fn rename(
        &mut self,
        command: &CloudFilesDfsMutationCommand,
    ) -> Result<CloudFilesDfsMutationOutcome, String> {
        let destination_path = command
            .destination_path
            .as_deref()
            .ok_or_else(|| "rename command has no destination path".to_string())?;
        let source = namespace_object_precondition(required_source_precondition(command)?)?;
        let destination = namespace_destination_precondition(&command.preconditions.destination)?;
        let result = self.handoff.run_captured_mutation(
            &self.service,
            &command.operation_id,
            cloud_files_mounted_mutation_label(&command.kind),
            |service| {
                classify_definitive_namespace_mutation_report(service.capture_mounted_mutation(
                    |service| {
                        service.posix_rename_if_generation(
                            &command.source_path,
                            destination_path,
                            command.operation_id.clone(),
                            source,
                            destination,
                        )
                    },
                ))
            },
        )?;
        Ok(match result {
            Ok(stat) => {
                CloudFilesDfsMutationOutcome::Applied(mutation_evidence_from_stat(command, stat))
            }
            Err(conflict) => CloudFilesDfsMutationOutcome::Conflict(conflict),
        })
    }

    fn reconcile(
        &mut self,
        command: &CloudFilesDfsMutationCommand,
    ) -> Result<CloudFilesDfsMutationReconciliation, String> {
        // Receipt reconciliation is read-only and never arms a new marker, but
        // a failed prior handoff fences it so no queue state can cross that
        // ambiguity before daemon recovery closes the mount session.
        self.handoff.ensure_ready()?;
        match command.kind {
            CloudFilesLocalOperationKind::CreateFile => {
                let content = required_staged_content(command)?;
                let receipt = self
                    .lock_service()?
                    .reconcile_stable_create_file_operation(
                        &command.source_path,
                        &command.operation_id,
                        &content.descriptor.content_hash,
                        content.descriptor.length,
                    )
                    .map_err(|error| error.to_string())?;
                Ok(classify_write_reconciliation(command, receipt))
            }
            CloudFilesLocalOperationKind::ReplaceFile => {
                if let Some(stat) = self.authoritative_unchanged_replace(command)? {
                    return Ok(CloudFilesDfsMutationReconciliation::Applied(
                        mutation_evidence_from_stat(command, stat),
                    ));
                }
                let content = required_staged_content(command)?;
                let expected = required_source_precondition(command)?;
                let object_id = parse_object_id(&expected.object_id)?;
                let receipt = self
                    .lock_service()?
                    .reconcile_stable_write_operation(
                        &command.source_path,
                        &object_id,
                        &command.operation_id,
                        &content.descriptor.content_hash,
                        content.descriptor.length,
                    )
                    .map_err(|error| error.to_string())?;
                Ok(classify_write_reconciliation(command, receipt))
            }
            CloudFilesLocalOperationKind::CreateDirectory
            | CloudFilesLocalOperationKind::DeleteFile
            | CloudFilesLocalOperationKind::DeleteDirectory
            | CloudFilesLocalOperationKind::Rename => {
                let operation = namespace_operation(command)?;
                let receipt = self
                    .lock_service()?
                    .reconcile_stable_namespace_operation(&command.operation_id, &operation)
                    .map_err(|error| error.to_string())?;
                self.classify_namespace_reconciliation(command, receipt)
            }
        }
    }
}

impl<T: dfs::DfsMessageTransport> CloudFilesDfsServiceMutationBackend<T> {
    fn lock_service(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, dfs::NetworkedDfsService<T>>, String> {
        lock_service_with_timeout(&self.service, CLOUD_FILES_SERVICE_LOCK_TIMEOUT)
    }

    /// Resolve a content-identical conditional replacement without entering
    /// the mounted-mutation handoff. This both avoids needless writes and lets
    /// older durable queues drain operations captured before the no-op filter
    /// existed. The authoritative generation is re-read first, so a remote
    /// change still follows normal receipt/CAS reconciliation.
    fn authoritative_unchanged_replace(
        &self,
        command: &CloudFilesDfsMutationCommand,
    ) -> Result<Option<dfs::DfsStat>, String> {
        if !replace_command_may_have_unchanged_content(command)? {
            return Ok(None);
        }
        let expected = required_source_precondition(command)?;
        let content = required_staged_content(command)?;
        let stat = self
            .lock_service()?
            .posix_lstat_distributed(&command.source_path)
            .map_err(|error| {
                format!(
                    "verify content-identical Cloud Files replacement `{}`: {error}",
                    command.source_path
                )
            })?;
        Ok((stat.kind == dfs::DfsObjectKind::File
            && stat.object_id.to_string() == expected.object_id
            && stat.version.to_string() == expected.version
            && stat.content_hash.as_deref() == Some(content.descriptor.content_hash.as_str())
            && stat.size_bytes == Some(content.descriptor.length))
        .then_some(stat))
    }

    fn classify_namespace_reconciliation(
        &self,
        command: &CloudFilesDfsMutationCommand,
        reconciliation: dfs::DfsStableNamespaceOperationReconciliation,
    ) -> Result<CloudFilesDfsMutationReconciliation, String> {
        match reconciliation {
            dfs::DfsStableNamespaceOperationReconciliation::Applied { receipt } => {
                let visible_path = match command.kind {
                    CloudFilesLocalOperationKind::CreateDirectory => {
                        Some(command.source_path.as_str())
                    }
                    CloudFilesLocalOperationKind::Rename => command.destination_path.as_deref(),
                    CloudFilesLocalOperationKind::DeleteFile
                    | CloudFilesLocalOperationKind::DeleteDirectory => None,
                    CloudFilesLocalOperationKind::CreateFile
                    | CloudFilesLocalOperationKind::ReplaceFile => {
                        unreachable!("file writes use stable write reconciliation")
                    }
                };
                if let Some(path) = visible_path {
                    let stat = self
                        .lock_service()?
                        .posix_lstat_distributed(path)
                        .map_err(|error| {
                            format!(
                                "namespace receipt {} is applied, but exact result stat is not yet visible: {error}",
                                receipt.operation_id
                            )
                        })?;
                    return Ok(CloudFilesDfsMutationReconciliation::Applied(
                        mutation_evidence_from_stat(command, stat),
                    ));
                }
                let expected = required_source_precondition(command)?;
                Ok(CloudFilesDfsMutationReconciliation::Applied(
                    CloudFilesRemoteMutationEvidence {
                        operation_id: command.operation_id.clone(),
                        object_id: Some(expected.object_id.clone()),
                        version: Some(expected.version.clone()),
                        content_hash: expected.content_hash.clone(),
                        content_size_bytes: None,
                        kind: Some(
                            match command.kind {
                                CloudFilesLocalOperationKind::DeleteDirectory => "directory",
                                CloudFilesLocalOperationKind::DeleteFile => "file",
                                _ => unreachable!("only deletion lacks a visible result stat"),
                            }
                            .to_string(),
                        ),
                        detail: format!(
                            "stable namespace receipt {} proves deletion applied; result tombstone generation is delivered by the namespace feed",
                            receipt.operation_id
                        ),
                    },
                ))
            }
            dfs::DfsStableNamespaceOperationReconciliation::NotApplied => {
                Ok(CloudFilesDfsMutationReconciliation::NotApplied)
            }
            dfs::DfsStableNamespaceOperationReconciliation::AmbiguousOrExpired { reason } => {
                Ok(CloudFilesDfsMutationReconciliation::Ambiguous { detail: reason })
            }
            dfs::DfsStableNamespaceOperationReconciliation::Conflict { reason } => {
                Ok(CloudFilesDfsMutationReconciliation::Conflict(
                    conflict_evidence("stable-namespace-operation-conflict", reason),
                ))
            }
        }
    }
}

fn lock_service_with_timeout<T>(
    service: &Arc<Mutex<T>>,
    timeout: Duration,
) -> Result<MutexGuard<'_, T>, String> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| "mounted SIFS service lock deadline overflowed".to_string())?;
    loop {
        match service.try_lock() {
            Ok(guard) => return Ok(guard),
            Err(TryLockError::Poisoned(_)) => {
                return Err("mounted SIFS service lock is poisoned".to_string());
            }
            Err(TryLockError::WouldBlock) if Instant::now() >= deadline => {
                return Err(format!(
                    "mounted SIFS service remained busy for {} ms",
                    timeout.as_millis()
                ));
            }
            Err(TryLockError::WouldBlock) => thread::sleep(CLOUD_FILES_SERVICE_LOCK_RETRY),
        }
    }
}

fn required_staged_content(
    command: &CloudFilesDfsMutationCommand,
) -> Result<&crate::sync_executor::CloudFilesDfsMutationContent, String> {
    command
        .staged_content
        .as_ref()
        .ok_or_else(|| "file mutation has no immutable staged content".to_string())
}

fn required_source_precondition(
    command: &CloudFilesDfsMutationCommand,
) -> Result<&crate::local_change_queue::CloudFilesExpectedObjectState, String> {
    command
        .preconditions
        .source
        .as_ref()
        .ok_or_else(|| "conditional mutation has no exact source generation".to_string())
}

fn replace_command_may_have_unchanged_content(
    command: &CloudFilesDfsMutationCommand,
) -> Result<bool, String> {
    if command.kind != CloudFilesLocalOperationKind::ReplaceFile {
        return Ok(false);
    }
    let expected = required_source_precondition(command)?;
    let content = required_staged_content(command)?;
    Ok(expected.content_hash.as_deref() == Some(content.descriptor.content_hash.as_str()))
}

fn parse_object_id(value: &str) -> Result<dfs::DfsObjectId, String> {
    value
        .parse()
        .map_err(|error| format!("invalid DFS object ID `{value}`: {error}"))
}

fn parse_version(value: &str) -> Result<dfs::DfsVersion, String> {
    value
        .parse()
        .map_err(|error| format!("invalid DFS version `{value}`: {error}"))
}

fn namespace_object_precondition(
    expected: &crate::local_change_queue::CloudFilesExpectedObjectState,
) -> Result<dfs::DfsNamespaceObjectPrecondition, String> {
    Ok(dfs::DfsNamespaceObjectPrecondition {
        object_id: parse_object_id(&expected.object_id)?,
        version: parse_version(&expected.version)?,
    })
}

fn namespace_destination_precondition(
    expected: &CloudFilesDestinationPrecondition,
) -> Result<dfs::DfsNamespaceDestinationPrecondition, String> {
    match expected {
        CloudFilesDestinationPrecondition::Absent => {
            Ok(dfs::DfsNamespaceDestinationPrecondition::Absent)
        }
        CloudFilesDestinationPrecondition::Exact(expected) => {
            Ok(dfs::DfsNamespaceDestinationPrecondition::Exact(
                namespace_object_precondition(expected)?,
            ))
        }
        CloudFilesDestinationPrecondition::Unspecified => {
            Err("conditional rename has no destination generation fence".to_string())
        }
    }
}

fn namespace_operation(
    command: &CloudFilesDfsMutationCommand,
) -> Result<dfs::DfsNamespaceOperation, String> {
    Ok(match command.kind {
        CloudFilesLocalOperationKind::CreateDirectory => dfs::DfsNamespaceOperation::Mkdir {
            path: command.source_path.clone(),
        },
        CloudFilesLocalOperationKind::DeleteFile => dfs::DfsNamespaceOperation::ConditionalUnlink {
            path: command.source_path.clone(),
            expected: namespace_object_precondition(required_source_precondition(command)?)?,
        },
        CloudFilesLocalOperationKind::DeleteDirectory => {
            dfs::DfsNamespaceOperation::ConditionalRmdir {
                path: command.source_path.clone(),
                expected: namespace_object_precondition(required_source_precondition(command)?)?,
            }
        }
        CloudFilesLocalOperationKind::Rename => dfs::DfsNamespaceOperation::ConditionalRename {
            from: command.source_path.clone(),
            to: command
                .destination_path
                .clone()
                .ok_or_else(|| "rename command has no destination path".to_string())?,
            expected_source: namespace_object_precondition(required_source_precondition(command)?)?,
            expected_destination: namespace_destination_precondition(
                &command.preconditions.destination,
            )?,
        },
        CloudFilesLocalOperationKind::CreateFile | CloudFilesLocalOperationKind::ReplaceFile => {
            return Err("file writes do not use namespace reconciliation".to_string());
        }
    })
}

fn mutation_evidence_from_stat(
    command: &CloudFilesDfsMutationCommand,
    stat: dfs::DfsStat,
) -> CloudFilesRemoteMutationEvidence {
    CloudFilesRemoteMutationEvidence {
        operation_id: command.operation_id.clone(),
        object_id: Some(stat.object_id.to_string()),
        version: Some(stat.version.to_string()),
        content_hash: stat.content_hash,
        content_size_bytes: stat.size_bytes,
        kind: Some(dfs_object_kind_name(&stat.kind).to_string()),
        detail: format!("stable DFS mutation applied at {}", stat.path),
    }
}

fn classify_write_reconciliation(
    command: &CloudFilesDfsMutationCommand,
    reconciliation: dfs::DfsStableWriteOperationReconciliation,
) -> CloudFilesDfsMutationReconciliation {
    match reconciliation {
        dfs::DfsStableWriteOperationReconciliation::Applied { receipt } => {
            CloudFilesDfsMutationReconciliation::Applied(CloudFilesRemoteMutationEvidence {
                operation_id: receipt.operation_id,
                object_id: Some(receipt.object_id.to_string()),
                version: Some(receipt.target_version.to_string()),
                content_hash: receipt.content_hash,
                content_size_bytes: receipt.content_size_bytes,
                kind: Some("file".to_string()),
                detail: format!(
                    "stable DFS write receipt {} is committed",
                    receipt.intent_id
                ),
            })
        }
        dfs::DfsStableWriteOperationReconciliation::NotApplied => {
            CloudFilesDfsMutationReconciliation::NotApplied
        }
        dfs::DfsStableWriteOperationReconciliation::AmbiguousOrExpired { reason } => {
            CloudFilesDfsMutationReconciliation::Ambiguous { detail: reason }
        }
        dfs::DfsStableWriteOperationReconciliation::Conflict { reason } => {
            CloudFilesDfsMutationReconciliation::Conflict(conflict_evidence(
                "stable-write-operation-conflict",
                format!("{}: {reason}", command.operation_id),
            ))
        }
    }
}

impl<B> CloudFilesConcreteDfsAdapter<B> {
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    pub fn backend(&self) -> &B {
        &self.backend
    }

    pub fn backend_mut(&mut self) -> &mut B {
        &mut self.backend
    }
}

impl<B: CloudFilesStableMutationBackend> CloudFilesDfsMutationAdapter
    for CloudFilesConcreteDfsAdapter<B>
{
    fn apply(
        &mut self,
        command: &CloudFilesDfsMutationCommand,
    ) -> Result<CloudFilesDfsMutationOutcome, String> {
        validate_mutation_command(command)?;
        self.backend.operation_started(command);
        match command.kind {
            CloudFilesLocalOperationKind::CreateFile => self.backend.create_file(command),
            CloudFilesLocalOperationKind::CreateDirectory => self.backend.create_directory(command),
            CloudFilesLocalOperationKind::ReplaceFile => self.backend.replace_file(command),
            CloudFilesLocalOperationKind::DeleteFile => self.backend.unlink(command),
            CloudFilesLocalOperationKind::DeleteDirectory => self.backend.rmdir(command),
            CloudFilesLocalOperationKind::Rename => self.backend.rename(command),
        }
    }

    fn reconcile(
        &mut self,
        command: &CloudFilesDfsMutationCommand,
    ) -> Result<CloudFilesDfsMutationReconciliation, String> {
        validate_mutation_command(command)?;
        self.backend.reconcile(command)
    }
}

impl<B: CloudFilesStableMutationBackend> CloudFilesWritableMutationAdapter
    for CloudFilesConcreteDfsAdapter<B>
{
    fn ensure_contracts_ready(&self) -> Result<(), String> {
        self.backend.ensure_contracts_ready()
    }
}

fn validate_mutation_command(command: &CloudFilesDfsMutationCommand) -> Result<(), String> {
    let has_source = command.preconditions.source.is_some();
    let destination = &command.preconditions.destination;
    let has_content = command.staged_content.is_some();
    let has_destination_path = command.destination_path.is_some();
    let valid = match command.kind {
        CloudFilesLocalOperationKind::CreateFile => {
            !has_source
                && has_content
                && !has_destination_path
                && matches!(destination, CloudFilesDestinationPrecondition::Absent)
        }
        CloudFilesLocalOperationKind::CreateDirectory => {
            !has_source
                && !has_content
                && !has_destination_path
                && matches!(destination, CloudFilesDestinationPrecondition::Absent)
        }
        CloudFilesLocalOperationKind::ReplaceFile => {
            has_source
                && has_content
                && !has_destination_path
                && matches!(destination, CloudFilesDestinationPrecondition::Unspecified)
        }
        CloudFilesLocalOperationKind::DeleteFile
        | CloudFilesLocalOperationKind::DeleteDirectory => {
            has_source
                && !has_content
                && !has_destination_path
                && matches!(destination, CloudFilesDestinationPrecondition::Unspecified)
        }
        CloudFilesLocalOperationKind::Rename => {
            has_source
                && !has_content
                && has_destination_path
                && !matches!(destination, CloudFilesDestinationPrecondition::Unspecified)
        }
    };
    if valid {
        Ok(())
    } else {
        Err(format!(
            "queued {:?} operation `{}` does not carry its required immutable content and CAS shape",
            command.kind, command.operation_id
        ))
    }
}

/// Durable queue and reconciliation state opened only after the Windows layer
/// has persisted StateOwned for this exact sync root.
pub struct CloudFilesWritableSyncState {
    mount: String,
    root: String,
    queue: CloudFilesLocalChangeQueue,
    reconciler: CloudFilesNamespaceReconciler,
    materialization_audit_intents: Arc<Mutex<CloudFilesMaterializationAuditIntentStore>>,
    authorized_cursor_recovery: bool,
    cursor_recovery_plan:
        Option<crate::namespace_reconciler::CloudFilesCursorRecoveryReconciliationPlan>,
    cursor_recovery_terminal_proof:
        Option<crate::namespace_reconciler::CloudFilesCursorRecoveryTerminalProof>,
    cursor_recovery_module_namespace_finalized: bool,
    cursor_recovery_namespace_rewrite_absent: bool,
    #[cfg(test)]
    cursor_recovery_test_bypass: bool,
    ready: bool,
    last_error: Option<CloudFilesModuleError>,
}

/// Provider-specific state projected into File Explorer. Hydration and the
/// final in-sync state remain owned by CFAPI; these values only explain local
/// writeback work which the standard availability badge cannot distinguish.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum CloudFilesExplorerItemStatus {
    WaitingToUpload,
    Uploading,
    Retrying,
    Conflict,
    Failed,
}

fn insert_explorer_item_status(
    statuses: &mut BTreeMap<String, CloudFilesExplorerItemStatus>,
    path: String,
    status: CloudFilesExplorerItemStatus,
) {
    if path.is_empty() {
        return;
    }
    statuses
        .entry(path)
        .and_modify(|current| *current = (*current).max(status))
        .or_insert(status);
}

fn authorized_cursor_recovery_suppressions(
    queue: &CloudFilesLocalChangeQueue,
) -> Result<BTreeMap<String, CloudFilesNamespaceSuppression>, CloudFilesWindowsSyncError> {
    let mut suppressions = BTreeMap::new();
    for operation in queue.operations().filter(|operation| {
        matches!(
            operation.status,
            CloudFilesLocalOperationStatus::Completed | CloudFilesLocalOperationStatus::Superseded
        ) && operation.conflict.is_none()
            && operation.last_error.is_none()
    }) {
        let Some(evidence) = operation.remote_evidence.as_ref() else {
            continue;
        };
        let command = CloudFilesDfsMutationCommand {
            operation_id: operation.request.operation_id.clone(),
            request_digest: operation.request_digest.clone(),
            kind: operation.request.kind.clone(),
            source_path: operation.request.source_path.clone(),
            destination_path: operation.request.destination_path.clone(),
            preconditions: operation.request.preconditions.clone(),
            // Completed and superseded records may have released their blobs.
            // The no-echo proof depends only on immutable request metadata and
            // authoritative remote evidence.
            staged_content: None,
        };
        let suppression = suppression_for_completed_mutation(&command, evidence)?;
        if suppressions
            .insert(operation.request.operation_id.clone(), suppression)
            .is_some()
        {
            return Err(sync_error(
                "cursor-recovery-suppression-duplicate",
                "two durable queue records name the same suppression operation ID",
            ));
        }
    }
    Ok(suppressions)
}

impl CloudFilesWritableSyncState {
    pub fn open(
        data_dir: &SinomoDataDir,
        mount: impl Into<String>,
        root: impl Into<String>,
    ) -> Result<Self, CloudFilesWindowsSyncError> {
        let mount = mount.into();
        let root = root.into();
        let queue = CloudFilesLocalChangeQueue::open(data_dir, mount.clone(), root.clone())
            .map_err(queue_error)?;
        let reconciler = CloudFilesNamespaceReconciler::open(data_dir, mount.clone(), root.clone())
            .map_err(reconcile_error)?;
        let materialization_audit_intents = Arc::new(Mutex::new(
            CloudFilesMaterializationAuditIntentStore::open(data_dir, mount.clone(), root.clone())
                .map_err(reconcile_error)?,
        ));
        Ok(Self {
            mount,
            root,
            queue,
            reconciler,
            materialization_audit_intents,
            authorized_cursor_recovery: false,
            cursor_recovery_plan: None,
            cursor_recovery_terminal_proof: None,
            cursor_recovery_module_namespace_finalized: false,
            cursor_recovery_namespace_rewrite_absent: false,
            #[cfg(test)]
            cursor_recovery_test_bypass: false,
            ready: false,
            last_error: None,
        })
    }

    pub(crate) fn open_authorized_cursor_recovery(
        data_dir: &SinomoDataDir,
        mount: impl Into<String>,
        root: impl Into<String>,
        plan: &crate::namespace_reconciler::CloudFilesCursorRecoveryReconciliationPlan,
    ) -> Result<Self, CloudFilesWindowsSyncError> {
        let mount = mount.into();
        let root = root.into();
        let queue = CloudFilesLocalChangeQueue::open_authorized_cursor_recovery(
            data_dir,
            mount.clone(),
            root.clone(),
        )
        .map_err(queue_error)?;
        let allowed_suppressions = authorized_cursor_recovery_suppressions(&queue)?;
        let reconciler =
            CloudFilesNamespaceReconciler::open_authorized_cursor_recovery_with_suppressions(
                data_dir,
                mount.clone(),
                root.clone(),
                plan,
                &allowed_suppressions,
            )
            .map_err(reconcile_error)?;
        let materialization_audit_intents = Arc::new(Mutex::new(
            CloudFilesMaterializationAuditIntentStore::open(data_dir, mount.clone(), root.clone())
                .map_err(reconcile_error)?,
        ));
        Ok(Self {
            mount,
            root,
            queue,
            reconciler,
            materialization_audit_intents,
            authorized_cursor_recovery: true,
            cursor_recovery_plan: Some(plan.clone()),
            cursor_recovery_terminal_proof: None,
            cursor_recovery_module_namespace_finalized: false,
            cursor_recovery_namespace_rewrite_absent: false,
            #[cfg(test)]
            cursor_recovery_test_bypass: false,
            ready: false,
            last_error: None,
        })
    }

    pub(crate) fn open_authorized_cursor_recovery_namespace_finalized(
        data_dir: &SinomoDataDir,
        mount: impl Into<String>,
        root: impl Into<String>,
        plan: &crate::namespace_reconciler::CloudFilesCursorRecoveryReconciliationPlan,
    ) -> Result<Self, CloudFilesWindowsSyncError> {
        let mount = mount.into();
        let root = root.into();
        let queue = CloudFilesLocalChangeQueue::open_authorized_cursor_recovery(
            data_dir,
            mount.clone(),
            root.clone(),
        )
        .map_err(queue_error)?;
        let (reconciler, namespace_rewrite_absent) =
            crate::namespace_reconciler::open_authorized_cursor_recovery_namespace_finalized(
                data_dir,
                mount.clone(),
                root.clone(),
                plan,
            )
            .map_err(reconcile_error)?;
        let materialization_audit_intents = Arc::new(Mutex::new(
            CloudFilesMaterializationAuditIntentStore::open(data_dir, mount.clone(), root.clone())
                .map_err(reconcile_error)?,
        ));
        Ok(Self {
            mount,
            root,
            queue,
            reconciler,
            materialization_audit_intents,
            authorized_cursor_recovery: true,
            cursor_recovery_plan: Some(plan.clone()),
            cursor_recovery_terminal_proof: None,
            cursor_recovery_module_namespace_finalized: true,
            cursor_recovery_namespace_rewrite_absent: namespace_rewrite_absent,
            #[cfg(test)]
            cursor_recovery_test_bypass: false,
            ready: false,
            last_error: None,
        })
    }

    /// Reopens a cycle whose authoritative scan and Terminal rewrite are
    /// already durable, but whose namespace-finalization handshake did not
    /// complete. This path requires the rewrite and terminal state and never
    /// polls the namespace again.
    pub(crate) fn open_authorized_cursor_recovery_terminal_pending_finalize(
        data_dir: &SinomoDataDir,
        mount: impl Into<String>,
        root: impl Into<String>,
        plan: &crate::namespace_reconciler::CloudFilesCursorRecoveryReconciliationPlan,
    ) -> Result<Self, CloudFilesWindowsSyncError> {
        let mount = mount.into();
        let root = root.into();
        let queue = CloudFilesLocalChangeQueue::open_authorized_cursor_recovery(
            data_dir,
            mount.clone(),
            root.clone(),
        )
        .map_err(queue_error)?;
        let reconciler =
            crate::namespace_reconciler::open_authorized_cursor_recovery_terminal_pending_finalize(
                data_dir,
                mount.clone(),
                root.clone(),
                plan,
            )
            .map_err(reconcile_error)?;
        let materialization_audit_intents = Arc::new(Mutex::new(
            CloudFilesMaterializationAuditIntentStore::open(data_dir, mount.clone(), root.clone())
                .map_err(reconcile_error)?,
        ));
        Ok(Self {
            mount,
            root,
            queue,
            reconciler,
            materialization_audit_intents,
            authorized_cursor_recovery: true,
            cursor_recovery_plan: Some(plan.clone()),
            cursor_recovery_terminal_proof: None,
            // The worker uses this bit to re-establish only the observer/root
            // proof and reconstruct the Terminal capability without rescanning.
            cursor_recovery_module_namespace_finalized: true,
            cursor_recovery_namespace_rewrite_absent: false,
            #[cfg(test)]
            cursor_recovery_test_bypass: false,
            ready: false,
            last_error: None,
        })
    }

    pub fn queue(&self) -> &CloudFilesLocalChangeQueue {
        &self.queue
    }

    pub fn reconciler(&self) -> &CloudFilesNamespaceReconciler {
        &self.reconciler
    }

    fn mark_cursor_recovery_terminal(&mut self) -> Result<(), CloudFilesWindowsSyncError> {
        #[cfg(test)]
        if self.cursor_recovery_test_bypass {
            return Ok(());
        }
        let plan = self.cursor_recovery_plan.as_ref().ok_or_else(|| {
            sync_error(
                "cursor-recovery-plan-missing",
                "authorized worker lost its exact reconciliation plan",
            )
        })?;
        self.cursor_recovery_terminal_proof =
            Some(if self.cursor_recovery_namespace_rewrite_absent {
                self.reconciler
                    .resume_authorized_cursor_recovery_namespace_finalized_proof(plan)
                    .map_err(reconcile_error)?
            } else if self.cursor_recovery_module_namespace_finalized {
                self.reconciler
                    .resume_authorized_cursor_recovery_terminal_proof(plan)
                    .map_err(reconcile_error)?
            } else {
                self.reconciler
                    .mark_authorized_cursor_recovery_terminal(plan)
                    .map_err(reconcile_error)?
            });
        Ok(())
    }

    fn finalize_cursor_recovery_after_host_ready(
        &mut self,
    ) -> Result<bool, CloudFilesWindowsSyncError> {
        #[cfg(test)]
        if self.cursor_recovery_test_bypass {
            return Ok(true);
        }
        let plan = self.cursor_recovery_plan.as_ref().ok_or_else(|| {
            sync_error(
                "cursor-recovery-plan-missing",
                "authorized worker lost its exact reconciliation plan",
            )
        })?;
        let terminal = match self.cursor_recovery_terminal_proof.take() {
            Some(terminal) => terminal,
            None if self.cursor_recovery_namespace_rewrite_absent => self
                .reconciler
                .resume_authorized_cursor_recovery_namespace_finalized_proof(plan)
                .map_err(reconcile_error)?,
            None => self
                .reconciler
                .resume_authorized_cursor_recovery_terminal_proof(plan)
                .map_err(reconcile_error)?,
        };
        self.reconciler
            .finalize_authorized_cursor_recovery_after_ready(
                plan,
                terminal.authorize_after_host_ready_observed(),
            )
            .map_err(reconcile_error)
    }

    #[cfg(test)]
    fn authorize_cursor_recovery_for_test(&mut self) {
        let recovery_id = "cursor-recovery-worker-test";
        let recovery_token = format!("v1:blake3:{}", "0".repeat(64));
        self.cursor_recovery_plan =
            Some(crate::namespace_reconciler::cursor_recovery_plan_fixture(
                &self.mount,
                &self.root,
                recovery_id,
                &recovery_token,
                "0123456789abcdef0123456789abcdef",
            ));
        self.authorized_cursor_recovery = true;
        self.cursor_recovery_test_bypass = true;
    }

    pub fn materialization_audit_intents(
        &self,
    ) -> Arc<Mutex<CloudFilesMaterializationAuditIntentStore>> {
        Arc::clone(&self.materialization_audit_intents)
    }

    pub fn is_ready(&self) -> bool {
        self.ready
    }

    pub fn status(&self) -> CloudFilesLocalChangeSyncStatus {
        let summary = self.queue.summary();
        let conflict_count = summary
            .conflict_count
            .saturating_add(self.reconciler.conflict_count())
            .saturating_add(self.reconciler.unsupported_projection_count());
        let (attention, total_attention) = self.attention_summaries();
        let status = if let Some(error) = self.last_error.clone() {
            CloudFilesLocalChangeSyncStatus::writable_blocked(
                summary.pending_operations,
                summary.retrying_operations,
                conflict_count,
                summary.failed_operations,
                error,
            )
        } else if self.ready {
            CloudFilesLocalChangeSyncStatus::writable_active(
                summary.pending_operations,
                summary.retrying_operations,
                conflict_count,
                summary.failed_operations,
            )
        } else {
            CloudFilesLocalChangeSyncStatus::writable_starting(
                summary.pending_operations,
                summary.retrying_operations,
                conflict_count,
                summary.failed_operations,
            )
        };
        status.with_attention(attention, total_attention)
    }

    /// Return one bounded, current status per affected path. This is a pure
    /// projection over already-open durable state: callers must not poll it or
    /// perform DFS work from an Explorer query path.
    pub(crate) fn explorer_item_statuses(&self) -> BTreeMap<String, CloudFilesExplorerItemStatus> {
        let mut statuses = BTreeMap::new();
        for operation in self.queue.operations() {
            let status = match operation.status {
                CloudFilesLocalOperationStatus::Queued => {
                    CloudFilesExplorerItemStatus::WaitingToUpload
                }
                CloudFilesLocalOperationStatus::InFlight
                | CloudFilesLocalOperationStatus::RemoteApplied => {
                    CloudFilesExplorerItemStatus::Uploading
                }
                CloudFilesLocalOperationStatus::Retrying => CloudFilesExplorerItemStatus::Retrying,
                CloudFilesLocalOperationStatus::Conflict => CloudFilesExplorerItemStatus::Conflict,
                CloudFilesLocalOperationStatus::Failed => CloudFilesExplorerItemStatus::Failed,
                CloudFilesLocalOperationStatus::Completed
                | CloudFilesLocalOperationStatus::Superseded => continue,
            };
            let path = match operation.request.kind {
                CloudFilesLocalOperationKind::DeleteFile
                | CloudFilesLocalOperationKind::DeleteDirectory => {
                    sifs_parent_path(&operation.request.source_path)
                }
                CloudFilesLocalOperationKind::Rename => Some(
                    operation
                        .request
                        .destination_path
                        .clone()
                        .unwrap_or_else(|| operation.request.source_path.clone()),
                ),
                _ => Some(operation.request.source_path.clone()),
            };
            if let Some(path) = path {
                insert_explorer_item_status(&mut statuses, path, status);
            }
        }
        for conflict in self.reconciler.conflicts().values() {
            let (path, _) = namespace_conflict_summary(conflict);
            insert_explorer_item_status(
                &mut statuses,
                path,
                CloudFilesExplorerItemStatus::Conflict,
            );
        }
        for conflict in self.reconciler.startup_conflicts().values() {
            insert_explorer_item_status(
                &mut statuses,
                conflict.path.clone(),
                CloudFilesExplorerItemStatus::Conflict,
            );
        }
        if self.last_error.is_some() {
            insert_explorer_item_status(
                &mut statuses,
                self.root.clone(),
                CloudFilesExplorerItemStatus::Failed,
            );
        }
        statuses
    }

    fn attention_summaries(&self) -> (Vec<CloudFilesSyncAttentionSummary>, u64) {
        let mut attention = Vec::new();
        let mut total = 0u64;
        let mut add = |summary: CloudFilesSyncAttentionSummary| {
            total = total.saturating_add(1);
            if attention.len() < CLOUD_FILES_SYNC_ATTENTION_SUMMARY_LIMIT {
                attention.push(summary);
            }
        };

        for operation in self.queue.operations() {
            let (kind, reason_code, reason, safe_next_action) = match operation.status {
                CloudFilesLocalOperationStatus::Conflict => {
                    let conflict = operation.conflict.as_ref();
                    (
                        CloudFilesSyncAttentionKind::LocalOperationConflict,
                        conflict.map_or("local-operation-conflict", |value| value.code.as_str()),
                        conflict.map_or_else(
                            || {
                                "local operation conflicts with authoritative SIFS state"
                                    .to_string()
                            },
                            |value| value.detail.clone(),
                        ),
                        CloudFilesSyncAttentionNextAction::ReviewAndResolve,
                    )
                }
                CloudFilesLocalOperationStatus::Failed => (
                    CloudFilesSyncAttentionKind::FailedLocalOperation,
                    "local-operation-failed",
                    operation
                        .last_error
                        .clone()
                        .unwrap_or_else(|| "local operation failed without detail".to_string()),
                    CloudFilesSyncAttentionNextAction::RepairCauseAndRetry,
                ),
                CloudFilesLocalOperationStatus::Retrying
                    if operation.last_error.as_ref().is_some_and(|detail| {
                        let detail = detail.to_ascii_lowercase();
                        detail.contains("ambiguous")
                            || detail.contains("expired")
                            || detail.contains("stale")
                    }) =>
                {
                    (
                        CloudFilesSyncAttentionKind::StaleOrAmbiguousReconciliation,
                        "stable-operation-reconciliation",
                        operation.last_error.clone().expect("matched error"),
                        CloudFilesSyncAttentionNextAction::WaitForAuthoritativeReconciliation,
                    )
                }
                _ => continue,
            };
            let staged = operation.request.staged_content.as_ref();
            add(CloudFilesSyncAttentionSummary {
                kind,
                path: operation
                    .request
                    .destination_path
                    .clone()
                    .unwrap_or_else(|| operation.request.source_path.clone()),
                reason_code: reason_code.to_string(),
                reason,
                retained_local_evidence: Some(CloudFilesRetainedLocalEvidenceSummary {
                    operation_id: Some(operation.request.operation_id.clone()),
                    evidence_id: operation
                        .remote_evidence
                        .as_ref()
                        .map(|evidence| evidence.operation_id.clone()),
                    staged_content_hash: staged.map(|content| content.content_hash.clone()),
                    staged_content_size_bytes: staged.map(|content| content.length),
                    local_state: Some(format!(
                        "status={:?}; attempts={}",
                        operation.status, operation.attempts
                    )),
                }),
                safe_next_action,
            });
        }

        for conflict in self.reconciler.conflicts().values() {
            let (path, local_state) = namespace_conflict_summary(conflict);
            add(CloudFilesSyncAttentionSummary {
                kind: CloudFilesSyncAttentionKind::RemoteNamespaceConflict,
                path,
                reason_code: "remote-namespace-conflict".to_string(),
                reason: conflict.detail.clone(),
                retained_local_evidence: Some(CloudFilesRetainedLocalEvidenceSummary {
                    evidence_id: Some(conflict.event_id.clone()),
                    local_state: Some(local_state),
                    ..CloudFilesRetainedLocalEvidenceSummary::default()
                }),
                safe_next_action: CloudFilesSyncAttentionNextAction::ReviewAndResolve,
            });
        }
        for conflict in self.reconciler.startup_conflicts().values() {
            let unsupported = is_unsupported_projection_detail(&conflict.detail);
            add(CloudFilesSyncAttentionSummary {
                kind: if unsupported {
                    CloudFilesSyncAttentionKind::UnsupportedObjectKind
                } else {
                    CloudFilesSyncAttentionKind::StartupConflict
                },
                path: conflict.path.clone(),
                reason_code: if unsupported {
                    "unsupported-object-kind"
                } else {
                    "startup-conflict"
                }
                .to_string(),
                reason: conflict.detail.clone(),
                retained_local_evidence: Some(CloudFilesRetainedLocalEvidenceSummary {
                    local_state: Some(
                        "durable startup conflict; local path is preserved".to_string(),
                    ),
                    ..CloudFilesRetainedLocalEvidenceSummary::default()
                }),
                safe_next_action: if unsupported {
                    CloudFilesSyncAttentionNextAction::ReviewUnsupportedEntry
                } else {
                    CloudFilesSyncAttentionNextAction::PreserveLocalCopyAndRescan
                },
            });
        }
        let unsupported_examples = self.reconciler.unsupported_projection_examples();
        for (path, kind) in unsupported_examples {
            add(CloudFilesSyncAttentionSummary {
                kind: CloudFilesSyncAttentionKind::UnsupportedObjectKind,
                path: path.clone(),
                reason_code: "unsupported-object-kind".to_string(),
                reason: format!(
                    "SIFS {kind} has no lossless Windows Cloud Files projection; no misleading file placeholder was created"
                ),
                retained_local_evidence: Some(CloudFilesRetainedLocalEvidenceSummary {
                    local_state: Some(format!(
                        "authoritative SIFS kind={kind}; Windows projection intentionally absent"
                    )),
                    ..CloudFilesRetainedLocalEvidenceSummary::default()
                }),
                safe_next_action: CloudFilesSyncAttentionNextAction::ReviewUnsupportedEntry,
            });
        }
        total = total.saturating_add(
            self.reconciler
                .unsupported_projection_count()
                .saturating_sub(unsupported_examples.len() as u64),
        );
        (attention, total)
    }

    pub fn enqueue_capture(
        &mut self,
        capture: CloudFilesCapturedLocalChange,
    ) -> Result<CloudFilesLocalOperation, CloudFilesWindowsSyncError> {
        if capture.capture_id.trim().is_empty() {
            return Err(sync_error(
                "invalid-capture-id",
                "Windows local change lacks a stable file/USN capture identity",
            ));
        }
        let operation_id = self
            .queue
            .operation_id_for_capture(&capture.capture_id)
            .map(ToOwned::to_owned)
            .unwrap_or_else(new_sortable_operation_id);
        let staged_content = match capture.captured_content {
            Some(CloudFilesCapturedContent::Inline(bytes)) => {
                Some(self.queue.stage_content(&bytes).map_err(queue_error)?)
            }
            Some(CloudFilesCapturedContent::Staged(content)) => {
                self.queue
                    .verify_staged_content(&content)
                    .map_err(queue_error)?;
                Some(content)
            }
            None => None,
        };
        let request = CloudFilesLocalOperationRequest {
            operation_id,
            capture_id: Some(capture.capture_id),
            mount: self.mount.clone(),
            root: self.root.clone(),
            kind: capture.kind,
            source_path: capture.source_path,
            destination_path: capture.destination_path,
            preconditions: capture.preconditions,
            staged_content,
        };
        self.queue.enqueue(request).map_err(queue_error)
    }

    fn mark_ready(&mut self) {
        self.ready = true;
        self.last_error = None;
    }

    fn mark_failed(&mut self, error: &CloudFilesWindowsSyncError) {
        self.ready = false;
        self.last_error = Some(CloudFilesModuleError {
            code: error.code.to_string(),
            detail: error.detail.clone(),
        });
    }
}

fn namespace_conflict_summary(conflict: &CloudFilesNamespaceConflict) -> (String, String) {
    let path = match &conflict.action {
        CloudFilesNamespaceAction::Upsert { path, .. }
        | CloudFilesNamespaceAction::Remove { path, .. } => path.clone(),
        CloudFilesNamespaceAction::Rename { to_path, .. } => to_path.clone(),
    };
    let local_state = conflict
        .local_states
        .iter()
        .map(|(path, state)| match state {
            CloudFilesLocalPathState::Clean => format!("{path}=clean"),
            CloudFilesLocalPathState::Dirty { detail } => {
                format!("{path}=dirty({detail})")
            }
            CloudFilesLocalPathState::Pending { operation_id } => {
                format!("{path}=pending({operation_id})")
            }
        })
        .collect::<Vec<_>>()
        .join("; ");
    (path, local_state)
}

fn is_unsupported_projection_detail(detail: &str) -> bool {
    let detail = detail.to_ascii_lowercase();
    detail.contains("unsupported")
        || detail.contains("no safe sifs projection")
        || detail.contains("reparse point")
}

pub(crate) fn new_sortable_operation_id() -> String {
    format!(
        "cf_{}",
        ulid::Ulid::generate().to_string().to_ascii_lowercase()
    )
}

pub(crate) fn completion_follow_up_request(
    mount: &str,
    root: &str,
    parent_operation_id: &str,
    entry: &CloudFilesReconciledManifestEntry,
    staged_content: CloudFilesStagedContent,
    local_signature: &str,
) -> CloudFilesLocalOperationRequest {
    let mut hasher = blake3::Hasher::new();
    for part in [
        "completion-follow-up",
        parent_operation_id,
        entry.path.as_str(),
        entry.object_id.as_str(),
        entry.version.as_str(),
        local_signature,
    ] {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    CloudFilesLocalOperationRequest {
        operation_id: new_sortable_operation_id(),
        capture_id: Some(format!("windows:{}", hasher.finalize().to_hex())),
        mount: mount.to_string(),
        root: root.to_string(),
        kind: CloudFilesLocalOperationKind::ReplaceFile,
        source_path: entry.path.clone(),
        destination_path: None,
        preconditions: CloudFilesLocalOperationPreconditions {
            source: Some(CloudFilesExpectedObjectState {
                object_id: entry.object_id.clone(),
                version: entry.version.clone(),
                content_hash: entry.content_hash.clone(),
            }),
            destination: CloudFilesDestinationPrecondition::Unspecified,
        },
        staged_content: Some(staged_content),
    }
}

const CURSOR_RECOVERY_SCANNING: u8 = 0;
const CURSOR_RECOVERY_TERMINAL: u8 = 1;
const CURSOR_RECOVERY_RELEASED: u8 = 2;
const CURSOR_RECOVERY_ACTIVE_PARKED: u8 = 3;
const CURSOR_RECOVERY_COMPLETE: u8 = 4;

/// Shared one-way gate between the recovery worker and its Windows owner.
/// Only the worker may publish Terminal, and the owner may release it only
/// from that exact phase after the registration rewrite is durable.
#[derive(Clone)]
pub(crate) struct CloudFilesCursorRecoveryControl {
    phase: Arc<AtomicU8>,
}

impl CloudFilesCursorRecoveryControl {
    fn new() -> Self {
        Self {
            phase: Arc::new(AtomicU8::new(CURSOR_RECOVERY_SCANNING)),
        }
    }

    pub(crate) fn terminal(&self) -> bool {
        matches!(
            self.phase.load(Ordering::SeqCst),
            CURSOR_RECOVERY_TERMINAL | CURSOR_RECOVERY_RELEASED
        )
    }

    pub(crate) fn should_run_immediately(&self) -> bool {
        matches!(
            self.phase.load(Ordering::SeqCst),
            CURSOR_RECOVERY_SCANNING | CURSOR_RECOVERY_RELEASED
        )
    }

    pub(crate) fn release(&self) -> Result<(), &'static str> {
        self.phase
            .compare_exchange(
                CURSOR_RECOVERY_TERMINAL,
                CURSOR_RECOVERY_RELEASED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map(|_| ())
            .map_err(|phase| match phase {
                CURSOR_RECOVERY_SCANNING => "cursor recovery is not terminal",
                CURSOR_RECOVERY_RELEASED
                | CURSOR_RECOVERY_ACTIVE_PARKED
                | CURSOR_RECOVERY_COMPLETE => "cursor recovery was already released",
                _ => "cursor recovery has an invalid phase",
            })
    }

    fn mark_terminal(&self) -> Result<(), CloudFilesWindowsSyncError> {
        self.phase
            .compare_exchange(
                CURSOR_RECOVERY_SCANNING,
                CURSOR_RECOVERY_TERMINAL,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map(|_| ())
            .map_err(|phase| {
                sync_error(
                    "cursor-recovery-phase-invalid",
                    format!("cannot publish cursor-recovery terminal from phase {phase}"),
                )
            })
    }

    fn released(&self) -> bool {
        self.phase.load(Ordering::SeqCst) == CURSOR_RECOVERY_RELEASED
    }

    fn active_parked(&self) -> bool {
        self.phase.load(Ordering::SeqCst) == CURSOR_RECOVERY_ACTIVE_PARKED
    }

    fn mark_active_parked(&self) -> Result<(), CloudFilesWindowsSyncError> {
        self.phase
            .compare_exchange(
                CURSOR_RECOVERY_RELEASED,
                CURSOR_RECOVERY_ACTIVE_PARKED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map(|_| ())
            .map_err(|phase| {
                sync_error(
                    "cursor-recovery-phase-invalid",
                    format!("cannot park recovered Active worker from phase {phase}"),
                )
            })
    }

    pub(crate) fn complete_after_host_active(&self) -> Result<(), &'static str> {
        self.phase
            .compare_exchange(
                CURSOR_RECOVERY_ACTIVE_PARKED,
                CURSOR_RECOVERY_COMPLETE,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map(|_| ())
            .map_err(|phase| match phase {
                CURSOR_RECOVERY_SCANNING | CURSOR_RECOVERY_TERMINAL => {
                    "cursor recovery has not published parked Active"
                }
                CURSOR_RECOVERY_RELEASED => "cursor recovery release pass has not completed",
                CURSOR_RECOVERY_COMPLETE => "cursor recovery was already completed",
                _ => "cursor recovery has an invalid phase",
            })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CloudFilesWritableSyncProgress {
    pub captures_enqueued: u64,
    pub mutation_progress: Vec<CloudFilesSyncExecutionProgress>,
    pub namespace_progress: Vec<CloudFilesNamespaceReconcileProgress>,
    pub full_scan_applied: bool,
    /// Distinct from ordinary Active progress. The worker remains Starting
    /// and parked until its owner durably completes the recovery rewrite.
    pub cursor_recovery_terminal: bool,
}

/// Cooperative worker shell. The Windows owner may run it on a dedicated
/// thread, but cancellation is checked between every durable or remote phase.
pub struct CloudFilesWritableSyncWorker<C, D, F, E> {
    state: CloudFilesWritableSyncState,
    capture: C,
    mutations: D,
    namespace: F,
    effects: E,
    cursor_recovery: Option<CloudFilesCursorRecoveryControl>,
    full_scan_pending: bool,
    startup_scan_baseline: Option<BTreeMap<String, CloudFilesReconciledManifestEntry>>,
}

impl<C, D, F, E> CloudFilesWritableSyncWorker<C, D, F, E>
where
    C: CloudFilesLocalChangeAccelerator,
    D: CloudFilesWritableMutationAdapter,
    F: CloudFilesNamespaceSource,
    E: CloudFilesWindowsPlaceholderEffect,
{
    pub fn new(
        state: CloudFilesWritableSyncState,
        capture: C,
        mutations: D,
        namespace: F,
        effects: E,
    ) -> Self {
        let cursor_recovery = state
            .authorized_cursor_recovery
            .then(CloudFilesCursorRecoveryControl::new);
        Self {
            state,
            capture,
            mutations,
            namespace,
            effects,
            cursor_recovery,
            full_scan_pending: false,
            startup_scan_baseline: None,
        }
    }

    pub fn state(&self) -> &CloudFilesWritableSyncState {
        &self.state
    }

    pub(crate) fn cursor_recovery_control(&self) -> Option<CloudFilesCursorRecoveryControl> {
        self.cursor_recovery.clone()
    }

    pub(crate) fn ordinary_reconciliation_remains_pending(&self) -> bool {
        self.full_scan_pending || self.startup_scan_baseline.is_some()
    }

    pub(crate) fn request_authoritative_rescan(&mut self) {
        if self.startup_scan_baseline.is_none() {
            self.startup_scan_baseline = Some(self.state.reconciler.manifest().clone());
        }
        self.full_scan_pending = true;
    }

    /// Return only work which already owns a causal reason to run. An idle
    /// mount has no deadline and can remain blocked until a local or remote
    /// notification arrives.
    pub(crate) fn next_scheduled_work_in(&self) -> Option<Duration> {
        if self.ordinary_reconciliation_remains_pending()
            || self.cursor_recovery_is_armed()
                && self
                    .cursor_recovery
                    .as_ref()
                    .is_some_and(CloudFilesCursorRecoveryControl::should_run_immediately)
            || self.state.queue.operations().any(|operation| {
                matches!(
                    operation.status,
                    CloudFilesLocalOperationStatus::Queued
                        | CloudFilesLocalOperationStatus::InFlight
                        | CloudFilesLocalOperationStatus::RemoteApplied
                )
            })
        {
            return Some(Duration::ZERO);
        }
        let durable_retry = self
            .state
            .queue
            .operations()
            .any(|operation| operation.status == CloudFilesLocalOperationStatus::Retrying)
            .then_some(Duration::from_secs(1));
        match (durable_retry, self.capture.next_capture_retry_in()) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(delay), None) | (None, Some(delay)) => Some(delay),
            (None, None) => None,
        }
    }

    pub(crate) fn finalize_cursor_recovery_after_host_ready(
        &mut self,
    ) -> Result<bool, CloudFilesWindowsSyncError> {
        if !self
            .cursor_recovery
            .as_ref()
            .is_some_and(CloudFilesCursorRecoveryControl::terminal)
        {
            return Err(sync_error(
                "cursor-recovery-not-terminal",
                "host Ready cannot finalize a nonterminal recovery worker",
            ));
        }
        self.capture
            .verify_cursor_recovery_root_audit()
            .map_err(capture_sync_error("cursor-recovery-root-audit-final"))?;
        let finalized = self.state.finalize_cursor_recovery_after_host_ready()?;
        self.capture.release_cursor_recovery_root_audit();
        Ok(finalized)
    }

    pub fn startup(
        &mut self,
        cancelled: &AtomicBool,
    ) -> Result<CloudFilesWritableSyncProgress, CloudFilesWindowsSyncError> {
        let result = self.startup_inner(cancelled);
        if let Err(error) = &result {
            self.state.mark_failed(error);
        }
        result
    }

    fn startup_inner(
        &mut self,
        cancelled: &AtomicBool,
    ) -> Result<CloudFilesWritableSyncProgress, CloudFilesWindowsSyncError> {
        if self.cursor_recovery_is_armed() {
            return self.run_authorized_cursor_recovery(cancelled);
        }
        check_cancelled(cancelled)?;
        self.mutations.ensure_contracts_ready().map_err(|detail| {
            sync_error(CLOUD_FILES_WRITABLE_SYNC_CONTRACT_NOT_READY_CODE, detail)
        })?;
        self.capture
            .activate()
            .map_err(|detail| sync_error("read-directory-changes-start", detail))?;
        // A namespace effect intent is durable before its local placeholder
        // side effect. Recover it before any full-scan install, which rejects
        // replacing state while that effect is still in flight.
        self.state
            .reconciler
            .recover_inflight_effect(&mut self.effects)
            .map_err(reconcile_error)?;
        // Recover durable in-flight/remote-applied/completed work before the
        // local scan. Otherwise a crash midway through placeholder completion
        // can be misclassified as a new local mutation or startup conflict.
        let mut progress = CloudFilesWritableSyncProgress {
            mutation_progress: self.execute_mutations()?,
            ..CloudFilesWritableSyncProgress::default()
        };
        let previous_manifest = self.state.reconciler.manifest().clone();
        let snapshot = self.full_scan_snapshot(cancelled)?;
        let scan_complete = self.apply_full_scan_snapshot(cancelled, snapshot)?;
        progress.full_scan_applied = true;
        if scan_complete {
            if self.finish_startup_local_scan(cancelled, &previous_manifest, &mut progress)? {
                self.poll_namespace(cancelled, &mut progress)?;
            } else {
                self.startup_scan_baseline = Some(previous_manifest);
            }
        } else {
            self.full_scan_pending = true;
            self.startup_scan_baseline = Some(previous_manifest);
        }
        // Readiness proves contracts, watcher activation, one bounded
        // authoritative root/scan quantum, and durable recovery. The remaining
        // sparse local/full audit continues cooperatively; requiring the whole
        // local namespace here makes activation O(namespace) and exceeds the
        // registration deadline for large owned roots.
        self.state.mark_ready();
        Ok(progress)
    }

    pub fn run_once(
        &mut self,
        cancelled: &AtomicBool,
    ) -> Result<CloudFilesWritableSyncProgress, CloudFilesWindowsSyncError> {
        self.run_once_with_namespace_poll(cancelled, true)
    }

    pub(crate) fn run_once_for_wake(
        &mut self,
        cancelled: &AtomicBool,
        namespace_poll_requested: bool,
    ) -> Result<CloudFilesWritableSyncProgress, CloudFilesWindowsSyncError> {
        self.run_once_with_namespace_poll(cancelled, namespace_poll_requested)
    }

    fn run_once_with_namespace_poll(
        &mut self,
        cancelled: &AtomicBool,
        namespace_poll_requested: bool,
    ) -> Result<CloudFilesWritableSyncProgress, CloudFilesWindowsSyncError> {
        let result = self.run_once_inner(cancelled, namespace_poll_requested);
        if let Err(error) = &result
            && !retryable_namespace_read_error(error)
        {
            self.state.mark_failed(error);
        }
        result
    }

    fn run_once_inner(
        &mut self,
        cancelled: &AtomicBool,
        namespace_poll_requested: bool,
    ) -> Result<CloudFilesWritableSyncProgress, CloudFilesWindowsSyncError> {
        if self.cursor_recovery_is_armed() {
            return self.run_authorized_cursor_recovery(cancelled);
        }
        if !self.state.ready && !self.full_scan_pending && self.startup_scan_baseline.is_none() {
            return Err(sync_error(
                "worker-not-ready",
                "writable Cloud Files worker has not completed startup reconciliation",
            ));
        }
        check_cancelled(cancelled)?;
        if self.full_scan_pending {
            // A sparse authoritative scan is deliberately cooperative and may
            // take many passes for a large materialized namespace. Keep
            // draining the already-active directory watcher while that scan is
            // in progress. Otherwise the bounded notification channel can sit
            // unread for the entire scan, and an apparently healthy worker can
            // starve local writeback indefinitely. Captured mutations remain
            // safe against the older scan boundary: their exact CAS
            // preconditions either publish or retain a conflict, and the feed
            // is replayed from the scan's earlier boundary afterward.
            let captures = self
                .capture
                .drain_changes(self.state.reconciler.manifest(), cancelled)
                .map_err(capture_sync_error("read-directory-changes"))?;
            let mut progress = CloudFilesWritableSyncProgress::default();
            if !captures.overflowed {
                self.resolve_sparse_placeholders(captures.sparse_placeholders)?;
                for capture in captures.changes {
                    check_cancelled(cancelled)?;
                    self.state.enqueue_capture(capture)?;
                    progress.captures_enqueued += 1;
                }
                check_cancelled(cancelled)?;
                progress.mutation_progress = self.execute_mutations()?;
            }
            let snapshot = self.full_scan_snapshot(cancelled)?;
            let scan_complete = self.apply_full_scan_snapshot(cancelled, snapshot)?;
            progress.full_scan_applied = true;
            if scan_complete {
                self.full_scan_pending = false;
                if let Some(baseline) = self.startup_scan_baseline.take()
                    && !self.finish_startup_local_scan(cancelled, &baseline, &mut progress)?
                {
                    self.startup_scan_baseline = Some(baseline);
                }
                if self.startup_scan_baseline.is_none() {
                    self.state.mark_ready();
                    self.poll_namespace(cancelled, &mut progress)?;
                }
            }
            self.run_maintenance()?;
            return Ok(progress);
        }
        if let Some(baseline) = self.startup_scan_baseline.take() {
            let mut progress = CloudFilesWritableSyncProgress::default();
            if !self.finish_startup_local_scan(cancelled, &baseline, &mut progress)? {
                self.startup_scan_baseline = Some(baseline);
            } else {
                self.state.mark_ready();
                self.poll_namespace(cancelled, &mut progress)?;
            }
            self.run_maintenance()?;
            return Ok(progress);
        }
        let materialization_audit_pending = self
            .namespace
            .has_pending_materialized_directories(self.state.reconciler.materialized_directories())
            .map_err(|detail| sync_error("materialization-audit-intents", detail))?;
        if materialization_audit_pending {
            let mut progress = CloudFilesWritableSyncProgress::default();
            let snapshot = self.full_scan_snapshot(cancelled)?;
            let scan_complete = self.apply_full_scan_snapshot(cancelled, snapshot)?;
            progress.full_scan_applied = true;
            if scan_complete {
                self.poll_namespace(cancelled, &mut progress)?;
            } else {
                self.full_scan_pending = true;
            }
            self.run_maintenance()?;
            return Ok(progress);
        }
        let captures = self
            .capture
            .drain_changes(self.state.reconciler.manifest(), cancelled)
            .map_err(capture_sync_error("read-directory-changes"))?;
        let mut progress = CloudFilesWritableSyncProgress::default();
        let mut maintenance_needed = if captures.overflowed {
            let previous_manifest = self.state.reconciler.manifest().clone();
            let snapshot = self.full_scan_snapshot(cancelled)?;
            let scan_complete = self.apply_full_scan_snapshot(cancelled, snapshot)?;
            progress.full_scan_applied = true;
            if scan_complete {
                if !self.finish_startup_local_scan(cancelled, &previous_manifest, &mut progress)? {
                    self.startup_scan_baseline = Some(previous_manifest);
                }
            } else {
                self.full_scan_pending = true;
                self.startup_scan_baseline = Some(previous_manifest);
            }
            true
        } else {
            let had_sparse_placeholders = !captures.sparse_placeholders.is_empty();
            self.resolve_sparse_placeholders(captures.sparse_placeholders)?;
            for capture in captures.changes {
                check_cancelled(cancelled)?;
                self.state.enqueue_capture(capture)?;
                progress.captures_enqueued += 1;
            }
            check_cancelled(cancelled)?;
            progress.mutation_progress = self.execute_mutations()?;
            had_sparse_placeholders
                || progress.captures_enqueued != 0
                || !progress.mutation_progress.is_empty()
        };
        if namespace_poll_requested
            && !self.full_scan_pending
            && self.startup_scan_baseline.is_none()
        {
            self.poll_namespace(cancelled, &mut progress)?;
            maintenance_needed = true;
        }
        if maintenance_needed {
            self.run_maintenance()?;
        }
        Ok(progress)
    }

    fn cursor_recovery_is_armed(&self) -> bool {
        self.cursor_recovery
            .as_ref()
            .is_some_and(|control| control.phase.load(Ordering::SeqCst) != CURSOR_RECOVERY_COMPLETE)
    }

    fn ensure_cursor_recovery_queue_settled(&self) -> Result<(), CloudFilesWindowsSyncError> {
        if let Some(operation) = self.state.queue.operations().find(|operation| {
            operation.status != CloudFilesLocalOperationStatus::Superseded
                || operation.remote_evidence.is_none()
                || operation.conflict.is_some()
                || operation.last_error.is_some()
                || self
                    .state
                    .reconciler
                    .has_suppression(&operation.request.operation_id)
        }) {
            return Err(sync_error(
                "cursor-recovery-queue-not-empty",
                format!(
                    "authorized cursor recovery cannot finalize while operation `{}` is {:?} or still retains unreconciled evidence",
                    operation.request.operation_id, operation.status
                ),
            ));
        }
        Ok(())
    }

    fn ensure_cursor_recovery_queue_can_scan(&self) -> Result<bool, CloudFilesWindowsSyncError> {
        let mut waiting = false;
        for operation in self.state.queue.operations() {
            match operation.status {
                CloudFilesLocalOperationStatus::Superseded => {}
                CloudFilesLocalOperationStatus::Queued
                | CloudFilesLocalOperationStatus::InFlight
                | CloudFilesLocalOperationStatus::Retrying
                | CloudFilesLocalOperationStatus::RemoteApplied
                | CloudFilesLocalOperationStatus::Completed => waiting = true,
                CloudFilesLocalOperationStatus::Conflict
                | CloudFilesLocalOperationStatus::Failed => {
                    return Err(sync_error(
                        "cursor-recovery-queue-attention-required",
                        format!(
                            "retained operation `{}` is {:?}: {}",
                            operation.request.operation_id,
                            operation.status,
                            operation
                                .last_error
                                .as_deref()
                                .or_else(|| operation
                                    .conflict
                                    .as_ref()
                                    .map(|value| value.detail.as_str()))
                                .unwrap_or(
                                    "terminal local writeback evidence requires operator review"
                                )
                        ),
                    ));
                }
            }
        }
        Ok(!waiting)
    }

    fn run_authorized_cursor_recovery(
        &mut self,
        cancelled: &AtomicBool,
    ) -> Result<CloudFilesWritableSyncProgress, CloudFilesWindowsSyncError> {
        check_cancelled(cancelled)?;
        let control = self.cursor_recovery.clone().ok_or_else(|| {
            sync_error(
                "cursor-recovery-control-missing",
                "authorized cursor recovery lost its worker control",
            )
        })?;

        if control.released() {
            self.ensure_cursor_recovery_queue_settled()?;
            return self.finish_released_cursor_recovery(cancelled, &control);
        }
        if control.active_parked() {
            self.ensure_cursor_recovery_queue_settled()?;
            return Ok(CloudFilesWritableSyncProgress::default());
        }
        if control.terminal() {
            self.ensure_cursor_recovery_queue_settled()?;
            return Ok(CloudFilesWritableSyncProgress {
                cursor_recovery_terminal: true,
                ..CloudFilesWritableSyncProgress::default()
            });
        }

        // NamespaceFinalized is a durable successor to host Ready. Its inner
        // Terminal rewrite may still be present or may already be removed.
        // In either cut, re-establish only the observer/fence/root-audit proof;
        // never rerun or mutate the authoritative scan after the successor is
        // durable.
        if self.state.cursor_recovery_module_namespace_finalized {
            self.ensure_cursor_recovery_queue_settled()?;
            if !self.state.reconciler.cursor_recovery_terminal() {
                return Err(sync_error(
                    "cursor-recovery-namespace-finalized-invalid",
                    "NamespaceFinalized recovery lost its exact terminal sparse state",
                ));
            }
            return self.establish_cursor_recovery_terminal_proof(cancelled, &control);
        }

        // The authoritative terminal state is durable before the observer
        // audit and Terminal rewrite. A crash or a later proof error in that
        // interval leaves ReconcileArmed plus exact terminal state. Re-run
        // only the proof boundary; launching another scan from terminal
        // cursors would reverse the plan's causal state machine.
        if self.state.reconciler.cursor_recovery_terminal() {
            self.ensure_cursor_recovery_queue_settled()?;
            return self.establish_cursor_recovery_terminal_proof(cancelled, &control);
        }

        self.mutations.ensure_contracts_ready().map_err(|detail| {
            sync_error(CLOUD_FILES_WRITABLE_SYNC_CONTRACT_NOT_READY_CODE, detail)
        })?;
        let mut progress = CloudFilesWritableSyncProgress::default();
        if self.state.queue.operations().next().is_some() {
            progress.mutation_progress = self.execute_mutations()?;
            if !self.ensure_cursor_recovery_queue_can_scan()? {
                return Ok(progress);
            }
        }
        let snapshot = self.full_scan_snapshot(cancelled)?;
        if snapshot.scan_complete && snapshot.cursors.is_empty() {
            return Err(sync_error(
                "cursor-recovery-terminal-cursor-missing",
                "authorized cursor recovery reached a terminal scan without a namespace cursor",
            ));
        }
        if !snapshot.scan_complete && !snapshot.cursors.is_empty() {
            return Err(sync_error(
                "cursor-recovery-partial-cursor",
                "authorized cursor recovery received a cursor before its terminal scan delta",
            ));
        }
        let scan_complete = self.apply_full_scan_snapshot(cancelled, snapshot)?;
        progress.full_scan_applied = true;
        if !scan_complete {
            return Ok(progress);
        }
        if !self.state.reconciler.cursor_recovery_terminal() {
            return Err(sync_error(
                "cursor-recovery-terminal-incomplete",
                "terminal cursor recovery did not durably clear the full-scan request, retain its authorized sparse materialization ledger, and leave no inflight namespace effect",
            ));
        }
        self.ensure_cursor_recovery_queue_settled()?;
        self.state
            .queue
            .prune_released_blobs()
            .map_err(queue_error)?;
        let mut terminal = self.establish_cursor_recovery_terminal_proof(cancelled, &control)?;
        terminal.full_scan_applied = progress.full_scan_applied;
        Ok(terminal)
    }

    fn establish_cursor_recovery_terminal_proof(
        &mut self,
        cancelled: &AtomicBool,
        control: &CloudFilesCursorRecoveryControl,
    ) -> Result<CloudFilesWritableSyncProgress, CloudFilesWindowsSyncError> {
        self.ensure_cursor_recovery_queue_settled()?;
        self.capture
            .arm_cursor_recovery_observer()
            .map_err(capture_sync_error("cursor-recovery-observer-start"))?;
        let pre_audit_epoch = self
            .capture
            .cursor_recovery_namespace_fence(
                &self
                    .state
                    .cursor_recovery_plan
                    .as_ref()
                    .ok_or_else(|| {
                        sync_error(
                            "cursor-recovery-plan-missing",
                            "authorized cursor recovery lost its reconciliation plan",
                        )
                    })?
                    .recovery_id,
                &self
                    .state
                    .cursor_recovery_plan
                    .as_ref()
                    .expect("cursor recovery plan checked above")
                    .recovery_token,
                &self
                    .state
                    .cursor_recovery_plan
                    .as_ref()
                    .expect("cursor recovery plan checked above")
                    .nonce,
            )
            .map_err(capture_sync_error("cursor-recovery-namespace-fence"))?;
        self.capture
            .audit_cursor_recovery_root(
                &self.state.mount,
                &self.state.root,
                self.state.reconciler.manifest(),
                cancelled,
            )
            .map_err(capture_sync_error("cursor-recovery-root-audit"))?;
        let post_audit_epoch = self
            .capture
            .cursor_recovery_namespace_fence(
                &self
                    .state
                    .cursor_recovery_plan
                    .as_ref()
                    .expect("cursor recovery plan remains owned")
                    .recovery_id,
                &self
                    .state
                    .cursor_recovery_plan
                    .as_ref()
                    .expect("cursor recovery plan remains owned")
                    .recovery_token,
                &self
                    .state
                    .cursor_recovery_plan
                    .as_ref()
                    .expect("cursor recovery plan remains owned")
                    .nonce,
            )
            .map_err(capture_sync_error("cursor-recovery-namespace-fence"))?;
        if post_audit_epoch != pre_audit_epoch {
            return Err(sync_error(
                "cursor-recovery-local-change-observed",
                "local change raced the terminal recovery-root audit",
            ));
        }
        check_cancelled(cancelled)?;
        self.ensure_cursor_recovery_queue_settled()?;
        self.namespace
            .acknowledge_materialized_directories_after_cursor_recovery_root_audit()
            .map_err(|detail| {
                sync_error("cursor-recovery-materialization-audit-acknowledge", detail)
            })?;
        self.state.mark_cursor_recovery_terminal()?;
        control.mark_terminal()?;
        Ok(CloudFilesWritableSyncProgress {
            cursor_recovery_terminal: true,
            ..CloudFilesWritableSyncProgress::default()
        })
    }

    fn finish_released_cursor_recovery(
        &mut self,
        cancelled: &AtomicBool,
        control: &CloudFilesCursorRecoveryControl,
    ) -> Result<CloudFilesWritableSyncProgress, CloudFilesWindowsSyncError> {
        check_cancelled(cancelled)?;
        self.ensure_cursor_recovery_queue_settled()?;
        self.mutations.ensure_contracts_ready().map_err(|detail| {
            sync_error(CLOUD_FILES_WRITABLE_SYNC_CONTRACT_NOT_READY_CODE, detail)
        })?;
        self.capture
            .activate()
            .map_err(|detail| sync_error("read-directory-changes-start", detail))?;
        // Publication of Active is a separate pass boundary from all local
        // classification and writeback. The newly active watcher closes the
        // observation gap; the next ordinary pass performs the startup scan
        // against this exact recovered baseline.
        self.startup_scan_baseline = Some(self.state.reconciler.manifest().clone());
        self.state.authorized_cursor_recovery = false;
        self.state.mark_ready();
        control.mark_active_parked()?;
        Ok(CloudFilesWritableSyncProgress::default())
    }

    fn finish_startup_local_scan(
        &mut self,
        cancelled: &AtomicBool,
        previous_manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
        progress: &mut CloudFilesWritableSyncProgress,
    ) -> Result<bool, CloudFilesWindowsSyncError> {
        let batch = self
            .capture
            .startup_scan(
                previous_manifest,
                self.state.reconciler.manifest(),
                cancelled,
            )
            .map_err(capture_sync_error("startup-local-scan"))?;
        self.resolve_sparse_placeholders(batch.sparse_placeholders)?;
        for capture in batch.changes {
            check_cancelled(cancelled)?;
            self.state.enqueue_capture(capture)?;
            progress.captures_enqueued += 1;
        }
        self.persist_startup_conflicts()?;
        progress.mutation_progress.extend(self.execute_mutations()?);
        Ok(batch.complete)
    }

    fn resolve_sparse_placeholders(
        &mut self,
        observations: Vec<CloudFilesSparsePlaceholderObservation>,
    ) -> Result<(), CloudFilesWindowsSyncError> {
        for observation in observations {
            let identity = &observation.identity;
            let scope_matches =
                identity.mount == self.state.mount && identity.root == self.state.root;
            let resolved = if scope_matches {
                self.namespace.resolve_placeholder_identity(identity)
            } else {
                Ok(None)
            };
            let exact = match resolved {
                Ok(Some(entry))
                    if entry.path == identity.path
                        && entry.object_id == identity.object_id
                        && entry.version == identity.version
                        && entry.content_hash == identity.content_hash =>
                {
                    Some(entry)
                }
                Ok(Some(entry)) => {
                    self.state
                        .reconciler
                        .record_startup_conflict(
                            &observation.local_path,
                            format!(
                                "sparse placeholder identity changed remotely: expected object={} version={} hash={:?}, observed object={} version={} hash={:?}",
                                identity.object_id,
                                identity.version,
                                identity.content_hash,
                                entry.object_id,
                                entry.version,
                                entry.content_hash
                            ),
                        )
                        .map_err(reconcile_error)?;
                    None
                }
                Ok(None) => {
                    self.state
                        .reconciler
                        .record_startup_conflict(
                            &observation.local_path,
                            format!(
                                "sparse placeholder source `{}` is authoritatively absent; local create/replace is blocked",
                                identity.path
                            ),
                        )
                        .map_err(reconcile_error)?;
                    None
                }
                Err(detail) => {
                    self.state
                        .reconciler
                        .record_startup_conflict(
                            &observation.local_path,
                            format!(
                                "cannot authoritatively recover sparse placeholder `{}`: {detail}; local mutation is blocked",
                                identity.path
                            ),
                        )
                        .map_err(reconcile_error)?;
                    None
                }
            };
            if let Some(entry) = exact {
                let directory = sifs_parent_path(&entry.path).ok_or_else(|| {
                    sync_error(
                        "invalid-sparse-placeholder",
                        format!("placeholder `{}` has no owning directory", entry.path),
                    )
                })?;
                self.state
                    .reconciler
                    .record_materialized_placeholder(&directory, entry)
                    .map_err(reconcile_error)?;
                self.state
                    .reconciler
                    .resolve_startup_conflict(&observation.local_path)
                    .map_err(reconcile_error)?;
                self.capture
                    .finish_sparse_placeholder_resolution(&observation.local_path, true);
            } else {
                self.capture
                    .finish_sparse_placeholder_resolution(&observation.local_path, false);
            }
        }
        Ok(())
    }

    fn run_maintenance(&mut self) -> Result<(), CloudFilesWindowsSyncError> {
        self.state.queue.compact_and_prune().map_err(queue_error)?;
        self.state
            .reconciler
            .compact_durable_state()
            .map_err(reconcile_error)?;
        Ok(())
    }

    fn persist_startup_conflicts(&mut self) -> Result<(), CloudFilesWindowsSyncError> {
        for conflict in self.capture.drain_startup_conflicts() {
            self.state
                .reconciler
                .record_startup_conflict(conflict.path, conflict.detail)
                .map_err(reconcile_error)?;
        }
        Ok(())
    }

    fn execute_mutations(
        &mut self,
    ) -> Result<Vec<CloudFilesSyncExecutionProgress>, CloudFilesWindowsSyncError> {
        self.repair_completed_local_applications()?;
        self.retire_feed_reconciled_operations()?;
        let progress = execute_ready_cloud_files_operations_bounded(
            &mut self.state.queue,
            &mut self.mutations,
            &mut self.effects,
            CLOUD_FILES_MUTATIONS_PER_PASS,
        )
        .map_err(executor_error)?;
        // An ambiguous service result or a lost daemon Commit response gates
        // the handoff inside `apply`. The executor durably leaves that request
        // Retrying; stop this worker pass before feed reconciliation or a later
        // receipt read can advance any local state across the ambiguity fence.
        self.mutations.ensure_contracts_ready().map_err(|detail| {
            sync_error(CLOUD_FILES_WRITABLE_SYNC_CONTRACT_NOT_READY_CODE, detail)
        })?;
        for suppression in self.effects.drain_namespace_suppressions() {
            let expected = self.completed_operation_suppression(&suppression.operation_id)?;
            if suppression != expected {
                return Err(sync_error(
                    "completion-suppression-mismatch",
                    format!(
                        "Windows completion returned suppression that does not match durable operation `{}`",
                        suppression.operation_id
                    ),
                ));
            }
            self.state
                .reconciler
                .record_local_application(suppression)
                .map_err(reconcile_error)?;
            self.state
                .queue
                .retire_terminal_operation(&expected.operation_id)
                .map_err(queue_error)?;
        }
        // A completion adapter may crash after its local effect and the queue's
        // Completed commit but before returning its in-memory suppression. The
        // durable request and remote evidence are sufficient to reconstruct the
        // exact manifest/no-echo commit before feed polling.
        self.repair_completed_local_applications()?;
        Ok(progress)
    }

    fn completed_operation_suppression(
        &self,
        operation_id: &str,
    ) -> Result<CloudFilesNamespaceSuppression, CloudFilesWindowsSyncError> {
        let operation = self.state.queue.operation(operation_id).ok_or_else(|| {
            sync_error(
                "completed-operation-missing",
                format!("completed operation `{operation_id}` is absent from the durable queue"),
            )
        })?;
        if operation.status != CloudFilesLocalOperationStatus::Completed {
            return Err(sync_error(
                "completion-status-mismatch",
                format!(
                    "operation `{operation_id}` is {:?}, not durably Completed",
                    operation.status
                ),
            ));
        }
        let evidence = operation.remote_evidence.as_ref().ok_or_else(|| {
            sync_error(
                "completion-evidence-missing",
                format!("completed operation `{operation_id}` has no durable remote evidence"),
            )
        })?;
        let command = CloudFilesDfsMutationCommand {
            operation_id: operation.request.operation_id.clone(),
            request_digest: operation.request_digest.clone(),
            kind: operation.request.kind.clone(),
            source_path: operation.request.source_path.clone(),
            destination_path: operation.request.destination_path.clone(),
            preconditions: operation.request.preconditions.clone(),
            // Suppression reconstruction consumes only immutable request
            // metadata and authoritative evidence; completed blobs may already
            // have been safely pruned.
            staged_content: None,
        };
        suppression_for_completed_mutation(&command, evidence)
    }

    fn repair_completed_local_applications(&mut self) -> Result<(), CloudFilesWindowsSyncError> {
        let operation_ids = self
            .state
            .queue
            .operations()
            .filter(|operation| operation.status == CloudFilesLocalOperationStatus::Completed)
            .map(|operation| operation.request.operation_id.clone())
            .take(CLOUD_FILES_MUTATIONS_PER_PASS)
            .collect::<Vec<_>>();
        for operation_id in operation_ids {
            let suppression = self.completed_operation_suppression(&operation_id)?;
            self.state
                .reconciler
                .record_local_application(suppression)
                .map_err(reconcile_error)?;
            self.state
                .queue
                .retire_terminal_operation(&operation_id)
                .map_err(queue_error)?;
        }
        Ok(())
    }

    fn retire_feed_reconciled_operations(&mut self) -> Result<(), CloudFilesWindowsSyncError> {
        let operation_ids = self
            .state
            .queue
            .operations()
            .filter(|operation| {
                operation.status == CloudFilesLocalOperationStatus::Superseded
                    && operation.remote_evidence.is_some()
                    && operation.conflict.is_none()
                    && operation.last_error.is_none()
                    && !self
                        .state
                        .reconciler
                        .has_suppression(&operation.request.operation_id)
            })
            .map(|operation| operation.request.operation_id.clone())
            .take(CLOUD_FILES_MUTATIONS_PER_PASS)
            .collect::<Vec<_>>();
        self.state
            .queue
            .acknowledge_reconciled_retirements(&operation_ids)
            .map_err(queue_error)?;
        Ok(())
    }

    fn poll_namespace(
        &mut self,
        cancelled: &AtomicBool,
        progress: &mut CloudFilesWritableSyncProgress,
    ) -> Result<(), CloudFilesWindowsSyncError> {
        check_cancelled(cancelled)?;
        let cursors = self
            .state
            .reconciler
            .cursors()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let batches = self
            .namespace
            .poll_batches(&self.state.root, &cursors)
            .map_err(|detail| sync_error("namespace-feed", detail))?;
        for batch in batches.into_iter().take(CLOUD_FILES_FEED_BATCHES_PER_PASS) {
            check_cancelled(cancelled)?;
            let batch = bound_namespace_feed_batch(batch, CLOUD_FILES_FEED_EVENTS_PER_PASS);
            let batch_progress = self
                .state
                .reconciler
                .reconcile_batch(batch, &mut self.effects)
                .map_err(reconcile_error)?;
            self.state
                .queue
                .acknowledge_reconciled_retirements(&batch_progress.reconciled_local_operation_ids)
                .map_err(queue_error)?;
            progress.namespace_progress.push(batch_progress);
        }
        if self.state.reconciler.full_scan_request().is_some() {
            let baseline = self.state.reconciler.manifest().clone();
            let scan_complete = self.full_reconcile(cancelled)?;
            if !scan_complete {
                self.full_scan_pending = true;
                self.startup_scan_baseline = Some(baseline);
            }
            progress.full_scan_applied = true;
        }
        Ok(())
    }

    fn full_reconcile(
        &mut self,
        cancelled: &AtomicBool,
    ) -> Result<bool, CloudFilesWindowsSyncError> {
        let snapshot = self.full_scan_snapshot(cancelled)?;
        self.apply_full_scan_snapshot(cancelled, snapshot)
    }

    fn full_scan_snapshot(
        &mut self,
        cancelled: &AtomicBool,
    ) -> Result<CloudFilesFullScanSnapshot, CloudFilesWindowsSyncError> {
        check_cancelled(cancelled)?;
        let cursors = self
            .state
            .reconciler
            .cursors()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        self.namespace
            .full_scan(
                &self.state.root,
                &cursors,
                self.state.reconciler.manifest(),
                self.state.reconciler.materialized_directories(),
            )
            .map_err(|detail| sync_error("full-reconciliation-scan", detail))
    }

    fn apply_full_scan_snapshot(
        &mut self,
        cancelled: &AtomicBool,
        snapshot: CloudFilesFullScanSnapshot,
    ) -> Result<bool, CloudFilesWindowsSyncError> {
        check_cancelled(cancelled)?;
        let scan_complete = snapshot.scan_complete;
        let materialized_directories = snapshot.materialized_directories.clone();
        self.effects
            .apply_full_scan(&snapshot)
            .map_err(|detail| sync_error("full-reconciliation-effect", detail))?;
        check_cancelled(cancelled)?;
        let reconciled_local_operation_ids = self
            .state
            .reconciler
            .install_full_scan_delta(
                snapshot.manifest,
                snapshot.removed_paths,
                materialized_directories.clone(),
                snapshot.cursors,
                snapshot.scan_complete,
            )
            .map_err(reconcile_error)?;
        self.namespace
            .acknowledge_materialized_directories(&materialized_directories)
            .map_err(|detail| sync_error("materialization-audit-acknowledge", detail))?;
        self.state
            .queue
            .acknowledge_reconciled_retirements(&reconciled_local_operation_ids)
            .map_err(queue_error)?;
        Ok(scan_complete)
    }
}

fn check_cancelled(cancelled: &AtomicBool) -> Result<(), CloudFilesWindowsSyncError> {
    if cancelled.load(Ordering::SeqCst) {
        Err(sync_error(
            "sync-cancelled",
            "writable Cloud Files worker was cancelled",
        ))
    } else {
        Ok(())
    }
}

fn capture_sync_error(code: &'static str) -> impl FnOnce(String) -> CloudFilesWindowsSyncError {
    move |detail| {
        if detail == "sync-cancelled" {
            sync_error(
                "sync-cancelled",
                "writable Cloud Files worker was cancelled",
            )
        } else {
            sync_error(code, detail)
        }
    }
}

/// Drive a worker loop whose health generation advances only after one whole
/// pass completes successfully. A blocked DFS/capture/effect phase therefore
/// cannot manufacture fresh liveness while it is making no progress.
#[cfg(test)]
pub(crate) fn run_progress_gated_worker_loop<S, E>(
    cancelled: &AtomicBool,
    initial_generation: u64,
    mut run_once: impl FnMut() -> Result<S, E>,
    mut on_progress: impl FnMut(S, u64),
    mut wait_between_passes: impl FnMut(&AtomicBool),
) -> Result<(), E> {
    let mut generation = initial_generation;
    while !cancelled.load(Ordering::SeqCst) {
        let status = run_once()?;
        if cancelled.load(Ordering::SeqCst) {
            break;
        }
        generation = generation.saturating_add(1);
        on_progress(status, generation);
        wait_between_passes(cancelled);
    }
    Ok(())
}

pub(crate) fn retryable_namespace_read_error(error: &CloudFilesWindowsSyncError) -> bool {
    matches!(error.code, "namespace-feed" | "full-reconciliation-scan")
}

/// A namespace feed poll and an authoritative full scan are read-only remote
/// observations. Losing their response is safe to retry in the same connected
/// provider; tearing the worker down instead unregisters the sync root and can
/// strand its still-present placeholders behind a disconnected filter.
pub(crate) fn run_retrying_namespace_read_pass<S>(
    cancelled: &AtomicBool,
    mut run_once: impl FnMut() -> Result<S, CloudFilesWindowsSyncError>,
    mut wait_before_retry: impl FnMut(&AtomicBool),
) -> Result<S, CloudFilesWindowsSyncError> {
    loop {
        match run_once() {
            Ok(status) => return Ok(status),
            Err(error)
                if retryable_namespace_read_error(&error) && !cancelled.load(Ordering::SeqCst) =>
            {
                wait_before_retry(cancelled);
            }
            Err(error) => return Err(error),
        }
    }
}

/// Build the durable no-echo entry before a local completion effect marks its
/// placeholder in sync. Missing authoritative evidence fails closed.
pub fn suppression_for_completed_mutation(
    command: &CloudFilesDfsMutationCommand,
    evidence: &CloudFilesRemoteMutationEvidence,
) -> Result<CloudFilesNamespaceSuppression, CloudFilesWindowsSyncError> {
    if command.operation_id != evidence.operation_id {
        return Err(sync_error(
            "suppression-operation-mismatch",
            "remote evidence belongs to a different stable operation ID",
        ));
    }
    let object_id = evidence.object_id.clone().ok_or_else(|| {
        sync_error(
            "suppression-evidence-missing",
            "remote evidence has no object identity",
        )
    })?;
    let version = evidence.version.clone().ok_or_else(|| {
        sync_error(
            "suppression-evidence-missing",
            "remote evidence has no object version",
        )
    })?;
    let action = match command.kind {
        CloudFilesLocalOperationKind::CreateFile | CloudFilesLocalOperationKind::ReplaceFile => {
            CloudFilesNamespaceAction::Upsert {
                path: command.source_path.clone(),
                object_id,
                version,
                content_hash: evidence.content_hash.clone(),
                content_size_bytes: evidence.content_size_bytes,
                kind: "file".to_string(),
            }
        }
        CloudFilesLocalOperationKind::CreateDirectory => CloudFilesNamespaceAction::Upsert {
            path: command.source_path.clone(),
            object_id,
            version,
            content_hash: evidence.content_hash.clone(),
            content_size_bytes: evidence.content_size_bytes,
            kind: "directory".to_string(),
        },
        CloudFilesLocalOperationKind::DeleteFile
        | CloudFilesLocalOperationKind::DeleteDirectory => CloudFilesNamespaceAction::Remove {
            path: command.source_path.clone(),
            object_id,
            version,
            content_hash: evidence.content_hash.clone(),
        },
        CloudFilesLocalOperationKind::Rename => CloudFilesNamespaceAction::Rename {
            from_path: command.source_path.clone(),
            to_path: command.destination_path.clone().ok_or_else(|| {
                sync_error(
                    "suppression-evidence-missing",
                    "rename command has no destination path",
                )
            })?,
            object_id,
            version,
            content_hash: evidence.content_hash.clone(),
            content_size_bytes: evidence.content_size_bytes,
            kind: evidence.kind.clone().ok_or_else(|| {
                sync_error(
                    "suppression-evidence-missing",
                    "rename evidence has no authoritative object kind",
                )
            })?,
        },
    };
    Ok(CloudFilesNamespaceSuppression {
        operation_id: command.operation_id.clone(),
        expected_action: action,
    })
}

pub fn cloud_namespace_cursor_from_dfs(
    cursor: &dfs::DfsNamespaceChangeCursor,
) -> CloudFilesNamespaceCursor {
    CloudFilesNamespaceCursor {
        file_system_id: cursor.file_system_id.to_string(),
        shard_id: cursor.shard_id.0.clone(),
        authority_epoch: cursor.authority_epoch,
        after_sequence: cursor.after_sequence,
    }
}

pub fn dfs_namespace_cursor_from_cloud(
    cursor: &CloudFilesNamespaceCursor,
) -> Result<dfs::DfsNamespaceChangeCursor, CloudFilesWindowsSyncError> {
    Ok(dfs::DfsNamespaceChangeCursor {
        file_system_id: cursor.file_system_id.parse().map_err(|error| {
            sync_error(
                "invalid-dfs-cursor",
                format!("parse filesystem ID: {error}"),
            )
        })?,
        shard_id: dfs::DfsMetadataShardId::new(cursor.shard_id.clone()),
        authority_epoch: cursor.authority_epoch,
        after_sequence: cursor.after_sequence,
    })
}

pub fn cloud_namespace_batch_from_dfs(
    input_cursor: &dfs::DfsNamespaceChangeCursor,
    batch: dfs::DfsNamespaceChangeBatch,
) -> CloudFilesNamespaceFeedBatch {
    CloudFilesNamespaceFeedBatch {
        input_cursor: cloud_namespace_cursor_from_dfs(input_cursor),
        next_cursor: cloud_namespace_cursor_from_dfs(&batch.next_cursor),
        events: batch
            .events
            .into_iter()
            .map(cloud_namespace_event_from_dfs)
            .collect(),
        journal_head_sequence: batch.journal_head_sequence,
        has_more: batch.has_more,
    }
}

/// Bound one reconciliation pass without splitting the multiple semantic
/// effects projected from a single journal sequence. Cursor durability is
/// sequence-based, so a pass either processes every event at that sequence or
/// leaves the sequence untouched for the next poll.
pub(crate) fn bound_namespace_feed_batch(
    mut batch: CloudFilesNamespaceFeedBatch,
    max_events: usize,
) -> CloudFilesNamespaceFeedBatch {
    if batch.events.len() <= max_events
        || matches!(
            batch.events.as_slice(),
            [CloudFilesNamespaceFeedEvent::RescanRequired { .. }]
        )
    {
        return batch;
    }
    let target_index = max_events.max(1).saturating_sub(1);
    let Some(target_sequence) = batch
        .events
        .get(target_index)
        .and_then(namespace_event_sequence)
    else {
        return batch;
    };
    let keep = batch
        .events
        .iter()
        .take_while(|event| {
            namespace_event_sequence(event).is_some_and(|sequence| sequence <= target_sequence)
        })
        .count();
    batch.events.truncate(keep);
    batch.next_cursor.after_sequence = target_sequence;
    batch.has_more = true;
    batch
}

fn namespace_event_sequence(event: &CloudFilesNamespaceFeedEvent) -> Option<u64> {
    match event {
        CloudFilesNamespaceFeedEvent::Upsert { provenance, .. }
        | CloudFilesNamespaceFeedEvent::Remove { provenance, .. }
        | CloudFilesNamespaceFeedEvent::Rename { provenance, .. } => {
            Some(provenance.journal_sequence)
        }
        CloudFilesNamespaceFeedEvent::RescanRequired { .. } => None,
    }
}

fn cloud_namespace_event_from_dfs(
    event: dfs::DfsNamespaceChangeEvent,
) -> CloudFilesNamespaceFeedEvent {
    match event {
        dfs::DfsNamespaceChangeEvent::Upsert {
            path,
            object_id,
            version,
            content_hash,
            content_size_bytes,
            kind,
            provenance,
        } => CloudFilesNamespaceFeedEvent::Upsert {
            path,
            object_id: object_id.to_string(),
            version: version.to_string(),
            content_hash,
            content_size_bytes,
            kind: dfs_object_kind_name(&kind).to_string(),
            provenance: cloud_namespace_provenance_from_dfs(provenance),
        },
        dfs::DfsNamespaceChangeEvent::Remove {
            path,
            object_id,
            version,
            content_hash,
            provenance,
        } => CloudFilesNamespaceFeedEvent::Remove {
            path,
            object_id: object_id.to_string(),
            version: version.to_string(),
            content_hash,
            provenance: cloud_namespace_provenance_from_dfs(provenance),
        },
        dfs::DfsNamespaceChangeEvent::Rename {
            from_path,
            to_path,
            object_id,
            version,
            content_hash,
            content_size_bytes,
            kind,
            provenance,
        } => CloudFilesNamespaceFeedEvent::Rename {
            from_path,
            to_path,
            object_id: object_id.to_string(),
            version: version.to_string(),
            content_hash,
            content_size_bytes,
            kind: dfs_object_kind_name(&kind).to_string(),
            provenance: cloud_namespace_provenance_from_dfs(provenance),
        },
        dfs::DfsNamespaceChangeEvent::RescanRequired { reason, detail } => {
            CloudFilesNamespaceFeedEvent::RescanRequired {
                reason: dfs_rescan_reason_name(reason).to_string(),
                detail,
            }
        }
    }
}

fn cloud_namespace_provenance_from_dfs(
    provenance: dfs::DfsNamespaceChangeProvenance,
) -> CloudFilesNamespaceProvenance {
    CloudFilesNamespaceProvenance {
        event_id: provenance.event_id,
        journal_sequence: provenance.journal_sequence,
        journal_ordinal: provenance.journal_ordinal,
        operation_id: provenance.operation_id,
    }
}

fn dfs_object_kind_name(kind: &dfs::DfsObjectKind) -> &'static str {
    match kind {
        dfs::DfsObjectKind::File => "file",
        dfs::DfsObjectKind::Directory => "directory",
        dfs::DfsObjectKind::Symlink => "symlink",
        dfs::DfsObjectKind::Hardlink => "hardlink",
        dfs::DfsObjectKind::Artifact => "artifact",
        dfs::DfsObjectKind::Mount => "mount",
        dfs::DfsObjectKind::Tombstone => "tombstone",
    }
}

fn dfs_rescan_reason_name(reason: dfs::DfsNamespaceRescanReason) -> &'static str {
    match reason {
        dfs::DfsNamespaceRescanReason::FileSystemChanged => "file-system-changed",
        dfs::DfsNamespaceRescanReason::ShardChanged => "shard-changed",
        dfs::DfsNamespaceRescanReason::AuthorityEpochChanged => "authority-epoch-changed",
        dfs::DfsNamespaceRescanReason::JournalCompactionGap => "journal-compaction-gap",
        dfs::DfsNamespaceRescanReason::JournalRewound => "journal-rewound",
        dfs::DfsNamespaceRescanReason::PayloadUnavailable => "payload-unavailable",
        dfs::DfsNamespaceRescanReason::CrossShardRenameCorrelationUnavailable => {
            "cross-shard-rename-correlation-unavailable"
        }
        dfs::DfsNamespaceRescanReason::NonAtomicRenameProjectionUnavailable => {
            "non-atomic-rename-projection-unavailable"
        }
    }
}

pub fn mutation_contract_blocker() -> CloudFilesModuleError {
    CloudFilesModuleError {
        code: CLOUD_FILES_WRITABLE_SYNC_CONTRACT_NOT_READY_CODE.to_string(),
        detail: "DFS has not yet exposed both caller-stable mkdir and a public read-only stable-operation receipt reconciliation facade"
            .to_string(),
    }
}

pub fn conflict_evidence(
    code: impl Into<String>,
    detail: impl Into<String>,
) -> CloudFilesConflictEvidence {
    CloudFilesConflictEvidence {
        code: code.into(),
        detail: detail.into(),
        observed_object_id: None,
        observed_version: None,
        observed_content_hash: None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, AtomicUsize};
    use std::sync::{Arc, Mutex, mpsc};
    use std::thread;
    use std::time::Duration;

    use sinomo_dfs_cloud_files_api::CloudFilesLocalChangeSyncMode;

    use super::*;
    use crate::local_change_queue::{CloudFilesExpectedObjectState, CloudFilesStagedContent};
    use crate::namespace_reconciler::{
        CloudFilesFullScanRequest, CloudFilesLocalPathState, CloudFilesNamespaceConflict,
    };
    use crate::sync_executor::CloudFilesDfsMutationContent;

    static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn namespace_effect_cancellation_stops_the_worker_without_a_failure_event() {
        let error = reconcile_error(CloudFilesNamespaceReconcileError {
            code: "namespace-effect",
            detail: "sync-cancelled".to_string(),
        });

        assert_eq!(error.code, "sync-cancelled");
        assert_eq!(error.detail, "writable Cloud Files worker was cancelled");
    }

    #[test]
    fn object_projection_never_fabricates_unsupported_file_kinds() {
        assert_eq!(
            cloud_files_object_projection(&dfs::DfsObjectKind::File),
            CloudFilesObjectProjection::File
        );
        assert_eq!(
            cloud_files_object_projection(&dfs::DfsObjectKind::Directory),
            CloudFilesObjectProjection::Directory
        );
        assert_eq!(
            cloud_files_object_projection(&dfs::DfsObjectKind::Mount),
            CloudFilesObjectProjection::Directory
        );
        assert!(matches!(
            cloud_files_object_projection(&dfs::DfsObjectKind::Symlink),
            CloudFilesObjectProjection::Unsupported("symlink")
        ));
        assert!(matches!(
            cloud_files_object_projection(&dfs::DfsObjectKind::Hardlink),
            CloudFilesObjectProjection::Unsupported("hardlink")
        ));
        assert!(matches!(
            cloud_files_object_projection(&dfs::DfsObjectKind::Artifact),
            CloudFilesObjectProjection::Unsupported("artifact")
        ));
    }

    #[test]
    fn content_identical_local_capture_and_replace_are_noop_candidates() {
        let staged = CloudFilesStagedContent {
            content_hash: "blake3:same-content".to_string(),
            length: 17,
        };
        let manifest = CloudFilesReconciledManifestEntry {
            path: "/services/same.txt".to_string(),
            object_id: "01sameobject".to_string(),
            version: "01sameversion".to_string(),
            content_hash: Some(staged.content_hash.clone()),
            content_size_bytes: Some(staged.length),
            kind: "file".to_string(),
        };
        assert!(staged_content_matches_manifest_entry(&staged, &manifest));

        let command = CloudFilesDfsMutationCommand {
            operation_id: "noop-replace".to_string(),
            request_digest: "digest".to_string(),
            kind: CloudFilesLocalOperationKind::ReplaceFile,
            source_path: manifest.path.clone(),
            destination_path: None,
            preconditions: CloudFilesLocalOperationPreconditions {
                source: Some(CloudFilesExpectedObjectState {
                    object_id: manifest.object_id.clone(),
                    version: manifest.version.clone(),
                    content_hash: manifest.content_hash.clone(),
                }),
                destination: CloudFilesDestinationPrecondition::Unspecified,
            },
            staged_content: Some(CloudFilesDfsMutationContent {
                descriptor: staged.clone(),
                blob_path: PathBuf::from("unused-noop-blob"),
            }),
        };
        assert!(replace_command_may_have_unchanged_content(&command).expect("valid replace"));

        let mut changed = staged;
        changed.content_hash = "blake3:changed-content".to_string();
        assert!(!staged_content_matches_manifest_entry(&changed, &manifest));
        let mut changed_command = command;
        changed_command.staged_content = Some(CloudFilesDfsMutationContent {
            descriptor: changed,
            blob_path: PathBuf::from("unused-changed-blob"),
        });
        assert!(
            !replace_command_may_have_unchanged_content(&changed_command)
                .expect("valid changed replace")
        );
    }

    #[test]
    fn blocked_run_once_cannot_advance_worker_health_generation() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let calls = Arc::new(AtomicUsize::new(0));
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let (progress_sender, progress_receiver) = mpsc::channel();
        let thread_cancelled = Arc::clone(&cancelled);
        let thread_calls = Arc::clone(&calls);
        let join = thread::spawn(move || {
            run_progress_gated_worker_loop(
                &thread_cancelled,
                41,
                || {
                    let call = thread_calls.fetch_add(1, Ordering::SeqCst);
                    if call == 0 {
                        started_sender.send(()).expect("signal blocked pass");
                        release_receiver.recv().expect("release blocked pass");
                    } else {
                        thread_cancelled.store(true, Ordering::SeqCst);
                    }
                    Ok::<_, ()>("healthy")
                },
                |status, generation| {
                    progress_sender
                        .send((status, generation))
                        .expect("record progress")
                },
                |_| {},
            )
        });

        started_receiver.recv().expect("worker pass started");
        assert!(matches!(
            progress_receiver.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        release_sender.send(()).expect("release worker pass");
        assert_eq!(
            progress_receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("completed pass advances health"),
            ("healthy", 42)
        );
        join.join().expect("join worker loop").expect("worker loop");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn read_only_namespace_failures_retry_without_masking_fatal_or_cancelled_passes() {
        let cancelled = AtomicBool::new(false);
        let mut calls = 0;
        let mut waits = 0;
        let recovered = run_retrying_namespace_read_pass(
            &cancelled,
            || {
                calls += 1;
                if calls == 1 {
                    Err(sync_error(
                        "namespace-feed",
                        "transient routed read timeout",
                    ))
                } else {
                    Ok("recovered")
                }
            },
            |_| waits += 1,
        )
        .expect("read-only namespace retry");
        assert_eq!(recovered, "recovered");
        assert_eq!(calls, 2);
        assert_eq!(waits, 1);

        let fatal = run_retrying_namespace_read_pass(
            &cancelled,
            || Err::<(), _>(sync_error("namespace-effect", "ambiguous local effect")),
            |_| panic!("fatal effects must not wait or retry"),
        )
        .expect_err("local effect failure remains fatal");
        assert_eq!(fatal.code, "namespace-effect");

        cancelled.store(true, Ordering::SeqCst);
        let cancelled_read = run_retrying_namespace_read_pass(
            &cancelled,
            || Err::<(), _>(sync_error("full-reconciliation-scan", "network timeout")),
            |_| panic!("cancelled read must not wait or retry"),
        )
        .expect_err("cancelled read returns immediately");
        assert_eq!(cancelled_read.code, "full-reconciliation-scan");
    }

    #[test]
    fn split_directory_change_buffers_preserve_one_atomic_rename() {
        let mut pairing = CloudFilesRenamePairingState::default();
        let mut first = Vec::new();
        pairing.push(
            CloudFilesDirectoryChangeAction::RenamedOldName,
            PathBuf::from("old.txt"),
            &mut first,
        );
        assert!(first.is_empty());

        let mut second = Vec::new();
        pairing.push(
            CloudFilesDirectoryChangeAction::RenamedNewName,
            PathBuf::from("new.txt"),
            &mut second,
        );
        assert_eq!(
            second,
            vec![CloudFilesDirectoryChange::Renamed {
                from: PathBuf::from("old.txt"),
                to: PathBuf::from("new.txt"),
            }]
        );
    }

    #[test]
    fn sparse_prepopulation_depth_is_exact_and_materialized_dirs_remain_audited() {
        assert!(!sparse_root_is_enumerated(0, false));
        assert!(sparse_root_is_enumerated(1, false));
        assert!(sparse_root_is_enumerated(0, true));

        assert!(!sparse_child_directory_is_enumerated(1, 1, false));
        assert!(sparse_child_directory_is_enumerated(1, 2, false));
        assert!(sparse_child_directory_is_enumerated(25, 0, true));
        assert!(!sparse_child_directory_is_enumerated(25, 0, false));
    }

    #[test]
    fn depth_zero_large_root_has_constant_activation_and_lazy_bounded_pages() {
        const REMOTE_ROOT_CHILDREN: usize = 1_000_000;
        assert!(!sparse_root_is_enumerated(0, false));

        let mut observed = 0usize;
        let mut page_count = 0usize;
        while observed < REMOTE_ROOT_CHILDREN {
            let page = REMOTE_ROOT_CHILDREN
                .saturating_sub(observed)
                .min(CLOUD_FILES_DIRECTORY_PAGE_ENTRIES);
            assert!(page <= CLOUD_FILES_DIRECTORY_PAGE_ENTRIES);
            observed = observed.saturating_add(page);
            page_count = page_count.saturating_add(1);
        }
        assert_eq!(observed, REMOTE_ROOT_CHILDREN);
        assert_eq!(
            page_count,
            REMOTE_ROOT_CHILDREN.div_ceil(CLOUD_FILES_DIRECTORY_PAGE_ENTRIES)
        );
    }

    #[test]
    fn sparse_missing_subtree_is_removed_leaf_to_root() {
        let paths = ["/services/a", "/services/a/b", "/services/a/b/file.txt"];
        let manifest = paths
            .iter()
            .map(|path| {
                (
                    (*path).to_string(),
                    CloudFilesReconciledManifestEntry {
                        path: (*path).to_string(),
                        object_id: format!("object-{path}"),
                        version: format!("version-{path}"),
                        content_hash: None,
                        content_size_bytes: None,
                        kind: if path.ends_with(".txt") {
                            "file".to_string()
                        } else {
                            "directory".to_string()
                        },
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let materialized = BTreeSet::from([
            "/services".to_string(),
            "/services/a".to_string(),
            "/services/a/b".to_string(),
        ]);
        let mut before = None;
        let mut ordered = Vec::new();
        for _ in 0..3 {
            let page = next_sparse_known_path(
                &manifest,
                &materialized,
                before.as_deref(),
                CLOUD_FILES_DIRECTORY_PAGE_ENTRIES,
            );
            before = page.before;
            ordered.push(page.path.expect("known path"));
        }
        assert_eq!(
            ordered,
            vec!["/services/a/b/file.txt", "/services/a/b", "/services/a",]
        );
    }

    #[test]
    fn sparse_manifest_cursor_is_bounded_and_does_not_clone_large_unmaterialized_namespace() {
        let mut manifest = BTreeMap::new();
        for index in 0..10_000 {
            let path = format!("/services/unmaterialized/{index:05}");
            manifest.insert(
                path.clone(),
                CloudFilesReconciledManifestEntry {
                    path,
                    object_id: format!("object-{index}"),
                    version: format!("version-{index}"),
                    content_hash: None,
                    content_size_bytes: None,
                    kind: "file".to_string(),
                },
            );
        }
        let owned_path = "/services/z/file.txt".to_string();
        manifest.insert(
            owned_path.clone(),
            CloudFilesReconciledManifestEntry {
                path: owned_path.clone(),
                object_id: "owned-object".to_string(),
                version: "owned-version".to_string(),
                content_hash: None,
                content_size_bytes: None,
                kind: "file".to_string(),
            },
        );
        let page = next_sparse_known_path(
            &manifest,
            &BTreeSet::from(["/services/z".to_string()]),
            None,
            CLOUD_FILES_DIRECTORY_PAGE_ENTRIES,
        );
        assert_eq!(page.path.as_deref(), Some(owned_path.as_str()));
        assert!(page.examined <= CLOUD_FILES_DIRECTORY_PAGE_ENTRIES);
    }

    #[test]
    fn cloud_files_root_requires_authoritative_directory_projection() {
        assert!(cloud_files_root_kind_is_supported(
            &dfs::DfsObjectKind::Directory
        ));
        assert!(cloud_files_root_kind_is_supported(
            &dfs::DfsObjectKind::Mount
        ));
        assert!(!cloud_files_root_kind_is_supported(
            &dfs::DfsObjectKind::File
        ));
        assert!(!cloud_files_root_kind_is_supported(
            &dfs::DfsObjectKind::Tombstone
        ));
    }

    #[test]
    fn dirty_completion_follow_up_is_exact_generation_and_capture_stable() {
        let entry = CloudFilesReconciledManifestEntry {
            path: "/services/renamed.txt".to_string(),
            object_id: "01postrenameobject".to_string(),
            version: "01postrenameversion".to_string(),
            content_hash: Some("blake3:remote".to_string()),
            content_size_bytes: Some(12),
            kind: "file".to_string(),
        };
        let staged = CloudFilesStagedContent {
            content_hash: "blake3:local-newer".to_string(),
            length: 17,
        };
        let first = completion_follow_up_request(
            "mount-a",
            "/services",
            "cf_parent",
            &entry,
            staged.clone(),
            "volume:file:usn:local-newer",
        );
        let second = completion_follow_up_request(
            "mount-a",
            "/services",
            "cf_parent",
            &entry,
            staged,
            "volume:file:usn:local-newer",
        );
        assert_eq!(first.capture_id, second.capture_id);
        assert_eq!(first.kind, CloudFilesLocalOperationKind::ReplaceFile);
        assert_eq!(first.source_path, entry.path);
        assert_eq!(
            first.preconditions.source,
            Some(CloudFilesExpectedObjectState {
                object_id: entry.object_id,
                version: entry.version,
                content_hash: entry.content_hash,
            })
        );
        assert!(matches!(
            first.preconditions.destination,
            CloudFilesDestinationPrecondition::Unspecified
        ));
    }

    #[test]
    fn staged_upload_reader_observes_cancellation_between_reads() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut reader = CancellationReader::new(
            std::io::Cursor::new(vec![7u8; 128 * 1024]),
            Arc::clone(&cancelled),
        );
        let mut first = [0u8; 4096];
        assert_eq!(reader.read(&mut first).expect("first read"), first.len());
        cancelled.store(true, Ordering::SeqCst);
        let error = reader
            .read(&mut first)
            .expect_err("cancelled upload read must stop");
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    }

    #[test]
    fn ambiguous_directory_change_order_forces_snapshot_reconciliation() {
        let modify_then_rename = vec![
            CloudFilesDirectoryChange::Modified(PathBuf::from("new.txt")),
            CloudFilesDirectoryChange::Renamed {
                from: PathBuf::from("old.txt"),
                to: PathBuf::from("new.txt"),
            },
        ];
        assert_eq!(
            plan_directory_changes(modify_then_rename),
            CloudFilesDirectoryChangePlan::FullScanRequired
        );

        let atomic_save = vec![
            CloudFilesDirectoryChange::Removed(PathBuf::from("report.txt")),
            CloudFilesDirectoryChange::Added(PathBuf::from("report.txt")),
        ];
        assert_eq!(
            plan_directory_changes(atomic_save),
            CloudFilesDirectoryChangePlan::FullScanRequired
        );

        let rename_chain = vec![
            CloudFilesDirectoryChange::Renamed {
                from: PathBuf::from("a.txt"),
                to: PathBuf::from("b.txt"),
            },
            CloudFilesDirectoryChange::Renamed {
                from: PathBuf::from("b.txt"),
                to: PathBuf::from("c.txt"),
            },
        ];
        assert_eq!(
            plan_directory_changes(rename_chain),
            CloudFilesDirectoryChangePlan::FullScanRequired
        );
    }

    #[test]
    fn repeated_upserts_coalesce_to_last_observation_without_reordering_paths() {
        let changes = vec![
            CloudFilesDirectoryChange::Modified(PathBuf::from("a.txt")),
            CloudFilesDirectoryChange::Added(PathBuf::from("b.txt")),
            CloudFilesDirectoryChange::Modified(PathBuf::from("a.txt")),
        ];
        assert_eq!(
            plan_directory_changes(changes),
            CloudFilesDirectoryChangePlan::Ordered(vec![
                CloudFilesDirectoryChange::Added(PathBuf::from("b.txt")),
                CloudFilesDirectoryChange::Modified(PathBuf::from("a.txt")),
            ])
        );
    }

    #[test]
    fn local_capture_retry_backoff_is_real_and_bounded() {
        assert_eq!(local_capture_retry_delay(1), Duration::from_millis(50));
        assert_eq!(local_capture_retry_delay(2), Duration::from_millis(100));
        assert_eq!(local_capture_retry_delay(3), Duration::from_millis(200));
        assert_eq!(local_capture_retry_delay(7), Duration::from_secs(2));
        assert_eq!(local_capture_retry_delay(u32::MAX), Duration::from_secs(2));
    }

    #[test]
    fn long_held_local_write_is_deferred_with_backoff_instead_of_failing_worker() {
        let path = PathBuf::from("held-open.txt");
        let mut retries = CloudFilesLocalCaptureRetries::default();
        assert_eq!(retries.schedule(path.clone(), Duration::ZERO), 1);
        assert!(retries.due(Duration::from_millis(49)).is_empty());
        assert_eq!(retries.due(Duration::from_millis(50)), vec![path.clone()]);

        assert_eq!(retries.schedule(path.clone(), Duration::from_millis(50)), 2);
        assert!(retries.due(Duration::from_millis(149)).is_empty());
        assert_eq!(retries.due(Duration::from_millis(150)), vec![path.clone()]);

        for attempt in 3..=12 {
            assert_eq!(
                retries.schedule(path.clone(), Duration::from_secs(1)),
                attempt
            );
        }
        assert!(retries.due(Duration::from_millis(2_999)).is_empty());
        assert_eq!(retries.due(Duration::from_secs(3)), vec![path.clone()]);
        retries.clear(&path);
        assert!(retries.due(Duration::from_secs(10)).is_empty());
    }

    #[test]
    fn deferred_capture_retry_pass_is_strictly_bounded() {
        let mut retries = CloudFilesLocalCaptureRetries::default();
        for index in 0..1_000 {
            retries.schedule(
                PathBuf::from(format!("held-{index:04}.txt")),
                Duration::ZERO,
            );
        }
        let due = retries.due_bounded(Duration::from_secs(10), 64);
        assert_eq!(due.len(), 64);
        assert_eq!(retries.len(), 1_000);
        assert_eq!(due.first(), Some(&PathBuf::from("held-0000.txt")));
        assert_eq!(due.last(), Some(&PathBuf::from("held-0063.txt")));
    }

    #[test]
    fn owned_restart_offline_creates_require_absence_and_parent_ordering() {
        let remote = BTreeMap::from([(
            "/services/existing".to_string(),
            CloudFilesReconciledManifestEntry {
                path: "/services/existing".to_string(),
                object_id: "01existing".to_string(),
                version: "01version".to_string(),
                content_hash: None,
                content_size_bytes: None,
                kind: "directory".to_string(),
            },
        )]);
        assert_eq!(
            plan_owned_offline_create("/services", "/services/new.txt", &remote),
            CloudFilesOfflineCreatePlan::QueueNow
        );
        assert_eq!(
            plan_owned_offline_create("/services", "/services/existing/child.txt", &remote),
            CloudFilesOfflineCreatePlan::QueueNow
        );
        assert_eq!(
            plan_owned_offline_create("/services", "/services/offline/child.txt", &remote),
            CloudFilesOfflineCreatePlan::DeferUntilParentIsRemote
        );
        assert_eq!(
            plan_owned_offline_create("/services", "/services/existing", &remote),
            CloudFilesOfflineCreatePlan::ConflictWithRemote
        );
    }

    #[test]
    fn owned_restart_offline_create_enters_durable_absent_cas_queue() {
        let root = TestRoot::new("offline-create-restart");
        let state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("open writable state");
        let capture = FakeCapture {
            startup_captures: vec![create_capture("offline-file-usn-7", b"offline")],
            ..FakeCapture::default()
        };
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            capture,
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            FakeNamespace::with_scans([
                CloudFilesFullScanSnapshot {
                    scan_complete: true,
                    ..CloudFilesFullScanSnapshot::default()
                },
                CloudFilesFullScanSnapshot {
                    scan_complete: true,
                    ..CloudFilesFullScanSnapshot::default()
                },
            ]),
            FakeEffects::default(),
        );

        worker
            .startup(&AtomicBool::new(false))
            .expect("startup queues offline create");
        let operation = worker
            .state()
            .queue()
            .operations()
            .next()
            .expect("durable offline operation");
        assert_eq!(
            operation.request.kind,
            CloudFilesLocalOperationKind::CreateFile
        );
        assert_eq!(
            operation.request.preconditions.destination,
            CloudFilesDestinationPrecondition::Absent
        );
        assert_eq!(operation.status, CloudFilesLocalOperationStatus::Retrying);
        assert_eq!(
            worker
                .state()
                .queue()
                .read_staged_content(&operation.request.operation_id)
                .expect("immutable offline bytes"),
            b"offline"
        );
    }

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new(_label: &str) -> Self {
            let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            // Worker fixtures inherit the reconciler's deeply nested scope
            // paths, so avoid spending the Windows path budget on labels.
            let path = std::env::temp_dir().join(format!("cfw-{}-{sequence}", std::process::id()));
            fs::create_dir_all(&path).expect("create test root");
            Self(path)
        }

        fn data_dir(&self) -> SinomoDataDir {
            SinomoDataDir::new(&self.0)
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum FakeCommitBehavior {
        Accept,
        FailBeforeAccept,
        LoseResponseAfterAccept,
    }

    struct FakeMountedMutationDaemon {
        store: dfs::MountedSifsMutationOutboxStore,
        events: Arc<Mutex<Vec<String>>>,
        probe_error: bool,
        commit_behavior: FakeCommitBehavior,
        commit_observer: Option<Arc<dyn Fn() + Send + Sync>>,
    }

    impl FakeMountedMutationDaemon {
        fn record(&self, event: impl Into<String>) {
            self.events.lock().expect("event log").push(event.into());
        }

        fn finish_commit(&self, accept: impl FnOnce() -> Result<(), String>) -> Result<(), String> {
            if let Some(observer) = &self.commit_observer {
                observer();
            }
            match self.commit_behavior {
                FakeCommitBehavior::Accept => accept(),
                FakeCommitBehavior::FailBeforeAccept => {
                    Err("simulated commit failure before acceptance".to_string())
                }
                FakeCommitBehavior::LoseResponseAfterAccept => {
                    accept()?;
                    Err("simulated lost commit response".to_string())
                }
            }
        }
    }

    impl CloudFilesMountedMutationDaemon for FakeMountedMutationDaemon {
        fn probe(&self) -> Result<(), String> {
            self.record("probe");
            if self.probe_error {
                Err("simulated probe failure".to_string())
            } else {
                Ok(())
            }
        }

        fn arm(
            &self,
            marker_id: dfs::MountedSifsMutationMarkerId,
            marker_nonce: dfs::MountedSifsMutationNonce,
            arm_request_id: dfs::MountedSifsMutationArmRequestId,
        ) -> Result<(), String> {
            self.record("arm");
            self.store
                .arm_live(&marker_id, &marker_nonce, &arm_request_id)
                .map(drop)
                .map_err(|error| error.to_string())
        }

        fn commit_applied(
            &self,
            marker_id: dfs::MountedSifsMutationMarkerId,
            marker_nonce: dfs::MountedSifsMutationNonce,
            arm_request_id: dfs::MountedSifsMutationArmRequestId,
            captured_work: dfs::MountedSifsMutationCapturedWork,
        ) -> Result<(), String> {
            self.record("commit-applied");
            self.finish_commit(|| {
                self.store
                    .accept_captured_live(
                        &marker_id,
                        &marker_nonce,
                        &arm_request_id,
                        &captured_work,
                    )
                    .map(drop)
                    .map_err(|error| error.to_string())
            })
        }

        fn commit_unknown(
            &self,
            marker_id: dfs::MountedSifsMutationMarkerId,
            marker_nonce: dfs::MountedSifsMutationNonce,
            arm_request_id: dfs::MountedSifsMutationArmRequestId,
            observation: dfs::MountedSifsMutationErrorObservation,
        ) -> Result<(), String> {
            self.record("commit-unknown");
            self.finish_commit(|| {
                self.store
                    .accept_unknown_live(&marker_id, &marker_nonce, &arm_request_id, &observation)
                    .map(drop)
                    .map_err(|error| error.to_string())
            })
        }

        fn prove_no_write(
            &self,
            marker_id: dfs::MountedSifsMutationMarkerId,
            marker_nonce: dfs::MountedSifsMutationNonce,
            arm_request_id: dfs::MountedSifsMutationArmRequestId,
            proof_code: &'static str,
        ) -> Result<(), String> {
            self.record("prove-no-write");
            self.store
                .prove_no_write_live(&marker_id, &marker_nonce, &arm_request_id, proof_code)
                .map(drop)
                .map_err(|error| error.to_string())
        }
    }

    fn fake_handoff(
        root: &TestRoot,
        behavior: FakeCommitBehavior,
        commit_observer: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> (
        Arc<CloudFilesMountedMutationHandoff<FakeMountedMutationDaemon>>,
        dfs::MountedSifsMutationOutboxStore,
        Arc<Mutex<Vec<String>>>,
    ) {
        let data_dir = root.data_dir();
        let store = dfs::MountedSifsMutationOutboxStore::new(&data_dir).expect("test outbox");
        let events = Arc::new(Mutex::new(Vec::new()));
        let client = FakeMountedMutationDaemon {
            store: store.clone(),
            events: Arc::clone(&events),
            probe_error: false,
            commit_behavior: behavior,
            commit_observer,
        };
        let handoff = CloudFilesMountedMutationHandoff::open_with_client(
            &data_dir,
            "cloud-files:test-root",
            client,
        )
        .expect("open fake Cloud Files handoff");
        (Arc::new(handoff), store, events)
    }

    fn empty_mutation_report<R, E>(
        outcome: Result<R, E>,
    ) -> dfs::MountedSifsMutationReport<Result<R, E>> {
        dfs::MountedSifsMutationReport {
            outcome,
            committed_work: dfs::DfsCommittedWork::default(),
            committed_repair_records: Vec::new(),
        }
    }

    fn mount_open_repair_record(label: &str) -> dfs::DfsRepairAttemptRecord {
        dfs::DfsRepairAttemptRecord {
            intent_id: format!("intent-{label}"),
            object_id: dfs::DfsObjectId::from_legacy_named(format!("object-{label}"))
                .expect("repair object ID"),
            version: dfs::DfsVersion::from_legacy_named(format!("version-{label}"))
                .expect("repair version"),
            content_hash: format!("blake3:{label}"),
            missing_replica_node_id: dfs::DfsNodeId::from_legacy_named(format!("node-{label}"))
                .expect("repair target"),
            attempts: 0,
            in_flight: false,
            last_attempt_unix_nanos: 0,
            next_retry_unix_nanos: 0,
            transfer_request_id: None,
            last_error: None,
        }
    }

    #[derive(Debug)]
    struct MountedReadPolicyProbeTransport {
        local_node_id: dfs::DfsNodeId,
        primary_node_id: dfs::DfsNodeId,
        calls: Arc<Mutex<Vec<&'static str>>>,
        incoming: VecDeque<dfs::DfsRoutedMessage>,
    }

    impl dfs::DfsTransportIdentity for MountedReadPolicyProbeTransport {
        fn local_node_id(&self) -> &dfs::DfsNodeId {
            &self.local_node_id
        }
    }

    impl dfs::DfsMessageTransport for MountedReadPolicyProbeTransport {
        fn send_routed_call(
            &mut self,
            target_node_id: &dfs::DfsNodeId,
            call: dfs::DfsRoutedCall,
        ) -> Result<(), sinomo_runtime::network::NetworkError> {
            assert_eq!(target_node_id, &self.primary_node_id);
            let routed = call.message;
            let dfs::DfsNetworkMessage::NamespaceRequest(namespace) = &routed.message else {
                panic!(
                    "unexpected Cloud Files mounted-read request: {:?}",
                    routed.message
                );
            };
            let label = match &namespace.operation {
                dfs::DfsNamespaceOperation::LookupPath { .. } => "lookup-path",
                dfs::DfsNamespaceOperation::SyncPath { .. } => "sync-path",
                operation => {
                    panic!("unexpected Cloud Files mounted-read namespace operation: {operation:?}")
                }
            };
            self.calls.lock().expect("mounted read calls").push(label);
            self.incoming.push_back(dfs::DfsRoutedMessage {
                source_node_id: self.primary_node_id.clone(),
                target_node_id: self.local_node_id.clone(),
                frame_id: routed.frame_id.saturating_add(1),
                message_id: routed.message_id.saturating_add(1),
                message: dfs::DfsNetworkMessage::NamespaceResponse(dfs::DfsNamespaceResponse {
                    request_id: namespace.request_id.clone(),
                    result: dfs::DfsNamespaceResult::Rejected {
                        reason: "injected authoritative lookup failure".to_string(),
                        posix_error: None,
                        current_primary_node_id: None,
                    },
                    synced_path_object: None,
                    synced_path_objects: Vec::new(),
                    metadata_journal_batches: Vec::new(),
                    metadata_journal_has_more: None,
                    posix_coordination_result: None,
                }),
            });
            Ok(())
        }

        fn drain_incoming_messages(
            &mut self,
        ) -> Result<Vec<dfs::DfsRoutedMessage>, sinomo_runtime::network::NetworkError> {
            Ok(self.incoming.drain(..).collect())
        }

        fn pending_incoming_count(
            &self,
        ) -> Result<Option<usize>, sinomo_runtime::network::NetworkError> {
            Ok(Some(self.incoming.len()))
        }
    }

    struct HandoffBackedTestAdapter {
        handoff: Arc<CloudFilesMountedMutationHandoff<FakeMountedMutationDaemon>>,
        service: Arc<Mutex<u64>>,
    }

    impl CloudFilesDfsMutationAdapter for HandoffBackedTestAdapter {
        fn apply(
            &mut self,
            command: &CloudFilesDfsMutationCommand,
        ) -> Result<CloudFilesDfsMutationOutcome, String> {
            self.handoff.run_captured_mutation(
                &self.service,
                &command.operation_id,
                cloud_files_mounted_mutation_label(&command.kind),
                |service| {
                    *service += 1;
                    empty_mutation_report(Ok::<_, &'static str>(()))
                },
            )?;
            Ok(CloudFilesDfsMutationOutcome::Applied(
                CloudFilesRemoteMutationEvidence {
                    operation_id: command.operation_id.clone(),
                    object_id: Some("remote-object".to_string()),
                    version: Some("remote-version".to_string()),
                    content_hash: command
                        .staged_content
                        .as_ref()
                        .map(|content| content.descriptor.content_hash.clone()),
                    content_size_bytes: command
                        .staged_content
                        .as_ref()
                        .map(|content| content.descriptor.length),
                    kind: Some("file".to_string()),
                    detail: "test mutation applied".to_string(),
                },
            ))
        }

        fn reconcile(
            &mut self,
            _command: &CloudFilesDfsMutationCommand,
        ) -> Result<CloudFilesDfsMutationReconciliation, String> {
            self.handoff.ensure_ready()?;
            Ok(CloudFilesDfsMutationReconciliation::NotApplied)
        }
    }

    #[test]
    fn mounted_mutation_probe_failure_precedes_host_session_open() {
        let root = TestRoot::new("handoff-probe-failure");
        let data_dir = root.data_dir();
        let store = dfs::MountedSifsMutationOutboxStore::new(&data_dir).expect("test outbox");
        let events = Arc::new(Mutex::new(Vec::new()));
        let client = FakeMountedMutationDaemon {
            store: store.clone(),
            events: Arc::clone(&events),
            probe_error: true,
            commit_behavior: FakeCommitBehavior::Accept,
            commit_observer: None,
        };
        assert!(
            CloudFilesMountedMutationHandoff::open_with_client(
                &data_dir,
                "cloud-files:must-not-open",
                client,
            )
            .is_err()
        );
        assert_eq!(&*events.lock().expect("events"), &["probe"]);
        let session = store
            .open_host_mount_session(
                dfs::MountedSifsMutationAdapter::CloudFiles,
                "cloud-files:proof-no-session-opened",
            )
            .expect("failed Probe left no live host session");
        session.close_clean().expect("close proof session");
    }

    #[test]
    fn durable_prepare_failure_gates_before_service_entry() {
        let root = TestRoot::new("handoff-prepare-failure");
        let service = Arc::new(Mutex::new(0_u64));
        let (handoff, _store, events) = fake_handoff(&root, FakeCommitBehavior::Accept, None);
        let oversized_operation_id =
            "x".repeat(dfs::MOUNTED_SIFS_MUTATION_OUTBOX_MAX_LABEL_BYTES.saturating_add(1));

        let error = handoff
            .run_captured_mutation(
                &service,
                &oversized_operation_id,
                "cloud-files.create-file",
                |service| {
                    *service += 1;
                    empty_mutation_report(Ok::<_, &'static str>(()))
                },
            )
            .expect_err("invalid durable Prepare must fail closed");

        assert!(error.contains("prepare durable Cloud Files mounted mutation"));
        assert_eq!(*service.lock().expect("service"), 0);
        assert_eq!(&*events.lock().expect("events"), &["probe"]);
        let gated = handoff
            .ensure_ready()
            .expect_err("invalid durable Prepare leaves its exact cause gated");
        assert!(
            gated.contains("prepare durable Cloud Files mounted mutation"),
            "{gated}"
        );
    }

    #[test]
    fn production_mounted_read_policy_never_escalates_stat_or_lstat_to_sync_path() {
        let root = TestRoot::new("mounted-read-policy");
        let local_node =
            dfs::DfsNodeId::from_legacy_named("node_cloud_files_mounted_read").expect("local node");
        let primary_node =
            dfs::DfsNodeId::from_legacy_named("node_cloud_files_mounted_read_primary")
                .expect("primary node");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let transport = MountedReadPolicyProbeTransport {
            local_node_id: local_node,
            primary_node_id: primary_node.clone(),
            calls: Arc::clone(&calls),
            incoming: VecDeque::new(),
        };
        let control_plane =
            dfs::SharedDfsControlPlaneKvClient::new(sinomo_runtime::kv::InProcessKvStore::new());
        let shard = dfs::DfsMetadataShardConfig {
            file_system_id: dfs::sifs_file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: primary_node,
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: dfs::DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: sinomo_runtime::core::DfsObjectPolicy::ReplicatedEventual,
        };
        control_plane.with_client(|kv| {
            dfs::DfsControlPlaneStore::new(kv)
                .put_metadata_shard(&shard)
                .expect("mounted-read authority shard")
        });
        let mut service = dfs::NetworkedDfsService::new(
            transport,
            dfs::LocalContentStore::new(root.0.join("content")).expect("mounted-read content"),
        )
        .with_control_plane(control_plane);

        // This is the exact policy hook invoked by open_sifs_service after its
        // Armed-window bootstrap and before the service can reach CFAPI.
        configure_cloud_files_mounted_read_policy(&mut service);
        assert!(
            service
                .posix_stat_distributed("/services/missing-stat.txt")
                .is_err()
        );
        assert!(
            service
                .posix_lstat_distributed("/services/missing-lstat.txt")
                .is_err()
        );
        assert_eq!(
            calls.lock().expect("mounted read calls").as_slice(),
            ["lookup-path", "lookup-path"],
            "Cloud Files stat/lstat must never perform an unowned SyncPath ingest"
        );
    }

    #[test]
    fn mount_open_arms_before_service_and_commits_full_work_with_repairs() {
        let root = TestRoot::new("mount-open-order");
        let observer_store =
            dfs::MountedSifsMutationOutboxStore::new(&root.data_dir()).expect("observer outbox");
        let captured_at_commit = Arc::new(Mutex::new(None));
        let observed_capture = Arc::clone(&captured_at_commit);
        let registration_calls = Arc::new(AtomicU64::new(0));
        let registration_calls_at_commit = Arc::clone(&registration_calls);
        let observer: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            assert_eq!(
                registration_calls_at_commit.load(Ordering::SeqCst),
                0,
                "registration and callbacks must remain unreachable before CommitApplied ACK"
            );
            let marker_id = observer_store
                .pending_marker_ids()
                .expect("pending mount-open marker")
                .into_iter()
                .next()
                .expect("captured mount-open marker");
            let marker = observer_store
                .marker(&marker_id)
                .expect("read mount-open marker")
                .expect("mount-open marker exists");
            let dfs::MountedSifsMutationOutboxPhase::Captured { captured_work, .. } = marker.phase
            else {
                panic!("mount open must be captured before CommitApplied");
            };
            *observed_capture.lock().expect("captured work") = Some(captured_work);
        });
        let (handoff, store, events) =
            fake_handoff(&root, FakeCommitBehavior::Accept, Some(observer));
        let repair = mount_open_repair_record("mount-open");
        let events_at_open = Arc::clone(&events);

        let opened = handoff
            .run_captured_mount_open(|| {
                events_at_open
                    .lock()
                    .expect("events")
                    .push("service-open".to_string());
                Ok::<_, &'static str>((41_u64, vec![repair.clone()]))
            })
            .expect("captured mount open");
        registration_calls.fetch_add(1, Ordering::SeqCst);
        events
            .lock()
            .expect("events")
            .push("registration".to_string());

        assert_eq!(opened, 41);
        assert_eq!(registration_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            &*events.lock().expect("events"),
            &[
                "probe",
                "arm",
                "service-open",
                "commit-applied",
                "registration"
            ]
        );
        let captured = captured_at_commit
            .lock()
            .expect("captured work")
            .clone()
            .expect("captured mount-open work");
        assert_eq!(
            captured.committed_work,
            cloud_files_mount_open_committed_work()
        );
        assert_eq!(captured.committed_repair_records, vec![repair]);
        assert_eq!(
            store
                .daemon_committed_work_latch()
                .expect("daemon committed-work latch")
                .committed_work,
            cloud_files_mount_open_committed_work()
        );
        assert!(
            store
                .pending_marker_ids()
                .expect("pending markers")
                .is_empty()
        );
        handoff
            .ensure_ready()
            .expect("successful open remains ready");
    }

    #[test]
    fn mount_open_construction_or_bootstrap_error_commits_unknown_and_fences_registration() {
        let root = TestRoot::new("mount-open-error");
        let (handoff, store, events) = fake_handoff(&root, FakeCommitBehavior::Accept, None);
        let events_at_open = Arc::clone(&events);

        let error = handoff
            .run_captured_mount_open(|| {
                events_at_open
                    .lock()
                    .expect("events")
                    .push("service-bootstrap".to_string());
                Err::<((), Vec<dfs::DfsRepairAttemptRecord>), _>(
                    "simulated service bootstrap error",
                )
            })
            .expect_err("service-open error must fail activation");

        assert!(error.contains("simulated service bootstrap error"));
        assert_eq!(
            &*events.lock().expect("events"),
            &["probe", "arm", "service-bootstrap", "commit-unknown"]
        );
        let pending = store.pending_marker_ids().expect("pending full recovery");
        assert_eq!(pending.len(), 1);
        let marker = store
            .marker(&pending[0])
            .expect("read mount-open error marker")
            .expect("mount-open error marker exists");
        let dfs::MountedSifsMutationOutboxPhase::DaemonAccepted {
            requires_full_recovery,
            error_observation: Some(observation),
            ..
        } = marker.phase
        else {
            panic!("mount-open error must remain daemon-accepted full recovery");
        };
        assert!(requires_full_recovery);
        assert_eq!(observation.error_code, CLOUD_FILES_MOUNT_OPEN_ERROR);
        assert_eq!(
            observation.captured_work.committed_work,
            cloud_files_mount_open_committed_work()
        );
        assert!(
            observation
                .captured_work
                .committed_repair_records
                .is_empty()
        );
        let later_opened = AtomicBool::new(false);
        assert!(
            handoff
                .run_captured_mount_open(|| {
                    later_opened.store(true, Ordering::SeqCst);
                    Ok::<_, &'static str>(((), Vec::new()))
                })
                .is_err()
        );
        assert!(!later_opened.load(Ordering::SeqCst));
        assert!(handoff.ensure_ready().is_err());
    }

    #[test]
    fn failed_or_lost_mount_open_commit_retains_recovery_and_prevents_registration() {
        for (index, behavior) in [
            FakeCommitBehavior::FailBeforeAccept,
            FakeCommitBehavior::LoseResponseAfterAccept,
        ]
        .into_iter()
        .enumerate()
        {
            let root = TestRoot::new(&format!("mount-open-commit-{index}"));
            let observer_store = dfs::MountedSifsMutationOutboxStore::new(&root.data_dir())
                .expect("observer outbox");
            let observed_marker_id = Arc::new(Mutex::new(None));
            let marker_at_commit = Arc::clone(&observed_marker_id);
            let observer: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
                *marker_at_commit.lock().expect("marker ID") = observer_store
                    .pending_marker_ids()
                    .expect("pending mount-open marker")
                    .into_iter()
                    .next();
            });
            let (handoff, store, _events) = fake_handoff(&root, behavior, Some(observer));
            let registration_calls = AtomicU64::new(0);

            let open_result =
                handoff.run_captured_mount_open(|| Ok::<_, &'static str>(((), Vec::new())));
            if open_result.is_ok() {
                registration_calls.fetch_add(1, Ordering::SeqCst);
            }

            assert!(open_result.is_err());
            assert_eq!(registration_calls.load(Ordering::SeqCst), 0);
            assert!(handoff.ensure_ready().is_err());
            let marker_id = observed_marker_id
                .lock()
                .expect("marker ID")
                .clone()
                .expect("captured marker ID");
            let marker = store
                .marker(&marker_id)
                .expect("read retained marker")
                .expect("retained mount-open marker");
            match behavior {
                FakeCommitBehavior::FailBeforeAccept => assert!(matches!(
                    marker.phase,
                    dfs::MountedSifsMutationOutboxPhase::Captured { .. }
                )),
                FakeCommitBehavior::LoseResponseAfterAccept => assert!(matches!(
                    marker.phase,
                    dfs::MountedSifsMutationOutboxPhase::DaemonAccepted {
                        requires_full_recovery: false,
                        ..
                    }
                )),
                FakeCommitBehavior::Accept => unreachable!(),
            }
        }
    }

    #[test]
    fn oversized_mount_open_repair_capture_commits_unknown_and_fails_activation() {
        let root = TestRoot::new("mount-open-repair-overflow");
        let (handoff, store, events) = fake_handoff(&root, FakeCommitBehavior::Accept, None);
        let repairs = vec![
            mount_open_repair_record("overflow");
            dfs::MOUNTED_SIFS_MUTATION_OUTBOX_MAX_REPAIR_RECORDS + 1
        ];

        let error = handoff
            .run_captured_mount_open(|| Ok::<_, &'static str>(((), repairs)))
            .expect_err("unrepresentable repair snapshot must fail activation");

        assert!(error.contains("captured 65 repair records"));
        assert_eq!(
            &*events.lock().expect("events"),
            &["probe", "arm", "commit-unknown"]
        );
        let pending = store.pending_marker_ids().expect("pending full recovery");
        assert_eq!(pending.len(), 1);
        let marker = store
            .marker(&pending[0])
            .expect("read overflow marker")
            .expect("overflow marker exists");
        assert!(matches!(
            marker.phase,
            dfs::MountedSifsMutationOutboxPhase::DaemonAccepted {
                requires_full_recovery: true,
                error_observation: Some(dfs::MountedSifsMutationErrorObservation {
                    error_code,
                    ..
                }),
                ..
            } if error_code == CLOUD_FILES_MOUNT_OPEN_CAPTURE_INVALID
        ));
    }

    #[test]
    fn mounted_mutation_arms_before_service_and_drops_service_lock_for_commit() {
        let root = TestRoot::new("handoff-order");
        let service = Arc::new(Mutex::new(()));
        let service_for_commit = Arc::clone(&service);
        let lock_was_free = Arc::new(AtomicBool::new(false));
        let lock_was_free_at_commit = Arc::clone(&lock_was_free);
        let observer: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            lock_was_free_at_commit.store(service_for_commit.try_lock().is_ok(), Ordering::SeqCst);
        });
        let (handoff, _store, events) =
            fake_handoff(&root, FakeCommitBehavior::Accept, Some(observer));
        let events_at_service = Arc::clone(&events);
        assert_eq!(
            handoff
                .run_captured_mutation(
                    &service,
                    "cloud-operation-1",
                    "cloud-files.create-file",
                    move |_| {
                        events_at_service
                            .lock()
                            .expect("events")
                            .push("service-entry".to_string());
                        empty_mutation_report(Ok::<_, &'static str>(7_u64))
                    },
                )
                .expect("captured mutation"),
            7
        );
        assert_eq!(
            &*events.lock().expect("events"),
            &["probe", "arm", "service-entry", "commit-applied"]
        );
        assert!(lock_was_free.load(Ordering::SeqCst));
    }

    #[test]
    fn mounted_read_ingest_commits_inner_error_without_gating() {
        let root = TestRoot::new("read-ingest-inner-error");
        let service = Arc::new(Mutex::new(0_u64));
        let (handoff, store, events) = fake_handoff(&root, FakeCommitBehavior::Accept, None);
        let events_at_service = Arc::clone(&events);

        let outcome = handoff
            .run_captured_read_ingest(
                &service,
                "cloud-files.test-directory-refresh",
                move |service| {
                    *service += 1;
                    events_at_service
                        .lock()
                        .expect("events")
                        .push("service-entry".to_string());
                    empty_mutation_report(Err::<(), _>("directory disappeared"))
                },
            )
            .expect("read handoff itself succeeds");

        assert_eq!(outcome, Err("directory disappeared"));
        assert_eq!(*service.lock().expect("service"), 1);
        assert_eq!(
            &*events.lock().expect("events"),
            &["probe", "arm", "service-entry", "commit-applied"]
        );
        assert!(
            store
                .pending_marker_ids()
                .expect("pending marker IDs")
                .is_empty(),
            "acknowledged inner read errors leave no recovery marker"
        );
        handoff
            .ensure_ready()
            .expect("ordinary inner read error does not gate the mount");
    }

    #[test]
    fn mounted_read_ingest_commit_failure_gates_later_service_entry() {
        for (index, behavior) in [
            FakeCommitBehavior::FailBeforeAccept,
            FakeCommitBehavior::LoseResponseAfterAccept,
        ]
        .into_iter()
        .enumerate()
        {
            let root = TestRoot::new(&format!("read-ingest-commit-fence-{index}"));
            let service = Arc::new(Mutex::new(0_u64));
            let (handoff, _store, _events) = fake_handoff(&root, behavior, None);

            assert!(
                handoff
                    .run_captured_read_ingest(
                        &service,
                        "cloud-files.test-directory-refresh",
                        |service| {
                            *service += 1;
                            empty_mutation_report(Ok::<_, &'static str>(()))
                        },
                    )
                    .is_err()
            );
            assert!(
                handoff
                    .run_captured_read_ingest(
                        &service,
                        "cloud-files.test-directory-refresh",
                        |service| {
                            *service += 1;
                            empty_mutation_report(Ok::<_, &'static str>(()))
                        },
                    )
                    .is_err()
            );
            assert_eq!(
                *service.lock().expect("service"),
                1,
                "a failed handoff fences the later read before service entry"
            );
            assert!(handoff.ensure_ready().is_err());
        }
    }

    #[test]
    fn mounted_mutation_lane_blocks_later_service_entry_until_first_ack() {
        let root = TestRoot::new("handoff-lane");
        let service = Arc::new(Mutex::new(0_u64));
        let observer_calls = Arc::new(AtomicUsize::new(0));
        let first_commit_started = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let release_first_commit = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let observer_started = Arc::clone(&first_commit_started);
        let observer_release = Arc::clone(&release_first_commit);
        let observer_calls_inner = Arc::clone(&observer_calls);
        let observer: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            if observer_calls_inner.fetch_add(1, Ordering::SeqCst) == 0 {
                let (started_lock, started_wake) = &*observer_started;
                *started_lock.lock().expect("started lock") = true;
                started_wake.notify_all();
                let (release_lock, release_wake) = &*observer_release;
                let mut released = release_lock.lock().expect("release lock");
                while !*released {
                    released = release_wake.wait(released).expect("release wait");
                }
            }
        });
        let (handoff, _store, _events) =
            fake_handoff(&root, FakeCommitBehavior::Accept, Some(observer));

        let first_handoff = Arc::clone(&handoff);
        let first_service = Arc::clone(&service);
        let first = thread::spawn(move || {
            first_handoff.run_captured_mutation(
                &first_service,
                "cloud-operation-a",
                "cloud-files.create-file",
                |service| {
                    *service += 1;
                    empty_mutation_report(Ok::<_, &'static str>(()))
                },
            )
        });
        let (started_lock, started_wake) = &*first_commit_started;
        let mut started = started_lock.lock().expect("started lock");
        while !*started {
            started = started_wake.wait(started).expect("started wait");
        }
        drop(started);

        let second_handoff = Arc::clone(&handoff);
        let second_service = Arc::clone(&service);
        let second = thread::spawn(move || {
            second_handoff.run_captured_mutation(
                &second_service,
                "cloud-operation-b",
                "cloud-files.rename",
                |service| {
                    *service += 1;
                    empty_mutation_report(Ok::<_, &'static str>(()))
                },
            )
        });
        thread::sleep(Duration::from_millis(50));
        assert_eq!(*service.lock().expect("service"), 1);
        let (release_lock, release_wake) = &*release_first_commit;
        *release_lock.lock().expect("release lock") = true;
        release_wake.notify_all();
        first.join().expect("first join").expect("first mutation");
        second
            .join()
            .expect("second join")
            .expect("second mutation");
        assert_eq!(*service.lock().expect("service"), 2);
    }

    #[test]
    fn failed_or_lost_commit_gates_every_later_mutation() {
        for (index, behavior) in [
            FakeCommitBehavior::FailBeforeAccept,
            FakeCommitBehavior::LoseResponseAfterAccept,
        ]
        .into_iter()
        .enumerate()
        {
            let root = TestRoot::new(&format!("handoff-commit-fence-{index}"));
            let service = Arc::new(Mutex::new(0_u64));
            let (handoff, _store, _events) = fake_handoff(&root, behavior, None);
            assert!(
                handoff
                    .run_captured_mutation(
                        &service,
                        "cloud-operation-a",
                        "cloud-files.create-file",
                        |service| {
                            *service += 1;
                            empty_mutation_report(Ok::<_, &'static str>(()))
                        },
                    )
                    .is_err()
            );
            assert!(
                handoff
                    .run_captured_mutation(
                        &service,
                        "cloud-operation-b",
                        "cloud-files.rename",
                        |service| {
                            *service += 1;
                            empty_mutation_report(Ok::<_, &'static str>(()))
                        },
                    )
                    .is_err()
            );
            assert_eq!(*service.lock().expect("service"), 1);
            assert!(handoff.ensure_ready().is_err());
        }
    }

    #[test]
    fn failed_or_lost_commit_cannot_advance_queue_or_local_completion() {
        for (index, behavior) in [
            FakeCommitBehavior::FailBeforeAccept,
            FakeCommitBehavior::LoseResponseAfterAccept,
        ]
        .into_iter()
        .enumerate()
        {
            let root = TestRoot::new(&format!("handoff-queue-fence-{index}"));
            let (handoff, _store, _events) = fake_handoff(&root, behavior, None);
            let mut state =
                CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
                    .expect("writable state");
            let operation = state
                .enqueue_capture(create_capture(
                    &format!("commit-failure-capture-{index}"),
                    b"durable bytes",
                ))
                .expect("enqueue operation");
            let operation_id = operation.request.operation_id.clone();
            let mut adapter = HandoffBackedTestAdapter {
                handoff,
                service: Arc::new(Mutex::new(0)),
            };
            let mut effects = FakeEffects::default();
            let progress = crate::sync_executor::execute_cloud_files_operation(
                &mut state.queue,
                &operation_id,
                &mut adapter,
                &mut effects,
            )
            .expect("executor durably retains ambiguity");
            assert!(matches!(
                progress,
                CloudFilesSyncExecutionProgress::AwaitingReconciliation { .. }
            ));
            assert_eq!(
                state
                    .queue()
                    .operation(&operation_id)
                    .expect("retained operation")
                    .status,
                CloudFilesLocalOperationStatus::Retrying
            );
            assert_eq!(effects.completions, 0);
            assert!(adapter.handoff.ensure_ready().is_err());
        }
    }

    #[test]
    fn host_session_close_waits_for_mutation_ack_and_lane_release() {
        let root = TestRoot::new("handoff-close-order");
        let service = Arc::new(Mutex::new(()));
        let commit_started = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let release_commit = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let observer_started = Arc::clone(&commit_started);
        let observer_release = Arc::clone(&release_commit);
        let observer: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            let (started_lock, started_wake) = &*observer_started;
            *started_lock.lock().expect("started lock") = true;
            started_wake.notify_all();
            let (release_lock, release_wake) = &*observer_release;
            let mut released = release_lock.lock().expect("release lock");
            while !*released {
                released = release_wake.wait(released).expect("release wait");
            }
        });
        let (handoff, store, _events) =
            fake_handoff(&root, FakeCommitBehavior::Accept, Some(observer));
        let mutation_handoff = Arc::clone(&handoff);
        let mutation_service = Arc::clone(&service);
        let mutation = thread::spawn(move || {
            mutation_handoff.run_captured_mutation(
                &mutation_service,
                "cloud-operation-before-close",
                "cloud-files.create-directory",
                |_| empty_mutation_report(Ok::<_, &'static str>(())),
            )
        });
        let (started_lock, started_wake) = &*commit_started;
        let mut started = started_lock.lock().expect("started lock");
        while !*started {
            started = started_wake.wait(started).expect("started wait");
        }
        drop(started);

        let close_handoff = Arc::clone(&handoff);
        let (closed_sender, closed_receiver) = mpsc::channel();
        let close = thread::spawn(move || {
            let result = close_handoff.close_clean();
            closed_sender.send(()).expect("close signal");
            result
        });
        assert!(matches!(
            closed_receiver.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        let (release_lock, release_wake) = &*release_commit;
        *release_lock.lock().expect("release lock") = true;
        release_wake.notify_all();
        mutation
            .join()
            .expect("mutation join")
            .expect("mutation acknowledged");
        close.join().expect("close join").expect("clean close");
        closed_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("close completed after mutation");
        let replacement = store
            .open_host_mount_session(
                dfs::MountedSifsMutationAdapter::CloudFiles,
                "cloud-files:replacement-after-clean-close",
            )
            .expect("clean close released session liveness");
        replacement.close_clean().expect("close replacement");
    }

    #[test]
    fn unclassified_service_error_is_unknown_and_requires_full_recovery() {
        let root = TestRoot::new("handoff-service-error");
        let service = Arc::new(Mutex::new(0_u64));
        let (handoff, store, events) = fake_handoff(&root, FakeCommitBehavior::Accept, None);
        let error = handoff
            .run_captured_mutation(
                &service,
                "cloud-operation-error",
                "cloud-files.rmdir",
                |service| {
                    *service += 1;
                    empty_mutation_report(Err::<(), _>("service returned conflict"))
                },
            )
            .expect_err("service Err is never accepted as a conflict/no-write fact");
        assert!(error.contains("after service entry"));
        assert_eq!(
            &*events.lock().expect("events"),
            &["probe", "arm", "commit-unknown"]
        );
        let pending = store.pending_marker_ids().expect("pending marker IDs");
        assert_eq!(pending.len(), 1);
        let marker = store
            .marker(&pending[0])
            .expect("read marker")
            .expect("marker exists");
        assert!(matches!(
            marker.phase,
            dfs::MountedSifsMutationOutboxPhase::DaemonAccepted {
                requires_full_recovery: true,
                error_observation: Some(_),
                ..
            }
        ));
        let gated = handoff
            .ensure_ready()
            .expect_err("service error leaves the original ambiguity gated");
        assert!(gated.contains("service returned conflict"), "{gated}");
    }

    #[test]
    fn definitive_namespace_conflict_commits_capture_without_gating_handoff() {
        let root = TestRoot::new("handoff-definitive-namespace-conflict");
        let service = Arc::new(Mutex::new(()));
        let (handoff, store, events) = fake_handoff(&root, FakeCommitBehavior::Accept, None);
        let outcome = handoff
            .run_captured_mutation(
                &service,
                "cloud-operation-directory-not-empty",
                "cloud-files.rmdir",
                |_| {
                    classify_definitive_namespace_mutation_report(empty_mutation_report(
                        Err::<(), _>(dfs::DfsPosixError::DirectoryNotEmpty {
                            path: "/services/non-empty".to_string(),
                        }),
                    ))
                },
            )
            .expect("typed namespace conflict has a definitive handoff outcome")
            .expect_err("directory-not-empty remains a user-visible conflict");
        assert_eq!(outcome.code, "namespace-directory-not-empty");
        assert!(outcome.detail.contains("directory is not empty"));
        assert_eq!(
            &*events.lock().expect("events"),
            &["probe", "arm", "commit-applied"]
        );
        assert!(
            store
                .pending_marker_ids()
                .expect("pending marker IDs")
                .is_empty()
        );
        handoff
            .ensure_ready()
            .expect("definitive conflict leaves later mutations admitted");
    }

    #[test]
    fn mounted_mutation_labels_cover_every_writable_cloud_operation() {
        assert_eq!(
            [
                CloudFilesLocalOperationKind::CreateFile,
                CloudFilesLocalOperationKind::CreateDirectory,
                CloudFilesLocalOperationKind::ReplaceFile,
                CloudFilesLocalOperationKind::DeleteFile,
                CloudFilesLocalOperationKind::DeleteDirectory,
                CloudFilesLocalOperationKind::Rename,
            ]
            .map(|kind| cloud_files_mounted_mutation_label(&kind)),
            [
                "cloud-files.create-file",
                "cloud-files.create-directory",
                "cloud-files.replace-file",
                "cloud-files.unlink",
                "cloud-files.rmdir",
                "cloud-files.rename",
            ]
        );
    }

    fn expected_object() -> CloudFilesExpectedObjectState {
        CloudFilesExpectedObjectState {
            object_id: "01oldobject".to_string(),
            version: "01oldversion".to_string(),
            content_hash: Some("blake3:old".to_string()),
        }
    }

    fn create_capture(capture_id: &str, bytes: &[u8]) -> CloudFilesCapturedLocalChange {
        CloudFilesCapturedLocalChange {
            capture_id: capture_id.to_string(),
            kind: CloudFilesLocalOperationKind::CreateFile,
            source_path: "/services/new.txt".to_string(),
            destination_path: None,
            preconditions: CloudFilesLocalOperationPreconditions {
                source: None,
                destination: CloudFilesDestinationPrecondition::Absent,
            },
            captured_content: Some(CloudFilesCapturedContent::Inline(bytes.to_vec())),
        }
    }

    fn cursor(sequence: u64) -> CloudFilesNamespaceCursor {
        CloudFilesNamespaceCursor {
            file_system_id: "filesystem-a".to_string(),
            shard_id: "root".to_string(),
            authority_epoch: 7,
            after_sequence: sequence,
        }
    }

    fn feed_upsert(sequence: u64, ordinal: u64, name: &str) -> CloudFilesNamespaceFeedEvent {
        CloudFilesNamespaceFeedEvent::Upsert {
            path: format!("/services/{name}"),
            object_id: format!("object-{sequence}-{ordinal}"),
            version: format!("version-{sequence}-{ordinal}"),
            content_hash: Some(format!("blake3:{sequence}-{ordinal}")),
            content_size_bytes: Some(1),
            kind: "file".to_string(),
            provenance: CloudFilesNamespaceProvenance {
                event_id: format!("event-{sequence}-{ordinal}"),
                journal_sequence: sequence,
                journal_ordinal: ordinal,
                operation_id: None,
            },
        }
    }

    #[test]
    fn feed_budget_never_splits_one_journal_sequence() {
        let batch = CloudFilesNamespaceFeedBatch {
            input_cursor: cursor(0),
            next_cursor: cursor(3),
            events: vec![
                feed_upsert(1, 0, "a"),
                feed_upsert(1, 1, "b"),
                feed_upsert(2, 0, "c"),
                feed_upsert(3, 0, "d"),
            ],
            journal_head_sequence: 7,
            has_more: false,
        };

        let first = bound_namespace_feed_batch(batch.clone(), 1);
        assert_eq!(first.events.len(), 2);
        assert_eq!(first.next_cursor.after_sequence, 1);
        assert!(first.has_more);

        let second = bound_namespace_feed_batch(batch, 3);
        assert_eq!(second.events.len(), 3);
        assert_eq!(second.next_cursor.after_sequence, 2);
        assert!(second.has_more);
    }

    fn snapshot(sequence: u64) -> CloudFilesFullScanSnapshot {
        let cursor = cursor(sequence);
        CloudFilesFullScanSnapshot {
            manifest: BTreeMap::from([(
                "/services/remote.txt".to_string(),
                CloudFilesReconciledManifestEntry {
                    path: "/services/remote.txt".to_string(),
                    object_id: "01remoteobject".to_string(),
                    version: format!("01version{sequence}"),
                    content_hash: Some(format!("blake3:{sequence}")),
                    content_size_bytes: Some(sequence),
                    kind: "file".to_string(),
                },
            )]),
            removed_paths: Vec::new(),
            materialized_directories: Vec::new(),
            scan_complete: true,
            cursors: BTreeMap::from([(cursor.shard_key().expect("key"), cursor)]),
        }
    }

    #[derive(Default)]
    struct FakeCapture {
        batches: VecDeque<Result<CloudFilesCapturedChangeBatch, String>>,
        activated: Arc<AtomicBool>,
        startup_scan_calls: Arc<AtomicUsize>,
        drain_calls: Arc<AtomicUsize>,
        recovery_audit_calls: Arc<AtomicUsize>,
        recovery_audit_error: Option<String>,
        recovery_observation_epoch: Arc<AtomicU64>,
        recovery_audit_records_change: bool,
        require_activation_before_scan: bool,
        startup_batches: VecDeque<CloudFilesStartupScanBatch>,
        startup_captures: Vec<CloudFilesCapturedLocalChange>,
        startup_conflicts: Vec<CloudFilesStartupConflictEvidence>,
    }

    impl CloudFilesLocalChangeAccelerator for FakeCapture {
        fn arm_cursor_recovery_observer(&mut self) -> Result<u64, String> {
            let epoch = self.recovery_observation_epoch.load(Ordering::SeqCst);
            self.activate()?;
            Ok(epoch)
        }

        fn cursor_recovery_observation_epoch(&self) -> Result<u64, String> {
            Ok(self.recovery_observation_epoch.load(Ordering::SeqCst))
        }

        fn audit_cursor_recovery_root(
            &mut self,
            _mount: &str,
            _root: &str,
            _remote_manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
            _cancelled: &AtomicBool,
        ) -> Result<(), String> {
            self.recovery_audit_calls.fetch_add(1, Ordering::SeqCst);
            if self.recovery_audit_records_change {
                self.recovery_observation_epoch
                    .fetch_add(1, Ordering::SeqCst);
            }
            self.recovery_audit_error.clone().map_or(Ok(()), Err)
        }

        fn startup_scan(
            &mut self,
            _previous_manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
            _remote_manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
            _cancelled: &AtomicBool,
        ) -> Result<CloudFilesStartupScanBatch, String> {
            self.startup_scan_calls.fetch_add(1, Ordering::SeqCst);
            if self.require_activation_before_scan && !self.activated.load(Ordering::SeqCst) {
                return Err("startup scan ran before change capture was active".to_string());
            }
            if let Some(batch) = self.startup_batches.pop_front() {
                return Ok(batch);
            }
            Ok(CloudFilesStartupScanBatch {
                changes: std::mem::take(&mut self.startup_captures),
                sparse_placeholders: Vec::new(),
                complete: true,
            })
        }

        fn drain_startup_conflicts(&mut self) -> Vec<CloudFilesStartupConflictEvidence> {
            std::mem::take(&mut self.startup_conflicts)
        }

        fn activate(&mut self) -> Result<(), String> {
            self.activated.store(true, Ordering::SeqCst);
            Ok(())
        }

        fn drain_changes(
            &mut self,
            _manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
            _cancelled: &AtomicBool,
        ) -> Result<CloudFilesCapturedChangeBatch, String> {
            self.drain_calls.fetch_add(1, Ordering::SeqCst);
            self.batches.pop_front().unwrap_or_else(|| {
                Ok(CloudFilesCapturedChangeBatch {
                    changes: Vec::new(),
                    sparse_placeholders: Vec::new(),
                    overflowed: false,
                })
            })
        }
    }

    struct FakeBackend {
        readiness_error: Option<String>,
        started_calls: usize,
        calls: Vec<CloudFilesLocalOperationKind>,
        call_count: Arc<AtomicUsize>,
        reconcile_calls: usize,
        outcome: CloudFilesDfsMutationOutcome,
        reconciliation: CloudFilesDfsMutationReconciliation,
    }

    impl Default for FakeBackend {
        fn default() -> Self {
            Self {
                readiness_error: None,
                started_calls: 0,
                calls: Vec::new(),
                call_count: Arc::new(AtomicUsize::new(0)),
                reconcile_calls: 0,
                outcome: CloudFilesDfsMutationOutcome::Ambiguous {
                    detail: "test outcome".to_string(),
                },
                reconciliation: CloudFilesDfsMutationReconciliation::Ambiguous {
                    detail: "test reconciliation".to_string(),
                },
            }
        }
    }

    impl FakeBackend {
        fn record(
            &mut self,
            kind: CloudFilesLocalOperationKind,
        ) -> Result<CloudFilesDfsMutationOutcome, String> {
            self.calls.push(kind);
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Ok(self.outcome.clone())
        }
    }

    impl CloudFilesStableMutationBackend for FakeBackend {
        fn ensure_contracts_ready(&self) -> Result<(), String> {
            self.readiness_error.clone().map_or(Ok(()), Err)
        }

        fn operation_started(&mut self, _command: &CloudFilesDfsMutationCommand) {
            self.started_calls += 1;
        }

        fn create_file(
            &mut self,
            _command: &CloudFilesDfsMutationCommand,
        ) -> Result<CloudFilesDfsMutationOutcome, String> {
            self.record(CloudFilesLocalOperationKind::CreateFile)
        }

        fn create_directory(
            &mut self,
            _command: &CloudFilesDfsMutationCommand,
        ) -> Result<CloudFilesDfsMutationOutcome, String> {
            self.record(CloudFilesLocalOperationKind::CreateDirectory)
        }

        fn replace_file(
            &mut self,
            _command: &CloudFilesDfsMutationCommand,
        ) -> Result<CloudFilesDfsMutationOutcome, String> {
            self.record(CloudFilesLocalOperationKind::ReplaceFile)
        }

        fn unlink(
            &mut self,
            _command: &CloudFilesDfsMutationCommand,
        ) -> Result<CloudFilesDfsMutationOutcome, String> {
            self.record(CloudFilesLocalOperationKind::DeleteFile)
        }

        fn rmdir(
            &mut self,
            _command: &CloudFilesDfsMutationCommand,
        ) -> Result<CloudFilesDfsMutationOutcome, String> {
            self.record(CloudFilesLocalOperationKind::DeleteDirectory)
        }

        fn rename(
            &mut self,
            _command: &CloudFilesDfsMutationCommand,
        ) -> Result<CloudFilesDfsMutationOutcome, String> {
            self.record(CloudFilesLocalOperationKind::Rename)
        }

        fn reconcile(
            &mut self,
            _command: &CloudFilesDfsMutationCommand,
        ) -> Result<CloudFilesDfsMutationReconciliation, String> {
            self.reconcile_calls += 1;
            Ok(self.reconciliation.clone())
        }
    }

    struct FakeNamespace {
        scans: VecDeque<Result<CloudFilesFullScanSnapshot, String>>,
        batches: VecDeque<Result<Vec<CloudFilesNamespaceFeedBatch>, String>>,
        scan_calls: Arc<AtomicUsize>,
        poll_calls: Arc<AtomicUsize>,
        resolved: BTreeMap<String, CloudFilesReconciledManifestEntry>,
        materialization_audit_intents:
            Option<Arc<Mutex<CloudFilesMaterializationAuditIntentStore>>>,
    }

    impl FakeNamespace {
        fn with_scans(scans: impl IntoIterator<Item = CloudFilesFullScanSnapshot>) -> Self {
            Self {
                scans: scans.into_iter().map(Ok).collect(),
                batches: VecDeque::new(),
                scan_calls: Arc::new(AtomicUsize::new(0)),
                poll_calls: Arc::new(AtomicUsize::new(0)),
                resolved: BTreeMap::new(),
                materialization_audit_intents: None,
            }
        }

        fn with_materialization_audit_intents(
            scans: impl IntoIterator<Item = CloudFilesFullScanSnapshot>,
            intents: Arc<Mutex<CloudFilesMaterializationAuditIntentStore>>,
        ) -> Self {
            Self {
                materialization_audit_intents: Some(intents),
                ..Self::with_scans(scans)
            }
        }
    }

    impl CloudFilesNamespaceSource for FakeNamespace {
        fn poll_batches(
            &mut self,
            _root: &str,
            _cursors: &[CloudFilesNamespaceCursor],
        ) -> Result<Vec<CloudFilesNamespaceFeedBatch>, String> {
            self.poll_calls.fetch_add(1, Ordering::SeqCst);
            self.batches.pop_front().unwrap_or_else(|| Ok(Vec::new()))
        }

        fn full_scan(
            &mut self,
            _root: &str,
            _previous_cursors: &[CloudFilesNamespaceCursor],
            _materialized_manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
            _materialized_directories: &std::collections::BTreeSet<String>,
        ) -> Result<CloudFilesFullScanSnapshot, String> {
            self.scan_calls.fetch_add(1, Ordering::Relaxed);
            self.scans
                .pop_front()
                .expect("configured full-scan snapshot")
        }

        fn resolve_placeholder_identity(
            &mut self,
            identity: &CloudFilesPlaceholderIdentity,
        ) -> Result<Option<CloudFilesReconciledManifestEntry>, String> {
            Ok(self.resolved.get(&identity.path).cloned())
        }

        fn has_pending_materialized_directories(
            &self,
            _durable_materialized_directories: &BTreeSet<String>,
        ) -> Result<bool, String> {
            let Some(intents) = &self.materialization_audit_intents else {
                return Ok(false);
            };
            let intents = intents
                .lock()
                .map_err(|_| "fake materialization audit store is poisoned".to_string())?;
            Ok(!intents.pending_directories().is_empty())
        }

        fn acknowledge_materialized_directories(
            &mut self,
            directories: &[String],
        ) -> Result<(), String> {
            let Some(intents) = &self.materialization_audit_intents else {
                return Ok(());
            };
            intents
                .lock()
                .map_err(|_| "fake materialization audit store is poisoned".to_string())?
                .acknowledge_directories(directories.iter().map(String::as_str))
                .map_err(|error| error.to_string())
        }

        fn acknowledge_materialized_directories_after_cursor_recovery_root_audit(
            &mut self,
        ) -> Result<(), String> {
            let Some(intents) = &self.materialization_audit_intents else {
                return Ok(());
            };
            let mut intents = intents
                .lock()
                .map_err(|_| "fake materialization audit store is poisoned".to_string())?;
            let audited = intents
                .pending_directories()
                .iter()
                .cloned()
                .collect::<Vec<_>>();
            intents
                .acknowledge_directories(audited.iter().map(String::as_str))
                .map_err(|error| error.to_string())
        }
    }

    #[derive(Default)]
    struct FakeEffects {
        path_states: BTreeMap<String, CloudFilesLocalPathState>,
        actions: Vec<CloudFilesNamespaceAction>,
        completions: usize,
        full_scan_effects: Arc<Mutex<Vec<CloudFilesFullScanSnapshot>>>,
        fail_full_scan: bool,
        apply_results: VecDeque<Result<(), String>>,
    }

    impl CloudFilesNamespaceEffectAdapter for FakeEffects {
        fn local_path_state(&mut self, path: &str) -> Result<CloudFilesLocalPathState, String> {
            Ok(self
                .path_states
                .get(path)
                .cloned()
                .unwrap_or(CloudFilesLocalPathState::Clean))
        }

        fn apply(&mut self, action: &CloudFilesNamespaceAction) -> Result<(), String> {
            self.actions.push(action.clone());
            self.apply_results.pop_front().unwrap_or(Ok(()))
        }

        fn record_conflict(
            &mut self,
            _conflict: &CloudFilesNamespaceConflict,
        ) -> Result<(), String> {
            Ok(())
        }

        fn request_full_scan(
            &mut self,
            _request: &CloudFilesFullScanRequest,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    impl CloudFilesLocalCompletionEffect for FakeEffects {
        fn complete(
            &mut self,
            _command: &CloudFilesDfsMutationCommand,
            _evidence: &CloudFilesRemoteMutationEvidence,
        ) -> Result<CloudFilesLocalCompletionOutcome, String> {
            self.completions += 1;
            Ok(CloudFilesLocalCompletionOutcome::Completed)
        }
    }

    impl CloudFilesWindowsPlaceholderEffect for FakeEffects {
        fn apply_full_scan(&mut self, snapshot: &CloudFilesFullScanSnapshot) -> Result<(), String> {
            self.full_scan_effects
                .lock()
                .expect("full-scan log")
                .push(snapshot.clone());
            if self.fail_full_scan {
                Err("simulated crash after idempotent placeholder effect".to_string())
            } else {
                Ok(())
            }
        }
    }

    fn cursor_recovery_snapshot(sequence: u64, scan_complete: bool) -> CloudFilesFullScanSnapshot {
        let mut snapshot = snapshot(sequence);
        snapshot.materialized_directories = vec!["/services".to_string()];
        snapshot.scan_complete = scan_complete;
        if !scan_complete {
            snapshot.cursors.clear();
        }
        snapshot
    }

    #[test]
    fn authorized_cursor_recovery_gates_all_local_writeback_until_terminal_release() {
        let root = TestRoot::new("cursor-recovery-gate");
        let mut state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("state");
        state.authorize_cursor_recovery_for_test();

        let activated = Arc::new(AtomicBool::new(false));
        let startup_scan_calls = Arc::new(AtomicUsize::new(0));
        let drain_calls = Arc::new(AtomicUsize::new(0));
        let recovery_audit_calls = Arc::new(AtomicUsize::new(0));
        let capture = FakeCapture {
            activated: Arc::clone(&activated),
            startup_scan_calls: Arc::clone(&startup_scan_calls),
            drain_calls: Arc::clone(&drain_calls),
            recovery_audit_calls: Arc::clone(&recovery_audit_calls),
            ..FakeCapture::default()
        };
        let backend = FakeBackend::default();
        let mutation_calls = Arc::clone(&backend.call_count);
        let namespace = FakeNamespace::with_scans([
            cursor_recovery_snapshot(1, false),
            cursor_recovery_snapshot(2, true),
        ]);
        let scan_calls = Arc::clone(&namespace.scan_calls);
        let poll_calls = Arc::clone(&namespace.poll_calls);
        let effects = FakeEffects::default();
        let effect_calls = Arc::clone(&effects.full_scan_effects);
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            capture,
            CloudFilesConcreteDfsAdapter::new(backend),
            namespace,
            effects,
        );
        let control = worker.cursor_recovery_control().expect("recovery control");
        assert_eq!(control.release(), Err("cursor recovery is not terminal"));
        assert!(control.should_run_immediately());

        let cancelled = AtomicBool::new(false);
        let first = worker.startup(&cancelled).expect("partial recovery scan");
        assert!(first.full_scan_applied);
        assert!(!first.cursor_recovery_terminal);
        assert_eq!(
            worker.state().status().mode,
            CloudFilesLocalChangeSyncMode::Starting
        );
        assert!(!worker.state().is_ready());
        assert!(!activated.load(Ordering::SeqCst));
        assert_eq!(startup_scan_calls.load(Ordering::SeqCst), 0);
        assert_eq!(drain_calls.load(Ordering::SeqCst), 0);
        assert_eq!(mutation_calls.load(Ordering::SeqCst), 0);
        assert_eq!(poll_calls.load(Ordering::SeqCst), 0);

        let terminal = worker.run_once(&cancelled).expect("terminal recovery scan");
        assert!(terminal.full_scan_applied);
        assert!(terminal.cursor_recovery_terminal);
        assert!(control.terminal());
        assert!(!control.should_run_immediately());
        assert_eq!(recovery_audit_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            worker.state().status().mode,
            CloudFilesLocalChangeSyncMode::Starting
        );
        assert!(!worker.state().is_ready());
        assert!(activated.load(Ordering::SeqCst));
        assert_eq!(startup_scan_calls.load(Ordering::SeqCst), 0);
        assert_eq!(drain_calls.load(Ordering::SeqCst), 0);
        assert_eq!(mutation_calls.load(Ordering::SeqCst), 0);
        assert_eq!(poll_calls.load(Ordering::SeqCst), 0);

        let parked = worker.run_once(&cancelled).expect("parked terminal pass");
        assert!(parked.cursor_recovery_terminal);
        assert!(!parked.full_scan_applied);
        assert_eq!(scan_calls.load(Ordering::SeqCst), 2);
        assert_eq!(effect_calls.lock().expect("effects").len(), 2);
        assert_eq!(recovery_audit_calls.load(Ordering::SeqCst), 1);
        assert_eq!(startup_scan_calls.load(Ordering::SeqCst), 0);
        assert_eq!(drain_calls.load(Ordering::SeqCst), 0);
        assert_eq!(mutation_calls.load(Ordering::SeqCst), 0);
        assert_eq!(poll_calls.load(Ordering::SeqCst), 0);

        control.release().expect("durable owner release");
        assert!(control.should_run_immediately());
        let released = worker.run_once(&cancelled).expect("released startup pass");
        assert!(!released.cursor_recovery_terminal);
        assert!(worker.state().is_ready());
        assert_eq!(
            worker.state().status().mode,
            CloudFilesLocalChangeSyncMode::Active
        );
        assert!(activated.load(Ordering::SeqCst));
        assert_eq!(startup_scan_calls.load(Ordering::SeqCst), 0);
        assert_eq!(drain_calls.load(Ordering::SeqCst), 0);
        assert_eq!(mutation_calls.load(Ordering::SeqCst), 0);
        assert_eq!(poll_calls.load(Ordering::SeqCst), 0);
        assert!(!control.should_run_immediately());

        let still_parked = worker
            .run_once(&cancelled)
            .expect("Active remains parked before durable host acknowledgement");
        assert_eq!(still_parked, CloudFilesWritableSyncProgress::default());
        assert_eq!(startup_scan_calls.load(Ordering::SeqCst), 0);
        assert_eq!(drain_calls.load(Ordering::SeqCst), 0);
        assert_eq!(mutation_calls.load(Ordering::SeqCst), 0);
        control
            .complete_after_host_active()
            .expect("durable host ActiveObserved completion");

        worker
            .run_once(&cancelled)
            .expect("post-Ready local classification pass");
        assert_eq!(startup_scan_calls.load(Ordering::SeqCst), 1);
        assert_eq!(drain_calls.load(Ordering::SeqCst), 0);
        assert_eq!(mutation_calls.load(Ordering::SeqCst), 0);
        assert_eq!(poll_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn empty_local_wake_does_not_poll_remote_namespace() {
        let root = TestRoot::new("empty-local-wake");
        let state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("open state");
        let namespace = FakeNamespace::with_scans([snapshot(1)]);
        let poll_calls = Arc::clone(&namespace.poll_calls);
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            FakeCapture::default(),
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            namespace,
            FakeEffects::default(),
        );
        let cancelled = AtomicBool::new(false);
        worker.startup(&cancelled).expect("startup");
        let polls_after_startup = poll_calls.load(Ordering::SeqCst);

        let progress = worker
            .run_once_for_wake(&cancelled, false)
            .expect("empty local wake");

        assert_eq!(progress, CloudFilesWritableSyncProgress::default());
        assert_eq!(poll_calls.load(Ordering::SeqCst), polls_after_startup);
    }

    #[test]
    fn authorized_cursor_recovery_accepts_a_retained_sparse_directory() {
        let root = TestRoot::new("cursor-recovery-retained-sparse-directory");
        let mut state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("state");
        let directory = CloudFilesReconciledManifestEntry {
            path: "/services/materialized".to_string(),
            object_id: "materialized-object".to_string(),
            version: "materialized-version".to_string(),
            content_hash: None,
            content_size_bytes: None,
            kind: "directory".to_string(),
        };
        state
            .reconciler
            .install_full_scan_delta(
                BTreeMap::from([(directory.path.clone(), directory.clone())]),
                Vec::new(),
                vec![directory.path.clone()],
                BTreeMap::new(),
                false,
            )
            .expect("seed retained sparse directory");
        state.authorize_cursor_recovery_for_test();
        let mut terminal = cursor_recovery_snapshot(1, true);
        terminal
            .manifest
            .insert(directory.path.clone(), directory.clone());
        terminal
            .materialized_directories
            .push(directory.path.clone());
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            FakeCapture::default(),
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            FakeNamespace::with_scans([terminal]),
            FakeEffects::default(),
        );

        let progress = worker
            .startup(&AtomicBool::new(false))
            .expect("terminal recovery retains sparse directory");
        assert!(progress.cursor_recovery_terminal);
        assert_eq!(
            worker.state().reconciler().materialized_directories(),
            &BTreeSet::from(["/services".to_string(), directory.path])
        );
    }

    #[test]
    fn authorized_cursor_recovery_reaudits_terminal_state_without_rescanning() {
        let root = TestRoot::new("cursor-recovery-terminal-before-rewrite");
        let mut state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("state");
        let terminal_cursor = cursor(9);
        state
            .reconciler
            .install_full_scan_delta(
                BTreeMap::new(),
                Vec::new(),
                vec!["/services".to_string()],
                BTreeMap::from([(
                    terminal_cursor.shard_key().expect("cursor key"),
                    terminal_cursor,
                )]),
                true,
            )
            .expect("persist terminal state before Terminal rewrite");
        state.authorize_cursor_recovery_for_test();
        let capture = FakeCapture::default();
        let audit_calls = Arc::clone(&capture.recovery_audit_calls);
        let namespace = FakeNamespace::with_scans([]);
        let scan_calls = Arc::clone(&namespace.scan_calls);
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            capture,
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            namespace,
            FakeEffects::default(),
        );

        let progress = worker
            .startup(&AtomicBool::new(false))
            .expect("re-establish terminal proof without scan");
        assert!(progress.cursor_recovery_terminal);
        assert!(!progress.full_scan_applied);
        assert_eq!(scan_calls.load(Ordering::SeqCst), 0);
        assert_eq!(audit_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn authorized_cursor_recovery_retires_post_terminal_population_intent_without_rescanning() {
        let root = TestRoot::new("cursor-recovery-post-terminal-population");
        let mut state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("state");
        let terminal_cursor = cursor(10);
        state
            .reconciler
            .install_full_scan_delta(
                BTreeMap::new(),
                Vec::new(),
                vec!["/services".to_string()],
                BTreeMap::from([(
                    terminal_cursor.shard_key().expect("cursor key"),
                    terminal_cursor,
                )]),
                true,
            )
            .expect("persist terminal state");
        state.authorize_cursor_recovery_for_test();
        let intents = state.materialization_audit_intents();
        intents
            .lock()
            .expect("audit intents")
            .record_directory("/services")
            .expect("record post-Terminal root population");
        let capture = FakeCapture::default();
        let audit_calls = Arc::clone(&capture.recovery_audit_calls);
        let namespace = FakeNamespace::with_materialization_audit_intents([], Arc::clone(&intents));
        let scan_calls = Arc::clone(&namespace.scan_calls);
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            capture,
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            namespace,
            FakeEffects::default(),
        );

        let progress = worker
            .startup(&AtomicBool::new(false))
            .expect("re-audit and retire the post-Terminal population intent");
        assert!(progress.cursor_recovery_terminal);
        assert!(!progress.full_scan_applied);
        assert_eq!(scan_calls.load(Ordering::SeqCst), 0);
        assert_eq!(audit_calls.load(Ordering::SeqCst), 1);
        assert!(
            intents
                .lock()
                .expect("audit intents")
                .pending_directories()
                .is_empty()
        );
    }

    #[test]
    fn cursor_recovery_terminal_handoff_accepts_only_reconciled_superseded_queue_records() {
        let root = TestRoot::new("cursor-recovery-superseded-handoff");
        let mut state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("state");
        let operation = state
            .enqueue_capture(create_capture("terminal-superseded", b"retained bytes"))
            .expect("enqueue retained capture");
        state.authorize_cursor_recovery_for_test();
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            FakeCapture::default(),
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            FakeNamespace::with_scans([]),
            FakeEffects::default(),
        );

        let queued = worker
            .ensure_cursor_recovery_queue_settled()
            .expect_err("queued operation must block the terminal handoff");
        assert_eq!(queued.code, "cursor-recovery-queue-not-empty");

        worker
            .state
            .queue
            .mark_in_flight(&operation.request.operation_id)
            .expect("mark in flight");
        worker
            .state
            .queue
            .mark_remote_applied_with_evidence(CloudFilesRemoteMutationEvidence {
                operation_id: operation.request.operation_id.clone(),
                object_id: Some("01object".to_string()),
                version: Some("01version".to_string()),
                content_hash: None,
                content_size_bytes: None,
                kind: Some("file".to_string()),
                detail: "authoritative terminal scan confirmed the stable request".to_string(),
            })
            .expect("mark remote applied");
        worker
            .state
            .queue
            .mark_completed(&operation.request.operation_id)
            .expect("mark completed");
        worker
            .state
            .queue
            .retire_terminal_operation(&operation.request.operation_id)
            .expect("mark superseded");

        worker
            .ensure_cursor_recovery_queue_settled()
            .expect("terminal handoff may retain the exact scan-reconciled predecessor");
    }

    #[test]
    fn authorized_cursor_recovery_rejects_terminal_without_cursor_before_effects() {
        let root = TestRoot::new("cursor-recovery-empty-terminal-cursor");
        let mut state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("state");
        state.authorize_cursor_recovery_for_test();
        let capture = FakeCapture::default();
        let recovery_audit_calls = Arc::clone(&capture.recovery_audit_calls);
        let mut invalid = cursor_recovery_snapshot(1, true);
        invalid.cursors.clear();
        let effects = FakeEffects::default();
        let effect_calls = Arc::clone(&effects.full_scan_effects);
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            capture,
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            FakeNamespace::with_scans([invalid]),
            effects,
        );

        let error = worker
            .startup(&AtomicBool::new(false))
            .expect_err("empty terminal cursor must fail closed");
        assert_eq!(error.code, "cursor-recovery-terminal-cursor-missing");
        assert!(effect_calls.lock().expect("effects").is_empty());
        assert_eq!(recovery_audit_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn authorized_cursor_recovery_never_publishes_terminal_when_root_audit_fails() {
        let root = TestRoot::new("cursor-recovery-root-audit");
        let mut state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("state");
        state.authorize_cursor_recovery_for_test();
        let capture = FakeCapture {
            recovery_audit_error: Some("local root contains offline.txt".to_string()),
            ..FakeCapture::default()
        };
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            capture,
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            FakeNamespace::with_scans([cursor_recovery_snapshot(1, true)]),
            FakeEffects::default(),
        );
        let control = worker.cursor_recovery_control().expect("recovery control");

        let error = worker
            .startup(&AtomicBool::new(false))
            .expect_err("root audit must fail closed");
        assert_eq!(error.code, "cursor-recovery-root-audit");
        assert!(!control.terminal());
        assert_eq!(control.release(), Err("cursor recovery is not terminal"));
        assert!(!worker.state().is_ready());
    }

    #[test]
    fn authorized_cursor_recovery_never_publishes_terminal_when_local_change_races_audit() {
        let root = TestRoot::new("cursor-recovery-audit-race");
        let mut state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("state");
        state.authorize_cursor_recovery_for_test();
        let activated = Arc::new(AtomicBool::new(false));
        let recovery_observation_epoch = Arc::new(AtomicU64::new(0));
        let capture = FakeCapture {
            activated: Arc::clone(&activated),
            recovery_observation_epoch: Arc::clone(&recovery_observation_epoch),
            recovery_audit_records_change: true,
            ..FakeCapture::default()
        };
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            capture,
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            FakeNamespace::with_scans([cursor_recovery_snapshot(1, true)]),
            FakeEffects::default(),
        );
        let control = worker.cursor_recovery_control().expect("recovery control");

        let error = worker
            .startup(&AtomicBool::new(false))
            .expect_err("observer barrier must catch the audit race");
        assert_eq!(error.code, "cursor-recovery-local-change-observed");
        assert!(activated.load(Ordering::SeqCst));
        assert_eq!(recovery_observation_epoch.load(Ordering::SeqCst), 1);
        assert!(!control.terminal());
        assert_eq!(control.release(), Err("cursor recovery is not terminal"));
        assert!(!worker.state().is_ready());
    }

    #[test]
    fn post_commit_ready_change_stays_buffered_until_after_release_and_active_progress() {
        let root = TestRoot::new("cursor-recovery-buffered-after-cut");
        let mut state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("state");
        state.authorize_cursor_recovery_for_test();
        let observation_epoch = Arc::new(AtomicU64::new(0));
        let startup_scan_calls = Arc::new(AtomicUsize::new(0));
        let capture = FakeCapture {
            recovery_observation_epoch: Arc::clone(&observation_epoch),
            startup_scan_calls: Arc::clone(&startup_scan_calls),
            startup_captures: vec![create_capture("post-cut-local-create", b"local")],
            ..FakeCapture::default()
        };
        let backend = FakeBackend::default();
        let mutation_calls = Arc::clone(&backend.call_count);
        let namespace = FakeNamespace::with_scans([cursor_recovery_snapshot(1, true)]);
        let poll_calls = Arc::clone(&namespace.poll_calls);
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            capture,
            CloudFilesConcreteDfsAdapter::new(backend),
            namespace,
            FakeEffects::default(),
        );
        let control = worker.cursor_recovery_control().expect("recovery control");
        let cancelled = AtomicBool::new(false);

        let terminal = worker
            .startup(&cancelled)
            .expect("commit-ready recovery cut");
        assert!(terminal.cursor_recovery_terminal);
        observation_epoch.fetch_add(1, Ordering::SeqCst);
        let parked = worker
            .run_once(&cancelled)
            .expect("post-cut event remains parked");
        assert!(parked.cursor_recovery_terminal);
        assert_eq!(startup_scan_calls.load(Ordering::SeqCst), 0);
        assert_eq!(mutation_calls.load(Ordering::SeqCst), 0);
        assert_eq!(poll_calls.load(Ordering::SeqCst), 0);

        control.release().expect("durable Ready owner release");
        worker
            .run_once(&cancelled)
            .expect("publish Active without classification");
        assert!(worker.state().is_ready());
        assert_eq!(startup_scan_calls.load(Ordering::SeqCst), 0);
        assert_eq!(mutation_calls.load(Ordering::SeqCst), 0);
        assert_eq!(poll_calls.load(Ordering::SeqCst), 0);
        control
            .complete_after_host_active()
            .expect("durable host ActiveObserved completion");

        let post_ready = worker
            .run_once(&cancelled)
            .expect("classify buffered local change after Active");
        assert_eq!(post_ready.captures_enqueued, 1);
        assert_eq!(startup_scan_calls.load(Ordering::SeqCst), 1);
        assert_eq!(mutation_calls.load(Ordering::SeqCst), 1);
        assert_eq!(poll_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn crash_after_commit_ready_reaudits_local_evidence_before_any_restart_upload() {
        let root = TestRoot::new("cursor-recovery-crash-after-cut");
        let mut state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("state");
        state.authorize_cursor_recovery_for_test();
        let observation_epoch = Arc::new(AtomicU64::new(0));
        let mut first = CloudFilesWritableSyncWorker::new(
            state,
            FakeCapture {
                recovery_observation_epoch: Arc::clone(&observation_epoch),
                ..FakeCapture::default()
            },
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            FakeNamespace::with_scans([cursor_recovery_snapshot(1, true)]),
            FakeEffects::default(),
        );
        let cancelled = AtomicBool::new(false);
        assert!(
            first
                .startup(&cancelled)
                .expect("first commit-ready cut")
                .cursor_recovery_terminal
        );
        // The watcher observed a local mutation after commit-ready, but the
        // process dies before the owner can durably publish Ready or release.
        observation_epoch.fetch_add(1, Ordering::SeqCst);
        drop(first);

        let mut reopened =
            CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
                .expect("reopen recovery state after crash");
        reopened.authorize_cursor_recovery_for_test();
        let startup_scan_calls = Arc::new(AtomicUsize::new(0));
        let capture = FakeCapture {
            startup_scan_calls: Arc::clone(&startup_scan_calls),
            recovery_audit_error: Some("restart audit found post-cut local evidence".to_string()),
            ..FakeCapture::default()
        };
        let backend = FakeBackend::default();
        let mutation_calls = Arc::clone(&backend.call_count);
        let mut restarted = CloudFilesWritableSyncWorker::new(
            reopened,
            capture,
            CloudFilesConcreteDfsAdapter::new(backend),
            FakeNamespace::with_scans([cursor_recovery_snapshot(2, true)]),
            FakeEffects::default(),
        );

        let error = restarted
            .startup(&cancelled)
            .expect_err("restart must re-audit before Ready");
        assert_eq!(error.code, "cursor-recovery-root-audit");
        assert!(!restarted.state().is_ready());
        assert_eq!(startup_scan_calls.load(Ordering::SeqCst), 0);
        assert_eq!(mutation_calls.load(Ordering::SeqCst), 0);
        assert_eq!(restarted.state().queue().operations().count(), 0);
    }

    #[test]
    fn authorized_cursor_recovery_reconciles_retained_queue_before_scanning() {
        let root = TestRoot::new("cursor-recovery-nonempty-queue");
        let mut state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("state");
        let operation = state
            .enqueue_capture(create_capture("recovery-must-not-upload", b"offline"))
            .expect("seed queued local operation");
        let staged = operation
            .request
            .staged_content
            .as_ref()
            .expect("staged content")
            .clone();
        state.authorize_cursor_recovery_for_test();
        let evidence = CloudFilesRemoteMutationEvidence {
            operation_id: operation.request.operation_id.clone(),
            object_id: Some("01recoveredobject".to_string()),
            version: Some("01recoveredversion".to_string()),
            content_hash: Some(staged.content_hash.clone()),
            content_size_bytes: Some(staged.length),
            kind: Some("file".to_string()),
            detail: "stable request already matches authoritative SIFS".to_string(),
        };
        let backend = FakeBackend {
            outcome: CloudFilesDfsMutationOutcome::Applied(evidence),
            ..FakeBackend::default()
        };
        let mutation_calls = Arc::clone(&backend.call_count);
        let mut scan = cursor_recovery_snapshot(1, true);
        scan.manifest.insert(
            operation.request.source_path.clone(),
            CloudFilesReconciledManifestEntry {
                path: operation.request.source_path.clone(),
                object_id: "01recoveredobject".to_string(),
                version: "01recoveredversion".to_string(),
                content_hash: Some(staged.content_hash),
                content_size_bytes: Some(staged.length),
                kind: "file".to_string(),
            },
        );
        let namespace = FakeNamespace::with_scans([scan]);
        let scan_calls = Arc::clone(&namespace.scan_calls);
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            FakeCapture::default(),
            CloudFilesConcreteDfsAdapter::new(backend),
            namespace,
            FakeEffects::default(),
        );

        let progress = worker
            .startup(&AtomicBool::new(false))
            .expect("retained operation reconciles before recovery scan");
        assert!(progress.cursor_recovery_terminal);
        assert!(progress.full_scan_applied);
        assert_eq!(scan_calls.load(Ordering::SeqCst), 1);
        assert_eq!(mutation_calls.load(Ordering::SeqCst), 1);
        assert_eq!(worker.state().queue().operations().count(), 0);
    }

    #[test]
    fn authorized_cursor_recovery_never_scans_across_ambiguous_retained_work() {
        let root = TestRoot::new("cursor-recovery-ambiguous-queue");
        let mut state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("state");
        state
            .enqueue_capture(create_capture("recovery-retained-ambiguous", b"offline"))
            .expect("seed queued local operation");
        state.authorize_cursor_recovery_for_test();
        let backend = FakeBackend::default();
        let mutation_calls = Arc::clone(&backend.call_count);
        let namespace = FakeNamespace::with_scans([cursor_recovery_snapshot(1, true)]);
        let scan_calls = Arc::clone(&namespace.scan_calls);
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            FakeCapture::default(),
            CloudFilesConcreteDfsAdapter::new(backend),
            namespace,
            FakeEffects::default(),
        );

        let progress = worker
            .startup(&AtomicBool::new(false))
            .expect("ambiguous retained work remains parked");
        assert!(!progress.cursor_recovery_terminal);
        assert!(!progress.full_scan_applied);
        assert_eq!(scan_calls.load(Ordering::SeqCst), 0);
        assert_eq!(mutation_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            worker
                .state()
                .queue()
                .operations()
                .next()
                .expect("retained operation")
                .status,
            CloudFilesLocalOperationStatus::Retrying
        );
    }

    fn content() -> CloudFilesDfsMutationContent {
        CloudFilesDfsMutationContent {
            descriptor: CloudFilesStagedContent {
                content_hash: "blake3:staged".to_string(),
                length: 6,
            },
            blob_path: PathBuf::from("staged-test-blob"),
        }
    }

    fn command(kind: CloudFilesLocalOperationKind) -> CloudFilesDfsMutationCommand {
        let (source, destination, destination_path, staged_content) = match kind {
            CloudFilesLocalOperationKind::CreateFile => (
                None,
                CloudFilesDestinationPrecondition::Absent,
                None,
                Some(content()),
            ),
            CloudFilesLocalOperationKind::CreateDirectory => {
                (None, CloudFilesDestinationPrecondition::Absent, None, None)
            }
            CloudFilesLocalOperationKind::ReplaceFile => (
                Some(expected_object()),
                CloudFilesDestinationPrecondition::Unspecified,
                None,
                Some(content()),
            ),
            CloudFilesLocalOperationKind::DeleteFile
            | CloudFilesLocalOperationKind::DeleteDirectory => (
                Some(expected_object()),
                CloudFilesDestinationPrecondition::Unspecified,
                None,
                None,
            ),
            CloudFilesLocalOperationKind::Rename => (
                Some(expected_object()),
                CloudFilesDestinationPrecondition::Absent,
                Some("/services/new-name.txt".to_string()),
                None,
            ),
        };
        CloudFilesDfsMutationCommand {
            operation_id: format!("operation-{kind:?}"),
            request_digest: "blake3:request".to_string(),
            kind,
            source_path: "/services/source.txt".to_string(),
            destination_path,
            preconditions: CloudFilesLocalOperationPreconditions {
                source,
                destination,
            },
            staged_content,
        }
    }

    #[test]
    fn stable_capture_id_is_the_idempotency_key_and_changed_payload_is_rejected() {
        let root = TestRoot::new("stable-capture");
        let mut state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("open state");
        let first = state
            .enqueue_capture(create_capture("volume-7:file-9:usn-11", b"first"))
            .expect("first capture");
        let replay = state
            .enqueue_capture(create_capture("volume-7:file-9:usn-11", b"first"))
            .expect("idempotent replay");
        assert_eq!(first.request.operation_id, replay.request.operation_id);
        let suffix = first
            .request
            .operation_id
            .strip_prefix("cf_")
            .expect("Cloud Files operation prefix");
        assert_eq!(suffix.len(), 26);
        assert_eq!(suffix, suffix.to_ascii_lowercase());
        assert!(suffix.parse::<ulid::Ulid>().is_ok());
        assert_eq!(
            state
                .queue()
                .read_staged_content(&first.request.operation_id)
                .expect("staged bytes"),
            b"first"
        );

        let error = state
            .enqueue_capture(create_capture("volume-7:file-9:usn-11", b"changed"))
            .expect_err("same capture cannot change bytes");
        assert_eq!(error.code, "local-change-queue");
        assert!(error.detail.contains("operation-id-reused"));
        assert_eq!(state.queue().operations().count(), 1);
        let operation_id = first.request.operation_id;
        drop(state);
        let mut reopened =
            CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
                .expect("reopen state");
        let replay = reopened
            .enqueue_capture(create_capture("volume-7:file-9:usn-11", b"first"))
            .expect("replay after restart");
        assert_eq!(replay.request.operation_id, operation_id);
    }

    #[test]
    fn completed_operation_repairs_manifest_before_retirement_after_restart() {
        let root = TestRoot::new("completed-repair");
        let data_dir = root.data_dir();
        let operation_id = {
            let mut state = CloudFilesWritableSyncState::open(&data_dir, "mount-a", "/services")
                .expect("open first state");
            let operation = state
                .enqueue_capture(create_capture("captured-before-crash", b"stable"))
                .expect("enqueue capture");
            let operation_id = operation.request.operation_id.clone();
            state
                .queue
                .mark_remote_applied_with_evidence(CloudFilesRemoteMutationEvidence {
                    operation_id: operation_id.clone(),
                    object_id: Some("01newobject".to_string()),
                    version: Some("01newversion".to_string()),
                    content_hash: Some("blake3:new".to_string()),
                    content_size_bytes: Some(6),
                    kind: Some("file".to_string()),
                    detail: "stable DFS mutation applied".to_string(),
                })
                .expect("mark remote applied");
            state
                .queue
                .mark_completed(&operation_id)
                .expect("simulate crash after Completed before reconciler commit");
            operation_id
        };

        let state = CloudFilesWritableSyncState::open(&data_dir, "mount-a", "/services")
            .expect("reopen state");
        assert!(
            !state
                .reconciler()
                .manifest()
                .contains_key("/services/new.txt")
        );
        let scan_cursor = cursor(1);
        let full_scan = CloudFilesFullScanSnapshot {
            manifest: BTreeMap::from([(
                "/services/new.txt".to_string(),
                CloudFilesReconciledManifestEntry {
                    path: "/services/new.txt".to_string(),
                    object_id: "01newobject".to_string(),
                    version: "01newversion".to_string(),
                    content_hash: Some("blake3:new".to_string()),
                    content_size_bytes: Some(6),
                    kind: "file".to_string(),
                },
            )]),
            removed_paths: Vec::new(),
            materialized_directories: Vec::new(),
            scan_complete: true,
            cursors: BTreeMap::from([(scan_cursor.shard_key().expect("cursor key"), scan_cursor)]),
        };
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            FakeCapture::default(),
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            FakeNamespace::with_scans([full_scan]),
            FakeEffects::default(),
        );

        worker
            .startup(&AtomicBool::new(false))
            .expect("startup repairs completed operation");

        assert!(
            worker.state().queue().operation(&operation_id).is_none(),
            "full-scan reconciliation moves the successful full record into the bounded retired-ID ledger"
        );
        let manifest = worker
            .state()
            .reconciler()
            .manifest()
            .get("/services/new.txt")
            .expect("repaired manifest entry");
        assert_eq!(manifest.object_id, "01newobject");
        assert_eq!(manifest.version, "01newversion");
    }

    #[test]
    fn concrete_adapter_validates_and_dispatches_all_mutation_kinds() {
        let mut adapter = CloudFilesConcreteDfsAdapter::new(FakeBackend::default());
        let kinds = [
            CloudFilesLocalOperationKind::CreateFile,
            CloudFilesLocalOperationKind::CreateDirectory,
            CloudFilesLocalOperationKind::ReplaceFile,
            CloudFilesLocalOperationKind::DeleteFile,
            CloudFilesLocalOperationKind::DeleteDirectory,
            CloudFilesLocalOperationKind::Rename,
        ];
        for kind in &kinds {
            assert!(matches!(
                adapter.apply(&command(kind.clone())).expect("dispatch"),
                CloudFilesDfsMutationOutcome::Ambiguous { .. }
            ));
        }
        assert_eq!(adapter.backend().calls, kinds);
        assert_eq!(adapter.backend().started_calls, kinds.len());

        let valid = command(CloudFilesLocalOperationKind::CreateFile);
        assert!(matches!(
            adapter.reconcile(&valid).expect("read-only reconcile"),
            CloudFilesDfsMutationReconciliation::Ambiguous { .. }
        ));
        assert_eq!(adapter.backend().reconcile_calls, 1);

        let mut invalid = valid;
        invalid.staged_content = None;
        assert!(adapter.apply(&invalid).is_err());
        assert_eq!(adapter.backend().calls.len(), kinds.len());
        assert_eq!(adapter.backend().started_calls, kinds.len());
    }

    #[test]
    fn cursor_recovery_suppressions_are_reconstructed_from_terminal_queue_evidence() {
        let root = TestRoot::new("cursor-recovery-suppression-authority");
        let mut state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("open writable state");
        let operation = state
            .enqueue_capture(create_capture("capture-recovery-suppression", b"retained"))
            .expect("enqueue retained mutation");
        let operation_id = operation.request.operation_id.clone();
        let evidence = CloudFilesRemoteMutationEvidence {
            operation_id: operation_id.clone(),
            object_id: Some("object-retained".to_string()),
            version: Some("version-retained".to_string()),
            content_hash: Some("blake3:retained".to_string()),
            content_size_bytes: Some(8),
            kind: Some("file".to_string()),
            detail: "stable DFS mutation applied".to_string(),
        };
        state
            .queue
            .mark_remote_applied_with_evidence(evidence.clone())
            .expect("mark remote applied");
        state
            .queue
            .mark_completed(&operation_id)
            .expect("mark completed");

        let completed = authorized_cursor_recovery_suppressions(&state.queue)
            .expect("derive completed suppression");
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[&operation_id].operation_id, operation_id);

        state
            .queue
            .retire_terminal_operation(&operation_id)
            .expect("retain superseded queue evidence");
        let superseded = authorized_cursor_recovery_suppressions(&state.queue)
            .expect("derive superseded suppression");
        assert_eq!(superseded, completed);
        assert_eq!(
            superseded[&operation_id].expected_action,
            suppression_for_completed_mutation(
                &CloudFilesDfsMutationCommand {
                    operation_id: operation.request.operation_id,
                    request_digest: operation.request_digest,
                    kind: operation.request.kind,
                    source_path: operation.request.source_path,
                    destination_path: operation.request.destination_path,
                    preconditions: operation.request.preconditions,
                    staged_content: None,
                },
                &evidence,
            )
            .expect("derive expected suppression")
            .expected_action
        );
    }

    #[test]
    fn readiness_failure_blocks_before_full_scan_or_placeholder_effect() {
        let root = TestRoot::new("readiness-gate");
        let state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("open state");
        let namespace = FakeNamespace::with_scans([snapshot(1)]);
        let scan_calls = Arc::clone(&namespace.scan_calls);
        let effects = FakeEffects::default();
        let effect_calls = Arc::clone(&effects.full_scan_effects);
        let backend = FakeBackend {
            readiness_error: Some("stable mkdir unavailable".to_string()),
            ..FakeBackend::default()
        };
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            FakeCapture::default(),
            CloudFilesConcreteDfsAdapter::new(backend),
            namespace,
            effects,
        );

        let error = worker
            .startup(&AtomicBool::new(false))
            .expect_err("readiness must fail closed");
        assert_eq!(
            error.code,
            CLOUD_FILES_WRITABLE_SYNC_CONTRACT_NOT_READY_CODE
        );
        assert_eq!(scan_calls.load(Ordering::Relaxed), 0);
        assert!(effect_calls.lock().expect("effects").is_empty());
        let status = worker.state().status();
        assert_eq!(status.mode, CloudFilesLocalChangeSyncMode::Blocked);
        assert!(status.local_mutations_blocked);
    }

    #[test]
    fn lifecycle_status_exposes_bounded_typed_local_and_projection_evidence() {
        let root = TestRoot::new("typed-attention");
        let mut state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("open state");
        let operation = state
            .enqueue_capture(create_capture("capture-attention", b"retained bytes"))
            .expect("enqueue retained content");
        state
            .queue
            .mark_conflict(
                &operation.request.operation_id,
                CloudFilesConflictEvidence {
                    code: "destination-exists".to_string(),
                    detail: "authoritative destination already exists".to_string(),
                    observed_object_id: Some("remote-object".to_string()),
                    observed_version: Some("remote-version".to_string()),
                    observed_content_hash: Some("blake3:remote".to_string()),
                },
            )
            .expect("record local conflict");
        state
            .reconciler
            .install_full_scan_delta(
                BTreeMap::from([(
                    "/services/link".to_string(),
                    CloudFilesReconciledManifestEntry {
                        path: "/services/link".to_string(),
                        object_id: "link-object".to_string(),
                        version: "link-version".to_string(),
                        content_hash: None,
                        content_size_bytes: None,
                        kind: "symlink".to_string(),
                    },
                )]),
                Vec::new(),
                Vec::new(),
                BTreeMap::new(),
                false,
            )
            .expect("record unsupported projection");

        let status = state.status();
        assert_eq!(status.conflict_count, 2);
        let local = status
            .attention
            .iter()
            .find(|summary| summary.kind == CloudFilesSyncAttentionKind::LocalOperationConflict)
            .expect("local conflict summary");
        assert_eq!(local.path, "/services/new.txt");
        let evidence = local
            .retained_local_evidence
            .as_ref()
            .expect("retained local evidence");
        assert_eq!(
            evidence.operation_id.as_deref(),
            Some(operation.request.operation_id.as_str())
        );
        assert_eq!(evidence.staged_content_size_bytes, Some(14));
        assert!(status.attention.iter().any(|summary| {
            summary.kind == CloudFilesSyncAttentionKind::UnsupportedObjectKind
                && summary.path == "/services/link"
        }));
    }

    #[test]
    fn change_capture_is_ready_before_scan_and_startup_conflicts_are_durable() {
        let root = TestRoot::new("capture-before-scan");
        let state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("open state");
        let activated = Arc::new(AtomicBool::new(false));
        let capture = FakeCapture {
            activated: Arc::clone(&activated),
            require_activation_before_scan: true,
            startup_conflicts: vec![CloudFilesStartupConflictEvidence {
                path: "/services/untracked.txt".to_string(),
                detail: "owned root contains an untracked local path".to_string(),
            }],
            ..FakeCapture::default()
        };
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            capture,
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            FakeNamespace::with_scans([snapshot(1)]),
            FakeEffects::default(),
        );

        worker
            .startup(&AtomicBool::new(false))
            .expect("startup with surfaced conflict");

        assert!(activated.load(Ordering::SeqCst));
        assert_eq!(worker.state().reconciler().startup_conflicts().len(), 1);
        assert_eq!(worker.state().status().conflict_count, 1);
    }

    #[test]
    fn partial_startup_local_scan_reports_active_after_one_safe_bounded_quantum() {
        let root = TestRoot::new("partial-local-startup");
        let state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("open state");
        let capture = FakeCapture {
            startup_batches: VecDeque::from([
                CloudFilesStartupScanBatch {
                    changes: Vec::new(),
                    sparse_placeholders: Vec::new(),
                    complete: false,
                },
                CloudFilesStartupScanBatch {
                    changes: Vec::new(),
                    sparse_placeholders: Vec::new(),
                    complete: false,
                },
                CloudFilesStartupScanBatch {
                    changes: Vec::new(),
                    sparse_placeholders: Vec::new(),
                    complete: true,
                },
            ]),
            ..FakeCapture::default()
        };
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            capture,
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            FakeNamespace::with_scans([snapshot(1)]),
            FakeEffects::default(),
        );
        let cancelled = AtomicBool::new(false);

        worker.startup(&cancelled).expect("first bounded scan page");
        assert_eq!(
            worker.state().status().mode,
            CloudFilesLocalChangeSyncMode::Active
        );
        worker
            .run_once(&cancelled)
            .expect("second bounded scan page");
        assert_eq!(
            worker.state().status().mode,
            CloudFilesLocalChangeSyncMode::Active
        );
        worker
            .run_once(&cancelled)
            .expect("terminal bounded scan page");
        assert_eq!(
            worker.state().status().mode,
            CloudFilesLocalChangeSyncMode::Active
        );
    }

    #[test]
    fn depth_zero_sparse_placeholder_is_authoritatively_adopted_before_restart_classification() {
        let root = TestRoot::new("depth-zero-sparse-restart");
        let state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("open state");
        let identity = CloudFilesPlaceholderIdentity::new(
            "mount-a",
            "/services",
            "/services/a.txt",
            "01aobject",
            "01aversion",
            Some("blake3:a".to_string()),
        );
        let capture = FakeCapture {
            startup_batches: VecDeque::from([
                CloudFilesStartupScanBatch {
                    changes: Vec::new(),
                    sparse_placeholders: vec![CloudFilesSparsePlaceholderObservation {
                        local_path: "/services/a.txt".to_string(),
                        identity: identity.clone(),
                    }],
                    complete: false,
                },
                CloudFilesStartupScanBatch {
                    changes: Vec::new(),
                    sparse_placeholders: Vec::new(),
                    complete: true,
                },
            ]),
            ..FakeCapture::default()
        };
        let mut namespace = FakeNamespace::with_scans([snapshot(1)]);
        namespace.resolved.insert(
            identity.path.clone(),
            CloudFilesReconciledManifestEntry {
                path: identity.path.clone(),
                object_id: identity.object_id.clone(),
                version: identity.version.clone(),
                content_hash: identity.content_hash.clone(),
                content_size_bytes: Some(7),
                kind: "file".to_string(),
            },
        );
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            capture,
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            namespace,
            FakeEffects::default(),
        );
        let cancelled = AtomicBool::new(false);
        worker.startup(&cancelled).expect("bounded startup");
        assert_eq!(
            worker.state().status().mode,
            CloudFilesLocalChangeSyncMode::Active
        );
        assert!(
            worker
                .state()
                .reconciler()
                .manifest()
                .contains_key("/services/a.txt")
        );
        assert!(
            worker
                .state()
                .reconciler()
                .materialized_directories()
                .contains("/services")
        );
        assert_eq!(worker.state().queue().operations().count(), 0);
        assert_eq!(worker.state().reconciler().conflict_count(), 0);
        worker.run_once(&cancelled).expect("finish sparse audit");
        assert_eq!(worker.state().queue().operations().count(), 0);
        drop(worker);

        let reopened = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("reopen durable sparse state");
        assert!(
            reopened
                .reconciler()
                .manifest()
                .contains_key("/services/a.txt")
        );
        assert!(
            reopened
                .reconciler()
                .materialized_directories()
                .contains("/services")
        );
        assert_eq!(reopened.queue().operations().count(), 0);
    }

    #[test]
    fn changed_sparse_placeholder_generation_is_conflict_never_create() {
        let root = TestRoot::new("sparse-generation-conflict");
        let state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("open state");
        let identity = CloudFilesPlaceholderIdentity::new(
            "mount-a",
            "/services",
            "/services/a.txt",
            "01oldobject",
            "01oldversion",
            Some("blake3:old".to_string()),
        );
        let capture = FakeCapture {
            startup_batches: VecDeque::from([CloudFilesStartupScanBatch {
                changes: Vec::new(),
                sparse_placeholders: vec![CloudFilesSparsePlaceholderObservation {
                    local_path: "/services/a.txt".to_string(),
                    identity: identity.clone(),
                }],
                complete: false,
            }]),
            ..FakeCapture::default()
        };
        let mut namespace = FakeNamespace::with_scans([snapshot(1)]);
        namespace.resolved.insert(
            identity.path.clone(),
            CloudFilesReconciledManifestEntry {
                path: identity.path,
                object_id: "01newobject".to_string(),
                version: "01newversion".to_string(),
                content_hash: Some("blake3:new".to_string()),
                content_size_bytes: Some(9),
                kind: "file".to_string(),
            },
        );
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            capture,
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            namespace,
            FakeEffects::default(),
        );
        worker
            .startup(&AtomicBool::new(false))
            .expect("fail-closed startup continues");
        assert_eq!(worker.state().queue().operations().count(), 0);
        assert!(
            worker
                .state()
                .reconciler()
                .startup_conflicts()
                .contains_key("/services/a.txt")
        );
        assert!(
            !worker
                .state()
                .reconciler()
                .manifest()
                .contains_key("/services/a.txt")
        );
    }

    #[test]
    fn sparse_adoption_reclassifies_dirty_path_as_exact_replace_not_create() {
        let root = TestRoot::new("sparse-dirty-replace");
        let state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("open state");
        let identity = CloudFilesPlaceholderIdentity::new(
            "mount-a",
            "/services",
            "/services/a.txt",
            "01aobject",
            "01aversion",
            Some("blake3:a".to_string()),
        );
        let expected = CloudFilesExpectedObjectState {
            object_id: identity.object_id.clone(),
            version: identity.version.clone(),
            content_hash: identity.content_hash.clone(),
        };
        let replacement = CloudFilesCapturedLocalChange {
            capture_id: "dirty-after-sparse-adoption".to_string(),
            kind: CloudFilesLocalOperationKind::ReplaceFile,
            source_path: identity.path.clone(),
            destination_path: None,
            preconditions: CloudFilesLocalOperationPreconditions {
                source: Some(expected.clone()),
                destination: CloudFilesDestinationPrecondition::Unspecified,
            },
            captured_content: Some(CloudFilesCapturedContent::Inline(b"local-newer".to_vec())),
        };
        let capture = FakeCapture {
            startup_batches: VecDeque::from([
                CloudFilesStartupScanBatch {
                    changes: Vec::new(),
                    sparse_placeholders: vec![CloudFilesSparsePlaceholderObservation {
                        local_path: identity.path.clone(),
                        identity: identity.clone(),
                    }],
                    complete: false,
                },
                CloudFilesStartupScanBatch {
                    changes: vec![replacement],
                    sparse_placeholders: Vec::new(),
                    complete: true,
                },
            ]),
            ..FakeCapture::default()
        };
        let mut namespace = FakeNamespace::with_scans([snapshot(1)]);
        namespace.resolved.insert(
            identity.path.clone(),
            CloudFilesReconciledManifestEntry {
                path: identity.path,
                object_id: expected.object_id.clone(),
                version: expected.version.clone(),
                content_hash: expected.content_hash.clone(),
                content_size_bytes: Some(7),
                kind: "file".to_string(),
            },
        );
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            capture,
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            namespace,
            FakeEffects::default(),
        );
        let cancelled = AtomicBool::new(false);
        worker.startup(&cancelled).expect("adopt sparse identity");
        worker
            .run_once(&cancelled)
            .expect("queue exact replacement");
        let operation = worker
            .state()
            .queue()
            .operations()
            .find(|operation| {
                operation.request.capture_id.as_deref() == Some("dirty-after-sparse-adoption")
            })
            .expect("durable replacement");
        assert_eq!(
            operation.request.kind,
            CloudFilesLocalOperationKind::ReplaceFile
        );
        assert_eq!(operation.request.preconditions.source, Some(expected));
        assert_ne!(
            operation.request.kind,
            CloudFilesLocalOperationKind::CreateFile
        );
    }

    #[test]
    fn full_scan_cursor_is_installed_only_after_effect_and_replays_after_crash() {
        let root = TestRoot::new("cursor-after-effect");
        let effect_calls = Arc::new(Mutex::new(Vec::new()));
        let state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("open first state");
        let mut first_effects = FakeEffects {
            full_scan_effects: Arc::clone(&effect_calls),
            fail_full_scan: true,
            ..FakeEffects::default()
        };
        first_effects.path_states.clear();
        let mut first = CloudFilesWritableSyncWorker::new(
            state,
            FakeCapture::default(),
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            FakeNamespace::with_scans([snapshot(9)]),
            first_effects,
        );
        let error = first
            .startup(&AtomicBool::new(false))
            .expect_err("simulated effect crash");
        assert_eq!(error.code, "full-reconciliation-effect");
        assert!(first.state().reconciler().cursors().is_empty());
        drop(first);

        let reopened = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("reopen after crash");
        assert!(reopened.reconciler().cursors().is_empty());
        let effects = FakeEffects {
            full_scan_effects: Arc::clone(&effect_calls),
            ..FakeEffects::default()
        };
        let mut second = CloudFilesWritableSyncWorker::new(
            reopened,
            FakeCapture::default(),
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            FakeNamespace::with_scans([snapshot(9)]),
            effects,
        );
        second
            .startup(&AtomicBool::new(false))
            .expect("idempotent effect replay");
        assert_eq!(effect_calls.lock().expect("effects").len(), 2);
        assert_eq!(
            second
                .state()
                .reconciler()
                .cursors()
                .values()
                .next()
                .expect("installed cursor")
                .after_sequence,
            9
        );
        assert_eq!(
            second.state().status().mode,
            CloudFilesLocalChangeSyncMode::Active
        );
    }

    #[test]
    fn empty_materialized_directory_intent_survives_crash_before_worker_pass() {
        let root = TestRoot::new("empty-materialization-intent-restart");
        let empty_directory = CloudFilesReconciledManifestEntry {
            path: "/services/empty".to_string(),
            object_id: "01emptydirectory".to_string(),
            version: "01emptydirectoryv1".to_string(),
            content_hash: None,
            content_size_bytes: None,
            kind: "directory".to_string(),
        };
        {
            let mut state =
                CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
                    .expect("open state before browse");
            state
                .reconciler
                .install_full_scan_delta(
                    BTreeMap::from([(empty_directory.path.clone(), empty_directory.clone())]),
                    Vec::new(),
                    Vec::new(),
                    BTreeMap::new(),
                    false,
                )
                .expect("seed remote directory identity without materialization ownership");
            state
                .materialization_audit_intents()
                .lock()
                .expect("audit intents")
                .record_directory(&empty_directory.path)
                .expect("fsync pre-CFAPI audit intent");
            // Simulate process loss after the callback's durable intent and
            // before the writable worker gets a pass.
        }

        let reopened = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("reopen after callback crash");
        let intents = reopened.materialization_audit_intents();
        assert!(
            intents
                .lock()
                .expect("audit intents")
                .pending_directories()
                .contains(&empty_directory.path)
        );
        let terminal_cursor = cursor(11);
        let scan = CloudFilesFullScanSnapshot {
            manifest: BTreeMap::new(),
            removed_paths: Vec::new(),
            materialized_directories: vec![empty_directory.path.clone()],
            scan_complete: true,
            cursors: BTreeMap::from([(
                terminal_cursor.shard_key().expect("cursor key"),
                terminal_cursor,
            )]),
        };
        let mut worker = CloudFilesWritableSyncWorker::new(
            reopened,
            FakeCapture::default(),
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            FakeNamespace::with_materialization_audit_intents([scan], Arc::clone(&intents)),
            FakeEffects::default(),
        );
        worker
            .startup(&AtomicBool::new(false))
            .expect("audit empty materialized directory");
        assert!(
            worker
                .state()
                .reconciler()
                .materialized_directories()
                .contains(&empty_directory.path)
        );
        assert!(
            intents
                .lock()
                .expect("audit intents")
                .pending_directories()
                .is_empty()
        );
        drop(worker);

        let reopened = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("reopen committed empty browse");
        assert!(
            reopened
                .reconciler()
                .materialized_directories()
                .contains(&empty_directory.path)
        );
        assert!(
            reopened
                .reconciler()
                .manifest()
                .contains_key(&empty_directory.path)
        );
        assert_eq!(reopened.reconciler().cursors().len(), 1);
    }

    #[test]
    fn already_matching_materialized_page_commits_ownership_before_cursor() {
        let root = TestRoot::new("noop-materialization-intent-restart");
        let existing = CloudFilesReconciledManifestEntry {
            path: "/services/existing.txt".to_string(),
            object_id: "01existingobject".to_string(),
            version: "01existingversion".to_string(),
            content_hash: Some("blake3:existing".to_string()),
            content_size_bytes: Some(8),
            kind: "file".to_string(),
        };
        let state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("open state before no-op browse");
        let intents = state.materialization_audit_intents();
        let initial_cursor = cursor(10);
        let initial_scan = CloudFilesFullScanSnapshot {
            manifest: BTreeMap::from([(existing.path.clone(), existing.clone())]),
            removed_paths: Vec::new(),
            materialized_directories: Vec::new(),
            scan_complete: true,
            cursors: BTreeMap::from([(
                initial_cursor.shard_key().expect("initial cursor key"),
                initial_cursor,
            )]),
        };
        let terminal_cursor = cursor(12);
        let materialization_scan = CloudFilesFullScanSnapshot {
            manifest: BTreeMap::from([(existing.path.clone(), existing.clone())]),
            removed_paths: Vec::new(),
            materialized_directories: vec!["/services".to_string()],
            scan_complete: true,
            cursors: BTreeMap::from([(
                terminal_cursor.shard_key().expect("cursor key"),
                terminal_cursor,
            )]),
        };
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            FakeCapture::default(),
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            FakeNamespace::with_materialization_audit_intents(
                [initial_scan, materialization_scan],
                Arc::clone(&intents),
            ),
            FakeEffects::default(),
        );
        worker
            .startup(&AtomicBool::new(false))
            .expect("seed exact child without directory ownership");
        assert!(
            !worker
                .state()
                .reconciler()
                .materialized_directories()
                .contains("/services")
        );
        intents
            .lock()
            .expect("audit intents")
            .record_directory("/services")
            .expect("fsync no-op page audit intent");
        worker
            .run_once(&AtomicBool::new(false))
            .expect("commit active no-op page ownership");
        assert_eq!(worker.state().queue().operations().count(), 0);
        assert_eq!(worker.state().reconciler().conflict_count(), 0);
        assert_eq!(
            worker.state().reconciler().manifest().get(&existing.path),
            Some(&existing)
        );
        assert!(
            worker
                .state()
                .reconciler()
                .materialized_directories()
                .contains("/services")
        );
        assert!(
            intents
                .lock()
                .expect("audit intents")
                .pending_directories()
                .is_empty()
        );
        drop(worker);

        let reopened = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("reopen committed no-op page");
        assert!(
            reopened
                .reconciler()
                .materialized_directories()
                .contains("/services")
        );
        assert_eq!(
            reopened.reconciler().manifest().get(&existing.path),
            Some(&existing)
        );
        assert_eq!(reopened.reconciler().cursors().len(), 1);
    }

    #[test]
    fn materialization_intent_replays_after_marker_commit_before_ack() {
        let root = TestRoot::new("materialization-marker-before-ack");
        {
            let mut state =
                CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
                    .expect("open state");
            state
                .materialization_audit_intents()
                .lock()
                .expect("audit intents")
                .record_directory("/services")
                .expect("record durable intent");
            let committed_cursor = cursor(20);
            state
                .reconciler
                .install_full_scan_delta(
                    BTreeMap::new(),
                    Vec::new(),
                    vec!["/services".to_string()],
                    BTreeMap::from([(
                        committed_cursor.shard_key().expect("cursor key"),
                        committed_cursor,
                    )]),
                    true,
                )
                .expect("commit marker and cursor before simulated crash");
            // Drop before acknowledgement: both the committed marker and the
            // older intent must survive and replay safely.
        }

        let reopened = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("reopen after marker-before-ack crash");
        let intents = reopened.materialization_audit_intents();
        assert!(
            reopened
                .reconciler()
                .materialized_directories()
                .contains("/services")
        );
        assert!(
            intents
                .lock()
                .expect("audit intents")
                .pending_directories()
                .contains("/services")
        );
        let replay_cursor = cursor(21);
        let replay = CloudFilesFullScanSnapshot {
            manifest: BTreeMap::new(),
            removed_paths: Vec::new(),
            materialized_directories: vec!["/services".to_string()],
            scan_complete: true,
            cursors: BTreeMap::from([(
                replay_cursor.shard_key().expect("cursor key"),
                replay_cursor,
            )]),
        };
        let mut worker = CloudFilesWritableSyncWorker::new(
            reopened,
            FakeCapture::default(),
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            FakeNamespace::with_materialization_audit_intents([replay], Arc::clone(&intents)),
            FakeEffects::default(),
        );
        worker
            .startup(&AtomicBool::new(false))
            .expect("replay durable materialization intent");
        assert!(
            intents
                .lock()
                .expect("audit intents")
                .pending_directories()
                .is_empty()
        );
    }

    #[test]
    fn authoritative_absence_retires_materialization_intent_without_marker() {
        let root = TestRoot::new("materialization-intent-not-found");
        let directory = CloudFilesReconciledManifestEntry {
            path: "/services/deleted".to_string(),
            object_id: "01deleteddirectory".to_string(),
            version: "01deletedv1".to_string(),
            content_hash: None,
            content_size_bytes: None,
            kind: "directory".to_string(),
        };
        let mut state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("open state");
        state
            .reconciler
            .install_full_scan_delta(
                BTreeMap::from([(directory.path.clone(), directory.clone())]),
                Vec::new(),
                vec![directory.path.clone()],
                BTreeMap::new(),
                false,
            )
            .expect("seed materialized directory");
        let intents = state.materialization_audit_intents();
        intents
            .lock()
            .expect("audit intents")
            .record_directory(&directory.path)
            .expect("record audit intent before authoritative absence");
        let terminal_cursor = cursor(22);
        let absent = CloudFilesFullScanSnapshot {
            manifest: BTreeMap::new(),
            removed_paths: vec![directory.path.clone()],
            // The adapter reports the intent as authoritatively resolved even
            // though list returned NotFound. The remove wins atomically.
            materialized_directories: vec![directory.path.clone()],
            scan_complete: true,
            cursors: BTreeMap::from([(
                terminal_cursor.shard_key().expect("cursor key"),
                terminal_cursor,
            )]),
        };
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            FakeCapture::default(),
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            FakeNamespace::with_materialization_audit_intents([absent], Arc::clone(&intents)),
            FakeEffects::default(),
        );
        worker
            .startup(&AtomicBool::new(false))
            .expect("commit authoritative absence and retire intent");
        assert!(
            !worker
                .state()
                .reconciler()
                .manifest()
                .contains_key(&directory.path)
        );
        assert!(
            !worker
                .state()
                .reconciler()
                .materialized_directories()
                .contains(&directory.path)
        );
        assert!(
            intents
                .lock()
                .expect("audit intents")
                .pending_directories()
                .is_empty()
        );
        worker
            .run_once(&AtomicBool::new(false))
            .expect("no repeated full scan after absence retirement");
    }

    #[test]
    fn startup_recovers_durable_namespace_rename_effect_before_full_scan_install() {
        let root = TestRoot::new("inflight-namespace-before-full-scan");
        let old_entry = CloudFilesReconciledManifestEntry {
            path: "/services/old.txt".to_string(),
            object_id: "01renameobject".to_string(),
            version: "01renamev1".to_string(),
            content_hash: Some("blake3:rename".to_string()),
            content_size_bytes: Some(6),
            kind: "file".to_string(),
        };
        {
            let mut state =
                CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
                    .expect("open pre-crash state");
            state
                .reconciler
                .install_full_scan_delta(
                    BTreeMap::from([(old_entry.path.clone(), old_entry.clone())]),
                    Vec::new(),
                    vec!["/services".to_string()],
                    BTreeMap::new(),
                    false,
                )
                .expect("seed old identity");
            let rename = CloudFilesNamespaceFeedEvent::Rename {
                from_path: old_entry.path.clone(),
                to_path: "/services/new.txt".to_string(),
                object_id: old_entry.object_id.clone(),
                version: "01renamev2".to_string(),
                content_hash: old_entry.content_hash.clone(),
                content_size_bytes: old_entry.content_size_bytes,
                kind: old_entry.kind.clone(),
                provenance: CloudFilesNamespaceProvenance {
                    event_id: "rename-crash-event".to_string(),
                    journal_sequence: 1,
                    journal_ordinal: 0,
                    operation_id: None,
                },
            };
            let batch = CloudFilesNamespaceFeedBatch {
                input_cursor: cursor(0),
                next_cursor: cursor(1),
                events: vec![rename],
                journal_head_sequence: 1,
                has_more: false,
            };
            let mut crashing = FakeEffects {
                apply_results: VecDeque::from([Err(
                    "simulated crash after idempotent local rename".to_string(),
                )]),
                ..FakeEffects::default()
            };
            let error = state
                .reconciler
                .reconcile_batch(batch, &mut crashing)
                .expect_err("leave durable inflight rename");
            assert_eq!(error.code, "namespace-effect");
        }

        let reopened = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("reopen with inflight rename");
        let terminal_cursor = cursor(1);
        let scan = CloudFilesFullScanSnapshot {
            manifest: BTreeMap::new(),
            removed_paths: Vec::new(),
            materialized_directories: Vec::new(),
            scan_complete: true,
            cursors: BTreeMap::from([(
                terminal_cursor.shard_key().expect("cursor key"),
                terminal_cursor,
            )]),
        };
        let mut worker = CloudFilesWritableSyncWorker::new(
            reopened,
            FakeCapture::default(),
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            FakeNamespace::with_scans([scan]),
            FakeEffects::default(),
        );
        worker
            .startup(&AtomicBool::new(false))
            .expect("recover rename before full scan install");
        assert!(
            !worker
                .state()
                .reconciler()
                .manifest()
                .contains_key(&old_entry.path)
        );
        let renamed = worker
            .state()
            .reconciler()
            .manifest()
            .get("/services/new.txt")
            .expect("recovered renamed identity");
        assert_eq!(renamed.object_id, old_entry.object_id);
        assert_eq!(renamed.version, "01renamev2");
        assert_eq!(worker.state().reconciler().cursors().len(), 1);
    }

    #[test]
    fn paged_full_scan_replays_partial_delta_after_restart_before_cursor_commit() {
        let root = TestRoot::new("paged-scan-restart");
        let entry = CloudFilesReconciledManifestEntry {
            path: "/services/page-a.txt".to_string(),
            object_id: "01pagea".to_string(),
            version: "01pageav1".to_string(),
            content_hash: Some("blake3:page-a".to_string()),
            content_size_bytes: Some(7),
            kind: "file".to_string(),
        };
        let partial = CloudFilesFullScanSnapshot {
            manifest: BTreeMap::from([(entry.path.clone(), entry.clone())]),
            removed_paths: Vec::new(),
            materialized_directories: Vec::new(),
            scan_complete: false,
            cursors: BTreeMap::new(),
        };
        let state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("open first state");
        let mut first = CloudFilesWritableSyncWorker::new(
            state,
            FakeCapture::default(),
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            FakeNamespace::with_scans([partial.clone()]),
            FakeEffects::default(),
        );
        first
            .startup(&AtomicBool::new(false))
            .expect("install first bounded page");
        assert!(first.state().reconciler().cursors().is_empty());
        assert_eq!(first.state().reconciler().manifest().len(), 1);
        drop(first);

        let reopened = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("reopen between pages");
        assert!(reopened.reconciler().cursors().is_empty());
        assert_eq!(
            reopened.reconciler().manifest().get(&entry.path),
            Some(&entry)
        );
        let terminal_cursor = cursor(17);
        let terminal = CloudFilesFullScanSnapshot {
            manifest: BTreeMap::from([(
                "/services/page-b.txt".to_string(),
                CloudFilesReconciledManifestEntry {
                    path: "/services/page-b.txt".to_string(),
                    object_id: "01pageb".to_string(),
                    version: "01pagebv1".to_string(),
                    content_hash: Some("blake3:page-b".to_string()),
                    content_size_bytes: Some(7),
                    kind: "file".to_string(),
                },
            )]),
            removed_paths: Vec::new(),
            materialized_directories: Vec::new(),
            scan_complete: true,
            cursors: BTreeMap::from([(
                terminal_cursor.shard_key().expect("cursor key"),
                terminal_cursor,
            )]),
        };
        let mut second = CloudFilesWritableSyncWorker::new(
            reopened,
            FakeCapture::default(),
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            FakeNamespace::with_scans([partial, terminal]),
            FakeEffects::default(),
        );
        second
            .startup(&AtomicBool::new(false))
            .expect("idempotently replay partial page");
        assert!(second.state().reconciler().cursors().is_empty());
        second
            .run_once(&AtomicBool::new(false))
            .expect("install terminal page");
        assert_eq!(second.state().reconciler().manifest().len(), 2);
        assert_eq!(
            second
                .state()
                .reconciler()
                .cursors()
                .values()
                .next()
                .expect("terminal cursor")
                .after_sequence,
            17
        );
    }

    #[test]
    fn pending_sparse_scan_does_not_starve_local_change_capture() {
        let root = TestRoot::new("pending-scan-local-capture");
        let state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("open state");
        let drain_calls = Arc::new(AtomicUsize::new(0));
        let capture = FakeCapture {
            batches: VecDeque::from([Ok(CloudFilesCapturedChangeBatch {
                changes: vec![create_capture("during-pending-scan", b"local")],
                sparse_placeholders: Vec::new(),
                overflowed: false,
            })]),
            drain_calls: Arc::clone(&drain_calls),
            ..FakeCapture::default()
        };
        let backend = FakeBackend::default();
        let mutation_calls = Arc::clone(&backend.call_count);
        let mut partial = snapshot(2);
        partial.scan_complete = false;
        partial.cursors.clear();
        let namespace = FakeNamespace::with_scans([snapshot(1), partial]);
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            capture,
            CloudFilesConcreteDfsAdapter::new(backend),
            namespace,
            FakeEffects::default(),
        );
        let cancelled = AtomicBool::new(false);
        worker.startup(&cancelled).expect("startup");
        worker.full_scan_pending = true;
        worker.startup_scan_baseline = Some(worker.state().reconciler().manifest().clone());

        let progress = worker
            .run_once(&cancelled)
            .expect("capture while sparse scan remains pending");

        assert_eq!(drain_calls.load(Ordering::SeqCst), 1);
        assert_eq!(progress.captures_enqueued, 1);
        assert_eq!(mutation_calls.load(Ordering::SeqCst), 1);
        assert!(progress.full_scan_applied);
        assert!(worker.full_scan_pending);
        assert!(worker.ordinary_reconciliation_remains_pending());
        assert!(worker.state().queue().operations().any(|operation| {
            operation.request.capture_id.as_deref() == Some("during-pending-scan")
        }));
    }

    #[test]
    fn notification_overflow_discards_partial_batch_and_forces_full_scan() {
        let root = TestRoot::new("overflow");
        let state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("open state");
        let capture = FakeCapture {
            batches: VecDeque::from([Ok(CloudFilesCapturedChangeBatch {
                changes: vec![create_capture("partial-overflow", b"must-not-stage")],
                sparse_placeholders: Vec::new(),
                overflowed: true,
            })]),
            ..FakeCapture::default()
        };
        let namespace = FakeNamespace::with_scans([snapshot(1), snapshot(2)]);
        let scan_calls = Arc::clone(&namespace.scan_calls);
        let effects = FakeEffects::default();
        let effect_calls = Arc::clone(&effects.full_scan_effects);
        let mut worker = CloudFilesWritableSyncWorker::new(
            state,
            capture,
            CloudFilesConcreteDfsAdapter::new(FakeBackend::default()),
            namespace,
            effects,
        );
        let cancelled = AtomicBool::new(false);
        worker.startup(&cancelled).expect("startup");
        let progress = worker.run_once(&cancelled).expect("overflow reconcile");

        assert!(progress.full_scan_applied);
        assert_eq!(progress.captures_enqueued, 0);
        assert_eq!(worker.state().queue().operations().count(), 0);
        assert_eq!(scan_calls.load(Ordering::Relaxed), 2);
        assert_eq!(effect_calls.lock().expect("effects").len(), 2);
        assert_eq!(
            worker
                .state()
                .reconciler()
                .cursors()
                .values()
                .next()
                .expect("overflow cursor")
                .after_sequence,
            2
        );
    }

    #[test]
    fn suppression_requires_authoritative_rename_kind_and_identity() {
        let command = command(CloudFilesLocalOperationKind::Rename);
        let evidence = CloudFilesRemoteMutationEvidence {
            operation_id: command.operation_id.clone(),
            object_id: Some("01renamedobject".to_string()),
            version: Some("01renamedversion".to_string()),
            content_hash: Some("blake3:renamed".to_string()),
            content_size_bytes: Some(44),
            kind: Some("directory".to_string()),
            detail: "applied".to_string(),
        };
        assert!(matches!(
            suppression_for_completed_mutation(&command, &evidence)
                .expect("authoritative suppression")
                .expected_action,
            CloudFilesNamespaceAction::Rename {
                ref from_path,
                ref to_path,
                ref kind,
                content_size_bytes: Some(44),
                ..
            } if from_path == "/services/source.txt"
                && to_path == "/services/new-name.txt"
                && kind == "directory"
        ));

        let mut missing_kind = evidence;
        missing_kind.kind = None;
        assert_eq!(
            suppression_for_completed_mutation(&command, &missing_kind)
                .expect_err("rename kind is required")
                .code,
            "suppression-evidence-missing"
        );
    }

    #[test]
    fn queued_capture_starts_as_pending_and_status_reports_it() {
        let root = TestRoot::new("status");
        let mut state = CloudFilesWritableSyncState::open(&root.data_dir(), "mount-a", "/services")
            .expect("open state");
        let operation = state
            .enqueue_capture(create_capture("capture-status", b"pending"))
            .expect("enqueue");
        assert_eq!(operation.status, CloudFilesLocalOperationStatus::Queued);
        let status = state.status();
        assert_eq!(status.mode, CloudFilesLocalChangeSyncMode::Starting);
        assert_eq!(status.pending_operations, 1);
        assert_eq!(status.retrying_operations, 0);
        assert_eq!(status.conflict_count, 0);

        assert_eq!(
            state.explorer_item_statuses().get("/services/new.txt"),
            Some(&CloudFilesExplorerItemStatus::WaitingToUpload)
        );
        state
            .queue
            .mark_in_flight(&operation.request.operation_id)
            .expect("mark uploading");
        assert_eq!(
            state.explorer_item_statuses().get("/services/new.txt"),
            Some(&CloudFilesExplorerItemStatus::Uploading)
        );
        state
            .queue
            .mark_retrying(&operation.request.operation_id, "network unavailable")
            .expect("mark retrying");
        assert_eq!(
            state.explorer_item_statuses().get("/services/new.txt"),
            Some(&CloudFilesExplorerItemStatus::Retrying)
        );
        state
            .queue
            .mark_conflict(
                &operation.request.operation_id,
                CloudFilesConflictEvidence {
                    code: "version-changed".to_string(),
                    detail: "remote object advanced".to_string(),
                    observed_object_id: Some("01object".to_string()),
                    observed_version: Some("01version".to_string()),
                    observed_content_hash: Some("blake3:changed".to_string()),
                },
            )
            .expect("mark conflict");
        assert_eq!(
            state.explorer_item_statuses().get("/services/new.txt"),
            Some(&CloudFilesExplorerItemStatus::Conflict)
        );
        state
            .queue
            .mark_failed(&operation.request.operation_id, "operator action required")
            .expect("mark failed");
        assert_eq!(
            state.explorer_item_statuses().get("/services/new.txt"),
            Some(&CloudFilesExplorerItemStatus::Failed)
        );
        state
            .queue
            .mark_completed(&operation.request.operation_id)
            .expect("mark completed");
        assert!(
            !state
                .explorer_item_statuses()
                .contains_key("/services/new.txt"),
            "a completed operation returns to Windows' standard in-sync state"
        );
    }
}
