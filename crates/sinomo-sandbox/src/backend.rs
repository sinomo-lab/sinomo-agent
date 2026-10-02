//! Host-agnostic sandbox backend abstraction + the local filesystem backend.
//!
//! A [`SandboxBackend`] serves the sandbox control routes against some concrete store:
//! the local filesystem store ([`LocalSandboxBackend`], used when running daemonless) or
//! the clustered SIFS-over-DFS store. The seam's `SandboxService`
//! (in `sinomo-service`) dispatches each route to whichever backend the host/context
//! selects, so there is a single sandbox code path regardless of whether a cluster exists.
//!
//! The trait is intentionally at request→response granularity (one method per route): it
//! keeps the dispatching service thin and lets each backend own its specifics (policy
//! evaluation, event emission, replica placement) without forcing the local and clustered
//! stores to share a method signature.

use crate::internal::*;

const SIMPLE_NATIVE_RUNTIME_BINDING: &str = "simple_sandbox";

/// Error surfaced by a [`SandboxBackend`] operation. Wraps the store/filesystem errors and
/// adds a policy-denial case (clustered backends evaluate node policy) plus an escape
/// hatch for host-specific failures a backend cannot express as the above.
#[derive(Debug, thiserror::Error)]
pub enum SandboxBackendError {
    #[error(transparent)]
    Store(#[from] SandboxStoreError),
    #[error(transparent)]
    Fs(#[from] SandboxFsError),
    #[error("sandbox policy denied: {0}")]
    PolicyDenied(String),
    #[error("{0}")]
    Other(String),
}

impl From<sinomo_dfs::DfsPosixError> for SandboxBackendError {
    fn from(error: sinomo_dfs::DfsPosixError) -> Self {
        Self::Other(error.to_string())
    }
}

impl From<crate::ClusteredSandboxLeaseError> for SandboxBackendError {
    fn from(error: crate::ClusteredSandboxLeaseError) -> Self {
        Self::Other(error.to_string())
    }
}

impl From<crate::SandboxRecordedHttpFetchError> for SandboxBackendError {
    fn from(error: crate::SandboxRecordedHttpFetchError) -> Self {
        Self::Other(error.to_string())
    }
}

impl From<crate::SandboxEventEmissionError> for SandboxBackendError {
    fn from(error: crate::SandboxEventEmissionError) -> Self {
        Self::Other(error.to_string())
    }
}

impl From<crate::SandboxToolchainError> for SandboxBackendError {
    fn from(error: crate::SandboxToolchainError) -> Self {
        Self::Other(error.to_string())
    }
}

/// Serves the sandbox control routes against a concrete store. Implemented by the
/// host-agnostic [`LocalSandboxBackend`] and by the component-owned clustered SIFS backend.
///
/// Execution (`run/*`) and mediated HTTP (`http/*`) routes are served separately and are
/// not part of this trait.
pub trait SandboxBackend: Send {
    fn create(
        &mut self,
        request: SandboxCreateInstanceRequest,
    ) -> Result<SandboxCreateInstanceResponse, SandboxBackendError>;

    fn status(
        &mut self,
        request: SandboxStatusRequest,
    ) -> Result<SandboxStatusResponse, SandboxBackendError>;

    fn update(
        &mut self,
        request: SandboxUpdateInstanceRequest,
    ) -> Result<SandboxUpdateInstanceResponse, SandboxBackendError>;

    fn checkpoint(
        &mut self,
        request: SandboxCheckpointRequest,
    ) -> Result<SandboxCheckpointResponse, SandboxBackendError>;

    fn resume(
        &mut self,
        request: SandboxResumeRequest,
    ) -> Result<SandboxResumeResponse, SandboxBackendError>;

    fn reboot(
        &mut self,
        request: SandboxRebootRequest,
    ) -> Result<SandboxRebootResponse, SandboxBackendError>;

    fn destroy(
        &mut self,
        request: SandboxDestroyRequest,
    ) -> Result<SandboxDestroyResponse, SandboxBackendError>;

    fn list(
        &mut self,
        request: SandboxListRequest,
    ) -> Result<SandboxListResponse, SandboxBackendError>;

    fn read(
        &mut self,
        request: SandboxReadRequest,
    ) -> Result<SandboxReadResponse, SandboxBackendError>;

    fn write(
        &mut self,
        request: SandboxWriteRequest,
    ) -> Result<SandboxWriteResponse, SandboxBackendError>;

    fn http_fetch(
        &mut self,
        request: SandboxHttpRequest,
    ) -> Result<SandboxHttpResponse, SandboxBackendError>;

    fn http_fetch_to_file(
        &mut self,
        request: SandboxHttpFetchToFileRequest,
    ) -> Result<SandboxHttpFetchFileResult, SandboxBackendError>;

    fn import_busybox(
        &mut self,
        request: SandboxImportBusyBoxToolchainRequest,
    ) -> Result<SandboxImportBusyBoxToolchainResponse, SandboxBackendError>;

    fn toolchain_add_command(
        &mut self,
        request: SandboxAddToolchainCommandRequest,
    ) -> Result<SandboxAddToolchainCommandResponse, SandboxBackendError>;

    fn toolchain_add_artifact(
        &mut self,
        request: SandboxAddToolchainArtifactRequest,
    ) -> Result<SandboxAddToolchainArtifactResponse, SandboxBackendError>;

    fn import_toolchain_package(
        &mut self,
        request: SandboxImportToolchainPackageRequest,
    ) -> Result<SandboxImportToolchainPackageResponse, SandboxBackendError>;

    fn import_toolchain_package_from_path(
        &mut self,
        request: SandboxImportToolchainPackageFromPathRequest,
    ) -> Result<SandboxImportToolchainPackageResponse, SandboxBackendError>;

    fn run_toolchain(
        &mut self,
        request: SandboxRunToolchainRequest,
    ) -> Result<SandboxLocalCommandOutcome, SandboxBackendError>;

    fn run_toolchain_with_cancellation(
        &mut self,
        request: SandboxRunToolchainRequest,
        cancellation_token: SandboxCancellationToken,
    ) -> Result<SandboxLocalCommandOutcome, SandboxBackendError> {
        let _ = cancellation_token;
        self.run_toolchain(request)
    }

    fn run_toolchain_with_output_sink_and_cancellation(
        &mut self,
        request: SandboxRunToolchainRequest,
        output_sink: Option<std::sync::Arc<dyn SandboxOutputSink>>,
        cancellation_token: SandboxCancellationToken,
    ) -> Result<SandboxLocalCommandOutcome, SandboxBackendError> {
        let outcome = self.run_toolchain_with_cancellation(request, cancellation_token)?;
        if let Some(output_sink) = output_sink {
            for chunk in &outcome.output_chunks {
                output_sink.output_chunk(chunk.clone());
            }
        }
        Ok(outcome)
    }

    fn run_process_plan(
        &mut self,
        request: SandboxRunProcessPlanRequest,
    ) -> Result<SandboxProcessPlanOutcome, SandboxBackendError>;

    fn run_process_plan_with_cancellation(
        &mut self,
        request: SandboxRunProcessPlanRequest,
        cancellation_token: SandboxCancellationToken,
    ) -> Result<SandboxProcessPlanOutcome, SandboxBackendError> {
        let _ = cancellation_token;
        self.run_process_plan(request)
    }

    fn run_process_plan_with_output_sink_and_cancellation(
        &mut self,
        request: SandboxRunProcessPlanRequest,
        output_sink: Option<std::sync::Arc<dyn SandboxOutputSink>>,
        cancellation_token: SandboxCancellationToken,
    ) -> Result<SandboxProcessPlanOutcome, SandboxBackendError> {
        let outcome = self.run_process_plan_with_cancellation(request, cancellation_token)?;
        if let Some(output_sink) = output_sink {
            for step in &outcome.step_outcomes {
                for chunk in &step.output_chunks {
                    output_sink.output_chunk(chunk.clone());
                }
            }
        }
        Ok(outcome)
    }
}

/// Result of applying an optional full mount-set replacement to a sandbox
/// manifest. Mount identity includes source, target, and mode, so changing any
/// of those fields is represented as one removal and one addition.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SandboxMountReconciliation {
    pub replacement_requested: bool,
    pub changed: bool,
    pub added: Vec<SandboxMount>,
    pub removed: Vec<SandboxMount>,
}

/// Apply the wire-level mount replacement semantics without persisting the
/// manifest. Callers validate the resulting manifest before saving it.
pub fn reconcile_sandbox_mounts(
    current: &mut Vec<SandboxMount>,
    replacement: Option<Vec<SandboxMount>>,
) -> SandboxMountReconciliation {
    let Some(replacement) = replacement else {
        return SandboxMountReconciliation::default();
    };
    let previous = std::mem::replace(current, replacement);
    let removed = sandbox_mount_multiset_difference(&previous, current);
    let added = sandbox_mount_multiset_difference(current, &previous);
    SandboxMountReconciliation {
        replacement_requested: true,
        changed: previous != *current,
        added,
        removed,
    }
}

fn sandbox_mount_multiset_difference(
    candidates: &[SandboxMount],
    retained: &[SandboxMount],
) -> Vec<SandboxMount> {
    let mut unmatched = retained.to_vec();
    let mut difference = Vec::new();
    for candidate in candidates {
        if let Some(index) = unmatched.iter().position(|mount| mount == candidate) {
            unmatched.remove(index);
        } else {
            difference.push(candidate.clone());
        }
    }
    difference
}

/// Daemonless sandbox backend backed by the local filesystem store. Works with no cluster
/// (and no daemon); resource limits recorded in the manifest are still enforced on writes.
/// Cluster-only concerns (node policy, cross-node replica placement, the cluster event
/// log) do not apply here.
pub struct LocalSandboxBackend {
    store: LocalSandboxInstanceStore,
    container_runner: Option<SandboxContainerCommandRunner>,
}

impl LocalSandboxBackend {
    /// Build a local backend rooted at `runtime_root` (e.g. `<data-dir>/sandbox/runtime`).
    pub fn new(runtime_root: impl Into<PathBuf>) -> Self {
        Self {
            store: LocalSandboxInstanceStore::new(runtime_root),
            container_runner: None,
        }
    }

    /// Build a local backend over an existing instance store. `LocalSandboxInstanceStore` is a
    /// cheap `Clone` (just a runtime-root path), so callers holding a `&store` can route
    /// through the shared backend without restructuring (e.g. CLI handlers and their tests).
    pub fn with_store(store: LocalSandboxInstanceStore) -> Self {
        Self {
            store,
            container_runner: None,
        }
    }

    pub fn with_container_runner(mut self, runner: SandboxContainerCommandRunner) -> Self {
        self.container_runner = Some(runner);
        self
    }

    pub fn with_container_config(self, config: SandboxContainerBackendConfig) -> Self {
        self.with_container_runner(SandboxContainerCommandRunner::new(config))
    }

    pub fn with_container_config_from_env(self) -> Result<Self, SandboxContainerConfigError> {
        match SandboxContainerBackendConfig::from_env()? {
            Some(config) => Ok(self.with_container_config(config)),
            None => Ok(self),
        }
    }

    fn settle_local_execution(
        &self,
        sandbox_id: &SandboxId,
    ) -> Result<SandboxLocalExecutionLease, SandboxBackendError> {
        let lease = try_acquire_sandbox_local_execution_lease(&self.store, sandbox_id)?;
        recover_sandbox_local_execution_journal_with_lease(
            &self.store,
            &lease,
            sandbox_id,
            OffsetDateTime::now_utc(),
        )?;
        Ok(lease)
    }
}

/// Error for routes the local (daemonless) backend cannot serve — mediated HTTP and
/// toolchain management require a daemon.
fn local_mode_unsupported(operation: &str) -> SandboxBackendError {
    SandboxBackendError::Other(format!(
        "sandbox {operation} requires a daemon and is not available in local (daemonless) mode"
    ))
}

impl SandboxBackend for LocalSandboxBackend {
    fn create(
        &mut self,
        request: SandboxCreateInstanceRequest,
    ) -> Result<SandboxCreateInstanceResponse, SandboxBackendError> {
        let sandbox_id = request.sandbox_id.unwrap_or_else(SandboxId::generate);
        let lease = try_acquire_sandbox_local_execution_lease(&self.store, &sandbox_id)?;
        let (mut manifest, _layout) = self.store.create_under_local_execution_lease(
            &lease,
            sandbox_id,
            request.toolchain_id,
            OffsetDateTime::now_utc(),
        )?;
        manifest.temp_policy = request.temp_policy;
        if let Some(memory_mb) = request.memory_mb {
            manifest.limits.memory_mb = memory_mb;
        }
        if let Some(wall_time_ms) = request.wall_time_ms {
            manifest.limits.wall_time_ms = wall_time_ms;
        }
        if let Some(max_input_bytes) = request.max_input_bytes {
            manifest.limits.max_input_bytes = max_input_bytes;
        }
        if let Some(max_output_bytes) = request.max_output_bytes {
            manifest.limits.max_output_bytes = max_output_bytes;
        }
        if let Some(max_workspace_bytes) = request.max_workspace_bytes {
            manifest.limits.max_workspace_bytes = max_workspace_bytes;
        }
        if let Some(max_written_bytes_per_command) = request.max_written_bytes_per_command {
            manifest.limits.max_written_bytes_per_command = max_written_bytes_per_command;
        }
        if let Some(max_file_count) = request.max_file_count {
            manifest.limits.max_file_count = max_file_count;
        }
        manifest.mounts = request.mounts;
        self.store.save_manifest(&manifest)?;
        Ok(SandboxCreateInstanceResponse { manifest })
    }

    fn status(
        &mut self,
        request: SandboxStatusRequest,
    ) -> Result<SandboxStatusResponse, SandboxBackendError> {
        let _lease = self.settle_local_execution(&request.sandbox_id)?;
        let (manifest, _layout) = self.store.load(&request.sandbox_id)?;
        Ok(SandboxStatusResponse { manifest })
    }

    fn update(
        &mut self,
        request: SandboxUpdateInstanceRequest,
    ) -> Result<SandboxUpdateInstanceResponse, SandboxBackendError> {
        let SandboxUpdateInstanceRequest {
            sandbox_id,
            cwd,
            mounts,
            env_set,
            env_unset,
        } = request;
        let _lease = self.settle_local_execution(&sandbox_id)?;
        let (mut manifest, layout) = self.store.load(&sandbox_id)?;
        for key in env_set.keys().chain(env_unset.iter()) {
            if !sandbox_env_name_is_valid(key) {
                return Err(SandboxBackendError::Other(format!(
                    "invalid sandbox env name `{key}`"
                )));
            }
        }
        for (key, value) in &env_set {
            if value.contains('\0') {
                return Err(SandboxBackendError::Other(format!(
                    "sandbox env `{key}` contains an embedded NUL byte"
                )));
            }
        }
        let mounts_reconciliation = reconcile_sandbox_mounts(&mut manifest.mounts, mounts);
        let effective_cwd = cwd.as_ref().unwrap_or(&manifest.cwd);
        // The simple-native adapter records its real host cwd in the manifest.
        // A mount-only update must preserve that runtime-owned value instead
        // of interpreting it through the guest VFS. Explicit cwd changes (and
        // every other runtime) continue through normal guest validation.
        let preserves_simple_native_host_cwd = cwd.is_none()
            && mounts_reconciliation.replacement_requested
            && manifest
                .runtime_binding
                .as_ref()
                .is_some_and(|binding| binding.runtime == SIMPLE_NATIVE_RUNTIME_BINDING);
        if (cwd.is_some() || mounts_reconciliation.replacement_requested)
            && !preserves_simple_native_host_cwd
        {
            let fs = LocalSandboxFileSystem::new(layout);
            validate_sandbox_working_directory(&fs, effective_cwd)?;
        }
        if let Some(cwd) = cwd {
            manifest.cwd = cwd;
        }
        for key in env_unset {
            manifest.env.remove(&key);
        }
        manifest.env.extend(env_set);
        manifest
            .advance_generation()
            .map_err(SandboxStoreError::from)?;
        manifest.updated_at = OffsetDateTime::now_utc();
        self.store.save_manifest(&manifest)?;
        Ok(SandboxUpdateInstanceResponse { manifest })
    }

    fn checkpoint(
        &mut self,
        request: SandboxCheckpointRequest,
    ) -> Result<SandboxCheckpointResponse, SandboxBackendError> {
        let _lease = self.settle_local_execution(&request.sandbox_id)?;
        let checkpoint = self
            .store
            .create_checkpoint(&request.sandbox_id, OffsetDateTime::now_utc())?;
        Ok(SandboxCheckpointResponse { checkpoint })
    }

    fn resume(
        &mut self,
        request: SandboxResumeRequest,
    ) -> Result<SandboxResumeResponse, SandboxBackendError> {
        let _lease = self.settle_local_execution(&request.sandbox_id)?;
        let (manifest, _layout) = self.store.resume_checkpoint(
            &request.sandbox_id,
            &request.checkpoint_id,
            OffsetDateTime::now_utc(),
        )?;
        Ok(SandboxResumeResponse { manifest })
    }

    fn reboot(
        &mut self,
        request: SandboxRebootRequest,
    ) -> Result<SandboxRebootResponse, SandboxBackendError> {
        let _lease = self.settle_local_execution(&request.sandbox_id)?;
        let (manifest, _layout, cleared_tmp) = self
            .store
            .reboot_session(&request.sandbox_id, OffsetDateTime::now_utc())?;
        Ok(SandboxRebootResponse {
            manifest,
            cleared_tmp,
        })
    }

    fn destroy(
        &mut self,
        request: SandboxDestroyRequest,
    ) -> Result<SandboxDestroyResponse, SandboxBackendError> {
        let _lease = self.settle_local_execution(&request.sandbox_id)?;
        self.store
            .destroy_under_local_execution_lease(&_lease, &request.sandbox_id)?;
        Ok(SandboxDestroyResponse {
            sandbox_id: request.sandbox_id,
        })
    }

    fn list(
        &mut self,
        request: SandboxListRequest,
    ) -> Result<SandboxListResponse, SandboxBackendError> {
        let _lease = self.settle_local_execution(&request.sandbox_id)?;
        let (_manifest, layout) = self.store.load(&request.sandbox_id)?;
        let fs = LocalSandboxFileSystem::new(layout);
        let entries = fs.list(&request.path)?;
        Ok(SandboxListResponse { entries })
    }

    fn read(
        &mut self,
        request: SandboxReadRequest,
    ) -> Result<SandboxReadResponse, SandboxBackendError> {
        let _lease = self.settle_local_execution(&request.sandbox_id)?;
        let (_manifest, layout) = self.store.load(&request.sandbox_id)?;
        let fs = LocalSandboxFileSystem::new(layout);
        let bytes = fs.read(&request.path, request.range)?;
        Ok(SandboxReadResponse { bytes })
    }

    fn write(
        &mut self,
        request: SandboxWriteRequest,
    ) -> Result<SandboxWriteResponse, SandboxBackendError> {
        let _lease = self.settle_local_execution(&request.sandbox_id)?;
        let (mut manifest, layout) = self.store.load(&request.sandbox_id)?;
        let mut fs = LocalSandboxFileSystem::new(layout);
        validate_sandbox_write_resource_limits(
            &fs,
            &request.path,
            request.bytes.len() as u64,
            &manifest.limits,
        )?;
        fs.write(&request.path, &request.bytes)?;
        manifest
            .advance_generation()
            .map_err(SandboxStoreError::from)?;
        manifest.updated_at = OffsetDateTime::now_utc();
        self.store.save_manifest(&manifest)?;
        let bytes_written = request.bytes.len() as u64;
        Ok(SandboxWriteResponse {
            path: request.path,
            bytes_written,
        })
    }

    fn http_fetch(
        &mut self,
        _request: SandboxHttpRequest,
    ) -> Result<SandboxHttpResponse, SandboxBackendError> {
        Err(local_mode_unsupported("http fetch"))
    }

    fn http_fetch_to_file(
        &mut self,
        _request: SandboxHttpFetchToFileRequest,
    ) -> Result<SandboxHttpFetchFileResult, SandboxBackendError> {
        Err(local_mode_unsupported("http fetch-to-file"))
    }

    fn import_busybox(
        &mut self,
        _request: SandboxImportBusyBoxToolchainRequest,
    ) -> Result<SandboxImportBusyBoxToolchainResponse, SandboxBackendError> {
        Err(local_mode_unsupported("toolchain import-busybox"))
    }

    fn toolchain_add_command(
        &mut self,
        _request: SandboxAddToolchainCommandRequest,
    ) -> Result<SandboxAddToolchainCommandResponse, SandboxBackendError> {
        Err(local_mode_unsupported("toolchain add-command"))
    }

    fn toolchain_add_artifact(
        &mut self,
        _request: SandboxAddToolchainArtifactRequest,
    ) -> Result<SandboxAddToolchainArtifactResponse, SandboxBackendError> {
        Err(local_mode_unsupported("toolchain add-artifact"))
    }

    fn import_toolchain_package(
        &mut self,
        _request: SandboxImportToolchainPackageRequest,
    ) -> Result<SandboxImportToolchainPackageResponse, SandboxBackendError> {
        Err(local_mode_unsupported("toolchain import-package"))
    }

    fn import_toolchain_package_from_path(
        &mut self,
        _request: SandboxImportToolchainPackageFromPathRequest,
    ) -> Result<SandboxImportToolchainPackageResponse, SandboxBackendError> {
        Err(local_mode_unsupported("toolchain import-package-from-path"))
    }

    fn run_toolchain(
        &mut self,
        request: SandboxRunToolchainRequest,
    ) -> Result<SandboxLocalCommandOutcome, SandboxBackendError> {
        self.run_toolchain_with_cancellation(request, SandboxCancellationToken::new())
    }

    fn run_toolchain_with_cancellation(
        &mut self,
        request: SandboxRunToolchainRequest,
        cancellation_token: SandboxCancellationToken,
    ) -> Result<SandboxLocalCommandOutcome, SandboxBackendError> {
        let Some(runner) = &self.container_runner else {
            return Err(local_mode_unsupported("toolchain execution"));
        };
        runner
            .run_toolchain_command_with_stdin_and_output_sink_and_cancellation(
                &self.store,
                &request.sandbox_id,
                &request.command_name,
                request.argv,
                request.stdin,
                request.timeout_ms,
                None,
                cancellation_token,
            )
            .map_err(|error| SandboxBackendError::Other(error.to_string()))
    }

    fn run_toolchain_with_output_sink_and_cancellation(
        &mut self,
        request: SandboxRunToolchainRequest,
        output_sink: Option<std::sync::Arc<dyn SandboxOutputSink>>,
        cancellation_token: SandboxCancellationToken,
    ) -> Result<SandboxLocalCommandOutcome, SandboxBackendError> {
        let Some(runner) = &self.container_runner else {
            return Err(local_mode_unsupported("toolchain execution"));
        };
        runner
            .run_toolchain_command_with_stdin_and_output_sink_and_cancellation(
                &self.store,
                &request.sandbox_id,
                &request.command_name,
                request.argv,
                request.stdin,
                request.timeout_ms,
                output_sink,
                cancellation_token,
            )
            .map_err(|error| SandboxBackendError::Other(error.to_string()))
    }

    fn run_process_plan(
        &mut self,
        request: SandboxRunProcessPlanRequest,
    ) -> Result<SandboxProcessPlanOutcome, SandboxBackendError> {
        self.run_process_plan_with_cancellation(request, SandboxCancellationToken::new())
    }

    fn run_process_plan_with_cancellation(
        &mut self,
        request: SandboxRunProcessPlanRequest,
        cancellation_token: SandboxCancellationToken,
    ) -> Result<SandboxProcessPlanOutcome, SandboxBackendError> {
        let Some(runner) = &self.container_runner else {
            return Err(local_mode_unsupported("process-plan execution"));
        };
        runner
            .run_process_plan_with_output_sink_and_cancellation(
                &self.store,
                &request.sandbox_id,
                &request.plan,
                request.timeout_ms,
                None,
                cancellation_token,
            )
            .map_err(|error| SandboxBackendError::Other(error.to_string()))
    }

    fn run_process_plan_with_output_sink_and_cancellation(
        &mut self,
        request: SandboxRunProcessPlanRequest,
        output_sink: Option<std::sync::Arc<dyn SandboxOutputSink>>,
        cancellation_token: SandboxCancellationToken,
    ) -> Result<SandboxProcessPlanOutcome, SandboxBackendError> {
        let Some(runner) = &self.container_runner else {
            return Err(local_mode_unsupported("process-plan execution"));
        };
        runner
            .run_process_plan_with_output_sink_and_cancellation(
                &self.store,
                &request.sandbox_id,
                &request.plan,
                request.timeout_ms,
                output_sink,
                cancellation_token,
            )
            .map_err(|error| SandboxBackendError::Other(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        LocalSandboxBackend, SIMPLE_NATIVE_RUNTIME_BINDING, SandboxBackend, SandboxBackendError,
        reconcile_sandbox_mounts,
    };
    use crate::internal::*;

    fn seed_guest_active_local_journal(
        store: &LocalSandboxInstanceStore,
        sandbox_id: &SandboxId,
        path: &SandboxPath,
        partial_bytes: &[u8],
    ) -> SandboxLocalExecutionLease {
        let (manifest, layout) = store.load(sandbox_id).expect("load sandbox");
        let lease = try_acquire_sandbox_local_execution_lease(store, sandbox_id)
            .expect("acquire execution lease");
        let mut journal = begin_sandbox_local_execution_journal(
            store,
            &lease,
            &manifest,
            OffsetDateTime::now_utc(),
        )
        .expect("begin local journal");
        activate_sandbox_local_execution_journal(
            store,
            &lease,
            &mut journal,
            OffsetDateTime::now_utc(),
        )
        .expect("activate local journal");
        let mut fs = LocalSandboxFileSystem::new(layout);
        fs.write(path, partial_bytes).expect("write partial effect");
        lease
    }

    /// Full daemonless round trip: create a sandbox on the local filesystem store, write a
    /// file, read it back, see it in a directory listing, then destroy the instance — all
    /// with no cluster and no daemon.
    #[test]
    fn local_backend_round_trips_without_a_cluster() {
        let root = sinomo_testdir::TestDir::new("sinomo-local-sandbox-backend");
        let mut backend = LocalSandboxBackend::new(&root);

        let created = backend
            .create(SandboxCreateInstanceRequest {
                sandbox_id: None,
                toolchain_id: SandboxToolchainId::from_legacy_named("toolchain_test")
                    .expect("toolchain id"),
                temp_policy: SandboxTempPolicy::RetainAcrossSessions,
                memory_mb: None,
                wall_time_ms: None,
                max_input_bytes: None,
                max_output_bytes: None,
                max_workspace_bytes: None,
                max_written_bytes_per_command: None,
                max_file_count: None,
                mounts: Vec::new(),
                portability: None,
            })
            .expect("create");
        let sandbox_id = created.manifest.sandbox_id.clone();

        let status = backend
            .status(SandboxStatusRequest {
                sandbox_id: sandbox_id.clone(),
            })
            .expect("status");
        assert_eq!(status.manifest.sandbox_id, sandbox_id);

        let path = SandboxPath::new("/work/hello.txt").expect("path");
        let write = backend
            .write(SandboxWriteRequest {
                sandbox_id: sandbox_id.clone(),
                path: path.clone(),
                bytes: b"hi".to_vec(),
            })
            .expect("write");
        assert_eq!(write.bytes_written, 2);

        let read = backend
            .read(SandboxReadRequest {
                sandbox_id: sandbox_id.clone(),
                path: path.clone(),
                range: None,
            })
            .expect("read");
        assert_eq!(read.bytes, b"hi".to_vec());

        let listing = backend
            .list(SandboxListRequest {
                sandbox_id: sandbox_id.clone(),
                path: SandboxPath::new("/work").expect("path"),
            })
            .expect("list");
        assert!(listing.entries.iter().any(|entry| entry.path == path));

        backend
            .destroy(SandboxDestroyRequest {
                sandbox_id: sandbox_id.clone(),
            })
            .expect("destroy");
        assert!(
            backend.status(SandboxStatusRequest { sandbox_id }).is_err(),
            "status should fail after destroy"
        );
    }

    #[test]
    fn local_backend_update_replaces_preserves_and_clears_mounts() {
        let root = sinomo_testdir::TestDir::new("sinomo-local-sandbox-update-mounts");
        let mut backend = LocalSandboxBackend::new(&root);
        let initial_mount = SandboxMount {
            source_sifs_path: "/services/sandbox-shared/initial".to_string(),
            mount_path: SandboxPath::new("/work/initial").expect("initial mount path"),
            mode: SandboxMountMode::ReadOnly,
        };
        let replacement_mount = SandboxMount {
            source_sifs_path: "/services/sandbox-shared/replacement".to_string(),
            mount_path: SandboxPath::new("/work/replacement").expect("replacement mount path"),
            mode: SandboxMountMode::ReadWrite,
        };
        let created = backend
            .create(SandboxCreateInstanceRequest {
                sandbox_id: None,
                toolchain_id: SandboxToolchainId::from_legacy_named("mount_update_toolchain")
                    .expect("toolchain id"),
                temp_policy: SandboxTempPolicy::RetainAcrossSessions,
                memory_mb: None,
                wall_time_ms: None,
                max_input_bytes: None,
                max_output_bytes: None,
                max_workspace_bytes: None,
                max_written_bytes_per_command: None,
                max_file_count: None,
                mounts: vec![initial_mount.clone()],
                portability: None,
            })
            .expect("create sandbox");
        let sandbox_id = created.manifest.sandbox_id;

        let preserved = backend
            .update(SandboxUpdateInstanceRequest {
                sandbox_id: sandbox_id.clone(),
                cwd: None,
                mounts: None,
                env_set: BTreeMap::new(),
                env_unset: Vec::new(),
            })
            .expect("preserve mounts");
        assert_eq!(preserved.manifest.mounts, vec![initial_mount.clone()]);

        let replaced = backend
            .update(SandboxUpdateInstanceRequest {
                sandbox_id: sandbox_id.clone(),
                cwd: None,
                mounts: Some(vec![replacement_mount.clone()]),
                env_set: BTreeMap::new(),
                env_unset: Vec::new(),
            })
            .expect("replace mounts");
        assert_eq!(replaced.manifest.mounts, vec![replacement_mount]);
        assert_eq!(
            replaced.manifest.generation,
            preserved.manifest.generation + 1
        );

        let cleared = backend
            .update(SandboxUpdateInstanceRequest {
                sandbox_id,
                cwd: None,
                mounts: Some(Vec::new()),
                env_set: BTreeMap::new(),
                env_unset: Vec::new(),
            })
            .expect("clear mounts");
        assert!(cleared.manifest.mounts.is_empty());
        assert_eq!(
            cleared.manifest.generation,
            replaced.manifest.generation + 1
        );
    }

    #[test]
    fn local_backend_mount_update_preserves_simple_native_host_cwd() {
        let root = sinomo_testdir::TestDir::new("sinomo-local-sandbox-simple-native-cwd");
        let runtime_root = root.join("runtime");
        let host_workspace = root.join("host-workspace");
        std::fs::create_dir_all(&host_workspace).expect("host workspace");
        let store = LocalSandboxInstanceStore::new(&runtime_root);
        let mut backend = LocalSandboxBackend::with_store(store.clone());
        let created = backend
            .create(SandboxCreateInstanceRequest {
                sandbox_id: None,
                toolchain_id: SandboxToolchainId::from_legacy_named(
                    "simple_native_cwd_update_toolchain",
                )
                .expect("toolchain id"),
                temp_policy: SandboxTempPolicy::RetainAcrossSessions,
                memory_mb: None,
                wall_time_ms: None,
                max_input_bytes: None,
                max_output_bytes: None,
                max_workspace_bytes: None,
                max_written_bytes_per_command: None,
                max_file_count: None,
                mounts: Vec::new(),
                portability: None,
            })
            .expect("create sandbox");
        let sandbox_id = created.manifest.sandbox_id.clone();
        let mut manifest = created.manifest;
        manifest.cwd = SandboxPath::new(host_workspace.display().to_string()).expect("host cwd");
        manifest.runtime_binding = Some(SandboxRuntimeBinding {
            runtime: SIMPLE_NATIVE_RUNTIME_BINDING.to_string(),
            node_id: NodeId::from_legacy_named("simple-native-cwd-node").expect("node id"),
            portability: SandboxInstancePortability::NonPortable,
        });
        store.save_manifest(&manifest).expect("persist runtime cwd");

        let mount = SandboxMount {
            source_sifs_path: "/conversations/test/workspaces/project".to_string(),
            mount_path: SandboxPath::new("/work").expect("mount path"),
            mode: SandboxMountMode::ReadWrite,
        };
        let updated = backend
            .update(SandboxUpdateInstanceRequest {
                sandbox_id: sandbox_id.clone(),
                cwd: None,
                mounts: Some(vec![mount.clone()]),
                env_set: BTreeMap::new(),
                env_unset: Vec::new(),
            })
            .expect("replace mounts while preserving simple-native host cwd");
        assert_eq!(updated.manifest.cwd, manifest.cwd);
        assert_eq!(updated.manifest.mounts, vec![mount]);

        let invalid_guest_cwd = SandboxPath::new("/missing-guest-cwd").expect("invalid guest cwd");
        let explicit_cwd_error = backend
            .update(SandboxUpdateInstanceRequest {
                sandbox_id: sandbox_id.clone(),
                cwd: Some(invalid_guest_cwd.clone()),
                mounts: None,
                env_set: BTreeMap::new(),
                env_unset: Vec::new(),
            })
            .expect_err("explicit cwd update must use guest filesystem validation");
        assert!(
            matches!(
                &explicit_cwd_error,
                SandboxBackendError::Fs(SandboxFsError::NotFound { path })
                    if path == &invalid_guest_cwd
            ),
            "unexpected explicit cwd error: {explicit_cwd_error}"
        );

        let (mut non_simple_manifest, _) = store.load(&sandbox_id).expect("reload manifest");
        non_simple_manifest
            .runtime_binding
            .as_mut()
            .expect("runtime binding")
            .runtime = "wasix".to_string();
        store
            .save_manifest(&non_simple_manifest)
            .expect("persist non-simple runtime binding");
        let mount_only_error = backend
            .update(SandboxUpdateInstanceRequest {
                sandbox_id,
                cwd: None,
                mounts: Some(Vec::new()),
                env_set: BTreeMap::new(),
                env_unset: Vec::new(),
            })
            .expect_err("other runtimes must validate a preserved cwd as a guest path");
        assert!(
            matches!(
                &mount_only_error,
                SandboxBackendError::Fs(SandboxFsError::NotFound { path })
                    if path == &manifest.cwd
            ),
            "unexpected non-simple mount update error: {mount_only_error}"
        );
    }

    #[test]
    fn mount_reconciliation_reports_exact_additions_and_removals() {
        let retained = SandboxMount {
            source_sifs_path: "/services/sandbox-shared/retained".to_string(),
            mount_path: SandboxPath::new("/retained").expect("retained path"),
            mode: SandboxMountMode::ReadOnly,
        };
        let removed = SandboxMount {
            source_sifs_path: "/services/sandbox-shared/changed".to_string(),
            mount_path: SandboxPath::new("/changed").expect("changed path"),
            mode: SandboxMountMode::ReadOnly,
        };
        let added = SandboxMount {
            mode: SandboxMountMode::ReadWrite,
            ..removed.clone()
        };
        let mut current = vec![retained.clone(), removed.clone()];

        let report =
            reconcile_sandbox_mounts(&mut current, Some(vec![retained.clone(), added.clone()]));

        assert!(report.replacement_requested);
        assert!(report.changed);
        assert_eq!(report.removed, vec![removed]);
        assert_eq!(report.added, vec![added.clone()]);
        assert_eq!(current, vec![retained, added]);
    }

    #[test]
    fn mount_reconciliation_preserves_order_only_and_duplicate_evidence() {
        let first = SandboxMount {
            source_sifs_path: "/services/sandbox-shared/first".to_string(),
            mount_path: SandboxPath::new("/first").expect("first path"),
            mode: SandboxMountMode::ReadOnly,
        };
        let second = SandboxMount {
            source_sifs_path: "/services/sandbox-shared/second".to_string(),
            mount_path: SandboxPath::new("/second").expect("second path"),
            mode: SandboxMountMode::ReadOnly,
        };
        let mut current = vec![first.clone(), second.clone()];

        let reordered =
            reconcile_sandbox_mounts(&mut current, Some(vec![second.clone(), first.clone()]));
        assert!(reordered.changed);
        assert!(reordered.added.is_empty());
        assert!(reordered.removed.is_empty());

        current = vec![first.clone(), first.clone()];
        let deduplicated = reconcile_sandbox_mounts(&mut current, Some(vec![first.clone()]));
        assert!(deduplicated.changed);
        assert_eq!(deduplicated.removed, vec![first.clone()]);
        assert!(deduplicated.added.is_empty());
        assert_eq!(current, vec![first]);
    }

    #[test]
    fn local_backend_settles_guest_active_marker_before_read_write_and_status() {
        let root = sinomo_testdir::TestDir::new("sinomo-local-backend-journal-fence");
        let store = LocalSandboxInstanceStore::new(root.join("runtime"));
        let mut backend = LocalSandboxBackend::with_store(store.clone());
        let created = backend
            .create(SandboxCreateInstanceRequest {
                sandbox_id: None,
                toolchain_id: SandboxToolchainId::from_legacy_named("txn_toolchain")
                    .expect("toolchain"),
                temp_policy: SandboxTempPolicy::RetainAcrossSessions,
                memory_mb: None,
                wall_time_ms: None,
                max_input_bytes: None,
                max_output_bytes: None,
                max_workspace_bytes: None,
                max_written_bytes_per_command: None,
                max_file_count: None,
                mounts: Vec::new(),
                portability: None,
            })
            .expect("create sandbox");
        let sandbox_id = created.manifest.sandbox_id;
        let path = SandboxPath::new("/work/state.txt").expect("path");
        backend
            .write(SandboxWriteRequest {
                sandbox_id: sandbox_id.clone(),
                path: path.clone(),
                bytes: b"baseline".to_vec(),
            })
            .expect("write baseline");

        seed_guest_active_local_journal(&store, &sandbox_id, &path, b"partial-read");
        assert_eq!(
            backend
                .read(SandboxReadRequest {
                    sandbox_id: sandbox_id.clone(),
                    path: path.clone(),
                    range: None,
                })
                .expect("read settles journal")
                .bytes,
            b"partial-read"
        );

        seed_guest_active_local_journal(&store, &sandbox_id, &path, b"partial-write");
        backend
            .write(SandboxWriteRequest {
                sandbox_id: sandbox_id.clone(),
                path: path.clone(),
                bytes: b"committed-write".to_vec(),
            })
            .expect("write settles journal first");

        seed_guest_active_local_journal(&store, &sandbox_id, &path, b"partial-status");
        backend
            .status(SandboxStatusRequest {
                sandbox_id: sandbox_id.clone(),
            })
            .expect("status settles journal");
        assert_eq!(
            backend
                .read(SandboxReadRequest {
                    sandbox_id,
                    path,
                    range: None,
                })
                .expect("read committed state")
                .bytes,
            b"partial-status"
        );
    }

    #[test]
    fn local_backend_surfaces_busy_without_rolling_back_a_live_guest() {
        let root = sinomo_testdir::TestDir::new("sinomo-local-backend-live-journal");
        let store = LocalSandboxInstanceStore::new(root.join("runtime"));
        let mut backend = LocalSandboxBackend::with_store(store.clone());
        let created = backend
            .create(SandboxCreateInstanceRequest {
                sandbox_id: None,
                toolchain_id: SandboxToolchainId::from_legacy_named("live_txn_toolchain")
                    .expect("toolchain"),
                temp_policy: SandboxTempPolicy::RetainAcrossSessions,
                memory_mb: None,
                wall_time_ms: None,
                max_input_bytes: None,
                max_output_bytes: None,
                max_workspace_bytes: None,
                max_written_bytes_per_command: None,
                max_file_count: None,
                mounts: Vec::new(),
                portability: None,
            })
            .expect("create sandbox");
        let sandbox_id = created.manifest.sandbox_id;
        let path = SandboxPath::new("/work/live.txt").expect("path");
        backend
            .write(SandboxWriteRequest {
                sandbox_id: sandbox_id.clone(),
                path: path.clone(),
                bytes: b"baseline".to_vec(),
            })
            .expect("write baseline");

        let live_lease =
            seed_guest_active_local_journal(&store, &sandbox_id, &path, b"live-effect");
        for error in [
            backend
                .status(SandboxStatusRequest {
                    sandbox_id: sandbox_id.clone(),
                })
                .expect_err("status must not recover a live guest"),
            backend
                .read(SandboxReadRequest {
                    sandbox_id: sandbox_id.clone(),
                    path: path.clone(),
                    range: None,
                })
                .expect_err("read must not recover a live guest"),
        ] {
            assert!(
                error.to_string().contains("active local execution owner"),
                "unexpected busy error: {error}"
            );
        }
        let raw_fs = LocalSandboxFileSystem::new(store.layout(&sandbox_id));
        assert_eq!(
            raw_fs.read(&path, None).expect("read live state directly"),
            b"live-effect",
            "observer must not roll back a live owner"
        );

        drop(live_lease);
        backend
            .status(SandboxStatusRequest {
                sandbox_id: sandbox_id.clone(),
            })
            .expect("status recovers after owner disappears");
        assert_eq!(
            backend
                .read(SandboxReadRequest {
                    sandbox_id,
                    path,
                    range: None,
                })
                .expect("read preserved guest state")
                .bytes,
            b"live-effect"
        );
    }

    #[test]
    fn persistent_lock_anchor_fences_destroy_and_explicit_id_recreate() {
        let root = sinomo_testdir::TestDir::new("sinomo-local-backend-destroy-recreate-lock");
        let store = LocalSandboxInstanceStore::new(root.join("runtime"));
        let mut first_backend = LocalSandboxBackend::with_store(store.clone());
        let mut second_backend = LocalSandboxBackend::with_store(store.clone());
        let sandbox_id = SandboxId::from_legacy_named("destroy_recreate_lock").expect("sandbox");
        first_backend
            .create(SandboxCreateInstanceRequest {
                sandbox_id: Some(sandbox_id.clone()),
                toolchain_id: SandboxToolchainId::from_legacy_named("lock_toolchain")
                    .expect("toolchain"),
                temp_policy: SandboxTempPolicy::RetainAcrossSessions,
                memory_mb: None,
                wall_time_ms: None,
                max_input_bytes: None,
                max_output_bytes: None,
                max_workspace_bytes: None,
                max_written_bytes_per_command: None,
                max_file_count: None,
                mounts: Vec::new(),
                portability: None,
            })
            .expect("create sandbox");
        let live_lease =
            try_acquire_sandbox_local_execution_lease(&store, &sandbox_id).expect("live lease");

        let destroy_error = second_backend
            .destroy(SandboxDestroyRequest {
                sandbox_id: sandbox_id.clone(),
            })
            .expect_err("destroy must be fenced by the persistent anchor");
        assert!(
            destroy_error
                .to_string()
                .contains("active local execution owner")
        );
        let create_error = second_backend
            .create(SandboxCreateInstanceRequest {
                sandbox_id: Some(sandbox_id.clone()),
                toolchain_id: SandboxToolchainId::from_legacy_named("lock_toolchain")
                    .expect("toolchain"),
                temp_policy: SandboxTempPolicy::RetainAcrossSessions,
                memory_mb: None,
                wall_time_ms: None,
                max_input_bytes: None,
                max_output_bytes: None,
                max_workspace_bytes: None,
                max_written_bytes_per_command: None,
                max_file_count: None,
                mounts: Vec::new(),
                portability: None,
            })
            .expect_err("explicit-ID create must contend before inspecting the instance");
        assert!(
            create_error
                .to_string()
                .contains("active local execution owner")
        );

        drop(live_lease);
        first_backend
            .destroy(SandboxDestroyRequest {
                sandbox_id: sandbox_id.clone(),
            })
            .expect("destroy after owner release");
        second_backend
            .create(SandboxCreateInstanceRequest {
                sandbox_id: Some(sandbox_id.clone()),
                toolchain_id: SandboxToolchainId::from_legacy_named("lock_toolchain")
                    .expect("toolchain"),
                temp_policy: SandboxTempPolicy::RetainAcrossSessions,
                memory_mb: None,
                wall_time_ms: None,
                max_input_bytes: None,
                max_output_bytes: None,
                max_workspace_bytes: None,
                max_written_bytes_per_command: None,
                max_file_count: None,
                mounts: Vec::new(),
                portability: None,
            })
            .expect("recreate after owner release");
    }
}
