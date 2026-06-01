//! Runtime-layer sandbox backend: a [`SandboxBackend`] that runs **daemonless**, with real
//! WASM execution.
//!
//! The host-agnostic `LocalSandboxBackend` (in `sinomo-sandbox`) covers filesystem and
//! lifecycle routes but cannot execute — `sinomo-sandbox` deliberately has no dependency on
//! this runtime crate. `RuntimeSandboxBackend` lives here (where the WASM runner is linked),
//! wraps a `LocalSandboxBackend` for the fs/lifecycle/toolchain-store routes, and adds the
//! execution + busybox-import routes that need the runtime. It lets a host (the CLI) serve
//! the full sandbox seam locally — create → import-busybox → write → run → read — with no
//! daemon and no cluster.
//!
//! Not yet served locally: `run_process_plan` (no local plan-execution primitive — the
//! daemon stages to a local store and orchestrates), the non-busybox toolchain routes, and
//! mediated HTTP; these delegate to `LocalSandboxBackend` (which reports them unsupported).

use std::path::PathBuf;

use sinomo_node::NodeId;
use time::OffsetDateTime;

use crate::*;

fn runtime_backend_error(error: impl std::fmt::Display) -> SandboxBackendError {
    SandboxBackendError::Other(error.to_string())
}

/// Daemonless sandbox backend with real local execution. Construct via [`Self::new`] with
/// the instance `runtime_root` (e.g. `<data-dir>/sandbox/runtime`) and the toolchain
/// `cache_root` (e.g. `<data-dir>/sandbox/cache`).
pub struct RuntimeSandboxBackend {
    local: LocalSandboxBackend,
    runtime_root: PathBuf,
    cache_root: PathBuf,
    node_id: NodeId,
}

impl RuntimeSandboxBackend {
    pub fn new(
        runtime_root: impl Into<PathBuf>,
        cache_root: impl Into<PathBuf>,
    ) -> Result<Self, SandboxBackendError> {
        let runtime_root = runtime_root.into();
        let node_id = NodeId::from_legacy_named("sandbox_local")
            .map_err(|error| runtime_backend_error(format!("sandbox runtime node id: {error}")))?;
        Ok(Self {
            local: LocalSandboxBackend::new(runtime_root.clone()),
            runtime_root,
            cache_root: cache_root.into(),
            node_id,
        })
    }
}

impl SandboxBackend for RuntimeSandboxBackend {
    // --- filesystem + lifecycle: delegate to the host-agnostic local backend ---

    fn create(
        &mut self,
        request: SandboxCreateInstanceRequest,
    ) -> Result<SandboxCreateInstanceResponse, SandboxBackendError> {
        self.local.create(request)
    }

    fn status(
        &mut self,
        request: SandboxStatusRequest,
    ) -> Result<SandboxStatusResponse, SandboxBackendError> {
        self.local.status(request)
    }

    fn update(
        &mut self,
        request: SandboxUpdateInstanceRequest,
    ) -> Result<SandboxUpdateInstanceResponse, SandboxBackendError> {
        self.local.update(request)
    }

    fn checkpoint(
        &mut self,
        request: SandboxCheckpointRequest,
    ) -> Result<SandboxCheckpointResponse, SandboxBackendError> {
        self.local.checkpoint(request)
    }

    fn resume(
        &mut self,
        request: SandboxResumeRequest,
    ) -> Result<SandboxResumeResponse, SandboxBackendError> {
        self.local.resume(request)
    }

    fn reboot(
        &mut self,
        request: SandboxRebootRequest,
    ) -> Result<SandboxRebootResponse, SandboxBackendError> {
        self.local.reboot(request)
    }

    fn destroy(
        &mut self,
        request: SandboxDestroyRequest,
    ) -> Result<SandboxDestroyResponse, SandboxBackendError> {
        self.local.destroy(request)
    }

    fn list(
        &mut self,
        request: SandboxListRequest,
    ) -> Result<SandboxListResponse, SandboxBackendError> {
        self.local.list(request)
    }

    fn read(
        &mut self,
        request: SandboxReadRequest,
    ) -> Result<SandboxReadResponse, SandboxBackendError> {
        self.local.read(request)
    }

    fn write(
        &mut self,
        request: SandboxWriteRequest,
    ) -> Result<SandboxWriteResponse, SandboxBackendError> {
        self.local.write(request)
    }

    // --- mediated HTTP: not available daemonless ---

    fn http_fetch(
        &mut self,
        request: SandboxHttpRequest,
    ) -> Result<SandboxHttpResponse, SandboxBackendError> {
        self.local.http_fetch(request)
    }

    fn http_fetch_to_file(
        &mut self,
        request: SandboxHttpFetchToFileRequest,
    ) -> Result<SandboxHttpFetchFileResult, SandboxBackendError> {
        self.local.http_fetch_to_file(request)
    }

    // --- toolchain: busybox import is real (store-only); the rest are follow-ups ---

    fn import_busybox(
        &mut self,
        request: SandboxImportBusyBoxToolchainRequest,
    ) -> Result<SandboxImportBusyBoxToolchainResponse, SandboxBackendError> {
        let toolchain_store = LocalSandboxToolchainStore::new(&self.cache_root);
        let spec = SandboxBusyBoxToolchainSpec::new(
            request.version,
            request.provenance,
            OffsetDateTime::now_utc(),
        )
        .map_err(runtime_backend_error)?;
        let module_hash = sandbox_module_hash(&request.module_bytes);
        let manifest = install_busybox_toolchain_to_local_store(
            &toolchain_store,
            &spec,
            &request.module_bytes,
        )
        .map_err(runtime_backend_error)?;
        Ok(SandboxImportBusyBoxToolchainResponse {
            manifest,
            module_hash,
        })
    }

    fn toolchain_add_command(
        &mut self,
        request: SandboxAddToolchainCommandRequest,
    ) -> Result<SandboxAddToolchainCommandResponse, SandboxBackendError> {
        self.local.toolchain_add_command(request)
    }

    fn toolchain_add_artifact(
        &mut self,
        request: SandboxAddToolchainArtifactRequest,
    ) -> Result<SandboxAddToolchainArtifactResponse, SandboxBackendError> {
        self.local.toolchain_add_artifact(request)
    }

    fn import_toolchain_package(
        &mut self,
        request: SandboxImportToolchainPackageRequest,
    ) -> Result<SandboxImportToolchainPackageResponse, SandboxBackendError> {
        self.local.import_toolchain_package(request)
    }

    fn import_toolchain_package_from_path(
        &mut self,
        request: SandboxImportToolchainPackageFromPathRequest,
    ) -> Result<SandboxImportToolchainPackageResponse, SandboxBackendError> {
        self.local.import_toolchain_package_from_path(request)
    }

    // --- execution: run a single toolchain command locally via the WASM runner ---

    fn run_toolchain(
        &mut self,
        request: SandboxRunToolchainRequest,
    ) -> Result<SandboxLocalCommandOutcome, SandboxBackendError> {
        let store = LocalSandboxInstanceStore::new(&self.runtime_root);
        let toolchain_store = LocalSandboxToolchainStore::new(&self.cache_root);
        let runner = LocalStagedSandboxCommandRunner::new().map_err(runtime_backend_error)?;
        let policy = SandboxPolicy::default();
        runner
            .run_toolchain_command_with_stdin_and_policy(
                &store,
                &toolchain_store,
                &request.sandbox_id,
                &request.command_name,
                request.argv,
                request.stdin,
                request.timeout_ms,
                &self.node_id,
                &policy,
            )
            .map_err(runtime_backend_error)
    }

    fn run_process_plan(
        &mut self,
        _request: SandboxRunProcessPlanRequest,
    ) -> Result<SandboxProcessPlanOutcome, SandboxBackendError> {
        Err(SandboxBackendError::Other(
            "daemonless process-plan execution is not yet supported".to_string(),
        ))
    }
}
