use std::collections::BTreeSet;
use std::io::Read as _;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sinomo_core::events::EventStore as _;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::node::NodeId;
use sinomo_core::service::{ClusterAccess, ServiceContext, ServiceError};

use crate::*;

const SANDBOX_LIFECYCLE_RECOVERY_STATE_DIR: &str = "daemon/sandbox-lifecycle-recovery";
pub const SANDBOX_LIFECYCLE_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(30);

type ClusteredDfsService = sinomo_dfs::NetworkedDfsService<sinomo_dfs::DfsNodeTransport>;
type ClusteredInstanceStore = SifsSandboxInstanceStore<sinomo_dfs::DfsNodeTransport>;
type ClusteredExecutionLease = ClusteredSandboxExecutionLease<sinomo_dfs::DfsNodeTransport>;

/// Default component backend selection. Clustered requests use the DFS-owned
/// runtime directly; local-only requests retain the standalone filesystem
/// backend.
pub struct ComponentSandboxBackendFactory {
    runtime: Option<tokio::runtime::Handle>,
}

impl ComponentSandboxBackendFactory {
    pub fn new(runtime: Option<tokio::runtime::Handle>) -> Self {
        Self { runtime }
    }
}

impl SandboxBackendFactory for ComponentSandboxBackendFactory {
    fn open(&self, ctx: &ServiceContext) -> Result<Box<dyn SandboxBackend>, ServiceError> {
        let Some(cluster) = ctx.cluster.as_ref() else {
            return LocalSandboxBackendFactory.open(ctx);
        };
        let runtime = self.runtime.clone().ok_or_else(|| {
            ServiceError::Service(
                "clustered sandbox backend requires the component runtime".to_string(),
            )
        })?;
        Ok(Box::new(ClusteredSandboxBackend::new(
            Arc::clone(&ctx.data_dir),
            Arc::clone(cluster),
            Some(runtime),
        )))
    }
}

#[derive(Clone)]
pub struct ClusteredSandboxBackend {
    data_dir: Arc<SinomoDataDir>,
    cluster: Arc<dyn ClusterAccess>,
    runtime: Option<tokio::runtime::Handle>,
    recovery: SandboxLifecycleRecoveryState,
}

impl ClusteredSandboxBackend {
    pub fn new(
        data_dir: Arc<SinomoDataDir>,
        cluster: Arc<dyn ClusterAccess>,
        runtime: Option<tokio::runtime::Handle>,
    ) -> Self {
        let recovery = SandboxLifecycleRecoveryState::new(
            data_dir.root().join(SANDBOX_LIFECYCLE_RECOVERY_STATE_DIR),
        );
        Self {
            data_dir,
            cluster,
            runtime,
            recovery,
        }
    }

    fn recovery_state(&self) -> SandboxLifecycleRecoveryState {
        // The component's existing task clones this backend for each tick.
        // Keep one cadence for that owner instead of reconstructing it per call.
        self.recovery.clone()
    }

    fn load_policy_config(&self) -> Result<SandboxClusterPolicyConfig, SandboxBackendError> {
        let definition = sandbox_policy_config_definition()
            .map_err(|error| SandboxBackendError::Other(error.to_string()))?;
        let entry = self
            .cluster
            .kv_get_local(&definition.cluster_key())
            .map_err(|error| SandboxBackendError::Other(error.to_string()))?
            .value;
        let entries = entry.into_iter().collect::<Vec<_>>();
        sandbox_policy_config_from_cluster_kv_entries(&entries)
            .map_err(|error| SandboxBackendError::Other(error.to_string()))
            .map(Option::unwrap_or_default)
    }

    fn event_store(&self) -> sinomo_core::events::JsonlEventStore {
        sinomo_core::events::JsonlEventStore::new(
            self.data_dir.root().join("events/event-log.jsonl"),
        )
    }

    fn append_event(
        &self,
        event: sinomo_core::EventEnvelope,
        durable: bool,
    ) -> Result<(), SandboxBackendError> {
        let event_id = event.event_id.clone();
        let expected_kind = event.kind.clone();
        let expected_payload = event.payload.clone();
        let mut store = self.event_store();
        match store.append(event) {
            Ok(_) => {}
            Err(sinomo_core::events::EventStoreError::DuplicateEventId {
                event_id: duplicate,
            }) if durable && duplicate == event_id => {
                let existing = store
                    .replay(sinomo_core::events::ReplayQuery::all())
                    .map_err(|error| SandboxBackendError::Other(error.to_string()))?
                    .into_iter()
                    .find(|stored| stored.event.event_id == event_id)
                    .ok_or_else(|| {
                        SandboxBackendError::Other(format!(
                            "event store reported duplicate {event_id} but replay could not find it"
                        ))
                    })?;
                if existing.event.kind != expected_kind
                    || existing.event.payload != expected_payload
                {
                    return Err(SandboxBackendError::Other(format!(
                        "event {event_id} duplicate payload does not match authoritative sandbox transaction"
                    )));
                }
            }
            Err(error) => return Err(SandboxBackendError::Other(error.to_string())),
        }
        if durable {
            store
                .sync_all()
                .map_err(|error| SandboxBackendError::Other(error.to_string()))?;
        }
        Ok(())
    }

    fn hooks(&self) -> ClusteredSandboxBackendHooks<'_> {
        ClusteredSandboxBackendHooks { backend: self }
    }

    fn open_dfs_service(&self) -> Result<ClusteredDfsService, SandboxBackendError> {
        sinomo_dfs::runtime_host::open_dfs_runtime_service(&self.data_dir, self.runtime.as_ref())
            .map_err(|error| SandboxBackendError::Other(error.to_string()))
    }

    fn open_dfs_service_with_content(
        &self,
        content: sinomo_dfs::LocalContentStore,
    ) -> Result<ClusteredDfsService, SandboxBackendError> {
        sinomo_dfs::runtime_host::open_dfs_runtime_service_with_content(
            &self.data_dir,
            self.runtime.as_ref(),
            content,
        )
        .map_err(|error| SandboxBackendError::Other(error.to_string()))
    }

    fn open_content_store(&self) -> Result<sinomo_dfs::LocalContentStore, SandboxBackendError> {
        sinomo_dfs::runtime_host::open_dfs_runtime_content_store(&self.data_dir)
            .map_err(|error| SandboxBackendError::Other(error.to_string()))
    }

    fn open_dfs_service_with_candidates(
        &self,
    ) -> Result<(ClusteredDfsService, Vec<NodeId>), SandboxBackendError> {
        let dfs = self.open_dfs_service()?;
        let candidates =
            sinomo_dfs::runtime_host::dfs_runtime_replica_candidates(&self.data_dir, dfs.node_id());
        Ok((dfs, candidates))
    }

    fn open_operation(
        &self,
        sandbox_id: &SandboxId,
        kind: ClusteredSandboxOperationKind,
        recover_existing: bool,
    ) -> Result<(ClusteredExecutionLease, ClusteredInstanceStore, Vec<NodeId>), SandboxBackendError>
    {
        let content = self.open_content_store()?;
        let lease_dfs = self.open_dfs_service_with_content(content.clone())?;
        let replica_candidates = sinomo_dfs::runtime_host::dfs_runtime_replica_candidates(
            &self.data_dir,
            lease_dfs.node_id(),
        );
        let dfs = self.open_dfs_service_with_content(content)?;
        let mut hooks = self.hooks();
        ClusteredSandboxLifecycleRuntime::new(&mut hooks).open_operation(
            lease_dfs,
            dfs,
            replica_candidates,
            sandbox_id,
            kind,
            recover_existing,
        )
    }

    fn open_mutation(
        &self,
        sandbox_id: &SandboxId,
    ) -> Result<(ClusteredExecutionLease, ClusteredInstanceStore, Vec<NodeId>), SandboxBackendError>
    {
        self.open_operation(
            sandbox_id,
            ClusteredSandboxOperationKind::Mutation,
            cfg!(feature = "wasix-runtime"),
        )
    }

    fn open_transaction(
        &self,
        sandbox_id: &SandboxId,
    ) -> Result<(ClusteredExecutionLease, ClusteredInstanceStore, Vec<NodeId>), SandboxBackendError>
    {
        self.open_operation(
            sandbox_id,
            ClusteredSandboxOperationKind::AuthoritativeTransaction,
            false,
        )
    }

    fn toolchain_store(
        &self,
    ) -> Result<SifsSandboxToolchainStore<sinomo_dfs::DfsNodeTransport>, SandboxBackendError> {
        let (dfs, candidates) = self.open_dfs_service_with_candidates()?;
        Ok(SifsSandboxToolchainStore::new(dfs).with_replica_candidates(candidates))
    }

    pub fn run_lifecycle_recovery_tick(&self) -> Result<usize, SandboxBackendError> {
        let now_unix_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
            .unwrap_or_default();
        self.recovery_state()
            .run_due_recovery(now_unix_nanos, |marked_sandbox_ids| {
                self.run_lifecycle_recovery_scan(marked_sandbox_ids)
                    .map_err(|error| crate::SandboxLifecycleRecoveryError::Io {
                        path: self.data_dir.root().display().to_string(),
                        detail: error.to_string(),
                    })
            })
            .map_err(|error| SandboxBackendError::Other(error.to_string()))
    }

    fn run_lifecycle_recovery_scan(
        &self,
        marked_sandbox_ids: BTreeSet<SandboxId>,
    ) -> Result<usize, SandboxBackendError> {
        let content = self.open_content_store()?;
        let dfs = self.open_dfs_service_with_content(content.clone())?;
        let replica_candidates =
            sinomo_dfs::runtime_host::dfs_runtime_replica_candidates(&self.data_dir, dfs.node_id());
        let mut authoritative_store =
            SifsSandboxInstanceStore::new(dfs).with_replica_candidates(replica_candidates.clone());
        let pending_manifests = authoritative_store.list_pending_authoritative_manifests()?;
        drop(authoritative_store);

        let pending_sandbox_ids = pending_manifests
            .into_iter()
            .map(|manifest| manifest.sandbox_id)
            .collect::<BTreeSet<_>>();
        let mut candidates = marked_sandbox_ids;
        for sandbox_id in &pending_sandbox_ids {
            self.recovery_state()
                .mark_authoritative_recovery_pending(sandbox_id)
                .map_err(|error| SandboxBackendError::Other(error.to_string()))?;
            candidates.insert(sandbox_id.clone());
        }

        let mut recovered = 0_usize;
        for sandbox_id in candidates {
            let lease_dfs = self.open_dfs_service_with_content(content.clone())?;
            let recovery_dfs = self.open_dfs_service_with_content(content.clone())?;
            let mut hooks = self.hooks();
            match ClusteredSandboxLifecycleRuntime::new(&mut hooks).open_operation(
                lease_dfs,
                recovery_dfs,
                replica_candidates.clone(),
                &sandbox_id,
                ClusteredSandboxOperationKind::Mutation,
                true,
            ) {
                Ok(_) if pending_sandbox_ids.contains(&sandbox_id) => {
                    recovered = recovered.saturating_add(1)
                }
                Ok(_) => {}
                Err(error) => tracing::warn!(
                    sandbox_id = %sandbox_id,
                    %error,
                    "authoritative sandbox transaction remains fenced for retry"
                ),
            }
        }
        Ok(recovered)
    }
}

struct ClusteredSandboxBackendHooks<'a> {
    backend: &'a ClusteredSandboxBackend,
}

impl ClusteredSandboxLifecycleHooks for ClusteredSandboxBackendHooks<'_> {
    type Error = SandboxBackendError;

    fn load_policy_config(&mut self) -> Result<SandboxClusterPolicyConfig, Self::Error> {
        self.backend.load_policy_config()
    }

    fn append_event(
        &mut self,
        event: sinomo_core::EventEnvelope,
        durability: SandboxAuditDurability,
    ) -> Result<(), Self::Error> {
        self.backend
            .append_event(event, durability == SandboxAuditDurability::Durable)
    }

    fn clear_authoritative_recovery_pending(
        &mut self,
        sandbox_id: &SandboxId,
        context: &'static str,
    ) {
        self.backend
            .recovery_state()
            .clear_authoritative_recovery_pending_best_effort(sandbox_id, context);
    }

    fn mark_authoritative_recovery_pending(
        &mut self,
        sandbox_id: &SandboxId,
    ) -> Result<(), Self::Error> {
        self.backend
            .recovery_state()
            .mark_authoritative_recovery_pending(sandbox_id)
            .map_err(|error| SandboxBackendError::Other(error.to_string()))
    }

    fn invalid(&mut self, detail: String) -> Self::Error {
        SandboxBackendError::Other(detail)
    }
}

impl ClusteredSandboxToolchainHooks for ClusteredSandboxBackendHooks<'_> {
    type Error = SandboxBackendError;

    fn load_policy_config(&mut self) -> Result<SandboxClusterPolicyConfig, Self::Error> {
        self.backend.load_policy_config()
    }

    fn append_event(&mut self, event: sinomo_core::EventEnvelope) -> Result<(), Self::Error> {
        self.backend.append_event(event, false)
    }

    fn invalid(&mut self, detail: String) -> Self::Error {
        SandboxBackendError::Other(detail)
    }
}

impl SandboxBackend for ClusteredSandboxBackend {
    fn create(
        &mut self,
        request: SandboxCreateInstanceRequest,
    ) -> Result<SandboxCreateInstanceResponse, SandboxBackendError> {
        let sandbox_id = request
            .sandbox_id
            .clone()
            .unwrap_or_else(SandboxId::generate);
        let (lease, mut store, _) = self.open_transaction(&sandbox_id)?;
        let mut hooks = self.hooks();
        ClusteredSandboxLifecycleRuntime::new(&mut hooks)
            .create(&lease, &mut store, sandbox_id, request)
    }

    fn status(
        &mut self,
        request: SandboxStatusRequest,
    ) -> Result<SandboxStatusResponse, SandboxBackendError> {
        let mut store = SifsSandboxInstanceStore::new(self.open_dfs_service()?);
        let mut hooks = self.hooks();
        ClusteredSandboxLifecycleRuntime::new(&mut hooks).status(&mut store, request)
    }

    fn update(
        &mut self,
        request: SandboxUpdateInstanceRequest,
    ) -> Result<SandboxUpdateInstanceResponse, SandboxBackendError> {
        let (lease, store, candidates) = self.open_mutation(&request.sandbox_id)?;
        let mut hooks = self.hooks();
        ClusteredSandboxLifecycleRuntime::new(&mut hooks).update(&lease, store, candidates, request)
    }

    fn checkpoint(
        &mut self,
        request: SandboxCheckpointRequest,
    ) -> Result<SandboxCheckpointResponse, SandboxBackendError> {
        let (lease, mut store, _) = self.open_mutation(&request.sandbox_id)?;
        let mut hooks = self.hooks();
        ClusteredSandboxLifecycleRuntime::new(&mut hooks).checkpoint(&lease, &mut store, request)
    }

    fn resume(
        &mut self,
        request: SandboxResumeRequest,
    ) -> Result<SandboxResumeResponse, SandboxBackendError> {
        let (lease, mut store, _) = self.open_transaction(&request.sandbox_id)?;
        let mut hooks = self.hooks();
        ClusteredSandboxLifecycleRuntime::new(&mut hooks).resume(&lease, &mut store, request)
    }

    fn reboot(
        &mut self,
        request: SandboxRebootRequest,
    ) -> Result<SandboxRebootResponse, SandboxBackendError> {
        let (lease, mut store, _) = self.open_transaction(&request.sandbox_id)?;
        let mut hooks = self.hooks();
        ClusteredSandboxLifecycleRuntime::new(&mut hooks).reboot(&lease, &mut store, request)
    }

    fn destroy(
        &mut self,
        request: SandboxDestroyRequest,
    ) -> Result<SandboxDestroyResponse, SandboxBackendError> {
        let (lease, mut store, _) = self.open_transaction(&request.sandbox_id)?;
        let mut hooks = self.hooks();
        ClusteredSandboxLifecycleRuntime::new(&mut hooks).destroy(&lease, &mut store, request)
    }

    fn list(
        &mut self,
        request: SandboxListRequest,
    ) -> Result<SandboxListResponse, SandboxBackendError> {
        let store = SifsSandboxInstanceStore::new(self.open_dfs_service()?);
        let mut hooks = self.hooks();
        ClusteredSandboxLifecycleRuntime::new(&mut hooks).list(store, request)
    }

    fn read(
        &mut self,
        request: SandboxReadRequest,
    ) -> Result<SandboxReadResponse, SandboxBackendError> {
        let store = SifsSandboxInstanceStore::new(self.open_dfs_service()?);
        let mut hooks = self.hooks();
        ClusteredSandboxLifecycleRuntime::new(&mut hooks).read(store, request)
    }

    fn write(
        &mut self,
        request: SandboxWriteRequest,
    ) -> Result<SandboxWriteResponse, SandboxBackendError> {
        let (lease, store, candidates) = self.open_mutation(&request.sandbox_id)?;
        let mut hooks = self.hooks();
        ClusteredSandboxLifecycleRuntime::new(&mut hooks).write(&lease, store, candidates, request)
    }

    fn http_fetch(
        &mut self,
        request: SandboxHttpRequest,
    ) -> Result<SandboxHttpResponse, SandboxBackendError> {
        self.load_policy_config()?
            .policy
            .validate_http_request(&request)
            .map_err(|error| SandboxBackendError::PolicyDenied(error.to_string()))?;
        sandbox_http_fetch_blocking(request)
    }

    fn http_fetch_to_file(
        &mut self,
        request: SandboxHttpFetchToFileRequest,
    ) -> Result<SandboxHttpFetchFileResult, SandboxBackendError> {
        let (lease, store, candidates) = self.open_mutation(&request.sandbox_id)?;
        let mut hooks = self.hooks();
        ClusteredSandboxLifecycleRuntime::new(&mut hooks).http_fetch_to_file(
            &lease,
            store,
            candidates,
            &ComponentSandboxHttpTransport,
            request,
        )
    }

    fn import_busybox(
        &mut self,
        request: SandboxImportBusyBoxToolchainRequest,
    ) -> Result<SandboxImportBusyBoxToolchainResponse, SandboxBackendError> {
        let store = self.toolchain_store()?;
        let mut hooks = self.hooks();
        ClusteredSandboxToolchainRuntime::new(&mut hooks).import_busybox(store, request)
    }

    fn toolchain_add_command(
        &mut self,
        request: SandboxAddToolchainCommandRequest,
    ) -> Result<SandboxAddToolchainCommandResponse, SandboxBackendError> {
        let store = self.toolchain_store()?;
        let mut hooks = self.hooks();
        ClusteredSandboxToolchainRuntime::new(&mut hooks).add_command(store, request)
    }

    fn toolchain_add_artifact(
        &mut self,
        request: SandboxAddToolchainArtifactRequest,
    ) -> Result<SandboxAddToolchainArtifactResponse, SandboxBackendError> {
        let store = self.toolchain_store()?;
        let mut hooks = self.hooks();
        ClusteredSandboxToolchainRuntime::new(&mut hooks).add_artifact(store, request)
    }

    fn import_toolchain_package(
        &mut self,
        request: SandboxImportToolchainPackageRequest,
    ) -> Result<SandboxImportToolchainPackageResponse, SandboxBackendError> {
        let store = self.toolchain_store()?;
        let mut hooks = self.hooks();
        ClusteredSandboxToolchainRuntime::new(&mut hooks).import_package(store, request)
    }

    fn import_toolchain_package_from_path(
        &mut self,
        request: SandboxImportToolchainPackageFromPathRequest,
    ) -> Result<SandboxImportToolchainPackageResponse, SandboxBackendError> {
        let request = load_sandbox_toolchain_package_from_path(request)
            .map_err(|error| SandboxBackendError::Other(error.to_string()))?;
        self.import_toolchain_package(request)
    }

    fn run_toolchain(
        &mut self,
        _request: SandboxRunToolchainRequest,
    ) -> Result<SandboxLocalCommandOutcome, SandboxBackendError> {
        Err(SandboxBackendError::Other(
            "clustered dynamic SIFS sandbox commands execute through an external sandbox executor or the live DFS/FUSE simple sandbox"
                .to_string(),
        ))
    }

    fn run_process_plan(
        &mut self,
        _request: SandboxRunProcessPlanRequest,
    ) -> Result<SandboxProcessPlanOutcome, SandboxBackendError> {
        Err(SandboxBackendError::Other(
            "clustered dynamic SIFS sandbox process plans execute through an external sandbox executor or the live DFS/FUSE simple sandbox"
                .to_string(),
        ))
    }
}

pub fn sandbox_http_fetch_blocking(
    request: SandboxHttpRequest,
) -> Result<SandboxHttpResponse, SandboxBackendError> {
    let timeout = Duration::from_millis(request.timeout_ms);
    let client = reqwest::blocking::Client::builder()
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()
        .map_err(|error| SandboxBackendError::Other(error.to_string()))?;
    let method = match request.method {
        SandboxHttpMethod::Get => reqwest::Method::GET,
        SandboxHttpMethod::Post => reqwest::Method::POST,
        SandboxHttpMethod::Put => reqwest::Method::PUT,
        SandboxHttpMethod::Delete => reqwest::Method::DELETE,
        SandboxHttpMethod::Head => reqwest::Method::HEAD,
    };
    let mut builder = client.request(method, &request.url).timeout(timeout);
    for (name, value) in &request.headers {
        builder = builder.header(name, value);
    }
    if !request.body.is_empty() {
        builder = builder.body(request.body);
    }
    let mut response = builder
        .send()
        .map_err(|error| SandboxBackendError::Other(error.to_string()))?;
    if let Some(length) = response.content_length()
        && length > request.max_response_bytes
    {
        return Err(SandboxBackendError::Other(format!(
            "sandbox HTTP response is {length} bytes; max is {}",
            request.max_response_bytes
        )));
    }
    let status = response.status().as_u16();
    let mut headers = std::collections::BTreeMap::new();
    for (name, value) in response.headers() {
        if let Ok(value) = value.to_str() {
            headers.insert(name.as_str().to_string(), value.to_string());
        }
    }
    let mut body = Vec::new();
    response
        .by_ref()
        .take(request.max_response_bytes.saturating_add(1))
        .read_to_end(&mut body)
        .map_err(|error| SandboxBackendError::Other(error.to_string()))?;
    if body.len() as u64 > request.max_response_bytes {
        return Err(SandboxBackendError::Other(format!(
            "sandbox HTTP response exceeded max of {} bytes",
            request.max_response_bytes
        )));
    }
    Ok(SandboxHttpResponse {
        status,
        headers,
        body,
    })
}

struct ComponentSandboxHttpTransport;

impl SandboxHttpTransport for ComponentSandboxHttpTransport {
    fn fetch(
        &self,
        request: &SandboxHttpRequest,
    ) -> Result<SandboxHttpResponse, SandboxNetworkError> {
        sandbox_http_fetch_blocking(request.clone()).map_err(|error| {
            SandboxNetworkError::Transport {
                detail: error.to_string(),
            }
        })
    }
}

/// Compatibility entry points for direct integration harnesses. Production
/// daemon traffic uses [`crate::SandboxService`]; these helpers keep tests and
/// embedders on the owner crate while they migrate to component dispatch.
pub fn handle_daemon_sandbox_http_fetch(
    paths: &sinomo_api::DaemonPaths,
    request: SandboxHttpRequest,
) -> Result<SandboxHttpResponse, SandboxBackendError> {
    compatibility_clustered_backend(paths)?.http_fetch(request)
}

pub fn handle_daemon_sandbox_http_fetch_to_file(
    paths: &sinomo_api::DaemonPaths,
    request: SandboxHttpFetchToFileRequest,
) -> Result<SandboxHttpFetchFileResult, SandboxBackendError> {
    compatibility_clustered_backend(paths)?.http_fetch_to_file(request)
}

pub fn handle_daemon_sandbox_import_busybox(
    paths: &sinomo_api::DaemonPaths,
    request: SandboxImportBusyBoxToolchainRequest,
) -> Result<SandboxImportBusyBoxToolchainResponse, SandboxBackendError> {
    compatibility_clustered_backend(paths)?.import_busybox(request)
}

pub fn handle_daemon_sandbox_toolchain_add_command(
    paths: &sinomo_api::DaemonPaths,
    request: SandboxAddToolchainCommandRequest,
) -> Result<SandboxAddToolchainCommandResponse, SandboxBackendError> {
    compatibility_clustered_backend(paths)?.toolchain_add_command(request)
}

pub fn handle_daemon_sandbox_toolchain_add_artifact(
    paths: &sinomo_api::DaemonPaths,
    request: SandboxAddToolchainArtifactRequest,
) -> Result<SandboxAddToolchainArtifactResponse, SandboxBackendError> {
    compatibility_clustered_backend(paths)?.toolchain_add_artifact(request)
}

pub fn handle_daemon_sandbox_toolchain_import_package(
    paths: &sinomo_api::DaemonPaths,
    request: SandboxImportToolchainPackageRequest,
) -> Result<SandboxImportToolchainPackageResponse, SandboxBackendError> {
    compatibility_clustered_backend(paths)?.import_toolchain_package(request)
}

pub fn handle_daemon_sandbox_read(
    paths: &sinomo_api::DaemonPaths,
    request: SandboxReadRequest,
) -> Result<SandboxReadResponse, SandboxBackendError> {
    compatibility_clustered_backend(paths)?.read(request)
}

pub fn handle_daemon_sandbox_write(
    paths: &sinomo_api::DaemonPaths,
    request: SandboxWriteRequest,
) -> Result<SandboxWriteResponse, SandboxBackendError> {
    compatibility_clustered_backend(paths)?.write(request)
}

pub fn open_daemon_sandbox_dfs_service(
    paths: &sinomo_api::DaemonPaths,
) -> Result<ClusteredDfsService, SandboxBackendError> {
    let data_dir = compatibility_data_dir(paths)?;
    sinomo_dfs::runtime_host::open_dfs_runtime_service(&data_dir, None)
        .map_err(|error| SandboxBackendError::Other(error.to_string()))
}

fn compatibility_clustered_backend(
    paths: &sinomo_api::DaemonPaths,
) -> Result<ClusteredSandboxBackend, SandboxBackendError> {
    let data_dir = Arc::new(compatibility_data_dir(paths)?);
    let cluster: Arc<dyn ClusterAccess> = Arc::new(LocalSnapshotSandboxClusterAccess::new(
        Arc::clone(&data_dir),
    ));
    Ok(ClusteredSandboxBackend::new(data_dir, cluster, None))
}

fn compatibility_data_dir(
    paths: &sinomo_api::DaemonPaths,
) -> Result<SinomoDataDir, SandboxBackendError> {
    paths.root.parent().map(SinomoDataDir::new).ok_or_else(|| {
        SandboxBackendError::Other(format!(
            "daemon root {} has no data-directory parent",
            paths.root.display()
        ))
    })
}

struct LocalSnapshotSandboxClusterAccess {
    data_dir: Arc<SinomoDataDir>,
}

impl LocalSnapshotSandboxClusterAccess {
    fn new(data_dir: Arc<SinomoDataDir>) -> Self {
        Self { data_dir }
    }

    fn snapshot(&self) -> Result<sinomo_core::kv::KvSnapshot, sinomo_core::service::ServiceError> {
        sinomo_core::cluster::read_cluster_kv_latest_observed_snapshot(
            &sinomo_core::network::ClusterPaths::new(self.data_dir.as_ref()),
        )
        .map_err(sinomo_core::service::ServiceError::Network)
    }

    fn state(
        &self,
    ) -> Result<sinomo_core::ClusterBootstrapState, sinomo_core::service::ServiceError> {
        sinomo_core::cluster::read_cluster_state_without_kv(
            &sinomo_core::network::ClusterPaths::new(self.data_dir.as_ref()),
        )
        .map_err(sinomo_core::service::ServiceError::Network)
    }
}

impl ClusterAccess for LocalSnapshotSandboxClusterAccess {
    fn kv_get_local(
        &self,
        key: &str,
    ) -> Result<
        sinomo_core::kv::KvLocalRead<Option<sinomo_core::kv::KvEntry>>,
        sinomo_core::service::ServiceError,
    > {
        let snapshot = self.snapshot()?;
        let state = self.state()?;
        Ok(sinomo_core::kv::KvLocalRead {
            value: snapshot.get(key).cloned(),
            revision: snapshot.revision,
            observed_term: state.term,
            applied_index: snapshot.revision.0,
            known_leader: Some(state.local_node_id),
        })
    }

    fn kv_range_prefix_local(
        &self,
        prefix: &str,
    ) -> Result<
        sinomo_core::kv::KvLocalRead<Vec<sinomo_core::kv::KvEntry>>,
        sinomo_core::service::ServiceError,
    > {
        let snapshot = self.snapshot()?;
        let state = self.state()?;
        let value = snapshot
            .range_prefix_limited(prefix, sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)
            .map_err(sinomo_core::service::ServiceError::Kv)?;
        Ok(sinomo_core::kv::KvLocalRead {
            value,
            revision: snapshot.revision,
            observed_term: state.term,
            applied_index: snapshot.revision.0,
            known_leader: Some(state.local_node_id),
        })
    }

    fn kv_get_authoritative_async<'a>(
        &'a self,
        _key: &'a str,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        sinomo_core::kv::KvAuthoritativeRead<Option<sinomo_core::kv::KvEntry>>,
                        sinomo_core::service::ServiceError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async {
            Err(sinomo_core::service::ServiceError::Service(
                "direct Sandbox compatibility access does not provide authoritative KV reads"
                    .to_string(),
            ))
        })
    }

    fn kv_commit_async<'a>(
        &'a self,
        _request: sinomo_core::kv::KvTxnRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        sinomo_core::kv::ClusterKvTxnCommitResult,
                        sinomo_core::service::ServiceError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async {
            Err(sinomo_core::service::ServiceError::Service(
                "direct Sandbox compatibility access does not provide cluster commits".to_string(),
            ))
        })
    }
}
