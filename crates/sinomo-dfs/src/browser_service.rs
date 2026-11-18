//! Hosted DFS browser and placement-authority services.

use crate::*;
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::kv::{KvEntry, KvError, KvOp, KvTxn, KvTxnResponse};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    ClusterConnectivityGraph, ClusterConnectivityState, ClusterPaths, ObjectRpcRequest,
    cluster_directory_for, cluster_route_next_hop,
};
use sinomo_core::service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute, ServiceSigner,
};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

mod no_links;

const DAEMON_RPC_CATEGORY: &str = "daemon";

/// Generic owner-checked access used by optional filesystem feature components.
pub const DFS_CORE_COMPONENT_INTERFACE: &str = "sinomo.dfs.semantic-filesystem";
pub const DFS_CORE_COMPONENT_INTERFACE_VERSION: &str = "1.0.0";
pub const DFS_CORE_COMPONENT_ACCESS_ROUTE: &str = "/dfs/component/access-v1";
pub const DFS_CORE_COMPONENT_ACCESS_OPERATION: &str = "dfs.semantic.access";
pub const DFS_CORE_BROWSER_ROUTES: &[&str] = &[
    DFS_CORE_COMPONENT_ACCESS_ROUTE,
    DAEMON_DFS_BROWSER_STATUS_ROUTE,
    DAEMON_DFS_BROWSER_STAT_ROUTE,
    DAEMON_DFS_BROWSER_NAMESPACE_LSTAT_ROUTE,
    DAEMON_DFS_BROWSER_NO_LINKS_ROUTE,
    DAEMON_DFS_BROWSER_LIST_ROUTE,
    DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
    DAEMON_DFS_BROWSER_READ_OBJECT_VERSION_ROUTE,
    DAEMON_DFS_BROWSER_READ_OBJECT_RANGE_ROUTE,
    DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE,
    DAEMON_DFS_BROWSER_READ_FILES_ROUTE,
    DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE,
    DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE,
    DAEMON_DFS_BROWSER_DURABLE_REPLACE_ROUTE,
    DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE,
    DAEMON_DFS_BROWSER_RENAME_ROUTE,
    DAEMON_DFS_BROWSER_DELETE_ROUTE,
    DAEMON_DFS_BROWSER_PIN_METADATA_ROUTE,
    DAEMON_DFS_BROWSER_UNPIN_METADATA_ROUTE,
    DAEMON_DFS_BROWSER_PIN_CONTENT_ROUTE,
    DAEMON_DFS_BROWSER_UNPIN_CONTENT_ROUTE,
    DAEMON_DFS_BROWSER_TRANSFER_METADATA_PRIMARY_ROUTE,
    DAEMON_DFS_BROWSER_PLACEMENT_PREVIEW_ROUTE,
    DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE,
    DAEMON_DFS_BROWSER_PLACEMENT_PROGRESS_ROUTE,
    DAEMON_DFS_BROWSER_PLACEMENT_ACTIVITY_ROUTE,
];

pub fn dfs_core_component_requirement() -> sinomo_module::ServiceRequirement {
    sinomo_module::ServiceRequirement::new(
        DFS_CORE_COMPONENT_INTERFACE,
        DFS_CORE_COMPONENT_INTERFACE_VERSION,
    )
    .from_service("dfs-semantic-filesystem")
    .with_caller_capability(DFS_CORE_COMPONENT_ACCESS_OPERATION)
    .with_operation(DFS_CORE_COMPONENT_ACCESS_OPERATION)
}

pub async fn authorize_dfs_core_component_access(
    client: &sinomo_module::ComponentServiceClient,
) -> Result<(), ServiceError> {
    let available: bool = client
        .call_local_on_provider(
            crate::dfs_module_id().as_str(),
            &dfs_core_component_requirement(),
            DFS_CORE_COMPONENT_ACCESS_OPERATION,
            "dfs-component-access",
            None,
            &(),
            Duration::from_secs(30),
            1024,
        )
        .await
        .map_err(dfs_browser_service_error)?;
    if !available {
        return Err(dfs_browser_service_error(
            "DFS component owner is unavailable",
        ));
    }
    Ok(())
}

pub(crate) fn register_dfs_browser_component_host(
    data_dir: &SinomoDataDir,
    host: &Arc<dyn DfsBrowserServiceHost>,
) -> Result<(), ServiceError> {
    *browser_backend_slot(data_dir)?
        .host
        .lock()
        .map_err(dfs_browser_service_error)? = Some(Arc::downgrade(host));
    Ok(())
}

/// Resolve only the core-owned runtime host. Optional modules cannot cold-open
/// a parallel filesystem authority when DFS is disabled or retiring.
pub fn dfs_browser_component_host(
    data_dir: &SinomoDataDir,
) -> Result<Arc<dyn DfsBrowserServiceHost>, ServiceError> {
    browser_backend_slot(data_dir)?
        .host
        .lock()
        .map_err(dfs_browser_service_error)?
        .as_ref()
        .and_then(std::sync::Weak::upgrade)
        .ok_or_else(|| dfs_browser_service_error("DFS component owner is unavailable"))
}

/// Daemon/process effects used by the DFS-owned browser services.
pub trait DfsBrowserServiceHost: Send + Sync + 'static {
    /// Reuse the component's existing Main data owner for inspections. Hosts
    /// without that owner retain ordinary mounted-service initialization.
    fn data_plane_host(&self) -> Option<Arc<dyn crate::DfsDataPlaneHost>> {
        None
    }

    /// Borrow the existing host runtime; never substitute the caller executor.
    fn browser_open_runtime(&self) -> Option<tokio::runtime::Handle> {
        None
    }

    /// Read input only. Hosted cold opens join their existing configuration
    /// owner without refreshing content observations or acquiring a writer.
    fn browser_open_configuration_readiness(
        &self,
    ) -> Result<crate::DfsWriteConfigurationReadiness, ServiceError> {
        Ok(crate::DfsWriteConfigurationReadiness::Ready(None))
    }

    fn ready_browser_open_configuration<'a>(
        &'a self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<
        Output = Result<Option<Arc<crate::DfsWriteConfigurationProjection>>, ServiceError>,
    > + Send + 'a>> {
        Box::pin(async move {
            match self.browser_open_configuration_readiness()? {
                crate::DfsWriteConfigurationReadiness::Ready(view) => Ok(view),
                crate::DfsWriteConfigurationReadiness::Pending => Err(dfs_browser_service_error(
                    "browser configuration is pending without a hosted readiness owner",
                )),
            }
        })
    }

    fn ensure_write_configuration_ready<'a>(
        &'a self,
        _data_dir: &'a SinomoDataDir,
        _runtime: &'a tokio::runtime::Handle,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ServiceError>> + Send + 'a>>
    {
        Box::pin(async { Ok(()) })
    }
    fn operation_guard(&self) -> Result<Option<Arc<dyn Send + Sync>>, ServiceError> {
        Ok(None)
    }
    /// Observe the existing shared publication after an already committed Apply.
    /// This runs without browser FIFO or writer custody and performs no mutation.
    fn placement_configuration_published<'a>(
        &'a self,
        _revision: sinomo_core::kv::KvRevision,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ServiceError>> + Send + 'a>>
    {
        Box::pin(async {
            Err(dfs_browser_service_error(
                "placement configuration publication owner is unavailable",
            ))
        })
    }
    /// Retain native execution ownership before borrowing the mutation service.
    /// Empty paths denote an opaque operation that may touch multiple shards.
    fn publication_execution_provider(
        &self,
        _paths: &[&str],
    ) -> Result<Option<Arc<crate::DfsPublicationExecutionProvider>>, ServiceError> {
        Ok(None)
    }
    fn control_plane_client(
        &self,
        cluster_paths: &ClusterPaths,
        runtime: &tokio::runtime::Handle,
    ) -> Option<Box<dyn DfsControlPlaneKvClient>>;

    fn peer_transports(
        &self,
        data_dir: &SinomoDataDir,
        runtime: &tokio::runtime::Handle,
    ) -> (Arc<dyn DfsPeerTransport>, Arc<dyn AsyncDfsPeerTransport>);

    fn placement_snapshot_cache(
        &self,
        _data_dir: &SinomoDataDir,
        _node_id: &sinomo_core::node::NodeId,
    ) -> DfsPlacementSnapshotCache {
        DfsPlacementSnapshotCache::default()
    }

    fn schedule_metadata_publish(
        &self,
        data_dir: &SinomoDataDir,
        runtime: &tokio::runtime::Handle,
        traceparent: Option<&str>,
    );

    fn notify_maintenance(&self);

    fn publish_namespace_change(
        &self,
        origin_node_id: sinomo_core::Id,
        operation: &str,
        paths: Vec<String>,
    );

    fn publish_placement_change(
        &self,
        origin_node_id: sinomo_core::Id,
        operation: &str,
        paths: Vec<String>,
    );

    fn inspect_placement_authority(
        &self,
        runtime: &tokio::runtime::Handle,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsPlacementAuthorityPermitStatus>, ServiceError>;

    fn recover_removed_placement_authority(
        &self,
        runtime: &tokio::runtime::Handle,
        file_system_id: &DfsFileSystemId,
        expected_recovery_token: &str,
        acknowledge_owner_stopped: bool,
    ) -> Result<DfsPlacementAuthorityRecoveryOutcome, ServiceError>;

    fn break_glass_recover_placement_authority(
        &self,
        runtime: &tokio::runtime::Handle,
        file_system_id: &DfsFileSystemId,
        expected_recovery_token: &str,
        acknowledgement: &str,
    ) -> Result<DfsPlacementAuthorityRecoveryOutcome, ServiceError>;
}

pub struct HostedDfsBrowserService {
    runtime: Option<tokio::runtime::Handle>,
    host: Arc<dyn DfsBrowserServiceHost>,
}

impl HostedDfsBrowserService {
    pub fn new(
        runtime: Option<tokio::runtime::Handle>,
        host: Arc<dyn DfsBrowserServiceHost>,
    ) -> Self {
        Self { runtime, host }
    }
}

fn require_service_runtime<'a>(
    runtime: Option<&'a tokio::runtime::Handle>,
    operation: &str,
) -> Result<&'a tokio::runtime::Handle, ServiceError> {
    runtime.ok_or_else(|| {
        ServiceError::Service(format!(
            "{operation} requires the daemon-owned Tokio runtime"
        ))
    })
}

pub async fn run_blocking_service_task<T>(
    route: impl Into<String>,
    label: &'static str,
    task: impl FnOnce() -> Result<T, ServiceError> + Send + 'static,
) -> Result<T, ServiceError>
where
    T: Send + 'static,
{
    let route = route.into();
    tokio::task::spawn_blocking(sinomo_core::service::observed_blocking_task(
        "dfs.browser",
        task,
    ))
    .await
    .map_err(|source| {
        ServiceError::Service(format!("{label} route {route} task failed: {source}"))
    })?
}

pub struct DfsBrowserBackend {
    data_dir: SinomoDataDir,
    host: Arc<dyn DfsBrowserServiceHost>,
    node_id: sinomo_core::node::NodeId,
    service: Mutex<crate::MountedSifsService>,
    replica_control_plane: Mutex<Option<Box<dyn crate::DfsControlPlaneKvClient>>>,
    peer_targets_refreshed_at: Mutex<Option<Instant>>,
    stat_cache: Mutex<HashMap<String, DfsBrowserCachedStat>>,
}

#[derive(Clone, Debug)]
struct DfsBrowserCachedStat {
    cached_at: Instant,
    response: crate::DaemonDfsBrowserStatResponse,
}

pub fn daemon_dfs_browser_read_file_response(
    outcome: crate::DfsPosixDistributedFileReadOutcome,
) -> crate::DaemonDfsBrowserReadFileResponse {
    match outcome {
        crate::DfsPosixDistributedFileReadOutcome::Found { bytes } => {
            crate::DaemonDfsBrowserReadFileResponse::found(bytes)
        }
        crate::DfsPosixDistributedFileReadOutcome::NotFound { path } => {
            crate::DaemonDfsBrowserReadFileResponse::not_found(path)
        }
        crate::DfsPosixDistributedFileReadOutcome::ContentUnavailable { path, detail } => {
            crate::DaemonDfsBrowserReadFileResponse::content_unavailable(path, detail)
        }
    }
}

pub fn daemon_dfs_browser_read_file_prefix_response(
    outcome: crate::DfsPosixDistributedFilePrefixReadOutcome,
) -> crate::DaemonDfsBrowserReadFilePrefixResponse {
    match outcome {
        crate::DfsPosixDistributedFilePrefixReadOutcome::Found { read } => {
            crate::DaemonDfsBrowserReadFilePrefixResponse::found(read.bytes, read.truncated)
        }
        crate::DfsPosixDistributedFilePrefixReadOutcome::NotFound { path } => {
            crate::DaemonDfsBrowserReadFilePrefixResponse::not_found(path)
        }
        crate::DfsPosixDistributedFilePrefixReadOutcome::ContentUnavailable { path, detail } => {
            crate::DaemonDfsBrowserReadFilePrefixResponse::content_unavailable(path, detail)
        }
    }
}

pub fn daemon_dfs_browser_bulk_file_read_response(
    requested_path: String,
    outcome: crate::DfsPosixDistributedFileReadOutcome,
) -> crate::DaemonDfsBrowserFileRead {
    match outcome {
        crate::DfsPosixDistributedFileReadOutcome::Found { bytes } => {
            crate::DaemonDfsBrowserFileRead::found(requested_path, bytes)
        }
        crate::DfsPosixDistributedFileReadOutcome::NotFound { path } => {
            let mut response = crate::DaemonDfsBrowserFileRead::not_found(path);
            response.path = requested_path;
            response
        }
        crate::DfsPosixDistributedFileReadOutcome::ContentUnavailable { path, detail } => {
            let mut response = crate::DaemonDfsBrowserFileRead::content_unavailable(path, detail);
            response.path = requested_path;
            response
        }
    }
}

#[derive(Clone, Debug)]
pub struct DfsBrowserObservedControlPlaneKvClient {
    cluster_paths: ClusterPaths,
}

impl DfsBrowserObservedControlPlaneKvClient {
    pub fn new(cluster_paths: &ClusterPaths) -> Self {
        Self {
            cluster_paths: cluster_paths.clone(),
        }
    }

    fn kv_error(&self, source: impl std::fmt::Display) -> KvError {
        KvError::Io {
            path: self.cluster_paths.kv_paths().snapshot.display().to_string(),
            detail: source.to_string(),
        }
    }
}

impl crate::DfsControlPlaneKvClient for DfsBrowserObservedControlPlaneKvClient {
    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        sinomo_core::kv::read_cluster_kv_entry(&self.cluster_paths.kv_paths(), key)
    }

    fn get_consistent(&self, keys: &[String]) -> Result<Vec<Option<KvEntry>>, KvError> {
        sinomo_core::kv::read_cluster_kv_cached_entries_consistently(
            &self.cluster_paths.kv_paths(),
            keys,
        )
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        sinomo_core::kv::read_cluster_kv_prefix_entries(&self.cluster_paths.kv_paths(), prefix)
    }

    fn get_authoritative_consistent(
        &mut self,
        _keys: &[String],
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        Err(KvError::AuthorityUnavailable {
            detail: "observational DFS browser KV checkpoint cannot authorize cluster state"
                .to_string(),
        })
    }

    fn put(&mut self, key: String, value: Vec<u8>) -> Result<sinomo_core::kv::KvRevision, KvError> {
        let response = self.txn(KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::Put {
                key,
                value,
                lease_id: None,
            }],
            failure: Vec::new(),
        })?;
        Ok(response.revision)
    }

    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        let _ = txn;
        Err(self.kv_error("observational DFS browser KV client cannot submit transactions"))
    }
}

#[derive(Default)]
struct DfsBrowserBackendSlot {
    backend: Mutex<Option<Arc<DfsBrowserBackend>>>,
    host: Mutex<Option<std::sync::Weak<dyn DfsBrowserServiceHost>>>,
    execution: Arc<tokio::sync::Mutex<()>>,
}

static DFS_BROWSER_BACKENDS: OnceLock<Mutex<HashMap<PathBuf, Arc<DfsBrowserBackendSlot>>>> =
    OnceLock::new();

fn browser_backend_slot(
    data_dir: &SinomoDataDir,
) -> Result<Arc<DfsBrowserBackendSlot>, ServiceError> {
    let cache = DFS_BROWSER_BACKENDS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut slots = cache.lock().map_err(|source| {
        ServiceError::Service(format!("DFS browser backend cache lock failed: {source}"))
    })?;
    Ok(Arc::clone(slots.entry(data_dir.root()).or_insert_with(
        || Arc::new(DfsBrowserBackendSlot::default()),
    )))
}

pub(crate) fn release_browser_backends(data_dir: &SinomoDataDir) {
    if let Some(backends) = DFS_BROWSER_BACKENDS.get() {
        backends
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&data_dir.root());
    }
}

/// Keep the notification with the synchronous mutation owner: an RPC caller
/// may disappear after the metadata commit and before receiving its response.
struct BrowserMutation<'a> {
    service: std::sync::MutexGuard<'a, crate::MountedSifsService>,
    tracker: crate::DfsCommittedWorkTracker,
    previous: Option<crate::DfsCommittedWorkTracker>,
    _publication_scope: Option<crate::publication_guard::DfsPublicationProviderRestore>,
    host: &'a dyn DfsBrowserServiceHost,
    origin_node_id: &'a sinomo_core::node::NodeId,
}
impl std::ops::Deref for BrowserMutation<'_> {
    type Target = crate::MountedSifsService;
    fn deref(&self) -> &Self::Target {
        &self.service
    }
}
impl std::ops::DerefMut for BrowserMutation<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.service
    }
}
impl Drop for BrowserMutation<'_> {
    fn drop(&mut self) {
        let work = self.tracker.take();
        let owns_effects = self.previous.is_none();
        if let Some(previous) = &self.previous {
            previous.record(work);
        }
        self.service.committed_work_tracker = self.previous.take();
        if owns_effects && work.semantic_changes {
            self.host.publish_namespace_change(
                self.origin_node_id.clone(),
                "metadata-committed",
                Vec::new(),
            );
        }
        // Restore capability custody while the service mutex is still held.
        // A later borrower must never inherit or erase this owner's scope.
        drop(self._publication_scope.take());
    }
}

/// Prepare hosted cold core inputs, then queue backend work fairly before
/// allocating a blocking worker. The route/caller owns the overall deadline.
/// Registered cold contexts join their existing configuration owner and open
/// the cached core backend before invoking the task at most once. Keep the
/// FIFO permit in the blocking closure through an executing filesystem call.
pub async fn run_blocking_browser_task<T>(
    data_dir: Arc<SinomoDataDir>,
    route: impl Into<String>,
    label: &'static str,
    task: impl FnOnce() -> Result<T, ServiceError> + Send + 'static,
) -> Result<T, ServiceError>
where
    T: Send + 'static,
{
    run_browser_task_with_admission(data_dir, route, label, task, true).await
}

/// Preserve callers that already have separate admission and intentionally do
/// not queue behind namespace work. Cold input still joins the SAME core owner.
pub async fn run_blocking_browser_task_unqueued<T>(
    data_dir: Arc<SinomoDataDir>,
    route: impl Into<String>,
    label: &'static str,
    task: impl FnOnce() -> Result<T, ServiceError> + Send + 'static,
) -> Result<T, ServiceError>
where T: Send + 'static,
{
    run_browser_task_with_admission(data_dir, route, label, task, false).await
}

async fn browser_backend_is_cold(
    slot: Arc<DfsBrowserBackendSlot>,
    route: String,
) -> Result<bool, ServiceError> {
    let observed = {
        match slot.backend.try_lock() {
            Ok(backend) => Some(backend.is_none()),
            Err(std::sync::TryLockError::WouldBlock) => None,
            Err(std::sync::TryLockError::Poisoned(error)) => {
                return Err(dfs_browser_service_error(error));
            }
        }
    };
    if let Some(cold) = observed { return Ok(cold); }
    // UNKNOWN contention is neither cold nor warm. Borrow the existing task
    // runtime to observe the actual cache owner after its mutex is released.
    // The enclosing caller/route still bounds this read-only observation.
    run_blocking_service_task(route, "DFS browser initialization state", move || {
        slot.backend.lock().map(|backend| backend.is_none())
            .map_err(dfs_browser_service_error)
    }).await
}

async fn run_browser_task_with_admission<T>(
    data_dir: Arc<SinomoDataDir>,
    route: impl Into<String>,
    label: &'static str,
    task: impl FnOnce() -> Result<T, ServiceError> + Send + 'static,
    namespace_admission: bool,
) -> Result<T, ServiceError>
where T: Send + 'static,
{
    let route = route.into();
    let slot = browser_backend_slot(&data_dir)?;
    let host = slot.host.lock().map_err(dfs_browser_service_error)?
        .as_ref().and_then(std::sync::Weak::upgrade);
    // The enclosing service route/caller owns the original overall deadline.
    // No new timeout begins on configuration, FIFO, or blocking-worker entry.
    loop {
        let cold = browser_backend_is_cold(Arc::clone(&slot), route.clone()).await?;
        if cold && let Some(host) = host.as_ref() {
            host.ready_browser_open_configuration().await?;
        }
        let execution = if namespace_admission {
            Some(tracing::Instrument::instrument(
                Arc::clone(&slot.execution).lock_owned(),
                tracing::trace_span!(target: "sinomo.perf", "dfs.browser.admission", stage = "dfs.browser.admission"),
            ).await)
        } else { None };
        let observed = {
            match slot.backend.try_lock() {
                Ok(backend) => Some(backend.is_none()),
                Err(std::sync::TryLockError::WouldBlock) => None,
                Err(std::sync::TryLockError::Poisoned(error)) => {
                    return Err(dfs_browser_service_error(error));
                }
            }
        };
        let Some(cold) = observed else {
            // An unqueued constructor may own the cache mutex. Never hold the
            // namespace FIFO while waiting to observe that existing owner.
            drop(execution);
            let cold = browser_backend_is_cold(Arc::clone(&slot), route.clone()).await?;
            if cold && let Some(host) = host.as_ref() {
                host.ready_browser_open_configuration().await?;
            }
            continue;
        };
        let configuration = if cold && let Some(host) = host.as_ref() {
            match host.browser_open_configuration_readiness()? {
                crate::DfsWriteConfigurationReadiness::Ready(view) => view,
                crate::DfsWriteConfigurationReadiness::Pending => {
                    // A queued request observed a later gap. Release the FIFO
                    // before joining that SAME owner; task has not run once.
                    drop(execution);
                    tokio::task::yield_now().await;
                    continue;
                }
            }
        } else { None };
        return run_blocking_service_task(route, label, move || {
            let _execution = execution;
            if let (Some(host), Some(configuration)) = (host, configuration) {
                // Reuse the actual Current cut instead of repeating the cold
                // replica-candidate projection read after FIFO admission.
                let runtime = host.browser_open_runtime().ok_or_else(||
                    dfs_browser_service_error("cold browser configuration has no hosted runtime"))?;
                DfsBrowserBackend::for_data_dir_with_configuration(
                    Arc::clone(&data_dir), &runtime, host, Some(configuration),
                )?;
            }
            task()
        }).await;
    }
}

/// Borrow the actual namespace FIFO for a test observation only. This does
/// not open storage, join configuration or supply native mutation permission.
#[cfg(any(test, feature = "test-support"))]
pub async fn acquire_browser_fifo_for_test(
    data_dir: &SinomoDataDir,
) -> Result<tokio::sync::OwnedMutexGuard<()>, ServiceError> {
    Ok(Arc::clone(&browser_backend_slot(data_dir)?.execution).lock_owned().await)
}

#[cfg(any(test, feature = "test-support"))]
static DFS_BROWSER_BACKEND_OPEN_COUNTS: OnceLock<Mutex<HashMap<PathBuf, usize>>> = OnceLock::new();

#[cfg(any(test, feature = "test-support"))]
fn record_dfs_browser_backend_open(data_dir: &SinomoDataDir) {
    let counts = DFS_BROWSER_BACKEND_OPEN_COUNTS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut counts = counts.lock().expect("DFS browser open-count lock");
    *counts.entry(data_dir.root()).or_default() += 1;
}

#[cfg(any(test, feature = "test-support"))]
pub fn dfs_browser_backend_open_count(data_dir: &SinomoDataDir) -> usize {
    DFS_BROWSER_BACKEND_OPEN_COUNTS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("DFS browser open-count lock")
        .get(&data_dir.root())
        .copied()
        .unwrap_or_default()
}

// Hosted RPCs queue asynchronously before entering the synchronous service.
// Direct backend callers retain a short lock-admission bound so they cannot
// park unrelated blocking workers behind a long remote read. This is not the
// end-to-end deadline of an admitted request.
const DAEMON_DFS_BROWSER_READ_LOCK_ADMISSION_TIMEOUT: Duration = Duration::from_millis(100);
const DAEMON_DFS_BROWSER_PLACEMENT_IMPACT_PAGE_SIZE: usize = 256;
const DAEMON_DFS_BROWSER_STAT_CACHE_MAX_ENTRIES: usize = 256;
const DAEMON_DFS_BROWSER_ROUTE_TIMEOUT: Duration = Duration::from_secs(60);

async fn finish_placement_configuration_visibility(
    backend: &DfsBrowserBackend,
    mut response: crate::DaemonDfsBrowserPlacementUpdateResponse,
    deadline: tokio::time::Instant,
) -> crate::DaemonDfsBrowserPlacementUpdateResponse {
    use crate::DfsPlacementConfigurationVisibility as Visibility;
    let visibility = if tokio::time::Instant::now() >= deadline {
        Visibility::Pending {
            detail: "original Apply deadline elapsed before configuration visibility was confirmed"
                .into(),
        }
    } else {
        match tokio::time::timeout_at(
            deadline,
            backend
                .host
                .placement_configuration_published(sinomo_core::kv::KvRevision(
                    response.committed_revision,
                )),
        )
        .await
        {
            Ok(Ok(())) => Visibility::Published,
            Ok(Err(error)) => Visibility::Pending {
                detail: error.to_string(),
            },
            Err(_) => Visibility::Pending {
                detail:
                    "original Apply deadline elapsed before configuration visibility was confirmed"
                        .into(),
            },
        }
    };
    // Other reads could have populated this cache while the effect-once task
    // had released its FIFO and waited for the publication. Invalidate here,
    // immediately before reporting the observed completion state.
    backend.invalidate_stat_cache();
    response.visibility = visibility;
    response
}

/// A pending configuration result is returned only before the mutation closure
/// runs. The caller retains the original payload without replaying an effect.
#[derive(Debug)]
pub enum DfsBrowserMutationAttempt<P, R> {
    Complete(R),
    ConfigurationPending(P),
}

type DfsBrowserWriteAttempt = DfsBrowserMutationAttempt<
    crate::DaemonDfsBrowserWriteFileRequest,
    crate::DaemonDfsBrowserWriteFileResponse,
>;

/// Rejoin configuration only after a typed preeffect refusal released the
/// worker/FIFO. An error after the task begins its effect is returned unchanged.
pub async fn run_configured_browser_task<P, R, F>(
    data_dir: Arc<SinomoDataDir>,
    route: String,
    label: &'static str,
    runtime: tokio::runtime::Handle,
    host: Arc<dyn DfsBrowserServiceHost>,
    mut payload: P,
    task: F,
) -> Result<R, ServiceError>
where
    P: Send + 'static,
    R: Send + 'static,
    F: Fn(P) -> Result<DfsBrowserMutationAttempt<P, R>, ServiceError> + Send + Sync + 'static,
{
    let projection_deadline =
        tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let task = Arc::new(task);
    let mut replans = 0;
    loop {
        tokio::time::timeout_at(
            projection_deadline,
            host.ensure_write_configuration_ready(&data_dir, &runtime),
        )
        .await
        .map_err(|_| {
            dfs_browser_service_error("DFS write configuration readiness deadline elapsed")
        })??;
        let attempt_task = Arc::clone(&task);
        let attempt =
            run_blocking_browser_task(Arc::clone(&data_dir), route.clone(), label, move || {
                attempt_task(payload)
            })
            .await?;
        match attempt {
            DfsBrowserMutationAttempt::Complete(response) => return Ok(response),
            DfsBrowserMutationAttempt::ConfigurationPending(original) => {
                if replans >= 2 {
                    return Err(dfs_browser_service_error(
                        "DFS write configuration remained pending after bounded preexecution replanning",
                    ));
                }
                payload = original;
                replans += 1;
                tracing::trace!(target: "sinomo.dfs.perf", replans, maximum = 2, %route,
                    "DFS browser write configuration preexecution replan");
            }
        }
    }
}

impl DfsBrowserBackend {
    /// Borrow the one resident core service through its normal authority path.
    pub fn with_read_service<R>(
        &self,
        task: impl FnOnce(&mut crate::MountedSifsService) -> Result<R, ServiceError>,
    ) -> Result<R, ServiceError> {
        let _owner = self.host.operation_guard()?;
        let mut service = self.lock_service_for_read()?;
        self.refresh_peer_targets(&mut service);
        task(&mut service)
    }

    /// Keep committed work attached to the synchronous mutation owner even
    /// when a feature caller is cancelled after its filesystem commit.
    pub fn with_mutation_service<R>(
        &self,
        task: impl FnOnce(&mut crate::MountedSifsService) -> Result<R, ServiceError>,
    ) -> Result<R, ServiceError> {
        let _owner = self.host.operation_guard()?;
        let mut service = self.lock_mutation()?;
        self.refresh_peer_targets(&mut service);
        task(&mut service)
    }

    /// Capture one coherent configuration before any content-writing effect.
    /// A pending attempt drops the mutation owner before its caller joins the
    /// existing async refresh outside the browser/FIFO worker.
    pub fn try_with_configured_mutation_service<P, R>(
        &self,
        paths: &[&str],
        payload: P,
        task: impl FnOnce(
            &mut crate::MountedSifsService,
            Option<&crate::DfsWriteConfigurationProjection>,
            P,
        ) -> Result<R, ServiceError>,
    ) -> Result<DfsBrowserMutationAttempt<P, R>, ServiceError> {
        let _owner = self.host.operation_guard()?;
        let mut service = self.lock_mutation_for_paths(paths)?;
        self.refresh_peer_targets(&mut service);
        let configuration = service
            .control_plane
            .as_deref()
            .map(|control| control.write_configuration_readiness())
            .transpose()
            .map_err(dfs_browser_service_error)?;
        let configuration = match configuration {
            Some(crate::DfsWriteConfigurationReadiness::Ready(view)) => view,
            None => None,
            Some(crate::DfsWriteConfigurationReadiness::Pending) => {
                return Ok(DfsBrowserMutationAttempt::ConfigurationPending(payload));
            }
        };
        task(&mut service, configuration.as_deref(), payload)
            .map(DfsBrowserMutationAttempt::Complete)
    }

    pub fn write_options_for_path_with_configuration(
        &self,
        service: &mut crate::MountedSifsService,
        path: &str,
        configuration: Option<&crate::DfsWriteConfigurationProjection>,
    ) -> Result<crate::DfsWriteOptions, ServiceError> {
        let candidates = match configuration {
            Some(configuration) => configuration
                .content_replica_candidates()
                .map_err(dfs_browser_service_error)?,
            None => self.content_replica_candidates()?,
        };
        sifs_browser_write_options_with_configuration(service, path, &candidates, configuration)
    }

    pub fn write_options_for_path(
        &self,
        service: &mut crate::MountedSifsService,
        path: &str,
    ) -> Result<crate::DfsWriteOptions, ServiceError> {
        let candidates = self.content_replica_candidates()?;
        sifs_browser_write_options(service, path, &candidates)
    }

    pub fn for_component_data_dir(
        data_dir: Arc<SinomoDataDir>,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Arc<Self>, ServiceError> {
        let host = dfs_browser_component_host(&data_dir)?;
        let _owner = host
            .operation_guard()?
            .ok_or_else(|| dfs_browser_service_error("DFS component owner is unavailable"))?;
        Self::for_data_dir(data_dir, runtime, host)
    }
    pub fn open(
        data_dir: SinomoDataDir,
        runtime: &tokio::runtime::Handle,
        host: Arc<dyn DfsBrowserServiceHost>,
    ) -> Result<Self, ServiceError> {
        Self::open_with_configuration(data_dir, runtime, host, None)
    }

    fn open_with_configuration(
        data_dir: SinomoDataDir,
        runtime: &tokio::runtime::Handle,
        host: Arc<dyn DfsBrowserServiceHost>,
        configuration: Option<Arc<crate::DfsWriteConfigurationProjection>>,
    ) -> Result<Self, ServiceError> {
        #[cfg(any(test, feature = "test-support"))]
        record_dfs_browser_backend_open(&data_dir);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let control_plane = host.control_plane_client(&cluster_paths, runtime);
        let replica_control_plane = host.control_plane_client(&cluster_paths, runtime);
        let (peer_transport, async_peer_transport) = host.peer_transports(&data_dir, runtime);
        let hosted_cluster = host.data_plane_host().is_some() && cluster_paths.state.exists();
        let configuration = if hosted_cluster && configuration.is_none() {
            match host.browser_open_configuration_readiness()? {
                crate::DfsWriteConfigurationReadiness::Ready(configuration) => configuration,
                crate::DfsWriteConfigurationReadiness::Pending => {
                    return Err(dfs_browser_service_error(
                        "DFS browser configuration is awaiting a coherent authoritative projection",
                    ));
                }
            }
        } else {
            configuration
        };
        let opened = if hosted_cluster {
            crate::mounted_sifs::open_hosted_browser_sifs_service(
                data_dir.clone(), peer_transport, async_peer_transport, control_plane,
                configuration,
            )
        } else {
            match configuration {
                Some(configuration) => crate::mounted_sifs::open_mounted_sifs_service_with_peer_transports_and_configuration(
                    data_dir.clone(), peer_transport, async_peer_transport, control_plane,
                    configuration,
                ),
                None => crate::open_mounted_sifs_service_with_peer_transports(
                    data_dir.clone(), peer_transport, async_peer_transport, control_plane,
                ),
            }
        }.map_err(dfs_browser_service_error)?;
        let node_id = opened.service.node_id().clone();
        let cache = host.placement_snapshot_cache(&data_dir, &node_id);
        let mut service = opened.service.with_placement_snapshot_cache(cache);
        refresh_browser_content_pin_snapshot(&data_dir, &mut service);
        Ok(Self {
            data_dir,
            host,
            node_id,
            service: Mutex::new(service),
            replica_control_plane: Mutex::new(replica_control_plane),
            peer_targets_refreshed_at: Mutex::new(Some(Instant::now())),
            stat_cache: Mutex::new(HashMap::new()),
        })
    }

    pub fn for_data_dir(
        data_dir: Arc<SinomoDataDir>,
        runtime: &tokio::runtime::Handle,
        host: Arc<dyn DfsBrowserServiceHost>,
    ) -> Result<Arc<Self>, ServiceError> {
        Self::for_data_dir_with_configuration(data_dir, runtime, host, None)
    }

    fn for_data_dir_with_configuration(
        data_dir: Arc<SinomoDataDir>,
        runtime: &tokio::runtime::Handle,
        host: Arc<dyn DfsBrowserServiceHost>,
        configuration: Option<Arc<crate::DfsWriteConfigurationProjection>>,
    ) -> Result<Arc<Self>, ServiceError> {
        let slot = browser_backend_slot(&data_dir)?;
        let mut initialized = slot.backend.lock().map_err(|source| {
            ServiceError::Service(format!(
                "DFS browser backend initialization lock failed: {source}"
            ))
        })?;
        if let Some(backend) = initialized.as_ref() {
            return Ok(Arc::clone(backend));
        }
        let backend = Arc::new(Self::open_with_configuration((*data_dir).clone(), runtime, host, configuration)?);
        *initialized = Some(Arc::clone(&backend));
        Ok(backend)
    }

    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.browser.lock",
        skip_all,
        fields(stage = "dfs.browser.lock")
    )]
    pub fn lock_service(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, crate::MountedSifsService>, ServiceError> {
        self.service.lock().map_err(|source| {
            ServiceError::Service(format!("DFS browser service lock failed: {source}"))
        })
    }

    fn lock_mutation(&self) -> Result<BrowserMutation<'_>, ServiceError> {
        // Feature callbacks and placement operations supply no bounded path set.
        self.lock_mutation_for_paths(&[])
    }

    fn lock_mutation_for_paths(&self, paths: &[&str]) -> Result<BrowserMutation<'_>, ServiceError> {
        let owners = self.host.publication_execution_provider(paths)?;
        let mut service = self.lock_service()?;
        let publication_scope = owners.map(|owners| service.publication_provider.install(owners));
        let tracker = crate::DfsCommittedWorkTracker::new();
        let previous = service.committed_work_tracker.replace(tracker.clone());
        Ok(BrowserMutation {
            service,
            tracker,
            previous,
            _publication_scope: publication_scope,
            host: self.host.as_ref(),
            origin_node_id: &self.node_id,
        })
    }

    fn lock_service_for_read(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, crate::MountedSifsService>, ServiceError> {
        let started = Instant::now();
        loop {
            match self.service.try_lock() {
                Ok(service) => return Ok(service),
                Err(std::sync::TryLockError::Poisoned(source)) => {
                    return Err(ServiceError::Service(format!(
                        "DFS browser service lock failed: {source}"
                    )));
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    let Some(remaining) = DAEMON_DFS_BROWSER_READ_LOCK_ADMISSION_TIMEOUT
                        .checked_sub(started.elapsed())
                        .filter(|remaining| !remaining.is_zero())
                    else {
                        return Err(ServiceError::Service(format!(
                            "DFS browser service is busy after waiting {}ms; retry the read",
                            DAEMON_DFS_BROWSER_READ_LOCK_ADMISSION_TIMEOUT.as_millis()
                        )));
                    };
                    std::thread::sleep(Duration::from_millis(1).min(remaining));
                }
            }
        }
    }

    fn refresh_peer_targets(&self, service: &mut crate::MountedSifsService) {
        let now = Instant::now();
        if let Ok(mut refreshed_at) = self.peer_targets_refreshed_at.lock() {
            if refreshed_at
                .as_ref()
                .is_some_and(|last| now.duration_since(*last) < Duration::from_secs(1))
            {
                return;
            }
            if self.refresh_peer_targets_now(service) {
                *refreshed_at = Some(now);
            }
            return;
        }
        let _ = self.refresh_peer_targets_now(service);
    }

    fn refresh_peer_targets_now(&self, service: &mut crate::MountedSifsService) -> bool {
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths) else {
            return false;
        };
        let Ok(peer_node_ids) = crate::discovered_cluster_dfs_peer_node_ids(&cluster_paths, &state)
        else {
            return false;
        };
        service.refresh_peer_targets(peer_node_ids);
        true
    }

    pub fn node_id(&self) -> Result<String, ServiceError> {
        // Provider discovery must not mistake a busy namespace operation for
        // an unavailable node. Identity is immutable for this backend's lifetime.
        Ok(self.node_id.to_string())
    }

    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.browser.candidates",
        skip_all,
        fields(stage = "dfs.browser.candidates")
    )]
    fn content_replica_candidates(&self) -> Result<Vec<sinomo_core::node::NodeId>, ServiceError> {
        let control_plane = self.replica_control_plane.lock().map_err(|_| {
            ServiceError::Service("daemon DFS browser control-plane mutex poisoned".to_string())
        })?;
        match control_plane.as_deref() {
            Some(control_plane) => {
                crate::discover_mounted_sifs_content_replica_candidates_with_control_plane(
                    control_plane,
                )
                .map_err(dfs_browser_service_error)
            }
            None => Ok(vec![self.node_id.clone()]),
        }
    }

    pub fn event_origin_node_id(&self) -> Result<sinomo_core::Id, ServiceError> {
        // Looking up event provenance after a committed mutation must not fail
        // just because another request acquired the service lock in between.
        Ok(self.node_id.clone())
    }

    pub fn health_summary(&self) -> Result<crate::DaemonDfsBrowserHealthSummary, ServiceError> {
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        let service = self.lock_service_for_read()?;
        let status = if let Some(metadata) = service.metadata_backend() {
            crate::inspect_dfs_status_from_metadata(&self.data_dir, &cluster_paths, metadata)
                .map_err(dfs_browser_service_error)?
        } else {
            drop(service);
            let owner = self.host.data_plane_host().ok_or_else(|| {
                dfs_browser_service_error("DFS browser data owner is unavailable")
            })?;
            let runtime = self.host.browser_open_runtime().ok_or_else(|| {
                dfs_browser_service_error("DFS browser host runtime is unavailable")
            })?;
            runtime
                .block_on(owner.status())
                .map_err(dfs_browser_service_error)?
        };
        Ok(daemon_dfs_browser_health_summary(status))
    }

    pub fn list(
        &self,
        request: crate::DaemonDfsBrowserListRequest,
    ) -> Result<crate::DaemonDfsBrowserListResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&request.path)?;
        let mut service = self.lock_service_for_read()?;
        self.refresh_peer_targets(&mut service);
        let include_metadata = request.include_metadata;
        let mut entries: Vec<_> = match service.posix_list_dir_distributed(&path) {
            Ok(entries) => entries,
            Err(crate::DfsPosixError::NotFound { .. }) if request.missing_is_empty => Vec::new(),
            Err(error) => return Err(dfs_browser_service_error(error)),
        }
        .into_iter()
        .map(|entry| {
            let metadata = include_metadata.then(|| crate::DaemonDfsBrowserEntryMetadata {
                object_id: entry.object_id.to_string(),
                version: entry.version.to_string(),
                modified_at_unix_nanos: entry.modified_at_unix_nanos,
                policy: entry.policy,
                current_locations: entry
                    .current_locations
                    .into_iter()
                    .map(|node_id| node_id.to_string())
                    .collect(),
                placement: None,
            });
            crate::DaemonDfsBrowserEntry {
                name: entry.name,
                path: entry.path,
                kind: entry.kind,
                size: entry.size_bytes,
                metadata,
            }
        })
        .collect();
        sort_daemon_dfs_browser_entries(&mut entries, &request.sort_key, &request.sort_direction);
        let total_count = entries.len();
        let offset = request.offset.min(total_count);
        let limit = request.limit;
        let mut entries: Vec<crate::DaemonDfsBrowserEntry> = match limit {
            Some(limit) => entries.into_iter().skip(offset).take(limit).collect(),
            None => entries.into_iter().skip(offset).collect(),
        };
        if include_metadata {
            for entry in &mut entries {
                let placement = entry.metadata.as_ref().and_then(|metadata| {
                    daemon_dfs_browser_entry_placement(self, &mut service, &entry.path, metadata)
                });
                if let Some(metadata) = entry.metadata.as_mut() {
                    metadata.placement = placement;
                }
            }
        }
        Ok(crate::DaemonDfsBrowserListResponse {
            node_id: service.node_id().to_string(),
            entries,
            total_count,
            offset,
            limit,
        })
    }

    /// Fresh namespace metadata uses the same routed authority checks as the
    /// existing stat API, but never follows the final link or reuses stat cache.
    pub fn namespace_lstat(
        &self,
        request: crate::DaemonDfsBrowserStatRequest,
    ) -> Result<crate::DaemonDfsBrowserStatOutcomeResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&request.path)?;
        let mut service = self.lock_service_for_read()?;
        self.refresh_peer_targets(&mut service);
        match service.posix_namespace_lstat_distributed(&path) {
            Ok(stat) => Ok(crate::DaemonDfsBrowserStatOutcomeResponse::found(
                daemon_dfs_browser_stat_response(stat, Vec::new(), None, None, None),
            )),
            Err(crate::DfsPosixError::NotFound { path }) => {
                Ok(crate::DaemonDfsBrowserStatOutcomeResponse::not_found(path))
            }
            Err(error) => Err(dfs_browser_service_error(error)),
        }
    }

    pub fn stat(
        &self,
        request: crate::DaemonDfsBrowserStatRequest,
    ) -> Result<crate::DaemonDfsBrowserStatOutcomeResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&request.path)?;
        if let Some(response) = self.cached_stat(&path, request.max_staleness_millis) {
            return Ok(crate::DaemonDfsBrowserStatOutcomeResponse::found(response));
        }
        let mut service = self.lock_service_for_read()?;
        self.refresh_peer_targets(&mut service);
        let stat = match service.posix_stat_distributed(&path) {
            Ok(stat) => stat,
            Err(crate::DfsPosixError::NotFound { path: missing_path }) => {
                match service.posix_namespace_lstat_distributed(&path) {
                    Err(crate::DfsPosixError::NotFound { .. }) => {
                        return Ok(crate::DaemonDfsBrowserStatOutcomeResponse::not_found(
                            missing_path,
                        ));
                    }
                    // A dangling symbolic link has a confirmed absent target,
                    // which is POSIX stat NotFound. Missing link-body content
                    // is reported separately as SymlinkContentPending.
                    Ok(candidate) if candidate.kind == crate::DfsObjectKind::Symlink => {
                        return Ok(crate::DaemonDfsBrowserStatOutcomeResponse::not_found(
                            missing_path,
                        ));
                    }
                    Ok(candidate) => {
                        return Err(dfs_browser_service_error(format!(
                            "SIFS stat could not resolve {path}, but authoritative namespace metadata still contains {:?} object {}",
                            candidate.kind, candidate.object_id
                        )));
                    }
                    Err(error) => return Err(dfs_browser_service_error(error)),
                }
            }
            Err(error) => return Err(dfs_browser_service_error(error)),
        };
        let placement = service
            .resolved_placement_policy_for_path(
                &stat.path,
                stat.policy.clone(),
                stat.current_locations.len().max(1),
            )
            .ok();
        let health = daemon_dfs_browser_object_health(self, &service, &stat.object_id);
        let local_pins =
            daemon_dfs_browser_local_pins(&self.data_dir, &stat.file_system_id, &stat.path);
        let local_node_id = service.node_id().clone();
        drop(service);
        let metadata_shard = self
            .observed_metadata_shard_for_stat(&stat.path)
            .ok()
            .flatten()
            .map(|shard| daemon_dfs_browser_metadata_shard(shard, &local_node_id));
        let response =
            daemon_dfs_browser_stat_response(stat, local_pins, health, metadata_shard, placement);
        self.cache_stat(path, response.clone());
        Ok(crate::DaemonDfsBrowserStatOutcomeResponse::found(response))
    }

    fn cached_stat(
        &self,
        path: &str,
        max_staleness_millis: u64,
    ) -> Option<crate::DaemonDfsBrowserStatResponse> {
        if max_staleness_millis == 0 {
            return None;
        }
        let max_staleness = Duration::from_millis(max_staleness_millis);
        let cache = self.stat_cache.lock().ok()?;
        let cached = cache.get(path)?;
        (cached.cached_at.elapsed() <= max_staleness).then(|| cached.response.clone())
    }

    fn cache_stat(&self, path: String, response: crate::DaemonDfsBrowserStatResponse) {
        let Ok(mut cache) = self.stat_cache.lock() else {
            return;
        };
        if cache.len() >= DAEMON_DFS_BROWSER_STAT_CACHE_MAX_ENTRIES
            && !cache.contains_key(&path)
            && let Some(oldest) = cache
                .iter()
                .min_by_key(|(_, cached)| cached.cached_at)
                .map(|(path, _)| path.clone())
        {
            cache.remove(&oldest);
        }
        cache.insert(
            path,
            DfsBrowserCachedStat {
                cached_at: Instant::now(),
                response,
            },
        );
    }

    pub fn invalidate_stat_cache(&self) {
        if let Ok(mut cache) = self.stat_cache.lock() {
            cache.clear();
        }
    }

    /// Add best-effort topology context to an already completed `stat` read.
    /// This materialized view never selects a route, authorizes a mutation, or
    /// turns absence into an operational decision; the caller drops any error.
    fn observed_metadata_shard_for_stat(
        &self,
        path: &str,
    ) -> Result<Option<crate::DfsMetadataShardConfig>, ServiceError> {
        let path = normalize_sifs_browser_path(path)?;
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        if !cluster_paths.state.exists() {
            return Ok(None);
        }
        let mut kv = DfsBrowserObservedControlPlaneKvClient::new(&cluster_paths);
        let store = crate::DfsControlPlaneStore::new(&mut kv);
        store
            .select_metadata_shard_for_path_strict_default_sifs_split(
                &crate::sifs_file_system_id(),
                &path,
            )
            .map_err(dfs_browser_service_error)
    }

    fn read_file(
        &self,
        request: crate::DaemonDfsBrowserReadFileRequest,
    ) -> Result<crate::DaemonDfsBrowserReadFileResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&request.path)?;
        let mut service = self.lock_service_for_read()?;
        self.refresh_peer_targets(&mut service);
        let outcome = service
            .posix_read_file_distributed_outcome(&path)
            .map_err(dfs_browser_service_error)?;
        Ok(daemon_dfs_browser_read_file_response(outcome))
    }

    fn read_object_version(
        &self,
        request: crate::DaemonDfsBrowserReadObjectVersionRequest,
    ) -> Result<crate::DaemonDfsBrowserReadObjectVersionResponse, ServiceError> {
        let object_id = crate::DfsObjectId::new(&request.object_id)
            .map_err(|source| ServiceError::Service(source.to_string()))?;
        let version = crate::DfsVersion::new(&request.version)
            .map_err(|source| ServiceError::Service(source.to_string()))?;
        let mut service = self.lock_service_for_read()?;
        self.refresh_peer_targets(&mut service);
        let bytes = service
            .read_object_version_content(
                &object_id,
                &version,
                &request.content_hash,
                request.size_bytes,
            )
            .map_err(dfs_browser_service_error)?;
        Ok(crate::DaemonDfsBrowserReadObjectVersionResponse { bytes })
    }

    fn read_object_range(
        &self,
        request: crate::DaemonDfsBrowserReadObjectRangeRequest,
    ) -> Result<crate::DaemonDfsBrowserReadObjectRangeResponse, ServiceError> {
        let identity = crate::DfsFileVersionIdentity {
            file_system_id: crate::sifs_file_system_id(),
            object_id: crate::DfsObjectId::new(&request.object_id)
                .map_err(dfs_browser_service_error)?,
            version: crate::DfsVersion::new(&request.version).map_err(dfs_browser_service_error)?,
        };
        let mut service = self.lock_service_for_read()?;
        self.refresh_peer_targets(&mut service);
        let bytes = service
            .read_sifs_version_range(
                &identity,
                &request.content_hash,
                request.size_bytes,
                request.offset,
                request.length,
            )
            .map_err(dfs_browser_service_error)?;
        Ok(crate::DaemonDfsBrowserReadObjectRangeResponse { request, bytes })
    }

    fn read_file_prefix(
        &self,
        request: crate::DaemonDfsBrowserReadFilePrefixRequest,
    ) -> Result<crate::DaemonDfsBrowserReadFilePrefixResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&request.path)?;
        let max_bytes = usize::try_from(request.max_bytes).unwrap_or(usize::MAX);
        let mut service = self.lock_service_for_read()?;
        self.refresh_peer_targets(&mut service);
        let outcome = service
            .posix_read_file_prefix_distributed_outcome(&path, max_bytes)
            .map_err(dfs_browser_service_error)?;
        Ok(daemon_dfs_browser_read_file_prefix_response(outcome))
    }

    pub fn read_files(
        &self,
        request: crate::DaemonDfsBrowserReadFilesRequest,
    ) -> Result<crate::DaemonDfsBrowserReadFilesResponse, ServiceError> {
        let paths = request
            .paths
            .into_iter()
            .map(|path| normalize_sifs_browser_path(&path).map(|normalized| (path, normalized)))
            .collect::<Result<Vec<_>, _>>()?;
        let mut service = self.lock_service_for_read()?;
        self.refresh_peer_targets(&mut service);
        let normalized_paths = paths
            .iter()
            .map(|(_, normalized)| normalized.clone())
            .collect::<Vec<_>>();
        // Keep one browser request as one DFS bulk read. Besides grouping
        // content fetches by source, this lets the DFS layer issue one
        // LookupPaths request per metadata primary instead of holding the
        // resident browser service lock across one routed lookup per file.
        let results = service.posix_read_files_distributed_outcomes(&normalized_paths);
        let files = paths
            .into_iter()
            .zip(results)
            .map(|((requested_path, _), result)| match result {
                Ok(outcome) => daemon_dfs_browser_bulk_file_read_response(requested_path, outcome),
                Err(error) => {
                    crate::DaemonDfsBrowserFileRead::read_failed(requested_path, error.to_string())
                }
            })
            .collect();
        Ok(crate::DaemonDfsBrowserReadFilesResponse { files })
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn write_file(
        &self,
        request: crate::DaemonDfsBrowserWriteFileRequest,
    ) -> Result<crate::DaemonDfsBrowserWriteFileResponse, ServiceError> {
        match self.try_write_file(request)? {
            DfsBrowserWriteAttempt::Complete(response) => Ok(response),
            DfsBrowserWriteAttempt::ConfigurationPending(_) => Err(dfs_browser_service_error(
                "DFS write configuration is awaiting a coherent authoritative projection",
            )),
        }
    }

    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.browser.write.attempt",
        skip_all,
        fields(stage = "dfs.browser.write.attempt")
    )]
    fn try_write_file(
        &self,
        request: crate::DaemonDfsBrowserWriteFileRequest,
    ) -> Result<DfsBrowserWriteAttempt, ServiceError> {
        let trace_started = crate::dfs_perf_trace_enabled().then(Instant::now);
        let mut phase_started = trace_started;
        let mut phase_elapsed = || {
            phase_started.as_mut().map_or(0, |started| {
                let now = Instant::now();
                let elapsed = now.duration_since(*started).as_micros();
                *started = now;
                elapsed
            })
        };
        let path = normalize_sifs_browser_path(&request.path)?;
        let mut service = self.lock_mutation()?;
        let lock_us = phase_elapsed();
        // One coherent, already-published view supplies provider eligibility
        // and the complete zone identity. Reject an unknown floor before any
        // parent creation or write intent can be admitted.
        let configuration = service
            .control_plane
            .as_deref()
            .map(|control| control.write_configuration_readiness())
            .transpose()
            .map_err(dfs_browser_service_error)?;
        let configuration = match configuration {
            Some(crate::DfsWriteConfigurationReadiness::Ready(view)) => view,
            None => None,
            Some(crate::DfsWriteConfigurationReadiness::Pending) => {
                return Ok(DfsBrowserWriteAttempt::ConfigurationPending(request));
            }
        };
        // Parent preparation, policy selection and the write resolve the same
        // path. One lookup scope lets them share each authority observation;
        // any routed mutation among them discards the earlier observations.
        let (mkdir_us, candidates_us, options_us, write_us) = service
            .with_metadata_lookup_cache_mut(|service| -> Result<_, ServiceError> {
                prepare_sifs_browser_write_parent(service, &path)
                    .map_err(dfs_browser_service_error)?;
                let mkdir_us = phase_elapsed();
                let replica_candidates = match configuration.as_deref() {
                    Some(configuration) => configuration
                        .content_replica_candidates()
                        .map_err(dfs_browser_service_error)?,
                    None => self.content_replica_candidates()?,
                };
                let candidates_us = phase_elapsed();
                let options = sifs_browser_write_options_with_configuration(
                    service,
                    &path,
                    &replica_candidates,
                    configuration.as_deref(),
                )?;
                let options_us = phase_elapsed();
                // The response carries no metadata, so the committed stat is
                // not read back.
                service
                    .posix_write_file_committed(&path, &request.bytes, options)
                    .map_err(dfs_browser_service_error)?;
                Ok((mkdir_us, candidates_us, options_us, phase_elapsed()))
            })?;
        self.flush_after_mutation(&mut service)?;
        let flush_us = phase_elapsed();
        if let Some(started) = trace_started {
            crate::dfs_perf_trace(format_args!(
                "dfs-browser-write node={} path={} bytes={} lock_us={} mkdir_us={} candidates_us={} options_us={} write_us={} flush_us={} total_us={}",
                self.node_id,
                path,
                request.bytes.len(),
                lock_us,
                mkdir_us,
                candidates_us,
                options_us,
                write_us,
                flush_us,
                started.elapsed().as_micros(),
            ));
        }
        Ok(DfsBrowserWriteAttempt::Complete(
            crate::DaemonDfsBrowserWriteFileResponse,
        ))
    }

    fn replace_file_if_version(
        &self,
        request: crate::DaemonDfsBrowserReplaceFileIfVersionRequest,
    ) -> Result<crate::DaemonDfsBrowserReplaceFileIfVersionResponse, ServiceError> {
        use crate::DaemonDfsBrowserReplaceFileIfVersionOutcome as Outcome;

        let path = match normalize_sifs_browser_path(&request.path) {
            Ok(path) => path,
            Err(error) => {
                return Ok(crate::DaemonDfsBrowserReplaceFileIfVersionResponse {
                    outcome: Outcome::Rejected {
                        detail: error.to_string(),
                    },
                });
            }
        };
        let object_id = match crate::DfsObjectId::new(&request.expected_object_id) {
            Ok(object_id) => object_id,
            Err(error) => {
                return Ok(crate::DaemonDfsBrowserReplaceFileIfVersionResponse {
                    outcome: Outcome::Rejected {
                        detail: format!("invalid expected SIFS object id: {error}"),
                    },
                });
            }
        };
        let version = match crate::DfsVersion::new(&request.expected_version) {
            Ok(version) => version,
            Err(error) => {
                return Ok(crate::DaemonDfsBrowserReplaceFileIfVersionResponse {
                    outcome: Outcome::Rejected {
                        detail: format!("invalid expected SIFS version: {error}"),
                    },
                });
            }
        };
        let mut service = self.lock_mutation_for_paths(&[path.as_str()])?;
        let replica_candidates = self.content_replica_candidates()?;
        let options = match sifs_browser_write_options(&mut service, &path, &replica_candidates) {
            Ok(options) => options,
            Err(error) => {
                return Ok(crate::DaemonDfsBrowserReplaceFileIfVersionResponse {
                    outcome: Outcome::Rejected {
                        detail: error.to_string(),
                    },
                });
            }
        };
        let stat = match service.posix_replace_file_if_version(
            &path,
            &object_id,
            &version,
            &request.bytes,
            options,
        ) {
            Ok(stat) => stat,
            Err(error) => {
                return Ok(crate::DaemonDfsBrowserReplaceFileIfVersionResponse {
                    outcome: daemon_dfs_browser_conditional_write_error(error),
                });
            }
        };
        let maintenance_warning = self.flush_after_mutation(&mut service).err().map(|error| {
            format!(
                "the file version committed, but follow-up DFS maintenance needs retry: {error}"
            )
        });
        Ok(crate::DaemonDfsBrowserReplaceFileIfVersionResponse {
            outcome: Outcome::Committed {
                receipt: daemon_dfs_browser_write_receipt(&stat),
                maintenance_warning,
            },
        })
    }

    fn durable_replace(
        &self,
        request: crate::DaemonDfsBrowserDurableReplaceRequest,
        changed: &mut bool,
    ) -> Result<crate::DaemonDfsBrowserDurableReplaceOutcome, ServiceError> {
        use crate::DaemonDfsBrowserDurableReplaceOutcome as Outcome;
        if self.node_id.to_string() != request.expected_node_id {
            return Ok(Outcome::Unknown {detail:"The daemon identity changed; the original requester must reconcile this operation".into()});
        }
        let path = normalize_sifs_browser_path(&request.path)?;
        let object_id = crate::DfsObjectId::new(&request.expected_object_id)
            .map_err(dfs_browser_service_error)?;
        let version =
            crate::DfsVersion::new(&request.expected_version).map_err(dfs_browser_service_error)?;
        if request.size_bytes > crate::DAEMON_DFS_BROWSER_DURABLE_REPLACE_MAX_BYTES
            || request.operation_id.is_empty()
        {
            return Ok(Outcome::Conflict {
                detail: "Invalid durable edit request or file exceeds 32 MiB".into(),
            });
        }
        if let Some(bytes) = &request.bytes {
            if bytes.len() as u64 != request.size_bytes
                || format!("blake3:{}", blake3::hash(bytes).to_hex()) != request.content_hash
            {
                return Ok(Outcome::Conflict {
                    detail: "Staged content does not match the durable request".into(),
                });
            }
        }
        let mut service = self.lock_mutation_for_paths(&[path.as_str()])?;
        match service
            .reconcile_stable_write_operation(
                &path,
                &object_id,
                &request.operation_id,
                &request.content_hash,
                request.size_bytes,
            )
            .map_err(dfs_browser_service_error)?
        {
            crate::DfsStableWriteOperationReconciliation::Applied { receipt } => {
                return Ok(Outcome::Applied {
                    operation_id: receipt.operation_id,
                    receipt: crate::DaemonDfsBrowserWriteReceipt {
                        object_id: receipt.object_id.to_string(),
                        version: receipt.target_version.to_string(),
                        content_hash: receipt.content_hash,
                        size_bytes: receipt.content_size_bytes,
                    },
                    maintenance_warning: None,
                });
            }
            crate::DfsStableWriteOperationReconciliation::Conflict { reason } => {
                return Ok(Outcome::Conflict { detail: reason });
            }
            crate::DfsStableWriteOperationReconciliation::AmbiguousOrExpired { reason } => {
                return Ok(Outcome::Unknown { detail: reason });
            }
            crate::DfsStableWriteOperationReconciliation::NotApplied => {}
        }
        let Some(bytes) = request.bytes else {
            return Ok(Outcome::NotApplied);
        };
        let candidates = self.content_replica_candidates()?;
        let options = sifs_browser_write_options(&mut service, &path, &candidates)
            .map_err(dfs_browser_service_error)?;
        let stat = match service.posix_replace_file_if_version_from_reader_with_operation_id(
            &path,
            &object_id,
            &version,
            std::io::Cursor::new(bytes),
            options,
            &request.operation_id,
        ) {
            Ok(stat) => stat,
            Err(error) => {
                return Ok(match daemon_dfs_browser_conditional_write_error(error) {
                    crate::DaemonDfsBrowserReplaceFileIfVersionOutcome::Conflict { detail }
                    | crate::DaemonDfsBrowserReplaceFileIfVersionOutcome::NotFound { detail } => {
                        Outcome::Conflict { detail }
                    }
                    other => Outcome::Unknown {
                        detail: format!("{other:?}"),
                    },
                });
            }
        };
        *changed = true;
        let maintenance_warning = self
            .flush_after_mutation(&mut service)
            .err()
            .map(|error| error.to_string());
        Ok(Outcome::Applied {
            operation_id: request.operation_id,
            receipt: daemon_dfs_browser_write_receipt(&stat),
            maintenance_warning,
        })
    }

    fn create_dir_all(
        &self,
        request: crate::DaemonDfsBrowserCreateDirAllRequest,
    ) -> Result<crate::DaemonDfsBrowserCreateDirAllResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&request.path)?;
        let mut service = self.lock_mutation()?;
        service
            .posix_mkdir_all(&path)
            .map_err(dfs_browser_service_error)?;
        self.flush_after_mutation(&mut service)?;
        Ok(crate::DaemonDfsBrowserCreateDirAllResponse)
    }

    fn rename(
        &self,
        request: crate::DaemonDfsBrowserRenameRequest,
    ) -> Result<crate::DaemonDfsBrowserRenameResponse, ServiceError> {
        let from = normalize_sifs_browser_path(&request.from)?;
        let to = normalize_sifs_browser_path(&request.to)?;
        let source_parent = crate::posix_path::parent_posix_path(&from);
        let target_parent = crate::posix_path::parent_posix_path(&to);
        let mut paths = vec![from.as_str(), to.as_str()];
        paths.extend(source_parent.as_deref());
        paths.extend(target_parent.as_deref());
        let mut service = self.lock_mutation_for_paths(&paths)?;
        service
            .posix_rename(&from, &to)
            .map_err(dfs_browser_service_error)?;
        self.flush_after_mutation(&mut service)?;
        Ok(crate::DaemonDfsBrowserRenameResponse)
    }

    fn delete(
        &self,
        request: crate::DaemonDfsBrowserDeleteRequest,
    ) -> Result<crate::DaemonDfsBrowserDeleteResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&request.path)?;
        let parent = crate::posix_path::parent_posix_path(&path);
        let mut paths = vec![path.as_str()];
        if let Some(parent) = parent.as_deref() {
            paths.push(parent);
        }
        let mut service = self.lock_mutation_for_paths(&paths)?;
        if request.directory {
            service
                .posix_rmdir(&path)
                .map_err(dfs_browser_service_error)?;
        } else {
            service
                .posix_unlink(&path)
                .map_err(dfs_browser_service_error)?;
        }
        self.flush_after_mutation(&mut service)?;
        Ok(crate::DaemonDfsBrowserDeleteResponse)
    }

    fn pin_path_mode(&self, path: &str, mode: crate::DfsPinMode) -> Result<String, ServiceError> {
        let path = normalize_sifs_browser_path(path)?;
        let mut service = self.lock_mutation()?;
        self.refresh_peer_targets(&mut service);
        let stat = service
            .posix_stat_distributed(&path)
            .map_err(dfs_browser_service_error)?;
        let node_id = service.node_id().clone();
        drop(service);
        crate::add_local_dfs_pin(
            crate::default_dfs_pin_root(&self.data_dir),
            crate::DfsPinAddRequest {
                file_system_id: stat.file_system_id,
                path: stat.path,
                object_id: Some(stat.object_id),
                pinned_by_node_id: node_id.clone(),
                mode,
                created_at: time::OffsetDateTime::now_utc(),
            },
        )
        .map_err(dfs_browser_service_error)?;
        if let Ok(mut service) = self.lock_mutation() {
            refresh_browser_content_pin_snapshot(&self.data_dir, &mut service);
        }
        self.invalidate_stat_cache();
        Ok(node_id.to_string())
    }

    fn unpin_path_mode(&self, path: &str, mode: crate::DfsPinMode) -> Result<bool, ServiceError> {
        let path = normalize_sifs_browser_path(path)?;
        let mut service = self.lock_mutation()?;
        let stat = service
            .posix_stat_distributed(&path)
            .map_err(dfs_browser_service_error)?;
        let node_id = service.node_id().clone();
        drop(service);
        let removed = crate::remove_local_dfs_pin(
            crate::default_dfs_pin_root(&self.data_dir),
            &stat.file_system_id,
            &stat.path,
            Some(&node_id),
            Some(&mode),
        )
        .map_err(dfs_browser_service_error)?;
        if removed {
            if let Ok(mut service) = self.lock_mutation() {
                refresh_browser_content_pin_snapshot(&self.data_dir, &mut service);
            }
            self.invalidate_stat_cache();
        }
        Ok(removed)
    }

    pub fn pin_metadata(
        &self,
        request: crate::DaemonDfsBrowserPinMetadataRequest,
    ) -> Result<crate::DaemonDfsBrowserPinMetadataResponse, ServiceError> {
        let node_id = self.pin_path_mode(&request.path, crate::DfsPinMode::Metadata)?;
        Ok(crate::DaemonDfsBrowserPinMetadataResponse { node_id })
    }

    fn unpin_metadata(
        &self,
        request: crate::DaemonDfsBrowserUnpinMetadataRequest,
    ) -> Result<crate::DaemonDfsBrowserUnpinMetadataResponse, ServiceError> {
        let removed = self.unpin_path_mode(&request.path, crate::DfsPinMode::Metadata)?;
        Ok(crate::DaemonDfsBrowserUnpinMetadataResponse { removed })
    }

    fn pin_content(
        &self,
        request: crate::DaemonDfsBrowserPinContentRequest,
    ) -> Result<crate::DaemonDfsBrowserPinContentResponse, ServiceError> {
        let node_id = self.pin_path_mode(&request.path, crate::DfsPinMode::Content)?;
        Ok(crate::DaemonDfsBrowserPinContentResponse { node_id })
    }

    fn unpin_content(
        &self,
        request: crate::DaemonDfsBrowserUnpinContentRequest,
    ) -> Result<crate::DaemonDfsBrowserUnpinContentResponse, ServiceError> {
        let removed = self.unpin_path_mode(&request.path, crate::DfsPinMode::Content)?;
        Ok(crate::DaemonDfsBrowserUnpinContentResponse { removed })
    }

    fn transfer_metadata_primary(
        &self,
        request: crate::DaemonDfsBrowserTransferMetadataPrimaryRequest,
        runtime: &tokio::runtime::Handle,
    ) -> Result<crate::DaemonDfsBrowserTransferMetadataPrimaryResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&request.path)?;
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        if !cluster_paths.state.exists() {
            return Err(ServiceError::Service(
                "SIFS metadata primary transfer requires a cluster".to_string(),
            ));
        }
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
            .map_err(|error| ServiceError::Service(error.to_string()))?;
        let local_node_id = state.local_node_id;
        let mut kv = self
            .host
            .control_plane_client(&cluster_paths, runtime)
            .ok_or_else(|| {
                ServiceError::Service(
                    "SIFS metadata primary transfer has no authoritative control plane".to_string(),
                )
            })?;
        let mut store = crate::DfsControlPlaneStore::new(kv.as_mut());
        let previous = store
            .select_metadata_shard_for_path_strict_default_sifs_split(
                &crate::sifs_file_system_id(),
                &path,
            )
            .map_err(dfs_browser_service_error)?
            .ok_or_else(|| {
                ServiceError::Service(format!("no SIFS metadata shard controls {path}"))
            })?;
        if let Some(expected_epoch) = request.expected_epoch
            && previous.epoch != expected_epoch
        {
            return Err(ServiceError::Service(format!(
                "SIFS metadata shard {} epoch is {}, expected {}",
                previous.shard_id, previous.epoch, expected_epoch
            )));
        }
        if previous.primary_node_id == local_node_id {
            return Ok(crate::DaemonDfsBrowserTransferMetadataPrimaryResponse {
                file_system_id: previous.file_system_id.to_string(),
                shard_id: previous.shard_id.clone(),
                previous_primary_node_id: previous.primary_node_id.to_string(),
                new_primary_node_id: previous.primary_node_id.to_string(),
                previous_epoch: previous.epoch,
                new_epoch: previous.epoch,
                changed: false,
                metadata_shard: daemon_dfs_browser_metadata_shard(previous, &local_node_id),
            });
        }
        if !previous
            .replica_node_ids
            .iter()
            .any(|node_id| node_id == &local_node_id)
        {
            return Err(ServiceError::Service(format!(
                "local node {} is not a metadata replica for SIFS shard {}",
                local_node_id, previous.shard_id
            )));
        }
        let expected_epoch = previous.epoch;
        let updated = store
            .transfer_metadata_shard_primary(
                &previous.file_system_id,
                &previous.shard_id,
                expected_epoch,
                local_node_id.clone(),
            )
            .map_err(dfs_browser_service_error)?
            .ok_or_else(|| {
                ServiceError::Service(format!(
                    "SIFS metadata shard {} changed while transferring primary; refresh placement and retry",
                    previous.shard_id
                ))
            })?;
        if let Ok(mut service) = self.lock_mutation() {
            service.invalidate_sifs_metadata_shard_cache();
        }
        self.invalidate_stat_cache();
        Ok(crate::DaemonDfsBrowserTransferMetadataPrimaryResponse {
            file_system_id: previous.file_system_id.to_string(),
            shard_id: previous.shard_id,
            previous_primary_node_id: previous.primary_node_id.to_string(),
            new_primary_node_id: updated.primary_node_id.to_string(),
            previous_epoch: expected_epoch,
            new_epoch: updated.epoch,
            changed: true,
            metadata_shard: daemon_dfs_browser_metadata_shard(updated, &local_node_id),
        })
    }

    fn placement_preview(
        &self,
        request: crate::DaemonDfsBrowserPlacementPreviewRequest,
        runtime: &tokio::runtime::Handle,
    ) -> Result<crate::DaemonDfsBrowserPlacementPreviewResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&request.path)?;
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        if !cluster_paths.state.exists() {
            return Err(ServiceError::Service(
                "managed DFS placement requires a cluster".to_string(),
            ));
        }
        let mut kv = self
            .host
            .control_plane_client(&cluster_paths, runtime)
            .ok_or_else(|| {
                ServiceError::Service(
                    "managed DFS placement preview has no authoritative control plane".to_string(),
                )
            })?;
        let base_config = crate::DfsControlPlaneStore::new(kv.as_mut())
            .get_file_system_config(&crate::sifs_file_system_id())
            .map_err(dfs_browser_service_error)?
            .ok_or_else(|| {
                ServiceError::Service("SIFS cluster policy is unavailable".to_string())
            })?;
        let mut config = base_config.clone();
        mutate_daemon_dfs_browser_placement_config(
            &mut config,
            &path,
            request.scope,
            request.policy.clone(),
        )?;
        // `config` already contains the proposed mutation, including a
        // `policy=None` removal. Passing a materialized config through the
        // shared helper prevents a second override from restoring that rule.
        let (preview, _, _, _) = self.preview_placement_for_config(
            &cluster_paths,
            &base_config,
            &config,
            &path,
            request.scope,
        )?;
        Ok(crate::DaemonDfsBrowserPlacementPreviewResponse { preview })
    }

    fn placement_update_v2(
        &self,
        request: crate::DaemonDfsBrowserPlacementUpdateV2Request,
        runtime: &tokio::runtime::Handle,
    ) -> Result<crate::DaemonDfsBrowserPlacementUpdateResponse, ServiceError> {
        self.placement_update_inner(
            request.path,
            request.policy,
            request.scope,
            request.expected_preview_token,
            runtime,
        )
    }

    fn placement_update_inner(
        &self,
        path: String,
        policy: Option<crate::DfsPlacementPolicy>,
        scope: crate::DfsPlacementScope,
        expected_preview_token: crate::DfsPlacementPreviewToken,
        runtime: &tokio::runtime::Handle,
    ) -> Result<crate::DaemonDfsBrowserPlacementUpdateResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&path)?;
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        if !cluster_paths.state.exists() {
            return Err(ServiceError::Service(
                "managed DFS placement requires a cluster".to_string(),
            ));
        }
        let mut kv = self
            .host
            .control_plane_client(&cluster_paths, runtime)
            .ok_or_else(|| {
                ServiceError::Service(
                    "managed DFS placement update has no authoritative control plane".to_string(),
                )
            })?;
        let mut store = crate::DfsControlPlaneStore::new(kv.as_mut());
        let expected = store
            .get_file_system_config(&crate::sifs_file_system_id())
            .map_err(dfs_browser_service_error)?
            .ok_or_else(|| {
                ServiceError::Service("SIFS cluster policy is unavailable".to_string())
            })?;
        let mut desired = expected.clone();
        mutate_daemon_dfs_browser_placement_config(&mut desired, &path, scope, policy)?;
        let (preview, unavailable_node_ids, candidate_snapshot, placement_snapshot) =
            self.preview_placement_for_config(&cluster_paths, &expected, &desired, &path, scope)?;
        require_matching_daemon_dfs_browser_placement_preview_token(
            &expected_preview_token,
            &preview.preview_token,
        )?;
        require_satisfiable_daemon_dfs_browser_placement(&preview)?;
        let now = time::OffsetDateTime::now_utc()
            .unix_timestamp_nanos()
            .try_into()
            .unwrap_or(1_000_000_000);
        let reconciliation = crate::control_plane::build_placement_reconciliation_job(
            sinomo_core::Id::generate().to_string(),
            &desired,
            path,
            scope,
            unavailable_node_ids,
            candidate_snapshot,
            placement_snapshot,
            now,
        )
        .map_err(dfs_browser_service_error)?;

        let committed = require_accepted_daemon_dfs_browser_placement_update(
            store.compare_and_put_placement_update(&expected, &desired, &reconciliation),
            &desired.file_system_id,
        )?;
        self.host.notify_maintenance();
        Ok(crate::DaemonDfsBrowserPlacementUpdateResponse {
            preview,
            reconciliation: committed.reconciliation,
            committed_revision: committed.revision.0,
            visibility: crate::DfsPlacementConfigurationVisibility::Pending {
                detail: "configuration publication has not yet been observed".into(),
            },
        })
    }

    fn preview_placement_for_config(
        &self,
        cluster_paths: &ClusterPaths,
        base_config: &crate::DfsFileSystemConfig,
        config: &crate::DfsFileSystemConfig,
        path: &str,
        scope: crate::DfsPlacementScope,
    ) -> Result<
        (
            crate::DfsPlacementPreview,
            BTreeSet<sinomo_core::node::NodeId>,
            Vec<crate::DfsNodeAdvertisement>,
            Option<crate::DfsPlacementSnapshot>,
        ),
        ServiceError,
    > {
        let (stat, observer_node_id, candidates, placement_snapshot, pinned_replica_node_ids) = {
            let mut service = self.lock_service_for_read()?;
            self.refresh_peer_targets(&mut service);
            let stat = service
                .posix_stat_distributed(path)
                .map_err(dfs_browser_service_error)?;
            let observer_node_id = service.node_id().clone();
            let candidates = service
                .placement_content_candidates()
                .map_err(dfs_browser_service_error)?;
            let placement_snapshot = service
                .placement_snapshot_cache()
                .snapshot()
                .map(|snapshot| (*snapshot).clone());
            let pinned_replica_node_ids = service.content_pin_nodes(&stat.object_id);
            (
                stat,
                observer_node_id,
                candidates,
                placement_snapshot,
                pinned_replica_node_ids,
            )
        };
        if candidates.is_empty() {
            return Err(ServiceError::Service(
                "cannot apply SIFS placement without content candidates".to_string(),
            ));
        }
        let object_fence = crate::DfsPlacementPreviewObjectFence::from(&stat);
        let initial_connectivity =
            daemon_dfs_browser_placement_connectivity_snapshot(cluster_paths);
        let unavailable_node_ids = initial_connectivity.explicitly_unavailable_node_ids();
        let mut preview = crate::preview_placement_with_snapshot_and_unavailable_nodes(
            config,
            &stat.path,
            stat.object_id.clone(),
            stat.version.clone(),
            stat.policy.clone(),
            observer_node_id,
            stat.current_locations.clone(),
            candidates.clone(),
            None,
            scope,
            placement_snapshot.as_ref(),
            &pinned_replica_node_ids,
            &unavailable_node_ids,
        )
        .map_err(dfs_browser_service_error)?;

        let mut aggregate_impact = crate::DfsPlacementImpact::default();
        let mut aggregate_diagnostics = Vec::new();
        let mut cursor = None;
        loop {
            // The cached service is shared by all browser calls. Holding its
            // mutex for one bounded metadata page keeps large subtree previews
            // from starving unrelated reads and writes.
            let page = {
                let mut service = self.lock_service_for_read()?;
                service
                    .placement_impact_page_for_config_with_snapshot_and_unavailable_nodes(
                        config,
                        &stat.path,
                        scope,
                        &candidates,
                        cursor.as_ref(),
                        DAEMON_DFS_BROWSER_PLACEMENT_IMPACT_PAGE_SIZE,
                        &unavailable_node_ids,
                        placement_snapshot.as_ref(),
                    )
                    .map_err(dfs_browser_service_error)?
            };
            crate::merge_placement_impact(&mut aggregate_impact, &page.impact);
            extend_unique_daemon_dfs_browser_placement_diagnostics(
                &mut aggregate_diagnostics,
                page.blocking_diagnostics,
            );
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        preview.impact = aggregate_impact;
        extend_daemon_dfs_browser_preview_diagnostics(&mut preview, aggregate_diagnostics);

        // Connectivity can change while a multi-page impact scan is running.
        // Re-read it after releasing the final service guard and fail closed if
        // a node selected by the stable plan has since become explicitly down.
        let final_connectivity = daemon_dfs_browser_placement_connectivity_snapshot(cluster_paths);
        enrich_daemon_dfs_browser_placement_connectivity_from_snapshot(
            &mut preview,
            &final_connectivity,
        );
        let compatibility_state = sinomo_core::cluster::read_cluster_state_without_kv(
            cluster_paths,
        )
        .map_err(|error| {
            ServiceError::Service(format!(
                "cannot verify placement daemon compatibility: {error}"
            ))
        })?;
        if let Some(diagnostic) =
            crate::placement_reconciliation_compatibility_diagnostic(&compatibility_state)
            && !preview.blocking_diagnostics.contains(&diagnostic)
        {
            preview.blocking_diagnostics.push(diagnostic);
        }
        if !preview.blocking_diagnostics.is_empty() {
            preview.impact.complete = false;
        }
        // Keep the planning exclusions stable across the scan. A candidate
        // that recovers before the final observational refresh must not be
        // silently reintroduced by reconciliation, while newly unavailable
        // optional candidates should remain excluded as well.
        let mut reconciliation_unavailable_node_ids = unavailable_node_ids;
        reconciliation_unavailable_node_ids.extend(preview.unavailable_node_ids());
        crate::bind_placement_preview_token(
            &mut preview,
            base_config,
            config,
            path,
            scope,
            &object_fence,
            &candidates,
            &reconciliation_unavailable_node_ids,
        )
        .map_err(dfs_browser_service_error)?;
        Ok((
            preview,
            reconciliation_unavailable_node_ids,
            candidates,
            placement_snapshot,
        ))
    }

    fn placement_progress(
        &self,
        request: crate::DaemonDfsBrowserPlacementProgressRequest,
    ) -> Result<crate::DaemonDfsBrowserPlacementProgressResponse, ServiceError> {
        let mut control_plane = self.replica_control_plane.lock().map_err(|_| {
            ServiceError::Service("daemon DFS browser control-plane mutex poisoned".to_string())
        })?;
        let kv = control_plane.as_deref_mut().ok_or_else(|| {
            ServiceError::Service(
                "managed DFS placement progress has no authoritative control plane".to_string(),
            )
        })?;
        let reconciliation = crate::DfsControlPlaneStore::new(kv)
            .placement_reconciliation(&crate::sifs_file_system_id(), &request.job_id)
            .map_err(dfs_browser_service_error)?
            .ok_or_else(|| {
                ServiceError::Service(format!(
                    "unknown DFS placement reconciliation job: {}",
                    request.job_id
                ))
            })?;
        Ok(crate::DaemonDfsBrowserPlacementProgressResponse { reconciliation })
    }

    pub fn placement_activity(
        &self,
        request: crate::DaemonDfsBrowserPlacementActivityRequest,
    ) -> Result<crate::DaemonDfsBrowserPlacementActivityResponse, ServiceError> {
        if request
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.job_id.trim().is_empty())
        {
            return Err(ServiceError::Service(
                "DFS placement activity cursor has an empty job id".to_string(),
            ));
        }
        let path_prefix = request
            .path_prefix
            .as_deref()
            .map(normalize_sifs_browser_path)
            .transpose()?;
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        let mut kv = DfsBrowserObservedControlPlaneKvClient::new(&cluster_paths);
        let page = crate::DfsControlPlaneStore::new(&mut kv)
            .placement_reconciliation_activity(
                &crate::sifs_file_system_id(),
                &request.statuses,
                path_prefix.as_deref(),
                request.cursor.as_ref(),
                request.limit,
            )
            .map_err(dfs_browser_service_error)?;
        Ok(crate::DaemonDfsBrowserPlacementActivityResponse { page })
    }

    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.browser.flush",
        skip_all,
        fields(stage = "dfs.browser.flush")
    )]
    fn flush_after_mutation(
        &self,
        service: &mut crate::MountedSifsService,
    ) -> Result<(), ServiceError> {
        self.invalidate_stat_cache();
        service
            .flush_detached_transport_sends()
            .map_err(dfs_browser_service_error)?;
        service
            .process_incoming()
            .map_err(dfs_browser_service_error)?;
        Ok(())
    }
}

fn refresh_browser_content_pin_snapshot(
    data_dir: &SinomoDataDir,
    service: &mut crate::MountedSifsService,
) {
    let pins = crate::list_local_dfs_pins(crate::default_dfs_pin_root(data_dir), None)
        .unwrap_or_default()
        .into_iter()
        .filter(|pin| pin.mode == crate::DfsPinMode::Content)
        .filter_map(|pin| {
            pin.object_id
                .map(|object_id| (object_id, pin.pinned_by_node_id))
        });
    service.set_content_pin_snapshot(pins);
}

impl Service for HostedDfsBrowserService {
    fn name(&self) -> &'static str {
        "dfs-browser"
    }

    fn routes(&self) -> &'static [&'static str] {
        DFS_CORE_BROWSER_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        self.routes()
            .iter()
            .copied()
            .map(|route| {
                if route == DFS_CORE_COMPONENT_ACCESS_ROUTE {
                    return ServiceRoute::new(route, "dfs")
                        .with_auth_scope("component")
                        .require_capability(DFS_CORE_COMPONENT_ACCESS_OPERATION);
                }
                let path_access = daemon_dfs_browser_route_is_path_access(route);
                let mut descriptor = ServiceRoute::new(route, DAEMON_RPC_CATEGORY)
                    .with_auth_scope(if path_access {
                        sinomo_module::ModuleRouteAuth::LocalOrClusterMember.as_scope()
                    } else {
                        sinomo_module::ModuleRouteAuth::LocalCommand.as_scope()
                    })
                    .require_capability("daemon.dfs.browser")
                    .with_concurrency_limit(4)
                    .with_timeout(DAEMON_DFS_BROWSER_ROUTE_TIMEOUT);
                if path_access {
                    descriptor = descriptor.require_capability(crate::SIFS_PATH_ACCESS_CAPABILITY);
                }
                descriptor
            })
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let runtime = self.runtime.clone();
        let backend_host = Arc::clone(&self.host);
        let effect_host = Arc::clone(&self.host);
        Box::pin(async move {
            let runtime = require_service_runtime(runtime.as_ref(), "DFS browser")?.clone();
            let route = request.route.clone();
            let request_traceparent = request.traceparent().map(ToString::to_string);
            let data_dir = Arc::clone(&ctx.data_dir);
            match route.as_str() {
                DAEMON_DFS_BROWSER_NO_LINKS_ROUTE => {
                    let payload: crate::DaemonDfsBrowserNoLinksRequest =
                        ctx.decode_request_body(&request).await?;
                    no_links::validate_cluster_namespace(
                        &data_dir,
                        payload.expected_cluster_id.as_ref(),
                    )?;
                    let content_write = matches!(
                        &payload.operation,
                        crate::DaemonDfsBrowserNoLinksOperation::CreateNew { .. }
                            | crate::DaemonDfsBrowserNoLinksOperation::ReplaceIfVersion { .. }
                    );
                    let backend_data_dir = Arc::clone(&data_dir);
                    let backend_runtime = runtime.clone();
                    let attempt_host = Arc::clone(&backend_host);
                    let result = if content_write {
                        run_configured_browser_task(
                            data_dir,
                            route.clone(),
                            "DFS scoped no-link content write",
                            runtime,
                            backend_host,
                            payload,
                            move |payload| {
                                DfsBrowserBackend::for_data_dir(
                                    Arc::clone(&backend_data_dir),
                                    &backend_runtime,
                                    Arc::clone(&attempt_host),
                                )?
                                .try_no_links(payload)
                            },
                        )
                        .await?
                    } else {
                        // Namespace-only mkdir and reads retain the normal
                        // browser path without a content-placement dependency.
                        run_blocking_browser_task(
                            data_dir,
                            route.clone(),
                            "DFS scoped no-link operation",
                            move || {
                                DfsBrowserBackend::for_data_dir(
                                    backend_data_dir,
                                    &backend_runtime,
                                    attempt_host,
                                )?
                                .no_links(payload)
                            },
                        )
                        .await?
                    };
                    ctx.respond_value(DAEMON_DFS_BROWSER_NO_LINKS_ROUTE, result)?;
                }
                DFS_CORE_COMPONENT_ACCESS_ROUTE => {
                    let _: () = ctx.decode_request_body(&request).await?;
                    let _owner = backend_host.operation_guard()?.ok_or_else(|| {
                        dfs_browser_service_error("DFS component owner is unavailable")
                    })?;
                    ctx.respond_value(route, true)?;
                }
                DAEMON_DFS_BROWSER_STATUS_ROUTE => {
                    let payload: crate::DaemonDfsBrowserStatusRequest =
                        ctx.decode_request_body(&request).await?;
                    let include_health = payload.include_health;
                    let task_data_dir = Arc::clone(&data_dir);
                    let task = move || {
                        if !include_health {
                            // Identity discovery does not require a filesystem
                            // open. In particular a cold browser must not start
                            // or wait behind representation preparation merely
                            // to identify the already-mounted native owner.
                            let _owner = backend_host.operation_guard()?.ok_or_else(|| {
                                dfs_browser_service_error("DFS component owner is unavailable")
                            })?;
                            let state = sinomo_core::cluster::read_cluster_state_without_kv(
                                &ClusterPaths::new(&task_data_dir),
                            )
                            .map_err(dfs_browser_service_error)?;
                            return Ok(crate::DaemonDfsBrowserStatusResponse {
                                node_id: state.local_node_id.to_string(),
                                health: None,
                                health_error: None,
                            });
                        }
                        let backend = DfsBrowserBackend::for_data_dir(
                            task_data_dir,
                            &runtime,
                            Arc::clone(&backend_host),
                        )?;
                        let (health, health_error) = if include_health {
                            match backend.health_summary() {
                                Ok(health) => (Some(health), None),
                                Err(error) => (None, Some(error.to_string())),
                            }
                        } else {
                            (None, None)
                        };
                        Ok(crate::DaemonDfsBrowserStatusResponse {
                            node_id: backend.node_id()?,
                            health,
                            health_error,
                        })
                    };
                    let result = if include_health {
                        run_blocking_browser_task(data_dir, route, "DFS browser status", task)
                            .await?
                    } else {
                        // Immutable discovery stays responsive behind long
                        // namespace operations; it does not use the service lock.
                        run_blocking_service_task(route, "DFS browser identity", task).await?
                    };
                    ctx.respond_value(DAEMON_DFS_BROWSER_STATUS_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_LIST_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result = run_blocking_browser_task(
                        Arc::clone(&data_dir),
                        route.clone(),
                        "DFS browser list",
                        move || {
                            DfsBrowserBackend::for_data_dir(
                                data_dir,
                                &runtime,
                                Arc::clone(&backend_host),
                            )?
                            .list(payload)
                        },
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_LIST_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_NAMESPACE_LSTAT_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result = run_blocking_browser_task(
                        Arc::clone(&data_dir),
                        route.clone(),
                        "DFS browser namespace lstat",
                        move || {
                            DfsBrowserBackend::for_data_dir(
                                data_dir,
                                &runtime,
                                Arc::clone(&backend_host),
                            )?
                            .namespace_lstat(payload)
                        },
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_NAMESPACE_LSTAT_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_STAT_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result = run_blocking_browser_task(
                        Arc::clone(&data_dir),
                        route.clone(),
                        "DFS browser stat",
                        move || {
                            DfsBrowserBackend::for_data_dir(
                                data_dir,
                                &runtime,
                                Arc::clone(&backend_host),
                            )?
                            .stat(payload)
                        },
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_STAT_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_READ_FILE_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result = run_blocking_browser_task(
                        Arc::clone(&data_dir),
                        route.clone(),
                        "DFS browser read file",
                        move || {
                            DfsBrowserBackend::for_data_dir(
                                data_dir,
                                &runtime,
                                Arc::clone(&backend_host),
                            )?
                            .read_file(payload)
                        },
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_READ_FILE_ROUTE, result)?;
                }

                DAEMON_DFS_BROWSER_READ_OBJECT_VERSION_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result = run_blocking_browser_task(
                        Arc::clone(&data_dir),
                        route.clone(),
                        "DFS browser read object version",
                        move || {
                            DfsBrowserBackend::for_data_dir(
                                data_dir,
                                &runtime,
                                Arc::clone(&backend_host),
                            )?
                            .read_object_version(payload)
                        },
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_READ_OBJECT_VERSION_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_READ_OBJECT_RANGE_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result = run_blocking_browser_task(
                        Arc::clone(&data_dir),
                        route.clone(),
                        "DFS browser read object version range",
                        move || {
                            DfsBrowserBackend::for_data_dir(
                                data_dir,
                                &runtime,
                                Arc::clone(&backend_host),
                            )?
                            .read_object_range(payload)
                        },
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_READ_OBJECT_RANGE_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result = run_blocking_browser_task(
                        Arc::clone(&data_dir),
                        route.clone(),
                        "DFS browser read file prefix",
                        move || {
                            DfsBrowserBackend::for_data_dir(
                                data_dir,
                                &runtime,
                                Arc::clone(&backend_host),
                            )?
                            .read_file_prefix(payload)
                        },
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_READ_FILES_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result = run_blocking_browser_task(
                        Arc::clone(&data_dir),
                        route.clone(),
                        "DFS browser read files",
                        move || {
                            DfsBrowserBackend::for_data_dir(
                                data_dir,
                                &runtime,
                                Arc::clone(&backend_host),
                            )?
                            .read_files(payload)
                        },
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_READ_FILES_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE => {
                    tracing::Instrument::instrument(async {
                    let mut payload: crate::DaemonDfsBrowserWriteFileRequest =
                        ctx.decode_request_body(&request).await?;
                    let changed_path = payload.path.clone();
                    let publish_data_dir = Arc::clone(&data_dir);
                    let projection_deadline =
                        tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
                    let mut replans = 0;
                    let result = loop {
                        // Readiness owns no browser/FIFO/backend admission. All
                        // retries retain this original projection deadline.
                        tokio::time::timeout_at(
                            projection_deadline,
                            backend_host.ensure_write_configuration_ready(&data_dir, &runtime),
                        )
                        .await
                        .map_err(|_| {
                            dfs_browser_service_error(
                                "DFS write configuration readiness deadline elapsed",
                            )
                        })??;
                        let backend_data_dir = Arc::clone(&data_dir);
                        let backend_runtime = runtime.clone();
                        let attempt_host = Arc::clone(&backend_host);
                        let traceparent = request_traceparent.clone();
                        let attempt = run_blocking_browser_task(
                            Arc::clone(&data_dir),
                            route.clone(),
                            "DFS browser write file",
                            move || {
                                crate::with_dfs_traceparent(traceparent.as_deref(), || {
                                    let backend = DfsBrowserBackend::for_data_dir(
                                        backend_data_dir,
                                        &backend_runtime,
                                        attempt_host,
                                    )?;
                                    match backend.try_write_file(payload)? {
                                        DfsBrowserWriteAttempt::Complete(response) => Ok((
                                            DfsBrowserWriteAttempt::Complete(response),
                                            Some(backend.event_origin_node_id()?),
                                        )),
                                        DfsBrowserWriteAttempt::ConfigurationPending(payload) => {
                                            Ok((
                                                DfsBrowserWriteAttempt::ConfigurationPending(
                                                    payload,
                                                ),
                                                None,
                                            ))
                                        }
                                    }
                                })
                            },
                        )
                        .await?;
                        match attempt {
                            (DfsBrowserWriteAttempt::Complete(response), Some(origin)) => {
                                break (response, origin);
                            }
                            (DfsBrowserWriteAttempt::ConfigurationPending(original), _) => {
                                if replans >= 2 {
                                    return Err(dfs_browser_service_error(
                                        "DFS write configuration remained pending after bounded preexecution replanning",
                                    ));
                                }
                                // The worker and FIFO permit have returned and
                                // no parent or intent was prepared. Reuse the
                                // owned original bytes and request unchanged.
                                payload = original;
                                replans += 1;
                                tracing::trace!(target: "sinomo.dfs.perf", replans, maximum = 2,
                                    "DFS browser write configuration preexecution replan");
                            }
                            _ => unreachable!("completed browser write retains its event origin"),
                        }
                    };
                    effect_host.schedule_metadata_publish(
                        publish_data_dir.as_ref(),
                        &runtime,
                        request_traceparent.as_deref(),
                    );
                    effect_host.publish_namespace_change(
                        result.1.clone(),
                        "write-file",
                        vec![changed_path],
                    );
                    ctx.respond_value(DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE, result.0)?;
                    Ok::<(), ServiceError>(())
                    }, tracing::trace_span!(target: "sinomo.perf", "dfs.browser.write", stage = "dfs.browser.write"))
                    .await?;
                }
                DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE => {
                    let payload: crate::DaemonDfsBrowserReplaceFileIfVersionRequest =
                        ctx.decode_request_body(&request).await?;
                    let changed_path = payload.path.clone();
                    let backend_data_dir = Arc::clone(&data_dir);
                    let publish_data_dir = Arc::clone(&data_dir);
                    let backend_runtime = runtime.clone();
                    let traceparent = request_traceparent.clone();
                    let result = run_blocking_browser_task(
                        Arc::clone(&data_dir),
                        route.clone(),
                        "DFS browser conditional replace file",
                        move || {
                            crate::with_dfs_traceparent(traceparent.as_deref(), || {
                                let backend = DfsBrowserBackend::for_data_dir(
                                    backend_data_dir,
                                    &backend_runtime,
                                    Arc::clone(&backend_host),
                                )?;
                                let result = backend.replace_file_if_version(payload)?;
                                Ok((result, backend.event_origin_node_id()?))
                            })
                        },
                    )
                    .await?;
                    if result.0.outcome.is_committed() {
                        effect_host.schedule_metadata_publish(
                            publish_data_dir.as_ref(),
                            &runtime,
                            request_traceparent.as_deref(),
                        );
                        effect_host.publish_namespace_change(
                            result.1.clone(),
                            "replace-file-if-version",
                            vec![changed_path],
                        );
                    }
                    ctx.respond_value(DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE, result.0)?;
                }
                DAEMON_DFS_BROWSER_DURABLE_REPLACE_ROUTE => {
                    let payload: crate::DaemonDfsBrowserDurableReplaceRequest =
                        ctx.decode_request_body(&request).await?;
                    let changed_path = payload.path.clone();
                    let backend_data_dir = Arc::clone(&data_dir);
                    let backend_runtime = runtime.clone();
                    let result = run_blocking_browser_task(
                        Arc::clone(&data_dir),
                        route.clone(),
                        "DFS durable external edit",
                        move || {
                            let backend = DfsBrowserBackend::for_data_dir(
                                backend_data_dir,
                                &backend_runtime,
                                Arc::clone(&backend_host),
                            )?;
                            let mut changed = false;
                            Ok((
                                backend.durable_replace(payload, &mut changed)?,
                                backend.event_origin_node_id()?,
                                changed,
                            ))
                        },
                    )
                    .await?;
                    if result.2 {
                        effect_host.schedule_metadata_publish(
                            data_dir.as_ref(),
                            &runtime,
                            request_traceparent.as_deref(),
                        );
                        effect_host.publish_namespace_change(
                            result.1,
                            "durable-replace",
                            vec![changed_path],
                        );
                    }
                    ctx.respond_value(DAEMON_DFS_BROWSER_DURABLE_REPLACE_ROUTE, result.0)?;
                }
                DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE => {
                    let payload: crate::DaemonDfsBrowserCreateDirAllRequest =
                        ctx.decode_request_body(&request).await?;
                    let changed_path = payload.path.clone();
                    let backend_data_dir = Arc::clone(&data_dir);
                    let publish_data_dir = Arc::clone(&data_dir);
                    let backend_runtime = runtime.clone();
                    let traceparent = request_traceparent.clone();
                    let result = run_blocking_browser_task(
                        Arc::clone(&data_dir),
                        route.clone(),
                        "DFS browser create dir all",
                        move || {
                            crate::with_dfs_traceparent(traceparent.as_deref(), || {
                                let backend = DfsBrowserBackend::for_data_dir(
                                    backend_data_dir,
                                    &backend_runtime,
                                    Arc::clone(&backend_host),
                                )?;
                                let result = backend.create_dir_all(payload)?;
                                Ok((result, backend.event_origin_node_id()?))
                            })
                        },
                    )
                    .await?;
                    effect_host.schedule_metadata_publish(
                        publish_data_dir.as_ref(),
                        &runtime,
                        request_traceparent.as_deref(),
                    );
                    effect_host.publish_namespace_change(
                        result.1.clone(),
                        "create-dir-all",
                        vec![changed_path],
                    );
                    ctx.respond_value(DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE, result.0)?;
                }
                DAEMON_DFS_BROWSER_RENAME_ROUTE => {
                    let payload: crate::DaemonDfsBrowserRenameRequest =
                        ctx.decode_request_body(&request).await?;
                    let changed_paths = vec![payload.from.clone(), payload.to.clone()];
                    let backend_data_dir = Arc::clone(&data_dir);
                    let publish_data_dir = Arc::clone(&data_dir);
                    let backend_runtime = runtime.clone();
                    let traceparent = request_traceparent.clone();
                    let result = run_blocking_browser_task(
                        Arc::clone(&data_dir),
                        route.clone(),
                        "DFS browser rename",
                        move || {
                            crate::with_dfs_traceparent(traceparent.as_deref(), || {
                                let backend = DfsBrowserBackend::for_data_dir(
                                    backend_data_dir,
                                    &backend_runtime,
                                    Arc::clone(&backend_host),
                                )?;
                                let result = backend.rename(payload)?;
                                Ok((result, backend.event_origin_node_id()?))
                            })
                        },
                    )
                    .await?;
                    effect_host.schedule_metadata_publish(
                        publish_data_dir.as_ref(),
                        &runtime,
                        request_traceparent.as_deref(),
                    );
                    effect_host.publish_namespace_change(result.1.clone(), "rename", changed_paths);
                    ctx.respond_value(DAEMON_DFS_BROWSER_RENAME_ROUTE, result.0)?;
                }
                DAEMON_DFS_BROWSER_DELETE_ROUTE => {
                    let payload: crate::DaemonDfsBrowserDeleteRequest =
                        ctx.decode_request_body(&request).await?;
                    let changed_path = payload.path.clone();
                    let backend_data_dir = Arc::clone(&data_dir);
                    let publish_data_dir = Arc::clone(&data_dir);
                    let backend_runtime = runtime.clone();
                    let traceparent = request_traceparent.clone();
                    let result = run_blocking_browser_task(
                        Arc::clone(&data_dir),
                        route.clone(),
                        "DFS browser delete",
                        move || {
                            crate::with_dfs_traceparent(traceparent.as_deref(), || {
                                let backend = DfsBrowserBackend::for_data_dir(
                                    backend_data_dir,
                                    &backend_runtime,
                                    Arc::clone(&backend_host),
                                )?;
                                let result = backend.delete(payload)?;
                                Ok((result, backend.event_origin_node_id()?))
                            })
                        },
                    )
                    .await?;
                    effect_host.schedule_metadata_publish(
                        publish_data_dir.as_ref(),
                        &runtime,
                        request_traceparent.as_deref(),
                    );
                    effect_host.publish_namespace_change(
                        result.1.clone(),
                        "delete",
                        vec![changed_path],
                    );
                    ctx.respond_value(DAEMON_DFS_BROWSER_DELETE_ROUTE, result.0)?;
                }
                DAEMON_DFS_BROWSER_PIN_METADATA_ROUTE => {
                    let payload: crate::DaemonDfsBrowserPinMetadataRequest =
                        ctx.decode_request_body(&request).await?;
                    let changed_path = payload.path.clone();
                    let result = run_blocking_browser_task(
                        Arc::clone(&data_dir),
                        route.clone(),
                        "DFS browser pin metadata",
                        move || {
                            let backend = DfsBrowserBackend::for_data_dir(
                                data_dir,
                                &runtime,
                                Arc::clone(&backend_host),
                            )?;
                            let result = backend.pin_metadata(payload)?;
                            Ok((result, backend.event_origin_node_id()?))
                        },
                    )
                    .await?;
                    effect_host.publish_placement_change(
                        result.1.clone(),
                        "pin-metadata",
                        vec![changed_path],
                    );
                    ctx.respond_value(DAEMON_DFS_BROWSER_PIN_METADATA_ROUTE, result.0)?;
                }
                DAEMON_DFS_BROWSER_UNPIN_METADATA_ROUTE => {
                    let payload: crate::DaemonDfsBrowserUnpinMetadataRequest =
                        ctx.decode_request_body(&request).await?;
                    let changed_path = payload.path.clone();
                    let result = run_blocking_browser_task(
                        Arc::clone(&data_dir),
                        route.clone(),
                        "DFS browser unpin metadata",
                        move || {
                            let backend = DfsBrowserBackend::for_data_dir(
                                data_dir,
                                &runtime,
                                Arc::clone(&backend_host),
                            )?;
                            let result = backend.unpin_metadata(payload)?;
                            Ok((result, backend.event_origin_node_id()?))
                        },
                    )
                    .await?;
                    if result.0.removed {
                        effect_host.publish_placement_change(
                            result.1.clone(),
                            "unpin-metadata",
                            vec![changed_path],
                        );
                    }
                    ctx.respond_value(DAEMON_DFS_BROWSER_UNPIN_METADATA_ROUTE, result.0)?;
                }
                DAEMON_DFS_BROWSER_PIN_CONTENT_ROUTE => {
                    let payload: crate::DaemonDfsBrowserPinContentRequest =
                        ctx.decode_request_body(&request).await?;
                    let changed_path = payload.path.clone();
                    let result = run_blocking_browser_task(
                        Arc::clone(&data_dir),
                        route.clone(),
                        "DFS browser pin content",
                        move || {
                            let backend = DfsBrowserBackend::for_data_dir(
                                data_dir,
                                &runtime,
                                Arc::clone(&backend_host),
                            )?;
                            let result = backend.pin_content(payload)?;
                            Ok((result, backend.event_origin_node_id()?))
                        },
                    )
                    .await?;
                    effect_host.publish_placement_change(
                        result.1.clone(),
                        "pin-content",
                        vec![changed_path],
                    );
                    ctx.respond_value(DAEMON_DFS_BROWSER_PIN_CONTENT_ROUTE, result.0)?;
                }
                DAEMON_DFS_BROWSER_UNPIN_CONTENT_ROUTE => {
                    let payload: crate::DaemonDfsBrowserUnpinContentRequest =
                        ctx.decode_request_body(&request).await?;
                    let changed_path = payload.path.clone();
                    let result = run_blocking_browser_task(
                        Arc::clone(&data_dir),
                        route.clone(),
                        "DFS browser unpin content",
                        move || {
                            let backend = DfsBrowserBackend::for_data_dir(
                                data_dir,
                                &runtime,
                                Arc::clone(&backend_host),
                            )?;
                            let result = backend.unpin_content(payload)?;
                            Ok((result, backend.event_origin_node_id()?))
                        },
                    )
                    .await?;
                    if result.0.removed {
                        effect_host.publish_placement_change(
                            result.1.clone(),
                            "unpin-content",
                            vec![changed_path],
                        );
                    }
                    ctx.respond_value(DAEMON_DFS_BROWSER_UNPIN_CONTENT_ROUTE, result.0)?;
                }
                DAEMON_DFS_BROWSER_TRANSFER_METADATA_PRIMARY_ROUTE => {
                    let payload: crate::DaemonDfsBrowserTransferMetadataPrimaryRequest =
                        ctx.decode_request_body(&request).await?;
                    let changed_path = payload.path.clone();
                    let result = run_blocking_browser_task(
                        Arc::clone(&data_dir),
                        route.clone(),
                        "DFS browser metadata primary transfer",
                        move || {
                            let backend = DfsBrowserBackend::for_data_dir(
                                data_dir,
                                &runtime,
                                Arc::clone(&backend_host),
                            )?;
                            let result = backend.transfer_metadata_primary(payload, &runtime)?;
                            Ok((result, backend.event_origin_node_id()?))
                        },
                    )
                    .await?;
                    if result.0.changed {
                        effect_host.publish_placement_change(
                            result.1.clone(),
                            "transfer-metadata-primary",
                            vec![changed_path],
                        );
                    }
                    ctx.respond_value(
                        DAEMON_DFS_BROWSER_TRANSFER_METADATA_PRIMARY_ROUTE,
                        result.0,
                    )?;
                }
                DAEMON_DFS_BROWSER_PLACEMENT_PREVIEW_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result = run_blocking_browser_task(
                        Arc::clone(&data_dir),
                        route.clone(),
                        "DFS browser placement preview",
                        move || {
                            DfsBrowserBackend::for_data_dir(
                                data_dir,
                                &runtime,
                                Arc::clone(&backend_host),
                            )?
                            .placement_preview(payload, &runtime)
                        },
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_PLACEMENT_PREVIEW_ROUTE, result)?;
                }

                DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE => {
                    let received_at = tokio::time::Instant::now();
                    let payload: crate::DaemonDfsBrowserPlacementUpdateV2Request =
                        ctx.decode_request_body(&request).await?;
                    let budget = payload
                        .timeout_millis
                        .map(Duration::from_millis)
                        .unwrap_or(DAEMON_DFS_BROWSER_ROUTE_TIMEOUT)
                        .min(DAEMON_DFS_BROWSER_ROUTE_TIMEOUT);
                    let deadline = received_at + budget;
                    let changed_path = payload.path.clone();
                    let result = run_blocking_browser_task(
                        Arc::clone(&data_dir),
                        route.clone(),
                        "DFS browser placement update v2",
                        move || {
                            if tokio::time::Instant::now() >= deadline {
                                return Err(dfs_browser_service_error(
                                    "Apply deadline elapsed before the placement mutation",
                                ));
                            }
                            let backend = DfsBrowserBackend::for_data_dir(
                                data_dir,
                                &runtime,
                                Arc::clone(&backend_host),
                            )?;
                            let result = backend.placement_update_v2(payload, &runtime)?;
                            Ok((result, backend))
                        },
                    )
                    .await?;
                    // The transaction closure has finished exactly once and
                    // released FIFO/service/writer custody before this wait.
                    let response =
                        finish_placement_configuration_visibility(&result.1, result.0, deadline)
                            .await;
                    effect_host.publish_placement_change(
                        result.1.node_id.clone(),
                        "update-policy",
                        vec![changed_path],
                    );
                    ctx.respond_value(DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE, response)?;
                }
                DAEMON_DFS_BROWSER_PLACEMENT_PROGRESS_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result = run_blocking_browser_task_unqueued(
                        Arc::clone(&data_dir),
                        route.clone(),
                        "DFS browser placement progress",
                        move || {
                            DfsBrowserBackend::for_data_dir(
                                data_dir,
                                &runtime,
                                Arc::clone(&backend_host),
                            )?
                            .placement_progress(payload)
                        },
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_PLACEMENT_PROGRESS_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_PLACEMENT_ACTIVITY_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result = run_blocking_browser_task_unqueued(
                        Arc::clone(&data_dir),
                        route.clone(),
                        "DFS browser placement activity",
                        move || {
                            DfsBrowserBackend::for_data_dir(
                                data_dir,
                                &runtime,
                                Arc::clone(&backend_host),
                            )?
                            .placement_activity(payload)
                        },
                    )
                    .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_PLACEMENT_ACTIVITY_ROUTE, result)?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub fn daemon_dfs_browser_route_is_path_access(route: &str) -> bool {
    matches!(
        route,
        DAEMON_DFS_BROWSER_STATUS_ROUTE
            | DAEMON_DFS_BROWSER_STAT_ROUTE
            | DAEMON_DFS_BROWSER_NAMESPACE_LSTAT_ROUTE
            | DAEMON_DFS_BROWSER_NO_LINKS_ROUTE
            | DAEMON_DFS_BROWSER_LIST_ROUTE
            | DAEMON_DFS_BROWSER_READ_FILE_ROUTE
            | DAEMON_DFS_BROWSER_READ_OBJECT_VERSION_ROUTE
            | DAEMON_DFS_BROWSER_READ_OBJECT_RANGE_ROUTE
            | DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE
            | DAEMON_DFS_BROWSER_READ_FILES_ROUTE
            | DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE
            | DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE
            | DAEMON_DFS_BROWSER_DURABLE_REPLACE_ROUTE
            | DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE
            | DAEMON_DFS_BROWSER_RENAME_ROUTE
            | DAEMON_DFS_BROWSER_DELETE_ROUTE
    )
}

pub struct HostedDfsPlacementAuthorityAdminService {
    runtime: Option<tokio::runtime::Handle>,
    host: Arc<dyn DfsBrowserServiceHost>,
}

impl HostedDfsPlacementAuthorityAdminService {
    pub fn new(
        runtime: Option<tokio::runtime::Handle>,
        host: Arc<dyn DfsBrowserServiceHost>,
    ) -> Self {
        Self { runtime, host }
    }
}

pub fn require_local_dfs_placement_authority_break_glass_signer(
    signer: &ServiceSigner,
) -> Result<(), ServiceError> {
    signer.is_local().then_some(()).ok_or_else(|| {
        ServiceError::Service(
            "DFS placement-authority break-glass recovery requires a local signer".to_string(),
        )
    })
}

impl Service for HostedDfsPlacementAuthorityAdminService {
    fn name(&self) -> &'static str {
        "dfs-placement-authority-admin"
    }

    fn routes(&self) -> &'static [&'static str] {
        &[DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE]
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        vec![
            ServiceRoute::new(
                DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE,
                DAEMON_RPC_CATEGORY,
            )
            .with_auth_scope("admin")
            .require_capability("dfs.placement-authority.admin")
            .with_concurrency_limit(1)
            .with_timeout(Duration::from_secs(30)),
        ]
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let runtime = self.runtime.clone();
        let host = Arc::clone(&self.host);
        Box::pin(async move {
            let runtime =
                require_service_runtime(runtime.as_ref(), "DFS placement authority admin")?.clone();
            let payload: crate::DaemonDfsPlacementAuthorityAdminRequest =
                ctx.decode_request_body(&request).await?;
            let response = match payload {
                crate::DaemonDfsPlacementAuthorityAdminRequest::Inspect { file_system_id } => {
                    let permit = run_blocking_service_task(
                        DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE,
                        "DFS placement authority inspection",
                        move || host.inspect_placement_authority(&runtime, &file_system_id),
                    )
                    .await?;
                    crate::DaemonDfsPlacementAuthorityAdminResponse::Inspection { permit }
                }
                crate::DaemonDfsPlacementAuthorityAdminRequest::RecoverRemovedNode {
                    file_system_id,
                    expected_recovery_token,
                    acknowledge_owner_stopped,
                } => {
                    let outcome = run_blocking_service_task(
                        DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE,
                        "DFS placement authority removed-node recovery",
                        move || {
                            host.recover_removed_placement_authority(
                                &runtime,
                                &file_system_id,
                                &expected_recovery_token,
                                acknowledge_owner_stopped,
                            )
                        },
                    )
                    .await?;
                    crate::DaemonDfsPlacementAuthorityAdminResponse::Recovery { outcome }
                }
                crate::DaemonDfsPlacementAuthorityAdminRequest::BreakGlassRecover {
                    file_system_id,
                    expected_recovery_token,
                    acknowledgement,
                } => {
                    require_local_dfs_placement_authority_break_glass_signer(&ctx.signer)?;
                    let outcome = run_blocking_service_task(
                        DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE,
                        "DFS placement authority break-glass recovery",
                        move || {
                            host.break_glass_recover_placement_authority(
                                &runtime,
                                &file_system_id,
                                &expected_recovery_token,
                                &acknowledgement,
                            )
                        },
                    )
                    .await?;
                    crate::DaemonDfsPlacementAuthorityAdminResponse::Recovery { outcome }
                }
            };
            ctx.respond_value(DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE, response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

/// Registers the DFS-owned browser and placement-authority services while the
/// process host supplies runtime, transport, event, and administrative effects.
pub fn register_dfs_browser_services(
    builder: ServiceRegistryBuilder,
    runtime: Option<tokio::runtime::Handle>,
    host: Arc<dyn DfsBrowserServiceHost>,
) -> ServiceRegistryBuilder {
    builder
        .register(HostedDfsBrowserService::new(
            runtime.clone(),
            Arc::clone(&host),
        ))
        .register(HostedDfsPlacementAuthorityAdminService::new(runtime, host))
}

fn daemon_dfs_browser_stat_response(
    stat: crate::DfsStat,
    local_pins: Vec<crate::DaemonDfsBrowserPinRecord>,
    health: Option<crate::DaemonDfsBrowserObjectHealth>,
    metadata_shard: Option<crate::DaemonDfsBrowserMetadataShard>,
    placement: Option<crate::DfsResolvedPlacementPolicy>,
) -> crate::DaemonDfsBrowserStatResponse {
    crate::DaemonDfsBrowserStatResponse {
        file_system_id: stat.file_system_id.to_string(),
        object_id: stat.object_id.to_string(),
        path: stat.path,
        kind: stat.kind,
        version: stat.version.to_string(),
        content_hash: stat.content_hash,
        size_bytes: stat.size_bytes,
        link_count: stat.link_count,
        mode: stat.mode,
        uid: stat.uid,
        gid: stat.gid,
        accessed_at_unix_nanos: stat.accessed_at_unix_nanos,
        modified_at_unix_nanos: stat.modified_at_unix_nanos,
        created_at_unix_nanos: stat.created_at_unix_nanos,
        changed_at_unix_nanos: stat.changed_at_unix_nanos,
        policy: stat.policy,
        current_locations: stat
            .current_locations
            .into_iter()
            .map(|node_id| node_id.to_string())
            .collect(),
        local_pins,
        health,
        metadata_shard,
        placement,
    }
}

fn sort_daemon_dfs_browser_entries(
    entries: &mut [crate::DaemonDfsBrowserEntry],
    sort_key: &crate::DaemonDfsBrowserSortKey,
    sort_direction: &crate::DaemonDfsBrowserSortDirection,
) {
    entries.sort_by(|left, right| {
        let directory_order = daemon_dfs_browser_kind_rank(&left.kind)
            .cmp(&daemon_dfs_browser_kind_rank(&right.kind));
        if directory_order != std::cmp::Ordering::Equal {
            return directory_order;
        }
        let ordering = match sort_key {
            crate::DaemonDfsBrowserSortKey::Name => compare_daemon_dfs_browser_names(left, right),
            crate::DaemonDfsBrowserSortKey::Kind => daemon_dfs_browser_kind_rank(&left.kind)
                .cmp(&daemon_dfs_browser_kind_rank(&right.kind))
                .then_with(|| compare_daemon_dfs_browser_names(left, right)),
            crate::DaemonDfsBrowserSortKey::Size => left
                .size
                .unwrap_or(0)
                .cmp(&right.size.unwrap_or(0))
                .then_with(|| compare_daemon_dfs_browser_names(left, right)),
        };
        match sort_direction {
            crate::DaemonDfsBrowserSortDirection::Ascending => ordering,
            crate::DaemonDfsBrowserSortDirection::Descending => ordering.reverse(),
        }
    });
}

fn daemon_dfs_browser_kind_rank(kind: &crate::DfsObjectKind) -> u8 {
    match kind {
        crate::DfsObjectKind::Directory | crate::DfsObjectKind::Mount => 0,
        crate::DfsObjectKind::Symlink | crate::DfsObjectKind::Hardlink => 1,
        crate::DfsObjectKind::File | crate::DfsObjectKind::Artifact => 2,
        crate::DfsObjectKind::Tombstone => 3,
    }
}

fn compare_daemon_dfs_browser_names(
    left: &crate::DaemonDfsBrowserEntry,
    right: &crate::DaemonDfsBrowserEntry,
) -> std::cmp::Ordering {
    left.name
        .to_ascii_lowercase()
        .cmp(&right.name.to_ascii_lowercase())
        .then_with(|| left.name.cmp(&right.name))
}

fn daemon_dfs_browser_metadata_shard(
    shard: crate::DfsMetadataShardConfig,
    local_node_id: &sinomo_core::node::NodeId,
) -> crate::DaemonDfsBrowserMetadataShard {
    let primary_node_id = shard.primary_node_id.to_string();
    let local_node_id = local_node_id.to_string();
    let replica_node_ids = shard
        .replica_node_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let local_is_primary = primary_node_id == local_node_id;
    let local_is_replica = replica_node_ids
        .iter()
        .any(|node_id| node_id == &local_node_id);
    crate::DaemonDfsBrowserMetadataShard {
        shard_id: shard.shard_id,
        primary_node_id,
        replica_node_ids,
        epoch: shard.epoch,
        local_node_id,
        local_is_primary,
        local_is_replica,
    }
}

fn daemon_dfs_browser_health_summary(
    status: crate::DfsOperatorStatus,
) -> crate::DaemonDfsBrowserHealthSummary {
    crate::DaemonDfsBrowserHealthSummary {
        file_system_count: status.file_system_count,
        object_count: status.object_count,
        version_count: status.version_count,
        pending_write_count: status.pending_write_count,

        pending_repair_count: status.pending_repair_count,
        pending_namespace_job_count: status.pending_namespace_job_count,
        failed_namespace_job_count: status.failed_namespace_job_count,
        metadata_shard_count: status.metadata_shards.len(),
        repair_attempt_count: status
            .repair_attempt_count
            .max(status.repair_attempts.len()),
    }
}

fn daemon_dfs_browser_local_pins(
    data_dir: &SinomoDataDir,
    file_system_id: &crate::DfsFileSystemId,
    path: &str,
) -> Vec<crate::DaemonDfsBrowserPinRecord> {
    let normalized_path = normalize_sifs_browser_path(path).unwrap_or_else(|_| path.to_string());
    let Ok(pins) =
        crate::list_local_dfs_pins(crate::default_dfs_pin_root(data_dir), Some(file_system_id))
    else {
        return Vec::new();
    };
    pins.into_iter()
        .filter(|pin| {
            normalize_sifs_browser_path(&pin.path).unwrap_or_else(|_| pin.path.clone())
                == normalized_path
        })
        .map(|pin| crate::DaemonDfsBrowserPinRecord {
            pinned_by_node_id: pin.pinned_by_node_id.to_string(),
            mode: pin.mode,
            created_at_unix_nanos: u64::try_from(pin.created_at.unix_timestamp_nanos()).ok(),
        })
        .collect()
}

fn daemon_dfs_browser_object_health(
    browser: &DfsBrowserBackend,
    service: &crate::MountedSifsService,
    object_id: &crate::DfsObjectId,
) -> Option<crate::DaemonDfsBrowserObjectHealth> {
    let view = if let Some(backend) = service.metadata_backend() {
        crate::DfsOperatorInspector::new(backend)
            .object(object_id)
            .ok()
            .flatten()?
    } else {
        let owner = browser.host.data_plane_host()?;
        let runtime = browser.host.browser_open_runtime()?;
        match runtime
            .block_on(owner.inspect(
                Arc::new(browser.data_dir.clone()),
                crate::DaemonDfsInspectionRequest::Object {
                    object_id: object_id.clone(),
                },
            ))
            .ok()?
        {
            crate::DaemonDfsInspectionResponse::Object { object } => object?,
            _ => return None,
        }
    };
    let missing_replica_nodes = view
        .repairs
        .iter()
        .map(|repair| repair.missing_replica_node_id.to_string())
        .collect();
    Some(crate::DaemonDfsBrowserObjectHealth {
        agent_state: view.agent_state,
        version_count: view.versions.len(),
        receipt_count: view.receipts.len(),
        intent_count: view.intents.len(),

        pending_repair_count: view.repairs.len(),
        missing_replica_nodes,
    })
}

fn daemon_dfs_browser_entry_placement(
    browser: &DfsBrowserBackend,
    service: &mut crate::MountedSifsService,
    path: &str,
    metadata: &crate::DaemonDfsBrowserEntryMetadata,
) -> Option<crate::DaemonDfsBrowserEntryPlacement> {
    let object_id = crate::DfsObjectId::new(&metadata.object_id).ok()?;
    let current_replicas = metadata.current_locations.len();
    let desired_replicas = service
        .resolved_placement_policy_for_path(path, metadata.policy.clone(), current_replicas.max(1))
        .map(|placement| placement.policy.desired_replicas)
        .unwrap_or_else(|_| current_replicas.max(1))
        .max(1);
    let agent_state = daemon_dfs_browser_object_health(browser, service, &object_id)
        .map(|health| health.agent_state)
        .unwrap_or_else(|| {
            if current_replicas == 0 {
                crate::DfsAgentObjectState::MetadataOnly
            } else if current_replicas < desired_replicas {
                crate::DfsAgentObjectState::PendingRepair
            } else {
                crate::DfsAgentObjectState::Current
            }
        });
    Some(crate::DaemonDfsBrowserEntryPlacement {
        agent_state,
        desired_replicas,
    })
}

fn sifs_browser_write_options<T: DfsMessageTransport>(
    service: &mut NetworkedDfsService<T>,
    path: &str,
    replica_candidates: &[sinomo_core::node::NodeId],
) -> Result<crate::DfsWriteOptions, ServiceError> {
    sifs_browser_write_options_with_configuration(service, path, replica_candidates, None)
}

#[tracing::instrument(
    target = "sinomo.perf",
    level = "trace",
    name = "dfs.browser.parent",
    skip_all,
    fields(stage = "dfs.browser.parent")
)]
pub(crate) fn prepare_sifs_browser_write_parent<T: DfsMessageTransport>(
    service: &mut NetworkedDfsService<T>,
    path: &str,
) -> Result<(), DfsPosixError> {
    let Some(parent) = parent_sifs_browser_path(path) else {
        return Ok(());
    };
    // This is only a hint that recursive creation is unnecessary. The write
    // still validates its parent, and the metadata primary checks the current
    // parent again before admitting and atomically publishing a new child.
    // A cached directory must never replace those authority-side predicates.
    // Avoid routing a no-op MkdirAll (and replaying its journal response) for
    // every write to an already observed directory.
    // Backend catchup can supersede the old row between hint insertion and
    // this call. That must not turn a write beneath a known parent into an
    // implicit directory recreation. An exact positive hint only skips work;
    // it cannot authorize the subsequent write or claim the parent is current.
    if service
        .metadata()
        .object_by_normalized_posix_path(&crate::sifs_file_system_id(), &parent)
        .is_some_and(|object| object.kind == DfsObjectKind::Directory)
        || service
            .posix_object_metadata_hint(&parent)
            .is_ok_and(|object| object.kind == DfsObjectKind::Directory)
    {
        return Ok(());
    }
    // A caller delegating to its Main has no local view and retains no row
    // for a parent it merely wrote beneath. Its parent's authority answers
    // the same question with a read; only a parent that authority does not
    // show as a directory needs the routed recursive creation.
    if service.posix_directory_observed_at_authority(&parent) {
        return Ok(());
    }
    service.posix_mkdir_all(&parent)
}

#[tracing::instrument(
    target = "sinomo.perf",
    level = "trace",
    name = "dfs.browser.policy",
    skip_all,
    fields(
        stage = "dfs.browser.policy",
        outcome = "unknown",
        failure_phase = tracing::field::Empty,
        error = tracing::field::Empty
    )
)]
fn sifs_browser_write_options_with_configuration<T: DfsMessageTransport>(
    service: &mut NetworkedDfsService<T>,
    path: &str,
    replica_candidates: &[sinomo_core::node::NodeId],
    configuration: Option<&crate::DfsWriteConfigurationProjection>,
) -> Result<crate::DfsWriteOptions, ServiceError> {
    let trace_started = crate::dfs_perf_trace_enabled().then(Instant::now);
    // An existing object's policy comes from its authority; a node that does
    // not replicate the path must not mistake its absence locally for a new
    // eventual object.
    let object_policy = service
        .posix_existing_object_policy(path)
        .map_err(dfs_browser_service_error)?
        .unwrap_or(sinomo_core::DfsObjectPolicy::ReplicatedEventual);
    let object_policy_us = trace_started.map_or(0, |start| start.elapsed().as_micros());
    let policy_started = trace_started.map(|_| Instant::now());
    let policy = match configuration {
        Some(configuration) => service.managed_write_replication_policy_from_configuration(
            path,
            object_policy,
            replica_candidates.len(),
            Some(configuration),
        ),
        None => service.managed_write_replication_policy_for_path(
            path,
            object_policy,
            replica_candidates.len(),
        ),
    }
    .map_err(|error| {
        let span = tracing::Span::current();
        span.record("outcome", "error");
        span.record("failure_phase", "managed_replication_policy");
        span.record("error", tracing::field::display(&error));
        dfs_browser_service_error(error)
    })?;
    if let Some(started) = trace_started {
        crate::dfs_perf_trace(format_args!(
            "dfs-browser-write-options node={} path={} object_policy_us={} managed_policy_us={} total_us={}",
            service.node_id(),
            path,
            object_policy_us,
            policy_started.map_or(0, |start| start.elapsed().as_micros()),
            started.elapsed().as_micros(),
        ));
    }
    let candidates = sifs_browser_write_candidates(replica_candidates, &policy);
    let options = crate::DfsWriteOptions {
        policy,
        replica_candidates: candidates,
        wait_for_commit: true,
        foreground_replica_repair: false,
    };
    tracing::Span::current().record("outcome", "ok");
    Ok(options)
}

pub fn sifs_browser_write_candidates(
    replica_candidates: &[sinomo_core::node::NodeId],
    policy: &crate::DfsReplicationPolicy,
) -> Vec<sinomo_core::node::NodeId> {
    let mut candidates = replica_candidates.to_vec();
    for required in &policy.required_node_ids {
        if !candidates.contains(required) {
            candidates.push(required.clone());
        }
    }
    candidates
}

fn dfs_browser_service_error(error: impl std::fmt::Display) -> ServiceError {
    ServiceError::Service(error.to_string())
}

pub fn mutate_daemon_dfs_browser_placement_config(
    config: &mut crate::DfsFileSystemConfig,
    path: &str,
    scope: crate::DfsPlacementScope,
    policy: Option<crate::DfsPlacementPolicy>,
) -> Result<(), ServiceError> {
    match policy {
        Some(policy) => {
            let rule = crate::normalize_placement_rule(path, scope, policy)
                .map_err(dfs_browser_service_error)?;
            crate::upsert_placement_rule(&mut config.subtree_policies, rule);
        }
        None => {
            crate::remove_placement_rule(&mut config.subtree_policies, path, scope)
                .map_err(dfs_browser_service_error)?;
        }
    }
    Ok(())
}

#[derive(Default)]
pub struct DaemonDfsBrowserPlacementConnectivitySnapshot {
    pub local_node_id: Option<sinomo_core::node::NodeId>,
    pub graph: Option<ClusterConnectivityGraph>,
    pub advertised_endpoints: Vec<sinomo_core::ClusterEndpointRecord>,
}

impl DaemonDfsBrowserPlacementConnectivitySnapshot {
    fn reachability(
        &self,
        node_id: &sinomo_core::node::NodeId,
    ) -> crate::DfsPlacementNodeReachability {
        let Some(local_node_id) = self.local_node_id.as_ref() else {
            return crate::DfsPlacementNodeReachability::Unknown;
        };
        if node_id == local_node_id {
            return crate::DfsPlacementNodeReachability::Local;
        }
        let Some(graph) = self.graph.as_ref() else {
            return crate::DfsPlacementNodeReachability::Unknown;
        };
        daemon_dfs_browser_placement_node_reachability(
            local_node_id,
            node_id,
            graph,
            &self.advertised_endpoints,
        )
    }

    pub fn explicitly_unavailable_node_ids(&self) -> BTreeSet<sinomo_core::node::NodeId> {
        self.advertised_endpoints
            .iter()
            .map(|endpoint| endpoint.node_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|node_id| {
                self.reachability(node_id) == crate::DfsPlacementNodeReachability::Unreachable
            })
            .collect()
    }
}

fn daemon_dfs_browser_placement_connectivity_snapshot(
    cluster_paths: &ClusterPaths,
) -> DaemonDfsBrowserPlacementConnectivitySnapshot {
    let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths) else {
        return DaemonDfsBrowserPlacementConnectivitySnapshot::default();
    };
    let mut advertised_endpoints = cluster_directory_for(cluster_paths)
        .endpoints()
        .unwrap_or_default();
    if advertised_endpoints.is_empty() {
        advertised_endpoints.extend(
            state
                .members
                .iter()
                .flat_map(|member| member.endpoints.iter().cloned()),
        );
    }
    let graph = ClusterConnectivityGraph::try_read_for_local(cluster_paths, &state).ok();
    DaemonDfsBrowserPlacementConnectivitySnapshot {
        local_node_id: Some(state.local_node_id),
        graph,
        advertised_endpoints,
    }
}

pub fn enrich_daemon_dfs_browser_placement_connectivity_from_snapshot(
    preview: &mut crate::DfsPlacementPreview,
    connectivity: &DaemonDfsBrowserPlacementConnectivitySnapshot,
) {
    const DESIRED_BECAME_UNREACHABLE: &str =
        "blocked: this desired replica became explicitly unreachable before apply";

    let mut new_diagnostics = Vec::new();
    for node in &mut preview.nodes {
        node.reachability = connectivity.reachability(&node.node_id);
        if node.reachability != crate::DfsPlacementNodeReachability::Unreachable {
            continue;
        }
        if node.required {
            new_diagnostics.push(
                crate::DfsPlacementBlockingDiagnostic::RequiredNodeUnavailable {
                    node_id: node.node_id.clone(),
                },
            );
        }
        if node.desired_replica {
            new_diagnostics.push(
                crate::DfsPlacementBlockingDiagnostic::DesiredNodeUnavailable {
                    node_id: node.node_id.clone(),
                },
            );
            if !node.explanation.contains(DESIRED_BECAME_UNREACHABLE) {
                if !node.explanation.is_empty() {
                    node.explanation.push_str("; ");
                }
                node.explanation.push_str(DESIRED_BECAME_UNREACHABLE);
            }
        }
    }
    extend_unique_daemon_dfs_browser_placement_diagnostics(
        &mut preview.blocking_diagnostics,
        new_diagnostics,
    );
}

#[cfg(feature = "test-support")]
pub fn merge_daemon_dfs_browser_placement_impact(
    aggregate: &mut crate::DfsPlacementImpact,
    page: &crate::DfsPlacementImpact,
) {
    crate::merge_placement_impact(aggregate, page);
}

fn extend_unique_daemon_dfs_browser_placement_diagnostics(
    diagnostics: &mut Vec<crate::DfsPlacementBlockingDiagnostic>,
    additional: impl IntoIterator<Item = crate::DfsPlacementBlockingDiagnostic>,
) {
    for diagnostic in additional {
        if !diagnostics.contains(&diagnostic) {
            diagnostics.push(diagnostic);
        }
    }
}

pub fn extend_daemon_dfs_browser_preview_diagnostics(
    preview: &mut crate::DfsPlacementPreview,
    additional: impl IntoIterator<Item = crate::DfsPlacementBlockingDiagnostic>,
) {
    let anchor_source = preview
        .resolved
        .source_path
        .clone()
        .zip(preview.resolved.source_scope)
        .unwrap_or_else(|| {
            (
                preview.resolved.path.clone(),
                crate::DfsPlacementScope::SelfOnly,
            )
        });
    for diagnostic in additional {
        let duplicates_anchor = matches!(
            &diagnostic,
            crate::DfsPlacementBlockingDiagnostic::ScopedPolicy {
                path,
                scope,
                diagnostic,
            } if path == &anchor_source.0
                && scope == &anchor_source.1
                && preview
                    .blocking_diagnostics
                    .iter()
                    .any(|existing| existing == diagnostic.as_ref())
        );
        if !duplicates_anchor && !preview.blocking_diagnostics.contains(&diagnostic) {
            preview.blocking_diagnostics.push(diagnostic);
        }
    }
}

fn daemon_dfs_browser_placement_node_reachability(
    local_node_id: &sinomo_core::node::NodeId,
    node_id: &sinomo_core::node::NodeId,
    graph: &ClusterConnectivityGraph,
    advertised_endpoints: &[sinomo_core::ClusterEndpointRecord],
) -> crate::DfsPlacementNodeReachability {
    use crate::DfsPlacementNodeReachability as Reachability;

    if node_id == local_node_id {
        return Reachability::Local;
    }
    if cluster_route_next_hop(graph, local_node_id, node_id).is_some() {
        return Reachability::Reachable;
    }
    let mut node_endpoints = advertised_endpoints
        .iter()
        .filter(|endpoint| &endpoint.node_id == node_id)
        .peekable();
    if node_endpoints.peek().is_some()
        && node_endpoints.all(|endpoint| {
            matches!(
                graph.state_for_endpoint(endpoint),
                Some(ClusterConnectivityState::Unreachable)
            )
        })
    {
        Reachability::Unreachable
    } else {
        Reachability::Unknown
    }
}

pub fn require_satisfiable_daemon_dfs_browser_placement(
    preview: &crate::DfsPlacementPreview,
) -> Result<(), ServiceError> {
    if preview.is_satisfiable() {
        return Ok(());
    }
    let diagnostics = preview
        .blocking_diagnostics
        .iter()
        .map(crate::DfsPlacementBlockingDiagnostic::message)
        .collect::<Vec<_>>()
        .join("; ");
    Err(ServiceError::Service(format!(
        "cannot apply unsatisfiable SIFS placement policy: {diagnostics}"
    )))
}

fn require_accepted_daemon_dfs_browser_placement_update(
    result: Result<Option<crate::DfsPlacementUpdateCommit>, KvError>,
    file_system_id: &crate::DfsFileSystemId,
) -> Result<crate::DfsPlacementUpdateCommit, ServiceError> {
    let reason = match result {
        Ok(Some(committed)) => return Ok(committed),
        // Configuration, reconciliation queue, activity index and permit
        // compares share one transaction. A failed compare publishes nothing;
        // it does not necessarily mean the policy itself changed.
        Ok(None) => "the placement control-plane snapshot changed",
        Err(KvError::Busy { path })
            if path.starts_with(&format!(
                "{}; active placement metadata mutation permit ",
                crate::placement_authority_permit_key(file_system_id)
            )) =>
        {
            // This exact Busy is raised while reading the mutation permit,
            // before submitting Apply. Do not classify generic transaction or
            // network errors as definite rejections.
            "an active placement metadata mutation excludes Apply"
        }
        Err(error) => return Err(dfs_browser_service_error(error)),
    };
    Err(ServiceError::Service(format!(
        "{} {reason}; refresh the preview and retry",
        crate::DFS_PLACEMENT_APPLY_REJECTED_PREFIX,
    )))
}

pub fn require_matching_daemon_dfs_browser_placement_preview_token(
    expected: &crate::DfsPlacementPreviewToken,
    current: &crate::DfsPlacementPreviewToken,
) -> Result<(), ServiceError> {
    if expected.is_well_formed() && current.is_well_formed() && expected == current {
        return Ok(());
    }
    Err(ServiceError::Service(
        "SIFS placement preview is stale; refresh the preview and review the updated plan before applying"
            .to_string(),
    ))
}

fn daemon_dfs_browser_write_receipt(stat: &crate::DfsStat) -> crate::DaemonDfsBrowserWriteReceipt {
    crate::DaemonDfsBrowserWriteReceipt {
        object_id: stat.object_id.to_string(),
        version: stat.version.to_string(),
        content_hash: stat.content_hash.clone(),
        size_bytes: stat.size_bytes,
    }
}

pub fn daemon_dfs_browser_conditional_write_error(
    error: crate::DfsPosixError,
) -> crate::DaemonDfsBrowserReplaceFileIfVersionOutcome {
    use crate::DaemonDfsBrowserReplaceFileIfVersionOutcome as Outcome;

    match error {
        crate::DfsPosixError::Conflict { path, reason } => Outcome::Conflict {
            detail: format!("write conflicted at {path}: {reason}"),
        },
        crate::DfsPosixError::NotFound { path } => Outcome::NotFound {
            detail: format!("not found: {path}"),
        },
        error if posix_error_outcome_is_unknown(&error) => Outcome::Unknown {
            detail: error.to_string(),
        },
        error => Outcome::Rejected {
            detail: error.to_string(),
        },
    }
}

/// Whether a failed mutation may still have applied, so it must not be
/// reported as rejected: a durable body whose commit is unconfirmed, a write
/// still pending, or a request that was dispatched but not answered.
pub fn posix_error_outcome_is_unknown(error: &crate::DfsPosixError) -> bool {
    matches!(
        error,
        crate::DfsPosixError::PendingWrite { .. }
            | crate::DfsPosixError::Dfs(
                crate::DfsError::MetadataCommitOutcomeUnknown { .. }
                    | crate::DfsError::NetworkDelivery {
                        delivery_state: sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                        ..
                    }
            )
    )
}

pub fn normalize_sifs_browser_path(path: &str) -> Result<String, ServiceError> {
    let path = path.trim();
    let path = if path.is_empty() {
        "/".to_string()
    } else if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    if path.bytes().any(|byte| byte == 0) {
        return Err(ServiceError::Service(format!("invalid DFS path: {path}")));
    }
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            value => parts.push(value),
        }
    }
    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

fn parent_sifs_browser_path(path: &str) -> Option<String> {
    let path = normalize_sifs_browser_path(path).ok()?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conditional_replace_reports_a_possibly_applied_save_as_unknown() {
        use crate::DaemonDfsBrowserReplaceFileIfVersionOutcome as Outcome;
        for error in [
            crate::DfsPosixError::Dfs(crate::DfsError::MetadataCommitOutcomeUnknown {
                file_system_id: crate::sifs_file_system_id(),
                shard_id: "root".into(),
                epoch: 1,
                operation_id: "write:dfs_write_op_1".into(),
                cause: crate::DfsCommitOutcomeUnknownCause::Deadline,
                current_primary_node_id: None,
            }),
            crate::DfsPosixError::PendingWrite {
                intent_id: "dfs_write_op_1".into(),
                durable_receipts: 1,
                required_receipts: 2,
            },
            crate::DfsPosixError::Dfs(crate::DfsError::NetworkDelivery {
                message: "reply lost".into(),
                delivery_state: sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
            }),
        ] {
            assert!(matches!(
                daemon_dfs_browser_conditional_write_error(error),
                Outcome::Unknown { .. }
            ));
        }
        assert!(matches!(
            daemon_dfs_browser_conditional_write_error(crate::DfsPosixError::Dfs(
                crate::DfsError::NetworkDelivery {
                    message: "route unavailable".into(),
                    delivery_state: sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
                }
            )),
            Outcome::Rejected { .. }
        ));
    }

    #[test]
    fn placement_apply_diagnostics_distinguish_rejection_from_unknown_outcomes() {
        let file_system_id = crate::sifs_file_system_id();
        let rejected =
            require_accepted_daemon_dfs_browser_placement_update(Ok(None), &file_system_id)
                .expect_err("failed compare")
                .to_string();
        assert!(rejected.starts_with(crate::DFS_PLACEMENT_APPLY_REJECTED_PREFIX));
        assert!(rejected.contains("control-plane snapshot changed"));
        let live_permit = require_accepted_daemon_dfs_browser_placement_update(
            Err(KvError::Busy {
                path: format!(
                    "{}; active placement metadata mutation permit held by node test",
                    crate::placement_authority_permit_key(&file_system_id)
                ),
            }),
            &file_system_id,
        )
        .expect_err("live permit")
        .to_string();
        assert!(live_permit.starts_with(crate::DFS_PLACEMENT_APPLY_REJECTED_PREFIX));
        for error in [
            KvError::Busy {
                path: "raft storage".into(),
            },
            KvError::Serialization {
                detail: "PossiblyApplied: lost transaction reply".into(),
            },
        ] {
            let unknown =
                require_accepted_daemon_dfs_browser_placement_update(Err(error), &file_system_id)
                    .expect_err("unknown transaction outcome")
                    .to_string();
            assert!(!unknown.contains(crate::DFS_PLACEMENT_APPLY_REJECTED_PREFIX));
        }
    }

    fn admission_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn optional_component_access_cannot_cold_open_a_missing_core_owner() {
        let root = sinomo_testdir::TestDir::new("missing-semantic-filesystem-owner");
        let data_dir = Arc::new(SinomoDataDir::new(root.path()));
        let runtime = admission_runtime();
        let error =
            DfsBrowserBackend::for_component_data_dir(Arc::clone(&data_dir), runtime.handle())
                .err()
                .expect("no active core owner");
        assert!(
            error
                .to_string()
                .contains("DFS component owner is unavailable")
        );
        assert!(
            !data_dir.dfs_dir().exists(),
            "a missing dependency cannot initialize DFS storage"
        );
        release_browser_backends(&data_dir);
    }

    #[test]
    fn browser_admission_is_fifo_and_does_not_park_blocking_workers() {
        let root = sinomo_testdir::TestDir::new("browser-fair-admission");
        let data_dir = Arc::new(SinomoDataDir::new(root.path()));
        admission_runtime().block_on(async {
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let first = tokio::spawn(run_blocking_browser_task(
                data_dir.clone(),
                "test",
                "first",
                move || {
                    let _ = entered_tx.send(());
                    release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok(())
                },
            ));
            entered_rx.await.unwrap();
            let order = Arc::new(Mutex::new(Vec::new()));
            let mut queued = Vec::new();
            for index in 0..3 {
                let (queued_tx, queued_rx) = tokio::sync::oneshot::channel();
                let data_dir = data_dir.clone();
                let order = order.clone();
                queued.push(tokio::spawn(async move {
                    let _ = queued_tx.send(());
                    run_blocking_browser_task(data_dir, "test", "queued", move || {
                        order.lock().unwrap().push(index);
                        Ok(())
                    })
                    .await
                }));
                queued_rx.await.unwrap();
            }
            // Wait past the old 100 ms lock-admission limit. Queued requests
            // remain pending without errors or occupying the other worker.
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(order.lock().unwrap().is_empty());
            tokio::time::timeout(
                Duration::from_millis(500),
                tokio::task::spawn_blocking(|| ()),
            )
            .await
            .expect("unrelated blocking work must remain schedulable")
            .unwrap();
            release_tx.send(()).unwrap();
            first.await.unwrap().unwrap();
            for request in queued {
                request.await.unwrap().unwrap();
            }
            assert_eq!(*order.lock().unwrap(), [0, 1, 2]);
        });
    }

    #[test]
    fn cancelling_browser_call_does_not_release_its_running_operation() {
        let root = sinomo_testdir::TestDir::new("browser-cancelled-admission");
        let data_dir = Arc::new(SinomoDataDir::new(root.path()));
        admission_runtime().block_on(async {
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let first = tokio::spawn(run_blocking_browser_task(
                data_dir.clone(),
                "test",
                "first",
                move || {
                    let _ = entered_tx.send(());
                    release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok(())
                },
            ));
            entered_rx.await.unwrap();
            first.abort();
            assert!(first.await.unwrap_err().is_cancelled());
            let cancelled_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let flag = cancelled_ran.clone();
            let cancelled = tokio::spawn(run_blocking_browser_task(
                data_dir.clone(),
                "test",
                "cancelled",
                move || {
                    flag.store(true, std::sync::atomic::Ordering::Release);
                    Ok(())
                },
            ));
            tokio::task::yield_now().await;
            cancelled.abort();
            assert!(cancelled.await.unwrap_err().is_cancelled());
            let (next_tx, mut next_rx) = tokio::sync::oneshot::channel();
            let next = tokio::spawn(run_blocking_browser_task(
                data_dir,
                "test",
                "next",
                move || {
                    let _ = next_tx.send(());
                    Ok(())
                },
            ));
            assert!(
                tokio::time::timeout(Duration::from_millis(150), &mut next_rx)
                    .await
                    .is_err(),
                "the cancelled caller's blocking operation still owns admission"
            );
            release_tx.send(()).unwrap();
            next_rx.await.unwrap();
            next.await.unwrap().unwrap();
            assert!(!cancelled_ran.load(std::sync::atomic::Ordering::Acquire));
        });
    }

    #[derive(Default)]
    struct UnusedHost {
        namespace_notifications: std::sync::atomic::AtomicUsize,
    }

    #[test]
    fn browser_parent_preparation_creates_missing_chain_and_preserves_non_directory() {
        let root = sinomo_testdir::TestDir::new("browser-write-parent-preparation");
        let (native, node) = crate::test_support::DfsTestRuntime::start_single_node_in_data_dir(
            "browser_parent_preparation_creates_missing_chain_and_preserves_non_directory",
            SinomoDataDir::new(root.path()),
        )
        .expect("normal initialized native browser owner");
        let mut service = native.client(&node).expect("normal native DFS client");
        let path = "/services/browser/new-parent/child.txt";
        prepare_sifs_browser_write_parent(&mut service, path).expect("create missing parents");
        let options = crate::DfsWriteOptions::replicated_eventual(vec![service.node_id().clone()]);
        service
            .posix_write_file(path, b"original", options.clone())
            .expect("write under new parent");
        prepare_sifs_browser_write_parent(&mut service, path).expect("reuse existing parent");
        service
            .posix_write_file(path, b"replacement", options)
            .expect("overwrite under existing parent");
        assert!(matches!(
            prepare_sifs_browser_write_parent(&mut service, &format!("{path}/forbidden.txt")),
            Err(DfsPosixError::NotDirectory { .. })
        ));
        assert_eq!(
            service
                .posix_read_file_distributed(path)
                .expect("preserved file"),
            b"replacement"
        );

        drop(service);
        native.close().expect("normal native browser shutdown");
    }

    impl DfsBrowserServiceHost for UnusedHost {
        fn control_plane_client(
            &self,
            _: &ClusterPaths,
            _: &tokio::runtime::Handle,
        ) -> Option<Box<dyn DfsControlPlaneKvClient>> {
            unreachable!()
        }
        fn peer_transports(
            &self,
            _: &SinomoDataDir,
            _: &tokio::runtime::Handle,
        ) -> (Arc<dyn DfsPeerTransport>, Arc<dyn AsyncDfsPeerTransport>) {
            unreachable!()
        }
        fn schedule_metadata_publish(
            &self,
            _: &SinomoDataDir,
            _: &tokio::runtime::Handle,
            _: Option<&str>,
        ) {
            unreachable!()
        }
        fn notify_maintenance(&self) {
            unreachable!()
        }
        fn publish_namespace_change(&self, _: sinomo_core::Id, _: &str, _: Vec<String>) {
            self.namespace_notifications
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        fn publish_placement_change(&self, _: sinomo_core::Id, _: &str, _: Vec<String>) {
            unreachable!()
        }
        fn inspect_placement_authority(
            &self,
            _: &tokio::runtime::Handle,
            _: &DfsFileSystemId,
        ) -> Result<Option<DfsPlacementAuthorityPermitStatus>, ServiceError> {
            unreachable!()
        }
        fn recover_removed_placement_authority(
            &self,
            _: &tokio::runtime::Handle,
            _: &DfsFileSystemId,
            _: &str,
            _: bool,
        ) -> Result<DfsPlacementAuthorityRecoveryOutcome, ServiceError> {
            unreachable!()
        }
        fn break_glass_recover_placement_authority(
            &self,
            _: &tokio::runtime::Handle,
            _: &DfsFileSystemId,
            _: &str,
            _: &str,
        ) -> Result<DfsPlacementAuthorityRecoveryOutcome, ServiceError> {
            unreachable!()
        }
    }

    #[test]
    fn committed_browser_tracker_publishes_semantic_wake_without_a_response() {
        let root = sinomo_testdir::TestDir::new("browser-response-independent-semantic-wake");
        let data_dir = SinomoDataDir::new(root.path());
        let (native, node) = crate::test_support::DfsTestRuntime::start_single_node_in_data_dir(
            "committed_browser_tracker_publishes_semantic_wake_without_a_response",
            data_dir.clone(),
        )
        .expect("normal initialized native browser owner");
        let service = native.client(&node).expect("normal native DFS client");
        let host = Arc::new(UnusedHost::default());
        let backend = DfsBrowserBackend {
            node_id: service.node_id().clone(),
            data_dir,
            host: host.clone(),
            service: Mutex::new(service),
            replica_control_plane: Mutex::new(None),
            peer_targets_refreshed_at: Mutex::new(None),
            stat_cache: Mutex::new(HashMap::new()),
        };
        {
            let mut mutation = backend.lock_mutation().unwrap();
            mutation.posix_mkdir("/committed-before-response").unwrap();
            // The frontend abandons its outcome. The synchronous owner still
            // publishes its committed semantic wake when this guard drops.
        }
        assert_eq!(
            host.namespace_notifications
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        {
            let mut mutation = backend.lock_mutation().unwrap();
            mutation.record_message_committed_work(crate::DfsCommittedWork {
                publish_metadata: true,
                ..Default::default()
            });
        }
        assert_eq!(
            host.namespace_notifications
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        let outer = crate::DfsCommittedWorkTracker::new();
        backend
            .lock_service()
            .unwrap()
            .set_committed_work_tracker(outer.clone());
        {
            let mut mutation = backend.lock_mutation().unwrap();
            mutation.posix_mkdir("/owned-by-outer-dispatcher").unwrap();
        }
        assert!(outer.take().semantic_changes);
        assert_eq!(
            host.namespace_notifications
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "an outer runtime/handoff tracker owns emission, so the browser does not duplicate it"
        );

        drop(backend);
        native.close().expect("normal native browser shutdown");
    }

    #[test]
    fn browser_identity_remains_available_while_namespace_service_is_busy() {
        let root = sinomo_testdir::TestDir::new("browser-identity-during-mutation");
        let data_dir = SinomoDataDir::new(root.path());
        let (native, node) = crate::test_support::DfsTestRuntime::start_single_node_in_data_dir(
            "browser_identity_remains_available_while_namespace_service_is_busy",
            data_dir.clone(),
        )
        .expect("normal initialized native browser owner");
        let service = native.client(&node).expect("normal native DFS client");
        let node_id = service.node_id().clone();
        let backend = DfsBrowserBackend {
            data_dir,
            host: Arc::new(UnusedHost::default()),
            node_id: node_id.clone(),
            service: Mutex::new(service),
            replica_control_plane: Mutex::new(None),
            peer_targets_refreshed_at: Mutex::new(None),
            stat_cache: Mutex::new(HashMap::new()),
        };
        let _busy = backend.lock_service().expect("hold namespace lock");
        assert_eq!(
            backend.node_id().expect("provider discovery identity"),
            node_id.to_string()
        );
        assert_eq!(
            backend
                .event_origin_node_id()
                .expect("committed mutation provenance"),
            node_id
        );

        drop(_busy);
        drop(backend);
        native.close().expect("normal native browser shutdown");
    }

    #[test]
    fn namespace_lstat_route_preserves_missing_entries_and_dangling_link_metadata() {
        let root = sinomo_testdir::TestDir::new("browser-namespace-lstat-contract");
        let data_dir = SinomoDataDir::new(root.path());
        let (native, node) = crate::test_support::DfsTestRuntime::start_single_node_in_data_dir(
            "namespace_lstat_route_preserves_missing_entries_and_dangling_link_metadata",
            data_dir.clone(),
        )
        .expect("normal initialized native browser owner");
        let mut service = native.client(&node).expect("normal native DFS client");
        service.posix_mkdir_all("/services/namespace-test").unwrap();
        let options = crate::DfsWriteOptions::replicated_eventual(vec![service.node_id().clone()]);
        service
            .posix_symlink(
                "/services/namespace-test/missing",
                "/services/namespace-test/dangling",
                options,
            )
            .unwrap();
        let backend = DfsBrowserBackend {
            node_id: service.node_id().clone(),
            data_dir,
            host: Arc::new(UnusedHost::default()),
            service: Mutex::new(service),
            replica_control_plane: Mutex::new(None),
            peer_targets_refreshed_at: Mutex::new(Some(Instant::now())),
            stat_cache: Mutex::new(HashMap::new()),
        };
        let request = |path: &str| crate::DaemonDfsBrowserStatRequest {
            path: path.into(),
            max_staleness_millis: 0,
        };
        let missing = backend
            .namespace_lstat(request("/services/namespace-test/missing"))
            .unwrap();
        assert!(matches!(
            missing.into_result(),
            Err(crate::DaemonDfsBrowserReadFileFailure::NotFound { .. })
        ));
        assert!(matches!(
            backend
                .stat(request("/services/namespace-test/dangling"))
                .unwrap()
                .into_result(),
            Err(crate::DaemonDfsBrowserReadFileFailure::NotFound { .. })
        ));
        let link = backend
            .namespace_lstat(request("/services/namespace-test/dangling"))
            .unwrap()
            .into_result()
            .unwrap();
        assert_eq!(link.kind, crate::DfsObjectKind::Symlink);
        let directory = backend
            .namespace_lstat(request("/services/namespace-test"))
            .unwrap()
            .into_result()
            .unwrap();
        assert_eq!(directory.kind, crate::DfsObjectKind::Directory);
        assert!(DFS_CORE_BROWSER_ROUTES.contains(&DAEMON_DFS_BROWSER_NAMESPACE_LSTAT_ROUTE));
        assert!(daemon_dfs_browser_route_is_path_access(
            DAEMON_DFS_BROWSER_NAMESPACE_LSTAT_ROUTE
        ));
        use sinomo_module::ComponentFactory as _;
        let hosted = HostedDfsBrowserService::new(None, Arc::new(UnusedHost::default()));
        let route = hosted
            .route_categories()
            .into_iter()
            .find(|route| route.route == DAEMON_DFS_BROWSER_NAMESPACE_LSTAT_ROUTE)
            .unwrap();
        assert_eq!(
            route.auth_scope.as_deref(),
            Some(sinomo_module::ModuleRouteAuth::LocalOrClusterMember.as_scope())
        );
        assert!(
            route
                .required_capabilities
                .contains(&"daemon.dfs.browser".into())
        );
        assert!(
            route
                .required_capabilities
                .contains(&crate::SIFS_PATH_ACCESS_CAPABILITY.into())
        );
        let factory = crate::service::component_factory().unwrap();
        let offered =
            sinomo_module::service_route_with_component_identity(route, factory.descriptor());
        let discovery = offered
            .discovery
            .expect("namespace lstat path-access offer");
        assert_eq!(discovery.service_id, crate::SIFS_PATH_ACCESS_CAPABILITY);
        assert_eq!(discovery.interface_id, "sinomo.sifs.path-access");
        assert!(factory.descriptor().offers.iter().any(|offer| {
            offer.service_id == crate::SIFS_PATH_ACCESS_CAPABILITY
                && offer
                    .routes
                    .iter()
                    .any(|route| route == DAEMON_DFS_BROWSER_NAMESPACE_LSTAT_ROUTE)
        }));

        drop(backend);
        native.close().expect("normal native browser shutdown");
    }

    #[test]
    fn versioned_bulk_read_reuses_core_namespace_outcomes_and_path_access_offer() {
        let root = sinomo_testdir::TestDir::new("browser-bulk-read-core-contract");
        let data_dir = SinomoDataDir::new(root.path());
        let (native, node) = crate::test_support::DfsTestRuntime::start_single_node_in_data_dir(
            "versioned_bulk_read_reuses_core_namespace_outcomes_and_path_access_offer",
            data_dir.clone(),
        )
        .expect("normal initialized native browser owner");
        let mut service = native.client(&node).expect("normal native DFS client");
        service.posix_mkdir_all("/services/bulk-read-test").unwrap();
        let options = crate::DfsWriteOptions::replicated_eventual(vec![service.node_id().clone()]);
        let bytes = (0..65536usize).map(|index| index as u8).collect::<Vec<_>>();
        service
            .posix_write_file("/services/bulk-read-test/data.bin", &bytes, options)
            .unwrap();
        let backend = DfsBrowserBackend {
            node_id: service.node_id().clone(),
            data_dir,
            host: Arc::new(UnusedHost::default()),
            service: Mutex::new(service),
            replica_control_plane: Mutex::new(None),
            peer_targets_refreshed_at: Mutex::new(Some(Instant::now())),
            stat_cache: Mutex::new(HashMap::new()),
        };
        let response = backend
            .read_file(crate::DaemonDfsBrowserReadFileRequest {
                path: "/services/bulk-read-test/data.bin".into(),
            })
            .unwrap();
        let bulk =
            sinomo_core::network::ObjectRpcResponse::encode_body("native-browser-read", &response)
                .unwrap();
        let decoded: crate::DaemonDfsBrowserReadFileResponse = bulk.decode_body().unwrap();
        assert_eq!(decoded.into_result().unwrap(), bytes);
        let absent = backend
            .read_file(crate::DaemonDfsBrowserReadFileRequest {
                path: "/services/bulk-read-test/missing.bin".into(),
            })
            .unwrap();
        assert!(matches!(absent.into_result(),
            Err(crate::DaemonDfsBrowserReadFileFailure::NotFound { path }) if path.ends_with("/missing.bin")));

        // This route belongs to the core browser, independent of History/Attachments components.
        assert!(DFS_CORE_BROWSER_ROUTES.contains(&DAEMON_DFS_BROWSER_READ_FILE_ROUTE));
        assert!(daemon_dfs_browser_route_is_path_access(
            DAEMON_DFS_BROWSER_READ_FILE_ROUTE
        ));
        use sinomo_module::ComponentFactory as _;
        let hosted = HostedDfsBrowserService::new(None, Arc::new(UnusedHost::default()));
        let route = hosted
            .route_categories()
            .into_iter()
            .find(|route| route.route == DAEMON_DFS_BROWSER_READ_FILE_ROUTE)
            .unwrap();
        assert_eq!(
            route.auth_scope.as_deref(),
            Some(sinomo_module::ModuleRouteAuth::LocalOrClusterMember.as_scope())
        );
        assert!(
            route
                .required_capabilities
                .contains(&"daemon.dfs.browser".into())
        );
        assert!(
            route
                .required_capabilities
                .contains(&crate::SIFS_PATH_ACCESS_CAPABILITY.into())
        );
        let factory = crate::service::component_factory().unwrap();
        let offered =
            sinomo_module::service_route_with_component_identity(route, factory.descriptor());
        let discovery = offered.discovery.expect("bulk-read path-access offer");
        assert_eq!(discovery.service_id, crate::SIFS_PATH_ACCESS_CAPABILITY);
        assert_eq!(discovery.interface_id, "sinomo.sifs.path-access");
        assert!(factory.descriptor().offers.iter().any(|offer| {
            offer.service_id == crate::SIFS_PATH_ACCESS_CAPABILITY
                && offer
                    .routes
                    .iter()
                    .any(|route| route == DAEMON_DFS_BROWSER_READ_FILE_ROUTE)
        }));

        drop(backend);
        native.close().expect("normal native browser shutdown");
    }

    #[test]
    fn hosted_browser_identity_bypasses_namespace_admission() {
        struct Sink(Arc<std::sync::atomic::AtomicBool>);
        impl sinomo_core::service::ResponseSink for Sink {
            fn send(
                self: Box<Self>,
                _: sinomo_core::network::ObjectRpcOutgoingResponse,
            ) -> Result<(), ServiceError> {
                self.0.store(true, std::sync::atomic::Ordering::Release);
                Ok(())
            }
        }
        let root = sinomo_testdir::TestDir::new("browser-hosted-identity-admission");
        let data_dir = Arc::new(SinomoDataDir::new(root.path()));
        let (native, node) = crate::test_support::DfsTestRuntime::start_single_node_in_data_dir(
            "hosted_browser_identity_bypasses_namespace_admission",
            (*data_dir).clone(),
        )
        .expect("normal initialized native browser owner");
        let service = native.client(&node).expect("normal native DFS client");
        let backend = Arc::new(DfsBrowserBackend {
            node_id: service.node_id().clone(),
            data_dir: (*data_dir).clone(),
            host: Arc::new(UnusedHost::default()),
            service: Mutex::new(service),
            replica_control_plane: Mutex::new(None),
            peer_targets_refreshed_at: Mutex::new(None),
            stat_cache: Mutex::new(HashMap::new()),
        });
        let slot = browser_backend_slot(&data_dir).unwrap();
        *slot.backend.lock().unwrap() = Some(backend.clone());
        let _busy = backend.lock_service().unwrap();
        admission_runtime().block_on(async {
            let _queued = slot.execution.lock().await;
            let responded = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let ctx = ServiceContext::new(
                data_dir.clone(),
                None,
                ServiceSigner::Local,
                sinomo_core::network::ObjectRpcIncomingBody::empty(),
                Box::new(Sink(responded.clone())),
            );
            let request = ObjectRpcRequest::encode_body(
                DAEMON_DFS_BROWSER_STATUS_ROUTE,
                DAEMON_RPC_CATEGORY,
                &crate::DaemonDfsBrowserStatusRequest::node_only(),
            )
            .unwrap();
            let service = HostedDfsBrowserService::new(
                Some(tokio::runtime::Handle::current()),
                dfs_browser_component_host(&data_dir).unwrap(),
            );
            tokio::time::timeout(Duration::from_secs(1), service.handle(ctx, request))
                .await
                .expect("identity must not wait for namespace admission")
                .unwrap();
            assert!(responded.load(std::sync::atomic::Ordering::Acquire));
        });

        drop(_busy);
        *slot.backend.lock().unwrap() = None;
        drop(slot);
        drop(backend);
        native.close().expect("normal native browser shutdown");
    }

    #[test]
    fn cold_hosted_node_identity_does_not_enter_storage_preparation() {
        struct Sink(tokio::sync::oneshot::Sender<sinomo_core::network::ObjectRpcOutgoingResponse>);
        impl sinomo_core::service::ResponseSink for Sink {
            fn send(
                self: Box<Self>,
                response: sinomo_core::network::ObjectRpcOutgoingResponse,
            ) -> Result<(), ServiceError> {
                self.0
                    .send(response)
                    .map_err(|_| ServiceError::Service("test response dropped".into()))
            }
        }
        let root = sinomo_testdir::TestDir::new("native-cold-browser-identity-pending-storage");
        let data = Arc::new(SinomoDataDir::new(root.path()));
        let (native, node) = crate::test_support::DfsTestRuntime::start_single_node_in_data_dir(
            "native-cold-browser-identity-pending-storage",
            (*data).clone(),
        )
        .unwrap();
        let host = dfs_browser_component_host(&data).unwrap();
        let slot = browser_backend_slot(&data).unwrap();
        assert!(
            slot.backend.lock().unwrap().is_none(),
            "this regression must exercise a cold browser"
        );
        assert_eq!(dfs_browser_backend_open_count(&data), 0);
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        // A negative-only representation barrier. It never supplies readiness,
        // progress, a receipt or authority, and does not alter retained DATA.
        let blocked = crate::register_dfs_storage_preparation_provider(
            data.root(),
            Arc::new(move |_, _, budget| {
                assert_eq!(budget, crate::DFS_STORAGE_PREPARATION_RECORD_BUDGET);
                observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(crate::DfsStoragePreparation::Pending {
                    processed_records: 0,
                })
            }),
            None,
        )
        .unwrap();
        let retired_host = Arc::clone(&host);
        let hosted = HostedDfsBrowserService::new(Some(native.runtime_handle()), host);
        let (outcome, response, health_result) = admission_runtime().block_on(async {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let ctx = ServiceContext::new(
                data.clone(),
                None,
                ServiceSigner::Local,
                sinomo_core::network::ObjectRpcIncomingBody::empty(),
                Box::new(Sink(tx)),
            );
            let request = ObjectRpcRequest::encode_body(
                DAEMON_DFS_BROWSER_STATUS_ROUTE,
                DAEMON_RPC_CATEGORY,
                &crate::DaemonDfsBrowserStatusRequest::node_only(),
            )
            .unwrap();
            let cold_initialization = slot.backend.lock().unwrap();
            assert!(cold_initialization.is_none());
            let outcome =
                tokio::time::timeout(Duration::from_secs(1), hosted.handle(ctx, request)).await;
            drop(cold_initialization);
            let response = if matches!(&outcome, Ok(Ok(_))) {
                let response = rx.await.unwrap();
                let bytes = response.body.collect_to_vec(16 * 1024).await.unwrap();
                let status: crate::DaemonDfsBrowserStatusResponse =
                    sinomo_core::network::ObjectRpcResponse::from_encoded_body(
                        response.response.route,
                        bytes,
                    )
                    .decode_body()
                    .unwrap();
                assert_eq!(
                    dfs_browser_backend_open_count(&data),
                    0,
                    "identity performs no browser storage open; autonomous Main preparation is separate"
                );
                Some(status)
            } else {
                None
            };
            let (tx, _rx) = tokio::sync::oneshot::channel();
            let ctx = ServiceContext::new(
                data.clone(),
                None,
                ServiceSigner::Local,
                sinomo_core::network::ObjectRpcIncomingBody::empty(),
                Box::new(Sink(tx)),
            );
            let request = ObjectRpcRequest::encode_body(
                DAEMON_DFS_BROWSER_STATUS_ROUTE,
                DAEMON_RPC_CATEGORY,
                &crate::DaemonDfsBrowserStatusRequest::with_health(),
            )
            .unwrap();
            let health =
                tokio::time::timeout(Duration::from_secs(1), hosted.handle(ctx, request)).await;
            assert_eq!(dfs_browser_backend_open_count(&data), 1,
                "the actual health request calibrates the same browser-open observer");
            (outcome, response, health)
        });
        drop(blocked);
        drop(hosted);
        drop(slot);
        native.close().unwrap();
        assert!(ClusterPaths::new(&data).state.exists());
        admission_runtime().block_on(async {
            let retired =
                HostedDfsBrowserService::new(Some(tokio::runtime::Handle::current()), retired_host);
            let (tx, _rx) = tokio::sync::oneshot::channel();
            let ctx = ServiceContext::new(
                data.clone(),
                None,
                ServiceSigner::Local,
                sinomo_core::network::ObjectRpcIncomingBody::empty(),
                Box::new(Sink(tx)),
            );
            let request = ObjectRpcRequest::encode_body(
                DAEMON_DFS_BROWSER_STATUS_ROUTE,
                DAEMON_RPC_CATEGORY,
                &crate::DaemonDfsBrowserStatusRequest::node_only(),
            )
            .unwrap();
            let error = tokio::time::timeout(Duration::from_secs(1), retired.handle(ctx, request))
                .await
                .expect("retired identity rejection stays bounded")
                .expect_err("a saved node identity cannot replace the retired component owner");
            assert!(error.to_string().contains("no longer active"), "{error}");
            assert_eq!(
                dfs_browser_backend_open_count(&data),
                1,
                "retired owner rejection must precede a storage open"
            );
        });
        outcome
            .expect("node-only status retains its existing one-second test budget")
            .expect("cold node identity must not open the metadata backend");
        let response = response.unwrap();
        assert_eq!(response.node_id, node.to_string());
        assert!(response.health.is_none() && response.health_error.is_none());
        assert!(
            matches!(health_result, Ok(Err(_))),
            "actual health must remain unavailable behind storage preparation"
        );
        assert!(
            calls.load(std::sync::atomic::Ordering::SeqCst) > 0,
            "health still observes the real negative preparation boundary"
        );
    }

    #[test]
    fn hosted_browser_reads_wait_for_a_slow_mutation_and_return_its_committed_bytes() {
        struct Sink(tokio::sync::oneshot::Sender<sinomo_core::network::ObjectRpcOutgoingResponse>);
        impl sinomo_core::service::ResponseSink for Sink {
            fn send(
                self: Box<Self>,
                response: sinomo_core::network::ObjectRpcOutgoingResponse,
            ) -> Result<(), ServiceError> {
                self.0
                    .send(response)
                    .map_err(|_| ServiceError::Service("test response dropped".into()))
            }
        }
        let root = sinomo_testdir::TestDir::new("browser-concurrent-hosted-reads");
        let data_dir = Arc::new(SinomoDataDir::new(root.path()));
        let (native, node) = crate::test_support::DfsTestRuntime::start_single_node_in_data_dir(
            "hosted_browser_reads_wait_for_a_slow_mutation_and_return_its_committed_bytes",
            (*data_dir).clone(),
        )
        .expect("normal initialized native browser owner");
        let mut service = native.client(&node).expect("normal native DFS client");
        let node = service.node_id().clone();
        let options = crate::DfsWriteOptions::replicated_eventual(vec![node.clone()]);
        service
            .posix_write_file("/admission.txt", b"base", options.clone())
            .unwrap();
        let backend = Arc::new(DfsBrowserBackend {
            node_id: node,
            data_dir: (*data_dir).clone(),
            host: Arc::new(UnusedHost::default()),
            service: Mutex::new(service),
            replica_control_plane: Mutex::new(None),
            peer_targets_refreshed_at: Mutex::new(None),
            stat_cache: Mutex::new(HashMap::new()),
        });
        *browser_backend_slot(&data_dir)
            .unwrap()
            .backend
            .lock()
            .unwrap() = Some(backend.clone());
        admission_runtime().block_on(async {
            let service = Arc::new(HostedDfsBrowserService::new(
                Some(tokio::runtime::Handle::current()),
                Arc::new(UnusedHost::default()),
            ));
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let mutation_backend = backend.clone();
            let mutation = tokio::spawn(run_blocking_browser_task(
                data_dir.clone(),
                "test",
                "slow mutation",
                move || {
                    let mut namespace = mutation_backend.lock_service()?;
                    let _ = entered_tx.send(());
                    release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    namespace
                        .posix_write_file("/admission.txt", b"committed winner", options)
                        .map_err(dfs_browser_service_error)?;
                    Ok(())
                },
            ));
            entered_rx.await.unwrap();
            let mut reads = Vec::new();
            for _ in 0..3 {
                let (tx, rx) = tokio::sync::oneshot::channel();
                let ctx = ServiceContext::new(
                    data_dir.clone(),
                    None,
                    ServiceSigner::Local,
                    sinomo_core::network::ObjectRpcIncomingBody::empty(),
                    Box::new(Sink(tx)),
                );
                let request = ObjectRpcRequest::encode_body(
                    DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
                    DAEMON_RPC_CATEGORY,
                    &crate::DaemonDfsBrowserReadFileRequest {
                        path: "/admission.txt".into(),
                    },
                )
                .unwrap();
                let service = service.clone();
                reads.push((
                    tokio::spawn(async move { service.handle(ctx, request).await }),
                    rx,
                ));
            }
            // Negative control: the old direct blocking-worker path rejects
            // this exact contention. The hosted calls above must keep waiting.
            let legacy_backend = Arc::clone(&backend);
            let legacy = tokio::task::spawn_blocking(move || {
                legacy_backend.read_file(crate::DaemonDfsBrowserReadFileRequest {
                    path: "/admission.txt".into(),
                })
            })
            .await
            .unwrap()
            .expect_err("direct lock admission reproduces the busy error");
            assert!(
                legacy
                    .to_string()
                    .contains("DFS browser service is busy after waiting 100ms")
            );
            assert!(
                reads.iter().all(|(read, _)| !read.is_finished()),
                "admitted reads must queue instead of returning 100 ms busy errors"
            );
            release_tx.send(()).unwrap();
            mutation.await.unwrap().unwrap();
            for (read, response) in reads {
                read.await.unwrap().unwrap();
                let response = response.await.unwrap();
                let bytes = response.body.collect_to_vec(1024 * 1024).await.unwrap();
                let response: crate::DaemonDfsBrowserReadFileResponse =
                    sinomo_core::network::ObjectRpcResponse::from_encoded_body(
                        response.response.route,
                        bytes,
                    )
                    .decode_body()
                    .unwrap();
                assert_eq!(response.into_result().unwrap(), b"committed winner");
            }
        });

        *browser_backend_slot(&data_dir)
            .unwrap()
            .backend
            .lock()
            .unwrap() = None;
        drop(backend);
        native.close().expect("normal native browser shutdown");
    }

    #[test]
    fn daemon_no_link_browser_preserves_receipts_and_rejects_links_stale_cas_and_cursors() {
        use crate::DaemonDfsBrowserNoLinksFailure as F;
        use crate::DaemonDfsBrowserNoLinksOperation as O;
        use crate::DaemonDfsBrowserNoLinksResponse as R;
        let root = sinomo_testdir::TestDir::new("native-browser-no-link-consumers");
        let data_dir = SinomoDataDir::new(root.path());
        let (native, node) = crate::test_support::DfsTestRuntime::start_single_node_in_data_dir(
            "daemon_no_link_browser_preserves_receipts_and_rejects_links_stale_cas_and_cursors",
            data_dir.clone(),
        )
        .unwrap();
        let service = native.client(&node).unwrap();
        let backend = DfsBrowserBackend {
            node_id: service.node_id().clone(),
            data_dir,
            host: Arc::new(UnusedHost::default()),
            service: Mutex::new(service),
            replica_control_plane: Mutex::new(None),
            peer_targets_refreshed_at: Mutex::new(Some(Instant::now())),
            stat_cache: Mutex::new(HashMap::new()),
        };
        let scope = "/services/no-link-consumers/memory";
        let request = |path: &str, operation| crate::DaemonDfsBrowserNoLinksRequest {
            expected_cluster_id: None,
            root: scope.into(),
            path: path.into(),
            operation,
        };
        let mut wrong_namespace = request(scope, O::CreateDirAll);
        wrong_namespace.expected_cluster_id = Some(sinomo_core::Id::generate());
        let error = backend.no_links(wrong_namespace).unwrap_err();
        assert!(error.to_string().contains("cluster namespace changed"));
        assert!(matches!(backend.no_links(request(scope, O::Stat)).unwrap(),
            R::Failed { failure: F::NotFound { .. } }));
        assert!(matches!(
            backend.no_links(request(scope, O::CreateDirAll)).unwrap(),
            R::DirectoryCreated
        ));
        let a = format!("{scope}/a.md");
        let R::Metadata { metadata: first } = backend
            .no_links(request(
                &a,
                O::CreateNew {
                    bytes: b"first".to_vec(),
                },
            ))
            .unwrap()
        else {
            panic!("exact create receipt");
        };
        assert!(matches!(
            backend
                .no_links(request(
                    &a,
                    O::CreateNew {
                        bytes: b"duplicate".to_vec()
                    }
                ))
                .unwrap(),
            R::Failed {
                failure: F::AlreadyExists { .. }
            }
        ));
        let replace = |version: &crate::DaemonDfsBrowserStatResponse| O::ReplaceIfVersion {
            expected_object_id: version.object_id.clone(),
            expected_version: version.version.clone(),
            bytes: b"second".to_vec(),
        };
        let R::Metadata { metadata: second } = backend.no_links(request(&a, replace(&first))).unwrap()
        else {
            panic!("exact replacement receipt");
        };
        assert_eq!(first.object_id, second.object_id);
        assert_ne!(first.version, second.version);
        assert!(matches!(
            backend.no_links(request(&a, replace(&first))).unwrap(),
            R::Failed {
                failure: F::Conflict { .. }
            }
        ));
        let R::Read {
            metadata,
            bytes,
            truncated,
        } = backend
            .no_links(request(
                &a,
                O::ReadRange {
                    offset: 1,
                    max_bytes: 2,
                },
            ))
            .unwrap()
        else {
            panic!("exact read receipt");
        };
        assert_eq!(bytes, b"ec");
        assert!(truncated);
        assert_eq!(metadata.object_id, second.object_id);
        assert_eq!(metadata.version, second.version);
        assert!(matches!(
            backend
                .no_links(request(
                    &a,
                    O::ReadRange {
                        offset: 99,
                        max_bytes: 2
                    }
                ))
                .unwrap(),
            R::Failed {
                failure: F::InvalidPath { .. }
            }
        ));
        assert!(matches!(
            backend
                .no_links(request(&format!("{scope}/missing.md"), O::Stat))
                .unwrap(),
            R::Failed {
                failure: F::NotFound { .. }
            }
        ));
        backend
            .no_links(request(
                &format!("{scope}/b.md"),
                O::CreateNew { bytes: vec![] },
            ))
            .unwrap();
        let R::DirectoryPage {
            next_cursor: Some(cursor),
            entries,
            ..
        } = backend
            .no_links(request(
                scope,
                O::ListPage {
                    cursor: None,
                    max_entries: 1,
                },
            ))
            .unwrap()
        else {
            panic!("bounded continuation");
        };
        assert_eq!(entries.len(), 1);
        backend
            .no_links(request(
                &format!("{scope}/c.md"),
                O::CreateNew { bytes: vec![] },
            ))
            .unwrap();
        assert!(matches!(
            backend
                .no_links(request(
                    scope,
                    O::ListPage {
                        cursor: Some(cursor),
                        max_entries: 1
                    }
                ))
                .unwrap(),
            R::Failed {
                failure: F::Conflict { .. }
            }
        ));
        {
            let mut service = backend.lock_service().unwrap();
            service
                .posix_mkdir_all("/services/outside-no-link")
                .unwrap();
            let options = crate::DfsWriteOptions::replicated_eventual(vec![service.node_id().clone()]);
            service
                .posix_symlink(
                    "/services/outside-no-link",
                    &format!("{scope}/linked"),
                    options,
                )
                .unwrap();
        }
        assert!(matches!(
            backend
                .no_links(request(&format!("{scope}/linked"), O::Stat))
                .unwrap(),
            R::Failed {
                failure: F::InvalidPath { .. }
            }
        ));
        assert!(matches!(
            backend
                .no_links(request(
                    "/services/outside-no-link/escape.md",
                    O::CreateNew {
                        bytes: b"must not escape".to_vec()
                    }
                ))
                .unwrap(),
            R::Failed {
                failure: F::InvalidPath { .. }
            }
        ));
        assert!(daemon_dfs_browser_route_is_path_access(
            DAEMON_DFS_BROWSER_NO_LINKS_ROUTE
        ));
        assert!(DFS_CORE_BROWSER_ROUTES.contains(&DAEMON_DFS_BROWSER_NO_LINKS_ROUTE));
        drop(backend);
        native.close().unwrap();
    }

}

#[cfg(test)]
#[path = "browser_service/write_options_tests.rs"]
mod write_options_tests;

#[cfg(test)]
#[path = "browser_service/configuration_consumer_tests.rs"]
mod configuration_consumer_tests;

#[cfg(test)]
#[path = "browser_service/placement_visibility_tests.rs"]
mod placement_visibility_tests;

#[cfg(test)]
#[path = "browser_service/remote_parent_tests.rs"]
mod remote_parent_tests;

#[cfg(test)]
#[path = "browser_service/no_links_race_tests.rs"]
mod no_links_race_tests;

#[cfg(test)]
mod same_primary_availability_native_tests;

#[cfg(test)]
mod cold_open_native_tests;
