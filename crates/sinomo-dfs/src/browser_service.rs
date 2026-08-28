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

const DAEMON_RPC_CATEGORY: &str = "daemon";

/// Daemon/process effects used by the DFS-owned browser services.
pub trait DfsBrowserServiceHost: Send + Sync + 'static {
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

async fn run_blocking_service_task<T>(
    route: impl Into<String>,
    label: &'static str,
    task: impl FnOnce() -> Result<T, ServiceError> + Send + 'static,
) -> Result<T, ServiceError>
where
    T: Send + 'static,
{
    let route = route.into();
    tokio::task::spawn_blocking(task).await.map_err(|source| {
        ServiceError::Service(format!("{label} route {route} task failed: {source}"))
    })?
}

pub struct DfsBrowserBackend {
    data_dir: SinomoDataDir,
    host: Arc<dyn DfsBrowserServiceHost>,
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
}

static DFS_BROWSER_BACKENDS: OnceLock<Mutex<HashMap<PathBuf, Arc<DfsBrowserBackendSlot>>>> =
    OnceLock::new();

#[cfg(feature = "test-support")]
static DFS_BROWSER_BACKEND_OPEN_COUNTS: OnceLock<Mutex<HashMap<PathBuf, usize>>> = OnceLock::new();

#[cfg(feature = "test-support")]
fn record_dfs_browser_backend_open(data_dir: &SinomoDataDir) {
    let counts = DFS_BROWSER_BACKEND_OPEN_COUNTS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut counts = counts.lock().expect("DFS browser open-count lock");
    *counts.entry(data_dir.root()).or_default() += 1;
}

#[cfg(feature = "test-support")]
pub fn dfs_browser_backend_open_count(data_dir: &SinomoDataDir) -> usize {
    DFS_BROWSER_BACKEND_OPEN_COUNTS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("DFS browser open-count lock")
        .get(&data_dir.root())
        .copied()
        .unwrap_or_default()
}

// Browser reads run on blocking workers because the DFS POSIX surface is
// synchronous. A remote content fetch can consume the full 30-second POSIX
// deadline while holding the shared service lock. Do not let the other browser
// workers park behind that request: ordinary short local operations still get
// a small handoff window, while degraded remote reads fail fast and let their
// callers retry without building an in-process queue. This is an admission
// bound only; it does not impose an end-to-end timeout on an admitted DFS read.
const DAEMON_DFS_BROWSER_READ_LOCK_ADMISSION_TIMEOUT: Duration = Duration::from_millis(100);
const DAEMON_DFS_BROWSER_PLACEMENT_IMPACT_PAGE_SIZE: usize = 256;
const DAEMON_DFS_BROWSER_STAT_CACHE_MAX_ENTRIES: usize = 256;

impl DfsBrowserBackend {
    pub fn open(
        data_dir: SinomoDataDir,
        runtime: &tokio::runtime::Handle,
        host: Arc<dyn DfsBrowserServiceHost>,
    ) -> Result<Self, ServiceError> {
        #[cfg(feature = "test-support")]
        record_dfs_browser_backend_open(&data_dir);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let control_plane = host.control_plane_client(&cluster_paths, runtime);
        let replica_control_plane = host.control_plane_client(&cluster_paths, runtime);
        let (peer_transport, async_peer_transport) = host.peer_transports(&data_dir, runtime);
        let opened = crate::open_mounted_sifs_service_with_peer_transports(
            data_dir.clone(),
            peer_transport,
            async_peer_transport,
            control_plane,
        )
        .map_err(dfs_browser_service_error)?;
        let node_id = opened.service.node_id().clone();
        let cache = host.placement_snapshot_cache(&data_dir, &node_id);
        let mut service = opened.service.with_placement_snapshot_cache(cache);
        refresh_browser_content_pin_snapshot(&data_dir, &mut service);
        Ok(Self {
            data_dir,
            host,
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
        let root = data_dir.root();
        let cache = DFS_BROWSER_BACKENDS.get_or_init(|| Mutex::new(HashMap::new()));
        let slot = {
            let mut guard = cache.lock().map_err(|source| {
                ServiceError::Service(format!("DFS browser backend cache lock failed: {source}"))
            })?;
            Arc::clone(
                guard
                    .entry(root)
                    .or_insert_with(|| Arc::new(DfsBrowserBackendSlot::default())),
            )
        };
        let mut initialized = slot.backend.lock().map_err(|source| {
            ServiceError::Service(format!(
                "DFS browser backend initialization lock failed: {source}"
            ))
        })?;
        if let Some(backend) = initialized.as_ref() {
            return Ok(Arc::clone(backend));
        }
        let backend = Arc::new(Self::open((*data_dir).clone(), runtime, host)?);
        *initialized = Some(Arc::clone(&backend));
        Ok(backend)
    }

    pub fn lock_service(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, crate::MountedSifsService>, ServiceError> {
        self.service.lock().map_err(|source| {
            ServiceError::Service(format!("DFS browser service lock failed: {source}"))
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
        Ok(self.lock_service_for_read()?.node_id().to_string())
    }

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
            None => Ok(vec![crate::fallback_dfs_node_id()]),
        }
    }

    fn event_origin_node_id(&self) -> Result<sinomo_core::Id, ServiceError> {
        Ok(self.lock_service_for_read()?.node_id().clone())
    }

    pub fn health_summary(&self) -> Result<crate::DaemonDfsBrowserHealthSummary, ServiceError> {
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        let service = self.lock_service_for_read()?;
        let metadata = service.metadata_backend().ok_or_else(|| {
            ServiceError::Service("DFS browser metadata backend is unavailable".to_string())
        })?;
        let status =
            crate::inspect_dfs_status_from_metadata(&self.data_dir, &cluster_paths, metadata)
                .map_err(dfs_browser_service_error)?;
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
                    daemon_dfs_browser_entry_placement(&mut service, &entry.path, metadata)
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
        let health = daemon_dfs_browser_object_health(&service, &stat.object_id);
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

    fn invalidate_stat_cache(&self) {
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

    pub fn write_file(
        &self,
        request: crate::DaemonDfsBrowserWriteFileRequest,
    ) -> Result<crate::DaemonDfsBrowserWriteFileResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&request.path)?;
        let mut service = self.lock_service()?;
        if let Some(parent) = parent_sifs_browser_path(&path) {
            service
                .posix_mkdir_all(&parent)
                .map_err(dfs_browser_service_error)?;
        }
        let replica_candidates = self.content_replica_candidates()?;
        let options = sifs_browser_write_options(&mut service, &path, &replica_candidates)?;
        service
            .posix_write_file(&path, &request.bytes, options)
            .map_err(dfs_browser_service_error)?;
        self.flush_after_mutation(&mut service)?;
        Ok(crate::DaemonDfsBrowserWriteFileResponse)
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
        let mut service = self.lock_service()?;
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

    fn create_dir_all(
        &self,
        request: crate::DaemonDfsBrowserCreateDirAllRequest,
    ) -> Result<crate::DaemonDfsBrowserCreateDirAllResponse, ServiceError> {
        let path = normalize_sifs_browser_path(&request.path)?;
        let mut service = self.lock_service()?;
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
        let mut service = self.lock_service()?;
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
        let mut service = self.lock_service()?;
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
        let mut service = self.lock_service()?;
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
        if let Ok(mut service) = self.lock_service() {
            refresh_browser_content_pin_snapshot(&self.data_dir, &mut service);
        }
        self.invalidate_stat_cache();
        Ok(node_id.to_string())
    }

    fn unpin_path_mode(&self, path: &str, mode: crate::DfsPinMode) -> Result<bool, ServiceError> {
        let path = normalize_sifs_browser_path(path)?;
        let mut service = self.lock_service()?;
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
            if let Ok(mut service) = self.lock_service() {
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
        if let Ok(mut service) = self.lock_service() {
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
        let reconciliation = crate::DfsPlacementReconciliationJob {
            schema_version: crate::DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION,
            job_id: sinomo_core::Id::generate().to_string(),
            file_system_id: desired.file_system_id.clone(),
            path,
            scope,
            unavailable_node_ids,
            candidate_snapshot,
            placement_snapshot,
            adaptive: false,
            adaptive_budget: None,
            processed_content_bytes: 0,
            accepted_config: Some(desired.clone()),
            state: crate::DfsPlacementReconciliationState::Pending,
            cursor: None,
            total_object_count: 0,
            scanned_object_count: 0,
            matched_object_count: 0,
            updated_object_count: 0,
            scheduled_repair_count: 0,
            completed_repair_count: 0,
            pending_repair_count: 0,
            revision: 0,
            created_at_unix_nanos: now,
            updated_at_unix_nanos: now,
            last_error: None,
        };
        if !store
            .compare_and_put_placement_update(&expected, &desired, &reconciliation)
            .map_err(dfs_browser_service_error)?
        {
            return Err(ServiceError::Service(
                "SIFS placement policy changed concurrently; refresh the preview and retry"
                    .to_string(),
            ));
        }
        self.invalidate_stat_cache();
        let reconciliation = store
            .placement_reconciliation(&desired.file_system_id, &reconciliation.job_id)
            .map_err(dfs_browser_service_error)?
            .ok_or_else(|| {
                ServiceError::Service(
                    "accepted SIFS placement job is missing from the control plane".to_string(),
                )
            })?;
        self.host.notify_maintenance();
        Ok(crate::DaemonDfsBrowserPlacementUpdateResponse {
            preview,
            reconciliation,
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
                let service = self.lock_service_for_read()?;
                service
                    .placement_impact_page_for_config_with_unavailable_nodes(
                        config,
                        &stat.path,
                        scope,
                        &candidates,
                        cursor.as_ref(),
                        DAEMON_DFS_BROWSER_PLACEMENT_IMPACT_PAGE_SIZE,
                        &unavailable_node_ids,
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
        DAEMON_DFS_BROWSER_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        self.routes()
            .iter()
            .copied()
            .map(|route| {
                let path_access = daemon_dfs_browser_route_is_path_access(route);
                let mut descriptor = ServiceRoute::new(route, DAEMON_RPC_CATEGORY)
                    .with_auth_scope(if path_access {
                        sinomo_module::ModuleRouteAuth::LocalOrClusterMember.as_scope()
                    } else {
                        sinomo_module::ModuleRouteAuth::LocalCommand.as_scope()
                    })
                    .require_capability("daemon.dfs.browser")
                    .with_concurrency_limit(4)
                    .with_timeout(Duration::from_secs(60));
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
                DAEMON_DFS_BROWSER_STATUS_ROUTE => {
                    let payload: crate::DaemonDfsBrowserStatusRequest =
                        ctx.decode_request_body(&request).await?;
                    let backend =
                        run_blocking_service_task(route.clone(), "DFS browser status", move || {
                            DfsBrowserBackend::for_data_dir(
                                data_dir,
                                &runtime,
                                Arc::clone(&backend_host),
                            )
                        })
                        .await?;
                    let (health, health_error) = if payload.include_health {
                        match backend.health_summary() {
                            Ok(health) => (Some(health), None),
                            Err(error) => (None, Some(error.to_string())),
                        }
                    } else {
                        (None, None)
                    };
                    ctx.respond_value(
                        DAEMON_DFS_BROWSER_STATUS_ROUTE,
                        crate::DaemonDfsBrowserStatusResponse {
                            node_id: backend.node_id()?,
                            health,
                            health_error,
                        },
                    )?;
                }
                DAEMON_DFS_BROWSER_LIST_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result =
                        run_blocking_service_task(route.clone(), "DFS browser list", move || {
                            DfsBrowserBackend::for_data_dir(
                                data_dir,
                                &runtime,
                                Arc::clone(&backend_host),
                            )?
                            .list(payload)
                        })
                        .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_LIST_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_STAT_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result =
                        run_blocking_service_task(route.clone(), "DFS browser stat", move || {
                            DfsBrowserBackend::for_data_dir(
                                data_dir,
                                &runtime,
                                Arc::clone(&backend_host),
                            )?
                            .stat(payload)
                        })
                        .await?;
                    ctx.respond_value(DAEMON_DFS_BROWSER_STAT_ROUTE, result)?;
                }
                DAEMON_DFS_BROWSER_READ_FILE_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result = run_blocking_service_task(
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
                    let result = run_blocking_service_task(
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
                DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result = run_blocking_service_task(
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
                    let result = run_blocking_service_task(
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
                    let payload: crate::DaemonDfsBrowserWriteFileRequest =
                        ctx.decode_request_body(&request).await?;
                    let changed_path = payload.path.clone();
                    let backend_data_dir = Arc::clone(&data_dir);
                    let publish_data_dir = Arc::clone(&data_dir);
                    let backend_runtime = runtime.clone();
                    let traceparent = request_traceparent.clone();
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser write file",
                        move || {
                            crate::with_dfs_traceparent(traceparent.as_deref(), || {
                                let backend = DfsBrowserBackend::for_data_dir(
                                    backend_data_dir,
                                    &backend_runtime,
                                    Arc::clone(&backend_host),
                                )?;
                                let result = backend.write_file(payload)?;
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
                        "write-file",
                        vec![changed_path],
                    );
                    ctx.respond_value(DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE, result.0)?;
                }
                DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE => {
                    let payload: crate::DaemonDfsBrowserReplaceFileIfVersionRequest =
                        ctx.decode_request_body(&request).await?;
                    let changed_path = payload.path.clone();
                    let backend_data_dir = Arc::clone(&data_dir);
                    let publish_data_dir = Arc::clone(&data_dir);
                    let backend_runtime = runtime.clone();
                    let traceparent = request_traceparent.clone();
                    let result = run_blocking_service_task(
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
                DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE => {
                    let payload: crate::DaemonDfsBrowserCreateDirAllRequest =
                        ctx.decode_request_body(&request).await?;
                    let changed_path = payload.path.clone();
                    let backend_data_dir = Arc::clone(&data_dir);
                    let publish_data_dir = Arc::clone(&data_dir);
                    let backend_runtime = runtime.clone();
                    let traceparent = request_traceparent.clone();
                    let result = run_blocking_service_task(
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
                    let result =
                        run_blocking_service_task(route.clone(), "DFS browser rename", move || {
                            crate::with_dfs_traceparent(traceparent.as_deref(), || {
                                let backend = DfsBrowserBackend::for_data_dir(
                                    backend_data_dir,
                                    &backend_runtime,
                                    Arc::clone(&backend_host),
                                )?;
                                let result = backend.rename(payload)?;
                                Ok((result, backend.event_origin_node_id()?))
                            })
                        })
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
                    let result =
                        run_blocking_service_task(route.clone(), "DFS browser delete", move || {
                            crate::with_dfs_traceparent(traceparent.as_deref(), || {
                                let backend = DfsBrowserBackend::for_data_dir(
                                    backend_data_dir,
                                    &backend_runtime,
                                    Arc::clone(&backend_host),
                                )?;
                                let result = backend.delete(payload)?;
                                Ok((result, backend.event_origin_node_id()?))
                            })
                        })
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
                    let result = run_blocking_service_task(
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
                    let result = run_blocking_service_task(
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
                    let result = run_blocking_service_task(
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
                    let result = run_blocking_service_task(
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
                    let result = run_blocking_service_task(
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
                    let result = run_blocking_service_task(
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
                DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_ROUTE => {
                    let _: crate::DaemonDfsBrowserPlacementUpdateRequest =
                        ctx.decode_request_body(&request).await?;
                    return Err(legacy_daemon_dfs_browser_placement_update_error());
                }
                DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE => {
                    let payload: crate::DaemonDfsBrowserPlacementUpdateV2Request =
                        ctx.decode_request_body(&request).await?;
                    let changed_path = payload.path.clone();
                    let result = run_blocking_service_task(
                        route.clone(),
                        "DFS browser placement update v2",
                        move || {
                            let backend = DfsBrowserBackend::for_data_dir(
                                data_dir,
                                &runtime,
                                Arc::clone(&backend_host),
                            )?;
                            let result = backend.placement_update_v2(payload, &runtime)?;
                            Ok((result, backend.event_origin_node_id()?))
                        },
                    )
                    .await?;
                    effect_host.publish_placement_change(
                        result.1.clone(),
                        "update-policy",
                        vec![changed_path],
                    );
                    ctx.respond_value(DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE, result.0)?;
                }
                DAEMON_DFS_BROWSER_PLACEMENT_PROGRESS_ROUTE => {
                    let payload = ctx.decode_request_body(&request).await?;
                    let result = run_blocking_service_task(
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
                    let result = run_blocking_service_task(
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
            | DAEMON_DFS_BROWSER_LIST_ROUTE
            | DAEMON_DFS_BROWSER_READ_FILE_ROUTE
            | DAEMON_DFS_BROWSER_READ_OBJECT_VERSION_ROUTE
            | DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE
            | DAEMON_DFS_BROWSER_READ_FILES_ROUTE
            | DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE
            | DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE
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
        open_conflict_count: status.open_conflict_count,
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
    service: &crate::MountedSifsService,
    object_id: &crate::DfsObjectId,
) -> Option<crate::DaemonDfsBrowserObjectHealth> {
    let backend = service.metadata_backend()?;
    let view = crate::DfsOperatorInspector::new(backend)
        .object(object_id)
        .ok()
        .flatten()?;
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
        open_conflict_count: view.conflicts.len(),
        pending_repair_count: view.repairs.len(),
        missing_replica_nodes,
    })
}

fn daemon_dfs_browser_entry_placement(
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
    let agent_state = daemon_dfs_browser_object_health(service, &object_id)
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

fn sifs_browser_write_options(
    service: &mut crate::MountedSifsService,
    path: &str,
    replica_candidates: &[sinomo_core::node::NodeId],
) -> Result<crate::DfsWriteOptions, ServiceError> {
    let object_policy = service
        .posix_stat(path)
        .map(|stat| stat.policy)
        .unwrap_or(sinomo_core::DfsObjectPolicy::ReplicatedEventual);
    let (_, _, policy) = service
        .managed_replication_policy_for_path(path, object_policy, replica_candidates.len())
        .map_err(dfs_browser_service_error)?;
    let candidates = sifs_browser_write_candidates(replica_candidates, &policy);
    let options = crate::DfsWriteOptions {
        policy,
        replica_candidates: candidates,
        wait_for_commit: true,
        foreground_replica_repair: false,
    };
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

fn legacy_daemon_dfs_browser_placement_update_error() -> ServiceError {
    ServiceError::Service(
        "legacy SIFS placement Apply is disabled because it cannot verify the reviewed preview; refresh the preview and retry with a client that uses /daemon/dfs/browser/placement-update-v2"
            .to_string(),
    )
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
        error => Outcome::Rejected {
            detail: error.to_string(),
        },
    }
}

fn normalize_sifs_browser_path(path: &str) -> Result<String, ServiceError> {
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
