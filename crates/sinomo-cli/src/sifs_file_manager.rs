use crate::commands::cli_commit_cluster_kv_txn_with_quorum_response;
use crate::file_manager::{
    FileManagerBackend, FileManagerEntry, FileManagerEntryKind, FileManagerMetadata,
    FileManagerRead, validated_relative_child_path,
};
use sinomo_runtime::core::ClusterEndpointRecord;
use sinomo_runtime::dfs::{
    AsyncDfsProtocolFrameSender, DfsControlPlaneKvClient, DfsEndpointTransport,
    DfsMetadataJournalBatchSender, DfsMetadataJournalReplicationOptions,
    DfsMetadataPeerTransportError, DfsMetadataShardConfig, DfsMetadataSnapshotInstallSender,
    DfsNodeAdvertisement, DfsObjectKind, DfsPeerTransportError, DfsProtocolFrameCommand,
    DfsProtocolFrameResponse, DfsProtocolFrameSender, DfsStat, DfsWriteOptions, LocalContentStore,
    NetworkedDfsService, content_services_prefix, default_dfs_metadata_root,
    default_dfs_metadata_shards_root, fallback_dfs_node_id, metadata_shards_prefix,
    open_local_sharded_metadata_backend, open_root_local_metadata_backend,
    publish_cluster_dfs_metadata_journal, sifs_file_system_id,
};
use sinomo_runtime::kv::{self, KvError, KvOp, KvTxn, KvTxnResponse};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{self, ClusterPaths};
use sinomo_runtime::node::NodeId;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub(crate) struct SifsFileManagerBackend {
    data_dir: SinomoDataDir,
    _rpc_runtime: SifsRpcRuntime,
    service: Mutex<NetworkedDfsService<DfsEndpointTransport>>,
    replica_candidates: Vec<NodeId>,
}

#[derive(Debug)]
pub(crate) struct SifsParallelReadPool {
    workers: Vec<SifsFileManagerBackend>,
}

impl SifsFileManagerBackend {
    pub(crate) fn open(data_dir: &SinomoDataDir) -> Result<Self, String> {
        let rpc_runtime = SifsRpcRuntime::new()?;
        let service = open_sifs_service(data_dir, &rpc_runtime)?;
        Self::from_service(data_dir, rpc_runtime, service)
    }

    pub(crate) fn open_read_only(data_dir: &SinomoDataDir) -> Result<Self, String> {
        let rpc_runtime = SifsRpcRuntime::new()?;
        Self::open_read_only_with_runtime(data_dir, rpc_runtime)
    }

    fn open_read_only_with_runtime(
        data_dir: &SinomoDataDir,
        rpc_runtime: SifsRpcRuntime,
    ) -> Result<Self, String> {
        let service = open_sifs_service_read_only(data_dir, &rpc_runtime)?;
        Self::from_service(data_dir, rpc_runtime, service)
    }

    fn from_service(
        data_dir: &SinomoDataDir,
        rpc_runtime: SifsRpcRuntime,
        service: NetworkedDfsService<DfsEndpointTransport>,
    ) -> Result<Self, String> {
        let replica_candidates = sifs_replica_candidates(data_dir, service.node_id());
        Ok(Self {
            data_dir: data_dir.clone(),
            _rpc_runtime: rpc_runtime,
            service: Mutex::new(service),
            replica_candidates,
        })
    }

    fn lock_service(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, NetworkedDfsService<DfsEndpointTransport>>, String> {
        self.service
            .lock()
            .map_err(|_| "SIFS service lock is poisoned".to_string())
    }

    pub(crate) fn publish_metadata_after_mutation(&self) {
        if let Ok(mut service) = self.lock_service() {
            if service.flush_deferred_content_durability().is_err() {
                return;
            }
            let _ = service.flush_detached_transport_sends();
            let _ = service.process_incoming();
        }
        let _ = publish_sifs_metadata_journal_from_data_dir(&self.data_dir);
    }

    /// The node id objects written here are placed on (the local SIFS node).
    pub(crate) fn node_label(&self) -> String {
        self.lock_service()
            .map(|service| service.node_id().to_string())
            .unwrap_or_default()
    }

    pub(crate) fn read_file(&self, path: &str) -> Result<Vec<u8>, String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        service
            .posix_read_file_distributed(&path)
            .map_err(|error| format!("cannot read SIFS file {path}: {error}"))
    }

    pub(crate) fn dfs_stat(&self, path: &str) -> Result<DfsStat, String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        service
            .posix_stat_distributed(&path)
            .map_err(|error| format!("cannot inspect {path}: {error}"))
    }

    pub(crate) fn debug_remove_local_loose_blob_for_path(
        &self,
        path: &str,
    ) -> Result<DfsStat, String> {
        let stat = self.dfs_stat(path)?;
        let local_node_id = self.node_label();
        if !stat
            .current_locations
            .iter()
            .any(|node_id| node_id.as_str() == local_node_id)
        {
            return Err(format!(
                "local node {local_node_id} is not a current location for {path}: {:?}",
                stat.current_locations
            ));
        }
        let Some(content_hash) = stat.content_hash.as_deref() else {
            return Err(format!("{path} has no content hash to damage"));
        };
        let store = LocalContentStore::new(self.data_dir.dfs_dir().join("content"))
            .map_err(|error| format!("cannot open DFS content store: {error}"))?;
        store
            .debug_remove_loose_blob(content_hash)
            .map_err(|error| format!("cannot remove local loose blob for {path}: {error}"))?;
        Ok(stat)
    }

    pub(crate) fn read_files(&self, paths: &[String]) -> Vec<Result<Vec<u8>, String>> {
        let mut normalized = Vec::with_capacity(paths.len());
        let mut results = (0..paths.len()).map(|_| None).collect::<Vec<_>>();
        for (index, path) in paths.iter().enumerate() {
            match normalize_sifs_path(path) {
                Ok(path) => normalized.push((index, path)),
                Err(error) => results[index] = Some(Err(error)),
            }
        }
        if !normalized.is_empty() {
            match self.lock_service() {
                Ok(mut service) => {
                    let read_paths = normalized
                        .iter()
                        .map(|(_, path)| path.clone())
                        .collect::<Vec<_>>();
                    for ((index, path), result) in normalized
                        .into_iter()
                        .zip(service.posix_read_files_distributed(&read_paths))
                    {
                        results[index] = Some(
                            result
                                .map_err(|error| format!("cannot read SIFS file {path}: {error}")),
                        );
                    }
                }
                Err(error) => {
                    for (index, path) in normalized {
                        results[index] =
                            Some(Err(format!("cannot read SIFS file {path}: {error}")));
                    }
                }
            }
        }
        results
            .into_iter()
            .enumerate()
            .map(|(index, result)| {
                result.unwrap_or_else(|| {
                    Err(format!(
                        "cannot read SIFS file {}: batch read did not report a result",
                        paths[index]
                    ))
                })
            })
            .collect()
    }

    pub(crate) fn open_parallel_read_pool(
        &self,
        requested_workers: usize,
    ) -> Result<SifsParallelReadPool, String> {
        let worker_count = sifs_parallel_read_worker_count(requested_workers);
        let rpc_runtime = SifsRpcRuntime::new()?;
        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            workers.push(Self::open_read_only_with_runtime(
                &self.data_dir,
                rpc_runtime.clone(),
            )?);
        }
        Ok(SifsParallelReadPool { workers })
    }

    pub(crate) fn write_file(&self, path: &str, bytes: &[u8]) -> Result<(), String> {
        self.write_file_with_publish(path, bytes, true)
    }

    fn write_file_with_publish(
        &self,
        path: &str,
        bytes: &[u8],
        publish_metadata: bool,
    ) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        if let Some(parent) = parent_sifs_path(&path) {
            service
                .posix_mkdir_all(&parent)
                .map_err(|error| format!("cannot create SIFS directory {parent}: {error}"))?;
        }
        let options = sifs_write_options(&service, &self.replica_candidates);
        service
            .posix_write_file(&path, bytes, options)
            .map(|_| ())
            .map_err(|error| format!("cannot write SIFS file {path}: {error}"))?;
        drop(service);
        if publish_metadata {
            self.publish_metadata_after_mutation();
        }
        Ok(())
    }

    pub(crate) fn write_files_with_known_parents_deferred_publish(
        &self,
        files: &[(String, Vec<u8>)],
    ) -> Result<(), String> {
        let normalized = files
            .iter()
            .map(|(path, bytes)| normalize_sifs_path(path).map(|path| (path, bytes.as_slice())))
            .collect::<Result<Vec<_>, _>>()?;
        let mut service = self.lock_service()?;
        service
            .posix_create_files_local_eventual_with_known_parents(
                normalized
                    .iter()
                    .map(|(path, bytes)| (path.as_str(), *bytes)),
            )
            .map(|_| ())
            .map_err(|error| format!("cannot write SIFS bulk batch: {error}"))?;
        Ok(())
    }

    pub(crate) fn write_file_with_known_parent_deferred_publish(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        let options = sifs_write_options(&service, &self.replica_candidates);
        service
            .posix_write_file_with_known_parent_deferred_local_replay(&path, bytes, options)
            .map(|_| ())
            .map_err(|error| format!("cannot write SIFS file {path}: {error}"))?;
        Ok(())
    }

    pub(crate) fn operational_metrics(
        &self,
    ) -> Result<sinomo_runtime::dfs::DfsOperationalMetrics, String> {
        let service = self.lock_service()?;
        service
            .operational_metrics()
            .map_err(|error| format!("cannot read SIFS operational metrics: {error}"))
    }

    pub(crate) fn create_dir_all(&self, path: &str) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        service
            .posix_mkdir_all(&path)
            .map_err(|error| format!("cannot create SIFS directory {path}: {error}"))?;
        drop(service);
        self.publish_metadata_after_mutation();
        Ok(())
    }

    pub(crate) fn create_dir_all_deferred_local_replay(&self, path: &str) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        service
            .posix_mkdir_all_without_local_replay(&path)
            .map_err(|error| format!("cannot create SIFS directory {path}: {error}"))
    }
}

impl SifsParallelReadPool {
    pub(crate) fn worker_count(&self) -> usize {
        self.workers.len()
    }

    pub(crate) fn read_files(&self, paths: &[String]) -> Vec<Result<Vec<u8>, String>> {
        if paths.is_empty() {
            return Vec::new();
        }
        if self.workers.is_empty() {
            return paths
                .iter()
                .map(|path| Err(format!("cannot read SIFS file {path}: no read workers")))
                .collect();
        }
        if paths.len() == 1 {
            return self.workers[0].read_files(paths);
        }
        let results = Arc::new(Mutex::new(
            (0..paths.len()).map(|_| None).collect::<Vec<_>>(),
        ));
        let worker_count = self.workers.len();
        std::thread::scope(|scope| {
            for (worker_index, worker) in self.workers.iter().enumerate() {
                let results = Arc::clone(&results);
                scope.spawn(move || {
                    let indexes = (worker_index..paths.len())
                        .step_by(worker_count)
                        .collect::<Vec<_>>();
                    if indexes.is_empty() {
                        return;
                    }
                    let worker_paths = indexes
                        .iter()
                        .map(|index| paths[*index].clone())
                        .collect::<Vec<_>>();
                    for (index, result) in indexes.into_iter().zip(worker.read_files(&worker_paths))
                    {
                        if let Ok(mut results) = results.lock() {
                            results[index] = Some(result);
                        }
                    }
                });
            }
        });
        let mut results = results
            .lock()
            .map(|mut results| std::mem::take(&mut *results))
            .unwrap_or_default();
        results
            .drain(..)
            .enumerate()
            .map(|(index, result)| {
                result.unwrap_or_else(|| {
                    Err(format!(
                        "cannot read SIFS file {}: read worker did not report a result",
                        paths[index]
                    ))
                })
            })
            .collect()
    }
}

impl FileManagerBackend for SifsFileManagerBackend {
    fn source_label(&self) -> &str {
        "SIFS"
    }

    fn normalize_path(&self, path: &str) -> Result<String, String> {
        normalize_sifs_path(path)
    }

    fn parent_path(&self, path: &str) -> Option<String> {
        let path = normalize_sifs_path(path).ok()?;
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

    fn child_path(&self, current_dir: &str, name: &str) -> Result<String, String> {
        let child = validated_relative_child_path(name)?;
        let mut current = normalize_sifs_path(current_dir)?;
        for component in child.components() {
            let std::path::Component::Normal(name) = component else {
                continue;
            };
            let name = name.to_string_lossy();
            if current == "/" {
                current.push_str(&name);
            } else {
                current.push('/');
                current.push_str(&name);
            }
        }
        normalize_sifs_path(&current)
    }

    fn display_name(&self, path: &str) -> String {
        let path = normalize_sifs_path(path).unwrap_or_else(|_| path.to_string());
        if path == "/" {
            return "/".to_string();
        }
        path.rsplit('/')
            .next()
            .filter(|name| !name.is_empty())
            .unwrap_or(path.as_str())
            .to_string()
    }

    fn stat(&self, path: &str) -> Result<FileManagerMetadata, String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        let stat = service
            .posix_stat_distributed(&path)
            .map_err(|error| format!("cannot inspect {path}: {error}"))?;
        Ok(FileManagerMetadata {
            kind: sifs_entry_kind(&stat.kind),
            size: stat.size_bytes,
        })
    }

    fn list_dir(&self, path: &str) -> Result<Vec<FileManagerEntry>, String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        let entries = service
            .posix_list_dir_distributed(&path)
            .map_err(|error| format!("cannot list {path}: {error}"))?;
        let mut results = Vec::new();
        for entry in entries {
            let stat = service.posix_stat_distributed(&entry.path).ok();
            let kind = stat
                .as_ref()
                .map(|stat| sifs_entry_kind(&stat.kind))
                .unwrap_or_else(|| sifs_entry_kind(&entry.kind));
            let size = stat.and_then(|stat| stat.size_bytes);
            results.push(FileManagerEntry {
                name: entry.name,
                path: entry.path,
                kind,
                size,
            });
        }
        Ok(results)
    }

    fn read_file_prefix(&self, path: &str, max_bytes: u64) -> Result<FileManagerRead, String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        let stat = service
            .posix_stat_distributed(&path)
            .map_err(|error| format!("cannot inspect {path}: {error}"))?;
        let truncated = stat
            .size_bytes
            .map(|size| size > max_bytes)
            .unwrap_or(false);
        let bytes = if truncated {
            service
                .posix_read_file_range(&path, 0, max_bytes as usize)
                .map_err(|error| format!("cannot read {path}: {error}"))?
        } else {
            service
                .posix_read_file_distributed(&path)
                .map_err(|error| format!("cannot read {path}: {error}"))?
        };
        Ok(FileManagerRead { bytes, truncated })
    }

    fn directory_entry_count(&self, path: &str) -> Result<usize, String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        service
            .posix_list_dir_distributed(&path)
            .map(|entries| entries.len())
            .map_err(|error| error.to_string())
    }

    fn create_file(&self, path: &str) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        let options = sifs_write_options(&service, &self.replica_candidates);
        service
            .posix_write_file(&path, &[], options)
            .map(|_| ())
            .map_err(|error| format!("cannot create SIFS file {path}: {error}"))?;
        drop(service);
        self.publish_metadata_after_mutation();
        Ok(())
    }

    fn create_dir(&self, path: &str) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        service
            .posix_mkdir(&path)
            .map(|_| ())
            .map_err(|error| format!("cannot create SIFS directory {path}: {error}"))?;
        drop(service);
        self.publish_metadata_after_mutation();
        Ok(())
    }

    fn rename(&self, from: &str, to: &str) -> Result<(), String> {
        let from = normalize_sifs_path(from)?;
        let to = normalize_sifs_path(to)?;
        let mut service = self.lock_service()?;
        service
            .posix_rename(&from, &to)
            .map(|_| ())
            .map_err(|error| format!("cannot rename SIFS item {from} to {to}: {error}"))?;
        drop(service);
        self.publish_metadata_after_mutation();
        Ok(())
    }

    fn delete_file(&self, path: &str) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        service
            .posix_unlink(&path)
            .map(|_| ())
            .map_err(|error| format!("cannot delete SIFS file {path}: {error}"))?;
        drop(service);
        self.publish_metadata_after_mutation();
        Ok(())
    }

    fn delete_dir(&self, path: &str) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        service
            .posix_rmdir(&path)
            .map(|_| ())
            .map_err(|error| {
                format!(
                    "cannot delete SIFS directory {path}: {error}; only empty directories can be deleted"
                )
            })?;
        drop(service);
        self.publish_metadata_after_mutation();
        Ok(())
    }
}

#[derive(Clone)]
struct SifsRpcRuntime {
    runtime: Arc<tokio::runtime::Runtime>,
}

impl std::fmt::Debug for SifsRpcRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SifsRpcRuntime")
    }
}

impl SifsRpcRuntime {
    fn new() -> Result<Self, String> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(sifs_rpc_runtime_worker_threads())
            .thread_name("sinomo-sifs-rpc")
            .enable_all()
            .build()
            .map_err(|error| format!("cannot create SIFS RPC runtime: {error}"))?;
        Ok(Self {
            runtime: Arc::new(runtime),
        })
    }
}

fn sifs_rpc_runtime_worker_threads() -> usize {
    const DEFAULT_WORKERS: usize = 16;
    const MAX_WORKERS: usize = 64;
    std::env::var("SINOMO_SIFS_RPC_WORKER_THREADS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_WORKERS)
        .clamp(1, MAX_WORKERS)
}

fn sifs_parallel_read_worker_count(requested_workers: usize) -> usize {
    const DEFAULT_WORKERS: usize = 32;
    const MAX_WORKERS: usize = 64;
    if requested_workers == 0 {
        return 1;
    }
    let configured = std::env::var("SINOMO_SIFS_PARALLEL_READ_WORKERS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_WORKERS);
    configured.min(requested_workers).clamp(1, MAX_WORKERS)
}

fn open_sifs_service(
    data_dir: &SinomoDataDir,
    rpc_runtime: &SifsRpcRuntime,
) -> Result<NetworkedDfsService<DfsEndpointTransport>, String> {
    open_sifs_service_with_hydration(data_dir, rpc_runtime, true)
}

fn open_sifs_service_read_only(
    data_dir: &SinomoDataDir,
    rpc_runtime: &SifsRpcRuntime,
) -> Result<NetworkedDfsService<DfsEndpointTransport>, String> {
    open_sifs_service_with_hydration(data_dir, rpc_runtime, false)
}

fn open_sifs_service_with_hydration(
    data_dir: &SinomoDataDir,
    rpc_runtime: &SifsRpcRuntime,
    hydrate_metadata: bool,
) -> Result<NetworkedDfsService<DfsEndpointTransport>, String> {
    data_dir
        .ensure_root_layout()
        .map_err(|error| format!("cannot prepare Sinomo data directory: {error}"))?;
    let cluster_paths = ClusterPaths::new(data_dir);
    let (local_node_id, endpoints) = if cluster_paths.state.exists() {
        let state = network::read_cluster_state_without_kv(&cluster_paths)
            .map_err(|error| format!("cannot read local cluster state: {error}"))?;
        let endpoints = network::cluster_dfs_protocol_targets(&cluster_paths, &state);
        (state.local_node_id, endpoints)
    } else {
        (fallback_dfs_node_id(), Vec::new())
    };
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))
        .map_err(|error| format!("cannot open DFS content store: {error}"))?;
    let mut service = NetworkedDfsService::new(
        DfsEndpointTransport::new_with_senders(
            local_node_id,
            endpoints,
            sifs_protocol_frame_sender(rpc_runtime.clone()),
            async_sifs_protocol_frame_sender(rpc_runtime.clone()),
        ),
        content,
    );
    if cluster_paths.state.exists() {
        service = service.with_control_plane(SifsControlPlaneKvClient::new(&cluster_paths));
    }
    let service =
        if let Some(metadata) = open_sifs_sharded_metadata_backend(data_dir, &cluster_paths)? {
            if hydrate_metadata {
                service
                    .with_metadata_backend(metadata)
                    .map_err(|error| format!("cannot open sharded SIFS metadata: {error}"))?
            } else {
                service
                    .with_metadata_backend_lazy(metadata)
                    .map_err(|error| format!("cannot open sharded SIFS metadata: {error}"))?
            }
        } else {
            let metadata = open_root_local_metadata_backend(default_dfs_metadata_root(data_dir))
                .map_err(|error| format!("cannot open local SIFS metadata: {error}"))?;
            if hydrate_metadata {
                service
                    .with_metadata_backend(metadata)
                    .map_err(|error| format!("cannot open local SIFS metadata: {error}"))?
            } else {
                service
                    .with_metadata_backend_lazy(metadata)
                    .map_err(|error| format!("cannot open local SIFS metadata: {error}"))?
            }
        };
    let mut service = service;
    service
        .bootstrap_distributed_sifs()
        .map_err(|error| format!("cannot bootstrap SIFS namespace: {error}"))?;
    Ok(service)
}

#[derive(Clone, Debug)]
struct SifsControlPlaneKvClient {
    cluster_paths: ClusterPaths,
}

impl SifsControlPlaneKvClient {
    fn new(cluster_paths: &ClusterPaths) -> Self {
        Self {
            cluster_paths: cluster_paths.clone(),
        }
    }

    fn kv_error(&self, source: impl std::fmt::Display) -> KvError {
        KvError::Io {
            path: self.cluster_paths.kv_snapshot.display().to_string(),
            detail: source.to_string(),
        }
    }
}

impl DfsControlPlaneKvClient for SifsControlPlaneKvClient {
    fn get(&self, key: &str) -> Result<Option<sinomo_runtime::kv::KvEntry>, KvError> {
        kv::read_cluster_kv_entry(&self.cluster_paths.kv_paths(), key)
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<sinomo_runtime::kv::KvEntry>, KvError> {
        kv::read_cluster_kv_prefix_entries(&self.cluster_paths.kv_paths(), prefix)
    }

    fn put(
        &mut self,
        key: String,
        value: Vec<u8>,
    ) -> Result<sinomo_runtime::kv::KvRevision, KvError> {
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
        let mut state = network::read_cluster_state(&self.cluster_paths)
            .map_err(|source| self.kv_error(source))?;
        let result =
            cli_commit_cluster_kv_txn_with_quorum_response(&self.cluster_paths, &mut state, txn)
                .map_err(|source| self.kv_error(source))?;
        Ok(result.txn_response.unwrap_or(KvTxnResponse {
            succeeded: true,
            revision: result.snapshot.revision,
            responses: Vec::new(),
        }))
    }
}

fn open_sifs_sharded_metadata_backend(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<Option<sinomo_runtime::dfs::LocalShardedMetadataBackend>, String> {
    if !cluster_paths.state.exists() {
        return Ok(None);
    }
    let file_system_id = sifs_file_system_id();
    let entries = kv::read_cluster_kv_prefix_entries(
        &cluster_paths.kv_paths(),
        &format!("{}/", metadata_shards_prefix(&file_system_id)),
    )
    .map_err(|error| format!("cannot read DFS metadata shard config: {error}"))?;
    let mut shards = Vec::new();
    for entry in entries {
        shards.push(
            entry
                .decode::<DfsMetadataShardConfig>()
                .map_err(|error| format!("cannot decode DFS metadata shard config: {error}"))?,
        );
    }
    shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    if shards.is_empty() {
        return Ok(None);
    }
    open_local_sharded_metadata_backend(default_dfs_metadata_shards_root(data_dir), shards)
        .map(Some)
        .map_err(|error| format!("cannot open DFS metadata shards: {error}"))
}

fn sifs_replica_candidates(data_dir: &SinomoDataDir, local_node_id: &NodeId) -> Vec<NodeId> {
    let mut candidates = vec![local_node_id.clone()];
    let cluster_paths = ClusterPaths::new(data_dir);
    if !cluster_paths.state.exists() {
        return candidates;
    }
    let Ok(state) = network::read_cluster_state_without_kv(&cluster_paths) else {
        return candidates;
    };
    let routable_node_ids = network::cluster_dfs_protocol_targets(&cluster_paths, &state)
        .into_iter()
        .map(|endpoint| endpoint.node_id)
        .collect::<BTreeSet<_>>();
    let graph = network::ClusterConnectivityGraph::read_for_local(&cluster_paths, &state);
    let Ok(entries) =
        kv::read_cluster_kv_prefix_entries(&cluster_paths.kv_paths(), content_services_prefix())
    else {
        return candidates;
    };
    let mut content_node_ids = entries
        .into_iter()
        .filter_map(|entry| entry.decode::<DfsNodeAdvertisement>().ok())
        .map(|advertisement| advertisement.node_id)
        .collect::<Vec<_>>();
    content_node_ids.sort();
    content_node_ids.dedup();
    if let Some(remote_node_id) = choose_sifs_remote_replica_candidate(
        &graph,
        local_node_id,
        content_node_ids,
        &routable_node_ids,
    ) {
        candidates.push(remote_node_id);
    }
    candidates
}

fn choose_sifs_remote_replica_candidate(
    graph: &network::ClusterConnectivityGraph,
    local_node_id: &NodeId,
    mut content_node_ids: Vec<NodeId>,
    routable_node_ids: &BTreeSet<NodeId>,
) -> Option<NodeId> {
    content_node_ids
        .sort_by_key(|node_id| sifs_replica_candidate_rank(&graph, local_node_id, node_id));
    content_node_ids.into_iter().find(|node_id| {
        node_id != local_node_id
            && routable_node_ids.contains(node_id)
            && graph
                .best_healthy_endpoint(local_node_id, node_id)
                .is_some()
    })
}

fn sifs_replica_candidate_rank(
    graph: &network::ClusterConnectivityGraph,
    local_node_id: &NodeId,
    node_id: &NodeId,
) -> (u8, u64, String) {
    if node_id == local_node_id {
        return (0, 0, node_id.to_string());
    }
    if let Some(endpoint) = graph.best_healthy_endpoint(local_node_id, node_id) {
        return (
            1,
            endpoint
                .last_observed_latency_ms
                .map(u64::from)
                .unwrap_or(u64::MAX),
            node_id.to_string(),
        );
    }
    if graph.reachable_targets(local_node_id).contains(node_id) {
        return (2, u64::MAX, node_id.to_string());
    }
    (3, u64::MAX, node_id.to_string())
}

fn sifs_protocol_frame_sender(rpc_runtime: SifsRpcRuntime) -> DfsProtocolFrameSender {
    Arc::new(move |endpoint, command, timeout| {
        sifs_send_dfs_protocol_frame(&rpc_runtime, endpoint, command, timeout)
    })
}

fn async_sifs_protocol_frame_sender(rpc_runtime: SifsRpcRuntime) -> AsyncDfsProtocolFrameSender {
    Arc::new(move |endpoint, command, timeout| {
        let handle = rpc_runtime.runtime.handle().clone();
        Box::pin(async move {
            sinomo_runtime::daemon::send_dfs_protocol_frame_to_endpoint_async(
                endpoint, command, timeout, &handle,
            )
            .await
            .map_err(|error| DfsPeerTransportError::new(error.to_string()))
        })
    })
}

fn sifs_send_dfs_protocol_frame(
    rpc_runtime: &SifsRpcRuntime,
    endpoint: &ClusterEndpointRecord,
    command: &DfsProtocolFrameCommand,
    timeout: std::time::Duration,
) -> Result<DfsProtocolFrameResponse, DfsPeerTransportError> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        if matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            return tokio::task::block_in_place(|| {
                handle.block_on(async {
                    sinomo_runtime::daemon::send_dfs_protocol_frame_to_endpoint_async(
                        endpoint, command, timeout, &handle,
                    )
                    .await
                    .map_err(|error| DfsPeerTransportError::new(error.to_string()))
                })
            });
        }
        return Err(DfsPeerTransportError::new(
            "SIFS protocol frame sync sender cannot run inside a current-thread Tokio runtime",
        ));
    }
    rpc_runtime
        .runtime
        .block_on(async {
            sinomo_runtime::daemon::send_dfs_protocol_frame_to_endpoint_async(
                endpoint,
                command,
                timeout,
                rpc_runtime.runtime.handle(),
            )
            .await
        })
        .map_err(|error| DfsPeerTransportError::new(error.to_string()))
}

fn publish_sifs_metadata_journal_from_data_dir(
    data_dir: &SinomoDataDir,
) -> Result<(), sinomo_runtime::dfs::DfsError> {
    let cluster_paths = ClusterPaths::new(data_dir);
    let sender: DfsMetadataJournalBatchSender = Arc::new(|endpoint, command, timeout| {
        let request = sinomo_runtime::dfs::dfs_metadata_journal_batch_rpc_request(command)
            .map_err(|error| DfsMetadataPeerTransportError::new(error.to_string()))?;
        let response =
            sinomo_runtime::daemon::send_daemon_rpc_to_endpoint(endpoint, &request, timeout)
                .map_err(|error| DfsMetadataPeerTransportError::new(error.to_string()))?;
        sinomo_runtime::dfs::decode_dfs_metadata_journal_batch_rpc_response(&response)
            .map_err(|error| DfsMetadataPeerTransportError::new(error.to_string()))
    });
    let snapshot_sender: DfsMetadataSnapshotInstallSender =
        Arc::new(|endpoint, command, timeout| {
            let request = sinomo_runtime::dfs::dfs_metadata_snapshot_install_rpc_request(command)
                .map_err(|error| DfsMetadataPeerTransportError::new(error.to_string()))?;
            let response =
                sinomo_runtime::daemon::send_daemon_rpc_to_endpoint(endpoint, &request, timeout)
                    .map_err(|error| DfsMetadataPeerTransportError::new(error.to_string()))?;
            sinomo_runtime::dfs::decode_dfs_metadata_snapshot_install_rpc_response(&response)
                .map_err(|error| DfsMetadataPeerTransportError::new(error.to_string()))
        });
    publish_cluster_dfs_metadata_journal(
        data_dir,
        &cluster_paths,
        sender,
        snapshot_sender,
        DfsMetadataJournalReplicationOptions::default(),
    )?;
    Ok(())
}

fn sifs_write_options(
    service: &NetworkedDfsService<DfsEndpointTransport>,
    replica_candidates: &[NodeId],
) -> DfsWriteOptions {
    let mut candidates = replica_candidates.to_vec();
    if !candidates.contains(service.node_id()) {
        candidates.insert(0, service.node_id().clone());
    }
    sifs_write_options_for_candidates(candidates)
}

fn sifs_write_options_for_candidates(candidates: Vec<NodeId>) -> DfsWriteOptions {
    let mut options = DfsWriteOptions::replicated_eventual(candidates);
    options.foreground_replica_repair = false;
    options
}

fn sifs_entry_kind(kind: &DfsObjectKind) -> FileManagerEntryKind {
    match kind {
        DfsObjectKind::Directory => FileManagerEntryKind::Directory,
        DfsObjectKind::File | DfsObjectKind::Artifact => FileManagerEntryKind::File,
        DfsObjectKind::Symlink => FileManagerEntryKind::Symlink,
        DfsObjectKind::Hardlink | DfsObjectKind::Mount | DfsObjectKind::Tombstone => {
            FileManagerEntryKind::Other
        }
    }
}

fn normalize_sifs_path(path: &str) -> Result<String, String> {
    let path = path.trim();
    let path = if path.is_empty() {
        "/".to_string()
    } else if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    if path.bytes().any(|byte| byte == 0) {
        return Err(format!("invalid SIFS path: {path}"));
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

fn parent_sifs_path(path: &str) -> Option<String> {
    let path = normalize_sifs_path(path).ok()?;
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
    use crate::file_manager::FileManagerState;
    use std::sync::Arc;

    #[test]
    fn normalize_sifs_paths() {
        assert_eq!(normalize_sifs_path("").expect("root"), "/");
        assert_eq!(
            normalize_sifs_path("/services/../users").expect("path"),
            "/users"
        );
        assert_eq!(
            normalize_sifs_path("sinomo/nodes").expect("relative"),
            "/sinomo/nodes"
        );
    }

    #[test]
    fn sifs_backend_manages_local_sifs_without_cluster() {
        let root = temp_sifs_data_dir("local");
        let data_dir = SinomoDataDir::new(&root);
        let backend = SifsFileManagerBackend::open(&data_dir).expect("backend");
        let mut manager = FileManagerState::open(Arc::new(backend), "/services").expect("manager");

        manager.create_dir("tui-test").expect("create dir");
        assert!(manager.entries.iter().any(|entry| entry.name == "tui-test"));
        manager.open_selected().expect("open dir");
        manager.create_file("alpha.txt").expect("create file");
        manager.rename_selected("beta.txt").expect("rename file");
        assert!(manager.entries.iter().any(|entry| entry.name == "beta.txt"));
        manager.delete_selected("DELETE").expect("delete file");
        manager.go_parent().expect("parent");
        manager.delete_selected("DELETE").expect("delete dir");
    }

    #[test]
    fn replica_candidate_rank_prefers_near_healthy_peer_over_lexicographic_peer() {
        let local = NodeId::from_legacy_named("node_z_local").expect("node");
        let far = NodeId::from_legacy_named("node_a_far").expect("node");
        let near = NodeId::from_legacy_named("node_y_near").expect("node");
        let graph = network::ClusterConnectivityGraph::from_records(
            local.clone(),
            vec![
                healthy_connectivity_record(&local, &far, 50),
                healthy_connectivity_record(&local, &near, 1),
            ],
        );
        let mut candidates = vec![far.clone(), near.clone()];

        candidates.sort_by_key(|node_id| sifs_replica_candidate_rank(&graph, &local, node_id));

        assert_eq!(
            candidates,
            vec![near, far],
            "replica selection should prefer the lower-latency healthy peer, not lexical order"
        );
    }

    #[test]
    fn remote_replica_candidate_requires_healthy_peer() {
        let local = NodeId::from_legacy_named("node_replica_select_local").expect("node");
        let degraded = NodeId::from_legacy_named("node_replica_select_degraded").expect("node");
        let healthy = NodeId::from_legacy_named("node_replica_select_healthy").expect("node");
        let mut routable = BTreeSet::new();
        routable.insert(degraded.clone());
        routable.insert(healthy.clone());
        let degraded_graph = network::ClusterConnectivityGraph::from_records(
            local.clone(),
            vec![connectivity_record(
                &local,
                &degraded,
                network::ClusterConnectivityState::Degraded,
                Some(1),
            )],
        );

        assert_eq!(
            choose_sifs_remote_replica_candidate(
                &degraded_graph,
                &local,
                vec![degraded.clone()],
                &routable,
            ),
            None,
            "reachable but degraded peers must not be selected as synchronous replicas"
        );

        let healthy_graph = network::ClusterConnectivityGraph::from_records(
            local.clone(),
            vec![
                connectivity_record(
                    &local,
                    &degraded,
                    network::ClusterConnectivityState::Degraded,
                    Some(1),
                ),
                connectivity_record(
                    &local,
                    &healthy,
                    network::ClusterConnectivityState::Healthy,
                    Some(25),
                ),
            ],
        );

        assert_eq!(
            choose_sifs_remote_replica_candidate(
                &healthy_graph,
                &local,
                vec![degraded, healthy.clone()],
                &routable,
            ),
            Some(healthy)
        );
    }

    #[test]
    fn sifs_write_options_commit_after_local_receipt_and_repair_async() {
        let local = NodeId::from_legacy_named("node_sifs_write_policy_local").expect("node");
        let remote = NodeId::from_legacy_named("node_sifs_write_policy_remote").expect("node");

        let options = sifs_write_options_for_candidates(vec![local, remote]);

        assert_eq!(options.policy.min_commit_replicas, 1);
        assert_eq!(options.policy.desired_replicas, 2);
        assert!(options.wait_for_commit);
        assert!(
            !options.foreground_replica_repair,
            "normal SIFS writes should not wait for the async replica repair path"
        );
    }

    fn healthy_connectivity_record(
        source: &NodeId,
        target: &NodeId,
        latency_ms: u64,
    ) -> network::ClusterConnectivityRecord {
        connectivity_record(
            source,
            target,
            network::ClusterConnectivityState::Healthy,
            Some(latency_ms),
        )
    }

    fn connectivity_record(
        source: &NodeId,
        target: &NodeId,
        state: network::ClusterConnectivityState,
        latency_ms: Option<u64>,
    ) -> network::ClusterConnectivityRecord {
        let now = time::OffsetDateTime::now_utc();
        network::ClusterConnectivityRecord {
            cluster_id: network::ClusterId::from_legacy_named("cluster_sifs_replica_rank")
                .expect("cluster"),
            source_node_id: source.clone(),
            target_node_id: target.clone(),
            endpoint_id: network::ClusterEndpointId::from_legacy_named(format!(
                "endpoint_sifs_replica_rank_{target}"
            ))
            .expect("endpoint"),
            transport: sinomo_runtime::core::ClusterTransportKind::LanTcp,
            address: format!("{target}:21353"),
            state,
            latency_ms,
            consecutive_failures: 0,
            last_success_at: None,
            last_failure_at: None,
            observed_at: now,
            expires_at: now + time::Duration::seconds(600),
        }
    }

    fn temp_sifs_data_dir(name: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(&format!("sinomo-tui-sifs-file-manager-{name}"))
    }
}
