use crate::commands::{
    cli_commit_cluster_kv_txn_with_quorum_response,
    refresh_cluster_kv_from_local_daemon_for_data_dir,
};
use crate::file_manager::{
    FileManagerBackend, FileManagerEntry, FileManagerEntryKind, FileManagerMetadata,
    FileManagerRead, validated_relative_child_path,
};
#[allow(unused_imports)]
use sinomo_runtime::cluster;
use sinomo_runtime::cluster::ClusterKvPathsExt;
use sinomo_runtime::dfs::{
    AsyncDfsPeerTransport, DfsControlPlaneKvClient, DfsControlPlaneStore, DfsError,
    DfsMetadataJournalReplicationOptions, DfsMetadataJournalReplicationReport,
    DfsMetadataShardConfig, DfsNodeAdvertisement, DfsNodeTransport, DfsObjectKind,
    DfsObjectRpcSendOptions, DfsPeerTransport, DfsPeerTransportError, DfsPeerTransportFuture,
    DfsPinAddRequest, DfsPinMode, DfsPosixCreateAttrs, DfsPosixError, DfsStat, DfsWriteOptions,
    LocalContentStore, NetworkedDfsService, cluster_dfs_peer_node_ids, content_services_prefix,
    default_dfs_metadata_root, default_dfs_metadata_shards_root, default_dfs_pin_root,
    fallback_dfs_node_id, metadata_shards_prefix,
    open_local_sharded_metadata_backend_for_replica_repair, open_root_local_metadata_backend,
    publish_cluster_dfs_metadata_journal_with_peer_transports_async,
    refresh_cluster_dfs_kv_snapshot_with_peer_transport, sifs_file_system_id,
};
use sinomo_runtime::kv::{KvError, KvOp, KvTxn, KvTxnResponse};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{self, ClusterPaths};
use sinomo_runtime::node::NodeId;
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const SIFS_PEER_TARGET_REFRESH_TTL: Duration = Duration::from_secs(1);
const SIFS_FOREGROUND_METADATA_PUBLISH_MAX_BATCHES_PER_TARGET: usize = 1;
const SIFS_MAINTENANCE_REFRESH_TIMEOUT: Duration = Duration::from_millis(750);

#[derive(Debug)]
pub(crate) struct SifsFileManagerBackend {
    data_dir: SinomoDataDir,
    rpc_runtime: SifsRpcRuntime,
    service: Mutex<NetworkedDfsService<DfsNodeTransport>>,
    peer_targets_refreshed_at: Mutex<Option<Instant>>,
    replica_candidates: Vec<NodeId>,
}

#[derive(Debug)]
pub(crate) struct SifsParallelReadPool {
    workers: Vec<SifsFileManagerBackend>,
}

#[derive(Clone, Debug)]
pub(crate) struct SifsMetadataPrimaryTransferReport {
    pub(crate) file_system_id: String,
    pub(crate) shard_id: String,
    pub(crate) previous_primary_node_id: String,
    pub(crate) new_primary_node_id: String,
    pub(crate) previous_epoch: u64,
    pub(crate) new_epoch: u64,
    pub(crate) changed: bool,
    pub(crate) shard: DfsMetadataShardConfig,
    pub(crate) local_node_id: String,
}

impl SifsFileManagerBackend {
    pub(crate) fn open(data_dir: &SinomoDataDir) -> Result<Self, String> {
        let rpc_runtime = SifsRpcRuntime::new()?;
        let service = open_sifs_service_with_hydration(data_dir, &rpc_runtime, false)?;
        Self::from_service(data_dir, rpc_runtime, service)
    }

    pub(crate) fn open_with_deadline(
        data_dir: &SinomoDataDir,
        deadline: Instant,
    ) -> Result<Self, String> {
        let operation_deadline = Some(deadline);
        sifs_operation_remaining(operation_deadline, "creating the SIFS RPC runtime")?;
        let rpc_runtime = SifsRpcRuntime::new()?;
        sifs_operation_remaining(operation_deadline, "creating the SIFS RPC runtime")?;
        let service = open_sifs_service_with_hydration_until(
            data_dir,
            &rpc_runtime,
            false,
            operation_deadline,
        )?;
        sifs_operation_remaining(operation_deadline, "opening the SIFS backend")?;
        Self::from_service(data_dir, rpc_runtime, service)
    }

    pub(crate) fn open_read_only(data_dir: &SinomoDataDir) -> Result<Self, String> {
        let rpc_runtime = SifsRpcRuntime::new()?;
        Self::open_read_only_with_runtime(data_dir, rpc_runtime)
    }

    pub(crate) fn open_read_only_with_deadline(
        data_dir: &SinomoDataDir,
        deadline: Instant,
    ) -> Result<Self, String> {
        let operation_deadline = Some(deadline);
        sifs_operation_remaining(operation_deadline, "creating the SIFS RPC runtime")?;
        let rpc_runtime = SifsRpcRuntime::new()?;
        let service = open_sifs_service_with_hydration_until(
            data_dir,
            &rpc_runtime,
            false,
            operation_deadline,
        )?;
        sifs_operation_remaining(operation_deadline, "opening the read-only SIFS backend")?;
        Self::from_service(data_dir, rpc_runtime, service)
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
        service: NetworkedDfsService<DfsNodeTransport>,
    ) -> Result<Self, String> {
        let replica_candidates = sifs_replica_candidates(data_dir, service.node_id());
        Ok(Self {
            data_dir: data_dir.clone(),
            rpc_runtime,
            service: Mutex::new(service),
            peer_targets_refreshed_at: Mutex::new(Some(Instant::now())),
            replica_candidates,
        })
    }

    fn lock_service(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, NetworkedDfsService<DfsNodeTransport>>, String> {
        self.service
            .lock()
            .map_err(|_| "SIFS service lock is poisoned".to_string())
    }

    fn lock_service_with_deadline(
        &self,
        operation_deadline: Option<Instant>,
    ) -> Result<std::sync::MutexGuard<'_, NetworkedDfsService<DfsNodeTransport>>, String> {
        let Some(deadline) = operation_deadline else {
            return self.lock_service();
        };
        loop {
            match self.service.try_lock() {
                Ok(service) => return Ok(service),
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err("SIFS service lock is poisoned".to_string());
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    let remaining = sifs_operation_remaining(
                        Some(deadline),
                        "waiting for the SIFS service lock",
                    )?
                    .expect("deadline is present");
                    std::thread::sleep(Duration::from_millis(1).min(remaining));
                }
            }
        }
    }

    fn refresh_service_peer_targets(&self, service: &mut NetworkedDfsService<DfsNodeTransport>) {
        self.refresh_service_peer_targets_with_deadline(service, None);
    }

    fn refresh_service_peer_targets_with_deadline(
        &self,
        service: &mut NetworkedDfsService<DfsNodeTransport>,
        operation_deadline: Option<Instant>,
    ) {
        let now = Instant::now();
        if operation_deadline.is_some() {
            let needs_refresh = match self.peer_targets_refreshed_at.try_lock() {
                Ok(refreshed_at) => !refreshed_at
                    .as_ref()
                    .is_some_and(|last| now.duration_since(*last) < SIFS_PEER_TARGET_REFRESH_TTL),
                Err(_) => false,
            };
            if !needs_refresh {
                return;
            }
            if self.refresh_service_peer_targets_now(service, operation_deadline)
                && let Ok(mut refreshed_at) = self.peer_targets_refreshed_at.try_lock()
            {
                *refreshed_at = Some(now);
            }
            return;
        }
        if let Ok(mut refreshed_at) = self.peer_targets_refreshed_at.lock() {
            if refreshed_at
                .as_ref()
                .is_some_and(|last| now.duration_since(*last) < SIFS_PEER_TARGET_REFRESH_TTL)
            {
                return;
            }
            if self.refresh_service_peer_targets_now(service, operation_deadline) {
                *refreshed_at = Some(now);
            }
            return;
        }
        let _ = self.refresh_service_peer_targets_now(service, operation_deadline);
    }

    fn refresh_service_peer_targets_now(
        &self,
        service: &mut NetworkedDfsService<DfsNodeTransport>,
        operation_deadline: Option<Instant>,
    ) -> bool {
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        if !cluster_paths.state.exists() {
            return false;
        }
        if let Some(deadline) = operation_deadline {
            if sifs_operation_remaining(Some(deadline), "refreshing SIFS peer targets").is_err() {
                return false;
            }
            let peer_transport: Arc<dyn DfsPeerTransport> = Arc::new(
                SifsDfsPeerTransport::new(self.rpc_runtime.clone(), self.data_dir.clone())
                    .with_operation_deadline(Some(deadline)),
            );
            if refresh_cluster_dfs_kv_snapshot_with_peer_transport(&cluster_paths, peer_transport)
                .is_err()
            {
                return false;
            }
        } else if refresh_sifs_cluster_kv_snapshot(&self.data_dir, &cluster_paths).is_err() {
            return false;
        }
        let Ok(state) = cluster::read_cluster_state(&cluster_paths) else {
            return false;
        };
        service.refresh_peer_targets(cluster_dfs_peer_node_ids(&state));
        true
    }

    pub(crate) fn publish_metadata_after_mutation(&self) -> Result<(), String> {
        self.publish_metadata_after_mutation_with_deadline(None)
    }

    fn publish_metadata_after_mutation_with_deadline(
        &self,
        operation_deadline: Option<Instant>,
    ) -> Result<(), String> {
        self.prepare_metadata_after_mutation(operation_deadline)?;
        handoff_or_publish_sifs_metadata(
            operation_deadline,
            |timeout| request_sifs_daemon_maintenance_refresh(&self.data_dir, timeout),
            || {
                publish_sifs_metadata_journal_from_data_dir(
                    &self.data_dir,
                    &self.rpc_runtime,
                    operation_deadline,
                )
                .map(|_| ())
                .map_err(|error| error.to_string())
            },
        )
    }

    /// Best-effort post-commit maintenance for a conditional editor save.
    /// Unlike the legacy mutation helper, this preserves a warning for the UI
    /// instead of printing and discarding publication failures.
    fn publish_metadata_after_conditional_mutation_warning(&self) -> Option<String> {
        if let Err(error) = self.prepare_metadata_after_mutation(None) {
            return Some(format!(
                "follow-up DFS maintenance preparation failed: {error}"
            ));
        }
        if request_sifs_daemon_maintenance_refresh(&self.data_dir, SIFS_MAINTENANCE_REFRESH_TIMEOUT)
        {
            return None;
        }
        publish_sifs_metadata_journal_from_data_dir(&self.data_dir, &self.rpc_runtime, None)
            .err()
            .map(|error| format!("SIFS metadata publication failed: {error}"))
    }

    pub(crate) fn publish_metadata_after_mutation_report(
        &self,
    ) -> Result<DfsMetadataJournalReplicationReport, String> {
        self.prepare_metadata_after_mutation(None)?;
        let report =
            publish_sifs_metadata_journal_from_data_dir(&self.data_dir, &self.rpc_runtime, None)
                .map_err(|error| error.to_string())?;
        let _ = request_sifs_daemon_maintenance_refresh(
            &self.data_dir,
            SIFS_MAINTENANCE_REFRESH_TIMEOUT,
        );
        Ok(report)
    }

    fn prepare_metadata_after_mutation(
        &self,
        operation_deadline: Option<Instant>,
    ) -> Result<(), String> {
        if let Ok(mut service) = self.lock_service_with_deadline(operation_deadline) {
            let previous_deadline = operation_deadline
                .map(|deadline| service.set_transport_operation_deadline(Some(deadline)));
            let result: Result<(), String> = (|| {
                sifs_operation_remaining(operation_deadline, "deferred content durability")?;
                service
                    .flush_deferred_content_durability()
                    .map_err(|error| error.to_string())?;
                sifs_operation_remaining(operation_deadline, "detached DFS sends")?;
                service
                    .flush_detached_transport_sends()
                    .map_err(|error| error.to_string())?;
                let response_wait =
                    sifs_operation_remaining(operation_deadline, "pending routed write responses")?
                        .map(|remaining| remaining.min(network::DEFAULT_NETWORK_TIMEOUT))
                        .unwrap_or(network::DEFAULT_NETWORK_TIMEOUT);
                service
                    .process_incoming_until_pending_routed_writes_ready(response_wait)
                    .map_err(|error| error.to_string())?;
                sifs_operation_remaining(operation_deadline, "pending routed write responses")?;
                Ok(())
            })();
            if let Some(previous_deadline) = previous_deadline {
                service.set_transport_operation_deadline(previous_deadline);
            }
            result?;
        }
        Ok(())
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
        self.refresh_service_peer_targets(&mut service);
        service
            .posix_read_file_distributed(&path)
            .map_err(|error| format!("cannot read SIFS file {path}: {error}"))
    }

    pub(crate) fn read_object_version(
        &self,
        object_id: &str,
        version: &str,
        content_hash: &str,
        size_bytes: u64,
    ) -> Result<Vec<u8>, String> {
        let object_id = sinomo_runtime::dfs::DfsObjectId::new(object_id)
            .map_err(|error| format!("invalid SIFS object id: {error}"))?;
        let version = sinomo_runtime::dfs::DfsVersion::new(version)
            .map_err(|error| format!("invalid SIFS version: {error}"))?;
        let mut service = self.lock_service()?;
        self.refresh_service_peer_targets(&mut service);
        service
            .read_object_version_content(&object_id, &version, content_hash, Some(size_bytes))
            .map_err(|error| {
                format!("cannot read SIFS object {object_id} at version {version}: {error}")
            })
    }

    pub(crate) fn dfs_stat(&self, path: &str) -> Result<DfsStat, String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        self.refresh_service_peer_targets(&mut service);
        service
            .posix_stat_distributed(&path)
            .map_err(|error| format!("cannot inspect {path}: {error}"))
    }

    pub(crate) fn list_dir_shallow(&self, path: &str) -> Result<Vec<FileManagerEntry>, String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        self.refresh_service_peer_targets(&mut service);
        service
            .posix_list_dir_distributed(&path)
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|entry| FileManagerEntry {
                        name: entry.name,
                        path: entry.path,
                        kind: sifs_entry_kind(&entry.kind),
                        size: None,
                    })
                    .collect()
            })
            .map_err(|error| format!("cannot list {path}: {error}"))
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
                    self.refresh_service_peer_targets(&mut service);
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
        self.write_file_with_options(path, bytes, false, false, true, None)
    }

    pub(crate) fn replace_file_if_version(
        &self,
        path: &str,
        expected_object_id: &str,
        expected_version: &str,
        bytes: &[u8],
    ) -> Result<sinomo_runtime::api::DaemonDfsBrowserReplaceFileIfVersionOutcome, String> {
        use sinomo_runtime::api::DaemonDfsBrowserReplaceFileIfVersionOutcome as Outcome;

        let path = match normalize_sifs_path(path) {
            Ok(path) => path,
            Err(detail) => return Ok(Outcome::Rejected { detail }),
        };
        let object_id = match sinomo_runtime::dfs::DfsObjectId::new(expected_object_id) {
            Ok(object_id) => object_id,
            Err(error) => {
                return Ok(Outcome::Rejected {
                    detail: format!("invalid expected SIFS object id: {error}"),
                });
            }
        };
        let version = match sinomo_runtime::dfs::DfsVersion::new(expected_version) {
            Ok(version) => version,
            Err(error) => {
                return Ok(Outcome::Rejected {
                    detail: format!("invalid expected SIFS version: {error}"),
                });
            }
        };
        let mut service = self.lock_service()?;
        self.refresh_service_peer_targets(&mut service);
        let options = sifs_write_options(&service, &self.replica_candidates);
        let stat = match service
            .posix_replace_file_if_version(&path, &object_id, &version, bytes, options)
        {
            Ok(stat) => stat,
            Err(error) => return Ok(sifs_conditional_write_error(error)),
        };
        drop(service);
        let maintenance_warning = self
            .publish_metadata_after_conditional_mutation_warning()
            .map(|error| {
                format!(
                    "the file version committed, but follow-up DFS maintenance needs retry: {error}"
                )
            });
        Ok(Outcome::Committed {
            receipt: sifs_write_receipt(&stat),
            maintenance_warning,
        })
    }

    pub(crate) fn write_file_with_deadline(
        &self,
        path: &str,
        bytes: &[u8],
        deadline: Instant,
    ) -> Result<(), String> {
        self.write_file_with_options(path, bytes, false, false, true, Some(deadline))
    }

    pub(crate) fn write_file_wait_for_commit_with_deadline(
        &self,
        path: &str,
        bytes: &[u8],
        deadline: Instant,
    ) -> Result<(), String> {
        self.write_file_with_options(path, bytes, false, true, true, Some(deadline))
    }

    pub(crate) fn write_file_with_foreground_replica_repair_deadline(
        &self,
        path: &str,
        bytes: &[u8],
        deadline: Instant,
    ) -> Result<(), String> {
        self.write_file_with_options(path, bytes, true, true, true, Some(deadline))
    }

    pub(crate) fn rename_with_deadline(
        &self,
        from: &str,
        to: &str,
        deadline: Instant,
    ) -> Result<(), String> {
        self.rename_with_operation_deadline(from, to, Some(deadline))
    }

    pub(crate) fn delete_file_with_deadline(
        &self,
        path: &str,
        deadline: Instant,
    ) -> Result<(), String> {
        self.delete_file_with_operation_deadline(path, Some(deadline))
    }

    pub(crate) fn remove_with_deadline(&self, path: &str, deadline: Instant) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        self.run_namespace_mutation_with_deadline(
            Some(deadline),
            "SIFS path remove",
            |service| match service.posix_unlink(&path) {
                Ok(_) => Ok(()),
                Err(DfsPosixError::IsDirectory { .. }) => service
                    .posix_rmdir(&path)
                    .map(|_| ())
                    .map_err(|error| {
                        format!(
                            "cannot delete SIFS directory {path}: {error}; only empty directories can be deleted"
                        )
                    }),
                Err(error) => Err(format!("cannot delete SIFS file {path}: {error}")),
            },
        )
    }

    fn run_namespace_mutation_with_deadline(
        &self,
        operation_deadline: Option<Instant>,
        operation: &str,
        mutation: impl FnOnce(&mut NetworkedDfsService<DfsNodeTransport>) -> Result<(), String>,
    ) -> Result<(), String> {
        sifs_operation_remaining(operation_deadline, operation)?;
        let mut service = self.lock_service_with_deadline(operation_deadline)?;
        let previous_deadline = operation_deadline
            .map(|deadline| service.set_transport_operation_deadline(Some(deadline)));
        let result = (|| {
            self.refresh_service_peer_targets_with_deadline(&mut service, operation_deadline);
            sifs_operation_remaining(operation_deadline, "refreshing SIFS peer targets")?;
            mutation(&mut service)
        })();
        if let Some(previous_deadline) = previous_deadline {
            service.set_transport_operation_deadline(previous_deadline);
        }
        result?;
        drop(service);
        self.publish_metadata_after_mutation_with_deadline(operation_deadline)
    }

    fn rename_with_operation_deadline(
        &self,
        from: &str,
        to: &str,
        operation_deadline: Option<Instant>,
    ) -> Result<(), String> {
        let from = normalize_sifs_path(from)?;
        let to = normalize_sifs_path(to)?;
        self.run_namespace_mutation_with_deadline(operation_deadline, "SIFS rename", |service| {
            service
                .posix_rename(&from, &to)
                .map(|_| ())
                .map_err(|error| format!("cannot rename SIFS item {from} to {to}: {error}"))
        })
    }

    fn delete_file_with_operation_deadline(
        &self,
        path: &str,
        operation_deadline: Option<Instant>,
    ) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        self.run_namespace_mutation_with_deadline(
            operation_deadline,
            "SIFS file delete",
            |service| {
                service
                    .posix_unlink(&path)
                    .map(|_| ())
                    .map_err(|error| format!("cannot delete SIFS file {path}: {error}"))
            },
        )
    }

    pub(crate) fn create_dir_all_deferred_publish_until(
        &self,
        path: &str,
        deadline: Instant,
    ) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        let operation_deadline = Some(deadline);
        sifs_operation_remaining(operation_deadline, "SIFS directory create")?;
        let mut service = self.lock_service_with_deadline(operation_deadline)?;
        let previous_deadline = service.set_transport_operation_deadline(Some(deadline));
        let result = (|| {
            self.refresh_service_peer_targets_with_deadline(&mut service, operation_deadline);
            sifs_operation_remaining(operation_deadline, "refreshing SIFS peer targets")?;
            service
                .posix_mkdir_all(&path)
                .map(|_| ())
                .map_err(|error| format!("cannot create SIFS directory {path}: {error}"))
        })();
        service.set_transport_operation_deadline(previous_deadline);
        result
    }

    fn write_file_with_options(
        &self,
        path: &str,
        bytes: &[u8],
        foreground_replica_repair: bool,
        wait_for_commit: bool,
        publish_metadata: bool,
        operation_deadline: Option<Instant>,
    ) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        sifs_operation_remaining(operation_deadline, "SIFS write")?;
        let mut service = self.lock_service_with_deadline(operation_deadline)?;
        let previous_deadline = operation_deadline
            .map(|deadline| service.set_transport_operation_deadline(Some(deadline)));
        let result = (|| {
            self.refresh_service_peer_targets_with_deadline(&mut service, operation_deadline);
            sifs_operation_remaining(operation_deadline, "refreshing SIFS peer targets")?;
            let mut options = if wait_for_commit {
                sifs_write_options(&service, &self.replica_candidates)
            } else {
                sifs_deferred_publish_write_options(&service, &self.replica_candidates)
            };
            options.foreground_replica_repair = foreground_replica_repair;
            if foreground_replica_repair {
                if let Some(parent) = parent_sifs_path(&path) {
                    service.posix_mkdir_all(&parent).map_err(|error| {
                        format!("cannot create SIFS directory {parent}: {error}")
                    })?;
                }
                service
                    .posix_write_file(&path, bytes, options)
                    .map(|_| ())
                    .map_err(|error| format!("cannot write SIFS file {path}: {error}"))
            } else {
                if let Some(parent) = parent_sifs_path(&path) {
                    ensure_sifs_parent_directory_for_write(&mut service, &parent)?;
                }
                match service.posix_write_file(&path, bytes, options.clone()) {
                    Ok(_) => Ok(()),
                    Err(error) if sifs_write_error_needs_parent_create(&error) => {
                        if let Some(parent) = parent_sifs_path(&path) {
                            service.posix_mkdir_all(&parent).map_err(|error| {
                                format!("cannot create SIFS directory {parent}: {error}")
                            })?;
                        }
                        service
                            .posix_write_file(&path, bytes, options)
                            .map(|_| ())
                            .map_err(|error| format!("cannot write SIFS file {path}: {error}"))
                    }
                    Err(error) => Err(format!("cannot write SIFS file {path}: {error}")),
                }
            }
        })();
        if let Some(previous_deadline) = previous_deadline {
            service.set_transport_operation_deadline(previous_deadline);
        }
        result?;
        drop(service);
        if publish_metadata {
            self.publish_metadata_after_mutation_with_deadline(operation_deadline)?;
        }
        Ok(())
    }

    pub(crate) fn write_files_with_known_parents_deferred_publish_until(
        &self,
        files: &[(String, Vec<u8>)],
        deadline: Instant,
    ) -> Result<(), String> {
        self.write_files_with_known_parents_deferred_publish_with_deadline(files, Some(deadline))
    }

    fn write_files_with_known_parents_deferred_publish_with_deadline(
        &self,
        files: &[(String, Vec<u8>)],
        operation_deadline: Option<Instant>,
    ) -> Result<(), String> {
        let normalized = files
            .iter()
            .map(|(path, bytes)| normalize_sifs_path(path).map(|path| (path, bytes.as_slice())))
            .collect::<Result<Vec<_>, _>>()?;
        sifs_operation_remaining(operation_deadline, "SIFS bulk write")?;
        let mut service = self.lock_service_with_deadline(operation_deadline)?;
        let previous_deadline = operation_deadline
            .map(|deadline| service.set_transport_operation_deadline(Some(deadline)));
        let result = (|| {
            self.refresh_service_peer_targets_with_deadline(&mut service, operation_deadline);
            sifs_operation_remaining(operation_deadline, "refreshing SIFS peer targets")?;
            service
                .posix_create_files_local_eventual_with_known_parents(
                    normalized
                        .iter()
                        .map(|(path, bytes)| (path.as_str(), *bytes)),
                )
                .map(|_| ())
                .map_err(|error| format!("cannot write SIFS bulk batch: {error}"))
        })();
        if let Some(previous_deadline) = previous_deadline {
            service.set_transport_operation_deadline(previous_deadline);
        }
        result
    }

    pub(crate) fn write_file_with_known_parent_deferred_publish_until(
        &self,
        path: &str,
        bytes: &[u8],
        deadline: Instant,
    ) -> Result<(), String> {
        self.write_file_with_known_parent_deferred_publish_with_deadline(
            path,
            bytes,
            Some(deadline),
        )
    }

    pub(crate) fn write_file_with_known_parent_no_publish_until(
        &self,
        path: &str,
        bytes: &[u8],
        deadline: Instant,
    ) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        let operation_deadline = Some(deadline);
        sifs_operation_remaining(operation_deadline, "SIFS known-parent write")?;
        let mut service = self.lock_service_with_deadline(operation_deadline)?;
        let previous_deadline = service.set_transport_operation_deadline(Some(deadline));
        let result = (|| {
            self.refresh_service_peer_targets_with_deadline(&mut service, operation_deadline);
            sifs_operation_remaining(operation_deadline, "refreshing SIFS peer targets")?;
            let options = sifs_write_options(&service, &self.replica_candidates);
            service
                .posix_write_file_with_known_parent(&path, bytes, options)
                .map(|_| ())
                .map_err(|error| format!("cannot write SIFS file {path}: {error}"))
        })();
        service.set_transport_operation_deadline(previous_deadline);
        result
    }

    fn write_file_with_known_parent_deferred_publish_with_deadline(
        &self,
        path: &str,
        bytes: &[u8],
        operation_deadline: Option<Instant>,
    ) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        sifs_operation_remaining(operation_deadline, "SIFS deferred known-parent write")?;
        let mut service = self.lock_service_with_deadline(operation_deadline)?;
        let previous_deadline = operation_deadline
            .map(|deadline| service.set_transport_operation_deadline(Some(deadline)));
        let result = (|| {
            self.refresh_service_peer_targets_with_deadline(&mut service, operation_deadline);
            sifs_operation_remaining(operation_deadline, "refreshing SIFS peer targets")?;
            let options = sifs_deferred_publish_write_options(&service, &self.replica_candidates);
            service
                .posix_write_file_with_known_parent_deferred_local_replay(&path, bytes, options)
                .map(|_| ())
                .map_err(|error| format!("cannot write SIFS file {path}: {error}"))
        })();
        if let Some(previous_deadline) = previous_deadline {
            service.set_transport_operation_deadline(previous_deadline);
        }
        result
    }

    pub(crate) fn operational_metrics(
        &self,
    ) -> Result<sinomo_runtime::dfs::DfsOperationalMetrics, String> {
        let service = self.lock_service()?;
        service
            .operational_metrics()
            .map_err(|error| format!("cannot read SIFS operational metrics: {error}"))
    }

    pub(crate) fn operator_status(
        &self,
    ) -> Result<Option<sinomo_runtime::dfs::DfsOperatorStatus>, String> {
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        if !cluster_paths.state.exists() {
            return Ok(None);
        }
        sinomo_runtime::dfs::inspect_cluster_aware_dfs_status(&self.data_dir, &cluster_paths)
            .map(Some)
            .map_err(|error| format!("cannot read SIFS operator status: {error}"))
    }

    pub(crate) fn local_pins_for_path(
        &self,
        file_system_id: &sinomo_runtime::dfs::DfsFileSystemId,
        path: &str,
    ) -> Result<Vec<sinomo_runtime::dfs::DfsPinRecord>, String> {
        let normalized_path = normalize_sifs_path(path)?;
        sinomo_runtime::dfs::list_local_dfs_pins(
            default_dfs_pin_root(&self.data_dir),
            Some(file_system_id),
        )
        .map(|pins| {
            pins.into_iter()
                .filter(|pin| {
                    normalize_sifs_path(&pin.path).is_ok_and(|path| path == normalized_path)
                })
                .collect()
        })
        .map_err(|error| format!("cannot read SIFS content pins: {error}"))
    }

    pub(crate) fn metadata_shard_for_path(
        &self,
        path: &str,
    ) -> Result<Option<DfsMetadataShardConfig>, String> {
        let path = normalize_sifs_path(path)?;
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        if !cluster_paths.state.exists() {
            return Ok(None);
        }
        refresh_sifs_cluster_kv_snapshot(&self.data_dir, &cluster_paths)?;
        let mut kv = SifsControlPlaneKvClient::new(&self.data_dir, &cluster_paths);
        let store = DfsControlPlaneStore::new(&mut kv);
        store
            .select_metadata_shard_for_path_strict_default_sifs_split(&sifs_file_system_id(), &path)
            .map_err(|error| format!("cannot read SIFS metadata shard for {path}: {error}"))
    }

    pub(crate) fn transfer_metadata_primary_to_local(
        &self,
        path: &str,
        expected_epoch: Option<u64>,
    ) -> Result<SifsMetadataPrimaryTransferReport, String> {
        let path = normalize_sifs_path(path)?;
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        if !cluster_paths.state.exists() {
            return Err("SIFS metadata primary transfer requires a cluster".to_string());
        }
        refresh_sifs_cluster_kv_snapshot(&self.data_dir, &cluster_paths)?;
        let state = cluster::read_cluster_state(&cluster_paths)
            .map_err(|error| format!("cannot read local cluster state: {error}"))?;
        let local_node_id = state.local_node_id;
        let mut kv = SifsControlPlaneKvClient::new(&self.data_dir, &cluster_paths);
        let mut store = DfsControlPlaneStore::new(&mut kv);
        let previous = store
            .select_metadata_shard_for_path_strict_default_sifs_split(&sifs_file_system_id(), &path)
            .map_err(|error| format!("cannot read SIFS metadata shard for {path}: {error}"))?
            .ok_or_else(|| format!("no SIFS metadata shard controls {path}"))?;
        if let Some(expected_epoch) = expected_epoch {
            if previous.epoch != expected_epoch {
                return Err(format!(
                    "SIFS metadata shard {} epoch is {}, expected {}",
                    previous.shard_id, previous.epoch, expected_epoch
                ));
            }
        }
        if previous.primary_node_id == local_node_id {
            return Ok(SifsMetadataPrimaryTransferReport {
                file_system_id: previous.file_system_id.to_string(),
                shard_id: previous.shard_id.clone(),
                previous_primary_node_id: previous.primary_node_id.to_string(),
                new_primary_node_id: previous.primary_node_id.to_string(),
                previous_epoch: previous.epoch,
                new_epoch: previous.epoch,
                changed: false,
                shard: previous,
                local_node_id: local_node_id.to_string(),
            });
        }
        if !previous
            .replica_node_ids
            .iter()
            .any(|node_id| node_id == &local_node_id)
        {
            return Err(format!(
                "local node {} is not a metadata replica for SIFS shard {}",
                local_node_id, previous.shard_id
            ));
        }
        let expected_epoch = previous.epoch;
        let updated = store
            .transfer_metadata_shard_primary(
                &previous.file_system_id,
                &previous.shard_id,
                expected_epoch,
                local_node_id.clone(),
            )
            .map_err(|error| {
                format!(
                    "cannot transfer SIFS metadata shard {} primary: {error}",
                    previous.shard_id
                )
            })?
            .ok_or_else(|| {
                format!(
                    "SIFS metadata shard {} changed while transferring primary; refresh placement and retry",
                    previous.shard_id
                )
            })?;
        if let Ok(mut service) = self.lock_service() {
            service.invalidate_sifs_metadata_shard_cache();
        }
        Ok(SifsMetadataPrimaryTransferReport {
            file_system_id: previous.file_system_id.to_string(),
            shard_id: previous.shard_id,
            previous_primary_node_id: previous.primary_node_id.to_string(),
            new_primary_node_id: updated.primary_node_id.to_string(),
            previous_epoch: expected_epoch,
            new_epoch: updated.epoch,
            changed: true,
            shard: updated,
            local_node_id: local_node_id.to_string(),
        })
    }

    pub(crate) fn create_dir_all(&self, path: &str) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        self.refresh_service_peer_targets(&mut service);
        service
            .posix_mkdir_all(&path)
            .map_err(|error| format!("cannot create SIFS directory {path}: {error}"))?;
        drop(service);
        self.publish_metadata_after_mutation()?;
        Ok(())
    }

    /// Provision a private POSIX directory through the authoritative DFS
    /// service rather than through a FUSE mount whose parent may not be
    /// writable by the local kernel identity. Existing directories are only
    /// accepted when their ownership and mode already match; roaming devices
    /// must never silently steal one another's session home.
    pub(crate) fn ensure_posix_directory_with_attrs(
        &self,
        path: &str,
        uid: u32,
        gid: u32,
        mode: u32,
    ) -> Result<DfsStat, String> {
        let path = normalize_sifs_path(path)?;
        let mode = mode & 0o7777;
        let mut service = self.lock_service()?;
        self.refresh_service_peer_targets(&mut service);
        let existing = match service.posix_lstat_distributed(&path) {
            Ok(stat) => Some(stat),
            Err(DfsPosixError::NotFound { .. } | DfsPosixError::MissingParent { .. }) => None,
            Err(error) => return Err(format!("cannot inspect SIFS directory {path}: {error}")),
        };
        if let Some(stat) = existing {
            validate_posix_directory_attrs(&path, &stat, uid, gid, mode)?;
            return Ok(stat);
        }

        let parent = sifs_parent_path(&path).ok_or_else(|| {
            format!("cannot provision POSIX attributes on the SIFS root directory {path}")
        })?;
        service
            .posix_mkdir_all(&parent)
            .map_err(|error| format!("cannot create SIFS directory parent {parent}: {error}"))?;
        let attrs = DfsPosixCreateAttrs::new(mode, uid, gid);
        let stat = match service.posix_mkdir_with_create_attrs(&path, attrs) {
            Ok(stat) => stat,
            Err(DfsPosixError::AlreadyExists { .. }) => service
                .posix_lstat_distributed(&path)
                .map_err(|error| format!("cannot inspect raced SIFS directory {path}: {error}"))?,
            Err(error) => {
                return Err(format!("cannot create SIFS directory {path}: {error}"));
            }
        };
        drop(service);
        self.publish_metadata_after_mutation()?;
        validate_posix_directory_attrs(&path, &stat, uid, gid, mode)?;
        Ok(stat)
    }

    pub(crate) fn create_dir_all_deferred_local_replay_until(
        &self,
        path: &str,
        deadline: Instant,
    ) -> Result<(), String> {
        self.create_dir_all_deferred_local_replay_with_deadline(path, Some(deadline))
    }

    fn create_dir_all_deferred_local_replay_with_deadline(
        &self,
        path: &str,
        operation_deadline: Option<Instant>,
    ) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        self.refresh_service_peer_targets(&mut service);
        let previous_deadline = operation_deadline
            .map(|deadline| service.set_transport_operation_deadline(Some(deadline)));
        let result = service
            .posix_mkdir_all_without_local_replay(&path)
            .map_err(|error| format!("cannot create SIFS directory {path}: {error}"));
        if let Some(previous_deadline) = previous_deadline {
            service.set_transport_operation_deadline(previous_deadline);
        }
        result
    }

    pub(crate) fn pin_content(&self, path: &str) -> Result<String, String> {
        self.pin_path_mode(path, DfsPinMode::Content, "content")
    }

    pub(crate) fn pin_metadata(&self, path: &str) -> Result<String, String> {
        self.pin_path_mode(path, DfsPinMode::Metadata, "metadata")
    }

    fn pin_path_mode(&self, path: &str, mode: DfsPinMode, label: &str) -> Result<String, String> {
        let stat = self.dfs_stat(path)?;
        let node_id = self.node_label();
        let node = NodeId::new(&node_id).map_err(|error| {
            format!("cannot pin SIFS {label} on invalid local node id {node_id}: {error}")
        })?;
        sinomo_runtime::dfs::add_local_dfs_pin(
            default_dfs_pin_root(&self.data_dir),
            DfsPinAddRequest {
                file_system_id: stat.file_system_id,
                path: stat.path,
                object_id: Some(stat.object_id),
                pinned_by_node_id: node,
                mode,
                created_at: time::OffsetDateTime::now_utc(),
            },
        )
        .map_err(|error| format!("cannot pin SIFS {label} {path}: {error}"))?;
        Ok(node_id)
    }

    pub(crate) fn unpin_content(&self, path: &str) -> Result<bool, String> {
        self.unpin_path_mode(path, DfsPinMode::Content, "content")
    }

    pub(crate) fn unpin_metadata(&self, path: &str) -> Result<bool, String> {
        self.unpin_path_mode(path, DfsPinMode::Metadata, "metadata")
    }

    fn unpin_path_mode(&self, path: &str, mode: DfsPinMode, label: &str) -> Result<bool, String> {
        let stat = self.dfs_stat(path)?;
        let node_id = self.node_label();
        let node = NodeId::new(&node_id).map_err(|error| {
            format!("cannot unpin SIFS {label} on invalid local node id {node_id}: {error}")
        })?;
        sinomo_runtime::dfs::remove_local_dfs_pin(
            default_dfs_pin_root(&self.data_dir),
            &stat.file_system_id,
            &stat.path,
            Some(&node),
            Some(&mode),
        )
        .map_err(|error| format!("cannot unpin SIFS {label} {path}: {error}"))
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
        self.refresh_service_peer_targets(&mut service);
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
        self.refresh_service_peer_targets(&mut service);
        let entries = service
            .posix_list_dir_distributed(&path)
            .map_err(|error| format!("cannot list {path}: {error}"))?;
        let mut results = Vec::new();
        for entry in entries {
            results.push(FileManagerEntry {
                name: entry.name,
                path: entry.path,
                kind: sifs_entry_kind(&entry.kind),
                size: None,
            });
        }
        Ok(results)
    }

    fn read_file_prefix(&self, path: &str, max_bytes: u64) -> Result<FileManagerRead, String> {
        let path = normalize_sifs_path(path)?;
        let max_bytes = usize::try_from(max_bytes)
            .map_err(|_| format!("cannot read {path}: prefix length does not fit this platform"))?;
        let mut service = self.lock_service()?;
        self.refresh_service_peer_targets(&mut service);
        let read = service
            .posix_read_file_prefix_distributed(&path, max_bytes)
            .map_err(|error| format!("cannot read {path}: {error}"))?;
        Ok(FileManagerRead {
            bytes: read.bytes,
            truncated: read.truncated,
        })
    }

    fn directory_entry_count(&self, path: &str) -> Result<usize, String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        self.refresh_service_peer_targets(&mut service);
        service
            .posix_list_dir_distributed(&path)
            .map(|entries| entries.len())
            .map_err(|error| error.to_string())
    }

    fn eager_preview_enabled(&self) -> bool {
        false
    }

    fn create_file(&self, path: &str) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        self.refresh_service_peer_targets(&mut service);
        let options = sifs_deferred_publish_write_options(&service, &self.replica_candidates);
        service
            .posix_write_file(&path, &[], options)
            .map(|_| ())
            .map_err(|error| format!("cannot create SIFS file {path}: {error}"))?;
        drop(service);
        self.publish_metadata_after_mutation()?;
        Ok(())
    }

    fn create_dir(&self, path: &str) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        self.refresh_service_peer_targets(&mut service);
        service
            .posix_mkdir(&path)
            .map(|_| ())
            .map_err(|error| format!("cannot create SIFS directory {path}: {error}"))?;
        drop(service);
        self.publish_metadata_after_mutation()?;
        Ok(())
    }

    fn rename(&self, from: &str, to: &str) -> Result<(), String> {
        self.rename_with_operation_deadline(from, to, None)
    }

    fn delete_file(&self, path: &str) -> Result<(), String> {
        self.delete_file_with_operation_deadline(path, None)
    }

    fn delete_dir(&self, path: &str) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        let mut service = self.lock_service()?;
        self.refresh_service_peer_targets(&mut service);
        service
            .posix_rmdir(&path)
            .map(|_| ())
            .map_err(|error| {
                format!(
                    "cannot delete SIFS directory {path}: {error}; only empty directories can be deleted"
                )
            })?;
        drop(service);
        self.publish_metadata_after_mutation()?;
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
    /// The process-wide SIFS RPC runtime.
    ///
    /// A single shared runtime — never dropped — instead of one per backend.
    /// Dropping a tokio runtime blocks until every started `spawn_blocking`
    /// task finishes, and this runtime's blocking tasks probe the daemon and
    /// forward object RPCs to cluster peers, which can stall for the full
    /// network timeout. Per-open runtimes therefore wedged whichever thread
    /// dropped the last handle (the app on chat persist/list churn, the test
    /// harness at teardown) and stranded 16 `sinomo-sifs-rpc` workers each
    /// time. Sharing also caps thread churn: chat session-list refreshes and
    /// persists open SIFS stores constantly and were paying a 16-thread
    /// runtime build/teardown per call.
    fn new() -> Result<Self, String> {
        static SHARED: std::sync::OnceLock<Result<Arc<tokio::runtime::Runtime>, String>> =
            std::sync::OnceLock::new();
        SHARED
            .get_or_init(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(sifs_rpc_runtime_worker_threads())
                    .thread_name("sinomo-sifs-rpc")
                    .enable_all()
                    .build()
                    .map(Arc::new)
                    .map_err(|error| format!("cannot create SIFS RPC runtime: {error}"))
            })
            .clone()
            .map(|runtime| Self { runtime })
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
    sifs_parallel_read_worker_count_for(requested_workers, configured.min(MAX_WORKERS))
}

fn sifs_parallel_read_worker_count_for(
    requested_workers: usize,
    configured_workers: usize,
) -> usize {
    const TARGET_PATHS_PER_WORKER: usize = 16;
    if requested_workers == 0 {
        return 1;
    }
    let batch_limited_workers = requested_workers.div_ceil(TARGET_PATHS_PER_WORKER).max(1);
    configured_workers
        .max(1)
        .min(requested_workers)
        .min(batch_limited_workers)
}

fn open_sifs_service_read_only(
    data_dir: &SinomoDataDir,
    rpc_runtime: &SifsRpcRuntime,
) -> Result<NetworkedDfsService<DfsNodeTransport>, String> {
    open_sifs_service_with_hydration(data_dir, rpc_runtime, false)
}

fn open_sifs_service_with_hydration(
    data_dir: &SinomoDataDir,
    rpc_runtime: &SifsRpcRuntime,
    hydrate_metadata: bool,
) -> Result<NetworkedDfsService<DfsNodeTransport>, String> {
    open_sifs_service_with_hydration_until(data_dir, rpc_runtime, hydrate_metadata, None)
}

fn open_sifs_service_with_hydration_until(
    data_dir: &SinomoDataDir,
    rpc_runtime: &SifsRpcRuntime,
    hydrate_metadata: bool,
    operation_deadline: Option<Instant>,
) -> Result<NetworkedDfsService<DfsNodeTransport>, String> {
    sifs_operation_remaining(operation_deadline, "preparing the Sinomo data directory")?;
    data_dir
        .ensure_root_layout()
        .map_err(|error| format!("cannot prepare Sinomo data directory: {error}"))?;
    sifs_operation_remaining(operation_deadline, "preparing the Sinomo data directory")?;
    let cluster_paths = ClusterPaths::new(data_dir);
    // This transport is scoped to backend opening. Its absolute deadline must
    // not be retained by the service, otherwise a later read inherits the
    // nearly-expired open budget instead of its own RPC timeout.
    let bootstrap_peer_transport: Arc<dyn DfsPeerTransport> = Arc::new(
        SifsDfsPeerTransport::new(rpc_runtime.clone(), data_dir.clone())
            .with_operation_deadline(operation_deadline),
    );
    let _ = refresh_sifs_cluster_kv_snapshot(data_dir, &cluster_paths);
    sifs_operation_remaining(operation_deadline, "refreshing the local cluster snapshot")?;
    let _ = refresh_cluster_dfs_kv_snapshot_with_peer_transport(
        &cluster_paths,
        bootstrap_peer_transport,
    );
    sifs_operation_remaining(operation_deadline, "refreshing the DFS cluster snapshot")?;
    let (local_node_id, peer_node_ids) = if cluster_paths.state.exists() {
        let state = cluster::read_cluster_state_without_kv(&cluster_paths)
            .map_err(|error| format!("cannot read local cluster state: {error}"))?;
        let peer_node_ids = cluster_dfs_peer_node_ids(&state);
        (state.local_node_id, peer_node_ids)
    } else {
        (fallback_dfs_node_id(), Vec::new())
    };
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))
        .map_err(|error| format!("cannot open DFS content store: {error}"))?;
    sifs_operation_remaining(operation_deadline, "opening the DFS content store")?;
    // Both daemon-backed and direct routing use the same canonical call/result
    // contract, preserving typed response values and explicit attachments.
    let (sync_peer_transport, async_peer_transport) =
        match sinomo_runtime::api::DaemonDfsPeerTransport::connect(data_dir.clone()) {
            Ok(peer_transport) => shared_sifs_peer_transports(peer_transport),
            Err(_) => shared_sifs_peer_transports(SifsDfsPeerTransport::new(
                rpc_runtime.clone(),
                data_dir.clone(),
            )),
        };
    let mut service = NetworkedDfsService::new(
        DfsNodeTransport::with_peer_transports_for_nodes(
            local_node_id,
            peer_node_ids,
            sinomo_runtime::network::DEFAULT_NETWORK_TIMEOUT,
            sync_peer_transport,
            async_peer_transport,
        ),
        content,
    );
    if cluster_paths.state.exists() {
        service =
            service.with_control_plane(SifsControlPlaneKvClient::new(data_dir, &cluster_paths));
    }
    sifs_operation_remaining(operation_deadline, "opening DFS metadata shards")?;
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
    sifs_operation_remaining(operation_deadline, "opening DFS metadata shards")?;
    let mut service = service;
    let previous_deadline =
        operation_deadline.map(|deadline| service.set_transport_operation_deadline(Some(deadline)));
    let bootstrap = service
        .bootstrap_distributed_sifs()
        .map_err(|error| format!("cannot bootstrap SIFS namespace: {error}"));
    if let Some(previous_deadline) = previous_deadline {
        service.set_transport_operation_deadline(previous_deadline);
    }
    bootstrap?;
    sifs_operation_remaining(operation_deadline, "bootstrapping the SIFS namespace")?;
    Ok(service)
}

fn shared_sifs_peer_transports<T>(
    peer_transport: T,
) -> (Arc<dyn DfsPeerTransport>, Arc<dyn AsyncDfsPeerTransport>)
where
    T: DfsPeerTransport + AsyncDfsPeerTransport + 'static,
{
    let peer_transport = Arc::new(peer_transport);
    let sync_peer_transport: Arc<dyn DfsPeerTransport> = peer_transport.clone();
    let async_peer_transport: Arc<dyn AsyncDfsPeerTransport> = peer_transport;
    (sync_peer_transport, async_peer_transport)
}

/// Refresh the cluster KV snapshot under `cluster_paths` from the daemon that
/// owns `data_dir`. Scoped on purpose: probing the env/home default daemon
/// from a backend opened on another data directory imported the developer's
/// live cluster KV (real peer targets included) into that directory — tests
/// then talked to the real cluster.
fn refresh_sifs_cluster_kv_snapshot(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<(), String> {
    if cluster_paths.state.exists() {
        refresh_cluster_kv_from_local_daemon_for_data_dir(data_dir, cluster_paths)
            .map_err(|error| format!("cannot refresh SIFS cluster KV snapshot: {error}"))?;
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct SifsControlPlaneKvClient {
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
}

impl SifsControlPlaneKvClient {
    fn new(data_dir: &SinomoDataDir, cluster_paths: &ClusterPaths) -> Self {
        Self {
            data_dir: data_dir.clone(),
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

impl DfsControlPlaneKvClient for SifsControlPlaneKvClient {
    fn get(&self, key: &str) -> Result<Option<sinomo_runtime::kv::KvEntry>, KvError> {
        let entry = cluster::read_cluster_kv_latest_observed_entry(&self.cluster_paths, key)
            .map_err(|source| self.kv_error(source))?;
        if entry.is_some() {
            return Ok(entry);
        }
        refresh_sifs_cluster_kv_snapshot(&self.data_dir, &self.cluster_paths)
            .map_err(|source| self.kv_error(source))?;
        cluster::read_cluster_kv_latest_observed_entry(&self.cluster_paths, key)
            .map_err(|source| self.kv_error(source))
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<sinomo_runtime::kv::KvEntry>, KvError> {
        let entries =
            cluster::read_cluster_kv_latest_observed_prefix_entries(&self.cluster_paths, prefix)
                .map_err(|source| self.kv_error(source))?;
        if !entries.is_empty() {
            return Ok(entries);
        }
        refresh_sifs_cluster_kv_snapshot(&self.data_dir, &self.cluster_paths)
            .map_err(|source| self.kv_error(source))?;
        cluster::read_cluster_kv_latest_observed_prefix_entries(&self.cluster_paths, prefix)
            .map_err(|source| self.kv_error(source))
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
        let mut state = cluster::read_cluster_state(&self.cluster_paths)
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
    let entries = cluster::read_cluster_kv_latest_observed_prefix_entries(
        cluster_paths,
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
    let state = cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(|error| format!("cannot read local cluster state: {error}"))?;
    open_local_sharded_metadata_backend_for_replica_repair(
        default_dfs_metadata_shards_root(data_dir),
        shards,
        &state.local_node_id,
    )
    .map(Some)
    .map_err(|error| format!("cannot open DFS metadata shards: {error}"))
}

fn sifs_replica_candidates(data_dir: &SinomoDataDir, local_node_id: &NodeId) -> Vec<NodeId> {
    let mut candidates = vec![local_node_id.clone()];
    let cluster_paths = ClusterPaths::new(data_dir);
    if !cluster_paths.state.exists() {
        return candidates;
    }
    let Ok(state) = cluster::read_cluster_state_without_kv(&cluster_paths) else {
        return candidates;
    };
    let routable_node_ids = cluster_dfs_peer_node_ids(&state)
        .into_iter()
        .collect::<BTreeSet<_>>();
    let graph = network::ClusterConnectivityGraph::read_for_local(&cluster_paths, &state);
    let Ok(entries) = cluster::read_cluster_kv_latest_observed_prefix_entries(
        &cluster_paths,
        content_services_prefix(),
    ) else {
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

#[derive(Clone, Debug)]
struct SifsDfsPeerTransport {
    rpc_runtime: SifsRpcRuntime,
    data_dir: SinomoDataDir,
    operation_deadline: Option<Instant>,
}

impl SifsDfsPeerTransport {
    fn new(rpc_runtime: SifsRpcRuntime, data_dir: SinomoDataDir) -> Self {
        Self {
            rpc_runtime,
            data_dir,
            operation_deadline: None,
        }
    }

    fn with_operation_deadline(mut self, operation_deadline: Option<Instant>) -> Self {
        self.operation_deadline = operation_deadline;
        self
    }
}

impl DfsPeerTransport for SifsDfsPeerTransport {
    fn call_object_rpc(
        &self,
        target_node_id: &NodeId,
        call: network::ObjectRpcSessionCall,
        options: DfsObjectRpcSendOptions,
    ) -> Result<network::ObjectRpcSessionResult, DfsPeerTransportError> {
        let operation_deadline =
            sifs_rpc_effective_operation_deadline(&options, self.operation_deadline);
        let options = sifs_rpc_options_with_operation_deadline(options, operation_deadline)?;
        sifs_send_dfs_object_rpc(
            &self.rpc_runtime,
            &self.data_dir,
            target_node_id,
            call,
            options,
            operation_deadline,
        )
    }
}

impl AsyncDfsPeerTransport for SifsDfsPeerTransport {
    fn call_object_rpc_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        call: network::ObjectRpcSessionCall,
        options: DfsObjectRpcSendOptions,
    ) -> DfsPeerTransportFuture<'a, Result<network::ObjectRpcSessionResult, DfsPeerTransportError>>
    {
        let operation_deadline =
            sifs_rpc_effective_operation_deadline(&options, self.operation_deadline);
        let options = match sifs_rpc_options_with_operation_deadline(options, operation_deadline) {
            Ok(options) => options,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let handle = self.rpc_runtime.runtime.handle().clone();
        let target_node_id = target_node_id.clone();
        let data_dir = self.data_dir.clone();
        Box::pin(async move {
            let daemon_target_node_id = target_node_id.clone();
            let daemon_runtime = handle.clone();
            let daemon_probe = tokio::task::spawn_blocking(move || {
                sifs_send_dfs_object_rpc_via_daemon(
                    &data_dir,
                    &daemon_target_node_id,
                    call,
                    options,
                    operation_deadline,
                    &daemon_runtime,
                )
            });
            // Discovery and the forwarded RPC are each bounded internally.
            // Do not add a competing outer deadline: the daemon-control client
            // intentionally waits one default network phase beyond the inner
            // route budget so it can return the terminal delivery state.
            let daemon_result = daemon_probe
                .await
                .map_err(|error| DfsPeerTransportError::new(error.to_string()))?;
            let call = match daemon_result {
                SifsDaemonRpcAttempt::Complete(result) => return result,
                SifsDaemonRpcAttempt::Unavailable(call) => call,
            };

            let options = sifs_rpc_options_with_operation_deadline(options, operation_deadline)?;
            sinomo_runtime::daemon::send_object_rpc_session_to_target_async_with_options(
                &target_node_id,
                call,
                options.cluster_rpc_options(),
                &handle,
                options.observe_connectivity,
            )
            .await
            .map_err(|error| DfsPeerTransportError::new(error.to_string()))
        })
    }
}

fn sifs_rpc_effective_operation_deadline(
    options: &DfsObjectRpcSendOptions,
    operation_deadline: Option<Instant>,
) -> Option<Instant> {
    let now = Instant::now();
    let relative_deadline = options
        .overall_timeout
        .map(|timeout| now.checked_add(timeout).unwrap_or(now));
    match (operation_deadline, relative_deadline) {
        (Some(operation), Some(relative)) => Some(operation.min(relative)),
        (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
        (None, None) => None,
    }
}

fn sifs_rpc_remaining_operation_budget(
    deadline: Instant,
    operation: &str,
) -> Result<Duration, DfsPeerTransportError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| {
            DfsPeerTransportError::new(format!(
                "SIFS operation deadline expired during {operation}"
            ))
        })
}

fn sifs_rpc_options_with_operation_deadline(
    mut options: DfsObjectRpcSendOptions,
    operation_deadline: Option<Instant>,
) -> Result<DfsObjectRpcSendOptions, DfsPeerTransportError> {
    let Some(deadline) = operation_deadline else {
        return Ok(options);
    };
    let remaining = sifs_rpc_remaining_operation_budget(deadline, "SIFS peer RPC dispatch")?;
    options.overall_timeout = Some(
        options
            .overall_timeout
            .map(|timeout| timeout.min(remaining))
            .unwrap_or(remaining),
    );
    Ok(options)
}

fn sifs_send_dfs_object_rpc(
    rpc_runtime: &SifsRpcRuntime,
    data_dir: &SinomoDataDir,
    target_node_id: &NodeId,
    call: network::ObjectRpcSessionCall,
    options: DfsObjectRpcSendOptions,
    operation_deadline: Option<Instant>,
) -> Result<network::ObjectRpcSessionResult, DfsPeerTransportError> {
    let options = sifs_rpc_options_with_operation_deadline(options, operation_deadline)?;
    let call = match sifs_send_dfs_object_rpc_via_daemon(
        data_dir,
        target_node_id,
        call,
        options,
        operation_deadline,
        rpc_runtime.runtime.handle(),
    ) {
        SifsDaemonRpcAttempt::Complete(result) => return result,
        SifsDaemonRpcAttempt::Unavailable(call) => call,
    };
    let options = sifs_rpc_options_with_operation_deadline(options, operation_deadline)?;

    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        if matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            return tokio::task::block_in_place(|| {
                handle
                    .block_on(
                        sinomo_runtime::daemon::send_object_rpc_session_to_target_async_with_options(
                            target_node_id,
                            call,
                            options.cluster_rpc_options(),
                            &handle,
                            options.observe_connectivity,
                        ),
                    )
                    .map_err(|error| DfsPeerTransportError::new(error.to_string()))
            });
        }
        return Err(DfsPeerTransportError::new(
            "SIFS DFS object RPC sync sender cannot run inside a current-thread Tokio runtime",
        ));
    }
    rpc_runtime
        .runtime
        .block_on(
            sinomo_runtime::daemon::send_object_rpc_session_to_target_async_with_options(
                target_node_id,
                call,
                options.cluster_rpc_options(),
                rpc_runtime.runtime.handle(),
                options.observe_connectivity,
            ),
        )
        .map_err(|error| DfsPeerTransportError::new(error.to_string()))
}

/// Forward a peer RPC through the daemon that owns `data_dir`, when it is
/// running. Scoped to the backend's own data directory on purpose — probing
/// the env/home default daemon routed backends opened on foreign directories
/// (tests, tooling) through the developer's live daemon and its real cluster.
enum SifsDaemonRpcAttempt {
    Unavailable(network::ObjectRpcSessionCall),
    Complete(Result<network::ObjectRpcSessionResult, DfsPeerTransportError>),
}

fn sifs_send_dfs_object_rpc_via_daemon(
    data_dir: &SinomoDataDir,
    target_node_id: &NodeId,
    call: network::ObjectRpcSessionCall,
    options: DfsObjectRpcSendOptions,
    operation_deadline: Option<Instant>,
    runtime: &tokio::runtime::Handle,
) -> SifsDaemonRpcAttempt {
    let paths = sinomo_runtime::daemon::DaemonPaths::new(data_dir);
    let client = if let Some(deadline) = operation_deadline {
        let remaining =
            match sifs_rpc_remaining_operation_budget(deadline, "local daemon discovery") {
                Ok(remaining) => remaining,
                Err(error) => return SifsDaemonRpcAttempt::Complete(Err(error)),
            };
        sinomo_runtime::daemon::DaemonApiClient::connect_for_paths_with_timeout(
            &paths,
            remaining.min(sinomo_runtime::daemon::DAEMON_API_PROBE_TIMEOUT),
        )
    } else {
        sinomo_runtime::daemon::DaemonApiClient::connect_for_paths(&paths)
    };
    let Ok(client) = client else {
        return SifsDaemonRpcAttempt::Unavailable(call);
    };
    let options = match sifs_rpc_options_with_operation_deadline(options, operation_deadline) {
        Ok(options) => options,
        Err(error) => return SifsDaemonRpcAttempt::Complete(Err(error)),
    };
    if let Some(deadline) = operation_deadline
        && let Err(error) = sifs_rpc_remaining_operation_budget(deadline, "daemon-forwarded RPC")
    {
        return SifsDaemonRpcAttempt::Complete(Err(error));
    }
    // Keep the inner routed RPC clamped to the DFS operation deadline through
    // `options`, but do not clamp the loopback control stream to that same
    // instant. `DaemonApiClient` deliberately gives canonical forwarding one
    // default network phase of grace so the daemon can return the terminal
    // response (including a precise possibly-applied error) after the inner
    // route budget expires.
    let send = async {
        client
            .object_rpc_session_to_target_with_deadline_async(
                target_node_id,
                call,
                options.connect_timeout,
                options.overall_timeout,
                options.observe_connectivity,
            )
            .await
            .map_err(|error| DfsPeerTransportError::new(error.to_string()))
    };
    let result = if let Ok(current) = tokio::runtime::Handle::try_current() {
        if matches!(
            current.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            tokio::task::block_in_place(|| runtime.block_on(send))
        } else {
            Err(DfsPeerTransportError::new(
                "SIFS daemon RPC cannot block a current-thread Tokio runtime; use the async transport",
            ))
        }
    } else {
        runtime.block_on(send)
    };
    SifsDaemonRpcAttempt::Complete(result)
}

fn publish_sifs_metadata_journal_from_data_dir(
    data_dir: &SinomoDataDir,
    rpc_runtime: &SifsRpcRuntime,
    operation_deadline: Option<Instant>,
) -> Result<DfsMetadataJournalReplicationReport, sinomo_runtime::dfs::DfsError> {
    let cluster_paths = ClusterPaths::new(data_dir);
    let peer_transport = Arc::new(
        SifsDfsPeerTransport::new(rpc_runtime.clone(), data_dir.clone())
            .with_operation_deadline(operation_deadline),
    );
    let sync_peer_transport: Arc<dyn DfsPeerTransport> = peer_transport.clone();
    let async_peer_transport: Arc<dyn AsyncDfsPeerTransport> = peer_transport;
    let options = sifs_foreground_metadata_publish_options_with_deadline(operation_deadline)
        .map_err(|message| sinomo_runtime::dfs::DfsError::Network { message })?;
    let publish = publish_cluster_dfs_metadata_journal_with_peer_transports_async(
        data_dir,
        &cluster_paths,
        sync_peer_transport,
        async_peer_transport,
        options,
    );
    block_on_sifs_rpc_runtime(rpc_runtime, publish)
}

fn block_on_sifs_rpc_runtime<F, T>(rpc_runtime: &SifsRpcRuntime, future: F) -> Result<T, DfsError>
where
    F: Future<Output = Result<T, DfsError>>,
{
    if let Ok(current) = tokio::runtime::Handle::try_current() {
        if matches!(
            current.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            return tokio::task::block_in_place(|| rpc_runtime.runtime.block_on(future));
        }
        return Err(DfsError::Network {
            message: "SIFS metadata publication cannot block a current-thread Tokio runtime; use the async transport"
                .to_string(),
        });
    }
    rpc_runtime.runtime.block_on(future)
}

fn sifs_foreground_metadata_publish_options() -> DfsMetadataJournalReplicationOptions {
    DfsMetadataJournalReplicationOptions {
        max_batches_per_target: SIFS_FOREGROUND_METADATA_PUBLISH_MAX_BATCHES_PER_TARGET,
        ..DfsMetadataJournalReplicationOptions::default()
    }
}

fn sifs_foreground_metadata_publish_options_with_deadline(
    operation_deadline: Option<Instant>,
) -> Result<DfsMetadataJournalReplicationOptions, String> {
    let mut options = sifs_foreground_metadata_publish_options();
    if let Some(remaining) = sifs_operation_remaining(operation_deadline, "metadata publication")? {
        options.attempt_timeout = options.attempt_timeout.min(remaining);
        options.target_timeout = options.target_timeout.min(remaining);
    }
    Ok(options)
}

fn handoff_or_publish_sifs_metadata<R, P>(
    operation_deadline: Option<Instant>,
    request_refresh: R,
    publish: P,
) -> Result<(), String>
where
    R: FnOnce(Duration) -> bool,
    P: FnOnce() -> Result<(), String>,
{
    let refresh_timeout = match sifs_operation_remaining(
        operation_deadline,
        "daemon metadata publication handoff",
    ) {
        Ok(remaining) => remaining
            .map(|remaining| remaining.min(SIFS_MAINTENANCE_REFRESH_TIMEOUT))
            .unwrap_or(SIFS_MAINTENANCE_REFRESH_TIMEOUT),
        Err(error) => {
            eprintln!(
                "warning: skipping SIFS metadata publication after a committed mutation because its operation budget expired: {error}"
            );
            return Ok(());
        }
    };
    if request_refresh(refresh_timeout) {
        return Ok(());
    }

    if let Err(error) = sifs_operation_remaining(operation_deadline, "metadata publication") {
        eprintln!(
            "warning: skipping foreground SIFS metadata publication after a committed mutation because its operation budget expired: {error}"
        );
        return Ok(());
    }
    if let Err(error) = publish() {
        eprintln!(
            "warning: foreground SIFS metadata publication failed after a committed mutation; durable metadata maintenance will retry: {error}"
        );
    }
    Ok(())
}

fn request_sifs_daemon_maintenance_refresh(data_dir: &SinomoDataDir, timeout: Duration) -> bool {
    let paths = sinomo_runtime::daemon::DaemonPaths::new(data_dir);
    if !paths.lock.exists() {
        return false;
    }
    let Ok(status) = sinomo_runtime::daemon::read_daemon_status(&paths) else {
        return false;
    };
    sinomo_runtime::daemon::send_daemon_cluster_maintenance_refresh(&status.address, timeout)
        .is_ok()
}

fn sifs_write_options(
    service: &NetworkedDfsService<DfsNodeTransport>,
    replica_candidates: &[NodeId],
) -> DfsWriteOptions {
    let mut candidates = replica_candidates.to_vec();
    if !candidates.contains(service.node_id()) {
        candidates.insert(0, service.node_id().clone());
    }
    sifs_write_options_for_candidates(candidates)
}

fn sifs_write_receipt(stat: &DfsStat) -> sinomo_runtime::api::DaemonDfsBrowserWriteReceipt {
    sinomo_runtime::api::DaemonDfsBrowserWriteReceipt {
        object_id: stat.object_id.to_string(),
        version: stat.version.to_string(),
        content_hash: stat.content_hash.clone(),
        size_bytes: stat.size_bytes,
    }
}

fn sifs_conditional_write_error(
    error: DfsPosixError,
) -> sinomo_runtime::api::DaemonDfsBrowserReplaceFileIfVersionOutcome {
    use sinomo_runtime::api::DaemonDfsBrowserReplaceFileIfVersionOutcome as Outcome;

    match error {
        DfsPosixError::Conflict { path, reason } => Outcome::Conflict {
            detail: format!("write conflicted at {path}: {reason}"),
        },
        DfsPosixError::NotFound { path } => Outcome::NotFound {
            detail: format!("not found: {path}"),
        },
        error => Outcome::Rejected {
            detail: error.to_string(),
        },
    }
}

fn sifs_deferred_publish_write_options(
    service: &NetworkedDfsService<DfsNodeTransport>,
    replica_candidates: &[NodeId],
) -> DfsWriteOptions {
    let mut candidates = replica_candidates.to_vec();
    if !candidates.contains(service.node_id()) {
        candidates.insert(0, service.node_id().clone());
    }
    sifs_deferred_publish_write_options_for_candidates(candidates)
}

fn sifs_write_options_for_candidates(candidates: Vec<NodeId>) -> DfsWriteOptions {
    let mut options = DfsWriteOptions::replicated_eventual(candidates);
    options.foreground_replica_repair = false;
    options
}

fn sifs_deferred_publish_write_options_for_candidates(candidates: Vec<NodeId>) -> DfsWriteOptions {
    let mut options = sifs_write_options_for_candidates(candidates);
    options.wait_for_commit = false;
    options
}

fn ensure_sifs_parent_directory_for_write(
    service: &mut NetworkedDfsService<DfsNodeTransport>,
    parent: &str,
) -> Result<(), String> {
    if service
        .posix_stat(parent)
        .is_ok_and(|stat| stat.kind == DfsObjectKind::Directory)
    {
        return Ok(());
    }
    match service.posix_stat_distributed(parent) {
        Ok(stat) if stat.kind == DfsObjectKind::Directory => Ok(()),
        Ok(stat) => Err(format!(
            "cannot create SIFS file under {parent}: parent is {:?}",
            stat.kind
        )),
        Err(DfsPosixError::NotFound { .. } | DfsPosixError::MissingParent { .. }) => service
            .posix_mkdir_all(parent)
            .map(|_| ())
            .map_err(|error| format!("cannot create SIFS directory {parent}: {error}")),
        Err(error) => Err(format!("cannot inspect SIFS directory {parent}: {error}")),
    }
}

fn sifs_write_error_needs_parent_create(error: &DfsPosixError) -> bool {
    matches!(error, DfsPosixError::MissingParent { .. })
}

fn sifs_operation_remaining(
    operation_deadline: Option<Instant>,
    operation: &str,
) -> Result<Option<Duration>, String> {
    let Some(deadline) = operation_deadline else {
        return Ok(None);
    };
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .map(Some)
        .ok_or_else(|| format!("SIFS operation deadline expired during {operation}"))
}

fn validate_posix_directory_attrs(
    path: &str,
    stat: &DfsStat,
    uid: u32,
    gid: u32,
    mode: u32,
) -> Result<(), String> {
    if stat.kind != DfsObjectKind::Directory {
        return Err(format!(
            "SIFS path {path} is {:?}, not a directory",
            stat.kind
        ));
    }
    if stat.uid != uid || stat.gid != gid || stat.mode != mode {
        return Err(format!(
            "SIFS directory {path} has owner {}:{} mode {:04o}, expected {uid}:{gid} mode {:04o}",
            stat.uid, stat.gid, stat.mode, mode
        ));
    }
    Ok(())
}

fn sifs_parent_path(path: &str) -> Option<String> {
    let (parent, name) = path.rsplit_once('/')?;
    if name.is_empty() {
        return None;
    }
    Some(if parent.is_empty() {
        "/".to_string()
    } else {
        parent.to_string()
    })
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

    #[derive(Clone, Debug)]
    struct CanonicalPeerTransport {
        response: network::ObjectRpcResponse,
        attachment: Vec<u8>,
    }

    impl CanonicalPeerTransport {
        fn rpc_result(&self) -> network::ObjectRpcSessionResult {
            network::ObjectRpcSessionResult::new(
                self.response.clone(),
                network::ObjectRpcIncomingBody::bytes(self.attachment.clone()),
            )
        }
    }

    impl DfsPeerTransport for CanonicalPeerTransport {
        fn call_object_rpc(
            &self,
            _target_node_id: &NodeId,
            _call: network::ObjectRpcSessionCall,
            _options: DfsObjectRpcSendOptions,
        ) -> Result<network::ObjectRpcSessionResult, DfsPeerTransportError> {
            Ok(self.rpc_result())
        }
    }

    impl AsyncDfsPeerTransport for CanonicalPeerTransport {
        fn call_object_rpc_async<'a>(
            &'a self,
            _target_node_id: &'a NodeId,
            _call: network::ObjectRpcSessionCall,
            _options: DfsObjectRpcSendOptions,
        ) -> DfsPeerTransportFuture<
            'a,
            Result<network::ObjectRpcSessionResult, DfsPeerTransportError>,
        > {
            let result = self.rpc_result();
            Box::pin(async move { Ok(result) })
        }
    }

    #[test]
    fn conditional_write_expected_failures_remain_typed() {
        assert!(matches!(
            sifs_conditional_write_error(DfsPosixError::Conflict {
                path: "/notes.txt".to_string(),
                reason: "stale base".to_string(),
            }),
            sinomo_runtime::api::DaemonDfsBrowserReplaceFileIfVersionOutcome::Conflict { .. }
        ));
        assert!(matches!(
            sifs_conditional_write_error(DfsPosixError::NotFound {
                path: "/notes.txt".to_string(),
            }),
            sinomo_runtime::api::DaemonDfsBrowserReplaceFileIfVersionOutcome::NotFound { .. }
        ));
        assert!(matches!(
            sifs_conditional_write_error(DfsPosixError::IsDirectory {
                path: "/notes.txt".to_string(),
            }),
            sinomo_runtime::api::DaemonDfsBrowserReplaceFileIfVersionOutcome::Rejected { .. }
        ));
    }

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
    fn parallel_read_worker_count_preserves_bulk_batches() {
        assert_eq!(sifs_parallel_read_worker_count_for(0, 32), 1);
        assert_eq!(sifs_parallel_read_worker_count_for(1, 32), 1);
        assert_eq!(sifs_parallel_read_worker_count_for(2, 32), 1);
        assert_eq!(sifs_parallel_read_worker_count_for(64, 32), 4);
        assert_eq!(sifs_parallel_read_worker_count_for(1024, 32), 32);
        assert_eq!(sifs_parallel_read_worker_count_for(1024, 64), 64);
    }

    #[test]
    fn sifs_rpc_runtime_is_process_shared() {
        let first = SifsRpcRuntime::new().expect("runtime");
        let second = SifsRpcRuntime::new().expect("runtime");
        assert!(
            Arc::ptr_eq(&first.runtime, &second.runtime),
            "the SIFS RPC runtime must stay process-shared: per-open runtimes \
             deadlock whichever thread drops the last handle while a blocking \
             daemon probe or peer RPC is still in flight"
        );
    }

    #[test]
    fn shared_peer_transport_preserves_response_value_and_attachment() {
        let route = "/dfs/test/session-preserving";
        let encoded_head = "encoded-response-head".to_string();
        let attachment = b"streamed-response-attachment".to_vec();
        let transport = CanonicalPeerTransport {
            response: network::ObjectRpcResponse::encode_body(route, &encoded_head)
                .expect("response head"),
            attachment: attachment.clone(),
        };
        let (sync_transport, async_transport) = shared_sifs_peer_transports(transport);
        let target = NodeId::from_legacy_named("node_sifs_session_target").expect("target node");
        let request = || {
            network::ObjectRpcRequest::encode_body(route, "dfs", &"request-head")
                .expect("request head")
        };
        let runtime = SifsRpcRuntime::new().expect("runtime");

        let sync_result = sync_transport
            .call_object_rpc(
                &target,
                network::ObjectRpcSessionCall::value(request()),
                DfsObjectRpcSendOptions::default(),
            )
            .expect("sync RPC result");
        assert_eq!(
            sync_result
                .response
                .decode_body::<String>()
                .expect("sync encoded response head"),
            encoded_head
        );
        assert_eq!(
            runtime
                .runtime
                .block_on(sync_result.body.collect_to_vec(1024))
                .expect("sync response attachment"),
            attachment
        );

        let async_result = runtime
            .runtime
            .block_on(async_transport.call_object_rpc_async(
                &target,
                network::ObjectRpcSessionCall::value(request()),
                DfsObjectRpcSendOptions::default(),
            ))
            .expect("async RPC result");
        assert_eq!(
            async_result
                .response
                .decode_body::<String>()
                .expect("async encoded response head"),
            encoded_head
        );
        assert_eq!(
            runtime
                .runtime
                .block_on(async_result.body.collect_to_vec(1024))
                .expect("async response attachment"),
            attachment
        );
    }

    #[test]
    fn persistent_peer_transport_does_not_retain_backend_open_deadline() {
        let root = temp_sifs_data_dir("persistent-peer-deadline");
        let data_dir = SinomoDataDir::new(&root);
        let runtime = SifsRpcRuntime::new().expect("runtime");
        let expired_open_deadline = Instant::now() - Duration::from_millis(1);
        let bootstrap = SifsDfsPeerTransport::new(runtime.clone(), data_dir.clone())
            .with_operation_deadline(Some(expired_open_deadline));
        let persistent = SifsDfsPeerTransport::new(runtime, data_dir);

        assert_eq!(
            bootstrap.operation_deadline,
            Some(expired_open_deadline),
            "the temporary bootstrap transport must retain the open deadline"
        );
        assert_eq!(
            persistent.operation_deadline, None,
            "the service transport must start with no backend-open deadline"
        );

        let started_at = Instant::now();
        let later_deadline = sifs_rpc_effective_operation_deadline(
            &DfsObjectRpcSendOptions::default().with_overall_timeout(Duration::from_millis(100)),
            persistent.operation_deadline,
        )
        .expect("later per-RPC deadline");
        assert!(
            later_deadline > started_at,
            "a later RPC was clamped by the expired backend-open deadline"
        );
    }

    #[test]
    fn metadata_peer_rpc_options_are_clamped_to_remaining_operation_budget() {
        let deadline = Instant::now() + Duration::from_millis(100);
        let options = sifs_rpc_options_with_operation_deadline(
            DfsObjectRpcSendOptions::default().with_overall_timeout(Duration::from_secs(5)),
            Some(deadline),
        )
        .expect("bounded metadata RPC options");
        let overall_timeout = options.overall_timeout.expect("overall timeout");
        assert!(overall_timeout > Duration::ZERO);
        assert!(
            overall_timeout <= Duration::from_millis(100),
            "metadata RPC must receive only the operation's remaining budget: {overall_timeout:?}"
        );
    }

    #[test]
    fn peer_rpc_budget_is_recomputed_after_daemon_discovery_time() {
        let deadline = Instant::now() + Duration::from_millis(200);
        let initial = sifs_rpc_options_with_operation_deadline(
            DfsObjectRpcSendOptions::default().with_overall_timeout(Duration::from_secs(5)),
            Some(deadline),
        )
        .expect("initial bounded options")
        .overall_timeout
        .expect("initial overall timeout");
        std::thread::sleep(Duration::from_millis(30));
        let before_dispatch = Instant::now();
        let after_discovery = sifs_rpc_options_with_operation_deadline(
            DfsObjectRpcSendOptions::default().with_overall_timeout(initial),
            Some(deadline),
        )
        .expect("dispatch-time bounded options")
        .overall_timeout
        .expect("dispatch-time overall timeout");

        assert!(
            after_discovery < initial,
            "daemon discovery time restarted the route budget: initial={initial:?}, after={after_discovery:?}"
        );
        assert!(
            after_discovery <= deadline.saturating_duration_since(before_dispatch),
            "dispatch timeout exceeds the remaining absolute operation budget"
        );
    }

    #[test]
    fn relative_rpc_timeout_becomes_an_absolute_dispatch_deadline() {
        let started_at = Instant::now();
        let options =
            DfsObjectRpcSendOptions::default().with_overall_timeout(Duration::from_millis(100));
        let deadline =
            sifs_rpc_effective_operation_deadline(&options, None).expect("relative RPC deadline");

        assert!(deadline > started_at);
        assert!(
            deadline.saturating_duration_since(started_at) <= Duration::from_millis(110),
            "relative route timeout was not converted at transport entry"
        );
    }

    #[test]
    fn metadata_peer_rpc_options_reject_an_expired_operation_budget() {
        let error = sifs_rpc_options_with_operation_deadline(
            DfsObjectRpcSendOptions::default(),
            Some(Instant::now() - Duration::from_millis(1)),
        )
        .expect_err("expired metadata RPC must fail before transport dispatch");

        assert!(error.to_string().contains("operation deadline expired"));
    }

    #[test]
    fn wait_for_commit_write_rejects_an_already_expired_deadline() {
        let root = temp_sifs_data_dir("expired-wait-for-commit");
        let data_dir = SinomoDataDir::new(&root);
        let backend = SifsFileManagerBackend::open(&data_dir).expect("backend");
        let path = "/services/expired-wait-for-commit.txt";

        let error = backend
            .write_file_wait_for_commit_with_deadline(
                path,
                b"must not be written",
                Instant::now() - Duration::from_millis(1),
            )
            .expect_err("expired write must fail before mutating DFS");

        assert!(error.contains("operation deadline expired"));
        assert!(
            backend.dfs_stat(path).is_err(),
            "expired write became visible"
        );
    }

    #[test]
    fn wait_for_commit_write_bounds_service_lock_contention() {
        let root = temp_sifs_data_dir("contended-wait-for-commit");
        let data_dir = SinomoDataDir::new(&root);
        let backend = Arc::new(SifsFileManagerBackend::open(&data_dir).expect("backend"));
        let contended = Arc::clone(&backend);
        let service_guard = backend.lock_service().expect("service lock");
        let started_at = Instant::now();
        let writer = std::thread::spawn(move || {
            contended.write_file_wait_for_commit_with_deadline(
                "/services/contended-wait-for-commit.txt",
                b"must not be written",
                Instant::now() + Duration::from_millis(30),
            )
        });

        let error = writer
            .join()
            .expect("writer thread")
            .expect_err("contended lock must respect the write deadline");
        drop(service_guard);

        assert!(error.contains("operation deadline expired"));
        assert!(
            started_at.elapsed() < Duration::from_millis(500),
            "service lock contention exceeded the caller deadline"
        );
    }

    #[test]
    fn deadline_named_deferred_helpers_bound_service_lock_contention() {
        type ContendedOperation = Box<
            dyn FnOnce(Arc<SifsFileManagerBackend>, Instant) -> Result<(), String> + Send + 'static,
        >;

        let root = temp_sifs_data_dir("contended-deadline-helpers");
        let data_dir = SinomoDataDir::new(&root);
        let backend = Arc::new(SifsFileManagerBackend::open(&data_dir).expect("backend"));
        let service_guard = backend.lock_service().expect("service lock");
        let run_contended = |label: &str, operation: ContendedOperation| {
            let contended = Arc::clone(&backend);
            let started_at = Instant::now();
            let worker = std::thread::spawn(move || {
                operation(contended, Instant::now() + Duration::from_millis(30))
            });
            let error = worker
                .join()
                .expect("deadline helper thread")
                .expect_err("contended helper must respect its deadline");
            assert!(
                error.contains("operation deadline expired"),
                "unexpected {label} error: {error}"
            );
            assert!(
                started_at.elapsed() < Duration::from_millis(500),
                "{label} exceeded its caller deadline"
            );
        };

        run_contended(
            "directory create",
            Box::new(|backend, deadline| {
                backend.create_dir_all_deferred_publish_until("/services/contended-dir", deadline)
            }),
        );
        run_contended(
            "bulk write",
            Box::new(|backend, deadline| {
                backend.write_files_with_known_parents_deferred_publish_until(
                    &[("/services/contended-bulk.txt".to_string(), b"bulk".to_vec())],
                    deadline,
                )
            }),
        );
        run_contended(
            "known-parent write",
            Box::new(|backend, deadline| {
                backend.write_file_with_known_parent_no_publish_until(
                    "/services/contended-known.txt",
                    b"known",
                    deadline,
                )
            }),
        );
        run_contended(
            "deferred known-parent write",
            Box::new(|backend, deadline| {
                backend.write_file_with_known_parent_deferred_publish_until(
                    "/services/contended-deferred.txt",
                    b"deferred",
                    deadline,
                )
            }),
        );
        run_contended(
            "rename",
            Box::new(|backend, deadline| {
                backend.rename_with_deadline(
                    "/services/contended-rename-source.txt",
                    "/services/contended-rename-target.txt",
                    deadline,
                )
            }),
        );
        run_contended(
            "file delete",
            Box::new(|backend, deadline| {
                backend.delete_file_with_deadline("/services/contended-delete.txt", deadline)
            }),
        );
        run_contended(
            "path remove",
            Box::new(|backend, deadline| {
                backend.remove_with_deadline("/services/contended-remove.txt", deadline)
            }),
        );
        drop(service_guard);
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
    fn private_posix_directory_bootstrap_is_idempotent_and_never_steals_owner() {
        let root = temp_sifs_data_dir("private-posix-directory");
        let data_dir = SinomoDataDir::new(&root);
        let backend = SifsFileManagerBackend::open(&data_dir).expect("backend");
        let path = "/sinomo/agents/test-session/home";

        let created = backend
            .ensure_posix_directory_with_attrs(path, 1001, 1002, 0o700)
            .expect("create private directory");
        assert_eq!(created.kind, DfsObjectKind::Directory);
        assert_eq!(
            (created.uid, created.gid, created.mode),
            (1001, 1002, 0o700)
        );
        let reopened = backend
            .ensure_posix_directory_with_attrs(path, 1001, 1002, 0o700)
            .expect("reopen private directory");
        assert_eq!(
            (reopened.uid, reopened.gid, reopened.mode),
            (1001, 1002, 0o700)
        );

        let mismatch = backend
            .ensure_posix_directory_with_attrs(path, 2001, 2002, 0o700)
            .expect_err("existing owner must not be replaced");
        assert!(mismatch.contains("has owner 1001:1002"), "{mismatch}");
        let unchanged = backend.dfs_stat(path).expect("unchanged private directory");
        assert_eq!((unchanged.uid, unchanged.gid), (1001, 1002));

        let file_path = "/sinomo/agents/not-a-directory";
        backend.write_file(file_path, b"file").expect("create file");
        let wrong_kind = backend
            .ensure_posix_directory_with_attrs(file_path, 1001, 1002, 0o700)
            .expect_err("file must not be accepted as a directory");
        assert!(wrong_kind.contains("not a directory"), "{wrong_kind}");

        let target_path = "/sinomo/agents/symlink-target-home";
        backend
            .ensure_posix_directory_with_attrs(target_path, 1001, 1002, 0o700)
            .expect("create matching symlink target");
        let link_path = "/sinomo/agents/symlink-home";
        {
            let mut service = backend.lock_service().expect("lock service");
            let node_id = service.node_id().clone();
            service
                .posix_symlink(
                    target_path,
                    link_path,
                    DfsWriteOptions::replicated_eventual(vec![node_id]),
                )
                .expect("create home symlink");
        }
        let symlink_error = backend
            .ensure_posix_directory_with_attrs(link_path, 1001, 1002, 0o700)
            .expect_err("a symlink leaf must never be accepted as a private home");
        assert!(symlink_error.contains("not a directory"), "{symlink_error}");
    }

    #[test]
    fn sifs_remove_with_deadline_handles_files_and_empty_directories() {
        let root = temp_sifs_data_dir("remove-with-deadline");
        let data_dir = SinomoDataDir::new(&root);
        let backend = SifsFileManagerBackend::open(&data_dir).expect("backend");
        let file = "/services/remove-with-deadline.txt";
        let directory = "/services/remove-with-deadline-dir";

        backend.write_file(file, b"remove me").expect("write file");
        backend.create_dir(directory).expect("create directory");

        backend
            .remove_with_deadline(file, Instant::now() + Duration::from_secs(2))
            .expect("remove file");
        backend
            .remove_with_deadline(directory, Instant::now() + Duration::from_secs(2))
            .expect("remove directory");

        assert!(backend.stat(file).is_err(), "removed file must be absent");
        assert!(
            backend.stat(directory).is_err(),
            "removed directory must be absent"
        );
    }

    #[test]
    fn sifs_read_only_backend_refreshes_visible_overwrite() {
        let root = temp_sifs_data_dir("current-read-refresh");
        let data_dir = SinomoDataDir::new(&root);
        let writer = SifsFileManagerBackend::open(&data_dir).expect("writer");
        let path = "/services/current-read-refresh.txt";
        let old = b"old visible payload";
        let new = b"new visible payload";

        writer.write_file(path, old).expect("write old payload");
        let reader = SifsFileManagerBackend::open_read_only(&data_dir).expect("reader");
        assert_eq!(reader.read_file(path).expect("read old payload"), old);

        writer.write_file(path, new).expect("write new payload");

        assert_eq!(reader.read_file(path).expect("read new payload"), new);
    }

    #[test]
    fn sifs_write_parent_preflight_accepts_local_directory() {
        let root = temp_sifs_data_dir("local-parent-preflight");
        let data_dir = SinomoDataDir::new(&root);
        let backend = SifsFileManagerBackend::open(&data_dir).expect("backend");
        backend
            .create_dir_all("/services/local-parent-preflight")
            .expect("create parent");

        let mut service = backend.lock_service().expect("service");
        ensure_sifs_parent_directory_for_write(&mut service, "/services/local-parent-preflight")
            .expect("local directory parent should not require distributed inspection");
    }

    #[test]
    fn sifs_write_parent_preflight_rejects_local_file_parent() {
        let root = temp_sifs_data_dir("local-file-parent-preflight");
        let data_dir = SinomoDataDir::new(&root);
        let backend = SifsFileManagerBackend::open(&data_dir).expect("backend");
        backend
            .write_file(
                "/services/local-file-parent-preflight.txt",
                b"not a directory",
            )
            .expect("create file");

        let mut service = backend.lock_service().expect("service");
        let error = ensure_sifs_parent_directory_for_write(
            &mut service,
            "/services/local-file-parent-preflight.txt",
        )
        .expect_err("file parent should be rejected locally");
        assert!(
            error.contains("parent is File"),
            "unexpected parent preflight error: {error}"
        );
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

    #[test]
    fn sifs_deferred_publish_write_options_do_not_wait_for_commit() {
        let local = NodeId::from_legacy_named("node_sifs_deferred_policy_local").expect("node");
        let remote = NodeId::from_legacy_named("node_sifs_deferred_policy_remote").expect("node");

        let options = sifs_deferred_publish_write_options_for_candidates(vec![local, remote]);

        assert_eq!(options.policy.min_commit_replicas, 1);
        assert_eq!(options.policy.desired_replicas, 2);
        assert!(!options.wait_for_commit);
        assert!(
            !options.foreground_replica_repair,
            "deferred publish writes should not block on the async replica repair path"
        );
    }

    #[test]
    fn sifs_foreground_metadata_publish_uses_network_timeout_defaults() {
        let options = sifs_foreground_metadata_publish_options();

        assert_eq!(options.max_batches_per_target, 1);
        assert_eq!(options.attempt_timeout, network::DEFAULT_NETWORK_TIMEOUT);
        assert_eq!(options.target_timeout, network::DEFAULT_NETWORK_TIMEOUT);
    }

    #[test]
    fn sifs_rpc_runtime_blocks_foreground_publish_from_plain_thread() {
        let rpc_runtime = SifsRpcRuntime::new().expect("SIFS RPC runtime");

        let value = block_on_sifs_rpc_runtime(&rpc_runtime, async {
            tokio::task::yield_now().await;
            Ok::<_, DfsError>(41)
        })
        .expect("foreground publish future");

        assert_eq!(value, 41);
    }

    #[test]
    fn sifs_rpc_runtime_blocks_foreground_publish_from_multithread_runtime() {
        let rpc_runtime = SifsRpcRuntime::new().expect("SIFS RPC runtime");
        let caller_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("caller runtime");

        let value = caller_runtime
            .block_on(async {
                block_on_sifs_rpc_runtime(&rpc_runtime, async {
                    tokio::task::yield_now().await;
                    Ok::<_, DfsError>(42)
                })
            })
            .expect("foreground publish future");

        assert_eq!(value, 42);
    }

    #[test]
    fn sifs_rpc_runtime_rejects_blocking_current_thread_runtime() {
        let rpc_runtime = SifsRpcRuntime::new().expect("SIFS RPC runtime");
        let caller_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("caller runtime");

        let error = caller_runtime
            .block_on(async {
                block_on_sifs_rpc_runtime(&rpc_runtime, async { Ok::<_, DfsError>(()) })
            })
            .expect_err("current-thread runtime must not be blocked");

        assert!(
            error
                .to_string()
                .contains("cannot block a current-thread Tokio runtime")
        );
    }

    #[test]
    fn acknowledged_daemon_metadata_handoff_skips_foreground_publish() {
        let refresh_requests = std::cell::Cell::new(0);
        let foreground_publishes = std::cell::Cell::new(0);

        handoff_or_publish_sifs_metadata(
            None,
            |timeout| {
                refresh_requests.set(refresh_requests.get() + 1);
                assert_eq!(timeout, SIFS_MAINTENANCE_REFRESH_TIMEOUT);
                true
            },
            || {
                foreground_publishes.set(foreground_publishes.get() + 1);
                Ok(())
            },
        )
        .expect("acknowledged daemon handoff");

        assert_eq!(refresh_requests.get(), 1);
        assert_eq!(foreground_publishes.get(), 0);
    }

    #[test]
    fn failed_daemon_metadata_handoff_falls_back_to_foreground_publish() {
        let refresh_requests = std::cell::Cell::new(0);
        let foreground_publishes = std::cell::Cell::new(0);

        handoff_or_publish_sifs_metadata(
            None,
            |_| {
                refresh_requests.set(refresh_requests.get() + 1);
                false
            },
            || {
                foreground_publishes.set(foreground_publishes.get() + 1);
                Ok(())
            },
        )
        .expect("foreground metadata publication fallback");

        assert_eq!(refresh_requests.get(), 1);
        assert_eq!(foreground_publishes.get(), 1);
    }

    #[test]
    fn failed_foreground_metadata_fallback_does_not_misreport_committed_mutation() {
        let foreground_publishes = std::cell::Cell::new(0);

        handoff_or_publish_sifs_metadata(
            None,
            |_| false,
            || {
                foreground_publishes.set(foreground_publishes.get() + 1);
                Err("simulated WAN publication failure".to_string())
            },
        )
        .expect("metadata publication failure must not fail a committed mutation");

        assert_eq!(foreground_publishes.get(), 1);
    }

    #[test]
    fn expired_publication_budget_does_not_misreport_committed_mutation() {
        let refresh_requests = std::cell::Cell::new(0);
        let foreground_publishes = std::cell::Cell::new(0);

        handoff_or_publish_sifs_metadata(
            Some(Instant::now() - Duration::from_millis(1)),
            |_| {
                refresh_requests.set(refresh_requests.get() + 1);
                true
            },
            || {
                foreground_publishes.set(foreground_publishes.get() + 1);
                Ok(())
            },
        )
        .expect("expired publication budget must not fail a committed mutation");

        assert_eq!(refresh_requests.get(), 0);
        assert_eq!(foreground_publishes.get(), 0);
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
            cluster_id: sinomo_runtime::node::ClusterId::from_legacy_named(
                "cluster_sifs_replica_rank",
            )
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
            failure_reason: None,
            observed_at: now,
            expires_at: now + time::Duration::seconds(600),
        }
    }

    fn temp_sifs_data_dir(name: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(&format!("sinomo-tui-sifs-file-manager-{name}"))
    }
}
