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
    DfsFileSystemConfig, DfsMetadataJournalReplicationOptions, DfsMetadataJournalReplicationReport,
    DfsMetadataShardConfig, DfsNamespaceJobKind, DfsNamespaceJobStatus, DfsNodeAdvertisement,
    DfsNodeTransport, DfsObjectKind, DfsObjectRpcSendOptions, DfsOperatorObjectView,
    DfsOperatorStatus, DfsPeerTransport, DfsPeerTransportError, DfsPeerTransportFuture,
    DfsPinAddRequest, DfsPinMode, DfsPlacementBlockingDiagnostic, DfsPlacementImpact,
    DfsPlacementNodePreview, DfsPlacementNodeReachability, DfsPlacementPolicy, DfsPlacementPreview,
    DfsPlacementPreviewToken, DfsPlacementReconciliationActivityCursor,
    DfsPlacementReconciliationActivityPage, DfsPlacementReconciliationActivityStatus,
    DfsPlacementReconciliationJob, DfsPlacementReconciliationState, DfsPlacementScope,
    DfsPosixCreateAttrs, DfsPosixError, DfsResolvedPlacementPolicy, DfsStat, DfsWriteOptions,
    LocalContentStore, NetworkedDfsService, cluster_dfs_peer_node_ids, content_services_prefix,
    default_dfs_metadata_root, default_dfs_metadata_shards_root, default_dfs_pin_root,
    fallback_dfs_node_id, metadata_shards_prefix,
    open_cluster_aware_dfs_metadata_backend_for_inspection,
    open_local_sharded_metadata_backend_for_replica_repair, open_root_local_metadata_backend,
    preview_placement_with_unavailable_nodes,
    publish_cluster_dfs_metadata_journal_with_peer_transports_async,
    refresh_cluster_dfs_kv_snapshot_with_peer_transport,
    select_metadata_shard_config_for_path_strict_default_sifs_split, sifs_file_system_id,
    with_cached_cluster_aware_dfs_metadata_inspection,
};
use sinomo_runtime::kv::{KvError, KvOp, KvRevision, KvSnapshot, KvTxn, KvTxnResponse};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{self, ClusterPaths};
use sinomo_runtime::node::NodeId;
use std::collections::BTreeSet;
use std::future::Future;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const SIFS_PEER_TARGET_REFRESH_TTL: Duration = Duration::from_secs(1);
const SIFS_FOREGROUND_METADATA_PUBLISH_MAX_BATCHES_PER_TARGET: usize = 1;
const SIFS_MAINTENANCE_REFRESH_TIMEOUT: Duration = Duration::from_millis(750);
const SIFS_PLACEMENT_IMPACT_PAGE_SIZE: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SifsWriteParentPolicy {
    MaterializeMissing,
    RequireExisting,
}

#[derive(Debug)]
pub(crate) struct SifsFileManagerBackend {
    data_dir: SinomoDataDir,
    rpc_runtime: SifsRpcRuntime,
    service: Mutex<NetworkedDfsService<DfsNodeTransport>>,
    peer_targets_refreshed_at: Mutex<Option<Instant>>,
    replica_candidates: Vec<NodeId>,
}

/// Narrow, metadata-less requester for the create-only DFS throughput probe.
///
/// This handle deliberately exposes only the three write stages used by
/// `cluster.dfs.bulk-write`: create its parent directory, create files under a
/// known parent, and hand post-commit maintenance back to the resident daemon.
/// General SIFS callers must use [`SifsFileManagerBackend`], whose ordinary
/// open always owns a durable metadata backend.
#[derive(Debug)]
pub(crate) struct SifsOneShotBulkCreateRequester {
    data_dir: SinomoDataDir,
    local_node_id: NodeId,
    service: Mutex<NetworkedDfsService<DfsNodeTransport>>,
    control_plane: SifsOneShotBulkCreateControlPlaneKvClient,
}

/// A metadata-only, observational SIFS view.
///
/// This is deliberately a separate type from [`SifsFileManagerBackend`]. An
/// observation must never construct a networked DFS service: service bootstrap
/// owns namespace initialization, recovery, and replication work, all of which
/// are mutations even when the caller only intends to inspect status.
///
/// Its operational metrics are also limited to durable, lane-independent
/// state. Whether authoritative writes are frozen is a property of one
/// long-lived backend instance: a cold inspection handle intentionally has no
/// process-local authority binding and therefore cannot report the daemon
/// lane's writability. Callers that need that evidence must exercise the live
/// daemon mutation lane.
#[derive(Clone, Debug)]
pub(crate) struct SifsFileManagerObservation {
    data_dir: SinomoDataDir,
    node_id: NodeId,
}

#[derive(Clone, Debug)]
pub(crate) struct SifsFileManagerObservationSnapshot {
    pub(crate) metrics: SifsFileManagerObservationMetrics,
    pub(crate) operator_status: DfsOperatorStatus,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SifsFileManagerObservationMetrics {
    pub(crate) object_count: usize,
    pub(crate) open_conflict_count: usize,
    pub(crate) repair_job_count: usize,
    pub(crate) repair_attempt_count: usize,
    pub(crate) pending_write_count: usize,
    pub(crate) pending_metadata_replication_count: usize,
    pub(crate) cross_shard_namespace_lock_count: usize,
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

impl SifsFileManagerObservation {
    pub(crate) fn open(data_dir: &SinomoDataDir) -> Result<Self, String> {
        let cluster_paths = ClusterPaths::new(data_dir);
        let node_id = if cluster_paths.state.exists() {
            cluster::read_cluster_state_without_kv(&cluster_paths)
                .map_err(|error| format!("cannot read local cluster state: {error}"))?
                .local_node_id
        } else {
            fallback_dfs_node_id()
        };
        Ok(Self {
            data_dir: data_dir.clone(),
            node_id,
        })
    }

    pub(crate) fn open_with_deadline(
        data_dir: &SinomoDataDir,
        deadline: Instant,
    ) -> Result<Self, String> {
        let operation_deadline = Some(deadline);
        sifs_operation_remaining(operation_deadline, "opening the SIFS observation")?;
        let observation = Self::open(data_dir)?;
        sifs_operation_remaining(operation_deadline, "opening the SIFS observation")?;
        Ok(observation)
    }

    pub(crate) fn node_label(&self) -> String {
        self.node_id.to_string()
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
        let mut kv = SifsSnapshotControlPlaneKvClient::new(&cluster_paths);
        DfsControlPlaneStore::new(&mut kv)
            .select_metadata_shard_for_path_strict_default_sifs_split(&sifs_file_system_id(), &path)
            .map_err(|error| format!("cannot read SIFS metadata shard for {path}: {error}"))
    }

    pub(crate) fn object_by_path(
        &self,
        path: &str,
    ) -> Result<Option<DfsOperatorObjectView>, String> {
        let path = normalize_sifs_path(path)?;
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        with_cached_cluster_aware_dfs_metadata_inspection(
            &self.data_dir,
            &cluster_paths,
            |metadata| {
                sinomo_runtime::dfs::DfsOperatorInspector::new(metadata)
                    .object_by_path(&sifs_file_system_id(), &path)
                    .map_err(Into::into)
            },
        )
        .map_err(|error| format!("cannot inspect SIFS path {path}: {error}"))
    }

    pub(crate) fn operational_snapshot(
        &self,
    ) -> Result<SifsFileManagerObservationSnapshot, String> {
        const NAMESPACE_JOB_PAGE_SIZE: usize = 256;

        let cluster_paths = ClusterPaths::new(&self.data_dir);
        if !cluster_paths.state.exists() {
            return Err("cluster SIFS observation requires local cluster state".to_string());
        }
        let operator_status =
            sinomo_runtime::dfs::inspect_cluster_aware_dfs_status(&self.data_dir, &cluster_paths)
                .map_err(|error| format!("cannot read SIFS operator status: {error}"))?;

        let cross_shard_namespace_locks = with_cached_cluster_aware_dfs_metadata_inspection(
            &self.data_dir,
            &cluster_paths,
            |metadata| {
                let mut locks = BTreeSet::new();
                let mut namespace_job_cursor = None;
                loop {
                    let page = metadata.list_namespace_jobs_page(
                        namespace_job_cursor.as_ref(),
                        NAMESPACE_JOB_PAGE_SIZE,
                    )?;
                    for job in page.jobs {
                        if job.status == DfsNamespaceJobStatus::Running
                            && matches!(
                                job.kind,
                                DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries
                                    | DfsNamespaceJobKind::HardlinkAcrossShardPrimaries
                                    | DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries
                            )
                        {
                            locks.insert((job.file_system_id, job.root_path));
                        }
                    }
                    let Some(next_cursor) = page.next_cursor else {
                        break;
                    };
                    namespace_job_cursor = Some(next_cursor);
                }
                Ok(locks)
            },
        )
        .map_err(|error| format!("cannot inspect SIFS namespace jobs: {error}"))?;

        let pending_metadata_replication_count = operator_status
            .metadata_shards
            .iter()
            .filter(|shard| shard.primary_node_id.as_ref() == Some(&self.node_id))
            .flat_map(|shard| &shard.replica_lag)
            .filter(|replica| replica.lag_entries != 0)
            .count();
        let metrics = SifsFileManagerObservationMetrics {
            object_count: operator_status.object_count,
            open_conflict_count: operator_status.open_conflict_count,
            repair_job_count: operator_status.pending_repair_count,
            repair_attempt_count: operator_status.repair_attempt_count,
            pending_write_count: operator_status.pending_write_count,
            pending_metadata_replication_count,
            cross_shard_namespace_lock_count: cross_shard_namespace_locks.len(),
        };
        Ok(SifsFileManagerObservationSnapshot {
            metrics,
            operator_status,
        })
    }
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
        let service =
            open_sifs_service_read_only_until(data_dir, &rpc_runtime, operation_deadline)?;
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
                *refreshed_at = Some(Instant::now());
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
                *refreshed_at = Some(Instant::now());
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
        }
        // Peer discovery is an observational hot path. The daemon already
        // projects committed membership into the durable bootstrap state; a
        // file operation must not download and rematerialize the full cluster
        // KV snapshot merely to refresh the transport's authorized node set.
        let Ok(state) = cluster::read_cluster_state_without_kv_cached_shared(&cluster_paths) else {
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
        if let Err(error) = self.prepare_metadata_after_mutation(operation_deadline) {
            // The namespace/content mutation has already crossed its durable
            // commit edge. Detached sends and metadata publication are
            // replay-safe maintenance owned by the daemon; reporting their
            // deadline as a mutation failure would invite an unsafe retry of
            // an operation which already took effect.
            eprintln!(
                "warning: follow-up DFS maintenance was deferred after a committed mutation; durable daemon maintenance will retry: {error}"
            );
            return Ok(());
        }
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
        if let Ok(mut local) = service.posix_open_file_stream(&path) {
            let size_bytes = usize::try_from(local.size_bytes)
                .map_err(|_| format!("cannot read SIFS file {path}: length does not fit usize"))?;
            let mut bytes = Vec::new();
            bytes.try_reserve_exact(size_bytes).map_err(|_| {
                format!("cannot read SIFS file {path}: cannot reserve {size_bytes} bytes")
            })?;
            local
                .reader
                .read_to_end(&mut bytes)
                .map_err(|error| format!("cannot read SIFS file {path}: {error}"))?;
            return Ok(bytes);
        }

        // `FileManagerBackend` remains a Vec-returning compatibility API, but
        // remote DFS now returns its response head before any body bytes and
        // preserves transport-owned chunks until this final boundary.
        let remote = block_on_sifs_posix_future(
            &self.rpc_runtime,
            service.posix_open_remote_file_stream(&path),
        )
        .map_err(|error| format!("cannot read SIFS file {path}: {error}"))?;
        drop(service);
        block_on_sifs_posix_future(&self.rpc_runtime, remote.collect_to_vec())
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

    pub(crate) fn resolved_placement(
        &self,
        stat: &DfsStat,
    ) -> Result<DfsResolvedPlacementPolicy, String> {
        let mut service = self.lock_service()?;
        service
            .resolved_placement_policy_for_path(
                &stat.path,
                stat.policy.clone(),
                stat.current_locations.len().max(1),
            )
            .map_err(|error| format!("cannot resolve SIFS placement for {}: {error}", stat.path))
    }

    fn preview_materialized_placement_with_paged_impact(
        &self,
        base_config: &DfsFileSystemConfig,
        config: &DfsFileSystemConfig,
        stat: &DfsStat,
        candidates: Vec<DfsNodeAdvertisement>,
        observer_node_id: NodeId,
        scope: DfsPlacementScope,
        cluster_paths: &ClusterPaths,
    ) -> Result<
        (
            DfsPlacementPreview,
            BTreeSet<NodeId>,
            Vec<DfsNodeAdvertisement>,
        ),
        String,
    > {
        if candidates.is_empty() {
            return Err("cannot apply SIFS placement without content candidates".to_string());
        }
        let object_fence = sinomo_runtime::dfs::DfsPlacementPreviewObjectFence::from(stat);
        let connectivity = sifs_placement_connectivity_snapshot(cluster_paths);
        let unavailable_node_ids = connectivity.explicitly_unavailable_node_ids();
        let mut preview = preview_placement_with_unavailable_nodes(
            config,
            &stat.path,
            stat.object_id.clone(),
            stat.version.clone(),
            stat.policy.clone(),
            observer_node_id,
            stat.current_locations.clone(),
            candidates.clone(),
            // The caller already materialized additions and removals in this
            // config. In particular, None here must preserve inherited-policy
            // previews instead of accidentally re-adding the removed rule.
            None,
            scope,
            &unavailable_node_ids,
        )
        .map_err(|error| format!("cannot preview SIFS placement: {error}"))?;

        let mut impact = DfsPlacementImpact::default();
        let mut cursor = None;
        loop {
            // The DFS service is shared by foreground file-manager work. Hold
            // its mutex for one bounded metadata page, then release it before
            // resuming from the returned cursor.
            let page = {
                let service = self.lock_service()?;
                service
                    .placement_impact_page_for_config_with_unavailable_nodes(
                        config,
                        &stat.path,
                        scope,
                        &candidates,
                        cursor.as_ref(),
                        SIFS_PLACEMENT_IMPACT_PAGE_SIZE,
                        &unavailable_node_ids,
                    )
                    .map_err(|error| format!("cannot preview SIFS placement impact: {error}"))?
            };
            sinomo_runtime::dfs::merge_placement_impact(&mut impact, &page.impact);
            merge_sifs_placement_page_diagnostics(&mut preview, page.blocking_diagnostics);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        if !preview.blocking_diagnostics.is_empty() {
            impact.complete = false;
        }
        preview.impact = impact;
        // Impact scans can span multiple pages. Refresh the observational view
        // once afterward so a desired node that transitioned down cannot slip
        // through the apply gate on a stale plan.
        let refreshed_connectivity = sifs_placement_connectivity_snapshot(cluster_paths);
        enrich_sifs_placement_preview_reachability(&mut preview, &refreshed_connectivity);
        let compatibility_state = cluster::read_cluster_state_without_kv(cluster_paths)
            .map_err(|error| format!("cannot verify placement daemon compatibility: {error}"))?;
        if let Some(diagnostic) =
            sinomo_runtime::dfs::placement_reconciliation_compatibility_diagnostic(
                &compatibility_state,
            )
            && !preview.blocking_diagnostics.contains(&diagnostic)
        {
            preview.blocking_diagnostics.push(diagnostic);
            preview.impact.complete = false;
        }
        // Preserve the exact exclusions used for planning even if a node
        // recovers during the paged scan, and also include nodes that became
        // explicitly unavailable before apply.
        let mut reconciliation_unavailable_node_ids = unavailable_node_ids;
        reconciliation_unavailable_node_ids.extend(preview.unavailable_node_ids());
        sinomo_runtime::dfs::bind_placement_preview_token(
            &mut preview,
            base_config,
            config,
            &stat.path,
            scope,
            &object_fence,
            &candidates,
            &reconciliation_unavailable_node_ids,
        )
        .map_err(|error| format!("cannot seal SIFS placement preview: {error}"))?;
        Ok((preview, reconciliation_unavailable_node_ids, candidates))
    }

    pub(crate) fn preview_placement(
        &self,
        path: &str,
        policy: Option<DfsPlacementPolicy>,
        scope: DfsPlacementScope,
    ) -> Result<DfsPlacementPreview, String> {
        let path = normalize_sifs_path(path)?;
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        if !cluster_paths.state.exists() {
            return Err("managed SIFS placement requires a cluster".to_string());
        }
        refresh_sifs_cluster_kv_snapshot(&self.data_dir, &cluster_paths)?;
        let mut kv = SifsControlPlaneKvClient::new(&self.data_dir, &cluster_paths);
        let base_config = DfsControlPlaneStore::new(&mut kv)
            .get_file_system_config(&sifs_file_system_id())
            .map_err(|error| format!("cannot read SIFS placement policy: {error}"))?
            .ok_or_else(|| "SIFS cluster policy is unavailable".to_string())?;
        let mut config = base_config.clone();
        match policy {
            Some(policy) => {
                let rule = sinomo_runtime::dfs::normalize_placement_rule(&path, scope, policy)
                    .map_err(|error| format!("invalid SIFS placement policy: {error}"))?;
                sinomo_runtime::dfs::upsert_placement_rule(&mut config.subtree_policies, rule);
            }
            None => {
                sinomo_runtime::dfs::remove_placement_rule(
                    &mut config.subtree_policies,
                    &path,
                    scope,
                )
                .map_err(|error| format!("invalid SIFS placement path: {error}"))?;
            }
        }
        let (stat, candidates, observer_node_id) = {
            let mut service = self.lock_service()?;
            self.refresh_service_peer_targets(&mut service);
            let stat = service
                .posix_stat_distributed(&path)
                .map_err(|error| format!("cannot inspect {path}: {error}"))?;
            let candidates = service
                .placement_content_candidates()
                .map_err(|error| format!("cannot read SIFS placement candidates: {error}"))?;
            (stat, candidates, service.node_id().clone())
        };
        let (preview, _, _) = self.preview_materialized_placement_with_paged_impact(
            &base_config,
            &config,
            &stat,
            candidates,
            observer_node_id,
            scope,
            &cluster_paths,
        )?;
        Ok(preview)
    }

    pub(crate) fn update_placement(
        &self,
        path: &str,
        policy: Option<DfsPlacementPolicy>,
        scope: DfsPlacementScope,
        expected_preview_token: DfsPlacementPreviewToken,
    ) -> Result<(DfsPlacementPreview, DfsPlacementReconciliationJob), String> {
        let path = normalize_sifs_path(path)?;
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        if !cluster_paths.state.exists() {
            return Err("managed SIFS placement requires a cluster".to_string());
        }
        refresh_sifs_cluster_kv_snapshot(&self.data_dir, &cluster_paths)?;
        let (stat, candidates, observer_node_id) = {
            let mut service = self.lock_service()?;
            self.refresh_service_peer_targets(&mut service);
            let stat = service
                .posix_stat_distributed(&path)
                .map_err(|error| format!("cannot inspect {path}: {error}"))?;
            let candidates = service
                .placement_content_candidates()
                .map_err(|error| format!("cannot read SIFS placement candidates: {error}"))?;
            (stat, candidates, service.node_id().clone())
        };
        let mut kv = SifsControlPlaneKvClient::new(&self.data_dir, &cluster_paths);
        let mut store = DfsControlPlaneStore::new(&mut kv);
        let expected = store
            .get_file_system_config(&sifs_file_system_id())
            .map_err(|error| format!("cannot read SIFS placement policy: {error}"))?
            .ok_or_else(|| "SIFS cluster policy is unavailable".to_string())?;
        let mut desired = expected.clone();
        match policy {
            Some(policy) => {
                let rule = sinomo_runtime::dfs::normalize_placement_rule(&path, scope, policy)
                    .map_err(|error| format!("invalid SIFS placement policy: {error}"))?;
                sinomo_runtime::dfs::upsert_placement_rule(&mut desired.subtree_policies, rule);
            }
            None => {
                sinomo_runtime::dfs::remove_placement_rule(
                    &mut desired.subtree_policies,
                    &path,
                    scope,
                )
                .map_err(|error| format!("invalid SIFS placement path: {error}"))?;
            }
        }
        let (preview, unavailable_node_ids, candidate_snapshot) = self
            .preview_materialized_placement_with_paged_impact(
                &expected,
                &desired,
                &stat,
                candidates,
                observer_node_id,
                scope,
                &cluster_paths,
            )?;
        if !expected_preview_token.is_well_formed()
            || expected_preview_token != preview.preview_token
        {
            return Err(
                "SIFS placement preview is stale; refresh the preview and review the updated plan before applying"
                    .to_string(),
            );
        }
        if !preview.is_satisfiable() {
            return Err(sifs_placement_blocking_error(&preview.blocking_diagnostics));
        }
        let now = time::OffsetDateTime::now_utc()
            .unix_timestamp_nanos()
            .try_into()
            .unwrap_or(1_000_000_000);
        let job = DfsPlacementReconciliationJob {
            schema_version: sinomo_runtime::dfs::DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION,
            job_id: sinomo_runtime::core::Id::generate().to_string(),
            file_system_id: desired.file_system_id.clone(),
            path,
            scope,
            unavailable_node_ids,
            candidate_snapshot,
            accepted_config: Some(desired.clone()),
            state: DfsPlacementReconciliationState::Pending,
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
            .compare_and_put_placement_update(&expected, &desired, &job)
            .map_err(|error| format!("cannot save SIFS placement policy: {error}"))?
        {
            return Err(
                "SIFS placement policy changed concurrently; refresh the preview and retry"
                    .to_string(),
            );
        }
        let job = store
            .placement_reconciliation(&desired.file_system_id, &job.job_id)
            .map_err(|error| format!("cannot read accepted SIFS placement job: {error}"))?
            .ok_or_else(|| {
                "accepted SIFS placement job is missing from the control plane".to_string()
            })?;
        // Reconciliation belongs to the main daemon, whose exclusive
        // data-owner lock makes prior-session permit recovery safe. A short-
        // lived CLI process must never become an indistinguishable placement
        // authority holder while that daemon remains active.
        Ok((preview, job))
    }

    pub(crate) fn placement_progress(
        &self,
        job_id: &str,
    ) -> Result<DfsPlacementReconciliationJob, String> {
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        refresh_sifs_cluster_kv_snapshot(&self.data_dir, &cluster_paths)?;
        let mut kv = SifsControlPlaneKvClient::new(&self.data_dir, &cluster_paths);
        DfsControlPlaneStore::new(&mut kv)
            .placement_reconciliation(&sifs_file_system_id(), job_id)
            .map_err(|error| format!("cannot read SIFS placement progress: {error}"))?
            .ok_or_else(|| format!("unknown SIFS placement reconciliation job: {job_id}"))
    }

    pub(crate) fn placement_activity(
        &self,
        statuses: &[DfsPlacementReconciliationActivityStatus],
        path_prefix: Option<&str>,
        cursor: Option<&DfsPlacementReconciliationActivityCursor>,
        limit: usize,
    ) -> Result<DfsPlacementReconciliationActivityPage, String> {
        if cursor.is_some_and(|cursor| cursor.job_id.trim().is_empty()) {
            return Err("SIFS placement activity cursor has an empty job id".to_string());
        }
        let path_prefix = path_prefix.map(normalize_sifs_path).transpose()?;
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        refresh_sifs_cluster_kv_snapshot(&self.data_dir, &cluster_paths)?;
        let mut kv = SifsControlPlaneKvClient::new(&self.data_dir, &cluster_paths);
        let page = DfsControlPlaneStore::new(&mut kv)
            .placement_reconciliation_activity(
                &sifs_file_system_id(),
                statuses,
                path_prefix.as_deref(),
                cursor,
                limit,
            )
            .map_err(|error| format!("cannot read SIFS placement activity: {error}"))?;
        Ok(page)
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
        self.write_file_with_options(
            path,
            bytes,
            false,
            false,
            true,
            SifsWriteParentPolicy::MaterializeMissing,
            None,
        )
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
        let options = match sifs_write_options(&mut service, &path, &self.replica_candidates) {
            Ok(options) => options,
            Err(detail) => return Ok(Outcome::Rejected { detail }),
        };
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
        self.write_file_with_options(
            path,
            bytes,
            false,
            false,
            true,
            SifsWriteParentPolicy::MaterializeMissing,
            Some(deadline),
        )
    }

    pub(crate) fn write_file_wait_for_commit_with_deadline(
        &self,
        path: &str,
        bytes: &[u8],
        deadline: Instant,
    ) -> Result<(), String> {
        self.write_file_with_options(
            path,
            bytes,
            false,
            true,
            true,
            SifsWriteParentPolicy::MaterializeMissing,
            Some(deadline),
        )
    }

    /// Writes only while the path's parent remains present.
    ///
    /// The authoritative DFS create transaction rechecks and touches that
    /// parent atomically with the child publication. Do not materialize a
    /// missing parent in this wrapper: if a concurrent `rmdir` wins, recreating
    /// the directory as a separate mutation can leave it visible when the
    /// child request later fails or has an indeterminate delivery result.
    pub(crate) fn write_file_with_existing_parent_wait_for_commit_with_deadline(
        &self,
        path: &str,
        bytes: &[u8],
        deadline: Instant,
    ) -> Result<(), String> {
        self.write_file_with_options(
            path,
            bytes,
            false,
            true,
            true,
            SifsWriteParentPolicy::RequireExisting,
            Some(deadline),
        )
    }

    pub(crate) fn write_file_with_foreground_replica_repair_deadline(
        &self,
        path: &str,
        bytes: &[u8],
        deadline: Instant,
    ) -> Result<(), String> {
        self.write_file_with_options(
            path,
            bytes,
            true,
            true,
            true,
            SifsWriteParentPolicy::MaterializeMissing,
            Some(deadline),
        )
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
        parent_policy: SifsWriteParentPolicy,
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
                sifs_write_options(&mut service, &path, &self.replica_candidates)?
            } else {
                sifs_deferred_publish_write_options(&mut service, &path, &self.replica_candidates)?
            };
            options.foreground_replica_repair = foreground_replica_repair;
            let create_missing_parents = parent_policy == SifsWriteParentPolicy::MaterializeMissing;
            if foreground_replica_repair && create_missing_parents {
                if let Some(parent) = parent_sifs_path(&path) {
                    service.posix_mkdir_all(&parent).map_err(|error| {
                        format!("cannot create SIFS directory {parent}: {error}")
                    })?;
                }
                service
                    .posix_write_file(&path, bytes, options)
                    .map(|_| ())
                    .map_err(|error| format!("cannot write SIFS file {path}: {error}"))
            } else if create_missing_parents {
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
            } else {
                service
                    .posix_write_file(&path, bytes, options)
                    .map(|_| ())
                    .map_err(|error| format!("cannot write SIFS file {path}: {error}"))
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

    #[cfg(test)]
    pub(crate) fn write_files_with_known_parents_deferred_publish_until(
        &self,
        files: &[(String, Vec<u8>)],
        deadline: Instant,
    ) -> Result<(), String> {
        self.write_files_with_known_parents_deferred_publish_mode_until(files, false, deadline)
    }

    pub(crate) fn write_files_with_known_parents_deferred_publish_mode_until(
        &self,
        files: &[(String, Vec<u8>)],
        overwrite_existing: bool,
        deadline: Instant,
    ) -> Result<(), String> {
        self.write_files_with_known_parents_deferred_publish_with_deadline(
            files,
            overwrite_existing,
            Some(deadline),
        )
    }

    fn write_files_with_known_parents_deferred_publish_with_deadline(
        &self,
        files: &[(String, Vec<u8>)],
        overwrite_existing: bool,
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
            let files = normalized
                .iter()
                .map(|(path, bytes)| (path.as_str(), *bytes));
            if overwrite_existing {
                service.posix_write_files_local_eventual_with_known_parents(files)
            } else {
                service.posix_create_files_local_eventual_with_known_parents(files)
            }
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
            let options = sifs_write_options(&mut service, &path, &self.replica_candidates)?;
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
            let options =
                sifs_deferred_publish_write_options(&mut service, &path, &self.replica_candidates)?;
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
        let options =
            sifs_deferred_publish_write_options(&mut service, &path, &self.replica_candidates)?;
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

impl SifsOneShotBulkCreateRequester {
    pub(crate) fn open(data_dir: &SinomoDataDir) -> Result<Self, String> {
        data_dir
            .ensure_root_layout()
            .map_err(|error| format!("cannot prepare Sinomo data directory: {error}"))?;
        let cluster_paths = ClusterPaths::new(data_dir);
        if !cluster_paths.state.exists() {
            return Err(
                "one-shot SIFS bulk create requires local cluster state and a resident daemon"
                    .to_string(),
            );
        }

        // A successful scoped daemon refresh is the freshness proof for the
        // catalog consumed below. Unlike the general backend, this requester
        // must never fall back to direct routing or an old local snapshot.
        refresh_sifs_cluster_kv_snapshot(data_dir, &cluster_paths)?;
        let state = cluster::read_cluster_state_without_kv(&cluster_paths)
            .map_err(|error| format!("cannot read local cluster state: {error}"))?;
        let peer_node_ids = cluster_dfs_peer_node_ids(&state);
        let local_node_id = state.local_node_id;

        let peer_transport = sinomo_runtime::api::DaemonDfsPeerTransport::connect(data_dir.clone())
            .map_err(|error| {
                format!(
                    "one-shot SIFS bulk create requires the resident daemon DFS transport: {error}"
                )
            })?;
        let (sync_peer_transport, async_peer_transport) =
            shared_sifs_peer_transports(peer_transport);
        let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))
            .map_err(|error| format!("cannot open DFS content store: {error}"))?;

        let control_plane = SifsOneShotBulkCreateControlPlaneKvClient::new(
            data_dir,
            &cluster_paths,
            local_node_id.clone(),
        );
        // The scoped refresh above already installed the latest snapshot. Load
        // that exact materialization without paying a second daemon barrier.
        control_plane
            .inner
            .replace_cached_snapshot(false)
            .map_err(|error| format!("cannot load one-shot DFS shard catalog: {error}"))?;
        control_plane
            .validate_cached_catalog()
            .map_err(|error| format!("cannot validate one-shot DFS shard catalog: {error}"))?;

        let mut service = NetworkedDfsService::new(
            DfsNodeTransport::with_peer_transports_for_nodes(
                local_node_id.clone(),
                peer_node_ids,
                sinomo_runtime::network::DEFAULT_NETWORK_TIMEOUT,
                sync_peer_transport,
                async_peer_transport,
            ),
            content,
        )
        .with_control_plane(control_plane.clone());
        service
            .bootstrap_distributed_sifs()
            .map_err(|error| format!("cannot bootstrap one-shot SIFS bulk create: {error}"))?;

        Ok(Self {
            data_dir: data_dir.clone(),
            local_node_id,
            service: Mutex::new(service),
            control_plane,
        })
    }

    pub(crate) fn node_label(&self) -> String {
        self.local_node_id.to_string()
    }

    fn validate_cached_authority(
        &self,
    ) -> Result<(Vec<NodeId>, Vec<DfsMetadataShardConfig>), String> {
        // `open` acquired a fresh authoritative snapshot for this one-shot
        // request. Reuse that installed generation across mkdir, create, and
        // publication instead of paying three additional daemon/KV barriers.
        //
        // This does not weaken the authority fence: every actual
        // control-plane refresh performed by the DFS service goes through the
        // validating wrapper below, and stale routed epochs are rejected by
        // the authoritative daemon. A stopped daemon also remains fatal
        // because this requester has no direct peer-transport fallback.
        let shards = self
            .control_plane
            .validated_cached_catalog()
            .map_err(|error| format!("cannot validate one-shot DFS shard catalog: {error}"))?;
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        let state = cluster::read_cluster_state_without_kv(&cluster_paths)
            .map_err(|error| format!("cannot read local cluster state: {error}"))?;
        if state.local_node_id != self.local_node_id {
            return Err(format!(
                "one-shot SIFS bulk create is fenced because the local node identity changed from {} to {}",
                self.local_node_id, state.local_node_id
            ));
        }
        Ok((cluster_dfs_peer_node_ids(&state), shards))
    }

    fn lock_service_with_deadline(
        &self,
        deadline: Instant,
    ) -> Result<std::sync::MutexGuard<'_, NetworkedDfsService<DfsNodeTransport>>, String> {
        loop {
            match self.service.try_lock() {
                Ok(service) => return Ok(service),
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err("one-shot SIFS bulk create service lock is poisoned".to_string());
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    let remaining = sifs_operation_remaining(
                        Some(deadline),
                        "waiting for the one-shot SIFS bulk create service lock",
                    )?
                    .expect("deadline is present");
                    std::thread::sleep(Duration::from_millis(1).min(remaining));
                }
            }
        }
    }

    pub(crate) fn create_dir_all_deferred_local_replay_until(
        &self,
        path: &str,
        deadline: Instant,
    ) -> Result<(), String> {
        let path = normalize_sifs_path(path)?;
        let parent = sifs_parent_path(&path);
        let (peer_node_ids, shards) = self.validate_cached_authority()?;
        validate_one_shot_bulk_create_paths(
            &shards,
            std::iter::once(path.as_str()).chain(parent.iter().map(String::as_str)),
        )?;
        let mut service = self.lock_service_with_deadline(deadline)?;
        service.refresh_peer_targets(peer_node_ids);
        let previous_deadline = service.set_transport_operation_deadline(Some(deadline));
        let result = service
            .posix_mkdir_all_without_local_replay(&path)
            .map_err(|error| format!("cannot create SIFS directory {path}: {error}"));
        service.set_transport_operation_deadline(previous_deadline);
        result
    }

    pub(crate) fn write_files_with_known_parents_deferred_publish_until(
        &self,
        files: &[(String, Vec<u8>)],
        deadline: Instant,
    ) -> Result<(), String> {
        let normalized = files
            .iter()
            .map(|(path, bytes)| normalize_sifs_path(path).map(|path| (path, bytes.as_slice())))
            .collect::<Result<Vec<_>, _>>()?;
        sifs_operation_remaining(Some(deadline), "one-shot SIFS bulk create")?;
        let (peer_node_ids, shards) = self.validate_cached_authority()?;
        validate_one_shot_bulk_create_paths(
            &shards,
            normalized.iter().map(|(path, _)| path.as_str()),
        )?;
        let mut service = self.lock_service_with_deadline(deadline)?;
        service.refresh_peer_targets(peer_node_ids);
        let previous_deadline = service.set_transport_operation_deadline(Some(deadline));
        let result = service
            .posix_create_files_local_eventual_with_known_parents(
                normalized
                    .iter()
                    .map(|(path, bytes)| (path.as_str(), *bytes)),
            )
            .map(|_| ())
            .map_err(|error| format!("cannot write SIFS bulk batch: {error}"));
        service.set_transport_operation_deadline(previous_deadline);
        result
    }

    pub(crate) fn publish_metadata_after_mutation(&self) -> Result<(), String> {
        let (peer_node_ids, _) = self.validate_cached_authority()?;
        {
            let mut service = self
                .service
                .lock()
                .map_err(|_| "one-shot SIFS bulk create service lock is poisoned".to_string())?;
            service.refresh_peer_targets(peer_node_ids);
            service
                .flush_detached_transport_sends()
                .map_err(|error| error.to_string())?;
            service
                .process_incoming_until_pending_routed_writes_ready(
                    network::DEFAULT_NETWORK_TIMEOUT,
                )
                .map_err(|error| error.to_string())?;
        }
        if !request_sifs_daemon_maintenance_refresh(
            &self.data_dir,
            SIFS_MAINTENANCE_REFRESH_TIMEOUT,
        ) {
            return Err(
                "resident daemon became unavailable during one-shot SIFS bulk create maintenance handoff"
                    .to_string(),
            );
        }
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
    open_sifs_service_read_only_until(data_dir, rpc_runtime, None)
}

fn open_sifs_service_read_only_until(
    data_dir: &SinomoDataDir,
    rpc_runtime: &SifsRpcRuntime,
    operation_deadline: Option<Instant>,
) -> Result<NetworkedDfsService<DfsNodeTransport>, String> {
    sifs_operation_remaining(operation_deadline, "reading local cluster state")?;
    let cluster_paths = ClusterPaths::new(data_dir);
    let (local_node_id, peer_node_ids) = if cluster_paths.state.exists() {
        let state = cluster::read_cluster_state_without_kv(&cluster_paths)
            .map_err(|error| format!("cannot read local cluster state: {error}"))?;
        let peer_node_ids = cluster_dfs_peer_node_ids(&state);
        (state.local_node_id, peer_node_ids)
    } else {
        (fallback_dfs_node_id(), Vec::new())
    };
    sifs_operation_remaining(operation_deadline, "opening the DFS content store")?;
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))
        .map_err(|error| format!("cannot open DFS content store: {error}"))?;
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
            local_node_id.clone(),
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
    sifs_operation_remaining(operation_deadline, "opening read-only DFS metadata")?;
    let metadata = open_cluster_aware_dfs_metadata_backend_for_inspection(data_dir, &cluster_paths)
        .map_err(|error| format!("cannot open read-only SIFS metadata: {error}"))?;
    let service = service
        .with_boxed_metadata_backend_lazy(metadata)
        .map_err(|error| format!("cannot open read-only SIFS metadata: {error}"))?;
    sifs_operation_remaining(operation_deadline, "opening read-only DFS metadata")?;
    Ok(service)
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
    let local_snapshot_refresh = refresh_sifs_cluster_kv_snapshot(data_dir, &cluster_paths);
    sifs_operation_remaining(operation_deadline, "refreshing the local cluster snapshot")?;
    if local_snapshot_refresh.is_err() {
        // The scoped local-daemon refresh already falls back to reachable
        // voters. Use the DFS transport only if that complete path failed;
        // repeating both successful barriers made every one-shot SIFS open
        // pay two cluster-KV round trips.
        let _ = refresh_cluster_dfs_kv_snapshot_with_peer_transport(
            &cluster_paths,
            bootstrap_peer_transport,
        );
    }
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
            local_node_id.clone(),
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
    let metadata_shards = read_sifs_metadata_shard_configs(&cluster_paths)?;
    let service = if let Some(metadata) =
        open_sifs_sharded_metadata_backend(data_dir, metadata_shards, &local_node_id)?
    {
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

/// Snapshot-only control-plane reader for strict observation paths.
///
/// Unlike the ordinary CLI client, a miss never probes the daemon and the
/// mutation methods are hard failures. The daemon owns reactive publication of
/// the local snapshot; an observer consumes that state without becoming a new
/// scheduling edge.
#[derive(Clone, Debug)]
struct SifsSnapshotControlPlaneKvClient {
    cluster_paths: ClusterPaths,
    snapshot: Arc<Mutex<SifsKvSnapshotCache>>,
}

#[derive(Debug, Default)]
struct SifsKvSnapshotCache {
    loaded: bool,
    snapshot: Option<Arc<KvSnapshot>>,
}

impl SifsSnapshotControlPlaneKvClient {
    fn new(cluster_paths: &ClusterPaths) -> Self {
        Self {
            cluster_paths: cluster_paths.clone(),
            snapshot: Arc::new(Mutex::new(SifsKvSnapshotCache::default())),
        }
    }

    fn kv_error(&self, detail: impl Into<String>) -> KvError {
        KvError::Io {
            path: self.cluster_paths.kv_paths().snapshot.display().to_string(),
            detail: detail.into(),
        }
    }

    fn cached_snapshot(&self) -> Result<Option<Arc<KvSnapshot>>, KvError> {
        {
            let cache = self
                .snapshot
                .lock()
                .map_err(|_| self.kv_error("SIFS snapshot cache lock is poisoned"))?;
            if cache.loaded {
                return Ok(cache.snapshot.clone());
            }
        }
        let snapshot =
            cluster::read_cluster_kv_latest_observed_snapshot_arc_if_present(&self.cluster_paths)
                .map_err(|source| self.kv_error(source.to_string()))?;
        let mut cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("SIFS snapshot cache lock is poisoned"))?;
        cache.loaded = true;
        cache.snapshot = snapshot.clone();
        Ok(snapshot)
    }
}

impl DfsControlPlaneKvClient for SifsSnapshotControlPlaneKvClient {
    fn get(&self, key: &str) -> Result<Option<sinomo_runtime::kv::KvEntry>, KvError> {
        Ok(self
            .cached_snapshot()?
            .and_then(|snapshot| snapshot.get(key).cloned()))
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<sinomo_runtime::kv::KvEntry>, KvError> {
        self.cached_snapshot()?.map_or_else(
            || Ok(Vec::new()),
            |snapshot| {
                snapshot.range_prefix_limited(
                    prefix,
                    sinomo_runtime::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
                )
            },
        )
    }

    fn put(&mut self, _key: String, _value: Vec<u8>) -> Result<KvRevision, KvError> {
        Err(self.kv_error("SIFS observation cannot mutate cluster KV"))
    }

    fn txn(&mut self, _txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        Err(self.kv_error("SIFS observation cannot mutate cluster KV"))
    }
}

#[derive(Clone, Debug)]
struct SifsControlPlaneKvClient {
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    snapshot: Arc<Mutex<SifsKvSnapshotCache>>,
}

impl SifsControlPlaneKvClient {
    fn new(data_dir: &SinomoDataDir, cluster_paths: &ClusterPaths) -> Self {
        Self {
            data_dir: data_dir.clone(),
            cluster_paths: cluster_paths.clone(),
            snapshot: Arc::new(Mutex::new(SifsKvSnapshotCache::default())),
        }
    }

    fn kv_error(&self, source: impl std::fmt::Display) -> KvError {
        KvError::Io {
            path: self.cluster_paths.kv_paths().snapshot.display().to_string(),
            detail: source.to_string(),
        }
    }

    fn read_latest_snapshot(&self) -> Result<Option<Arc<KvSnapshot>>, KvError> {
        cluster::read_cluster_kv_latest_observed_snapshot_arc_if_present(&self.cluster_paths)
            .map_err(|source| self.kv_error(source))
    }

    fn replace_cached_snapshot(&self, refresh_daemon: bool) -> Result<(), KvError> {
        if refresh_daemon {
            refresh_sifs_cluster_kv_snapshot(&self.data_dir, &self.cluster_paths)
                .map_err(|source| self.kv_error(source))?;
        }
        let snapshot = self.read_latest_snapshot()?;
        let mut cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("SIFS snapshot cache lock is poisoned"))?;
        cache.loaded = true;
        cache.snapshot = snapshot;
        Ok(())
    }

    fn cached_snapshot(&self) -> Result<Option<Arc<KvSnapshot>>, KvError> {
        {
            let cache = self
                .snapshot
                .lock()
                .map_err(|_| self.kv_error("SIFS snapshot cache lock is poisoned"))?;
            if cache.loaded {
                return Ok(cache.snapshot.clone());
            }
        }
        // A CLI operation refreshes through the long-lived daemon once, then
        // all of its DFS lookups share this immutable snapshot.
        self.replace_cached_snapshot(true)?;
        self.snapshot
            .lock()
            .map_err(|_| self.kv_error("SIFS snapshot cache lock is poisoned"))
            .map(|cache| cache.snapshot.clone())
    }
}

impl DfsControlPlaneKvClient for SifsControlPlaneKvClient {
    fn refresh_latest(&mut self) -> Result<(), KvError> {
        self.replace_cached_snapshot(true)
    }

    fn get(&self, key: &str) -> Result<Option<sinomo_runtime::kv::KvEntry>, KvError> {
        Ok(self
            .cached_snapshot()?
            .and_then(|snapshot| snapshot.get(key).cloned()))
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<sinomo_runtime::kv::KvEntry>, KvError> {
        self.cached_snapshot()?.map_or_else(
            || Ok(Vec::new()),
            |snapshot| {
                snapshot.range_prefix_limited(
                    prefix,
                    sinomo_runtime::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
                )
            },
        )
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
        let response = result.txn_response.ok_or_else(|| KvError::Serialization {
            detail: "cluster KV returned a barrier result for a SIFS control-plane transaction"
                .to_string(),
        })?;
        self.replace_cached_snapshot(false)?;
        Ok(response)
    }
}

/// Control-plane client used by the metadata-less bulk requester.
///
/// The underlying client refreshes its immutable KV snapshot atomically. This
/// wrapper validates each newly installed snapshot exactly once and refuses to
/// expose it if the local node has become a metadata primary. That closes the
/// authority-refresh race between the requester's explicit preflight and an
/// internal DFS retry which refreshes shard routing.
#[derive(Clone, Debug)]
struct SifsOneShotBulkCreateControlPlaneKvClient {
    inner: SifsControlPlaneKvClient,
    local_node_id: NodeId,
    validated_snapshot: Arc<Mutex<Option<Arc<KvSnapshot>>>>,
}

impl SifsOneShotBulkCreateControlPlaneKvClient {
    fn new(data_dir: &SinomoDataDir, cluster_paths: &ClusterPaths, local_node_id: NodeId) -> Self {
        Self {
            inner: SifsControlPlaneKvClient::new(data_dir, cluster_paths),
            local_node_id,
            validated_snapshot: Arc::new(Mutex::new(None)),
        }
    }

    fn validate_cached_catalog(&self) -> Result<(), KvError> {
        self.validated_cached_catalog().map(|_| ())
    }

    fn validated_cached_catalog(&self) -> Result<Vec<DfsMetadataShardConfig>, KvError> {
        let (snapshot, shards) = self.cached_catalog()?;
        {
            let validated = self.validated_snapshot.lock().map_err(|_| {
                self.inner
                    .kv_error("DFS catalog validation lock is poisoned")
            })?;
            if validated
                .as_ref()
                .is_some_and(|validated| Arc::ptr_eq(validated, &snapshot))
            {
                return Ok(shards);
            }
        }

        validate_one_shot_bulk_create_catalog(true, &self.local_node_id, &shards)
            .map_err(|error| self.inner.kv_error(error))?;

        *self.validated_snapshot.lock().map_err(|_| {
            self.inner
                .kv_error("DFS catalog validation lock is poisoned")
        })? = Some(snapshot);
        Ok(shards)
    }

    fn cached_catalog(&self) -> Result<(Arc<KvSnapshot>, Vec<DfsMetadataShardConfig>), KvError> {
        let snapshot = self.inner.cached_snapshot()?.ok_or_else(|| {
            self.inner
                .kv_error("DFS metadata shard catalog is unavailable")
        })?;
        let entries = snapshot.range_prefix_limited(
            &format!("{}/", metadata_shards_prefix(&sifs_file_system_id())),
            sinomo_runtime::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
        )?;
        let shards = decode_sifs_metadata_shard_configs(entries)
            .map_err(|error| self.inner.kv_error(error))?;
        Ok((snapshot, shards))
    }

    fn refresh_and_validate(&mut self) -> Result<(), KvError> {
        self.inner.refresh_latest()?;
        self.validate_cached_catalog()
    }
}

impl DfsControlPlaneKvClient for SifsOneShotBulkCreateControlPlaneKvClient {
    fn refresh_latest(&mut self) -> Result<(), KvError> {
        self.refresh_and_validate()
    }

    fn get(&self, key: &str) -> Result<Option<sinomo_runtime::kv::KvEntry>, KvError> {
        let value = self.inner.get(key)?;
        self.validate_cached_catalog()?;
        Ok(value)
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<sinomo_runtime::kv::KvEntry>, KvError> {
        let entries = self.inner.range_prefix(prefix)?;
        self.validate_cached_catalog()?;
        Ok(entries)
    }

    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
        self.validate_cached_catalog()?;
        let revision = self.inner.put(key, value)?;
        self.validate_cached_catalog()?;
        Ok(revision)
    }

    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        self.validate_cached_catalog()?;
        let response = self.inner.txn(txn)?;
        self.validate_cached_catalog()?;
        Ok(response)
    }
}

fn read_sifs_metadata_shard_configs(
    cluster_paths: &ClusterPaths,
) -> Result<Vec<DfsMetadataShardConfig>, String> {
    if !cluster_paths.state.exists() {
        return Ok(Vec::new());
    }
    let file_system_id = sifs_file_system_id();
    let entries = cluster::read_cluster_kv_latest_observed_prefix_entries(
        cluster_paths,
        &format!("{}/", metadata_shards_prefix(&file_system_id)),
    )
    .map_err(|error| format!("cannot read DFS metadata shard config: {error}"))?;
    decode_sifs_metadata_shard_configs(entries)
}

fn decode_sifs_metadata_shard_configs(
    entries: Vec<sinomo_runtime::kv::KvEntry>,
) -> Result<Vec<DfsMetadataShardConfig>, String> {
    let mut shards = Vec::new();
    for entry in entries {
        shards.push(
            entry
                .decode::<DfsMetadataShardConfig>()
                .map_err(|error| format!("cannot decode DFS metadata shard config: {error}"))?,
        );
    }
    shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    Ok(shards)
}

fn validate_one_shot_bulk_create_catalog(
    daemon_backed_transport: bool,
    local_node_id: &NodeId,
    shards: &[DfsMetadataShardConfig],
) -> Result<(), String> {
    if !daemon_backed_transport {
        return Err(
            "one-shot SIFS bulk create requires the resident daemon DFS transport".to_string(),
        );
    }
    if shards.is_empty() {
        return Err(
            "one-shot SIFS bulk create requires a nonempty metadata shard catalog".to_string(),
        );
    }
    let file_system_id = sifs_file_system_id();
    let mut shard_ids = BTreeSet::new();
    for shard in shards {
        if shard.file_system_id != file_system_id {
            return Err(format!(
                "one-shot SIFS bulk create catalog contains shard {} for filesystem {}",
                shard.shard_id, shard.file_system_id
            ));
        }
        if !shard_ids.insert(shard.shard_id.clone()) {
            return Err(format!(
                "one-shot SIFS bulk create catalog contains duplicate shard id {}",
                shard.shard_id
            ));
        }
    }
    if let Some(shard) = shards
        .iter()
        .find(|shard| shard.primary_node_id == *local_node_id)
    {
        return Err(format!(
            "one-shot SIFS bulk create is fenced because local node {local_node_id} is primary for metadata shard {} at epoch {}",
            shard.shard_id, shard.epoch
        ));
    }
    validate_one_shot_bulk_create_paths(
        shards,
        [
            "/",
            "/services/.catalog-coverage",
            "/users/.catalog-coverage",
        ],
    )?;
    Ok(())
}

fn validate_one_shot_bulk_create_paths<'a>(
    shards: &[DfsMetadataShardConfig],
    paths: impl IntoIterator<Item = &'a str>,
) -> Result<(), String> {
    for path in paths {
        if select_metadata_shard_config_for_path_strict_default_sifs_split(
            &sifs_file_system_id(),
            shards,
            path,
        )
        .map_err(|error| format!("metadata shard catalog cannot route {path}: {error}"))?
        .is_none()
        {
            return Err(format!(
                "one-shot SIFS bulk create metadata shard catalog has no route for {path}"
            ));
        }
    }
    Ok(())
}

fn open_sifs_sharded_metadata_backend(
    data_dir: &SinomoDataDir,
    shards: Vec<DfsMetadataShardConfig>,
    local_node_id: &NodeId,
) -> Result<Option<sinomo_runtime::dfs::LocalShardedMetadataBackend>, String> {
    if shards.is_empty() {
        return Ok(None);
    }
    open_local_sharded_metadata_backend_for_replica_repair(
        default_dfs_metadata_shards_root(data_dir),
        shards,
        local_node_id,
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
    content_node_ids
        .sort_by_key(|node_id| sifs_replica_candidate_rank(&graph, local_node_id, node_id));
    for remote_node_id in content_node_ids {
        if remote_node_id != *local_node_id
            && routable_node_ids.contains(&remote_node_id)
            && graph
                .best_healthy_endpoint(local_node_id, &remote_node_id)
                .is_some()
        {
            candidates.push(remote_node_id);
        }
    }
    candidates
}

#[derive(Clone, Debug, Default)]
struct SifsPlacementConnectivitySnapshot {
    local_node_id: Option<NodeId>,
    graph: Option<network::ClusterConnectivityGraph>,
    advertised_endpoints: Vec<sinomo_runtime::core::ClusterEndpointRecord>,
}

impl SifsPlacementConnectivitySnapshot {
    fn reachability(&self, target_node_id: &NodeId) -> DfsPlacementNodeReachability {
        let Some(local_node_id) = self.local_node_id.as_ref() else {
            return DfsPlacementNodeReachability::Unknown;
        };
        if target_node_id == local_node_id {
            return DfsPlacementNodeReachability::Local;
        }
        let Some(graph) = self.graph.as_ref() else {
            return DfsPlacementNodeReachability::Unknown;
        };
        sifs_placement_node_reachability(
            graph,
            &self.advertised_endpoints,
            local_node_id,
            target_node_id,
        )
    }

    fn explicitly_unavailable_node_ids(&self) -> BTreeSet<NodeId> {
        self.advertised_endpoints
            .iter()
            .map(|endpoint| endpoint.node_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|node_id| {
                self.reachability(node_id) == DfsPlacementNodeReachability::Unreachable
            })
            .collect()
    }
}

fn sifs_placement_connectivity_snapshot(
    cluster_paths: &ClusterPaths,
) -> SifsPlacementConnectivitySnapshot {
    let Ok(state) = cluster::read_cluster_state_without_kv(cluster_paths) else {
        return SifsPlacementConnectivitySnapshot::default();
    };
    let local_node_id = Some(state.local_node_id.clone());

    let mut advertised_endpoints = network::cluster_directory_for(cluster_paths)
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
    // Connectivity is observational. If the graph cannot be read, retain the
    // local identity and directory snapshot but leave remote nodes Unknown
    // instead of treating a missing observation as an outage.
    let graph = network::ClusterConnectivityGraph::try_read_for_local(cluster_paths, &state).ok();
    SifsPlacementConnectivitySnapshot {
        local_node_id,
        graph,
        advertised_endpoints,
    }
}

fn enrich_sifs_placement_preview_reachability(
    preview: &mut DfsPlacementPreview,
    connectivity: &SifsPlacementConnectivitySnapshot,
) {
    for node in &mut preview.nodes {
        node.reachability = connectivity.reachability(&node.node_id);
    }
    enrich_sifs_placement_nodes_from_reachability(
        &mut preview.nodes,
        &mut preview.blocking_diagnostics,
    );
    if !preview.blocking_diagnostics.is_empty() {
        preview.impact.complete = false;
    }
}

fn enrich_sifs_placement_nodes_from_reachability(
    nodes: &mut [DfsPlacementNodePreview],
    blocking_diagnostics: &mut Vec<DfsPlacementBlockingDiagnostic>,
) {
    const DESIRED_BECAME_UNREACHABLE: &str =
        "blocked: this desired replica became explicitly unreachable before apply";

    let mut additional_diagnostics = Vec::new();
    for node in nodes
        .iter_mut()
        .filter(|node| node.reachability == DfsPlacementNodeReachability::Unreachable)
    {
        if node.required {
            additional_diagnostics.push(DfsPlacementBlockingDiagnostic::RequiredNodeUnavailable {
                node_id: node.node_id.clone(),
            });
        }
        if node.desired_replica {
            additional_diagnostics.push(DfsPlacementBlockingDiagnostic::DesiredNodeUnavailable {
                node_id: node.node_id.clone(),
            });
            if !node.explanation.contains(DESIRED_BECAME_UNREACHABLE) {
                if !node.explanation.is_empty() {
                    node.explanation.push_str("; ");
                }
                node.explanation.push_str(DESIRED_BECAME_UNREACHABLE);
            }
        }
    }
    extend_unique_sifs_placement_diagnostics(blocking_diagnostics, additional_diagnostics);
}

fn sifs_placement_node_reachability(
    graph: &network::ClusterConnectivityGraph,
    advertised_endpoints: &[sinomo_runtime::core::ClusterEndpointRecord],
    local_node_id: &NodeId,
    target_node_id: &NodeId,
) -> DfsPlacementNodeReachability {
    if target_node_id == local_node_id {
        return DfsPlacementNodeReachability::Local;
    }
    if network::cluster_route_next_hop(graph, local_node_id, target_node_id).is_some() {
        return DfsPlacementNodeReachability::Reachable;
    }

    let mut target_endpoints = advertised_endpoints
        .iter()
        .filter(|endpoint| &endpoint.node_id == target_node_id)
        .peekable();
    if target_endpoints.peek().is_some()
        && target_endpoints.all(|endpoint| {
            matches!(
                graph.state_for_endpoint(endpoint),
                Some(network::ClusterConnectivityState::Unreachable)
            )
        })
    {
        DfsPlacementNodeReachability::Unreachable
    } else {
        DfsPlacementNodeReachability::Unknown
    }
}

#[cfg(test)]
fn merge_sifs_placement_impact(aggregate: &mut DfsPlacementImpact, subtotal: &DfsPlacementImpact) {
    sinomo_runtime::dfs::merge_placement_impact(aggregate, subtotal);
}

fn extend_unique_sifs_placement_diagnostics(
    target: &mut Vec<DfsPlacementBlockingDiagnostic>,
    diagnostics: impl IntoIterator<Item = DfsPlacementBlockingDiagnostic>,
) {
    for diagnostic in diagnostics {
        if !target.contains(&diagnostic) {
            target.push(diagnostic);
        }
    }
}

fn merge_sifs_placement_page_diagnostics(
    preview: &mut DfsPlacementPreview,
    diagnostics: impl IntoIterator<Item = DfsPlacementBlockingDiagnostic>,
) {
    let anchor_path = preview
        .resolved
        .source_path
        .clone()
        .unwrap_or_else(|| preview.resolved.path.clone());
    let anchor_scope = preview
        .resolved
        .source_scope
        .unwrap_or(DfsPlacementScope::SelfOnly);
    merge_sifs_scoped_placement_diagnostics(
        &mut preview.blocking_diagnostics,
        &anchor_path,
        anchor_scope,
        diagnostics,
    );
}

fn merge_sifs_scoped_placement_diagnostics(
    target: &mut Vec<DfsPlacementBlockingDiagnostic>,
    anchor_path: &str,
    anchor_scope: DfsPlacementScope,
    diagnostics: impl IntoIterator<Item = DfsPlacementBlockingDiagnostic>,
) {
    for diagnostic in diagnostics {
        let duplicates_unscoped_anchor = matches!(
            &diagnostic,
            DfsPlacementBlockingDiagnostic::ScopedPolicy {
                path,
                scope,
                diagnostic,
            } if path == anchor_path
                && *scope == anchor_scope
                && target.contains(diagnostic.as_ref())
        );
        if !duplicates_unscoped_anchor && !target.contains(&diagnostic) {
            target.push(diagnostic);
        }
    }
}

fn sifs_placement_blocking_error(diagnostics: &[DfsPlacementBlockingDiagnostic]) -> String {
    let detail = diagnostics
        .iter()
        .map(DfsPlacementBlockingDiagnostic::message)
        .collect::<Vec<_>>()
        .join("; ");
    if detail.is_empty() {
        "cannot apply unsatisfiable SIFS placement policy".to_string()
    } else {
        format!("cannot apply unsatisfiable SIFS placement policy: {detail}")
    }
}

#[cfg(test)]
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
        call: network::ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> Result<network::ObjectRpcResult, DfsPeerTransportError> {
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
        call: network::ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> DfsPeerTransportFuture<'a, Result<network::ObjectRpcResult, DfsPeerTransportError>> {
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
            sinomo_runtime::daemon::send_object_rpc_call_to_target_async_with_options(
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
    call: network::ObjectRpcCall,
    options: DfsObjectRpcSendOptions,
    operation_deadline: Option<Instant>,
) -> Result<network::ObjectRpcResult, DfsPeerTransportError> {
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
                        sinomo_runtime::daemon::send_object_rpc_call_to_target_async_with_options(
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
            sinomo_runtime::daemon::send_object_rpc_call_to_target_async_with_options(
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
    Unavailable(network::ObjectRpcCall),
    Complete(Result<network::ObjectRpcResult, DfsPeerTransportError>),
}

fn sifs_send_dfs_object_rpc_via_daemon(
    data_dir: &SinomoDataDir,
    target_node_id: &NodeId,
    call: network::ObjectRpcCall,
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
            .object_rpc_call_to_target_with_deadline_async(
                target_node_id,
                call,
                options.connect_timeout,
                options.overall_timeout,
                options.observe_connectivity,
            )
            .await
            .map_err(|error| {
                DfsPeerTransportError::with_delivery_state(
                    error.to_string(),
                    error.delivery_state(),
                )
            })
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

fn block_on_sifs_posix_future<F, T>(
    rpc_runtime: &SifsRpcRuntime,
    future: F,
) -> Result<T, DfsPosixError>
where
    F: Future<Output = Result<T, DfsPosixError>>,
{
    if let Ok(current) = tokio::runtime::Handle::try_current() {
        if matches!(
            current.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            return tokio::task::block_in_place(|| rpc_runtime.runtime.block_on(future));
        }
        return Err(DfsError::Network {
            message:
                "SIFS streamed read cannot block a current-thread Tokio runtime; use the async POSIX stream API"
                    .to_string(),
        }
        .into());
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
    service: &mut NetworkedDfsService<DfsNodeTransport>,
    path: &str,
    replica_candidates: &[NodeId],
) -> Result<DfsWriteOptions, String> {
    let mut candidates = replica_candidates.to_vec();
    if !candidates.contains(service.node_id()) {
        candidates.insert(0, service.node_id().clone());
    }
    let object_policy = service
        .posix_stat(path)
        .map(|stat| stat.policy)
        .unwrap_or(sinomo_runtime::core::DfsObjectPolicy::ReplicatedEventual);
    let (_, advertisements, policy) = service
        .managed_replication_policy_for_path(path, object_policy, candidates.len())
        .map_err(|error| error.to_string())?;
    let advertised_ids = advertisements
        .into_iter()
        .filter(|candidate| candidate.accepts_content)
        .map(|candidate| candidate.node_id)
        .collect::<BTreeSet<_>>();
    candidates.retain(|node_id| advertised_ids.contains(node_id));
    for required in &policy.required_node_ids {
        if !candidates.contains(required) {
            candidates.push(required.clone());
        }
    }
    let mut options = DfsWriteOptions {
        policy,
        replica_candidates: candidates,
        wait_for_commit: true,
        foreground_replica_repair: false,
    };
    options.foreground_replica_repair = false;
    Ok(options)
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
    service: &mut NetworkedDfsService<DfsNodeTransport>,
    path: &str,
    replica_candidates: &[NodeId],
) -> Result<DfsWriteOptions, String> {
    let mut options = sifs_write_options(service, path, replica_candidates)?;
    options.wait_for_commit = false;
    Ok(options)
}

#[cfg(test)]
fn sifs_write_options_for_candidates(candidates: Vec<NodeId>) -> DfsWriteOptions {
    let mut options = DfsWriteOptions::replicated_eventual(candidates);
    options.foreground_replica_repair = false;
    options
}

#[cfg(test)]
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
    use sinomo_runtime::dfs::DfsMetadataBackend as _;
    use std::fs;
    use std::sync::Arc;

    #[derive(Clone, Debug)]
    struct CanonicalPeerTransport {
        response: network::ObjectRpcResponse,
        attachment: Vec<u8>,
    }

    impl CanonicalPeerTransport {
        fn rpc_result(&self) -> network::ObjectRpcResult {
            network::ObjectRpcResult::new(
                self.response.clone(),
                network::ObjectRpcIncomingBody::bytes(self.attachment.clone()),
            )
        }
    }

    impl DfsPeerTransport for CanonicalPeerTransport {
        fn call_object_rpc(
            &self,
            _target_node_id: &NodeId,
            _call: network::ObjectRpcCall,
            _options: DfsObjectRpcSendOptions,
        ) -> Result<network::ObjectRpcResult, DfsPeerTransportError> {
            Ok(self.rpc_result())
        }
    }

    impl AsyncDfsPeerTransport for CanonicalPeerTransport {
        fn call_object_rpc_async<'a>(
            &'a self,
            _target_node_id: &'a NodeId,
            _call: network::ObjectRpcCall,
            _options: DfsObjectRpcSendOptions,
        ) -> DfsPeerTransportFuture<'a, Result<network::ObjectRpcResult, DfsPeerTransportError>>
        {
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
    fn one_shot_bulk_create_catalog_requires_daemon_and_no_local_primary() {
        let primary = NodeId::from_legacy_named("node_stateless_primary").expect("primary");
        let replica = NodeId::from_legacy_named("node_stateless_replica").expect("replica");
        let outsider = NodeId::from_legacy_named("node_stateless_outsider").expect("outsider");
        let shard = DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: primary.clone(),
            replica_node_ids: vec![replica.clone()],
            epoch: 1,
            assignment: sinomo_runtime::dfs::DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: sinomo_runtime::core::DfsObjectPolicy::ReplicatedEventual,
        };

        assert!(
            validate_one_shot_bulk_create_catalog(true, &outsider, std::slice::from_ref(&shard),)
                .is_ok()
        );
        assert!(
            validate_one_shot_bulk_create_catalog(false, &outsider, std::slice::from_ref(&shard),)
                .is_err()
        );
        assert!(
            validate_one_shot_bulk_create_catalog(true, &primary, std::slice::from_ref(&shard),)
                .is_err()
        );
        assert!(
            validate_one_shot_bulk_create_catalog(true, &replica, std::slice::from_ref(&shard),)
                .is_ok()
        );
        assert!(validate_one_shot_bulk_create_catalog(true, &outsider, &[]).is_err());

        let services = DfsMetadataShardConfig {
            shard_id: "services".to_string(),
            assignment: sinomo_runtime::dfs::DfsMetadataShardAssignment::PathPrefix {
                prefix: "/services".to_string(),
            },
            ..shard.clone()
        };
        let incomplete =
            validate_one_shot_bulk_create_catalog(true, &outsider, &[shard.clone(), services])
                .expect_err("a partial default SIFS split must not be treated as complete");
        assert!(
            incomplete.contains("cannot route /users"),
            "unexpected incomplete-catalog fence: {incomplete}"
        );

        let duplicate =
            validate_one_shot_bulk_create_catalog(true, &outsider, &[shard.clone(), shard])
                .expect_err("duplicate shard ids must be rejected");
        assert!(
            duplicate.contains("duplicate shard id"),
            "unexpected duplicate-catalog fence: {duplicate}"
        );
    }

    #[test]
    fn ordinary_sifs_backend_open_always_attaches_metadata() {
        let root = temp_sifs_data_dir("ordinary-open-metadata");
        let data_dir = SinomoDataDir::new(&root);
        let backend = SifsFileManagerBackend::open(&data_dir).expect("ordinary SIFS backend");
        assert!(
            backend
                .lock_service()
                .expect("ordinary SIFS service")
                .metadata_backend()
                .is_some(),
            "ordinary TUI/app/general backends must never use the metadata-less one-shot lane"
        );
    }

    #[test]
    fn one_shot_bulk_create_control_plane_refresh_fences_new_local_primary() {
        let root = temp_sifs_data_dir("one-shot-authority-refresh");
        let data_dir = SinomoDataDir::new(&root);
        let mut shards = seed_observation_cluster(&data_dir);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node_id = cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("cluster state")
            .local_node_id;
        let remote_primary =
            NodeId::from_legacy_named("node_one_shot_remote_primary").expect("remote primary");
        for shard in &mut shards {
            shard.primary_node_id = remote_primary.clone();
            shard.replica_node_ids = vec![local_node_id.clone()];
        }
        write_metadata_shard_snapshot(&cluster_paths, &shards);

        let control_plane = SifsOneShotBulkCreateControlPlaneKvClient::new(
            &data_dir,
            &cluster_paths,
            local_node_id.clone(),
        );
        control_plane
            .inner
            .replace_cached_snapshot(false)
            .expect("load remote-primary catalog");
        control_plane
            .validate_cached_catalog()
            .expect("replica-only catalog is eligible");

        shards[0].primary_node_id = local_node_id;
        shards[0].epoch += 1;
        write_metadata_shard_snapshot(&cluster_paths, &shards);
        control_plane
            .inner
            .replace_cached_snapshot(false)
            .expect("install reassigned catalog");
        let error = control_plane
            .validate_cached_catalog()
            .expect_err("new local primary must fence the metadata-less requester");
        assert!(
            error.to_string().contains("is primary for metadata shard"),
            "unexpected authority fence: {error}"
        );
    }

    #[test]
    fn one_shot_bulk_create_reuses_the_validated_installed_catalog() {
        let root = temp_sifs_data_dir("one-shot-installed-catalog");
        let data_dir = SinomoDataDir::new(&root);
        let mut shards = seed_observation_cluster(&data_dir);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node_id = cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("cluster state")
            .local_node_id;
        let remote_primary =
            NodeId::from_legacy_named("node_one_shot_cached_primary").expect("remote primary");
        for shard in &mut shards {
            shard.primary_node_id = remote_primary.clone();
            shard.replica_node_ids = vec![local_node_id.clone()];
        }
        write_metadata_shard_snapshot(&cluster_paths, &shards);

        let control_plane = SifsOneShotBulkCreateControlPlaneKvClient::new(
            &data_dir,
            &cluster_paths,
            local_node_id,
        );
        control_plane
            .inner
            .replace_cached_snapshot(false)
            .expect("install fresh catalog");
        control_plane
            .validate_cached_catalog()
            .expect("validate fresh catalog");

        let snapshot_path = cluster_paths.kv_paths().snapshot;
        fs::remove_file(&snapshot_path).expect("remove materialized snapshot");
        assert_eq!(
            control_plane
                .validated_cached_catalog()
                .expect("reuse installed catalog"),
            shards,
            "mkdir, bulk create, and publish must share the generation validated at open"
        );
        assert!(
            !snapshot_path.exists(),
            "cached validation must not trigger another daemon/KV materialization"
        );
    }

    #[test]
    fn sifs_observation_backend_does_not_advance_metadata_journal_heads() {
        let root = temp_sifs_data_dir("observation-journal-heads");
        let data_dir = SinomoDataDir::new(&root);
        let shards = seed_observation_cluster(&data_dir);
        let before = metadata_journal_heads(&data_dir, &shards);

        let observation = SifsFileManagerObservation::open(&data_dir).expect("observation");
        for (path, expected_shard) in [
            ("/cluster-tests/layout-probe", "root"),
            ("/services/cluster-tests/layout-probe", "services"),
            ("/users/cluster-tests/layout-probe", "users"),
        ] {
            let shard = observation
                .metadata_shard_for_path(path)
                .expect("observe shard layout")
                .expect("configured shard");
            assert_eq!(shard.shard_id, expected_shard);
        }
        let snapshot = observation
            .operational_snapshot()
            .expect("observe DFS invariants");
        assert_eq!(snapshot.operator_status.metadata_shards.len(), 3);
        assert!(
            observation
                .object_by_path("/absent")
                .expect("observe absent path")
                .is_none()
        );

        assert_eq!(
            metadata_journal_heads(&data_dir, &shards),
            before,
            "layout, invariant, and path observations must not append metadata"
        );
    }

    #[test]
    fn activated_handoff_marker_is_not_a_cold_observation_invariant() {
        let root = temp_sifs_data_dir("observation-activated-handoff-marker");
        let data_dir = SinomoDataDir::new(&root);
        let shards = seed_observation_cluster(&data_dir);
        let root_shard = sinomo_runtime::dfs::DfsMetadataShardId::new("root");
        let mut retiring =
            sinomo_runtime::dfs::open_local_metadata_shard_backend(&data_dir, root_shard.clone())
                .expect("retiring metadata backend");
        retiring
            .freeze_metadata_shard_for_membership_handoff(
                &root_shard,
                "observation-activated-handoff",
                1,
            )
            .expect("install handoff marker")
            .expect("matching local shard");
        assert!(
            retiring
                .mark_metadata_shard_membership_handoff_activated(
                    &root_shard,
                    "observation-activated-handoff",
                    2,
                )
                .expect("activate handoff marker")
        );
        let activated_marker = retiring
            .metadata_shard_write_freeze(&root_shard)
            .expect("read activated marker")
            .expect("durable activated marker");
        drop(retiring);

        let before = metadata_journal_heads(&data_dir, &shards);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let inspection =
            open_cluster_aware_dfs_metadata_backend_for_inspection(&data_dir, &cluster_paths)
                .expect("fresh inspection backend");
        assert!(
            inspection
                .metadata_shard_authoritative_writes_frozen(&root_shard)
                .expect("inspect process-local authority fence"),
            "a fresh inspection backend has no process-local authority binding"
        );
        drop(inspection);

        let snapshot = SifsFileManagerObservation::open(&data_dir)
            .expect("observation")
            .operational_snapshot()
            .expect("activated markers are not durable invariant failures");
        assert_eq!(snapshot.metrics.open_conflict_count, 0);
        assert_eq!(snapshot.metrics.cross_shard_namespace_lock_count, 0);
        assert_eq!(snapshot.metrics.pending_metadata_replication_count, 0);

        let inspection =
            open_cluster_aware_dfs_metadata_backend_for_inspection(&data_dir, &cluster_paths)
                .expect("reopen inspection backend");
        assert_eq!(
            inspection
                .metadata_shard_write_freeze(&root_shard)
                .expect("read marker after observation"),
            Some(activated_marker),
            "observation must not rebind or rewrite the durable handoff marker"
        );
        assert_eq!(
            metadata_journal_heads(&data_dir, &shards),
            before,
            "observing an activated handoff marker must not append metadata"
        );
    }

    #[test]
    fn sifs_service_backed_reader_open_does_not_advance_metadata_journal_heads() {
        let root = temp_sifs_data_dir("read-only-journal-heads");
        let data_dir = SinomoDataDir::new(&root);
        let shards = seed_observation_cluster(&data_dir);
        let before = metadata_journal_heads(&data_dir, &shards);

        let _reader = SifsFileManagerBackend::open_read_only(&data_dir).expect("read-only reader");

        assert_eq!(
            metadata_journal_heads(&data_dir, &shards),
            before,
            "opening a data reader must not bootstrap or recover metadata"
        );
    }

    #[test]
    fn peer_target_refresh_uses_local_membership_without_materializing_cluster_kv() {
        let root = temp_sifs_data_dir("peer-target-local-membership");
        let data_dir = SinomoDataDir::new(&root);
        seed_observation_cluster(&data_dir);
        let backend = SifsFileManagerBackend::open(&data_dir).expect("SIFS backend");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let kv_snapshot = cluster_paths.kv_paths().snapshot;
        fs::remove_file(&kv_snapshot).expect("remove materialized KV snapshot");

        let mut service = backend.lock_service().expect("SIFS service");
        assert!(
            backend.refresh_service_peer_targets_now(&mut service, None),
            "peer refresh must consume daemon-projected local membership without requiring a KV snapshot or daemon RPC"
        );
        assert!(
            !kv_snapshot.exists(),
            "observational peer refresh must not recreate the cluster KV materialization"
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
                network::ObjectRpcCall::value(request()),
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
                network::ObjectRpcCall::value(request()),
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
    fn existing_parent_wait_for_commit_write_does_not_recreate_a_removed_parent() {
        let root = temp_sifs_data_dir("wait-for-commit-removed-parent");
        let data_dir = SinomoDataDir::new(&root);
        let backend = SifsFileManagerBackend::open(&data_dir).expect("backend");
        let parent = "/services/wait-for-commit-removed-parent";
        let child = "/services/wait-for-commit-removed-parent/child.txt";
        backend.create_dir(parent).expect("create parent");
        backend.delete_dir(parent).expect("remove parent");

        let error = backend
            .write_file_with_existing_parent_wait_for_commit_with_deadline(
                child,
                b"must not resurrect parent",
                Instant::now() + Duration::from_secs(5),
            )
            .expect_err("write below a removed parent must fail");

        assert!(
            error.contains("parent directory does not exist")
                || error.contains("not found")
                || error.contains("without parent directory"),
            "unexpected write error: {error}"
        );
        assert!(
            backend.dfs_stat(parent).is_err(),
            "strict child write resurrected removed parent"
        );
        assert!(
            backend.dfs_stat(child).is_err(),
            "strict child write published an orphan"
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
    fn bulk_write_mode_preserves_create_exclusivity_and_can_overwrite_existing_files() {
        let root = temp_sifs_data_dir("bulk-write-overwrite-existing");
        let data_dir = SinomoDataDir::new(&root);
        let backend = SifsFileManagerBackend::open(&data_dir).expect("backend");
        let parent = "/services/bulk-write-overwrite-existing";
        let path = format!("{parent}/payload.bin");
        let initial = vec![(path.clone(), b"initial payload".to_vec())];
        let replacement = vec![(path.clone(), b"replacement payload".to_vec())];

        backend.create_dir_all(parent).expect("create bulk parent");
        backend
            .write_files_with_known_parents_deferred_publish_until(
                &initial,
                Instant::now() + Duration::from_secs(5),
            )
            .expect("create bulk file");
        assert_eq!(
            backend.read_file(&path).expect("read initial bulk file"),
            initial[0].1
        );

        let create_error = backend
            .write_files_with_known_parents_deferred_publish_until(
                &replacement,
                Instant::now() + Duration::from_secs(5),
            )
            .expect_err("default bulk mode must retain create exclusivity");
        assert!(
            create_error.contains("already exists"),
            "unexpected create-only bulk error: {create_error}"
        );

        backend
            .write_files_with_known_parents_deferred_publish_mode_until(
                &replacement,
                true,
                Instant::now() + Duration::from_secs(5),
            )
            .expect("overwrite existing bulk file");
        assert_eq!(
            backend
                .read_file(&path)
                .expect("read replacement bulk file"),
            replacement[0].1
        );
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
    fn placement_reachability_requires_explicit_unreachable_observations() {
        let local = NodeId::from_legacy_named("node_placement_reachability_local").expect("node");
        let reachable =
            NodeId::from_legacy_named("node_placement_reachability_reachable").expect("node");
        let unreachable_required =
            NodeId::from_legacy_named("node_placement_reachability_required").expect("node");
        let unreachable_desired =
            NodeId::from_legacy_named("node_placement_reachability_desired").expect("node");
        let unknown =
            NodeId::from_legacy_named("node_placement_reachability_unknown").expect("node");
        let reachable_record = healthy_connectivity_record(&local, &reachable, 3);
        let required_record = connectivity_record(
            &local,
            &unreachable_required,
            network::ClusterConnectivityState::Unreachable,
            None,
        );
        let desired_record = connectivity_record(
            &local,
            &unreachable_desired,
            network::ClusterConnectivityState::Unreachable,
            None,
        );
        let endpoints = vec![
            connectivity_endpoint(&reachable_record),
            connectivity_endpoint(&required_record),
            connectivity_endpoint(&desired_record),
            connectivity_endpoint(&healthy_connectivity_record(&local, &unknown, 5)),
        ];
        let graph = network::ClusterConnectivityGraph::from_records(
            local.clone(),
            vec![reachable_record, required_record, desired_record],
        );
        let snapshot = SifsPlacementConnectivitySnapshot {
            local_node_id: Some(local.clone()),
            graph: Some(graph),
            advertised_endpoints: endpoints,
        };

        assert_eq!(
            snapshot.reachability(&local),
            DfsPlacementNodeReachability::Local
        );
        assert_eq!(
            snapshot.reachability(&reachable),
            DfsPlacementNodeReachability::Reachable
        );
        assert_eq!(
            snapshot.reachability(&unreachable_required),
            DfsPlacementNodeReachability::Unreachable
        );
        assert_eq!(
            snapshot.reachability(&unknown),
            DfsPlacementNodeReachability::Unknown,
            "an advertised endpoint without a connectivity observation must remain unknown"
        );
        assert_eq!(
            snapshot.explicitly_unavailable_node_ids(),
            BTreeSet::from([unreachable_desired.clone(), unreachable_required.clone(),]),
            "only positively unreachable nodes should be excluded from planning"
        );

        let mut nodes = vec![
            {
                let mut node = placement_node_preview(unreachable_required.clone(), true);
                node.desired_replica = true;
                node
            },
            {
                let mut node = placement_node_preview(unreachable_desired.clone(), false);
                node.desired_replica = true;
                node.explanation = "selected by dry run".to_string();
                node
            },
            placement_node_preview(unknown.clone(), true),
        ];
        for node in &mut nodes {
            node.reachability = snapshot.reachability(&node.node_id);
        }
        let mut diagnostics = Vec::new();
        enrich_sifs_placement_nodes_from_reachability(&mut nodes, &mut diagnostics);
        enrich_sifs_placement_nodes_from_reachability(&mut nodes, &mut diagnostics);
        assert_eq!(
            nodes[0].reachability,
            DfsPlacementNodeReachability::Unreachable
        );
        assert_eq!(nodes[2].reachability, DfsPlacementNodeReachability::Unknown);
        assert!(nodes[0].explanation.contains("explicitly unreachable"));
        assert!(nodes[1].explanation.contains("explicitly unreachable"));
        assert_eq!(
            diagnostics,
            vec![
                DfsPlacementBlockingDiagnostic::RequiredNodeUnavailable {
                    node_id: unreachable_required.clone(),
                },
                DfsPlacementBlockingDiagnostic::DesiredNodeUnavailable {
                    node_id: unreachable_required,
                },
                DfsPlacementBlockingDiagnostic::DesiredNodeUnavailable {
                    node_id: unreachable_desired,
                },
            ],
            "required or selected unavailable nodes should block without duplicates"
        );
    }

    #[test]
    fn placement_activity_rejects_an_empty_cursor_before_cluster_access() {
        let root = temp_sifs_data_dir("placement-activity-empty-cursor");
        let data_dir = SinomoDataDir::new(&root);
        let backend = SifsFileManagerBackend::open(&data_dir).expect("backend");
        let error = backend
            .placement_activity(
                &[],
                None,
                Some(&DfsPlacementReconciliationActivityCursor {
                    created_at_unix_nanos: 1,
                    job_id: "  ".to_string(),
                }),
                50,
            )
            .expect_err("empty activity cursor must be rejected");
        assert!(error.contains("empty job id"));
    }

    #[test]
    fn placement_activity_local_browser_reads_compact_filtered_state() {
        let root = temp_sifs_data_dir("placement-activity-local-browser");
        let data_dir = SinomoDataDir::new(&root);
        // Open before installing the synthetic cluster snapshot so the test
        // remains an isolated local browser and never probes a daemon.
        let backend = SifsFileManagerBackend::open(&data_dir).expect("backend");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let owner =
            NodeId::from_legacy_named("placement-activity-local-owner").expect("activity owner");
        let file_system_id = sifs_file_system_id();
        let base = DfsFileSystemConfig {
            file_system_id: file_system_id.clone(),
            display_name: "SIFS".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
        };
        let policy = DfsPlacementPolicy::new(
            sinomo_runtime::core::DfsObjectPolicy::ReplicatedEventual,
            1,
            1,
        )
        .expect("activity policy");
        let mut desired = base.clone();
        desired.subtree_policies.push(
            sinomo_runtime::dfs::normalize_placement_rule(
                "/team/report.txt",
                DfsPlacementScope::SelfOnly,
                policy,
            )
            .expect("activity rule"),
        );
        let job = DfsPlacementReconciliationJob {
            schema_version: sinomo_runtime::dfs::DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION,
            job_id: "placement-activity-local-job".to_string(),
            file_system_id: file_system_id.clone(),
            path: "/team/report.txt".to_string(),
            scope: DfsPlacementScope::SelfOnly,
            unavailable_node_ids: BTreeSet::new(),
            candidate_snapshot: vec![DfsNodeAdvertisement::content(owner)],
            accepted_config: None,
            state: DfsPlacementReconciliationState::Pending,
            cursor: None,
            total_object_count: 0,
            scanned_object_count: 0,
            matched_object_count: 0,
            updated_object_count: 0,
            scheduled_repair_count: 0,
            completed_repair_count: 0,
            pending_repair_count: 0,
            revision: 0,
            created_at_unix_nanos: 10,
            updated_at_unix_nanos: 10,
            last_error: Some("temporary route failure".to_string()),
        };
        let mut kv = sinomo_runtime::kv::InProcessKvStore::new();
        let mut store = DfsControlPlaneStore::new(&mut kv);
        store.put_file_system_config(&base).expect("base config");
        assert!(
            store
                .compare_and_put_placement_update(&base, &desired, &job)
                .expect("activity Apply")
        );
        drop(store);
        sinomo_runtime::kv::write_cluster_kv_snapshot(
            &cluster_paths.kv_paths(),
            &kv.try_snapshot().expect("activity KV snapshot"),
        )
        .expect("write activity snapshot");

        let page = backend
            .placement_activity(
                &[DfsPlacementReconciliationActivityStatus::Retrying],
                Some("/team/../team"),
                None,
                10,
            )
            .expect("local filtered activity");
        assert_eq!(page.activities.len(), 1);
        assert_eq!(page.activities[0].job_id, job.job_id);
        assert!(
            page.partial,
            "without cluster capability state the local browser must stay conservative"
        );
        assert!(!page.history_truncated);
    }

    #[test]
    fn placement_impact_merge_is_saturating_and_preserves_incomplete_pages() {
        let mut aggregate = DfsPlacementImpact {
            affected_object_count: u64::MAX,
            affected_bytes: 10,
            unknown_size_object_count: 1,
            copies_added: 2,
            copies_removed: 3,
            metadata_snapshot_digest: String::new(),
            complete: true,
        };
        merge_sifs_placement_impact(
            &mut aggregate,
            &DfsPlacementImpact {
                affected_object_count: 1,
                affected_bytes: 20,
                unknown_size_object_count: 4,
                copies_added: 5,
                copies_removed: 6,
                metadata_snapshot_digest: String::new(),
                complete: false,
            },
        );

        assert_eq!(aggregate.affected_object_count, u64::MAX);
        assert_eq!(aggregate.affected_bytes, 30);
        assert_eq!(aggregate.unknown_size_object_count, 5);
        assert_eq!(aggregate.copies_added, 7);
        assert_eq!(aggregate.copies_removed, 9);
        assert!(!aggregate.complete);
    }

    #[test]
    fn placement_page_diagnostics_do_not_duplicate_the_anchor_failure() {
        let root_failure = DfsPlacementBlockingDiagnostic::InsufficientCandidates {
            available: 1,
            required: 2,
        };
        let nested_failure = DfsPlacementBlockingDiagnostic::InsufficientZones {
            available: 1,
            required: 2,
        };
        let mut diagnostics = vec![root_failure.clone()];
        merge_sifs_scoped_placement_diagnostics(
            &mut diagnostics,
            "/services",
            DfsPlacementScope::Subtree,
            [
                DfsPlacementBlockingDiagnostic::ScopedPolicy {
                    path: "/services".to_string(),
                    scope: DfsPlacementScope::Subtree,
                    diagnostic: Box::new(root_failure),
                },
                DfsPlacementBlockingDiagnostic::ScopedPolicy {
                    path: "/services/nested".to_string(),
                    scope: DfsPlacementScope::Subtree,
                    diagnostic: Box::new(nested_failure),
                },
            ],
        );

        assert_eq!(diagnostics.len(), 2);
        assert!(matches!(
            &diagnostics[1],
            DfsPlacementBlockingDiagnostic::ScopedPolicy { path, .. }
                if path == "/services/nested"
        ));
    }

    #[test]
    fn placement_blocking_error_names_each_unsatisfied_constraint() {
        let required =
            NodeId::from_legacy_named("node_placement_required_unavailable").expect("node");
        let error = sifs_placement_blocking_error(&[
            DfsPlacementBlockingDiagnostic::InsufficientCandidates {
                available: 1,
                required: 2,
            },
            DfsPlacementBlockingDiagnostic::RequiredNodeUnavailable {
                node_id: required.clone(),
            },
        ]);

        assert!(error.contains("cannot apply unsatisfiable SIFS placement policy"));
        assert!(error.contains("only 1 eligible content nodes"));
        assert!(error.contains(&required.to_string()));
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

    #[test]
    fn expired_detached_send_budget_does_not_misreport_committed_mutation() {
        let root = temp_sifs_data_dir("committed-mutation-detached-send-deadline");
        let data_dir = SinomoDataDir::new(&root);
        let backend = SifsFileManagerBackend::open(&data_dir).expect("SIFS backend");

        backend
            .publish_metadata_after_mutation_with_deadline(Some(
                Instant::now() - Duration::from_millis(1),
            ))
            .expect("expired follow-up budget must not fail a committed mutation");
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

    fn connectivity_endpoint(
        record: &network::ClusterConnectivityRecord,
    ) -> sinomo_runtime::core::ClusterEndpointRecord {
        sinomo_runtime::core::ClusterEndpointRecord {
            endpoint_id: record.endpoint_id.clone(),
            node_id: record.target_node_id.clone(),
            transport: record.transport.clone(),
            address: record.address.clone(),
            source: sinomo_runtime::core::ClusterEndpointSource::Static,
            privacy: sinomo_runtime::core::ClusterEndpointPrivacy::Lan,
            priority: 0,
            last_observed_latency_ms: record
                .latency_ms
                .and_then(|latency| latency.try_into().ok()),
        }
    }

    fn placement_node_preview(node_id: NodeId, required: bool) -> DfsPlacementNodePreview {
        DfsPlacementNodePreview {
            node_id,
            zone: None,
            capacity_weight: 1,
            accepts_content: true,
            reachability: DfsPlacementNodeReachability::Unknown,
            current_replica: false,
            desired_replica: false,
            preferred_for_commit: false,
            required,
            forbidden: false,
            explanation: String::new(),
        }
    }

    fn seed_observation_cluster(data_dir: &SinomoDataDir) -> Vec<DfsMetadataShardConfig> {
        let cluster_paths = ClusterPaths::new(data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let cluster_id = sinomo_runtime::node::ClusterId::from_legacy_named(
            "cluster_sifs_observation_journal_heads",
        )
        .expect("cluster id");
        let local_node_id =
            NodeId::from_legacy_named("node_sifs_observation_local").expect("node id");
        let state = sinomo_runtime::core::ClusterBootstrapState::initialized_locally(
            cluster_id,
            "SIFS observation journal heads",
            local_node_id.clone(),
            "local",
            "local-public-key",
            time::OffsetDateTime::now_utc(),
        );
        cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");

        let file_system_id = sifs_file_system_id();
        let shards = vec![
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "root".to_string(),
                primary_node_id: local_node_id.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: sinomo_runtime::dfs::DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: Default::default(),
                default_policy: sinomo_runtime::core::DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "services".to_string(),
                primary_node_id: local_node_id.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: sinomo_runtime::dfs::DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
                namespace_prepare_codec: Default::default(),
                default_policy: sinomo_runtime::core::DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id,
                shard_id: "users".to_string(),
                primary_node_id: local_node_id.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: sinomo_runtime::dfs::DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/users".to_string(),
                },
                namespace_prepare_codec: Default::default(),
                default_policy: sinomo_runtime::core::DfsObjectPolicy::ReplicatedEventual,
            },
        ];
        let mut kv = sinomo_runtime::kv::InProcessKvStore::new();
        {
            let mut control = DfsControlPlaneStore::new(&mut kv);
            for shard in &shards {
                control.put_metadata_shard(shard).expect("metadata shard");
            }
        }
        sinomo_runtime::kv::write_cluster_kv_snapshot(
            &cluster_paths.kv_paths(),
            &kv.try_snapshot().expect("KV snapshot"),
        )
        .expect("write KV snapshot");

        let file_system = sinomo_runtime::dfs::DfsFileSystem::sifs(local_node_id);
        for shard in &shards {
            let mut metadata = sinomo_runtime::dfs::open_local_metadata_shard_backend(
                data_dir,
                sinomo_runtime::dfs::DfsMetadataShardId::new(&shard.shard_id),
            )
            .expect("metadata shard backend");
            metadata
                .put_file_system(&file_system)
                .expect("seed metadata journal");
        }
        shards
    }

    fn write_metadata_shard_snapshot(
        cluster_paths: &ClusterPaths,
        shards: &[DfsMetadataShardConfig],
    ) {
        let mut kv = sinomo_runtime::kv::InProcessKvStore::new();
        {
            let mut control = DfsControlPlaneStore::new(&mut kv);
            for shard in shards {
                control.put_metadata_shard(shard).expect("metadata shard");
            }
        }
        sinomo_runtime::kv::write_cluster_kv_snapshot(
            &cluster_paths.kv_paths(),
            &kv.try_snapshot().expect("KV snapshot"),
        )
        .expect("write metadata shard snapshot");
    }

    fn metadata_journal_heads(
        data_dir: &SinomoDataDir,
        shards: &[DfsMetadataShardConfig],
    ) -> Vec<(String, Vec<u8>)> {
        shards
            .iter()
            .map(|shard| {
                let shard_id = sinomo_runtime::dfs::DfsMetadataShardId::new(&shard.shard_id);
                let head = sinomo_runtime::dfs::local_dfs_metadata_shard_path(data_dir, &shard_id)
                    .join("journal/head.sobj");
                (
                    shard.shard_id.clone(),
                    fs::read(&head)
                        .unwrap_or_else(|error| panic!("cannot read {}: {error}", head.display())),
                )
            })
            .collect()
    }

    fn temp_sifs_data_dir(name: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(&format!("sinomo-tui-sifs-file-manager-{name}"))
    }
}
