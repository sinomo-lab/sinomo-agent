use crate::file_manager::{
    FileManagerBackend, FileManagerEntry, FileManagerEntryKind, FileManagerMetadata,
    FileManagerRead, validated_relative_child_path,
};
#[allow(unused_imports)]
use sinomo_runtime::cluster;
use sinomo_runtime::cluster::ClusterKvPathsExt;
#[cfg(any(unix, test))]
use sinomo_runtime::dfs::DfsPosixCreateAttrs;
use sinomo_runtime::dfs::{
    AsyncDfsPeerTransport, DaemonDfsInspectionRequest, DaemonDfsInspectionResponse,
    DfsControlPlaneKvClient, DfsControlPlaneStore, DfsError, DfsFileSystemConfig,
    DfsMetadataJournalReplicationOptions, DfsMetadataJournalReplicationReport,
    DfsMetadataShardConfig, DfsNamespaceOperation, DfsNodeAdvertisement, DfsNodeTransport,
    DfsObjectKind, DfsObjectRpcSendOptions, DfsOperatorObjectView, DfsOperatorStatus,
    DfsPeerTransport, DfsPeerTransportError, DfsPeerTransportFuture, DfsPinAddRequest, DfsPinMode,
    DfsPlacementBlockingDiagnostic, DfsPlacementImpact, DfsPlacementNodePreview,
    DfsPlacementNodeReachability, DfsPlacementPolicy, DfsPlacementPreview,
    DfsPlacementPreviewToken, DfsPlacementReconciliationActivityCursor,
    DfsPlacementReconciliationActivityPage, DfsPlacementReconciliationActivityStatus,
    DfsPlacementReconciliationJob, DfsPlacementReconciliationState, DfsPlacementScope,
    DfsPosixDistributedFilePrefixReadOutcome, DfsPosixDistributedFileReadOutcome, DfsPosixError,
    DfsResolvedPlacementPolicy, DfsStableNamespaceOperationReconciliation, DfsStat,
    DfsWriteOptions, LocalContentStore, NetworkedDfsService, content_services_prefix,
    default_dfs_metadata_root, default_dfs_metadata_shards_root, default_dfs_pin_root,
    discovered_cluster_dfs_peer_node_ids, fallback_dfs_node_id, metadata_shards_prefix,
    open_cluster_aware_dfs_metadata_backend_for_inspection,
    open_cluster_aware_dfs_metadata_backend_for_inspection_with_topology,
    open_local_sharded_metadata_backend_for_replica_repair, open_root_local_metadata_backend,
    preview_placement_with_unavailable_nodes,
    publish_cluster_dfs_metadata_journal_with_peer_transports_async,
    select_metadata_shard_config_for_path_strict_default_sifs_split, sifs_file_system_id,
    with_cached_cluster_aware_dfs_metadata_inspection,
};
use sinomo_runtime::kv::{
    KvError, KvOp, KvRevision, KvSnapshot, KvTxn, KvTxnRequest, KvTxnResponse,
};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{self, ClusterPaths};
use sinomo_runtime::node::NodeId;
use sinomo_runtime::test::SinomoTestAuthoritativeClusterSnapshot;
use std::collections::BTreeSet;
use std::future::Future;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const SIFS_PEER_TARGET_REFRESH_TTL: Duration = Duration::from_secs(1);
const SIFS_FOREGROUND_METADATA_PUBLISH_MAX_BATCHES_PER_TARGET: usize = 1;
const SIFS_MAINTENANCE_REFRESH_TIMEOUT: Duration = Duration::from_millis(750);
const SIFS_PLACEMENT_IMPACT_PAGE_SIZE: usize = 256;
const SIFS_AUTHORITATIVE_SCOPE_CACHE_MAX_ENTRIES: usize =
    sinomo_runtime::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;
const SIFS_AUTHORITATIVE_PROJECTION_MAX_ENTRIES: usize =
    sinomo_runtime::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;
const SIFS_AUTHORITATIVE_PROJECTION_MAX_PAYLOAD_BYTES: usize =
    sinomo_runtime::kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES;
const SIFS_AUTHORITATIVE_REPLAN_MAX_ATTEMPTS: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SifsWriteParentPolicy {
    MaterializeMissing,
    RequireExisting,
}

fn sifs_distributed_read_outcome(
    outcome: DfsPosixDistributedFileReadOutcome,
) -> Result<Vec<u8>, String> {
    match outcome {
        DfsPosixDistributedFileReadOutcome::Found { bytes } => Ok(bytes),
        DfsPosixDistributedFileReadOutcome::NotFound { path } => {
            Err(sinomo_runtime::api::DaemonDfsBrowserReadFileFailure::NotFound { path }.to_string())
        }
        DfsPosixDistributedFileReadOutcome::ContentUnavailable { path, detail } => Err(
            sinomo_runtime::api::DaemonDfsBrowserReadFileFailure::ContentUnavailable {
                path,
                detail,
            }
            .to_string(),
        ),
    }
}

fn sifs_distributed_prefix_read_outcome(
    outcome: DfsPosixDistributedFilePrefixReadOutcome,
) -> Result<FileManagerRead, String> {
    match outcome {
        DfsPosixDistributedFilePrefixReadOutcome::Found { read } => Ok(FileManagerRead {
            bytes: read.bytes,
            truncated: read.truncated,
        }),
        DfsPosixDistributedFilePrefixReadOutcome::NotFound { path } => {
            Err(sinomo_runtime::api::DaemonDfsBrowserReadFileFailure::NotFound { path }.to_string())
        }
        DfsPosixDistributedFilePrefixReadOutcome::ContentUnavailable { path, detail } => Err(
            sinomo_runtime::api::DaemonDfsBrowserReadFileFailure::ContentUnavailable {
                path,
                detail,
            }
            .to_string(),
        ),
    }
}

/// Resolve the process which owns clustered local storage for this exact data
/// directory. A healthy main daemon is the only production owner allowed to
/// serve clustered inspection. When no daemon is running, the legacy
/// standalone path remains available for embedded/test callers which already
/// own a process-local resident image; a locked-but-unreachable daemon never
/// falls back across the ownership boundary.
fn sifs_clustered_daemon_client(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<Option<sinomo_runtime::daemon::DaemonApiClient>, String> {
    sifs_clustered_daemon_client_until(data_dir, cluster_paths, None)
}

fn sifs_clustered_daemon_client_until(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    operation_deadline: Option<Instant>,
) -> Result<Option<sinomo_runtime::daemon::DaemonApiClient>, String> {
    if !cluster_paths.state.exists() {
        return Ok(None);
    }
    let state = cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(|error| format!("cannot read local cluster state: {error}"))?;
    let daemon_paths = sinomo_runtime::daemon::DaemonPaths::new(data_dir);
    let probe_timeout =
        sifs_operation_remaining(operation_deadline, "probing the resident daemon")?
            .map(|remaining| remaining.min(sinomo_runtime::daemon::DAEMON_API_PROBE_TIMEOUT))
            .unwrap_or(sinomo_runtime::daemon::DAEMON_API_PROBE_TIMEOUT);
    let probe = sinomo_runtime::daemon::probe_daemon_status(&daemon_paths, probe_timeout);
    sifs_clustered_daemon_client_from_probe(&state, data_dir, probe)
}

fn sifs_clustered_daemon_client_from_probe(
    state: &sinomo_runtime::core::ClusterBootstrapState,
    data_dir: &SinomoDataDir,
    probe: sinomo_runtime::daemon::DaemonProbeStatus,
) -> Result<Option<sinomo_runtime::daemon::DaemonApiClient>, String> {
    match probe {
        sinomo_runtime::daemon::DaemonProbeStatus::Running(status) => {
            let daemon_cluster = status.cluster.as_ref().ok_or_else(|| {
                "clustered SIFS inspection reached a daemon without cluster identity".to_string()
            })?;
            if daemon_cluster.cluster_id != state.cluster_id
                || daemon_cluster.node_id != state.local_node_id
                || daemon_cluster.process != sinomo_runtime::node::DAEMON_MAIN_CLUSTER_ROLE
            {
                return Err(format!(
                    "clustered SIFS inspection reached daemon cluster/node/process {}/{}/{}, expected {}/{}/{}",
                    daemon_cluster.cluster_id,
                    daemon_cluster.node_id,
                    daemon_cluster.process,
                    state.cluster_id,
                    state.local_node_id,
                    sinomo_runtime::node::DAEMON_MAIN_CLUSTER_ROLE,
                ));
            }
            Ok(Some(sinomo_runtime::daemon::DaemonApiClient::at_address(
                status.address,
            )))
        }
        sinomo_runtime::daemon::DaemonProbeStatus::Locked { .. } => Err(format!(
            "the daemon owns clustered SIFS storage at {}, but its resident inspection service is unavailable",
            data_dir.root().display(),
        )),
        sinomo_runtime::daemon::DaemonProbeStatus::NotRunning => Ok(None),
    }
}

fn sifs_operator_status_from_current_owner(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<DfsOperatorStatus, String> {
    let status = if let Some(client) = sifs_clustered_daemon_client(data_dir, cluster_paths)? {
        let request = sinomo_runtime::dfs::dfs_status_rpc_request()
            .map_err(|error| format!("cannot build resident DFS status request: {error}"))?;
        let response = client
            .call_request(&request)
            .map_err(|error| format!("cannot read resident DFS status: {error}"))?;
        sinomo_runtime::dfs::decode_dfs_status_rpc_response(&response)
            .map_err(|error| format!("cannot decode resident DFS status: {error}"))?
    } else {
        sinomo_runtime::dfs::inspect_cluster_aware_dfs_status(data_dir, cluster_paths)
            .map_err(|error| format!("cannot read standalone DFS status: {error}"))?
    };
    if status.cross_shard_namespace_lock_count.is_none() {
        return Err(
            "resident DFS status did not observe durable cross-shard namespace locks".to_string(),
        );
    }
    Ok(status)
}

fn sifs_object_by_path_from_current_owner(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    path: &str,
) -> Result<Option<DfsOperatorObjectView>, String> {
    if let Some(client) = sifs_clustered_daemon_client(data_dir, cluster_paths)? {
        let request =
            sinomo_runtime::dfs::dfs_inspection_rpc_request(&DaemonDfsInspectionRequest::Path {
                file_system_id: sifs_file_system_id(),
                path: path.to_string(),
            })
            .map_err(|error| format!("cannot build resident DFS path inspection: {error}"))?;
        let response = client
            .call_request(&request)
            .map_err(|error| format!("cannot inspect resident SIFS path {path}: {error}"))?;
        return match sinomo_runtime::dfs::decode_dfs_inspection_rpc_response(&response)
            .map_err(|error| format!("cannot decode resident SIFS path {path}: {error}"))?
        {
            DaemonDfsInspectionResponse::Path { object } => Ok(object),
            response => Err(format!(
                "resident SIFS path inspection returned an unexpected response: {response:?}"
            )),
        };
    }
    with_cached_cluster_aware_dfs_metadata_inspection(data_dir, cluster_paths, |metadata| {
        sinomo_runtime::dfs::DfsOperatorInspector::new(metadata)
            .object_by_path(&sifs_file_system_id(), path)
            .map_err(Into::into)
    })
    .map_err(|error| format!("cannot inspect standalone SIFS path {path}: {error}"))
}

#[derive(Debug)]
pub(crate) struct SifsFileManagerBackend {
    data_dir: SinomoDataDir,
    rpc_runtime: SifsRpcRuntime,
    service: Mutex<NetworkedDfsService<DfsNodeTransport>>,
    /// `None` marks a strict read-only backend. Writable backends defer
    /// authority rebind/recovery until their first mutation so cold reads stay
    /// observational.
    mutation_bootstrapped: Mutex<Option<bool>>,
    peer_targets_refreshed_at: Mutex<Option<Instant>>,
    replica_candidates: Vec<NodeId>,
}

/// Narrow, metadata-less requester for the one-shot DFS throughput probe.
///
/// This handle deliberately exposes only the three write stages used by
/// `cluster.dfs.bulk-write`: create its parent directory, create or replace
/// files under a known parent, and hand post-commit maintenance back to the
/// resident daemon.
/// General SIFS callers must use [`SifsFileManagerBackend`], whose ordinary
/// open always owns a durable metadata backend.
#[derive(Debug)]
pub(crate) struct SifsOneShotBulkWriteRequester {
    data_dir: SinomoDataDir,
    local_node_id: NodeId,
    service: Mutex<NetworkedDfsService<DfsNodeTransport>>,
    control_plane: SifsOneShotBulkWriteControlPlaneKvClient,
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

    #[cfg(test)]
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

    pub(crate) fn metadata_shard_for_path_at_authority(
        &self,
        path: &str,
        authority: &SinomoTestAuthoritativeClusterSnapshot,
    ) -> Result<Option<DfsMetadataShardConfig>, String> {
        let path = normalize_sifs_path(path)?;
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        let expected_source = cluster_paths.kv_paths().snapshot;
        if authority.source() != expected_source {
            return Err(format!(
                "SIFS observation authority source {} does not match local cluster snapshot {}",
                authority.source().display(),
                expected_source.display()
            ));
        }
        let mut kv = SifsSnapshotControlPlaneKvClient::from_validated_snapshot(
            &cluster_paths,
            authority.snapshot(),
        );
        DfsControlPlaneStore::new(&mut kv)
            .select_metadata_shard_for_path_strict_default_sifs_split(&sifs_file_system_id(), &path)
            .map_err(|error| {
                format!("cannot read authoritative SIFS metadata shard for {path}: {error}")
            })
    }

    pub(crate) fn object_by_path(
        &self,
        path: &str,
    ) -> Result<Option<DfsOperatorObjectView>, String> {
        let path = normalize_sifs_path(path)?;
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        sifs_object_by_path_from_current_owner(&self.data_dir, &cluster_paths, &path)
    }

    pub(crate) fn operational_snapshot(
        &self,
    ) -> Result<SifsFileManagerObservationSnapshot, String> {
        let cluster_paths = ClusterPaths::new(&self.data_dir);
        if !cluster_paths.state.exists() {
            return Err("cluster SIFS observation requires local cluster state".to_string());
        }
        let operator_status =
            sifs_operator_status_from_current_owner(&self.data_dir, &cluster_paths)?;
        let cross_shard_namespace_lock_count = operator_status
            .cross_shard_namespace_lock_count
            .ok_or_else(|| {
                "resident DFS status did not observe durable cross-shard namespace locks"
                    .to_string()
            })?;

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
            cross_shard_namespace_lock_count,
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
        Self::from_service(data_dir, rpc_runtime, service, true, None)
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
        Self::from_service(data_dir, rpc_runtime, service, true, operation_deadline)
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
        Self::from_service(data_dir, rpc_runtime, service, false, operation_deadline)
    }

    fn open_read_only_with_runtime(
        data_dir: &SinomoDataDir,
        rpc_runtime: SifsRpcRuntime,
    ) -> Result<Self, String> {
        let service = open_sifs_service_read_only(data_dir, &rpc_runtime)?;
        Self::from_service(data_dir, rpc_runtime, service, false, None)
    }

    fn from_service(
        data_dir: &SinomoDataDir,
        rpc_runtime: SifsRpcRuntime,
        mut service: NetworkedDfsService<DfsNodeTransport>,
        writable: bool,
        operation_deadline: Option<Instant>,
    ) -> Result<Self, String> {
        let clustered = ClusterPaths::new(data_dir).state.exists();
        let mutation_bootstrapped = if writable && !clustered {
            // A brand-new standalone SIFS has no daemon projection or remote
            // authority to wait for, and its fixed namespace must exist before
            // FileManagerState performs its initial stat/list. Keep this local
            // initialization eager while clustered opens stay observational.
            sifs_operation_remaining(operation_deadline, "initializing standalone SIFS")?;
            let previous_deadline = operation_deadline
                .map(|deadline| service.set_transport_operation_deadline(Some(deadline)));
            let bootstrap = service
                .bootstrap_distributed_sifs()
                .map_err(|error| format!("cannot initialize standalone SIFS: {error}"));
            if let Some(previous_deadline) = previous_deadline {
                service.set_transport_operation_deadline(previous_deadline);
            }
            bootstrap?;
            sifs_operation_remaining(operation_deadline, "initializing standalone SIFS")?;
            Some(true)
        } else {
            writable.then_some(false)
        };
        let replica_candidates = sifs_replica_candidates(data_dir, service.node_id());
        Ok(Self {
            data_dir: data_dir.clone(),
            rpc_runtime,
            service: Mutex::new(service),
            mutation_bootstrapped: Mutex::new(mutation_bootstrapped),
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

    fn lock_service_for_mutation(
        &self,
        operation_deadline: Option<Instant>,
    ) -> Result<std::sync::MutexGuard<'_, NetworkedDfsService<DfsNodeTransport>>, String> {
        let mut service = self.lock_service_with_deadline(operation_deadline)?;
        let mut mutation_bootstrapped = self
            .mutation_bootstrapped
            .lock()
            .map_err(|_| "SIFS mutation bootstrap lock is poisoned".to_string())?;
        let Some(bootstrapped) = mutation_bootstrapped.as_mut() else {
            return Err("this SIFS backend is read-only".to_string());
        };
        if *bootstrapped {
            return Ok(service);
        }

        sifs_operation_remaining(operation_deadline, "preparing SIFS mutation authority")?;
        let previous_deadline = operation_deadline
            .map(|deadline| service.set_transport_operation_deadline(Some(deadline)));
        let bootstrap = service
            .bootstrap_distributed_sifs()
            .map_err(|error| format!("cannot prepare SIFS mutation authority: {error}"));
        if let Some(previous_deadline) = previous_deadline {
            service.set_transport_operation_deadline(previous_deadline);
        }
        bootstrap?;
        sifs_operation_remaining(operation_deadline, "preparing SIFS mutation authority")?;
        *bootstrapped = true;
        Ok(service)
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
                Ok(refreshed_at) => refreshed_at
                    .as_ref()
                    .is_none_or(|last| now.duration_since(*last) >= SIFS_PEER_TARGET_REFRESH_TTL),
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
        if let Some(deadline) = operation_deadline
            && sifs_operation_remaining(Some(deadline), "refreshing SIFS peer targets").is_err()
        {
            return false;
        }
        // Peer discovery is an observational hot path. The daemon already
        // projects committed membership into the durable bootstrap state; a
        // file operation must not download and rematerialize the full cluster
        // KV snapshot merely to refresh the transport's authorized node set.
        let Ok(state) = cluster::read_cluster_state_without_kv_cached_shared(&cluster_paths) else {
            return false;
        };
        let Ok(peer_node_ids) = discovered_cluster_dfs_peer_node_ids(&cluster_paths, &state) else {
            return false;
        };
        service.refresh_peer_targets(peer_node_ids);
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
        match service.posix_open_current_file_stream(&path) {
            Ok(mut local) => {
                let size_bytes = usize::try_from(local.size_bytes).map_err(|_| {
                    format!("cannot read SIFS file {path}: length does not fit usize")
                })?;
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
            Err(DfsPosixError::NotFound { .. }) => {}
            Err(error) => return Err(format!("cannot read SIFS file {path}: {error}")),
        }

        // `FileManagerBackend` remains a Vec-returning compatibility API, but
        // remote DFS now returns its response head before any body bytes and
        // preserves transport-owned chunks until this final boundary.
        let remote = match block_on_sifs_posix_future(
            &self.rpc_runtime,
            service.posix_open_remote_file_stream(&path),
        ) {
            Ok(remote) => remote,
            Err(DfsPosixError::NotFound { .. }) => {
                // The legacy streaming API uses NotFound for both namespace
                // absence and an exact content version that no source can
                // serve. Reclassify only this error path through the typed
                // bounded-state read; normal remote streaming remains
                // unchanged.
                let outcome = service
                    .posix_read_file_distributed_outcome(&path)
                    .map_err(|error| format!("cannot read SIFS file {path}: {error}"))?;
                return sifs_distributed_read_outcome(outcome);
            }
            Err(error) => return Err(format!("cannot read SIFS file {path}: {error}")),
        };
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
        let mut kv = SifsControlPlaneKvClient::new(&self.data_dir, &cluster_paths);
        kv.refresh_latest()
            .map_err(|error| format!("cannot refresh SIFS control plane: {error}"))?;
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
        kv.refresh_latest()
            .map_err(|error| format!("cannot refresh SIFS control plane: {error}"))?;
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
            placement_snapshot: None,
            adaptive: false,
            adaptive_budget: None,
            processed_content_bytes: 0,
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
        let mut kv = SifsControlPlaneKvClient::new(&self.data_dir, &cluster_paths);
        kv.refresh_latest()
            .map_err(|error| format!("cannot refresh SIFS control plane: {error}"))?;
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
        let mut kv = SifsControlPlaneKvClient::new(&self.data_dir, &cluster_paths);
        kv.refresh_latest()
            .map_err(|error| format!("cannot refresh SIFS control plane: {error}"))?;
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
                        .zip(service.posix_read_files_distributed_outcomes(&read_paths))
                    {
                        results[index] = Some(
                            result
                                .map_err(|error| format!("cannot read SIFS file {path}: {error}"))
                                .and_then(sifs_distributed_read_outcome),
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
        let mut service = self.lock_service_for_mutation(None)?;
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

    pub(crate) fn rename_with_operation_id_and_deadline(
        &self,
        from: &str,
        to: &str,
        operation_id: &str,
        deadline: Instant,
    ) -> Result<(), String> {
        let from = normalize_sifs_path(from)?;
        let to = normalize_sifs_path(to)?;
        self.run_namespace_mutation_with_deadline(Some(deadline), "SIFS rename", |service| {
            service
                .posix_rename_with_operation_id(&from, &to, operation_id)
                .map(|_| ())
                .map_err(|error| format!("cannot rename SIFS item {from} to {to}: {error}"))
        })
    }

    pub(crate) fn reconcile_rename_with_operation_id_and_deadline(
        &self,
        from: &str,
        to: &str,
        operation_id: &str,
        deadline: Instant,
    ) -> Result<DfsStableNamespaceOperationReconciliation, String> {
        let from = normalize_sifs_path(from)?;
        let to = normalize_sifs_path(to)?;
        let operation_deadline = Some(deadline);
        sifs_operation_remaining(operation_deadline, "SIFS rename reconciliation")?;
        let mut service = self.lock_service_with_deadline(operation_deadline)?;
        let previous_deadline = service.set_transport_operation_deadline(Some(deadline));
        let result = (|| {
            self.refresh_service_peer_targets_with_deadline(&mut service, operation_deadline);
            sifs_operation_remaining(operation_deadline, "refreshing SIFS peer targets")?;
            service
                .reconcile_stable_namespace_operation(
                    operation_id,
                    &DfsNamespaceOperation::Rename {
                        from: from.clone(),
                        to: to.clone(),
                    },
                )
                .map_err(|error| {
                    format!(
                        "cannot reconcile SIFS rename {from} to {to} under operation {operation_id}: DFS error: {error}"
                    )
                })
        })();
        service.set_transport_operation_deadline(previous_deadline);
        result
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
        let mut service = self.lock_service_for_mutation(operation_deadline)?;
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
        let mut service = self.lock_service_for_mutation(operation_deadline)?;
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
        let mut service = self.lock_service_for_mutation(operation_deadline)?;
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

    #[cfg(test)]
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

    #[cfg(test)]
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
        let mut service = self.lock_service_for_mutation(operation_deadline)?;
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
        let mut service = self.lock_service_for_mutation(operation_deadline)?;
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
        let mut service = self.lock_service_for_mutation(operation_deadline)?;
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
        sifs_operator_status_from_current_owner(&self.data_dir, &cluster_paths).map(Some)
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
        let mut kv = SifsControlPlaneKvClient::new(&self.data_dir, &cluster_paths);
        kv.refresh_latest()
            .map_err(|error| format!("cannot refresh SIFS control plane: {error}"))?;
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
        let state = cluster::read_cluster_state_without_kv(&cluster_paths)
            .map_err(|error| format!("cannot read local cluster state: {error}"))?;
        let local_node_id = state.local_node_id;
        let mut kv = SifsControlPlaneKvClient::new(&self.data_dir, &cluster_paths);
        kv.refresh_latest()
            .map_err(|error| format!("cannot refresh SIFS control plane: {error}"))?;
        let mut store = DfsControlPlaneStore::new(&mut kv);
        let previous = store
            .select_metadata_shard_for_path_strict_default_sifs_split(&sifs_file_system_id(), &path)
            .map_err(|error| format!("cannot read SIFS metadata shard for {path}: {error}"))?
            .ok_or_else(|| format!("no SIFS metadata shard controls {path}"))?;
        if let Some(expected_epoch) = expected_epoch
            && previous.epoch != expected_epoch
        {
            return Err(format!(
                "SIFS metadata shard {} epoch is {}, expected {}",
                previous.shard_id, previous.epoch, expected_epoch
            ));
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
        let mut service = self.lock_service_for_mutation(None)?;
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
    #[cfg(any(unix, test))]
    pub(crate) fn ensure_posix_directory_with_attrs(
        &self,
        path: &str,
        uid: u32,
        gid: u32,
        mode: u32,
    ) -> Result<DfsStat, String> {
        let path = normalize_sifs_path(path)?;
        let mode = mode & 0o7777;
        let mut service = self.lock_service_for_mutation(None)?;
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
        let mut service = self.lock_service_for_mutation(operation_deadline)?;
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
        let entries = match service.posix_list_dir_distributed(&path) {
            Ok(entries) => entries,
            Err(DfsPosixError::NotFound { .. }) => {
                // Directory listing may follow a final link. Confirm that the
                // namespace dentry itself is absent before producing the one
                // optional-read error category; a present link whose target
                // metadata is unavailable must fail closed.
                match service.posix_namespace_lstat_distributed(&path) {
                    Err(DfsPosixError::NotFound { .. }) => {
                        return Err(
                            sinomo_runtime::api::DaemonDfsBrowserReadFileFailure::NotFound { path }
                                .to_string(),
                        );
                    }
                    Ok(_) => {
                        return Err(format!(
                            "SIFS directory target is unavailable at {path}: the namespace entry still exists"
                        ));
                    }
                    Err(error) => return Err(format!("cannot inspect {path}: {error}")),
                }
            }
            Err(error) => return Err(format!("cannot list {path}: {error}")),
        };
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
        let outcome = service
            .posix_read_file_prefix_distributed_outcome(&path, max_bytes)
            .map_err(|error| format!("cannot read {path}: {error}"))?;
        sifs_distributed_prefix_read_outcome(outcome)
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
        let mut service = self.lock_service_for_mutation(None)?;
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
        let mut service = self.lock_service_for_mutation(None)?;
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
        let mut service = self.lock_service_for_mutation(None)?;
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

impl SifsOneShotBulkWriteRequester {
    pub(crate) fn open(
        data_dir: &SinomoDataDir,
        authoritative_snapshot: Option<SinomoTestAuthoritativeClusterSnapshot>,
    ) -> Result<Self, String> {
        data_dir
            .ensure_root_layout()
            .map_err(|error| format!("cannot prepare Sinomo data directory: {error}"))?;
        let cluster_paths = ClusterPaths::new(data_dir);
        if !cluster_paths.state.exists() {
            return Err(
                "one-shot SIFS bulk write requires local cluster state and a resident daemon"
                    .to_string(),
            );
        }

        if let Some(snapshot) = authoritative_snapshot.as_ref() {
            let expected_source = cluster_paths.kv_paths().snapshot;
            if snapshot.source() != expected_source {
                return Err(format!(
                    "one-shot SIFS bulk write authority source {} does not match local cluster snapshot {}",
                    snapshot.source().display(),
                    expected_source.display()
                ));
            }
        }
        let state = cluster::read_cluster_state_without_kv(&cluster_paths)
            .map_err(|error| format!("cannot read local cluster state: {error}"))?;
        let peer_node_ids = discovered_cluster_dfs_peer_node_ids(&cluster_paths, &state)
            .map_err(|error| format!("cannot discover DFS service peers: {error}"))?;
        let local_node_id = state.local_node_id;

        let peer_transport = sinomo_runtime::dfs::DaemonDfsPeerTransport::connect(data_dir.clone())
            .map_err(|error| {
                format!(
                    "one-shot SIFS bulk write requires the resident daemon DFS transport: {error}"
                )
            })?;
        let (sync_peer_transport, async_peer_transport) =
            shared_sifs_peer_transports(peer_transport);
        let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))
            .map_err(|error| format!("cannot open DFS content store: {error}"))?;

        let mut control_plane = SifsOneShotBulkWriteControlPlaneKvClient::new(
            data_dir,
            &cluster_paths,
            local_node_id.clone(),
        );
        if let Some(authoritative_snapshot) = authoritative_snapshot {
            // The exact single-case test context fetched and canonically
            // validated this generation under a current-term quorum barrier.
            // Install its Arc directly instead of serializing a duplicate Raft
            // barrier and rereading the materialized snapshot from disk.
            control_plane
                .inner
                .install_cached_snapshot(authoritative_snapshot.snapshot())
                .map_err(|error| format!("cannot install one-shot DFS shard catalog: {error}"))?;
        } else {
            // Suite/tag and fixture contexts do not carry an immediately fresh
            // proof. Acquire the bounded authoritative DFS projection through
            // the resident daemon; a full cluster snapshot is neither needed
            // for shard routing nor acceptable on this foreground path.
            control_plane
                .refresh_and_validate()
                .map_err(|error| format!("cannot refresh one-shot DFS shard catalog: {error}"))?;
        }
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
            .map_err(|error| format!("cannot bootstrap one-shot SIFS bulk write: {error}"))?;

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
                "one-shot SIFS bulk write is fenced because the local node identity changed from {} to {}",
                self.local_node_id, state.local_node_id
            ));
        }
        let peer_node_ids = discovered_cluster_dfs_peer_node_ids(&cluster_paths, &state)
            .map_err(|error| format!("cannot discover DFS service peers: {error}"))?;
        Ok((peer_node_ids, shards))
    }

    fn lock_service_with_deadline(
        &self,
        deadline: Instant,
    ) -> Result<std::sync::MutexGuard<'_, NetworkedDfsService<DfsNodeTransport>>, String> {
        loop {
            match self.service.try_lock() {
                Ok(service) => return Ok(service),
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err("one-shot SIFS bulk write service lock is poisoned".to_string());
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    let remaining = sifs_operation_remaining(
                        Some(deadline),
                        "waiting for the one-shot SIFS bulk write service lock",
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
        validate_one_shot_bulk_write_paths(
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

    pub(crate) fn write_files_with_known_parents_deferred_publish_mode_until(
        &self,
        files: &[(String, Vec<u8>)],
        overwrite_existing: bool,
        deadline: Instant,
    ) -> Result<(), String> {
        let normalized = files
            .iter()
            .map(|(path, bytes)| normalize_sifs_path(path).map(|path| (path, bytes.as_slice())))
            .collect::<Result<Vec<_>, _>>()?;
        sifs_operation_remaining(Some(deadline), "one-shot SIFS bulk write")?;
        let (peer_node_ids, shards) = self.validate_cached_authority()?;
        validate_one_shot_bulk_write_paths(
            &shards,
            normalized.iter().map(|(path, _)| path.as_str()),
        )?;
        let mut service = self.lock_service_with_deadline(deadline)?;
        service.refresh_peer_targets(peer_node_ids);
        let previous_deadline = service.set_transport_operation_deadline(Some(deadline));
        let files = normalized
            .iter()
            .map(|(path, bytes)| (path.as_str(), *bytes));
        let result = if overwrite_existing {
            service.posix_write_files_local_eventual_with_known_parents(files)
        } else {
            service.posix_create_files_local_eventual_with_known_parents(files)
        }
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
                .map_err(|_| "one-shot SIFS bulk write service lock is poisoned".to_string())?;
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
                "resident daemon became unavailable during one-shot SIFS bulk write maintenance handoff"
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
    let cluster_state = cluster_paths
        .state
        .exists()
        .then(|| {
            cluster::read_cluster_state_without_kv(&cluster_paths)
                .map_err(|error| format!("cannot read local cluster state: {error}"))
        })
        .transpose()?;
    let (local_node_id, peer_node_ids) = match cluster_state.as_ref() {
        Some(state) => (
            state.local_node_id.clone(),
            discovered_cluster_dfs_peer_node_ids(&cluster_paths, state)
                .map_err(|error| format!("cannot discover DFS service peers: {error}"))?,
        ),
        None => (fallback_dfs_node_id(), Vec::new()),
    };
    sifs_operation_remaining(operation_deadline, "opening the DFS content store")?;
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))
        .map_err(|error| format!("cannot open DFS content store: {error}"))?;
    let (sync_peer_transport, async_peer_transport) =
        match sinomo_runtime::dfs::DaemonDfsPeerTransport::connect(data_dir.clone()) {
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
    let mut control_plane = cluster_state
        .as_ref()
        .map(|_| SifsControlPlaneKvClient::new(data_dir, &cluster_paths));
    sifs_operation_remaining(operation_deadline, "opening read-only DFS metadata")?;
    let metadata = if let Some(state) = cluster_state.as_ref()
        && sifs_clustered_daemon_client_until(data_dir, &cluster_paths, operation_deadline)?
            .is_some()
    {
        let kv = control_plane
            .as_mut()
            .expect("clustered read-only SIFS has a control-plane client");
        let prefix = format!("{}/", metadata_shards_prefix(&sifs_file_system_id()));
        let topology = kv
            .range_prefix_authoritative(&prefix)
            .map_err(|error| format!("cannot read authoritative SIFS shard topology: {error}"))?;
        open_cluster_aware_dfs_metadata_backend_for_inspection_with_topology(
            data_dir,
            &state.local_node_id,
            topology,
        )
        .map_err(|error| format!("cannot open daemon-certified SIFS metadata: {error}"))?
    } else {
        open_cluster_aware_dfs_metadata_backend_for_inspection(data_dir, &cluster_paths)
            .map_err(|error| format!("cannot open read-only SIFS metadata: {error}"))?
    };
    if let Some(control_plane) = control_plane {
        service = service.with_control_plane(control_plane);
    }
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
    // Opening a SIFS client is an observational operation. The long-lived
    // daemon owns cluster-KV convergence and projects the latest observed
    // snapshot locally; initiating authoritative quorum reads here made every
    // cold open pay multiple network round trips for the same full snapshot.
    // Explicit control-plane refreshes and mutations still use the daemon
    // through `SifsControlPlaneKvClient::refresh_latest` and `txn`.
    let (local_node_id, peer_node_ids) = if cluster_paths.state.exists() {
        let state = cluster::read_cluster_state_without_kv(&cluster_paths)
            .map_err(|error| format!("cannot read local cluster state: {error}"))?;
        let peer_node_ids = discovered_cluster_dfs_peer_node_ids(&cluster_paths, &state)
            .map_err(|error| format!("cannot discover DFS service peers: {error}"))?;
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
        match sinomo_runtime::dfs::DaemonDfsPeerTransport::connect(data_dir.clone()) {
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
    // Cold client hydration is observational. Authority rebind/recovery and
    // fixed-namespace maintenance are deferred until a writable backend's
    // first mutation; the daemon-owned service remains the recurring
    // maintenance owner.
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

/// Reads only the materialized cluster-KV projection published by the daemon.
///
/// `read_cluster_kv_latest_observed_snapshot_*` is appropriate inside the
/// daemon because it can consult the resident Raft view. An application
/// process must not use it here: a cache miss attempts to open the durable Raft
/// node before falling back to the materialized file, contending with the
/// daemon's exclusive `node.lock` and replaying durable state unnecessarily.
fn read_sifs_materialized_cluster_kv_snapshot(
    cluster_paths: &ClusterPaths,
) -> Result<Option<Arc<KvSnapshot>>, KvError> {
    cluster_paths
        .kv_paths()
        .local_cache()
        .cached_snapshot_arc_if_present()
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
    /// `None` consumes the daemon's materialized observational projection.
    /// Every explicit refresh advances this generation and requires each
    /// exact scope to cross one fresh authority barrier before it is reused.
    authoritative_generation: Option<u64>,
    validated_authoritative_entries: BTreeSet<String>,
    validated_authoritative_prefixes: BTreeSet<String>,
    validated_authoritative_scope_bytes: usize,
}

impl SifsKvSnapshotCache {
    fn authoritative_scope_count(&self) -> usize {
        self.validated_authoritative_entries
            .len()
            .saturating_add(self.validated_authoritative_prefixes.len())
    }

    fn clear_authoritative_scopes(&mut self) {
        self.validated_authoritative_entries.clear();
        self.validated_authoritative_prefixes.clear();
        self.validated_authoritative_scope_bytes = 0;
    }

    fn can_admit_authoritative_scopes(
        &self,
        additional_count: usize,
        additional_bytes: usize,
    ) -> bool {
        self.authoritative_scope_count()
            .saturating_add(additional_count)
            <= SIFS_AUTHORITATIVE_SCOPE_CACHE_MAX_ENTRIES
            && self
                .validated_authoritative_scope_bytes
                .saturating_add(additional_bytes)
                <= SIFS_AUTHORITATIVE_PROJECTION_MAX_PAYLOAD_BYTES
    }

    fn begin_authoritative_generation(&mut self) -> u64 {
        if self.authoritative_generation.is_none() {
            let revision = self
                .snapshot
                .as_ref()
                .map_or(KvRevision::ZERO, |snapshot| snapshot.revision);
            // The exact projection must not retain the complete materialized
            // cluster keyspace. It is populated only by scopes callers use.
            self.snapshot = Some(Arc::new(empty_sifs_kv_snapshot(revision)));
            self.loaded = true;
        }
        let generation = self
            .authoritative_generation
            .and_then(|generation| generation.checked_add(1))
            .unwrap_or(1);
        self.authoritative_generation = Some(generation);
        self.clear_authoritative_scopes();
        generation
    }

    fn ensure_authoritative_generation(&mut self) -> u64 {
        match self.authoritative_generation {
            Some(generation) => generation,
            None => self.begin_authoritative_generation(),
        }
    }

    fn entry_is_validated(&self, key: &str) -> bool {
        self.validated_authoritative_entries.contains(key)
            || self
                .validated_authoritative_prefixes
                .iter()
                .any(|prefix| key.starts_with(prefix))
    }

    fn prefix_is_validated(&self, prefix: &str) -> bool {
        self.validated_authoritative_prefixes
            .iter()
            .any(|validated| prefix.starts_with(validated))
    }

    fn invalidate_entry_scope(&mut self, key: &str) {
        if self.validated_authoritative_entries.remove(key) {
            self.validated_authoritative_scope_bytes = self
                .validated_authoritative_scope_bytes
                .saturating_sub(key.len());
        }
        let removed_prefix_bytes = self
            .validated_authoritative_prefixes
            .iter()
            .filter(|prefix| key.starts_with(prefix.as_str()))
            .map(String::len)
            .sum::<usize>();
        self.validated_authoritative_prefixes
            .retain(|prefix| !key.starts_with(prefix));
        self.validated_authoritative_scope_bytes = self
            .validated_authoritative_scope_bytes
            .saturating_sub(removed_prefix_bytes);
    }

    fn invalidate_prefix_scope(&mut self, prefix: &str) {
        let removed_entry_bytes = self
            .validated_authoritative_entries
            .iter()
            .filter(|key| key.starts_with(prefix))
            .map(String::len)
            .sum::<usize>();
        self.validated_authoritative_entries
            .retain(|key| !key.starts_with(prefix));
        let removed_prefix_bytes = self
            .validated_authoritative_prefixes
            .iter()
            .filter(|validated| {
                validated.starts_with(prefix) || prefix.starts_with(validated.as_str())
            })
            .map(String::len)
            .sum::<usize>();
        self.validated_authoritative_prefixes
            .retain(|validated| !validated.starts_with(prefix) && !prefix.starts_with(validated));
        self.validated_authoritative_scope_bytes = self
            .validated_authoritative_scope_bytes
            .saturating_sub(removed_entry_bytes.saturating_add(removed_prefix_bytes));
    }

    fn mark_entry_validated(&mut self, key: &str) {
        if self.validated_authoritative_entries.contains(key) {
            return;
        }
        if self.can_admit_authoritative_scopes(1, key.len()) {
            self.validated_authoritative_entries.insert(key.to_string());
            self.validated_authoritative_scope_bytes = self
                .validated_authoritative_scope_bytes
                .saturating_add(key.len());
        }
    }

    fn mark_prefix_validated(&mut self, prefix: &str) {
        if self.validated_authoritative_prefixes.contains(prefix) {
            return;
        }
        if self.can_admit_authoritative_scopes(1, prefix.len()) {
            self.validated_authoritative_prefixes
                .insert(prefix.to_string());
            self.validated_authoritative_scope_bytes = self
                .validated_authoritative_scope_bytes
                .saturating_add(prefix.len());
        }
    }
}

fn sifs_authoritative_projection_entries_fit(entries: &[sinomo_runtime::kv::KvEntry]) -> bool {
    if entries.len() > SIFS_AUTHORITATIVE_PROJECTION_MAX_ENTRIES {
        return false;
    }
    entries
        .iter()
        .try_fold(0_usize, |payload_bytes, entry| {
            payload_bytes.checked_add(entry.key.len().saturating_add(entry.value.len()))
        })
        .is_some_and(|payload_bytes| {
            payload_bytes <= SIFS_AUTHORITATIVE_PROJECTION_MAX_PAYLOAD_BYTES
        })
}

#[derive(Debug)]
enum SifsKvReadPlan<T> {
    Cached(T),
    Authoritative {
        generation: u64,
        minimum_revision: KvRevision,
    },
}

impl SifsSnapshotControlPlaneKvClient {
    #[cfg(test)]
    fn new(cluster_paths: &ClusterPaths) -> Self {
        Self {
            cluster_paths: cluster_paths.clone(),
            snapshot: Arc::new(Mutex::new(SifsKvSnapshotCache::default())),
        }
    }

    fn from_validated_snapshot(cluster_paths: &ClusterPaths, snapshot: Arc<KvSnapshot>) -> Self {
        Self {
            cluster_paths: cluster_paths.clone(),
            snapshot: Arc::new(Mutex::new(SifsKvSnapshotCache {
                loaded: true,
                snapshot: Some(snapshot),
                ..SifsKvSnapshotCache::default()
            })),
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
        let snapshot = read_sifs_materialized_cluster_kv_snapshot(&self.cluster_paths)
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

#[derive(Clone)]
pub(crate) struct SifsControlPlaneKvClient {
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    snapshot: Arc<Mutex<SifsKvSnapshotCache>>,
    #[cfg(test)]
    peer_transport: Option<Arc<dyn DfsPeerTransport>>,
}

impl std::fmt::Debug for SifsControlPlaneKvClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SifsControlPlaneKvClient")
            .field("data_dir", &self.data_dir)
            .field("cluster_paths", &self.cluster_paths)
            .finish_non_exhaustive()
    }
}

impl SifsControlPlaneKvClient {
    pub(crate) fn new(data_dir: &SinomoDataDir, cluster_paths: &ClusterPaths) -> Self {
        Self {
            data_dir: data_dir.clone(),
            cluster_paths: cluster_paths.clone(),
            snapshot: Arc::new(Mutex::new(SifsKvSnapshotCache::default())),
            #[cfg(test)]
            peer_transport: None,
        }
    }

    #[cfg(test)]
    fn with_peer_transport(mut self, peer_transport: Arc<dyn DfsPeerTransport>) -> Self {
        self.peer_transport = Some(peer_transport);
        self
    }

    fn kv_error(&self, source: impl std::fmt::Display) -> KvError {
        KvError::Io {
            path: self.cluster_paths.kv_paths().snapshot.display().to_string(),
            detail: source.to_string(),
        }
    }

    fn read_latest_snapshot(&self) -> Result<Option<Arc<KvSnapshot>>, KvError> {
        read_sifs_materialized_cluster_kv_snapshot(&self.cluster_paths)
            .map_err(|source| self.kv_error(source))
    }

    fn call_local_daemon_kv_rpc(
        &self,
        payload: sinomo_runtime::kv::ClusterKvRpcPayload,
        max_response_bytes: usize,
    ) -> Result<network::ObjectRpcResponse, KvError> {
        self.call_local_daemon_kv_rpc_raw(payload, max_response_bytes)
            .map_err(|source| self.kv_error(source))
    }

    fn call_local_daemon_kv_rpc_raw(
        &self,
        payload: sinomo_runtime::kv::ClusterKvRpcPayload,
        max_response_bytes: usize,
    ) -> Result<network::ObjectRpcResponse, DfsPeerTransportError> {
        let expected_route = payload.route;
        let call = network::ObjectRpcCall::value(network::ObjectRpcRequest::from_encoded_body(
            payload.route,
            payload.category,
            payload.body,
        ));
        self.call_local_daemon_kv_call_raw(call, expected_route, max_response_bytes)
    }

    fn call_local_daemon_kv_call_raw(
        &self,
        call: network::ObjectRpcCall,
        expected_route: &str,
        max_response_bytes: usize,
    ) -> Result<network::ObjectRpcResponse, DfsPeerTransportError> {
        let state = cluster::read_cluster_state_without_kv(&self.cluster_paths)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
        #[cfg(test)]
        let peer_transport = self.peer_transport.clone().map_or_else(
            || {
                sinomo_runtime::dfs::DaemonDfsPeerTransport::connect(self.data_dir.clone())
                    .map(|transport| Arc::new(transport) as Arc<dyn DfsPeerTransport>)
                    .map_err(|source| DfsPeerTransportError::new(source.to_string()))
            },
            Ok,
        )?;
        #[cfg(not(test))]
        let peer_transport: Arc<dyn DfsPeerTransport> = Arc::new(
            sinomo_runtime::dfs::DaemonDfsPeerTransport::connect(self.data_dir.clone())
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?,
        );
        let result = peer_transport.call_object_rpc(
            &state.local_node_id,
            call,
            DfsObjectRpcSendOptions::default()
                .with_overall_timeout(network::DEFAULT_NETWORK_TIMEOUT),
        )?;
        let rpc_runtime = SifsRpcRuntime::new().map_err(DfsPeerTransportError::new)?;
        let response = block_on_sifs_network_rpc(&rpc_runtime, async move {
            result
                .into_materialized_value_response(max_response_bytes)
                .await
        })
        .map_err(DfsPeerTransportError::from_network)?;
        if response.route != expected_route {
            return Err(DfsPeerTransportError::new(format!(
                "SIFS cluster KV route {expected_route} returned mismatched route {}",
                response.route
            )));
        }
        response
            .error_detail()
            .map_err(DfsPeerTransportError::from_network)?;
        Ok(response)
    }

    fn authoritative_generation(&self) -> Result<Option<u64>, KvError> {
        self.snapshot
            .lock()
            .map_err(|_| self.kv_error("SIFS snapshot cache lock is poisoned"))
            .map(|cache| cache.authoritative_generation)
    }

    fn begin_authoritative_refresh_generation(&self) -> Result<u64, KvError> {
        let mut cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("SIFS snapshot cache lock is poisoned"))?;
        Ok(cache.begin_authoritative_generation())
    }

    fn ensure_authoritative_projection(&self) -> Result<u64, KvError> {
        let mut cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("SIFS snapshot cache lock is poisoned"))?;
        Ok(cache.ensure_authoritative_generation())
    }

    fn authoritative_entry_read_plan(
        &self,
        key: &str,
    ) -> Result<SifsKvReadPlan<Option<sinomo_runtime::kv::KvEntry>>, KvError> {
        // Load the observational projection before taking the planning lock.
        // Once authoritative mode is active this is an Arc clone of the exact
        // projection and performs no disk or network work.
        self.cached_snapshot()?;
        let cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("SIFS snapshot cache lock is poisoned"))?;
        let cached = || {
            cache
                .snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.get(key).cloned())
        };
        let Some(generation) = cache.authoritative_generation else {
            return Ok(SifsKvReadPlan::Cached(cached()));
        };
        if cache.entry_is_validated(key) {
            return Ok(SifsKvReadPlan::Cached(cached()));
        }
        Ok(SifsKvReadPlan::Authoritative {
            generation,
            minimum_revision: cache
                .snapshot
                .as_ref()
                .map_or(KvRevision::ZERO, |snapshot| snapshot.revision),
        })
    }

    fn authoritative_prefix_read_plan(
        &self,
        prefix: &str,
    ) -> Result<SifsKvReadPlan<Vec<sinomo_runtime::kv::KvEntry>>, KvError> {
        self.cached_snapshot()?;
        let cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("SIFS snapshot cache lock is poisoned"))?;
        let cached = || {
            cache.snapshot.as_ref().map_or_else(
                || Ok(Vec::new()),
                |snapshot| {
                    snapshot.range_prefix_limited(
                        prefix,
                        sinomo_runtime::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
                    )
                },
            )
        };
        let Some(generation) = cache.authoritative_generation else {
            return cached().map(SifsKvReadPlan::Cached);
        };
        if cache.prefix_is_validated(prefix) {
            return cached().map(SifsKvReadPlan::Cached);
        }
        Ok(SifsKvReadPlan::Authoritative {
            generation,
            minimum_revision: cache
                .snapshot
                .as_ref()
                .map_or(KvRevision::ZERO, |snapshot| snapshot.revision),
        })
    }

    fn authoritative_read_minimum_revision(
        &self,
        requested: Option<KvRevision>,
    ) -> Result<KvRevision, KvError> {
        let cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("SIFS snapshot cache lock is poisoned"))?;
        Ok(requested.unwrap_or(KvRevision::ZERO).max(
            cache
                .snapshot
                .as_ref()
                .map_or(KvRevision::ZERO, |snapshot| snapshot.revision),
        ))
    }

    fn invalidate_authoritative_entry_scope(
        &self,
        generation: u64,
        key: &str,
    ) -> Result<(), KvError> {
        let mut cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("SIFS snapshot cache lock is poisoned"))?;
        if cache.authoritative_generation == Some(generation) {
            cache.invalidate_entry_scope(key);
        }
        Ok(())
    }

    fn invalidate_authoritative_prefix_scope(
        &self,
        generation: u64,
        prefix: &str,
    ) -> Result<(), KvError> {
        let mut cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("SIFS snapshot cache lock is poisoned"))?;
        if cache.authoritative_generation == Some(generation) {
            cache.invalidate_prefix_scope(prefix);
        }
        Ok(())
    }

    fn invalidate_authoritative_mutation_scopes(
        &self,
        generation: u64,
        keys: &BTreeSet<&str>,
        prefixes: &BTreeSet<&str>,
        invalidate_all: bool,
    ) -> Result<(), KvError> {
        let mut cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("SIFS snapshot cache lock is poisoned"))?;
        if cache.authoritative_generation != Some(generation) {
            // A newer generation has already retired every prior certificate.
            return Ok(());
        }
        if invalidate_all {
            // Lease revocation can delete any key attached to the lease, and
            // this compact projection intentionally does not retain the lease
            // index. Fail closed by retiring every scope certificate.
            cache.clear_authoritative_scopes();
            return Ok(());
        }
        for prefix in prefixes {
            cache.invalidate_prefix_scope(prefix);
        }
        for key in keys {
            cache.invalidate_entry_scope(key);
        }
        Ok(())
    }

    fn read_authoritative_entry(
        &self,
        key: &str,
        minimum_revision: Option<KvRevision>,
        generation: u64,
    ) -> Result<
        Option<sinomo_runtime::kv::KvAuthoritativeRead<Option<sinomo_runtime::kv::KvEntry>>>,
        KvError,
    > {
        let read = self.fetch_authoritative_entry(key, minimum_revision)?;
        if !self.merge_authoritative_entry(key, &read, generation)? {
            return Ok(None);
        }
        Ok(Some(read))
    }

    fn fetch_authoritative_entry(
        &self,
        key: &str,
        minimum_revision: Option<KvRevision>,
    ) -> Result<sinomo_runtime::kv::KvAuthoritativeRead<Option<sinomo_runtime::kv::KvEntry>>, KvError>
    {
        let minimum_revision = self.authoritative_read_minimum_revision(minimum_revision)?;
        let payload = sinomo_runtime::kv::cluster_kv_authoritative_entry_rpc_payload(key)?;
        let response = self.call_local_daemon_kv_rpc(payload, 1024 * 1024)?;
        let read =
            sinomo_runtime::kv::decode_cluster_kv_authoritative_entry_rpc_body(&response.body)?;
        self.ensure_authoritative_revision(read.authority.revision, Some(minimum_revision), key)?;
        if read.value.as_ref().is_some_and(|entry| entry.key != key) {
            return Err(KvError::Serialization {
                detail: format!(
                    "authoritative SIFS read for {key:?} returned entry for {:?}",
                    read.value.as_ref().expect("checked entry").key,
                ),
            });
        }
        Ok(read)
    }

    fn fetch_authoritative_entries(
        &self,
        keys: &[String],
        minimum_revision: KvRevision,
    ) -> Result<
        sinomo_runtime::kv::KvAuthoritativeRead<Vec<Option<sinomo_runtime::kv::KvEntry>>>,
        KvError,
    > {
        let minimum_revision = self.authoritative_read_minimum_revision(Some(minimum_revision))?;
        sinomo_runtime::kv::validate_cluster_kv_authoritative_entries_request_keys(keys)?;
        let route = sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE;
        let call = network::ObjectRpcCall::encode_value(
            route,
            sinomo_runtime::kv::CLUSTER_KV_RPC_CATEGORY,
            sinomo_runtime::kv::ClusterKvAuthoritativeEntriesRequest {
                keys: keys.to_vec(),
            },
        )
        .map_err(|source| self.kv_error(source))?;
        let response = match self.call_local_daemon_kv_call_raw(
            call,
            route,
            sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_MAX_PAYLOAD_BYTES
                .saturating_add(1024 * 1024),
        ) {
            Ok(response) => response,
            Err(error) if error.is_unknown_route(route) => {
                let payload = sinomo_runtime::kv::cluster_kv_authoritative_snapshot_rpc_payload()?;
                let response = self
                    .call_local_daemon_kv_rpc_raw(
                        payload,
                        (sinomo_runtime::kv::MAX_CLUSTER_KV_RAFT_SNAPSHOT_BYTES as usize)
                            .saturating_add(1024 * 1024),
                    )
                    .map_err(|source| self.kv_error(source))?;
                let snapshot =
                    sinomo_runtime::kv::decode_cluster_kv_authoritative_snapshot_rpc_body(
                        &response.body,
                    )?;
                self.ensure_authoritative_revision(
                    snapshot.authority.revision,
                    Some(minimum_revision),
                    "mixed-version exact-key snapshot",
                )?;
                return sinomo_runtime::kv::project_cluster_kv_authoritative_entries(
                    &snapshot, keys,
                );
            }
            Err(error) => return Err(self.kv_error(error)),
        };
        let read = sinomo_runtime::kv::decode_cluster_kv_authoritative_entries_rpc_body(
            &response.body,
            keys,
        )?;
        self.ensure_authoritative_revision(
            read.authority.revision,
            Some(minimum_revision),
            "exact-key set",
        )?;
        Ok(read)
    }

    fn read_authoritative_prefix(
        &self,
        prefix: &str,
        minimum_revision: Option<KvRevision>,
        generation: u64,
    ) -> Result<
        Option<sinomo_runtime::kv::KvAuthoritativeRead<Vec<sinomo_runtime::kv::KvEntry>>>,
        KvError,
    > {
        let minimum_revision = self.authoritative_read_minimum_revision(minimum_revision)?;
        let payload =
            sinomo_runtime::kv::cluster_kv_authoritative_range_prefix_rpc_payload(prefix)?;
        let response = self.call_local_daemon_kv_rpc(
            payload,
            sinomo_runtime::kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES
                .saturating_add(1024 * 1024),
        )?;
        let read = sinomo_runtime::kv::decode_cluster_kv_authoritative_range_prefix_rpc_body(
            &response.body,
        )?;
        self.ensure_authoritative_revision(
            read.authority.revision,
            Some(minimum_revision),
            prefix,
        )?;
        if let Some(entry) = read
            .value
            .iter()
            .find(|entry| !entry.key.starts_with(prefix))
        {
            return Err(KvError::Serialization {
                detail: format!(
                    "authoritative SIFS prefix read for {prefix:?} returned entry for {:?}",
                    entry.key,
                ),
            });
        }
        if !self.merge_authoritative_prefix(prefix, &read, generation)? {
            return Ok(None);
        }
        Ok(Some(read))
    }

    fn ensure_authoritative_revision(
        &self,
        observed: KvRevision,
        minimum: Option<KvRevision>,
        scope: &str,
    ) -> Result<(), KvError> {
        if minimum.is_some_and(|minimum| observed < minimum) {
            return Err(KvError::AuthorityUnavailable {
                detail: format!(
                    "authoritative SIFS read for {scope:?} returned revision {}, below committed revision {}",
                    observed.0,
                    minimum.expect("checked minimum").0,
                ),
            });
        }
        Ok(())
    }

    fn merge_authoritative_entry(
        &self,
        key: &str,
        read: &sinomo_runtime::kv::KvAuthoritativeRead<Option<sinomo_runtime::kv::KvEntry>>,
        generation: u64,
    ) -> Result<bool, KvError> {
        let mut cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("SIFS snapshot cache lock is poisoned"))?;
        if cache.authoritative_generation != Some(generation)
            || cache
                .snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.revision > read.authority.revision)
        {
            return Ok(false);
        }
        let mut reset_projection = !cache.can_admit_authoritative_scopes(1, key.len());
        let mut snapshot = if reset_projection {
            empty_sifs_kv_snapshot(read.authority.revision)
        } else {
            cache
                .snapshot
                .as_deref()
                .cloned()
                .unwrap_or_else(|| empty_sifs_kv_snapshot(KvRevision::ZERO))
        };
        snapshot.entries.retain(|entry| entry.key != key);
        if let Some(entry) = &read.value {
            snapshot.entries.push(entry.clone());
        }
        snapshot.revision = read.authority.revision;
        snapshot.canonicalize();
        if !sifs_authoritative_projection_entries_fit(&snapshot.entries) {
            reset_projection = true;
            snapshot = empty_sifs_kv_snapshot(read.authority.revision);
            if let Some(entry) = &read.value {
                snapshot.entries.push(entry.clone());
            }
            snapshot.canonicalize();
        }
        snapshot.validate_resource_bounds()?;
        cache.loaded = true;
        if reset_projection {
            cache.clear_authoritative_scopes();
        }
        if !sifs_authoritative_projection_entries_fit(&snapshot.entries) {
            cache.snapshot = Some(Arc::new(empty_sifs_kv_snapshot(read.authority.revision)));
            return Ok(true);
        }
        // A point read does not revalidate the rest of any prefix which used
        // to cover it. Retire those prefix scopes before marking the exact key.
        cache.invalidate_entry_scope(key);
        cache.mark_entry_validated(key);
        snapshot
            .entries
            .retain(|entry| cache.entry_is_validated(&entry.key));
        cache.snapshot = Some(Arc::new(snapshot));
        Ok(true)
    }

    fn merge_authoritative_entries_at_one_revision(
        &self,
        keys: &[String],
        read: &sinomo_runtime::kv::KvAuthoritativeRead<Vec<Option<sinomo_runtime::kv::KvEntry>>>,
        generation: u64,
    ) -> Result<bool, KvError> {
        let revision = read.authority.revision;
        if keys.len() != read.value.len() {
            return Err(KvError::Serialization {
                detail: format!(
                    "authoritative SIFS exact-key set returned {} values for {} keys",
                    read.value.len(),
                    keys.len(),
                ),
            });
        }
        for (key, value) in keys.iter().zip(&read.value) {
            if value.as_ref().is_some_and(|entry| entry.key != *key) {
                return Err(KvError::Serialization {
                    detail: format!(
                        "authoritative SIFS read for {key:?} returned entry for {:?}",
                        value.as_ref().expect("checked entry").key,
                    ),
                });
            }
        }

        let mut cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("SIFS snapshot cache lock is poisoned"))?;
        if cache.authoritative_generation != Some(generation)
            || cache
                .snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.revision > revision)
        {
            return Ok(false);
        }
        let requested_keys = keys.iter().map(String::as_str).collect::<BTreeSet<_>>();
        let requested_key_bytes = requested_keys.iter().map(|key| key.len()).sum::<usize>();
        let mut reset_projection =
            !cache.can_admit_authoritative_scopes(requested_keys.len(), requested_key_bytes);
        let mut snapshot = if reset_projection {
            empty_sifs_kv_snapshot(revision)
        } else {
            cache
                .snapshot
                .as_deref()
                .cloned()
                .unwrap_or_else(|| empty_sifs_kv_snapshot(KvRevision::ZERO))
        };
        snapshot
            .entries
            .retain(|entry| !requested_keys.contains(entry.key.as_str()));
        snapshot
            .entries
            .extend(read.value.iter().filter_map(Clone::clone));
        snapshot.revision = revision;
        snapshot.canonicalize();
        if !sifs_authoritative_projection_entries_fit(&snapshot.entries) {
            reset_projection = true;
            snapshot = empty_sifs_kv_snapshot(revision);
            snapshot
                .entries
                .extend(read.value.iter().filter_map(Clone::clone));
            snapshot.canonicalize();
        }
        snapshot.validate_resource_bounds()?;
        cache.loaded = true;
        if reset_projection {
            cache.clear_authoritative_scopes();
        }
        if !sifs_authoritative_projection_entries_fit(&snapshot.entries) {
            cache.snapshot = Some(Arc::new(empty_sifs_kv_snapshot(revision)));
            return Ok(true);
        }
        for key in keys {
            // Exact reads do not revalidate any prefix which used to cover
            // them, even when the entries are published atomically.
            cache.invalidate_entry_scope(key);
            cache.mark_entry_validated(key);
        }
        snapshot
            .entries
            .retain(|entry| cache.entry_is_validated(&entry.key));
        cache.snapshot = Some(Arc::new(snapshot));
        Ok(true)
    }

    fn read_authoritative_entries_consistently(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<sinomo_runtime::kv::KvEntry>>, KvError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        if keys.len() > SIFS_AUTHORITATIVE_SCOPE_CACHE_MAX_ENTRIES {
            return Err(KvError::ResourceExhausted {
                operation: "SIFS consistent exact-key read".to_string(),
                requested_bytes: keys.len().saturating_mul(std::mem::size_of::<String>()),
            });
        }
        let requested_key_bytes = keys
            .iter()
            .fold(0_usize, |bytes, key| bytes.saturating_add(key.len()));
        if requested_key_bytes > SIFS_AUTHORITATIVE_PROJECTION_MAX_PAYLOAD_BYTES {
            return Err(KvError::ResourceExhausted {
                operation: "SIFS consistent exact-key request".to_string(),
                requested_bytes: requested_key_bytes,
            });
        }

        // Populate the observational projection before taking the cache lock;
        // an unrefreshed client can answer the whole set from that one view.
        let _ = self.cached_snapshot()?;
        for _ in 0..SIFS_AUTHORITATIVE_REPLAN_MAX_ATTEMPTS {
            let (generation, minimum_revision) = {
                let cache = self
                    .snapshot
                    .lock()
                    .map_err(|_| self.kv_error("SIFS snapshot cache lock is poisoned"))?;
                let Some(generation) = cache.authoritative_generation else {
                    return Ok(keys
                        .iter()
                        .map(|key| {
                            cache
                                .snapshot
                                .as_ref()
                                .and_then(|snapshot| snapshot.get(key).cloned())
                        })
                        .collect());
                };
                (
                    generation,
                    cache
                        .snapshot
                        .as_ref()
                        .map_or(KvRevision::ZERO, |snapshot| snapshot.revision),
                )
            };

            let read = self.fetch_authoritative_entries(keys, minimum_revision)?;
            if !self.merge_authoritative_entries_at_one_revision(keys, &read, generation)? {
                continue;
            }
            return Ok(read.value);
        }

        Err(self.superseded_authority_error("consistent exact-key set"))
    }

    fn merge_authoritative_prefix(
        &self,
        prefix: &str,
        read: &sinomo_runtime::kv::KvAuthoritativeRead<Vec<sinomo_runtime::kv::KvEntry>>,
        generation: u64,
    ) -> Result<bool, KvError> {
        let mut cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("SIFS snapshot cache lock is poisoned"))?;
        if cache.authoritative_generation != Some(generation)
            || cache
                .snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.revision > read.authority.revision)
        {
            return Ok(false);
        }
        let mut reset_projection = !cache.can_admit_authoritative_scopes(1, prefix.len());
        let mut snapshot = if reset_projection {
            empty_sifs_kv_snapshot(read.authority.revision)
        } else {
            cache
                .snapshot
                .as_deref()
                .cloned()
                .unwrap_or_else(|| empty_sifs_kv_snapshot(KvRevision::ZERO))
        };
        snapshot
            .entries
            .retain(|entry| !entry.key.starts_with(prefix));
        snapshot.entries.extend(read.value.iter().cloned());
        snapshot.revision = read.authority.revision;
        snapshot.canonicalize();
        if !sifs_authoritative_projection_entries_fit(&snapshot.entries) {
            reset_projection = true;
            snapshot = empty_sifs_kv_snapshot(read.authority.revision);
            snapshot.entries.extend(read.value.iter().cloned());
            snapshot.canonicalize();
        }
        snapshot.validate_resource_bounds()?;
        cache.loaded = true;
        if reset_projection {
            cache.clear_authoritative_scopes();
        }
        if !sifs_authoritative_projection_entries_fit(&snapshot.entries) {
            cache.snapshot = Some(Arc::new(empty_sifs_kv_snapshot(read.authority.revision)));
            return Ok(true);
        }
        cache.invalidate_prefix_scope(prefix);
        cache.mark_prefix_validated(prefix);
        snapshot
            .entries
            .retain(|entry| cache.entry_is_validated(&entry.key));
        cache.snapshot = Some(Arc::new(snapshot));
        Ok(true)
    }

    fn superseded_authority_error(&self, scope: &str) -> KvError {
        KvError::AuthorityUnavailable {
            detail: format!(
                "authoritative SIFS read for {scope:?} was superseded by {} consecutive projection changes",
                SIFS_AUTHORITATIVE_REPLAN_MAX_ATTEMPTS,
            ),
        }
    }

    fn revalidate_mutated_entry(
        &self,
        key: &str,
        minimum_revision: KvRevision,
    ) -> Result<(), KvError> {
        for _ in 0..SIFS_AUTHORITATIVE_REPLAN_MAX_ATTEMPTS {
            let generation = self.ensure_authoritative_projection()?;
            self.invalidate_authoritative_entry_scope(generation, key)?;
            if self
                .read_authoritative_entry(key, Some(minimum_revision), generation)?
                .is_some()
            {
                return Ok(());
            }
        }
        Err(self.superseded_authority_error(key))
    }

    fn revalidate_mutated_prefix(
        &self,
        prefix: &str,
        minimum_revision: KvRevision,
    ) -> Result<(), KvError> {
        for _ in 0..SIFS_AUTHORITATIVE_REPLAN_MAX_ATTEMPTS {
            let generation = self.ensure_authoritative_projection()?;
            self.invalidate_authoritative_prefix_scope(generation, prefix)?;
            if self
                .read_authoritative_prefix(prefix, Some(minimum_revision), generation)?
                .is_some()
            {
                return Ok(());
            }
        }
        Err(self.superseded_authority_error(prefix))
    }

    fn refresh_control_plane_snapshot(&self) -> Result<(), KvError> {
        if self.cluster_paths.state.exists() {
            if self.authoritative_generation()?.is_none() {
                self.replace_cached_snapshot()?;
            }
            // Keep an already-authoritative projection private and compact,
            // but invalidate its scope certificates. Each exact key/prefix
            // used after this explicit refresh crosses one new authority
            // barrier; repeated ordinary reads then reuse that validated
            // scope without another Raft round.
            self.begin_authoritative_refresh_generation()?;
        } else {
            self.replace_cached_snapshot()?;
        }
        Ok(())
    }

    fn replace_cached_snapshot(&self) -> Result<(), KvError> {
        let snapshot = self.read_latest_snapshot()?;
        let mut cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("SIFS snapshot cache lock is poisoned"))?;
        if let Some(snapshot) = snapshot
            && cache
                .snapshot
                .as_ref()
                .is_none_or(|cached| cached.revision <= snapshot.revision)
        {
            cache.snapshot = Some(snapshot);
        }
        cache.loaded = true;
        Ok(())
    }

    fn install_cached_snapshot(&self, snapshot: Arc<KvSnapshot>) -> Result<(), KvError> {
        let mut cache = self
            .snapshot
            .lock()
            .map_err(|_| self.kv_error("SIFS snapshot cache lock is poisoned"))?;
        if cache
            .snapshot
            .as_ref()
            .is_none_or(|cached| cached.revision <= snapshot.revision)
        {
            cache.snapshot = Some(snapshot);
        }
        cache.loaded = true;
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
        // The daemon owns cluster-KV convergence. Cold client hydration
        // consumes its latest local projection without creating another
        // authority-read scheduling edge.
        self.replace_cached_snapshot()?;
        self.snapshot
            .lock()
            .map_err(|_| self.kv_error("SIFS snapshot cache lock is poisoned"))
            .map(|cache| cache.snapshot.clone())
    }

    fn cached_prefix_with_snapshot(
        &self,
        prefix: &str,
    ) -> Result<(Arc<KvSnapshot>, Vec<sinomo_runtime::kv::KvEntry>), KvError> {
        for _ in 0..SIFS_AUTHORITATIVE_REPLAN_MAX_ATTEMPTS {
            // Establish the prefix certificate first. We deliberately ignore
            // the detached result and derive values from the Arc captured
            // below, so a concurrent cache replacement cannot bind catalog A
            // to snapshot identity B.
            let _ = self.range_prefix(prefix)?;
            let cache = self
                .snapshot
                .lock()
                .map_err(|_| self.kv_error("SIFS snapshot cache lock is poisoned"))?;
            if cache.authoritative_generation.is_some() && !cache.prefix_is_validated(prefix) {
                continue;
            }
            let snapshot = cache.snapshot.clone().ok_or_else(|| {
                self.kv_error("SIFS metadata shard catalog projection is unavailable")
            })?;
            let entries = snapshot.range_prefix_limited(
                prefix,
                sinomo_runtime::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
            )?;
            return Ok((snapshot, entries));
        }
        Err(self.superseded_authority_error(prefix))
    }
}

impl DfsControlPlaneKvClient for SifsControlPlaneKvClient {
    fn refresh_latest(&mut self) -> Result<(), KvError> {
        // Routine DFS refreshes need only exact DFS keys and prefixes. A full
        // authoritative snapshot makes every metadata retry proportional to
        // the complete cluster keyspace and contends with the daemon-owned
        // Raft node. Keep this projection private to the SIFS client: it must
        // not be published as a complete materialized KV snapshot.
        self.refresh_control_plane_snapshot()
    }

    fn get(&self, key: &str) -> Result<Option<sinomo_runtime::kv::KvEntry>, KvError> {
        for _ in 0..SIFS_AUTHORITATIVE_REPLAN_MAX_ATTEMPTS {
            match self.authoritative_entry_read_plan(key)? {
                SifsKvReadPlan::Cached(value) => return Ok(value),
                SifsKvReadPlan::Authoritative {
                    generation,
                    minimum_revision,
                } => {
                    if let Some(read) =
                        self.read_authoritative_entry(key, Some(minimum_revision), generation)?
                    {
                        return Ok(read.value);
                    }
                }
            }
        }
        Err(self.superseded_authority_error(key))
    }

    fn get_consistent(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<sinomo_runtime::kv::KvEntry>>, KvError> {
        // One exact-set ReadIndex pins every key to the same committed view.
        self.read_authoritative_entries_consistently(keys)
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<sinomo_runtime::kv::KvEntry>, KvError> {
        for _ in 0..SIFS_AUTHORITATIVE_REPLAN_MAX_ATTEMPTS {
            match self.authoritative_prefix_read_plan(prefix)? {
                SifsKvReadPlan::Cached(value) => return Ok(value),
                SifsKvReadPlan::Authoritative {
                    generation,
                    minimum_revision,
                } => {
                    if let Some(read) =
                        self.read_authoritative_prefix(prefix, Some(minimum_revision), generation)?
                    {
                        return Ok(read.value);
                    }
                }
            }
        }
        Err(self.superseded_authority_error(prefix))
    }

    fn range_prefix_authoritative(
        &mut self,
        prefix: &str,
    ) -> Result<sinomo_runtime::dfs::DfsAuthoritativePrefixProjection, KvError> {
        for _ in 0..SIFS_AUTHORITATIVE_REPLAN_MAX_ATTEMPTS {
            let generation = self.begin_authoritative_refresh_generation()?;
            if let Some(read) = self.read_authoritative_prefix(prefix, None, generation)? {
                return sinomo_runtime::dfs::DfsAuthoritativePrefixProjection::complete(
                    prefix,
                    read.value,
                    read.authority.revision,
                );
            }
        }
        Err(self.superseded_authority_error("operation-bound prefix"))
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
        let request = KvTxnRequest::generated(txn);
        let submitted_digest = sinomo_runtime::kv::kv_command_digest(
            &sinomo_runtime::kv::KvCommand::Txn(request.txn.clone()),
        )?;
        let payload = sinomo_runtime::kv::cluster_kv_txn_rpc_payload(&request)?;
        let response = self.call_local_daemon_kv_rpc(payload, 1024 * 1024)?;
        let result = sinomo_runtime::kv::decode_cluster_kv_txn_rpc_body(&response.body)?;
        sinomo_runtime::kv::validate_cluster_kv_current_term_authority_proof(&result.authority)?;
        if result.request_id != request.request_id {
            return Err(KvError::Serialization {
                detail: format!(
                    "SIFS cluster KV transaction response request id {} does not match {}",
                    result.request_id, request.request_id
                ),
            });
        }
        if result
            .submitted_command_digest
            .is_some_and(|digest| digest != submitted_digest)
        {
            return Err(KvError::Serialization {
                detail: "SIFS cluster KV transaction response does not match the submitted command"
                    .to_string(),
            });
        }
        let response = result.txn_response.ok_or_else(|| KvError::Serialization {
            detail: "cluster KV returned a barrier result for a SIFS control-plane transaction"
                .to_string(),
        })?;
        if result.authority.revision < response.revision {
            return Err(KvError::AuthorityUnavailable {
                detail: format!(
                    "SIFS cluster KV transaction proof revision {} predates result revision {}",
                    result.authority.revision.0, response.revision.0,
                ),
            });
        }
        let generation = self.ensure_authoritative_projection()?;
        let executed = if response.succeeded {
            &request.txn.success
        } else {
            &request.txn.failure
        };
        let mut refreshed_keys = BTreeSet::new();
        let mut refreshed_prefixes = BTreeSet::new();
        let mut invalidate_all = false;
        for operation in executed {
            match operation {
                KvOp::Put { key, .. } | KvOp::Delete { key } => {
                    refreshed_keys.insert(key.as_str());
                }
                KvOp::DeletePrefix { prefix } => {
                    refreshed_prefixes.insert(prefix.as_str());
                }
                KvOp::LeaseRevoke { .. } => invalidate_all = true,
                KvOp::LeaseGrant { .. } | KvOp::LeaseKeepAlive { .. } => {}
            }
        }
        // The transaction is already committed. Retire every certificate it
        // could have changed before the first fallible authority RPC so a
        // middle revalidation failure cannot leave a later stale scope live.
        self.invalidate_authoritative_mutation_scopes(
            generation,
            &refreshed_keys,
            &refreshed_prefixes,
            invalidate_all,
        )?;
        for key in refreshed_keys {
            self.revalidate_mutated_entry(key, response.revision)?;
        }
        for prefix in refreshed_prefixes {
            self.revalidate_mutated_prefix(prefix, response.revision)?;
        }
        Ok(response)
    }
}

fn empty_sifs_kv_snapshot(revision: KvRevision) -> KvSnapshot {
    KvSnapshot {
        revision,
        compacted_revision: KvRevision::ZERO,
        entries: Vec::new(),
        leases: Vec::new(),
        events: Vec::new(),
        committed_requests: Vec::new(),
        expired_requests: Vec::new(),
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
struct SifsOneShotBulkWriteControlPlaneKvClient {
    inner: SifsControlPlaneKvClient,
    local_node_id: NodeId,
    validated_snapshot: Arc<Mutex<Option<Arc<KvSnapshot>>>>,
}

impl SifsOneShotBulkWriteControlPlaneKvClient {
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

        validate_one_shot_bulk_write_catalog(true, &self.local_node_id, &shards)
            .map_err(|error| self.inner.kv_error(error))?;

        *self.validated_snapshot.lock().map_err(|_| {
            self.inner
                .kv_error("DFS catalog validation lock is poisoned")
        })? = Some(snapshot);
        Ok(shards)
    }

    fn cached_catalog(&self) -> Result<(Arc<KvSnapshot>, Vec<DfsMetadataShardConfig>), KvError> {
        let prefix = format!("{}/", metadata_shards_prefix(&sifs_file_system_id()));
        let (snapshot, entries) = self.inner.cached_prefix_with_snapshot(&prefix)?;
        let shards = decode_sifs_metadata_shard_configs(entries)
            .map_err(|error| self.inner.kv_error(error))?;
        Ok((snapshot, shards))
    }

    fn refresh_and_validate(&mut self) -> Result<(), KvError> {
        self.inner.refresh_latest()?;
        self.validate_cached_catalog()
    }
}

impl DfsControlPlaneKvClient for SifsOneShotBulkWriteControlPlaneKvClient {
    fn refresh_latest(&mut self) -> Result<(), KvError> {
        self.refresh_and_validate()
    }

    fn get(&self, key: &str) -> Result<Option<sinomo_runtime::kv::KvEntry>, KvError> {
        let value = self.inner.get(key)?;
        self.validate_cached_catalog()?;
        Ok(value)
    }

    fn get_consistent(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<sinomo_runtime::kv::KvEntry>>, KvError> {
        let values = self.inner.get_consistent(keys)?;
        self.validate_cached_catalog()?;
        Ok(values)
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<sinomo_runtime::kv::KvEntry>, KvError> {
        let entries = self.inner.range_prefix(prefix)?;
        self.validate_cached_catalog()?;
        Ok(entries)
    }

    fn range_prefix_authoritative(
        &mut self,
        prefix: &str,
    ) -> Result<sinomo_runtime::dfs::DfsAuthoritativePrefixProjection, KvError> {
        let projection = self.inner.range_prefix_authoritative(prefix)?;
        self.validate_cached_catalog()?;
        Ok(projection)
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
    let prefix = format!("{}/", metadata_shards_prefix(&file_system_id));
    let entries = read_sifs_materialized_cluster_kv_snapshot(cluster_paths)
        .map_err(|error| format!("cannot read DFS metadata shard config: {error}"))?
        .map_or_else(
            || Ok(Vec::new()),
            |snapshot| {
                snapshot.range_prefix_limited(
                    &prefix,
                    sinomo_runtime::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
                )
            },
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

fn validate_one_shot_bulk_write_catalog(
    daemon_backed_transport: bool,
    local_node_id: &NodeId,
    shards: &[DfsMetadataShardConfig],
) -> Result<(), String> {
    if !daemon_backed_transport {
        return Err(
            "one-shot SIFS bulk write requires the resident daemon DFS transport".to_string(),
        );
    }
    if shards.is_empty() {
        return Err(
            "one-shot SIFS bulk write requires a nonempty metadata shard catalog".to_string(),
        );
    }
    let file_system_id = sifs_file_system_id();
    let mut shard_ids = BTreeSet::new();
    for shard in shards {
        if shard.file_system_id != file_system_id {
            return Err(format!(
                "one-shot SIFS bulk write catalog contains shard {} for filesystem {}",
                shard.shard_id, shard.file_system_id
            ));
        }
        if !shard_ids.insert(shard.shard_id.clone()) {
            return Err(format!(
                "one-shot SIFS bulk write catalog contains duplicate shard id {}",
                shard.shard_id
            ));
        }
    }
    if let Some(shard) = shards
        .iter()
        .find(|shard| shard.primary_node_id == *local_node_id)
    {
        return Err(format!(
            "one-shot SIFS bulk write is fenced because local node {local_node_id} is primary for metadata shard {} at epoch {}",
            shard.shard_id, shard.epoch
        ));
    }
    validate_one_shot_bulk_write_paths(
        shards,
        [
            "/",
            "/services/.catalog-coverage",
            "/users/.catalog-coverage",
        ],
    )?;
    Ok(())
}

fn validate_one_shot_bulk_write_paths<'a>(
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
                "one-shot SIFS bulk write metadata shard catalog has no route for {path}"
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
    let routable_node_ids = discovered_cluster_dfs_peer_node_ids(&cluster_paths, &state)
        .unwrap_or_else(|_| vec![local_node_id.clone()])
        .into_iter()
        .collect::<BTreeSet<_>>();
    let graph = network::ClusterConnectivityGraph::read_for_local(&cluster_paths, &state);
    let Ok(Some(snapshot)) = read_sifs_materialized_cluster_kv_snapshot(&cluster_paths) else {
        return candidates;
    };
    let Ok(entries) = snapshot.range_prefix_limited(
        content_services_prefix(),
        sinomo_runtime::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
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
        .sort_by_key(|node_id| sifs_replica_candidate_rank(graph, local_node_id, node_id));
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
            .object_rpc_call_to_target_with_deadline_and_affinity_async(
                target_node_id,
                call,
                options.connect_timeout,
                options.overall_timeout,
                options.observe_connectivity,
                sinomo_runtime::api::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
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

fn block_on_sifs_network_rpc<F, T>(
    rpc_runtime: &SifsRpcRuntime,
    future: F,
) -> Result<T, network::NetworkError>
where
    F: Future<Output = Result<T, network::NetworkError>>,
{
    if let Ok(current) = tokio::runtime::Handle::try_current() {
        if matches!(
            current.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            return tokio::task::block_in_place(|| rpc_runtime.runtime.block_on(future));
        }
        return Err(network::NetworkError::Io {
            operation: "materialize SIFS cluster KV response".to_string(),
            detail: "cannot block a current-thread Tokio runtime; use the async transport"
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

#[cfg(any(unix, test))]
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
    use std::sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    };

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

    #[derive(Debug)]
    struct RecordingSifsKvDaemonTransport {
        local_node_id: NodeId,
        routes: Mutex<Vec<String>>,
        committed: AtomicBool,
        commit_after_next_entry_response: AtomicBool,
        fail_authoritative_entry_key_once: Mutex<Option<String>>,
        wrong_authoritative_entry_key_once: AtomicBool,
        out_of_prefix_authoritative_entry_once: AtomicBool,
        unknown_exact_set_route_once: AtomicBool,
        fail_exact_set_route_once: AtomicBool,
        exact_set_keys: Mutex<Vec<String>>,
        generation_race: Mutex<Option<(String, Weak<Mutex<SifsKvSnapshotCache>>)>>,
    }

    impl RecordingSifsKvDaemonTransport {
        fn new(local_node_id: NodeId) -> Self {
            Self {
                local_node_id,
                routes: Mutex::new(Vec::new()),
                committed: AtomicBool::new(false),
                commit_after_next_entry_response: AtomicBool::new(false),
                fail_authoritative_entry_key_once: Mutex::new(None),
                wrong_authoritative_entry_key_once: AtomicBool::new(false),
                out_of_prefix_authoritative_entry_once: AtomicBool::new(false),
                unknown_exact_set_route_once: AtomicBool::new(false),
                fail_exact_set_route_once: AtomicBool::new(false),
                exact_set_keys: Mutex::new(Vec::new()),
                generation_race: Mutex::new(None),
            }
        }

        fn authority(revision: KvRevision) -> sinomo_runtime::kv::KvAuthorityProof {
            sinomo_runtime::kv::KvAuthorityProof {
                term: 3,
                commit_index: revision.0.saturating_add(10),
                revision,
                basis: sinomo_runtime::kv::KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            }
        }

        fn routes(&self) -> Vec<String> {
            self.routes.lock().expect("recorded routes").clone()
        }

        fn advance_generation_during_next_route(
            &self,
            route: &str,
            client: &SifsControlPlaneKvClient,
        ) {
            *self.generation_race.lock().expect("generation race") =
                Some((route.to_string(), Arc::downgrade(&client.snapshot)));
        }

        fn commit_after_next_entry_response(&self) {
            self.commit_after_next_entry_response
                .store(true, Ordering::SeqCst);
        }

        fn fail_authoritative_entry_key_once(&self, key: &str) {
            *self
                .fail_authoritative_entry_key_once
                .lock()
                .expect("entry failure key") = Some(key.to_string());
        }

        fn return_wrong_authoritative_entry_key_once(&self) {
            self.wrong_authoritative_entry_key_once
                .store(true, Ordering::SeqCst);
        }

        fn return_out_of_prefix_authoritative_entry_once(&self) {
            self.out_of_prefix_authoritative_entry_once
                .store(true, Ordering::SeqCst);
        }

        fn return_unknown_exact_set_route_once(&self) {
            self.unknown_exact_set_route_once
                .store(true, Ordering::SeqCst);
        }

        fn fail_exact_set_route_once(&self) {
            self.fail_exact_set_route_once.store(true, Ordering::SeqCst);
        }

        fn trigger_generation_race(&self, route: &str) {
            let race = {
                let mut pending = self.generation_race.lock().expect("generation race");
                match pending.as_ref() {
                    Some((expected, _)) if expected == route => pending.take(),
                    _ => None,
                }
            };
            let Some((_, cache)) = race else {
                return;
            };
            let cache = cache.upgrade().expect("SIFS cache remains live during RPC");
            cache
                .lock()
                .expect("SIFS cache")
                .begin_authoritative_generation();
            // The first response was selected from revision 7 before this
            // refresh edge. The replanned request observes revision 8.
            self.committed.store(true, Ordering::SeqCst);
        }
    }

    impl DfsPeerTransport for RecordingSifsKvDaemonTransport {
        fn call_object_rpc(
            &self,
            target_node_id: &NodeId,
            call: network::ObjectRpcCall,
            _options: DfsObjectRpcSendOptions,
        ) -> Result<network::ObjectRpcResult, DfsPeerTransportError> {
            assert_eq!(target_node_id, &self.local_node_id);
            let route = call.request.route.clone();
            self.routes
                .lock()
                .expect("record route")
                .push(route.clone());
            let payload = match route.as_str() {
                sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE => {
                    assert!(
                        call.request.body.is_empty(),
                        "exact-key-set values must remain outside the bounded RPC request head"
                    );
                    let runtime = SifsRpcRuntime::new().expect("exact-key-set fixture runtime");
                    let encoded = block_on_sifs_network_rpc(
                        &runtime,
                        call.body.collect_to_vec(
                            sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_MAX_PAYLOAD_BYTES
                                .saturating_add(1024 * 1024),
                        ),
                    )
                    .expect("collect streamed bounded SIFS exact-key-set request");
                    let request = sinomo_runtime::kv::decode_cluster_kv_authoritative_entries_rpc_request_body(
                        &encoded,
                    )
                    .expect("decode streamed bounded SIFS exact-key-set request");
                    *self.exact_set_keys.lock().expect("exact-set keys") = request.keys.clone();
                    if self
                        .unknown_exact_set_route_once
                        .swap(false, Ordering::SeqCst)
                    {
                        return Err(DfsPeerTransportError::from_network(
                            network::NetworkError::UnknownRoute {
                                route: route.clone(),
                            },
                        ));
                    }
                    if self.fail_exact_set_route_once.swap(false, Ordering::SeqCst) {
                        return Err(DfsPeerTransportError::new(
                            "injected exact-key-set transport failure",
                        ));
                    }
                    let fail_key = {
                        let mut pending = self
                            .fail_authoritative_entry_key_once
                            .lock()
                            .expect("entry failure key");
                        match pending.as_deref() {
                            Some(key) if request.keys.iter().any(|requested| requested == key) => {
                                pending.take()
                            }
                            _ => None,
                        }
                    };
                    if let Some(key) = fail_key {
                        return Err(DfsPeerTransportError::new(format!(
                            "injected authority-read failure for {key}"
                        )));
                    }
                    let mut committed = self.committed.load(Ordering::SeqCst);
                    if self
                        .commit_after_next_entry_response
                        .swap(false, Ordering::SeqCst)
                    {
                        // The authority selects one coherent post-transition
                        // view for the complete set even when the transition
                        // races request arrival.
                        self.committed.store(true, Ordering::SeqCst);
                        committed = true;
                    }
                    self.trigger_generation_race(&route);
                    let revision = KvRevision(if committed { 8 } else { 7 });
                    let mut values = request
                        .keys
                        .iter()
                        .map(|key| match key.as_str() {
                            "/dfs/test/daemon-projection" => Some(sinomo_runtime::kv::KvEntry {
                                key: key.clone(),
                                value: b"fresh".to_vec(),
                                create_revision: KvRevision(7),
                                modify_revision: KvRevision(7),
                                version: sinomo_runtime::kv::KvVersion(1),
                                lease_id: None,
                            }),
                            "/dfs/test/committed" if committed => {
                                Some(sinomo_runtime::kv::KvEntry {
                                    key: key.clone(),
                                    value: b"value".to_vec(),
                                    create_revision: KvRevision(8),
                                    modify_revision: KvRevision(8),
                                    version: sinomo_runtime::kv::KvVersion(1),
                                    lease_id: None,
                                })
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    if self
                        .wrong_authoritative_entry_key_once
                        .swap(false, Ordering::SeqCst)
                    {
                        values[0] = Some(sinomo_runtime::kv::KvEntry {
                            key: "/dfs/foreign/entry".to_string(),
                            value: b"foreign".to_vec(),
                            create_revision: revision,
                            modify_revision: revision,
                            version: sinomo_runtime::kv::KvVersion(1),
                            lease_id: None,
                        });
                    }
                    sinomo_runtime::kv::cluster_kv_authoritative_entries_response_rpc_payload(
                        &request.keys,
                        &sinomo_runtime::kv::KvAuthoritativeRead {
                            value: values,
                            authority: Self::authority(revision),
                        },
                    )
                    .expect("encode bounded SIFS exact-key-set response")
                }
                sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE => {
                    let _request = sinomo_runtime::kv::decode_cluster_kv_authoritative_snapshot_rpc_request_body(
                        &call.request.body,
                    )
                    .expect("decode SIFS authoritative snapshot request");
                    let keys = self.exact_set_keys.lock().expect("exact-set keys").clone();
                    let mut snapshot = empty_sifs_kv_snapshot(KvRevision(8));
                    snapshot.entries = keys
                        .iter()
                        .map(|key| sinomo_runtime::kv::KvEntry {
                            key: key.clone(),
                            value: format!("authoritative:{key}").into_bytes(),
                            create_revision: KvRevision(8),
                            modify_revision: KvRevision(8),
                            version: sinomo_runtime::kv::KvVersion(1),
                            lease_id: None,
                        })
                        .collect();
                    snapshot.canonicalize();
                    sinomo_runtime::kv::cluster_kv_authoritative_snapshot_response_rpc_payload(
                        &sinomo_runtime::kv::KvAuthoritativeRead {
                            value: snapshot,
                            authority: Self::authority(KvRevision(8)),
                        },
                    )
                    .expect("encode SIFS authoritative snapshot response")
                }
                sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE => {
                    let request =
                        sinomo_runtime::kv::decode_cluster_kv_authoritative_entry_rpc_request_body(
                            &call.request.body,
                        )
                        .expect("decode bounded SIFS entry request");
                    let fail = {
                        let mut pending = self
                            .fail_authoritative_entry_key_once
                            .lock()
                            .expect("entry failure key");
                        if pending.as_deref() == Some(request.key.as_str()) {
                            pending.take();
                            true
                        } else {
                            false
                        }
                    };
                    if fail {
                        return Err(DfsPeerTransportError::new(format!(
                            "injected authority-read failure for {}",
                            request.key
                        )));
                    }
                    let committed = self.committed.load(Ordering::SeqCst);
                    self.trigger_generation_race(&route);
                    if self
                        .commit_after_next_entry_response
                        .swap(false, Ordering::SeqCst)
                    {
                        // Keep this response on the selected pre-transition
                        // revision, then expose the transition to the next
                        // exact-key request in the same attempted set.
                        self.committed.store(true, Ordering::SeqCst);
                    }
                    let (mut value, revision) = match request.key.as_str() {
                        "/dfs/test/daemon-projection" => (
                            Some(sinomo_runtime::kv::KvEntry {
                                key: request.key,
                                value: b"fresh".to_vec(),
                                create_revision: KvRevision(7),
                                modify_revision: KvRevision(7),
                                version: sinomo_runtime::kv::KvVersion(1),
                                lease_id: None,
                            }),
                            KvRevision(if committed { 8 } else { 7 }),
                        ),
                        "/dfs/test/committed" if committed => (
                            Some(sinomo_runtime::kv::KvEntry {
                                key: request.key,
                                value: b"value".to_vec(),
                                create_revision: KvRevision(8),
                                modify_revision: KvRevision(8),
                                version: sinomo_runtime::kv::KvVersion(1),
                                lease_id: None,
                            }),
                            KvRevision(8),
                        ),
                        _ => (None, KvRevision(if committed { 8 } else { 7 })),
                    };
                    if self
                        .wrong_authoritative_entry_key_once
                        .swap(false, Ordering::SeqCst)
                    {
                        value = Some(sinomo_runtime::kv::KvEntry {
                            key: "/dfs/foreign/entry".to_string(),
                            value: b"foreign".to_vec(),
                            create_revision: revision,
                            modify_revision: revision,
                            version: sinomo_runtime::kv::KvVersion(1),
                            lease_id: None,
                        });
                    }
                    sinomo_runtime::kv::cluster_kv_authoritative_entry_response_rpc_payload(
                        &sinomo_runtime::kv::KvAuthoritativeRead {
                            value,
                            authority: Self::authority(revision),
                        },
                    )
                    .expect("encode bounded SIFS entry response")
                }
                sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE => {
                    let request = sinomo_runtime::kv::decode_cluster_kv_authoritative_range_prefix_rpc_request_body(
                        &call.request.body,
                    )
                    .expect("decode bounded SIFS projection request");
                    assert_eq!(request.prefix, "/dfs/test/");
                    let committed = self.committed.load(Ordering::SeqCst);
                    self.trigger_generation_race(&route);
                    let mut value = vec![sinomo_runtime::kv::KvEntry {
                        key: "/dfs/test/daemon-projection".to_string(),
                        value: b"fresh".to_vec(),
                        create_revision: KvRevision(7),
                        modify_revision: KvRevision(7),
                        version: sinomo_runtime::kv::KvVersion(1),
                        lease_id: None,
                    }];
                    if committed {
                        value.push(sinomo_runtime::kv::KvEntry {
                            key: "/dfs/test/committed".to_string(),
                            value: b"value".to_vec(),
                            create_revision: KvRevision(8),
                            modify_revision: KvRevision(8),
                            version: sinomo_runtime::kv::KvVersion(1),
                            lease_id: None,
                        });
                    }
                    if self
                        .out_of_prefix_authoritative_entry_once
                        .swap(false, Ordering::SeqCst)
                    {
                        let revision = KvRevision(if committed { 8 } else { 7 });
                        value.push(sinomo_runtime::kv::KvEntry {
                            key: "/dfs/test-sibling".to_string(),
                            value: b"foreign".to_vec(),
                            create_revision: revision,
                            modify_revision: revision,
                            version: sinomo_runtime::kv::KvVersion(1),
                            lease_id: None,
                        });
                    }
                    sinomo_runtime::kv::cluster_kv_authoritative_range_prefix_response_rpc_payload(
                        &sinomo_runtime::kv::KvAuthoritativeRead {
                            value,
                            authority: Self::authority(KvRevision(if committed { 8 } else { 7 })),
                        },
                    )
                    .expect("encode bounded SIFS projection response")
                }
                sinomo_runtime::kv::CLUSTER_KV_TXN_ROUTE => {
                    let request = sinomo_runtime::kv::decode_cluster_kv_txn_rpc_request_body(
                        &call.request.body,
                    )
                    .expect("decode SIFS transaction request");
                    let command = sinomo_runtime::kv::KvCommand::Txn(request.txn.clone());
                    let digest = sinomo_runtime::kv::kv_command_digest(&command)
                        .expect("transaction digest");
                    self.committed.store(true, Ordering::SeqCst);
                    let snapshot = sinomo_runtime::kv::InProcessKvStore::new()
                        .try_snapshot()
                        .expect("empty compact-response snapshot");
                    sinomo_runtime::kv::cluster_kv_txn_response_rpc_payload(
                        &sinomo_runtime::kv::ClusterKvTxnCommitResult {
                            request_id: request.request_id,
                            command_digest: digest,
                            snapshot,
                            txn_response: Some(KvTxnResponse {
                                succeeded: true,
                                revision: KvRevision(8),
                                responses: Vec::new(),
                            }),
                            authority: Self::authority(KvRevision(8)),
                            replayed_committed_request: false,
                        },
                    )
                    .expect("encode compact SIFS transaction response")
                }
                _ => panic!("SIFS control plane used unexpected daemon route {route}"),
            };
            assert!(
                payload.body.len() < 64 * 1024,
                "routine SIFS control-plane RPC returned an unbounded snapshot payload"
            );
            Ok(network::ObjectRpcResult::value(
                network::ObjectRpcResponse::from_encoded_body(payload.route, payload.body),
            ))
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
    fn typed_local_reads_distinguish_absence_from_content_unavailability() {
        let missing = sifs_distributed_read_outcome(DfsPosixDistributedFileReadOutcome::NotFound {
            path: "/channels/missing.json".to_string(),
        })
        .expect_err("missing local path");
        assert_eq!(missing, "SIFS path not found: /channels/missing.json");

        let unavailable =
            sifs_distributed_read_outcome(DfsPosixDistributedFileReadOutcome::ContentUnavailable {
                path: "/channels/present.json".to_string(),
                detail: "metadata exists but no replica is readable".to_string(),
            })
            .expect_err("unavailable local content");
        assert!(unavailable.starts_with("SIFS content is unavailable at "));
        assert!(!unavailable.starts_with("SIFS path not found:"));

        let prefix_unavailable = sifs_distributed_prefix_read_outcome(
            DfsPosixDistributedFilePrefixReadOutcome::ContentUnavailable {
                path: "/files/present.bin".to_string(),
                detail: "exact prefix version is not readable".to_string(),
            },
        )
        .expect_err("unavailable local prefix");
        assert!(prefix_unavailable.starts_with("SIFS content is unavailable at "));
        assert!(!prefix_unavailable.starts_with("SIFS path not found:"));
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
    fn one_shot_bulk_write_catalog_requires_daemon_and_no_local_primary() {
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
            validate_one_shot_bulk_write_catalog(true, &outsider, std::slice::from_ref(&shard),)
                .is_ok()
        );
        assert!(
            validate_one_shot_bulk_write_catalog(false, &outsider, std::slice::from_ref(&shard),)
                .is_err()
        );
        assert!(
            validate_one_shot_bulk_write_catalog(true, &primary, std::slice::from_ref(&shard),)
                .is_err()
        );
        assert!(
            validate_one_shot_bulk_write_catalog(true, &replica, std::slice::from_ref(&shard),)
                .is_ok()
        );
        assert!(validate_one_shot_bulk_write_catalog(true, &outsider, &[]).is_err());

        let services = DfsMetadataShardConfig {
            shard_id: "services".to_string(),
            assignment: sinomo_runtime::dfs::DfsMetadataShardAssignment::PathPrefix {
                prefix: "/services".to_string(),
            },
            ..shard.clone()
        };
        let incomplete =
            validate_one_shot_bulk_write_catalog(true, &outsider, &[shard.clone(), services])
                .expect_err("a partial default SIFS split must not be treated as complete");
        assert!(
            incomplete.contains("cannot route /users"),
            "unexpected incomplete-catalog fence: {incomplete}"
        );

        let duplicate =
            validate_one_shot_bulk_write_catalog(true, &outsider, &[shard.clone(), shard])
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
    fn one_shot_bulk_write_control_plane_refresh_fences_new_local_primary() {
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

        let control_plane = SifsOneShotBulkWriteControlPlaneKvClient::new(
            &data_dir,
            &cluster_paths,
            local_node_id.clone(),
        );
        control_plane
            .inner
            .replace_cached_snapshot()
            .expect("load remote-primary catalog");
        control_plane
            .validate_cached_catalog()
            .expect("replica-only catalog is eligible");

        shards[0].primary_node_id = local_node_id;
        shards[0].epoch += 1;
        write_metadata_shard_snapshot(&cluster_paths, &shards);
        control_plane
            .inner
            .replace_cached_snapshot()
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
    fn one_shot_bulk_write_reuses_the_validated_installed_catalog() {
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

        let control_plane =
            SifsOneShotBulkWriteControlPlaneKvClient::new(&data_dir, &cluster_paths, local_node_id);
        let authoritative_snapshot = read_sifs_materialized_cluster_kv_snapshot(&cluster_paths)
            .expect("read authoritative fixture snapshot")
            .expect("authoritative fixture snapshot");
        control_plane
            .inner
            .install_cached_snapshot(authoritative_snapshot)
            .expect("install returned authoritative catalog");

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
    fn one_shot_bulk_write_rejects_authority_from_another_data_dir() {
        let root = temp_sifs_data_dir("one-shot-foreign-authority");
        let data_dir = SinomoDataDir::new(&root);
        seed_observation_cluster(&data_dir);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let snapshot = read_sifs_materialized_cluster_kv_snapshot(&cluster_paths)
            .expect("read fixture snapshot")
            .expect("fixture snapshot");
        let authority = sinomo_runtime::kv::KvAuthorityProof {
            term: 4,
            commit_index: 12,
            revision: snapshot.revision,
            basis: sinomo_runtime::kv::KvAuthorityBasis::CurrentTermQuorum {
                voter_acks: 2,
                required_quorum: 2,
            },
        };
        let token = sinomo_runtime::test::SinomoTestContext::new(&root)
            .try_with_authoritative_cluster_snapshot(
                sinomo_runtime::kv::KvAuthoritativeRead {
                    value: (*snapshot).clone(),
                    authority,
                },
                root.join("another-cluster/kv-snapshot.sobj"),
            )
            .expect("valid but foreign authoritative snapshot")
            .authoritative_cluster_snapshot()
            .expect("authoritative token");
        let error = SifsOneShotBulkWriteRequester::open(&data_dir, Some(token))
            .expect_err("foreign authority source must fail before transport open");
        assert!(
            error.contains("does not match local cluster snapshot"),
            "unexpected foreign authority error: {error}"
        );
    }

    #[test]
    fn clustered_sifs_inspection_uses_only_the_exact_main_daemon_owner() {
        let root = temp_sifs_data_dir("inspection-daemon-owner");
        let data_dir = SinomoDataDir::new(&root);
        seed_observation_cluster(&data_dir);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let state = cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("read fixture cluster state");
        let local_member = state
            .members
            .iter()
            .find(|member| member.node_id == state.local_node_id)
            .expect("local member");
        let daemon_paths = sinomo_runtime::daemon::DaemonPaths::new(&data_dir);
        let status = sinomo_runtime::daemon::DaemonStatus::new(
            &sinomo_runtime::daemon::DaemonRunOptions::default(),
            &daemon_paths,
            Some(sinomo_runtime::daemon::DaemonClusterStatus {
                cluster_id: state.cluster_id.clone(),
                node_id: state.local_node_id.clone(),
                process: sinomo_runtime::node::DAEMON_MAIN_CLUSTER_ROLE.to_string(),
                member_role: local_member.role.clone(),
                member_state: local_member.state.clone(),
            }),
        );

        assert!(
            sifs_clustered_daemon_client_from_probe(
                &state,
                &data_dir,
                sinomo_runtime::daemon::DaemonProbeStatus::Running(status.clone()),
            )
            .expect("exact main daemon")
            .is_some()
        );

        let mut shadow_status = status.clone();
        shadow_status
            .cluster
            .as_mut()
            .expect("cluster status")
            .process = sinomo_runtime::node::DAEMON_SHADOW_CLUSTER_ROLE.to_string();
        let shadow_error = sifs_clustered_daemon_client_from_probe(
            &state,
            &data_dir,
            sinomo_runtime::daemon::DaemonProbeStatus::Running(shadow_status),
        )
        .expect_err("shadow must not own clustered SIFS inspection");
        assert!(shadow_error.contains("expected"), "{shadow_error}");

        let locked_error = sifs_clustered_daemon_client_from_probe(
            &state,
            &data_dir,
            sinomo_runtime::daemon::DaemonProbeStatus::Locked {
                status: Some(status),
            },
        )
        .expect_err("a locked owner must not fall back to process-local storage");
        assert!(locked_error.contains("owns clustered SIFS storage"));

        assert!(
            sifs_clustered_daemon_client_from_probe(
                &state,
                &data_dir,
                sinomo_runtime::daemon::DaemonProbeStatus::NotRunning,
            )
            .expect("standalone inspection")
            .is_none(),
            "a genuinely offline standalone inspector may use its own resident fixture"
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
    fn sifs_observation_uses_invocation_authority_over_stale_materialized_topology() {
        let root = temp_sifs_data_dir("observation-authoritative-topology");
        let data_dir = SinomoDataDir::new(&root);
        let shards = seed_observation_cluster(&data_dir);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let materialized = read_sifs_materialized_cluster_kv_snapshot(&cluster_paths)
            .expect("read materialized snapshot")
            .expect("materialized snapshot");
        let mut authoritative = materialized.as_ref().clone();
        let mut current_users = shards
            .iter()
            .find(|shard| shard.shard_id == "users")
            .expect("users shard")
            .clone();
        current_users.epoch = 9;
        current_users.primary_node_id =
            NodeId::from_legacy_named("node_sifs_observation_current_users").expect("current node");
        let users_key = sinomo_runtime::dfs::metadata_shard_key(
            &current_users.file_system_id,
            &current_users.shard_id,
        );
        let KvOp::Put { value, .. } =
            KvOp::put_serde(&users_key, &current_users, None).expect("encode current users shard")
        else {
            unreachable!("put_serde constructs a put operation")
        };
        let revision = KvRevision(authoritative.revision.0.saturating_add(1));
        let users_entry = authoritative
            .entries
            .iter_mut()
            .find(|entry| entry.key == users_key)
            .expect("materialized users entry");
        users_entry.value = value;
        users_entry.modify_revision = revision;
        users_entry.version = sinomo_runtime::kv::KvVersion(users_entry.version.0 + 1);
        authoritative.revision = revision;
        authoritative.canonicalize();
        let token = sinomo_runtime::test::SinomoTestContext::new(&root)
            .try_with_authoritative_cluster_snapshot(
                sinomo_runtime::kv::KvAuthoritativeRead {
                    value: authoritative,
                    authority: sinomo_runtime::kv::KvAuthorityProof {
                        term: 4,
                        commit_index: revision.0.saturating_add(10),
                        revision,
                        basis: sinomo_runtime::kv::KvAuthorityBasis::CurrentTermQuorum {
                            voter_acks: 1,
                            required_quorum: 1,
                        },
                    },
                },
                cluster_paths.kv_paths().snapshot.clone(),
            )
            .expect("valid authoritative context")
            .authoritative_cluster_snapshot()
            .expect("authoritative token");

        let observation = SifsFileManagerObservation::open(&data_dir).expect("observation");
        assert_eq!(
            observation
                .metadata_shard_for_path("/users/layout-probe")
                .expect("materialized users shard")
                .expect("materialized users assignment")
                .epoch,
            1,
            "the fixture must retain the stale observational checkpoint"
        );
        assert_eq!(
            observation
                .metadata_shard_for_path_at_authority("/users/layout-probe", &token)
                .expect("authoritative users shard")
                .expect("authoritative users assignment"),
            current_users
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
    fn sifs_open_read_refresh_and_txn_stay_off_daemon_owned_raft() {
        let root = temp_sifs_data_dir("control-plane-compact-daemon-routes");
        let data_dir = SinomoDataDir::new(&root);
        let shards = seed_observation_cluster(&data_dir);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let state = cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("cluster state without KV");
        let mut oversized_materialized = read_sifs_materialized_cluster_kv_snapshot(&cluster_paths)
            .expect("read materialized snapshot")
            .expect("materialized snapshot")
            .as_ref()
            .clone();
        for index in 0..=sinomo_runtime::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES {
            oversized_materialized
                .entries
                .push(sinomo_runtime::kv::KvEntry {
                    key: format!("/dfs/noise/{index:08}"),
                    value: Vec::new(),
                    create_revision: oversized_materialized.revision,
                    modify_revision: oversized_materialized.revision,
                    version: sinomo_runtime::kv::KvVersion(1),
                    lease_id: None,
                });
        }
        oversized_materialized.canonicalize();
        sinomo_runtime::kv::write_cluster_kv_snapshot(
            &cluster_paths.kv_paths(),
            &oversized_materialized,
        )
        .expect("write oversized materialized DFS namespace");
        assert!(
            oversized_materialized
                .entries
                .iter()
                .filter(|entry| entry.key.starts_with("/dfs/"))
                .count()
                > sinomo_runtime::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
            "fixture must exceed the global DFS range cardinality limit"
        );
        let _daemon_owned_raft =
            cluster::open_cluster_raft_kv_node(&cluster_paths, &state.local_node_id)
                .expect("hold canonical Raft node as the daemon owner");
        assert_eq!(
            read_sifs_metadata_shard_configs(&cluster_paths)
                .expect("open-path metadata catalog from materialized projection"),
            shards,
        );
        assert!(
            sifs_replica_candidates(&data_dir, &state.local_node_id).contains(&state.local_node_id),
            "open-path replica discovery must remain available with daemon-owned Raft locked"
        );

        let transport = Arc::new(RecordingSifsKvDaemonTransport::new(
            state.local_node_id.clone(),
        ));
        let mut client = SifsControlPlaneKvClient::new(&data_dir, &cluster_paths)
            .with_peer_transport(transport.clone());

        let materialized_key =
            sinomo_runtime::dfs::metadata_shard_key(&shards[0].file_system_id, &shards[0].shard_id);
        assert!(
            client
                .get(&materialized_key)
                .expect("cold point read from daemon projection")
                .is_some(),
            "cold SIFS point reads must use the materialized projection"
        );
        assert!(
            !client
                .range_prefix("/dfs/filesystems/")
                .expect("cold range read from daemon projection")
                .is_empty(),
            "cold SIFS range reads must use the materialized projection"
        );
        client
            .refresh_latest()
            .expect("refresh bounded authoritative DFS projection");
        assert_eq!(
            client
                .get("/dfs/test/daemon-projection")
                .expect("read refreshed point entry")
                .expect("refreshed point entry")
                .value,
            b"fresh"
        );
        assert_eq!(
            client
                .range_prefix("/dfs/test/")
                .expect("read refreshed prefix")
                .len(),
            1
        );
        let txn = client
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![KvOp::Put {
                    key: "/dfs/test/committed".to_string(),
                    value: b"value".to_vec(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            })
            .expect("commit through resident daemon route");
        assert_eq!(txn.revision, KvRevision(8));
        assert!(
            read_sifs_materialized_cluster_kv_snapshot(&cluster_paths)
                .expect("read intentionally stale materialized projection")
                .expect("materialized projection")
                .get("/dfs/test/committed")
                .is_none(),
            "fixture must prove read-your-write without waiting for daemon materialization"
        );
        let cached = client
            .cached_snapshot()
            .expect("read coherent DFS projection")
            .expect("coherent DFS projection");
        assert_eq!(cached.revision, KvRevision(8));
        assert_eq!(
            cached
                .get("/dfs/test/committed")
                .expect("committed key is visible before txn returns")
                .value,
            b"value",
        );
        assert!(
            cached.entries.len() < 16,
            "exact authoritative overlays must not clone the oversized materialized keyspace"
        );
        client
            .refresh_latest()
            .expect("repeat refresh preserves authoritative projection");
        let refreshed = client
            .cached_snapshot()
            .expect("read repeated-refresh projection")
            .expect("repeated-refresh projection");
        assert!(
            Arc::ptr_eq(&cached, &refreshed),
            "repeat refresh must not re-import the oversized materialized snapshot"
        );
        assert_eq!(
            transport.routes(),
            vec![
                sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
                sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE.to_string(),
                sinomo_runtime::kv::CLUSTER_KV_TXN_ROUTE.to_string(),
                sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
            ],
            "SIFS refresh/txn must use only exact bounded routes, never a global DFS/full snapshot route"
        );
    }

    #[test]
    fn sifs_txn_failure_mid_revalidation_retires_every_committed_scope() {
        let root = temp_sifs_data_dir("control-plane-txn-middle-read-failure");
        let data_dir = SinomoDataDir::new(&root);
        seed_observation_cluster(&data_dir);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let state = cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("cluster state without KV");
        let transport = Arc::new(RecordingSifsKvDaemonTransport::new(
            state.local_node_id.clone(),
        ));
        let mut client = SifsControlPlaneKvClient::new(&data_dir, &cluster_paths)
            .with_peer_transport(transport.clone());
        client
            .refresh_latest()
            .expect("begin authoritative generation");

        let keys = [
            "/dfs/test/txn-a".to_string(),
            "/dfs/test/txn-b".to_string(),
            "/dfs/test/txn-c".to_string(),
        ];
        {
            let mut snapshot = empty_sifs_kv_snapshot(KvRevision(7));
            snapshot.entries = keys
                .iter()
                .map(|key| sinomo_runtime::kv::KvEntry {
                    key: key.clone(),
                    value: b"stale".to_vec(),
                    create_revision: KvRevision(7),
                    modify_revision: KvRevision(7),
                    version: sinomo_runtime::kv::KvVersion(1),
                    lease_id: None,
                })
                .collect();
            snapshot.canonicalize();
            let mut cache = client.snapshot.lock().expect("SIFS projection cache");
            cache.snapshot = Some(Arc::new(snapshot));
            cache.loaded = true;
            for key in &keys {
                cache.mark_entry_validated(key);
            }
        }
        transport.fail_authoritative_entry_key_once(&keys[1]);

        let error = client
            .txn(KvTxn {
                compares: Vec::new(),
                success: keys
                    .iter()
                    .map(|key| KvOp::Put {
                        key: key.clone(),
                        value: b"committed".to_vec(),
                        lease_id: None,
                    })
                    .collect(),
                failure: Vec::new(),
            })
            .expect_err("second committed-scope read must fail");
        assert!(
            error
                .to_string()
                .contains("injected authority-read failure"),
            "unexpected revalidation error: {error}"
        );
        {
            let cache = client.snapshot.lock().expect("SIFS projection cache");
            assert!(cache.validated_authoritative_entries.contains(&keys[0]));
            assert!(!cache.validated_authoritative_entries.contains(&keys[1]));
            assert!(
                !cache.validated_authoritative_entries.contains(&keys[2]),
                "the unvisited third committed key must not retain its stale certificate"
            );
        }
        assert!(
            client
                .get(&keys[2])
                .expect("unvisited key must cross authority")
                .is_none(),
            "the stale pre-commit value must not escape after a partial revalidation failure"
        );
        assert_eq!(
            transport.routes(),
            vec![
                sinomo_runtime::kv::CLUSTER_KV_TXN_ROUTE.to_string(),
                sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
                sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
                sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
            ]
        );
    }

    #[test]
    fn sifs_authoritative_scope_cache_revalidates_once_per_refresh_generation() {
        let root = temp_sifs_data_dir("control-plane-scope-generations");
        let data_dir = SinomoDataDir::new(&root);
        seed_observation_cluster(&data_dir);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let state = cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("cluster state without KV");
        let transport = Arc::new(RecordingSifsKvDaemonTransport::new(
            state.local_node_id.clone(),
        ));
        let mut client = SifsControlPlaneKvClient::new(&data_dir, &cluster_paths)
            .with_peer_transport(transport.clone());

        client
            .refresh_latest()
            .expect("start first authoritative refresh generation");
        for _ in 0..16 {
            assert_eq!(
                client
                    .get("/dfs/test/daemon-projection")
                    .expect("reuse validated point scope")
                    .expect("point entry")
                    .value,
                b"fresh"
            );
        }
        for _ in 0..16 {
            assert_eq!(
                client
                    .range_prefix("/dfs/test/")
                    .expect("reuse validated prefix scope")
                    .len(),
                1
            );
        }
        assert!(
            client
                .get("/dfs/test/absent")
                .expect("validated prefix proves exact absence")
                .is_none()
        );
        assert_eq!(
            transport.routes(),
            vec![
                sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
                sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE.to_string(),
            ],
            "ordinary repeated reads must reuse each validated authority scope"
        );

        client
            .refresh_latest()
            .expect("advance authoritative refresh generation");
        assert_eq!(
            transport.routes().len(),
            2,
            "refresh invalidates certificates lazily without issuing an eager RPC"
        );
        client
            .get("/dfs/test/daemon-projection")
            .expect("revalidate point in second generation")
            .expect("point entry");
        client
            .range_prefix("/dfs/test/")
            .expect("revalidate prefix in second generation");
        for _ in 0..16 {
            client
                .get("/dfs/test/daemon-projection")
                .expect("reuse second-generation point scope");
            client
                .range_prefix("/dfs/test/")
                .expect("reuse second-generation prefix scope");
        }
        assert_eq!(
            transport.routes(),
            vec![
                sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
                sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE.to_string(),
                sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
                sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE.to_string(),
            ],
            "each explicit refresh must revalidate each reused scope exactly once"
        );
    }

    #[test]
    fn sifs_authoritative_point_read_replans_after_inflight_refresh() {
        let root = temp_sifs_data_dir("control-plane-point-generation-race");
        let data_dir = SinomoDataDir::new(&root);
        seed_observation_cluster(&data_dir);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let state = cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("cluster state without KV");
        let transport = Arc::new(RecordingSifsKvDaemonTransport::new(
            state.local_node_id.clone(),
        ));
        let mut client = SifsControlPlaneKvClient::new(&data_dir, &cluster_paths)
            .with_peer_transport(transport.clone());
        client
            .refresh_latest()
            .expect("start authoritative generation");
        transport.advance_generation_during_next_route(
            sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE,
            &client,
        );

        let entry = client
            .get("/dfs/test/committed")
            .expect("replan superseded point read")
            .expect("second generation committed entry");
        assert_eq!(entry.value, b"value");
        assert_eq!(
            transport.routes(),
            vec![
                sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
                sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
            ],
            "the obsolete first-generation absence must be discarded and replanned"
        );
        assert!(
            client
                .get("/dfs/test/committed")
                .expect("reuse replanned point scope")
                .is_some()
        );
        assert_eq!(transport.routes().len(), 2);
    }

    #[test]
    fn sifs_authoritative_consistent_exact_reads_replan_transition_revision_mismatch() {
        let root = temp_sifs_data_dir("control-plane-consistent-transition-race");
        let data_dir = SinomoDataDir::new(&root);
        seed_observation_cluster(&data_dir);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let state = cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("cluster state without KV");
        let transport = Arc::new(RecordingSifsKvDaemonTransport::new(
            state.local_node_id.clone(),
        ));
        let mut client = SifsControlPlaneKvClient::new(&data_dir, &cluster_paths)
            .with_peer_transport(transport.clone());
        client
            .refresh_latest()
            .expect("start authoritative generation");
        transport.commit_after_next_entry_response();

        let keys = vec![
            "/dfs/test/daemon-projection".to_string(),
            "/dfs/test/committed".to_string(),
        ];
        let values = client
            .get_consistent(&keys)
            .expect("replan exact reads which straddle a transition");
        assert_eq!(
            values[0].as_ref().map(|entry| entry.value.as_slice()),
            Some(b"fresh".as_slice())
        );
        assert_eq!(
            values[1].as_ref().map(|entry| entry.value.as_slice()),
            Some(b"value".as_slice())
        );
        assert_eq!(
            transport.routes(),
            vec![sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE.to_string()],
            "the complete transition view must use one exact-key-set authority read"
        );

        let snapshot = client
            .cached_snapshot()
            .expect("cached coherent projection")
            .expect("materialized coherent projection");
        assert_eq!(snapshot.revision, KvRevision(8));
        assert_eq!(
            snapshot
                .get("/dfs/test/daemon-projection")
                .map(|entry| entry.value.as_slice()),
            Some(b"fresh".as_slice())
        );
        assert_eq!(
            snapshot
                .get("/dfs/test/committed")
                .map(|entry| entry.value.as_slice()),
            Some(b"value".as_slice())
        );
    }

    #[test]
    fn sifs_exact_set_uses_one_snapshot_only_for_a_definite_legacy_route() {
        let root = temp_sifs_data_dir("control-plane-legacy-exact-set");
        let data_dir = SinomoDataDir::new(&root);
        seed_observation_cluster(&data_dir);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let state = cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("cluster state without KV");
        let keys = vec![
            "/dfs/test/legacy-a".to_string(),
            "/dfs/test/legacy-b".to_string(),
        ];

        let transport = Arc::new(RecordingSifsKvDaemonTransport::new(
            state.local_node_id.clone(),
        ));
        transport.return_unknown_exact_set_route_once();
        let mut client = SifsControlPlaneKvClient::new(&data_dir, &cluster_paths)
            .with_peer_transport(transport.clone());
        client
            .refresh_latest()
            .expect("start legacy authority generation");
        let values = client
            .get_consistent(&keys)
            .expect("legacy daemon snapshot projection fallback");
        assert_eq!(values.len(), keys.len());
        for (key, value) in keys.iter().zip(&values) {
            assert_eq!(
                value.as_ref().map(|entry| entry.key.as_str()),
                Some(key.as_str())
            );
        }
        assert_eq!(
            transport.routes(),
            vec![
                sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE.to_string(),
                sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE.to_string(),
            ],
            "mixed-version fallback must project the complete key set from one authoritative snapshot"
        );

        let transport = Arc::new(RecordingSifsKvDaemonTransport::new(state.local_node_id));
        transport.fail_exact_set_route_once();
        let mut client = SifsControlPlaneKvClient::new(&data_dir, &cluster_paths)
            .with_peer_transport(transport.clone());
        client
            .refresh_latest()
            .expect("start strict authority generation");
        let error = client
            .get_consistent(&keys)
            .expect_err("an ambiguous transport failure must not use compatibility fallback");
        assert!(
            error
                .to_string()
                .contains("exact-key-set transport failure")
        );
        assert_eq!(
            transport.routes(),
            vec![sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE.to_string()],
            "only a definite UnknownRoute may use the snapshot compatibility path"
        );
    }

    #[test]
    fn sifs_exact_set_larger_than_the_rpc_head_uses_a_streamed_value() {
        let root = temp_sifs_data_dir("control-plane-streamed-exact-set");
        let data_dir = SinomoDataDir::new(&root);
        seed_observation_cluster(&data_dir);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let state = cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("cluster state without KV");
        let transport = Arc::new(RecordingSifsKvDaemonTransport::new(state.local_node_id));
        let mut client = SifsControlPlaneKvClient::new(&data_dir, &cluster_paths)
            .with_peer_transport(transport.clone());
        client
            .refresh_latest()
            .expect("start streamed exact-key authority generation");

        let keys = (0..1024)
            .map(|index| format!("/dfs/test/streamed-exact-key-{index:04}"))
            .collect::<Vec<_>>();
        assert!(
            keys.iter().map(String::len).sum::<usize>() > 16 * 1024,
            "the regression request must exceed the RPC control-head bound"
        );
        let values = client
            .get_consistent(&keys)
            .expect("streamed exact-key-set authority read");

        assert_eq!(values.len(), keys.len());
        assert!(values.iter().all(Option::is_none));
        assert_eq!(
            transport.routes(),
            vec![sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE.to_string()],
            "one streamed exact-key-set request must cover the complete bounded set"
        );
    }

    #[test]
    fn sifs_authoritative_topology_client_remains_seeded_for_service_reads() {
        let root = temp_sifs_data_dir("control-plane-topology-client-reuse");
        let data_dir = SinomoDataDir::new(&root);
        seed_observation_cluster(&data_dir);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let state = cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("cluster state without KV");
        let transport = Arc::new(RecordingSifsKvDaemonTransport::new(state.local_node_id));
        let mut client = SifsControlPlaneKvClient::new(&data_dir, &cluster_paths)
            .with_peer_transport(transport.clone());
        let prefix = "/dfs/test/";

        client
            .range_prefix_authoritative(prefix)
            .expect("certify the read-only service topology");
        let certified_routes = transport.routes();
        assert_eq!(
            certified_routes,
            vec![sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE.to_string()]
        );

        let entries = client
            .range_prefix(prefix)
            .expect("reuse the certified topology after moving the client into the service");
        assert!(!entries.is_empty());
        assert_eq!(
            transport.routes(),
            certified_routes,
            "distributed reads must reuse the exact authority generation established at open"
        );
    }

    #[test]
    fn sifs_authoritative_prefix_read_replans_after_inflight_refresh() {
        let root = temp_sifs_data_dir("control-plane-prefix-generation-race");
        let data_dir = SinomoDataDir::new(&root);
        seed_observation_cluster(&data_dir);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let state = cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("cluster state without KV");
        let transport = Arc::new(RecordingSifsKvDaemonTransport::new(
            state.local_node_id.clone(),
        ));
        let mut client = SifsControlPlaneKvClient::new(&data_dir, &cluster_paths)
            .with_peer_transport(transport.clone());
        client
            .refresh_latest()
            .expect("start authoritative generation");
        transport.advance_generation_during_next_route(
            sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE,
            &client,
        );

        let entries = client
            .range_prefix("/dfs/test/")
            .expect("replan superseded prefix read");
        assert_eq!(entries.len(), 2);
        assert!(
            entries
                .iter()
                .any(|entry| entry.key == "/dfs/test/committed")
        );
        assert_eq!(
            transport.routes(),
            vec![
                sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE.to_string(),
                sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE.to_string(),
            ],
            "the obsolete first-generation prefix must be discarded and replanned"
        );
        assert_eq!(
            client
                .range_prefix("/dfs/test/")
                .expect("reuse replanned prefix scope")
                .len(),
            2
        );
        assert_eq!(transport.routes().len(), 2);
    }

    #[test]
    fn sifs_authority_rejects_entries_outside_the_requested_scope() {
        let root = temp_sifs_data_dir("control-plane-authority-containment");
        let data_dir = SinomoDataDir::new(&root);
        seed_observation_cluster(&data_dir);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let state = cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("cluster state without KV");
        let transport = Arc::new(RecordingSifsKvDaemonTransport::new(
            state.local_node_id.clone(),
        ));
        let mut client = SifsControlPlaneKvClient::new(&data_dir, &cluster_paths)
            .with_peer_transport(transport.clone());
        client
            .refresh_latest()
            .expect("begin authoritative generation");

        transport.return_wrong_authoritative_entry_key_once();
        let error = client
            .get("/dfs/requested/entry")
            .expect_err("a mismatched exact-key reply must be rejected");
        assert!(error.to_string().contains("returned entry"));
        {
            let cache = client.snapshot.lock().expect("SIFS projection cache");
            assert!(!cache.entry_is_validated("/dfs/requested/entry"));
            assert!(
                cache
                    .snapshot
                    .as_ref()
                    .is_none_or(|snapshot| snapshot.get("/dfs/foreign/entry").is_none())
            );
        }

        transport.return_out_of_prefix_authoritative_entry_once();
        let error = client
            .range_prefix("/dfs/test/")
            .expect_err("an out-of-prefix reply must be rejected");
        assert!(error.to_string().contains("returned entry"));
        let cache = client.snapshot.lock().expect("SIFS projection cache");
        assert!(!cache.prefix_is_validated("/dfs/test/"));
        assert!(
            cache
                .snapshot
                .as_ref()
                .is_none_or(|snapshot| snapshot.get("/dfs/test-sibling").is_none())
        );
    }

    #[test]
    fn sifs_authoritative_scope_markers_are_bounded() {
        let mut cache = SifsKvSnapshotCache::default();
        cache.begin_authoritative_generation();
        for index in 0..SIFS_AUTHORITATIVE_SCOPE_CACHE_MAX_ENTRIES.saturating_add(32) {
            cache.mark_entry_validated(&format!("/dfs/exact/{index}"));
            cache.mark_prefix_validated(&format!("/dfs/prefix/{index}/"));
        }
        assert_eq!(
            cache.authoritative_scope_count(),
            SIFS_AUTHORITATIVE_SCOPE_CACHE_MAX_ENTRIES
        );

        let mut byte_bounded = SifsKvSnapshotCache::default();
        byte_bounded.begin_authoritative_generation();
        let long_component = "x".repeat(4096);
        for index in 0..SIFS_AUTHORITATIVE_SCOPE_CACHE_MAX_ENTRIES {
            byte_bounded
                .mark_entry_validated(&format!("/dfs/byte-bounded/{index:04}/{long_component}"));
        }
        assert!(
            byte_bounded.validated_authoritative_scope_bytes
                <= SIFS_AUTHORITATIVE_PROJECTION_MAX_PAYLOAD_BYTES
        );
        assert!(
            byte_bounded.authoritative_scope_count() < SIFS_AUTHORITATIVE_SCOPE_CACHE_MAX_ENTRIES,
            "the aggregate byte bound must stop long absence certificates before the count bound"
        );
    }

    #[test]
    fn sifs_authoritative_projection_evicts_before_distinct_scope_capacity_growth() {
        let root = temp_sifs_data_dir("control-plane-projection-capacity");
        let data_dir = SinomoDataDir::new(&root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let client = SifsControlPlaneKvClient::new(&data_dir, &cluster_paths);
        {
            let mut snapshot = empty_sifs_kv_snapshot(KvRevision(1));
            snapshot.entries = (0..SIFS_AUTHORITATIVE_PROJECTION_MAX_ENTRIES)
                .map(|index| sinomo_runtime::kv::KvEntry {
                    key: format!("/dfs/capacity/{index:04}"),
                    value: vec![index as u8],
                    create_revision: KvRevision(1),
                    modify_revision: KvRevision(1),
                    version: sinomo_runtime::kv::KvVersion(1),
                    lease_id: None,
                })
                .collect();
            snapshot.canonicalize();
            let mut cache = client.snapshot.lock().expect("SIFS projection cache");
            cache.loaded = true;
            cache.snapshot = Some(Arc::new(snapshot));
            cache.authoritative_generation = Some(1);
            for index in 0..SIFS_AUTHORITATIVE_SCOPE_CACHE_MAX_ENTRIES {
                cache.mark_entry_validated(&format!("/dfs/capacity/{index:04}"));
            }
        }

        let new_key = "/dfs/capacity/overflow";
        assert!(
            client
                .merge_authoritative_entry(
                    new_key,
                    &sinomo_runtime::kv::KvAuthoritativeRead {
                        value: Some(sinomo_runtime::kv::KvEntry {
                            key: new_key.to_string(),
                            value: b"bounded".to_vec(),
                            create_revision: KvRevision(2),
                            modify_revision: KvRevision(2),
                            version: sinomo_runtime::kv::KvVersion(1),
                            lease_id: None,
                        }),
                        authority: RecordingSifsKvDaemonTransport::authority(KvRevision(2)),
                    },
                    1,
                )
                .expect("merge overflow scope")
        );
        let cache = client.snapshot.lock().expect("SIFS projection cache");
        let snapshot = cache.snapshot.as_ref().expect("bounded projection");
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(cache.authoritative_scope_count(), 1);
        assert!(
            snapshot
                .entries
                .iter()
                .all(|entry| cache.entry_is_validated(&entry.key)),
            "every retained projection entry must have an exact or prefix certificate"
        );
        assert!(
            snapshot
                .entries
                .len()
                .saturating_add(cache.authoritative_scope_count())
                <= SIFS_AUTHORITATIVE_PROJECTION_MAX_ENTRIES.saturating_mul(2),
            "projection entries plus certificates must remain O(capacity)"
        );
        assert!(cache.validated_authoritative_entries.contains(new_key));
        assert!(sifs_authoritative_projection_entries_fit(&snapshot.entries));
        drop(cache);
        assert!(matches!(
            client
                .authoritative_entry_read_plan("/dfs/capacity/0000")
                .expect("plan evicted scope"),
            SifsKvReadPlan::Authoritative { .. }
        ));
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
    fn clustered_backend_cold_open_does_not_probe_kv_authority() {
        let root = temp_sifs_data_dir("cold-open-local-projection");
        let data_dir = SinomoDataDir::new(&root);
        seed_observation_cluster(&data_dir);
        let root_shard = sinomo_runtime::dfs::DfsMetadataShardId::new("root");
        let mut metadata =
            sinomo_runtime::dfs::open_local_metadata_shard_backend(&data_dir, root_shard.clone())
                .expect("root metadata backend");
        metadata
            .freeze_metadata_shard_for_membership_handoff(
                &root_shard,
                "cold-open-activated-marker",
                1,
            )
            .expect("install cold-open marker")
            .expect("root shard");
        assert!(
            metadata
                .mark_metadata_shard_membership_handoff_activated(
                    &root_shard,
                    "cold-open-activated-marker",
                    2,
                )
                .expect("activate cold-open marker")
        );
        drop(metadata);

        let cluster_paths = ClusterPaths::new(&data_dir);
        let mut state =
            cluster::read_cluster_state_without_kv(&cluster_paths).expect("cluster state");
        let now = time::OffsetDateTime::now_utc();
        let local_node_id = state.local_node_id.clone();
        let remote_node_id =
            NodeId::from_legacy_named("node_sifs_cold_open_remote").expect("remote node");
        let local = sinomo_runtime::core::ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            local_node_id,
            "local non-voter",
            "local-public-key",
            now,
        );
        let mut remote = sinomo_runtime::core::ClusterMemberRecord::local_voter(
            state.cluster_id.clone(),
            remote_node_id.clone(),
            "remote voter",
            "remote-public-key",
            now,
        );
        remote.capabilities.push(
            sinomo_runtime::core::CapabilityName::new(
                sinomo_runtime::core::DAEMON_MAIN_RUN_CAPABILITY,
            )
            .expect("daemon capability"),
        );

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("blackhole listener");
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        let mut endpoint = sinomo_runtime::core::ClusterEndpointRecord::loopback(
            remote_node_id,
            listener.local_addr().expect("listener address").to_string(),
        );
        endpoint.transport = sinomo_runtime::core::ClusterTransportKind::LanTcp;
        endpoint.privacy = sinomo_runtime::core::ClusterEndpointPrivacy::Lan;
        remote.endpoints.push(endpoint);
        state.members = vec![local, remote];
        cluster::write_cluster_state(&cluster_paths, &state).expect("non-voter cluster state");

        let authority_probed = Arc::new(AtomicBool::new(false));
        let authority_probed_from_listener = Arc::clone(&authority_probed);
        std::thread::spawn(move || {
            let accept_deadline = Instant::now() + Duration::from_secs(2);
            while Instant::now() < accept_deadline {
                match listener.accept() {
                    Ok((_stream, _address)) => {
                        authority_probed_from_listener.store(true, Ordering::SeqCst);
                        break;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });

        let _backend = SifsFileManagerBackend::open_with_deadline(
            &data_dir,
            Instant::now() + Duration::from_secs(1),
        )
        .expect("cold SIFS open from the daemon-projected snapshot");
        std::thread::sleep(Duration::from_millis(20));
        assert!(
            !authority_probed.load(Ordering::SeqCst),
            "cold SIFS hydration must not initiate an authoritative cluster-KV network read"
        );
    }

    #[test]
    fn writable_backend_defers_clustered_bootstrap_attempt_until_first_mutation() {
        let root = temp_sifs_data_dir("deferred-mutation-bootstrap");
        let data_dir = SinomoDataDir::new(&root);
        seed_observation_cluster(&data_dir);
        let backend = SifsFileManagerBackend::open(&data_dir).expect("writable backend");

        assert_eq!(
            *backend
                .mutation_bootstrapped
                .lock()
                .expect("mutation bootstrap state"),
            Some(false),
            "opening a writable backend must stay observational"
        );

        let error = backend
            .create_dir_all("/services/deferred-bootstrap")
            .expect_err("clustered mutation must fail closed without the local daemon");
        assert!(
            error.contains("cannot prepare SIFS mutation authority")
                && error.contains("daemon is not running"),
            "{error}"
        );
        assert_eq!(
            *backend
                .mutation_bootstrapped
                .lock()
                .expect("mutation bootstrap state"),
            Some(false),
            "failed authority bootstrap must remain retryable"
        );

        let read_only =
            SifsFileManagerBackend::open_read_only(&data_dir).expect("read-only backend");
        assert_eq!(
            *read_only
                .mutation_bootstrapped
                .lock()
                .expect("read-only mutation bootstrap state"),
            None
        );
        let error = read_only
            .create_dir_all("/services/read-only-mutation")
            .expect_err("read-only backend must reject mutations");
        assert!(error.contains("read-only"), "{error}");
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
            file_history: Default::default(),
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
            placement_snapshot: None,
            adaptive: false,
            adaptive_budget: None,
            processed_content_bytes: 0,
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
            storage_health: None,
            storage_freshness: None,
            usable_capacity_bytes: None,
            reserved_free_bytes: None,
            path_state: None,
            path_latency_micros: None,
            path_throughput_bytes_per_second: None,
            path_metering: None,
            observation_freshness: None,
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
        let mut state = sinomo_runtime::core::ClusterBootstrapState::initialized_locally(
            cluster_id,
            "SIFS observation journal heads",
            local_node_id.clone(),
            "local",
            "local-public-key",
            time::OffsetDateTime::now_utc(),
        );
        state.members[0].capabilities =
            sinomo_runtime::node::main_daemon_capabilities(&state.members[0].role);
        state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
        cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        cluster::locked_set_cluster_raft_leader(&cluster_paths, &local_node_id, state.term)
            .expect("seed initial local Raft leadership");

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
        cluster::bootstrap_single_voter_cluster_state_to_local_kv(&cluster_paths, &state)
            .expect("bootstrap canonical cluster KV authority");
        let shard_ops = shards
            .iter()
            .map(|shard| {
                KvOp::put_serde(
                    sinomo_runtime::dfs::metadata_shard_key(&shard.file_system_id, &shard.shard_id),
                    shard,
                    None,
                )
                .expect("metadata shard op")
            })
            .collect();
        let canonical_snapshot = cluster::append_cluster_raft_command(
            &cluster_paths,
            &local_node_id,
            sinomo_runtime::kv::KvCommand::Txn(KvTxn {
                compares: Vec::new(),
                success: shard_ops,
                failure: Vec::new(),
            }),
        )
        .expect("commit metadata shards to canonical cluster KV authority");
        // Keep the daemon-style materialized projection too: observation-only
        // readers consume this file, while strict topology reads use the
        // canonical resident Raft node seeded above.
        sinomo_runtime::kv::write_cluster_kv_snapshot(
            &cluster_paths.kv_paths(),
            &canonical_snapshot,
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
