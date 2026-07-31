use crate::client::{
    send_object_rpc_call_to_target_async_with_daemon_paths_and_options,
    send_object_rpc_call_to_target_on_runtime_with_daemon_paths_and_options,
};
use crate::control::DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL;
use crate::{DaemonDataOwnerLock, DaemonError, DaemonPaths, DaemonRunOptions};
use sinomo_core::ClusterNodeAvailability;
#[allow(unused_imports)]
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::kv::{
    KvEntry, KvError, KvOp, KvRevision, KvSnapshot, KvTxn, KvTxnResponse, KvVersion,
};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    ClusterConnectivityGraph, ClusterPaths, ObjectRpcCall, ObjectRpcResult,
    cluster_nodes_share_healthy_component,
};
use sinomo_core::node::NodeId;
#[cfg(test)]
use sinomo_dfs::DfsFileSystemConfig;
#[cfg(test)]
use sinomo_dfs::{
    ContentStoreError, ContentStoreGcOptions, ContentStoreVersionGcOptions,
    DaemonDfsInspectionRequest, DaemonDfsInspectionResponse, DfsConflictRecord, DfsConflictStatus,
    DfsContentRootCatalogCollectionValidation, DfsContentRootCatalogIncompleteReason,
    DfsMaintenanceRunOptions, DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchResponse,
    DfsMetadataJournalBatchResult, DfsMetadataJournalBatchSender, DfsMetadataPeerTransportError,
    DfsMetadataShardAssignment, DfsMetadataShardConfig, DfsMetadataShardStatus,
    DfsMetadataSnapshotInstallResponse, DfsMetadataSnapshotInstallResult,
    DfsMetadataSnapshotInstallSender, DfsMetadataSummary, DfsObjectId, DfsObjectMetadata,
    DfsObjectVersionRecord, DfsOperatorInspector, DfsOperatorRepairView, DfsReplicaReceiptRecord,
    DfsVersion, DfsWriteIntentRecord, DfsWriteIntentStatus, LocalSobjMetadataBackend,
    cluster_dfs_all_file_system_configs, cluster_dfs_all_metadata_shard_head_watermarks,
    cluster_dfs_all_metadata_shard_membership_handoffs, cluster_dfs_all_metadata_shards,
    default_dfs_content_root_catalog_store, default_dfs_metadata_root, distributed_sifs,
    distributed_sifs_bootstrap_objects, file_system_config_key, metadata_shard_key,
    sifs_node_files_path,
};
use sinomo_dfs::{
    DfsCommittedWorkTracker, DfsDataPlaneHost, DfsDataPlaneHostError, DfsDataPlaneHostFuture,
    DfsDataPlaneResponse, DfsDefaultClusterBootstrapOptions, DfsDefaultClusterSmokeOptions,
    DfsFileSystemId, DfsInboundAttachment, DfsMaintenanceExecutionExclusions,
    DfsMaintenanceTickOptions, DfsMaintenanceTickReport, DfsMessageCommand,
    DfsMessageCommittedWork, DfsMessageExecutionScope, DfsMessageProcessResult, DfsMessageResponse,
    DfsMetadataBackend, DfsMetadataJournalReplicationOptions, DfsMetadataJournalReplicationReport,
    DfsMetadataPublishScheduleOptions, DfsMetadataPublishScheduleStart, DfsMetadataShardId,
    DfsMetadataShardMembershipHandoff, DfsNodeDetachedSendState, DfsNodeDetachedSendWorker,
    DfsNodeServedMessageTransport, DfsNodeTransport, DfsObjectRpcSendOptions, DfsPeerTransport,
    DfsPeerTransportError, DfsPlacementReconciliationJob, DfsPlacementReconciliationState,
    DfsRepairAttemptRecord, DfsRepairAttemptSnapshot, LocalContentStore, NetworkedDfsService,
    default_dfs_repair_attempt_snapshot_path, dfs_message_execution_scope,
    read_dfs_repair_attempt_snapshot, write_dfs_repair_attempt_snapshot,
};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex, OnceLock, Weak,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
#[cfg(test)]
use time::OffsetDateTime;

// Data-plane object-RPC messages keep the cached service hot by applying
// inbound messages and routed journal batches. A full backend hydrate is a
// defensive catch-up for out-of-band metadata writes, and doing it every few
// seconds makes multi-node DFS writes pay repeated disk scans while a request is
// still in flight.
const DAEMON_DFS_MESSAGE_CACHE_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const DAEMON_DFS_PROMOTION_RETRY_DELAY: Duration = Duration::from_secs(5);
const DAEMON_DFS_REPAIR_MAX_JOBS_PER_TICK: usize = 8;
const DAEMON_DFS_TOMBSTONE_COMPACTION_MIN_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const DAEMON_DFS_TOMBSTONE_COMPACTION_MAX_RECORDS: u64 = 256;
const DAEMON_DFS_METADATA_HISTORY_GC_MIN_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);
#[cfg(test)]
const DAEMON_DFS_METADATA_HISTORY_GC_MAX_AGE: Duration = Duration::from_secs(14 * 24 * 60 * 60);
const DAEMON_DFS_METADATA_HISTORY_GC_KEEP_LATEST_VERSIONS: u64 = 2;
const DAEMON_DFS_METADATA_HISTORY_GC_MAX_RECORDS: u64 = 256;
#[cfg(test)]
const DAEMON_DFS_METADATA_HISTORY_GC_RECORD_TRIGGER: u64 = 10_000;
#[cfg(test)]
const DAEMON_DFS_METADATA_HISTORY_GC_BYTES_TRIGGER: u64 = 64 * 1024 * 1024;
const DAEMON_DFS_CONTENT_RESERVATION_RECONCILIATION_MAX_RECORDS: u64 = 4_096;
const DAEMON_DFS_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET: usize = 64;
// A replica journal apply is durable WAN work. Give daemon maintenance a larger
// response budget than the library default while keeping foreground publication
// on its explicit two-second best-effort bound.
const DAEMON_DFS_METADATA_REPLICATION_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
const DAEMON_DFS_METADATA_REPLICATION_TARGET_TIMEOUT: Duration = Duration::from_secs(15);
const DAEMON_DFS_METADATA_PUBLISH_MAX_RETRY_ROUNDS: usize = 3;
const DAEMON_DFS_METADATA_PUBLISH_RETRY_MAX_DELAY: Duration = Duration::from_secs(5 * 60);
const DAEMON_DFS_METADATA_DISCOVERY_RETRY_INITIAL: Duration = Duration::from_secs(1);
const DAEMON_DFS_METADATA_DISCOVERY_RETRY_MAX: Duration = Duration::from_secs(60);
const DAEMON_DFS_EXTERNAL_AUTHORITY_SNAPSHOT_DEBOUNCE: Duration = Duration::from_secs(1);
const DAEMON_DFS_SMOKE_TIMEOUT: Duration = Duration::from_millis(9_500);
const DAEMON_DFS_MUTATION_SHARD_GATE_LIMIT: usize = 256;
const SINOMO_DFS_ZONE_ENV: &str = "SINOMO_DFS_ZONE";
const DAEMON_DEFAULT_DFS_RECONCILIATION_RETRY_INITIAL: Duration = Duration::from_secs(1);
const DAEMON_DEFAULT_DFS_RECONCILIATION_RETRY_MAX: Duration = Duration::from_secs(60);
const DAEMON_DEFAULT_DFS_MEMBER_PREFIX: &str = "/cluster/members/";
const DAEMON_DEFAULT_DFS_ENDPOINT_PREFIX: &str = "/cluster/endpoints/";
const DAEMON_DEFAULT_DFS_CONNECTIVITY_PREFIX: &str = "/cluster/network/connectivity/";

fn daemon_dfs_local_zone() -> Option<String> {
    std::env::var(SINOMO_DFS_ZONE_ENV)
        .ok()
        .map(|zone| zone.trim().to_string())
        .filter(|zone| !zone.is_empty())
}

/// Bootstrap reconciliation and full maintenance both open and recover the
/// local metadata tables. Keep those mandatory lifecycle paths single-flight
/// so startup cannot retain multiple decoded table images at once.
static DAEMON_DEFAULT_DFS_LIFECYCLE_GATES: OnceLock<
    Mutex<BTreeMap<PathBuf, Weak<tokio::sync::Mutex<()>>>>,
> = OnceLock::new();

#[cfg(test)]
static DAEMON_DEFAULT_DFS_RECONCILIATION_RUNS: OnceLock<Mutex<BTreeMap<PathBuf, usize>>> =
    OnceLock::new();

#[cfg(test)]
fn record_daemon_default_dfs_reconciliation_run(paths: &DaemonPaths) {
    let Ok(data_dir) = daemon_data_dir(paths) else {
        return;
    };
    if let Ok(mut runs) = DAEMON_DEFAULT_DFS_RECONCILIATION_RUNS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
    {
        let count = runs.entry(data_dir.to_path_buf()).or_default();
        *count = count.saturating_add(1);
    }
}

#[cfg(test)]
pub(crate) fn daemon_default_dfs_reconciliation_run_count(paths: &DaemonPaths) -> usize {
    let Ok(data_dir) = daemon_data_dir(paths) else {
        return 0;
    };
    DAEMON_DEFAULT_DFS_RECONCILIATION_RUNS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .ok()
        .and_then(|runs| runs.get(data_dir).copied())
        .unwrap_or_default()
}

type DaemonDfsMessageService = NetworkedDfsService<DfsNodeServedMessageTransport>;

fn daemon_dfs_message_kind(message: &sinomo_dfs::DfsNetworkMessage) -> &'static str {
    match message {
        sinomo_dfs::DfsNetworkMessage::DistributedWriteRequest(_) => "distributed-write-request",
        sinomo_dfs::DfsNetworkMessage::DistributedWriteResponse(_) => "distributed-write-response",
        sinomo_dfs::DfsNetworkMessage::NamespaceRequest(_) => "namespace-request",
        sinomo_dfs::DfsNetworkMessage::NamespaceResponse(_) => "namespace-response",
        sinomo_dfs::DfsNetworkMessage::MetadataJournalBatch(_) => "metadata-journal-batch",
        sinomo_dfs::DfsNetworkMessage::MetadataJournalBatchResponse(_) => {
            "metadata-journal-batch-response"
        }
        sinomo_dfs::DfsNetworkMessage::StageBlob(_) => "stage-blob",
        sinomo_dfs::DfsNetworkMessage::StageBlobResponse(_) => "stage-blob-response",
        sinomo_dfs::DfsNetworkMessage::FetchBlob(_) => "fetch-blob",
        sinomo_dfs::DfsNetworkMessage::FetchBlobResponse(_) => "fetch-blob-response",
        _ => "other",
    }
}

#[derive(Debug)]
struct DaemonDfsMessageCommandProcessResult {
    response: DfsMessageResponse,
    attachment: Option<sinomo_dfs::DfsOutboundAttachment>,
    committed_edges: DaemonDfsMessageCommittedEdges,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct DaemonDfsMessageCommittedEdges {
    publish_metadata: bool,
    replica_progress: bool,
    repair_maintenance: bool,
    namespace_maintenance: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DaemonDfsMessageServiceSignature {
    cluster_id: sinomo_core::Id,
    local_node_id: sinomo_core::node::NodeId,
    kv_snapshot_available: bool,
    metadata_authority_generation: u64,
}

#[derive(Clone, Debug)]
struct DaemonDfsMessagePeerSnapshot {
    cluster_id: sinomo_core::Id,
    local_node_id: sinomo_core::node::NodeId,
    kv_snapshot_available: bool,
    peer_node_ids: Vec<NodeId>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DaemonDfsMetadataAuthorityProjection {
    revision: Option<KvRevision>,
    entries: Vec<(String, Vec<u8>)>,
}

struct DaemonDfsMetadataAuthorityGeneration {
    projection: Mutex<DaemonDfsMetadataAuthorityProjection>,
    generation: AtomicU64,
    #[cfg(test)]
    projection_rebuild_count: AtomicU64,
    publish_wake: Mutex<Option<DaemonDfsMetadataAuthorityPublishWake>>,
    subscription: OnceLock<sinomo_core::cluster::ClusterKvCommittedSnapshotSubscription>,
    snapshot_watcher: OnceLock<notify::RecommendedWatcher>,
}

impl std::fmt::Debug for DaemonDfsMetadataAuthorityGeneration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonDfsMetadataAuthorityGeneration")
            .field("generation", &self.generation())
            .field("snapshot_watcher", &self.snapshot_watcher.get().is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
struct DaemonDfsMetadataAuthorityPublishWake {
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    runtime: tokio::runtime::Handle,
}

impl DaemonDfsMetadataAuthorityGeneration {
    fn new() -> Self {
        Self {
            projection: Mutex::new(DaemonDfsMetadataAuthorityProjection {
                revision: None,
                entries: Vec::new(),
            }),
            generation: AtomicU64::new(1),
            #[cfg(test)]
            projection_rebuild_count: AtomicU64::new(0),
            publish_wake: Mutex::new(None),
            subscription: OnceLock::new(),
            snapshot_watcher: OnceLock::new(),
        }
    }

    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn projection_rebuild_count(&self) -> u64 {
        self.projection_rebuild_count.load(Ordering::Acquire)
    }

    fn attach_publish_wake(
        &self,
        cluster_paths: &ClusterPaths,
        runtime: Option<&tokio::runtime::Handle>,
    ) {
        let Some(runtime) = runtime else {
            return;
        };
        let Some(data_root) = cluster_paths.root.parent() else {
            return;
        };
        if let Ok(mut publish_wake) = self.publish_wake.lock() {
            *publish_wake = Some(DaemonDfsMetadataAuthorityPublishWake {
                data_dir: SinomoDataDir::new(data_root),
                cluster_paths: cluster_paths.clone(),
                runtime: runtime.clone(),
            });
        }
    }

    fn observe(&self, snapshot: &KvSnapshot) {
        let authority_changed = {
            let Ok(mut observed) = self.projection.lock() else {
                return;
            };
            let previous_revision = observed.revision;
            if previous_revision.is_some_and(|revision| snapshot.revision <= revision) {
                return;
            }
            let rebuild = match previous_revision {
                None => true,
                Some(revision) if revision < snapshot.compacted_revision => true,
                Some(revision) => {
                    let first_new = snapshot
                        .events
                        .partition_point(|event| event.revision <= revision);
                    snapshot.events[first_new..]
                        .iter()
                        .any(|event| daemon_dfs_metadata_authority_key_is_exact(&event.key))
                }
            };
            if !rebuild {
                // Authority consumes only exact shard/configuration keys.
                // Advance the retained cursor without scanning or cloning the
                // resident KV image for unrelated commits.
                observed.revision = Some(snapshot.revision);
                return;
            }
            let entries = daemon_dfs_metadata_authority_projection(snapshot);
            #[cfg(test)]
            self.projection_rebuild_count.fetch_add(1, Ordering::AcqRel);
            // `None` means the retained observer has not seen any committed
            // authority image yet, not that the authority image is known to
            // be empty. A first non-empty image is therefore a real semantic
            // edge: consumers may already have observed the initial
            // generation while waiting for the cluster snapshot, and local
            // primary backlog must be discovered without unrelated traffic.
            let authority_changed = observed.entries != entries;
            if authority_changed {
                let _ = self.generation.fetch_update(
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    |generation| Some(generation.saturating_add(1)),
                );
            }
            observed.revision = Some(snapshot.revision);
            observed.entries = entries;
            authority_changed
        };
        let wake = authority_changed
            .then(|| {
                self.publish_wake
                    .lock()
                    .ok()
                    .and_then(|publish_wake| publish_wake.clone())
            })
            .flatten();
        if let Some(wake) = wake {
            schedule_daemon_dfs_metadata_publish_discovery_on_runtime(
                wake.data_dir,
                wake.cluster_paths,
                &wake.runtime,
                None,
            );
        }
    }
}

fn daemon_dfs_metadata_authority_snapshot_watcher(
    cluster_paths: &ClusterPaths,
    generation: Weak<DaemonDfsMetadataAuthorityGeneration>,
    runtime: &tokio::runtime::Handle,
) -> Result<notify::RecommendedWatcher, sinomo_dfs::DfsError> {
    let snapshot_path = cluster_paths.kv_paths().snapshot;
    let callback_snapshot_path = snapshot_path.clone();
    let callback_cluster_paths = cluster_paths.clone();
    let callback_runtime = runtime.clone();
    let event_generation = Arc::new(AtomicU64::new(0));
    let mut watcher = notify::recommended_watcher(
        move |result: Result<notify::Event, notify::Error>| match result {
            Ok(event)
                if event
                    .paths
                    .iter()
                    .any(|path| path == &callback_snapshot_path) =>
            {
                let event_id = event_generation
                    .fetch_add(1, Ordering::AcqRel)
                    .saturating_add(1);
                let event_generation = Arc::clone(&event_generation);
                let generation = generation.clone();
                let cluster_paths = callback_cluster_paths.clone();
                let snapshot_path = callback_snapshot_path.clone();
                callback_runtime.spawn(async move {
                    tokio::time::sleep(DAEMON_DFS_EXTERNAL_AUTHORITY_SNAPSHOT_DEBOUNCE).await;
                    if event_generation.load(Ordering::Acquire) != event_id {
                        return;
                    }
                    let Some(generation) = generation.upgrade() else {
                        return;
                    };
                    match sinomo_core::cluster::read_cluster_kv_latest_observed_snapshot_arc_if_present(
                        &cluster_paths,
                    ) {
                        Ok(Some(snapshot)) => generation.observe(snapshot.as_ref()),
                        Ok(None) => {}
                        Err(error) => tracing::warn!(
                            target: "sinomo.dfs.metadata_authority",
                            path = %snapshot_path.display(),
                            "DFS metadata-authority snapshot change could not be read: {error}"
                        ),
                    }
                });
            }
            Ok(_) => {}
            Err(error) => tracing::warn!(
                target: "sinomo.dfs.metadata_authority",
                path = %callback_snapshot_path.display(),
                "DFS metadata-authority snapshot watcher failed: {error}"
            ),
        },
    )
    .map_err(|source| sinomo_dfs::DfsError::Network {
        message: format!(
            "cannot watch DFS metadata-authority snapshot {}: {source}",
            snapshot_path.display()
        ),
    })?;
    let watch_root = snapshot_path
        .parent()
        .ok_or_else(|| sinomo_dfs::DfsError::Network {
            message: format!(
                "DFS metadata-authority snapshot {} has no parent directory",
                snapshot_path.display()
            ),
        })?;
    notify::Watcher::watch(
        &mut watcher,
        watch_root,
        notify::RecursiveMode::NonRecursive,
    )
    .map_err(|source| sinomo_dfs::DfsError::Network {
        message: format!(
            "cannot watch DFS metadata-authority snapshot directory {}: {source}",
            watch_root.display()
        ),
    })?;
    Ok(watcher)
}

#[derive(Debug)]
struct DaemonDfsMessageServiceLane {
    service: DaemonDfsMessageService,
    peer_node_ids: Vec<NodeId>,
    last_metadata_refresh: Instant,
    metadata_hydrated: bool,
}

impl DaemonDfsMessageServiceLane {
    fn refresh_peer_targets(&mut self, peer_node_ids: Vec<NodeId>) {
        if self.peer_node_ids == peer_node_ids {
            return;
        }
        self.service
            .refresh_message_peer_targets(peer_node_ids.clone());
        self.peer_node_ids = peer_node_ids;
    }
}

#[derive(Debug)]
struct DaemonDfsMessageServiceLaneHandle {
    lane: Mutex<DaemonDfsMessageServiceLane>,
    detached_send_runtime: Arc<DaemonDfsDetachedSendRuntime>,
}

impl Deref for DaemonDfsMessageServiceLaneHandle {
    type Target = Mutex<DaemonDfsMessageServiceLane>;

    fn deref(&self) -> &Self::Target {
        &self.lane
    }
}

#[derive(Debug)]
struct DaemonDfsDetachedSendRuntime {
    state: DfsNodeDetachedSendState,
    recovery_scan_generation: AtomicU64,
    metadata_authority_generation: OnceLock<Arc<DaemonDfsMetadataAuthorityGeneration>>,
    message_worker_active: AtomicBool,
    message_worker_schedule: Mutex<DaemonDfsDetachedWorkerSchedule>,
}

#[derive(Debug, Default)]
struct DaemonDfsDetachedWorkerSchedule {
    generation: u64,
    latest_worker: Option<DfsNodeDetachedSendWorker>,
    deferred_target_node_ids: Option<BTreeSet<NodeId>>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DaemonDfsDetachedSendRuntimeKey {
    data_root: PathBuf,
    cluster_id: String,
    local_node_id: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DaemonDfsMetadataAuthorityGenerationKey {
    snapshot: PathBuf,
    cluster_id: String,
    local_node_id: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DaemonDfsMetadataPublishLaneKey {
    data_root: PathBuf,
    file_system_id: DfsFileSystemId,
    shard_id: DfsMetadataShardId,
}

impl DaemonDfsDetachedSendRuntime {
    fn new() -> Self {
        Self {
            state: DfsNodeDetachedSendState::new(),
            recovery_scan_generation: AtomicU64::new(0),
            metadata_authority_generation: OnceLock::new(),
            message_worker_active: AtomicBool::new(false),
            message_worker_schedule: Mutex::new(DaemonDfsDetachedWorkerSchedule::default()),
        }
    }
}

#[derive(Debug)]
struct DaemonDfsMessageServiceCacheEntry {
    signature: DaemonDfsMessageServiceSignature,
    metadata_authority_generation: Arc<DaemonDfsMetadataAuthorityGeneration>,
    peer_node_ids: Vec<NodeId>,
    /// Lazily opened mutation/full-hydrate lane. Maintenance drops only this
    /// lane so its broad metadata generation cannot overlap mutable message
    /// state.
    primary_service: Option<Arc<DaemonDfsMessageServiceLaneHandle>>,
    /// Lazily opened lane for targeted immutable reads and namespace lookups.
    /// It is never fully hydrated, so keeping reads off the mutation lane does
    /// not retain a second all-shard metadata projection while idle.
    lightweight_read_service: Option<Arc<DaemonDfsMessageServiceLaneHandle>>,
    /// Lazily opened lane for exact immutable content reads. These requests
    /// already carry the selected version and content identity, so serving
    /// them must not open or rebind a metadata backend on the RPC path.
    content_read_service: Option<Arc<DaemonDfsMessageServiceLaneHandle>>,
}

#[derive(Debug)]
struct DaemonDfsMaintenanceServiceLane {
    signature: DaemonDfsMessageServiceSignature,
    metadata_authority_generation: Arc<DaemonDfsMetadataAuthorityGeneration>,
    peer_node_ids: Vec<NodeId>,
    lane: DaemonDfsMessageServiceLane,
    detached_send_runtime: Arc<DaemonDfsDetachedSendRuntime>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DaemonDfsMutationLaneKey {
    file_system_id: DfsFileSystemId,
    shard_id: DfsMetadataShardId,
}

impl DaemonDfsMutationLaneKey {
    fn from_scope(scope: &DfsMessageExecutionScope) -> Option<Self> {
        match scope {
            DfsMessageExecutionScope::ExactShard {
                file_system_id,
                shard_id,
            } => Some(Self {
                file_system_id: file_system_id.clone(),
                shard_id: shard_id.clone(),
            }),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct DaemonDfsMutationScheduler {
    global: Arc<tokio::sync::RwLock<()>>,
    shard_gates: Mutex<BTreeMap<DaemonDfsMutationLaneKey, Arc<tokio::sync::Semaphore>>>,
}

impl DaemonDfsMutationScheduler {
    fn new() -> Self {
        Self {
            global: Arc::new(tokio::sync::RwLock::new(())),
            shard_gates: Mutex::new(BTreeMap::new()),
        }
    }

    fn shard_gate(
        &self,
        key: &DaemonDfsMutationLaneKey,
    ) -> Result<Option<Arc<tokio::sync::Semaphore>>, DaemonError> {
        let mut gates = self.shard_gates.lock().map_err(|_| DaemonError::Network {
            address: "DFS mutation scheduler".to_string(),
            detail: "DFS mutation scheduler shard-gate map is poisoned".to_string(),
        })?;
        if let Some(gate) = gates.get(key) {
            return Ok(Some(Arc::clone(gate)));
        }
        if gates.len() >= DAEMON_DFS_MUTATION_SHARD_GATE_LIMIT {
            return Ok(None);
        }
        let gate = Arc::new(tokio::sync::Semaphore::new(1));
        gates.insert(key.clone(), Arc::clone(&gate));
        Ok(Some(gate))
    }

    async fn admit(
        self: &Arc<Self>,
        scope: &DfsMessageExecutionScope,
    ) -> Result<DaemonDfsMutationAdmission, DaemonError> {
        let Some(key) = DaemonDfsMutationLaneKey::from_scope(scope) else {
            if matches!(scope, DfsMessageExecutionScope::ReadOnly) {
                return Ok(DaemonDfsMutationAdmission::ReadOnly {
                    _scheduler: Arc::clone(self),
                });
            }
            let global = Arc::clone(&self.global).write_owned().await;
            return Ok(DaemonDfsMutationAdmission::Global {
                _scheduler: Arc::clone(self),
                _global: global,
            });
        };
        // Reserve the exact shard before joining the global read side. A
        // same-shard backlog therefore waits without holding read guards, so
        // Tokio's write-preferring RwLock can admit a queued global mutation
        // after only the currently active exact owners finish. The companion
        // Quint model represents this as a shard-only reservation waiting on
        // a queued global fence. Admission happens before spawn_blocking, so
        // neither kind of waiter consumes a blocking worker.
        let Some(shard_gate) = self.shard_gate(&key)? else {
            let global = Arc::clone(&self.global).write_owned().await;
            return Ok(DaemonDfsMutationAdmission::Global {
                _scheduler: Arc::clone(self),
                _global: global,
            });
        };
        let shard = shard_gate
            .acquire_owned()
            .await
            .map_err(|_| DaemonError::Network {
                address: "DFS mutation scheduler".to_string(),
                detail: format!(
                    "DFS mutation scheduler closed the {}:{} shard gate",
                    key.file_system_id, key.shard_id.0
                ),
            })?;
        let global = Arc::clone(&self.global).read_owned().await;
        Ok(DaemonDfsMutationAdmission::ExactShard {
            _scheduler: Arc::clone(self),
            _global: global,
            _shard: shard,
        })
    }
}

enum DaemonDfsMutationAdmission {
    ReadOnly {
        _scheduler: Arc<DaemonDfsMutationScheduler>,
    },
    ExactShard {
        _scheduler: Arc<DaemonDfsMutationScheduler>,
        _global: tokio::sync::OwnedRwLockReadGuard<()>,
        _shard: tokio::sync::OwnedSemaphorePermit,
    },
    Global {
        _scheduler: Arc<DaemonDfsMutationScheduler>,
        _global: tokio::sync::OwnedRwLockWriteGuard<()>,
    },
}

static DAEMON_DFS_MESSAGE_SERVICE_CACHE: OnceLock<
    Mutex<BTreeMap<PathBuf, DaemonDfsMessageServiceCacheEntry>>,
> = OnceLock::new();
#[cfg(test)]
static DAEMON_DFS_TEST_MESSAGE_SERVICE_LANE_OPENS: OnceLock<Mutex<BTreeMap<PathBuf, usize>>> =
    OnceLock::new();
#[cfg(test)]
static DAEMON_DFS_TEST_METADATA_PUBLISH_DISCOVERY_ATTEMPTS: OnceLock<
    Mutex<BTreeMap<PathBuf, (usize, usize)>>,
> = OnceLock::new();
#[cfg(test)]
static DAEMON_DFS_TEST_METADATA_PUBLISH_LANE_REQUESTS: OnceLock<
    Mutex<BTreeMap<DaemonDfsMetadataPublishLaneKey, usize>>,
> = OnceLock::new();
#[cfg(test)]
static DAEMON_DFS_TEST_REPAIR_MAINTENANCE_EDGE_REQUESTS: OnceLock<Mutex<BTreeMap<PathBuf, usize>>> =
    OnceLock::new();
#[cfg(test)]
static DAEMON_DFS_TEST_MESSAGE_COMMITTED_EDGE_HOOKS: OnceLock<
    Mutex<BTreeMap<String, Arc<DaemonDfsMessageCommittedEdgeTestHook>>>,
> = OnceLock::new();
static DAEMON_DFS_MUTATION_SCHEDULERS: OnceLock<
    Mutex<BTreeMap<PathBuf, Weak<DaemonDfsMutationScheduler>>>,
> = OnceLock::new();
static DAEMON_DFS_METADATA_PUBLISH_SCHEDULERS: OnceLock<
    Mutex<BTreeMap<DaemonDfsMetadataPublishLaneKey, Weak<sinomo_dfs::DfsMetadataPublishScheduler>>>,
> = OnceLock::new();
static DAEMON_DFS_METADATA_PUBLISH_DISCOVERY_SCHEDULERS: OnceLock<
    Mutex<BTreeMap<PathBuf, Weak<sinomo_dfs::DfsMetadataPublishScheduler>>>,
> = OnceLock::new();
static DAEMON_DFS_MESSAGE_EXECUTION_GATES: OnceLock<
    Mutex<BTreeMap<PathBuf, Weak<DaemonDfsMessageExecutionGate>>>,
> = OnceLock::new();
static DAEMON_DEFAULT_DFS_RECONCILIATION_WAKES: OnceLock<
    Mutex<BTreeMap<PathBuf, Weak<DaemonDefaultDfsReconciliationWake>>>,
> = OnceLock::new();
static DAEMON_DFS_METADATA_AUTHORITY_GENERATIONS: OnceLock<
    Mutex<
        BTreeMap<
            DaemonDfsMetadataAuthorityGenerationKey,
            Weak<DaemonDfsMetadataAuthorityGeneration>,
        >,
    >,
> = OnceLock::new();
static DAEMON_DFS_DETACHED_SEND_RUNTIMES: OnceLock<
    Mutex<BTreeMap<DaemonDfsDetachedSendRuntimeKey, Arc<DaemonDfsDetachedSendRuntime>>>,
> = OnceLock::new();
static DAEMON_DFS_MAINTENANCE_NOTIFY: OnceLock<Arc<tokio::sync::Notify>> = OnceLock::new();
static DAEMON_DFS_REPAIR_QUEUE_NONEMPTY: OnceLock<Mutex<BTreeMap<PathBuf, bool>>> = OnceLock::new();
#[cfg(test)]
static DAEMON_DFS_MAINTENANCE_WAKE_COUNT: AtomicU64 = AtomicU64::new(0);
static DAEMON_DFS_REPAIR_QUEUE_SNAPSHOT_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static DAEMON_DFS_PLACEMENT_AUTHORITY_SESSION_ID: OnceLock<String> = OnceLock::new();
static DAEMON_DFS_DATA_OWNER_LOCKS: OnceLock<Mutex<BTreeMap<PathBuf, Weak<DaemonDataOwnerLock>>>> =
    OnceLock::new();
static DAEMON_DFS_SMOKE_IN_FLIGHT: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
#[derive(Debug)]
struct DaemonDfsMessageCommittedEdgeTestHook {
    started_tx: std::sync::mpsc::Sender<()>,
    release_rx: Mutex<std::sync::mpsc::Receiver<()>>,
}

/// Publish the process-lifetime main-daemon data lock to DFS blocking work.
/// The registry stores only a weak reference; the control loop remains the
/// owner, while each backend-bearing blocking task upgrades and retains its
/// own `Arc` until the actual synchronous work stops.
pub(crate) fn register_daemon_dfs_data_owner_lock(
    paths: &DaemonPaths,
    lock: &Arc<DaemonDataOwnerLock>,
) -> Result<(), DaemonError> {
    DAEMON_DFS_DATA_OWNER_LOCKS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .map_err(|_| DaemonError::Network {
            address: "DFS data-owner lock registry".to_string(),
            detail: "data-owner lock registry is poisoned".to_string(),
        })?
        .insert(paths.root.clone(), Arc::downgrade(lock));
    Ok(())
}

fn daemon_dfs_data_owner_lock(
    paths: &DaemonPaths,
) -> Result<Option<Arc<DaemonDataOwnerLock>>, DaemonError> {
    Ok(DAEMON_DFS_DATA_OWNER_LOCKS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .map_err(|_| DaemonError::Network {
            address: "DFS data-owner lock registry".to_string(),
            detail: "data-owner lock registry is poisoned".to_string(),
        })?
        .get(&paths.root)
        .and_then(Weak::upgrade))
}

#[derive(Debug)]
struct DaemonDfsSmokeInFlightGuard;

impl DaemonDfsSmokeInFlightGuard {
    fn acquire() -> Result<Self, DaemonError> {
        DAEMON_DFS_SMOKE_IN_FLIGHT
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| Self)
            .map_err(|_| DaemonError::Network {
                address: "DFS smoke".to_string(),
                detail: "DFS smoke is already running".to_string(),
            })
    }
}

impl Drop for DaemonDfsSmokeInFlightGuard {
    fn drop(&mut self) {
        DAEMON_DFS_SMOKE_IN_FLIGHT.store(false, Ordering::Release);
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct DaemonDfsPendingMaintenanceWork {
    pub pending_write_count: usize,
    pub pending_repair_count: usize,
    pub repair_attempt_count: usize,
    pub due_repair_attempt_count: usize,
    pub unattempted_repair_count: usize,
    pub next_repair_retry_unix_nanos: Option<u64>,
    pub due_time_based_maintenance_count: usize,
    pub next_time_based_maintenance_unix_nanos: Option<u64>,
    pub deferred_work_count: usize,
    pub pending_namespace_job_count: usize,
    pub actionable_conflict_count: usize,
    pub actionable_placement_reconciliation_count: usize,
    pub metadata_replica_lag_count: usize,
    pub pending_local_membership_handoff_count: usize,
}

impl DaemonDfsPendingMaintenanceWork {
    #[cfg(test)]
    pub(crate) fn has_work(&self) -> bool {
        self.pending_write_count > 0
            || self.pending_repair_count > 0
            || self.pending_namespace_job_count > 0
            || self.actionable_conflict_count > 0
            || self.actionable_placement_reconciliation_count > 0
            || self.metadata_replica_lag_count > 0
            || self.pending_local_membership_handoff_count > 0
            || self.due_time_based_maintenance_count > 0
    }

    pub(crate) fn should_trigger_background_maintenance(&self) -> bool {
        // Pending repairs can represent unreachable replicas and are expensive
        // to rescan. Trigger the short loop only when a repair is due or has
        // never been attempted; repair backoff snapshots keep unreachable
        // replicas from hot-looping. Metadata replica lag is intentionally not
        // a broad-maintenance trigger: retained exact-shard publisher lanes own
        // those durable cursors and their connectivity/backoff wakes.
        self.pending_write_count > 0
            || self.pending_namespace_job_count > 0
            || self.actionable_conflict_count > 0
            || self.actionable_placement_reconciliation_count > 0
            || self.pending_local_membership_handoff_count > 0
            || self.due_repair_attempt_count > 0
            || self.unattempted_repair_count > 0
            || self.due_time_based_maintenance_count > 0
    }

    pub(crate) fn next_maintenance_deadline_unix_nanos(&self) -> Option<u64> {
        self.next_repair_retry_unix_nanos
            .into_iter()
            .chain(self.next_time_based_maintenance_unix_nanos)
            .min()
    }

    pub(crate) fn has_deferred_work(&self) -> bool {
        self.deferred_work_count > 0
    }
}

pub(crate) fn daemon_dfs_now_unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
fn daemon_dfs_duration_nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct DaemonDfsMetadataHistoryGcSchedule {
    due_record_count: usize,
    next_deadline_unix_nanos: Option<u64>,
}

#[cfg(test)]
fn daemon_dfs_metadata_history_gc_schedule(
    projection: &sinomo_dfs::DfsMetadataHistoryGcReport,
    now_unix_nanos: u64,
    record_trigger: u64,
    bytes_trigger: u64,
    max_history_age: Duration,
) -> DaemonDfsMetadataHistoryGcSchedule {
    let eligible_record_count = projection.eligible_record_count();
    let retained_pressure = projection
        .shards
        .iter()
        .filter(|shard| shard.collection_authorized)
        .any(|shard| {
            let largest_retained_representation = shard
                .estimated_serialized_bytes
                .max(shard.snapshot_size_bytes.unwrap_or(0))
                .max(shard.journal_size_bytes);
            shard.total_record_count >= record_trigger
                || largest_retained_representation >= bytes_trigger
        });
    // Clamp the lifecycle age trigger to the planner's configured recovery
    // window. Count and byte pressure are likewise gated on an age-eligible
    // candidate, so neither path can shorten the retention contract.
    let trigger_age_nanos =
        daemon_dfs_duration_nanos(max_history_age).max(projection.min_history_age_nanos);
    let oldest_age_deadline = projection
        .oldest_eligible_record_unix_nanos
        .map(|retained_at| retained_at.saturating_add(trigger_age_nanos));
    let age_due = eligible_record_count > 0
        && oldest_age_deadline.is_none_or(|deadline| deadline <= now_unix_nanos);
    let pressure_due = eligible_record_count > 0 && retained_pressure;
    if age_due || pressure_due {
        return DaemonDfsMetadataHistoryGcSchedule {
            due_record_count: usize::try_from(eligible_record_count).unwrap_or(usize::MAX),
            next_deadline_unix_nanos: None,
        };
    }

    // A footprint made only of young or recovery-rooted records cannot advance
    // GC. Keep its first eligibility deadline observable without advertising
    // immediately actionable work; otherwise a static high-water condition
    // would launch a no-op lifecycle pass every short maintenance interval.
    let next_deadline_unix_nanos = if eligible_record_count == 0 {
        projection.next_eligible_at_unix_nanos
    } else {
        oldest_age_deadline.filter(|deadline| *deadline > now_unix_nanos)
    };
    DaemonDfsMetadataHistoryGcSchedule {
        due_record_count: 0,
        next_deadline_unix_nanos,
    }
}

#[derive(Debug)]
struct DaemonDfsMaintenanceAdmission {
    _lifecycle_guard: tokio::sync::OwnedMutexGuard<()>,
    _message_execution_guard: tokio::sync::OwnedSemaphorePermit,
}

async fn acquire_daemon_dfs_maintenance_admission(
    data_dir: &SinomoDataDir,
) -> Result<DaemonDfsMaintenanceAdmission, DaemonError> {
    // DFS metadata journals and repair queues are node-local. Cross-node
    // serialization prevents a primary on another daemon from advancing its
    // own shard, so admission is intentionally limited to this data root.
    let lifecycle_guard = daemon_default_dfs_lifecycle_gate(&data_dir.root())?
        .lock_owned()
        .await;
    let message_execution_gate = daemon_dfs_message_execution_gate(data_dir)?;
    let message_execution_guard = Arc::clone(&message_execution_gate.semaphore)
        .acquire_owned()
        .await
        .map_err(|_| DaemonError::Network {
            address: "DFS message execution".to_string(),
            detail: "DFS message execution gate closed".to_string(),
        })?;
    Ok(DaemonDfsMaintenanceAdmission {
        _lifecycle_guard: lifecycle_guard,
        _message_execution_guard: message_execution_guard,
    })
}

#[derive(Clone, Debug)]
struct DaemonDfsControlPlaneKvClient {
    cluster_paths: ClusterPaths,
    runtime: tokio::runtime::Handle,
    /// One immutable latest-observed view is shared by all reads in an
    /// operation. Refreshes and local commits replace it atomically instead of
    /// replaying the Raft WAL for every `get` and `range_prefix`.
    snapshot: Arc<Mutex<Option<Arc<KvSnapshot>>>>,
    /// Cached and short-lived services normally retain only a weak capability;
    /// their surrounding blocking task owns the strong guard. This prevents a
    /// static message cache from pinning an old daemon's OS lock after shutdown.
    data_owner_lock: Option<Weak<DaemonDataOwnerLock>>,
    /// Sandbox services may outlive the handler which opened them, so those
    /// services explicitly retain the strong capability for their lifetime.
    _owned_data_owner_lock: Option<Arc<DaemonDataOwnerLock>>,
}

impl DaemonDfsControlPlaneKvClient {
    fn new_on_runtime(cluster_paths: &ClusterPaths, runtime: &tokio::runtime::Handle) -> Self {
        let data_owner_lock = Self::discover_data_owner_lock(cluster_paths);
        Self {
            cluster_paths: cluster_paths.clone(),
            runtime: runtime.clone(),
            snapshot: Arc::new(Mutex::new(None)),
            data_owner_lock: data_owner_lock.as_ref().map(Arc::downgrade),
            _owned_data_owner_lock: None,
        }
    }

    fn new_owned_on_runtime(
        cluster_paths: &ClusterPaths,
        runtime: &tokio::runtime::Handle,
    ) -> Self {
        let data_owner_lock = Self::discover_data_owner_lock(cluster_paths);
        Self {
            cluster_paths: cluster_paths.clone(),
            runtime: runtime.clone(),
            snapshot: Arc::new(Mutex::new(None)),
            data_owner_lock: data_owner_lock.as_ref().map(Arc::downgrade),
            _owned_data_owner_lock: data_owner_lock,
        }
    }

    fn discover_data_owner_lock(cluster_paths: &ClusterPaths) -> Option<Arc<DaemonDataOwnerLock>> {
        cluster_paths
            .root
            .parent()
            .map(SinomoDataDir::new)
            .map(|data_dir| DaemonPaths::new(&data_dir))
            .and_then(|paths| daemon_dfs_data_owner_lock(&paths).ok().flatten())
    }

    fn data_owner_lock(&self) -> Option<Arc<DaemonDataOwnerLock>> {
        self.data_owner_lock.as_ref().and_then(Weak::upgrade)
    }

    fn daemon_error(&self, source: DaemonError) -> KvError {
        KvError::Io {
            path: self.cluster_paths.kv_paths().snapshot.display().to_string(),
            detail: source.to_string(),
        }
    }

    fn snapshot_lock_error(&self) -> KvError {
        KvError::Io {
            path: self.cluster_paths.kv_paths().snapshot.display().to_string(),
            detail: "DFS cluster KV snapshot cache lock is poisoned".to_string(),
        }
    }

    fn load_latest_snapshot(&self) -> Result<Option<Arc<KvSnapshot>>, KvError> {
        sinomo_core::cluster::read_cluster_kv_latest_observed_snapshot_arc_if_present(
            &self.cluster_paths,
        )
        .map_err(|source| self.daemon_error(source.into()))
    }

    fn cached_snapshot(&self) -> Result<Option<Arc<KvSnapshot>>, KvError> {
        if let Some(snapshot) = self
            .snapshot
            .lock()
            .map_err(|_| self.snapshot_lock_error())?
            .as_ref()
            .cloned()
        {
            return Ok(Some(snapshot));
        }
        let snapshot = self.load_latest_snapshot()?;
        if let Some(snapshot) = snapshot.as_ref() {
            *self
                .snapshot
                .lock()
                .map_err(|_| self.snapshot_lock_error())? = Some(Arc::clone(snapshot));
        }
        Ok(snapshot)
    }

    fn replace_cached_snapshot(&self) -> Result<(), KvError> {
        let snapshot = self.load_latest_snapshot()?;
        *self
            .snapshot
            .lock()
            .map_err(|_| self.snapshot_lock_error())? = snapshot;
        Ok(())
    }

    fn invalidate_cached_snapshot(&self) -> Result<(), KvError> {
        *self
            .snapshot
            .lock()
            .map_err(|_| self.snapshot_lock_error())? = None;
        Ok(())
    }
}

fn require_dfs_placement_authority_admin_owner(
    paths: &DaemonPaths,
) -> Result<Arc<DaemonDataOwnerLock>, DaemonError> {
    daemon_dfs_data_owner_lock(paths)?.ok_or_else(|| DaemonError::Parse {
        detail: "DFS placement-authority administration requires the live exclusive main-daemon data-owner capability"
            .to_string(),
    })
}

pub(crate) fn inspect_dfs_placement_authority_permit(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    file_system_id: &DfsFileSystemId,
) -> Result<Option<sinomo_dfs::DfsPlacementAuthorityPermitStatus>, DaemonError> {
    let _data_owner_lock = require_dfs_placement_authority_admin_owner(paths)?;
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let mut client = DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, runtime);
    sinomo_dfs::DfsControlPlaneKvClient::refresh_latest(&mut client)?;
    sinomo_dfs::DfsControlPlaneStore::new(&mut client)
        .placement_authority_permit_status(file_system_id)
        .map_err(Into::into)
}

pub(crate) fn recover_removed_node_dfs_placement_authority_permit(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    file_system_id: &DfsFileSystemId,
    expected_recovery_token: &str,
    acknowledge_owner_stopped: bool,
) -> Result<sinomo_dfs::DfsPlacementAuthorityRecoveryOutcome, DaemonError> {
    let _data_owner_lock = require_dfs_placement_authority_admin_owner(paths)?;
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let mut client = DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, runtime);
    sinomo_dfs::DfsControlPlaneKvClient::refresh_latest(&mut client)?;
    sinomo_dfs::DfsControlPlaneStore::new(&mut client)
        .recover_removed_node_placement_authority_permit(
            file_system_id,
            &state.cluster_id.to_string(),
            &state.local_node_id,
            expected_recovery_token,
            acknowledge_owner_stopped,
        )
        .map_err(Into::into)
}

pub(crate) fn break_glass_recover_dfs_placement_authority_permit(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    file_system_id: &DfsFileSystemId,
    expected_recovery_token: &str,
    acknowledgement: &str,
) -> Result<sinomo_dfs::DfsPlacementAuthorityRecoveryOutcome, DaemonError> {
    let _data_owner_lock = require_dfs_placement_authority_admin_owner(paths)?;
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let mut client = DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, runtime);
    sinomo_dfs::DfsControlPlaneKvClient::refresh_latest(&mut client)?;
    sinomo_dfs::DfsControlPlaneStore::new(&mut client)
        .break_glass_recover_placement_authority_permit(
            file_system_id,
            &state.cluster_id.to_string(),
            expected_recovery_token,
            acknowledgement,
        )
        .map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn recover_prior_process_default_dfs_placement_authority(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<bool, DaemonError> {
    recover_prior_process_default_dfs_placement_authority_from_observation(paths, runtime, true)
}

/// Recover only a prior-process placement permit owned by this main daemon's
/// local cluster node. Callers are required to hold `DaemonLock` and
/// `DaemonDataOwnerLock`; the control loop acquires both for its full lifetime
/// before this helper is reachable. Exact-value CAS in the DFS store prevents
/// deleting a concurrently replaced holder.
fn recover_prior_process_default_dfs_placement_authority_from_observation(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    refresh_latest: bool,
) -> Result<bool, DaemonError> {
    let _data_owner_lock = daemon_dfs_data_owner_lock(paths)?.ok_or_else(|| {
        DaemonError::Parse {
            detail: "refusing placement-authority recovery without a live exclusive main-daemon data-owner lock"
                .to_string(),
        }
    })?;
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(false);
    }
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let mut client = DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, runtime);
    let current_process =
        sinomo_dfs::DfsControlPlaneKvClient::placement_authority_process_identity(&client)
            .ok_or_else(|| DaemonError::Parse {
                detail: "main daemon has no DFS placement-authority process identity".to_string(),
            })?;
    if refresh_latest {
        sinomo_dfs::DfsControlPlaneKvClient::refresh_latest(&mut client)?;
    }
    let recovered = sinomo_dfs::DfsControlPlaneStore::new(&mut client)
        .recover_prior_process_placement_authority_permit_for_local_node(
            &sinomo_dfs::sifs_file_system_id(),
            &state.local_node_id,
            &current_process,
        )?;
    if recovered {
        tracing::info!(
            target: "sinomo.dfs.placement",
            node_id = %state.local_node_id,
            "recovered exact prior-process SIFS placement authority permit"
        );
    }
    Ok(recovered)
}

fn daemon_dfs_txn_puts_local_membership_handoff(cluster_paths: &ClusterPaths, txn: &KvTxn) -> bool {
    if !txn.success.iter().any(
        |op| matches!(op, KvOp::Put { key, .. } if daemon_dfs_membership_handoff_key_is_exact(key)),
    ) {
        return false;
    }
    let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths) else {
        return false;
    };
    daemon_dfs_txn_puts_membership_handoff_for_node(txn, &state.local_node_id)
}

fn daemon_dfs_membership_handoff_key_is_exact(key: &str) -> bool {
    key.strip_prefix("/dfs/filesystems/")
        .and_then(|suffix| suffix.split_once("/metadata-membership-handoffs/"))
        .is_some_and(|(file_system_id, shard_id)| {
            !file_system_id.is_empty() && !shard_id.is_empty() && !shard_id.contains('/')
        })
}

fn daemon_dfs_txn_puts_membership_handoff_for_node(txn: &KvTxn, local_node_id: &NodeId) -> bool {
    txn.success.iter().any(|op| {
        let KvOp::Put { key, value, .. } = op else {
            return false;
        };
        if !daemon_dfs_membership_handoff_key_is_exact(key) {
            return false;
        }
        sinomo_core::kv::decode_kv_value::<DfsMetadataShardMembershipHandoff>(value)
            .is_ok_and(|handoff| handoff.retiring_shard.primary_node_id == *local_node_id)
    })
}

impl sinomo_dfs::DfsControlPlaneKvClient for DaemonDfsControlPlaneKvClient {
    fn refresh_latest(&mut self) -> Result<(), KvError> {
        crate::cluster::refresh_cluster_kv_store_from_peers_on_runtime(
            &self.cluster_paths,
            &self.runtime,
        )
        .map_err(|source| self.daemon_error(source))?;
        self.replace_cached_snapshot()
    }

    fn placement_authority_process_identity(
        &self,
    ) -> Option<sinomo_dfs::DfsPlacementAuthorityProcessIdentity> {
        // The shadow daemon shares cluster state but never owns local data.
        // Only a process which currently retains the main daemon's exclusive
        // data-owner lock may advertise placement mutation authority.
        let _data_owner_lock = self.data_owner_lock()?;
        let state =
            sinomo_core::cluster::read_cluster_state_without_kv(&self.cluster_paths).ok()?;
        Some(sinomo_dfs::DfsPlacementAuthorityProcessIdentity {
            cluster_id: state.cluster_id.to_string(),
            process_session_id: DAEMON_DFS_PLACEMENT_AUTHORITY_SESSION_ID
                .get_or_init(|| sinomo_core::Id::generate().to_string())
                .clone(),
        })
    }

    fn authorizes_prior_process_placement_authority_recovery(
        &self,
        local_node_id: &NodeId,
        current_process: &sinomo_dfs::DfsPlacementAuthorityProcessIdentity,
    ) -> bool {
        let Some(_data_owner_lock) = self.data_owner_lock() else {
            return false;
        };
        let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(&self.cluster_paths)
        else {
            return false;
        };
        state.local_node_id == *local_node_id
            && sinomo_dfs::DfsControlPlaneKvClient::placement_authority_process_identity(self)
                .as_ref()
                == Some(current_process)
    }

    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        Ok(self
            .cached_snapshot()?
            .and_then(|snapshot| snapshot.get(key).cloned()))
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        self.cached_snapshot()?.map_or_else(
            || Ok(Vec::new()),
            |snapshot| {
                snapshot.range_prefix_limited(
                    prefix,
                    sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
                )
            },
        )
    }

    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
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
        let notify_handoff =
            daemon_dfs_txn_puts_local_membership_handoff(&self.cluster_paths, &txn);
        let response = crate::cluster::handle_cluster_kv_txn_request_with_cluster_paths_on_runtime(
            &self.cluster_paths,
            txn,
            true,
            &self.runtime,
        )
        .map_err(|source| self.daemon_error(source))?;
        if response.succeeded && notify_handoff {
            notify_daemon_dfs_maintenance_work();
        }
        self.invalidate_cached_snapshot()?;
        Ok(response)
    }
}

impl sinomo_dfs::AsyncDfsControlPlaneKvClient for DaemonDfsControlPlaneKvClient {
    fn refresh_latest_async<'a>(&'a mut self) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, ()> {
        Box::pin(async move {
            crate::cluster::refresh_cluster_kv_store_from_peers_async_on_runtime(
                &self.cluster_paths,
                &self.runtime,
            )
            .await
            .map_err(|source| self.daemon_error(source))?;
            self.replace_cached_snapshot()
        })
    }

    fn get_async<'a>(
        &'a self,
        key: &'a str,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, Option<KvEntry>> {
        Box::pin(async move {
            Ok(self
                .cached_snapshot()?
                .and_then(|snapshot| snapshot.get(key).cloned()))
        })
    }

    fn range_prefix_async<'a>(
        &'a self,
        prefix: &'a str,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, Vec<KvEntry>> {
        Box::pin(async move {
            self.cached_snapshot()?.map_or_else(
                || Ok(Vec::new()),
                |snapshot| {
                    snapshot.range_prefix_limited(
                        prefix,
                        sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
                    )
                },
            )
        })
    }

    fn put_async<'a>(
        &'a mut self,
        key: String,
        value: Vec<u8>,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, KvRevision> {
        Box::pin(async move {
            let response = self
                .txn_async(KvTxn {
                    compares: Vec::new(),
                    success: vec![KvOp::Put {
                        key,
                        value,
                        lease_id: None,
                    }],
                    failure: Vec::new(),
                })
                .await?;
            Ok(response.revision)
        })
    }

    fn txn_async<'a>(
        &'a mut self,
        txn: KvTxn,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, KvTxnResponse> {
        Box::pin(async move {
            let notify_handoff =
                daemon_dfs_txn_puts_local_membership_handoff(&self.cluster_paths, &txn);
            let response =
                crate::cluster::handle_cluster_kv_txn_request_with_cluster_paths_async_on_runtime(
                    &self.cluster_paths,
                    txn,
                    true,
                    &self.runtime,
                )
                .await
                .map_err(|source| self.daemon_error(source))?;
            if response.succeeded && notify_handoff {
                notify_daemon_dfs_maintenance_work();
            }
            self.invalidate_cached_snapshot()?;
            Ok(response)
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DaemonDfsDataPlaneHost {
    paths: DaemonPaths,
    runtime: tokio::runtime::Handle,
}

impl DaemonDfsDataPlaneHost {
    pub(crate) fn new_on_runtime(paths: DaemonPaths, runtime: &tokio::runtime::Handle) -> Self {
        Self {
            paths,
            runtime: runtime.clone(),
        }
    }
}

impl DfsDataPlaneHost for DaemonDfsDataPlaneHost {
    fn run_smoke<'a>(&'a self) -> DfsDataPlaneHostFuture<'a, sinomo_dfs::DfsSmokeReport> {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            run_daemon_dfs_smoke_async(&paths, &runtime)
                .await
                .map_err(daemon_dfs_data_plane_error)
        })
    }

    fn handle_message<'a>(
        &'a self,
        command: DfsMessageCommand,
        attachment: Option<DfsInboundAttachment>,
        traceparent: Option<String>,
    ) -> DfsDataPlaneHostFuture<'a, DfsDataPlaneResponse> {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            handle_daemon_dfs_message_async(&paths, command, attachment, traceparent, &runtime)
                .await
                .map_err(daemon_dfs_data_plane_error)
        })
    }
}

fn daemon_dfs_data_plane_error(source: DaemonError) -> DfsDataPlaneHostError {
    DfsDataPlaneHostError::new(source.to_string())
}

pub(crate) fn with_daemon_dfs_control_plane_on_runtime<T: sinomo_dfs::DfsMessageTransport>(
    service: NetworkedDfsService<T>,
    cluster_paths: &ClusterPaths,
    runtime: Option<&tokio::runtime::Handle>,
) -> NetworkedDfsService<T> {
    if let Some(control_plane) = daemon_dfs_control_plane_client_on_runtime(cluster_paths, runtime)
    {
        service.with_boxed_control_plane(control_plane)
    } else {
        service
    }
}

/// Attach a control plane which retains the main daemon's ownership proof for
/// the complete service lifetime. Sandbox services can escape the blocking
/// handler which creates them; ordinary cached/message services must use the
/// weak-capability helper above.
pub(crate) fn with_daemon_owned_dfs_control_plane_on_runtime<T: sinomo_dfs::DfsMessageTransport>(
    service: NetworkedDfsService<T>,
    cluster_paths: &ClusterPaths,
    runtime: Option<&tokio::runtime::Handle>,
) -> NetworkedDfsService<T> {
    let Some(runtime) = runtime else {
        return service;
    };
    if !cluster_paths.kv_paths().snapshot.exists() {
        return service;
    }
    service.with_boxed_control_plane(Box::new(
        DaemonDfsControlPlaneKvClient::new_owned_on_runtime(cluster_paths, runtime),
    ))
}

fn daemon_dfs_control_plane_client_on_runtime(
    cluster_paths: &ClusterPaths,
    runtime: Option<&tokio::runtime::Handle>,
) -> Option<Box<dyn sinomo_dfs::DfsControlPlaneKvClient>> {
    let runtime = runtime?;
    cluster_paths.kv_paths().snapshot.exists().then(|| {
        Box::new(DaemonDfsControlPlaneKvClient::new_on_runtime(
            cluster_paths,
            runtime,
        )) as Box<dyn sinomo_dfs::DfsControlPlaneKvClient>
    })
}

pub(crate) fn daemon_dfs_node_transport(
    local_node_id: sinomo_core::node::NodeId,
    peer_node_ids: impl IntoIterator<Item = NodeId>,
) -> DfsNodeTransport {
    let peer_transport = Arc::new(DaemonDfsUnavailablePeerTransport);
    let sync_peer_transport: Arc<dyn DfsPeerTransport> = peer_transport.clone();
    let async_peer_transport: Arc<dyn sinomo_dfs::AsyncDfsPeerTransport> = peer_transport;
    DfsNodeTransport::with_peer_transports_for_nodes(
        local_node_id,
        peer_node_ids,
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        sync_peer_transport,
        async_peer_transport,
    )
}

pub(crate) fn daemon_dfs_node_transport_on_runtime(
    local_node_id: sinomo_core::node::NodeId,
    peer_node_ids: impl IntoIterator<Item = NodeId>,
    data_dir: &SinomoDataDir,
    runtime: &tokio::runtime::Handle,
) -> DfsNodeTransport {
    let peer_transport = daemon_dfs_blocking_peer_transport_on_runtime(data_dir, runtime);
    let async_peer_transport = daemon_dfs_async_peer_transport(data_dir, runtime);
    DfsNodeTransport::with_peer_transports_for_nodes(
        local_node_id,
        peer_node_ids,
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        peer_transport,
        async_peer_transport,
    )
}

#[derive(Clone, Debug)]
struct DaemonDfsBlockingPeerTransport {
    paths: DaemonPaths,
    runtime: tokio::runtime::Handle,
}

#[derive(Clone, Debug)]
struct DaemonDfsAsyncPeerTransport {
    paths: DaemonPaths,
    runtime: tokio::runtime::Handle,
}

#[derive(Clone, Debug)]
struct DaemonDfsUnavailablePeerTransport;

fn daemon_dfs_unavailable_peer_transport_error() -> DfsPeerTransportError {
    DfsPeerTransportError::new(
        "local-only DFS peer transport cannot send without the daemon-owned Tokio runtime",
    )
}

impl DfsPeerTransport for DaemonDfsUnavailablePeerTransport {
    fn call_object_rpc(
        &self,
        _target_node_id: &NodeId,
        _call: ObjectRpcCall,
        _options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
        Err(daemon_dfs_unavailable_peer_transport_error())
    }
}

impl sinomo_dfs::AsyncDfsPeerTransport for DaemonDfsUnavailablePeerTransport {
    fn call_object_rpc_async<'a>(
        &'a self,
        _target_node_id: &'a NodeId,
        _call: ObjectRpcCall,
        _options: DfsObjectRpcSendOptions,
    ) -> sinomo_dfs::DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>>
    {
        Box::pin(async { Err(daemon_dfs_unavailable_peer_transport_error()) })
    }
}

impl DfsPeerTransport for DaemonDfsBlockingPeerTransport {
    fn call_object_rpc(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
        send_object_rpc_call_to_target_on_runtime_with_daemon_paths_and_options(
            &self.paths,
            target_node_id,
            call,
            options.cluster_rpc_options(),
            &self.runtime,
            options.observe_connectivity,
        )
        .map_err(|source| DfsPeerTransportError::new(source.to_string()))
    }
}

impl sinomo_dfs::AsyncDfsPeerTransport for DaemonDfsAsyncPeerTransport {
    fn call_object_rpc_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> sinomo_dfs::DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>>
    {
        let runtime = self.runtime.clone();
        let paths = self.paths.clone();
        Box::pin(async move {
            send_object_rpc_call_to_target_async_with_daemon_paths_and_options(
                &paths,
                target_node_id,
                call,
                options.cluster_rpc_options(),
                &runtime,
                options.observe_connectivity,
            )
            .await
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))
        })
    }
}

pub(crate) fn daemon_dfs_blocking_peer_transport_on_runtime(
    data_dir: &SinomoDataDir,
    runtime: &tokio::runtime::Handle,
) -> Arc<dyn DfsPeerTransport> {
    Arc::new(DaemonDfsBlockingPeerTransport {
        paths: DaemonPaths::new(data_dir),
        runtime: runtime.clone(),
    })
}

pub(crate) fn daemon_dfs_async_peer_transport(
    data_dir: &SinomoDataDir,
    runtime: &tokio::runtime::Handle,
) -> Arc<dyn sinomo_dfs::AsyncDfsPeerTransport> {
    Arc::new(DaemonDfsAsyncPeerTransport {
        paths: DaemonPaths::new(data_dir),
        runtime: runtime.clone(),
    })
}

#[cfg(test)]
fn daemon_dfs_metadata_snapshot_install_sender() -> DfsMetadataSnapshotInstallSender {
    Arc::new(|_endpoint, _command, _timeout| {
        Err(DfsMetadataPeerTransportError::new(
            "test DFS metadata snapshot sender has no daemon runtime",
        ))
    })
}

// DFS inspection request/response types live in `sinomo-dfs`; this module only
// adapts them to daemon-local paths and metadata backends.

pub(crate) fn open_daemon_dfs_metadata_backend(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<Box<dyn DfsMetadataBackend>, DaemonError> {
    sinomo_dfs::open_cluster_aware_dfs_metadata_backend(data_dir, cluster_paths).map_err(Into::into)
}

pub(crate) fn daemon_dfs_maintenance_notify() -> Arc<tokio::sync::Notify> {
    Arc::clone(DAEMON_DFS_MAINTENANCE_NOTIFY.get_or_init(|| Arc::new(tokio::sync::Notify::new())))
}

pub(crate) fn notify_daemon_dfs_maintenance_work() {
    #[cfg(test)]
    DAEMON_DFS_MAINTENANCE_WAKE_COUNT.fetch_add(1, Ordering::Relaxed);
    daemon_dfs_maintenance_notify().notify_one();
}

fn record_daemon_dfs_repair_queue_nonempty(data_dir: &SinomoDataDir, nonempty: bool) {
    DAEMON_DFS_REPAIR_QUEUE_NONEMPTY
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(data_dir.root(), nonempty);
}

fn refresh_daemon_dfs_repair_queue_nonempty(
    data_dir: &SinomoDataDir,
) -> Result<bool, sinomo_dfs::DfsError> {
    let nonempty =
        read_dfs_repair_attempt_snapshot(&default_dfs_repair_attempt_snapshot_path(data_dir))?
            .is_some_and(|snapshot| !snapshot.records.is_empty());
    record_daemon_dfs_repair_queue_nonempty(data_dir, nonempty);
    Ok(nonempty)
}

/// Return the process-local queue state sampled at startup, a committed repair
/// edge, or the end of the previous bounded maintenance pass.
///
/// Connectivity generation changes use this to wake only a repair executor
/// whose external route blocker may have cleared. An empty cluster must not
/// turn routine route refreshes into broad DFS maintenance passes.
pub(crate) fn daemon_dfs_repair_queue_work_is_known(paths: &DaemonPaths) -> bool {
    let Some(data_root) = paths.root.parent() else {
        return false;
    };
    DAEMON_DFS_REPAIR_QUEUE_NONEMPTY
        .get()
        .and_then(|states| {
            states
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(data_root)
                .copied()
        })
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn daemon_dfs_maintenance_wake_count() -> u64 {
    DAEMON_DFS_MAINTENANCE_WAKE_COUNT.load(Ordering::Relaxed)
}

#[cfg(test)]
fn daemon_dfs_local_membership_handoff_keys(
    metadata_shard_statuses: &[DfsMetadataShardStatus],
    local_node_id: &NodeId,
) -> Vec<String> {
    metadata_shard_statuses
        .iter()
        .filter(|status| status.primary_node_id.as_ref() == Some(local_node_id))
        .filter_map(|status| {
            status.file_system_id.as_ref().map(|file_system_id| {
                sinomo_dfs::metadata_shard_membership_handoff_key(
                    file_system_id,
                    &status.shard_id.0,
                )
            })
        })
        .collect()
}

struct DaemonDfsConnectivityStatusProbe;

impl sinomo_core::cluster::ClusterStatusProbe for DaemonDfsConnectivityStatusProbe {
    fn probe_member_status(
        &self,
        _state: &sinomo_core::ClusterBootstrapState,
        _member: &sinomo_core::ClusterMemberRecord,
    ) -> Option<sinomo_core::ClusterNodeStatusRecord> {
        None
    }
}

fn daemon_dfs_node_is_offline_for_maintenance(
    availability: &ClusterNodeAvailability,
    connectivity: &ClusterConnectivityGraph,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
) -> bool {
    availability != &ClusterNodeAvailability::Online
        && local_node_id != target_node_id
        // A failed direct endpoint does not park durable work when the
        // canonical network layer still has a fully healthy relay route to it.
        // Unknown connectivity is not an invitation for DFS to discover a
        // path by repeatedly sending foreground work: route establishment
        // belongs to the network maintenance loop, which will republish an
        // Online projection after it has an executable carrier.
        && !cluster_nodes_share_healthy_component(connectivity, local_node_id, target_node_id)
}

fn daemon_dfs_offline_node_ids(
    cluster_paths: &ClusterPaths,
    state: &sinomo_core::ClusterBootstrapState,
) -> BTreeSet<NodeId> {
    // The stored self-status of a crashed node remains Online. Reuse the
    // cluster-health availability projection so proven-unreachable connectivity
    // also marks the node Offline. The no-op probe keeps this scheduling read
    // local and bounded. Unknown nodes without a healthy route are parked, not
    // discarded; network maintenance owns connectivity recovery and later DFS
    // maintenance republishes the durable work.
    let connectivity = ClusterConnectivityGraph::read_for_local(cluster_paths, state);
    sinomo_core::cluster::cluster_health_rows_with_probe(
        cluster_paths,
        state,
        &DaemonDfsConnectivityStatusProbe,
        false,
        time::OffsetDateTime::now_utc(),
    )
    .unwrap_or_default()
    .into_iter()
    .filter_map(|row| {
        daemon_dfs_node_is_offline_for_maintenance(
            &row.availability,
            &connectivity,
            &state.local_node_id,
            &row.member.node_id,
        )
        .then_some(row.member.node_id)
    })
    .collect()
}

fn daemon_dfs_maintenance_execution_exclusions(
    cluster_paths: &ClusterPaths,
    state: &sinomo_core::ClusterBootstrapState,
) -> DfsMaintenanceExecutionExclusions {
    DfsMaintenanceExecutionExclusions::new(daemon_dfs_offline_node_ids(cluster_paths, state))
}

#[derive(Clone, Debug)]
#[cfg(test)]
enum DaemonDfsMetadataLookup<T> {
    Found(T),
    Missing,
    Unavailable,
}

#[cfg(test)]
trait DaemonDfsMetadataLookupSource {
    fn object_lookup(&self, object_id: &DfsObjectId) -> DaemonDfsMetadataLookup<DfsObjectMetadata>;

    fn version_lookup(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> DaemonDfsMetadataLookup<DfsObjectVersionRecord>;
}

#[cfg(test)]
impl<T> DaemonDfsMetadataLookupSource for T
where
    T: DfsMetadataBackend + ?Sized,
{
    fn object_lookup(&self, object_id: &DfsObjectId) -> DaemonDfsMetadataLookup<DfsObjectMetadata> {
        match self.get_object(object_id) {
            Ok(Some(object)) => DaemonDfsMetadataLookup::Found(object),
            Ok(None) => DaemonDfsMetadataLookup::Missing,
            Err(_) => DaemonDfsMetadataLookup::Unavailable,
        }
    }

    fn version_lookup(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> DaemonDfsMetadataLookup<DfsObjectVersionRecord> {
        match self.get_version(object_id, version) {
            Ok(Some(version)) => DaemonDfsMetadataLookup::Found(version),
            Ok(None) => DaemonDfsMetadataLookup::Missing,
            Err(_) => DaemonDfsMetadataLookup::Unavailable,
        }
    }
}

#[derive(Debug, Default)]
#[cfg(test)]
struct DaemonDfsMetadataLookups {
    objects: BTreeMap<DfsObjectId, DaemonDfsMetadataLookup<DfsObjectMetadata>>,
    versions: BTreeMap<(DfsObjectId, DfsVersion), DaemonDfsMetadataLookup<DfsObjectVersionRecord>>,
}

#[cfg(test)]
impl DaemonDfsMetadataLookupSource for DaemonDfsMetadataLookups {
    fn object_lookup(&self, object_id: &DfsObjectId) -> DaemonDfsMetadataLookup<DfsObjectMetadata> {
        self.objects
            .get(object_id)
            .cloned()
            .unwrap_or(DaemonDfsMetadataLookup::Unavailable)
    }

    fn version_lookup(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> DaemonDfsMetadataLookup<DfsObjectVersionRecord> {
        self.versions
            .get(&(object_id.clone(), version.clone()))
            .cloned()
            .unwrap_or(DaemonDfsMetadataLookup::Unavailable)
    }
}

#[cfg(test)]
fn daemon_dfs_intents_without_recovery_source<M>(
    data_dir: &SinomoDataDir,
    metadata: &M,
    write_intents: &[DfsWriteIntentRecord],
    receipt_records: &[DfsReplicaReceiptRecord],
    offline_node_ids: &BTreeSet<NodeId>,
    local_node_id: &NodeId,
) -> BTreeSet<String>
where
    M: DaemonDfsMetadataLookupSource + ?Sized,
{
    if !write_intents.iter().any(|intent| {
        matches!(
            intent.status,
            DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
        )
    }) {
        return BTreeSet::new();
    }
    let content = LocalContentStore::open_for_inspection(data_dir.dfs_dir().join("content"));
    let mut receipts_by_version = BTreeMap::<
        (
            &DfsFileSystemId,
            &sinomo_dfs::DfsObjectId,
            &sinomo_dfs::DfsVersion,
        ),
        Vec<&DfsReplicaReceiptRecord>,
    >::new();
    for record in receipt_records {
        receipts_by_version
            .entry((
                &record.receipt.file_system_id,
                &record.receipt.object_id,
                &record.receipt.committed_version,
            ))
            .or_default()
            .push(record);
    }
    write_intents
        .iter()
        .filter(|intent| {
            matches!(
                intent.status,
                DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
            )
        })
        .filter(|intent| {
            let is_invisible_create = match metadata.object_lookup(&intent.object_id) {
                DaemonDfsMetadataLookup::Found(object)
                    if object.file_system_id == intent.file_system_id
                        && object.version == intent.target_version =>
                {
                    // The target version is already visible. Hydration either
                    // marks the stale intent committed or reconciles an exact
                    // hash disagreement; neither path needs a content source.
                    return false;
                }
                DaemonDfsMetadataLookup::Found(object)
                    if object.file_system_id == intent.file_system_id =>
                {
                    false
                }
                DaemonDfsMetadataLookup::Found(_) => return false,
                DaemonDfsMetadataLookup::Missing => true,
                // Storage inspection is advisory scheduling state. Fail open
                // rather than hiding work behind an ambiguous metadata error.
                DaemonDfsMetadataLookup::Unavailable => return false,
            };
            let Some(content_hash) = intent.content_hash.as_ref() else {
                // Hydration cannot reconstruct a pending write without either
                // an already-visible exact target (handled above) or a content
                // identity. Keep the durable intent for periodic reconciliation
                // without selecting the short maintenance cadence.
                return true;
            };
            let has_local_storage_candidate = match content.object_version_storage_candidate_exists(
                intent.object_id.as_str(),
                intent.target_version.as_str(),
                content_hash,
            ) {
                Ok(available) => available,
                // Invalid metadata or any local I/O ambiguity must keep the
                // normal fail-open maintenance cadence.
                Err(_) => return false,
            };
            let has_durable_size_hint =
                match metadata.version_lookup(&intent.object_id, &intent.target_version) {
                    DaemonDfsMetadataLookup::Found(version)
                        if version.content_hash.as_ref() == Some(content_hash)
                            && version.size_bytes.is_some() =>
                    {
                        true
                    }
                    DaemonDfsMetadataLookup::Found(_) | DaemonDfsMetadataLookup::Missing => false,
                    DaemonDfsMetadataLookup::Unavailable => return false,
                };
            let matching_receipt_node_ids = receipts_by_version
                .get(&(
                    &intent.file_system_id,
                    &intent.object_id,
                    &intent.target_version,
                ))
                .into_iter()
                .flatten()
                .filter(|record| {
                    record.content_hash.as_ref() == Some(content_hash)
                        && intent
                            .desired_replica_node_ids
                            .contains(&record.replica_node_id)
                })
                .map(|record| &record.replica_node_id)
                .collect::<BTreeSet<_>>();
            let commit_ready = matching_receipt_node_ids.len() >= intent.policy.min_commit_replicas;
            if is_invisible_create {
                let has_valid_object_metadata =
                    intent.object_metadata.as_ref().is_some_and(|object| {
                        object.object_id == intent.object_id
                            && object.file_system_id == intent.file_system_id
                    });
                let has_local_manifest_size_hint = match content
                    .read_manifest(intent.object_id.as_str(), intent.target_version.as_str())
                {
                    Ok(manifest)
                        if manifest.object_id == intent.object_id.as_str()
                            && manifest.version == intent.target_version.as_str()
                            && manifest.content_hash == *content_hash =>
                    {
                        true
                    }
                    Ok(_) => return false,
                    Err(ContentStoreError::Io { source, .. })
                        if source.kind() == std::io::ErrorKind::NotFound =>
                    {
                        false
                    }
                    Err(_) => return false,
                };
                // Hydration deliberately skips an invisible create until it
                // has all three ingredients needed by commit promotion: the
                // durable create metadata, receipt quorum, and an exact size
                // hint backed by a version record or matching manifest. A raw
                // whole-content blob without a manifest is not a size hint.
                return !(has_valid_object_metadata
                    && commit_ready
                    && (has_durable_size_hint || has_local_manifest_size_hint));
            }
            if has_local_storage_candidate || commit_ready {
                // A visible base object lets hydration reconstruct the pending
                // update with a zero size fallback. Local content can continue
                // replica staging below quorum; receipt quorum can commit even
                // when every receipt holder is currently offline.
                return false;
            }
            if matching_receipt_node_ids
                .iter()
                .any(|node_id| *node_id != local_node_id && !offline_node_ids.contains(*node_id))
            {
                // A visible base object lets hydration reconstruct the pending
                // update before repair selects this reachable exact-version
                // source. Invisible creates returned above because hydration
                // cannot build that pending write without a local size hint.
                return false;
            }
            true
        })
        .map(|intent| intent.intent_id.clone())
        .collect()
}

#[cfg(test)]
fn daemon_dfs_repairs_without_recovery_source<M>(
    data_dir: &SinomoDataDir,
    metadata: &M,
    repair_views: &[DfsOperatorRepairView],
    receipt_records: &[DfsReplicaReceiptRecord],
    offline_node_ids: &BTreeSet<NodeId>,
    local_node_id: &NodeId,
) -> BTreeSet<(String, NodeId)>
where
    M: DaemonDfsMetadataLookupSource + ?Sized,
{
    if repair_views.is_empty() {
        return BTreeSet::new();
    }
    let content = LocalContentStore::open_for_inspection(data_dir.dfs_dir().join("content"));
    let mut receipts_by_version = BTreeMap::<
        (
            &DfsFileSystemId,
            &sinomo_dfs::DfsObjectId,
            &sinomo_dfs::DfsVersion,
        ),
        Vec<&DfsReplicaReceiptRecord>,
    >::new();
    for record in receipt_records {
        receipts_by_version
            .entry((
                &record.receipt.file_system_id,
                &record.receipt.object_id,
                &record.receipt.committed_version,
            ))
            .or_default()
            .push(record);
    }
    repair_views
        .iter()
        .filter(|repair| {
            let Some(content_hash) = repair.content_hash.as_ref() else {
                return false;
            };
            match content.object_version_storage_candidate_exists(
                repair.object_id.as_str(),
                repair.version.as_str(),
                content_hash,
            ) {
                Ok(true) => return false,
                Ok(false) => {}
                // Advisory source inspection must remain fail-open.
                Err(_) => return false,
            }
            let has_reachable_receipt = receipts_by_version
                .get(&(&repair.file_system_id, &repair.object_id, &repair.version))
                .into_iter()
                .flatten()
                .any(|record| {
                    record.content_hash.as_ref() == Some(content_hash)
                        && &record.replica_node_id != local_node_id
                        && record.replica_node_id != repair.missing_replica_node_id
                        && !offline_node_ids.contains(&record.replica_node_id)
                });
            if has_reachable_receipt {
                return false;
            }
            match metadata.object_lookup(&repair.object_id) {
                DaemonDfsMetadataLookup::Found(object)
                    if object.file_system_id == repair.file_system_id
                        && object.version == repair.version
                        && object.content_hash.as_ref() == Some(content_hash) =>
                {
                    !object.current_locations.iter().any(|node_id| {
                        node_id != local_node_id
                            && node_id != &repair.missing_replica_node_id
                            && !offline_node_ids.contains(node_id)
                    })
                }
                // The repair view may have become stale between the operator
                // scan and this lookup. Let maintenance reconcile/remove it
                // instead of classifying the stale record as source-less.
                DaemonDfsMetadataLookup::Found(_) | DaemonDfsMetadataLookup::Missing => false,
                DaemonDfsMetadataLookup::Unavailable => false,
            }
        })
        .map(|repair| {
            (
                repair.intent_id.clone(),
                repair.missing_replica_node_id.clone(),
            )
        })
        .collect()
}

#[cfg(test)]
fn daemon_dfs_actionable_conflict_count(
    metadata: &dyn DfsMetadataBackend,
    conflicts: &[DfsConflictRecord],
) -> Result<usize, sinomo_dfs::MetadataBackendError> {
    let mut actionable = 0usize;
    for conflict in conflicts
        .iter()
        .filter(|conflict| conflict.status == DfsConflictStatus::Open)
    {
        let current = metadata.get_object(&conflict.object_id)?;
        let requires_maintenance =
            daemon_dfs_conflict_requires_maintenance(conflict, current.as_ref());
        actionable = actionable.saturating_add(usize::from(requires_maintenance));
    }
    Ok(actionable)
}

#[cfg(test)]
fn daemon_dfs_conflict_requires_maintenance(
    conflict: &DfsConflictRecord,
    current: Option<&DfsObjectMetadata>,
) -> bool {
    match current {
        // These are the exact cases that
        // `reconcile_stale_conflicts_bounded` can advance: a deleted object
        // supersedes the conflict, either branch becoming visible resolves
        // it, and any version beyond the recorded base supersedes it. A
        // conflict whose base remains visible is an intentional
        // operator-facing conflict and must not select the short background
        // cadence forever.
        None => true,
        Some(object)
            if object.version == conflict.local_version
                || object.version == conflict.remote_version =>
        {
            true
        }
        Some(object)
            if conflict
                .base_version
                .as_ref()
                .is_some_and(|base_version| object.version == *base_version) =>
        {
            false
        }
        Some(_) => true,
    }
}

fn daemon_dfs_actionable_placement_reconciliation_count(
    jobs: &[DfsPlacementReconciliationJob],
    pending_repair_count: usize,
) -> usize {
    jobs.iter()
        .filter(|job| match job.state {
            DfsPlacementReconciliationState::Pending
            | DfsPlacementReconciliationState::Scanning => true,
            // A Repairing job waiting on durable repairs is driven by the
            // reachability/backoff-aware repair classifier. A freshly
            // transitioned job (zero pending) and a paged scan must continue
            // even when unrelated repairs exist; once this job finishes a
            // pass with pending repairs and no cursor, it can park until the
            // repair summary drains and one final pass CASes it Completed.
            DfsPlacementReconciliationState::Repairing => {
                job.cursor.is_some() || job.pending_repair_count == 0 || pending_repair_count == 0
            }
            DfsPlacementReconciliationState::Completed
            | DfsPlacementReconciliationState::Failed => false,
        })
        .count()
}

#[cfg(test)]
fn daemon_dfs_pending_work_from_records(
    pending_write_count: usize,
    pending_repair_count: usize,
    pending_namespace_job_count: usize,
    actionable_conflict_count: usize,
    actionable_placement_reconciliation_count: usize,
    write_intents: &[DfsWriteIntentRecord],
    receipt_records: &[DfsReplicaReceiptRecord],
    metadata_shard_statuses: &[DfsMetadataShardStatus],
    repair_views: &[DfsOperatorRepairView],
    repair_attempt_records: &[DfsRepairAttemptRecord],
    intents_without_recovery_source: &BTreeSet<String>,
    repairs_without_recovery_source: &BTreeSet<(String, NodeId)>,
    offline_node_ids: &BTreeSet<NodeId>,
    local_node_id: &NodeId,
    pending_local_membership_handoff_count: usize,
    now_unix_nanos: u64,
) -> DaemonDfsPendingMaintenanceWork {
    // Keep offline repair state durable, but do not let it select the short
    // triggered-maintenance cadence. Reachability changes notify the scheduler,
    // while the deferred-work projection retains a bounded read-only re-probe.
    let mut receipts_by_version = BTreeMap::<
        (
            &DfsFileSystemId,
            &sinomo_dfs::DfsObjectId,
            &sinomo_dfs::DfsVersion,
        ),
        Vec<&DfsReplicaReceiptRecord>,
    >::new();
    for record in receipt_records {
        receipts_by_version
            .entry((
                &record.receipt.file_system_id,
                &record.receipt.object_id,
                &record.receipt.committed_version,
            ))
            .or_default()
            .push(record);
    }
    let active_write_intents = write_intents
        .iter()
        .filter(|intent| {
            matches!(
                &intent.status,
                DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
            )
        })
        .collect::<Vec<_>>();
    let stale_write_intent_min_age_nanos =
        daemon_dfs_duration_nanos(sinomo_dfs::DEFAULT_DFS_STALE_WRITE_INTENT_MIN_AGE);
    let stale_write_intent_deadlines = active_write_intents
        .iter()
        .filter(|intent| intents_without_recovery_source.contains(&intent.intent_id))
        .map(|intent| {
            intent
                .updated_at
                .unwrap_or(intent.created_at)
                .unix_timestamp_nanos()
                .try_into()
                .unwrap_or(0_u64)
                .saturating_add(stale_write_intent_min_age_nanos)
        })
        .collect::<Vec<_>>();
    let projected_stale_write_intent_count = stale_write_intent_deadlines.len();
    let due_stale_write_intent_count = stale_write_intent_deadlines
        .iter()
        .filter(|deadline| **deadline <= now_unix_nanos)
        .count();
    let next_stale_write_intent_deadline_unix_nanos = stale_write_intent_deadlines
        .into_iter()
        .filter(|deadline| *deadline > now_unix_nanos)
        .min();
    let eligible_pending_write_count = active_write_intents
        .iter()
        .filter(|intent| {
            if intents_without_recovery_source.contains(&intent.intent_id) {
                // A fresh abandoned intent has no executable short-cadence
                // action when its object, receipts, and readable hash blob are
                // all absent. Keep it durable until its exact stale-intent
                // deadline or a state-change notification makes it actionable.
                return false;
            }
            let desired_replica_node_ids = intent
                .desired_replica_node_ids
                .iter()
                .collect::<BTreeSet<_>>();
            if desired_replica_node_ids.is_empty() {
                return true;
            }
            let receipt_nodes = receipts_by_version
                .get(&(
                    &intent.file_system_id,
                    &intent.object_id,
                    &intent.target_version,
                ))
                .into_iter()
                .flatten()
                .filter(|record| {
                    intent
                        .content_hash
                        .as_ref()
                        .is_none_or(|hash| record.content_hash.as_ref() == Some(hash))
                })
                .map(|record| &record.replica_node_id)
                .collect::<BTreeSet<_>>();
            let received_replica_count = desired_replica_node_ids
                .iter()
                .filter(|node_id| receipt_nodes.contains(*node_id))
                .count();
            if received_replica_count >= intent.policy.min_commit_replicas {
                // The intent can make progress locally even if every remaining
                // desired replica is offline.
                return true;
            }
            let missing_replica_node_ids = desired_replica_node_ids
                .iter()
                .filter(|node_id| !receipt_nodes.contains(*node_id))
                .collect::<Vec<_>>();
            missing_replica_node_ids.is_empty()
                || missing_replica_node_ids
                    .iter()
                    .any(|node_id| !offline_node_ids.contains(**node_id))
        })
        .count()
        // If the summary and full scan ever disagree, retain the old fail-open
        // behavior rather than hiding work whose target cannot be classified.
        .saturating_add(pending_write_count.saturating_sub(active_write_intents.len()));

    let mut eligible_repair_keys = BTreeSet::new();
    for repair in repair_views {
        if !offline_node_ids.contains(&repair.missing_replica_node_id)
            && !repairs_without_recovery_source.contains(&(
                repair.intent_id.clone(),
                repair.missing_replica_node_id.clone(),
            ))
        {
            eligible_repair_keys
                .insert((repair.intent_id.as_str(), &repair.missing_replica_node_id));
        }
    }

    let eligible_repair_attempt_records = repair_attempt_records
        .iter()
        .filter(|record| {
            eligible_repair_keys
                .contains(&(record.intent_id.as_str(), &record.missing_replica_node_id))
        })
        .collect::<Vec<_>>();
    let attempted_eligible_repair_keys = eligible_repair_attempt_records
        .iter()
        .map(|record| (record.intent_id.as_str(), &record.missing_replica_node_id))
        .collect::<BTreeSet<_>>();
    let due_repair_attempt_count = eligible_repair_attempt_records
        .iter()
        .filter(|record| {
            record.next_retry_unix_nanos != u64::MAX
                && record.next_retry_unix_nanos <= now_unix_nanos
        })
        .count();
    let next_repair_retry_unix_nanos = eligible_repair_attempt_records
        .iter()
        .filter(|record| record.next_retry_unix_nanos != u64::MAX)
        .filter_map(|record| {
            (record.next_retry_unix_nanos > now_unix_nanos).then_some(record.next_retry_unix_nanos)
        })
        .min();
    let unattempted_repair_count = eligible_repair_keys
        .difference(&attempted_eligible_repair_keys)
        .count()
        // Preserve fail-open triggering if operator inspection returns fewer
        // repair records than the metadata summary counted.
        .saturating_add(pending_repair_count.saturating_sub(repair_views.len()));
    let metadata_replica_lag_count = metadata_shard_statuses
        .iter()
        .filter(|status| status.primary_node_id.as_ref() == Some(local_node_id))
        .flat_map(|status| status.replica_lag.iter())
        .filter(|lag| lag.lag_entries > 0 && !offline_node_ids.contains(&lag.target_node_id))
        .count();
    let all_metadata_replica_lag_count = metadata_shard_statuses
        .iter()
        .filter(|status| status.primary_node_id.as_ref() == Some(local_node_id))
        .flat_map(|status| status.replica_lag.iter())
        .filter(|lag| lag.lag_entries > 0)
        .count();
    let finite_future_repair_attempt_count = eligible_repair_attempt_records
        .iter()
        .filter(|record| {
            record.next_retry_unix_nanos != u64::MAX
                && record.next_retry_unix_nanos > now_unix_nanos
        })
        .count();
    let deferred_work_count = pending_write_count
        .saturating_sub(eligible_pending_write_count)
        .saturating_sub(projected_stale_write_intent_count)
        .saturating_add(
            pending_repair_count.saturating_sub(
                due_repair_attempt_count
                    .saturating_add(unattempted_repair_count)
                    .saturating_add(finite_future_repair_attempt_count),
            ),
        )
        .saturating_add(all_metadata_replica_lag_count.saturating_sub(metadata_replica_lag_count));

    DaemonDfsPendingMaintenanceWork {
        pending_write_count: eligible_pending_write_count,
        pending_repair_count,
        repair_attempt_count: repair_attempt_records.len(),
        due_repair_attempt_count,
        unattempted_repair_count,
        next_repair_retry_unix_nanos,
        due_time_based_maintenance_count: due_stale_write_intent_count,
        next_time_based_maintenance_unix_nanos: next_stale_write_intent_deadline_unix_nanos,
        deferred_work_count,
        pending_namespace_job_count,
        actionable_conflict_count,
        actionable_placement_reconciliation_count,
        metadata_replica_lag_count,
        pending_local_membership_handoff_count,
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
struct DaemonDfsContentGcProjection {
    expected_shard_count: usize,
    covered_shard_count: usize,
    catalog_journal_lag: u64,
    blocked_reasons: Vec<DfsContentRootCatalogIncompleteReason>,
    scrub_blocked_issue_count: usize,
    version_candidate_count: usize,
    orphan_candidate_count: usize,
    due_maintenance_count: usize,
    reservation_due_maintenance_count: usize,
    next_deadline_unix_nanos: Option<u64>,
}

#[cfg(test)]
fn daemon_dfs_version_gc_candidate_count(
    report: &sinomo_dfs::ContentStoreVersionGcReport,
) -> usize {
    usize::try_from(
        report
            .newly_observed_absent_version_count
            .saturating_add(report.deletion_ready_version_count)
            .saturating_add(report.retained_same_or_stale_catalog_version_count),
    )
    .unwrap_or(usize::MAX)
}

#[cfg(test)]
fn daemon_dfs_orphan_gc_candidate_count(report: &sinomo_dfs::ContentStoreGcReport) -> usize {
    usize::try_from(
        report
            .newly_observed_orphan_count
            .saturating_add(report.eligible_orphan_count)
            .saturating_add(report.stale_orphan_observation_count)
            .saturating_add(report.retained_same_or_stale_catalog_orphan_count)
            .saturating_add(report.eligible_packed_segment_count),
    )
    .unwrap_or(usize::MAX)
}

#[cfg(test)]
fn daemon_dfs_content_gc_blocked_reason_requires_refresh(
    reason: &DfsContentRootCatalogIncompleteReason,
) -> bool {
    match reason {
        // These describe topology which cannot produce complete coverage yet.
        // Cluster/topology and handoff notifications re-probe them, while the
        // catalog deadline below remains a bounded safety wake.
        DfsContentRootCatalogIncompleteReason::NoFileSystemsConfigured
        | DfsContentRootCatalogIncompleteReason::NoMetadataShardsConfigured
        | DfsContentRootCatalogIncompleteReason::FileSystemWithoutMetadataShards { .. }
        | DfsContentRootCatalogIncompleteReason::MembershipHandoffFrozen { .. }
        | DfsContentRootCatalogIncompleteReason::MembershipHandoffInProgress { .. }
        | DfsContentRootCatalogIncompleteReason::PageBudgetExceeded { .. }
        | DfsContentRootCatalogIncompleteReason::UnsupportedCatalogFormat { .. } => false,
        DfsContentRootCatalogIncompleteReason::ShardIncomplete { reasons, .. } => reasons
            .iter()
            .any(daemon_dfs_content_gc_blocked_reason_requires_refresh),
        DfsContentRootCatalogIncompleteReason::NotAuthoritativePrimary { .. }
        | DfsContentRootCatalogIncompleteReason::UnknownShard
        | DfsContentRootCatalogIncompleteReason::AuthorityChanged { .. }
        | DfsContentRootCatalogIncompleteReason::TopologyChanged { .. }
        | DfsContentRootCatalogIncompleteReason::DurableCutUnavailable { .. }
        | DfsContentRootCatalogIncompleteReason::StateFingerprintChanged
        | DfsContentRootCatalogIncompleteReason::MetadataBackendUnavailable
        | DfsContentRootCatalogIncompleteReason::TransportUnavailable { .. }
        | DfsContentRootCatalogIncompleteReason::MissingFileSystem { .. }
        | DfsContentRootCatalogIncompleteReason::UnexpectedFileSystem { .. }
        | DfsContentRootCatalogIncompleteReason::CatalogRefreshFailed { .. }
        | DfsContentRootCatalogIncompleteReason::CatalogNotDurablyInstalled
        | DfsContentRootCatalogIncompleteReason::MissingShard { .. }
        | DfsContentRootCatalogIncompleteReason::UnexpectedShard { .. }
        | DfsContentRootCatalogIncompleteReason::DuplicateShard { .. }
        | DfsContentRootCatalogIncompleteReason::CatalogExpired { .. }
        | DfsContentRootCatalogIncompleteReason::CatalogTimestampInFuture { .. }
        | DfsContentRootCatalogIncompleteReason::HeadWatermarkUnavailable { .. }
        | DfsContentRootCatalogIncompleteReason::HeadWatermarkIdentityMismatch { .. }
        | DfsContentRootCatalogIncompleteReason::CatalogBehindHeadWatermark { .. }
        | DfsContentRootCatalogIncompleteReason::HeadWatermarkFingerprintChanged { .. }
        | DfsContentRootCatalogIncompleteReason::MalformedCatalog { .. } => true,
    }
}

#[cfg(test)]
fn daemon_dfs_content_gc_blocked_reason_is_ordinary_head_advance(
    reason: &DfsContentRootCatalogIncompleteReason,
) -> bool {
    match reason {
        DfsContentRootCatalogIncompleteReason::CatalogBehindHeadWatermark { .. } => true,
        DfsContentRootCatalogIncompleteReason::ShardIncomplete { reasons, .. } => {
            !reasons.is_empty()
                && reasons
                    .iter()
                    .all(daemon_dfs_content_gc_blocked_reason_is_ordinary_head_advance)
        }
        _ => false,
    }
}

#[cfg(test)]
fn daemon_dfs_content_gc_projection(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    content: &LocalContentStore,
    reservations: &sinomo_dfs::ContentStoreReservationRoots,
    lifecycle_options: DfsMaintenanceRunOptions,
    now_unix_nanos: u64,
) -> Result<DaemonDfsContentGcProjection, DaemonError> {
    let expected_file_systems = cluster_dfs_all_file_system_configs(cluster_paths)?;
    let expected_shards = cluster_dfs_all_metadata_shards(cluster_paths)?;
    let pending_handoffs = cluster_dfs_all_metadata_shard_membership_handoffs(cluster_paths)?;
    let head_watermarks = cluster_dfs_all_metadata_shard_head_watermarks(cluster_paths)?;
    let catalog_work_expected = !expected_file_systems.is_empty()
        || !expected_shards.is_empty()
        || !pending_handoffs.is_empty();
    let catalog_store = default_dfs_content_root_catalog_store(data_dir.dfs_dir().join("content"));
    let catalog = match catalog_store.load() {
        Ok(catalog) => catalog,
        Err(error) => {
            return Ok(DaemonDfsContentGcProjection {
                expected_shard_count: expected_shards.len(),
                blocked_reasons: vec![
                    DfsContentRootCatalogIncompleteReason::CatalogRefreshFailed {
                        reason: format!("durable catalog is unreadable: {error}"),
                    },
                ],
                due_maintenance_count: usize::from(catalog_work_expected),
                ..DaemonDfsContentGcProjection::default()
            });
        }
    };
    let Some(catalog) = catalog else {
        return Ok(DaemonDfsContentGcProjection {
            expected_shard_count: expected_shards.len(),
            blocked_reasons: vec![if expected_file_systems.is_empty() {
                DfsContentRootCatalogIncompleteReason::NoFileSystemsConfigured
            } else if expected_shards.is_empty() {
                DfsContentRootCatalogIncompleteReason::NoMetadataShardsConfigured
            } else {
                DfsContentRootCatalogIncompleteReason::CatalogNotDurablyInstalled
            }],
            due_maintenance_count: usize::from(catalog_work_expected),
            ..DaemonDfsContentGcProjection::default()
        });
    };

    let validation = match catalog.validate_for_collection_with_head_watermark_policy(
        &expected_file_systems,
        &expected_shards,
        &pending_handoffs,
        &head_watermarks,
        false,
        now_unix_nanos,
        lifecycle_options.content_root_catalog_max_age,
    ) {
        Ok(validation) => validation,
        Err(error) => DfsContentRootCatalogCollectionValidation::incomplete_for_expected_shards(
            vec![DfsContentRootCatalogIncompleteReason::MalformedCatalog {
                reason: error.to_string(),
            }],
            expected_shards.len(),
        ),
    };
    let max_age_nanos = daemon_dfs_duration_nanos(lifecycle_options.content_root_catalog_max_age);
    let catalog_refresh_deadline = catalog
        .generated_at_unix_nanos()
        .saturating_add(max_age_nanos);
    let mut projection = DaemonDfsContentGcProjection {
        expected_shard_count: validation.expected_shard_count(),
        covered_shard_count: validation.covered_shard_count(),
        catalog_journal_lag: validation.max_journal_lag_entries(),
        blocked_reasons: validation.reasons().to_vec(),
        ..DaemonDfsContentGcProjection::default()
    };
    if !validation.is_collection_authorized() {
        let only_ordinary_head_advance = !validation.reasons().is_empty()
            && validation
                .reasons()
                .iter()
                .all(daemon_dfs_content_gc_blocked_reason_is_ordinary_head_advance);
        if only_ordinary_head_advance && catalog_refresh_deadline > now_unix_nanos {
            let catalog_versions = catalog
                .protected_version_pairs()
                .expect("a behind-head catalog with otherwise complete coverage has version roots");
            projection.reservation_due_maintenance_count = reservations
                .reservations
                .iter()
                .filter(|reservation| {
                    catalog_versions.contains(&(
                        reservation.identity.object_id.clone(),
                        reservation.identity.version.clone(),
                    ))
                })
                .count();
            // A newer quorum head is normal during sustained writes. The old
            // catalog cannot authorize deletion, but durable reservations
            // protect every post-catalog publication. Historical positive
            // coverage remains sufficient to release exact covered
            // reservations; it can never reject an uncovered reservation or
            // authorize deletion. Coalesce only the destructive refresh to
            // the existing bounded expiry deadline.
            projection.next_deadline_unix_nanos = Some(catalog_refresh_deadline);
            return Ok(projection);
        }
        let repairable_block = validation
            .reasons()
            .iter()
            .any(daemon_dfs_content_gc_blocked_reason_requires_refresh);
        if repairable_block || catalog_refresh_deadline <= now_unix_nanos {
            projection.due_maintenance_count = projection.due_maintenance_count.saturating_add(1);
        } else {
            // Structural coverage gaps and active handoffs remain parked on
            // their state-change notifications, with catalog expiry as a
            // bounded fallback wake.
            projection.next_deadline_unix_nanos = Some(catalog_refresh_deadline);
        }
        return Ok(projection);
    }
    let protection = validation
        .protection()
        .expect("authorized content-root catalog validation has protection");
    let catalog_generation = validation
        .catalog_generation()
        .expect("authorized content-root catalog validation has a generation");
    let catalog_installed_at_unix_nanos = validation
        .catalog_installed_at_unix_nanos()
        .expect("authorized content-root catalog validation has a durable install observation");
    let version_projection = content.collect_versions(
        ContentStoreVersionGcOptions::dry_run(
            lifecycle_options.content_version_gc_min_age,
            lifecycle_options.content_version_gc_keep_latest_versions,
        )
        .with_max_deletions(lifecycle_options.content_version_gc_max_records)
        .with_min_absence_age(lifecycle_options.content_version_gc_min_age)
        .with_catalog_projection_observation(catalog_generation, catalog_installed_at_unix_nanos)
        .with_protected_versions(
            protection
                .versions
                .iter()
                .map(|identity| (identity.object_id.as_str(), identity.version.as_str())),
        ),
    )?;
    let orphan_projection = content.collect_garbage(
        ContentStoreGcOptions::dry_run(lifecycle_options.content_orphan_gc_min_age)
            .with_max_deletions(lifecycle_options.content_orphan_gc_max_records)
            .with_catalog_projection_observation(
                catalog_generation,
                catalog_installed_at_unix_nanos,
            )
            .with_protected_content_hashes(protection.content_hashes.iter().map(String::as_str)),
    )?;
    projection.scrub_blocked_issue_count = usize::try_from(
        version_projection
            .blocked_by_scrub_issue_count
            .max(orphan_projection.blocked_by_scrub_issue_count),
    )
    .unwrap_or(usize::MAX);
    projection.version_candidate_count = daemon_dfs_version_gc_candidate_count(&version_projection);
    projection.orphan_candidate_count = daemon_dfs_orphan_gc_candidate_count(&orphan_projection);
    projection.due_maintenance_count = projection
        .due_maintenance_count
        .saturating_add(projection.version_candidate_count)
        .saturating_add(projection.orphan_candidate_count);
    projection.next_deadline_unix_nanos = projection
        .next_deadline_unix_nanos
        .into_iter()
        .chain(std::iter::once(catalog_refresh_deadline))
        .chain(version_projection.next_eligible_at_unix_nanos)
        .chain(orphan_projection.next_eligible_at_unix_nanos)
        .min();

    for reservation in &reservations.reservations {
        let covered = protection
            .versions
            .binary_search_by(|identity| {
                identity
                    .object_id
                    .as_str()
                    .cmp(reservation.identity.object_id.as_str())
                    .then_with(|| {
                        identity
                            .version
                            .as_str()
                            .cmp(reservation.identity.version.as_str())
                    })
            })
            .is_ok();
        let rejection_deadline =
            reservation
                .reserved_at_unix_nanos
                .saturating_add(daemon_dfs_duration_nanos(
                    lifecycle_options.content_reservation_rejection_grace,
                ));
        if covered {
            projection.reservation_due_maintenance_count = projection
                .reservation_due_maintenance_count
                .saturating_add(1);
        } else if catalog_installed_at_unix_nanos > rejection_deadline
            || now_unix_nanos >= rejection_deadline
        {
            projection.due_maintenance_count = projection.due_maintenance_count.saturating_add(1);
        } else {
            projection.next_deadline_unix_nanos = projection
                .next_deadline_unix_nanos
                .into_iter()
                .chain(std::iter::once(rejection_deadline))
                .min();
        }
    }

    Ok(projection)
}

#[derive(Debug)]
#[cfg(test)]
struct DaemonDfsMaintenanceMetadataObservation {
    summary: DfsMetadataSummary,
    metadata_shard_statuses: Vec<DfsMetadataShardStatus>,
    write_intents: Vec<DfsWriteIntentRecord>,
    repair_views: Vec<DfsOperatorRepairView>,
    actionable_conflict_count: usize,
    receipt_records: Vec<DfsReplicaReceiptRecord>,
    lookups: DaemonDfsMetadataLookups,
}

#[cfg(test)]
fn daemon_dfs_maintenance_metadata_observation(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<DaemonDfsMaintenanceMetadataObservation, DaemonError> {
    sinomo_dfs::with_cached_cluster_aware_dfs_metadata_inspection(
        data_dir,
        cluster_paths,
        |metadata| {
            let summary = metadata.metadata_summary()?;
            let metadata_shard_statuses = metadata.metadata_shard_statuses()?;
            let write_intents = if summary.pending_write_count > 0 {
                metadata.all_intents()?
            } else {
                Vec::new()
            };
            let repair_views = if summary.pending_repair_count > 0 {
                DfsOperatorInspector::new(metadata).repairs(None, None)?
            } else {
                Vec::new()
            };
            let conflicts = if summary.open_conflict_count > 0 {
                metadata.all_conflicts()?
            } else {
                Vec::new()
            };
            let actionable_conflict_count =
                daemon_dfs_actionable_conflict_count(metadata, &conflicts)?;

            let mut receipt_records = Vec::new();
            let mut receipt_versions = BTreeSet::new();
            for intent in write_intents.iter().filter(|intent| {
                matches!(
                    &intent.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                )
            }) {
                if receipt_versions
                    .insert((intent.object_id.clone(), intent.target_version.clone()))
                {
                    receipt_records.extend(
                        metadata.receipts_for_version(&intent.object_id, &intent.target_version)?,
                    );
                }
            }
            for repair in &repair_views {
                if receipt_versions.insert((repair.object_id.clone(), repair.version.clone())) {
                    receipt_records
                        .extend(metadata.receipts_for_version(&repair.object_id, &repair.version)?);
                }
            }

            let mut object_ids = BTreeSet::new();
            let mut version_ids = BTreeSet::new();
            for intent in write_intents.iter().filter(|intent| {
                matches!(
                    &intent.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                )
            }) {
                object_ids.insert(intent.object_id.clone());
                version_ids.insert((intent.object_id.clone(), intent.target_version.clone()));
            }
            for repair in &repair_views {
                object_ids.insert(repair.object_id.clone());
            }
            let mut lookups = DaemonDfsMetadataLookups::default();
            for object_id in object_ids {
                let lookup = metadata.object_lookup(&object_id);
                lookups.objects.insert(object_id, lookup);
            }
            for (object_id, version) in version_ids {
                let lookup = metadata.version_lookup(&object_id, &version);
                lookups.versions.insert((object_id, version), lookup);
            }

            Ok(DaemonDfsMaintenanceMetadataObservation {
                summary,
                metadata_shard_statuses,
                write_intents,
                repair_views,
                actionable_conflict_count,
                receipt_records,
                lookups,
            })
        },
    )
    .map_err(Into::into)
}

/// Add newly committed repair identities to the bounded durable queue.
///
/// This runs at the mutation's committed edge, before its live service lane is
/// released. Existing retry/backoff state wins for an already queued target;
/// maintenance remains the sole owner of advancing or clearing that state.
/// The work is O(active repair targets) and never opens DFS metadata or content
/// inventory.
fn persist_daemon_dfs_repair_queue_records(
    data_dir: &SinomoDataDir,
    committed_records: Vec<DfsRepairAttemptRecord>,
) -> Result<(), sinomo_dfs::DfsError> {
    if committed_records.is_empty() {
        return Ok(());
    }
    let _guard = DAEMON_DFS_REPAIR_QUEUE_SNAPSHOT_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|_| sinomo_dfs::DfsError::Metadata {
            message: "daemon DFS repair queue snapshot lock poisoned".to_string(),
        })?;
    let path = default_dfs_repair_attempt_snapshot_path(data_dir);
    let existing_records = read_dfs_repair_attempt_snapshot(&path)?
        .map(|snapshot| snapshot.records)
        .unwrap_or_default();
    let mut records_by_target = BTreeMap::<(String, NodeId), DfsRepairAttemptRecord>::new();
    for record in existing_records {
        records_by_target.insert(
            (
                record.intent_id.clone(),
                record.missing_replica_node_id.clone(),
            ),
            record,
        );
    }
    for record in committed_records {
        records_by_target
            .entry((
                record.intent_id.clone(),
                record.missing_replica_node_id.clone(),
            ))
            .or_insert(record);
    }
    write_dfs_repair_attempt_snapshot(
        &path,
        &DfsRepairAttemptSnapshot {
            records: records_by_target.into_values().collect(),
        },
    )?;
    record_daemon_dfs_repair_queue_nonempty(data_dir, true);
    Ok(())
}

/// Inspect only the bounded durable queues owned by normal reactive healing.
///
/// This function is safe on daemon startup and notification handling: it does
/// not open a DFS metadata backend, enumerate object/shard indexes, or inspect
/// content. Full metadata reconstruction belongs to explicit disaster
/// recovery, not to the automatic scheduler.
pub(crate) fn daemon_dfs_pending_reactive_maintenance_work(
    paths: &DaemonPaths,
) -> Result<DaemonDfsPendingMaintenanceWork, DaemonError> {
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(DaemonDfsPendingMaintenanceWork::default());
    }
    let now_unix_nanos = daemon_dfs_now_unix_nanos();
    let repair_attempt_records =
        read_dfs_repair_attempt_snapshot(&default_dfs_repair_attempt_snapshot_path(&data_dir))?
            .map(|snapshot| snapshot.records)
            .unwrap_or_default();
    record_daemon_dfs_repair_queue_nonempty(&data_dir, !repair_attempt_records.is_empty());
    let due_repair_attempt_count = repair_attempt_records
        .iter()
        .filter(|record| {
            record.next_retry_unix_nanos != u64::MAX
                && record.next_retry_unix_nanos <= now_unix_nanos
        })
        .count();
    let next_repair_retry_unix_nanos = repair_attempt_records
        .iter()
        .filter(|record| {
            record.next_retry_unix_nanos != u64::MAX
                && record.next_retry_unix_nanos > now_unix_nanos
        })
        .map(|record| record.next_retry_unix_nanos)
        .min();
    let placement_reconciliation_jobs =
        sinomo_core::cluster::read_cluster_kv_latest_observed_prefix_entries(
            &cluster_paths,
            &sinomo_dfs::placement_reconciliations_prefix(&sinomo_dfs::sifs_file_system_id()),
        )?
        .into_iter()
        .map(|entry| entry.decode::<DfsPlacementReconciliationJob>())
        .collect::<Result<Vec<_>, _>>()?;
    let actionable_placement_reconciliation_count =
        daemon_dfs_actionable_placement_reconciliation_count(
            &placement_reconciliation_jobs,
            repair_attempt_records.len(),
        );

    Ok(DaemonDfsPendingMaintenanceWork {
        pending_repair_count: repair_attempt_records.len(),
        repair_attempt_count: repair_attempt_records.len(),
        due_repair_attempt_count,
        next_repair_retry_unix_nanos,
        actionable_placement_reconciliation_count,
        ..DaemonDfsPendingMaintenanceWork::default()
    })
}

/// Full metadata classifier retained for explicit recovery/operator tests.
///
/// Unlike [`daemon_dfs_pending_reactive_maintenance_work`], this opens the
/// cluster-aware metadata image and is therefore O(total metadata). Production
/// daemon scheduling must never call it.
#[cfg(test)]
pub(crate) fn daemon_dfs_pending_maintenance_work(
    paths: &DaemonPaths,
) -> Result<DaemonDfsPendingMaintenanceWork, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(DaemonDfsPendingMaintenanceWork::default());
    }
    let now_unix_nanos = daemon_dfs_now_unix_nanos();
    // Routine scheduling observes only indexed metadata work queues and their
    // bounded retry records. Content-store inventory (reservations, manifests,
    // versions, blobs, and orphan observations) is deliberately excluded:
    // those O(total-content) scans belong to explicit disaster recovery, never
    // to daemon startup, notification handling, or an idle safety poll.
    let DaemonDfsMaintenanceMetadataObservation {
        summary,
        metadata_shard_statuses,
        write_intents,
        repair_views,
        actionable_conflict_count,
        receipt_records,
        lookups,
    } = daemon_dfs_maintenance_metadata_observation(&data_dir, &cluster_paths)?;
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .map_err(DaemonError::from)?;
    let pending_local_membership_handoff_count =
        daemon_dfs_local_membership_handoff_keys(&metadata_shard_statuses, &state.local_node_id)
            .into_iter()
            .try_fold(0usize, |count, key| {
                sinomo_core::cluster::read_cluster_kv_latest_observed_entry(&cluster_paths, &key)
                    .map(|entry| count.saturating_add(usize::from(entry.is_some())))
                    .map_err(DaemonError::from)
            })?;
    let repair_attempt_records =
        read_dfs_repair_attempt_snapshot(&default_dfs_repair_attempt_snapshot_path(&data_dir))?
            .map(|snapshot| snapshot.records)
            .unwrap_or_default();
    let placement_reconciliation_jobs =
        sinomo_core::cluster::read_cluster_kv_latest_observed_prefix_entries(
            &cluster_paths,
            &sinomo_dfs::placement_reconciliations_prefix(&sinomo_dfs::sifs_file_system_id()),
        )?
        .into_iter()
        .map(|entry| entry.decode::<DfsPlacementReconciliationJob>())
        .collect::<Result<Vec<_>, _>>()?;
    let actionable_placement_reconciliation_count =
        daemon_dfs_actionable_placement_reconciliation_count(
            &placement_reconciliation_jobs,
            summary.pending_repair_count,
        );
    let offline_node_ids = daemon_dfs_offline_node_ids(&cluster_paths, &state);
    let intents_without_recovery_source = daemon_dfs_intents_without_recovery_source(
        &data_dir,
        &lookups,
        &write_intents,
        &receipt_records,
        &offline_node_ids,
        &state.local_node_id,
    );
    let repairs_without_recovery_source = daemon_dfs_repairs_without_recovery_source(
        &data_dir,
        &lookups,
        &repair_views,
        &receipt_records,
        &offline_node_ids,
        &state.local_node_id,
    );
    let work = daemon_dfs_pending_work_from_records(
        summary.pending_write_count,
        summary.pending_repair_count,
        summary.pending_namespace_job_count,
        actionable_conflict_count,
        actionable_placement_reconciliation_count,
        &write_intents,
        &receipt_records,
        &metadata_shard_statuses,
        &repair_views,
        &repair_attempt_records,
        &intents_without_recovery_source,
        &repairs_without_recovery_source,
        &offline_node_ids,
        &state.local_node_id,
        pending_local_membership_handoff_count,
        now_unix_nanos,
    );
    Ok(work)
}

fn daemon_dfs_message_service_cache()
-> &'static Mutex<BTreeMap<PathBuf, DaemonDfsMessageServiceCacheEntry>> {
    DAEMON_DFS_MESSAGE_SERVICE_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
fn record_daemon_dfs_test_message_service_lane_open(data_dir: &SinomoDataDir) {
    let mut opens = DAEMON_DFS_TEST_MESSAGE_SERVICE_LANE_OPENS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("test message service lane open counts");
    *opens.entry(data_dir.root()).or_default() += 1;
}

#[cfg(test)]
fn daemon_dfs_test_message_service_lane_open_count(data_dir: &SinomoDataDir) -> usize {
    DAEMON_DFS_TEST_MESSAGE_SERVICE_LANE_OPENS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("test message service lane open counts")
        .get(&data_dir.root())
        .copied()
        .unwrap_or_default()
}

fn evict_daemon_dfs_message_service_cache(data_dir: &SinomoDataDir) -> Result<bool, DaemonError> {
    let cache_key = data_dir.root();
    let mut cache =
        daemon_dfs_message_service_cache()
            .lock()
            .map_err(|_| DaemonError::Network {
                address: "DFS message service cache".to_string(),
                detail: "DFS message service cache is poisoned".to_string(),
            })?;
    let removed = cache.remove(&cache_key).is_some();
    if removed {
        tracing::info!(
            data_root = %cache_key.display(),
            "released daemon DFS message service before mandatory lifecycle work"
        );
    }
    Ok(removed)
}

fn take_daemon_dfs_message_mutation_service_for_maintenance(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    cluster_state: &sinomo_core::ClusterBootstrapState,
    runtime: &tokio::runtime::Handle,
) -> Result<DaemonDfsMaintenanceServiceLane, sinomo_dfs::DfsError> {
    let peer_node_ids = sinomo_dfs::cluster_dfs_peer_node_ids(cluster_state);
    let metadata_authority_generation = daemon_dfs_metadata_authority_generation_for_identity(
        cluster_paths,
        &cluster_state.cluster_id,
        &cluster_state.local_node_id,
        Some(runtime),
    )?;
    if let Some(snapshot) =
        sinomo_core::cluster::read_cluster_kv_latest_observed_snapshot_arc_if_present(cluster_paths)
            .map_err(daemon_dfs_network_error)?
    {
        // The filesystem watcher is deliberately debounced. Maintenance has
        // just refreshed cluster KV synchronously, so observe that exact image
        // before deciding whether an existing backend generation is reusable.
        metadata_authority_generation.observe(snapshot.as_ref());
    }
    let signature = DaemonDfsMessageServiceSignature {
        cluster_id: cluster_state.cluster_id.clone(),
        local_node_id: cluster_state.local_node_id.clone(),
        kv_snapshot_available: cluster_paths.kv_paths().snapshot.exists(),
        metadata_authority_generation: metadata_authority_generation.generation(),
    };
    let cache_key = data_dir.root();
    let cached = {
        let mut cache = daemon_dfs_message_service_cache()
            .lock()
            .map_err(|_| daemon_dfs_cache_lock_error())?;
        let stale = cache.get(&cache_key).is_some_and(|entry| {
            entry.signature != signature
                || !Arc::ptr_eq(
                    &entry.metadata_authority_generation,
                    &metadata_authority_generation,
                )
        });
        if stale {
            drop(cache.remove(&cache_key));
        }
        let entry = cache
            .entry(cache_key)
            .or_insert_with(|| DaemonDfsMessageServiceCacheEntry {
                signature: signature.clone(),
                metadata_authority_generation: Arc::clone(&metadata_authority_generation),
                peer_node_ids: peer_node_ids.clone(),
                primary_service: None,
                lightweight_read_service: None,
                content_read_service: None,
            });
        entry.peer_node_ids.clone_from(&peer_node_ids);
        entry.primary_service.take()
    };

    let handle = match cached {
        Some(handle) => handle,
        None => {
            let content_store =
                sinomo_dfs::LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
            open_daemon_dfs_message_service_lane_with_content(
                data_dir,
                cluster_paths,
                &signature.local_node_id,
                peer_node_ids.clone(),
                runtime,
                content_store,
            )?
        }
    };
    let handle = Arc::try_unwrap(handle).map_err(|_| sinomo_dfs::DfsError::Network {
        message:
            "daemon DFS mutation lane remained referenced after exclusive maintenance admission"
                .to_string(),
    })?;
    let DaemonDfsMessageServiceLaneHandle {
        lane,
        detached_send_runtime,
    } = handle;
    let mut lane = lane
        .into_inner()
        .map_err(|_| daemon_dfs_cache_lock_error())?;
    lane.refresh_peer_targets(peer_node_ids.clone());
    Ok(DaemonDfsMaintenanceServiceLane {
        signature,
        metadata_authority_generation,
        peer_node_ids,
        lane,
        detached_send_runtime,
    })
}

fn restore_daemon_dfs_message_mutation_service_after_maintenance(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    runtime: &tokio::runtime::Handle,
    mut retained: DaemonDfsMaintenanceServiceLane,
) -> Result<bool, sinomo_dfs::DfsError> {
    let Some(peer_snapshot) = read_daemon_dfs_message_peer_snapshot(cluster_paths)? else {
        return Ok(false);
    };
    let metadata_authority_generation = daemon_dfs_metadata_authority_generation_for_identity(
        cluster_paths,
        &peer_snapshot.cluster_id,
        &peer_snapshot.local_node_id,
        Some(runtime),
    )?;
    if let Some(snapshot) =
        sinomo_core::cluster::read_cluster_kv_latest_observed_snapshot_arc_if_present(cluster_paths)
            .map_err(daemon_dfs_network_error)?
    {
        metadata_authority_generation.observe(snapshot.as_ref());
    }
    let current_signature = DaemonDfsMessageServiceSignature {
        cluster_id: peer_snapshot.cluster_id,
        local_node_id: peer_snapshot.local_node_id,
        kv_snapshot_available: peer_snapshot.kv_snapshot_available,
        metadata_authority_generation: metadata_authority_generation.generation(),
    };
    if current_signature != retained.signature
        || !Arc::ptr_eq(
            &metadata_authority_generation,
            &retained.metadata_authority_generation,
        )
    {
        return Ok(false);
    }
    if retained
        .lane
        .service
        .operational_metrics()?
        .authoritative_write_frozen_shard_count
        != 0
    {
        return Ok(false);
    }
    retained
        .lane
        .refresh_peer_targets(peer_snapshot.peer_node_ids.clone());
    retained
        .peer_node_ids
        .clone_from(&peer_snapshot.peer_node_ids);

    let cache_key = data_dir.root();
    let mut cache = daemon_dfs_message_service_cache()
        .lock()
        .map_err(|_| daemon_dfs_cache_lock_error())?;
    let Some(entry) = cache.get_mut(&cache_key) else {
        return Ok(false);
    };
    if entry.signature != retained.signature
        || !Arc::ptr_eq(
            &entry.metadata_authority_generation,
            &retained.metadata_authority_generation,
        )
        || entry.primary_service.is_some()
    {
        return Ok(false);
    }
    entry.peer_node_ids.clone_from(&retained.peer_node_ids);
    entry.primary_service = Some(Arc::new(DaemonDfsMessageServiceLaneHandle {
        lane: Mutex::new(retained.lane),
        detached_send_runtime: retained.detached_send_runtime,
    }));
    Ok(true)
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn release_daemon_dfs_idle_allocator_pages() {
    unsafe extern "C" {
        fn malloc_trim(pad: usize) -> std::ffi::c_int;
    }

    // Broad maintenance temporarily materializes all locally configured
    // metadata shards. glibc's per-thread arenas otherwise retain those freed
    // pages indefinitely, leaving an idle daemon with the peak RSS of the last
    // maintenance pass.
    let _ = unsafe { malloc_trim(0) };
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn release_daemon_dfs_idle_allocator_pages() {}

/// Release the process-global cached lane during main-daemon shutdown. Active
/// blocking handlers retain both their lane Arc and an explicit data-owner Arc,
/// so eviction drops only idle cache ownership and cannot shorten live work.
pub(crate) fn evict_daemon_dfs_message_service_cache_on_shutdown(
    paths: &DaemonPaths,
) -> Result<bool, DaemonError> {
    evict_daemon_dfs_message_service_cache(&SinomoDataDir::new(daemon_data_dir(paths)?))
}

fn daemon_dfs_mutation_scheduler(
    data_dir: &SinomoDataDir,
) -> Result<Arc<DaemonDfsMutationScheduler>, DaemonError> {
    let key = data_dir.root();
    let mut schedulers = DAEMON_DFS_MUTATION_SCHEDULERS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .map_err(|_| DaemonError::Network {
            address: "DFS mutation scheduler".to_string(),
            detail: "DFS mutation scheduler cache is poisoned".to_string(),
        })?;
    if let Some(scheduler) = schedulers.get(&key).and_then(Weak::upgrade) {
        return Ok(scheduler);
    }
    let scheduler = Arc::new(DaemonDfsMutationScheduler::new());
    schedulers.insert(key, Arc::downgrade(&scheduler));
    Ok(scheduler)
}

fn daemon_dfs_metadata_publish_lane_scheduler(
    data_dir: &SinomoDataDir,
    file_system_id: &DfsFileSystemId,
    shard_id: &DfsMetadataShardId,
) -> Result<Arc<sinomo_dfs::DfsMetadataPublishScheduler>, DaemonError> {
    let key = DaemonDfsMetadataPublishLaneKey {
        data_root: data_dir.root(),
        file_system_id: file_system_id.clone(),
        shard_id: shard_id.clone(),
    };
    let mut schedulers = DAEMON_DFS_METADATA_PUBLISH_SCHEDULERS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .map_err(|_| DaemonError::Network {
            address: "DFS metadata publish scheduler".to_string(),
            detail: "DFS metadata publish scheduler map is poisoned".to_string(),
        })?;
    if let Some(scheduler) = schedulers.get(&key).and_then(Weak::upgrade) {
        return Ok(scheduler);
    }
    // The registry is only an index for active lanes. The spawned publisher
    // owns the strong reference, so a completed lane disappears naturally and
    // cannot leak one closure per historical shard. Pruning here keeps the
    // index bounded by live work without aliasing unrelated shards onto a
    // shared fallback scheduler.
    schedulers.retain(|_, scheduler| scheduler.strong_count() > 0);
    let scheduler = Arc::new(sinomo_dfs::DfsMetadataPublishScheduler::new());
    schedulers.insert(key, Arc::downgrade(&scheduler));
    Ok(scheduler)
}

fn daemon_dfs_metadata_publish_discovery_scheduler(
    data_dir: &SinomoDataDir,
) -> Result<Arc<sinomo_dfs::DfsMetadataPublishScheduler>, DaemonError> {
    let key = data_dir.root();
    let mut schedulers = DAEMON_DFS_METADATA_PUBLISH_DISCOVERY_SCHEDULERS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .map_err(|_| DaemonError::Network {
            address: "DFS metadata publish discovery scheduler".to_string(),
            detail: "DFS metadata publish discovery scheduler map is poisoned".to_string(),
        })?;
    if let Some(scheduler) = schedulers.get(&key).and_then(Weak::upgrade) {
        return Ok(scheduler);
    }
    schedulers.retain(|_, scheduler| scheduler.strong_count() > 0);
    let scheduler = Arc::new(sinomo_dfs::DfsMetadataPublishScheduler::new());
    schedulers.insert(key, Arc::downgrade(&scheduler));
    Ok(scheduler)
}

#[cfg(test)]
fn record_daemon_dfs_metadata_publish_discovery_attempt(
    data_root: &std::path::Path,
    success: bool,
) {
    if let Ok(mut attempts) = DAEMON_DFS_TEST_METADATA_PUBLISH_DISCOVERY_ATTEMPTS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
    {
        let counts = attempts.entry(data_root.to_path_buf()).or_default();
        counts.0 = counts.0.saturating_add(1);
        if success {
            counts.1 = counts.1.saturating_add(1);
        }
    }
}

#[cfg(test)]
fn daemon_dfs_metadata_publish_discovery_attempts(data_root: &std::path::Path) -> (usize, usize) {
    DAEMON_DFS_TEST_METADATA_PUBLISH_DISCOVERY_ATTEMPTS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .ok()
        .and_then(|attempts| attempts.get(data_root).copied())
        .unwrap_or_default()
}

#[cfg(test)]
fn record_daemon_dfs_metadata_publish_lane_request(
    data_dir: &SinomoDataDir,
    lane: &DaemonDfsMutationLaneKey,
) {
    if let Ok(mut requests) = DAEMON_DFS_TEST_METADATA_PUBLISH_LANE_REQUESTS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
    {
        let key = DaemonDfsMetadataPublishLaneKey {
            data_root: data_dir.root(),
            file_system_id: lane.file_system_id.clone(),
            shard_id: lane.shard_id.clone(),
        };
        let count = requests.entry(key).or_default();
        *count = count.saturating_add(1);
    }
}

#[cfg(test)]
fn daemon_dfs_metadata_publish_lane_request_count(
    data_dir: &SinomoDataDir,
    lane: &DaemonDfsMutationLaneKey,
) -> usize {
    let key = DaemonDfsMetadataPublishLaneKey {
        data_root: data_dir.root(),
        file_system_id: lane.file_system_id.clone(),
        shard_id: lane.shard_id.clone(),
    };
    DAEMON_DFS_TEST_METADATA_PUBLISH_LANE_REQUESTS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .ok()
        .and_then(|requests| requests.get(&key).copied())
        .unwrap_or_default()
}

#[cfg(test)]
fn record_daemon_dfs_repair_maintenance_edge_request(data_dir: &SinomoDataDir) {
    if let Ok(mut requests) = DAEMON_DFS_TEST_REPAIR_MAINTENANCE_EDGE_REQUESTS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
    {
        let count = requests.entry(data_dir.root()).or_default();
        *count = count.saturating_add(1);
    }
}

#[cfg(test)]
fn daemon_dfs_repair_maintenance_edge_request_count(data_dir: &SinomoDataDir) -> usize {
    DAEMON_DFS_TEST_REPAIR_MAINTENANCE_EDGE_REQUESTS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .ok()
        .and_then(|requests| requests.get(&data_dir.root()).copied())
        .unwrap_or_default()
}

#[cfg(test)]
fn install_daemon_dfs_message_committed_edge_test_hook(
    request_id: &str,
) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    DAEMON_DFS_TEST_MESSAGE_COMMITTED_EDGE_HOOKS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("message committed-edge test-hook lock")
        .insert(
            request_id.to_string(),
            Arc::new(DaemonDfsMessageCommittedEdgeTestHook {
                started_tx,
                release_rx: Mutex::new(release_rx),
            }),
        );
    (started_rx, release_tx)
}

#[cfg(test)]
fn wait_at_daemon_dfs_message_committed_edge_test_hook(request_id: &str) {
    let hook = DAEMON_DFS_TEST_MESSAGE_COMMITTED_EDGE_HOOKS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .ok()
        .and_then(|mut hooks| hooks.remove(request_id));
    let Some(hook) = hook else {
        return;
    };
    let _ = hook.started_tx.send(());
    let _ = hook
        .release_rx
        .lock()
        .ok()
        .and_then(|release_rx| release_rx.recv_timeout(Duration::from_secs(5)).ok());
}

/// Serializes metadata mutation/maintenance generations for one data root.
///
/// The lightweight read lane has its own service mutex and reads immutable
/// metadata objects, so it deliberately bypasses this gate. Mutation-capable
/// work keeps the permit inside `spawn_blocking` so cancellation cannot let a
/// newer generation overlap work that is still running.
#[derive(Debug)]
struct DaemonDfsMessageExecutionGate {
    semaphore: Arc<tokio::sync::Semaphore>,
    maintenance_interest_count: AtomicUsize,
    foreground_generation: AtomicU64,
    foreground_notify: tokio::sync::Notify,
}

impl DaemonDfsMessageExecutionGate {
    fn new() -> Self {
        Self {
            semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
            maintenance_interest_count: AtomicUsize::new(0),
            foreground_generation: AtomicU64::new(0),
            foreground_notify: tokio::sync::Notify::new(),
        }
    }

    fn begin_maintenance_interest(self: &Arc<Self>) -> DaemonDfsMaintenanceForegroundPreemption {
        // Capture the baseline before publishing maintenance interest. Any
        // foreground waiter that can observe the interest therefore advances
        // a generation this guard will observe, including while cluster/local
        // admission is still in progress.
        let foreground_generation = self.foreground_generation.load(Ordering::Acquire);
        self.maintenance_interest_count
            .fetch_add(1, Ordering::AcqRel);
        DaemonDfsMaintenanceForegroundPreemption {
            gate: Arc::clone(self),
            foreground_generation,
        }
    }

    fn request_foreground_admission(&self) {
        if self.maintenance_interest_count.load(Ordering::Acquire) == 0 {
            return;
        }
        self.foreground_generation.fetch_add(1, Ordering::AcqRel);
        self.foreground_notify.notify_waiters();
    }
}

#[derive(Debug)]
struct DaemonDfsMaintenanceForegroundPreemption {
    gate: Arc<DaemonDfsMessageExecutionGate>,
    foreground_generation: u64,
}

impl DaemonDfsMaintenanceForegroundPreemption {
    fn is_requested(&self) -> bool {
        self.gate.foreground_generation.load(Ordering::Acquire) != self.foreground_generation
    }

    async fn requested(&self) {
        loop {
            let notified = self.gate.foreground_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_requested() {
                return;
            }
            notified.await;
        }
    }
}

impl Drop for DaemonDfsMaintenanceForegroundPreemption {
    fn drop(&mut self) {
        let previous = self
            .gate
            .maintenance_interest_count
            .fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "DFS maintenance interest underflow");
    }
}

fn daemon_dfs_message_execution_gate(
    data_dir: &SinomoDataDir,
) -> Result<Arc<DaemonDfsMessageExecutionGate>, DaemonError> {
    let key = data_dir.root();
    let mut gates = DAEMON_DFS_MESSAGE_EXECUTION_GATES
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .map_err(|_| DaemonError::Network {
            address: "DFS message execution".to_string(),
            detail: "DFS message execution gate map is poisoned".to_string(),
        })?;
    if let Some(gate) = gates.get(&key).and_then(Weak::upgrade) {
        return Ok(gate);
    }
    let gate = Arc::new(DaemonDfsMessageExecutionGate::new());
    gates.insert(key, Arc::downgrade(&gate));
    Ok(gate)
}

async fn acquire_daemon_dfs_foreground_message_execution(
    gate: &Arc<DaemonDfsMessageExecutionGate>,
) -> Result<tokio::sync::OwnedSemaphorePermit, DaemonError> {
    match Arc::clone(&gate.semaphore).try_acquire_owned() {
        Ok(permit) => return Ok(permit),
        Err(tokio::sync::TryAcquireError::NoPermits) => gate.request_foreground_admission(),
        Err(tokio::sync::TryAcquireError::Closed) => {
            return Err(DaemonError::Network {
                address: "DFS message execution".to_string(),
                detail: "DFS message execution gate closed".to_string(),
            });
        }
    }
    Arc::clone(&gate.semaphore)
        .acquire_owned()
        .await
        .map_err(|_| DaemonError::Network {
            address: "DFS message execution".to_string(),
            detail: "DFS message execution gate closed".to_string(),
        })
}

async fn acquire_daemon_dfs_message_execution(
    gate: &Arc<DaemonDfsMessageExecutionGate>,
    use_lightweight_read_lane: bool,
) -> Result<Option<tokio::sync::OwnedSemaphorePermit>, DaemonError> {
    if use_lightweight_read_lane {
        return Ok(None);
    }
    acquire_daemon_dfs_foreground_message_execution(gate)
        .await
        .map(Some)
}

fn daemon_default_dfs_lifecycle_gate(
    data_dir: &std::path::Path,
) -> Result<Arc<tokio::sync::Mutex<()>>, DaemonError> {
    let key = data_dir.to_path_buf();
    let mut gates = DAEMON_DEFAULT_DFS_LIFECYCLE_GATES
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .map_err(|_| DaemonError::Network {
            address: "default DFS lifecycle".to_string(),
            detail: "default DFS lifecycle gate map is poisoned".to_string(),
        })?;
    if let Some(gate) = gates.get(&key).and_then(Weak::upgrade) {
        return Ok(gate);
    }
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    gates.insert(key, Arc::downgrade(&gate));
    Ok(gate)
}

fn daemon_dfs_cache_lock_error() -> sinomo_dfs::DfsError {
    sinomo_dfs::DfsError::Network {
        message: "daemon DFS message service cache lock poisoned".to_string(),
    }
}

fn daemon_dfs_detached_send_runtime(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<Arc<DaemonDfsDetachedSendRuntime>, sinomo_dfs::DfsError> {
    let cluster_state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(daemon_dfs_network_error)?;
    daemon_dfs_detached_send_runtime_for_identity(
        data_dir,
        &cluster_state.cluster_id,
        &cluster_state.local_node_id,
    )
}

fn daemon_dfs_detached_send_runtime_for_identity(
    data_dir: &SinomoDataDir,
    cluster_id: &sinomo_core::Id,
    local_node_id: &NodeId,
) -> Result<Arc<DaemonDfsDetachedSendRuntime>, sinomo_dfs::DfsError> {
    let runtimes = DAEMON_DFS_DETACHED_SEND_RUNTIMES.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut runtimes = runtimes.lock().map_err(|_| sinomo_dfs::DfsError::Network {
        message: "daemon DFS detached-send runtime lock poisoned".to_string(),
    })?;
    let key = DaemonDfsDetachedSendRuntimeKey {
        data_root: data_dir.root(),
        cluster_id: cluster_id.to_string(),
        local_node_id: local_node_id.to_string(),
    };
    Ok(Arc::clone(runtimes.entry(key).or_insert_with(|| {
        Arc::new(DaemonDfsDetachedSendRuntime::new())
    })))
}

fn daemon_dfs_network_error(source: sinomo_core::network::NetworkError) -> sinomo_dfs::DfsError {
    sinomo_dfs::DfsError::Network {
        message: source.to_string(),
    }
}

fn daemon_dfs_metadata_authority_key_is_exact(key: &str) -> bool {
    let mut segments = key.split('/');
    matches!(
        (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
        ),
        (
            Some(""),
            Some("dfs"),
            Some("filesystems"),
            Some(file_system_id),
            Some(
                "metadata-shards"
                    | "metadata-head-watermarks"
                    | "metadata-membership-handoffs"
                    | "metadata-membership-handoff-proofs"
            ),
            Some(shard_id),
            None,
        ) if !file_system_id.is_empty() && !shard_id.is_empty()
    )
}

fn daemon_dfs_metadata_shard_config_key_is_exact(key: &str) -> bool {
    let mut segments = key.split('/');
    matches!(
        (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
        ),
        (
            Some(""),
            Some("dfs"),
            Some("filesystems"),
            Some(file_system_id),
            Some("metadata-shards"),
            Some(shard_id),
            None,
        ) if !file_system_id.is_empty() && !shard_id.is_empty()
    )
}

fn daemon_dfs_metadata_authority_projection(snapshot: &KvSnapshot) -> Vec<(String, Vec<u8>)> {
    // All retained authority keys share this prefix. `prefix_entries` seeks
    // directly into the canonical snapshot, avoiding an O(total cluster KV)
    // scan even when a relevant authority generation does change.
    snapshot
        .prefix_entries("/dfs/filesystems/")
        .filter(|entry| daemon_dfs_metadata_authority_key_is_exact(&entry.key))
        .map(|entry| (entry.key.clone(), entry.value.clone()))
        .collect()
}

fn daemon_dfs_metadata_authority_generations() -> &'static Mutex<
    BTreeMap<DaemonDfsMetadataAuthorityGenerationKey, Weak<DaemonDfsMetadataAuthorityGeneration>>,
> {
    DAEMON_DFS_METADATA_AUTHORITY_GENERATIONS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
fn daemon_dfs_metadata_authority_generation(
    cluster_paths: &ClusterPaths,
) -> Result<Arc<DaemonDfsMetadataAuthorityGeneration>, sinomo_dfs::DfsError> {
    let cluster_state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(daemon_dfs_network_error)?;
    daemon_dfs_metadata_authority_generation_for_identity(
        cluster_paths,
        &cluster_state.cluster_id,
        &cluster_state.local_node_id,
        None,
    )
}

fn daemon_dfs_metadata_authority_generation_for_identity(
    cluster_paths: &ClusterPaths,
    cluster_id: &sinomo_core::Id,
    local_node_id: &NodeId,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<Arc<DaemonDfsMetadataAuthorityGeneration>, sinomo_dfs::DfsError> {
    let key = DaemonDfsMetadataAuthorityGenerationKey {
        snapshot: cluster_paths.kv_paths().snapshot,
        cluster_id: cluster_id.to_string(),
        local_node_id: local_node_id.to_string(),
    };
    let mut generations = daemon_dfs_metadata_authority_generations()
        .lock()
        .map_err(|_| sinomo_dfs::DfsError::Network {
            message: "daemon DFS metadata-authority generation lock poisoned".to_string(),
        })?;
    if let Some(generation) = generations.get(&key).and_then(Weak::upgrade) {
        generation.attach_publish_wake(cluster_paths, runtime);
        return Ok(generation);
    }

    let generation = Arc::new(DaemonDfsMetadataAuthorityGeneration::new());
    generation.attach_publish_wake(cluster_paths, runtime);
    let weak_generation = Arc::downgrade(&generation);
    let subscription = sinomo_core::cluster::subscribe_cluster_kv_committed_snapshots(
        cluster_paths,
        move |snapshot| {
            if let Some(generation) = weak_generation.upgrade() {
                generation.observe(snapshot);
            }
        },
    );
    generation
        .subscription
        .set(subscription)
        .expect("new metadata-authority generation subscription");
    if let Some(runtime) = runtime {
        let watcher = daemon_dfs_metadata_authority_snapshot_watcher(
            cluster_paths,
            Arc::downgrade(&generation),
            runtime,
        )?;
        let _ = generation.snapshot_watcher.set(watcher);
    }
    if let Some(snapshot) =
        sinomo_core::cluster::read_cluster_kv_latest_observed_snapshot_arc_if_present(cluster_paths)
            .map_err(daemon_dfs_network_error)?
    {
        generation.observe(snapshot.as_ref());
    }
    generations.insert(key, Arc::downgrade(&generation));
    Ok(generation)
}

fn daemon_dfs_retained_metadata_authority_generation(
    detached_send_runtime: &DaemonDfsDetachedSendRuntime,
    cluster_paths: &ClusterPaths,
    cluster_id: &sinomo_core::Id,
    local_node_id: &NodeId,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<Arc<DaemonDfsMetadataAuthorityGeneration>, sinomo_dfs::DfsError> {
    if let Some(generation) = detached_send_runtime.metadata_authority_generation.get() {
        generation.attach_publish_wake(cluster_paths, runtime);
        return Ok(Arc::clone(generation));
    }
    let generation = daemon_dfs_metadata_authority_generation_for_identity(
        cluster_paths,
        cluster_id,
        local_node_id,
        runtime,
    )?;
    let _ = detached_send_runtime
        .metadata_authority_generation
        .set(Arc::clone(&generation));
    Ok(Arc::clone(
        detached_send_runtime
            .metadata_authority_generation
            .get()
            .unwrap_or(&generation),
    ))
}

fn daemon_dfs_perf_trace(args: std::fmt::Arguments<'_>) {
    tracing::trace!(target: "sinomo.dfs.perf", detail = %args, "daemon dfs perf");
}

fn daemon_dfs_perf_trace_with_traceparent(
    traceparent: Option<&str>,
    args: std::fmt::Arguments<'_>,
) {
    tracing::trace!(
        target: "sinomo.dfs.perf",
        traceparent = %traceparent.unwrap_or_default(),
        detail = %args,
        "daemon dfs perf"
    );
}

fn read_daemon_dfs_message_peer_snapshot(
    cluster_paths: &ClusterPaths,
) -> Result<Option<DaemonDfsMessagePeerSnapshot>, sinomo_dfs::DfsError> {
    if !cluster_paths.state.exists() {
        return Ok(None);
    }
    let state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(daemon_dfs_network_error)?;
    let peer_node_ids = sinomo_dfs::cluster_dfs_peer_node_ids(&state);
    Ok(Some(DaemonDfsMessagePeerSnapshot {
        cluster_id: state.cluster_id,
        local_node_id: state.local_node_id,
        kv_snapshot_available: cluster_paths.kv_paths().snapshot.exists(),
        peer_node_ids,
    }))
}

fn schedule_daemon_dfs_message_deferred_work(
    tokio_runtime: &tokio::runtime::Handle,
    cluster_paths: &ClusterPaths,
    runtime: Arc<DaemonDfsDetachedSendRuntime>,
    worker: DfsNodeDetachedSendWorker,
) {
    let deferred_target_node_ids =
        sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
            .map(|state| daemon_dfs_offline_node_ids(cluster_paths, &state))
            // Unknown/unreadable cluster state remains fail-open. Durable
            // detached work must never be discarded because a local status
            // projection is temporarily unavailable.
            .unwrap_or_default();
    schedule_daemon_dfs_message_deferred_work_with_policy(
        tokio_runtime,
        runtime,
        worker,
        deferred_target_node_ids,
    );
}

fn schedule_daemon_dfs_message_deferred_work_with_policy(
    tokio_runtime: &tokio::runtime::Handle,
    runtime: Arc<DaemonDfsDetachedSendRuntime>,
    mut worker: DfsNodeDetachedSendWorker,
    deferred_target_node_ids: BTreeSet<NodeId>,
) {
    worker.set_deferred_target_node_ids(deferred_target_node_ids.clone());
    let policy_wakeup = worker.clone();
    let mut schedule = match runtime.message_worker_schedule.lock() {
        Ok(schedule) => schedule,
        Err(_) => {
            tracing::warn!("sinomo daemon DFS deferred worker schedule is poisoned");
            return;
        }
    };
    let policy_changed =
        schedule.deferred_target_node_ids.as_ref() != Some(&deferred_target_node_ids);
    schedule.deferred_target_node_ids = Some(deferred_target_node_ids);
    schedule.generation = schedule.generation.saturating_add(1);
    schedule.latest_worker = Some(worker);
    drop(schedule);
    let start_worker = runtime
        .message_worker_active
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok();
    // A previous worker may be parked because every retained send was
    // deferred. Publish the refreshed worker before waking it so an
    // offline-to-online transition cannot miss the policy change. Outbox
    // enqueues have their own shared notification; do not churn a parked
    // worker merely because another service lane republished the same policy.
    if policy_changed && !start_worker {
        policy_wakeup.notify_scheduler_policy_change();
    }
    if !start_worker {
        return;
    }
    let _deferred_work = tokio_runtime.spawn(run_daemon_dfs_message_deferred_worker(runtime));
}

async fn run_daemon_dfs_message_deferred_worker(runtime: Arc<DaemonDfsDetachedSendRuntime>) {
    let mut worker: Option<DfsNodeDetachedSendWorker> = None;
    loop {
        let observed_generation = match runtime.message_worker_schedule.lock() {
            Ok(mut schedule) => {
                if let Some(latest_worker) = schedule.latest_worker.take() {
                    worker = Some(latest_worker);
                }
                schedule.generation
            }
            Err(_) => {
                tracing::warn!("sinomo daemon DFS deferred worker schedule is poisoned");
                break;
            }
        };
        let Some(active_worker) = worker.as_ref() else {
            break;
        };
        let mut attempt_worker = active_worker.clone();
        let result =
            match tokio::task::spawn_blocking(move || attempt_worker.drain_exclusive()).await {
                Ok(result) => result,
                Err(error) => {
                    tracing::warn!("sinomo daemon DFS deferred worker task failed: {error}");
                    tokio::time::sleep(DAEMON_DFS_PROMOTION_RETRY_DELAY).await;
                    continue;
                }
            };
        if let Err(error) = result {
            tracing::warn!(
                "sinomo daemon DFS deferred served-message work failed; retrying: {error}"
            );
        }
        let (ready, retry_not_before, outbox_generation) = match active_worker.scheduling_snapshot()
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                tracing::warn!("sinomo daemon DFS deferred send schedule failed: {error}");
                tokio::time::sleep(DAEMON_DFS_PROMOTION_RETRY_DELAY).await;
                continue;
            }
        };
        let current_generation = match runtime.message_worker_schedule.lock() {
            Ok(schedule) => schedule.generation,
            Err(_) => {
                tracing::warn!("sinomo daemon DFS deferred worker schedule is poisoned");
                break;
            }
        };
        if ready || current_generation != observed_generation {
            continue;
        }
        if let Some(retry_not_before) = retry_not_before {
            if let Err(error) = active_worker
                .wait_for_ready_or_retry(outbox_generation, retry_not_before)
                .await
            {
                tracing::warn!("sinomo daemon DFS deferred send wait failed: {error}");
                tokio::time::sleep(DAEMON_DFS_PROMOTION_RETRY_DELAY).await;
            }
            continue;
        }

        // Stay parked on the lightweight shared outbox even while it is empty.
        // Another production drainer may have moved work into an in-flight
        // batch after our exclusive drain released its gate. If that batch is
        // restored after failure, `record_change` wakes this scheduler; exiting
        // here would strand the retry until unrelated future traffic.
        if let Err(error) = active_worker.wait_for_change(outbox_generation).await {
            tracing::warn!("sinomo daemon DFS deferred send wait failed: {error}");
            tokio::time::sleep(DAEMON_DFS_PROMOTION_RETRY_DELAY).await;
        }
    }
    runtime
        .message_worker_active
        .store(false, Ordering::Release);
}

#[cfg(test)]
fn open_daemon_dfs_message_service_lane(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: &sinomo_core::node::NodeId,
    peer_node_ids: Vec<NodeId>,
    runtime: &tokio::runtime::Handle,
) -> Result<Arc<DaemonDfsMessageServiceLaneHandle>, sinomo_dfs::DfsError> {
    let content = sinomo_dfs::LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
    open_daemon_dfs_message_service_lane_with_content(
        data_dir,
        cluster_paths,
        local_node_id,
        peer_node_ids,
        runtime,
        content,
    )
}

fn open_daemon_dfs_message_service_lane_with_content(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: &sinomo_core::node::NodeId,
    peer_node_ids: Vec<NodeId>,
    runtime: &tokio::runtime::Handle,
    content: sinomo_dfs::LocalContentStore,
) -> Result<Arc<DaemonDfsMessageServiceLaneHandle>, sinomo_dfs::DfsError> {
    let metadata_backend =
        sinomo_dfs::open_cluster_aware_dfs_metadata_backend(data_dir, cluster_paths)?;
    #[cfg(test)]
    record_daemon_dfs_test_message_service_lane_open(data_dir);
    let detached_send_runtime = daemon_dfs_detached_send_runtime(data_dir, cluster_paths)?;
    let transport =
        DfsNodeServedMessageTransport::new_with_peer_transports_for_nodes_and_detached_state(
            local_node_id.clone(),
            peer_node_ids.clone(),
            daemon_dfs_blocking_peer_transport_on_runtime(data_dir, runtime),
            daemon_dfs_async_peer_transport(data_dir, runtime),
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            detached_send_runtime.state.clone(),
        );
    let service = NetworkedDfsService::new(transport, content);
    // Message lanes hydrate only the filesystem catalog and durable mutation
    // guards at construction. A full metadata hydrate here is especially costly
    // because the cache creates the primary lane and the read-lane pool together
    // while holding its lock. Read requests that need the complete metadata view
    // hydrate their selected lane below; routed mutations use targeted backend
    // reads instead.
    let service = with_daemon_dfs_control_plane_on_runtime(service, cluster_paths, Some(runtime))
        .with_boxed_metadata_backend_lazy(metadata_backend)?;
    Ok(Arc::new(DaemonDfsMessageServiceLaneHandle {
        lane: Mutex::new(DaemonDfsMessageServiceLane {
            service,
            peer_node_ids,
            last_metadata_refresh: Instant::now(),
            // Routed mutations and namespace operations check the exact shard
            // marker and rebind only when that shard is actually frozen. An
            // unconditional all-shard rebind here makes every cold request pay
            // for control-plane proof reads that are irrelevant in steady state.
            metadata_hydrated: false,
        }),
        detached_send_runtime,
    }))
}

fn open_daemon_dfs_message_content_read_lane_with_content(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: &sinomo_core::node::NodeId,
    peer_node_ids: Vec<NodeId>,
    runtime: &tokio::runtime::Handle,
    content: sinomo_dfs::LocalContentStore,
) -> Result<Arc<DaemonDfsMessageServiceLaneHandle>, sinomo_dfs::DfsError> {
    let detached_send_runtime = daemon_dfs_detached_send_runtime(data_dir, cluster_paths)?;
    let transport =
        DfsNodeServedMessageTransport::new_with_peer_transports_for_nodes_and_detached_state(
            local_node_id.clone(),
            peer_node_ids.clone(),
            daemon_dfs_blocking_peer_transport_on_runtime(data_dir, runtime),
            daemon_dfs_async_peer_transport(data_dir, runtime),
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            detached_send_runtime.state.clone(),
        );
    Ok(Arc::new(DaemonDfsMessageServiceLaneHandle {
        lane: Mutex::new(DaemonDfsMessageServiceLane {
            service: NetworkedDfsService::new(transport, content),
            peer_node_ids,
            last_metadata_refresh: Instant::now(),
            metadata_hydrated: false,
        }),
        detached_send_runtime,
    }))
}

struct DaemonDfsCommitPromotionRecovery {
    detached_runtime: Arc<DaemonDfsDetachedSendRuntime>,
    worker: DfsNodeDetachedSendWorker,
    scan_result: Result<(), sinomo_dfs::DfsError>,
}

/// Rebuild the process-local promotion outbox once per observed authority
/// generation after restart, then prepare the detached retry worker.
///
/// This uses a dedicated service that shares only detached identity state with
/// cached message lanes. The finite metadata scan runs under lifecycle
/// maintenance, but promotion RPCs are handed to the process-wide detached
/// worker instead of being drained while the cluster maintenance lease is held.
/// An offline replica can therefore delay only its own convergence; it cannot
/// fence another node out of unrelated cluster maintenance. An empty committed
/// shard topology is transient bootstrap state, so it leaves recovery
/// incomplete for the next maintenance tick.
fn prepare_daemon_dfs_commit_promotion_recovery(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<DaemonDfsCommitPromotionRecovery, sinomo_dfs::DfsError> {
    let cluster_state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(daemon_dfs_network_error)?;
    let detached_send_runtime = daemon_dfs_detached_send_runtime_for_identity(
        data_dir,
        &cluster_state.cluster_id,
        &cluster_state.local_node_id,
    )?;
    let file_system = sinomo_dfs::distributed_sifs(cluster_state.local_node_id.clone());
    let authority_generation = daemon_dfs_retained_metadata_authority_generation(
        detached_send_runtime.as_ref(),
        cluster_paths,
        &cluster_state.cluster_id,
        &cluster_state.local_node_id,
        Some(runtime),
    )?
    .generation();
    let recovery_scan_required = detached_send_runtime
        .recovery_scan_generation
        .load(Ordering::Acquire)
        != authority_generation;
    let peer_node_ids = sinomo_dfs::cluster_dfs_peer_node_ids(&cluster_state);
    let open_recovery_service = || {
        let content = sinomo_dfs::LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
        let transport =
            DfsNodeServedMessageTransport::new_with_peer_transports_for_nodes_and_detached_state(
                cluster_state.local_node_id.clone(),
                peer_node_ids.clone(),
                daemon_dfs_blocking_peer_transport_on_runtime(data_dir, runtime),
                daemon_dfs_async_peer_transport(data_dir, runtime),
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                detached_send_runtime.state.clone(),
            );
        Ok::<_, sinomo_dfs::DfsError>(NetworkedDfsService::new(transport, content))
    };

    // Create the lightweight worker before scanning. It shares the outbox, so
    // work reconstructed below is visible without retaining the metadata-heavy
    // recovery service or draining network calls inline.
    let mut recovery_service = open_recovery_service()?;
    let worker = recovery_service.prepare_message_deferred_work();

    let recovery_scan_started = Instant::now();
    let mut scan_deferred_for_empty_topology = false;
    let scan_result = if recovery_scan_required {
        (|| {
            if sinomo_dfs::cluster_dfs_metadata_shards(cluster_paths, &file_system.file_system_id)?
                .is_empty()
            {
                scan_deferred_for_empty_topology = true;
                return Ok(());
            }
            let metadata_backend =
                sinomo_dfs::open_cluster_aware_dfs_metadata_backend(data_dir, cluster_paths)?;
            with_daemon_dfs_control_plane_on_runtime(
                open_recovery_service()?,
                cluster_paths,
                Some(runtime),
            )
            .with_boxed_metadata_backend_for_commit_promotion_recovery(metadata_backend)
            .map(|_| {
                // The durable scan is complete even when one target remains
                // offline. Its exact send stays in the shared outbox.
                detached_send_runtime
                    .recovery_scan_generation
                    .store(authority_generation, Ordering::Release);
            })
        })()
    } else {
        Ok(())
    };
    let recovery_scan_ms = recovery_scan_started.elapsed().as_millis();

    // Return the worker even when the scan failed: it may have queued valid
    // work before encountering a later corrupt/unavailable record, and retained
    // ready work must not depend on a successful fresh scan. The caller drops
    // its local lifecycle and execution guards before waking the scheduler.
    let pending = worker.has_pending()?;
    daemon_dfs_perf_trace(format_args!(
        "commit-promotion-recovery generation={} scanned={} scan_deferred_empty_topology={} scan_ms={} prepared_pending={}",
        authority_generation,
        recovery_scan_required && !scan_deferred_for_empty_topology,
        scan_deferred_for_empty_topology,
        recovery_scan_ms,
        pending
    ));
    Ok(DaemonDfsCommitPromotionRecovery {
        detached_runtime: detached_send_runtime,
        worker,
        scan_result,
    })
}

fn daemon_dfs_message_uses_lightweight_read_lane(
    command: &DfsMessageCommand,
    execution_scope: &DfsMessageExecutionScope,
) -> bool {
    matches!(execution_scope, DfsMessageExecutionScope::ReadOnly)
        && !sinomo_dfs::dfs_message_requires_full_metadata_hydrate(command)
}

fn daemon_dfs_message_uses_content_read_lane(command: &DfsMessageCommand) -> bool {
    match &command.routed_message.message {
        sinomo_dfs::DfsNetworkMessage::ReadRequest(request) => {
            request.version.is_some() && request.content_hash.is_some()
        }
        sinomo_dfs::DfsNetworkMessage::BulkReadRequest(_) => true,
        _ => false,
    }
}

fn process_daemon_dfs_message_command_on_runtime(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    command: DfsMessageCommand,
    preingested_blob: Option<sinomo_dfs::ContentStoreQuarantinedBlob>,
    execution_scope: &DfsMessageExecutionScope,
    runtime: &tokio::runtime::Handle,
    traceparent: Option<String>,
) -> Result<Option<DaemonDfsMessageCommandProcessResult>, sinomo_dfs::DfsError> {
    let trace_started = Instant::now();
    let ensure_started = Instant::now();
    data_dir
        .ensure_root_layout()
        .map_err(|source| sinomo_dfs::DfsError::Metadata {
            message: source.to_string(),
        })?;
    let ensure_ms = ensure_started.elapsed().as_millis();
    let cache_key = data_dir.root();
    let is_read_only = matches!(execution_scope, DfsMessageExecutionScope::ReadOnly);
    let requires_full_metadata_hydrate =
        sinomo_dfs::dfs_message_requires_full_metadata_hydrate(&command);
    let use_lightweight_read_lane =
        daemon_dfs_message_uses_lightweight_read_lane(&command, execution_scope);
    let use_content_read_lane = daemon_dfs_message_uses_content_read_lane(&command);
    let request_id = command.request_id.clone();
    let source_node_id = command.routed_message.source_node_id.clone();
    let target_node_id = command.routed_message.target_node_id.clone();
    let frame_id = command.routed_message.frame_id;
    let message_id = command.routed_message.message_id;
    let message_kind = daemon_dfs_message_kind(&command.routed_message.message);
    daemon_dfs_perf_trace(format_args!(
        "dfs-message-dispatch-start request={} kind={} source={} target={} frame_id={} message_id={} read_lane={} content_lane={} full_hydrate_required={} ensure_ms={}",
        request_id,
        message_kind,
        source_node_id,
        target_node_id,
        frame_id,
        message_id,
        use_lightweight_read_lane,
        use_content_read_lane,
        requires_full_metadata_hydrate,
        ensure_ms
    ));
    let peer_snapshot_started = Instant::now();
    let Some(peer_snapshot) = read_daemon_dfs_message_peer_snapshot(cluster_paths)? else {
        return Ok(None);
    };
    let peer_snapshot_ms = peer_snapshot_started.elapsed().as_millis();
    if command.routed_message.target_node_id != peer_snapshot.local_node_id {
        return Err(sinomo_dfs::DfsError::Network {
            message: format!(
                "dfs message target {} does not match local node {}",
                command.routed_message.target_node_id, peer_snapshot.local_node_id
            ),
        });
    }
    let peer_node_ids = peer_snapshot.peer_node_ids.clone();
    let cache_select_started = Instant::now();
    let cache_lock_wait_started = Instant::now();
    let (service_lane_handle, cache_lock_wait) = {
        let mut cache = daemon_dfs_message_service_cache()
            .lock()
            .map_err(|_| daemon_dfs_cache_lock_error())?;
        let cache_lock_wait = cache_lock_wait_started.elapsed();
        let metadata_authority_generation = match cache.get(&cache_key) {
            Some(entry)
                if entry.signature.cluster_id == peer_snapshot.cluster_id
                    && entry.signature.local_node_id == peer_snapshot.local_node_id =>
            {
                Arc::clone(&entry.metadata_authority_generation)
            }
            _ => daemon_dfs_metadata_authority_generation_for_identity(
                cluster_paths,
                &peer_snapshot.cluster_id,
                &peer_snapshot.local_node_id,
                Some(runtime),
            )?,
        };
        let signature = DaemonDfsMessageServiceSignature {
            cluster_id: peer_snapshot.cluster_id.clone(),
            local_node_id: peer_snapshot.local_node_id.clone(),
            kv_snapshot_available: peer_snapshot.kv_snapshot_available,
            metadata_authority_generation: metadata_authority_generation.generation(),
        };
        let should_rebuild = cache
            .get(&cache_key)
            .is_none_or(|entry| entry.signature != signature);
        if should_rebuild {
            // The cached lane may contain a fully hydrated all-shard view.
            // Release the superseded generation before opening its replacement
            // so an authority or topology change cannot transiently retain two
            // metadata-heavy service generations.
            drop(cache.remove(&cache_key));
            tracing::info!(
                data_root = %cache_key.display(),
                metadata_authority_generation = signature.metadata_authority_generation,
                peer_count = peer_node_ids.len(),
                "opened bounded daemon DFS message service cache generation"
            );
            cache.insert(
                cache_key.clone(),
                DaemonDfsMessageServiceCacheEntry {
                    signature: signature.clone(),
                    metadata_authority_generation,
                    peer_node_ids: peer_node_ids.clone(),
                    primary_service: None,
                    lightweight_read_service: None,
                    content_read_service: None,
                },
            );
        }

        let entry = cache
            .get_mut(&cache_key)
            .expect("daemon DFS message cache entry inserted");
        entry.peer_node_ids.clone_from(&peer_node_ids);
        let service = if use_content_read_lane {
            if entry.content_read_service.is_none() {
                let content_store =
                    sinomo_dfs::LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
                entry.content_read_service =
                    Some(open_daemon_dfs_message_content_read_lane_with_content(
                        data_dir,
                        cluster_paths,
                        &entry.signature.local_node_id,
                        entry.peer_node_ids.clone(),
                        runtime,
                        content_store,
                    )?);
                tracing::info!(
                    data_root = %cache_key.display(),
                    "opened bounded daemon DFS exact-content read lane"
                );
            }
            Arc::clone(
                entry
                    .content_read_service
                    .as_ref()
                    .expect("exact-content read lane inserted"),
            )
        } else if use_lightweight_read_lane {
            if entry.lightweight_read_service.is_none() {
                let content_store =
                    sinomo_dfs::LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
                entry.lightweight_read_service =
                    Some(open_daemon_dfs_message_service_lane_with_content(
                        data_dir,
                        cluster_paths,
                        &entry.signature.local_node_id,
                        entry.peer_node_ids.clone(),
                        runtime,
                        content_store,
                    )?);
                tracing::info!(
                    data_root = %cache_key.display(),
                    "opened bounded daemon DFS lightweight read lane"
                );
            }
            Arc::clone(
                entry
                    .lightweight_read_service
                    .as_ref()
                    .expect("lightweight read lane inserted"),
            )
        } else {
            if entry.primary_service.is_none() {
                let content_store =
                    sinomo_dfs::LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
                entry.primary_service = Some(open_daemon_dfs_message_service_lane_with_content(
                    data_dir,
                    cluster_paths,
                    &entry.signature.local_node_id,
                    entry.peer_node_ids.clone(),
                    runtime,
                    content_store,
                )?);
                tracing::info!(
                    data_root = %cache_key.display(),
                    "opened bounded daemon DFS mutation service lane"
                );
            }
            Arc::clone(
                entry
                    .primary_service
                    .as_ref()
                    .expect("mutation service lane inserted"),
            )
        };
        (service, cache_lock_wait)
    };
    let cache_select = cache_select_started.elapsed();

    let lane_lock_wait_started = Instant::now();
    let mut service_lane = service_lane_handle
        .lock()
        .map_err(|_| daemon_dfs_cache_lock_error())?;
    let lane_lock_wait = lane_lock_wait_started.elapsed();
    service_lane.refresh_peer_targets(peer_node_ids);
    let hydrate_started = Instant::now();
    let mut hydrated_metadata = false;
    if requires_full_metadata_hydrate
        && (!service_lane.metadata_hydrated
            || service_lane.last_metadata_refresh.elapsed()
                >= DAEMON_DFS_MESSAGE_CACHE_REFRESH_INTERVAL)
    {
        tracing::info!(
            data_root = %cache_key.display(),
            request = %request_id,
            kind = message_kind,
            "hydrating bounded daemon DFS message service metadata"
        );
        service_lane.service.hydrate_from_metadata_backend()?;
        service_lane.last_metadata_refresh = Instant::now();
        service_lane.metadata_hydrated = true;
        hydrated_metadata = true;
    }
    let hydrate = hydrate_started.elapsed();
    let process_started = Instant::now();
    daemon_dfs_perf_trace(format_args!(
        "dfs-message-dispatch-before-process request={} kind={} peer_snapshot_ms={} cache_select_ms={} cache_select_us={} cache_lock_wait_ms={} cache_lock_wait_us={} lane_lock_wait_ms={} lane_lock_wait_us={} full_hydrate={} hydrate_ms={} hydrate_us={} pre_process_total_ms={}",
        request_id,
        message_kind,
        peer_snapshot_ms,
        cache_select.as_millis(),
        cache_select.as_micros(),
        cache_lock_wait.as_millis(),
        cache_lock_wait.as_micros(),
        lane_lock_wait.as_millis(),
        lane_lock_wait.as_micros(),
        hydrated_metadata,
        hydrate.as_millis(),
        hydrate.as_micros(),
        trace_started.elapsed().as_millis()
    ));
    let process_report = service_lane
        .service
        .process_message_command_report_with_preingested_blob(command, preingested_blob);
    let deferred_work = if is_read_only {
        Ok(None)
    } else {
        service_lane
            .service
            .message_deferred_work_pending()
            .map(|pending| {
                pending.then(|| {
                    (
                        Arc::clone(&service_lane_handle.detached_send_runtime),
                        service_lane.service.detach_message_deferred_work(),
                    )
                })
            })
    };
    let repair_queue_persistence_error = if !is_read_only
        && (process_report.committed_work.repair_maintenance
            || process_report.outcome.as_ref().is_ok_and(|result| {
                sinomo_dfs::dfs_events_should_wake_repair_maintenance(&result.events)
            })) {
        persist_daemon_dfs_repair_queue_records(
            data_dir,
            service_lane.service.durable_repair_queue_records(),
        )
        .err()
    } else {
        None
    };
    let process = process_started.elapsed();
    let (outbound_message_count, event_count) = process_report
        .outcome
        .as_ref()
        .map(|result| (result.response.outbound_messages.len(), result.events.len()))
        .unwrap_or_default();
    daemon_dfs_perf_trace(format_args!(
        "dfs-message-dispatch request={} read_lane={} peer_snapshot_ms={} ensure_ms={} cache_select_ms={} cache_select_us={} cache_lock_wait_ms={} cache_lock_wait_us={} lane_lock_wait_ms={} lane_lock_wait_us={} full_hydrate={} hydrate_ms={} hydrate_us={} process_ms={} process_us={} process_ok={} outbound_messages={} events={} total_ms={}",
        request_id,
        use_lightweight_read_lane,
        peer_snapshot_ms,
        ensure_ms,
        cache_select.as_millis(),
        cache_select.as_micros(),
        cache_lock_wait.as_millis(),
        cache_lock_wait.as_micros(),
        lane_lock_wait.as_millis(),
        lane_lock_wait.as_micros(),
        hydrated_metadata,
        hydrate.as_millis(),
        hydrate.as_micros(),
        process.as_millis(),
        process.as_micros(),
        process_report.outcome.is_ok(),
        outbound_message_count,
        event_count,
        trace_started.elapsed().as_millis()
    ));
    drop(service_lane);
    if let Some(error) = repair_queue_persistence_error {
        tracing::error!(
            target: "sinomo.dfs.fault_healing",
            data_root = %cache_key.display(),
            request = %request_id,
            kind = message_kind,
            error = %error,
            "failed to persist committed DFS repair queue; restart recovery may require explicit disaster recovery"
        );
    }
    #[cfg(test)]
    wait_at_daemon_dfs_message_committed_edge_test_hook(&request_id);
    let committed_edges = emit_daemon_dfs_committed_edges(
        data_dir,
        cluster_paths,
        execution_scope,
        runtime,
        traceparent,
        process_report.committed_work,
        process_report
            .outcome
            .as_ref()
            .map(|result| result.events.as_slice())
            .unwrap_or_default(),
    );
    // Schedule detached convergence before returning across the cancellable
    // async JoinHandle boundary. A disconnected RPC drops its JoinHandle while
    // this blocking task keeps running, so deferring this wake to the caller
    // could leave restored promotion work idle until unrelated later traffic.
    let deferred_work_error = match deferred_work {
        Ok(Some((detached_runtime, worker))) => {
            schedule_daemon_dfs_message_deferred_work(
                runtime,
                cluster_paths,
                detached_runtime,
                worker,
            );
            None
        }
        Ok(None) => None,
        Err(error) => Some(error),
    };
    let DfsMessageProcessResult {
        response,
        attachment,
        events: _,
    } = process_report.outcome?;
    if let Some(error) = deferred_work_error {
        return Err(error);
    }
    Ok(Some(DaemonDfsMessageCommandProcessResult {
        response,
        attachment,
        committed_edges,
    }))
}

/// Emit retained work produced by a durably processed DFS mutation scope.
///
/// Served-message and maintenance paths invoke this only after releasing their
/// backend-bearing generation, but before unwrapping the operation outcome. A
/// caller can abandon an outer `JoinHandle`, or later work can fail after a
/// durable mutation; transferring a caller-retained receipt here guarantees
/// committed work cannot depend on unrelated traffic.
fn emit_daemon_dfs_committed_edges(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    execution_scope: &DfsMessageExecutionScope,
    runtime: &tokio::runtime::Handle,
    traceparent: Option<String>,
    committed_work: DfsMessageCommittedWork,
    events: &[sinomo_dfs::NetworkedDfsServiceEvent],
) -> DaemonDfsMessageCommittedEdges {
    let advances_replica = committed_work.metadata_replica_progress
        || sinomo_dfs::dfs_events_advance_metadata_replica(events);
    let replica_progress = advances_replica
        && sinomo_dfs::invalidate_default_cluster_dfs_materialization_for_replica_progress(
            data_dir,
        );
    if replica_progress {
        // Only a materialization pass which explicitly parked behind exact
        // replica evidence subscribes to this local edge. Healthy journal
        // replication therefore cannot wake the default-control reactor.
        notify_daemon_default_dfs_reconciliation_replica_progress(cluster_paths);
    }

    let repair_maintenance = committed_work.repair_maintenance
        || sinomo_dfs::dfs_events_should_wake_repair_maintenance(events);
    let namespace_maintenance = committed_work.namespace_maintenance;
    if repair_maintenance || namespace_maintenance {
        // The serving metadata authority owns the durable pending intent or
        // resumable namespace job. Wake its coalescing classifier at this
        // committed edge; requester-side completion events are deliberately
        // excluded by the event classifier.
        #[cfg(test)]
        if repair_maintenance {
            record_daemon_dfs_repair_maintenance_edge_request(data_dir);
        }
        notify_daemon_dfs_maintenance_work();
    }

    let publish_metadata =
        committed_work.publish_metadata || sinomo_dfs::dfs_events_should_publish_metadata(events);
    if publish_metadata {
        schedule_daemon_dfs_metadata_publish_for_data_dir_on_runtime(
            data_dir.clone(),
            cluster_paths.clone(),
            runtime,
            traceparent,
            DaemonDfsMutationLaneKey::from_scope(execution_scope),
        );
    }

    DaemonDfsMessageCommittedEdges {
        publish_metadata,
        replica_progress,
        repair_maintenance,
        namespace_maintenance,
    }
}

async fn ingest_daemon_dfs_message_attachment(
    data_dir: &SinomoDataDir,
    mut command: DfsMessageCommand,
    attachment: Option<DfsInboundAttachment>,
    runtime: &tokio::runtime::Handle,
) -> Result<
    (
        DfsMessageCommand,
        Option<sinomo_dfs::ContentStoreQuarantinedBlob>,
    ),
    DaemonError,
> {
    let Some(attachment) = attachment else {
        return Ok((command, None));
    };
    let task_data_dir = data_dir.clone();
    let task_runtime = runtime.clone();
    tokio::task::spawn_blocking(move || {
        let (expected_hash, stage_blob) = match &command.routed_message.message {
            sinomo_dfs::DfsNetworkMessage::StageBlob(stage) => {
                (Some(stage.content_hash.clone()), true)
            }
            sinomo_dfs::DfsNetworkMessage::DistributedWriteRequest(_)
            | sinomo_dfs::DfsNetworkMessage::ExclusiveCreateRequest(_) => {
                (attachment.descriptor.content_hash.clone(), false)
            }
            other => {
                return Err(sinomo_dfs::DfsError::Distributed {
                    message: format!("streamed DFS attachment is not supported for {other:?}"),
                });
            }
        };
        if stage_blob
            && let Some(descriptor_hash) = attachment.descriptor.content_hash.as_deref()
            && Some(descriptor_hash) != expected_hash.as_deref()
        {
            return Err(sinomo_dfs::DfsError::ContentHashMismatch {
                expected: expected_hash.unwrap_or_default(),
                actual: descriptor_hash.to_string(),
            });
        }

        let content = LocalContentStore::new(task_data_dir.dfs_dir().join("content"))?;
        let mut ingest = content.begin_quarantined_blob_ingest(
            attachment.descriptor.total_size_bytes,
            expected_hash.as_deref(),
        )?;
        let mut body = attachment.body;
        loop {
            let chunk = task_runtime.block_on(body.next_chunk()).map_err(|source| {
                sinomo_dfs::DfsError::Network {
                    message: format!("read streamed DFS request attachment: {source}"),
                }
            })?;
            let Some(chunk) = chunk else {
                break;
            };
            ingest.write_chunk(chunk.as_ref())?;
        }
        let blob = ingest.finish_quarantined()?;
        if !stage_blob {
            match &mut command.routed_message.message {
                sinomo_dfs::DfsNetworkMessage::DistributedWriteRequest(write)
                | sinomo_dfs::DfsNetworkMessage::ExclusiveCreateRequest(write) => {
                    write.bytes.clear();
                    write.content_hash = Some(blob.content_hash.clone());
                    write.content_size_bytes = Some(blob.size_bytes);
                    write.requester_blob_pre_staged = false;
                }
                _ => unreachable!("streamed distributed write kind was checked before ingest"),
            }
        }
        Ok((command, Some(blob)))
    })
    .await
    .map_err(|source| DaemonError::Network {
        address: "DFS streamed attachment ingest".to_string(),
        detail: format!("DFS streamed attachment task failed: {source}"),
    })?
    .map_err(Into::into)
}

async fn handle_daemon_dfs_message_async(
    paths: &DaemonPaths,
    command: DfsMessageCommand,
    attachment: Option<DfsInboundAttachment>,
    traceparent: Option<String>,
    runtime: &tokio::runtime::Handle,
) -> Result<DfsDataPlaneResponse, DaemonError> {
    let trace_started = Instant::now();
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let (command, preingested_blob) =
        ingest_daemon_dfs_message_attachment(&data_dir, command, attachment, runtime).await?;
    let cluster_paths = ClusterPaths::new(&data_dir);
    let request_id = command.request_id.clone();
    let source_node_id = command.routed_message.source_node_id.clone();
    let target_node_id = command.routed_message.target_node_id.clone();
    let frame_id = command.routed_message.frame_id;
    let message_id = command.routed_message.message_id;
    let message_kind = daemon_dfs_message_kind(&command.routed_message.message);
    let execution_scope = dfs_message_execution_scope(&command);
    let use_lightweight_read_lane =
        daemon_dfs_message_uses_lightweight_read_lane(&command, &execution_scope);
    let execution_scope_label = match &execution_scope {
        DfsMessageExecutionScope::ReadOnly => "read-only".to_string(),
        DfsMessageExecutionScope::ExactShard {
            file_system_id,
            shard_id,
        } => format!("shard:{file_system_id}:{}", shard_id.0),
        DfsMessageExecutionScope::NamespacePaths { .. } => "namespace-global".to_string(),
        DfsMessageExecutionScope::GlobalExclusive => "global".to_string(),
    };
    daemon_dfs_perf_trace_with_traceparent(
        traceparent.as_deref(),
        format_args!(
            "dfs-message-handler-start request={} kind={} source={} target={} frame_id={} message_id={}",
            request_id, message_kind, source_node_id, target_node_id, frame_id, message_id
        ),
    );
    let process_data_dir = data_dir.clone();
    let process_cluster_paths = cluster_paths.clone();
    let process_runtime = runtime.clone();
    let process_traceparent = traceparent.clone();
    let process_data_owner_lock = daemon_dfs_data_owner_lock(paths)?;
    let scheduler = daemon_dfs_mutation_scheduler(&data_dir)?;
    let admission_started = Instant::now();
    // Wait for the metadata-memory bound before acquiring logical mutation
    // authority. Queued mutation-capable work therefore cannot hold a
    // shard/global gate while it waits for the sole backend-bearing service
    // generation. Lightweight immutable reads use a separately locked service
    // lane and must not queue behind maintenance or mutations.
    let execution_gate = daemon_dfs_message_execution_gate(&data_dir)?;
    let execution_slot =
        acquire_daemon_dfs_message_execution(&execution_gate, use_lightweight_read_lane).await?;
    let admission = scheduler.admit(&execution_scope).await?;
    let admission_wait = admission_started.elapsed();
    let process_execution_scope = execution_scope.clone();
    let spawn_started = Instant::now();
    let blocking_queue_started = spawn_started;
    let (result, blocking_queue, blocking_work) = tokio::task::spawn_blocking(move || {
        // The async connection task and runtime shutdown may abandon this
        // JoinHandle while blocking work continues. Retain the OS-backed data
        // lock inside the blocking closure so no replacement daemon can prove
        // this placement-authority session dead prematurely.
        let _data_owner_lock = process_data_owner_lock;
        let blocking_started = Instant::now();
        let blocking_queue = blocking_queue_started.elapsed();
        let result = sinomo_dfs::with_dfs_traceparent(process_traceparent.as_deref(), || {
            process_daemon_dfs_message_command_on_runtime(
                &process_data_dir,
                &process_cluster_paths,
                command,
                preingested_blob,
                &process_execution_scope,
                &process_runtime,
                process_traceparent.clone(),
            )
        });
        // A blocking task keeps running when its async JoinHandle is dropped.
        // Own admission here so a disconnected/cancelled RPC cannot release
        // the shard (or global) guard before the actual DFS mutation stops.
        drop(admission);
        drop(execution_slot);
        (result, blocking_queue, blocking_started.elapsed())
    })
    .await
    .map_err(|source| DaemonError::Network {
        address: "DFS message".to_string(),
        detail: format!("DFS message task failed: {source}"),
    })?;
    let result = result?;
    let spawn_blocking = spawn_started.elapsed();
    let Some(result) = result else {
        return Err(DaemonError::ClusterOrphan);
    };
    let DaemonDfsMessageCommandProcessResult {
        response,
        attachment,
        committed_edges,
    } = result;
    daemon_dfs_perf_trace_with_traceparent(
        traceparent.as_deref(),
        format_args!(
            "dfs-message-handler request={} scope={} admission_wait_ms={} admission_wait_us={} blocking_queue_ms={} blocking_queue_us={} blocking_work_ms={} blocking_work_us={} spawn_blocking_ms={} spawn_blocking_us={} publish_metadata={} repair_maintenance={} namespace_maintenance={} outbound_messages={} total_ms={}",
            request_id,
            execution_scope_label,
            admission_wait.as_millis(),
            admission_wait.as_micros(),
            blocking_queue.as_millis(),
            blocking_queue.as_micros(),
            blocking_work.as_millis(),
            blocking_work.as_micros(),
            spawn_blocking.as_millis(),
            spawn_blocking.as_micros(),
            committed_edges.publish_metadata,
            committed_edges.repair_maintenance,
            committed_edges.namespace_maintenance,
            response.outbound_messages.len(),
            trace_started.elapsed().as_millis()
        ),
    );
    Ok(DfsDataPlaneResponse {
        response,
        attachment,
    })
}

#[cfg(test)]
fn handle_daemon_dfs_message_on_runtime(
    paths: &DaemonPaths,
    command: DfsMessageCommand,
    runtime: &tokio::runtime::Handle,
) -> Result<DfsMessageResponse, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let execution_scope = dfs_message_execution_scope(&command);
    let result = process_daemon_dfs_message_command_on_runtime(
        &data_dir,
        &cluster_paths,
        command,
        None,
        &execution_scope,
        runtime,
        None,
    )?;
    let Some(result) = result else {
        return Err(DaemonError::ClusterOrphan);
    };
    let DaemonDfsMessageCommandProcessResult {
        response: message_response,
        attachment: _,
        committed_edges: _,
    } = result;
    Ok(message_response)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DaemonDefaultDfsPermitGeneration {
    modify_revision: KvRevision,
    version: KvVersion,
    value: Vec<u8>,
}

#[derive(Debug)]
struct DaemonDefaultDfsReconciliationWake {
    generation_tx: tokio::sync::watch::Sender<u64>,
}

fn daemon_default_dfs_reconciliation_wake(
    cluster_paths: &ClusterPaths,
) -> Result<Arc<DaemonDefaultDfsReconciliationWake>, DaemonError> {
    let key = cluster_paths.root.clone();
    let mut wakes = DAEMON_DEFAULT_DFS_RECONCILIATION_WAKES
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .map_err(|_| DaemonError::Network {
            address: "default DFS reconciliation wake".to_string(),
            detail: "default DFS reconciliation wake registry is poisoned".to_string(),
        })?;
    if let Some(wake) = wakes.get(&key).and_then(Weak::upgrade) {
        return Ok(wake);
    }
    let (generation_tx, _) = tokio::sync::watch::channel(0);
    let wake = Arc::new(DaemonDefaultDfsReconciliationWake { generation_tx });
    wakes.insert(key, Arc::downgrade(&wake));
    Ok(wake)
}

fn notify_daemon_default_dfs_reconciliation_replica_progress(cluster_paths: &ClusterPaths) {
    let Some(wakes) = DAEMON_DEFAULT_DFS_RECONCILIATION_WAKES.get() else {
        return;
    };
    let Ok(wakes) = wakes.lock() else {
        return;
    };
    let Some(wake) = wakes.get(&cluster_paths.root).and_then(Weak::upgrade) else {
        return;
    };
    let next_generation = (*wake.generation_tx.borrow()).wrapping_add(1);
    wake.generation_tx.send_replace(next_generation);
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DaemonDefaultDfsReconciliationProjection {
    revision: Option<KvRevision>,
    inputs: Vec<(String, Vec<u8>)>,
    placement_authority_permit: Option<DaemonDefaultDfsPermitGeneration>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DaemonDefaultDfsReconciliationWork {
    inputs: Vec<(String, Vec<u8>)>,
    placement_authority_permit: Option<DaemonDefaultDfsPermitGeneration>,
    local_generation: u64,
    reconcile_defaults: bool,
    recover_prior_process_permit: bool,
    refresh_before_recovery: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DaemonDefaultDfsParkedHandoff {
    file_system_id: DfsFileSystemId,
    shard_id: DfsMetadataShardId,
    handoff_id: String,
    retiring_epoch: u64,
    desired_epoch: u64,
    wake_keys: [String; 4],
}

impl DaemonDefaultDfsParkedHandoff {
    fn new(
        file_system_id: DfsFileSystemId,
        shard_id: String,
        handoff_id: String,
        retiring_epoch: u64,
        desired_epoch: u64,
    ) -> Self {
        let shard = shard_id.as_str();
        let wake_keys = [
            sinomo_dfs::metadata_shard_key(&file_system_id, shard),
            sinomo_dfs::metadata_shard_membership_handoff_key(&file_system_id, shard),
            sinomo_dfs::metadata_shard_membership_handoff_proof_key(&file_system_id, shard),
            sinomo_dfs::metadata_shard_head_watermark_key(&file_system_id, shard),
        ];
        Self {
            file_system_id,
            shard_id: DfsMetadataShardId::new(shard_id),
            handoff_id,
            retiring_epoch,
            desired_epoch,
            wake_keys,
        }
    }

    fn owns_key(&self, key: &str) -> bool {
        self.wake_keys.iter().any(|owned| owned == key)
    }

    fn wake_inputs(&self, inputs: &[(String, Vec<u8>)]) -> Vec<(String, Vec<u8>)> {
        inputs
            .iter()
            .filter(|(key, _)| self.wake_keys.iter().any(|owned| owned == key))
            .cloned()
            .collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DaemonDefaultDfsParkedGeneration {
    handoff: DaemonDefaultDfsParkedHandoff,
    wake_inputs: Vec<(String, Vec<u8>)>,
    local_generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DaemonDefaultDfsExactRetryGeneration {
    inputs: Vec<(String, Vec<u8>)>,
    placement_authority_permit: Option<DaemonDefaultDfsPermitGeneration>,
    local_generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DaemonDefaultDfsReconciliationStep {
    Idle,
    Progress,
    Parked,
    IdentityChanged,
}

/// Main-daemon owner for the derived default-DFS control-plane state.
///
/// One dirty startup pass covers state committed while this process was down.
/// Afterwards only semantic changes to the exact inputs consumed by default
/// DFS reconciliation wake the reactor. Derived service advertisements remain
/// in that semantic projection so deletion or corruption self-heals, while
/// their refresh-only `updated_at` field is excluded so a correct publication
/// cannot create a feedback loop.
pub(crate) struct DaemonDefaultDfsReconciliationReactor {
    _projection_tx: tokio::sync::watch::Sender<DaemonDefaultDfsReconciliationProjection>,
    projection_rx: tokio::sync::watch::Receiver<DaemonDefaultDfsReconciliationProjection>,
    _subscription: sinomo_core::cluster::ClusterKvCommittedSnapshotSubscription,
    _metadata_authority_generation: Option<Arc<DaemonDfsMetadataAuthorityGeneration>>,
    _local_wake: Arc<DaemonDefaultDfsReconciliationWake>,
    local_wake_rx: tokio::sync::watch::Receiver<u64>,
    parked_subscription: Arc<Mutex<Option<DaemonDefaultDfsParkedHandoff>>>,
    reconciled_inputs: Option<Vec<(String, Vec<u8>)>>,
    reconciled_local_generation: Option<u64>,
    recovered_placement_authority_permit: Option<Option<DaemonDefaultDfsPermitGeneration>>,
    parked_generation: Option<DaemonDefaultDfsParkedGeneration>,
}

impl std::fmt::Debug for DaemonDefaultDfsReconciliationReactor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonDefaultDfsReconciliationReactor")
            .field(
                "startup_reconciliation_pending",
                &self.reconciled_inputs.is_none(),
            )
            .field(
                "startup_recovery_pending",
                &self.recovered_placement_authority_permit.is_none(),
            )
            .field(
                "local_replica_progress_pending",
                &self.reconciled_local_generation.is_none(),
            )
            .finish_non_exhaustive()
    }
}

impl DaemonDefaultDfsReconciliationReactor {
    #[cfg(test)]
    pub(crate) fn new(
        cluster_paths: &ClusterPaths,
        local_node_id: &NodeId,
    ) -> Result<Self, DaemonError> {
        Self::new_with_runtime(cluster_paths, local_node_id, None)
    }

    pub(crate) fn new_on_runtime(
        cluster_paths: &ClusterPaths,
        local_node_id: &NodeId,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Self, DaemonError> {
        Self::new_with_runtime(cluster_paths, local_node_id, Some(runtime))
    }

    fn new_with_runtime(
        cluster_paths: &ClusterPaths,
        local_node_id: &NodeId,
        runtime: Option<&tokio::runtime::Handle>,
    ) -> Result<Self, DaemonError> {
        let local_wake = daemon_default_dfs_reconciliation_wake(cluster_paths)?;
        let local_wake_rx = local_wake.generation_tx.subscribe();
        let (projection_tx, projection_rx) =
            tokio::sync::watch::channel(DaemonDefaultDfsReconciliationProjection::default());
        let parked_subscription = Arc::new(Mutex::new(None));
        let observer_tx = projection_tx.clone();
        let observer_local_node_id = local_node_id.clone();
        let observer_data_dir = cluster_paths
            .root
            .parent()
            .map(SinomoDataDir::new)
            .ok_or_else(|| DaemonError::Network {
                address: "default DFS committed projection".to_string(),
                detail: "cluster root has no owning data directory".to_string(),
            })?;
        let projection_observer = Arc::new(Mutex::new(
            DaemonDefaultDfsCommittedProjectionObserver::new_with_parked_subscription(
                observer_data_dir,
                Arc::clone(&parked_subscription),
            ),
        ));
        let subscription_observer = Arc::clone(&projection_observer);
        let subscription = sinomo_core::cluster::subscribe_cluster_kv_committed_snapshots(
            cluster_paths,
            move |snapshot| {
                let mut observer = subscription_observer
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                observer.observe(&observer_tx, &observer_local_node_id, snapshot);
            },
        );
        // Subscribe before sampling the resident snapshot. If a newer commit
        // wins the race, the projection's monotonic revision rejects this
        // older sample instead of regressing the reactor.
        if let Some(snapshot) =
            sinomo_core::cluster::read_cluster_kv_latest_observed_snapshot_arc_if_present(
                cluster_paths,
            )?
        {
            projection_observer
                .lock()
                .map_err(|_| DaemonError::Network {
                    address: "default DFS committed projection".to_string(),
                    detail: "default DFS committed projection observer is poisoned".to_string(),
                })?
                .observe(&projection_tx, local_node_id, snapshot.as_ref());
        }
        let metadata_authority_generation = runtime
            .map(|runtime| {
                let state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)?;
                daemon_dfs_metadata_authority_generation_for_identity(
                    cluster_paths,
                    &state.cluster_id,
                    local_node_id,
                    Some(runtime),
                )
                .map_err(DaemonError::from)
            })
            .transpose()?;
        Ok(Self {
            _projection_tx: projection_tx,
            projection_rx,
            _subscription: subscription,
            _metadata_authority_generation: metadata_authority_generation,
            _local_wake: local_wake,
            local_wake_rx,
            parked_subscription,
            // `None` is the dirty startup generation. A successful startup
            // pass records even an empty projection as `Some(Vec::new())`.
            reconciled_inputs: None,
            reconciled_local_generation: None,
            // The nested option distinguishes "startup recovery has not run"
            // from "startup recovery observed no permit".
            recovered_placement_authority_permit: None,
            parked_generation: None,
        })
    }

    pub(crate) async fn changed(&mut self) {
        loop {
            if self.pending_work().is_some() {
                return;
            }
            self.changed_signal().await;
            // A notification can race the transition into a parked state, or
            // a watch sender can conservatively publish a generation which is
            // irrelevant to the exact parked handoff. Consume those hints
            // here without re-entering DFS lifecycle work.
            if self.pending_work().is_some() {
                return;
            }
        }
    }

    async fn changed_signal(&mut self) {
        tokio::select! {
            _ = self.projection_rx.changed() => {}
            _ = self.local_wake_rx.changed() => {}
        }
    }

    fn exact_retry_generation(&self) -> DaemonDefaultDfsExactRetryGeneration {
        let current = self.projection_rx.borrow().clone();
        DaemonDefaultDfsExactRetryGeneration {
            inputs: current
                .inputs
                .into_iter()
                .filter(|(key, _)| daemon_default_dfs_reconciliation_exact_retry_key(key))
                .collect(),
            placement_authority_permit: current.placement_authority_permit,
            local_generation: *self.local_wake_rx.borrow(),
        }
    }

    /// Completes the dirty startup generation before a settled solo daemon
    /// begins serving. The same retained reactor is then moved into the
    /// background task, so startup and committed-change handling share one
    /// ownership boundary and one observed generation.
    pub(crate) async fn reconcile_startup_until_idle_async(
        &mut self,
        paths: &DaemonPaths,
        options: &DaemonRunOptions,
        expected_cluster_id: &sinomo_core::Id,
        expected_node_id: &NodeId,
        runtime: &tokio::runtime::Handle,
    ) -> Result<(), DaemonError> {
        loop {
            match self
                .reconcile_pending_async(
                    paths,
                    options,
                    expected_cluster_id,
                    expected_node_id,
                    runtime,
                )
                .await?
            {
                DaemonDefaultDfsReconciliationStep::Progress => continue,
                DaemonDefaultDfsReconciliationStep::Parked => return Ok(()),
                DaemonDefaultDfsReconciliationStep::Idle => return Ok(()),
                DaemonDefaultDfsReconciliationStep::IdentityChanged => {
                    return Err(DaemonError::ClusterOrphan);
                }
            }
        }
    }

    fn pending_work(&mut self) -> Option<DaemonDefaultDfsReconciliationWork> {
        let current = self.projection_rx.borrow_and_update().clone();
        let local_generation = *self.local_wake_rx.borrow_and_update();
        let parked_wake_inputs = self
            .parked_generation
            .as_ref()
            .map(|parked| parked.handoff.wake_inputs(&current.inputs))
            .unwrap_or_default();
        let reconcile_defaults = match self.parked_generation.as_ref() {
            Some(parked) => {
                parked.wake_inputs != parked_wake_inputs
                    || parked.local_generation != local_generation
            }
            None => {
                self.reconciled_inputs.as_ref() != Some(&current.inputs)
                    || self.reconciled_local_generation != Some(local_generation)
            }
        };
        let recover_prior_process_permit = self.recovered_placement_authority_permit.as_ref()
            != Some(&current.placement_authority_permit);
        (reconcile_defaults || recover_prior_process_permit).then_some(
            DaemonDefaultDfsReconciliationWork {
                inputs: current.inputs,
                placement_authority_permit: current.placement_authority_permit,
                local_generation,
                reconcile_defaults,
                recover_prior_process_permit,
                refresh_before_recovery: self.recovered_placement_authority_permit.is_none(),
            },
        )
    }

    fn complete_default_reconciliation(&mut self, work: &DaemonDefaultDfsReconciliationWork) {
        self.reconciled_inputs = Some(work.inputs.clone());
        self.reconciled_local_generation = Some(work.local_generation);
        self.parked_generation = None;
        if let Ok(mut parked) = self.parked_subscription.lock() {
            *parked = None;
        }
    }

    fn park_default_reconciliation(
        &mut self,
        work: &DaemonDefaultDfsReconciliationWork,
        handoff: DaemonDefaultDfsParkedHandoff,
    ) {
        let wake_inputs = handoff.wake_inputs(&work.inputs);
        self.reconciled_inputs = Some(work.inputs.clone());
        self.reconciled_local_generation = Some(work.local_generation);
        self.parked_generation = Some(DaemonDefaultDfsParkedGeneration {
            handoff: handoff.clone(),
            wake_inputs,
            local_generation: work.local_generation,
        });
        if let Ok(mut parked) = self.parked_subscription.lock() {
            *parked = Some(handoff);
        }
    }

    fn complete_prior_process_recovery(&mut self, work: &DaemonDefaultDfsReconciliationWork) {
        self.recovered_placement_authority_permit = Some(work.placement_authority_permit.clone());
    }

    async fn reconcile_pending_async(
        &mut self,
        paths: &DaemonPaths,
        options: &DaemonRunOptions,
        expected_cluster_id: &sinomo_core::Id,
        expected_node_id: &NodeId,
        runtime: &tokio::runtime::Handle,
    ) -> Result<DaemonDefaultDfsReconciliationStep, DaemonError> {
        let Some(work) = self.pending_work() else {
            return Ok(DaemonDefaultDfsReconciliationStep::Idle);
        };
        let Some(publish_cluster_control) = daemon_default_dfs_reconciliation_local_is_voter(
            paths,
            expected_cluster_id,
            expected_node_id,
            runtime,
        )
        .await?
        else {
            return Ok(DaemonDefaultDfsReconciliationStep::IdentityChanged);
        };

        let mut deferred_recovery_error = None;
        if work.recover_prior_process_permit {
            match recover_prior_process_default_dfs_placement_authority_async_on_runtime(
                paths,
                runtime,
                work.refresh_before_recovery,
            )
            .await
            {
                Ok(_) => self.complete_prior_process_recovery(&work),
                Err(error) if publish_cluster_control => return Err(error),
                Err(error) => {
                    tracing::warn!(
                        target: "sinomo.dfs.placement",
                        "prior-process SIFS placement authority recovery deferred: {error}"
                    );
                    deferred_recovery_error = Some(error);
                }
            }
        }

        if work.reconcile_defaults {
            match ensure_default_distributed_dfs_outcome_async_on_runtime(
                paths,
                options,
                publish_cluster_control,
                runtime.clone(),
            )
            .await?
            {
                DaemonDefaultDfsLifecycleOutcome::Completed => {
                    self.complete_default_reconciliation(&work);
                }
                DaemonDefaultDfsLifecycleOutcome::Parked(handoff) => {
                    self.park_default_reconciliation(&work, handoff);
                    if let Some(error) = deferred_recovery_error {
                        return Err(error);
                    }
                    return Ok(DaemonDefaultDfsReconciliationStep::Parked);
                }
            }
        }
        if let Some(error) = deferred_recovery_error {
            return Err(error);
        }
        Ok(DaemonDefaultDfsReconciliationStep::Progress)
    }
}

fn daemon_default_dfs_reconciliation_exact_input_keys() -> &'static BTreeSet<String> {
    static KEYS: OnceLock<BTreeSet<String>> = OnceLock::new();
    KEYS.get_or_init(|| {
        let file_system_id = sinomo_dfs::sifs_file_system_id();
        let mut keys = BTreeSet::new();
        keys.insert(sinomo_core::kv::cluster_kv_raft_config_key());
        keys.insert(sinomo_dfs::file_system_config_key(&file_system_id));
        for shard_id in ["root", "services", "users"] {
            keys.insert(sinomo_dfs::metadata_shard_key(&file_system_id, shard_id));
            keys.insert(sinomo_dfs::metadata_shard_membership_handoff_key(
                &file_system_id,
                shard_id,
            ));
            keys.insert(sinomo_dfs::metadata_shard_membership_handoff_proof_key(
                &file_system_id,
                shard_id,
            ));
            keys.insert(sinomo_dfs::metadata_shard_head_watermark_key(
                &file_system_id,
                shard_id,
            ));
        }
        keys
    })
}

fn daemon_default_dfs_reconciliation_exact_retry_key(key: &str) -> bool {
    let file_system_id = sinomo_dfs::sifs_file_system_id();
    key == sinomo_dfs::placement_authority_permit_key(&file_system_id)
        || ["root", "services", "users"].into_iter().any(|shard_id| {
            key == sinomo_dfs::metadata_shard_key(&file_system_id, shard_id)
                || key
                    == sinomo_dfs::metadata_shard_membership_handoff_key(&file_system_id, shard_id)
                || key
                    == sinomo_dfs::metadata_shard_membership_handoff_proof_key(
                        &file_system_id,
                        shard_id,
                    )
                || key == sinomo_dfs::metadata_shard_head_watermark_key(&file_system_id, shard_id)
        })
}

#[derive(serde::Serialize)]
struct DaemonDefaultDfsEndpointTopologyProjection<'a> {
    endpoint_id: &'a sinomo_core::Id,
    node_id: &'a NodeId,
    transport: &'a sinomo_core::ClusterTransportKind,
    address: &'a str,
    source: &'a sinomo_core::ClusterEndpointSource,
    privacy: &'a sinomo_core::ClusterEndpointPrivacy,
    priority: u16,
}

impl<'a> From<&'a sinomo_core::ClusterEndpointRecord>
    for DaemonDefaultDfsEndpointTopologyProjection<'a>
{
    fn from(endpoint: &'a sinomo_core::ClusterEndpointRecord) -> Self {
        Self {
            endpoint_id: &endpoint.endpoint_id,
            node_id: &endpoint.node_id,
            transport: &endpoint.transport,
            address: &endpoint.address,
            source: &endpoint.source,
            privacy: &endpoint.privacy,
            priority: endpoint.priority,
        }
    }
}

#[derive(serde::Serialize)]
struct DaemonDefaultDfsMemberProjection<'a> {
    cluster_id: &'a sinomo_core::Id,
    node_id: &'a NodeId,
    role: &'a sinomo_core::ClusterMemberRole,
    state: &'a sinomo_core::ClusterMemberState,
    endpoints: Vec<DaemonDefaultDfsEndpointTopologyProjection<'a>>,
}

#[derive(serde::Serialize)]
struct DaemonDefaultDfsConnectivityProjection<'a> {
    cluster_id: &'a sinomo_core::Id,
    source_node_id: &'a NodeId,
    target_node_id: &'a NodeId,
    endpoint_id: &'a sinomo_core::network::ClusterEndpointId,
    transport: &'a sinomo_core::ClusterTransportKind,
    address: &'a str,
    state: &'a sinomo_core::network::ClusterConnectivityState,
    failure_reason: &'a Option<sinomo_core::network::ClusterConnectivityFailureReason>,
}

#[derive(serde::Serialize)]
struct DaemonDefaultDfsAdvertisementProjection<'a> {
    node_id: &'a NodeId,
    capacity_weight: u32,
    accepts_metadata: bool,
    accepts_content: bool,
    zone: &'a Option<String>,
}

fn daemon_default_dfs_reconciliation_input_value(entry: &KvEntry) -> Vec<u8> {
    if entry.key.starts_with(DAEMON_DEFAULT_DFS_MEMBER_PREFIX)
        && let Ok(member) = entry.decode::<sinomo_core::ClusterMemberRecord>()
    {
        let mut endpoints = member
            .endpoints
            .iter()
            .map(DaemonDefaultDfsEndpointTopologyProjection::from)
            .collect::<Vec<_>>();
        endpoints.sort_by(|left, right| left.endpoint_id.cmp(right.endpoint_id));
        // DFS placement consumes membership eligibility/order and the stable
        // endpoint topology used to filter its connectivity graph. Exclude
        // liveness timestamps and observed latency so refresh-only commits do
        // not wake the reactor.
        let value = sinomo_core::kv::encode_kv_value(&DaemonDefaultDfsMemberProjection {
            cluster_id: &member.cluster_id,
            node_id: &member.node_id,
            role: &member.role,
            state: &member.state,
            endpoints,
        })
        .unwrap_or_else(|_| entry.value.clone());
        return value;
    }
    if entry.key.starts_with(DAEMON_DEFAULT_DFS_ENDPOINT_PREFIX)
        && let Ok(endpoint) = entry.decode::<sinomo_core::ClusterEndpointRecord>()
    {
        // This intentionally overlaps the endpoint projection embedded in a
        // member record. Route construction consumes both the bootstrap member
        // image and independently updated normalized endpoint KV entries; a
        // topology change in either representation must invalidate placement.
        let value = sinomo_core::kv::encode_kv_value(
            &DaemonDefaultDfsEndpointTopologyProjection::from(&endpoint),
        )
        .unwrap_or_else(|_| entry.value.clone());
        return value;
    }
    if entry
        .key
        .starts_with(DAEMON_DEFAULT_DFS_CONNECTIVITY_PREFIX)
        && let Ok(record) = entry.decode::<sinomo_core::network::ClusterConnectivityRecord>()
    {
        // Match `cluster_connectivity_stable_eq`: expiry, observations and
        // latency refreshes do not change the healthy-route existence used by
        // default metadata-primary failover.
        let value = sinomo_core::kv::encode_kv_value(&DaemonDefaultDfsConnectivityProjection {
            cluster_id: &record.cluster_id,
            source_node_id: &record.source_node_id,
            target_node_id: &record.target_node_id,
            endpoint_id: &record.endpoint_id,
            transport: &record.transport,
            address: &record.address,
            state: &record.state,
            failure_reason: &record.failure_reason,
        })
        .unwrap_or_else(|_| entry.value.clone());
        return value;
    }
    if (entry
        .key
        .starts_with(sinomo_dfs::metadata_services_prefix())
        || entry.key.starts_with(sinomo_dfs::content_services_prefix()))
        && let Ok(advertisement) = entry.decode::<sinomo_dfs::DfsNodeAdvertisement>()
    {
        // Advertisements are derived state, but excluding them entirely means
        // deletion or semantic corruption has no committed edge that can
        // repair it. `updated_at` is deliberately excluded: a refresh of an
        // already-correct advertisement is not control-plane work.
        return sinomo_core::kv::encode_kv_value(&DaemonDefaultDfsAdvertisementProjection {
            node_id: &advertisement.node_id,
            capacity_weight: advertisement.capacity_weight,
            accepts_metadata: advertisement.accepts_metadata,
            accepts_content: advertisement.accepts_content,
            zone: &advertisement.zone,
        })
        .unwrap_or_else(|_| entry.value.clone());
    }
    entry.value.clone()
}

fn daemon_default_dfs_reconciliation_projection(
    snapshot: &KvSnapshot,
    _local_node_id: &NodeId,
) -> (
    Vec<(String, Vec<u8>)>,
    Option<DaemonDefaultDfsPermitGeneration>,
) {
    let exact_keys = daemon_default_dfs_reconciliation_exact_input_keys();
    let permit_key = sinomo_dfs::placement_authority_permit_key(&sinomo_dfs::sifs_file_system_id());
    let mut inputs = Vec::new();
    for key in exact_keys {
        if let Some(entry) = snapshot.get(key) {
            inputs.push((
                entry.key.clone(),
                daemon_default_dfs_reconciliation_input_value(entry),
            ));
        }
    }
    // `KvSnapshot` is canonical, so prefix iterators jump to the first key by
    // binary search. The committed-snapshot callback therefore scales with
    // the bounded relevant projection rather than scanning unrelated KV data.
    for prefix in [
        DAEMON_DEFAULT_DFS_MEMBER_PREFIX,
        DAEMON_DEFAULT_DFS_ENDPOINT_PREFIX,
        DAEMON_DEFAULT_DFS_CONNECTIVITY_PREFIX,
        sinomo_dfs::metadata_services_prefix(),
        sinomo_dfs::content_services_prefix(),
    ] {
        for entry in snapshot.prefix_entries(prefix) {
            inputs.push((
                entry.key.clone(),
                daemon_default_dfs_reconciliation_input_value(entry),
            ));
        }
    }
    inputs.sort_by(|left, right| left.0.cmp(&right.0));
    let placement_authority_permit =
        snapshot
            .get(&permit_key)
            .map(|entry| DaemonDefaultDfsPermitGeneration {
                modify_revision: entry.modify_revision,
                version: entry.version,
                value: entry.value.clone(),
            });
    (inputs, placement_authority_permit)
}

#[cfg(test)]
fn observe_daemon_default_dfs_reconciliation(
    projection_tx: &tokio::sync::watch::Sender<DaemonDefaultDfsReconciliationProjection>,
    local_node_id: &NodeId,
    snapshot: &KvSnapshot,
) {
    observe_daemon_default_dfs_reconciliation_with_replica_subscription(
        projection_tx,
        local_node_id,
        snapshot,
        None,
    );
}

fn observe_daemon_default_dfs_reconciliation_with_replica_subscription(
    projection_tx: &tokio::sync::watch::Sender<DaemonDefaultDfsReconciliationProjection>,
    local_node_id: &NodeId,
    snapshot: &KvSnapshot,
    parked_handoff: Option<&DaemonDefaultDfsParkedHandoff>,
) {
    let (inputs, placement_authority_permit) =
        daemon_default_dfs_reconciliation_projection(snapshot, local_node_id);
    projection_tx.send_if_modified(|observed| {
        if observed
            .revision
            .is_some_and(|revision| snapshot.revision < revision)
        {
            return false;
        }
        let changed = match parked_handoff {
            Some(parked) => {
                parked.wake_inputs(&observed.inputs) != parked.wake_inputs(&inputs)
                    || observed.placement_authority_permit != placement_authority_permit
            }
            None => {
                observed.inputs != inputs
                    || observed.placement_authority_permit != placement_authority_permit
            }
        };
        observed.revision = Some(snapshot.revision);
        if changed {
            observed.inputs.clone_from(&inputs);
            observed
                .placement_authority_permit
                .clone_from(&placement_authority_permit);
        }
        changed
    });
}

fn daemon_default_dfs_reconciliation_key_is_relevant(key: &str) -> bool {
    daemon_default_dfs_reconciliation_exact_input_keys().contains(key)
        || key == sinomo_dfs::placement_authority_permit_key(&sinomo_dfs::sifs_file_system_id())
        || key.starts_with(DAEMON_DEFAULT_DFS_MEMBER_PREFIX)
        || key.starts_with(DAEMON_DEFAULT_DFS_ENDPOINT_PREFIX)
        || key.starts_with(DAEMON_DEFAULT_DFS_CONNECTIVITY_PREFIX)
        || key.starts_with(sinomo_dfs::metadata_services_prefix())
        || key.starts_with(sinomo_dfs::content_services_prefix())
}

/// Incremental committed-snapshot observer for the default DFS reactor.
///
/// The resident snapshot is rebuilt only at startup/compaction or when an
/// event touches the bounded semantic projection. Revisions containing only
/// unrelated KV work advance the cursor without decoding, cloning, sorting,
/// or reconstructing connectivity inputs.
#[derive(Debug, Default)]
struct DaemonDefaultDfsCommittedProjectionObserver {
    revision: Option<KvRevision>,
    parked_subscription: Option<Arc<Mutex<Option<DaemonDefaultDfsParkedHandoff>>>>,
    #[cfg(test)]
    rebuild_count: usize,
}

impl DaemonDefaultDfsCommittedProjectionObserver {
    fn new_with_parked_subscription(
        _data_dir: SinomoDataDir,
        parked_subscription: Arc<Mutex<Option<DaemonDefaultDfsParkedHandoff>>>,
    ) -> Self {
        Self {
            parked_subscription: Some(parked_subscription),
            ..Self::default()
        }
    }

    fn parked_handoff(&self) -> Option<DaemonDefaultDfsParkedHandoff> {
        self.parked_subscription
            .as_ref()
            .and_then(|parked| parked.lock().ok()?.clone())
    }

    fn observe(
        &mut self,
        projection_tx: &tokio::sync::watch::Sender<DaemonDefaultDfsReconciliationProjection>,
        local_node_id: &NodeId,
        snapshot: &KvSnapshot,
    ) {
        let Some(previous_revision) = self.revision else {
            self.rebuild(projection_tx, local_node_id, snapshot);
            return;
        };
        if snapshot.revision <= previous_revision {
            return;
        }
        if previous_revision < snapshot.compacted_revision {
            self.rebuild(projection_tx, local_node_id, snapshot);
            return;
        }

        let first_new = snapshot
            .events
            .partition_point(|event| event.revision <= previous_revision);
        let parked_handoff = self.parked_handoff();
        let relevant_change = snapshot.events[first_new..].iter().any(|event| {
            parked_handoff.as_ref().map_or_else(
                || daemon_default_dfs_reconciliation_key_is_relevant(&event.key),
                |parked| {
                    parked.owns_key(&event.key)
                        || event.key
                            == sinomo_dfs::placement_authority_permit_key(
                                &sinomo_dfs::sifs_file_system_id(),
                            )
                },
            )
        });
        self.revision = Some(snapshot.revision);
        if relevant_change {
            self.rebuild_projection(projection_tx, local_node_id, snapshot);
        } else if let Some(parked) = parked_handoff.as_ref() {
            self.apply_parked_projection_deltas(projection_tx, snapshot, first_new, parked);
        } else {
            projection_tx.send_if_modified(|observed| {
                if observed
                    .revision
                    .is_some_and(|revision| snapshot.revision <= revision)
                {
                    return false;
                }
                observed.revision = Some(snapshot.revision);
                false
            });
        }
    }

    fn apply_parked_projection_deltas(
        &self,
        projection_tx: &tokio::sync::watch::Sender<DaemonDefaultDfsReconciliationProjection>,
        snapshot: &KvSnapshot,
        first_new: usize,
        parked: &DaemonDefaultDfsParkedHandoff,
    ) {
        let permit_key =
            sinomo_dfs::placement_authority_permit_key(&sinomo_dfs::sifs_file_system_id());
        projection_tx.send_if_modified(|observed| {
            if observed
                .revision
                .is_some_and(|revision| snapshot.revision <= revision)
            {
                return false;
            }
            let previous_wake_inputs = parked.wake_inputs(&observed.inputs);
            let previous_permit = observed.placement_authority_permit.clone();
            for event in &snapshot.events[first_new..] {
                if event.key == permit_key {
                    observed.placement_authority_permit =
                        snapshot
                            .get(&permit_key)
                            .map(|entry| DaemonDefaultDfsPermitGeneration {
                                modify_revision: entry.modify_revision,
                                version: entry.version,
                                value: entry.value.clone(),
                            });
                    continue;
                }
                if !daemon_default_dfs_reconciliation_key_is_relevant(&event.key) {
                    continue;
                }
                let position = observed
                    .inputs
                    .binary_search_by(|(key, _)| key.as_str().cmp(event.key.as_str()));
                match snapshot.get(&event.key) {
                    Some(entry) => {
                        let value = daemon_default_dfs_reconciliation_input_value(entry);
                        match position {
                            Ok(index) => observed.inputs[index].1 = value,
                            Err(index) => observed.inputs.insert(index, (event.key.clone(), value)),
                        }
                    }
                    None => {
                        if let Ok(index) = position {
                            observed.inputs.remove(index);
                        }
                    }
                }
            }
            observed.revision = Some(snapshot.revision);
            previous_wake_inputs != parked.wake_inputs(&observed.inputs)
                || previous_permit != observed.placement_authority_permit
        });
    }

    fn rebuild(
        &mut self,
        projection_tx: &tokio::sync::watch::Sender<DaemonDefaultDfsReconciliationProjection>,
        local_node_id: &NodeId,
        snapshot: &KvSnapshot,
    ) {
        self.revision = Some(snapshot.revision);
        self.rebuild_projection(projection_tx, local_node_id, snapshot);
    }

    fn rebuild_projection(
        &mut self,
        projection_tx: &tokio::sync::watch::Sender<DaemonDefaultDfsReconciliationProjection>,
        local_node_id: &NodeId,
        snapshot: &KvSnapshot,
    ) {
        #[cfg(test)]
        {
            self.rebuild_count = self.rebuild_count.saturating_add(1);
        }
        let parked_handoff = self.parked_handoff();
        observe_daemon_default_dfs_reconciliation_with_replica_subscription(
            projection_tx,
            local_node_id,
            snapshot,
            parked_handoff.as_ref(),
        );
    }
}

async fn daemon_default_dfs_reconciliation_local_is_voter(
    paths: &DaemonPaths,
    expected_cluster_id: &sinomo_core::Id,
    expected_node_id: &NodeId,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<bool>, DaemonError> {
    let task_paths = paths.clone();
    let expected_cluster_id = expected_cluster_id.clone();
    let expected_node_id = expected_node_id.clone();
    runtime
        .spawn_blocking(move || {
            let data_dir = SinomoDataDir::new(daemon_data_dir(&task_paths)?);
            let cluster_paths = ClusterPaths::new(&data_dir);
            let state = sinomo_core::cluster::read_cluster_consensus_state(&cluster_paths)?;
            if state.cluster_id != expected_cluster_id || state.local_node_id != expected_node_id {
                return Ok(None);
            }
            Ok(Some(sinomo_core::kv::cluster_kv_raft_local_is_voter(
                &state,
            )))
        })
        .await
        .map_err(|source| DaemonError::Network {
            address: "default DFS reconciliation identity".to_string(),
            detail: format!("default DFS identity task failed: {source}"),
        })?
}

async fn recover_prior_process_default_dfs_placement_authority_async_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    refresh_latest: bool,
) -> Result<bool, DaemonError> {
    let task_paths = paths.clone();
    let task_runtime = runtime.clone();
    runtime
        .spawn_blocking(move || {
            recover_prior_process_default_dfs_placement_authority_from_observation(
                &task_paths,
                &task_runtime,
                refresh_latest,
            )
        })
        .await
        .map_err(|source| DaemonError::Network {
            address: "default DFS placement-authority recovery".to_string(),
            detail: format!("default DFS recovery task failed: {source}"),
        })?
}

fn daemon_default_dfs_reconciliation_retry_delay(failure_streak: u32) -> Duration {
    let shift = failure_streak.saturating_sub(1).min(6);
    DAEMON_DEFAULT_DFS_RECONCILIATION_RETRY_INITIAL
        .saturating_mul(1_u32.checked_shl(shift).unwrap_or(64))
        .min(DAEMON_DEFAULT_DFS_RECONCILIATION_RETRY_MAX)
}

pub(crate) async fn run_daemon_default_dfs_reconciliation_reactor(
    mut reactor: DaemonDefaultDfsReconciliationReactor,
    paths: DaemonPaths,
    options: DaemonRunOptions,
    expected_cluster_id: sinomo_core::Id,
    expected_node_id: NodeId,
    runtime: tokio::runtime::Handle,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let mut failure_streak = 0_u32;
    loop {
        if *shutdown_rx.borrow() {
            return;
        }
        match reactor
            .reconcile_pending_async(
                &paths,
                &options,
                &expected_cluster_id,
                &expected_node_id,
                &runtime,
            )
            .await
        {
            Ok(DaemonDefaultDfsReconciliationStep::Progress) => {
                failure_streak = 0;
                continue;
            }
            Ok(DaemonDefaultDfsReconciliationStep::Parked) => {
                failure_streak = 0;
                // A durable handoff is waiting on exact state owners, not a
                // failed reconciliation attempt. It has no retry timer: only
                // the handoff/proof/shard generation or subscribed replica
                // progress can make the parked work runnable again.
                tokio::select! {
                    _ = reactor.changed() => {}
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            return;
                        }
                    }
                }
            }
            Ok(DaemonDefaultDfsReconciliationStep::Idle) => {
                failure_streak = 0;
                tokio::select! {
                    _ = reactor.changed() => {}
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            return;
                        }
                    }
                }
            }
            Ok(DaemonDefaultDfsReconciliationStep::IdentityChanged) => {
                tracing::info!(
                    target: "sinomo.dfs.maintenance",
                    "default DFS reconciliation stopped after daemon identity changed"
                );
                return;
            }
            Err(error) => {
                failure_streak = failure_streak.saturating_add(1);
                let retry_delay = daemon_default_dfs_reconciliation_retry_delay(failure_streak);
                tracing::warn!(
                    target: "sinomo.dfs.maintenance",
                    failure_streak,
                    retry_delay_ms = retry_delay.as_millis().min(u128::from(u64::MAX)) as u64,
                    "default DFS reconciliation failed; retrying after source change or backoff: {error}"
                );
                // A timer exists only while real work is failing. Preserve its
                // absolute deadline across ordinary placement/connectivity
                // churn: only an exact handoff, proof, shard-authority, permit,
                // or subscribed replica generation can make the failed work
                // runnable before that deadline.
                let failed_generation = reactor.exact_retry_generation();
                let retry_deadline = tokio::time::Instant::now() + retry_delay;
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep_until(retry_deadline) => break,
                        _ = reactor.changed_signal() => {
                            if reactor.exact_retry_generation() != failed_generation {
                                break;
                            }
                        }
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                return;
                            }
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DaemonDefaultDfsLifecycleOutcome {
    Completed,
    Parked(DaemonDefaultDfsParkedHandoff),
}

#[cfg(test)]
pub(crate) async fn ensure_default_distributed_dfs_async_on_runtime(
    paths: &DaemonPaths,
    options: &DaemonRunOptions,
    publish_cluster_control: bool,
    runtime: tokio::runtime::Handle,
) -> Result<(), DaemonError> {
    match ensure_default_distributed_dfs_outcome_async_on_runtime(
        paths,
        options,
        publish_cluster_control,
        runtime,
    )
    .await?
    {
        DaemonDefaultDfsLifecycleOutcome::Completed => Ok(()),
        DaemonDefaultDfsLifecycleOutcome::Parked(handoff) => {
            Err(sinomo_dfs::DfsError::MetadataShardMembershipHandoffParked {
                file_system_id: handoff.file_system_id,
                shard_id: handoff.shard_id.0,
                handoff_id: handoff.handoff_id,
                retiring_epoch: handoff.retiring_epoch,
                desired_epoch: handoff.desired_epoch,
            }
            .into())
        }
    }
}

async fn ensure_default_distributed_dfs_outcome_async_on_runtime(
    paths: &DaemonPaths,
    options: &DaemonRunOptions,
    publish_cluster_control: bool,
    runtime: tokio::runtime::Handle,
) -> Result<DaemonDefaultDfsLifecycleOutcome, DaemonError> {
    #[cfg(test)]
    record_daemon_default_dfs_reconciliation_run(paths);
    let data_dir = daemon_data_dir(paths)?;
    let lifecycle_gate = daemon_default_dfs_lifecycle_gate(data_dir)?;
    // The dedicated reactor is the sole owner of default reconciliation. Wait
    // behind an active backend-bearing maintenance pass instead of treating
    // overlap as success and losing a committed input generation.
    let lifecycle_guard = lifecycle_gate.lock_owned().await;
    let message_execution_gate = daemon_dfs_message_execution_gate(&SinomoDataDir::new(data_dir))?;
    let message_execution_guard = Arc::clone(&message_execution_gate.semaphore)
        .acquire_owned()
        .await
        .map_err(|_| DaemonError::Network {
            address: "DFS message execution".to_string(),
            detail: "DFS message execution gate closed".to_string(),
        })?;
    // Own the permit inside the blocking task that polls the complete lifecycle
    // future. Dropping the JoinHandle cannot cancel a running blocking task, so
    // neither guard can be released while backend-bearing DFS work still runs.
    let task_paths = paths.clone();
    let task_options = options.clone();
    let task_runtime = runtime.clone();
    let task_data_owner_lock = daemon_dfs_data_owner_lock(paths)?;
    let lifecycle_result = runtime
        .spawn_blocking(move || {
            let _data_owner_lock = task_data_owner_lock;
            let _lifecycle_guard = lifecycle_guard;
            let _message_execution_guard = message_execution_guard;
            task_runtime.block_on(ensure_default_distributed_dfs_lifecycle_inner(
                &task_paths,
                &task_options,
                publish_cluster_control,
                task_runtime.clone(),
            ))
        })
        .await
        .map_err(|source| DaemonError::Network {
            address: "default DFS lifecycle".to_string(),
            detail: format!("default DFS lifecycle task failed: {source}"),
        })?;
    let lifecycle_outcome = lifecycle_result?;

    let data_dir = SinomoDataDir::new(data_dir);
    if sinomo_dfs::take_default_cluster_dfs_materialization_immediate_retry(&data_dir) {
        notify_daemon_default_dfs_reconciliation_replica_progress(&ClusterPaths::new(&data_dir));
    }

    // Default materialization and handoff activation append directly to local
    // primary journals rather than passing through a served-message event. A
    // lifecycle pass can append those entries and then park on the next exact
    // handoff proof, so wake the node-local replication lanes before returning
    // either outcome. Authority-change discovery may already have run before
    // these entries existed and is not a substitute for this committed edge.
    schedule_daemon_dfs_metadata_publish_on_runtime(paths, &runtime, None)?;
    Ok(lifecycle_outcome)
}

async fn ensure_default_distributed_dfs_lifecycle_inner(
    paths: &DaemonPaths,
    _options: &DaemonRunOptions,
    publish_cluster_control: bool,
    runtime: tokio::runtime::Handle,
) -> Result<DaemonDefaultDfsLifecycleOutcome, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let control_plane = Box::new(DaemonDfsControlPlaneKvClient::new_on_runtime(
        &cluster_paths,
        &runtime,
    ));
    match sinomo_dfs::ensure_default_cluster_dfs_async(
        data_dir,
        cluster_paths,
        control_plane,
        DfsDefaultClusterBootstrapOptions {
            publish_cluster_control,
            materialize_local_metadata: true,
            local_zone: daemon_dfs_local_zone(),
        },
    )
    .await
    {
        Ok(_) => Ok(DaemonDefaultDfsLifecycleOutcome::Completed),
        Err(sinomo_dfs::DfsError::MetadataShardMembershipHandoffParked {
            file_system_id,
            shard_id,
            handoff_id,
            retiring_epoch,
            desired_epoch,
        }) => Ok(DaemonDefaultDfsLifecycleOutcome::Parked(
            DaemonDefaultDfsParkedHandoff::new(
                file_system_id,
                shard_id,
                handoff_id,
                retiring_epoch,
                desired_epoch,
            ),
        )),
        Err(error) => Err(error.into()),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DaemonDfsMaintenanceTickOutcome {
    Completed { reached_limit: bool },
    Deferred,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DaemonDfsMaintenanceKind {
    Routine,
    ExplicitDisasterRecovery {
        content_gc_requested: bool,
        content_reservation_reconciliation_requested: bool,
        commit_promotion_recovery_requested: bool,
    },
}

#[derive(Debug)]
enum DaemonDfsMaintenancePreemptionOutcome<T> {
    Finished(T),
    ForegroundWaiting,
}

async fn run_daemon_dfs_maintenance_until_foreground_waits<F>(
    foreground_preemption: &DaemonDfsMaintenanceForegroundPreemption,
    maintenance: F,
) -> DaemonDfsMaintenancePreemptionOutcome<F::Output>
where
    F: std::future::Future,
{
    let mut maintenance = Box::pin(maintenance);
    let outcome = tokio::select! {
        biased;
        _ = foreground_preemption.requested() => {
            DaemonDfsMaintenancePreemptionOutcome::ForegroundWaiting
        }
        result = &mut maintenance => DaemonDfsMaintenancePreemptionOutcome::Finished(result),
    };
    // A preempted or failed future owns and destroys the broad tick's
    // `NetworkedDfsService` here. A successful future may return a reset,
    // backend-bearing lane to the cache before this sole message-execution
    // permit is released, so foreground admission still cannot overlap a live
    // maintenance generation.
    drop(maintenance);
    outcome
}

pub(crate) async fn run_daemon_dfs_maintenance_tick_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<DaemonDfsMaintenanceTickOutcome, DaemonError> {
    run_daemon_dfs_maintenance_tick_kind_on_runtime(
        paths,
        runtime,
        DaemonDfsMaintenanceKind::Routine,
    )
    .await
}

pub(crate) async fn run_daemon_dfs_repair_queue_recovery_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<crate::DaemonDfsRepairQueueRecoveryResponse, DaemonError> {
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    tracing::warn!(
        target: "sinomo.dfs.fault_healing",
        data_root = %data_dir.root().display(),
        "operator requested explicit full-metadata DFS repair queue recovery"
    );
    let outcome = run_daemon_dfs_maintenance_tick_kind_on_runtime(
        paths,
        runtime,
        DaemonDfsMaintenanceKind::ExplicitDisasterRecovery {
            content_gc_requested: false,
            content_reservation_reconciliation_requested: false,
            commit_promotion_recovery_requested: false,
        },
    )
    .await?;
    let remaining_queued_repairs =
        read_dfs_repair_attempt_snapshot(&default_dfs_repair_attempt_snapshot_path(&data_dir))?
            .map(|snapshot| snapshot.records.len())
            .unwrap_or_default();
    let (completed, reached_limit) = match outcome {
        DaemonDfsMaintenanceTickOutcome::Completed { reached_limit } => (true, reached_limit),
        DaemonDfsMaintenanceTickOutcome::Deferred => (false, false),
    };
    if remaining_queued_repairs > 0 || !completed {
        notify_daemon_dfs_maintenance_work();
    }
    tracing::warn!(
        target: "sinomo.dfs.fault_healing",
        data_root = %data_dir.root().display(),
        completed,
        reached_limit,
        remaining_queued_repairs,
        "explicit full-metadata DFS repair queue recovery finished"
    );
    Ok(crate::DaemonDfsRepairQueueRecoveryResponse {
        completed,
        reached_limit,
        remaining_queued_repairs,
    })
}

async fn run_daemon_dfs_maintenance_tick_kind_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    maintenance_kind: DaemonDfsMaintenanceKind,
) -> Result<DaemonDfsMaintenanceTickOutcome, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(DaemonDfsMaintenanceTickOutcome::Completed {
            reached_limit: false,
        });
    }
    // Publish maintenance interest before cluster/local admission. A
    // foreground request that queues anywhere behind this pass can then ask it
    // to yield once its current synchronous poll reaches an await boundary.
    let foreground_preemption =
        daemon_dfs_message_execution_gate(&data_dir)?.begin_maintenance_interest();
    let admission = acquire_daemon_dfs_maintenance_admission(&data_dir).await?;
    let DaemonDfsMaintenanceAdmission {
        _lifecycle_guard: lifecycle_guard,
        _message_execution_guard: message_execution_guard,
    } = admission;
    let mut deferred_work = None;
    let committed_work_tracker = DfsCommittedWorkTracker::new();
    let maintenance_result =
        run_daemon_dfs_maintenance_until_foreground_waits(&foreground_preemption, async {
            run_daemon_dfs_maintenance_tick_with_admission(
                data_dir.clone(),
                cluster_paths.clone(),
                runtime,
                maintenance_kind,
                committed_work_tracker.clone(),
                &mut deferred_work,
                &foreground_preemption,
            )
            .await
        })
        .await;
    // The preemption helper has already completed or dropped the entire
    // maintenance future before this permit is released. On success it may
    // have restored a reset backend-bearing request lane; on preemption/error
    // the owned lane was destroyed. Local mutation therefore remains
    // single-generation even when a repair or metadata RPC is cancelled to
    // admit foreground work. Those sends are replay-safe; return Deferred
    // below so durable work is retried promptly.
    drop(message_execution_guard);
    drop(lifecycle_guard);
    drop(foreground_preemption);
    if let Some((detached_runtime, worker)) = deferred_work {
        schedule_daemon_dfs_message_deferred_work(
            runtime,
            &cluster_paths,
            detached_runtime,
            worker,
        );
    }
    // The tracker is owned outside the cancellable maintenance future and is
    // updated at each durable mutation boundary. Transfer those exact bits
    // only after destroying the backend-bearing generation and dropping its
    // guards, even when a later maintenance step failed or was preempted.
    emit_daemon_dfs_committed_edges(
        &data_dir,
        &cluster_paths,
        &DfsMessageExecutionScope::GlobalExclusive,
        runtime,
        None,
        committed_work_tracker.take(),
        &[],
    );
    release_daemon_dfs_idle_allocator_pages();
    let outcome = match maintenance_result {
        DaemonDfsMaintenancePreemptionOutcome::Finished(result) => {
            let reached_limit = result?;
            DaemonDfsMaintenanceTickOutcome::Completed { reached_limit }
        }
        DaemonDfsMaintenancePreemptionOutcome::ForegroundWaiting => {
            DaemonDfsMaintenanceTickOutcome::Deferred
        }
    };
    if let Err(error) = refresh_daemon_dfs_repair_queue_nonempty(&data_dir) {
        // Retain fail-open wake eligibility. A queue whose snapshot cannot be
        // observed must never be mistaken for proof that repair work is empty.
        record_daemon_dfs_repair_queue_nonempty(&data_dir, true);
        tracing::warn!(
            target: "sinomo.dfs.fault_healing",
            data_root = %data_dir.root().display(),
            error = %error,
            "could not refresh bounded DFS repair queue state after maintenance"
        );
    }
    Ok(outcome)
}

async fn run_daemon_dfs_maintenance_tick_with_admission(
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    runtime: &tokio::runtime::Handle,
    maintenance_kind: DaemonDfsMaintenanceKind,
    committed_work_tracker: DfsCommittedWorkTracker,
    _deferred_work: &mut Option<(Arc<DaemonDfsDetachedSendRuntime>, DfsNodeDetachedSendWorker)>,
    _foreground_preemption: &DaemonDfsMaintenanceForegroundPreemption,
) -> Result<bool, DaemonError> {
    // The scheduler's retained read-only image is useful between probes, but a
    // broad writable generation will decode the same tables. Release it first
    // so accumulated metadata does not double peak RSS during recovery.
    sinomo_dfs::evict_cluster_aware_dfs_metadata_inspection_cache(&data_dir);
    if let Some(stats) = crate::process_idle_stats() {
        stats.incr_disk_reads_dfs_metadata();
    }
    let peer_transport = daemon_dfs_blocking_peer_transport_on_runtime(&data_dir, runtime);
    let cluster_state = sinomo_dfs::refresh_cluster_dfs_maintenance_state_with_peer_transport(
        runtime,
        &cluster_paths,
        peer_transport.as_ref(),
    )
    .await
    .map_err(DaemonError::from)?;
    let execution_exclusions =
        daemon_dfs_maintenance_execution_exclusions(&cluster_paths, &cluster_state);
    // Transfer the mutation lane only after refreshing KV and re-reading the
    // admitted cluster identity. The owned lane remains inside this
    // cancellable future, so preemption destroys it before the execution
    // permit can be released.
    let retained_lane = take_daemon_dfs_message_mutation_service_for_maintenance(
        &data_dir,
        &cluster_paths,
        &cluster_state,
        runtime,
    )
    .map_err(DaemonError::from)?;
    let DaemonDfsMaintenanceServiceLane {
        signature,
        metadata_authority_generation,
        peer_node_ids,
        lane,
        detached_send_runtime,
    } = retained_lane;
    let DaemonDfsMessageServiceLane {
        service: mut maintenance_service,
        metadata_hydrated: service_was_fully_hydrated,
        ..
    } = lane;
    let authority_generation_before_maintenance = metadata_authority_generation.generation();
    let async_peer_transport = daemon_dfs_async_peer_transport(&data_dir, runtime);
    let maintenance_result = match maintenance_kind {
        DaemonDfsMaintenanceKind::Routine => {
            sinomo_dfs::run_cluster_dfs_reactive_maintenance_tick_with_reusable_service(
                &mut maintenance_service,
                data_dir.clone(),
                cluster_paths.clone(),
                cluster_state,
                async_peer_transport,
                execution_exclusions,
                committed_work_tracker,
                daemon_dfs_routine_maintenance_tick_options(),
            )
            .await
            .map(Some)
            .map_err(DaemonError::from)
        }
        DaemonDfsMaintenanceKind::ExplicitDisasterRecovery {
            content_gc_requested,
            content_reservation_reconciliation_requested,
            ..
        } => {
            sinomo_dfs::run_cluster_dfs_maintenance_tick_with_reusable_service_until_hydration_cancelled(
                &mut maintenance_service,
                data_dir.clone(),
                cluster_paths.clone(),
                cluster_state,
                async_peer_transport,
                execution_exclusions,
                committed_work_tracker,
                &|| {
                    _foreground_preemption.is_requested()
                        || metadata_authority_generation.generation()
                            != authority_generation_before_maintenance
                },
                daemon_dfs_explicit_disaster_recovery_tick_options(
                    content_gc_requested,
                    content_reservation_reconciliation_requested,
                ),
            )
            .await
            .map_err(DaemonError::from)
        }
    };
    match &maintenance_result {
        Ok(Some(_))
            if matches!(
                maintenance_kind,
                DaemonDfsMaintenanceKind::ExplicitDisasterRecovery { .. }
            ) =>
        {
            let authority_generation_after_maintenance = metadata_authority_generation.generation();
            if authority_generation_after_maintenance == authority_generation_before_maintenance {
                // The broad maintenance service already performed the explicit
                // one-pass recovery hydrate into this shared outbox. Mark only
                // the exact unchanged topology it scanned; recovery preparation
                // below can wake the outbox without repeating the full scan.
                detached_send_runtime
                    .recovery_scan_generation
                    .store(authority_generation_after_maintenance, Ordering::Release);
            }
        }
        Err(_)
            if matches!(
                maintenance_kind,
                DaemonDfsMaintenanceKind::ExplicitDisasterRecovery { .. }
            ) =>
        {
            // A disaster-recovery hydrate can repair a durable intent before
            // failing later. Preserve that explicit recovery obligation.
            detached_send_runtime
                .recovery_scan_generation
                .store(0, Ordering::Release);
        }
        Ok(Some(_)) | Ok(None) | Err(_) => {}
    }
    // Routine maintenance is deliberately reactive and must retain the exact
    // request-lane state which owns queued repair retries. An explicit
    // disaster-recovery hydrate, by contrast, is collapsed back to a lazy
    // shell so its complete projection is never retained during normal work.
    let reusable_service = if matches!(&maintenance_result, Ok(Some(_))) {
        match maintenance_service.operational_metrics() {
            Ok(metrics) if metrics.authoritative_write_frozen_shard_count == 0 => {
                let reset_to_lazy = service_was_fully_hydrated;
                let reset_to_lazy = reset_to_lazy
                    || matches!(
                        maintenance_kind,
                        DaemonDfsMaintenanceKind::ExplicitDisasterRecovery { .. }
                    );
                if reset_to_lazy {
                    match maintenance_service.into_lazy_metadata_backend_service() {
                        Ok(service) => Some(service),
                        Err(error) => {
                            tracing::warn!(
                                data_root = %data_dir.root().display(),
                                "dropping daemon DFS disaster-recovery service after lazy reset failed: {error}"
                            );
                            None
                        }
                    }
                } else {
                    Some(maintenance_service)
                }
            }
            Ok(metrics) => {
                tracing::info!(
                    data_root = %data_dir.root().display(),
                    frozen_shard_count = metrics.authoritative_write_frozen_shard_count,
                    "dropping daemon DFS maintenance service while an authoritative shard is frozen"
                );
                drop(maintenance_service);
                None
            }
            Err(error) => {
                tracing::warn!(
                    data_root = %data_dir.root().display(),
                    "dropping daemon DFS maintenance service after frozen-shard validation failed: {error}"
                );
                drop(maintenance_service);
                None
            }
        }
    } else {
        drop(maintenance_service);
        None
    };
    if matches!(
        maintenance_kind,
        DaemonDfsMaintenanceKind::ExplicitDisasterRecovery {
            commit_promotion_recovery_requested: true,
            ..
        }
    ) {
        let promotion_recovery_started = Instant::now();
        match prepare_daemon_dfs_commit_promotion_recovery(&data_dir, &cluster_paths, runtime) {
            Ok(DaemonDfsCommitPromotionRecovery {
                detached_runtime,
                worker,
                scan_result,
            }) => {
                *_deferred_work = Some((detached_runtime, worker));
                if let Err(error) = scan_result {
                    tracing::warn!(
                        "sinomo daemon DFS commit-promotion disaster-recovery scan remains pending: {error}"
                    );
                }
            }
            Err(error) => {
                tracing::warn!(
                    "sinomo daemon DFS commit-promotion disaster recovery remains pending: {error}"
                );
            }
        }
        daemon_dfs_perf_trace(format_args!(
            "commit-promotion-disaster-recovery maintenance_ms={}",
            promotion_recovery_started.elapsed().as_millis()
        ));
    }
    if let Some(service) = reusable_service {
        let retained = DaemonDfsMaintenanceServiceLane {
            signature,
            metadata_authority_generation,
            peer_node_ids: peer_node_ids.clone(),
            lane: DaemonDfsMessageServiceLane {
                service,
                peer_node_ids,
                last_metadata_refresh: Instant::now(),
                metadata_hydrated: false,
            },
            detached_send_runtime,
        };
        match restore_daemon_dfs_message_mutation_service_after_maintenance(
            &data_dir,
            &cluster_paths,
            runtime,
            retained,
        ) {
            Ok(true) => tracing::debug!(
                data_root = %data_dir.root().display(),
                "restored daemon DFS mutation service after maintenance"
            ),
            Ok(false) => tracing::info!(
                data_root = %data_dir.root().display(),
                "dropped daemon DFS mutation service after maintenance identity changed"
            ),
            Err(error) => tracing::warn!(
                data_root = %data_dir.root().display(),
                "dropping daemon DFS mutation service after maintenance validation failed: {error}"
            ),
        }
    }
    let report = maintenance_result?;
    let Some(report) = report else {
        return Ok(false);
    };
    let reached_limit = daemon_dfs_maintenance_report_reached_limit(&report);
    let observations = sinomo_dfs::dfs_maintenance_tick_observations(&report);
    tracing::debug!(
        target: "sinomo.dfs.maintenance",
        metadata_replications_sent = observations.metadata_replications_sent,
        repair_attempt_count = observations.repair_attempt_count,
        repair_attempt_in_flight_count = observations.repair_attempt_in_flight_count,
        repair_attempt_backoff_count = observations.repair_attempt_backoff_count,
        repair_attempt_suppressed_count = observations.repair_attempt_suppressed_count,
        scheduled_repair_count = report.scheduled_repairs.len(),
        followup_event_count = report.followup_events.len(),
        namespace_jobs_scanned = report.namespace_jobs.scanned_job_count,
        namespace_jobs_resumed = report.namespace_jobs.resumed_job_count,
        bounded_continuation = reached_limit,
        "DFS maintenance tick completed"
    );
    if let Some(stats) = crate::process_idle_stats() {
        for _ in 0..observations.metadata_replications_sent {
            stats.incr_dfs_metadata_replications_sent();
        }
    }
    for line in observations.notice_lines {
        tracing::warn!("sinomo daemon {line}");
    }
    Ok(reached_limit)
}

/// Whether this pass itself proved that it stopped with bounded, immediately
/// continuable work.
///
/// Hitting a scan count is not enough: an exact-sized workload and a newly
/// arrived record are indistinguishable from counts alone. Likewise, a
/// running namespace job may have made no progress while waiting on external
/// state. Only durable cursors or explicit retained-limit reports, paired with
/// evidence that this pass advanced, may bypass the ordinary maintenance gate.
fn daemon_dfs_maintenance_report_reached_limit(report: &DfsMaintenanceTickReport) -> bool {
    let terminal_namespace_jobs = report
        .namespace_jobs
        .completed_job_count
        .saturating_add(report.namespace_jobs.failed_job_count);
    let resumed_namespace_jobs_still_running = report
        .namespace_jobs
        .resumed_job_count
        .saturating_sub(terminal_namespace_jobs);
    let skipped_namespace_jobs = report
        .namespace_jobs
        .remaining_running_job_count
        .saturating_sub(resumed_namespace_jobs_still_running);
    let namespace_job_limit_reached = terminal_namespace_jobs > 0 && skipped_namespace_jobs > 0;

    let placement_page_remaining = report.placement_reconciliation.as_ref().is_some_and(|job| {
        job.last_error.is_none()
            && job.cursor.is_some()
            && matches!(
                job.state,
                DfsPlacementReconciliationState::Scanning
                    | DfsPlacementReconciliationState::Repairing
            )
    });

    let lifecycle_limit_reached = report.lifecycle_reports.iter().any(|lifecycle| {
        lifecycle
            .stale_staging_gc
            .as_ref()
            .is_some_and(|gc| gc.max_deletions_reached && gc.deleted_staging_count > 0)
            || lifecycle
                .version_gc
                .as_ref()
                .is_some_and(|gc| gc.max_deletions_reached && gc.deleted_version_count > 0)
            || lifecycle.orphan_gc.as_ref().is_some_and(|gc| {
                gc.max_deletions_reached
                    && gc
                        .deleted_orphan_count
                        .saturating_add(gc.deleted_stale_orphan_observation_count)
                        .saturating_add(gc.persisted_orphan_observation_count)
                        .saturating_add(gc.compacted_packed_segment_count)
                        > 0
            })
            || lifecycle
                .metadata_history_gc
                .as_ref()
                .is_some_and(|gc| gc.max_removals_reached && gc.applied_record_count() > 0)
            || lifecycle
                .content_reservation_reconciliation
                .as_ref()
                .is_some_and(|reconciliation| {
                    reconciliation.has_more && reconciliation.examined_reservation_count > 0
                })
            || lifecycle
                .tombstone_compaction
                .as_ref()
                .is_some_and(|compaction| {
                    compaction.max_compactions_reached && compaction.compacted_tombstone_count > 0
                })
    });

    namespace_job_limit_reached
        || placement_page_remaining
        || lifecycle_limit_reached
        || report.repair_budget_exhausted
}

/// Run the complete DFS maintenance future from Tokio's blocking pool.
///
/// The reusable maintenance entrypoint is async because metadata replication
/// uses async transports, but it also performs substantial synchronous
/// backend recovery and metadata scans between awaits. Polling that future on
/// a daemon worker can starve unrelated control traffic on slower nodes.
pub(crate) async fn run_daemon_dfs_maintenance_tick_in_blocking_task(
    paths: DaemonPaths,
    runtime: tokio::runtime::Handle,
) -> Result<DaemonDfsMaintenanceTickOutcome, DaemonError> {
    let tick_runtime = runtime.clone();
    let data_owner_lock = daemon_dfs_data_owner_lock(&paths)?;
    tokio::task::spawn_blocking(move || {
        let _data_owner_lock = data_owner_lock;
        runtime.block_on(run_daemon_dfs_maintenance_tick_on_runtime(
            &paths,
            &tick_runtime,
        ))
    })
    .await
    .map_err(|source| DaemonError::Network {
        address: "DFS maintenance".to_string(),
        detail: format!("DFS maintenance blocking task failed: {source}"),
    })?
}

fn daemon_dfs_routine_maintenance_tick_options() -> DfsMaintenanceTickOptions {
    let mut options = DfsMaintenanceTickOptions::default();
    options.repair_max_jobs = DAEMON_DFS_REPAIR_MAX_JOBS_PER_TICK;
    // Routine repair/namespace maintenance consumes bounded authoritative
    // queues. Every lifecycle inventory below can walk O(total metadata or
    // content), so the production daemon tick cannot enable any of them.
    options.lifecycle.repair_metadata = false;
    options.lifecycle.cleanup_stale_staging = false;
    options.lifecycle.compact_tombstones = false;
    options.lifecycle.tombstone_compaction_min_age = DAEMON_DFS_TOMBSTONE_COMPACTION_MIN_AGE;
    options.lifecycle.tombstone_compaction_max_records =
        DAEMON_DFS_TOMBSTONE_COMPACTION_MAX_RECORDS;
    options.lifecycle.collect_metadata_history = false;
    options.lifecycle.metadata_history_gc_min_age = DAEMON_DFS_METADATA_HISTORY_GC_MIN_AGE;
    options.lifecycle.metadata_history_gc_keep_latest_versions =
        DAEMON_DFS_METADATA_HISTORY_GC_KEEP_LATEST_VERSIONS;
    options.lifecycle.metadata_history_gc_max_records = DAEMON_DFS_METADATA_HISTORY_GC_MAX_RECORDS;
    options.lifecycle.collect_content_versions = false;
    options.lifecycle.collect_orphan_content = false;
    options.lifecycle.reconcile_content_reservations = false;
    options
        .lifecycle
        .content_reservation_reconciliation_max_records =
        DAEMON_DFS_CONTENT_RESERVATION_RECONCILIATION_MAX_RECORDS;
    // Metadata cursor ownership belongs exclusively to retained exact-shard
    // publisher lanes. A broad maintenance pass must not race those lanes or
    // duplicate their outbound RPCs.
    options.publish_metadata = false;
    options.metadata_replication = daemon_dfs_metadata_replication_options();
    options
}

fn daemon_dfs_explicit_disaster_recovery_tick_options(
    content_gc_requested: bool,
    content_reservation_reconciliation_requested: bool,
) -> DfsMaintenanceTickOptions {
    let mut options = daemon_dfs_routine_maintenance_tick_options();
    let inventory_requested = content_gc_requested || content_reservation_reconciliation_requested;
    options.lifecycle.repair_metadata = inventory_requested;
    options.lifecycle.cleanup_stale_staging = inventory_requested;
    options.lifecycle.compact_tombstones = inventory_requested;
    options.lifecycle.collect_metadata_history = inventory_requested;
    options.lifecycle.collect_content_versions = content_gc_requested;
    options.lifecycle.collect_orphan_content = content_gc_requested;
    options.lifecycle.reconcile_content_reservations = inventory_requested;
    options
}

fn daemon_dfs_metadata_replication_options() -> DfsMetadataJournalReplicationOptions {
    DfsMetadataJournalReplicationOptions {
        max_batches_per_target: DAEMON_DFS_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET,
        attempt_timeout: DAEMON_DFS_METADATA_REPLICATION_ATTEMPT_TIMEOUT,
        target_timeout: DAEMON_DFS_METADATA_REPLICATION_TARGET_TIMEOUT,
        probe_caught_up_replicas: true,
        ..DfsMetadataJournalReplicationOptions::default()
    }
}

fn daemon_dfs_smoke_options() -> sinomo_dfs::DfsSmokeOptions {
    sinomo_dfs::DfsSmokeOptions {
        stable_path: true,
        remote_replica_candidates: false,
        foreground_replica_repair: false,
        inspect_pending_repairs: false,
        publish_metadata: false,
        metadata_replication: DfsMetadataJournalReplicationOptions {
            max_batches_per_target: 0,
            ..daemon_dfs_metadata_replication_options()
        },
        ..sinomo_dfs::DfsSmokeOptions::default()
    }
}

fn daemon_dfs_metadata_publish_schedule_options() -> DfsMetadataPublishScheduleOptions {
    DfsMetadataPublishScheduleOptions {
        max_retry_rounds: DAEMON_DFS_METADATA_PUBLISH_MAX_RETRY_ROUNDS,
        retry_delay: DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL,
        max_retry_delay: DAEMON_DFS_METADATA_PUBLISH_RETRY_MAX_DELAY,
    }
}

fn daemon_dfs_metadata_publish_discovery_schedule_options() -> DfsMetadataPublishScheduleOptions {
    DfsMetadataPublishScheduleOptions {
        max_retry_rounds: DAEMON_DFS_METADATA_PUBLISH_MAX_RETRY_ROUNDS,
        retry_delay: DAEMON_DFS_METADATA_DISCOVERY_RETRY_INITIAL,
        max_retry_delay: DAEMON_DFS_METADATA_DISCOVERY_RETRY_MAX,
    }
}

fn log_dfs_metadata_replication_report(report: DfsMetadataJournalReplicationReport) {
    for line in sinomo_dfs::dfs_metadata_replication_notice_lines(&report) {
        tracing::warn!("sinomo daemon {line}");
    }
    for line in sinomo_dfs::dfs_metadata_replication_fault_notice_lines(&report) {
        tracing::warn!(
            healing_class = "fault-inconsistency",
            "sinomo daemon {line}"
        );
    }
}

pub(crate) fn schedule_daemon_dfs_metadata_publish_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    traceparent: Option<String>,
) -> Result<DfsMetadataPublishScheduleStart, DaemonError> {
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    Ok(
        schedule_daemon_dfs_metadata_publish_for_data_dir_on_runtime(
            data_dir,
            cluster_paths,
            runtime,
            traceparent,
            None,
        ),
    )
}

fn schedule_daemon_dfs_metadata_publish_for_data_dir_on_runtime(
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    runtime: &tokio::runtime::Handle,
    traceparent: Option<String>,
    exact_lane: Option<DaemonDfsMutationLaneKey>,
) -> DfsMetadataPublishScheduleStart {
    // A served mutation already carries its authoritative filesystem/shard
    // identity. Mark that exact lane dirty before any fallible topology read;
    // planning inside the retained publisher will retry transient state or KV
    // projection failures without losing the committed journal edge.
    if let Some(lane) = exact_lane {
        return schedule_daemon_dfs_metadata_publish_lane_on_runtime(
            data_dir,
            cluster_paths,
            runtime,
            traceparent,
            lane,
        );
    }

    // Namespace/global operations and lifecycle materialization can append to
    // several shards. Retain the discovery edge before reading fallible
    // cluster state or topology; a transient read failure therefore retries
    // from this per-root lane without needing another DFS mutation.
    schedule_daemon_dfs_metadata_publish_discovery_on_runtime(
        data_dir,
        cluster_paths,
        runtime,
        traceparent,
    )
}

fn daemon_dfs_metadata_publish_discovery_lanes(
    cluster_paths: &ClusterPaths,
) -> Result<Vec<DaemonDfsMutationLaneKey>, String> {
    let state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(|error| format!("DFS metadata lanes lack cluster state: {error}"))?;
    // Shard authority is committed Raft state. A voter's materialized snapshot
    // can briefly trail its applied log, especially while a primary handoff is
    // completing. Read the same latest-observed view used by DFS routing and
    // status so a promotion wake cannot rediscover the retiring topology and
    // drop the new primary's publish lane.
    let entries = sinomo_core::cluster::read_cluster_kv_latest_observed_prefix_entries(
        cluster_paths,
        "/dfs/filesystems/",
    )
    .map_err(|error| format!("DFS metadata lanes lack shard topology: {error}"))?;
    let mut lanes = entries
        .into_iter()
        .filter(|entry| daemon_dfs_metadata_shard_config_key_is_exact(&entry.key))
        .map(|entry| {
            entry
                .decode::<sinomo_dfs::DfsMetadataShardConfig>()
                .map_err(|error| format!("DFS metadata shard topology is malformed: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|shard| shard.primary_node_id == state.local_node_id)
        .map(|shard| DaemonDfsMutationLaneKey {
            file_system_id: shard.file_system_id,
            shard_id: DfsMetadataShardId::new(shard.shard_id),
        })
        .collect::<Vec<_>>();
    lanes.sort();
    lanes.dedup();
    Ok(lanes)
}

fn schedule_daemon_dfs_metadata_publish_discovery_on_runtime(
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    runtime: &tokio::runtime::Handle,
    traceparent: Option<String>,
) -> DfsMetadataPublishScheduleStart {
    let scheduler = match daemon_dfs_metadata_publish_discovery_scheduler(&data_dir) {
        Ok(scheduler) => scheduler,
        Err(error) => {
            tracing::warn!("sinomo daemon DFS metadata discovery could not be scheduled: {error}");
            return DfsMetadataPublishScheduleStart::AlreadyRunning;
        }
    };
    let publish_data_dir = data_dir;
    let publish_cluster_paths = cluster_paths;
    let publish_runtime = runtime.clone();
    let report_traceparent = traceparent.clone();
    let error_traceparent = traceparent;
    sinomo_dfs::schedule_dfs_metadata_publish_async_on_runtime(
        scheduler,
        runtime.clone(),
        daemon_dfs_metadata_publish_discovery_schedule_options(),
        move || {
            let data_dir = publish_data_dir.clone();
            let cluster_paths = publish_cluster_paths.clone();
            let runtime = publish_runtime.clone();
            async move {
                let planning_paths = cluster_paths.clone();
                let discovered = tokio::task::spawn_blocking(move || {
                    daemon_dfs_metadata_publish_discovery_lanes(&planning_paths)
                })
                .await
                .map_err(|error| format!("DFS metadata lane discovery task failed: {error}"))?;
                #[cfg(test)]
                record_daemon_dfs_metadata_publish_discovery_attempt(
                    &data_dir.root(),
                    discovered.is_ok(),
                );
                let lanes = discovered?;
                for lane in lanes {
                    schedule_daemon_dfs_metadata_publish_lane_on_runtime(
                        data_dir.clone(),
                        cluster_paths.clone(),
                        &runtime,
                        None,
                        lane,
                    );
                }
                Ok(DfsMetadataJournalReplicationReport {
                    outcomes: Vec::new(),
                })
            }
        },
        move |report| {
            daemon_dfs_perf_trace_with_traceparent(
                report_traceparent.as_deref(),
                format_args!(
                    "dfs-metadata-publish-discovery-complete outcomes={}",
                    report.outcomes.len()
                ),
            );
        },
        move |error| {
            daemon_dfs_perf_trace_with_traceparent(
                error_traceparent.as_deref(),
                format_args!("dfs-metadata-publish-discovery-failed error={error}"),
            );
            tracing::warn!("sinomo daemon DFS metadata lane discovery failed: {error}");
        },
    )
}

/// Re-dirty only publisher lanes that are currently retained by durable
/// backlog/failure state for this data root. Connectivity owns the route
/// generation and calls this edge when its semantic projection changes; no
/// DFS-wide scan or steady-state timer is involved.
pub(crate) fn notify_daemon_dfs_metadata_publish_route_generation_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<usize, DaemonError> {
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let data_root = data_dir.root();
    let lanes = {
        let mut schedulers = DAEMON_DFS_METADATA_PUBLISH_SCHEDULERS
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .map_err(|_| DaemonError::Network {
                address: "DFS metadata publish scheduler".to_string(),
                detail: "DFS metadata publish scheduler map is poisoned".to_string(),
            })?;
        schedulers.retain(|_, scheduler| scheduler.strong_count() > 0);
        schedulers
            .keys()
            .filter(|key| key.data_root == data_root)
            .map(|key| DaemonDfsMutationLaneKey {
                file_system_id: key.file_system_id.clone(),
                shard_id: key.shard_id.clone(),
            })
            .collect::<Vec<_>>()
    };
    let cluster_paths = ClusterPaths::new(&data_dir);
    for lane in &lanes {
        schedule_daemon_dfs_metadata_publish_lane_on_runtime(
            data_dir.clone(),
            cluster_paths.clone(),
            runtime,
            None,
            lane.clone(),
        );
    }
    Ok(lanes.len())
}

fn schedule_daemon_dfs_metadata_publish_lane_on_runtime(
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    runtime: &tokio::runtime::Handle,
    traceparent: Option<String>,
    lane: DaemonDfsMutationLaneKey,
) -> DfsMetadataPublishScheduleStart {
    let scheduler = match daemon_dfs_metadata_publish_lane_scheduler(
        &data_dir,
        &lane.file_system_id,
        &lane.shard_id,
    ) {
        Ok(scheduler) => scheduler,
        Err(error) => {
            tracing::warn!(
                file_system_id = %lane.file_system_id,
                shard_id = %lane.shard_id.0,
                "sinomo daemon DFS metadata lane could not be scheduled: {error}"
            );
            return DfsMetadataPublishScheduleStart::AlreadyRunning;
        }
    };
    let Some(data_owner_lock) =
        DaemonDfsControlPlaneKvClient::discover_data_owner_lock(&cluster_paths)
    else {
        tracing::warn!(
            file_system_id = %lane.file_system_id,
            shard_id = %lane.shard_id.0,
            "sinomo daemon DFS metadata lane lacks the live data-owner capability"
        );
        return DfsMetadataPublishScheduleStart::AlreadyRunning;
    };
    #[cfg(test)]
    record_daemon_dfs_metadata_publish_lane_request(&data_dir, &lane);
    let peer_transport = daemon_dfs_blocking_peer_transport_on_runtime(&data_dir, runtime);
    let async_peer_transport = daemon_dfs_async_peer_transport(&data_dir, runtime);
    let publish_data_dir = data_dir;
    let publish_cluster_paths = cluster_paths;
    let publish_file_system_id = lane.file_system_id.clone();
    let publish_shard_id = lane.shard_id.clone();
    let publish_runtime = runtime.clone();
    let report_lane = lane.clone();
    let error_lane = lane;
    let report_traceparent = traceparent.clone();
    let error_traceparent = traceparent;
    sinomo_dfs::schedule_dfs_metadata_publish_async_on_runtime(
        scheduler,
        runtime.clone(),
        daemon_dfs_metadata_publish_schedule_options(),
        move || {
            let data_dir = publish_data_dir.clone();
            let cluster_paths = publish_cluster_paths.clone();
            let exclusion_paths = cluster_paths.clone();
            let file_system_id = publish_file_system_id.clone();
            let shard_id = publish_shard_id.clone();
            let peer_transport = Arc::clone(&peer_transport);
            let async_peer_transport = Arc::clone(&async_peer_transport);
            let runtime = publish_runtime.clone();
            let data_owner_lock = Arc::clone(&data_owner_lock);
            async move {
                // The scheduler task can be cancelled during runtime shutdown,
                // while a blocking planner or transport task keeps running.
                // Own the daemon's OS-backed data capability in the outer
                // blocking closure for the complete publish future so an old
                // authority generation cannot overlap its replacement.
                tokio::task::spawn_blocking(move || {
                    let _data_owner_lock = data_owner_lock;
                    let execution_exclusions =
                        sinomo_core::cluster::read_cluster_state_without_kv(&exclusion_paths)
                            .map(|state| {
                                daemon_dfs_maintenance_execution_exclusions(
                                    &exclusion_paths,
                                    &state,
                                )
                            })
                            .unwrap_or_default();
                    runtime.block_on(
                        sinomo_dfs::publish_cluster_dfs_metadata_shard_with_peer_transports_async_and_execution_exclusions(
                            &data_dir,
                            &cluster_paths,
                            peer_transport,
                            async_peer_transport,
                            file_system_id,
                            shard_id,
                            &execution_exclusions,
                            daemon_dfs_metadata_replication_options(),
                        ),
                    )
                })
                .await
                .map_err(|source| format!("DFS metadata publish task failed: {source}"))?
                .map_err(|error| error.to_string())
            }
        },
        move |report| {
            daemon_dfs_perf_trace_with_traceparent(
                report_traceparent.as_deref(),
                format_args!(
                    "dfs-metadata-publish-complete filesystem={} shard={} outcomes={}",
                    report_lane.file_system_id,
                    report_lane.shard_id.0,
                    report.outcomes.len()
                ),
            );
            log_dfs_metadata_replication_report(report);
        },
        move |error| {
            daemon_dfs_perf_trace_with_traceparent(
                error_traceparent.as_deref(),
                format_args!(
                    "dfs-metadata-publish-failed filesystem={} shard={} error={error}",
                    error_lane.file_system_id, error_lane.shard_id.0
                ),
            );
            tracing::warn!(
                file_system_id = %error_lane.file_system_id,
                shard_id = %error_lane.shard_id.0,
                "sinomo daemon DFS scheduled metadata publish failed: {error}"
            );
        },
    )
}

#[cfg(test)]
fn publish_default_dfs_metadata_journal_with_sender(
    paths: &DaemonPaths,
    sender: DfsMetadataJournalBatchSender,
) -> Result<DfsMetadataJournalReplicationReport, DaemonError> {
    publish_default_dfs_metadata_journal_with_senders(
        paths,
        sender,
        daemon_dfs_metadata_snapshot_install_sender(),
    )
}

#[cfg(test)]
fn publish_default_dfs_metadata_journal_with_senders(
    paths: &DaemonPaths,
    sender: DfsMetadataJournalBatchSender,
    snapshot_sender: DfsMetadataSnapshotInstallSender,
) -> Result<DfsMetadataJournalReplicationReport, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    Ok(sinomo_dfs::publish_cluster_dfs_metadata_journal(
        &data_dir,
        &cluster_paths,
        sender,
        snapshot_sender,
        DfsMetadataJournalReplicationOptions::default(),
    )?)
}

async fn run_daemon_dfs_smoke_async(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_dfs::DfsSmokeReport, DaemonError> {
    let smoke_guard = DaemonDfsSmokeInFlightGuard::acquire()?;
    let data_owner_lock = daemon_dfs_data_owner_lock(paths)?;
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let runtime = runtime.clone();
    let smoke_task = tokio::task::spawn_blocking(move || {
        let _data_owner_lock = data_owner_lock;
        let _smoke_guard = smoke_guard;
        sinomo_dfs::run_default_cluster_dfs_smoke_with_peer_transport(
            &data_dir,
            &cluster_paths,
            daemon_dfs_blocking_peer_transport_on_runtime(&data_dir, &runtime),
            Box::new(DaemonDfsControlPlaneKvClient::new_on_runtime(
                &cluster_paths,
                &runtime,
            )),
            daemon_dfs_control_plane_client_on_runtime(&cluster_paths, Some(&runtime)),
            DfsDefaultClusterSmokeOptions {
                bootstrap: DfsDefaultClusterBootstrapOptions {
                    publish_cluster_control: false,
                    materialize_local_metadata: false,
                    local_zone: None,
                },
                smoke: daemon_dfs_smoke_options(),
            },
        )
    });
    let Some(report) = tokio::time::timeout(DAEMON_DFS_SMOKE_TIMEOUT, smoke_task)
        .await
        .map_err(|_| DaemonError::Network {
            address: "DFS smoke".to_string(),
            detail: format!(
                "DFS smoke did not finish within {}ms",
                DAEMON_DFS_SMOKE_TIMEOUT.as_millis()
            ),
        })?
        .map_err(|source| DaemonError::Network {
            address: "DFS smoke".to_string(),
            detail: format!("DFS smoke task failed: {source}"),
        })??
    else {
        return Err(DaemonError::ClusterOrphan);
    };
    Ok(report)
}

fn daemon_data_dir(paths: &DaemonPaths) -> Result<&std::path::Path, DaemonError> {
    paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon root has no data directory parent".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::DfsWriteReceipt;
    use sinomo_core::events::EventId;
    use sinomo_core::network::{
        ClusterConnectivityRecord, ClusterConnectivityState, ClusterEndpointId,
        cluster_route_next_hop,
    };
    use sinomo_core::node::{ClusterId, NodeId};
    use sinomo_core::{
        ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterEndpointSource, ClusterTransportKind,
        DfsObjectPolicy, DfsWriteRequest,
    };
    use sinomo_dfs::{
        DfsBlobTransferResult, DfsDistributedWriteCommand, DfsMetadataJournalReplicationOutcome,
        DfsMetadataReplicaLagStatus, DfsNamespaceCommand, DfsNamespaceJobStatus,
        DfsNamespaceOperation, DfsNetworkMessage, DfsObjectId, DfsObjectKind, DfsObjectMetadata,
        DfsReplicationPolicy, DfsRoutedMessage, DfsStageBlobCommand, DfsVersion, DfsWriteOptions,
    };
    use std::collections::BTreeMap;
    use std::fs;
    use std::sync::{Arc, Mutex};

    // Restart recovery deliberately keeps the permit record private. Tests
    // encode the durable shape directly to model residue from a dead process;
    // `inspect_dfs_placement_authority_permit` below proves it remains
    // compatible with the production decoder before recovery runs.
    #[derive(serde::Serialize)]
    struct TestPlacementAuthorityPermitRecord {
        job_id: String,
        holder_id: String,
        holder_cluster_id: Option<String>,
        holder_node_id: Option<NodeId>,
        holder_session_id: Option<String>,
    }

    #[test]
    fn daemon_dfs_blocking_owner_clone_retains_exclusive_data_lock() {
        let root = temp_root("blocking-owner-lock");
        let data_dir = SinomoDataDir::new(root.path());
        let paths = DaemonPaths::new(&data_dir);
        let control_owner =
            Arc::new(DaemonDataOwnerLock::acquire(&paths).expect("control loop data-owner lock"));
        register_daemon_dfs_data_owner_lock(&paths, &control_owner)
            .expect("register data-owner lock");
        let blocking_owner = daemon_dfs_data_owner_lock(&paths)
            .expect("read registered lock")
            .expect("blocking task owner clone");

        drop(control_owner);
        assert!(
            DaemonDataOwnerLock::acquire(&paths).is_err(),
            "a replacement daemon must remain excluded while abandoned blocking DFS work runs"
        );

        drop(blocking_owner);
        DaemonDataOwnerLock::acquire(&paths)
            .expect("data-owner lock is released after blocking work stops");
    }

    #[test]
    fn daemon_dfs_placement_identity_requires_live_main_data_owner_lock() {
        let root = temp_root("placement-identity-owner-lock");
        let data_dir = SinomoDataDir::new(root.path());
        let paths = DaemonPaths::new(&data_dir);
        let local_node_id = NodeId::generate();
        write_test_cluster_state(root.path(), &local_node_id);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let runtime = daemon_test_runtime("placement-identity-owner-lock");
        let unowned_client =
            DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, runtime.handle());

        assert!(
            sinomo_dfs::DfsControlPlaneKvClient::placement_authority_process_identity(
                &unowned_client
            )
            .is_none(),
            "a shadow or utility process without the main data lock must not acquire placement authority"
        );

        let owner =
            Arc::new(DaemonDataOwnerLock::acquire(&paths).expect("main daemon data-owner lock"));
        register_daemon_dfs_data_owner_lock(&paths, &owner).expect("register main lock");
        let owned_client =
            DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, runtime.handle());
        let owned_identity =
            sinomo_dfs::DfsControlPlaneKvClient::placement_authority_process_identity(
                &owned_client,
            )
            .expect("owned placement identity");
        assert!(
            sinomo_dfs::DfsControlPlaneKvClient::authorizes_prior_process_placement_authority_recovery(
                &owned_client,
                &local_node_id,
                &owned_identity,
            ),
            "the main daemon may acquire placement authority while its exclusive lock is live"
        );
        assert!(
            !sinomo_dfs::DfsControlPlaneKvClient::authorizes_prior_process_placement_authority_recovery(
                &owned_client,
                &NodeId::generate(),
                &owned_identity,
            ),
            "host authorization is bound to the local cluster node"
        );
        let forged_identity = sinomo_dfs::DfsPlacementAuthorityProcessIdentity {
            cluster_id: owned_identity.cluster_id.clone(),
            process_session_id: "forged-placement-session".to_string(),
        };
        assert!(
            !sinomo_dfs::DfsControlPlaneKvClient::authorizes_prior_process_placement_authority_recovery(
                &owned_client,
                &local_node_id,
                &forged_identity,
            ),
            "host authorization is bound to the current process session"
        );
        drop(owner);
        assert!(
            sinomo_dfs::DfsControlPlaneKvClient::placement_authority_process_identity(
                &owned_client
            )
            .is_none(),
            "an ordinary cached client must not pin the data-owner lock after its task guard exits"
        );
        let replacement = DaemonDataOwnerLock::acquire(&paths)
            .expect("weak cached client released data-owner lock");
        drop(replacement);
        drop(owned_client);

        let owner = Arc::new(DaemonDataOwnerLock::acquire(&paths).expect("replacement main owner"));
        register_daemon_dfs_data_owner_lock(&paths, &owner).expect("register replacement lock");
        let retained_client =
            DaemonDfsControlPlaneKvClient::new_owned_on_runtime(&cluster_paths, runtime.handle());
        drop(owner);
        assert!(
            sinomo_dfs::DfsControlPlaneKvClient::placement_authority_process_identity(
                &retained_client
            )
            .is_some(),
            "an explicitly owned sandbox service retains authority for its full lifetime"
        );
        assert!(DaemonDataOwnerLock::acquire(&paths).is_err());
        drop(retained_client);
        DaemonDataOwnerLock::acquire(&paths).expect("owned sandbox service released lock");
    }

    #[test]
    fn daemon_dfs_message_execution_is_single_flight_per_data_dir() {
        let first_root = temp_root("message-execution-first");
        let second_root = temp_root("message-execution-second");
        let first_data = SinomoDataDir::new(first_root.path());
        let second_data = SinomoDataDir::new(second_root.path());
        let first_gate = daemon_dfs_message_execution_gate(&first_data).expect("first gate");
        let same_gate = daemon_dfs_message_execution_gate(&first_data).expect("same gate");
        let other_gate = daemon_dfs_message_execution_gate(&second_data).expect("other gate");

        assert!(Arc::ptr_eq(&first_gate, &same_gate));
        assert!(!Arc::ptr_eq(&first_gate, &other_gate));
        let first = Arc::clone(&first_gate.semaphore)
            .try_acquire_owned()
            .expect("first message execution");
        assert!(
            Arc::clone(&same_gate.semaphore)
                .try_acquire_owned()
                .is_err(),
            "same-root message execution must be single flight"
        );
        let other = Arc::clone(&other_gate.semaphore)
            .try_acquire_owned()
            .expect("other-root message execution");
        drop(other);
        drop(first);
    }

    #[test]
    fn lightweight_dfs_reads_bypass_the_mutation_execution_gate() {
        let root = temp_root("lightweight-read-bypasses-mutation-gate");
        let gate = daemon_dfs_message_execution_gate(&SinomoDataDir::new(root.path()))
            .expect("message gate");
        let runtime = daemon_test_runtime("lightweight-read-bypasses-mutation-gate");
        runtime.block_on(async {
            let mutation_permit = Arc::clone(&gate.semaphore)
                .acquire_owned()
                .await
                .expect("mutation permit");

            let read_slot = tokio::time::timeout(
                Duration::from_millis(100),
                acquire_daemon_dfs_message_execution(&gate, true),
            )
            .await
            .expect("lightweight read must not wait for mutation execution")
            .expect("lightweight read admission");
            assert!(read_slot.is_none());

            assert!(
                tokio::time::timeout(
                    Duration::from_millis(100),
                    acquire_daemon_dfs_message_execution(&gate, false),
                )
                .await
                .is_err(),
                "mutation-capable work must retain single-flight execution"
            );

            drop(mutation_permit);
            let mutation_slot = tokio::time::timeout(
                Duration::from_secs(1),
                acquire_daemon_dfs_message_execution(&gate, false),
            )
            .await
            .expect("mutation admission after permit release")
            .expect("mutation execution slot");
            assert!(mutation_slot.is_some());
        });
    }

    #[test]
    fn foreground_dfs_preempts_pending_maintenance_before_permit_release() {
        #[derive(Debug)]
        struct MaintenanceGenerationDrop(Arc<AtomicBool>);

        impl Drop for MaintenanceGenerationDrop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let root = temp_root("foreground-preempts-pending-maintenance");
        let gate = daemon_dfs_message_execution_gate(&SinomoDataDir::new(root.path()))
            .expect("message gate");
        let runtime = daemon_test_runtime("foreground-preempts-pending-maintenance");
        runtime.block_on(async {
            let maintenance_interest = gate.begin_maintenance_interest();
            let maintenance_permit = Arc::clone(&gate.semaphore)
                .acquire_owned()
                .await
                .expect("maintenance permit");
            let (pending_tx, pending_rx) = tokio::sync::oneshot::channel();
            let generation_dropped = Arc::new(AtomicBool::new(false));
            let maintenance_gate = Arc::clone(&gate);
            let pending_maintenance_gate = Arc::clone(&gate);
            let maintenance_generation_dropped = Arc::clone(&generation_dropped);
            let observed_generation_dropped = Arc::clone(&generation_dropped);
            let maintenance_task = tokio::spawn(async move {
                let outcome = run_daemon_dfs_maintenance_until_foreground_waits(
                    &maintenance_interest,
                    async move {
                        let _generation = MaintenanceGenerationDrop(maintenance_generation_dropped);
                        assert!(
                            Arc::clone(&pending_maintenance_gate.semaphore)
                                .try_acquire_owned()
                                .is_err(),
                            "maintenance local work must retain exclusive mutation admission"
                        );
                        pending_tx
                            .send(())
                            .expect("report outbound maintenance wait");
                        std::future::pending::<()>().await;
                    },
                )
                .await;
                assert!(matches!(
                    outcome,
                    DaemonDfsMaintenancePreemptionOutcome::ForegroundWaiting
                ));
                assert!(
                    observed_generation_dropped.load(Ordering::Acquire),
                    "the maintenance service generation must be destroyed before admission returns"
                );
                assert!(
                    Arc::clone(&maintenance_gate.semaphore)
                        .try_acquire_owned()
                        .is_err(),
                    "preemption must not release the permit before dropping maintenance state"
                );
                drop(maintenance_permit);
            });

            pending_rx.await.expect("maintenance reached outbound wait");
            let foreground_permit = tokio::time::timeout(
                Duration::from_secs(1),
                acquire_daemon_dfs_foreground_message_execution(&gate),
            )
            .await
            .expect("foreground admission must preempt pending maintenance")
            .expect("foreground permit");
            maintenance_task.await.expect("maintenance preemption task");
            assert!(generation_dropped.load(Ordering::Acquire));
            drop(foreground_permit);
        });
    }

    #[test]
    fn simultaneous_two_node_maintenance_rpc_waits_make_foreground_progress() {
        let first_root = temp_root("simultaneous-maintenance-first-node");
        let second_root = temp_root("simultaneous-maintenance-second-node");
        let first_gate = daemon_dfs_message_execution_gate(&SinomoDataDir::new(first_root.path()))
            .expect("first message gate");
        let second_gate =
            daemon_dfs_message_execution_gate(&SinomoDataDir::new(second_root.path()))
                .expect("second message gate");
        let runtime = daemon_test_runtime("simultaneous-two-node-maintenance-rpc-waits");
        runtime.block_on(async {
            let first_interest = first_gate.begin_maintenance_interest();
            let second_interest = second_gate.begin_maintenance_interest();
            let first_permit = Arc::clone(&first_gate.semaphore)
                .acquire_owned()
                .await
                .expect("first maintenance permit");
            let second_permit = Arc::clone(&second_gate.semaphore)
                .acquire_owned()
                .await
                .expect("second maintenance permit");
            let (first_waiting_tx, first_waiting_rx) = tokio::sync::oneshot::channel();
            let (second_waiting_tx, second_waiting_rx) = tokio::sync::oneshot::channel();

            let first_maintenance = tokio::spawn(async move {
                let outcome = run_daemon_dfs_maintenance_until_foreground_waits(
                    &first_interest,
                    async move {
                        first_waiting_tx.send(()).expect("first waiting signal");
                        std::future::pending::<()>().await;
                    },
                )
                .await;
                drop(first_permit);
                outcome
            });
            let second_maintenance = tokio::spawn(async move {
                let outcome = run_daemon_dfs_maintenance_until_foreground_waits(
                    &second_interest,
                    async move {
                        second_waiting_tx.send(()).expect("second waiting signal");
                        std::future::pending::<()>().await;
                    },
                )
                .await;
                drop(second_permit);
                outcome
            });

            // Both nodes now hold local maintenance admission while awaiting
            // an outbound peer response. Simultaneous reciprocal inbound RPCs
            // must request foreground admission and cancel both replay-safe
            // maintenance futures before either network timeout is needed.
            first_waiting_rx.await.expect("first outbound wait");
            second_waiting_rx.await.expect("second outbound wait");
            let (first_foreground, second_foreground) =
                tokio::time::timeout(Duration::from_secs(1), async {
                    tokio::join!(
                        acquire_daemon_dfs_foreground_message_execution(&first_gate),
                        acquire_daemon_dfs_foreground_message_execution(&second_gate),
                    )
                })
                .await
                .expect("reciprocal inbound RPCs must make progress without timeout");
            let first_foreground = first_foreground.expect("first foreground permit");
            let second_foreground = second_foreground.expect("second foreground permit");
            assert!(matches!(
                first_maintenance.await.expect("first maintenance task"),
                DaemonDfsMaintenancePreemptionOutcome::ForegroundWaiting
            ));
            assert!(matches!(
                second_maintenance.await.expect("second maintenance task"),
                DaemonDfsMaintenancePreemptionOutcome::ForegroundWaiting
            ));
            drop(first_foreground);
            drop(second_foreground);
        });
    }

    #[test]
    fn mandatory_default_dfs_lifecycle_is_single_flight_per_data_dir() {
        let first_root = temp_root("lifecycle-single-flight-first");
        let second_root = temp_root("lifecycle-single-flight-second");
        let first_gate = daemon_default_dfs_lifecycle_gate(first_root.path()).expect("first gate");
        let same_gate = daemon_default_dfs_lifecycle_gate(first_root.path()).expect("same gate");
        let other_gate = daemon_default_dfs_lifecycle_gate(second_root.path()).expect("other gate");

        assert!(Arc::ptr_eq(&first_gate, &same_gate));
        assert!(!Arc::ptr_eq(&first_gate, &other_gate));

        let first = first_gate.clone().try_lock_owned().expect("first pass");
        assert!(
            same_gate.try_lock_owned().is_err(),
            "concurrent reconciliation must coalesce while a pass owns the gate"
        );
        assert!(
            other_gate.try_lock_owned().is_ok(),
            "independent data directories must not block each other"
        );
        drop(first);
        assert!(first_gate.try_lock_owned().is_ok());
    }

    #[test]
    fn metadata_publish_schedulers_are_independent_per_shard() {
        let root = temp_root("metadata-publish-shard-lanes");
        let data_dir = SinomoDataDir::new(root.path());
        let file_system_id =
            DfsFileSystemId::from_legacy_named("metadata-publish-lane-fs").expect("filesystem id");
        let root_shard = DfsMetadataShardId::new("root");
        let services_shard = DfsMetadataShardId::new("services");
        let first =
            daemon_dfs_metadata_publish_lane_scheduler(&data_dir, &file_system_id, &root_shard)
                .expect("root publish lane");
        let same =
            daemon_dfs_metadata_publish_lane_scheduler(&data_dir, &file_system_id, &root_shard)
                .expect("same root publish lane");
        let other =
            daemon_dfs_metadata_publish_lane_scheduler(&data_dir, &file_system_id, &services_shard)
                .expect("services publish lane");

        assert!(Arc::ptr_eq(&first, &same));
        assert!(
            !Arc::ptr_eq(&first, &other),
            "a blocked replica for one shard must not occupy another shard's reactor lane"
        );
    }

    #[test]
    fn metadata_publish_discovery_uses_applied_topology_ahead_of_materialized_snapshot() {
        let root = temp_root("metadata-publish-latest-observed-topology");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local =
            NodeId::from_legacy_named("node_metadata_publish_promoted_local").expect("local node");
        let remote = NodeId::from_legacy_named("node_metadata_publish_retiring_remote")
            .expect("remote node");
        write_test_cluster_state_with_remote_endpoint(
            &data_root,
            &local,
            &remote,
            "10.79.0.1:21353",
        );
        let stale_materialized =
            sinomo_core::cluster::read_cluster_kv_current_snapshot(&cluster_paths)
                .expect("pre-promotion snapshot");
        let file_system = distributed_sifs(local.clone());
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "services".to_string(),
            primary_node_id: local.clone(),
            replica_node_ids: vec![remote],
            epoch: 2,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: "/services".to_string(),
            },
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "services"),
                    &shard,
                    None,
                )
                .expect("promoted shard op"),
            ),
        )
        .expect("commit promoted shard");

        // Model the short production window where the local Raft state has
        // applied the promotion but the derived materialized snapshot still
        // describes the retiring topology.
        sinomo_core::kv::write_cluster_kv_snapshot(&cluster_paths.kv_paths(), &stale_materialized)
            .expect("restore stale materialized snapshot");
        assert!(
            sinomo_core::kv::read_cluster_kv_prefix_entries(
                &cluster_paths.kv_paths(),
                "/dfs/filesystems/",
            )
            .expect("stale materialized topology")
            .is_empty(),
            "the regression requires the materialized projection to trail Raft"
        );

        assert_eq!(
            daemon_dfs_metadata_publish_discovery_lanes(&cluster_paths)
                .expect("discover promoted primary lane"),
            vec![DaemonDfsMutationLaneKey {
                file_system_id: file_system.file_system_id,
                shard_id: DfsMetadataShardId::new("services"),
            }],
            "promotion discovery must use applied authority rather than the stale materialized projection"
        );
    }

    #[test]
    fn committed_work_emitter_is_quiet_for_noop_and_retains_publish_edge() {
        let root = temp_root("committed-work-publish-edge");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let runtime = daemon_test_runtime("committed-work-publish-edge");
        let initial = daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root());

        emit_daemon_dfs_committed_edges(
            &data_dir,
            &cluster_paths,
            &DfsMessageExecutionScope::GlobalExclusive,
            runtime.handle(),
            None,
            DfsMessageCommittedWork::default(),
            &[],
        );
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(
            daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root()),
            initial,
            "an empty committed-work receipt must not wake discovery"
        );

        emit_daemon_dfs_committed_edges(
            &data_dir,
            &cluster_paths,
            &DfsMessageExecutionScope::GlobalExclusive,
            runtime.handle(),
            None,
            DfsMessageCommittedWork {
                publish_metadata: true,
                ..DfsMessageCommittedWork::default()
            },
            &[],
        );
        let deadline = Instant::now() + Duration::from_secs(2);
        while daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root()).0 == initial.0
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root()).0 > initial.0,
            "a committed metadata edge must remain owned even when discovery cannot yet read cluster topology"
        );
    }

    #[test]
    fn metadata_publish_discovery_retains_global_edge_across_planning_failure() {
        let root = temp_root("metadata-publish-discovery-retry");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let paths = DaemonPaths::new(&data_dir);
        let control_owner = Arc::new(
            DaemonDataOwnerLock::acquire(&paths).expect("control-loop data-owner capability"),
        );
        register_daemon_dfs_data_owner_lock(&paths, &control_owner)
            .expect("register data-owner capability");
        let runtime = daemon_test_runtime("metadata-publish-discovery-retry");

        assert_eq!(
            schedule_daemon_dfs_metadata_publish_for_data_dir_on_runtime(
                data_dir.clone(),
                cluster_paths.clone(),
                runtime.handle(),
                None,
                None,
            ),
            DfsMetadataPublishScheduleStart::Started,
            "the discovery lane must be retained before cluster planning succeeds"
        );
        let first_deadline = Instant::now() + Duration::from_secs(2);
        while daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root()).0 == 0
            && Instant::now() < first_deadline
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        let (failed_attempts, successes) =
            daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root());
        assert!(failed_attempts > 0);
        assert_eq!(successes, 0, "cluster state is intentionally absent");

        let local = NodeId::from_legacy_named("node_dfs_discovery_retry").expect("node");
        write_test_cluster_state(&data_root, &local);
        let file_system = distributed_sifs(local.clone());
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: local.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
                compares: Vec::new(),
                success: vec![
                    sinomo_core::kv::KvOp::put_serde(
                        file_system_config_key(&file_system.file_system_id),
                        &DfsFileSystemConfig::from(&file_system),
                        None,
                    )
                    .expect("filesystem config op"),
                    sinomo_core::kv::KvOp::put_serde(
                        metadata_shard_key(&file_system.file_system_id, "root"),
                        &shard,
                        None,
                    )
                    .expect("metadata shard op"),
                ],
                failure: Vec::new(),
            }),
        )
        .expect("install recovered topology");

        let recovery_deadline = Instant::now() + Duration::from_secs(5);
        while daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root()).1 == 0
            && Instant::now() < recovery_deadline
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        let (attempts, successes) =
            daemon_dfs_metadata_publish_discovery_attempts(&data_dir.root());
        assert!(attempts >= 2, "the retained edge must retry planning");
        assert_eq!(
            successes, 1,
            "topology recovery alone must drain the retained discovery edge"
        );
    }

    #[test]
    fn metadata_publish_missing_route_wakes_on_semantic_route_generation() {
        let root = temp_root("metadata-publish-route-generation");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let control_owner = Arc::new(
            DaemonDataOwnerLock::acquire(&paths).expect("control-loop data-owner capability"),
        );
        register_daemon_dfs_data_owner_lock(&paths, &control_owner)
            .expect("register data-owner capability");
        let runtime = daemon_test_runtime("metadata-publish-route-generation");
        let file_system_id = DfsFileSystemId::from_legacy_named("metadata-route-generation-fs")
            .expect("filesystem id");
        let shard_id = DfsMetadataShardId::new("services");
        let lane = DaemonDfsMutationLaneKey {
            file_system_id: file_system_id.clone(),
            shard_id: shard_id.clone(),
        };
        let scheduler =
            daemon_dfs_metadata_publish_lane_scheduler(&data_dir, &file_system_id, &shard_id)
                .expect("publish lane");
        let attempts = Arc::new(AtomicUsize::new(0));
        let route_available = Arc::new(AtomicBool::new(false));
        let publish_attempts = Arc::clone(&attempts);
        let publish_route_available = Arc::clone(&route_available);
        let target = NodeId::from_legacy_named("node_metadata_route_generation_target")
            .expect("target node");
        let publish_shard = shard_id.clone();
        assert_eq!(
            sinomo_dfs::schedule_dfs_metadata_publish_async_on_runtime(
                scheduler,
                runtime.handle().clone(),
                DfsMetadataPublishScheduleOptions {
                    max_retry_rounds: 1,
                    retry_delay: Duration::from_secs(60 * 60),
                    max_retry_delay: Duration::from_secs(60 * 60),
                },
                move || {
                    let attempts = Arc::clone(&publish_attempts);
                    let route_available = Arc::clone(&publish_route_available);
                    let target = target.clone();
                    let shard_id = publish_shard.clone();
                    async move {
                        attempts.fetch_add(1, Ordering::AcqRel);
                        let outcomes = if route_available.load(Ordering::Acquire) {
                            Vec::new()
                        } else {
                            vec![DfsMetadataJournalReplicationOutcome::MissingRoute {
                                target_node_id: target,
                                shard_id,
                                after_sequence: 0,
                            }]
                        };
                        Ok::<_, String>(DfsMetadataJournalReplicationReport { outcomes })
                    }
                },
                |_| {},
                |error| panic!("unexpected publish error: {error}"),
            ),
            DfsMetadataPublishScheduleStart::Started
        );

        let first_deadline = Instant::now() + Duration::from_secs(2);
        while attempts.load(Ordering::Acquire) == 0 && Instant::now() < first_deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(attempts.load(Ordering::Acquire), 1);
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(
            attempts.load(Ordering::Acquire),
            1,
            "the safety reprobe must remain sparse while the route is unavailable"
        );

        route_available.store(true, Ordering::Release);
        assert_eq!(
            notify_daemon_dfs_metadata_publish_route_generation_on_runtime(
                &paths,
                runtime.handle(),
            )
            .expect("route generation wake"),
            1,
            "only the retained exact lane should be re-dirtied"
        );
        let recovery_deadline = Instant::now() + Duration::from_secs(2);
        while attempts.load(Ordering::Acquire) < 2 && Instant::now() < recovery_deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            attempts.load(Ordering::Acquire),
            2,
            "route recovery must wake immediately without another DFS mutation"
        );

        // Keep the key in scope to make the test's exact-lane identity clear.
        assert_eq!(lane.shard_id, shard_id);
    }

    #[test]
    fn daemon_dfs_maintenance_admission_is_node_local_and_per_data_root() {
        let first_root = temp_root("daemon-dfs-maintenance-admission-first");
        let second_root = temp_root("daemon-dfs-maintenance-admission-second");
        let first_data_dir = SinomoDataDir::new(first_root.path());
        let second_data_dir = SinomoDataDir::new(second_root.path());
        let runtime = daemon_test_runtime("daemon-dfs-maintenance-admission-node-local");
        runtime.block_on(async {
            let first_admission = acquire_daemon_dfs_maintenance_admission(&first_data_dir)
                .await
                .expect("first data root admission");

            assert!(
                tokio::time::timeout(
                    Duration::from_millis(20),
                    acquire_daemon_dfs_maintenance_admission(&first_data_dir),
                )
                .await
                .is_err(),
                "one data root must serialize its lifecycle and metadata mutation generation"
            );

            let second_admission = tokio::time::timeout(
                Duration::from_secs(1),
                acquire_daemon_dfs_maintenance_admission(&second_data_dir),
            )
            .await
            .expect("a different node-local data root must not wait for unrelated maintenance")
            .expect("second data root admission");

            drop(second_admission);
            drop(first_admission);
            tokio::time::timeout(
                Duration::from_secs(1),
                acquire_daemon_dfs_maintenance_admission(&first_data_dir),
            )
            .await
            .expect("released data-root admission must become available")
            .expect("reacquire first data root");
        });
    }
    #[test]
    fn mandatory_default_dfs_lifecycle_guard_survives_dropped_blocking_join() {
        let root = temp_root("lifecycle-cancel-safety");
        let gate = daemon_default_dfs_lifecycle_gate(root.path()).expect("lifecycle gate");
        let guard = gate.clone().try_lock_owned().expect("lifecycle pass");
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (completed_tx, completed_rx) = std::sync::mpsc::channel();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let task = runtime.spawn_blocking(move || {
            let lifecycle_guard = guard;
            started_tx.send(()).expect("signal lifecycle start");
            release_rx.recv().expect("release lifecycle task");
            drop(lifecycle_guard);
            completed_tx.send(()).expect("signal lifecycle completion");
        });

        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocking lifecycle task should start");
        drop(task);
        assert!(
            gate.clone().try_lock_owned().is_err(),
            "dropping a blocking JoinHandle must not release its owned lifecycle guard"
        );

        release_tx.send(()).expect("release lifecycle task");
        completed_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocking lifecycle task should complete");
        assert!(gate.try_lock_owned().is_ok());
        runtime.shutdown_timeout(Duration::from_secs(1));
    }

    fn test_dfs_lookup_routed_message(
        source_node_id: &NodeId,
        target_node_id: &NodeId,
        frame_id: u64,
        path: &str,
    ) -> DfsRoutedMessage {
        let message = DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
            request_id: format!("daemon_dfs_lookup_{frame_id}"),
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            requesting_node_id: source_node_id.clone(),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
            operation: DfsNamespaceOperation::LookupPath {
                path: path.to_string(),
            },
        });
        DfsRoutedMessage {
            source_node_id: source_node_id.clone(),
            target_node_id: target_node_id.clone(),
            frame_id,
            message_id: frame_id,
            message,
        }
    }

    fn test_dfs_exact_read_routed_message(
        source_node_id: &NodeId,
        target_node_id: &NodeId,
        frame_id: u64,
    ) -> DfsRoutedMessage {
        DfsRoutedMessage {
            source_node_id: source_node_id.clone(),
            target_node_id: target_node_id.clone(),
            frame_id,
            message_id: frame_id,
            message: DfsNetworkMessage::ReadRequest(sinomo_dfs::DfsReadRequest {
                request_id: format!("daemon_dfs_exact_read_{frame_id}"),
                object_id: DfsObjectId::from_legacy_named(format!(
                    "obj_daemon_dfs_exact_read_{frame_id}"
                ))
                .expect("object id"),
                version: Some(
                    DfsVersion::from_legacy_named(format!(
                        "version_daemon_dfs_exact_read_{frame_id}"
                    ))
                    .expect("version"),
                ),
                content_hash: Some(format!("blake3:missing-exact-read-{frame_id}")),
                content_size_bytes: Some(12),
                range_offset: None,
                range_max_bytes: None,
            }),
        }
    }

    fn test_dfs_write_routed_message(
        source_node_id: &NodeId,
        target_node_id: &NodeId,
        frame_id: u64,
        path: &str,
    ) -> DfsRoutedMessage {
        let object_id = DfsObjectId::from_legacy_named(format!("obj_daemon_dfs_write_{frame_id}"))
            .expect("object id");
        let base_version =
            DfsVersion::from_legacy_named(format!("version_daemon_dfs_write_base_{frame_id}"))
                .expect("base version");
        let target_version =
            DfsVersion::from_legacy_named(format!("version_daemon_dfs_write_target_{frame_id}"))
                .expect("target version");
        let bytes = format!("daemon DFS write message {frame_id}").into_bytes();
        let object = DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            path: path.to_string(),
            kind: DfsObjectKind::File,
            owner_node_id: target_node_id.clone(),
            current_locations: vec![source_node_id.clone()],
            content_hash: None,
            content_size_bytes: None,
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: base_version.clone(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
            tombstone_provenance: None,
        };
        let message = DfsNetworkMessage::DistributedWriteRequest(DfsDistributedWriteCommand {
            request_id: format!("daemon_dfs_write_{frame_id}"),
            request: DfsWriteRequest {
                file_system_id: sinomo_dfs::sifs_file_system_id(),
                object_id,
                target_version,
                requesting_node_id: source_node_id.clone(),
            },
            expected_base_version: Some(base_version),
            metadata_journal_after_sequence: None,
            bytes,
            content_hash: None,
            content_size_bytes: None,
            requester_blob_pre_staged: true,
            desired_replica_nodes: vec![source_node_id.clone()],
            policy: DfsReplicationPolicy::new(1, 1).expect("replication policy"),
            object_metadata: Some(object),
            authority_shard: None,
            touch_parent_on_create: false,
            exclusive_create: false,
            foreground_replica_repair: false,
        });
        DfsRoutedMessage {
            source_node_id: source_node_id.clone(),
            target_node_id: target_node_id.clone(),
            frame_id,
            message_id: frame_id,
            message,
        }
    }

    fn test_dfs_message_command(
        request_id: impl Into<String>,
        routed_message: DfsRoutedMessage,
    ) -> DfsMessageCommand {
        DfsMessageCommand {
            request_id: request_id.into(),
            routed_message,
        }
    }

    fn append_cluster_raft_command(
        paths: &ClusterPaths,
        node_id: &NodeId,
        command: sinomo_core::kv::KvCommand,
    ) -> Result<sinomo_core::kv::KvSnapshot, sinomo_core::network::NetworkError> {
        sinomo_core::cluster::append_cluster_raft_command(paths, node_id, command)
    }

    fn install_test_metadata_replica_shard(
        cluster_paths: &ClusterPaths,
        cluster_name: &str,
        local_node_id: &NodeId,
        shard: &DfsMetadataShardConfig,
    ) {
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(cluster_name).expect("cluster"),
            cluster_name,
            local_node_id.clone(),
            "local",
            "pk-local",
            OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(cluster_paths, &state)
            .expect("write cluster state");
        append_cluster_raft_command(
            cluster_paths,
            local_node_id,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    sinomo_dfs::file_system_config_key(&shard.file_system_id),
                    &test_file_system_config_for_shard(shard),
                    None,
                )
                .expect("filesystem config op"),
            ),
        )
        .expect("write filesystem config");
        append_cluster_raft_command(
            cluster_paths,
            local_node_id,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&shard.file_system_id, &shard.shard_id),
                    shard,
                    None,
                )
                .expect("metadata shard op"),
            ),
        )
        .expect("write metadata shard config");
    }

    fn test_file_system_config_for_shard(shard: &DfsMetadataShardConfig) -> DfsFileSystemConfig {
        DfsFileSystemConfig {
            file_system_id: shard.file_system_id.clone(),
            display_name: format!("{} test filesystem", shard.file_system_id),
            owner_node_id: shard.primary_node_id.clone(),
            subtree_policies: Vec::new(),
        }
    }

    fn daemon_test_runtime(name: &str) -> tokio::runtime::Runtime {
        crate::build_daemon_async_runtime(name).expect("test daemon runtime")
    }

    #[test]
    fn daemon_streams_owned_dfs_attachment_into_unbound_quarantine() {
        let root = temp_root("streamed-primary-content-ingest");
        let data_dir = SinomoDataDir::new(root.path());
        let source = NodeId::from_legacy_named("node_streamed_ingest_source").expect("source");
        let target = NodeId::from_legacy_named("node_streamed_ingest_target").expect("target");
        let mut routed =
            test_dfs_write_routed_message(&source, &target, 71_001, "/services/streamed.bin");
        let DfsNetworkMessage::DistributedWriteRequest(write) = &mut routed.message else {
            panic!("distributed write request");
        };
        let bytes = std::mem::take(&mut write.bytes);
        let object_id = write.request.object_id.clone();
        let version = write.request.target_version.clone();
        write.requester_blob_pre_staged = false;
        let command = test_dfs_message_command("streamed-primary-content-ingest", routed);
        let attachment = DfsInboundAttachment {
            descriptor: sinomo_dfs::DfsRpcAttachmentDescriptor {
                total_size_bytes: bytes.len() as u64,
                content_hash: None,
            },
            body: sinomo_core::network::ObjectRpcIncomingBody::bytes(bytes.clone()),
        };
        let runtime = daemon_test_runtime("daemon-dfs-streamed-primary-content-ingest");

        let (command, preingested_blob) = runtime
            .block_on(ingest_daemon_dfs_message_attachment(
                &data_dir,
                command,
                Some(attachment),
                runtime.handle(),
            ))
            .expect("stream attachment into content store");

        let preingested_blob = preingested_blob.expect("quarantined attachment");
        let DfsNetworkMessage::DistributedWriteRequest(write) = command.routed_message.message
        else {
            panic!("distributed write request");
        };
        assert!(write.bytes.is_empty());
        assert_eq!(write.content_size_bytes, Some(bytes.len() as u64));
        let content_hash = write.content_hash.expect("primary finalized hash");
        let content =
            LocalContentStore::new(data_dir.dfs_dir().join("content")).expect("content store");
        assert!(
            content
                .read_manifest(object_id.as_str(), version.as_str())
                .is_err(),
            "attachment ingest must not reserve its requested object version before admission"
        );
        assert_eq!(preingested_blob.content_hash, content_hash);
        content
            .bind_quarantined_blob(object_id.as_str(), version.as_str(), preingested_blob)
            .expect("bind admitted test blob");
        let (manifest, stored) = content
            .read_object_version(object_id.as_str(), version.as_str())
            .expect("read streamed object");
        assert_eq!(stored, bytes);
        assert_eq!(content_hash, manifest.content_hash);
    }

    fn run_daemon_dfs_maintenance_body_for_test(
        paths: &DaemonPaths,
        runtime: &tokio::runtime::Runtime,
    ) -> Result<(), DaemonError> {
        let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let foreground_preemption =
            daemon_dfs_message_execution_gate(&data_dir)?.begin_maintenance_interest();
        let mut deferred_work = None;
        let result = runtime.block_on(run_daemon_dfs_maintenance_tick_with_admission(
            data_dir,
            cluster_paths.clone(),
            runtime.handle(),
            DaemonDfsMaintenanceKind::ExplicitDisasterRecovery {
                content_gc_requested: true,
                content_reservation_reconciliation_requested: true,
                commit_promotion_recovery_requested: true,
            },
            DfsCommittedWorkTracker::new(),
            &mut deferred_work,
            &foreground_preemption,
        ));
        if let Some((detached_runtime, worker)) = deferred_work {
            schedule_daemon_dfs_message_deferred_work(
                runtime.handle(),
                &cluster_paths,
                detached_runtime,
                worker,
            );
        }
        result.map(|_| ())
    }

    fn daemon_dfs_test_execution_scope(
        file_system_id: &str,
        shard_id: &str,
    ) -> DfsMessageExecutionScope {
        DfsMessageExecutionScope::ExactShard {
            file_system_id: DfsFileSystemId::from_legacy_named(file_system_id)
                .expect("test filesystem id"),
            shard_id: DfsMetadataShardId::new(shard_id),
        }
    }

    #[test]
    fn daemon_dfs_cancelled_execution_waiter_does_not_leak_the_memory_slot() {
        let root = temp_root("daemon-dfs-cancelled-execution-waiter");
        let gate = daemon_dfs_message_execution_gate(&SinomoDataDir::new(root.path()))
            .expect("execution gate");
        let runtime = daemon_test_runtime("daemon-dfs-cancelled-execution-waiter");
        runtime.block_on(async {
            let first = Arc::clone(&gate.semaphore)
                .acquire_owned()
                .await
                .expect("first execution");
            let queued_gate = Arc::clone(&gate.semaphore);
            let mut queued = tokio::spawn(async move {
                queued_gate.acquire_owned().await.expect("queued execution")
            });
            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut queued)
                    .await
                    .is_err(),
                "second backend-bearing execution must remain queued"
            );
            queued.abort();
            assert!(
                queued.await.err().is_some_and(|error| error.is_cancelled()),
                "queued execution should report cancellation"
            );

            drop(first);
            let next = tokio::time::timeout(Duration::from_secs(1), gate.semaphore.acquire())
                .await
                .expect("execution should advance after cancellation")
                .expect("execution gate remains open");
            drop(next);
        });
    }

    #[test]
    fn daemon_dfs_cancelled_blocking_task_retains_the_memory_slot_until_exit() {
        let root = temp_root("daemon-dfs-cancelled-blocking-execution");
        let gate = daemon_dfs_message_execution_gate(&SinomoDataDir::new(root.path()))
            .expect("execution gate");
        let runtime = daemon_test_runtime("daemon-dfs-cancelled-blocking-execution");
        let permit = runtime.block_on(async {
            Arc::clone(&gate.semaphore)
                .acquire_owned()
                .await
                .expect("execution permit")
        });
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let blocking = runtime.spawn_blocking(move || {
            let permit = permit;
            started_tx.send(()).expect("report blocking start");
            release_rx.recv().expect("release blocking task");
            drop(permit);
            done_tx.send(()).expect("report blocking completion");
        });
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocking task started");
        blocking.abort();
        assert!(
            Arc::clone(&gate.semaphore).try_acquire_owned().is_err(),
            "aborting a running JoinHandle must not admit another backend generation"
        );
        release_tx.send(()).expect("release blocking task");
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocking task completed");
        let next = Arc::clone(&gate.semaphore)
            .try_acquire_owned()
            .expect("slot released by actual blocking completion");
        drop(next);
    }

    #[test]
    fn daemon_dfs_mutation_scheduler_bounds_wire_derived_shard_gates() {
        let scheduler = DaemonDfsMutationScheduler::new();
        let file_system_id = DfsFileSystemId::from_legacy_named("scheduler-gate-bound-fs")
            .expect("test filesystem id");
        for index in 0..DAEMON_DFS_MUTATION_SHARD_GATE_LIMIT {
            let key = DaemonDfsMutationLaneKey {
                file_system_id: file_system_id.clone(),
                shard_id: DfsMetadataShardId::new(format!("shard-{index}")),
            };
            assert!(
                scheduler
                    .shard_gate(&key)
                    .expect("bounded shard gate")
                    .is_some()
            );
        }
        let overflow = DaemonDfsMutationLaneKey {
            file_system_id,
            shard_id: DfsMetadataShardId::new("overflow"),
        };
        assert!(
            scheduler
                .shard_gate(&overflow)
                .expect("overflow shard gate")
                .is_none(),
            "wire-derived shard identities must not grow the scheduler without bound"
        );
    }

    #[test]
    fn daemon_dfs_mutation_scheduler_admits_disjoint_shards_concurrently() {
        let runtime = daemon_test_runtime("daemon-dfs-disjoint-shard-admission");
        runtime.block_on(async {
            let scheduler = Arc::new(DaemonDfsMutationScheduler::new());
            let root = daemon_dfs_test_execution_scope("scheduler-fs", "root");
            let services = daemon_dfs_test_execution_scope("scheduler-fs", "services");
            let other_file_system_root =
                daemon_dfs_test_execution_scope("scheduler-other-fs", "root");

            let root_admission = scheduler.admit(&root).await.expect("root admission");
            let services_admission =
                tokio::time::timeout(Duration::from_secs(1), scheduler.admit(&services))
                    .await
                    .expect("disjoint shard admission must not wait")
                    .expect("services admission");
            let other_file_system_admission = tokio::time::timeout(
                Duration::from_secs(1),
                scheduler.admit(&other_file_system_root),
            )
            .await
            .expect("same shard id in another filesystem must not wait")
            .expect("other filesystem admission");

            drop(other_file_system_admission);
            drop(services_admission);
            drop(root_admission);
        });
    }

    #[test]
    fn daemon_dfs_mutation_scheduler_serializes_the_same_shard() {
        let runtime = daemon_test_runtime("daemon-dfs-same-shard-admission");
        runtime.block_on(async {
            let scheduler = Arc::new(DaemonDfsMutationScheduler::new());
            let scope = daemon_dfs_test_execution_scope("scheduler-fs", "root");
            let first = scheduler.admit(&scope).await.expect("first admission");
            let waiter_scheduler = Arc::clone(&scheduler);
            let waiter_scope = scope.clone();
            let mut waiter = tokio::spawn(async move {
                waiter_scheduler
                    .admit(&waiter_scope)
                    .await
                    .expect("queued admission")
            });

            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut waiter)
                    .await
                    .is_err(),
                "same-shard work must remain queued while the shard is owned"
            );
            drop(first);
            let second = tokio::time::timeout(Duration::from_secs(1), &mut waiter)
                .await
                .expect("same-shard waiter should advance after release")
                .expect("same-shard waiter task");
            drop(second);
        });
    }

    #[test]
    fn daemon_dfs_mutation_scheduler_global_scope_excludes_exact_shards() {
        let runtime = daemon_test_runtime("daemon-dfs-global-shard-admission");
        runtime.block_on(async {
            let scheduler = Arc::new(DaemonDfsMutationScheduler::new());
            let scope = daemon_dfs_test_execution_scope("scheduler-fs", "root");
            let exact = scheduler.admit(&scope).await.expect("exact admission");
            let waiter_scheduler = Arc::clone(&scheduler);
            let mut global_waiter = tokio::spawn(async move {
                waiter_scheduler
                    .admit(&DfsMessageExecutionScope::GlobalExclusive)
                    .await
                    .expect("global admission")
            });

            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut global_waiter)
                    .await
                    .is_err(),
                "global work must wait for an admitted exact-shard mutation"
            );
            drop(exact);
            let global = tokio::time::timeout(Duration::from_secs(1), &mut global_waiter)
                .await
                .expect("global waiter should advance after exact release")
                .expect("global waiter task");

            let exact_waiter_scheduler = Arc::clone(&scheduler);
            let exact_waiter_scope = scope.clone();
            let mut exact_waiter = tokio::spawn(async move {
                exact_waiter_scheduler
                    .admit(&exact_waiter_scope)
                    .await
                    .expect("exact admission after global")
            });
            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut exact_waiter)
                    .await
                    .is_err(),
                "exact-shard work must wait for an admitted global mutation"
            );
            drop(global);
            let exact = tokio::time::timeout(Duration::from_secs(1), &mut exact_waiter)
                .await
                .expect("exact waiter should advance after global release")
                .expect("exact waiter task");
            drop(exact);
        });
    }

    #[test]
    fn daemon_dfs_mutation_scheduler_same_shard_backlog_does_not_delay_global() {
        let runtime = daemon_test_runtime("daemon-dfs-global-bypasses-shard-backlog");
        runtime.block_on(async {
            let scheduler = Arc::new(DaemonDfsMutationScheduler::new());
            let scope = daemon_dfs_test_execution_scope("scheduler-fs", "root");
            let first = scheduler
                .admit(&scope)
                .await
                .expect("first exact admission");

            let queued_exact_scheduler = Arc::clone(&scheduler);
            let queued_exact_scope = scope.clone();
            let mut queued_exact = tokio::spawn(async move {
                queued_exact_scheduler
                    .admit(&queued_exact_scope)
                    .await
                    .expect("queued exact admission")
            });
            tokio::task::yield_now().await;

            let global_scheduler = Arc::clone(&scheduler);
            let mut global_waiter = tokio::spawn(async move {
                global_scheduler
                    .admit(&DfsMessageExecutionScope::GlobalExclusive)
                    .await
                    .expect("queued global admission")
            });
            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut global_waiter)
                    .await
                    .is_err(),
                "global work must wait for the active exact owner"
            );

            drop(first);
            let global = tokio::time::timeout(Duration::from_secs(1), &mut global_waiter)
                .await
                .expect("global must bypass the same-shard backlog")
                .expect("global waiter task");
            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut queued_exact)
                    .await
                    .is_err(),
                "the queued exact request must retain only its shard while global is active"
            );

            drop(global);
            let exact = tokio::time::timeout(Duration::from_secs(1), &mut queued_exact)
                .await
                .expect("exact waiter should advance after global release")
                .expect("exact waiter task");
            drop(exact);
        });
    }

    #[test]
    fn daemon_dfs_blocking_task_retains_admission_after_rpc_cancellation() {
        let runtime = daemon_test_runtime("daemon-dfs-cancelled-rpc-admission");
        runtime.block_on(async {
            let scheduler = Arc::new(DaemonDfsMutationScheduler::new());
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let blocking_scheduler = Arc::clone(&scheduler);
            let handler = tokio::spawn(async move {
                let admission = blocking_scheduler
                    .admit(&DfsMessageExecutionScope::GlobalExclusive)
                    .await
                    .expect("global admission");
                tokio::task::spawn_blocking(move || {
                    let _ = started_tx.send(());
                    release_rx.recv().expect("release blocking DFS task");
                    drop(admission);
                })
                .await
                .expect("blocking DFS task")
            });
            started_rx.await.expect("blocking task started");
            handler.abort();
            assert!(
                handler
                    .await
                    .expect_err("handler cancellation")
                    .is_cancelled()
            );

            let exact_scope = daemon_dfs_test_execution_scope("scheduler-fs", "root");
            let exact_scheduler = Arc::clone(&scheduler);
            let mut exact_waiter = tokio::spawn(async move {
                exact_scheduler
                    .admit(&exact_scope)
                    .await
                    .expect("exact admission")
            });
            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut exact_waiter)
                    .await
                    .is_err(),
                "cancelling the RPC future must not release a running blocking task's admission"
            );

            release_tx.send(()).expect("release task");
            let exact = tokio::time::timeout(Duration::from_secs(1), &mut exact_waiter)
                .await
                .expect("exact waiter should advance after blocking work stops")
                .expect("exact waiter task");
            drop(exact);
        });
    }

    #[test]
    fn daemon_dfs_explicit_recovery_uses_bounds_and_routine_work_skips_inventory() {
        let explicit_recovery = daemon_dfs_explicit_disaster_recovery_tick_options(true, true);
        assert_eq!(
            explicit_recovery.metadata_replication,
            daemon_dfs_metadata_replication_options()
        );
        assert_eq!(
            daemon_dfs_metadata_replication_options().max_batches_per_target,
            DAEMON_DFS_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET
        );
        assert_eq!(
            explicit_recovery.repair_max_jobs,
            DAEMON_DFS_REPAIR_MAX_JOBS_PER_TICK
        );
        assert!(explicit_recovery.lifecycle.compact_tombstones);
        assert_eq!(
            explicit_recovery.lifecycle.tombstone_compaction_min_age,
            DAEMON_DFS_TOMBSTONE_COMPACTION_MIN_AGE
        );
        assert_eq!(
            explicit_recovery.lifecycle.tombstone_compaction_max_records,
            DAEMON_DFS_TOMBSTONE_COMPACTION_MAX_RECORDS
        );
        let lifecycle = explicit_recovery.lifecycle.clone();
        assert!(lifecycle.collect_metadata_history);
        assert_eq!(
            lifecycle.metadata_history_gc_min_age,
            DAEMON_DFS_METADATA_HISTORY_GC_MIN_AGE
        );
        assert_eq!(
            lifecycle.metadata_history_gc_keep_latest_versions,
            DAEMON_DFS_METADATA_HISTORY_GC_KEEP_LATEST_VERSIONS
        );
        assert_eq!(
            lifecycle.metadata_history_gc_max_records,
            DAEMON_DFS_METADATA_HISTORY_GC_MAX_RECORDS
        );
        assert!(DAEMON_DFS_METADATA_HISTORY_GC_MAX_AGE >= DAEMON_DFS_METADATA_HISTORY_GC_MIN_AGE);
        assert!(DAEMON_DFS_METADATA_HISTORY_GC_RECORD_TRIGGER > 0);
        assert!(DAEMON_DFS_METADATA_HISTORY_GC_BYTES_TRIGGER > 0);
        assert!(lifecycle.collect_content_versions);
        assert!(lifecycle.collect_orphan_content);
        assert!(lifecycle.reconcile_content_reservations);
        assert_eq!(
            lifecycle.content_reservation_reconciliation_max_records,
            DAEMON_DFS_CONTENT_RESERVATION_RECONCILIATION_MAX_RECORDS
        );
        let unrelated_work = daemon_dfs_routine_maintenance_tick_options().lifecycle;
        assert!(!unrelated_work.repair_metadata);
        assert!(!unrelated_work.cleanup_stale_staging);
        assert!(!unrelated_work.compact_tombstones);
        assert!(!unrelated_work.collect_metadata_history);
        assert!(!unrelated_work.collect_content_versions);
        assert!(!unrelated_work.collect_orphan_content);
        assert!(!unrelated_work.reconcile_content_reservations);
        let reservation_only =
            daemon_dfs_explicit_disaster_recovery_tick_options(false, true).lifecycle;
        assert!(reservation_only.repair_metadata);
        assert!(reservation_only.cleanup_stale_staging);
        assert!(reservation_only.compact_tombstones);
        assert!(reservation_only.collect_metadata_history);
        assert!(!reservation_only.collect_content_versions);
        assert!(!reservation_only.collect_orphan_content);
        assert!(reservation_only.reconcile_content_reservations);
        let destructive_only =
            daemon_dfs_explicit_disaster_recovery_tick_options(true, false).lifecycle;
        assert!(destructive_only.collect_content_versions);
        assert!(destructive_only.collect_orphan_content);
        assert!(destructive_only.reconcile_content_reservations);
        assert!(
            !explicit_recovery.publish_metadata,
            "broad maintenance must not race retained exact-shard publishers"
        );
        assert_eq!(
            daemon_dfs_metadata_replication_options().attempt_timeout,
            DAEMON_DFS_METADATA_REPLICATION_ATTEMPT_TIMEOUT
        );
        assert_eq!(
            daemon_dfs_metadata_replication_options().target_timeout,
            DAEMON_DFS_METADATA_REPLICATION_TARGET_TIMEOUT
        );
        assert!(daemon_dfs_metadata_replication_options().probe_caught_up_replicas);
        assert_eq!(
            daemon_dfs_metadata_publish_schedule_options().max_retry_rounds,
            DAEMON_DFS_METADATA_PUBLISH_MAX_RETRY_ROUNDS
        );
        assert_eq!(
            daemon_dfs_metadata_publish_schedule_options().retry_delay,
            DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL
        );
        assert_eq!(
            daemon_dfs_metadata_publish_schedule_options().max_retry_delay,
            DAEMON_DFS_METADATA_PUBLISH_RETRY_MAX_DELAY
        );
    }

    #[test]
    fn daemon_dfs_pending_probe_does_not_schedule_unprotected_content_gc() {
        let root = temp_root("pending-unprotected-content-gc");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let node_id = NodeId::from_legacy_named("node_pending_unprotected_content").expect("node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_pending_unprotected_content").expect("cluster"),
            "pending unprotected content",
            node_id,
            "local",
            "pk-local",
            OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("sync raft");
        drop(
            open_daemon_dfs_metadata_backend(&data_dir, &cluster_paths)
                .expect("initialize metadata backend"),
        );
        let content =
            LocalContentStore::new(data_dir.dfs_dir().join("content")).expect("content store");
        let orphan = content
            .put_object_version_blob_only(
                "content-only-object",
                "content-only-version",
                b"content without an authoritative metadata root",
            )
            .expect("store content-only blob");
        assert!(
            content
                .has_blob(&orphan.content_hash)
                .expect("content-only blob exists")
        );

        let pending = daemon_dfs_pending_maintenance_work(&paths).expect("pending maintenance");
        assert_eq!(pending.due_time_based_maintenance_count, 0);
        assert!(
            !pending.should_trigger_background_maintenance(),
            "unprotected content candidates must not create a daemon maintenance loop"
        );
        assert!(
            content
                .has_blob(&orphan.content_hash)
                .expect("content-only blob remains"),
            "the scheduling probe must remain read-only"
        );
    }

    #[test]
    fn daemon_dfs_content_gc_refresh_classifier_parks_only_structural_or_handoff_gaps() {
        for reason in [
            DfsContentRootCatalogIncompleteReason::NoFileSystemsConfigured,
            DfsContentRootCatalogIncompleteReason::NoMetadataShardsConfigured,
            DfsContentRootCatalogIncompleteReason::MembershipHandoffFrozen {
                handoff_id: Some("handoff".to_string()),
            },
            DfsContentRootCatalogIncompleteReason::PageBudgetExceeded { max_pages: 1 },
        ] {
            assert!(
                !daemon_dfs_content_gc_blocked_reason_requires_refresh(&reason),
                "{reason:?} must remain parked on its state change and bounded deadline"
            );
        }
        for reason in [
            DfsContentRootCatalogIncompleteReason::MetadataBackendUnavailable,
            DfsContentRootCatalogIncompleteReason::StateFingerprintChanged,
            DfsContentRootCatalogIncompleteReason::CatalogNotDurablyInstalled,
            DfsContentRootCatalogIncompleteReason::CatalogTimestampInFuture {
                identity: None,
                observed_unix_nanos: u64::MAX,
                now_unix_nanos: 1,
            },
        ] {
            assert!(
                daemon_dfs_content_gc_blocked_reason_requires_refresh(&reason),
                "{reason:?} must request a fresh authoritative catalog immediately"
            );
        }
        let ordinary_advance = DfsContentRootCatalogIncompleteReason::CatalogBehindHeadWatermark {
            identity: sinomo_dfs::DfsContentRootCatalogShardIdentity {
                file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("filesystem"),
                shard_id: DfsMetadataShardId::new("root"),
                primary_node_id: NodeId::from_legacy_named("node_catalog").expect("primary"),
                epoch: 1,
            },
            catalog_journal_sequence: 1,
            watermark_journal_sequence: 2,
        };
        assert!(daemon_dfs_content_gc_blocked_reason_is_ordinary_head_advance(&ordinary_advance));
        assert!(
            !daemon_dfs_content_gc_blocked_reason_is_ordinary_head_advance(
                &DfsContentRootCatalogIncompleteReason::ShardIncomplete {
                    identity: sinomo_dfs::DfsContentRootCatalogShardIdentity {
                        file_system_id: DfsFileSystemId::from_legacy_named("sifs")
                            .expect("filesystem"),
                        shard_id: DfsMetadataShardId::new("root"),
                        primary_node_id: NodeId::from_legacy_named("node_catalog")
                            .expect("primary"),
                        epoch: 1,
                    },
                    reasons: vec![
                        ordinary_advance,
                        DfsContentRootCatalogIncompleteReason::StateFingerprintChanged,
                    ],
                }
            ),
            "a mixed head-advance and fingerprint failure must remain immediately repairable"
        );
    }

    #[test]
    fn daemon_dfs_content_gc_stale_catalog_observations_request_a_new_generation() {
        let version_report = sinomo_dfs::ContentStoreVersionGcReport {
            retained_same_or_stale_catalog_version_count: 2,
            ..sinomo_dfs::ContentStoreVersionGcReport::default()
        };
        assert_eq!(daemon_dfs_version_gc_candidate_count(&version_report), 2);

        let orphan_report = sinomo_dfs::ContentStoreGcReport {
            retained_same_or_stale_catalog_orphan_count: 3,
            ..sinomo_dfs::ContentStoreGcReport::default()
        };
        assert_eq!(daemon_dfs_orphan_gc_candidate_count(&orphan_report), 3);
    }

    #[test]
    fn daemon_dfs_content_gc_projection_requires_complete_catalog_coverage() {
        let root = temp_root("pending-content-gc-catalog-coverage");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local = NodeId::from_legacy_named("node_pending_content_catalog").expect("local node");
        let file_system = distributed_sifs(local.clone());
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: local.clone(),
            replica_node_ids: Vec::new(),
            epoch: 3,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        install_test_metadata_replica_shard(
            &cluster_paths,
            "cluster_pending_content_catalog",
            &local,
            &shard,
        );
        let state_fingerprint = format!("dfs-metadata-prefix-v1:blake3:{}", "0".repeat(64));
        let watermark = sinomo_dfs::DfsMetadataShardHeadWatermark {
            file_system_id: shard.file_system_id.clone(),
            shard_id: shard.shard_id.clone(),
            primary_node_id: local.clone(),
            epoch: shard.epoch,
            journal_sequence: 7,
            state_fingerprint: state_fingerprint.clone(),
            acknowledged_node_ids: vec![local],
        };
        append_cluster_raft_command(
            &cluster_paths,
            &shard.primary_node_id,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    sinomo_dfs::metadata_shard_head_watermark_key(
                        &shard.file_system_id,
                        &shard.shard_id,
                    ),
                    &watermark,
                    None,
                )
                .expect("head watermark op"),
            ),
        )
        .expect("write head watermark");

        let content_root = data_dir.dfs_dir().join("content");
        let content = LocalContentStore::new(&content_root).expect("content store");
        content
            .put_object_version(
                "pending-content-object",
                "pending-content-version-1",
                b"old manifest",
            )
            .expect("old manifest");
        content
            .put_object_version(
                "pending-content-object",
                "pending-content-version-2",
                b"latest manifest",
            )
            .expect("latest manifest");
        content
            .put_object_version_blob_only(
                "pending-orphan-object",
                "pending-orphan-version",
                b"orphan blob",
            )
            .expect("orphan blob");
        for reservation in content
            .content_reservation_roots()
            .expect("reservation roots")
            .reservations
        {
            content
                .release_content_reservation_for_test(
                    &reservation.identity.object_id,
                    &reservation.identity.version,
                    &reservation.identity.content_hash,
                )
                .expect("release fixture reservation");
        }
        let reservations = content
            .content_reservation_roots()
            .expect("empty reservation roots");
        assert_eq!(reservations.reservation_count, 0);

        let generated_at_unix_nanos = daemon_dfs_now_unix_nanos().saturating_sub(1);
        let file_system_config = test_file_system_config_for_shard(&shard);
        let topology_digest =
            sinomo_dfs::dfs_content_root_catalog_topology_digest_with_file_systems(
                std::slice::from_ref(&file_system_config),
                std::slice::from_ref(&shard),
            )
            .expect("topology digest");
        let identity = sinomo_dfs::DfsContentRootCatalogShardIdentity::from_config(&shard);
        let complete_catalog = sinomo_dfs::DfsContentRootCatalog::from_fragments(
            topology_digest.clone(),
            vec![identity.clone()],
            generated_at_unix_nanos,
            vec![sinomo_dfs::DfsContentRootCatalogFragment {
                identity: identity.clone(),
                topology_digest: topology_digest.clone(),
                generated_at_unix_nanos,
                coverage: sinomo_dfs::DfsContentRootCatalogCoverage::Complete {
                    prefix_proof_version: sinomo_dfs::DFS_CONTENT_ROOT_CATALOG_PREFIX_PROOF_VERSION,
                    journal_sequence: watermark.journal_sequence,
                    state_fingerprint,
                },
                entries: Vec::new(),
            }],
        )
        .expect("complete catalog");
        default_dfs_content_root_catalog_store(&content_root)
            .install_test_catalog_at(complete_catalog.clone(), generated_at_unix_nanos)
            .expect("install complete catalog");
        let mut lifecycle = DfsMaintenanceRunOptions::default();
        lifecycle.content_version_gc_min_age = Duration::ZERO;
        lifecycle.content_version_gc_keep_latest_versions = 1;
        lifecycle.content_orphan_gc_min_age = Duration::ZERO;
        lifecycle.content_root_catalog_max_age = Duration::from_secs(60);
        let complete = daemon_dfs_content_gc_projection(
            &data_dir,
            &cluster_paths,
            &content,
            &reservations,
            lifecycle,
            daemon_dfs_now_unix_nanos(),
        )
        .expect("complete projection");
        assert_eq!(complete.covered_shard_count, 1);
        assert_eq!(complete.expected_shard_count, 1);
        assert!(complete.blocked_reasons.is_empty());
        assert_eq!(complete.scrub_blocked_issue_count, 0);
        assert_eq!(complete.version_candidate_count, 1);
        assert_eq!(complete.orphan_candidate_count, 1);
        assert!(complete.due_maintenance_count >= 2);
        assert_eq!(complete.reservation_due_maintenance_count, 0);
        assert!(complete.next_deadline_unix_nanos.is_some());

        let covered_object_id =
            DfsObjectId::from_legacy_named("covered-reservation-object").expect("object id");
        let covered_version =
            DfsVersion::from_legacy_named("covered-reservation-version").expect("version");
        let covered_content_hash = format!("blake3:{}", "2".repeat(64));
        let covered_catalog = sinomo_dfs::DfsContentRootCatalog::from_fragments(
            topology_digest.clone(),
            vec![identity.clone()],
            generated_at_unix_nanos,
            vec![sinomo_dfs::DfsContentRootCatalogFragment {
                identity: identity.clone(),
                topology_digest: topology_digest.clone(),
                generated_at_unix_nanos,
                coverage: sinomo_dfs::DfsContentRootCatalogCoverage::Complete {
                    prefix_proof_version: sinomo_dfs::DFS_CONTENT_ROOT_CATALOG_PREFIX_PROOF_VERSION,
                    journal_sequence: watermark.journal_sequence,
                    state_fingerprint: watermark.state_fingerprint.clone(),
                },
                entries: vec![sinomo_dfs::DfsContentRootCatalogEntry::Version(
                    sinomo_dfs::DfsMetadataVersionIdentity::new(
                        covered_object_id.clone(),
                        covered_version.clone(),
                    ),
                )],
            }],
        )
        .expect("covered-reservation catalog");
        default_dfs_content_root_catalog_store(&content_root)
            .install_test_catalog_at(covered_catalog, generated_at_unix_nanos)
            .expect("install covered-reservation catalog");
        let covered_reservations = sinomo_dfs::ContentStoreReservationRoots {
            reservation_count: 1,
            reservations: vec![sinomo_dfs::ContentStoreReservationRecord {
                identity: sinomo_dfs::ContentStoreReservationIdentity {
                    object_id: covered_object_id.as_str().to_string(),
                    version: covered_version.as_str().to_string(),
                    content_hash: covered_content_hash.clone(),
                },
                reserved_at_unix_nanos: generated_at_unix_nanos,
            }],
            versions: BTreeSet::from([(
                covered_object_id.as_str().to_string(),
                covered_version.as_str().to_string(),
            )]),
            content_hashes: BTreeSet::from([covered_content_hash]),
        };
        let covered_reservation = daemon_dfs_content_gc_projection(
            &data_dir,
            &cluster_paths,
            &content,
            &covered_reservations,
            lifecycle,
            daemon_dfs_now_unix_nanos(),
        )
        .expect("covered-reservation projection");
        assert_eq!(
            covered_reservation.due_maintenance_count, complete.due_maintenance_count,
            "catalog-covered reservations must not request destructive GC or another catalog refresh"
        );
        assert_eq!(
            covered_reservation.reservation_due_maintenance_count, 1,
            "catalog-covered reservations must request only bounded local reconciliation"
        );
        default_dfs_content_root_catalog_store(&content_root)
            .install_test_catalog_at(complete_catalog.clone(), generated_at_unix_nanos)
            .expect("restore catalog without rejected reservation");
        let rejected_object_id = "rejected-reservation-object".to_string();
        let rejected_version = "rejected-reservation-version".to_string();
        let rejection_grace_nanos =
            daemon_dfs_duration_nanos(lifecycle.content_reservation_rejection_grace);
        let rejected_reservations = sinomo_dfs::ContentStoreReservationRoots {
            reservation_count: 1,
            reservations: vec![sinomo_dfs::ContentStoreReservationRecord {
                identity: sinomo_dfs::ContentStoreReservationIdentity {
                    object_id: rejected_object_id.clone(),
                    version: rejected_version.clone(),
                    content_hash: format!("blake3:{}", "3".repeat(64)),
                },
                reserved_at_unix_nanos: generated_at_unix_nanos
                    .saturating_sub(rejection_grace_nanos.saturating_add(1)),
            }],
            versions: BTreeSet::from([(rejected_object_id, rejected_version)]),
            content_hashes: BTreeSet::new(),
        };
        let rejected_reservation = daemon_dfs_content_gc_projection(
            &data_dir,
            &cluster_paths,
            &content,
            &rejected_reservations,
            lifecycle,
            daemon_dfs_now_unix_nanos(),
        )
        .expect("rejection-due reservation projection");
        assert_eq!(
            rejected_reservation.due_maintenance_count,
            complete.due_maintenance_count.saturating_add(1),
            "an uncovered reservation with durable rejection evidence must request a fresh destructive pass"
        );
        assert_eq!(
            rejected_reservation.reservation_due_maintenance_count, 0,
            "rejection requires refreshed catalog admission rather than reuse-only reconciliation"
        );

        let behind_catalog = sinomo_dfs::DfsContentRootCatalog::from_fragments(
            topology_digest.clone(),
            vec![identity.clone()],
            generated_at_unix_nanos,
            vec![sinomo_dfs::DfsContentRootCatalogFragment {
                identity: identity.clone(),
                topology_digest: topology_digest.clone(),
                generated_at_unix_nanos,
                coverage: sinomo_dfs::DfsContentRootCatalogCoverage::Complete {
                    prefix_proof_version: sinomo_dfs::DFS_CONTENT_ROOT_CATALOG_PREFIX_PROOF_VERSION,
                    journal_sequence: watermark.journal_sequence.saturating_sub(1),
                    state_fingerprint: format!("dfs-metadata-prefix-v1:blake3:{}", "1".repeat(64)),
                },
                entries: vec![sinomo_dfs::DfsContentRootCatalogEntry::Version(
                    sinomo_dfs::DfsMetadataVersionIdentity::new(covered_object_id, covered_version),
                )],
            }],
        )
        .expect("behind catalog");
        default_dfs_content_root_catalog_store(&content_root)
            .install_test_catalog_at(behind_catalog, generated_at_unix_nanos)
            .expect("install behind catalog");
        let coalesced_now = generated_at_unix_nanos.saturating_add(1);
        let behind = daemon_dfs_content_gc_projection(
            &data_dir,
            &cluster_paths,
            &content,
            &covered_reservations,
            lifecycle,
            coalesced_now,
        )
        .expect("behind-head projection");
        assert_eq!(behind.catalog_journal_lag, 1);
        assert_eq!(
            behind.due_maintenance_count, 0,
            "ordinary head churn must not immediately fan out another full catalog refresh"
        );
        assert_eq!(
            behind.reservation_due_maintenance_count, 1,
            "historical positive coverage must keep draining reservations while destructive refresh is coalesced"
        );
        assert_eq!(
            behind.next_deadline_unix_nanos,
            Some(
                generated_at_unix_nanos.saturating_add(daemon_dfs_duration_nanos(
                    lifecycle.content_root_catalog_max_age,
                ))
            )
        );
        let repeated = daemon_dfs_content_gc_projection(
            &data_dir,
            &cluster_paths,
            &content,
            &covered_reservations,
            lifecycle,
            coalesced_now.saturating_add(1),
        )
        .expect("repeated behind-head projection");
        assert_eq!(
            repeated.next_deadline_unix_nanos, behind.next_deadline_unix_nanos,
            "head-lag coalescing must retain one absolute deadline"
        );
        let refresh_deadline = behind
            .next_deadline_unix_nanos
            .expect("coalesced refresh deadline");
        let expired = daemon_dfs_content_gc_projection(
            &data_dir,
            &cluster_paths,
            &content,
            &covered_reservations,
            lifecycle,
            refresh_deadline,
        )
        .expect("expired behind-head projection");
        assert!(
            expired.due_maintenance_count > 0,
            "the bounded coalescing deadline must make refresh due"
        );

        default_dfs_content_root_catalog_store(&content_root)
            .install_test_catalog_at(complete_catalog, generated_at_unix_nanos)
            .expect("restore complete catalog");
        std::fs::write(
            content_root
                .join("objects")
                .join("pending-content-object")
                .join("pending-content-version-2")
                .join("manifest.sobj"),
            b"torn manifest",
        )
        .expect("corrupt fixture manifest");
        let scrub_blocked = daemon_dfs_content_gc_projection(
            &data_dir,
            &cluster_paths,
            &content,
            &reservations,
            lifecycle,
            daemon_dfs_now_unix_nanos(),
        )
        .expect("scrub-blocked projection");
        assert!(
            scrub_blocked.scrub_blocked_issue_count > 0,
            "local scrub failures must remain visible when catalog coverage is complete"
        );

        let incomplete_catalog = sinomo_dfs::DfsContentRootCatalog::from_fragments(
            topology_digest.clone(),
            vec![identity.clone()],
            generated_at_unix_nanos,
            vec![sinomo_dfs::DfsContentRootCatalogFragment {
                identity: identity.clone(),
                topology_digest: topology_digest.clone(),
                generated_at_unix_nanos,
                coverage: sinomo_dfs::DfsContentRootCatalogCoverage::Incomplete {
                    reasons: vec![
                        DfsContentRootCatalogIncompleteReason::MetadataBackendUnavailable,
                    ],
                },
                entries: Vec::new(),
            }],
        )
        .expect("incomplete catalog");
        default_dfs_content_root_catalog_store(&content_root)
            .install_test_catalog_at(incomplete_catalog, generated_at_unix_nanos)
            .expect("install incomplete catalog");
        let incomplete = daemon_dfs_content_gc_projection(
            &data_dir,
            &cluster_paths,
            &content,
            &reservations,
            lifecycle,
            daemon_dfs_now_unix_nanos(),
        )
        .expect("incomplete projection");
        assert_eq!(incomplete.covered_shard_count, 0);
        assert_eq!(incomplete.expected_shard_count, 1);
        assert_eq!(incomplete.version_candidate_count, 0);
        assert_eq!(incomplete.orphan_candidate_count, 0);
        assert!(incomplete.due_maintenance_count > 0);
        assert_eq!(incomplete.next_deadline_unix_nanos, None);
        assert!(matches!(
            incomplete.blocked_reasons.as_slice(),
            [DfsContentRootCatalogIncompleteReason::ShardIncomplete { .. }]
        ));

        let parked_catalog = sinomo_dfs::DfsContentRootCatalog::from_fragments(
            topology_digest.clone(),
            vec![identity.clone()],
            generated_at_unix_nanos,
            vec![sinomo_dfs::DfsContentRootCatalogFragment {
                identity,
                topology_digest,
                generated_at_unix_nanos,
                coverage: sinomo_dfs::DfsContentRootCatalogCoverage::Incomplete {
                    reasons: vec![
                        DfsContentRootCatalogIncompleteReason::MembershipHandoffFrozen {
                            handoff_id: Some("pending-catalog-handoff".to_string()),
                        },
                    ],
                },
                entries: Vec::new(),
            }],
        )
        .expect("parked handoff catalog");
        default_dfs_content_root_catalog_store(&content_root)
            .install_test_catalog_at(parked_catalog, generated_at_unix_nanos)
            .expect("install parked handoff catalog");
        let parked = daemon_dfs_content_gc_projection(
            &data_dir,
            &cluster_paths,
            &content,
            &reservations,
            DfsMaintenanceRunOptions::default(),
            daemon_dfs_now_unix_nanos(),
        )
        .expect("parked handoff projection");
        assert_eq!(parked.due_maintenance_count, 0);
        assert_eq!(
            parked.next_deadline_unix_nanos,
            Some(
                generated_at_unix_nanos.saturating_add(daemon_dfs_duration_nanos(
                    DfsMaintenanceRunOptions::default().content_root_catalog_max_age,
                ))
            )
        );

        let paths = DaemonPaths::with_root(data_dir.root().join("daemon"));
        let pending = daemon_dfs_pending_maintenance_work(&paths).expect("pending maintenance");
        assert_eq!(
            pending.due_time_based_maintenance_count, 0,
            "routine scheduling must not import a content-inventory deadline"
        );
        assert!(
            !pending.should_trigger_background_maintenance(),
            "an explicit disaster-recovery projection must not leak into routine scheduling"
        );
    }

    #[test]
    fn daemon_dfs_metadata_history_gc_schedule_combines_pressure_and_safe_age() {
        fn projection(
            eligible_record_count: u64,
            oldest_eligible_record_unix_nanos: Option<u64>,
            next_eligible_at_unix_nanos: Option<u64>,
            total_record_count: u64,
            estimated_serialized_bytes: u64,
            snapshot_size_bytes: u64,
            journal_size_bytes: u64,
        ) -> sinomo_dfs::DfsMetadataHistoryGcReport {
            sinomo_dfs::DfsMetadataHistoryGcReport {
                min_history_age_nanos: 100,
                eligible_version_count: eligible_record_count,
                oldest_eligible_record_unix_nanos,
                next_eligible_at_unix_nanos,
                shards: vec![sinomo_dfs::DfsMetadataHistoryShardStats {
                    shard_id: DfsMetadataShardId::new("root"),
                    collection_authorized: true,
                    snapshot_size_bytes: Some(snapshot_size_bytes),
                    journal_size_bytes,
                    journal_segment_count: 1,
                    journal_head_sequence: 1,
                    total_record_count,
                    estimated_serialized_bytes,
                    tables: Vec::new(),
                }],
                ..Default::default()
            }
        }

        let young_pressure = projection(0, None, Some(1_100), 10, 10, 10, 10);
        assert_eq!(
            daemon_dfs_metadata_history_gc_schedule(
                &young_pressure,
                1_000,
                10,
                10,
                Duration::from_nanos(200),
            ),
            DaemonDfsMetadataHistoryGcSchedule {
                due_record_count: 0,
                next_deadline_unix_nanos: Some(1_100),
            },
            "young-only high-water pressure must not launch a no-op GC pass"
        );

        let below_pressure = projection(1, Some(850), None, 9, 9, 9, 9);
        assert_eq!(
            daemon_dfs_metadata_history_gc_schedule(
                &below_pressure,
                1_000,
                10,
                10,
                Duration::from_nanos(200),
            ),
            DaemonDfsMetadataHistoryGcSchedule {
                due_record_count: 0,
                next_deadline_unix_nanos: Some(1_050),
            },
            "low-volume eligible history should park until its maximum-age deadline"
        );

        let record_pressure = projection(1, Some(850), None, 10, 9, 9, 9);
        assert_eq!(
            daemon_dfs_metadata_history_gc_schedule(
                &record_pressure,
                1_000,
                10,
                10,
                Duration::from_nanos(200),
            )
            .due_record_count,
            1
        );

        let mut replica_pressure = projection(1, Some(850), None, 9, 9, 9, 9);
        replica_pressure
            .shards
            .push(sinomo_dfs::DfsMetadataHistoryShardStats {
                shard_id: DfsMetadataShardId::new("replica"),
                collection_authorized: false,
                snapshot_size_bytes: Some(100),
                journal_size_bytes: 100,
                journal_segment_count: 1,
                journal_head_sequence: 1,
                total_record_count: 100,
                estimated_serialized_bytes: 100,
                tables: Vec::new(),
            });
        assert_eq!(
            daemon_dfs_metadata_history_gc_schedule(
                &replica_pressure,
                1_000,
                10,
                10,
                Duration::from_nanos(200),
            ),
            DaemonDfsMetadataHistoryGcSchedule {
                due_record_count: 0,
                next_deadline_unix_nanos: Some(1_050),
            },
            "replica and frozen-shard pressure must not schedule authoritative GC"
        );

        let byte_pressure = projection(1, Some(850), None, 9, 9, 9, 10);
        assert_eq!(
            daemon_dfs_metadata_history_gc_schedule(
                &byte_pressure,
                1_000,
                10,
                10,
                Duration::from_nanos(200),
            )
            .due_record_count,
            1
        );

        let age_pressure = projection(1, Some(800), None, 9, 9, 9, 9);
        assert_eq!(
            daemon_dfs_metadata_history_gc_schedule(
                &age_pressure,
                1_000,
                10,
                10,
                Duration::from_nanos(200),
            )
            .due_record_count,
            1
        );

        let configured_minimum_wins = projection(1, Some(850), None, 9, 9, 9, 9);
        let mut configured_minimum_wins = configured_minimum_wins;
        configured_minimum_wins.min_history_age_nanos = 200;
        assert_eq!(
            daemon_dfs_metadata_history_gc_schedule(
                &configured_minimum_wins,
                1_000,
                10,
                10,
                Duration::from_nanos(50),
            ),
            DaemonDfsMetadataHistoryGcSchedule {
                due_record_count: 0,
                next_deadline_unix_nanos: Some(1_050),
            },
            "the age trigger must never shorten the configured recovery window"
        );
    }

    #[test]
    fn daemon_dfs_maintenance_continuation_requires_explicit_progress_and_remainder() {
        fn empty_report() -> DfsMaintenanceTickReport {
            DfsMaintenanceTickReport {
                namespace_jobs: Default::default(),
                conflicts: Default::default(),
                write_intents: Default::default(),
                placement_reconciliation: None,
                lifecycle_reports: Vec::new(),
                scheduled_repairs: Vec::new(),
                followup_events: Vec::new(),
                repair_attempts_before: Vec::new(),
                repair_attempts_after: Vec::new(),
                repair_budget_exhausted: false,
                metadata_replication: DfsMetadataJournalReplicationReport {
                    outcomes: Vec::new(),
                },
            }
        }

        let mut no_progress_namespace = empty_report();
        no_progress_namespace.namespace_jobs.resumed_job_count = 1;
        no_progress_namespace
            .namespace_jobs
            .remaining_running_job_count = 1;
        assert!(
            !daemon_dfs_maintenance_report_reached_limit(&no_progress_namespace),
            "a still-running job is not evidence that its successful pass advanced"
        );

        let mut bounded_namespace = empty_report();
        bounded_namespace.namespace_jobs.resumed_job_count = 1;
        bounded_namespace.namespace_jobs.completed_job_count = 1;
        bounded_namespace.namespace_jobs.remaining_running_job_count = 1;
        assert!(
            daemon_dfs_maintenance_report_reached_limit(&bounded_namespace),
            "a completed bounded job plus an explicitly skipped running job owns continuation"
        );

        let mut bounded_repairs = empty_report();
        bounded_repairs.repair_budget_exhausted = true;
        assert!(
            daemon_dfs_maintenance_report_reached_limit(&bounded_repairs),
            "an immediately runnable repair remainder owns continuation"
        );

        let mut no_progress_compaction = empty_report();
        no_progress_compaction
            .lifecycle_reports
            .push(sinomo_dfs::DfsLifecycleMaintenanceReport {
                tombstone_compaction: Some(sinomo_dfs::DfsTombstoneCompactionReport {
                    max_compactions_reached: true,
                    ..Default::default()
                }),
                ..Default::default()
            });
        assert!(
            !daemon_dfs_maintenance_report_reached_limit(&no_progress_compaction),
            "a zero-progress retained-limit report must keep ordinary backoff"
        );
        no_progress_compaction.lifecycle_reports[0]
            .tombstone_compaction
            .as_mut()
            .expect("compaction report")
            .compacted_tombstone_count = 1;
        assert!(daemon_dfs_maintenance_report_reached_limit(
            &no_progress_compaction
        ));

        let mut no_progress_history_gc = empty_report();
        no_progress_history_gc
            .lifecycle_reports
            .push(sinomo_dfs::DfsLifecycleMaintenanceReport {
                metadata_history_gc: Some(sinomo_dfs::DfsMetadataHistoryGcReport {
                    max_removals_reached: true,
                    ..Default::default()
                }),
                ..Default::default()
            });
        assert!(
            !daemon_dfs_maintenance_report_reached_limit(&no_progress_history_gc),
            "a zero-progress metadata-history limit must keep ordinary backoff"
        );
        no_progress_history_gc.lifecycle_reports[0]
            .metadata_history_gc
            .as_mut()
            .expect("metadata history GC report")
            .removed_version_count = 1;
        assert!(daemon_dfs_maintenance_report_reached_limit(
            &no_progress_history_gc
        ));

        let mut reservation_batch = empty_report();
        reservation_batch
            .lifecycle_reports
            .push(sinomo_dfs::DfsLifecycleMaintenanceReport {
                content_reservation_reconciliation: Some(
                    sinomo_dfs::ContentStoreReservationReconciliationReport {
                        has_more: true,
                        ..Default::default()
                    },
                ),
                ..Default::default()
            });
        assert!(
            !daemon_dfs_maintenance_report_reached_limit(&reservation_batch),
            "an empty reservation batch must not spin an immediate continuation"
        );
        reservation_batch.lifecycle_reports[0]
            .content_reservation_reconciliation
            .as_mut()
            .expect("reservation reconciliation report")
            .examined_reservation_count = 1;
        assert!(
            daemon_dfs_maintenance_report_reached_limit(&reservation_batch),
            "a bounded reservation batch with remaining identities must drain immediately"
        );
    }

    fn test_default_cluster_bootstrap_host(
        data_dir: &SinomoDataDir,
        cluster_paths: &ClusterPaths,
        local_node_id: NodeId,
        runtime: &tokio::runtime::Handle,
    ) -> sinomo_dfs::DfsControlPlaneDefaultClusterBootstrapHost {
        sinomo_dfs::DfsControlPlaneDefaultClusterBootstrapHost::new(
            data_dir.clone(),
            cluster_paths.clone(),
            local_node_id,
            Box::new(DaemonDfsControlPlaneKvClient::new_on_runtime(
                cluster_paths,
                runtime,
            )),
        )
    }

    fn process_test_dfs_metadata_journal_batch(
        paths: &DaemonPaths,
        command: DfsMetadataJournalBatchCommand,
    ) -> Result<DfsMetadataJournalBatchResponse, DaemonError> {
        let data_dir = daemon_data_dir(paths)?;
        let data_dir = SinomoDataDir::new(data_dir);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let authenticated_source_node_id =
            command
                .source_node_id
                .clone()
                .ok_or_else(|| DaemonError::Network {
                    address: "DFS metadata journal test source".to_string(),
                    detail: "metadata journal test command has no claimed source".to_string(),
                })?;
        sinomo_dfs::process_cluster_dfs_metadata_journal_batch(
            &data_dir,
            &cluster_paths,
            &authenticated_source_node_id,
            command,
        )
        .map_err(Into::into)
    }

    #[test]
    fn daemon_dfs_metadata_journal_batch_applies_to_local_backend() {
        let root = temp_root("daemon_dfs_metadata_journal_batch");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let source_root = root.join("source-meta");
        let source_node_id =
            NodeId::from_legacy_named("node_daemon_dfs_metadata_source").expect("source node");
        let local_node_id =
            NodeId::from_legacy_named("node_daemon_dfs_metadata_local").expect("local node");
        let file_system = distributed_sifs(source_node_id.clone());
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: source_node_id.clone(),
            replica_node_ids: vec![local_node_id.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        install_test_metadata_replica_shard(
            &cluster_paths,
            "cluster_daemon_dfs_metadata_apply",
            &local_node_id,
            &shard,
        );
        let object = distributed_sifs_bootstrap_objects(source_node_id.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        let mut source =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), source_root)
                .expect("source backend");
        source.put_file_system(&file_system).expect("filesystem");
        source.put_object(&object).expect("object");
        source
            .put_version(&DfsObjectVersionRecord {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                size_bytes: object.content_size_bytes,
                locations: object.current_locations.clone(),
                created_by_event_id: object.created_by_event_id.clone(),
                created_at: OffsetDateTime::now_utc(),
                visible: true,
            })
            .expect("version");
        let mut subscription = source.subscribe(0);
        let batch = source
            .poll_subscription(&mut subscription)
            .expect("source batch");

        let response = process_test_dfs_metadata_journal_batch(
            &paths,
            DfsMetadataJournalBatchCommand {
                request_id: "dfs_meta_req_1".to_string(),
                source_node_id: Some(source_node_id),
                file_system_id: Some(file_system.file_system_id.clone()),
                authority_epoch: Some(shard.epoch),
                verify_prefix_only: false,
                prefix_probe: None,
                authority_probe: None,
                batch,
            },
        )
        .expect("apply batch");
        assert_eq!(response.result, DfsMetadataJournalBatchResult::Applied);
        assert_eq!(response.applied_entries, 3);

        let target = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            root.join("data").join("dfs").join("metadata").join("root"),
        )
        .expect("target backend");
        assert_eq!(
            target.get_object(&object.object_id).expect("target object"),
            Some(object)
        );
    }

    #[test]
    fn daemon_dfs_metadata_journal_batch_uses_path_safe_shard_directory() {
        let root = temp_root("daemon_dfs_metadata_journal_batch_safe_shard");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let source_root = root.join("source-meta");
        let source_node_id =
            NodeId::from_legacy_named("node_daemon_dfs_metadata_safe_source").expect("source node");
        let local_node_id =
            NodeId::from_legacy_named("node_daemon_dfs_metadata_safe_local").expect("local node");
        let file_system = distributed_sifs(source_node_id.clone());
        let object = distributed_sifs_bootstrap_objects(source_node_id.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        let shard_id = DfsMetadataShardId::new("root/frag");
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: shard_id.0.clone(),
            primary_node_id: source_node_id.clone(),
            replica_node_ids: vec![local_node_id.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        install_test_metadata_replica_shard(
            &cluster_paths,
            "cluster_daemon_dfs_metadata_safe",
            &local_node_id,
            &shard,
        );
        let mut source =
            LocalSobjMetadataBackend::new(shard_id.clone(), source_root).expect("source backend");
        source.put_file_system(&file_system).expect("filesystem");
        source.put_object(&object).expect("object");
        let mut subscription = source.subscribe(0);
        let batch = source
            .poll_subscription(&mut subscription)
            .expect("source batch");

        let response = process_test_dfs_metadata_journal_batch(
            &paths,
            DfsMetadataJournalBatchCommand {
                request_id: "dfs_meta_req_safe".to_string(),
                source_node_id: Some(source_node_id),
                file_system_id: Some(file_system.file_system_id.clone()),
                authority_epoch: Some(shard.epoch),
                verify_prefix_only: false,
                prefix_probe: None,
                authority_probe: None,
                batch,
            },
        )
        .expect("apply batch");
        assert_eq!(response.result, DfsMetadataJournalBatchResult::Applied);

        let target = LocalSobjMetadataBackend::new(
            shard_id,
            root.join("data")
                .join("dfs")
                .join("metadata")
                .join("root_frag"),
        )
        .expect("target backend");
        assert_eq!(
            target.get_object(&object.object_id).expect("target object"),
            Some(object)
        );
        assert!(
            !root
                .join("data")
                .join("dfs")
                .join("metadata")
                .join("root")
                .join("frag")
                .exists()
        );
    }

    #[test]
    fn daemon_dfs_metadata_journal_batch_rejects_non_primary_source() {
        let root = temp_root("daemon_dfs_metadata_journal_batch_rejects_stale_source");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local = NodeId::from_legacy_named("node_daemon_dfs_meta_local").expect("local");
        let primary = NodeId::from_legacy_named("node_daemon_dfs_meta_primary").expect("primary");
        let stale = NodeId::from_legacy_named("node_daemon_dfs_meta_stale").expect("stale");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named("cluster_daemon_dfs_meta_reject")
                .expect("cluster"),
            "daemon dfs metadata reject",
            local.clone(),
            "local",
            "pk-local",
            OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        let file_system = distributed_sifs(primary.clone());
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: primary.clone(),
            replica_node_ids: vec![local.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &shard,
                    None,
                )
                .expect("metadata shard op"),
            ),
        )
        .expect("write shard config");
        let source_root = root.join("source-meta");
        let mut source =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), source_root)
                .expect("source backend");
        source.put_file_system(&file_system).expect("filesystem");
        let object = distributed_sifs_bootstrap_objects(primary.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        source.put_object(&object).expect("object");
        let mut subscription = source.subscribe(0);
        let batch = source
            .poll_subscription(&mut subscription)
            .expect("source batch");

        let response = process_test_dfs_metadata_journal_batch(
            &paths,
            DfsMetadataJournalBatchCommand {
                request_id: "dfs_meta_req_stale_source".to_string(),
                source_node_id: Some(stale.clone()),
                file_system_id: Some(file_system.file_system_id.clone()),
                authority_epoch: Some(shard.epoch),
                verify_prefix_only: false,
                prefix_probe: None,
                authority_probe: None,
                batch,
            },
        )
        .expect("reject batch");

        assert!(matches!(
            response.result,
            DfsMetadataJournalBatchResult::Rejected { ref reason }
                if reason.contains(&format!("source {stale} is not shard root primary {primary}"))
        ));
        let target = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            data_dir.dfs_dir().join("metadata").join("root"),
        )
        .expect("target backend");
        assert!(
            target
                .get_object(&object.object_id)
                .expect("target object")
                .is_none(),
            "rejected non-primary journal batch must not mutate the local replica"
        );
    }

    #[test]
    fn daemon_dfs_status_reads_default_metadata_backend() {
        let root = temp_root("daemon_dfs_status");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let node_id = NodeId::from_legacy_named("node_daemon_dfs_status").expect("node");
        let file_system = distributed_sifs(node_id.clone());
        let object = distributed_sifs_bootstrap_objects(node_id)
            .into_iter()
            .next()
            .expect("bootstrap object");
        let mut backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            data_dir.dfs_dir().join("metadata").join("root"),
        )
        .expect("metadata backend");
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");
        backend
            .put_version(&DfsObjectVersionRecord {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                size_bytes: object.content_size_bytes,
                locations: object.current_locations.clone(),
                created_by_event_id: object.created_by_event_id.clone(),
                created_at: OffsetDateTime::now_utc(),
                visible: true,
            })
            .expect("version");

        let status = sinomo_dfs::inspect_cluster_aware_dfs_status(&data_dir, &cluster_paths)
            .expect("dfs status");

        assert_eq!(status.file_system_count, 1);
        assert_eq!(status.object_count, 1);
        assert_eq!(status.version_count, 1);
    }

    #[test]
    fn daemon_dfs_maintenance_tick_resumes_namespace_jobs() {
        let root = temp_root("daemon_dfs_maintenance_namespace_jobs");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_namespace_jobs")
                .expect("cluster");
        let node_id = NodeId::from_legacy_named("node_daemon_dfs_namespace_jobs").expect("node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id,
            "daemon dfs namespace jobs",
            node_id.clone(),
            "local",
            "pk-local",
            now,
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("sync raft");

        let old_dir = "/services/sandbox/instances/daemon_namespace_job/work/old";
        let new_dir = "/services/sandbox/instances/daemon_namespace_job/work/new";
        let old_file = format!("{old_dir}/nested/file.txt");
        let new_file = format!("{new_dir}/nested/file.txt");
        let job_id = {
            let content =
                LocalContentStore::new(data_dir.dfs_dir().join("content")).expect("content store");
            let metadata = open_daemon_dfs_metadata_backend(&data_dir, &cluster_paths)
                .expect("metadata backend");
            let mut service = NetworkedDfsService::new(
                daemon_dfs_node_transport(node_id.clone(), Vec::new()),
                content,
            )
            .with_boxed_metadata_backend(metadata)
            .expect("service backend");
            service
                .bootstrap_distributed_sifs()
                .expect("bootstrap sifs");
            service
                .posix_mkdir_all(&format!("{old_dir}/nested"))
                .expect("mkdir old tree");
            service
                .posix_write_file(
                    &old_file,
                    b"daemon namespace job",
                    DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
                )
                .expect("write file");
            let job = service
                .posix_rename_directory_bounded(old_dir, new_dir, 1)
                .expect("start bounded rename");
            assert_eq!(job.status, DfsNamespaceJobStatus::Running);
            job.job_id
        };

        let status = sinomo_dfs::inspect_cluster_aware_dfs_status(&data_dir, &cluster_paths)
            .expect("status before maintenance");
        assert_eq!(status.pending_namespace_job_count, 1);
        let pending = daemon_dfs_pending_maintenance_work(&paths).expect("pending maintenance");
        assert_eq!(pending.pending_write_count, 0);
        assert_eq!(pending.pending_namespace_job_count, 1);
        assert_eq!(pending.pending_repair_count, 0);
        assert_eq!(pending.repair_attempt_count, 0);
        assert_eq!(pending.due_repair_attempt_count, 0);
        assert_eq!(pending.unattempted_repair_count, 0);
        assert_eq!(pending.next_repair_retry_unix_nanos, None);
        assert_eq!(pending.metadata_replica_lag_count, 0);
        assert_eq!(pending.pending_local_membership_handoff_count, 0);
        assert!(pending.has_work());
        assert!(pending.should_trigger_background_maintenance());

        let runtime = daemon_test_runtime("daemon-dfs-maintenance-namespace-jobs");
        run_daemon_dfs_maintenance_body_for_test(&paths, &runtime)
            .expect("maintenance tick before shard bootstrap");

        let status = sinomo_dfs::inspect_cluster_aware_dfs_status(&data_dir, &cluster_paths)
            .expect("status after deferred maintenance");
        assert_eq!(status.pending_namespace_job_count, 1);
        assert_eq!(status.failed_namespace_job_count, 0);
        assert!(
            daemon_dfs_pending_maintenance_work(&paths)
                .expect("pending maintenance after deferred tick")
                .has_work()
        );
        let backend =
            open_daemon_dfs_metadata_backend(&data_dir, &cluster_paths).expect("metadata backend");
        let deferred_job = backend
            .get_namespace_job(&job_id)
            .expect("get deferred namespace job")
            .expect("deferred namespace job");
        assert_eq!(deferred_job.status, DfsNamespaceJobStatus::Running);
        assert!(deferred_job.completed_at.is_none());
        drop(backend);

        let file_system = sinomo_dfs::default_cluster_sifs(&state);
        let root_shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id,
            shard_id: "root".to_string(),
            primary_node_id: node_id.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &node_id,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&root_shard.file_system_id, &root_shard.shard_id),
                    &root_shard,
                    None,
                )
                .expect("root metadata shard op"),
            ),
        )
        .expect("commit root metadata shard");

        run_daemon_dfs_maintenance_body_for_test(&paths, &runtime)
            .expect("maintenance tick after shard bootstrap");

        let status = sinomo_dfs::inspect_cluster_aware_dfs_status(&data_dir, &cluster_paths)
            .expect("status after configured maintenance");
        assert_eq!(status.pending_namespace_job_count, 0);
        assert_eq!(status.failed_namespace_job_count, 0);
        let pending =
            daemon_dfs_pending_maintenance_work(&paths).expect("pending maintenance after tick");
        assert!(!pending.has_work(), "unexpected pending work: {pending:#?}");
        let backend =
            open_daemon_dfs_metadata_backend(&data_dir, &cluster_paths).expect("metadata backend");
        let job = backend
            .get_namespace_job(&job_id)
            .expect("get namespace job")
            .expect("namespace job");
        assert_eq!(job.status, DfsNamespaceJobStatus::Completed);
        assert!(job.completed_at.is_some());

        let content =
            LocalContentStore::new(data_dir.dfs_dir().join("content")).expect("content store");
        let service =
            NetworkedDfsService::new(daemon_dfs_node_transport(node_id, Vec::new()), content)
                .with_boxed_metadata_backend(backend)
                .expect("service backend");
        assert_eq!(
            service.posix_read_file(&new_file).expect("read moved file"),
            b"daemon namespace job"
        );
        assert!(service.posix_read_file(&old_file).is_err());
    }

    #[test]
    fn daemon_dfs_conflict_probe_parks_an_open_base_conflict() {
        let root = temp_root("pending-conflict-classifier");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let node_id = NodeId::from_legacy_named("node_pending_conflict").expect("node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_pending_conflict").expect("cluster"),
            "pending conflict classifier",
            node_id.clone(),
            "local",
            "pk-local",
            OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("sync raft");

        let file_system = sinomo_dfs::default_cluster_sifs(&state);
        let object_id =
            DfsObjectId::from_legacy_named("object_pending_conflict").expect("object id");
        let base_version =
            DfsVersion::from_legacy_named("version_pending_conflict_base").expect("base version");
        let local_version =
            DfsVersion::from_legacy_named("version_pending_conflict_local").expect("local version");
        let remote_version = DfsVersion::from_legacy_named("version_pending_conflict_remote")
            .expect("remote version");
        let mut object = pending_object_metadata_for_test(
            object_id.clone(),
            file_system.file_system_id.clone(),
            base_version.clone(),
            node_id.clone(),
            "/services/pending-conflict.txt",
        );
        let conflict = DfsConflictRecord {
            conflict_id: "conflict_pending_base".to_string(),
            file_system_id: file_system.file_system_id.clone(),
            object_id: object_id.clone(),
            path: object.path.clone(),
            base_version: Some(base_version),
            local_version: local_version.clone(),
            remote_version,
            detected_by_node_id: node_id,
            reason: "base remains visible".to_string(),
            local_content_hash: Some("blake3:local-pending-conflict".to_string()),
            remote_content_hash: Some("blake3:remote-pending-conflict".to_string()),
            created_by_intent_id: None,
            strategy: Some("branch".to_string()),
            status: DfsConflictStatus::Open,
            detected_at: OffsetDateTime::UNIX_EPOCH,
        };
        let mut metadata =
            open_daemon_dfs_metadata_backend(&data_dir, &cluster_paths).expect("metadata");
        metadata.put_file_system(&file_system).expect("filesystem");
        metadata.put_object(&object).expect("base object");
        metadata.put_conflict(&conflict).expect("open conflict");
        drop(metadata);

        let pending = daemon_dfs_pending_maintenance_work(&paths).expect("pending maintenance");
        assert_eq!(pending.actionable_conflict_count, 0);
        assert!(
            !pending.should_trigger_background_maintenance(),
            "an intentional open conflict at its base version must stay parked"
        );

        object.version = local_version;
        let mut metadata =
            open_daemon_dfs_metadata_backend(&data_dir, &cluster_paths).expect("metadata");
        metadata.put_object(&object).expect("resolved object");
        drop(metadata);
        let pending = daemon_dfs_pending_maintenance_work(&paths).expect("resolved pending work");
        assert_eq!(pending.actionable_conflict_count, 1);
        assert!(pending.should_trigger_background_maintenance());
    }

    fn placement_reconciliation_job_for_test(
        state: DfsPlacementReconciliationState,
    ) -> DfsPlacementReconciliationJob {
        DfsPlacementReconciliationJob {
            schema_version: sinomo_dfs::DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION,
            job_id: "placement-probe-job".to_string(),
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            path: "/services".to_string(),
            scope: sinomo_dfs::DfsPlacementScope::Subtree,
            unavailable_node_ids: BTreeSet::new(),
            candidate_snapshot: Vec::new(),
            accepted_config: None,
            state,
            cursor: None,
            total_object_count: 0,
            scanned_object_count: 0,
            matched_object_count: 0,
            updated_object_count: 0,
            scheduled_repair_count: 0,
            completed_repair_count: 0,
            pending_repair_count: 0,
            revision: 0,
            created_at_unix_nanos: 1,
            updated_at_unix_nanos: 1,
            last_error: None,
        }
    }

    #[test]
    fn daemon_dfs_placement_probe_triggers_new_work_but_parks_offline_repairs() {
        let pending =
            placement_reconciliation_job_for_test(DfsPlacementReconciliationState::Pending);
        assert_eq!(
            daemon_dfs_actionable_placement_reconciliation_count(&[pending], 0),
            1
        );
        let scanning =
            placement_reconciliation_job_for_test(DfsPlacementReconciliationState::Scanning);
        assert_eq!(
            daemon_dfs_actionable_placement_reconciliation_count(&[scanning], 0),
            1
        );
        let mut repairing =
            placement_reconciliation_job_for_test(DfsPlacementReconciliationState::Repairing);
        assert_eq!(
            daemon_dfs_actionable_placement_reconciliation_count(
                std::slice::from_ref(&repairing),
                3,
            ),
            1,
            "unrelated repair backlog must not stall a fresh Repairing scan"
        );
        repairing.pending_repair_count = 1;
        assert_eq!(
            daemon_dfs_actionable_placement_reconciliation_count(
                std::slice::from_ref(&repairing),
                3,
            ),
            0,
            "a completed Repairing scan with offline work must stay parked"
        );
        assert_eq!(
            daemon_dfs_actionable_placement_reconciliation_count(&[repairing], 0),
            1,
            "a repair-free job needs one final pass to CAS Completed"
        );
    }

    #[test]
    fn daemon_dfs_reactive_probe_never_opens_the_metadata_or_content_inventory() {
        let root = temp_root("reactive-probe-skips-inventory");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let node_id = NodeId::from_legacy_named("node_reactive_probe").expect("node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_reactive_probe").expect("cluster"),
            "reactive probe",
            node_id.clone(),
            "local",
            "pk-local",
            OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("sync raft");

        let corrupt_metadata = data_dir
            .dfs_dir()
            .join("metadata")
            .join("root")
            .join("metadata-tables.snapshot.sobj");
        fs::create_dir_all(corrupt_metadata.parent().expect("metadata parent"))
            .expect("create corrupt metadata parent");
        fs::write(&corrupt_metadata, b"must not be opened by reactive probe")
            .expect("write corrupt metadata");
        let corrupt_manifest = data_dir
            .dfs_dir()
            .join("content")
            .join("objects")
            .join("ignored")
            .join("ignored")
            .join("manifest.sobj");
        fs::create_dir_all(corrupt_manifest.parent().expect("manifest parent"))
            .expect("create corrupt manifest parent");
        fs::write(&corrupt_manifest, b"must not be opened by reactive probe")
            .expect("write corrupt manifest");

        let pending = daemon_dfs_pending_reactive_maintenance_work(&paths).expect("reactive probe");
        assert_eq!(pending, DaemonDfsPendingMaintenanceWork::default());
        assert!(
            !daemon_dfs_repair_queue_work_is_known(&paths),
            "an empty startup queue must not make connectivity refresh launch DFS maintenance"
        );

        let retry_target =
            NodeId::from_legacy_named("node_reactive_retry_target").expect("retry target");
        let new_target = NodeId::from_legacy_named("node_reactive_new_target").expect("new target");
        let now = daemon_dfs_now_unix_nanos();
        let preserved_retry = DfsRepairAttemptRecord {
            intent_id: "intent-reactive-retry".to_string(),
            object_id: DfsObjectId::from_legacy_named("object_reactive_retry")
                .expect("retry object"),
            version: DfsVersion::from_legacy_named("version_reactive_retry")
                .expect("retry version"),
            content_hash: "blake3:reactive-retry".to_string(),
            missing_replica_node_id: retry_target,
            attempts: 3,
            in_flight: false,
            last_attempt_unix_nanos: now.saturating_sub(1),
            next_retry_unix_nanos: now.saturating_add(1_000_000_000),
            transfer_request_id: None,
            last_error: Some("retry later".to_string()),
        };
        write_dfs_repair_attempt_snapshot(
            &default_dfs_repair_attempt_snapshot_path(&data_dir),
            &DfsRepairAttemptSnapshot {
                records: vec![preserved_retry.clone()],
            },
        )
        .expect("initial repair queue");
        let new_repair = DfsRepairAttemptRecord {
            intent_id: "intent-reactive-new".to_string(),
            object_id: DfsObjectId::from_legacy_named("object_reactive_new").expect("new object"),
            version: DfsVersion::from_legacy_named("version_reactive_new").expect("new version"),
            content_hash: "blake3:reactive-new".to_string(),
            missing_replica_node_id: new_target,
            attempts: 0,
            in_flight: false,
            last_attempt_unix_nanos: 0,
            next_retry_unix_nanos: 0,
            transfer_request_id: None,
            last_error: None,
        };
        let mut stale_retry = preserved_retry.clone();
        stale_retry.attempts = 0;
        stale_retry.next_retry_unix_nanos = 0;
        persist_daemon_dfs_repair_queue_records(&data_dir, vec![stale_retry, new_repair])
            .expect("merge committed repair queue");

        let snapshot =
            read_dfs_repair_attempt_snapshot(&default_dfs_repair_attempt_snapshot_path(&data_dir))
                .expect("read merged repair queue")
                .expect("merged repair queue");
        assert_eq!(snapshot.records.len(), 2);
        assert!(
            snapshot
                .records
                .iter()
                .any(|record| record == &preserved_retry),
            "committed queue persistence must not regress existing retry/backoff state"
        );
        let pending =
            daemon_dfs_pending_reactive_maintenance_work(&paths).expect("queued reactive probe");
        assert_eq!(pending.pending_repair_count, 2);
        assert_eq!(pending.due_repair_attempt_count, 1);
        assert!(pending.should_trigger_background_maintenance());
        assert!(
            daemon_dfs_repair_queue_work_is_known(&paths),
            "the bounded queue sample must retain blocker-clear wake eligibility"
        );
    }

    #[test]
    fn daemon_dfs_pending_probe_reads_placement_jobs_from_cluster_kv() {
        let root = temp_root("pending-placement-classifier");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let node_id = NodeId::from_legacy_named("node_pending_placement").expect("node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_pending_placement").expect("cluster"),
            "pending placement classifier",
            node_id.clone(),
            "local",
            "pk-local",
            OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("sync raft");
        let job = placement_reconciliation_job_for_test(DfsPlacementReconciliationState::Pending);
        append_cluster_raft_command(
            &cluster_paths,
            &node_id,
            sinomo_core::kv::KvCommand::Op(
                KvOp::put_serde(
                    sinomo_dfs::placement_reconciliation_key(&job.file_system_id, &job.job_id),
                    &job,
                    None,
                )
                .expect("placement job op"),
            ),
        )
        .expect("placement job");

        let pending = daemon_dfs_pending_maintenance_work(&paths).expect("pending maintenance");
        assert_eq!(pending.actionable_placement_reconciliation_count, 1);
        assert!(pending.should_trigger_background_maintenance());
    }

    #[test]
    fn daemon_dfs_pending_repair_backlog_in_backoff_does_not_force_triggered_loop() {
        let pending = DaemonDfsPendingMaintenanceWork {
            pending_write_count: 0,
            pending_repair_count: 8,
            repair_attempt_count: 8,
            due_repair_attempt_count: 0,
            unattempted_repair_count: 0,
            next_repair_retry_unix_nanos: Some(42),
            pending_namespace_job_count: 0,
            actionable_conflict_count: 0,
            actionable_placement_reconciliation_count: 0,
            metadata_replica_lag_count: 0,
            pending_local_membership_handoff_count: 0,
            ..DaemonDfsPendingMaintenanceWork::default()
        };

        assert!(pending.has_work());
        assert!(!pending.should_trigger_background_maintenance());
    }

    fn pending_work_connectivity_record(
        cluster_id: &ClusterId,
        source_node_id: &NodeId,
        target_node_id: &NodeId,
        endpoint_id: &str,
        state: ClusterConnectivityState,
    ) -> ClusterConnectivityRecord {
        let now = OffsetDateTime::now_utc();
        ClusterConnectivityRecord {
            cluster_id: cluster_id.clone(),
            source_node_id: source_node_id.clone(),
            target_node_id: target_node_id.clone(),
            endpoint_id: ClusterEndpointId::from_legacy_named(endpoint_id).expect("endpoint id"),
            transport: ClusterTransportKind::LanTcp,
            address: format!("{}.example.test:21353", target_node_id.as_str()),
            latency_ms: (state == ClusterConnectivityState::Healthy).then_some(1),
            consecutive_failures: if state == ClusterConnectivityState::Unreachable {
                1
            } else {
                0
            },
            last_success_at: None,
            last_failure_at: None,
            failure_reason: None,
            state,
            observed_at: now,
            expires_at: now + time::Duration::minutes(5),
        }
    }

    #[test]
    fn daemon_dfs_offline_node_with_only_degraded_route_is_parked() {
        let cluster_id =
            ClusterId::from_legacy_named("cluster_pending_degraded").expect("cluster id");
        let local_node_id =
            NodeId::from_legacy_named("node_pending_degraded_local").expect("local node");
        let target_node_id =
            NodeId::from_legacy_named("node_pending_degraded_target").expect("target node");
        let connectivity = ClusterConnectivityGraph::from_records(
            local_node_id.clone(),
            [pending_work_connectivity_record(
                &cluster_id,
                &local_node_id,
                &target_node_id,
                "endpoint_pending_degraded_direct",
                ClusterConnectivityState::Degraded,
            )],
        );

        assert!(
            cluster_route_next_hop(&connectivity, &local_node_id, &target_node_id).is_some(),
            "the fixture must retain the stale degraded route"
        );
        assert!(daemon_dfs_node_is_offline_for_maintenance(
            &ClusterNodeAvailability::Offline,
            &connectivity,
            &local_node_id,
            &target_node_id,
        ));
        assert!(!daemon_dfs_node_is_offline_for_maintenance(
            &ClusterNodeAvailability::Online,
            &connectivity,
            &local_node_id,
            &target_node_id,
        ));
    }

    #[test]
    fn daemon_dfs_relay_routable_node_is_not_treated_as_offline() {
        let cluster_id =
            ClusterId::from_legacy_named("cluster_pending_routed").expect("cluster id");
        let local_node_id =
            NodeId::from_legacy_named("node_pending_routed_local").expect("local node");
        let bridge_node_id =
            NodeId::from_legacy_named("node_pending_routed_bridge").expect("bridge node");
        let target_node_id =
            NodeId::from_legacy_named("node_pending_routed_target").expect("target node");
        let connectivity = ClusterConnectivityGraph::from_records(
            local_node_id.clone(),
            [
                pending_work_connectivity_record(
                    &cluster_id,
                    &local_node_id,
                    &target_node_id,
                    "endpoint_pending_routed_direct",
                    ClusterConnectivityState::Unreachable,
                ),
                pending_work_connectivity_record(
                    &cluster_id,
                    &local_node_id,
                    &bridge_node_id,
                    "endpoint_pending_routed_bridge",
                    ClusterConnectivityState::Healthy,
                ),
                pending_work_connectivity_record(
                    &cluster_id,
                    &bridge_node_id,
                    &target_node_id,
                    "endpoint_pending_routed_target",
                    ClusterConnectivityState::Healthy,
                ),
            ],
        );

        assert!(!daemon_dfs_node_is_offline_for_maintenance(
            &ClusterNodeAvailability::Offline,
            &connectivity,
            &local_node_id,
            &target_node_id,
        ));

        let unroutable = ClusterConnectivityGraph::from_records(
            local_node_id.clone(),
            [pending_work_connectivity_record(
                &cluster_id,
                &local_node_id,
                &target_node_id,
                "endpoint_pending_unroutable_direct",
                ClusterConnectivityState::Unreachable,
            )],
        );
        assert!(daemon_dfs_node_is_offline_for_maintenance(
            &ClusterNodeAvailability::Offline,
            &unroutable,
            &local_node_id,
            &target_node_id,
        ));
        assert!(!daemon_dfs_node_is_offline_for_maintenance(
            &ClusterNodeAvailability::Offline,
            &unroutable,
            &local_node_id,
            &local_node_id,
        ));
        assert!(daemon_dfs_node_is_offline_for_maintenance(
            &ClusterNodeAvailability::Unknown,
            &unroutable,
            &local_node_id,
            &target_node_id,
        ));
    }

    fn pending_write_intent_for_test(
        intent_id: &str,
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        target_version: DfsVersion,
        coordinator_node_id: NodeId,
        desired_replica_node_ids: Vec<NodeId>,
        min_commit_replicas: usize,
    ) -> DfsWriteIntentRecord {
        DfsWriteIntentRecord {
            intent_id: intent_id.to_string(),
            file_system_id,
            object_id,
            base_version: None,
            target_version,
            coordinator_node_id,
            owner_epoch: 1,
            policy: DfsReplicationPolicy::new(min_commit_replicas, desired_replica_node_ids.len())
                .expect("replication policy"),
            desired_replica_node_ids,
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: Some("blake3:pending-write".to_string()),
            object_metadata: None,
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        }
    }

    fn pending_write_receipt_for_test(
        intent: &DfsWriteIntentRecord,
        replica_node_id: NodeId,
        event_id: &str,
    ) -> DfsReplicaReceiptRecord {
        DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: intent.file_system_id.clone(),
                object_id: intent.object_id.clone(),
                committed_version: intent.target_version.clone(),
                owner_node_id: intent.coordinator_node_id.clone(),
                event_id: EventId::from_legacy_named(event_id).expect("event id"),
            },
            replica_node_id,
            content_hash: intent.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    fn pending_object_metadata_for_test(
        object_id: DfsObjectId,
        file_system_id: DfsFileSystemId,
        version: DfsVersion,
        owner_node_id: NodeId,
        path: &str,
    ) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id,
            file_system_id,
            path: path.to_string(),
            kind: DfsObjectKind::File,
            owner_node_id: owner_node_id.clone(),
            current_locations: vec![owner_node_id],
            content_hash: Some("blake3:pending-base".to_string()),
            content_size_bytes: Some(1),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version,
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
            tombstone_provenance: None,
        }
    }

    #[test]
    fn daemon_dfs_explicitly_offline_replica_backlog_stays_durable_without_triggering() {
        let local_node_id =
            NodeId::from_legacy_named("node_pending_offline_local").expect("local node");
        let offline_node_id =
            NodeId::from_legacy_named("node_pending_offline_target").expect("offline node");
        let file_system_id = sinomo_dfs::sifs_file_system_id();
        let object_id =
            DfsObjectId::from_legacy_named("object_pending_offline").expect("object id");
        let version = DfsVersion::from_legacy_named("version_pending_offline").expect("version");
        let intent = pending_write_intent_for_test(
            "intent-active-pending-offline",
            file_system_id.clone(),
            object_id.clone(),
            version.clone(),
            local_node_id.clone(),
            vec![local_node_id.clone(), offline_node_id.clone()],
            2,
        );
        let receipt = pending_write_receipt_for_test(
            &intent,
            local_node_id.clone(),
            "event_pending_offline_local_receipt",
        );
        let repair = DfsOperatorRepairView {
            intent_id: "intent-pending-offline".to_string(),
            file_system_id: file_system_id.clone(),
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: Some("blake3:pending-offline".to_string()),
            missing_replica_node_id: offline_node_id.clone(),
        };
        let attempt = DfsRepairAttemptRecord {
            intent_id: repair.intent_id.clone(),
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: "blake3:pending-offline".to_string(),
            missing_replica_node_id: offline_node_id.clone(),
            attempts: 3,
            in_flight: false,
            last_attempt_unix_nanos: 1,
            next_retry_unix_nanos: 2,
            transfer_request_id: None,
            last_error: Some("offline".to_string()),
        };
        let shard_status = DfsMetadataShardStatus {
            shard_id: DfsMetadataShardId::new("root"),
            file_system_id: Some(file_system_id),
            primary_node_id: Some(local_node_id.clone()),
            replica_node_ids: vec![offline_node_id.clone()],
            epoch: Some(1),
            journal_next_sequence: 10,
            journal_last_sequence: 9,
            replica_lag: vec![DfsMetadataReplicaLagStatus {
                target_node_id: offline_node_id.clone(),
                replicated_through_sequence: 1,
                lag_entries: 8,
            }],
            replay_cursors: Vec::new(),
        };

        let pending = daemon_dfs_pending_work_from_records(
            1,
            1,
            0,
            0,
            0,
            &[intent],
            &[receipt],
            &[shard_status],
            &[repair],
            &[attempt],
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::from([offline_node_id]),
            &local_node_id,
            0,
            3,
        );

        assert_eq!(pending.pending_write_count, 0);
        assert_eq!(pending.pending_repair_count, 1);
        assert_eq!(pending.repair_attempt_count, 1);
        assert_eq!(pending.due_repair_attempt_count, 0);
        assert_eq!(pending.unattempted_repair_count, 0);
        assert_eq!(pending.next_repair_retry_unix_nanos, None);
        assert_eq!(pending.metadata_replica_lag_count, 0);
        assert!(
            pending.has_work(),
            "the durable repair backlog remains visible"
        );
        assert!(!pending.should_trigger_background_maintenance());
    }

    #[test]
    fn daemon_dfs_non_offline_replica_backlog_still_triggers() {
        let local_node_id =
            NodeId::from_legacy_named("node_pending_online_local").expect("local node");
        let target_node_id =
            NodeId::from_legacy_named("node_pending_online_target").expect("target node");
        let file_system_id = sinomo_dfs::sifs_file_system_id();
        let object_id = DfsObjectId::from_legacy_named("object_pending_online").expect("object id");
        let version = DfsVersion::from_legacy_named("version_pending_online").expect("version");
        let intent = pending_write_intent_for_test(
            "intent-active-pending-online",
            file_system_id.clone(),
            object_id.clone(),
            version.clone(),
            local_node_id.clone(),
            vec![local_node_id.clone(), target_node_id.clone()],
            2,
        );
        let receipt = pending_write_receipt_for_test(
            &intent,
            local_node_id.clone(),
            "event_pending_online_local_receipt",
        );
        let repair = DfsOperatorRepairView {
            intent_id: "intent-pending-online".to_string(),
            file_system_id: file_system_id.clone(),
            object_id,
            version,
            content_hash: Some("blake3:pending-online".to_string()),
            missing_replica_node_id: target_node_id.clone(),
        };
        let shard_status = DfsMetadataShardStatus {
            shard_id: DfsMetadataShardId::new("root"),
            file_system_id: Some(file_system_id),
            primary_node_id: Some(local_node_id.clone()),
            replica_node_ids: vec![target_node_id.clone()],
            epoch: Some(1),
            journal_next_sequence: 10,
            journal_last_sequence: 9,
            replica_lag: vec![DfsMetadataReplicaLagStatus {
                target_node_id,
                replicated_through_sequence: 1,
                lag_entries: 8,
            }],
            replay_cursors: Vec::new(),
        };

        let pending = daemon_dfs_pending_work_from_records(
            1,
            1,
            0,
            0,
            0,
            &[intent],
            &[receipt],
            &[shard_status],
            &[repair],
            &[],
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &local_node_id,
            0,
            3,
        );

        assert_eq!(pending.pending_write_count, 1);
        assert_eq!(pending.unattempted_repair_count, 1);
        assert_eq!(pending.metadata_replica_lag_count, 1);
        assert!(pending.should_trigger_background_maintenance());
    }

    #[test]
    fn daemon_dfs_inflight_repair_projects_retry_deadline() {
        let local_node_id =
            NodeId::from_legacy_named("node_inflight_repair_local").expect("local node");
        let target_node_id =
            NodeId::from_legacy_named("node_inflight_repair_target").expect("target node");
        let repair = DfsOperatorRepairView {
            intent_id: "intent-inflight-repair".to_string(),
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            object_id: DfsObjectId::from_legacy_named("object_inflight_repair").expect("object id"),
            version: DfsVersion::from_legacy_named("version_inflight_repair").expect("version"),
            content_hash: Some("blake3:inflight-repair".to_string()),
            missing_replica_node_id: target_node_id.clone(),
        };
        let attempt = DfsRepairAttemptRecord {
            intent_id: repair.intent_id.clone(),
            object_id: repair.object_id.clone(),
            version: repair.version.clone(),
            content_hash: repair.content_hash.clone().expect("content hash"),
            missing_replica_node_id: target_node_id,
            attempts: 1,
            in_flight: true,
            last_attempt_unix_nanos: 1,
            next_retry_unix_nanos: 100,
            transfer_request_id: Some("repair-inflight-request".to_string()),
            last_error: None,
        };
        let project = |now_unix_nanos| {
            daemon_dfs_pending_work_from_records(
                0,
                1,
                0,
                0,
                0,
                &[],
                &[],
                &[],
                std::slice::from_ref(&repair),
                std::slice::from_ref(&attempt),
                &BTreeSet::new(),
                &BTreeSet::new(),
                &BTreeSet::new(),
                &local_node_id,
                0,
                now_unix_nanos,
            )
        };

        let waiting = project(99);
        assert_eq!(waiting.due_repair_attempt_count, 0);
        assert_eq!(waiting.unattempted_repair_count, 0);
        assert_eq!(waiting.next_repair_retry_unix_nanos, Some(100));
        assert!(!waiting.should_trigger_background_maintenance());
        assert!(
            !waiting.has_deferred_work(),
            "an in-flight transfer timeout is scheduled work, not parked work"
        );

        let due = project(100);
        assert_eq!(due.due_repair_attempt_count, 1);
        assert_eq!(due.next_repair_retry_unix_nanos, None);
        assert!(due.should_trigger_background_maintenance());
    }

    #[test]
    fn daemon_dfs_missing_orphan_does_not_hide_valid_repair_trigger() {
        let root = temp_root("pending-missing-orphan-valid-repair");
        let data_dir = SinomoDataDir::new(root.path());
        let metadata = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            root.path().join("metadata"),
        )
        .expect("metadata backend");
        let local_node_id =
            NodeId::from_legacy_named("node_pending_orphan_local").expect("local node");
        let target_node_id =
            NodeId::from_legacy_named("node_pending_orphan_healthy").expect("target node");
        let file_system_id = sinomo_dfs::sifs_file_system_id();
        let orphan_object_id =
            DfsObjectId::from_legacy_named("object_pending_orphan").expect("object id");
        let orphan_version =
            DfsVersion::from_legacy_named("version_pending_orphan").expect("version");
        let mut orphan = pending_write_intent_for_test(
            "intent-pending-orphan",
            file_system_id.clone(),
            orphan_object_id,
            orphan_version,
            local_node_id.clone(),
            vec![local_node_id.clone(), target_node_id.clone()],
            2,
        );
        orphan.content_hash = Some(format!("blake3:{}", "0".repeat(64)));
        let repair = DfsOperatorRepairView {
            intent_id: "intent-pending-valid-repair".to_string(),
            file_system_id,
            object_id: DfsObjectId::from_legacy_named("object_pending_valid_repair")
                .expect("repair object"),
            version: DfsVersion::from_legacy_named("version_pending_valid_repair")
                .expect("repair version"),
            content_hash: Some("blake3:pending-valid-repair".to_string()),
            missing_replica_node_id: target_node_id,
        };
        let intents_without_recovery_source = daemon_dfs_intents_without_recovery_source(
            &data_dir,
            &metadata,
            std::slice::from_ref(&orphan),
            &[],
            &BTreeSet::new(),
            &local_node_id,
        );
        assert_eq!(
            intents_without_recovery_source,
            BTreeSet::from([orphan.intent_id.clone()])
        );

        let pending = daemon_dfs_pending_work_from_records(
            1,
            1,
            0,
            0,
            0,
            &[orphan],
            &[],
            &[],
            &[repair],
            &[],
            &intents_without_recovery_source,
            &BTreeSet::new(),
            &BTreeSet::new(),
            &local_node_id,
            0,
            3,
        );

        assert_eq!(pending.pending_write_count, 0);
        assert_eq!(pending.unattempted_repair_count, 1);
        assert!(
            pending.should_trigger_background_maintenance(),
            "the executable healthy repair must retain the short cadence"
        );

        let orphan_only = daemon_dfs_pending_work_from_records(
            1,
            0,
            0,
            0,
            0,
            &[pending_write_intent_for_test(
                "intent-pending-orphan",
                sinomo_dfs::sifs_file_system_id(),
                DfsObjectId::from_legacy_named("object_pending_orphan_only").expect("object"),
                DfsVersion::from_legacy_named("version_pending_orphan_only").expect("version"),
                local_node_id.clone(),
                vec![local_node_id.clone()],
                1,
            )],
            &[],
            &[],
            &[],
            &[],
            &intents_without_recovery_source,
            &BTreeSet::new(),
            &BTreeSet::new(),
            &local_node_id,
            0,
            3,
        );
        assert!(!orphan_only.should_trigger_background_maintenance());
    }

    #[test]
    fn source_less_write_intent_projects_exact_stale_deadline() {
        let local_node_id =
            NodeId::from_legacy_named("node_pending_stale_local").expect("local node");
        let intent = pending_write_intent_for_test(
            "intent-pending-stale",
            sinomo_dfs::sifs_file_system_id(),
            DfsObjectId::from_legacy_named("object_pending_stale").expect("object"),
            DfsVersion::from_legacy_named("version_pending_stale").expect("version"),
            local_node_id.clone(),
            vec![local_node_id.clone()],
            1,
        );
        let source_less = BTreeSet::from([intent.intent_id.clone()]);
        let stale_at =
            daemon_dfs_duration_nanos(sinomo_dfs::DEFAULT_DFS_STALE_WRITE_INTENT_MIN_AGE);
        let project = |now_unix_nanos| {
            daemon_dfs_pending_work_from_records(
                1,
                0,
                0,
                0,
                0,
                std::slice::from_ref(&intent),
                &[],
                &[],
                &[],
                &[],
                &source_less,
                &BTreeSet::new(),
                &BTreeSet::new(),
                &local_node_id,
                0,
                now_unix_nanos,
            )
        };

        let future = project(stale_at.saturating_sub(1));
        assert_eq!(future.pending_write_count, 0);
        assert_eq!(
            future.next_time_based_maintenance_unix_nanos,
            Some(stale_at)
        );
        assert!(!future.should_trigger_background_maintenance());
        assert!(
            !future.has_deferred_work(),
            "an exact stale deadline is scheduled work, not parked work"
        );

        let due = project(stale_at);
        assert_eq!(due.due_time_based_maintenance_count, 1);
        assert!(due.should_trigger_background_maintenance());
    }

    #[test]
    fn daemon_dfs_offline_only_receipt_below_quorum_is_not_executable_work() {
        let root = temp_root("pending-offline-only-source");
        let data_dir = SinomoDataDir::new(root.path());
        let mut metadata = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            root.path().join("metadata"),
        )
        .expect("metadata backend");
        let local_node_id =
            NodeId::from_legacy_named("node_pending_source_local").expect("local node");
        let offline_node_id =
            NodeId::from_legacy_named("node_pending_source_offline").expect("offline node");
        let healthy_target_node_id =
            NodeId::from_legacy_named("node_pending_source_healthy").expect("healthy node");
        let mut intent = pending_write_intent_for_test(
            "intent-pending-offline-source",
            sinomo_dfs::sifs_file_system_id(),
            DfsObjectId::from_legacy_named("object_pending_offline_source").expect("object"),
            DfsVersion::from_legacy_named("version_pending_offline_source").expect("version"),
            local_node_id.clone(),
            vec![
                local_node_id.clone(),
                offline_node_id.clone(),
                healthy_target_node_id.clone(),
            ],
            2,
        );
        intent.content_hash = Some(format!("blake3:{}", "1".repeat(64)));
        let offline_receipt = pending_write_receipt_for_test(
            &intent,
            offline_node_id.clone(),
            "event_pending_offline_source",
        );
        let offline_node_ids = BTreeSet::from([offline_node_id.clone()]);

        let source_less = daemon_dfs_intents_without_recovery_source(
            &data_dir,
            &metadata,
            std::slice::from_ref(&intent),
            std::slice::from_ref(&offline_receipt),
            &offline_node_ids,
            &local_node_id,
        );
        assert_eq!(source_less, BTreeSet::from([intent.intent_id.clone()]));

        let reachable_source = daemon_dfs_intents_without_recovery_source(
            &data_dir,
            &metadata,
            std::slice::from_ref(&intent),
            std::slice::from_ref(&offline_receipt),
            &BTreeSet::new(),
            &local_node_id,
        );
        assert_eq!(
            reachable_source,
            BTreeSet::from([intent.intent_id.clone()]),
            "a reachable receipt cannot hydrate an invisible create before a pending write exists"
        );

        let second_offline_receipt = pending_write_receipt_for_test(
            &intent,
            healthy_target_node_id.clone(),
            "event_pending_second_offline_source",
        );
        let quorum_offline_sources = daemon_dfs_intents_without_recovery_source(
            &data_dir,
            &metadata,
            std::slice::from_ref(&intent),
            &[offline_receipt.clone(), second_offline_receipt],
            &BTreeSet::from([offline_node_id.clone(), healthy_target_node_id.clone()]),
            &local_node_id,
        );
        assert_eq!(
            quorum_offline_sources,
            BTreeSet::from([intent.intent_id.clone()]),
            "quorum proof does not replace an accessible content or size source"
        );

        let repair = DfsOperatorRepairView {
            intent_id: intent.intent_id.clone(),
            file_system_id: intent.file_system_id.clone(),
            object_id: intent.object_id.clone(),
            version: intent.target_version.clone(),
            content_hash: intent.content_hash.clone(),
            missing_replica_node_id: healthy_target_node_id,
        };
        assert!(
            daemon_dfs_repairs_without_recovery_source(
                &data_dir,
                &metadata,
                std::slice::from_ref(&repair),
                std::slice::from_ref(&offline_receipt),
                &offline_node_ids,
                &local_node_id,
            )
            .is_empty(),
            "a concurrently removed repair object remains actionable for stale-record cleanup"
        );
        metadata
            .put_file_system(&distributed_sifs(local_node_id.clone()))
            .expect("filesystem");
        metadata
            .put_object(&DfsObjectMetadata {
                object_id: repair.object_id.clone(),
                file_system_id: repair.file_system_id.clone(),
                path: "/pending/offline-source".to_string(),
                kind: DfsObjectKind::File,
                owner_node_id: local_node_id.clone(),
                current_locations: vec![offline_node_id.clone()],
                content_hash: repair.content_hash.clone(),
                content_size_bytes: Some(1),
                link_target_object_id: None,
                mode: 0o644,
                uid: 0,
                gid: 0,
                accessed_at_unix_nanos: 0,
                modified_at_unix_nanos: 0,
                created_at_unix_nanos: 0,
                changed_at_unix_nanos: 0,
                version: repair.version.clone(),
                policy: DfsObjectPolicy::ReplicatedEventual,
                created_by_event_id: None,
                last_modified_by_event_id: None,
                tombstone_provenance: None,
            })
            .expect("repair object");
        let repairs_without_recovery_source = daemon_dfs_repairs_without_recovery_source(
            &data_dir,
            &metadata,
            std::slice::from_ref(&repair),
            std::slice::from_ref(&offline_receipt),
            &offline_node_ids,
            &local_node_id,
        );
        assert_eq!(
            repairs_without_recovery_source,
            BTreeSet::from([(
                repair.intent_id.clone(),
                repair.missing_replica_node_id.clone(),
            )])
        );
        assert!(
            daemon_dfs_repairs_without_recovery_source(
                &data_dir,
                &metadata,
                std::slice::from_ref(&repair),
                std::slice::from_ref(&offline_receipt),
                &BTreeSet::new(),
                &local_node_id,
            )
            .is_empty(),
            "a reachable exact-version receipt is an executable repair source"
        );

        let pending = daemon_dfs_pending_work_from_records(
            1,
            1,
            0,
            0,
            0,
            std::slice::from_ref(&intent),
            std::slice::from_ref(&offline_receipt),
            &[],
            &[repair],
            &[],
            &source_less,
            &repairs_without_recovery_source,
            &offline_node_ids,
            &local_node_id,
            0,
            3,
        );
        assert_eq!(pending.pending_write_count, 0);
        assert_eq!(pending.unattempted_repair_count, 0);
        assert!(!pending.should_trigger_background_maintenance());
    }

    #[test]
    fn daemon_dfs_stale_local_quorum_receipt_without_content_is_parked() {
        let root = temp_root("pending-stale-local-quorum");
        let data_dir = SinomoDataDir::new(root.path());
        let mut metadata = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            root.path().join("metadata"),
        )
        .expect("metadata backend");
        let local_node_id =
            NodeId::from_legacy_named("node_pending_stale_local").expect("local node");
        let mut intent = pending_write_intent_for_test(
            "intent-pending-stale-local-quorum",
            sinomo_dfs::sifs_file_system_id(),
            DfsObjectId::from_legacy_named("object_pending_stale_local").expect("object"),
            DfsVersion::from_legacy_named("version_pending_stale_local").expect("version"),
            local_node_id.clone(),
            vec![local_node_id.clone()],
            1,
        );
        let base_version = DfsVersion::from_legacy_named("version_pending_stale_local_base")
            .expect("base version");
        intent.base_version = Some(base_version.clone());
        intent.object_metadata = Some(pending_object_metadata_for_test(
            intent.object_id.clone(),
            intent.file_system_id.clone(),
            base_version,
            local_node_id.clone(),
            "/services/pending-stale-local.txt",
        ));
        intent.content_hash = Some(format!("blake3:{}", "2".repeat(64)));
        let local_receipt = pending_write_receipt_for_test(
            &intent,
            local_node_id.clone(),
            "event_pending_stale_local",
        );

        let source_less = daemon_dfs_intents_without_recovery_source(
            &data_dir,
            &metadata,
            std::slice::from_ref(&intent),
            std::slice::from_ref(&local_receipt),
            &BTreeSet::new(),
            &local_node_id,
        );
        assert_eq!(source_less, BTreeSet::from([intent.intent_id.clone()]));

        let pending = daemon_dfs_pending_work_from_records(
            1,
            0,
            0,
            0,
            0,
            std::slice::from_ref(&intent),
            std::slice::from_ref(&local_receipt),
            &[],
            &[],
            &[],
            &source_less,
            &BTreeSet::new(),
            &BTreeSet::new(),
            &local_node_id,
            0,
            3,
        );
        assert_eq!(pending.pending_write_count, 0);
        assert!(!pending.should_trigger_background_maintenance());

        metadata
            .put_version(&DfsObjectVersionRecord {
                file_system_id: intent.file_system_id.clone(),
                object_id: intent.object_id.clone(),
                version: intent.target_version.clone(),
                content_hash: intent.content_hash.clone(),
                size_bytes: Some(1),
                locations: vec![local_node_id.clone()],
                created_by_event_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                visible: false,
            })
            .expect("durable target size hint");
        let create_metadata = intent.object_metadata.take().expect("create metadata");
        let no_create_metadata = daemon_dfs_intents_without_recovery_source(
            &data_dir,
            &metadata,
            std::slice::from_ref(&intent),
            std::slice::from_ref(&local_receipt),
            &BTreeSet::new(),
            &local_node_id,
        );
        assert_eq!(
            no_create_metadata,
            BTreeSet::from([intent.intent_id.clone()]),
            "receipt quorum and a size hint cannot reconstruct missing create metadata"
        );
        intent.object_metadata = Some(create_metadata);
        assert!(
            daemon_dfs_intents_without_recovery_source(
                &data_dir,
                &metadata,
                std::slice::from_ref(&intent),
                std::slice::from_ref(&local_receipt),
                &BTreeSet::new(),
                &local_node_id,
            )
            .is_empty(),
            "valid create metadata plus quorum and a durable size hint is promotable"
        );

        intent.content_hash = None;
        let no_hash_source_less = daemon_dfs_intents_without_recovery_source(
            &data_dir,
            &metadata,
            std::slice::from_ref(&intent),
            &[],
            &BTreeSet::new(),
            &local_node_id,
        );
        assert_eq!(
            no_hash_source_less,
            BTreeSet::from([intent.intent_id]),
            "a non-visible no-hash intent has no hydration recovery path"
        );
    }

    #[test]
    fn daemon_dfs_extent_backed_pending_write_is_an_executable_local_source() {
        let root = temp_root("pending-extent-source");
        let data_dir = SinomoDataDir::new(root.path());
        let mut metadata = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            root.path().join("metadata"),
        )
        .expect("metadata backend");
        let content =
            LocalContentStore::new(data_dir.dfs_dir().join("content")).expect("content store");
        let local_node_id =
            NodeId::from_legacy_named("node_pending_extent_local").expect("local node");
        let target_node_id =
            NodeId::from_legacy_named("node_pending_extent_target").expect("target node");
        let object_id = DfsObjectId::from_legacy_named("object_pending_extent").expect("object id");
        let base_version =
            DfsVersion::from_legacy_named("version_pending_extent_base").expect("base version");
        let target_version =
            DfsVersion::from_legacy_named("version_pending_extent_target").expect("target version");
        metadata
            .put_file_system(&distributed_sifs(local_node_id.clone()))
            .expect("filesystem");
        metadata
            .put_object(&pending_object_metadata_for_test(
                object_id.clone(),
                sinomo_dfs::sifs_file_system_id(),
                base_version.clone(),
                local_node_id.clone(),
                "/pending/extent-update.txt",
            ))
            .expect("base object");
        content
            .put_object_version(object_id.as_str(), base_version.as_str(), b"abcdefgh")
            .expect("base object version");
        let patched = content
            .put_object_version_patch(
                object_id.as_str(),
                base_version.as_str(),
                target_version.as_str(),
                2,
                b"ZZ",
            )
            .expect("extent-backed target version");
        assert!(!patched.extents.is_empty());
        let mut intent = pending_write_intent_for_test(
            "intent-pending-extent-source",
            sinomo_dfs::sifs_file_system_id(),
            object_id,
            target_version,
            local_node_id.clone(),
            vec![local_node_id.clone(), target_node_id.clone()],
            2,
        );
        intent.content_hash = Some(patched.content_hash);

        assert!(
            daemon_dfs_intents_without_recovery_source(
                &data_dir,
                &metadata,
                std::slice::from_ref(&intent),
                &[],
                &BTreeSet::new(),
                &local_node_id,
            )
            .is_empty()
        );
        assert!(
            daemon_dfs_repairs_without_recovery_source(
                &data_dir,
                &metadata,
                &[DfsOperatorRepairView {
                    intent_id: intent.intent_id,
                    file_system_id: intent.file_system_id,
                    object_id: intent.object_id,
                    version: intent.target_version,
                    content_hash: intent.content_hash,
                    missing_replica_node_id: target_node_id,
                }],
                &[],
                &BTreeSet::new(),
                &local_node_id,
            )
            .is_empty(),
            "the extent-backed local version is also an executable repair source"
        );
    }

    #[test]
    fn daemon_dfs_quorum_ready_write_still_triggers_when_remaining_replica_is_offline() {
        let root = temp_root("pending-quorum-ready-update");
        let data_dir = SinomoDataDir::new(root.path());
        let mut metadata = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            root.path().join("metadata"),
        )
        .expect("metadata backend");
        let local_node_id =
            NodeId::from_legacy_named("node_pending_quorum_local").expect("local node");
        let offline_node_id =
            NodeId::from_legacy_named("node_pending_quorum_offline").expect("offline node");
        let object_id =
            DfsObjectId::from_legacy_named("object_pending_quorum_ready").expect("object id");
        let base_version =
            DfsVersion::from_legacy_named("version_pending_quorum_base").expect("base version");
        metadata
            .put_file_system(&distributed_sifs(local_node_id.clone()))
            .expect("filesystem");
        metadata
            .put_object(&pending_object_metadata_for_test(
                object_id.clone(),
                sinomo_dfs::sifs_file_system_id(),
                base_version.clone(),
                local_node_id.clone(),
                "/pending/quorum-update.txt",
            ))
            .expect("base object");
        let mut intent = pending_write_intent_for_test(
            "intent-pending-quorum-ready",
            sinomo_dfs::sifs_file_system_id(),
            object_id,
            DfsVersion::from_legacy_named("version_pending_quorum_ready").expect("version"),
            local_node_id.clone(),
            vec![local_node_id.clone(), offline_node_id.clone()],
            1,
        );
        intent.base_version = Some(base_version);
        let receipt = pending_write_receipt_for_test(
            &intent,
            local_node_id.clone(),
            "event_pending_quorum_ready_receipt",
        );
        let source_less = daemon_dfs_intents_without_recovery_source(
            &data_dir,
            &metadata,
            std::slice::from_ref(&intent),
            std::slice::from_ref(&receipt),
            &BTreeSet::from([offline_node_id.clone()]),
            &local_node_id,
        );
        assert!(
            source_less.is_empty(),
            "a visible base lets receipt quorum promote an update without a target size hint"
        );

        let pending = daemon_dfs_pending_work_from_records(
            1,
            0,
            0,
            0,
            0,
            std::slice::from_ref(&intent),
            std::slice::from_ref(&receipt),
            &[],
            &[],
            &[],
            &source_less,
            &BTreeSet::new(),
            &BTreeSet::from([offline_node_id]),
            &local_node_id,
            0,
            3,
        );

        assert_eq!(pending.pending_write_count, 1);
        assert!(pending.should_trigger_background_maintenance());
    }

    #[test]
    fn daemon_dfs_wrong_hash_receipt_does_not_hide_actionable_pending_write() {
        let local_node_id =
            NodeId::from_legacy_named("node_pending_hash_local").expect("local node");
        let offline_node_id =
            NodeId::from_legacy_named("node_pending_hash_offline").expect("offline node");
        let intent = pending_write_intent_for_test(
            "intent-pending-wrong-hash",
            sinomo_dfs::sifs_file_system_id(),
            DfsObjectId::from_legacy_named("object_pending_wrong_hash").expect("object id"),
            DfsVersion::from_legacy_named("version_pending_wrong_hash").expect("version"),
            local_node_id.clone(),
            vec![local_node_id.clone(), offline_node_id.clone()],
            2,
        );
        let mut wrong_hash_receipt = pending_write_receipt_for_test(
            &intent,
            local_node_id.clone(),
            "event_pending_wrong_hash_receipt",
        );
        wrong_hash_receipt.content_hash = Some("blake3:different-content".to_string());

        let pending = daemon_dfs_pending_work_from_records(
            1,
            0,
            0,
            0,
            0,
            &[intent],
            &[wrong_hash_receipt],
            &[],
            &[],
            &[],
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::from([offline_node_id]),
            &local_node_id,
            0,
            3,
        );

        assert_eq!(pending.pending_write_count, 1);
        assert!(pending.should_trigger_background_maintenance());
    }

    #[test]
    fn daemon_dfs_due_repair_backlog_forces_triggered_loop() {
        let pending = DaemonDfsPendingMaintenanceWork {
            pending_write_count: 0,
            pending_repair_count: 8,
            repair_attempt_count: 8,
            due_repair_attempt_count: 1,
            unattempted_repair_count: 0,
            next_repair_retry_unix_nanos: None,
            pending_namespace_job_count: 0,
            actionable_conflict_count: 0,
            actionable_placement_reconciliation_count: 0,
            metadata_replica_lag_count: 0,
            pending_local_membership_handoff_count: 0,
            ..DaemonDfsPendingMaintenanceWork::default()
        };

        assert!(pending.has_work());
        assert!(pending.should_trigger_background_maintenance());
    }

    #[test]
    fn daemon_dfs_unattempted_repair_backlog_forces_triggered_loop() {
        let pending = DaemonDfsPendingMaintenanceWork {
            pending_write_count: 0,
            pending_repair_count: 8,
            repair_attempt_count: 4,
            due_repair_attempt_count: 0,
            unattempted_repair_count: 4,
            next_repair_retry_unix_nanos: Some(42),
            pending_namespace_job_count: 0,
            actionable_conflict_count: 0,
            actionable_placement_reconciliation_count: 0,
            metadata_replica_lag_count: 0,
            pending_local_membership_handoff_count: 0,
            ..DaemonDfsPendingMaintenanceWork::default()
        };

        assert!(pending.has_work());
        assert!(pending.should_trigger_background_maintenance());
    }

    #[test]
    fn daemon_dfs_pending_writes_force_triggered_loop() {
        let pending = DaemonDfsPendingMaintenanceWork {
            pending_write_count: 8,
            pending_repair_count: 0,
            repair_attempt_count: 0,
            due_repair_attempt_count: 0,
            unattempted_repair_count: 0,
            next_repair_retry_unix_nanos: None,
            pending_namespace_job_count: 0,
            actionable_conflict_count: 0,
            actionable_placement_reconciliation_count: 0,
            metadata_replica_lag_count: 0,
            pending_local_membership_handoff_count: 0,
            ..DaemonDfsPendingMaintenanceWork::default()
        };

        assert!(pending.has_work());
        assert!(pending.should_trigger_background_maintenance());
    }

    #[test]
    fn daemon_dfs_metadata_replica_lag_stays_owned_by_exact_lane() {
        let pending = DaemonDfsPendingMaintenanceWork {
            pending_write_count: 0,
            pending_repair_count: 0,
            repair_attempt_count: 0,
            due_repair_attempt_count: 0,
            unattempted_repair_count: 0,
            next_repair_retry_unix_nanos: None,
            pending_namespace_job_count: 0,
            actionable_conflict_count: 0,
            actionable_placement_reconciliation_count: 0,
            metadata_replica_lag_count: 1,
            pending_local_membership_handoff_count: 0,
            ..DaemonDfsPendingMaintenanceWork::default()
        };

        assert!(pending.has_work());
        assert!(!pending.should_trigger_background_maintenance());
    }

    #[test]
    fn daemon_dfs_local_membership_handoff_forces_triggered_loop() {
        let pending = DaemonDfsPendingMaintenanceWork {
            pending_local_membership_handoff_count: 1,
            ..DaemonDfsPendingMaintenanceWork::default()
        };

        assert!(pending.has_work());
        assert!(pending.should_trigger_background_maintenance());
    }

    #[test]
    fn daemon_dfs_handoff_probe_only_tracks_locally_primary_shards() {
        let local_node_id = NodeId::from_legacy_named("node_handoff_probe_local").expect("node");
        let remote_node_id = NodeId::from_legacy_named("node_handoff_probe_remote").expect("node");
        let file_system_id = sinomo_dfs::sifs_file_system_id();
        let status =
            |shard_id: &str, primary_node_id: NodeId, configured: bool| DfsMetadataShardStatus {
                shard_id: DfsMetadataShardId::new(shard_id),
                file_system_id: configured.then(|| file_system_id.clone()),
                primary_node_id: Some(primary_node_id),
                replica_node_ids: Vec::new(),
                epoch: Some(1),
                journal_next_sequence: 1,
                journal_last_sequence: 0,
                replica_lag: Vec::new(),
                replay_cursors: Vec::new(),
            };
        let statuses = vec![
            status("root", local_node_id.clone(), true),
            status("services", remote_node_id, true),
            status("unconfigured", local_node_id.clone(), false),
        ];

        assert_eq!(
            daemon_dfs_local_membership_handoff_keys(&statuses, &local_node_id),
            vec![sinomo_dfs::metadata_shard_membership_handoff_key(
                &file_system_id,
                "root"
            )]
        );
    }

    #[test]
    fn daemon_dfs_control_plane_txn_notifies_only_local_handoff_puts() {
        let local_node_id = NodeId::from_legacy_named("node_handoff_notify_local").expect("node");
        let remote_node_id = NodeId::from_legacy_named("node_handoff_notify_remote").expect("node");
        let file_system_id = sinomo_dfs::sifs_file_system_id();
        let retiring = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "services".to_string(),
            primary_node_id: local_node_id.clone(),
            replica_node_ids: vec![remote_node_id.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: "/services".to_string(),
            },
            namespace_prepare_codec: Default::default(),
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let handoff = DfsMetadataShardMembershipHandoff {
            handoff_id: "handoff_notify_local".to_string(),
            retiring_shard: retiring.clone(),
            desired_shard: DfsMetadataShardConfig {
                epoch: 2,
                replica_node_ids: vec![remote_node_id.clone()],
                ..retiring
            },
            required_prepared_sequence: 0,
            required_head_watermark: None,
            retiring_primary_fence: None,
        };
        let handoff_key =
            sinomo_dfs::metadata_shard_membership_handoff_key(&file_system_id, "services");
        assert!(daemon_dfs_membership_handoff_key_is_exact(&handoff_key));
        let txn = KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::put_serde(handoff_key, &handoff, None).expect("handoff put")],
            failure: Vec::new(),
        };

        assert!(daemon_dfs_txn_puts_membership_handoff_for_node(
            &txn,
            &local_node_id
        ));
        assert!(!daemon_dfs_txn_puts_membership_handoff_for_node(
            &txn,
            &remote_node_id
        ));

        let proof_key =
            sinomo_dfs::metadata_shard_membership_handoff_proof_key(&file_system_id, "services");
        assert!(!daemon_dfs_membership_handoff_key_is_exact(&proof_key));
        let proof_key_txn = KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::put_serde(proof_key, &handoff, None).expect("proof-key put")],
            failure: Vec::new(),
        };
        assert!(!daemon_dfs_txn_puts_membership_handoff_for_node(
            &proof_key_txn,
            &local_node_id
        ));
    }

    #[test]
    fn daemon_dfs_maintenance_tick_runs_anti_entropy() {
        let root = temp_root("daemon_dfs_maintenance_anti_entropy");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_anti_entropy_tick")
                .expect("cluster");
        let node_id = NodeId::from_legacy_named("node_daemon_dfs_anti_entropy_tick").expect("node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id,
            "daemon dfs anti entropy",
            node_id.clone(),
            "local",
            "pk-local",
            now,
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("sync raft");

        let file_system = sinomo_dfs::default_cluster_sifs(&state);
        let object_id =
            DfsObjectId::from_legacy_named("obj_daemon_dfs_anti_entropy_tick").expect("object");
        let version =
            DfsVersion::from_legacy_named("version_daemon_dfs_anti_entropy_tick").expect("version");
        let missing_hash = format!(
            "blake3:{}",
            blake3::hash(b"missing daemon anti entropy bytes").to_hex()
        );
        let object = DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id: file_system.file_system_id.clone(),
            path: "/services/daemon-anti-entropy/missing.bin".to_string(),
            kind: DfsObjectKind::File,
            owner_node_id: node_id.clone(),
            current_locations: vec![node_id.clone()],
            content_hash: Some(missing_hash),
            content_size_bytes: Some(33),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version.clone(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
            tombstone_provenance: None,
        };
        {
            let content =
                LocalContentStore::new(data_dir.dfs_dir().join("content")).expect("content store");
            let metadata = open_daemon_dfs_metadata_backend(&data_dir, &cluster_paths)
                .expect("metadata backend");
            let mut service = NetworkedDfsService::new(
                daemon_dfs_node_transport(node_id.clone(), Vec::new()),
                content,
            )
            .with_boxed_metadata_backend(metadata)
            .expect("service backend");
            service
                .bootstrap_distributed_sifs()
                .expect("bootstrap sifs");
            service
                .add_object_metadata(object.clone())
                .expect("missing object metadata");
        }

        let runtime = daemon_test_runtime("daemon-dfs-maintenance-anti-entropy");
        run_daemon_dfs_maintenance_body_for_test(&paths, &runtime).expect("maintenance tick");

        let backend =
            open_daemon_dfs_metadata_backend(&data_dir, &cluster_paths).expect("metadata backend");
        let repaired_object = backend
            .get_object(&object_id)
            .expect("get repaired object")
            .expect("repaired object");
        assert!(
            !repaired_object.current_locations.contains(&node_id),
            "anti-entropy must remove stale local object locations"
        );
        let repaired_version = backend
            .get_version(&object_id, &version)
            .expect("get repaired version")
            .expect("repaired version");
        assert!(
            !repaired_version.locations.contains(&node_id),
            "anti-entropy must remove stale local version locations"
        );
        assert!(
            !sinomo_dfs::default_dfs_anti_entropy_cursor_path(
                &data_dir,
                &file_system.file_system_id
            )
            .exists(),
            "completed scans should clear their cursor"
        );
    }

    #[test]
    fn daemon_dfs_status_reads_configured_sharded_metadata_backend() {
        let root = temp_root("daemon_dfs_status_sharded");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_status_sharded")
                .expect("cluster");
        let node_id = NodeId::from_legacy_named("node_daemon_dfs_status_sharded").expect("node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id,
            "daemon dfs status sharded",
            node_id.clone(),
            "local",
            "pk-local",
            now,
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("sync raft");
        let remote_node_id =
            NodeId::from_legacy_named("node_daemon_dfs_status_sharded_remote").expect("remote");
        let file_system = distributed_sifs(node_id.clone());
        let root_shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: node_id.clone(),
            replica_node_ids: vec![remote_node_id.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let services_shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "services".to_string(),
            primary_node_id: node_id.clone(),
            replica_node_ids: vec![remote_node_id.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: "/services".to_string(),
            },
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &node_id,
            sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
                compares: Vec::new(),
                success: vec![
                    sinomo_core::kv::KvOp::put_serde(
                        file_system_config_key(&file_system.file_system_id),
                        &DfsFileSystemConfig::from(&file_system),
                        None,
                    )
                    .expect("filesystem config op"),
                    sinomo_core::kv::KvOp::put_serde(
                        metadata_shard_key(&file_system.file_system_id, "root"),
                        &root_shard,
                        None,
                    )
                    .expect("root shard op"),
                    sinomo_core::kv::KvOp::put_serde(
                        metadata_shard_key(&file_system.file_system_id, "services"),
                        &services_shard,
                        None,
                    )
                    .expect("services shard op"),
                ],
                failure: Vec::new(),
            }),
        )
        .expect("write shard configs");
        let mut object = distributed_sifs_bootstrap_objects(node_id)
            .into_iter()
            .next()
            .expect("bootstrap object");
        object.path = "/services/status-sharded.txt".to_string();
        let mut backend =
            sinomo_dfs::open_cluster_sharded_dfs_metadata_backend(&data_dir, &cluster_paths)
                .expect("open sharded backend")
                .expect("configured backend");
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");
        backend
            .put_version(&DfsObjectVersionRecord {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                size_bytes: object.content_size_bytes,
                locations: object.current_locations.clone(),
                created_by_event_id: object.created_by_event_id.clone(),
                created_at: OffsetDateTime::now_utc(),
                visible: true,
            })
            .expect("version");
        backend
            .put_metadata_replication_cursor(
                &remote_node_id,
                &DfsMetadataShardId::new("services"),
                1,
            )
            .expect("metadata cursor");

        let status = sinomo_dfs::inspect_cluster_aware_dfs_status(&data_dir, &cluster_paths)
            .expect("dfs status");

        assert_eq!(status.file_system_count, 1);
        assert_eq!(status.object_count, 1);
        assert_eq!(status.version_count, 1);
        assert_eq!(status.metadata_shards.len(), 2);
        let services_status = status
            .metadata_shards
            .iter()
            .find(|shard| shard.shard_id == DfsMetadataShardId::new("services"))
            .expect("services shard status");
        assert_eq!(services_status.journal_last_sequence, 3);
        assert!(services_status.replay_cursors.is_empty());
        assert_eq!(services_status.replica_lag.len(), 1);
        assert_eq!(
            services_status.replica_lag[0],
            sinomo_dfs::DfsMetadataReplicaLagStatus {
                target_node_id: remote_node_id.clone(),
                replicated_through_sequence: 1,
                lag_entries: 2,
            }
        );
        let pending = daemon_dfs_pending_maintenance_work(&paths).expect("pending maintenance");
        assert_eq!(pending.pending_write_count, 0);
        assert_eq!(pending.pending_repair_count, 0);
        assert_eq!(pending.pending_namespace_job_count, 0);
        assert_eq!(pending.metadata_replica_lag_count, 2);
        assert_eq!(pending.pending_local_membership_handoff_count, 0);
        assert_eq!(pending.due_time_based_maintenance_count, 0);
        assert!(
            !pending.should_trigger_background_maintenance(),
            "metadata lag alone is observable but owned by retained exact-shard publishers: {pending:#?}"
        );
        let DaemonDfsInspectionResponse::FileSystem {
            file_system: Some(file_system_view),
        } = sinomo_dfs::inspect_cluster_aware_dfs(
            &data_dir,
            &cluster_paths,
            DaemonDfsInspectionRequest::FileSystem {
                file_system_id: file_system.file_system_id.clone(),
            },
        )
        .expect("filesystem inspection")
        else {
            panic!("expected filesystem inspection response")
        };
        assert_eq!(file_system_view.object_count, 1);
        assert_eq!(file_system_view.open_conflict_count, 0);
        assert_eq!(file_system_view.pending_repair_count, 0);
    }

    #[test]
    fn daemon_dfs_status_reports_replica_lag_only_from_primary() {
        let root = temp_root("daemon_dfs_status_replica_lag_primary_only");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_status_replica_lag")
                .expect("cluster");
        let local = NodeId::from_legacy_named("node_dfs_status_replica_local").expect("local");
        let primary =
            NodeId::from_legacy_named("node_dfs_status_replica_primary").expect("primary");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id,
            "daemon dfs status replica lag",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("write state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("sync raft");
        let file_system = distributed_sifs(local.clone());
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: primary,
            replica_node_ids: vec![local.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &shard,
                    None,
                )
                .expect("root shard op"),
            ),
        )
        .expect("write shard config");
        let mut backend =
            sinomo_dfs::open_cluster_sharded_dfs_metadata_backend(&data_dir, &cluster_paths)
                .expect("open sharded backend")
                .expect("configured backend");
        backend.put_file_system(&file_system).expect("filesystem");

        let status = sinomo_dfs::inspect_cluster_aware_dfs_status(&data_dir, &cluster_paths)
            .expect("dfs status");

        assert_eq!(status.metadata_shards.len(), 1);
        assert!(status.metadata_shards[0].replica_lag.is_empty());
    }

    #[test]
    fn daemon_dfs_metadata_journal_publish_migrates_legacy_cursor_to_backend() {
        let root = temp_root("daemon_dfs_metadata_journal_publish_cursor_migration");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_metadata_publish_cursor")
                .expect("cluster");
        let local =
            NodeId::from_legacy_named("node_dfs_metadata_publish_local").expect("local node");
        let remote =
            NodeId::from_legacy_named("node_dfs_metadata_publish_remote").expect("remote node");
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "daemon dfs metadata publish",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut remote_member = sinomo_core::ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            remote.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
        remote_member.endpoints = vec![test_endpoint(&remote, "192.0.2.21:21353")];
        state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");

        let file_system = distributed_sifs(local.clone());
        let shard_id = DfsMetadataShardId::new("root");
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: shard_id.0.clone(),
            primary_node_id: local.clone(),
            replica_node_ids: vec![remote.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &shard,
                    None,
                )
                .expect("put shard op"),
            ),
        )
        .expect("write shard config");

        let object = distributed_sifs_bootstrap_objects(local.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        let mut backend = LocalSobjMetadataBackend::new(
            shard_id.clone(),
            data_dir.dfs_dir().join("metadata").join("root"),
        )
        .expect("metadata backend");
        backend.put_file_system(&file_system).expect("filesystem");
        backend.put_object(&object).expect("object");
        backend
            .put_version(&DfsObjectVersionRecord {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                size_bytes: object.content_size_bytes,
                locations: object.current_locations.clone(),
                created_by_event_id: object.created_by_event_id.clone(),
                created_at: OffsetDateTime::now_utc(),
                visible: true,
            })
            .expect("version");
        let legacy_path =
            sinomo_dfs::legacy_dfs_metadata_replication_cursor_path(&data_dir, &remote, &shard_id);
        if let Some(parent) = legacy_path.parent() {
            fs::create_dir_all(parent).expect("legacy cursor parent");
        }
        let legacy_cursor = sinomo_dfs::LegacyDfsMetadataReplicationCursor {
            target_node_id: remote.clone(),
            shard_id: shard_id.clone(),
            next_after_sequence: 1,
        };
        fs::write(
            &legacy_path,
            serde_json::to_vec_pretty(&legacy_cursor).expect("legacy cursor json"),
        )
        .expect("legacy cursor");

        let sent = Arc::new(Mutex::new(Vec::new()));
        let sent_for_sender = Arc::clone(&sent);
        let sender: DfsMetadataJournalBatchSender =
            Arc::new(move |_endpoint, command, _timeout| {
                sent_for_sender
                    .lock()
                    .expect("sent lock")
                    .push(command.clone());
                Ok(DfsMetadataJournalBatchResponse {
                    request_id: command.request_id.clone(),
                    shard_id: command.batch.shard_id.clone(),
                    next_after_sequence: command.batch.next_after_sequence,
                    applied_entries: command.batch.entries.len(),
                    verified_prefix_only: command.verify_prefix_only,
                    verified_authority_probe_id: None,
                    result: DfsMetadataJournalBatchResult::Applied,
                })
            });

        let report = publish_default_dfs_metadata_journal_with_sender(&paths, sender)
            .expect("publish metadata journal");

        assert_eq!(
            report.outcomes,
            vec![DfsMetadataJournalReplicationOutcome::Applied {
                target_node_id: remote.clone(),
                shard_id: shard_id.clone(),
                entries: 2,
                next_after_sequence: 3,
            }]
        );
        let sent = sent.lock().expect("sent lock");
        assert_eq!(sent.len(), 1);
        assert_eq!(
            sent[0]
                .batch
                .entries
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        drop(sent);
        let backend = LocalSobjMetadataBackend::new(
            shard_id.clone(),
            data_dir.dfs_dir().join("metadata").join("root"),
        )
        .expect("metadata backend reload");
        assert_eq!(
            backend
                .metadata_replication_cursor(&remote, &shard_id)
                .expect("backend cursor"),
            3
        );
        assert!(!legacy_path.exists());
    }

    #[test]
    fn daemon_dfs_metadata_journal_publish_replicates_all_local_primary_shards() {
        let root = temp_root("daemon_dfs_metadata_journal_publish_all_shards");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_metadata_publish_all")
                .expect("cluster");
        let local =
            NodeId::from_legacy_named("node_dfs_metadata_publish_all_local").expect("local node");
        let remote =
            NodeId::from_legacy_named("node_dfs_metadata_publish_all_remote").expect("remote node");
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "daemon dfs metadata publish all",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut remote_member = sinomo_core::ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            remote.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
        remote_member.endpoints = vec![test_endpoint(&remote, "192.0.2.22:21353")];
        state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");

        let file_system = distributed_sifs(local.clone());
        for shard_name in ["root", "services"] {
            let shard = DfsMetadataShardConfig {
                file_system_id: file_system.file_system_id.clone(),
                shard_id: shard_name.to_string(),
                primary_node_id: local.clone(),
                replica_node_ids: vec![remote.clone()],
                epoch: 1,
                assignment: if shard_name == "root" {
                    DfsMetadataShardAssignment::Root
                } else {
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: "/services".to_string(),
                    }
                },
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            };
            append_cluster_raft_command(
                &cluster_paths,
                &local,
                sinomo_core::kv::KvCommand::Op(
                    sinomo_core::kv::KvOp::put_serde(
                        metadata_shard_key(&file_system.file_system_id, shard_name),
                        &shard,
                        None,
                    )
                    .expect("put shard op"),
                ),
            )
            .expect("write shard config");

            let mut object = distributed_sifs_bootstrap_objects(local.clone())
                .into_iter()
                .next()
                .expect("bootstrap object");
            object.object_id =
                DfsObjectId::from_legacy_named(format!("obj_metadata_shard_{shard_name}"))
                    .expect("object");
            object.path = format!("/{shard_name}/metadata.txt");
            let mut backend = sinomo_dfs::open_local_metadata_shard_backend(
                &data_dir,
                DfsMetadataShardId::new(shard_name),
            )
            .expect("metadata backend");
            backend.put_file_system(&file_system).expect("filesystem");
            backend.put_object(&object).expect("object");
        }

        let sent = Arc::new(Mutex::new(Vec::new()));
        let sent_for_sender = Arc::clone(&sent);
        let sender: DfsMetadataJournalBatchSender =
            Arc::new(move |_endpoint, command, _timeout| {
                sent_for_sender
                    .lock()
                    .expect("sent lock")
                    .push(command.clone());
                Ok(DfsMetadataJournalBatchResponse {
                    request_id: command.request_id.clone(),
                    shard_id: command.batch.shard_id.clone(),
                    next_after_sequence: command.batch.next_after_sequence,
                    applied_entries: command.batch.entries.len(),
                    verified_prefix_only: command.verify_prefix_only,
                    verified_authority_probe_id: None,
                    result: DfsMetadataJournalBatchResult::Applied,
                })
            });

        let report = publish_default_dfs_metadata_journal_with_sender(&paths, sender)
            .expect("publish metadata journal");

        assert_eq!(
            report.outcomes,
            vec![
                DfsMetadataJournalReplicationOutcome::Applied {
                    target_node_id: remote.clone(),
                    shard_id: DfsMetadataShardId::new("root"),
                    entries: 2,
                    next_after_sequence: 2,
                },
                DfsMetadataJournalReplicationOutcome::Applied {
                    target_node_id: remote.clone(),
                    shard_id: DfsMetadataShardId::new("services"),
                    entries: 2,
                    next_after_sequence: 2,
                },
            ]
        );
        let sent = sent.lock().expect("sent lock");
        assert_eq!(
            sent.iter()
                .map(|command| command.batch.shard_id.clone())
                .collect::<Vec<_>>(),
            vec![
                DfsMetadataShardId::new("root"),
                DfsMetadataShardId::new("services")
            ]
        );
    }

    #[test]
    fn daemon_dfs_metadata_journal_publish_drains_bounded_batches() {
        let root = temp_root("daemon_dfs_metadata_journal_publish_drains_batches");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_metadata_publish_drain")
                .expect("cluster");
        let local =
            NodeId::from_legacy_named("node_dfs_metadata_publish_drain_local").expect("local node");
        let remote = NodeId::from_legacy_named("node_dfs_metadata_publish_drain_remote")
            .expect("remote node");
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "daemon dfs metadata publish drain",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut remote_member = sinomo_core::ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            remote.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
        remote_member.endpoints = vec![test_endpoint(&remote, "192.0.2.23:21353")];
        state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");

        let file_system = distributed_sifs(local.clone());
        let shard_id = DfsMetadataShardId::new("root");
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: shard_id.0.clone(),
            primary_node_id: local.clone(),
            replica_node_ids: vec![remote.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &shard,
                    None,
                )
                .expect("put shard op"),
            ),
        )
        .expect("write shard config");

        let mut backend = LocalSobjMetadataBackend::new(
            shard_id.clone(),
            data_dir.dfs_dir().join("metadata").join("root"),
        )
        .expect("metadata backend");
        backend.put_file_system(&file_system).expect("filesystem");
        let base_object = distributed_sifs_bootstrap_objects(local.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        let object_count = sinomo_dfs::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT + 7;
        for index in 0..object_count {
            let mut object = base_object.clone();
            object.object_id =
                DfsObjectId::from_legacy_named(format!("obj_daemon_metadata_drain_{index}"))
                    .expect("object");
            object.path = format!("/services/daemon-metadata-drain-{index}.txt");
            object.version =
                DfsVersion::from_legacy_named(format!("version_daemon_metadata_drain_{index}"))
                    .expect("version");
            object.content_hash = Some(format!("blake3:daemon-metadata-drain-{index}"));
            object.content_size_bytes = Some(index as u64);
            backend.put_object(&object).expect("object");
        }
        drop(backend);

        let sent = Arc::new(Mutex::new(Vec::new()));
        let sent_for_sender = Arc::clone(&sent);
        let sender: DfsMetadataJournalBatchSender =
            Arc::new(move |_endpoint, command, _timeout| {
                sent_for_sender
                    .lock()
                    .expect("sent lock")
                    .push(command.clone());
                Ok(DfsMetadataJournalBatchResponse {
                    request_id: command.request_id.clone(),
                    shard_id: command.batch.shard_id.clone(),
                    next_after_sequence: command.batch.next_after_sequence,
                    applied_entries: command.batch.entries.len(),
                    verified_prefix_only: command.verify_prefix_only,
                    verified_authority_probe_id: None,
                    result: DfsMetadataJournalBatchResult::Applied,
                })
            });

        let report = publish_default_dfs_metadata_journal_with_sender(&paths, sender)
            .expect("publish metadata journal");

        let expected_entries = 1 + object_count;
        assert_eq!(
            report.outcomes,
            vec![
                DfsMetadataJournalReplicationOutcome::Applied {
                    target_node_id: remote.clone(),
                    shard_id: shard_id.clone(),
                    entries: sinomo_dfs::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                    next_after_sequence: sinomo_dfs::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT
                        as u64,
                },
                DfsMetadataJournalReplicationOutcome::Applied {
                    target_node_id: remote.clone(),
                    shard_id: shard_id.clone(),
                    entries: expected_entries
                        - sinomo_dfs::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                    next_after_sequence: expected_entries as u64,
                },
            ]
        );
        let sent = sent.lock().expect("sent lock");
        assert_eq!(sent.len(), 2);
        assert_eq!(
            sent.iter()
                .map(|command| command.batch.entries.len())
                .collect::<Vec<_>>(),
            vec![
                sinomo_dfs::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                expected_entries - sinomo_dfs::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            ]
        );
        drop(sent);
        let backend = LocalSobjMetadataBackend::new(
            shard_id.clone(),
            data_dir.dfs_dir().join("metadata").join("root"),
        )
        .expect("metadata backend reload");
        assert_eq!(
            backend
                .metadata_replication_cursor(&remote, &shard_id)
                .expect("backend cursor"),
            expected_entries as u64
        );
    }

    #[test]
    fn daemon_dfs_metadata_journal_publish_rejects_mismatched_ack_cursor() {
        let root = temp_root("daemon_dfs_metadata_journal_publish_mismatched_ack");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_metadata_publish_bad_ack")
                .expect("cluster");
        let local =
            NodeId::from_legacy_named("node_dfs_metadata_publish_bad_ack_local").expect("local");
        let remote =
            NodeId::from_legacy_named("node_dfs_metadata_publish_bad_ack_remote").expect("remote");
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "daemon dfs metadata publish bad ack",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut remote_member = sinomo_core::ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            remote.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
        remote_member.endpoints = vec![test_endpoint(&remote, "192.0.2.24:21353")];
        state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");

        let file_system = distributed_sifs(local.clone());
        let shard_id = DfsMetadataShardId::new("root");
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: shard_id.0.clone(),
            primary_node_id: local.clone(),
            replica_node_ids: vec![remote.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &shard,
                    None,
                )
                .expect("put shard op"),
            ),
        )
        .expect("write shard config");

        let mut backend = LocalSobjMetadataBackend::new(
            shard_id.clone(),
            data_dir.dfs_dir().join("metadata").join("root"),
        )
        .expect("metadata backend");
        backend.put_file_system(&file_system).expect("filesystem");
        drop(backend);

        let sender: DfsMetadataJournalBatchSender =
            Arc::new(move |_endpoint, command, _timeout| {
                Ok(DfsMetadataJournalBatchResponse {
                    request_id: command.request_id.clone(),
                    shard_id: command.batch.shard_id.clone(),
                    next_after_sequence: command.batch.next_after_sequence + 1,
                    applied_entries: command.batch.entries.len(),
                    verified_prefix_only: command.verify_prefix_only,
                    verified_authority_probe_id: None,
                    result: DfsMetadataJournalBatchResult::Applied,
                })
            });

        let report = publish_default_dfs_metadata_journal_with_sender(&paths, sender)
            .expect("publish metadata journal");

        assert!(matches!(
            report.outcomes.as_slice(),
            [DfsMetadataJournalReplicationOutcome::Failed { reason, .. }]
                if reason.contains("mismatched metadata journal response")
        ));
        let backend = LocalSobjMetadataBackend::new(
            shard_id.clone(),
            data_dir.dfs_dir().join("metadata").join("root"),
        )
        .expect("metadata backend reload");
        assert_eq!(
            backend
                .metadata_replication_cursor(&remote, &shard_id)
                .expect("backend cursor"),
            0
        );
    }

    #[test]
    fn daemon_dfs_metadata_journal_publish_stops_after_replica_rejection() {
        let root = temp_root("daemon_dfs_metadata_journal_publish_rejected_batch");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id = sinomo_core::node::ClusterId::from_legacy_named(
            "cluster_dfs_metadata_publish_rejected",
        )
        .expect("cluster");
        let local =
            NodeId::from_legacy_named("node_dfs_metadata_publish_rejected_local").expect("local");
        let remote =
            NodeId::from_legacy_named("node_dfs_metadata_publish_rejected_remote").expect("remote");
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "daemon dfs metadata publish rejected",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut remote_member = sinomo_core::ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            remote.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
        remote_member.endpoints = vec![test_endpoint(&remote, "192.0.2.25:21353")];
        state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");

        let file_system = distributed_sifs(local.clone());
        let shard_id = DfsMetadataShardId::new("root");
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: shard_id.0.clone(),
            primary_node_id: local.clone(),
            replica_node_ids: vec![remote.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &shard,
                    None,
                )
                .expect("put shard op"),
            ),
        )
        .expect("write shard config");

        let mut backend = LocalSobjMetadataBackend::new(
            shard_id.clone(),
            data_dir.dfs_dir().join("metadata").join("root"),
        )
        .expect("metadata backend");
        backend.put_file_system(&file_system).expect("filesystem");
        drop(backend);

        let attempts = Arc::new(Mutex::new(0usize));
        let attempts_for_sender = Arc::clone(&attempts);
        let sender: DfsMetadataJournalBatchSender =
            Arc::new(move |_endpoint, command, _timeout| {
                let mut attempts = attempts_for_sender.lock().expect("attempts lock");
                *attempts += 1;
                assert_eq!(
                    *attempts, 1,
                    "replica rejection must be terminal for this publish target"
                );
                Ok(DfsMetadataJournalBatchResponse {
                    request_id: command.request_id.clone(),
                    shard_id: command.batch.shard_id.clone(),
                    next_after_sequence: command.batch.next_after_sequence,
                    applied_entries: 0,
                    verified_prefix_only: false,
                    verified_authority_probe_id: None,
                    result: DfsMetadataJournalBatchResult::Rejected {
                        reason: "replica already has a conflicting journal entry".to_string(),
                    },
                })
            });

        let report = publish_default_dfs_metadata_journal_with_sender(&paths, sender)
            .expect("publish metadata journal");

        assert_eq!(*attempts.lock().expect("attempts lock"), 1);
        assert!(matches!(
            report.outcomes.as_slice(),
            [DfsMetadataJournalReplicationOutcome::Rejected { reason, .. }]
                if reason.contains("conflicting journal entry")
        ));
        let backend = LocalSobjMetadataBackend::new(
            shard_id.clone(),
            data_dir.dfs_dir().join("metadata").join("root"),
        )
        .expect("metadata backend reload");
        assert_eq!(
            backend
                .metadata_replication_cursor(&remote, &shard_id)
                .expect("backend cursor"),
            0
        );
    }

    #[test]
    fn daemon_dfs_metadata_journal_publish_repairs_sequence_conflict_with_snapshot() {
        let root = temp_root("daemon_dfs_metadata_journal_publish_snapshot_repair");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_metadata_snapshot_repair")
                .expect("cluster");
        let local =
            NodeId::from_legacy_named("node_dfs_metadata_snapshot_repair_local").expect("local");
        let remote =
            NodeId::from_legacy_named("node_dfs_metadata_snapshot_repair_remote").expect("remote");
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "daemon dfs metadata snapshot repair",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut remote_member = sinomo_core::ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            remote.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
        remote_member.endpoints = vec![test_endpoint(&remote, "192.0.2.26:21353")];
        state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");

        let file_system = distributed_sifs(local.clone());
        let shard_id = DfsMetadataShardId::new("root");
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: shard_id.0.clone(),
            primary_node_id: local.clone(),
            replica_node_ids: vec![remote.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &shard,
                    None,
                )
                .expect("put shard op"),
            ),
        )
        .expect("write shard config");

        let mut backend = LocalSobjMetadataBackend::new(
            shard_id.clone(),
            data_dir.dfs_dir().join("metadata").join("root"),
        )
        .expect("metadata backend");
        backend.put_file_system(&file_system).expect("filesystem");
        drop(backend);

        let sender: DfsMetadataJournalBatchSender = Arc::new(
            move |_endpoint, command, _timeout| {
                Ok(DfsMetadataJournalBatchResponse {
                    request_id: command.request_id.clone(),
                    shard_id: command.batch.shard_id.clone(),
                    next_after_sequence: command.batch.next_after_sequence,
                    applied_entries: 0,
                    verified_prefix_only: false,
                    verified_authority_probe_id: None,
                    result: DfsMetadataJournalBatchResult::Rejected {
                        reason: format!(
                            "metadata journal sequence {} already exists with a different entry while replaying shard root",
                            command.batch.next_after_sequence
                        ),
                    },
                })
            },
        );
        let snapshot_installs = Arc::new(Mutex::new(Vec::new()));
        let snapshot_installs_for_sender = Arc::clone(&snapshot_installs);
        let snapshot_sender: DfsMetadataSnapshotInstallSender =
            Arc::new(move |_endpoint, command, _timeout| {
                snapshot_installs_for_sender
                    .lock()
                    .expect("snapshot installs lock")
                    .push(command.clone());
                Ok(DfsMetadataSnapshotInstallResponse {
                    request_id: command.request_id.clone(),
                    shard_id: command.shard_id.clone(),
                    next_after_sequence: command.next_after_sequence,
                    result: DfsMetadataSnapshotInstallResult::Installed {
                        archived_existing: true,
                    },
                })
            });

        let report =
            publish_default_dfs_metadata_journal_with_senders(&paths, sender, snapshot_sender)
                .expect("publish metadata journal");

        assert_eq!(
            report.outcomes,
            vec![DfsMetadataJournalReplicationOutcome::SnapshotInstalled {
                target_node_id: remote.clone(),
                shard_id: shard_id.clone(),
                next_after_sequence: 1,
                archived_existing: true,
            }]
        );
        assert_eq!(snapshot_installs.lock().expect("lock").len(), 1);
        let backend = LocalSobjMetadataBackend::new(
            shard_id.clone(),
            data_dir.dfs_dir().join("metadata").join("root"),
        )
        .expect("metadata backend reload");
        assert_eq!(
            backend
                .metadata_replication_cursor(&remote, &shard_id)
                .expect("backend cursor"),
            1
        );
    }

    #[test]
    fn daemon_dfs_metadata_journal_publish_bounds_endpoint_fallbacks_per_target() {
        let root = temp_root("daemon_dfs_metadata_journal_publish_bounds_endpoints");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_metadata_publish_bounds")
                .expect("cluster");
        let local =
            NodeId::from_legacy_named("node_dfs_metadata_publish_bounds_local").expect("local");
        let remote =
            NodeId::from_legacy_named("node_dfs_metadata_publish_bounds_remote").expect("remote");
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "daemon dfs metadata publish bounds",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let mut remote_member = sinomo_core::ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            remote.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            sinomo_core::node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
        let lan_primary = test_endpoint(&remote, "10.0.0.10:21353");
        let lan_secondary = test_endpoint(&remote, "10.0.0.11:21353");
        let mut public = test_endpoint(&remote, "203.0.113.10:21353");
        public.privacy = ClusterEndpointPrivacy::InternetDirect;
        let iroh = ClusterEndpointRecord {
            endpoint_id: sinomo_core::network::ClusterEndpointId::derived(
                "sinomo.test.endpoint.iroh",
                remote.as_str(),
            ),
            node_id: remote.clone(),
            transport: ClusterTransportKind::Iroh,
            address: "iroh://iroh_public_key_dfs_metadata_bounds?relay=https://relay.example./"
                .to_string(),
            source: ClusterEndpointSource::Rendezvous,
            privacy: ClusterEndpointPrivacy::Relayed,
            priority: 0,
            last_observed_latency_ms: Some(40),
        };
        remote_member.endpoints = vec![
            lan_secondary,
            iroh.clone(),
            public.clone(),
            lan_primary.clone(),
        ];
        state.members.push(remote_member);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");

        let file_system = distributed_sifs(local.clone());
        let shard_id = DfsMetadataShardId::new("root");
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: shard_id.0.clone(),
            primary_node_id: local.clone(),
            replica_node_ids: vec![remote.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &shard,
                    None,
                )
                .expect("put shard op"),
            ),
        )
        .expect("write shard config");
        let mut backend = LocalSobjMetadataBackend::new(
            shard_id.clone(),
            data_dir.dfs_dir().join("metadata").join("root"),
        )
        .expect("metadata backend");
        backend.put_file_system(&file_system).expect("filesystem");
        drop(backend);

        let attempted = Arc::new(Mutex::new(Vec::new()));
        let attempted_for_sender = Arc::clone(&attempted);
        let sender: DfsMetadataJournalBatchSender =
            Arc::new(move |endpoint, _command, _timeout| {
                attempted_for_sender
                    .lock()
                    .expect("attempted lock")
                    .push(endpoint.to_string());
                Err(DfsMetadataPeerTransportError::new(format!(
                    "{} simulated failure",
                    endpoint
                )))
            });

        let report = publish_default_dfs_metadata_journal_with_sender(&paths, sender)
            .expect("publish metadata journal");

        assert!(matches!(
            report.outcomes.as_slice(),
            [DfsMetadataJournalReplicationOutcome::Failed { .. }]
        ));
        assert_eq!(
            attempted.lock().expect("attempted lock").as_slice(),
            &[remote.to_string()],
            "metadata replication should make one target-level attempt"
        );
    }

    #[test]
    fn cancelled_daemon_dfs_rpc_retains_committed_work_edges() {
        let root = temp_root("daemon-dfs-cancelled-rpc-retains-publish-edge");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let control_owner = Arc::new(
            DaemonDataOwnerLock::acquire(&paths).expect("control-loop data-owner capability"),
        );
        register_daemon_dfs_data_owner_lock(&paths, &control_owner)
            .expect("register data-owner capability");
        let local =
            NodeId::from_legacy_named("node_dfs_cancelled_publish_local").expect("local node");
        let remote =
            NodeId::from_legacy_named("node_dfs_cancelled_publish_remote").expect("remote node");
        let missing_replica =
            NodeId::from_legacy_named("node_dfs_cancelled_publish_missing").expect("missing node");
        write_test_cluster_state_with_remote_endpoint(&data_root, &local, &remote, "127.0.0.1:9");
        let mut cluster_state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("read test cluster state");
        let local_member = cluster_state
            .members
            .iter_mut()
            .find(|member| member.node_id == local)
            .expect("local test member");
        local_member.capabilities = sinomo_core::node::main_daemon_capabilities(&local_member.role);
        local_member.allowed_capabilities = local_member.capabilities.clone();
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &cluster_state)
            .expect("write voter-capable test cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(
            &cluster_paths,
            &cluster_state,
        )
        .expect("sync test voter membership to KV");

        let file_system = distributed_sifs(local.clone());
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: local.clone(),
            replica_node_ids: vec![remote.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
                compares: Vec::new(),
                success: vec![
                    sinomo_core::kv::KvOp::put_serde(
                        file_system_config_key(&file_system.file_system_id),
                        &DfsFileSystemConfig::from(&file_system),
                        None,
                    )
                    .expect("filesystem config op"),
                    sinomo_core::kv::KvOp::put_serde(
                        metadata_shard_key(&file_system.file_system_id, "root"),
                        &shard,
                        None,
                    )
                    .expect("metadata shard op"),
                ],
                failure: Vec::new(),
            }),
        )
        .expect("write DFS control plane");
        let mut backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            default_dfs_metadata_root(&data_dir),
        )
        .expect("metadata backend");
        backend
            .put_file_system(&file_system)
            .expect("filesystem metadata");
        for object in distributed_sifs_bootstrap_objects(local.clone()) {
            backend.put_object(&object).expect("bootstrap object");
        }
        drop(backend);

        let request_id = "dfs_message_cancelled_publish_edge";
        let frame_id = 90_101;
        let object_id = DfsObjectId::from_legacy_named(format!("obj_daemon_dfs_write_{frame_id}"))
            .expect("object id");
        let mut routed_message = test_dfs_write_routed_message(
            &remote,
            &local,
            frame_id,
            "/services/cancelled-publish-edge.txt",
        );
        let DfsNetworkMessage::DistributedWriteRequest(write) = &mut routed_message.message else {
            panic!("distributed write request");
        };
        write.desired_replica_nodes = vec![local.clone(), missing_replica];
        write.policy = DfsReplicationPolicy::new(1, 2).expect("W=1, N=2 policy");
        write
            .object_metadata
            .as_mut()
            .expect("write object metadata")
            .current_locations = vec![local.clone()];
        let command = test_dfs_message_command(request_id, routed_message);
        let lane = DaemonDfsMutationLaneKey {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new("root"),
        };
        let initial_requests = daemon_dfs_metadata_publish_lane_request_count(&data_dir, &lane);
        let initial_repair_edges = daemon_dfs_repair_maintenance_edge_request_count(&data_dir);
        let (committed_rx, release_tx) =
            install_daemon_dfs_message_committed_edge_test_hook(request_id);
        let runtime = daemon_test_runtime("daemon-dfs-cancelled-rpc-publish-edge");
        let task_paths = paths.clone();
        let task_runtime = runtime.handle().clone();
        let rpc = runtime.handle().spawn(async move {
            handle_daemon_dfs_message_async(&task_paths, command, None, None, &task_runtime).await
        });

        committed_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("durable message mutation reached committed-edge boundary");
        assert!(
            LocalSobjMetadataBackend::new(
                DfsMetadataShardId::new("root"),
                default_dfs_metadata_root(&data_dir),
            )
            .expect("reopen metadata backend")
            .get_object(&object_id)
            .expect("read committed object")
            .is_some(),
            "the mutation must be durable before the cancellable RPC boundary"
        );
        assert_eq!(
            LocalSobjMetadataBackend::new(
                DfsMetadataShardId::new("root"),
                default_dfs_metadata_root(&data_dir),
            )
            .expect("reopen metadata backend for repair summary")
            .metadata_summary()
            .expect("read repair summary")
            .pending_repair_count,
            1,
            "the authority must durably own one repair before edge delivery"
        );
        rpc.abort();
        let cancelled = runtime
            .block_on(rpc)
            .expect_err("RPC task must be cancelled");
        assert!(cancelled.is_cancelled());
        release_tx
            .send(())
            .expect("release non-cancellable committed-edge delivery");

        let deadline = Instant::now() + Duration::from_secs(3);
        while (daemon_dfs_metadata_publish_lane_request_count(&data_dir, &lane) == initial_requests
            || daemon_dfs_repair_maintenance_edge_request_count(&data_dir) == initial_repair_edges)
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            daemon_dfs_metadata_publish_lane_request_count(&data_dir, &lane) > initial_requests,
            "a cancelled RPC must still dirty the exact retained publisher lane"
        );
        assert!(
            daemon_dfs_repair_maintenance_edge_request_count(&data_dir) > initial_repair_edges,
            "a cancelled RPC must still wake the authority's repair classifier"
        );
    }

    #[test]
    fn cancelled_daemon_dfs_rpc_retains_subscribed_replica_progress_edge() {
        let root = temp_root("daemon-dfs-cancelled-rpc-retains-replica-progress");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let control_owner = Arc::new(
            DaemonDataOwnerLock::acquire(&paths).expect("control-loop data-owner capability"),
        );
        register_daemon_dfs_data_owner_lock(&paths, &control_owner)
            .expect("register data-owner capability");
        let primary =
            NodeId::from_legacy_named("node_dfs_cancelled_replica_primary").expect("primary");
        let replica =
            NodeId::from_legacy_named("node_dfs_cancelled_replica_local").expect("replica");
        let file_system = distributed_sifs(primary.clone());
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: primary.clone(),
            replica_node_ids: vec![replica.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        install_test_metadata_replica_shard(
            &cluster_paths,
            "cluster_daemon_dfs_cancelled_replica_progress",
            &replica,
            &shard,
        );
        let object = distributed_sifs_bootstrap_objects(primary.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        let mut source = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            root.join("source-metadata"),
        )
        .expect("source backend");
        source.put_file_system(&file_system).expect("filesystem");
        source.put_object(&object).expect("object");
        let mut subscription = source.subscribe(0);
        let batch = source
            .poll_subscription(&mut subscription)
            .expect("source journal batch");

        sinomo_dfs::test_establish_default_cluster_dfs_replica_progress_subscription(&data_dir);
        assert!(
            sinomo_dfs::take_default_cluster_dfs_materialization_immediate_retry(&data_dir),
            "test subscription establishes and consumes its verification generation"
        );
        let reconciliation_wake = daemon_default_dfs_reconciliation_wake(&cluster_paths)
            .expect("default DFS reconciliation wake");
        let mut reconciliation_rx = reconciliation_wake.generation_tx.subscribe();
        let _ = reconciliation_rx.borrow_and_update();

        let request_id = "dfs_message_cancelled_replica_progress";
        let command = test_dfs_message_command(
            request_id,
            DfsRoutedMessage {
                source_node_id: primary.clone(),
                target_node_id: replica,
                frame_id: 90_102,
                message_id: 90_102,
                message: DfsNetworkMessage::MetadataJournalBatch(DfsMetadataJournalBatchCommand {
                    request_id: "dfs_metadata_cancelled_replica_progress".to_string(),
                    source_node_id: Some(primary),
                    file_system_id: Some(file_system.file_system_id.clone()),
                    authority_epoch: Some(shard.epoch),
                    verify_prefix_only: false,
                    prefix_probe: None,
                    authority_probe: None,
                    batch,
                }),
            },
        );
        let (committed_rx, release_tx) =
            install_daemon_dfs_message_committed_edge_test_hook(request_id);
        let runtime = daemon_test_runtime("daemon-dfs-cancelled-rpc-replica-progress");
        let task_paths = paths.clone();
        let task_runtime = runtime.handle().clone();
        let rpc = runtime.handle().spawn(async move {
            handle_daemon_dfs_message_async(&task_paths, command, None, None, &task_runtime).await
        });

        committed_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("replica mutation reached committed-edge boundary");
        assert_eq!(
            LocalSobjMetadataBackend::new(
                DfsMetadataShardId::new("root"),
                default_dfs_metadata_root(&data_dir),
            )
            .expect("reopen replica backend")
            .get_object(&object.object_id)
            .expect("read replicated object"),
            Some(object),
            "the replica journal batch must be durable before RPC cancellation"
        );
        rpc.abort();
        let cancelled = runtime
            .block_on(rpc)
            .expect_err("RPC task must be cancelled");
        assert!(cancelled.is_cancelled());
        release_tx
            .send(())
            .expect("release non-cancellable replica-progress delivery");

        let deadline = Instant::now() + Duration::from_secs(3);
        while !reconciliation_rx
            .has_changed()
            .expect("reconciliation wake channel remains open")
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            reconciliation_rx
                .has_changed()
                .expect("reconciliation wake channel remains open"),
            "a cancelled RPC must still emit the subscribed replica-progress generation"
        );
    }

    #[test]
    fn daemon_dfs_message_processes_stage_blob_and_returns_response() {
        let root = temp_root("daemon_dfs_message_stage_blob");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let owner = NodeId::from_legacy_named("node_dfs_protocol_owner").expect("owner");
        let replica = NodeId::from_legacy_named("node_dfs_protocol_replica").expect("replica");
        write_test_cluster_state(&data_root, &replica);
        let data_dir = SinomoDataDir::new(&data_root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let file_system_id = sinomo_dfs::sifs_file_system_id();
        let file_system = distributed_sifs(owner.clone());
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![owner.clone(), replica.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &replica,
            sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
                compares: Vec::new(),
                success: vec![
                    sinomo_core::kv::KvOp::put_serde(
                        file_system_config_key(&file_system_id),
                        &DfsFileSystemConfig::from(&file_system),
                        None,
                    )
                    .expect("filesystem config op"),
                    sinomo_core::kv::KvOp::put_serde(
                        metadata_shard_key(&file_system_id, "root"),
                        &shard,
                        None,
                    )
                    .expect("metadata shard op"),
                ],
                failure: Vec::new(),
            }),
        )
        .expect("write dfs control plane");
        let object_id = DfsObjectId::from_legacy_named("obj_dfs_data_plane_stage").expect("object");
        let version =
            DfsVersion::from_legacy_named("version_dfs_data_plane_stage").expect("version");
        let bytes = b"daemon dfs data-plane message bytes".to_vec();
        let content_hash = format!("blake3:{}", blake3::hash(&bytes).to_hex());
        let object = DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id: file_system_id.clone(),
            path: "services/daemon-dfs-data-plane.bin".to_string(),
            kind: DfsObjectKind::File,
            owner_node_id: owner.clone(),
            current_locations: vec![owner.clone()],
            content_hash: Some(content_hash.clone()),
            content_size_bytes: Some(bytes.len() as u64),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version.clone(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
            tombstone_provenance: None,
        };
        let message = DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
            request_id: "dfs_stage_rpc_1".to_string(),
            intent_id: "dfs_intent_protocol_stage".to_string(),
            file_system_id: file_system_id.clone(),
            object_id: object_id.clone(),
            expected_base_version: None,
            version: version.clone(),
            content_hash: content_hash.clone(),
            bytes: bytes.clone(),
            object_metadata: Some(object),
            authority_shard: None,
        });
        let routed_message = DfsRoutedMessage {
            source_node_id: owner.clone(),
            target_node_id: replica.clone(),
            frame_id: 1,
            message_id: 1,
            message,
        };

        let runtime = daemon_test_runtime("daemon-dfs-message-replica-stage");
        let response = handle_daemon_dfs_message_on_runtime(
            &paths,
            test_dfs_message_command("dfs_message_rpc_1", routed_message),
            runtime.handle(),
        )
        .expect("DFS message");

        assert_eq!(response.request_id, "dfs_message_rpc_1");
        assert_eq!(response.outbound_messages.len(), 1);
        let response_message = &response.outbound_messages[0].message;
        assert!(matches!(
            response_message,
            DfsNetworkMessage::StageBlobResponse(response)
                if response.request_id == "dfs_stage_rpc_1"
                    && response.object_id == object_id
                    && response.version == version
                    && response.replica_node_id == replica
                    && response.result == DfsBlobTransferResult::Durable { content_hash }
        ));

        let store = LocalContentStore::new(root.join("data").join("dfs").join("content"))
            .expect("content store");
        let manifest = store
            .read_manifest(object_id.as_str(), version.as_str())
            .expect("read staged manifest");
        let read = store.read_blob(&manifest.content_hash).expect("read blob");
        assert_eq!(read, bytes);
        let backend =
            sinomo_dfs::open_cluster_sharded_dfs_metadata_backend(&data_dir, &cluster_paths)
                .expect("open sharded backend")
                .expect("configured backend");
        assert!(
            backend
                .get_object(&object_id)
                .expect("backend object")
                .is_none(),
            "non-primary content replicas must not append authoritative metadata while handling DFS messages"
        );
    }

    #[test]
    fn daemon_dfs_message_rejects_wrong_target_node() {
        let root = temp_root("daemon_dfs_message_wrong_target");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let local = NodeId::from_legacy_named("node_dfs_protocol_local").expect("local");
        let remote = NodeId::from_legacy_named("node_dfs_protocol_remote").expect("remote");
        write_test_cluster_state(&data_root, &local);
        let routed_message = test_dfs_lookup_routed_message(
            &remote,
            &remote,
            1,
            "/services/daemon-wrong-target.txt",
        );

        let runtime = daemon_test_runtime("daemon-dfs-message-wrong-target");
        let error = handle_daemon_dfs_message_on_runtime(
            &paths,
            test_dfs_message_command("dfs_message_rpc_wrong_target", routed_message),
            runtime.handle(),
        )
        .expect_err("wrong target rejected");
        assert!(error.to_string().contains("does not match local node"));
    }

    #[test]
    fn daemon_dfs_distributed_write_skips_cold_full_metadata_hydrate() {
        let root = temp_root("daemon_dfs_write_skips_cold_hydrate");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let local = NodeId::from_legacy_named("node_dfs_protocol_write_local").expect("local");
        let remote = NodeId::from_legacy_named("node_dfs_protocol_write_remote").expect("remote");
        write_test_cluster_state_with_remote_endpoint(&data_root, &local, &remote, "127.0.0.1:9");
        let runtime = daemon_test_runtime("daemon-dfs-write-skips-cold-hydrate");

        let data_dir = SinomoDataDir::new(&data_root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let mut cluster_state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("read test cluster state");
        let local_member = cluster_state
            .members
            .iter_mut()
            .find(|member| member.node_id == local)
            .expect("local test member");
        local_member.capabilities = sinomo_core::node::main_daemon_capabilities(&local_member.role);
        local_member.allowed_capabilities = local_member.capabilities.clone();
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &cluster_state)
            .expect("write voter-capable test cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(
            &cluster_paths,
            &cluster_state,
        )
        .expect("sync test voter membership to KV");
        let file_system = distributed_sifs(local.clone());
        let root_shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: local.clone(),
            replica_node_ids: vec![remote.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
                compares: Vec::new(),
                success: vec![
                    sinomo_core::kv::KvOp::put_serde(
                        file_system_config_key(&file_system.file_system_id),
                        &DfsFileSystemConfig::from(&file_system),
                        None,
                    )
                    .expect("filesystem config op"),
                    sinomo_core::kv::KvOp::put_serde(
                        metadata_shard_key(&file_system.file_system_id, "root"),
                        &root_shard,
                        None,
                    )
                    .expect("root shard op"),
                ],
                failure: Vec::new(),
            }),
        )
        .expect("write DFS control plane");
        let mut unrelated = distributed_sifs_bootstrap_objects(local.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        unrelated.object_id =
            sinomo_dfs::DfsObjectId::from_legacy_named("obj_daemon_unrelated_cold_metadata")
                .expect("object id");
        unrelated.path = "/users/unrelated-cold-metadata.txt".to_string();
        unrelated.version =
            sinomo_dfs::DfsVersion::from_legacy_named("version_daemon_unrelated_cold_metadata")
                .expect("version");
        let unrelated_object_id = unrelated.object_id.clone();
        let mut backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            default_dfs_metadata_root(&data_dir),
        )
        .expect("metadata backend");
        backend.put_file_system(&file_system).expect("filesystem");
        for object in distributed_sifs_bootstrap_objects(local.clone()) {
            backend.put_object(&object).expect("bootstrap object");
        }
        backend.put_object(&unrelated).expect("unrelated object");
        drop(backend);

        let cache_key = data_dir.root();
        let mut routed_message = test_dfs_write_routed_message(
            &remote,
            &local,
            1,
            "/services/daemon-write-skips-cold-hydrate.txt",
        );
        let sinomo_dfs::DfsNetworkMessage::DistributedWriteRequest(write) =
            &mut routed_message.message
        else {
            panic!("write routed message");
        };
        write.desired_replica_nodes = vec![local.clone()];
        write
            .object_metadata
            .as_mut()
            .expect("write object metadata")
            .current_locations = vec![local.clone()];
        let response = handle_daemon_dfs_message_on_runtime(
            &paths,
            test_dfs_message_command("dfs_message_write_skips_cold_hydrate", routed_message),
            runtime.handle(),
        )
        .expect("distributed write DFS message");
        assert_eq!(response.request_id, "dfs_message_write_skips_cold_hydrate");
        let written_object_id =
            sinomo_dfs::DfsObjectId::from_legacy_named("obj_daemon_dfs_write_1")
                .expect("written object id");
        let persisted = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            default_dfs_metadata_root(&data_dir),
        )
        .expect("reopen metadata backend")
        .get_object(&written_object_id)
        .expect("read written object")
        .unwrap_or_else(|| {
            panic!(
                "cold routed write must persist its object: {:?}",
                response.outbound_messages
            )
        });
        assert_eq!(
            persisted.path,
            "/services/daemon-write-skips-cold-hydrate.txt"
        );
        let primary_service = {
            let cache = daemon_dfs_message_service_cache()
                .lock()
                .expect("cache lock");
            Arc::clone(
                cache
                    .get(&cache_key)
                    .expect("cache entry")
                    .primary_service
                    .as_ref()
                    .expect("mutation service lane"),
            )
        };
        let lane = primary_service.lock().expect("primary service lock");
        assert!(
            !lane.metadata_hydrated,
            "distributed writes must not force a full metadata hydrate on a cold daemon message lane"
        );
        assert!(
            lane.service
                .metadata()
                .object(&unrelated_object_id)
                .is_none(),
            "cold message-lane construction must not eagerly load unrelated metadata"
        );
        drop(lane);
        let cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("cache lock");
        assert!(Arc::ptr_eq(
            &primary_service,
            cache
                .get(&cache_key)
                .expect("cache entry")
                .primary_service
                .as_ref()
                .expect("mutation service lane")
        ));
        drop(cache);
        assert_eq!(
            daemon_dfs_test_message_service_lane_open_count(&data_dir),
            1,
            "a cold exact-shard mutation must reuse the one backend-bearing cache lane"
        );
        handle_daemon_dfs_message_on_runtime(
            &paths,
            test_dfs_message_command(
                "dfs_message_read_survives_maintenance",
                test_dfs_lookup_routed_message(
                    &remote,
                    &local,
                    2,
                    "/services/daemon-write-skips-cold-hydrate.txt",
                ),
            ),
            runtime.handle(),
        )
        .expect("lightweight lookup DFS message");
        let read_service = {
            let cache = daemon_dfs_message_service_cache()
                .lock()
                .expect("cache lock");
            Arc::clone(
                cache
                    .get(&cache_key)
                    .expect("cache entry")
                    .lightweight_read_service
                    .as_ref()
                    .expect("lightweight read lane"),
            )
        };
        let lane_open_count_before_maintenance =
            daemon_dfs_test_message_service_lane_open_count(&data_dir);
        let weak_primary = Arc::downgrade(&primary_service);
        let weak_read = Arc::downgrade(&read_service);
        drop(primary_service);
        run_daemon_dfs_maintenance_body_for_test(&paths, &runtime)
            .expect("successful maintenance tick");
        assert!(
            weak_primary.upgrade().is_none(),
            "maintenance transfers ownership out of the old cache handle"
        );
        assert!(
            weak_read.upgrade().is_some(),
            "maintenance must preserve the independently locked lightweight read lane"
        );
        {
            let cache = daemon_dfs_message_service_cache()
                .lock()
                .expect("cache lock");
            let lane = cache
                .get(&cache_key)
                .expect("cache entry")
                .primary_service
                .as_ref()
                .expect("restored mutation service")
                .lock()
                .expect("restored mutation service lock");
            assert!(
                !lane.metadata_hydrated,
                "maintenance must collapse the full projection before restoring the lane"
            );
            assert!(
                lane.service
                    .metadata()
                    .object(&unrelated_object_id)
                    .is_none(),
                "restored maintenance lane must not retain accumulated object metadata"
            );
        }
        assert_eq!(
            daemon_dfs_test_message_service_lane_open_count(&data_dir),
            lane_open_count_before_maintenance,
            "successful maintenance must not open another backend-bearing lane"
        );

        let mut second_write = test_dfs_write_routed_message(
            &remote,
            &local,
            3,
            "/services/daemon-write-after-maintenance.txt",
        );
        let DfsNetworkMessage::DistributedWriteRequest(write) = &mut second_write.message else {
            panic!("distributed write request");
        };
        write.desired_replica_nodes = vec![local.clone()];
        write
            .object_metadata
            .as_mut()
            .expect("write object metadata")
            .current_locations = vec![local.clone()];
        handle_daemon_dfs_message_on_runtime(
            &paths,
            test_dfs_message_command("dfs_message_write_after_maintenance", second_write),
            runtime.handle(),
        )
        .expect("distributed write after maintenance");
        assert_eq!(
            daemon_dfs_test_message_service_lane_open_count(&data_dir),
            lane_open_count_before_maintenance,
            "the first post-maintenance mutation must not reopen metadata"
        );

        let repair_snapshot = sinomo_dfs::default_dfs_repair_attempt_snapshot_path(&data_dir);
        fs::create_dir_all(
            repair_snapshot
                .parent()
                .expect("repair snapshot parent directory"),
        )
        .expect("create repair snapshot directory");
        fs::write(&repair_snapshot, b"invalid repair snapshot").expect("corrupt repair snapshot");
        let error = run_daemon_dfs_maintenance_body_for_test(&paths, &runtime)
            .expect_err("corrupt repair state must fail maintenance");
        assert!(
            error.to_string().contains("repair"),
            "unexpected maintenance error: {error}"
        );
        {
            let cache = daemon_dfs_message_service_cache()
                .lock()
                .expect("cache lock");
            assert!(
                cache
                    .get(&cache_key)
                    .expect("cache entry")
                    .primary_service
                    .is_none(),
                "failed maintenance must drop its backend-bearing mutation lane"
            );
        }
        fs::remove_file(&repair_snapshot).expect("remove corrupt repair snapshot");

        let mut third_write = test_dfs_write_routed_message(
            &remote,
            &local,
            4,
            "/services/daemon-write-after-failed-maintenance.txt",
        );
        let DfsNetworkMessage::DistributedWriteRequest(write) = &mut third_write.message else {
            panic!("distributed write request");
        };
        write.desired_replica_nodes = vec![local.clone()];
        write
            .object_metadata
            .as_mut()
            .expect("write object metadata")
            .current_locations = vec![local.clone()];
        handle_daemon_dfs_message_on_runtime(
            &paths,
            test_dfs_message_command("dfs_message_write_after_failed_maintenance", third_write),
            runtime.handle(),
        )
        .expect("distributed write after failed maintenance");
        assert_eq!(
            daemon_dfs_test_message_service_lane_open_count(&data_dir),
            lane_open_count_before_maintenance.saturating_add(1),
            "the first mutation after failed maintenance must open a new lane"
        );

        let current_state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("cluster state before peer-only maintenance refresh");
        let retained = take_daemon_dfs_message_mutation_service_for_maintenance(
            &data_dir,
            &cluster_paths,
            &current_state,
            runtime.handle(),
        )
        .expect("take mutation lane before peer-only refresh");
        let added_peer =
            NodeId::from_legacy_named("node_dfs_protocol_added_peer").expect("added peer");
        let mut changed_state = current_state;
        changed_state
            .members
            .push(test_remote_cluster_member_with_endpoint(
                &changed_state.cluster_id,
                &added_peer,
                "127.0.0.2:9",
            ));
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &changed_state)
            .expect("write peer-only cluster-state change");
        assert!(
            restore_daemon_dfs_message_mutation_service_after_maintenance(
                &data_dir,
                &cluster_paths,
                runtime.handle(),
                retained,
            )
            .expect("restore mutation lane after peer-only refresh"),
            "peer membership changes must refresh, not discard, the retained mutation lane"
        );
        {
            let cache = daemon_dfs_message_service_cache()
                .lock()
                .expect("cache lock");
            let entry = cache.get(&cache_key).expect("cache entry");
            let lane = entry
                .primary_service
                .as_ref()
                .expect("restored mutation lane")
                .lock()
                .expect("mutation lane lock");
            assert!(
                lane.peer_node_ids.contains(&added_peer),
                "the retained mutation lane must install the current peer targets"
            );
        }
        assert_eq!(
            daemon_dfs_test_message_service_lane_open_count(&data_dir),
            lane_open_count_before_maintenance.saturating_add(1),
            "peer-only maintenance refresh must retain the open metadata backend"
        );

        let current_state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("current cluster state");
        let retained = take_daemon_dfs_message_mutation_service_for_maintenance(
            &data_dir,
            &cluster_paths,
            &current_state,
            runtime.handle(),
        )
        .expect("take mutation lane for authority-change validation");
        let changed_root_shard = DfsMetadataShardConfig {
            epoch: root_shard.epoch.saturating_add(1),
            ..root_shard
        };
        append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(
                        &changed_root_shard.file_system_id,
                        &changed_root_shard.shard_id,
                    ),
                    &changed_root_shard,
                    None,
                )
                .expect("changed root shard op"),
            ),
        )
        .expect("commit changed root shard");
        assert!(
            !restore_daemon_dfs_message_mutation_service_after_maintenance(
                &data_dir,
                &cluster_paths,
                runtime.handle(),
                retained,
            )
            .expect("validate changed maintenance signature"),
            "authority changes must reject a maintenance lane captured from the old generation"
        );
        assert!(
            daemon_dfs_message_service_cache()
                .lock()
                .expect("cache lock")
                .get(&cache_key)
                .expect("cache entry")
                .primary_service
                .is_none(),
            "a rejected authority generation must leave the mutation lane empty"
        );
        drop(read_service);
        assert!(
            evict_daemon_dfs_message_service_cache(&data_dir).expect("evict message cache"),
            "shutdown must release the complete message cache"
        );
        assert!(
            weak_read.upgrade().is_none(),
            "shutdown must collect the lightweight read lane"
        );
    }

    #[test]
    fn daemon_dfs_lookup_message_reuses_service_without_full_hydrate() {
        let root = temp_root("daemon_dfs_message_warm_cache");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let local = NodeId::from_legacy_named("node_dfs_protocol_warm_local").expect("local");
        let remote = NodeId::from_legacy_named("node_dfs_protocol_warm_remote").expect("remote");
        write_test_cluster_state_with_remote_endpoint(
            &data_root,
            &local,
            &remote,
            "10.77.0.1:21353",
        );
        let runtime = daemon_test_runtime("daemon-dfs-message-warm-cache");

        let cache_key = SinomoDataDir::new(&data_root).root();
        let response = handle_daemon_dfs_message_on_runtime(
            &paths,
            test_dfs_message_command(
                "dfs_message_warm_cache_1",
                test_dfs_lookup_routed_message(&remote, &local, 1, "/services/warm-cache-1.txt"),
            ),
            runtime.handle(),
        )
        .expect("first DFS message");
        assert_eq!(response.request_id, "dfs_message_warm_cache_1");
        let read_service = {
            let cache = daemon_dfs_message_service_cache()
                .lock()
                .expect("cache lock");
            let entry = cache.get(&cache_key).expect("cache entry");
            assert!(
                entry.primary_service.is_none(),
                "a cold lightweight lookup must not open the mutation lane"
            );
            Arc::clone(
                entry
                    .lightweight_read_service
                    .as_ref()
                    .expect("lookup opens lightweight read lane"),
            )
        };
        let first_refresh = {
            let lane = read_service.lock().expect("read service lock");
            assert!(
                !lane.metadata_hydrated,
                "lookup messages should use targeted metadata reads instead of forcing a full cold-lane hydrate"
            );
            lane.last_metadata_refresh
        };
        let response = handle_daemon_dfs_message_on_runtime(
            &paths,
            test_dfs_message_command(
                "dfs_message_warm_cache_2",
                test_dfs_lookup_routed_message(&remote, &local, 2, "/services/warm-cache-2.txt"),
            ),
            runtime.handle(),
        )
        .expect("second DFS message");
        assert_eq!(response.request_id, "dfs_message_warm_cache_2");
        let second_refresh = read_service
            .lock()
            .expect("read service lock")
            .last_metadata_refresh;
        assert_eq!(
            second_refresh, first_refresh,
            "recently hydrated fresh-metadata commands should reuse the warm lane"
        );
        write_test_cluster_state_with_remote_endpoint(
            &data_root,
            &local,
            &remote,
            "10.77.0.2:21353",
        );
        let response = handle_daemon_dfs_message_on_runtime(
            &paths,
            test_dfs_message_command(
                "dfs_message_warm_cache_3",
                test_dfs_lookup_routed_message(&remote, &local, 3, "/services/warm-cache-3.txt"),
            ),
            runtime.handle(),
        )
        .expect("third DFS message");
        assert_eq!(response.request_id, "dfs_message_warm_cache_3");
        let third_refresh = read_service
            .lock()
            .expect("read service lock")
            .last_metadata_refresh;
        assert_eq!(
            third_refresh, first_refresh,
            "endpoint refreshes should not force another full metadata hydrate"
        );
        let second_read_service = {
            let cache = daemon_dfs_message_service_cache()
                .lock()
                .expect("cache lock");
            let entry = cache.get(&cache_key).expect("cache entry");
            assert!(entry.primary_service.is_none());
            Arc::clone(
                entry
                    .lightweight_read_service
                    .as_ref()
                    .expect("lightweight read lane"),
            )
        };
        assert!(
            Arc::ptr_eq(&read_service, &second_read_service),
            "endpoint-list refreshes should reuse the lightweight read lane"
        );
        assert_eq!(
            daemon_dfs_test_message_service_lane_open_count(&SinomoDataDir::new(&data_root)),
            1,
            "lightweight reads should open only their targeted lane"
        );
    }

    #[test]
    fn daemon_dfs_peer_membership_refresh_reuses_lightweight_lane() {
        let root = temp_root("daemon_dfs_message_peer_refresh");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let local = NodeId::from_legacy_named("node_dfs_peer_refresh_local").expect("local");
        let remote = NodeId::from_legacy_named("node_dfs_peer_refresh_remote").expect("remote");
        let added = NodeId::from_legacy_named("node_dfs_peer_refresh_added").expect("added");
        write_test_cluster_state_with_remote_endpoint(&data_root, &local, &remote, "127.0.0.1:9");
        let runtime = daemon_test_runtime("daemon-dfs-message-peer-refresh");

        handle_daemon_dfs_message_on_runtime(
            &paths,
            test_dfs_message_command(
                "dfs_message_peer_refresh_1",
                test_dfs_lookup_routed_message(&remote, &local, 1, "/services/peer-refresh-1.txt"),
            ),
            runtime.handle(),
        )
        .expect("initial lookup");
        let cache_key = data_dir.root();
        let read_service = {
            let cache = daemon_dfs_message_service_cache()
                .lock()
                .expect("cache lock");
            Arc::clone(
                cache
                    .get(&cache_key)
                    .expect("cache entry")
                    .lightweight_read_service
                    .as_ref()
                    .expect("lightweight read lane"),
            )
        };
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_daemon_dfs_protocol")
                .expect("cluster");
        write_test_cluster_state_with_members(
            &data_root,
            &local,
            vec![
                test_remote_cluster_member_with_endpoint(&cluster_id, &remote, "127.0.0.1:9"),
                test_remote_cluster_member_with_endpoint(&cluster_id, &added, "127.0.0.2:9"),
            ],
        );
        handle_daemon_dfs_message_on_runtime(
            &paths,
            test_dfs_message_command(
                "dfs_message_peer_refresh_2",
                test_dfs_lookup_routed_message(&remote, &local, 2, "/services/peer-refresh-2.txt"),
            ),
            runtime.handle(),
        )
        .expect("lookup after peer addition");
        {
            let cache = daemon_dfs_message_service_cache()
                .lock()
                .expect("cache lock");
            let entry = cache.get(&cache_key).expect("cache entry");
            let current = entry
                .lightweight_read_service
                .as_ref()
                .expect("lightweight read lane");
            assert!(
                Arc::ptr_eq(&read_service, current),
                "peer addition must not reopen the lightweight metadata backend"
            );
            let lane = current.lock().expect("lightweight lane lock");
            assert!(lane.peer_node_ids.contains(&remote));
            assert!(lane.peer_node_ids.contains(&added));
        }
        assert_eq!(
            daemon_dfs_test_message_service_lane_open_count(&data_dir),
            1,
            "peer membership changes must keep the one open metadata backend"
        );
        assert!(
            evict_daemon_dfs_message_service_cache(&data_dir).expect("evict message cache"),
            "test cleanup must release the cached lane"
        );
    }

    #[test]
    fn daemon_dfs_exact_read_lane_does_not_open_metadata_backend() {
        let root = temp_root("daemon_dfs_exact_read_content_lane");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        let local = NodeId::from_legacy_named("node_dfs_exact_read_local").expect("local");
        let remote = NodeId::from_legacy_named("node_dfs_exact_read_remote").expect("remote");
        write_test_cluster_state_with_remote_endpoint(&data_root, &local, &remote, "127.0.0.1:9");
        let runtime = daemon_test_runtime("daemon-dfs-exact-read-content-lane");

        let response = handle_daemon_dfs_message_on_runtime(
            &paths,
            test_dfs_message_command(
                "dfs_message_exact_content_read",
                test_dfs_exact_read_routed_message(&remote, &local, 1),
            ),
            runtime.handle(),
        )
        .expect("exact content read");
        assert_eq!(response.request_id, "dfs_message_exact_content_read");

        let cache = daemon_dfs_message_service_cache()
            .lock()
            .expect("cache lock");
        let entry = cache.get(&data_dir.root()).expect("cache entry");
        assert!(entry.content_read_service.is_some());
        assert!(entry.lightweight_read_service.is_none());
        assert!(entry.primary_service.is_none());
        drop(cache);
        assert_eq!(
            daemon_dfs_test_message_service_lane_open_count(&data_dir),
            0,
            "exact immutable reads must not open a metadata-bearing service lane"
        );
    }

    #[test]
    fn daemon_dfs_metadata_authority_generation_tracks_only_exact_projection_changes() {
        let root = temp_root("daemon_dfs_metadata_authority_generation");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local = NodeId::from_legacy_named("node_dfs_authority_generation").expect("local");
        write_test_cluster_state(&data_root, &local);
        let generation =
            daemon_dfs_metadata_authority_generation(&cluster_paths).expect("generation");
        let initial = generation.generation();
        let initial_rebuilds = generation.projection_rebuild_count();
        assert!(
            initial_rebuilds > 0,
            "the resident snapshot must initialize once"
        );

        let append = |key: &str, value: &[u8]| {
            append_cluster_raft_command(
                &cluster_paths,
                &local,
                sinomo_core::kv::KvCommand::Op(sinomo_core::kv::KvOp::Put {
                    key: key.to_string(),
                    value: value.to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append KV value");
        };
        append("/cluster/unrelated", b"one");
        append("/cluster/unrelated", b"two");
        assert_eq!(generation.generation(), initial);
        assert_eq!(
            generation.projection_rebuild_count(),
            initial_rebuilds,
            "unrelated commits must advance the cursor without rebuilding the authority projection"
        );

        let file_system_id = sinomo_dfs::sifs_file_system_id();
        let shard_key = metadata_shard_key(&file_system_id, "root");
        append(&shard_key, b"shard-v1");
        let after_shard = generation.generation();
        assert_eq!(after_shard, initial + 1);
        let after_shard_rebuilds = generation.projection_rebuild_count();
        assert_eq!(after_shard_rebuilds, initial_rebuilds + 1);
        append(&shard_key, b"shard-v1");
        let after_identical_shard_rebuilds = generation.projection_rebuild_count();
        assert_eq!(
            after_identical_shard_rebuilds, after_shard_rebuilds,
            "an idempotent put produces no committed authority generation"
        );
        append("/dfs/filesystems/unrelated/config", b"config-churn");
        assert_eq!(generation.generation(), after_shard);
        assert_eq!(
            generation.projection_rebuild_count(),
            after_identical_shard_rebuilds,
            "nearby but non-authority DFS keys must not rebuild the projection"
        );

        append(
            &sinomo_dfs::metadata_shard_membership_handoff_key(&file_system_id, "root"),
            b"handoff-v1",
        );
        let after_handoff = generation.generation();
        assert_eq!(after_handoff, after_shard + 1);
        append(
            &sinomo_dfs::metadata_shard_membership_handoff_proof_key(&file_system_id, "root"),
            b"proof-v1",
        );
        assert_eq!(generation.generation(), after_handoff + 1);

        let cluster_state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("cluster state");
        let detached_send_runtime = daemon_dfs_detached_send_runtime(&data_dir, &cluster_paths)
            .expect("detached send runtime");
        let retained = daemon_dfs_retained_metadata_authority_generation(
            detached_send_runtime.as_ref(),
            &cluster_paths,
            &cluster_state.cluster_id,
            &cluster_state.local_node_id,
            None,
        )
        .expect("retained authority observer");
        assert!(Arc::ptr_eq(&generation, &retained));
        let retained_pointer = Arc::as_ptr(&retained);
        let before_no_cache_change = retained.generation();
        drop(generation);
        drop(retained);

        append(&shard_key, b"shard-v2");
        let retained_after_change = daemon_dfs_retained_metadata_authority_generation(
            detached_send_runtime.as_ref(),
            &cluster_paths,
            &cluster_state.cluster_id,
            &cluster_state.local_node_id,
            None,
        )
        .expect("authority observer retained without a message cache lane");
        assert_eq!(Arc::as_ptr(&retained_after_change), retained_pointer);
        assert_eq!(
            retained_after_change.generation(),
            before_no_cache_change + 1,
            "a no-read daemon must observe topology changes from the retained subscription"
        );
    }

    #[test]
    fn metadata_authority_change_schedules_inherited_local_primary_backlog() {
        let root = temp_root("daemon-dfs-authority-inherited-backlog");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local = NodeId::from_legacy_named("node_dfs_inherited_backlog_local").expect("node");
        let remote = NodeId::from_legacy_named("node_dfs_inherited_backlog_remote").expect("node");
        write_test_cluster_state(&data_root, &local);
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("cluster state");
        let file_system = distributed_sifs(local.clone());
        let mut shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: remote.clone(),
            replica_node_ids: vec![local.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
                compares: Vec::new(),
                success: vec![
                    sinomo_core::kv::KvOp::put_serde(
                        file_system_config_key(&file_system.file_system_id),
                        &DfsFileSystemConfig::from(&file_system),
                        None,
                    )
                    .expect("filesystem config op"),
                    sinomo_core::kv::KvOp::put_serde(
                        metadata_shard_key(&file_system.file_system_id, "root"),
                        &shard,
                        None,
                    )
                    .expect("remote-primary shard op"),
                ],
                failure: Vec::new(),
            }),
        )
        .expect("initial remote authority");
        let mut inherited = sinomo_dfs::open_local_metadata_shard_backend(
            &data_dir,
            DfsMetadataShardId::new("root"),
        )
        .expect("local replica backend");
        inherited
            .put_file_system(&file_system)
            .expect("replica filesystem");
        let inherited_object = distributed_sifs_bootstrap_objects(local.clone())
            .into_iter()
            .next()
            .expect("inherited object");
        inherited
            .put_object(&inherited_object)
            .expect("inherited journal entry");
        drop(inherited);

        let runtime = daemon_test_runtime("daemon-dfs-authority-inherited-backlog");
        let paths = DaemonPaths::new(&data_dir);
        let control_owner = Arc::new(
            DaemonDataOwnerLock::acquire(&paths).expect("control-loop data-owner capability"),
        );
        register_daemon_dfs_data_owner_lock(&paths, &control_owner)
            .expect("register data-owner capability");
        let generation = daemon_dfs_metadata_authority_generation_for_identity(
            &cluster_paths,
            &state.cluster_id,
            &local,
            Some(runtime.handle()),
        )
        .expect("authority observer");
        let initial_generation = generation.generation();
        let lane = DaemonDfsMutationLaneKey {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new("root"),
        };
        let initial_requests = daemon_dfs_metadata_publish_lane_request_count(&data_dir, &lane);

        shard.primary_node_id = local.clone();
        shard.replica_node_ids = vec![remote];
        shard.epoch = 2;
        append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &shard,
                    None,
                )
                .expect("local-primary shard op"),
            ),
        )
        .expect("transfer authority to local node");

        let deadline = Instant::now() + Duration::from_secs(3);
        while daemon_dfs_metadata_publish_lane_request_count(&data_dir, &lane) == initial_requests
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(generation.generation(), initial_generation + 1);
        assert!(
            daemon_dfs_metadata_publish_lane_request_count(&data_dir, &lane) > initial_requests,
            "the authority edge must publish an inherited journal without another DFS mutation"
        );
    }

    #[test]
    fn external_snapshot_materialization_schedules_inherited_local_primary_backlog() {
        let root = temp_root("daemon-dfs-external-authority-inherited-backlog");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local =
            NodeId::from_legacy_named("node_dfs_external_inherited_backlog_local").expect("node");
        let remote =
            NodeId::from_legacy_named("node_dfs_external_inherited_backlog_remote").expect("node");
        write_test_cluster_state(&data_root, &local);
        let file_system = distributed_sifs(local.clone());
        let mut shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: remote.clone(),
            replica_node_ids: vec![local.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
                compares: Vec::new(),
                success: vec![
                    sinomo_core::kv::KvOp::put_serde(
                        file_system_config_key(&file_system.file_system_id),
                        &DfsFileSystemConfig::from(&file_system),
                        None,
                    )
                    .expect("filesystem config op"),
                    sinomo_core::kv::KvOp::put_serde(
                        metadata_shard_key(&file_system.file_system_id, "root"),
                        &shard,
                        None,
                    )
                    .expect("remote-primary shard op"),
                ],
                failure: Vec::new(),
            }),
        )
        .expect("initial remote authority");
        let mut inherited = sinomo_dfs::open_local_metadata_shard_backend(
            &data_dir,
            DfsMetadataShardId::new("root"),
        )
        .expect("local replica backend");
        inherited
            .put_file_system(&file_system)
            .expect("replica filesystem");
        let inherited_object = distributed_sifs_bootstrap_objects(local.clone())
            .into_iter()
            .next()
            .expect("inherited object");
        inherited
            .put_object(&inherited_object)
            .expect("inherited journal entry");
        drop(inherited);

        let runtime = daemon_test_runtime("daemon-dfs-external-authority-inherited-backlog");
        let paths = DaemonPaths::new(&data_dir);
        let control_owner = Arc::new(
            DaemonDataOwnerLock::acquire(&paths).expect("control-loop data-owner capability"),
        );
        register_daemon_dfs_data_owner_lock(&paths, &control_owner)
            .expect("register data-owner capability");
        // Construct only the production filesystem edge: no in-process
        // committed-snapshot subscription is installed in this regression.
        let generation = Arc::new(DaemonDfsMetadataAuthorityGeneration::new());
        let initial_snapshot =
            sinomo_core::cluster::read_cluster_kv_latest_observed_snapshot_arc_if_present(
                &cluster_paths,
            )
            .expect("read initial authority")
            .expect("initial authority snapshot");
        generation.observe(initial_snapshot.as_ref());
        generation.attach_publish_wake(&cluster_paths, Some(runtime.handle()));
        let watcher = daemon_dfs_metadata_authority_snapshot_watcher(
            &cluster_paths,
            Arc::downgrade(&generation),
            runtime.handle(),
        )
        .expect("authority snapshot watcher");
        generation
            .snapshot_watcher
            .set(watcher)
            .expect("retain authority snapshot watcher");
        assert!(
            generation.snapshot_watcher.get().is_some(),
            "a live daemon authority observer must retain the cross-process snapshot watcher"
        );
        let initial_generation = generation.generation();
        let lane = DaemonDfsMutationLaneKey {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new("root"),
        };
        let initial_requests = daemon_dfs_metadata_publish_lane_request_count(&data_dir, &lane);

        shard.primary_node_id = local.clone();
        shard.replica_node_ids = vec![remote];
        shard.epoch = 2;
        let current = sinomo_core::cluster::read_cluster_kv_current_snapshot(&cluster_paths)
            .expect("current snapshot before external materialization");
        let mut external_store =
            sinomo_core::kv::InProcessKvStore::restore(current).expect("restore external store");
        external_store
            .put_serde(
                metadata_shard_key(&file_system.file_system_id, "root"),
                &shard,
                None,
            )
            .expect("apply externally committed authority");
        let external_snapshot = external_store.snapshot().expect("external snapshot");
        // Write through the R0 materialized-snapshot API, deliberately bypassing
        // sinomo-cluster's in-process committed-snapshot observer. This models
        // a separate CLI process advancing the shared Docker data directory.
        sinomo_core::kv::write_cluster_kv_snapshot(&cluster_paths.kv_paths(), &external_snapshot)
            .expect("materialize external authority snapshot");

        let external_write_at = Instant::now();
        let deadline = Instant::now() + Duration::from_secs(3);
        while generation.generation() == initial_generation && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(generation.generation(), initial_generation + 1);
        assert!(
            external_write_at.elapsed()
                >= DAEMON_DFS_EXTERNAL_AUTHORITY_SNAPSHOT_DEBOUNCE
                    .saturating_sub(Duration::from_millis(100)),
            "the external write must be observed by the debounced filesystem edge, not an in-process fallback"
        );
        while daemon_dfs_metadata_publish_lane_request_count(&data_dir, &lane) == initial_requests
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            daemon_dfs_metadata_publish_lane_request_count(&data_dir, &lane) > initial_requests,
            "an authority snapshot materialized by another process must publish inherited backlog"
        );
    }

    #[test]
    fn daemon_dfs_detached_send_runtime_is_scoped_to_cluster_and_local_identity() {
        let root = temp_root("daemon_dfs_detached_send_runtime_identity");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local = NodeId::from_legacy_named("node_dfs_runtime_identity").expect("local");
        write_test_cluster_state(&data_root, &local);
        let cluster_state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("cluster state");

        let original = daemon_dfs_detached_send_runtime_for_identity(
            &data_dir,
            &cluster_state.cluster_id,
            &cluster_state.local_node_id,
        )
        .expect("original identity runtime");
        let same = daemon_dfs_detached_send_runtime_for_identity(
            &data_dir,
            &cluster_state.cluster_id,
            &cluster_state.local_node_id,
        )
        .expect("same identity runtime");
        assert!(Arc::ptr_eq(&original, &same));

        let other_cluster =
            sinomo_core::Id::from_legacy_named("cluster_dfs_runtime_other").expect("other cluster");
        let other_local = NodeId::from_legacy_named("node_dfs_runtime_other").expect("other node");
        let changed_cluster = daemon_dfs_detached_send_runtime_for_identity(
            &data_dir,
            &other_cluster,
            &cluster_state.local_node_id,
        )
        .expect("changed cluster runtime");
        let changed_local = daemon_dfs_detached_send_runtime_for_identity(
            &data_dir,
            &cluster_state.cluster_id,
            &other_local,
        )
        .expect("changed local runtime");
        assert!(!Arc::ptr_eq(&original, &changed_cluster));
        assert!(!Arc::ptr_eq(&original, &changed_local));
    }

    fn default_dfs_reactor_test_snapshot(
        revision: u64,
        entries: Vec<(String, Vec<u8>)>,
    ) -> KvSnapshot {
        let mut entries = entries
            .into_iter()
            .map(|(key, value)| KvEntry {
                key,
                value,
                create_revision: KvRevision(revision),
                modify_revision: KvRevision(revision),
                version: sinomo_core::kv::KvVersion(1),
                lease_id: None,
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.key.cmp(&right.key));
        KvSnapshot {
            revision: KvRevision(revision),
            compacted_revision: KvRevision::ZERO,
            entries,
            leases: Vec::new(),
            events: Vec::new(),
            committed_requests: Vec::new(),
            expired_requests: Vec::new(),
        }
    }

    #[test]
    fn default_dfs_projection_skips_unrelated_commits_incrementally() {
        let local = NodeId::from_legacy_named("node_default_dfs_incremental").expect("node");
        let config_key = sinomo_dfs::file_system_config_key(&sinomo_dfs::sifs_file_system_id());
        let (projection_tx, mut projection_rx) =
            tokio::sync::watch::channel(DaemonDefaultDfsReconciliationProjection::default());
        let mut observer = DaemonDefaultDfsCommittedProjectionObserver::default();

        let initial = default_dfs_reactor_test_snapshot(1, vec![(config_key.clone(), vec![1])]);
        observer.observe(&projection_tx, &local, &initial);
        assert_eq!(observer.rebuild_count, 1);
        let _ = projection_rx.borrow_and_update();

        let mut unrelated = default_dfs_reactor_test_snapshot(
            2,
            vec![
                (config_key.clone(), vec![1]),
                ("/unrelated/application/state".to_string(), vec![9]),
            ],
        );
        unrelated.events.push(sinomo_core::kv::KvEvent {
            revision: KvRevision(2),
            key: "/unrelated/application/state".to_string(),
            kind: sinomo_core::kv::KvEventKind::Put { value: vec![9] },
        });
        observer.observe(&projection_tx, &local, &unrelated);
        assert_eq!(
            observer.rebuild_count, 1,
            "an unrelated commit must not reconstruct the default DFS projection"
        );
        assert!(
            !projection_rx
                .has_changed()
                .expect("projection channel open"),
            "unrelated commits must not wake the reactor"
        );
        assert_eq!(projection_tx.borrow().revision, Some(KvRevision(2)));

        let mut relevant =
            default_dfs_reactor_test_snapshot(3, vec![(config_key.clone(), vec![2])]);
        relevant.events.push(sinomo_core::kv::KvEvent {
            revision: KvRevision(3),
            key: config_key,
            kind: sinomo_core::kv::KvEventKind::Put { value: vec![2] },
        });
        observer.observe(&projection_tx, &local, &relevant);
        assert_eq!(observer.rebuild_count, 2);
        assert!(
            projection_rx
                .has_changed()
                .expect("projection channel open"),
            "relevant commits must wake the reactor"
        );
        let _ = projection_rx.borrow_and_update();

        let mut compacted = default_dfs_reactor_test_snapshot(
            4,
            vec![("/unrelated/application/state".to_string(), vec![10])],
        );
        compacted.compacted_revision = KvRevision(4);
        observer.observe(&projection_tx, &local, &compacted);
        assert_eq!(
            observer.rebuild_count, 3,
            "compaction must rebuild from the resident snapshot"
        );
    }

    #[test]
    fn default_dfs_reactor_starts_dirty_and_coalesces_snapshot_inputs() {
        let root = temp_root("default-dfs-reactor-coalesces-inputs");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let local = NodeId::from_legacy_named("node_default_dfs_coalesce").expect("node");
        let mut reactor =
            DaemonDefaultDfsReconciliationReactor::new(&cluster_paths, &local).expect("reactor");

        let startup = reactor.pending_work().expect("dirty startup work");
        assert!(startup.reconcile_defaults);
        assert!(startup.recover_prior_process_permit);
        assert!(startup.refresh_before_recovery);
        reactor.complete_default_reconciliation(&startup);
        reactor.complete_prior_process_recovery(&startup);
        assert_eq!(reactor.pending_work(), None);

        let config_key = sinomo_dfs::file_system_config_key(&sinomo_dfs::sifs_file_system_id());
        observe_daemon_default_dfs_reconciliation(
            &reactor._projection_tx,
            &local,
            &default_dfs_reactor_test_snapshot(1, vec![(config_key.clone(), vec![1])]),
        );
        observe_daemon_default_dfs_reconciliation(
            &reactor._projection_tx,
            &local,
            &default_dfs_reactor_test_snapshot(2, vec![(config_key, vec![2])]),
        );

        let changed = reactor.pending_work().expect("coalesced input change");
        assert!(changed.reconcile_defaults);
        assert!(!changed.recover_prior_process_permit);
        assert!(!changed.refresh_before_recovery);
        assert!(changed.inputs.iter().any(|(_, value)| value == &vec![2]));
        reactor.complete_default_reconciliation(&changed);
        assert_eq!(
            reactor.pending_work(),
            None,
            "the latest committed input generation must reconcile exactly once"
        );
    }

    #[test]
    fn default_dfs_reactor_consumes_local_replica_progress_once() {
        let root = temp_root("default-dfs-reactor-local-replica-progress");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let local = NodeId::from_legacy_named("node_default_dfs_local_progress").expect("node");
        let mut reactor =
            DaemonDefaultDfsReconciliationReactor::new(&cluster_paths, &local).expect("reactor");
        let startup = reactor.pending_work().expect("startup work");
        reactor.complete_default_reconciliation(&startup);
        reactor.complete_prior_process_recovery(&startup);
        assert_eq!(reactor.pending_work(), None);

        notify_daemon_default_dfs_reconciliation_replica_progress(&cluster_paths);
        let progress = reactor.pending_work().expect("local replica progress");
        assert!(progress.reconcile_defaults);
        assert!(!progress.recover_prior_process_permit);
        reactor.complete_default_reconciliation(&progress);
        assert_eq!(
            reactor.pending_work(),
            None,
            "one local replica generation must reconcile exactly once"
        );
    }

    #[test]
    fn default_dfs_parked_handoff_ignores_unowned_inputs_and_completes_once() {
        let root = temp_root("default-dfs-reactor-parked-handoff");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let local = NodeId::from_legacy_named("node_default_dfs_parked").expect("local node");
        let replica =
            NodeId::from_legacy_named("node_default_dfs_parked_replica").expect("replica node");
        let mut reactor =
            DaemonDefaultDfsReconciliationReactor::new(&cluster_paths, &local).expect("reactor");
        let startup = reactor.pending_work().expect("startup work");
        reactor.complete_default_reconciliation(&startup);
        reactor.complete_prior_process_recovery(&startup);

        let file_system_id = sinomo_dfs::sifs_file_system_id();
        let retiring = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: local.clone(),
            replica_node_ids: vec![replica.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let desired = DfsMetadataShardConfig {
            primary_node_id: replica.clone(),
            replica_node_ids: vec![local.clone()],
            epoch: 2,
            ..retiring.clone()
        };
        let handoff = DfsMetadataShardMembershipHandoff {
            handoff_id: "handoff-default-dfs-parked".to_string(),
            retiring_shard: retiring.clone(),
            desired_shard: desired.clone(),
            required_prepared_sequence: 0,
            required_head_watermark: None,
            retiring_primary_fence: None,
        };
        let shard_key = sinomo_dfs::metadata_shard_key(&file_system_id, "root");
        let handoff_key =
            sinomo_dfs::metadata_shard_membership_handoff_key(&file_system_id, "root");
        let proof_key =
            sinomo_dfs::metadata_shard_membership_handoff_proof_key(&file_system_id, "root");
        let unrelated_key =
            format!("{DAEMON_DEFAULT_DFS_CONNECTIVITY_PREFIX}{local}/{replica}/test-endpoint");
        let parked_entries = vec![
            (
                shard_key.clone(),
                sinomo_core::kv::encode_kv_value(&retiring).expect("retiring shard"),
            ),
            (
                handoff_key.clone(),
                sinomo_core::kv::encode_kv_value(&handoff).expect("handoff"),
            ),
            (unrelated_key.clone(), vec![1]),
        ];
        observe_daemon_default_dfs_reconciliation(
            &reactor._projection_tx,
            &local,
            &default_dfs_reactor_test_snapshot(1, parked_entries.clone()),
        );
        let work = reactor.pending_work().expect("handoff input generation");
        let parked = DaemonDefaultDfsParkedHandoff::new(
            file_system_id.clone(),
            "root".to_string(),
            handoff.handoff_id.clone(),
            retiring.epoch,
            desired.epoch,
        );
        reactor.park_default_reconciliation(&work, parked);
        assert_eq!(
            reactor.pending_work(),
            None,
            "recording the durable wait must consume the attempted generation without a failure loop"
        );

        let mut unrelated_entries = parked_entries;
        unrelated_entries
            .iter_mut()
            .find(|(key, _)| key == &unrelated_key)
            .expect("unrelated input")
            .1 = vec![2];
        observe_daemon_default_dfs_reconciliation(
            &reactor._projection_tx,
            &local,
            &default_dfs_reactor_test_snapshot(2, unrelated_entries),
        );
        assert_eq!(
            reactor.pending_work(),
            None,
            "connectivity/source churn cannot own a parked handoff retry"
        );

        let proof = sinomo_dfs::DfsMetadataShardMembershipHandoffProof {
            handoff_id: handoff.handoff_id,
            retiring_shard: retiring,
            desired_shard: desired.clone(),
            journal_sequence: 7,
            state_fingerprint: "blake3:default-dfs-parked".to_string(),
            acknowledged_node_ids: vec![local.clone(), replica],
        };
        let completed_entries = vec![
            (
                shard_key,
                sinomo_core::kv::encode_kv_value(&desired).expect("active shard"),
            ),
            (
                proof_key,
                sinomo_core::kv::encode_kv_value(&proof).expect("completion proof"),
            ),
            (unrelated_key, vec![2]),
        ];
        observe_daemon_default_dfs_reconciliation(
            &reactor._projection_tx,
            &local,
            &default_dfs_reactor_test_snapshot(3, completed_entries.clone()),
        );
        let completion = reactor
            .pending_work()
            .expect("proof and authority completion generation");
        reactor.complete_default_reconciliation(&completion);
        assert_eq!(reactor.pending_work(), None);
        observe_daemon_default_dfs_reconciliation(
            &reactor._projection_tx,
            &local,
            &default_dfs_reactor_test_snapshot(4, completed_entries),
        );
        assert_eq!(
            reactor.pending_work(),
            None,
            "the same completed handoff generation must not activate or clear twice"
        );
    }

    #[test]
    fn parked_default_dfs_projection_rebuilds_only_for_exact_wake_owners() {
        let root = temp_root("default-dfs-projection-parked-owners");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let local =
            NodeId::from_legacy_named("node_default_dfs_projection_parked").expect("local node");
        let file_system_id = sinomo_dfs::sifs_file_system_id();
        let parked = DaemonDefaultDfsParkedHandoff::new(
            file_system_id.clone(),
            "services".to_string(),
            "handoff-projection-parked".to_string(),
            4,
            5,
        );
        let parked_subscription = Arc::new(Mutex::new(Some(parked)));
        let mut observer =
            DaemonDefaultDfsCommittedProjectionObserver::new_with_parked_subscription(
                data_dir,
                parked_subscription,
            );
        let (projection_tx, mut projection_rx) =
            tokio::sync::watch::channel(DaemonDefaultDfsReconciliationProjection::default());
        let shard_key = sinomo_dfs::metadata_shard_key(&file_system_id, "services");
        let proof_key =
            sinomo_dfs::metadata_shard_membership_handoff_proof_key(&file_system_id, "services");
        let connectivity_key =
            format!("{DAEMON_DEFAULT_DFS_CONNECTIVITY_PREFIX}source/target/endpoint");

        let initial = default_dfs_reactor_test_snapshot(
            1,
            vec![
                (shard_key.clone(), vec![1]),
                (connectivity_key.clone(), vec![1]),
            ],
        );
        observer.observe(&projection_tx, &local, &initial);
        assert_eq!(observer.rebuild_count, 1);
        let _ = projection_rx.borrow_and_update();

        let mut unrelated = default_dfs_reactor_test_snapshot(
            2,
            vec![
                (shard_key.clone(), vec![1]),
                (connectivity_key.clone(), vec![2]),
            ],
        );
        unrelated.events.push(sinomo_core::kv::KvEvent {
            revision: KvRevision(2),
            key: connectivity_key.clone(),
            kind: sinomo_core::kv::KvEventKind::Put { value: vec![2] },
        });
        observer.observe(&projection_tx, &local, &unrelated);
        assert_eq!(
            observer.rebuild_count, 1,
            "a parked projection consumes deltas without reopening/rebuilding resident state for unrelated connectivity"
        );
        assert!(!projection_rx.has_changed().expect("projection open"));
        assert!(
            projection_tx
                .borrow()
                .inputs
                .iter()
                .any(|(key, value)| key == &connectivity_key && value == &vec![2]),
            "the ignored wake must still update the resident projection for the generation after handoff completion"
        );

        let mut proof = default_dfs_reactor_test_snapshot(
            3,
            vec![(shard_key, vec![1]), (proof_key.clone(), vec![9])],
        );
        proof.events.push(sinomo_core::kv::KvEvent {
            revision: KvRevision(3),
            key: proof_key,
            kind: sinomo_core::kv::KvEventKind::Put { value: vec![9] },
        });
        observer.observe(&projection_tx, &local, &proof);
        assert_eq!(observer.rebuild_count, 2);
        assert!(
            projection_rx.has_changed().expect("projection open"),
            "the exact completion proof must wake the parked owner"
        );
    }

    #[test]
    fn default_dfs_failure_retry_generation_excludes_unrelated_projection_wakes() {
        let root = temp_root("default-dfs-failure-retry-generation");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let local =
            NodeId::from_legacy_named("node_default_dfs_failure_generation").expect("local node");
        let mut reactor =
            DaemonDefaultDfsReconciliationReactor::new(&cluster_paths, &local).expect("reactor");
        let startup = reactor.pending_work().expect("startup work");
        reactor.complete_default_reconciliation(&startup);
        reactor.complete_prior_process_recovery(&startup);
        let file_system_id = sinomo_dfs::sifs_file_system_id();
        let shard_key = sinomo_dfs::metadata_shard_key(&file_system_id, "users");
        let connectivity_key =
            format!("{DAEMON_DEFAULT_DFS_CONNECTIVITY_PREFIX}failure/source/target");
        observe_daemon_default_dfs_reconciliation(
            &reactor._projection_tx,
            &local,
            &default_dfs_reactor_test_snapshot(
                1,
                vec![
                    (shard_key.clone(), vec![1]),
                    (connectivity_key.clone(), vec![1]),
                ],
            ),
        );
        let failed_generation = reactor.exact_retry_generation();

        observe_daemon_default_dfs_reconciliation(
            &reactor._projection_tx,
            &local,
            &default_dfs_reactor_test_snapshot(
                2,
                vec![(shard_key.clone(), vec![1]), (connectivity_key, vec![2])],
            ),
        );
        assert_eq!(
            reactor.exact_retry_generation(),
            failed_generation,
            "an unrelated source wake must preserve the existing failure backoff deadline"
        );

        observe_daemon_default_dfs_reconciliation(
            &reactor._projection_tx,
            &local,
            &default_dfs_reactor_test_snapshot(3, vec![(shard_key, vec![2])]),
        );
        assert_ne!(
            reactor.exact_retry_generation(),
            failed_generation,
            "a committed shard-authority generation may wake before the failure deadline"
        );
    }

    #[test]
    fn default_dfs_reactor_endpoint_topology_ignores_observed_latency() {
        let root = temp_root("default-dfs-reactor-endpoint-topology");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let local = NodeId::from_legacy_named("node_default_dfs_endpoint").expect("node");
        let mut reactor =
            DaemonDefaultDfsReconciliationReactor::new(&cluster_paths, &local).expect("reactor");
        let startup = reactor.pending_work().expect("startup work");
        reactor.complete_default_reconciliation(&startup);
        reactor.complete_prior_process_recovery(&startup);

        let mut endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_default_dfs_topology")
                .expect("endpoint"),
            node_id: local.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "10.0.0.1:21353".to_string(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 10,
            last_observed_latency_ms: Some(1),
        };
        let endpoint_key = format!(
            "{DAEMON_DEFAULT_DFS_ENDPOINT_PREFIX}{}/{}",
            local, endpoint.endpoint_id
        );
        observe_daemon_default_dfs_reconciliation(
            &reactor._projection_tx,
            &local,
            &default_dfs_reactor_test_snapshot(
                1,
                vec![(
                    endpoint_key.clone(),
                    sinomo_core::kv::encode_kv_value(&endpoint).expect("endpoint value"),
                )],
            ),
        );
        let published = reactor.pending_work().expect("endpoint generation");
        reactor.complete_default_reconciliation(&published);

        endpoint.last_observed_latency_ms = Some(250);
        observe_daemon_default_dfs_reconciliation(
            &reactor._projection_tx,
            &local,
            &default_dfs_reactor_test_snapshot(
                2,
                vec![(
                    endpoint_key.clone(),
                    sinomo_core::kv::encode_kv_value(&endpoint).expect("refreshed endpoint"),
                )],
            ),
        );
        assert_eq!(
            reactor.pending_work(),
            None,
            "latency observations are not DFS placement topology"
        );

        endpoint.address = "10.0.0.2:21353".to_string();
        observe_daemon_default_dfs_reconciliation(
            &reactor._projection_tx,
            &local,
            &default_dfs_reactor_test_snapshot(
                3,
                vec![(
                    endpoint_key,
                    sinomo_core::kv::encode_kv_value(&endpoint).expect("moved endpoint"),
                )],
            ),
        );
        assert!(
            reactor
                .pending_work()
                .is_some_and(|work| work.reconcile_defaults),
            "endpoint address changes must rebuild failover placement"
        );
    }

    #[test]
    fn default_dfs_reactor_repairs_semantic_advertisement_drift_without_refresh_loop() {
        let root = temp_root("default-dfs-reactor-semantic-advertisement");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let local = NodeId::from_legacy_named("node_default_dfs_output").expect("node");
        let mut reactor =
            DaemonDefaultDfsReconciliationReactor::new(&cluster_paths, &local).expect("reactor");
        let startup = reactor.pending_work().expect("startup work");
        reactor.complete_default_reconciliation(&startup);
        reactor.complete_prior_process_recovery(&startup);

        let metadata = sinomo_dfs::DfsNodeAdvertisement::metadata(local.clone());
        let content = sinomo_dfs::DfsNodeAdvertisement::content(local.clone());
        let snapshot = default_dfs_reactor_test_snapshot(
            1,
            vec![
                (
                    sinomo_dfs::metadata_service_key(&local),
                    sinomo_core::kv::encode_kv_value(&metadata).expect("metadata ad"),
                ),
                (
                    sinomo_dfs::content_service_key(&local),
                    sinomo_core::kv::encode_kv_value(&content).expect("content ad"),
                ),
            ],
        );
        observe_daemon_default_dfs_reconciliation(&reactor._projection_tx, &local, &snapshot);
        let published = reactor
            .pending_work()
            .expect("semantic advertisement generation");
        assert!(published.reconcile_defaults);
        reactor.complete_default_reconciliation(&published);

        let mut refreshed_metadata = metadata.clone();
        refreshed_metadata.updated_at += time::Duration::seconds(30);
        let mut refreshed_content = content.clone();
        refreshed_content.updated_at += time::Duration::seconds(30);
        observe_daemon_default_dfs_reconciliation(
            &reactor._projection_tx,
            &local,
            &default_dfs_reactor_test_snapshot(
                2,
                vec![
                    (
                        sinomo_dfs::metadata_service_key(&local),
                        sinomo_core::kv::encode_kv_value(&refreshed_metadata)
                            .expect("refreshed metadata ad"),
                    ),
                    (
                        sinomo_dfs::content_service_key(&local),
                        sinomo_core::kv::encode_kv_value(&refreshed_content)
                            .expect("refreshed content ad"),
                    ),
                ],
            ),
        );
        assert_eq!(
            reactor.pending_work(),
            None,
            "updated_at-only advertisement refreshes must remain quiet"
        );

        refreshed_metadata.accepts_metadata = false;
        observe_daemon_default_dfs_reconciliation(
            &reactor._projection_tx,
            &local,
            &default_dfs_reactor_test_snapshot(
                3,
                vec![
                    (
                        sinomo_dfs::metadata_service_key(&local),
                        sinomo_core::kv::encode_kv_value(&refreshed_metadata)
                            .expect("invalid metadata ad"),
                    ),
                    (
                        sinomo_dfs::content_service_key(&local),
                        sinomo_core::kv::encode_kv_value(&refreshed_content).expect("content ad"),
                    ),
                ],
            ),
        );
        let corrupted = reactor
            .pending_work()
            .expect("semantic advertisement corruption");
        assert!(
            corrupted.reconcile_defaults,
            "semantic advertisement corruption must request repair"
        );
        reactor.complete_default_reconciliation(&corrupted);
        observe_daemon_default_dfs_reconciliation(
            &reactor._projection_tx,
            &local,
            &default_dfs_reactor_test_snapshot(4, Vec::new()),
        );
        assert!(
            reactor
                .pending_work()
                .is_some_and(|work| work.reconcile_defaults),
            "advertisement deletion must request self-healing publication"
        );
    }

    #[test]
    fn default_dfs_reactor_permit_change_requests_only_exact_recovery() {
        let root = temp_root("default-dfs-reactor-permit-change");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let local = NodeId::from_legacy_named("node_default_dfs_permit").expect("node");
        let mut reactor =
            DaemonDefaultDfsReconciliationReactor::new(&cluster_paths, &local).expect("reactor");
        let startup = reactor.pending_work().expect("startup work");
        reactor.complete_default_reconciliation(&startup);
        reactor.complete_prior_process_recovery(&startup);

        let permit_key =
            sinomo_dfs::placement_authority_permit_key(&sinomo_dfs::sifs_file_system_id());
        observe_daemon_default_dfs_reconciliation(
            &reactor._projection_tx,
            &local,
            &default_dfs_reactor_test_snapshot(1, vec![(permit_key.clone(), vec![9])]),
        );
        let changed = reactor.pending_work().expect("permit change work");
        assert!(!changed.reconcile_defaults);
        assert!(changed.recover_prior_process_permit);
        assert!(
            !changed.refresh_before_recovery,
            "a committed exact-key observation is already the refresh edge"
        );
        reactor.complete_prior_process_recovery(&changed);

        // A watch channel may coalesce these two committed snapshots before
        // the reactor runs. Value-only tracking would mistake the final permit
        // for the already-recovered generation and skip exact recovery.
        observe_daemon_default_dfs_reconciliation(
            &reactor._projection_tx,
            &local,
            &default_dfs_reactor_test_snapshot(2, Vec::new()),
        );
        observe_daemon_default_dfs_reconciliation(
            &reactor._projection_tx,
            &local,
            &default_dfs_reactor_test_snapshot(3, vec![(permit_key, vec![9])]),
        );
        let reinserted = reactor
            .pending_work()
            .expect("identical reinsert is a new exact generation");
        assert!(!reinserted.reconcile_defaults);
        assert!(reinserted.recover_prior_process_permit);
        assert_eq!(
            reinserted
                .placement_authority_permit
                .as_ref()
                .map(|permit| permit.modify_revision),
            Some(KvRevision(3))
        );
    }

    #[test]
    fn daemon_dfs_metadata_authority_observer_is_scoped_to_cluster_incarnation() {
        let root = temp_root("daemon_dfs_authority_observer_cluster_incarnation");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local = NodeId::from_legacy_named("node_dfs_authority_incarnation").expect("local");
        let old_cluster =
            sinomo_core::Id::from_legacy_named("cluster_dfs_authority_old").expect("old cluster");
        let new_cluster =
            sinomo_core::Id::from_legacy_named("cluster_dfs_authority_new").expect("new cluster");
        let shard_key = metadata_shard_key(&sinomo_dfs::sifs_file_system_id(), "root");
        let snapshot = |revision: u64, value: &[u8]| KvSnapshot {
            revision: KvRevision(revision),
            compacted_revision: KvRevision::ZERO,
            entries: vec![KvEntry {
                key: shard_key.clone(),
                value: value.to_vec(),
                create_revision: KvRevision(revision),
                modify_revision: KvRevision(revision),
                version: sinomo_core::kv::KvVersion(1),
                lease_id: None,
            }],
            leases: Vec::new(),
            events: vec![sinomo_core::kv::KvEvent {
                revision: KvRevision(revision),
                key: shard_key.clone(),
                kind: sinomo_core::kv::KvEventKind::Put {
                    value: value.to_vec(),
                },
            }],
            committed_requests: Vec::new(),
            expired_requests: Vec::new(),
        };

        let old_runtime =
            daemon_dfs_detached_send_runtime_for_identity(&data_dir, &old_cluster, &local)
                .expect("old runtime");
        let old_observer = daemon_dfs_retained_metadata_authority_generation(
            old_runtime.as_ref(),
            &cluster_paths,
            &old_cluster,
            &local,
            None,
        )
        .expect("old observer");
        old_observer.observe(&snapshot(100, b"old topology"));
        let old_generation = old_observer.generation();
        old_observer.observe(&snapshot(1, b"new topology"));
        assert_eq!(
            old_observer.generation(),
            old_generation,
            "one observer must reject a stale revision"
        );

        let new_runtime =
            daemon_dfs_detached_send_runtime_for_identity(&data_dir, &new_cluster, &local)
                .expect("new runtime");
        let new_observer = daemon_dfs_retained_metadata_authority_generation(
            new_runtime.as_ref(),
            &cluster_paths,
            &new_cluster,
            &local,
            None,
        )
        .expect("new observer");
        assert!(
            !Arc::ptr_eq(&old_observer, &new_observer),
            "a replacement cluster on the same data root needs a fresh revision domain"
        );
        let new_generation = new_observer.generation();
        new_observer.observe(&snapshot(1, b"new topology"));
        assert_eq!(
            new_observer.generation(),
            new_generation + 1,
            "the replacement cluster must accept its lower starting revision"
        );
    }

    #[test]
    fn daemon_dfs_message_cache_rebuilds_only_for_metadata_authority_changes() {
        let root = temp_root("daemon_dfs_message_authority_cache");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local = NodeId::from_legacy_named("node_dfs_authority_cache_local").expect("local");
        let remote = NodeId::from_legacy_named("node_dfs_authority_cache_remote").expect("remote");
        write_test_cluster_state_with_remote_endpoint(
            &data_root,
            &local,
            &remote,
            "10.78.0.1:21353",
        );
        append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Op(sinomo_core::kv::KvOp::Put {
                key: "/cluster/authority-cache-unrelated".to_string(),
                value: b"initial".to_vec(),
                lease_id: None,
            }),
        )
        .expect("initial unrelated KV");
        let runtime = daemon_test_runtime("daemon-dfs-message-authority-cache");
        let cache_key = data_dir.root();

        handle_daemon_dfs_message_on_runtime(
            &paths,
            test_dfs_message_command(
                "dfs_message_authority_cache_1",
                test_dfs_lookup_routed_message(&remote, &local, 1, "/services/cache-1.txt"),
            ),
            runtime.handle(),
        )
        .expect("first DFS message");
        let first_lane = {
            let cache = daemon_dfs_message_service_cache()
                .lock()
                .expect("cache lock");
            Arc::clone(
                cache
                    .get(&cache_key)
                    .expect("cache entry")
                    .lightweight_read_service
                    .as_ref()
                    .expect("lightweight read lane"),
            )
        };
        let first_lane_weak = Arc::downgrade(&first_lane);

        append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Op(sinomo_core::kv::KvOp::Put {
                key: "/cluster/authority-cache-unrelated".to_string(),
                value: b"changed".to_vec(),
                lease_id: None,
            }),
        )
        .expect("unrelated KV churn");
        handle_daemon_dfs_message_on_runtime(
            &paths,
            test_dfs_message_command(
                "dfs_message_authority_cache_2",
                test_dfs_lookup_routed_message(&remote, &local, 2, "/services/cache-2.txt"),
            ),
            runtime.handle(),
        )
        .expect("second DFS message");
        let unchanged_lane = {
            let cache = daemon_dfs_message_service_cache()
                .lock()
                .expect("cache lock");
            Arc::clone(
                cache
                    .get(&cache_key)
                    .expect("cache entry")
                    .lightweight_read_service
                    .as_ref()
                    .expect("lightweight read lane"),
            )
        };
        assert!(Arc::ptr_eq(&first_lane, &unchanged_lane));

        let file_system = distributed_sifs(local.clone());
        let root_shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: local.clone(),
            replica_node_ids: vec![remote.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &local,
            sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
                compares: Vec::new(),
                success: vec![
                    sinomo_core::kv::KvOp::put_serde(
                        file_system_config_key(&file_system.file_system_id),
                        &DfsFileSystemConfig::from(&file_system),
                        None,
                    )
                    .expect("filesystem config"),
                    sinomo_core::kv::KvOp::put_serde(
                        metadata_shard_key(&file_system.file_system_id, "root"),
                        &root_shard,
                        None,
                    )
                    .expect("root shard"),
                ],
                failure: Vec::new(),
            }),
        )
        .expect("metadata authority KV");
        handle_daemon_dfs_message_on_runtime(
            &paths,
            test_dfs_message_command(
                "dfs_message_authority_cache_3",
                test_dfs_lookup_routed_message(&remote, &local, 3, "/services/cache-3.txt"),
            ),
            runtime.handle(),
        )
        .expect("third DFS message");
        let rebuilt_lane = {
            let cache = daemon_dfs_message_service_cache()
                .lock()
                .expect("cache lock");
            Arc::clone(
                cache
                    .get(&cache_key)
                    .expect("cache entry")
                    .lightweight_read_service
                    .as_ref()
                    .expect("lightweight read lane"),
            )
        };
        assert!(!Arc::ptr_eq(&first_lane, &rebuilt_lane));
        drop(unchanged_lane);
        drop(first_lane);
        assert!(
            first_lane_weak.upgrade().is_none(),
            "a completed superseded service generation must be collectible"
        );
    }

    #[test]
    fn daemon_dfs_prepared_deferred_worker_does_not_retain_service_lane() {
        let root = temp_root("daemon_dfs_detached_worker_releases_lane");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local = NodeId::from_legacy_named("node_dfs_detached_worker_local").expect("local");
        write_test_cluster_state(&data_root, &local);
        let runtime = daemon_test_runtime("daemon-dfs-detached-worker-releases-lane");
        let lane = open_daemon_dfs_message_service_lane(
            &data_dir,
            &cluster_paths,
            &local,
            Vec::new(),
            runtime.handle(),
        )
        .expect("open message service lane");
        let weak_lane = Arc::downgrade(&lane);
        let detached_runtime = Arc::clone(&lane.detached_send_runtime);
        let worker = lane
            .lock()
            .expect("service lane")
            .service
            .detach_message_deferred_work();
        drop(lane);

        assert!(
            weak_lane.upgrade().is_none(),
            "a lightweight retry worker must not retain the metadata-heavy service lane"
        );
        assert!(!worker.has_pending().expect("inspect detached worker"));
        runtime.block_on(async {
            schedule_daemon_dfs_message_deferred_work(
                runtime.handle(),
                &cluster_paths,
                detached_runtime.clone(),
                worker,
            );
            tokio::task::yield_now().await;
            assert!(
                detached_runtime
                    .message_worker_active
                    .load(Ordering::Acquire),
                "the lightweight shared-outbox watcher remains parked for restored work"
            );
        });
    }

    #[test]
    fn daemon_dfs_deferred_worker_notifies_only_for_target_policy_changes() {
        let root = temp_root("daemon_dfs_detached_worker_policy_notifications");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local = NodeId::from_legacy_named("node_dfs_detached_policy_local").expect("local");
        let offline =
            NodeId::from_legacy_named("node_dfs_detached_policy_offline").expect("offline");
        write_test_cluster_state(&data_root, &local);
        let runtime = daemon_test_runtime("daemon-dfs-detached-worker-policy-notifications");
        let lane = open_daemon_dfs_message_service_lane(
            &data_dir,
            &cluster_paths,
            &local,
            Vec::new(),
            runtime.handle(),
        )
        .expect("open message service lane");
        let detached_runtime = Arc::clone(&lane.detached_send_runtime);
        let worker = lane
            .lock()
            .expect("service lane")
            .service
            .detach_message_deferred_work();
        let inspection_worker = worker.clone();

        runtime.block_on(async {
            let baseline_generation = inspection_worker
                .scheduling_snapshot()
                .expect("baseline detached schedule")
                .2;
            let offline_policy = BTreeSet::from([offline]);
            schedule_daemon_dfs_message_deferred_work_with_policy(
                runtime.handle(),
                Arc::clone(&detached_runtime),
                worker.clone(),
                offline_policy.clone(),
            );
            tokio::task::yield_now().await;
            assert_eq!(
                inspection_worker
                    .scheduling_snapshot()
                    .expect("initial detached policy schedule")
                    .2,
                baseline_generation,
                "starting the worker does not need a synthetic policy wake"
            );

            schedule_daemon_dfs_message_deferred_work_with_policy(
                runtime.handle(),
                Arc::clone(&detached_runtime),
                worker.clone(),
                offline_policy,
            );
            tokio::task::yield_now().await;
            assert_eq!(
                inspection_worker
                    .scheduling_snapshot()
                    .expect("unchanged detached policy schedule")
                    .2,
                baseline_generation,
                "republishing an unchanged target policy must not wake the parked worker"
            );

            schedule_daemon_dfs_message_deferred_work_with_policy(
                runtime.handle(),
                Arc::clone(&detached_runtime),
                worker,
                BTreeSet::new(),
            );
            assert_eq!(
                inspection_worker
                    .scheduling_snapshot()
                    .expect("recovered detached policy schedule")
                    .2,
                baseline_generation.wrapping_add(1),
                "an offline-to-online target policy change must wake the parked worker"
            );
        });
    }

    #[test]
    fn daemon_dfs_deferred_runtime_is_shared_across_service_lanes() {
        let root = temp_root("daemon_dfs_deferred_runtime_coalesces_lanes");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local = NodeId::from_legacy_named("node_dfs_deferred_coalesce_local").expect("local");
        write_test_cluster_state(&data_root, &local);
        let runtime = daemon_test_runtime("daemon-dfs-deferred-runtime-coalesces-lanes");
        let first_lane = open_daemon_dfs_message_service_lane(
            &data_dir,
            &cluster_paths,
            &local,
            Vec::new(),
            runtime.handle(),
        )
        .expect("open first lane");
        let second_lane = open_daemon_dfs_message_service_lane(
            &data_dir,
            &cluster_paths,
            &local,
            Vec::new(),
            runtime.handle(),
        )
        .expect("open second lane");
        assert!(Arc::ptr_eq(
            &first_lane.detached_send_runtime,
            &second_lane.detached_send_runtime
        ));
        let detached_runtime = Arc::clone(&first_lane.detached_send_runtime);
        let first_worker = first_lane
            .lock()
            .expect("first lane")
            .service
            .detach_message_deferred_work();
        let second_worker = second_lane
            .lock()
            .expect("second lane")
            .service
            .detach_message_deferred_work();
        let first_weak = Arc::downgrade(&first_lane);
        let second_weak = Arc::downgrade(&second_lane);

        runtime.block_on(async {
            schedule_daemon_dfs_message_deferred_work(
                runtime.handle(),
                &cluster_paths,
                Arc::clone(&detached_runtime),
                first_worker,
            );
            schedule_daemon_dfs_message_deferred_work(
                runtime.handle(),
                &cluster_paths,
                Arc::clone(&detached_runtime),
                second_worker,
            );
            drop(first_lane);
            drop(second_lane);
            assert!(first_weak.upgrade().is_none());
            assert!(second_weak.upgrade().is_none());
            assert_eq!(
                detached_runtime
                    .message_worker_schedule
                    .lock()
                    .expect("worker schedule")
                    .generation,
                2,
                "both service lanes must publish their wake to the shared runtime"
            );
            assert!(
                detached_runtime
                    .message_worker_active
                    .load(Ordering::Acquire),
                "one lightweight watcher must own the shared outbox"
            );
        });
    }

    #[test]
    fn dfs_service_members_include_authorized_voters_and_learners_only() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_service_members")
                .expect("cluster");
        let local = NodeId::from_legacy_named("node_dfs_service_local").expect("local");
        let voter = NodeId::from_legacy_named("node_dfs_service_voter").expect("voter");
        let learner = NodeId::from_legacy_named("node_dfs_service_learner").expect("learner");
        let non_voting =
            NodeId::from_legacy_named("node_dfs_service_non_voting").expect("non voting");
        let removed = NodeId::from_legacy_named("node_dfs_service_removed").expect("removed");
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "dfs service members",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        state
            .members
            .push(sinomo_core::ClusterMemberRecord::local_voter(
                cluster_id.clone(),
                voter.clone(),
                "voter",
                "pk-voter",
                now,
            ));
        let mut learner_member = sinomo_core::ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            learner.clone(),
            "learner",
            "pk-learner",
            now,
        );
        learner_member.role = sinomo_core::ClusterMemberRole::Learner;
        learner_member.state = sinomo_core::ClusterMemberState::Learner;
        state.members.push(learner_member);
        state
            .members
            .push(sinomo_core::ClusterMemberRecord::local_non_voting(
                cluster_id.clone(),
                non_voting.clone(),
                "shadow",
                "pk-shadow",
                now,
            ));
        let mut removed_member = sinomo_core::ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            removed.clone(),
            "removed",
            "pk-removed",
            now,
        );
        removed_member.state = sinomo_core::ClusterMemberState::Removed;
        state.members.push(removed_member);

        let service_nodes = sinomo_dfs::dfs_service_members(&state)
            .into_iter()
            .map(|member| member.node_id.clone())
            .collect::<Vec<_>>();

        assert_eq!(service_nodes, vec![local, voter, learner]);
    }

    #[test]
    fn default_cluster_sifs_covers_every_dfs_service_node() {
        let state = test_cluster_state_with_members(
            "cluster_dfs_default_sifs",
            "node_dfs_default_local",
            &["node_dfs_default_remote_a", "node_dfs_default_remote_b"],
        );
        let file_system = sinomo_dfs::default_cluster_sifs(&state);

        let expected_node_ids = sinomo_dfs::stable_dfs_service_node_ids(&state);
        assert_eq!(file_system.owner_node_id, expected_node_ids[0]);
        for node_id in expected_node_ids {
            assert!(file_system.subtree_policies.iter().any(|policy| {
                policy.path_prefix == sifs_node_files_path(&node_id)
                    && policy.policy == DfsObjectPolicy::LocalOwned
            }));
        }
        assert!(file_system.subtree_policies.iter().any(|policy| {
            policy.path_prefix == sinomo_dfs::SIFS_USERS_PATH
                && policy.policy == DfsObjectPolicy::ReplicatedEventual
        }));
        assert!(file_system.subtree_policies.iter().any(|policy| {
            policy.path_prefix == sinomo_dfs::SIFS_SERVICES_PATH
                && policy.policy == DfsObjectPolicy::ReplicatedEventual
        }));
    }

    #[test]
    fn default_root_metadata_shard_is_deterministic_and_epoch_stable() {
        let root = temp_root("default_root_metadata_shard_is_deterministic");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let state = test_cluster_state_with_members(
            "cluster_dfs_default_root_shard",
            "node_dfs_default_root_local",
            &[
                "node_dfs_default_root_remote_a",
                "node_dfs_default_root_remote_b",
            ],
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        let file_system = sinomo_dfs::default_cluster_sifs(&state);
        let expected_primary = sinomo_dfs::stable_dfs_service_node_ids(&state)[0].clone();
        let old_shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: expected_primary.clone(),
            replica_node_ids: Vec::new(),
            epoch: 9,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &state.local_node_id,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &old_shard,
                    None,
                )
                .expect("put shard op"),
            ),
        )
        .expect("write old shard");

        let runtime = daemon_test_runtime("default-dfs-bootstrap-migrates-shard-primary");
        let mut host = test_default_cluster_bootstrap_host(
            &data_dir,
            &cluster_paths,
            state.local_node_id.clone(),
            runtime.handle(),
        );
        let migrated = sinomo_dfs::default_cluster_metadata_shard_with_host(
            &mut host,
            &state,
            &file_system,
            "root",
            DfsMetadataShardAssignment::Root,
            0,
        )
        .expect("default shard");
        assert_eq!(migrated.primary_node_id, expected_primary);
        assert_eq!(migrated.epoch, 10);

        append_cluster_raft_command(
            &cluster_paths,
            &state.local_node_id,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "root"),
                    &migrated,
                    None,
                )
                .expect("put migrated op"),
            ),
        )
        .expect("write migrated shard");
        let stable = sinomo_dfs::default_cluster_metadata_shard_with_host(
            &mut host,
            &state,
            &file_system,
            "root",
            DfsMetadataShardAssignment::Root,
            0,
        )
        .expect("stable shard");
        assert_eq!(stable, migrated);
    }

    #[test]
    fn default_cluster_metadata_shards_cover_root_users_and_services() {
        let root = temp_root("default_cluster_metadata_shards_cover_sifs_paths");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let state = test_cluster_state_with_members(
            "cluster_dfs_default_path_shards",
            "node_dfs_default_path_shards_local",
            &[
                "node_dfs_default_path_shards_remote_a",
                "node_dfs_default_path_shards_remote_b",
            ],
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        let file_system = sinomo_dfs::default_cluster_sifs(&state);
        let runtime = daemon_test_runtime("default-dfs-bootstrap-default-path-shards");
        let mut host = test_default_cluster_bootstrap_host(
            &data_dir,
            &cluster_paths,
            state.local_node_id.clone(),
            runtime.handle(),
        );
        let shards =
            sinomo_dfs::default_cluster_metadata_shards_with_host(&mut host, &state, &file_system)
                .expect("metadata shards");
        let by_id = shards
            .into_iter()
            .map(|shard| (shard.shard_id.clone(), shard))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(by_id.len(), 3);
        assert!(matches!(
            by_id.get("root").expect("root shard").assignment,
            DfsMetadataShardAssignment::Root
        ));
        assert!(matches!(
            &by_id.get("services").expect("services shard").assignment,
            DfsMetadataShardAssignment::PathPrefix { prefix }
                if prefix == sinomo_dfs::SIFS_SERVICES_PATH
        ));
        assert!(matches!(
            &by_id.get("users").expect("users shard").assignment,
            DfsMetadataShardAssignment::PathPrefix { prefix }
                if prefix == sinomo_dfs::SIFS_USERS_PATH
        ));
        assert_eq!(
            by_id
                .get("services")
                .expect("services shard")
                .primary_node_id,
            by_id.get("root").expect("root shard").primary_node_id,
            "default path-prefix shards start on the root primary until explicit primary transfer is used"
        );
    }

    #[test]
    fn default_cluster_metadata_shards_replicate_to_all_service_nodes() {
        let root = temp_root("default_cluster_metadata_shards_replicate_to_all_services");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let state = test_cluster_state_with_members(
            "cluster_dfs_default_all_service_replicas",
            "node_dfs_default_all_replicas_local",
            &[
                "node_dfs_default_all_replicas_remote_a",
                "node_dfs_default_all_replicas_remote_b",
                "node_dfs_default_all_replicas_remote_c",
                "node_dfs_default_all_replicas_remote_d",
                "node_dfs_default_all_replicas_remote_e",
            ],
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        let file_system = sinomo_dfs::default_cluster_sifs(&state);
        let service_nodes = sinomo_dfs::stable_dfs_service_node_ids(&state);
        assert_eq!(service_nodes.len(), 6);
        let runtime = daemon_test_runtime("default-dfs-bootstrap-all-service-replicas");
        let mut host = test_default_cluster_bootstrap_host(
            &data_dir,
            &cluster_paths,
            state.local_node_id.clone(),
            runtime.handle(),
        );

        let shards =
            sinomo_dfs::default_cluster_metadata_shards_with_host(&mut host, &state, &file_system)
                .expect("metadata shards");

        for shard in shards {
            let expected_replicas = service_nodes
                .iter()
                .filter(|node_id| *node_id != &shard.primary_node_id)
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(
                shard.replica_node_ids, expected_replicas,
                "default shard {} should push metadata to every non-primary DFS service node",
                shard.shard_id
            );
        }
    }

    #[test]
    fn default_cluster_metadata_shard_expands_replicas_before_primary_transfer() {
        let root = temp_root("default_cluster_metadata_shard_expands_before_transfer");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let mut state = test_cluster_state_with_members(
            "cluster_dfs_default_expand_replicas",
            "node_dfs_default_expand_local",
            &[
                "node_dfs_default_expand_remote_a",
                "node_dfs_default_expand_remote_b",
                "node_dfs_default_expand_remote_c",
                "node_dfs_default_expand_remote_d",
                "node_dfs_default_expand_remote_e",
            ],
        );
        let service_nodes = sinomo_dfs::stable_dfs_service_node_ids(&state);
        assert_eq!(service_nodes.len(), 6);
        let bootstrap_primary = service_nodes[0].clone();
        let target_primary = service_nodes[1].clone();
        state.local_node_id = bootstrap_primary.clone();
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        let file_system = sinomo_dfs::default_cluster_sifs(&state);
        let old_services_shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "services".to_string(),
            primary_node_id: bootstrap_primary.clone(),
            replica_node_ids: service_nodes[2..4].to_vec(),
            epoch: 4,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: sinomo_dfs::SIFS_SERVICES_PATH.to_string(),
            },
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &state.local_node_id,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "services"),
                    &old_services_shard,
                    None,
                )
                .expect("put old services shard op"),
            ),
        )
        .expect("write old services shard");

        let runtime = daemon_test_runtime("default-dfs-bootstrap-expands-before-transfer");
        let mut host = test_default_cluster_bootstrap_host(
            &data_dir,
            &cluster_paths,
            state.local_node_id.clone(),
            runtime.handle(),
        );
        let expanded =
            sinomo_dfs::default_cluster_metadata_shards_with_host(&mut host, &state, &file_system)
                .expect("expanded shards")
                .into_iter()
                .find(|shard| shard.shard_id == "services")
                .expect("services shard");
        let expected_replicas = service_nodes
            .iter()
            .filter(|node_id| *node_id != &bootstrap_primary)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            expanded.primary_node_id, bootstrap_primary,
            "primary should not move until the candidate has replayed the current journal"
        );
        assert_eq!(expanded.replica_node_ids, expected_replicas);
        assert_eq!(expanded.epoch, old_services_shard.epoch + 1);
        assert!(
            expanded
                .replica_node_ids
                .iter()
                .any(|node_id| node_id == &target_primary),
            "the future primary must become a replica before primary transfer can become ready"
        );
    }

    #[test]
    fn default_cluster_metadata_shard_transfers_after_candidate_replays_current_journal() {
        let root = temp_root("default_cluster_metadata_shard_transfer_after_replay");
        let data_root = root.join("data");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let mut state = test_cluster_state_with_members(
            "cluster_dfs_default_path_shard_transfer",
            "node_dfs_default_transfer_local",
            &[
                "node_dfs_default_transfer_remote_a",
                "node_dfs_default_transfer_remote_b",
            ],
        );
        let service_nodes = sinomo_dfs::stable_dfs_service_node_ids(&state);
        let bootstrap_primary = service_nodes[0].clone();
        let target_primary = service_nodes[1].clone();
        state.local_node_id = bootstrap_primary.clone();
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        let file_system = sinomo_dfs::default_cluster_sifs(&state);
        let services_shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "services".to_string(),
            primary_node_id: bootstrap_primary.clone(),
            replica_node_ids: service_nodes
                .iter()
                .filter(|node_id| *node_id != &bootstrap_primary)
                .cloned()
                .collect(),
            epoch: 3,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: sinomo_dfs::SIFS_SERVICES_PATH.to_string(),
            },
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &state.local_node_id,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    metadata_shard_key(&file_system.file_system_id, "services"),
                    &services_shard,
                    None,
                )
                .expect("put services shard op"),
            ),
        )
        .expect("write services shard");
        let shard_id = DfsMetadataShardId::new("services");
        let mut backend =
            sinomo_dfs::open_local_metadata_shard_backend(&data_dir, shard_id.clone())
                .expect("services backend");
        backend.put_file_system(&file_system).expect("filesystem");

        let runtime = daemon_test_runtime("default-dfs-bootstrap-waits-for-shard-transfer");
        let mut host = test_default_cluster_bootstrap_host(
            &data_dir,
            &cluster_paths,
            state.local_node_id.clone(),
            runtime.handle(),
        );
        let waiting =
            sinomo_dfs::default_cluster_metadata_shards_with_host(&mut host, &state, &file_system)
                .expect("waiting shards")
                .into_iter()
                .find(|shard| shard.shard_id == "services")
                .expect("services shard");
        assert_eq!(
            waiting.primary_node_id, bootstrap_primary,
            "primary transfer should wait until the candidate has replayed the current journal"
        );
        assert_eq!(waiting.epoch, services_shard.epoch);

        backend
            .put_metadata_replication_cursor(&target_primary, &shard_id, 1)
            .expect("candidate replay cursor");

        let transferred =
            sinomo_dfs::default_cluster_metadata_shards_with_host(&mut host, &state, &file_system)
                .expect("transferred shards")
                .into_iter()
                .find(|shard| shard.shard_id == "services")
                .expect("services shard");
        assert_eq!(transferred.primary_node_id, target_primary);
        assert_eq!(transferred.epoch, services_shard.epoch + 1);
        assert!(
            transferred
                .replica_node_ids
                .iter()
                .any(|node_id| node_id == &state.local_node_id)
        );
    }

    #[test]
    fn default_dfs_reactor_propagates_mandatory_recovery_failure() {
        let root = temp_root("default-dfs-bootstrap-recovery-required");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let cluster_id =
            ClusterId::from_legacy_named("cluster_dfs_recovery_required").expect("cluster");
        let local = NodeId::from_legacy_named("node_dfs_recovery_required").expect("node");
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "dfs recovery required",
            local.clone(),
            "local",
            "pk-local",
            OffsetDateTime::now_utc(),
        );
        state.members[0].capabilities =
            sinomo_core::node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("sync cluster state");
        let runtime = daemon_test_runtime("default-dfs-bootstrap-recovery-required");
        let mut reactor =
            DaemonDefaultDfsReconciliationReactor::new(&cluster_paths, &local).expect("reactor");

        let error = runtime
            .block_on(reactor.reconcile_startup_until_idle_async(
                &paths,
                &DaemonRunOptions::default(),
                &cluster_id,
                &local,
                runtime.handle(),
            ))
            .expect_err("authoritative startup must propagate recovery failure");

        assert!(
            matches!(&error, DaemonError::Parse { detail } if detail.contains("without a live exclusive main-daemon data-owner lock")),
            "unexpected recovery error: {error}"
        );
    }

    #[test]
    fn default_dfs_reactor_recovers_prior_process_permit_on_multi_member_startup() {
        let root = temp_root("default-dfs-bootstrap-recovers-prior-process-permit");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id = sinomo_core::node::ClusterId::from_legacy_named(
            "cluster_dfs_bootstrap_recovers_prior_process_permit",
        )
        .expect("cluster");
        let local =
            NodeId::from_legacy_named("node_dfs_bootstrap_recovery_local").expect("local node");
        let remote =
            NodeId::from_legacy_named("node_dfs_bootstrap_recovery_remote").expect("remote node");
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "dfs bootstrap recovers prior process permit",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        state.members[0].capabilities =
            sinomo_core::node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
        state
            .members
            .push(sinomo_core::ClusterMemberRecord::local_non_voting(
                cluster_id.clone(),
                remote,
                "remote",
                "pk-remote",
                now,
            ));
        assert!(!sinomo_core::network::cluster_is_settled_solo(&state));
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("sync raft");

        let permit_key = seed_test_prior_process_placement_permit(
            &cluster_paths,
            &local,
            &cluster_id,
            "recoverable",
        );
        let file_system_id = sinomo_dfs::sifs_file_system_id();

        let data_owner_lock =
            Arc::new(DaemonDataOwnerLock::acquire(&paths).expect("main daemon data-owner lock"));
        register_daemon_dfs_data_owner_lock(&paths, &data_owner_lock)
            .expect("register main daemon data-owner lock");
        let runtime = daemon_test_runtime("default-dfs-bootstrap-recovers-prior-process-permit");
        assert!(
            inspect_dfs_placement_authority_permit(&paths, runtime.handle(), &file_system_id,)
                .expect("inspect seeded permit")
                .is_some(),
            "test must seed a decodable prior-process permit"
        );

        let mut reactor =
            DaemonDefaultDfsReconciliationReactor::new(&cluster_paths, &local).expect("reactor");
        runtime
            .block_on(reactor.reconcile_startup_until_idle_async(
                &paths,
                &DaemonRunOptions::default(),
                &cluster_id,
                &local,
                runtime.handle(),
            ))
            .expect("run multi-member default DFS lifecycle");

        assert!(
            sinomo_core::cluster::read_cluster_kv_latest_observed_entry(
                &cluster_paths,
                &permit_key,
            )
            .expect("read recovered permit")
            .is_none(),
            "multi-member lifecycle must exact-CAS clear its local prior-process permit"
        );
    }

    #[test]
    fn default_distributed_dfs_bootstrap_migrates_legacy_services_metadata_before_publish() {
        let root = temp_root("default_distributed_dfs_bootstrap_migrates_services");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_owner_lock =
            Arc::new(DaemonDataOwnerLock::acquire(&paths).expect("main daemon data-owner lock"));
        register_daemon_dfs_data_owner_lock(&paths, &data_owner_lock)
            .expect("register main daemon data-owner lock");
        let data_dir = SinomoDataDir::new(&data_root);
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id = sinomo_core::node::ClusterId::from_legacy_named(
            "cluster_dfs_bootstrap_migrate_services",
        )
        .expect("cluster");
        let local = NodeId::from_legacy_named("node_dfs_bootstrap_migrate_services").expect("node");
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "dfs bootstrap migrate services",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        state.members[0].capabilities =
            sinomo_core::node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("sync raft");
        let file_system = sinomo_dfs::default_cluster_sifs(&state);
        let mut legacy_object = distributed_sifs_bootstrap_objects(local.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        legacy_object.object_id =
            DfsObjectId::from_legacy_named("obj_legacy_services_metadata").expect("object");
        legacy_object.path = "/services/legacy.txt".to_string();
        {
            let mut legacy_root = LocalSobjMetadataBackend::new(
                DfsMetadataShardId::new("root"),
                data_dir.dfs_dir().join("metadata").join("root"),
            )
            .expect("legacy root backend");
            legacy_root
                .put_file_system(&file_system)
                .expect("filesystem");
            legacy_root
                .put_object(&legacy_object)
                .expect("legacy services object");
        }

        let runtime = daemon_test_runtime("default-dfs-bootstrap-migrates-legacy");
        runtime
            .block_on(ensure_default_distributed_dfs_async_on_runtime(
                &paths,
                &DaemonRunOptions::default(),
                true,
                runtime.handle().clone(),
            ))
            .expect("bootstrap dfs");

        let shards = sinomo_core::kv::read_cluster_kv_prefix_entries(
            &cluster_paths.kv_paths(),
            &format!(
                "{}/",
                sinomo_dfs::metadata_shards_prefix(&file_system.file_system_id)
            ),
        )
        .expect("shard kv")
        .into_iter()
        .map(|entry| entry.decode::<DfsMetadataShardConfig>())
        .collect::<Result<Vec<_>, _>>()
        .expect("decode shards");
        assert_eq!(shards.len(), 3);

        let backend =
            sinomo_dfs::open_cluster_sharded_dfs_metadata_backend(&data_dir, &cluster_paths)
                .expect("open sharded backend")
                .expect("configured backend");
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &legacy_object.path)
                .expect("migrated services path"),
            Some(legacy_object.clone())
        );
        assert_eq!(
            LocalSobjMetadataBackend::new(
                DfsMetadataShardId::new("root"),
                data_dir.dfs_dir().join("metadata").join("root"),
            )
            .expect("root shard")
            .get_object(&legacy_object.object_id)
            .expect("root object")
            .as_ref()
            .map(|object| object.kind.clone()),
            Some(DfsObjectKind::Tombstone)
        );
    }

    #[test]
    fn default_dfs_reactor_defers_recovery_failure_on_non_voter() {
        let root = temp_root("default_distributed_dfs_bootstrap_non_primary");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_dir = SinomoDataDir::new(&data_root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_bootstrap_non_primary")
                .expect("cluster");
        let mut node_ids = [
            NodeId::from_legacy_named("node_dfs_bootstrap_non_primary_a").expect("node a"),
            NodeId::from_legacy_named("node_dfs_bootstrap_non_primary_b").expect("node b"),
        ];
        node_ids.sort();
        let primary = node_ids[0].clone();
        let local = node_ids[1].clone();
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "dfs bootstrap non primary",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        state.members[0].role = sinomo_core::ClusterMemberRole::Learner;
        state.members[0].state = sinomo_core::ClusterMemberState::Learner;
        state.members[0].capabilities =
            sinomo_core::node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Learner);
        state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
        state
            .members
            .push(sinomo_core::ClusterMemberRecord::local_voter(
                cluster_id.clone(),
                primary,
                "primary",
                "pk-primary",
                now,
            ));
        let voter_capabilities =
            sinomo_core::node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
        state.members[1].capabilities = voter_capabilities.clone();
        state.members[1].allowed_capabilities = voter_capabilities;
        assert_ne!(sinomo_dfs::default_cluster_sifs_owner(&state), local);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("sync cluster state");
        let permit_key = seed_test_prior_process_placement_permit(
            &cluster_paths,
            &local,
            &cluster_id,
            "deferred",
        );

        let runtime = daemon_test_runtime("default-dfs-bootstrap-non-primary");
        let recovery_error =
            recover_prior_process_default_dfs_placement_authority(&paths, runtime.handle())
                .expect_err("test intentionally omits the main daemon data-owner lock");
        assert!(
            matches!(&recovery_error, DaemonError::Parse { detail } if detail.contains("without a live exclusive main-daemon data-owner lock")),
            "unexpected recovery error: {recovery_error}"
        );
        let mut reactor =
            DaemonDefaultDfsReconciliationReactor::new(&cluster_paths, &local).expect("reactor");
        let reactor_error = runtime
            .block_on(reactor.reconcile_pending_async(
                &paths,
                &DaemonRunOptions::default(),
                &cluster_id,
                &local,
                runtime.handle(),
            ))
            .expect_err("non-voter recovery failure remains pending for retry");
        assert!(
            matches!(&reactor_error, DaemonError::Parse { detail } if detail.contains("without a live exclusive main-daemon data-owner lock")),
            "unexpected reactor error: {reactor_error}"
        );
        assert!(
            sinomo_core::cluster::read_cluster_kv_latest_observed_entry(
                &cluster_paths,
                &permit_key,
            )
            .expect("read deferred permit")
            .is_some(),
            "failed best-effort recovery must leave placement fenced"
        );

        let backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            data_dir.dfs_dir().join("metadata").join("root"),
        )
        .expect("metadata backend");
        assert!(
            backend
                .journal_since_limited(0, 1)
                .expect("metadata journal")
                .is_empty(),
            "non-primary bootstrap must not create root-shard journal entries"
        );
    }

    #[test]
    fn daemon_dfs_smoke_commits_single_node_cluster_locally() {
        let root = temp_root("daemon_dfs_smoke_single_node");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
        let data_owner_lock =
            Arc::new(DaemonDataOwnerLock::acquire(&paths).expect("main daemon data-owner lock"));
        register_daemon_dfs_data_owner_lock(&paths, &data_owner_lock)
            .expect("register main daemon data-owner lock");
        let data_dir = SinomoDataDir::new(&data_root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let local = NodeId::from_legacy_named("node_dfs_smoke_single").expect("local node");
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_dfs_smoke_single")
                .expect("cluster");
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id,
            "dfs smoke single",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        state.members[0].capabilities =
            sinomo_core::node::main_daemon_capabilities(&sinomo_core::ClusterMemberRole::Voter);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("sync raft");
        let runtime = daemon_test_runtime("daemon-dfs-smoke-single-node");
        runtime
            .block_on(ensure_default_distributed_dfs_async_on_runtime(
                &paths,
                &DaemonRunOptions::default(),
                true,
                runtime.handle().clone(),
            ))
            .expect("bootstrap dfs");

        let report = runtime
            .block_on(run_daemon_dfs_smoke_async(&paths, runtime.handle()))
            .expect("dfs smoke");

        assert_eq!(report.requested_replica_node_ids, vec![local.clone()]);
        assert_eq!(report.committed_replica_node_ids, vec![local]);
        assert_eq!(report.attempts.len(), 1);
        assert!(report.attempts[0].committed);
        assert_eq!(
            report.attempts[0].replica_node_ids,
            report.requested_replica_node_ids
        );
        assert!(report.path.starts_with("/dfs-smoke/"));
        assert!(
            !report
                .metadata_replication
                .outcomes
                .iter()
                .any(|outcome| matches!(
                    outcome,
                    DfsMetadataJournalReplicationOutcome::MissingShard { .. }
                ))
        );
    }

    #[test]
    fn dfs_service_advertisement_uses_node_id_for_remote_members() {
        let remote =
            NodeId::from_legacy_named("node_dfs_service_node_route_remote").expect("remote");
        let advertisement = sinomo_dfs::DfsNodeAdvertisement::content(remote.clone());

        assert_eq!(advertisement.node_id, remote);
        assert!(advertisement.accepts_content);
    }

    fn write_test_cluster_state(data_root: &std::path::Path, node_id: &NodeId) {
        write_test_cluster_state_with_members(data_root, node_id, Vec::new());
    }

    fn seed_test_prior_process_placement_permit(
        cluster_paths: &ClusterPaths,
        local_node_id: &NodeId,
        cluster_id: &ClusterId,
        case: &str,
    ) -> String {
        let file_system_id = sinomo_dfs::sifs_file_system_id();
        let permit_key = format!("/dfs/filesystems/{file_system_id}/placement-authority-permit-v1");
        let permit = TestPlacementAuthorityPermitRecord {
            job_id: format!("prior-process-{case}-job"),
            holder_id: format!("prior-process-{case}-holder"),
            holder_cluster_id: Some(cluster_id.to_string()),
            holder_node_id: Some(local_node_id.clone()),
            holder_session_id: Some(format!("prior-process-{case}-session")),
        };
        append_cluster_raft_command(
            cluster_paths,
            local_node_id,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(permit_key.clone(), &permit, None)
                    .expect("encode prior-process permit"),
            ),
        )
        .expect("seed prior-process permit");
        permit_key
    }

    fn write_test_cluster_state_with_remote_endpoint(
        data_root: &std::path::Path,
        local_node_id: &NodeId,
        remote_node_id: &NodeId,
        address: &str,
    ) {
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_daemon_dfs_protocol")
                .expect("cluster");
        let remote = test_remote_cluster_member_with_endpoint(&cluster_id, remote_node_id, address);
        write_test_cluster_state_with_members(data_root, local_node_id, vec![remote]);
    }

    fn test_remote_cluster_member_with_endpoint(
        cluster_id: &sinomo_core::node::ClusterId,
        remote_node_id: &NodeId,
        address: &str,
    ) -> sinomo_core::ClusterMemberRecord {
        let now = OffsetDateTime::now_utc();
        let mut remote = sinomo_core::ClusterMemberRecord::local_non_voting(
            cluster_id.clone(),
            remote_node_id.clone(),
            "daemon dfs protocol remote",
            format!("pk-{remote_node_id}"),
            now,
        );
        remote.capabilities = sinomo_core::node::main_daemon_capabilities(&remote.role);
        remote.allowed_capabilities = remote.capabilities.clone();
        remote
            .endpoints
            .push(sinomo_core::network::daemon_lan_endpoint(
                remote_node_id,
                address.to_string(),
                5,
            ));
        remote
    }

    fn write_test_cluster_state_with_members(
        data_root: &std::path::Path,
        node_id: &NodeId,
        members: Vec<sinomo_core::ClusterMemberRecord>,
    ) {
        let data_dir = SinomoDataDir::new(data_root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_daemon_dfs_protocol")
                .expect("cluster");
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id,
            "daemon dfs protocol",
            node_id.clone(),
            "daemon dfs protocol node",
            format!("pk-{node_id}"),
            now,
        );
        state.members.extend(members);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");
    }

    fn test_cluster_state_with_members(
        cluster_name: &str,
        local_name: &str,
        remote_names: &[&str],
    ) -> sinomo_core::ClusterBootstrapState {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named(cluster_name).expect("cluster id");
        let local = NodeId::from_legacy_named(local_name).expect("local node");
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "daemon dfs test cluster",
            local,
            "local",
            "pk-local",
            now,
        );
        for remote_name in remote_names {
            let node_id = NodeId::from_legacy_named(*remote_name).expect("remote node");
            state
                .members
                .push(sinomo_core::ClusterMemberRecord::local_voter(
                    cluster_id.clone(),
                    node_id,
                    *remote_name,
                    format!("pk-{remote_name}"),
                    now,
                ));
        }
        state
    }

    fn test_endpoint(node_id: &NodeId, address: &str) -> ClusterEndpointRecord {
        ClusterEndpointRecord {
            endpoint_id: sinomo_core::network::ClusterEndpointId::derived(
                "sinomo.test.endpoint",
                format!("{}\0{address}", node_id.as_str()),
            ),
            node_id: node_id.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: address.to_string(),
            source: ClusterEndpointSource::Observed,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 10,
            last_observed_latency_ms: None,
        }
    }

    fn temp_root(name: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(&format!("sinomo-daemon-dfs-{name}"))
    }
}
