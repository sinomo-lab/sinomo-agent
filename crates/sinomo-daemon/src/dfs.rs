use crate::client::{
    send_object_rpc_session_to_target_async_with_daemon_paths_and_options,
    send_object_rpc_session_to_target_on_runtime_with_daemon_paths_and_options,
};
use crate::control::DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL;
use crate::{DaemonError, DaemonPaths, DaemonRunOptions};
use sinomo_core::ClusterNodeAvailability;
#[allow(unused_imports)]
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::kv::{
    KvCompare, KvEntry, KvError, KvOp, KvRevision, KvSnapshot, KvTxn, KvTxnResponse,
};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    ClusterConnectivityGraph, ClusterPaths, ObjectRpcSessionCall, ObjectRpcSessionResult,
    cluster_route_next_hop,
};
use sinomo_core::node::NodeId;
#[cfg(test)]
use sinomo_dfs::DfsFileSystemConfig;
#[cfg(test)]
use sinomo_dfs::{
    DaemonDfsInspectionRequest, DaemonDfsInspectionResponse, DfsMetadataJournalBatchCommand,
    DfsMetadataJournalBatchResponse, DfsMetadataJournalBatchResult, DfsMetadataJournalBatchSender,
    DfsMetadataPeerTransportError, DfsMetadataShardAssignment, DfsMetadataShardConfig,
    DfsMetadataSnapshotInstallResponse, DfsMetadataSnapshotInstallResult,
    DfsMetadataSnapshotInstallSender, DfsObjectVersionRecord, LocalContentStore,
    LocalSobjMetadataBackend, default_dfs_metadata_root, distributed_sifs,
    distributed_sifs_bootstrap_objects, file_system_config_key, metadata_shard_key,
    sifs_node_files_path,
};
use sinomo_dfs::{
    DfsDataPlaneHost, DfsDataPlaneHostError, DfsDataPlaneHostFuture, DfsDataPlaneResponse,
    DfsDefaultClusterBootstrapOptions, DfsDefaultClusterSmokeOptions, DfsFileSystemId,
    DfsMaintenanceTickOptions, DfsMessageCommand, DfsMessageExecutionScope,
    DfsMessageProcessResult, DfsMessageResponse, DfsMetadataBackend,
    DfsMetadataJournalReplicationOptions, DfsMetadataJournalReplicationReport,
    DfsMetadataPublishScheduleOptions, DfsMetadataPublishScheduleStart, DfsMetadataShardId,
    DfsMetadataShardMembershipHandoff, DfsMetadataShardStatus, DfsNodeDetachedSendState,
    DfsNodeDetachedSendWorker, DfsNodeServedMessageTransport, DfsNodeTransport,
    DfsObjectRpcSendOptions, DfsOperatorInspector, DfsOperatorRepairView, DfsPeerTransport,
    DfsPeerTransportError, DfsRepairAttemptRecord, DfsReplicaReceiptRecord, DfsWriteIntentRecord,
    DfsWriteIntentStatus, NetworkedDfsService, default_dfs_repair_attempt_snapshot_path,
    dfs_message_execution_scope, read_dfs_repair_attempt_snapshot,
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
const DAEMON_DFS_MAINTENANCE_LEASE_KEY: &str = "/dfs/runtime/maintenance-lease";
const DAEMON_DFS_MAINTENANCE_LEASE_TTL: Duration = Duration::from_secs(60);
const DAEMON_DFS_MAINTENANCE_LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(10);
const DAEMON_DFS_MAINTENANCE_LEASE_RELEASE_TIMEOUT: Duration = Duration::from_secs(5);
// The shared wall-clock expiry remains valid beyond the holder's monotonic
// authority deadline. The two margins tolerate up to 30 seconds of pairwise
// clock skew and still leave 30 seconds for an expired holder's bounded peer
// calls to unwind before an ahead claimant can take over.
const DAEMON_DFS_MAINTENANCE_LEASE_MAX_CLOCK_SKEW: Duration = Duration::from_secs(30);
const DAEMON_DFS_MAINTENANCE_LEASE_UNWIND_GRACE: Duration = Duration::from_secs(30);
const DAEMON_DFS_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET: usize = 64;
// A replica journal apply is durable WAN work. Give daemon maintenance a larger
// response budget than the library default while keeping foreground publication
// on its explicit two-second best-effort bound.
const DAEMON_DFS_METADATA_REPLICATION_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
const DAEMON_DFS_METADATA_REPLICATION_TARGET_TIMEOUT: Duration = Duration::from_secs(15);
const DAEMON_DFS_METADATA_PUBLISH_MAX_RETRY_ROUNDS: usize = 3;
const DAEMON_DFS_SMOKE_TIMEOUT: Duration = Duration::from_millis(9_500);
const DAEMON_DFS_MUTATION_SHARD_GATE_LIMIT: usize = 256;

/// Bootstrap reconciliation and full maintenance both open and recover the
/// local metadata tables. Keep those mandatory lifecycle paths single-flight
/// so startup cannot retain multiple decoded table images at once.
static DAEMON_DEFAULT_DFS_LIFECYCLE_GATES: OnceLock<
    Mutex<BTreeMap<PathBuf, Weak<tokio::sync::Mutex<()>>>>,
> = OnceLock::new();

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
    events: Vec<sinomo_dfs::NetworkedDfsServiceEvent>,
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

#[derive(Debug)]
struct DaemonDfsMetadataAuthorityGeneration {
    projection: Mutex<DaemonDfsMetadataAuthorityProjection>,
    generation: AtomicU64,
    subscription: OnceLock<sinomo_core::cluster::ClusterKvCommittedSnapshotSubscription>,
}

impl DaemonDfsMetadataAuthorityGeneration {
    fn new() -> Self {
        Self {
            projection: Mutex::new(DaemonDfsMetadataAuthorityProjection {
                revision: Some(KvRevision::ZERO),
                entries: Vec::new(),
            }),
            generation: AtomicU64::new(1),
            subscription: OnceLock::new(),
        }
    }

    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    fn observe(&self, snapshot: &KvSnapshot) {
        let entries = daemon_dfs_metadata_authority_projection(snapshot);
        let Ok(mut observed) = self.projection.lock() else {
            return;
        };
        if observed
            .revision
            .is_some_and(|revision| snapshot.revision < revision)
        {
            return;
        }
        let initialized = observed.revision.is_some();
        if initialized && observed.entries != entries {
            let _ =
                self.generation
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |generation| {
                        Some(generation.saturating_add(1))
                    });
        }
        observed.revision = Some(snapshot.revision);
        observed.entries = entries;
    }
}

#[derive(Debug)]
struct DaemonDfsMessageServiceLane {
    service: DaemonDfsMessageService,
    last_metadata_refresh: Instant,
    metadata_hydrated: bool,
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
    primary_service: Arc<DaemonDfsMessageServiceLaneHandle>,
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
static DAEMON_DFS_MUTATION_SCHEDULERS: OnceLock<
    Mutex<BTreeMap<PathBuf, Weak<DaemonDfsMutationScheduler>>>,
> = OnceLock::new();
static DAEMON_DFS_MESSAGE_EXECUTION_GATES: OnceLock<
    Mutex<BTreeMap<PathBuf, Weak<DaemonDfsMessageExecutionGate>>>,
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
static DAEMON_DFS_SMOKE_IN_FLIGHT: AtomicBool = AtomicBool::new(false);

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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct DaemonDfsPendingMaintenanceWork {
    pub pending_write_count: usize,
    pub pending_repair_count: usize,
    pub repair_attempt_count: usize,
    pub due_repair_attempt_count: usize,
    pub unattempted_repair_count: usize,
    pub next_repair_retry_unix_nanos: Option<u64>,
    pub pending_namespace_job_count: usize,
    pub metadata_replica_lag_count: usize,
    pub pending_local_membership_handoff_count: usize,
}

impl DaemonDfsPendingMaintenanceWork {
    #[cfg(test)]
    pub(crate) fn has_work(self) -> bool {
        self.pending_write_count > 0
            || self.pending_repair_count > 0
            || self.pending_namespace_job_count > 0
            || self.metadata_replica_lag_count > 0
            || self.pending_local_membership_handoff_count > 0
    }

    pub(crate) fn should_trigger_background_maintenance(self) -> bool {
        // Pending repairs can represent unreachable replicas and are expensive
        // to rescan. Trigger the short loop only when a repair is due or has
        // never been attempted; repair backoff snapshots keep unreachable
        // replicas from hot-looping.
        self.pending_write_count > 0
            || self.pending_namespace_job_count > 0
            || self.metadata_replica_lag_count > 0
            || self.pending_local_membership_handoff_count > 0
            || self.due_repair_attempt_count > 0
            || self.unattempted_repair_count > 0
    }
}

fn daemon_dfs_now_unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

fn daemon_dfs_duration_nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, Eq, PartialEq)]
struct DaemonDfsMaintenanceLeaseRecord {
    holder_node_id: NodeId,
    owner_token: String,
    #[serde(default)]
    renewal_sequence: u64,
    expires_unix_nanos: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DaemonDfsMaintenanceLeaseAcquire {
    Acquired(DaemonDfsMaintenanceLeaseRecord),
    Deferred(Option<DaemonDfsMaintenanceLeaseRecord>),
}

fn daemon_dfs_maintenance_lease_record(
    holder_node_id: NodeId,
    owner_token: String,
    now_unix_nanos: u64,
) -> DaemonDfsMaintenanceLeaseRecord {
    DaemonDfsMaintenanceLeaseRecord {
        holder_node_id,
        owner_token,
        renewal_sequence: 0,
        expires_unix_nanos: now_unix_nanos
            .saturating_add(daemon_dfs_duration_nanos(DAEMON_DFS_MAINTENANCE_LEASE_TTL))
            .saturating_add(daemon_dfs_duration_nanos(
                DAEMON_DFS_MAINTENANCE_LEASE_MAX_CLOCK_SKEW,
            ))
            .saturating_add(daemon_dfs_duration_nanos(
                DAEMON_DFS_MAINTENANCE_LEASE_UNWIND_GRACE,
            )),
    }
}

async fn acquire_daemon_dfs_maintenance_lease(
    kv: &mut dyn sinomo_dfs::AsyncDfsControlPlaneKvClient,
    holder_node_id: NodeId,
    owner_token: String,
    now_unix_nanos: u64,
) -> Result<DaemonDfsMaintenanceLeaseAcquire, KvError> {
    kv.refresh_latest_async().await?;
    let current = kv.get_async(DAEMON_DFS_MAINTENANCE_LEASE_KEY).await?;
    let compare = match current.as_ref() {
        None => KvCompare::Missing {
            key: DAEMON_DFS_MAINTENANCE_LEASE_KEY.to_string(),
        },
        Some(entry) => {
            let record =
                sinomo_core::kv::decode_kv_value::<DaemonDfsMaintenanceLeaseRecord>(&entry.value)?;
            if record.expires_unix_nanos > now_unix_nanos {
                return Ok(DaemonDfsMaintenanceLeaseAcquire::Deferred(Some(record)));
            }
            KvCompare::ValueEquals {
                key: DAEMON_DFS_MAINTENANCE_LEASE_KEY.to_string(),
                value: entry.value.clone(),
            }
        }
    };
    let record = daemon_dfs_maintenance_lease_record(holder_node_id, owner_token, now_unix_nanos);
    let response = kv
        .txn_async(KvTxn {
            compares: vec![compare],
            success: vec![KvOp::Put {
                key: DAEMON_DFS_MAINTENANCE_LEASE_KEY.to_string(),
                value: sinomo_core::kv::encode_kv_value(&record)?,
                lease_id: None,
            }],
            failure: Vec::new(),
        })
        .await?;
    if response.succeeded {
        Ok(DaemonDfsMaintenanceLeaseAcquire::Acquired(record))
    } else {
        Ok(DaemonDfsMaintenanceLeaseAcquire::Deferred(None))
    }
}

async fn renew_daemon_dfs_maintenance_lease(
    kv: &mut dyn sinomo_dfs::AsyncDfsControlPlaneKvClient,
    current: &DaemonDfsMaintenanceLeaseRecord,
    now_unix_nanos: u64,
    minimum_expiry_advance: Duration,
) -> Result<Option<DaemonDfsMaintenanceLeaseRecord>, KvError> {
    if current.expires_unix_nanos <= now_unix_nanos {
        return Ok(None);
    }
    let renewal_sequence =
        current
            .renewal_sequence
            .checked_add(1)
            .ok_or_else(|| KvError::Serialization {
                detail: "DFS maintenance lease renewal sequence exhausted".to_string(),
            })?;
    let renewed = DaemonDfsMaintenanceLeaseRecord {
        holder_node_id: current.holder_node_id.clone(),
        owner_token: current.owner_token.clone(),
        renewal_sequence,
        expires_unix_nanos: current
            .expires_unix_nanos
            .saturating_add(daemon_dfs_duration_nanos(minimum_expiry_advance))
            .max(
                now_unix_nanos
                    .saturating_add(daemon_dfs_duration_nanos(DAEMON_DFS_MAINTENANCE_LEASE_TTL))
                    .saturating_add(daemon_dfs_duration_nanos(
                        DAEMON_DFS_MAINTENANCE_LEASE_MAX_CLOCK_SKEW,
                    ))
                    .saturating_add(daemon_dfs_duration_nanos(
                        DAEMON_DFS_MAINTENANCE_LEASE_UNWIND_GRACE,
                    )),
            ),
    };
    let response = kv
        .txn_async(KvTxn {
            compares: vec![KvCompare::ValueEquals {
                key: DAEMON_DFS_MAINTENANCE_LEASE_KEY.to_string(),
                value: sinomo_core::kv::encode_kv_value(current)?,
            }],
            success: vec![KvOp::Put {
                key: DAEMON_DFS_MAINTENANCE_LEASE_KEY.to_string(),
                value: sinomo_core::kv::encode_kv_value(&renewed)?,
                lease_id: None,
            }],
            failure: Vec::new(),
        })
        .await?;
    Ok(response.succeeded.then_some(renewed))
}

async fn release_daemon_dfs_maintenance_lease(
    kv: &mut dyn sinomo_dfs::AsyncDfsControlPlaneKvClient,
    current: &DaemonDfsMaintenanceLeaseRecord,
) -> Result<bool, KvError> {
    let response = kv
        .txn_async(KvTxn {
            compares: vec![KvCompare::ValueEquals {
                key: DAEMON_DFS_MAINTENANCE_LEASE_KEY.to_string(),
                value: sinomo_core::kv::encode_kv_value(current)?,
            }],
            success: vec![KvOp::Delete {
                key: DAEMON_DFS_MAINTENANCE_LEASE_KEY.to_string(),
            }],
            failure: Vec::new(),
        })
        .await?;
    Ok(response.succeeded)
}

fn daemon_dfs_maintenance_lease_error(detail: impl Into<String>) -> DaemonError {
    DaemonError::Network {
        address: "DFS maintenance lease".to_string(),
        detail: detail.into(),
    }
}

#[derive(Debug)]
struct DaemonDfsMaintenanceLeaseRenewalState {
    client: DaemonDfsControlPlaneKvClient,
    record: DaemonDfsMaintenanceLeaseRecord,
    loss_detail: Option<String>,
}

#[derive(Clone, Debug)]
struct DaemonDfsMaintenanceLeaseAuthority {
    deadline: Instant,
    loss_detail: Option<String>,
}

#[derive(Debug)]
struct DaemonDfsMaintenanceLease {
    stop_tx: tokio::sync::oneshot::Sender<()>,
    authority_rx: tokio::sync::watch::Receiver<DaemonDfsMaintenanceLeaseAuthority>,
    renewal_task: tokio::task::JoinHandle<DaemonDfsMaintenanceLeaseRenewalState>,
}

impl DaemonDfsMaintenanceLease {
    fn authority_receiver(
        &self,
    ) -> tokio::sync::watch::Receiver<DaemonDfsMaintenanceLeaseAuthority> {
        self.authority_rx.clone()
    }

    fn ownership_loss_detail(&self) -> Option<String> {
        let authority = self.authority_rx.borrow();
        if let Some(detail) = authority.loss_detail.clone() {
            return Some(detail);
        }
        (Instant::now() >= authority.deadline).then(|| {
            "local authority deadline elapsed before DFS maintenance admission".to_string()
        })
    }

    async fn release(self) -> Result<(), DaemonError> {
        let _ = self.stop_tx.send(());
        let mut state = self.renewal_task.await.map_err(|source| {
            daemon_dfs_maintenance_lease_error(format!(
                "renewal task stopped before release: {source}"
            ))
        })?;
        let released = tokio::time::timeout(
            DAEMON_DFS_MAINTENANCE_LEASE_RELEASE_TIMEOUT,
            release_daemon_dfs_maintenance_lease(&mut state.client, &state.record),
        )
        .await
        .map_err(|_| {
            daemon_dfs_maintenance_lease_error(format!(
                "timed out releasing owner {} held by {}",
                state.record.owner_token, state.record.holder_node_id
            ))
        })?
        .map_err(|source| {
            daemon_dfs_maintenance_lease_error(format!(
                "failed to release owner {} held by {}: {source}",
                state.record.owner_token, state.record.holder_node_id
            ))
        })?;
        if let Some(detail) = state.loss_detail {
            return Err(daemon_dfs_maintenance_lease_error(detail));
        }
        if !released {
            return Err(daemon_dfs_maintenance_lease_error(format!(
                "owner {} held by {} lost ownership before release",
                state.record.owner_token, state.record.holder_node_id
            )));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct DaemonDfsMaintenanceAdmission {
    lease: DaemonDfsMaintenanceLease,
    _lifecycle_guard: tokio::sync::OwnedMutexGuard<()>,
    _message_execution_guard: tokio::sync::OwnedSemaphorePermit,
}

#[derive(Debug)]
struct DaemonDfsMaintenanceLocalAdmission {
    lifecycle_guard: tokio::sync::OwnedMutexGuard<()>,
    message_execution_guard: tokio::sync::OwnedSemaphorePermit,
}

async fn wait_for_daemon_dfs_maintenance_lease_loss(
    authority_rx: &mut tokio::sync::watch::Receiver<DaemonDfsMaintenanceLeaseAuthority>,
) -> String {
    loop {
        let authority = authority_rx.borrow().clone();
        if let Some(detail) = authority.loss_detail {
            return detail;
        }
        if Instant::now() >= authority.deadline {
            return "local authority deadline elapsed while waiting for DFS maintenance lease"
                .to_string();
        }
        let authority_timeout =
            tokio::time::sleep_until(tokio::time::Instant::from_std(authority.deadline));
        tokio::pin!(authority_timeout);
        tokio::select! {
            biased;
            _ = &mut authority_timeout => {
                return "local authority deadline elapsed while waiting for DFS maintenance lease"
                    .to_string();
            }
            changed = authority_rx.changed() => {
                if changed.is_err() {
                    return "renewal task stopped without preserving lease ownership".to_string();
                }
            }
        }
    }
}

fn daemon_dfs_maintenance_admission_loss(
    authority_rx: &tokio::sync::watch::Receiver<DaemonDfsMaintenanceLeaseAuthority>,
) -> Option<String> {
    let authority = authority_rx.borrow();
    authority.loss_detail.clone().or_else(|| {
        (Instant::now() >= authority.deadline).then(|| {
            "local authority deadline elapsed during DFS maintenance admission".to_string()
        })
    })
}

async fn run_daemon_dfs_maintenance_lease_renewal(
    mut client: DaemonDfsControlPlaneKvClient,
    mut record: DaemonDfsMaintenanceLeaseRecord,
    mut authority_started: Instant,
    mut authority_deadline: Instant,
    mut stop_rx: tokio::sync::oneshot::Receiver<()>,
    authority_tx: tokio::sync::watch::Sender<DaemonDfsMaintenanceLeaseAuthority>,
) -> DaemonDfsMaintenanceLeaseRenewalState {
    let loss_detail = loop {
        let renew_at = Instant::now()
            .checked_add(DAEMON_DFS_MAINTENANCE_LEASE_RENEW_INTERVAL)
            .unwrap_or(authority_deadline)
            .min(authority_deadline);
        let renew_sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(renew_at));
        tokio::pin!(renew_sleep);
        tokio::select! {
            biased;
            _ = &mut stop_rx => break None,
            _ = &mut renew_sleep => {}
        }
        if Instant::now() >= authority_deadline {
            let detail = format!(
                "owner {} held by {} reached its local authority deadline",
                record.owner_token, record.holder_node_id
            );
            authority_tx.send_replace(DaemonDfsMaintenanceLeaseAuthority {
                deadline: authority_deadline,
                loss_detail: Some(detail.clone()),
            });
            break Some(detail);
        }
        let renewal_started = Instant::now();
        let minimum_expiry_advance = renewal_started.saturating_duration_since(authority_started);
        let renewal_result = {
            let renewal = renew_daemon_dfs_maintenance_lease(
                &mut client,
                &record,
                daemon_dfs_now_unix_nanos(),
                minimum_expiry_advance,
            );
            tokio::pin!(renewal);
            let authority_timeout =
                tokio::time::sleep_until(tokio::time::Instant::from_std(authority_deadline));
            tokio::pin!(authority_timeout);
            tokio::select! {
                biased;
                _ = &mut stop_rx => break None,
                _ = &mut authority_timeout => {
                    let detail = format!(
                        "owner {} held by {} reached its local authority deadline while renewing",
                        record.owner_token, record.holder_node_id
                    );
                    authority_tx.send_replace(DaemonDfsMaintenanceLeaseAuthority {
                        deadline: authority_deadline,
                        loss_detail: Some(detail.clone()),
                    });
                    break Some(detail);
                }
                result = &mut renewal => result,
            }
        };
        match renewal_result {
            Ok(Some(renewed)) => {
                record = renewed;
                authority_started = renewal_started;
                authority_deadline = renewal_started
                    .checked_add(DAEMON_DFS_MAINTENANCE_LEASE_TTL)
                    .unwrap_or(renewal_started);
                authority_tx.send_replace(DaemonDfsMaintenanceLeaseAuthority {
                    deadline: authority_deadline,
                    loss_detail: None,
                });
            }
            Ok(None) => {
                let detail = format!(
                    "owner {} held by {} expired or lost its compare-and-swap",
                    record.owner_token, record.holder_node_id
                );
                authority_tx.send_replace(DaemonDfsMaintenanceLeaseAuthority {
                    deadline: authority_deadline,
                    loss_detail: Some(detail.clone()),
                });
                break Some(detail);
            }
            Err(source) => {
                let detail = format!(
                    "owner {} held by {} could not renew: {source}",
                    record.owner_token, record.holder_node_id
                );
                authority_tx.send_replace(DaemonDfsMaintenanceLeaseAuthority {
                    deadline: authority_deadline,
                    loss_detail: Some(detail.clone()),
                });
                break Some(detail);
            }
        }
    };
    DaemonDfsMaintenanceLeaseRenewalState {
        client,
        record,
        loss_detail,
    }
}

fn start_daemon_dfs_maintenance_lease(
    client: DaemonDfsControlPlaneKvClient,
    record: DaemonDfsMaintenanceLeaseRecord,
    authority_started: Instant,
) -> DaemonDfsMaintenanceLease {
    let authority_deadline = authority_started
        .checked_add(DAEMON_DFS_MAINTENANCE_LEASE_TTL)
        .unwrap_or(authority_started);
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let (authority_tx, authority_rx) =
        tokio::sync::watch::channel(DaemonDfsMaintenanceLeaseAuthority {
            deadline: authority_deadline,
            loss_detail: None,
        });
    let renewal_task = tokio::spawn(run_daemon_dfs_maintenance_lease_renewal(
        client,
        record,
        authority_started,
        authority_deadline,
        stop_rx,
        authority_tx,
    ));
    DaemonDfsMaintenanceLease {
        stop_tx,
        authority_rx,
        renewal_task,
    }
}

async fn acquire_daemon_dfs_maintenance_admission_in_order<Acquire, AcquireFuture, StartLease>(
    data_dir: &SinomoDataDir,
    acquire_cluster_lease: Acquire,
    start_lease: StartLease,
) -> Result<Option<DaemonDfsMaintenanceLocalAdmission>, DaemonError>
where
    Acquire: FnOnce() -> AcquireFuture,
    AcquireFuture:
        std::future::Future<Output = Result<DaemonDfsMaintenanceLeaseAcquire, DaemonError>>,
    StartLease: FnOnce(
        DaemonDfsMaintenanceLeaseRecord,
    ) -> tokio::sync::watch::Receiver<DaemonDfsMaintenanceLeaseAuthority>,
{
    let record = match acquire_cluster_lease().await? {
        DaemonDfsMaintenanceLeaseAcquire::Acquired(record) => record,
        DaemonDfsMaintenanceLeaseAcquire::Deferred(holder) => {
            if let Some(holder) = holder {
                tracing::debug!(
                    target: "sinomo.dfs.maintenance",
                    holder_node_id = %holder.holder_node_id,
                    owner_token = %holder.owner_token,
                    expires_unix_nanos = holder.expires_unix_nanos,
                    "deferred DFS maintenance to the cluster lease holder"
                );
            } else {
                tracing::debug!(
                    target: "sinomo.dfs.maintenance",
                    "deferred DFS maintenance after losing cluster lease acquisition CAS"
                );
            }
            return Ok(None);
        }
    };

    // Start renewal immediately after cluster admission, before either local
    // gate can wait behind an already-running foreground generation.
    let mut admission_authority_rx = start_lease(record);
    if let Some(detail) = daemon_dfs_maintenance_admission_loss(&admission_authority_rx) {
        return Err(daemon_dfs_maintenance_lease_error(detail));
    }
    let lifecycle_gate = daemon_default_dfs_lifecycle_gate(&data_dir.root())?;
    let message_execution_gate = daemon_dfs_message_execution_gate(data_dir)?;
    let lifecycle_guard = tokio::select! {
        biased;
        detail = wait_for_daemon_dfs_maintenance_lease_loss(&mut admission_authority_rx) => {
            return Err(daemon_dfs_maintenance_lease_error(detail));
        }
        guard = lifecycle_gate.lock_owned() => guard,
    };
    let message_execution_guard = tokio::select! {
        biased;
        detail = wait_for_daemon_dfs_maintenance_lease_loss(&mut admission_authority_rx) => {
            drop(lifecycle_guard);
            return Err(daemon_dfs_maintenance_lease_error(detail));
        }
        result = Arc::clone(&message_execution_gate.semaphore).acquire_owned() => result.map_err(|_| DaemonError::Network {
            address: "DFS message execution".to_string(),
            detail: "DFS message execution gate closed".to_string(),
        })?,
    };
    if let Some(detail) = daemon_dfs_maintenance_admission_loss(&admission_authority_rx) {
        drop(message_execution_guard);
        drop(lifecycle_guard);
        return Err(daemon_dfs_maintenance_lease_error(detail));
    }
    Ok(Some(DaemonDfsMaintenanceLocalAdmission {
        lifecycle_guard,
        message_execution_guard,
    }))
}

async fn acquire_daemon_dfs_maintenance_admission(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    runtime: &tokio::runtime::Handle,
    local_node_id: NodeId,
) -> Result<Option<DaemonDfsMaintenanceAdmission>, DaemonError> {
    let mut client = DaemonDfsControlPlaneKvClient::new_on_runtime(cluster_paths, runtime);
    let renewal_client = client.clone();
    let owner_token = sinomo_core::Id::generate().to_string();
    let acquisition_started = Instant::now();
    let mut lease = None;
    let local_admission = acquire_daemon_dfs_maintenance_admission_in_order(
        data_dir,
        || async {
            acquire_daemon_dfs_maintenance_lease(
                &mut client,
                local_node_id,
                owner_token,
                daemon_dfs_now_unix_nanos(),
            )
            .await
            .map_err(|source| {
                daemon_dfs_maintenance_lease_error(format!(
                    "failed to acquire cluster lease: {source}"
                ))
            })
        },
        |record| {
            let started =
                start_daemon_dfs_maintenance_lease(renewal_client, record, acquisition_started);
            let authority_rx = started.authority_receiver();
            lease = Some(started);
            authority_rx
        },
    )
    .await;
    let local_admission = match local_admission {
        Ok(local_admission) => local_admission,
        Err(error) => {
            if let Some(lease) = lease {
                let _ = lease.release().await;
            }
            return Err(error);
        }
    };
    let Some(local_admission) = local_admission else {
        return Ok(None);
    };
    Ok(Some(DaemonDfsMaintenanceAdmission {
        lease: lease.expect("cluster admission starts DFS maintenance lease"),
        _lifecycle_guard: local_admission.lifecycle_guard,
        _message_execution_guard: local_admission.message_execution_guard,
    }))
}

#[derive(Clone, Debug)]
struct DaemonDfsControlPlaneKvClient {
    cluster_paths: ClusterPaths,
    runtime: tokio::runtime::Handle,
}

impl DaemonDfsControlPlaneKvClient {
    fn new_on_runtime(cluster_paths: &ClusterPaths, runtime: &tokio::runtime::Handle) -> Self {
        Self {
            cluster_paths: cluster_paths.clone(),
            runtime: runtime.clone(),
        }
    }

    fn daemon_error(&self, source: DaemonError) -> KvError {
        KvError::Io {
            path: self.cluster_paths.kv_paths().snapshot.display().to_string(),
            detail: source.to_string(),
        }
    }
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
        .map(drop)
        .map_err(|source| self.daemon_error(source))
    }

    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        sinomo_core::cluster::read_cluster_kv_latest_observed_entry(&self.cluster_paths, key)
            .map_err(|source| self.daemon_error(source.into()))
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        sinomo_core::cluster::read_cluster_kv_latest_observed_prefix_entries(
            &self.cluster_paths,
            prefix,
        )
        .map_err(|source| self.daemon_error(source.into()))
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
            .map(drop)
            .map_err(|source| self.daemon_error(source))
        })
    }

    fn get_async<'a>(
        &'a self,
        key: &'a str,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, Option<KvEntry>> {
        Box::pin(async move {
            sinomo_core::cluster::read_cluster_kv_latest_observed_entry(&self.cluster_paths, key)
                .map_err(|source| self.daemon_error(source.into()))
        })
    }

    fn range_prefix_async<'a>(
        &'a self,
        prefix: &'a str,
    ) -> sinomo_dfs::DfsControlPlaneKvFuture<'a, Vec<KvEntry>> {
        Box::pin(async move {
            sinomo_core::cluster::read_cluster_kv_latest_observed_prefix_entries(
                &self.cluster_paths,
                prefix,
            )
            .map_err(|source| self.daemon_error(source.into()))
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
        traceparent: Option<String>,
    ) -> DfsDataPlaneHostFuture<'a, DfsDataPlaneResponse> {
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            handle_daemon_dfs_message_async(&paths, command, traceparent, &runtime)
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
        _call: ObjectRpcSessionCall,
        _options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcSessionResult, DfsPeerTransportError> {
        Err(daemon_dfs_unavailable_peer_transport_error())
    }
}

impl sinomo_dfs::AsyncDfsPeerTransport for DaemonDfsUnavailablePeerTransport {
    fn call_object_rpc_async<'a>(
        &'a self,
        _target_node_id: &'a NodeId,
        _call: ObjectRpcSessionCall,
        _options: DfsObjectRpcSendOptions,
    ) -> sinomo_dfs::DfsPeerTransportFuture<'a, Result<ObjectRpcSessionResult, DfsPeerTransportError>>
    {
        Box::pin(async { Err(daemon_dfs_unavailable_peer_transport_error()) })
    }
}

impl DfsPeerTransport for DaemonDfsBlockingPeerTransport {
    fn call_object_rpc(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcSessionCall,
        options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcSessionResult, DfsPeerTransportError> {
        send_object_rpc_session_to_target_on_runtime_with_daemon_paths_and_options(
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
        call: ObjectRpcSessionCall,
        options: DfsObjectRpcSendOptions,
    ) -> sinomo_dfs::DfsPeerTransportFuture<'a, Result<ObjectRpcSessionResult, DfsPeerTransportError>>
    {
        let runtime = self.runtime.clone();
        let paths = self.paths.clone();
        Box::pin(async move {
            send_object_rpc_session_to_target_async_with_daemon_paths_and_options(
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

pub(crate) fn ensure_default_local_dfs_expose(paths: &DaemonPaths) -> Result<(), DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    data_dir.ensure_root_layout()?;
    let cluster_paths = ClusterPaths::new(&data_dir);
    sinomo_dfs::ensure_default_cluster_aware_dfs_expose_config(&data_dir, &cluster_paths)?;
    Ok(())
}

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
    daemon_dfs_maintenance_notify().notify_one();
}

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
    availability == &ClusterNodeAvailability::Offline
        && local_node_id != target_node_id
        // A failed direct endpoint does not make the node offline when the
        // canonical network layer still has a relay route to it.
        && cluster_route_next_hop(connectivity, local_node_id, target_node_id).is_none()
}

fn daemon_dfs_offline_node_ids(
    cluster_paths: &ClusterPaths,
    state: &sinomo_core::ClusterBootstrapState,
) -> BTreeSet<NodeId> {
    // The stored self-status of a crashed node remains Online. Reuse the
    // cluster-health availability projection so proven-unreachable connectivity
    // also marks the node Offline. The no-op probe keeps this scheduling read
    // local and bounded; Unknown nodes remain eligible (fail open).
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

fn daemon_dfs_pending_work_from_records(
    pending_write_count: usize,
    pending_repair_count: usize,
    pending_namespace_job_count: usize,
    write_intents: &[DfsWriteIntentRecord],
    receipt_records: &[DfsReplicaReceiptRecord],
    metadata_shard_statuses: &[DfsMetadataShardStatus],
    repair_views: &[DfsOperatorRepairView],
    repair_attempt_records: &[DfsRepairAttemptRecord],
    offline_node_ids: &BTreeSet<NodeId>,
    local_node_id: &NodeId,
    pending_local_membership_handoff_count: usize,
    now_unix_nanos: u64,
) -> DaemonDfsPendingMaintenanceWork {
    // Keep offline repair state durable, but do not let it select the short
    // triggered-maintenance cadence. A periodic pass still re-evaluates the
    // backlog after node status changes.
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
    let eligible_pending_write_count = active_write_intents
        .iter()
        .filter(|intent| {
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
        if !offline_node_ids.contains(&repair.missing_replica_node_id) {
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
            !record.in_flight
                && record.next_retry_unix_nanos != u64::MAX
                && record.next_retry_unix_nanos <= now_unix_nanos
        })
        .count();
    let next_repair_retry_unix_nanos = eligible_repair_attempt_records
        .iter()
        .filter(|record| !record.in_flight && record.next_retry_unix_nanos != u64::MAX)
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

    DaemonDfsPendingMaintenanceWork {
        pending_write_count: eligible_pending_write_count,
        pending_repair_count,
        repair_attempt_count: repair_attempt_records.len(),
        due_repair_attempt_count,
        unattempted_repair_count,
        next_repair_retry_unix_nanos,
        pending_namespace_job_count,
        metadata_replica_lag_count,
        pending_local_membership_handoff_count,
    }
}

pub(crate) fn daemon_dfs_pending_maintenance_work(
    paths: &DaemonPaths,
) -> Result<DaemonDfsPendingMaintenanceWork, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(DaemonDfsPendingMaintenanceWork::default());
    }
    let metadata = open_daemon_dfs_metadata_backend(&data_dir, &cluster_paths)?;
    let summary = metadata.metadata_summary()?;
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .map_err(DaemonError::from)?;
    let metadata_shard_statuses = metadata.metadata_shard_statuses()?;
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
    let write_intents = if summary.pending_write_count > 0 {
        metadata.all_intents()?
    } else {
        Vec::new()
    };
    let mut receipt_records = Vec::new();
    let mut receipt_versions = BTreeSet::new();
    for intent in write_intents.iter().filter(|intent| {
        matches!(
            &intent.status,
            DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
        )
    }) {
        if receipt_versions.insert((intent.object_id.clone(), intent.target_version.clone())) {
            receipt_records
                .extend(metadata.receipts_for_version(&intent.object_id, &intent.target_version)?);
        }
    }
    let repair_views = if summary.pending_repair_count > 0 {
        DfsOperatorInspector::new(metadata.as_ref()).repairs(None, None)?
    } else {
        Vec::new()
    };
    let offline_node_ids = daemon_dfs_offline_node_ids(&cluster_paths, &state);
    Ok(daemon_dfs_pending_work_from_records(
        summary.pending_write_count,
        summary.pending_repair_count,
        summary.pending_namespace_job_count,
        &write_intents,
        &receipt_records,
        &metadata_shard_statuses,
        &repair_views,
        &repair_attempt_records,
        &offline_node_ids,
        &state.local_node_id,
        pending_local_membership_handoff_count,
        daemon_dfs_now_unix_nanos(),
    ))
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

/// Each served-message lane owns a decoded view of every configured metadata
/// shard. Keep one foreground service generation live at a time per data root;
/// the permit moves into `spawn_blocking` so cancellation cannot let a newer
/// generation overlap work that is still running.
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
    async fn requested(&self) {
        loop {
            let notified = self.gate.foreground_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.gate.foreground_generation.load(Ordering::Acquire) != self.foreground_generation
            {
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
                    | "metadata-membership-handoffs"
                    | "metadata-membership-handoff-proofs"
            ),
            Some(shard_id),
            None,
        ) if !file_system_id.is_empty() && !shard_id.is_empty()
    )
}

fn daemon_dfs_metadata_authority_projection(snapshot: &KvSnapshot) -> Vec<(String, Vec<u8>)> {
    let mut projection = snapshot
        .entries
        .iter()
        .filter(|entry| daemon_dfs_metadata_authority_key_is_exact(&entry.key))
        .map(|entry| (entry.key.clone(), entry.value.clone()))
        .collect::<Vec<_>>();
    projection.sort_by(|left, right| left.0.cmp(&right.0));
    projection
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
    )
}

fn daemon_dfs_metadata_authority_generation_for_identity(
    cluster_paths: &ClusterPaths,
    cluster_id: &sinomo_core::Id,
    local_node_id: &NodeId,
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
        return Ok(generation);
    }

    let generation = Arc::new(DaemonDfsMetadataAuthorityGeneration::new());
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
) -> Result<Arc<DaemonDfsMetadataAuthorityGeneration>, sinomo_dfs::DfsError> {
    if let Some(generation) = detached_send_runtime.metadata_authority_generation.get() {
        return Ok(Arc::clone(generation));
    }
    let generation = daemon_dfs_metadata_authority_generation_for_identity(
        cluster_paths,
        cluster_id,
        local_node_id,
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
    runtime: Arc<DaemonDfsDetachedSendRuntime>,
    worker: DfsNodeDetachedSendWorker,
) {
    let mut schedule = match runtime.message_worker_schedule.lock() {
        Ok(schedule) => schedule,
        Err(_) => {
            tracing::warn!("sinomo daemon DFS deferred worker schedule is poisoned");
            return;
        }
    };
    schedule.generation = schedule.generation.saturating_add(1);
    schedule.latest_worker = Some(worker);
    drop(schedule);
    if runtime
        .message_worker_active
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
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
            peer_node_ids,
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
    let mut service =
        with_daemon_dfs_control_plane_on_runtime(service, cluster_paths, Some(runtime))
            .with_boxed_metadata_backend_lazy(metadata_backend)?;
    let metadata_hydrated = service.rebind_completed_sifs_metadata_shard_membership_handoffs()? > 0;
    Ok(Arc::new(DaemonDfsMessageServiceLaneHandle {
        lane: Mutex::new(DaemonDfsMessageServiceLane {
            service,
            last_metadata_refresh: Instant::now(),
            metadata_hydrated,
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
    // local guards and releases the cluster lease before waking the scheduler.
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

fn process_daemon_dfs_message_command_on_runtime(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    command: DfsMessageCommand,
    execution_scope: &DfsMessageExecutionScope,
    runtime: &tokio::runtime::Handle,
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
    let use_read_lane = matches!(execution_scope, DfsMessageExecutionScope::ReadOnly);
    let requires_full_metadata_hydrate =
        sinomo_dfs::dfs_message_requires_full_metadata_hydrate(&command);
    let request_id = command.request_id.clone();
    let source_node_id = command.routed_message.source_node_id.clone();
    let target_node_id = command.routed_message.target_node_id.clone();
    let frame_id = command.routed_message.frame_id;
    let message_id = command.routed_message.message_id;
    let message_kind = daemon_dfs_message_kind(&command.routed_message.message);
    daemon_dfs_perf_trace(format_args!(
        "dfs-message-dispatch-start request={} kind={} source={} target={} frame_id={} message_id={} read_lane={} full_hydrate_required={} ensure_ms={}",
        request_id,
        message_kind,
        source_node_id,
        target_node_id,
        frame_id,
        message_id,
        use_read_lane,
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
            )?,
        };
        let signature = DaemonDfsMessageServiceSignature {
            cluster_id: peer_snapshot.cluster_id.clone(),
            local_node_id: peer_snapshot.local_node_id.clone(),
            kv_snapshot_available: peer_snapshot.kv_snapshot_available,
            metadata_authority_generation: metadata_authority_generation.generation(),
        };
        let should_rebuild = cache.get(&cache_key).is_none_or(|entry| {
            entry.signature != signature || entry.peer_node_ids != peer_node_ids
        });
        if should_rebuild {
            // The cached lane may contain a fully hydrated all-shard view.
            // Release the superseded generation before opening its replacement
            // so an authority or topology change cannot transiently retain two
            // metadata-heavy service generations.
            drop(cache.remove(&cache_key));
            let content_store =
                sinomo_dfs::LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
            let primary_service = open_daemon_dfs_message_service_lane_with_content(
                data_dir,
                cluster_paths,
                &signature.local_node_id,
                peer_node_ids.clone(),
                runtime,
                content_store,
            )?;
            tracing::info!(
                data_root = %cache_key.display(),
                metadata_authority_generation = signature.metadata_authority_generation,
                peer_count = peer_node_ids.len(),
                "opened bounded daemon DFS message service generation"
            );
            cache.insert(
                cache_key.clone(),
                DaemonDfsMessageServiceCacheEntry {
                    signature: signature.clone(),
                    metadata_authority_generation,
                    peer_node_ids: peer_node_ids.clone(),
                    primary_service,
                },
            );
        }

        let entry = cache
            .get(&cache_key)
            .expect("daemon DFS message cache entry inserted");
        (Arc::clone(&entry.primary_service), cache_lock_wait)
    };
    let cache_select = cache_select_started.elapsed();

    let lane_lock_wait_started = Instant::now();
    let mut service_lane = service_lane_handle
        .lock()
        .map_err(|_| daemon_dfs_cache_lock_error())?;
    let lane_lock_wait = lane_lock_wait_started.elapsed();
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
    let DfsMessageProcessResult {
        response,
        attachment,
        events,
    } = service_lane.service.process_message_command(command)?;
    let deferred_work = if use_read_lane {
        None
    } else if service_lane.service.message_deferred_work_pending()? {
        Some((
            Arc::clone(&service_lane_handle.detached_send_runtime),
            service_lane.service.detach_message_deferred_work(),
        ))
    } else {
        None
    };
    let process = process_started.elapsed();
    daemon_dfs_perf_trace(format_args!(
        "dfs-message-dispatch request={} read_lane={} peer_snapshot_ms={} ensure_ms={} cache_select_ms={} cache_select_us={} cache_lock_wait_ms={} cache_lock_wait_us={} lane_lock_wait_ms={} lane_lock_wait_us={} full_hydrate={} hydrate_ms={} hydrate_us={} process_ms={} process_us={} outbound_messages={} events={} total_ms={}",
        request_id,
        use_read_lane,
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
        response.outbound_messages.len(),
        events.len(),
        trace_started.elapsed().as_millis()
    ));
    drop(service_lane);
    // Schedule detached convergence before returning across the cancellable
    // async JoinHandle boundary. A disconnected RPC drops its JoinHandle while
    // this blocking task keeps running, so deferring this wake to the caller
    // could leave restored promotion work idle until unrelated later traffic.
    if let Some((detached_runtime, worker)) = deferred_work {
        schedule_daemon_dfs_message_deferred_work(runtime, detached_runtime, worker);
    }
    Ok(Some(DaemonDfsMessageCommandProcessResult {
        response,
        attachment,
        events,
    }))
}

async fn handle_daemon_dfs_message_async(
    paths: &DaemonPaths,
    command: DfsMessageCommand,
    traceparent: Option<String>,
    runtime: &tokio::runtime::Handle,
) -> Result<DfsDataPlaneResponse, DaemonError> {
    let trace_started = Instant::now();
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let request_id = command.request_id.clone();
    let source_node_id = command.routed_message.source_node_id.clone();
    let target_node_id = command.routed_message.target_node_id.clone();
    let frame_id = command.routed_message.frame_id;
    let message_id = command.routed_message.message_id;
    let message_kind = daemon_dfs_message_kind(&command.routed_message.message);
    let execution_scope = dfs_message_execution_scope(&command);
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
    let scheduler = daemon_dfs_mutation_scheduler(&data_dir)?;
    let admission_started = Instant::now();
    // Wait for the metadata-memory bound before acquiring logical mutation
    // authority. Queued work therefore cannot hold a shard/global gate while
    // it waits for the sole backend-bearing service generation.
    let execution_gate = daemon_dfs_message_execution_gate(&data_dir)?;
    let execution_slot = acquire_daemon_dfs_foreground_message_execution(&execution_gate).await?;
    let admission = scheduler.admit(&execution_scope).await?;
    let admission_wait = admission_started.elapsed();
    let process_execution_scope = execution_scope.clone();
    let spawn_started = Instant::now();
    let blocking_queue_started = spawn_started;
    let (result, blocking_queue, blocking_work) = tokio::task::spawn_blocking(move || {
        let blocking_started = Instant::now();
        let blocking_queue = blocking_queue_started.elapsed();
        let result = sinomo_dfs::with_dfs_traceparent(process_traceparent.as_deref(), || {
            process_daemon_dfs_message_command_on_runtime(
                &process_data_dir,
                &process_cluster_paths,
                command,
                &process_execution_scope,
                &process_runtime,
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
        events,
    } = result;
    let should_publish_metadata = sinomo_dfs::dfs_events_should_publish_metadata(&events);
    if should_publish_metadata {
        notify_daemon_dfs_maintenance_work();
        let peer_transport = daemon_dfs_blocking_peer_transport_on_runtime(&data_dir, runtime);
        let async_peer_transport = daemon_dfs_async_peer_transport(&data_dir, runtime);
        sinomo_dfs::schedule_cluster_dfs_metadata_publish_after_events_with_peer_transports_on_runtime(
            runtime.clone(),
            data_dir,
            cluster_paths,
            peer_transport,
            async_peer_transport,
            &events,
            daemon_dfs_metadata_publish_schedule_options(),
            daemon_dfs_metadata_replication_options(),
            log_dfs_metadata_replication_report,
            |error| {
                tracing::warn!("sinomo daemon DFS metadata publish after message mutation failed: {error}");
            },
        );
    }
    daemon_dfs_perf_trace_with_traceparent(
        traceparent.as_deref(),
        format_args!(
            "dfs-message-handler request={} scope={} admission_wait_ms={} admission_wait_us={} blocking_queue_ms={} blocking_queue_us={} blocking_work_ms={} blocking_work_us={} spawn_blocking_ms={} spawn_blocking_us={} publish_metadata={} outbound_messages={} total_ms={}",
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
            should_publish_metadata,
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
        &execution_scope,
        runtime,
    )?;
    let Some(result) = result else {
        return Err(DaemonError::ClusterOrphan);
    };
    let DaemonDfsMessageCommandProcessResult {
        response: message_response,
        attachment: _,
        events,
    } = result;
    sinomo_dfs::run_cluster_dfs_metadata_publish_after_events_with_peer_transport(
        &data_dir,
        &cluster_paths,
        daemon_dfs_blocking_peer_transport_on_runtime(&data_dir, runtime),
        &events,
        daemon_dfs_metadata_replication_options(),
        log_dfs_metadata_replication_report,
        |error| {
            tracing::warn!(
                "sinomo daemon DFS metadata publish after message mutation failed: {error}"
            );
        },
    );
    Ok(message_response)
}

pub(crate) async fn ensure_default_distributed_dfs_async_on_runtime(
    paths: &DaemonPaths,
    options: &DaemonRunOptions,
    publish_cluster_control: bool,
    runtime: tokio::runtime::Handle,
) -> Result<(), DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let lifecycle_gate = daemon_default_dfs_lifecycle_gate(data_dir)?;
    let Ok(lifecycle_guard) = lifecycle_gate.try_lock_owned() else {
        tracing::debug!(
            target: "sinomo.dfs.maintenance",
            data_dir = %data_dir.display(),
            "coalesced default DFS reconciliation with active lifecycle pass"
        );
        return Ok(());
    };
    let message_execution_gate = daemon_dfs_message_execution_gate(&SinomoDataDir::new(data_dir))?;
    let message_execution_guard = Arc::clone(&message_execution_gate.semaphore)
        .acquire_owned()
        .await
        .map_err(|_| DaemonError::Network {
            address: "DFS message execution".to_string(),
            detail: "DFS message execution gate closed".to_string(),
        })?;
    // Own the permit inside the blocking task that polls the complete lifecycle
    // future. Cluster phase timeouts drop their JoinHandle but cannot cancel a
    // running blocking task, so synchronous metadata recovery cannot outlive
    // either guard and overlap another backend-bearing DFS path.
    let task_paths = paths.clone();
    let task_options = options.clone();
    let task_runtime = runtime.clone();
    runtime
        .spawn_blocking(move || {
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
        })?
}

async fn ensure_default_distributed_dfs_lifecycle_inner(
    paths: &DaemonPaths,
    _options: &DaemonRunOptions,
    publish_cluster_control: bool,
    runtime: tokio::runtime::Handle,
) -> Result<(), DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let control_plane = Box::new(DaemonDfsControlPlaneKvClient::new_on_runtime(
        &cluster_paths,
        &runtime,
    ));
    sinomo_dfs::ensure_default_cluster_dfs_async(
        data_dir,
        cluster_paths,
        control_plane,
        DfsDefaultClusterBootstrapOptions {
            publish_cluster_control,
            materialize_local_metadata: true,
        },
    )
    .await?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DaemonDfsMaintenanceTickOutcome {
    Completed,
    Deferred,
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
    // `maintenance` owns the broad tick's `NetworkedDfsService`. Destroy it
    // here, while the caller still owns the sole message-execution permit, so
    // foreground admission can never overlap a live maintenance generation.
    drop(maintenance);
    outcome
}

pub(crate) async fn run_daemon_dfs_maintenance_tick_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<DaemonDfsMaintenanceTickOutcome, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(DaemonDfsMaintenanceTickOutcome::Completed);
    }
    let cluster_state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .map_err(DaemonError::from)?;
    // Publish maintenance interest before cluster/local admission. A
    // foreground request that queues anywhere behind this pass can then ask it
    // to yield once its current synchronous poll reaches an await boundary.
    let foreground_preemption =
        daemon_dfs_message_execution_gate(&data_dir)?.begin_maintenance_interest();
    let Some(admission) = acquire_daemon_dfs_maintenance_admission(
        &data_dir,
        &cluster_paths,
        runtime,
        cluster_state.local_node_id.clone(),
    )
    .await?
    else {
        return Ok(DaemonDfsMaintenanceTickOutcome::Deferred);
    };
    let DaemonDfsMaintenanceAdmission {
        lease,
        _lifecycle_guard: lifecycle_guard,
        _message_execution_guard: message_execution_guard,
    } = admission;
    if let Some(detail) = lease.ownership_loss_detail() {
        drop(message_execution_guard);
        drop(lifecycle_guard);
        let _ = lease.release().await;
        return Err(daemon_dfs_maintenance_lease_error(detail));
    }
    let mut authority_rx = lease.authority_receiver();
    let mut deferred_work = None;
    let maintenance_result =
        run_daemon_dfs_maintenance_until_foreground_waits(&foreground_preemption, async {
            let mut maintenance = Box::pin(run_daemon_dfs_maintenance_tick_with_admission(
                data_dir,
                cluster_paths,
                cluster_state,
                runtime,
                &mut deferred_work,
            ));
            tokio::select! {
                biased;
                detail = wait_for_daemon_dfs_maintenance_lease_loss(&mut authority_rx) => {
                    Err(daemon_dfs_maintenance_lease_error(detail))
                },
                result = &mut maintenance => result,
            }
        })
        .await;
    // The preemption helper has already dropped the entire maintenance future
    // (and its backend-bearing service) before this permit is released. Local
    // mutation therefore remains single-generation even when a repair or
    // metadata RPC is cancelled to admit foreground work. Those sends are
    // replay-safe; return Deferred below so durable work is retried promptly.
    drop(message_execution_guard);
    drop(lifecycle_guard);
    drop(foreground_preemption);
    let lease_release_result = lease.release().await;
    if let Some((detached_runtime, worker)) = deferred_work {
        schedule_daemon_dfs_message_deferred_work(runtime, detached_runtime, worker);
    }
    lease_release_result?;
    match maintenance_result {
        DaemonDfsMaintenancePreemptionOutcome::Finished(result) => {
            result?;
            Ok(DaemonDfsMaintenanceTickOutcome::Completed)
        }
        DaemonDfsMaintenancePreemptionOutcome::ForegroundWaiting => {
            Ok(DaemonDfsMaintenanceTickOutcome::Deferred)
        }
    }
}

async fn run_daemon_dfs_maintenance_tick_with_admission(
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    cluster_state: sinomo_core::ClusterBootstrapState,
    runtime: &tokio::runtime::Handle,
    deferred_work: &mut Option<(Arc<DaemonDfsDetachedSendRuntime>, DfsNodeDetachedSendWorker)>,
) -> Result<(), DaemonError> {
    evict_daemon_dfs_message_service_cache(&data_dir)?;
    if let Some(stats) = crate::process_idle_stats() {
        stats.incr_disk_reads_dfs_metadata();
    }
    let detached_send_runtime = daemon_dfs_detached_send_runtime_for_identity(
        &data_dir,
        &cluster_state.cluster_id,
        &cluster_state.local_node_id,
    )
    .map_err(DaemonError::from)?;
    let metadata_authority_generation = daemon_dfs_retained_metadata_authority_generation(
        detached_send_runtime.as_ref(),
        &cluster_paths,
        &cluster_state.cluster_id,
        &cluster_state.local_node_id,
    )
    .map_err(DaemonError::from)?;
    let authority_generation_before_maintenance = metadata_authority_generation.generation();
    let peer_transport = daemon_dfs_blocking_peer_transport_on_runtime(&data_dir, runtime);
    let async_peer_transport = daemon_dfs_async_peer_transport(&data_dir, runtime);
    let maintenance_result =
        sinomo_dfs::run_cluster_dfs_maintenance_tick_with_async_peer_transport_and_detached_state(
            runtime,
            data_dir.clone(),
            cluster_paths.clone(),
            peer_transport,
            async_peer_transport,
            detached_send_runtime.state.clone(),
            daemon_dfs_control_plane_client_on_runtime(&cluster_paths, Some(runtime)),
            daemon_dfs_maintenance_tick_options(),
        )
        .await
        .map_err(DaemonError::from);
    match &maintenance_result {
        Ok(Some(_)) => {
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
        Err(_) => {
            // The broad tick can repair a durable intent before failing later
            // in unrelated replication or namespace work. Force the
            // independent recovery scan to close that partial-progress gap.
            detached_send_runtime
                .recovery_scan_generation
                .store(0, Ordering::Release);
        }
        Ok(None) => {}
    }
    let promotion_recovery_started = Instant::now();
    match prepare_daemon_dfs_commit_promotion_recovery(&data_dir, &cluster_paths, runtime) {
        Ok(DaemonDfsCommitPromotionRecovery {
            detached_runtime,
            worker,
            scan_result,
        }) => {
            *deferred_work = Some((detached_runtime, worker));
            if let Err(error) = scan_result {
                tracing::warn!(
                    "sinomo daemon DFS commit-promotion recovery scan remains pending; retrying on maintenance: {error}"
                );
            }
        }
        Err(error) => {
            tracing::warn!(
                "sinomo daemon DFS commit-promotion recovery remains pending; retrying on maintenance: {error}"
            );
        }
    }
    daemon_dfs_perf_trace(format_args!(
        "commit-promotion-recovery maintenance_ms={}",
        promotion_recovery_started.elapsed().as_millis()
    ));
    let report = maintenance_result?;
    let Some(report) = report else {
        return Ok(());
    };
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
    Ok(())
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
    tokio::task::spawn_blocking(move || {
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

fn daemon_dfs_maintenance_tick_options() -> DfsMaintenanceTickOptions {
    let mut options = DfsMaintenanceTickOptions::default();
    options.repair_max_jobs = DAEMON_DFS_REPAIR_MAX_JOBS_PER_TICK;
    options.metadata_replication = daemon_dfs_metadata_replication_options();
    options
}

fn daemon_dfs_metadata_replication_options() -> DfsMetadataJournalReplicationOptions {
    DfsMetadataJournalReplicationOptions {
        max_batches_per_target: DAEMON_DFS_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET,
        attempt_timeout: DAEMON_DFS_METADATA_REPLICATION_ATTEMPT_TIMEOUT,
        target_timeout: DAEMON_DFS_METADATA_REPLICATION_TARGET_TIMEOUT,
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
    }
}

fn log_dfs_metadata_replication_report(report: DfsMetadataJournalReplicationReport) {
    for line in sinomo_dfs::dfs_metadata_replication_notice_lines(&report) {
        tracing::warn!("sinomo daemon {line}");
    }
}

pub(crate) fn schedule_daemon_dfs_metadata_publish_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    traceparent: Option<String>,
) -> Result<DfsMetadataPublishScheduleStart, DaemonError> {
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let peer_transport = daemon_dfs_blocking_peer_transport_on_runtime(&data_dir, runtime);
    let async_peer_transport = daemon_dfs_async_peer_transport(&data_dir, runtime);
    let report_traceparent = traceparent.clone();
    let error_traceparent = traceparent;
    Ok(
        sinomo_dfs::schedule_cluster_dfs_metadata_publish_with_peer_transports_on_runtime(
            runtime.clone(),
            data_dir,
            cluster_paths,
            peer_transport,
            async_peer_transport,
            daemon_dfs_metadata_publish_schedule_options(),
            daemon_dfs_metadata_replication_options(),
            move |report| {
                daemon_dfs_perf_trace_with_traceparent(
                    report_traceparent.as_deref(),
                    format_args!(
                        "dfs-metadata-publish-complete outcomes={}",
                        report.outcomes.len()
                    ),
                );
                log_dfs_metadata_replication_report(report);
            },
            move |error| {
                daemon_dfs_perf_trace_with_traceparent(
                    error_traceparent.as_deref(),
                    format_args!("dfs-metadata-publish-failed error={error}"),
                );
                tracing::warn!("sinomo daemon DFS scheduled metadata publish failed: {error}");
            },
        ),
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
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let runtime = runtime.clone();
    let smoke_task = tokio::task::spawn_blocking(move || {
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
    use sinomo_core::kv::InProcessKvStore;
    use sinomo_core::network::{
        ClusterConnectivityRecord, ClusterConnectivityState, ClusterEndpointId,
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
    fn daemon_dfs_maintenance_lease_acquires_defers_renews_and_releases_with_cas() {
        let runtime = daemon_test_runtime("daemon-dfs-maintenance-lease-cas");
        runtime.block_on(async {
            let mut kv = InProcessKvStore::new();
            let first_node = NodeId::generate();
            let second_node = NodeId::generate();
            let acquired = acquire_daemon_dfs_maintenance_lease(
                &mut kv,
                first_node.clone(),
                "first-owner".to_string(),
                1_000,
            )
            .await
            .expect("acquire first lease");
            let DaemonDfsMaintenanceLeaseAcquire::Acquired(first) = acquired else {
                panic!("first lease must be acquired");
            };
            assert_eq!(
                first.expires_unix_nanos.saturating_sub(1_000),
                daemon_dfs_duration_nanos(DAEMON_DFS_MAINTENANCE_LEASE_TTL)
                    .saturating_add(daemon_dfs_duration_nanos(
                        DAEMON_DFS_MAINTENANCE_LEASE_MAX_CLOCK_SKEW,
                    ))
                    .saturating_add(daemon_dfs_duration_nanos(
                        DAEMON_DFS_MAINTENANCE_LEASE_UNWIND_GRACE,
                    ))
            );

            assert_eq!(
                acquire_daemon_dfs_maintenance_lease(
                    &mut kv,
                    second_node.clone(),
                    "second-owner".to_string(),
                    2_000,
                )
                .await
                .expect("defer second lease"),
                DaemonDfsMaintenanceLeaseAcquire::Deferred(Some(first.clone()))
            );

            let renewed = renew_daemon_dfs_maintenance_lease(
                &mut kv,
                &first,
                3_000,
                Duration::from_nanos(2_000),
            )
            .await
            .expect("renew first lease")
            .expect("first lease still owned");
            assert!(renewed.expires_unix_nanos > first.expires_unix_nanos);
            assert_eq!(renewed.renewal_sequence, first.renewal_sequence + 1);
            assert!(
                release_daemon_dfs_maintenance_lease(&mut kv, &renewed)
                    .await
                    .expect("release first lease")
            );
            assert!(matches!(
                acquire_daemon_dfs_maintenance_lease(
                    &mut kv,
                    second_node,
                    "second-owner".to_string(),
                    4_000,
                )
                .await
                .expect("acquire released lease"),
                DaemonDfsMaintenanceLeaseAcquire::Acquired(_)
            ));
        });
    }

    #[test]
    fn daemon_dfs_maintenance_lease_allows_expired_takeover_without_stale_release() {
        let runtime = daemon_test_runtime("daemon-dfs-maintenance-lease-expiry");
        runtime.block_on(async {
            let mut kv = InProcessKvStore::new();
            let first = match acquire_daemon_dfs_maintenance_lease(
                &mut kv,
                NodeId::generate(),
                "expired-owner".to_string(),
                10_000,
            )
            .await
            .expect("acquire expiring lease")
            {
                DaemonDfsMaintenanceLeaseAcquire::Acquired(record) => record,
                other => panic!("unexpected acquisition: {other:?}"),
            };
            let replacement = match acquire_daemon_dfs_maintenance_lease(
                &mut kv,
                NodeId::generate(),
                "replacement-owner".to_string(),
                first.expires_unix_nanos,
            )
            .await
            .expect("take over expired lease")
            {
                DaemonDfsMaintenanceLeaseAcquire::Acquired(record) => record,
                other => panic!("unexpected takeover: {other:?}"),
            };
            assert_ne!(replacement.owner_token, first.owner_token);
            assert!(
                !release_daemon_dfs_maintenance_lease(&mut kv, &first)
                    .await
                    .expect("stale release CAS")
            );
            assert!(
                release_daemon_dfs_maintenance_lease(&mut kv, &replacement)
                    .await
                    .expect("replacement release")
            );
        });
    }

    #[test]
    fn daemon_dfs_maintenance_lease_renewal_advances_through_backward_wall_clock_step() {
        let runtime = daemon_test_runtime("daemon-dfs-maintenance-lease-clock-regression");
        runtime.block_on(async {
            let mut kv = InProcessKvStore::new();
            let current = match acquire_daemon_dfs_maintenance_lease(
                &mut kv,
                NodeId::generate(),
                "clock-owner".to_string(),
                50_000,
            )
            .await
            .expect("acquire lease")
            {
                DaemonDfsMaintenanceLeaseAcquire::Acquired(record) => record,
                other => panic!("unexpected acquisition: {other:?}"),
            };
            let monotonic_advance = Duration::from_secs(10);
            let renewed =
                renew_daemon_dfs_maintenance_lease(&mut kv, &current, 1_000, monotonic_advance)
                    .await
                    .expect("renew after backward clock step")
                    .expect("lease remains owned");
            assert_eq!(
                renewed.expires_unix_nanos,
                current
                    .expires_unix_nanos
                    .saturating_add(daemon_dfs_duration_nanos(monotonic_advance))
            );
            assert_eq!(renewed.renewal_sequence, current.renewal_sequence + 1);
            assert!(
                release_daemon_dfs_maintenance_lease(&mut kv, &renewed)
                    .await
                    .expect("release renewed lease")
            );
        });
    }

    #[test]
    fn daemon_dfs_maintenance_lease_waiter_enforces_deadline_without_renewer_signal() {
        let runtime = daemon_test_runtime("daemon-dfs-maintenance-lease-local-deadline");
        runtime.block_on(async {
            let (_authority_tx, mut authority_rx) =
                tokio::sync::watch::channel(DaemonDfsMaintenanceLeaseAuthority {
                    deadline: Instant::now()
                        .checked_add(Duration::from_millis(10))
                        .expect("test authority deadline"),
                    loss_detail: None,
                });
            let detail = tokio::time::timeout(
                Duration::from_secs(1),
                wait_for_daemon_dfs_maintenance_lease_loss(&mut authority_rx),
            )
            .await
            .expect("local authority deadline must wake the waiter");
            assert!(detail.contains("local authority deadline elapsed"));
        });
    }

    #[test]
    fn daemon_dfs_maintenance_cluster_admission_precedes_local_message_gate() {
        let root = temp_root("daemon-dfs-maintenance-admission-order");
        let data_dir = SinomoDataDir::new(root.path());
        let lifecycle_gate =
            daemon_default_dfs_lifecycle_gate(&data_dir.root()).expect("lifecycle gate");
        let message_gate = daemon_dfs_message_execution_gate(&data_dir).expect("message gate");
        let runtime = daemon_test_runtime("daemon-dfs-maintenance-admission-order");
        runtime.block_on(async {
            let held_lifecycle_gate = lifecycle_gate.clone().lock_owned().await;
            let held_message_gate = message_gate
                .semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("hold local message gate");
            let mut kv = InProcessKvStore::new();
            let first = match acquire_daemon_dfs_maintenance_lease(
                &mut kv,
                NodeId::generate(),
                "node-a-owner".to_string(),
                100,
            )
            .await
            .expect("node A lease")
            {
                DaemonDfsMaintenanceLeaseAcquire::Acquired(record) => record,
                other => panic!("unexpected node A admission: {other:?}"),
            };

            let deferred_admission = tokio::time::timeout(
                Duration::from_millis(50),
                acquire_daemon_dfs_maintenance_admission_in_order(
                    &data_dir,
                    || async {
                        acquire_daemon_dfs_maintenance_lease(
                            &mut kv,
                            NodeId::generate(),
                            "node-b-deferred-owner".to_string(),
                            200,
                        )
                        .await
                        .map_err(DaemonError::from)
                    },
                    |_| -> tokio::sync::watch::Receiver<DaemonDfsMaintenanceLeaseAuthority> {
                        panic!("a deferred cluster lease must not start local admission")
                    },
                ),
            )
            .await
            .expect("cluster lease denial must not wait for the held local gate")
            .expect("node B admission decision");
            assert!(deferred_admission.is_none());

            assert!(
                release_daemon_dfs_maintenance_lease(&mut kv, &first)
                    .await
                    .expect("release node A lease")
            );
            drop(held_lifecycle_gate);
            let mut second = None;
            let mut admission_authority_tx = None;
            let (lease_started_tx, mut lease_started_rx) = tokio::sync::oneshot::channel();
            let mut admitted_future = Box::pin(acquire_daemon_dfs_maintenance_admission_in_order(
                &data_dir,
                || async {
                    acquire_daemon_dfs_maintenance_lease(
                        &mut kv,
                        NodeId::generate(),
                        "node-b-owner".to_string(),
                        300,
                    )
                    .await
                    .map_err(DaemonError::from)
                },
                |record| {
                    second = Some(record);
                    lease_started_tx
                        .send(())
                        .expect("signal lease renewal start");
                    let (authority_tx, authority_rx) =
                        tokio::sync::watch::channel(DaemonDfsMaintenanceLeaseAuthority {
                            deadline: Instant::now()
                                .checked_add(DAEMON_DFS_MAINTENANCE_LEASE_TTL)
                                .expect("test authority deadline"),
                            loss_detail: None,
                        });
                    admission_authority_tx = Some(authority_tx);
                    authority_rx
                },
            ));
            tokio::time::timeout(Duration::from_millis(50), async {
                tokio::select! {
                    result = &mut admitted_future => {
                        panic!("admission returned before the held message gate: {result:?}")
                    }
                    result = &mut lease_started_rx => result.expect("lease start signal"),
                }
            })
            .await
            .expect("lease renewal must start before waiting for the local message gate");
            assert!(
                tokio::time::timeout(Duration::from_millis(10), &mut admitted_future)
                    .await
                    .is_err(),
                "admission must remain blocked while the local message gate is held"
            );
            drop(held_message_gate);
            let admitted = tokio::time::timeout(Duration::from_millis(50), admitted_future)
                .await
                .expect("released local gate must admit node B")
                .expect("node B admission")
                .expect("node B admitted");
            drop(admitted);
            drop(admission_authority_tx);
            let second = second.expect("node B lease record");
            assert!(
                release_daemon_dfs_maintenance_lease(&mut kv, &second)
                    .await
                    .expect("release node B lease")
            );
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
                    metadata_shard_key(&shard.file_system_id, &shard.shard_id),
                    shard,
                    None,
                )
                .expect("metadata shard op"),
            ),
        )
        .expect("write metadata shard config");
    }

    fn daemon_test_runtime(name: &str) -> tokio::runtime::Runtime {
        crate::build_daemon_async_runtime(name).expect("test daemon runtime")
    }

    fn run_daemon_dfs_maintenance_body_for_test(
        paths: &DaemonPaths,
        runtime: &tokio::runtime::Runtime,
    ) -> Result<(), DaemonError> {
        let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let cluster_state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
            .map_err(DaemonError::from)?;
        let mut deferred_work = None;
        let result = runtime.block_on(run_daemon_dfs_maintenance_tick_with_admission(
            data_dir,
            cluster_paths,
            cluster_state,
            runtime.handle(),
            &mut deferred_work,
        ));
        if let Some((detached_runtime, worker)) = deferred_work {
            schedule_daemon_dfs_message_deferred_work(runtime.handle(), detached_runtime, worker);
        }
        result
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
    fn daemon_dfs_metadata_publish_uses_daemon_bounds() {
        assert_eq!(
            daemon_dfs_maintenance_tick_options().metadata_replication,
            daemon_dfs_metadata_replication_options()
        );
        assert_eq!(
            daemon_dfs_metadata_replication_options().max_batches_per_target,
            DAEMON_DFS_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET
        );
        assert_eq!(
            daemon_dfs_maintenance_tick_options().repair_max_jobs,
            DAEMON_DFS_REPAIR_MAX_JOBS_PER_TICK
        );
        assert_eq!(
            daemon_dfs_metadata_replication_options().attempt_timeout,
            DAEMON_DFS_METADATA_REPLICATION_ATTEMPT_TIMEOUT
        );
        assert_eq!(
            daemon_dfs_metadata_replication_options().target_timeout,
            DAEMON_DFS_METADATA_REPLICATION_TARGET_TIMEOUT
        );
        assert_eq!(
            daemon_dfs_metadata_publish_schedule_options().max_retry_rounds,
            DAEMON_DFS_METADATA_PUBLISH_MAX_RETRY_ROUNDS
        );
        assert_eq!(
            daemon_dfs_metadata_publish_schedule_options().retry_delay,
            DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL
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
        assert!(
            !daemon_dfs_pending_maintenance_work(&paths)
                .expect("pending maintenance after tick")
                .has_work()
        );
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
    fn daemon_dfs_pending_repair_backlog_in_backoff_does_not_force_triggered_loop() {
        let pending = DaemonDfsPendingMaintenanceWork {
            pending_write_count: 0,
            pending_repair_count: 8,
            repair_attempt_count: 8,
            due_repair_attempt_count: 0,
            unattempted_repair_count: 0,
            next_repair_retry_unix_nanos: Some(42),
            pending_namespace_job_count: 0,
            metadata_replica_lag_count: 0,
            pending_local_membership_handoff_count: 0,
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
        assert!(!daemon_dfs_node_is_offline_for_maintenance(
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
            &[intent],
            &[receipt],
            &[shard_status],
            &[repair],
            &[attempt],
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
            &[intent],
            &[receipt],
            &[shard_status],
            &[repair],
            &[],
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
    fn daemon_dfs_quorum_ready_write_still_triggers_when_remaining_replica_is_offline() {
        let local_node_id =
            NodeId::from_legacy_named("node_pending_quorum_local").expect("local node");
        let offline_node_id =
            NodeId::from_legacy_named("node_pending_quorum_offline").expect("offline node");
        let intent = pending_write_intent_for_test(
            "intent-pending-quorum-ready",
            sinomo_dfs::sifs_file_system_id(),
            DfsObjectId::from_legacy_named("object_pending_quorum_ready").expect("object id"),
            DfsVersion::from_legacy_named("version_pending_quorum_ready").expect("version"),
            local_node_id.clone(),
            vec![local_node_id.clone(), offline_node_id.clone()],
            1,
        );
        let receipt = pending_write_receipt_for_test(
            &intent,
            local_node_id.clone(),
            "event_pending_quorum_ready_receipt",
        );

        let pending = daemon_dfs_pending_work_from_records(
            1,
            0,
            0,
            &[intent],
            &[receipt],
            &[],
            &[],
            &[],
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
            &[intent],
            &[wrong_hash_receipt],
            &[],
            &[],
            &[],
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
            metadata_replica_lag_count: 0,
            pending_local_membership_handoff_count: 0,
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
            metadata_replica_lag_count: 0,
            pending_local_membership_handoff_count: 0,
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
            metadata_replica_lag_count: 0,
            pending_local_membership_handoff_count: 0,
        };

        assert!(pending.has_work());
        assert!(pending.should_trigger_background_maintenance());
    }

    #[test]
    fn daemon_dfs_metadata_replica_lag_forces_triggered_loop() {
        let pending = DaemonDfsPendingMaintenanceWork {
            pending_write_count: 0,
            pending_repair_count: 0,
            repair_attempt_count: 0,
            due_repair_attempt_count: 0,
            unattempted_repair_count: 0,
            next_repair_retry_unix_nanos: None,
            pending_namespace_job_count: 0,
            metadata_replica_lag_count: 1,
            pending_local_membership_handoff_count: 0,
        };

        assert!(pending.has_work());
        assert!(pending.should_trigger_background_maintenance());
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
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        append_cluster_raft_command(
            &cluster_paths,
            &node_id,
            sinomo_core::kv::KvCommand::Txn(sinomo_core::kv::KvTxn {
                compares: Vec::new(),
                success: vec![
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
        assert!(pending.should_trigger_background_maintenance());
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
            Arc::clone(&cache.get(&cache_key).expect("cache entry").primary_service)
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
            &cache.get(&cache_key).expect("cache entry").primary_service
        ));
        drop(cache);
        assert_eq!(
            daemon_dfs_test_message_service_lane_open_count(&data_dir),
            1,
            "a cold exact-shard mutation must reuse the one backend-bearing cache lane"
        );
        let weak_primary = Arc::downgrade(&primary_service);
        drop(primary_service);
        assert!(
            evict_daemon_dfs_message_service_cache(&data_dir).expect("evict message cache"),
            "mandatory maintenance must release even a lazily hydrated service lane"
        );
        assert!(
            weak_primary.upgrade().is_none(),
            "maintenance must collect the cached backend before opening its own"
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
        let primary_service = {
            let cache = daemon_dfs_message_service_cache()
                .lock()
                .expect("cache lock");
            Arc::clone(&cache.get(&cache_key).expect("cache entry").primary_service)
        };
        let first_refresh = {
            let lane = primary_service.lock().expect("primary service lock");
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
        let second_refresh = primary_service
            .lock()
            .expect("primary service lock")
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
        let third_refresh = primary_service
            .lock()
            .expect("primary service lock")
            .last_metadata_refresh;
        assert_eq!(
            third_refresh, first_refresh,
            "endpoint refreshes should not force another full metadata hydrate"
        );
        let second_primary_service = {
            let cache = daemon_dfs_message_service_cache()
                .lock()
                .expect("cache lock");
            Arc::clone(&cache.get(&cache_key).expect("cache entry").primary_service)
        };
        assert!(
            Arc::ptr_eq(&primary_service, &second_primary_service),
            "endpoint-list refreshes should reuse the service lane"
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

        let file_system_id = sinomo_dfs::sifs_file_system_id();
        let shard_key = metadata_shard_key(&file_system_id, "root");
        append(&shard_key, b"shard-v1");
        let after_shard = generation.generation();
        assert_eq!(after_shard, initial + 1);
        append(&shard_key, b"shard-v1");
        append("/dfs/filesystems/unrelated/config", b"config-churn");
        assert_eq!(generation.generation(), after_shard);

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
            events: Vec::new(),
        };

        let old_runtime =
            daemon_dfs_detached_send_runtime_for_identity(&data_dir, &old_cluster, &local)
                .expect("old runtime");
        let old_observer = daemon_dfs_retained_metadata_authority_generation(
            old_runtime.as_ref(),
            &cluster_paths,
            &old_cluster,
            &local,
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
            Arc::clone(&cache.get(&cache_key).expect("cache entry").primary_service)
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
            Arc::clone(&cache.get(&cache_key).expect("cache entry").primary_service)
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
            Arc::clone(&cache.get(&cache_key).expect("cache entry").primary_service)
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
                Arc::clone(&detached_runtime),
                first_worker,
            );
            schedule_daemon_dfs_message_deferred_work(
                runtime.handle(),
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
    fn default_cluster_metadata_shard_transfers_after_candidate_replays_certified_journal() {
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

        let replayed_but_uncertified =
            sinomo_dfs::default_cluster_metadata_shards_with_host(&mut host, &state, &file_system)
                .expect("uncertified shards")
                .into_iter()
                .find(|shard| shard.shard_id == "services")
                .expect("services shard");
        assert_eq!(
            replayed_but_uncertified.primary_node_id, bootstrap_primary,
            "a replica cursor alone must not authorize primary transfer"
        );
        assert_eq!(replayed_but_uncertified.epoch, services_shard.epoch);

        let journal_sequence = backend.journal_last_sequence();
        let prefix_proof = backend
            .metadata_prefix_proof(&shard_id)
            .expect("read exact shard prefix proof")
            .expect("services shard prefix proof");
        assert_eq!(prefix_proof.journal_sequence, journal_sequence);
        let watermark = sinomo_dfs::DfsMetadataShardHeadWatermark {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: services_shard.shard_id.clone(),
            primary_node_id: bootstrap_primary.clone(),
            epoch: services_shard.epoch,
            journal_sequence,
            state_fingerprint: prefix_proof.digest,
            acknowledged_node_ids: vec![bootstrap_primary.clone(), target_primary.clone()],
        };
        append_cluster_raft_command(
            &cluster_paths,
            &state.local_node_id,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(
                    sinomo_dfs::metadata_shard_head_watermark_key(
                        &file_system.file_system_id,
                        "services",
                    ),
                    &watermark,
                    None,
                )
                .expect("put certified shard head op"),
            ),
        )
        .expect("publish certified shard head");

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
    fn default_distributed_dfs_bootstrap_migrates_legacy_services_metadata_before_publish() {
        let root = temp_root("default_distributed_dfs_bootstrap_migrates_services");
        let data_root = root.join("data");
        let paths = DaemonPaths::with_root(data_root.join("daemon"));
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
    fn default_distributed_dfs_bootstrap_does_not_append_metadata_on_non_primary() {
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
        state
            .members
            .push(sinomo_core::ClusterMemberRecord::local_voter(
                cluster_id,
                primary,
                "primary",
                "pk-primary",
                now,
            ));
        assert_ne!(sinomo_dfs::default_cluster_sifs_owner(&state), local);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write cluster state");

        let runtime = daemon_test_runtime("default-dfs-bootstrap-non-primary");
        runtime
            .block_on(ensure_default_distributed_dfs_async_on_runtime(
                &paths,
                &DaemonRunOptions::default(),
                false,
                runtime.handle().clone(),
            ))
            .expect("bootstrap dfs");

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

    fn write_test_cluster_state_with_remote_endpoint(
        data_root: &std::path::Path,
        local_node_id: &NodeId,
        remote_node_id: &NodeId,
        address: &str,
    ) {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_daemon_dfs_protocol")
                .expect("cluster");
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
        write_test_cluster_state_with_members(data_root, local_node_id, vec![remote]);
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
