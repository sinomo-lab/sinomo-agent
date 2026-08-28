use crate::{
    DfsFileSystemId, DfsInboundAttachment, DfsMessageCommand, DfsMessageCommittedWork,
    DfsMessageResponse, DfsMetadataShardId, DfsNetworkMessage, DfsNodeServedMessageTransport,
    DfsOutboundAttachment, LocalContentStore, NetworkedDfsService,
    ResidentLocalMetadataShardHandle, ResidentLocalShardedMetadataBackend,
};
use sinomo_core::network::{ClusterPaths, ObjectRpcBodyChunk};
use sinomo_core::node::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

pub const DFS_ATTACHMENT_INGEST_BUFFERED_CHUNKS: usize = 4;

pub type DfsRuntimeMessageService = NetworkedDfsService<DfsNodeServedMessageTransport>;
pub type DfsRuntimeTask = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

static DFS_RUNTIME_MESSAGE_CACHE: OnceLock<Mutex<BTreeMap<PathBuf, DfsRuntimeMessageCacheEntry>>> =
    OnceLock::new();

static DFS_DETACHED_SEND_RUNTIMES: OnceLock<
    Mutex<BTreeMap<DfsDetachedSendRuntimeKey, Arc<DfsDetachedSendRuntime>>>,
> = OnceLock::new();

static DFS_RUNTIME_PEER_DISCOVERY_CACHE: OnceLock<
    Mutex<BTreeMap<PathBuf, DfsRuntimePeerDiscoveryCacheEntry>>,
> = OnceLock::new();

#[derive(Clone, Debug)]
struct DfsRuntimePeerDiscoveryCacheEntry {
    cluster_state: Arc<sinomo_core::ClusterBootstrapState>,
    peer_node_ids: Vec<NodeId>,
    validated_at: Instant,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DfsDetachedSendRuntimeKey {
    data_root: PathBuf,
    cluster_id: String,
    local_node_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsRuntimeMessageServiceSignature {
    pub cluster_id: sinomo_core::Id,
    pub local_node_id: NodeId,
    pub metadata_authority_generation: u64,
}

#[derive(Clone, Debug)]
pub struct DfsRuntimeMessagePeerSnapshot {
    pub cluster_id: sinomo_core::Id,
    pub local_node_id: NodeId,
    pub peer_node_ids: Vec<NodeId>,
}

/// Read the cluster-owned resident identity and retain a short-lived DFS peer
/// discovery projection. Pointer identity invalidates in-process membership
/// changes immediately; the lease bounds service-only discovery staleness.
pub fn read_dfs_runtime_message_peer_snapshot(
    cluster_paths: &ClusterPaths,
    validation_lease: Duration,
) -> Result<Option<DfsRuntimeMessagePeerSnapshot>, crate::DfsError> {
    let state =
        match sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(cluster_paths) {
            Ok(state) => state,
            Err(sinomo_core::network::NetworkError::ClusterOrphan) => return Ok(None),
            Err(error) => return Err(crate::DfsError::from_network(error)),
        };
    let cache_key = cluster_paths.root.clone();
    let cached_peer_node_ids = DFS_RUNTIME_PEER_DISCOVERY_CACHE
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .ok()
        .and_then(|cache| cache.get(&cache_key).cloned())
        .filter(|cached| {
            Arc::ptr_eq(&cached.cluster_state, &state)
                && cached.validated_at.elapsed() <= validation_lease
        })
        .map(|cached| cached.peer_node_ids);
    let peer_node_ids = match cached_peer_node_ids {
        Some(peer_node_ids) => peer_node_ids,
        None => {
            let peer_node_ids = crate::discovered_cluster_dfs_peer_node_ids(cluster_paths, &state)?;
            if let Ok(mut cache) = DFS_RUNTIME_PEER_DISCOVERY_CACHE
                .get_or_init(|| Mutex::new(BTreeMap::new()))
                .lock()
            {
                cache.insert(
                    cache_key,
                    DfsRuntimePeerDiscoveryCacheEntry {
                        cluster_state: Arc::clone(&state),
                        peer_node_ids: peer_node_ids.clone(),
                        validated_at: Instant::now(),
                    },
                );
            }
            peer_node_ids
        }
    };
    Ok(Some(DfsRuntimeMessagePeerSnapshot {
        cluster_id: state.cluster_id.clone(),
        local_node_id: state.local_node_id.clone(),
        peer_node_ids,
    }))
}

#[cfg(any(test, feature = "test-support"))]
pub fn test_expire_dfs_runtime_peer_discovery_cache(
    cluster_paths: &ClusterPaths,
    validation_lease: Duration,
) {
    if let Ok(mut cache) = DFS_RUNTIME_PEER_DISCOVERY_CACHE
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        && let Some(entry) = cache.get_mut(&cluster_paths.root)
        && let Some(validated_at) =
            Instant::now().checked_sub(validation_lease.saturating_add(Duration::from_millis(1)))
    {
        entry.validated_at = validated_at;
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn test_clear_dfs_runtime_peer_discovery_cache(cluster_paths: &ClusterPaths) {
    if let Some(cache) = DFS_RUNTIME_PEER_DISCOVERY_CACHE.get()
        && let Ok(mut cache) = cache.lock()
    {
        cache.remove(&cluster_paths.root);
    }
}

/// Process-resident durable-send state shared by every DFS message lane for
/// one local cluster identity.
///
/// The DFS owner keeps retry coalescing, recovery-generation fencing, and the
/// lightweight detached outbox watcher here. The embedding daemon contributes
/// only the current offline-target policy and a callback that spawns the
/// prepared task on its runtime.
#[derive(Debug)]
pub struct DfsDetachedSendRuntime {
    state: crate::DfsNodeDetachedSendState,
    recovery_scan_generation: AtomicU64,
    metadata_authority_generation: OnceLock<Arc<crate::DfsMetadataAuthorityRuntime>>,
    worker_active: AtomicBool,
    worker_schedule: Mutex<DfsDetachedWorkerSchedule>,
}

#[derive(Debug, Default)]
struct DfsDetachedWorkerSchedule {
    generation: u64,
    latest_worker: Option<crate::DfsNodeDetachedSendWorker>,
    deferred_target_node_ids: Option<BTreeSet<NodeId>>,
}

impl Default for DfsDetachedSendRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl DfsDetachedSendRuntime {
    pub fn new() -> Self {
        Self {
            state: crate::DfsNodeDetachedSendState::new(),
            recovery_scan_generation: AtomicU64::new(0),
            metadata_authority_generation: OnceLock::new(),
            worker_active: AtomicBool::new(false),
            worker_schedule: Mutex::new(DfsDetachedWorkerSchedule::default()),
        }
    }

    pub fn detached_state(&self) -> crate::DfsNodeDetachedSendState {
        self.state.clone()
    }

    pub fn metadata_authority_generation(&self) -> Option<Arc<crate::DfsMetadataAuthorityRuntime>> {
        self.metadata_authority_generation.get().cloned()
    }

    pub fn retain_metadata_authority_generation(
        &self,
        generation: Arc<crate::DfsMetadataAuthorityRuntime>,
    ) -> Arc<crate::DfsMetadataAuthorityRuntime> {
        let _ = self.metadata_authority_generation.set(generation);
        Arc::clone(
            self.metadata_authority_generation
                .get()
                .expect("metadata authority generation retained"),
        )
    }

    pub fn recovery_scan_generation(&self) -> u64 {
        self.recovery_scan_generation.load(Ordering::Acquire)
    }

    pub fn record_recovery_scan_generation(&self, generation: u64) {
        self.recovery_scan_generation
            .store(generation, Ordering::Release);
    }

    pub fn reset_recovery_scan_generation(&self) {
        self.record_recovery_scan_generation(0);
    }

    pub fn schedule_deferred_work<F>(
        self: &Arc<Self>,
        mut worker: crate::DfsNodeDetachedSendWorker,
        deferred_target_node_ids: BTreeSet<NodeId>,
        retry_delay: Duration,
        spawn: F,
    ) where
        F: FnOnce(DfsRuntimeTask),
    {
        worker.set_deferred_target_node_ids(deferred_target_node_ids.clone());
        let policy_wakeup = worker.clone();
        let mut schedule = match self.worker_schedule.lock() {
            Ok(schedule) => schedule,
            Err(_) => {
                tracing::warn!("DFS deferred worker schedule is poisoned");
                return;
            }
        };
        let policy_changed =
            schedule.deferred_target_node_ids.as_ref() != Some(&deferred_target_node_ids);
        schedule.deferred_target_node_ids = Some(deferred_target_node_ids);
        schedule.generation = schedule.generation.saturating_add(1);
        schedule.latest_worker = Some(worker);
        drop(schedule);
        let start_worker = self
            .worker_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        // Publish the refreshed worker before waking a parked watcher so an
        // offline-to-online transition cannot miss the policy change.
        if policy_changed && !start_worker {
            policy_wakeup.notify_scheduler_policy_change();
        }
        if start_worker {
            let runtime = Arc::clone(self);
            spawn(Box::pin(async move {
                runtime.run_deferred_worker(retry_delay).await;
            }));
        }
    }

    async fn run_deferred_worker(self: Arc<Self>, retry_delay: Duration) {
        let mut worker: Option<crate::DfsNodeDetachedSendWorker> = None;
        loop {
            let observed_generation = match self.worker_schedule.lock() {
                Ok(mut schedule) => {
                    if let Some(latest_worker) = schedule.latest_worker.take() {
                        worker = Some(latest_worker);
                    }
                    schedule.generation
                }
                Err(_) => {
                    tracing::warn!("DFS deferred worker schedule is poisoned");
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
                        tracing::warn!("DFS deferred worker task failed: {error}");
                        tokio::time::sleep(retry_delay).await;
                        continue;
                    }
                };
            if let Err(error) = result {
                tracing::warn!("DFS deferred served-message work failed; retrying: {error}");
            }
            let (ready, retry_not_before, outbox_generation) =
                match active_worker.scheduling_snapshot() {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        tracing::warn!("DFS deferred send schedule failed: {error}");
                        tokio::time::sleep(retry_delay).await;
                        continue;
                    }
                };
            let current_generation = match self.worker_schedule.lock() {
                Ok(schedule) => schedule.generation,
                Err(_) => {
                    tracing::warn!("DFS deferred worker schedule is poisoned");
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
                    tracing::warn!("DFS deferred send wait failed: {error}");
                    tokio::time::sleep(retry_delay).await;
                }
                continue;
            }
            if let Err(error) = active_worker.wait_for_change(outbox_generation).await {
                tracing::warn!("DFS deferred send wait failed: {error}");
                tokio::time::sleep(retry_delay).await;
            }
        }
        self.worker_active.store(false, Ordering::Release);
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn worker_is_active(&self) -> bool {
        self.worker_active.load(Ordering::Acquire)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn worker_schedule_generation(&self) -> u64 {
        self.worker_schedule
            .lock()
            .map(|schedule| schedule.generation)
            .unwrap_or_default()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DfsCommittedEffects {
    pub publish_metadata: bool,
    pub replica_progress: bool,
    pub repair_maintenance: bool,
    pub namespace_maintenance: bool,
}

impl DfsCommittedEffects {
    pub fn classify(
        committed_work: &DfsMessageCommittedWork,
        events: &[crate::NetworkedDfsServiceEvent],
    ) -> Self {
        Self {
            publish_metadata: committed_work.publish_metadata
                || crate::dfs_events_should_publish_metadata(events),
            replica_progress: committed_work.metadata_replica_progress
                || crate::dfs_events_advance_metadata_replica(events),
            repair_maintenance: committed_work.repair_maintenance
                || crate::dfs_events_should_wake_repair_maintenance(events),
            namespace_maintenance: committed_work.namespace_maintenance,
        }
    }
}

/// Host callbacks for effects that cross from a committed DFS mutation into
/// process scheduling. The owner classifies and orders the effects; the host
/// only supplies its local wake/publication mechanisms.
pub trait DfsCommittedEffectsHost {
    fn record_replica_progress(&self) -> bool;
    fn wake_maintenance(&self, repair: bool, namespace: bool);
    fn publish_metadata(&self);
}

pub fn emit_runtime_committed_effects(
    host: &dyn DfsCommittedEffectsHost,
    committed_work: &DfsMessageCommittedWork,
    events: &[crate::NetworkedDfsServiceEvent],
) -> DfsCommittedEffects {
    let mut effects = DfsCommittedEffects::classify(committed_work, events);
    if effects.replica_progress {
        effects.replica_progress = host.record_replica_progress();
    }
    if effects.repair_maintenance || effects.namespace_maintenance {
        host.wake_maintenance(effects.repair_maintenance, effects.namespace_maintenance);
    }
    if effects.publish_metadata {
        host.publish_metadata();
    }
    effects
}

#[derive(Debug)]
pub struct DfsRuntimeMessageResult {
    pub response: DfsMessageResponse,
    pub attachment: Option<DfsOutboundAttachment>,
    pub committed_edges: DfsCommittedEffects,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsVisibleMutationEvent {
    pub origin_node_id: NodeId,
    pub operation: &'static str,
    pub paths: Vec<String>,
}

pub fn dfs_visible_mutation_event(command: &DfsMessageCommand) -> Option<DfsVisibleMutationEvent> {
    let origin_node_id = command.routed_message.target_node_id.clone();
    match &command.routed_message.message {
        DfsNetworkMessage::NamespaceRequest(command)
            if command.operation.changes_visible_namespace() =>
        {
            Some(DfsVisibleMutationEvent {
                origin_node_id,
                operation: "namespace-operation",
                paths: command
                    .operation
                    .involved_paths()
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
            })
        }
        DfsNetworkMessage::DistributedWriteRequest(command)
        | DfsNetworkMessage::ExclusiveCreateRequest(command) => Some(DfsVisibleMutationEvent {
            origin_node_id,
            operation: "distributed-write",
            paths: command
                .object_metadata
                .as_ref()
                .map(|object| vec![object.path.clone()])
                .unwrap_or_else(|| vec!["/".to_string()]),
        }),
        _ => None,
    }
}

pub fn dfs_response_committed_visible_mutation(response: &crate::DfsDataPlaneResponse) -> bool {
    response
        .response
        .outbound_messages
        .iter()
        .any(|message| match &message.message {
            DfsNetworkMessage::NamespaceResponse(response) => {
                response.result == crate::DfsNamespaceResult::Applied
            }
            DfsNetworkMessage::DistributedWriteResponse(response) => matches!(
                response.result,
                crate::DfsDistributedWriteResult::Outcome {
                    outcome: crate::NetworkedDfsWriteOutcome::Committed { .. }
                }
            ),
            _ => false,
        })
}

pub fn dfs_metadata_journal_rejection(
    command: &crate::DfsMetadataJournalBatchCommand,
    reason: impl Into<String>,
) -> crate::DfsMetadataJournalBatchResponse {
    dfs_metadata_journal_rejection_for(
        command.request_id.clone(),
        command.batch.shard_id.clone(),
        command.batch.next_after_sequence,
        reason,
    )
}

pub fn dfs_metadata_journal_rejection_for(
    request_id: String,
    shard_id: DfsMetadataShardId,
    next_after_sequence: u64,
    reason: impl Into<String>,
) -> crate::DfsMetadataJournalBatchResponse {
    crate::DfsMetadataJournalBatchResponse {
        request_id,
        shard_id,
        next_after_sequence,
        applied_entries: 0,
        verified_prefix_only: false,
        verified_authority_probe_id: None,
        result: crate::DfsMetadataJournalBatchResult::Rejected {
            reason: reason.into(),
        },
    }
}

pub fn dfs_metadata_snapshot_rejection(
    command: &crate::DfsMetadataSnapshotInstallCommand,
    reason: impl Into<String>,
) -> crate::DfsMetadataSnapshotInstallResponse {
    dfs_metadata_snapshot_rejection_for(
        command.request_id.clone(),
        command.shard_id.clone(),
        command.next_after_sequence,
        reason,
    )
}

pub fn dfs_metadata_snapshot_rejection_for(
    request_id: String,
    shard_id: DfsMetadataShardId,
    next_after_sequence: u64,
    reason: impl Into<String>,
) -> crate::DfsMetadataSnapshotInstallResponse {
    crate::DfsMetadataSnapshotInstallResponse {
        request_id,
        shard_id,
        next_after_sequence,
        result: crate::DfsMetadataSnapshotInstallResult::Rejected {
            reason: reason.into(),
        },
    }
}

#[derive(Clone, Debug)]
pub enum DfsResidentMetadataScope {
    ExactShard(ResidentLocalMetadataShardHandle),
    Global(ResidentLocalShardedMetadataBackend),
}

impl DfsResidentMetadataScope {
    pub fn attach(&self, service: &mut DfsRuntimeMessageService) -> Result<(), crate::DfsError> {
        match self {
            Self::ExactShard(shard) => {
                service.attach_resident_metadata_shard_backend(shard.checkout()?)
            }
            Self::Global(backend) => {
                service.attach_resident_sharded_metadata_backend(backend.checkout_global()?)
            }
        }
    }

    pub fn exact_shard_handle(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<ResidentLocalMetadataShardHandle, crate::DfsError> {
        match self {
            Self::ExactShard(handle)
                if handle.file_system_id() == file_system_id && handle.shard_id() == shard_id =>
            {
                Ok(handle.clone())
            }
            Self::ExactShard(handle) => Err(crate::DfsError::Metadata {
                message: format!(
                    "resident metadata shard {}/{} cannot materialize requested shard {file_system_id}/{}",
                    handle.file_system_id(),
                    handle.shard_id().0,
                    shard_id.0
                ),
            }),
            Self::Global(backend) => backend
                .scope_shard(file_system_id, shard_id)
                .map_err(Into::into),
        }
    }
}

#[derive(Debug)]
pub struct DfsRuntimeMessageServiceLane {
    pub service: DfsRuntimeMessageService,
    pub peer_node_ids: Vec<sinomo_core::node::NodeId>,
    pub resident_metadata_scope: Option<DfsResidentMetadataScope>,
}

#[derive(Debug)]
pub struct DfsRuntimeMessageServiceLaneHandle {
    lane: Mutex<DfsRuntimeMessageServiceLane>,
    detached_send_runtime: Arc<DfsDetachedSendRuntime>,
}

#[derive(Debug)]
pub struct DfsRuntimeMessageCacheEntry {
    pub signature: DfsRuntimeMessageServiceSignature,
    pub metadata_authority_generation: Arc<crate::DfsMetadataAuthorityRuntime>,
    pub peer_node_ids: Vec<NodeId>,
    pub content_store: LocalContentStore,
    pub resident_metadata_backend: Option<ResidentLocalShardedMetadataBackend>,
    pub resident_metadata_initialized: bool,
    pub cross_shard_namespace_locks: crate::DfsCrossShardNamespaceLockRegistry,
    pub primary_service: Option<Arc<DfsRuntimeMessageServiceLaneHandle>>,
    pub exact_shard_services: std::collections::BTreeMap<
        crate::DfsMutationLaneKey,
        Arc<DfsRuntimeMessageServiceLaneHandle>,
    >,
    pub content_read_service: Option<Arc<DfsRuntimeMessageServiceLaneHandle>>,
}

impl DfsRuntimeMessageCacheEntry {
    fn matches_authority_generation(
        &self,
        peer_snapshot: &DfsRuntimeMessagePeerSnapshot,
        metadata_authority_generation: &Arc<crate::DfsMetadataAuthorityRuntime>,
        observed_generation: u64,
    ) -> bool {
        self.signature.cluster_id == peer_snapshot.cluster_id
            && self.signature.local_node_id == peer_snapshot.local_node_id
            && self.signature.metadata_authority_generation == observed_generation
            && Arc::ptr_eq(
                &self.metadata_authority_generation,
                metadata_authority_generation,
            )
            && self.resident_metadata_initialized
    }

    pub fn bounded_status_resident(
        &self,
        peer_snapshot: &DfsRuntimeMessagePeerSnapshot,
    ) -> Result<ResidentLocalShardedMetadataBackend, crate::DfsError> {
        let observed_generation = self.metadata_authority_generation.generation();
        if self.signature.cluster_id != peer_snapshot.cluster_id
            || self.signature.local_node_id != peer_snapshot.local_node_id
            || self.signature.metadata_authority_generation != observed_generation
        {
            return Err(crate::DfsError::Network {
                message: format!(
                    "bounded DFS status unavailable while resident metadata authority advances from generation {} to {observed_generation}",
                    self.signature.metadata_authority_generation
                ),
            });
        }
        if !self.resident_metadata_initialized {
            return Err(crate::DfsError::Network {
                message: "bounded DFS status unavailable because the resident metadata generation is not hydrated"
                    .to_string(),
            });
        }
        self.resident_metadata_backend.clone().ok_or_else(|| {
            crate::DfsError::Network {
                message: "bounded DFS status unavailable because this resident generation has no sharded metadata backend"
                    .to_string(),
            }
        })
    }

    pub fn exact_admission_handle(
        &self,
        peer_snapshot: &DfsRuntimeMessagePeerSnapshot,
        metadata_authority_generation: &Arc<crate::DfsMetadataAuthorityRuntime>,
        observed_generation: u64,
        lane: &crate::DfsMutationLaneKey,
    ) -> Result<Option<ResidentLocalMetadataShardHandle>, crate::DfsError> {
        if !self.matches_authority_generation(
            peer_snapshot,
            metadata_authority_generation,
            observed_generation,
        ) {
            return Ok(None);
        }
        self.resident_metadata_backend
            .as_ref()
            .map(|backend| backend.scope_shard(&lane.file_system_id, &lane.shard_id))
            .transpose()
            .map_err(Into::into)
    }

    pub fn replica_admission_handle(
        &self,
        peer_snapshot: &DfsRuntimeMessagePeerSnapshot,
        metadata_authority_generation: &Arc<crate::DfsMetadataAuthorityRuntime>,
        observed_generation: u64,
        context: &crate::DfsMetadataReplicaShardContext,
    ) -> Result<Option<ResidentLocalMetadataShardHandle>, crate::DfsError> {
        if !self.matches_authority_generation(
            peer_snapshot,
            metadata_authority_generation,
            observed_generation,
        ) {
            return Ok(None);
        }
        let Some(resident) = self.resident_metadata_backend.as_ref() else {
            return Ok(None);
        };
        let shard_id = DfsMetadataShardId::new(context.shard.shard_id.clone());
        if resident.shard_config(&shard_id)? != context.shard {
            return Ok(None);
        }
        resident
            .scope_shard(&context.shard.file_system_id, &shard_id)
            .map(Some)
            .map_err(Into::into)
    }

    pub fn exact_namespace_read_admission_is_current(
        &self,
        peer_snapshot: &DfsRuntimeMessagePeerSnapshot,
        metadata_authority_generation: &Arc<crate::DfsMetadataAuthorityRuntime>,
        observed_generation: u64,
        command: &crate::DfsNamespaceCommand,
    ) -> Result<bool, crate::DfsError> {
        let [stamped] = command.authority_shards.as_slice() else {
            return Ok(false);
        };
        if !self.matches_authority_generation(
            peer_snapshot,
            metadata_authority_generation,
            observed_generation,
        ) {
            return Ok(false);
        }
        let Some(resident) = self.resident_metadata_backend.as_ref() else {
            return Ok(false);
        };
        let shard_id = DfsMetadataShardId::new(stamped.shard_id.clone());
        if resident.shard_config(&shard_id)? != *stamped {
            return Ok(false);
        }
        Ok(resident
            .scope_shard(&stamped.file_system_id, &shard_id)?
            .is_open())
    }
}

pub fn dfs_runtime_message_uses_content_read_lane(command: &DfsMessageCommand) -> bool {
    match &command.routed_message.message {
        DfsNetworkMessage::ReadRequest(request) => {
            request.version.is_some() && request.content_hash.is_some()
        }
        DfsNetworkMessage::BulkReadRequest(_) => true,
        _ => false,
    }
}

pub fn dfs_runtime_message_supports_exact_service_lane(command: &DfsMessageCommand) -> bool {
    matches!(
        &command.routed_message.message,
        DfsNetworkMessage::MetadataJournalBatch(_) | DfsNetworkMessage::MetadataSnapshotInstall(_)
    ) || matches!(
        &command.routed_message.message,
        DfsNetworkMessage::NamespaceRequest(namespace) if namespace.authority_shards.len() == 1
    )
}

pub fn dfs_runtime_message_cache() -> &'static Mutex<BTreeMap<PathBuf, DfsRuntimeMessageCacheEntry>>
{
    DFS_RUNTIME_MESSAGE_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

pub fn dfs_detached_send_runtime_for_identity(
    data_root: &Path,
    cluster_id: &sinomo_core::Id,
    local_node_id: &NodeId,
) -> Result<Arc<DfsDetachedSendRuntime>, crate::DfsError> {
    let runtimes = DFS_DETACHED_SEND_RUNTIMES.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut runtimes = runtimes.lock().map_err(|_| crate::DfsError::Network {
        message: "DFS detached-send runtime lock poisoned".to_string(),
    })?;
    let key = DfsDetachedSendRuntimeKey {
        data_root: data_root.to_path_buf(),
        cluster_id: cluster_id.to_string(),
        local_node_id: local_node_id.to_string(),
    };
    Ok(Arc::clone(runtimes.entry(key).or_insert_with(|| {
        Arc::new(DfsDetachedSendRuntime::new())
    })))
}

#[derive(Debug)]
pub struct DfsRuntimeMaintenanceServiceLane {
    pub signature: DfsRuntimeMessageServiceSignature,
    pub metadata_authority_generation: Arc<crate::DfsMetadataAuthorityRuntime>,
    pub peer_node_ids: Vec<NodeId>,
    pub lane: DfsRuntimeMessageServiceLane,
    pub detached_send_runtime: Arc<DfsDetachedSendRuntime>,
}

impl DfsRuntimeMessageServiceLaneHandle {
    pub fn new(
        lane: DfsRuntimeMessageServiceLane,
        detached_send_runtime: Arc<DfsDetachedSendRuntime>,
    ) -> Self {
        Self {
            lane: Mutex::new(lane),
            detached_send_runtime,
        }
    }

    pub fn detached_send_runtime(&self) -> &Arc<DfsDetachedSendRuntime> {
        &self.detached_send_runtime
    }

    pub fn into_parts(
        self,
    ) -> (
        Mutex<DfsRuntimeMessageServiceLane>,
        Arc<DfsDetachedSendRuntime>,
    ) {
        (self.lane, self.detached_send_runtime)
    }
}

impl Deref for DfsRuntimeMessageServiceLaneHandle {
    type Target = Mutex<DfsRuntimeMessageServiceLane>;

    fn deref(&self) -> &Self::Target {
        &self.lane
    }
}

#[derive(Debug)]
pub struct DfsRuntimeMessageExecution {
    pub process_report: crate::DfsMessageProcessReport,
    pub committed_work: DfsMessageCommittedWork,
    pub replica_materialization_shard:
        Result<Option<ResidentLocalMetadataShardHandle>, crate::DfsError>,
    pub deferred_work: Result<
        Option<(
            Arc<DfsDetachedSendRuntime>,
            crate::DfsNodeDetachedSendWorker,
        )>,
        crate::DfsError,
    >,
    pub repair_queue_record_count: usize,
    pub repair_queue_persistence: Result<usize, crate::DfsError>,
    pub exact_projection_release_error: Option<crate::DfsError>,
    pub resident_backend_release_error: Option<crate::DfsError>,
    pub lane_lock_wait: Duration,
    pub backend_checkout: Duration,
    pub process: Duration,
    pub repair_queue_elapsed: Duration,
}

/// Execute one message against an already selected resident lane.
///
/// The persistence hook is invoked before the exact projection and resident
/// backend lease are released. This preserves the committed-edge ordering
/// contract while leaving the durable queue location and its host wake bit to
/// the embedding runtime.
pub fn execute_runtime_message_command<F>(
    service_lane_handle: &Arc<DfsRuntimeMessageServiceLaneHandle>,
    peer_node_ids: Vec<NodeId>,
    command: DfsMessageCommand,
    preingested_blob: Option<crate::ContentStoreQuarantinedBlob>,
    execution_scope: &crate::DfsMessageExecutionScope,
    persist_repair_records: F,
) -> Result<DfsRuntimeMessageExecution, crate::DfsError>
where
    F: FnOnce(Vec<crate::DfsRepairAttemptRecord>) -> Result<usize, crate::DfsError>,
{
    let is_read_only = crate::dfs_message_is_read_only_request(&command);
    let lane_lock_wait_started = std::time::Instant::now();
    let mut service_lane = service_lane_handle
        .lock()
        .map_err(|_| crate::DfsError::Network {
            message: "DFS runtime message lane lock poisoned".to_string(),
        })?;
    let lane_lock_wait = lane_lock_wait_started.elapsed();
    service_lane.refresh_peer_targets(peer_node_ids);
    let resident_metadata_scope = service_lane.resident_metadata_scope.clone();
    let backend_checkout_started = std::time::Instant::now();
    if let Some(scope) = resident_metadata_scope.as_ref() {
        scope.attach(&mut service_lane.service)?;
    }
    let backend_checkout = backend_checkout_started.elapsed();
    let process_started = std::time::Instant::now();
    let committed_work_tracker = crate::DfsCommittedWorkTracker::new();
    service_lane
        .service
        .set_committed_work_tracker(committed_work_tracker.clone());
    let mut process_report = service_lane
        .service
        .process_message_command_report_with_preingested_blob(command, preingested_blob);
    let replica_materialization_shard = process_report
        .committed_work
        .metadata_materialization
        .then(|| {
            let Some(lane) = crate::DfsMutationLaneKey::from_scope(execution_scope) else {
                return Err(crate::DfsError::Metadata {
                    message:
                        "durable replica ingest completed without an exact materialization lane"
                            .to_string(),
                });
            };
            resident_metadata_scope
                .as_ref()
                .ok_or_else(|| crate::DfsError::Metadata {
                    message: "durable replica ingest has no resident metadata generation"
                        .to_string(),
                })?
                .exact_shard_handle(&lane.file_system_id, &lane.shard_id)
        })
        .transpose();
    let deferred_work = if is_read_only {
        Ok(None)
    } else {
        service_lane
            .service
            .message_deferred_work_pending()
            .map(|pending| {
                pending.then(|| {
                    (
                        Arc::clone(service_lane_handle.detached_send_runtime()),
                        service_lane.service.detach_message_deferred_work(),
                    )
                })
            })
    };
    let repair_queue_records = if is_read_only {
        Vec::new()
    } else {
        std::mem::take(&mut process_report.committed_repair_records)
    };
    let committed_work = committed_work_tracker.take();
    let repair_queue_record_count = repair_queue_records.len();
    let repair_queue_started = std::time::Instant::now();
    let repair_queue_persistence = if repair_queue_records.is_empty() {
        Ok(0)
    } else {
        persist_repair_records(repair_queue_records)
    };
    let repair_queue_elapsed = repair_queue_started.elapsed();
    let exact_projection_release_error = if repair_queue_persistence.is_ok()
        && matches!(
            &resident_metadata_scope,
            Some(DfsResidentMetadataScope::ExactShard(_))
        ) {
        service_lane
            .service
            .release_hydrated_metadata_projection_if_quiescent()
            .err()
    } else {
        None
    };
    let resident_backend_release_error = if resident_metadata_scope.is_some() {
        service_lane
            .service
            .release_resident_metadata_backend()
            .err()
    } else {
        None
    };
    let process = process_started.elapsed();
    drop(service_lane);
    Ok(DfsRuntimeMessageExecution {
        process_report,
        committed_work,
        replica_materialization_shard,
        deferred_work,
        repair_queue_record_count,
        repair_queue_persistence,
        exact_projection_release_error,
        resident_backend_release_error,
        lane_lock_wait,
        backend_checkout,
        process,
        repair_queue_elapsed,
    })
}

impl DfsRuntimeMessageServiceLane {
    pub fn new(
        service: DfsRuntimeMessageService,
        peer_node_ids: Vec<sinomo_core::node::NodeId>,
        resident_metadata_scope: Option<DfsResidentMetadataScope>,
    ) -> Self {
        Self {
            service,
            peer_node_ids,
            resident_metadata_scope,
        }
    }

    pub fn refresh_peer_targets(&mut self, peer_node_ids: Vec<sinomo_core::node::NodeId>) {
        if self.peer_node_ids == peer_node_ids {
            return;
        }
        self.service
            .refresh_message_peer_targets(peer_node_ids.clone());
        self.peer_node_ids = peer_node_ids;
    }
}

pub fn dfs_network_message_kind(message: &DfsNetworkMessage) -> &'static str {
    match message {
        DfsNetworkMessage::DistributedWriteRequest(_) => "distributed-write-request",
        DfsNetworkMessage::DistributedWriteResponse(_) => "distributed-write-response",
        DfsNetworkMessage::NamespaceRequest(_) => "namespace-request",
        DfsNetworkMessage::NamespaceResponse(_) => "namespace-response",
        DfsNetworkMessage::MetadataJournalBatch(_) => "metadata-journal-batch",
        DfsNetworkMessage::MetadataJournalBatchResponse(_) => "metadata-journal-batch-response",
        DfsNetworkMessage::StageBlob(_) => "stage-blob",
        DfsNetworkMessage::StageBlobResponse(_) => "stage-blob-response",
        DfsNetworkMessage::FetchBlob(_) => "fetch-blob",
        DfsNetworkMessage::FetchBlobResponse(_) => "fetch-blob-response",
        _ => "other",
    }
}

/// Stream an inbound RPC attachment into the DFS quarantine store and bind the
/// resulting content identity to the request. Network/body plumbing remains a
/// host concern; validation, hashing, quarantine, and command mutation are DFS
/// semantics and therefore live in the owner crate.
pub async fn ingest_dfs_message_attachment(
    content: Option<LocalContentStore>,
    mut command: DfsMessageCommand,
    attachment: Option<DfsInboundAttachment>,
) -> Result<
    (
        DfsMessageCommand,
        Option<crate::ContentStoreQuarantinedBlob>,
    ),
    crate::DfsError,
> {
    let Some(attachment) = attachment else {
        return Ok((command, None));
    };
    let content = content.ok_or_else(|| crate::DfsError::Distributed {
        message: "streamed DFS attachment is missing its shared content store".to_string(),
    })?;
    let (expected_hash, stage_blob) = match &command.routed_message.message {
        DfsNetworkMessage::StageBlob(stage) => (Some(stage.content_hash.clone()), true),
        DfsNetworkMessage::DistributedWriteRequest(_)
        | DfsNetworkMessage::ExclusiveCreateRequest(_) => {
            (attachment.descriptor.content_hash.clone(), false)
        }
        other => {
            return Err(crate::DfsError::Distributed {
                message: format!("streamed DFS attachment is not supported for {other:?}"),
            });
        }
    };
    if stage_blob
        && let Some(descriptor_hash) = attachment.descriptor.content_hash.as_deref()
        && Some(descriptor_hash) != expected_hash.as_deref()
    {
        return Err(crate::DfsError::ContentHashMismatch {
            expected: expected_hash.unwrap_or_default(),
            actual: descriptor_hash.to_string(),
        });
    }

    let total_size_bytes = attachment.descriptor.total_size_bytes;
    let mut body = attachment.body;
    let (chunk_sender, mut chunk_receiver) =
        tokio::sync::mpsc::channel::<ObjectRpcBodyChunk>(DFS_ATTACHMENT_INGEST_BUFFERED_CHUNKS);
    let ingest_worker = tokio::task::spawn_blocking(move || -> Result<_, crate::DfsError> {
        let mut ingest =
            content.begin_quarantined_blob_ingest(total_size_bytes, expected_hash.as_deref())?;
        while let Some(chunk) = chunk_receiver.blocking_recv() {
            ingest.write_chunk(chunk.as_ref())?;
        }
        ingest.finish_quarantined().map_err(Into::into)
    });

    let feed_result = loop {
        let chunk = match body.next_chunk().await {
            Ok(chunk) => chunk,
            Err(source) => {
                break Err(crate::DfsError::Network {
                    message: format!("read streamed DFS request attachment: {source}"),
                });
            }
        };
        let Some(chunk) = chunk else {
            break Ok(());
        };
        if chunk_sender.send(chunk).await.is_err() {
            // The ingest worker owns the authoritative storage error.
            break Ok(());
        }
    };
    drop(chunk_sender);
    let ingest_result = ingest_worker
        .await
        .map_err(|source| crate::DfsError::Network {
            message: format!("DFS streamed attachment task failed: {source}"),
        })?;
    feed_result?;
    let blob = ingest_result?;
    if !stage_blob {
        match &mut command.routed_message.message {
            DfsNetworkMessage::DistributedWriteRequest(write)
            | DfsNetworkMessage::ExclusiveCreateRequest(write) => {
                write.bytes.clear();
                write.content_hash = Some(blob.content_hash.clone());
                write.content_size_bytes = Some(blob.size_bytes);
                write.requester_blob_pre_staged = false;
            }
            _ => unreachable!("streamed distributed write kind was checked before ingest"),
        }
    }
    Ok((command, Some(blob)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DfsDataPlaneResponse, DfsMessageResponse, DfsNamespaceCommand, DfsNamespaceOperation,
        DfsNamespaceResponse, DfsNamespaceResult, DfsRoutedMessage,
    };

    #[test]
    fn reactive_namespace_event_requires_committed_visible_mutation() {
        let source = NodeId::from_legacy_named("node_reactive_event_source").expect("source");
        let target = NodeId::from_legacy_named("node_reactive_event_target").expect("target");
        let command_for = |operation| DfsMessageCommand {
            request_id: "reactive-event-request".to_string(),
            routed_message: DfsRoutedMessage {
                source_node_id: source.clone(),
                target_node_id: target.clone(),
                frame_id: 1,
                message_id: 1,
                message: DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
                    request_id: "reactive-event-namespace".to_string(),
                    file_system_id: crate::sifs_file_system_id(),
                    requesting_node_id: source.clone(),
                    include_metadata_journal_batches: false,
                    metadata_journal_after_sequences: Vec::new(),
                    authority_shards: Vec::new(),
                    operation,
                }),
            },
        };

        assert!(
            dfs_visible_mutation_event(&command_for(DfsNamespaceOperation::LookupPath {
                path: "/services/reactive".to_string(),
                inline_range: None,
            }))
            .is_none()
        );
        let event = dfs_visible_mutation_event(&command_for(DfsNamespaceOperation::Mkdir {
            path: "/services/reactive".to_string(),
        }))
        .expect("visible namespace mutation event");
        assert_eq!(event.origin_node_id, target);
        assert_eq!(event.operation, "namespace-operation");
        assert_eq!(event.paths, vec!["/services/reactive".to_string()]);

        let response_for = |result| {
            DfsDataPlaneResponse::value(DfsMessageResponse {
                request_id: "reactive-event-response".to_string(),
                outbound_messages: vec![DfsRoutedMessage {
                    source_node_id: target.clone(),
                    target_node_id: source.clone(),
                    frame_id: 2,
                    message_id: 2,
                    message: DfsNetworkMessage::NamespaceResponse(DfsNamespaceResponse {
                        request_id: "reactive-event-namespace".to_string(),
                        result,
                        synced_path_object: None,
                        synced_path_objects: Vec::new(),
                        metadata_journal_batches: Vec::new(),
                        metadata_journal_has_more: Some(false),
                        posix_coordination_result: None,
                    }),
                }],
            })
        };
        assert!(dfs_response_committed_visible_mutation(&response_for(
            DfsNamespaceResult::Applied
        )));
        assert!(!dfs_response_committed_visible_mutation(&response_for(
            DfsNamespaceResult::Rejected {
                reason: "not applied".to_string(),
                posix_error: None,
                current_primary_node_id: None,
            }
        )));
    }

    #[test]
    fn committed_effects_combine_receipts_and_events() {
        let committed = DfsMessageCommittedWork {
            publish_metadata: true,
            namespace_maintenance: true,
            ..DfsMessageCommittedWork::default()
        };
        let effects = DfsCommittedEffects::classify(&committed, &[]);
        assert!(effects.publish_metadata);
        assert!(effects.namespace_maintenance);
        assert!(!effects.replica_progress);
        assert!(!effects.repair_maintenance);
    }
}
