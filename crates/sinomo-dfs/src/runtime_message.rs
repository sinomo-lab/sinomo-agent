use crate::{
    DfsFileSystemId, DfsInboundAttachment, DfsMessageCommand, DfsMessageCommittedWork,
    DfsMessageResponse, DfsMetadataShardId, DfsNetworkMessage, DfsNodeTransport,
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

pub(crate) const DFS_ATTACHMENT_INGEST_BUFFERED_CHUNKS: usize = 4;

/// Checkpoint only the exact actual named transfer observed before dispatch.
/// This is queue DATA reconciliation, not metadata receipt/effect authority.
pub(crate) fn checkpoint_detached_replica_delivery(
    data_dir: &sinomo_core::local::SinomoDataDir,
    event: crate::node_transport::DfsBackgroundReplicaDeliveryEvent,
) -> Result<crate::node_transport::DfsBackgroundReplicaDispatch, crate::DfsError> {
    checkpoint_detached_replica_delivery_with_layout(data_dir, event, None, None)
}

pub(crate) fn checkpoint_detached_replica_delivery_with_layout(
    data_dir: &sinomo_core::local::SinomoDataDir,
    event: crate::node_transport::DfsBackgroundReplicaDeliveryEvent,
    layout: Option<&crate::DfsSelectedOriginalDataLayout>,
    inspection: Option<&crate::content_store::RepairPayloadInspectionContext>,
) -> Result<crate::node_transport::DfsBackgroundReplicaDispatch, crate::DfsError> {
    use crate::node_transport::DfsBackgroundReplicaDeliveryEvent;
    use crate::node_transport::DfsBackgroundReplicaDispatch;
    use sinomo_api::dfs::maintenance::DfsRepairTransferKind;
    use sinomo_core::network::NetworkDeliveryState;
    let store = if let Some(layout) = layout {
        layout.queue()?
    } else {
        crate::repair_queue_store::LocalDfsRepairQueueStore::new(
            crate::default_dfs_repair_attempt_snapshot_path(data_dir),
        )
    };
    let matches = |message: &crate::DfsRoutedMessage, record: &crate::DfsRepairAttemptRecord| {
        let DfsNetworkMessage::RepairReplica(command) = &message.message else {
            return false;
        };
        command.background_repair
            && record.transfer_kind == Some(DfsRepairTransferKind::Repair)
            && record.transfer_request_id.as_ref() == Some(&command.request_id)
            && record.completed_transfer_request_id.is_none()
            && record.intent_id == command.intent_id
            && record.object_id == command.object_id
            && record.version == command.version
            && record.content_hash == command.content_hash
            && record.missing_replica_node_id == message.target_node_id
    };
    match event {
        DfsBackgroundReplicaDeliveryEvent::BeforeDispatch { message } => {
            let DfsNetworkMessage::RepairReplica(command) = &message.message else {
                return Err(crate::DfsError::Distributed {
                    message: "detached delivery observation requires its actual repair command"
                        .into(),
                });
            };
            let shard =
                command
                    .authority_shard
                    .as_ref()
                    .ok_or_else(|| crate::DfsError::Metadata {
                        message: "native background repair has no original authority shard".into(),
                    })?;
            if crate::local_state::read_local_dfs_metadata_shard_write_freeze_with_layout(
                data_dir, &crate::DfsMetadataShardId::new(&shard.shard_id), layout,
            )?
            .is_some_and(|freeze| freeze.activated_epoch != Some(shard.epoch))
            {
                return Ok(DfsBackgroundReplicaDispatch::Fenced);
            }
            let original = store.validate_original_repair_dispatch(&message)?;
            if let Some(inspection) = inspection {
                inspection.inspect(&original.root)?;
            } else {
                LocalContentStore::open_for_inspection(data_dir.dfs_dir().join("content"))
                    .inspect_repair_payload_root(&original.root)?;
            }
            let (records, _) = store.observe_transfer_targets(
                &command.intent_id,
                std::slice::from_ref(&message.target_node_id),
            )?;
            let Some(record) = records.into_iter().next() else {
                return Ok(DfsBackgroundReplicaDispatch::Observed(None));
            };
            if !matches(&message, &record) {
                return Err(crate::DfsError::Distributed {
                    message: "detached repair differs from its named durable custody".into(),
                });
            }
            Ok(DfsBackgroundReplicaDispatch::Observed(Some(record)))
        }
        DfsBackgroundReplicaDeliveryEvent::Failed {
            message,
            before,
            producer,
            delivery_state,
            last_error,
        } => {
            if !matches(&message, &before) {
                return Err(crate::DfsError::Distributed {
                    message: "detached repair outcome differs from its pre-dispatch custody".into(),
                });
            }
            store.record_original_repair_delivery(&message, delivery_state)?;
            let mut desired = before.clone();
            desired.transfer_delivery_state = if producer.retrying_unresolved_transfer {
                crate::service_repair::join_transfer_delivery(
                    producer.previous_transfer_delivery_state,
                    delivery_state,
                )
            } else {
                delivery_state
            };
            desired.in_flight = false;
            if desired.transfer_delivery_state == Some(NetworkDeliveryState::DefinitelyNotSent) {
                desired.transfer_request_id = None;
            }
            desired.last_error = Some(last_error);
            let now = crate::dfs_now_unix_nanos();
            desired.last_attempt_unix_nanos = now;
            let ticks = crate::service_repair::repair_retry_delay_ticks_for_error(
                desired.attempts,
                desired.last_error.as_deref(),
            );
            desired.next_retry_unix_nanos =
                now.saturating_add(crate::transfer::repair_retry_delay_nanos_for_ticks(ticks));
            store.checkpoint_transfer_record(&before, &desired)?;
            Ok(DfsBackgroundReplicaDispatch::Observed(None))
        }
    }
}

pub(crate) type DfsRuntimeMessageService = NetworkedDfsService<DfsNodeTransport>;
pub(crate) type DfsRuntimeTask = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

static DFS_RUNTIME_MESSAGE_CACHE: OnceLock<DfsRuntimeMessageCache> = OnceLock::new();

/// One cache with a short root registry lock. Store/lane generation ownership
/// remains serialized by the selected actual data root, never by other roots.
#[derive(Default)]
pub(crate) struct DfsRuntimeMessageCache {
    roots: Mutex<BTreeMap<PathBuf, Arc<Mutex<Option<DfsRuntimeMessageCacheEntry>>>>>,
}

impl DfsRuntimeMessageCache {
    pub(crate) fn root(
        &self,
        root: &Path,
    ) -> Result<Arc<Mutex<Option<DfsRuntimeMessageCacheEntry>>>, crate::DfsError> {
        let root =
            crate::runtime_host::dfs_runtime_message_cache_root(root, false).map_err(|error| {
                crate::DfsError::Network {
                    message: error.to_string(),
                }
            })?;
        let root = root.as_path();
        let mut roots = self.roots.lock().map_err(|_| Self::poisoned())?;
        Ok(Arc::clone(
            roots
                .entry(root.to_path_buf())
                .or_insert_with(|| Arc::new(Mutex::new(None))),
        ))
    }

    pub(crate) fn existing_root(
        &self,
        root: &Path,
    ) -> Result<Option<Arc<Mutex<Option<DfsRuntimeMessageCacheEntry>>>>, crate::DfsError> {
        let root =
            crate::runtime_host::dfs_runtime_message_cache_root(root, false).map_err(|error| {
                crate::DfsError::Network {
                    message: error.to_string(),
                }
            })?;
        let root = root.as_path();
        let roots = self.roots.lock().map_err(|_| Self::poisoned())?;
        Ok(roots.get(root).cloned())
    }

    pub(crate) fn try_existing_root(
        &self,
        root: &Path,
    ) -> Result<Option<Arc<Mutex<Option<DfsRuntimeMessageCacheEntry>>>>, crate::DfsError> {
        let root =
            crate::runtime_host::dfs_runtime_message_cache_root(root, true).map_err(|error| {
                crate::DfsError::Network {
                    message: error.to_string(),
                }
            })?;
        let root = root.as_path();
        let roots = match self.roots.try_lock() {
            Ok(roots) => roots,
            Err(std::sync::TryLockError::WouldBlock) => {
                tracing::trace!(target: "sinomo.dfs.cache_diagnostic",
                    data_root = %root.display(), branch = "root-registry",
                    "bounded status cache busy");
                return Err(crate::DfsError::Network {
                    message: "bounded DFS status unavailable while the resident generation cache is being updated".into(),
                });
            }
            Err(std::sync::TryLockError::Poisoned(_)) => return Err(Self::poisoned()),
        };
        Ok(roots.get(root).cloned())
    }

    pub(crate) fn evict(&self, root: &Path) -> Result<bool, crate::DfsError> {
        let root =
            crate::runtime_host::dfs_runtime_message_cache_root(root, false).map_err(|error| {
                crate::DfsError::Network {
                    message: error.to_string(),
                }
            })?;
        let root = root.as_path();
        let Some(slot) = self.existing_root(root)? else {
            return Ok(false);
        };
        let mut entry = slot.lock().map_err(|_| Self::poisoned())?;
        let removed = entry.take();
        let was_present = removed.is_some();
        // Drop all idle backend-bearing ownership before permitting another
        // caller at this root to initialize its replacement.
        drop(removed);
        let mut roots = self.roots.lock().map_err(|_| Self::poisoned())?;
        if roots
            .get(root)
            .is_some_and(|stored| Arc::ptr_eq(stored, &slot))
            && Arc::strong_count(&slot) == 2
        {
            // No queued or active caller owns this empty slot. Root->registry
            // is the only nested order; every lookup drops registry ownership
            // before acquiring any root lock. Otherwise keep this SAME slot so
            // an already-queued caller cannot create a parallel generation.
            roots.remove(root);
        }
        Ok(was_present)
    }

    fn poisoned() -> crate::DfsError {
        crate::DfsError::Network {
            message: "daemon DFS message service cache lock poisoned".into(),
        }
    }
}

static DFS_DETACHED_SEND_RUNTIMES: OnceLock<
    Mutex<BTreeMap<DfsDetachedSendRuntimeKey, Arc<DfsDetachedSendRuntime>>>,
> = OnceLock::new();

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DfsDetachedSendRuntimeKey {
    data_root: PathBuf,
    cluster_id: String,
    local_node_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DfsRuntimeMessageServiceSignature {
    pub cluster_id: sinomo_core::Id,
    pub local_node_id: NodeId,
    pub metadata_authority_generation: u64,
    pub metadata_storage_generation: u64,
}

impl DfsRuntimeMessageServiceSignature {
    /// Configuration and proof deltas continue to fence authority without
    /// replacing stores whose lifecycle has not changed.
    pub(crate) fn shares_metadata_storage(&self, other: &Self) -> bool {
        self.cluster_id == other.cluster_id
            && self.local_node_id == other.local_node_id
            && self.metadata_storage_generation == other.metadata_storage_generation
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DfsRuntimeMessagePeerSnapshot {
    pub cluster_id: sinomo_core::Id,
    pub local_node_id: NodeId,
    pub peer_node_ids: Vec<NodeId>,
}

/// Read the cluster-owned merged membership and retain a short-lived DFS peer
/// discovery projection. The local bootstrap checkpoint can omit members that
/// have already joined through KV. This routing view does not grant mutation
/// authority; transport and metadata admission retain their own checks.

/// Process-resident durable-send state shared by every DFS message lane for
/// one local cluster identity.
///
/// The DFS owner keeps retry coalescing, recovery-generation fencing, and the
/// lightweight detached outbox watcher here. The embedding daemon contributes
/// only the current offline-target policy and a callback that spawns the
/// prepared task on its runtime.
#[derive(Debug)]
pub(crate) struct DfsDetachedSendRuntime {
    state: crate::DfsNodeDetachedSendState,
    // The physical store/assignment lifecycle owns recovery enumeration.
    // Policy and proof updates fence authority but cannot own another scan.
    recovery_scan_generation: AtomicU64,
    metadata_authority_generation: OnceLock<Arc<crate::DfsMetadataAuthorityRuntime>>,
    worker_active: AtomicBool,
    worker_schedule: Mutex<DfsDetachedWorkerSchedule>,
    #[cfg(test)]
    worker_wait_count: AtomicU64,
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

// Own the active flag before spawning, including cancellation before first poll.
// A cancelled loop returns its actual prepared payloads to the same runtime;
// the next normal schedule can replace its transport while preserving custody.
struct DfsDeferredWorkerOwner {
    runtime: Arc<DfsDetachedSendRuntime>,
    worker: Option<crate::DfsNodeDetachedSendWorker>,
}

impl Drop for DfsDeferredWorkerOwner {
    fn drop(&mut self) {
        let mut schedule = self
            .runtime
            .worker_schedule
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(previous) = self.worker.take() {
            if let Some(latest) = schedule.latest_worker.as_mut() {
                latest.retain_prepared_calls_from(&previous);
            } else {
                schedule.latest_worker = Some(previous);
            }
        }
        self.runtime.worker_active.store(false, Ordering::Release);
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
            #[cfg(test)]
            worker_wait_count: AtomicU64::new(0),
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
        worker: crate::DfsNodeDetachedSendWorker,
        deferred_target_node_ids: BTreeSet<NodeId>,
        retry_delay: Duration,
        spawn: F,
    ) where
        F: FnOnce(DfsRuntimeTask),
    {
        self.schedule_deferred_work_owned(
            worker,
            deferred_target_node_ids,
            retry_delay,
            None,
            spawn,
        );
    }

    pub(crate) fn schedule_deferred_work_owned<F>(
        self: &Arc<Self>,
        mut worker: crate::DfsNodeDetachedSendWorker,
        deferred_target_node_ids: BTreeSet<NodeId>,
        retry_delay: Duration,
        task_owner: Option<Arc<crate::runtime_host::DfsRuntimeHost>>,
        spawn: F,
    ) where
        F: FnOnce(DfsRuntimeTask),
    {
        worker.set_deferred_target_node_ids(deferred_target_node_ids.clone());
        let scheduler_wakeup = worker.clone();
        let mut schedule = match self.worker_schedule.lock() {
            Ok(schedule) => schedule,
            Err(_) => {
                tracing::warn!("DFS deferred worker schedule is poisoned");
                return;
            }
        };
        schedule.deferred_target_node_ids = Some(deferred_target_node_ids);
        schedule.generation = schedule.generation.saturating_add(1);
        if let Some(previous) = schedule.latest_worker.as_ref() {
            worker.retain_prepared_calls_from(previous);
        }
        schedule.latest_worker = Some(worker);
        drop(schedule);
        let start_worker = self
            .worker_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        // Publish every replacement before waking a parked watcher. Fresh
        // prepared calls also need a wake when the target policy is unchanged.
        if !start_worker {
            scheduler_wakeup.notify_scheduler_policy_change();
        }
        if start_worker {
            let runtime = Arc::clone(self);
            let owner = DfsDeferredWorkerOwner {
                runtime: Arc::clone(self),
                worker: None,
            };
            spawn(Box::pin(async move {
                runtime
                    .run_deferred_worker(retry_delay, owner, task_owner)
                    .await;
            }));
        }
    }

    async fn run_deferred_worker(
        self: Arc<Self>,
        retry_delay: Duration,
        mut owner: DfsDeferredWorkerOwner,
        task_owner: Option<Arc<crate::runtime_host::DfsRuntimeHost>>,
    ) {
        loop {
            let observed_generation = match self.worker_schedule.lock() {
                Ok(mut schedule) => {
                    if let Some(mut latest_worker) = schedule.latest_worker.take() {
                        if let Some(previous) = owner.worker.as_ref() {
                            latest_worker.retain_prepared_calls_from(previous);
                        }
                        owner.worker = Some(latest_worker);
                    }
                    schedule.generation
                }
                Err(_) => {
                    tracing::warn!("DFS deferred worker schedule is poisoned");
                    break;
                }
            };
            let Some(active_worker) = owner.worker.as_ref() else {
                break;
            };
            let mut attempt_worker = active_worker.clone();
            let attempt = if let Some(task_owner) = task_owner.as_ref() {
                let (result_tx, result_rx) = tokio::sync::oneshot::channel();
                let task = tokio::task::spawn_blocking(move || {
                    let result = attempt_worker.drain_exclusive_with_retry_observation();
                    drop(attempt_worker);
                    let _ = result_tx.send(result);
                });
                // Aborting this outer scheduler cannot stop an already-running
                // blocking drain. Its synchronous native response handler can
                // still own admission and the OS data lock. Register that SAME
                // child before awaiting, so component shutdown joins it under
                // the existing deadline before exposing storage to remount.
                task_owner.own_task(task);
                result_rx.await.map_err(|error| error.to_string())
            } else {
                tokio::task::spawn_blocking(move || {
                    attempt_worker.drain_exclusive_with_retry_observation()
                })
                .await
                .map_err(|error| error.to_string())
            };
            let (result, background_retry) = match attempt {
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
            if current_generation != observed_generation {
                continue;
            }
            if background_retry {
                if let Err(error) = active_worker
                    .wait_for_background_delivery_retry(outbox_generation, retry_delay)
                    .await
                {
                    tracing::warn!("DFS deferred send wait failed: {error}");
                    tokio::time::sleep(retry_delay).await;
                }
                continue;
            }
            if ready {
                continue;
            }
            #[cfg(test)]
            self.worker_wait_count.fetch_add(1, Ordering::Release);
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
    }

    #[cfg(test)]
    pub fn worker_is_active(&self) -> bool {
        self.worker_active.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub fn worker_schedule_generation(&self) -> u64 {
        self.worker_schedule
            .lock()
            .map(|schedule| schedule.generation)
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub(crate) fn worker_wait_count(&self) -> u64 {
        self.worker_wait_count.load(Ordering::Acquire)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct DfsCommittedEffects {
    pub publish_metadata: bool,
    pub replica_progress: bool,
    pub repair_maintenance: bool,
    pub namespace_maintenance: bool,
    pub semantic_changes: bool,
    pub storage_environment: bool,
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
            semantic_changes: committed_work.semantic_changes,
            storage_environment: *committed_work != DfsMessageCommittedWork::default()
                || events.iter().any(|event| {
                    matches!(
                        event,
                        crate::NetworkedDfsServiceEvent::BlobStaged { .. }
                            | crate::NetworkedDfsServiceEvent::ReplicaRepaired { .. }
                            | crate::NetworkedDfsServiceEvent::BlobFetched { .. }
                    )
                }),
        }
    }
}

/// Host callbacks for effects that cross from a committed DFS mutation into
/// process scheduling. The owner classifies and orders the effects; the host
/// only supplies its local wake/publication mechanisms.
pub(crate) trait DfsCommittedEffectsHost {
    fn record_replica_progress(&self) -> bool;
    fn wake_maintenance(&self, repair: bool, namespace: bool);
    fn publish_metadata(&self);
    fn semantic_changes(&self) {}

    /// Coalesced hint that local DFS bytes or durable metadata changed enough
    /// to make cached capacity evidence worth resampling. Hosts may debounce
    /// this edge; it is advisory and grants no storage or metadata authority.
    fn storage_environment_changed(&self) {}
}

pub(crate) fn emit_runtime_committed_effects(
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
    if effects.semantic_changes {
        host.semantic_changes();
    }
    if effects.storage_environment {
        host.storage_environment_changed();
    }
    effects
}

#[derive(Debug)]
pub(crate) struct DfsRuntimeMessageResult {
    pub response: DfsMessageResponse,
    pub attachment: Option<DfsOutboundAttachment>,
    pub committed_edges: DfsCommittedEffects,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DfsVisibleMutationEvent {
    pub origin_node_id: NodeId,
    pub operation: &'static str,
    pub paths: Vec<String>,
}

pub(crate) fn dfs_visible_mutation_event(
    command: &DfsMessageCommand,
) -> Option<DfsVisibleMutationEvent> {
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

pub(crate) fn dfs_response_committed_visible_mutation(
    response: &crate::DfsDataPlaneResponse,
) -> bool {
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

pub(crate) fn dfs_metadata_journal_rejection(
    command: &crate::DfsMetadataJournalBatchCommand,
    reason: impl Into<crate::DfsMetadataRejection>,
) -> crate::DfsMetadataJournalBatchResponse {
    dfs_metadata_journal_rejection_for(
        command.request_id.clone(),
        command.batch.shard_id.clone(),
        command.batch.next_after_sequence,
        reason,
    )
}

pub(crate) fn dfs_metadata_journal_rejection_for(
    request_id: String,
    shard_id: DfsMetadataShardId,
    next_after_sequence: u64,
    reason: impl Into<crate::DfsMetadataRejection>,
) -> crate::DfsMetadataJournalBatchResponse {
    crate::DfsMetadataJournalBatchResponse {
        request_id,
        shard_id,
        next_after_sequence,
        applied_entries: 0,
        verified_prefix_only: false,
        verified_authority_probe_id: None,
        result: crate::DfsMetadataJournalBatchResult::from_rejection(reason),
    }
}

pub(crate) fn dfs_metadata_snapshot_rejection(
    command: &crate::DfsMetadataSnapshotInstallCommand,
    reason: impl Into<crate::DfsMetadataRejection>,
) -> crate::DfsMetadataSnapshotInstallResponse {
    dfs_metadata_snapshot_rejection_for(
        command.request_id.clone(),
        command.shard_id.clone(),
        command.next_after_sequence,
        reason,
    )
}

pub(crate) fn dfs_metadata_snapshot_rejection_for(
    request_id: String,
    shard_id: DfsMetadataShardId,
    next_after_sequence: u64,
    reason: impl Into<crate::DfsMetadataRejection>,
) -> crate::DfsMetadataSnapshotInstallResponse {
    crate::DfsMetadataSnapshotInstallResponse {
        request_id,
        shard_id,
        next_after_sequence,
        result: crate::DfsMetadataSnapshotInstallResult::from_rejection(reason),
    }
}

#[derive(Clone, Debug)]
pub(crate) enum DfsResidentMetadataScope {
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
pub(crate) struct DfsRuntimeMessageServiceLane {
    pub service: DfsRuntimeMessageService,
    pub peer_node_ids: Vec<sinomo_core::node::NodeId>,
    pub resident_metadata_scope: Option<DfsResidentMetadataScope>,
}

#[derive(Debug)]
pub(crate) struct DfsRuntimeMessageServiceLaneHandle {
    lane: Mutex<DfsRuntimeMessageServiceLane>,
    detached_send_runtime: Arc<DfsDetachedSendRuntime>,
}

#[derive(Debug)]
pub(crate) struct DfsRuntimeMessageCacheEntry {
    pub signature: DfsRuntimeMessageServiceSignature,
    pub metadata_authority_generation: Arc<crate::DfsMetadataAuthorityRuntime>,
    pub peer_node_ids: Vec<NodeId>,
    pub content_store: LocalContentStore,
    pub resident_metadata_backend: Option<ResidentLocalShardedMetadataBackend>,
    pub resident_metadata_initialized: bool,
    pub(crate) write_intent_maintenance_scan:
        Arc<crate::metadata_intent_maintenance::DfsWriteIntentMaintenanceScanState>,

    pub(crate) maintenance_progress: Arc<crate::maintenance::DfsMaintenanceProgressState>,
    pub cross_shard_namespace_locks: crate::DfsCrossShardNamespaceLockRegistry,
    pub primary_service: Option<Arc<DfsRuntimeMessageServiceLaneHandle>>,
    pub exact_shard_services: std::collections::BTreeMap<
        crate::DfsMutationLaneKey,
        Arc<DfsRuntimeMessageServiceLaneHandle>,
    >,
    pub content_read_service: Option<Arc<DfsRuntimeMessageServiceLaneHandle>>,
}

impl DfsRuntimeMessageCacheEntry {
    fn matches_runtime_owner(
        &self,
        peer_snapshot: &DfsRuntimeMessagePeerSnapshot,
        metadata_authority_generation: &Arc<crate::DfsMetadataAuthorityRuntime>,
    ) -> bool {
        self.signature.cluster_id == peer_snapshot.cluster_id
            && self.signature.local_node_id == peer_snapshot.local_node_id
            && Arc::ptr_eq(
                &self.metadata_authority_generation,
                metadata_authority_generation,
            )
            && self.resident_metadata_initialized
    }

    fn matches_authority_generation(
        &self,
        peer_snapshot: &DfsRuntimeMessagePeerSnapshot,
        metadata_authority_generation: &Arc<crate::DfsMetadataAuthorityRuntime>,
        observed_generation: u64,
    ) -> bool {
        self.matches_runtime_owner(peer_snapshot, metadata_authority_generation)
            && self.signature.metadata_authority_generation == observed_generation
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

    /// Borrow the already-validated process-local resident generation for a
    /// passive observation. Unlike authority-sensitive admission, this does
    /// not refresh peer discovery or read durable state: the cache signature
    /// and retained authority generation are the complete validity fence.
    pub fn bounded_status_resident_current(
        &self,
    ) -> Result<ResidentLocalShardedMetadataBackend, crate::DfsError> {
        let observed_generation = self.metadata_authority_generation.generation();
        if self.signature.metadata_authority_generation != observed_generation {
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
        current_signature: &DfsRuntimeMessageServiceSignature,
        context: &crate::DfsMetadataReplicaShardContext,
    ) -> Result<Option<ResidentLocalMetadataShardHandle>, crate::DfsError> {
        // The caller has validated the current exact assignment/handoff.
        // Replica ingress borrows only its physical shard, not the service
        // shell's configuration view. Reuse the existing storage-lifecycle
        // fence so a provider-only delta cannot queue a global upgrade behind
        // another shard whose publication needs this acknowledgement.
        // General service/read admission retains its authority-generation fence.
        if !self.matches_runtime_owner(peer_snapshot, metadata_authority_generation)
            || !self.signature.shares_metadata_storage(current_signature)
        {
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

pub(crate) fn dfs_runtime_message_uses_content_read_lane(command: &DfsMessageCommand) -> bool {
    match &command.routed_message.message {
        DfsNetworkMessage::OriginalRepairHistoryRequest(_) => true,
        DfsNetworkMessage::ReadRequest(request) => {
            request.version.is_some() && request.content_hash.is_some()
        }
        DfsNetworkMessage::BulkReadRequest(_) => true,
        _ => false,
    }
}

// Repair replies have no wire shard. Only the host that still owns the exact
// original local call may supply their scoped admission; unpaired ingress keeps
// its existing global scope and current intent/policy validation.
pub(crate) fn dfs_runtime_message_supports_exact_service_lane(command: &DfsMessageCommand) -> bool {
    matches!(
        &command.routed_message.message,
        DfsNetworkMessage::MetadataJournalBatch(_)
            | DfsNetworkMessage::MetadataSnapshotInstall(_)
            | DfsNetworkMessage::RepairReplicaResponse(_)
    ) || matches!(
        &command.routed_message.message,
        DfsNetworkMessage::NamespaceRequest(namespace) if namespace.authority_shards.len() == 1
    )
}

pub(crate) fn dfs_runtime_message_cache() -> &'static DfsRuntimeMessageCache {
    DFS_RUNTIME_MESSAGE_CACHE.get_or_init(DfsRuntimeMessageCache::default)
}

pub(crate) fn dfs_detached_send_runtime_for_identity(
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

/// Resolve only the already registered Main carrier. A caller's transport can
/// have a private outbox; it cannot stand in for this actual native owner.
pub(crate) fn registered_repair_handoff_drain_gate(
    owner: &crate::runtime_host::DfsDataOwnerLock,
    node: &NodeId,
) -> Result<Arc<Mutex<()>>, crate::DfsError> {
    let error = || crate::DfsError::Metadata {
        message: "repair handoff has no unique live Main carrier owner".into(),
    };
    let data_dir = sinomo_core::local::SinomoDataDir::new(owner.physical_data_root());
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&ClusterPaths::new(&data_dir))
        .map_err(|_| error())?;
    if state.local_node_id != *node {
        return Err(error());
    }
    let registry = DFS_DETACHED_SEND_RUNTIMES
        .get()
        .ok_or_else(error)?
        .lock()
        .map_err(|_| error())?;
    let mut candidates = registry.iter().filter(|(key, _)| {
        key.cluster_id == state.cluster_id.to_string()
            && key.local_node_id == node.to_string()
            && key
                .data_root
                .canonicalize()
                .is_ok_and(|root| root == owner.physical_data_root())
    });
    let (_, runtime) = candidates.next().ok_or_else(error)?;
    if candidates.next().is_some() {
        return Err(error());
    }
    Ok(runtime.state.repair_handoff_drain_gate())
}

#[derive(Debug)]
pub(crate) struct DfsRuntimeMaintenanceServiceLane {
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

/// Global scheduler admission still owns this response. Reuse only the live
/// native fetch owner; durable reconstruction cannot supply this correlation.
pub(crate) fn existing_fetch_response_service_lane(
    candidates: &[(
        crate::DfsMetadataShardConfig,
        Arc<DfsRuntimeMessageServiceLaneHandle>,
    )],
    command: &DfsMessageCommand,
) -> Result<Option<Arc<DfsRuntimeMessageServiceLaneHandle>>, crate::DfsError> {
    let crate::DfsNetworkMessage::FetchBlobResponse(response) = &command.routed_message.message
    else {
        return Ok(None);
    };
    let source = &command.routed_message.source_node_id;
    if source != &response.source_node_id {
        return Err(crate::DfsError::Network {
            message: "DFS fetch response changed its authenticated source".into(),
        });
    }
    let mut owner = None;
    for (config, handle) in candidates {
        // The root cache lock has already been released. A busy read of this
        // actual owner waits under the original global admission/RPC budget;
        // it must not be mistaken for missing custody and reconstructed.
        let lane = handle.lock().map_err(|_| crate::DfsError::Network {
            message: "DFS runtime message lane lock poisoned".into(),
        })?;
        let Some(pending) = lane.service.writes.pending_writes.get(&response.intent_id) else {
            continue;
        };
        if pending.object_id != response.object_id
            || pending.version != response.version
            || matches!(&response.result, crate::DfsFetchBlobResult::Found { content_hash, .. }
                if content_hash != &pending.content_hash)
        {
            return Err(crate::DfsError::Metadata {
                message: "DFS fetch response changed the original pending object/version/hash"
                    .into(),
            });
        }
        // A retained exact request owns its late reply after scheduling expiry.
        if (!pending.fetch_inflight_from.is_empty()
            || !pending.fetch_inflight_request_ids.is_empty())
            && pending.fetch_inflight_request_ids.get(source) != Some(&response.request_id)
        {
            return Err(crate::DfsError::Network {
                message: "unexpected DFS fetch response source or request id".into(),
            });
        }
        // A completed fetch can leave this SAME pending write owning N-W
        // repair. Its duplicate stays here for the existing full source/body
        // validation and replay; clearing a fetch ID is not owner retirement.
        let Some(intent) = lane.service.coordinator.intent(&response.intent_id) else {
            return Err(crate::DfsError::Metadata {
                message: "live fetch owner lost its original intent".into(),
            });
        };
        if intent.object_id != pending.object_id || intent.target_version != pending.version {
            return Err(crate::DfsError::Metadata {
                message: "native fetch owner changed its original intent object/version".into(),
            });
        }
        if intent.file_system_id != config.file_system_id
            || config.primary_node_id != *lane.service.node_id()
            || intent.owner_epoch != config.epoch
        {
            return Err(crate::DfsError::StaleOwnerEpoch {
                file_system_id: intent.file_system_id.clone(),
                local_node_id: lane.service.node_id().clone(),
                primary_node_id: config.primary_node_id.clone(),
                epoch: config.epoch,
            });
        }
        if owner.is_some() {
            return Err(crate::DfsError::Metadata {
                message: "multiple native service lanes own the same fetch response".into(),
            });
        }
        owner = Some(Arc::clone(handle));
    }
    Ok(owner)
}

#[derive(Debug)]
pub(crate) struct DfsRuntimeMessageExecution {
    pub remote_namespace_phase: Option<(
        DfsMessageCommand,
        Box<crate::source_adoption::RemoteNamespacePhase>,
    )>,
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
    #[cfg(test)]
    pub new_repair_records_unattempted: bool,
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
pub(crate) fn execute_runtime_message_command_with_verified_fetch<F>(
    service_lane_handle: &Arc<DfsRuntimeMessageServiceLaneHandle>,
    peer_node_ids: Vec<NodeId>,
    command: DfsMessageCommand,
    preingested_blob: Option<crate::ContentStoreQuarantinedBlob>,
    execution_scope: &crate::DfsMessageExecutionScope,
    verified_fetch: Option<crate::service_fetch_transfer::VerifiedDfsBlobFetch>,
    verified_history: Option<crate::publication_guard::VerifiedOriginalRepairHistoryRead>,
    publication_owners: Option<Arc<crate::publication_guard::DfsPublicationExecutionOwners>>,
    publication_provider: Option<Arc<crate::DfsPublicationExecutionProvider>>,
    completed_phase: Option<crate::source_adoption::CompletedRemoteNamespacePhase>,
    persist_repair_records: F,
) -> Result<DfsRuntimeMessageExecution, crate::DfsError>
where
    F: FnOnce(Vec<crate::DfsRepairAttemptRecord>) -> Result<usize, crate::DfsError>,
{
    #[cfg(test)]
    if matches!(&command.routed_message.message,
        DfsNetworkMessage::SelectedLocalRecoveryFenceRequest(_) | DfsNetworkMessage::SelectedLocalRecoveryFenceResponse(_)) {
        return Err(crate::DfsError::Metadata { message: "selected local recovery fence cannot enter a generic runtime message lane".into() });
    }
    let completes_fetch = matches!(
        &command.routed_message.message,
        DfsNetworkMessage::FetchBlobResponse(_)
    );
    let is_verified_fetch = verified_fetch.is_some();
    let is_verified_history = verified_history.is_some();
    if matches!(&command.routed_message.message, DfsNetworkMessage::OriginalRepairHistoryRequest(_)) != is_verified_history
        || matches!(&command.routed_message.message, DfsNetworkMessage::OriginalRepairHistoryResponse(_))
        || (is_verified_history && verified_fetch.is_some())
        || (is_verified_history && (completed_phase.is_some() || publication_owners.is_some()
            || publication_provider.is_some()))
    { return Err(crate::DfsError::Metadata { message: "history request/context has no exact verified per-call owner".into() }); }
    let is_read_only = is_verified_fetch || crate::dfs_message_is_read_only_request(&command);
    let lane_lock_wait_started = std::time::Instant::now();
    let mut service_lane = service_lane_handle
        .lock()
        .map_err(|_| crate::DfsError::Network {
            message: "DFS runtime message lane lock poisoned".to_string(),
        })?;
    let lane_lock_wait = lane_lock_wait_started.elapsed();
    if is_verified_history && (service_lane.service.remote_namespace_phase.is_some()
        || service_lane.service.publication_execution.current().is_some()
        || service_lane.service.publication_provider.current().is_some()) {
        return Err(crate::DfsError::Metadata { message: "history read cannot consume a preexisting mutation/remote phase".into() });
    }
    if (is_verified_fetch || is_verified_history)
        && (service_lane.resident_metadata_scope.is_some()
            || preingested_blob.is_some()
            || !service_lane
                .service
                .writes
                .deferred_routed_create_parent_touches
                .is_empty())
    {
        return Err(crate::DfsError::Metadata {
            message: "immutable fetch requires a content-only lane without mutation callbacks"
                .to_string(),
        });
    }
    let _publication_scope =
        publication_owners.map(|owners| service_lane.service.publication_execution.install(owners));
    // Cached idle lanes must not pin the daemon's OS owner. The provider and
    // actual admission remain together only for this synchronous execution.
    let _publication_provider_scope = publication_provider
        .map(|provider| service_lane.service.publication_provider.install(provider));
    service_lane.refresh_peer_targets(peer_node_ids);
    let resident_metadata_scope = service_lane.resident_metadata_scope.clone();
    let backend_checkout_started = std::time::Instant::now();
    if let Some(scope) = resident_metadata_scope.as_ref() {
        scope.attach(&mut service_lane.service)?;
    }
    let backend_checkout = backend_checkout_started.elapsed();
    if let Some(completed) = completed_phase {
        service_lane
            .service
            .complete_remote_namespace_phase(completed)?;
    }
    // Diagnostic DATA from the SAME decoded reply; no attachment or getter.
    let repair_reply_observation = if tracing::enabled!(target: "sinomo.dfs.perf", tracing::Level::TRACE)
        && let crate::DfsNetworkMessage::RepairReplicaResponse(response) =
            &command.routed_message.message
    {
        Some((
            command.routed_message.source_node_id.clone(),
            command.routed_message.target_node_id.clone(),
            command.routed_message.frame_id,
            command.routed_message.message_id,
            response.clone(),
        ))
    } else {
        None
    };
    let resume_command = matches!(
        &command.routed_message.message,
        crate::DfsNetworkMessage::NamespaceRequest(_)
    )
    .then(|| command.clone());
    let process_started = std::time::Instant::now();
    let committed_work_tracker = crate::DfsCommittedWorkTracker::new();
    service_lane
        .service
        .set_committed_work_tracker(committed_work_tracker.clone());
    let mut process_report = match (verified_fetch, verified_history) {
        (None, Some(verified)) => service_lane.service.process_verified_original_history_report(command, verified),
        (Some(verified), None) => service_lane
            .service
            .process_verified_blob_fetch_report(command, verified),
        (None, None) => service_lane
            .service
            .process_message_command_report_with_preingested_blob(command, preingested_blob),
        (Some(_), Some(_)) => unreachable!("mutually exclusive verified request families"),
    };
    if let Some((source, target, frame_id, message_id, response)) = repair_reply_observation {
        match &process_report.outcome {
            Ok(result) => tracing::trace!(target: "sinomo.dfs.perf",
                source = %source, target = %target, frame_id, message_id,
                response = ?response, events = ?result.events,
                "normal Main decoded repair reply and actual process events"),
            Err(error) => tracing::trace!(target: "sinomo.dfs.perf",
                source = %source, target = %target, frame_id, message_id,
                response = ?response, error = %error,
                "normal Main decoded repair reply execution failed"),
        }
    }
    if is_verified_history && service_lane.service.remote_namespace_phase.is_some() {
        return Err(crate::DfsError::Metadata { message: "history handler cannot consume a remote namespace phase".into() });
    }
    let remote_namespace_phase = service_lane
        .service
        .remote_namespace_phase
        .take()
        .map(|phase| {
            (
                resume_command.expect("only native namespace commands yield a remote phase"),
                phase,
            )
        });
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
    // Main owns pending Fetch work and the terminal write reply emitted by its
    // Fetch response. Neither belongs to a private caller's inbox pump or to a
    // discarded background-handler envelope. The existing worker runs only
    // after this admission is released.
    let mut fetch_enqueue_error = None;
    if let Ok(result) = &mut process_report.outcome {
        result.response.outbound_messages.retain(|message| {
            let owned_followup = matches!(&message.message, DfsNetworkMessage::FetchBlob(_))
                || (completes_fetch
                    && matches!(&message.message,
                    DfsNetworkMessage::DistributedWriteResponse(response)
                        if !matches!(&response.result,
                            crate::DfsDistributedWriteResult::Outcome {
                                outcome: crate::NetworkedDfsWriteOutcome::Pending { .. }
                            })));
            if !owned_followup || fetch_enqueue_error.is_some() {
                return true;
            }
            match crate::DfsMessageTransport::send_detached_routed_message(
                &mut service_lane.service.transport,
                &message.target_node_id,
                message.clone(),
            ) {
                Ok(()) => false,
                Err(error) => {
                    fetch_enqueue_error = Some(crate::DfsError::from_network(error));
                    true
                }
            }
        });
    }
    if let Some(error) = fetch_enqueue_error {
        process_report.outcome = Err(error);
    }
    let deferred_work = if is_read_only && !is_verified_fetch {
        Ok(None)
    } else {
        service_lane
            .service
            .message_deferred_work_pending()
            .map(|pending| {
                pending.then(|| {
                    (
                        Arc::clone(service_lane_handle.detached_send_runtime()),
                        if is_verified_fetch {
                            // Only transport custody is detached. Parent touches
                            // and other mutation callbacks require their lane.
                            service_lane.service.prepare_background_replica_worker()
                        } else {
                            service_lane.service.detach_message_deferred_work()
                        },
                    )
                })
            })
    };
    let repair_queue_records = if is_read_only {
        Vec::new()
    } else {
        std::mem::take(&mut process_report.committed_repair_records)
    };
    #[cfg(test)]
    let new_repair_records_unattempted = repair_queue_records
        .iter()
        .all(|record| record.attempts == 0 && !record.in_flight);
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
        remote_namespace_phase,
        process_report,
        committed_work,
        replica_materialization_shard,
        deferred_work,
        #[cfg(test)]
        new_repair_records_unattempted,
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
    pub fn refresh_peer_targets(&mut self, peer_node_ids: Vec<sinomo_core::node::NodeId>) {
        if self.peer_node_ids == peer_node_ids {
            return;
        }
        self.service
            .refresh_message_peer_targets(peer_node_ids.clone());
        self.peer_node_ids = peer_node_ids;
    }
}

pub(crate) fn dfs_network_message_kind(message: &DfsNetworkMessage) -> &'static str {
    match message {
        #[cfg(test)]
        DfsNetworkMessage::SelectedLocalRecoveryFenceRequest(_) => "selected-local-recovery-fence-request",
        #[cfg(test)]
        DfsNetworkMessage::SelectedLocalRecoveryFenceResponse(_) => "selected-local-recovery-fence-response",
        DfsNetworkMessage::DistributedWriteRequest(_) => "distributed-write-request",
        DfsNetworkMessage::DistributedWriteResponse(_) => "distributed-write-response",
        DfsNetworkMessage::NamespaceRequest(_) => "namespace-request",
        DfsNetworkMessage::NamespaceResponse(_) => "namespace-response",
        DfsNetworkMessage::MetadataJournalBatch(_) => "metadata-journal-batch",
        DfsNetworkMessage::MetadataJournalBatchResponse(_) => "metadata-journal-batch-response",
        DfsNetworkMessage::StageBlob(_) => "stage-blob",
        DfsNetworkMessage::StageRetainedBlob(_) => "stage-retained-blob",
        DfsNetworkMessage::RepairRetainedBlob(_) => "repair-retained-blob",
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
#[cfg(test)]
pub(crate) async fn ingest_dfs_message_attachment(
    content: Option<LocalContentStore>,
    command: DfsMessageCommand,
    attachment: Option<DfsInboundAttachment>,
) -> Result<
    (
        DfsMessageCommand,
        Option<crate::ContentStoreQuarantinedBlob>,
    ),
    crate::DfsError,
> {
    ingest_dfs_message_attachment_with_owner(
        content,
        None,
        command,
        attachment,
        (),
        crate::current_dfs_traceparent(),
    )
    .await
}

/// Retain the embedding runtime's existing storage-owner capability inside the
/// blocking ingest worker. Dropping the RPC future closes the input channel;
/// its worker still owns that capability through cleanup or finalization.
pub(crate) async fn ingest_dfs_message_attachment_with_owner<G: Send + 'static>(
    content: Option<LocalContentStore>,
    snapshot_root: Option<std::path::PathBuf>,
    mut command: DfsMessageCommand,
    attachment: Option<DfsInboundAttachment>,
    ingest_owner: G,
    traceparent: Option<String>,
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
    if let DfsNetworkMessage::MetadataSnapshotInstall(snapshot) =
        &mut command.routed_message.message
    {
        let root = snapshot_root.ok_or_else(|| crate::DfsError::Distributed {
            message: "native snapshot receive has no actual owned root".into(),
        })?;
        snapshot.image = Some(
            ingest_native_snapshot_attachment(
                root,
                snapshot.snapshot.clone(),
                attachment,
                ingest_owner,
                traceparent,
            )
            .await?,
        );
        return Ok((command, None));
    }
    let content = content.ok_or_else(|| crate::DfsError::Distributed {
        message: "streamed DFS attachment is missing its shared content store".to_string(),
    })?;
    let (expected_hash, stage_blob) = match &command.routed_message.message {
        DfsNetworkMessage::StageBlob(stage)
        | DfsNetworkMessage::StageRetainedBlob(stage)
        | DfsNetworkMessage::RepairRetainedBlob(stage) => (Some(stage.content_hash.clone()), true),
        DfsNetworkMessage::DistributedWriteRequest(_)
        | DfsNetworkMessage::ExclusiveCreateRequest(_)
        | DfsNetworkMessage::RetainedWriteRequest(_) => {
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
    let ingest_span = tracing::trace_span!(target: "sinomo.perf", "dfs.message.attachment_ingest",
        stage = "dfs.message.attachment_ingest",
        traceparent = traceparent.as_deref().unwrap_or_default());
    let ingest_worker = tokio::task::spawn_blocking(move || {
        let _entered = ingest_span.enter();
        let result: Result<_, crate::DfsError> =
            crate::with_dfs_traceparent(traceparent.as_deref(), || {
                let mut ingest = content
                    .begin_quarantined_blob_ingest(total_size_bytes, expected_hash.as_deref())?;
                while let Some(chunk) = chunk_receiver.blocking_recv() {
                    ingest.write_chunk(chunk.as_ref())?;
                }
                ingest.finish_quarantined().map_err(Into::into)
            });
        // If the JoinHandle was abandoned, drop the returned quarantine before
        // its owner capability. The tuple also keeps ownership through delivery.
        (result, ingest_owner)
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
    let (ingest_result, _ingest_owner) =
        ingest_worker
            .await
            .map_err(|source| crate::DfsError::Network {
                message: format!("DFS streamed attachment task failed: {source}"),
            })?;
    feed_result?;
    let blob = ingest_result?;
    if !stage_blob {
        match &mut command.routed_message.message {
            DfsNetworkMessage::DistributedWriteRequest(write)
            | DfsNetworkMessage::ExclusiveCreateRequest(write)
            | DfsNetworkMessage::RetainedWriteRequest(write) => {
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

/// Stream the sole native record image through the SAME owned/chunk worker.
/// It stages DATA only; the normal replica admission/permit path still owns
/// installation and revalidates current source/config/handoff after this wait.
pub(crate) async fn ingest_native_snapshot_attachment<G: Send + 'static>(
    root: std::path::PathBuf,
    descriptor: crate::DfsMetadataSnapshotImageDescriptor,
    attachment: DfsInboundAttachment,
    ingest_owner: G,
    traceparent: Option<String>,
) -> Result<crate::DfsMetadataSnapshotImage, crate::DfsError> {
    descriptor.validate()?;
    if attachment.descriptor.total_size_bytes != descriptor.total_size_bytes {
        return Err(crate::DfsError::Distributed {
            message: "native snapshot attachment/head length differs".into(),
        });
    }
    crate::body_rpc::validate_incoming_attachment_length(
        &attachment.descriptor,
        &attachment.body,
        "native snapshot",
    )
    .map_err(crate::DfsError::from_network)?;
    let mut body = attachment.body;
    let (sender, mut receiver) =
        tokio::sync::mpsc::channel::<ObjectRpcBodyChunk>(DFS_ATTACHMENT_INGEST_BUFFERED_CHUNKS);
    let worker = tokio::task::spawn_blocking(move || {
        let result = crate::with_dfs_traceparent(traceparent.as_deref(), || {
            let mut ingest = crate::DfsMetadataSnapshotImage::begin_received(&root, descriptor)?;
            while let Some(chunk) = receiver.blocking_recv() {
                ingest.write_chunk(chunk.as_ref())?;
            }
            ingest.finish()
        });
        // The actual root owner survives cancellation and image cleanup.
        (result, ingest_owner)
    });
    let feed = loop {
        match body.next_chunk().await {
            Ok(Some(chunk)) => {
                if sender.send(chunk).await.is_err() {
                    break Ok(());
                }
            }
            Ok(None) => break Ok(()),
            Err(source) => {
                break Err(crate::DfsError::Network {
                    message: format!("read native snapshot image attachment: {source}"),
                });
            }
        }
    };
    drop(sender);
    let (result, _owner) = worker.await.map_err(|source| crate::DfsError::Network {
        message: format!("native snapshot image task failed: {source}"),
    })?;
    feed?;
    Ok(result?)
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
                        committed_work: Default::default(),
                        result,
                        synced_path_object: None,
                        synced_path_objects: Vec::new(),
                        metadata_journal_batches: Vec::new(),
                        metadata_journal_has_more: false,
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
        assert!(effects.storage_environment);
    }
}

#[cfg(test)]
mod message_cache_registry_tests {
    use super::*;

    #[test]
    fn empty_queued_root_slot_survives_eviction_without_a_parallel_cache() {
        let cache = DfsRuntimeMessageCache::default();
        let root = Path::new("pure-cache-root");
        let current = cache.root(root).unwrap();
        let queued = Arc::clone(&current);
        assert!(current.lock().unwrap().is_none());
        assert!(!cache.evict(root).unwrap());
        let next = cache.root(root).unwrap();
        assert!(Arc::ptr_eq(&queued, &next));
        drop(next);
        drop(queued);
        drop(current);
        assert!(!cache.evict(root).unwrap());
        assert!(cache.existing_root(root).unwrap().is_none());
    }

    #[test]
    fn empty_root_slot_poison_is_isolated_and_never_recovered_as_ready() {
        let cache = DfsRuntimeMessageCache::default();
        let first = cache.root(Path::new("pure-poisoned-root")).unwrap();
        let doomed = Arc::clone(&first);
        let panicked = std::thread::spawn(move || {
            let _empty = doomed.lock().unwrap();
            panic!("negative fault poisons only this empty DATA cache slot");
        })
        .join();
        assert!(panicked.is_err());
        assert!(first.lock().is_err());
        assert!(cache.evict(Path::new("pure-poisoned-root")).is_err());
        let other = cache.root(Path::new("pure-unrelated-root")).unwrap();
        assert!(other.lock().unwrap().is_none());
    }
}
